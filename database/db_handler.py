# database/db_handler.py
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, date
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .db_schema import create_all


# --------------------------
# helpers
# --------------------------

def _date_str(d: Any) -> str:
    if d is None:
        return datetime.utcnow().date().isoformat()
    if isinstance(d, date) and not isinstance(d, datetime):
        return d.isoformat()
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, str) and len(d) >= 10:
        return d[:10]
    return str(d)[:10]


_ws_re = re.compile(r"\s+")
_punct_re = re.compile(r"[^\w\u4e00-\u9fff]+")


def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip().lower()
    s = _ws_re.sub(" ", s)
    s = _punct_re.sub(" ", s)
    s = _ws_re.sub(" ", s).strip()
    return s


def tokenize(s: str) -> List[str]:
    s = normalize_text(s)
    return [t for t in s.split(" ") if t] if s else []


def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def normalize_status(platform: str, raw_status: str) -> str:
    """
    Normalize platform statuses into:
      - 连载
      - 完本
    """
    s = (raw_status or "").strip()
    if not s:
        return "连载"

    if platform == "qidian":
        # 起点：连载 / 完本
        if "完" in s:
            return "完本"
        return "连载"

    if platform == "fanqie":
        # 番茄：连载中 / 已完结
        if "完结" in s:
            return "完本"
        return "连载"

    # fallback heuristic
    if any(k in s for k in ["完结", "完本", "已完结"]):
        return "完本"
    return "连载"


# --------------------------
# payload
# --------------------------

@dataclass(frozen=True)
class NovelPayload:
    platform: str
    platform_novel_id: str

    title: str
    author: str
    intro: str
    main_category: str
    tags: List[str]

    url: str = ""

    status: str = "ongoing"
    total_words: int = 0

    rank: int = -1

    # platform-specific metric for rankings
    total_recommend: Optional[int] = None   # qidian
    reading_count: Optional[int] = None     # fanqie

    extra: Dict[str, Any] = None

    # optional for dedup accuracy
    opening_chapters: Optional[List[Dict[str, Any]]] = None  # chapter_title helps


# --------------------------
# handler
# --------------------------

class DatabaseHandler:
    """
    Phase1 handler (date-only snapshots):

    - novels: canonical entity AND platform ownership (exclusive across platforms)
    - novel_titles: rename history / aliases
    - tags + novel_tag_map: unified fine-grained topics
    - rank_lists / rank_snapshots / rank_entries: daily rankings
    - first_n_chapters: opening N chapters stored per novel_uid

    Key invariants:
      - One novel belongs to exactly one platform (platform, platform_novel_id is unique).
    """

    def __init__(self, db_path: str, *, is_test: bool = False, logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.is_test = is_test
        self.logger = logger or logging.getLogger("DatabaseHandler")
        self._lock = threading.RLock()
        self._init_db()

    # ---------- connection / tx ----------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        conn.execute("PRAGMA cache_size = -20000;")
        return conn

    @contextmanager
    def _tx(self, *, immediate: bool = True):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE;" if immediate else "BEGIN;")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _run_with_retry(self, fn, *, max_retries: int = 5, base_sleep: float = 0.15):
        for attempt in range(max_retries):
            try:
                return fn()
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if ("locked" in msg or "busy" in msg) and attempt < max_retries - 1:
                    time.sleep(base_sleep * (2 ** attempt))
                    continue
                raise

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)

        def _do():
            conn = self._connect()
            try:
                create_all(conn, drop=self.is_test)
                conn.commit()
            finally:
                conn.close()

        self._run_with_retry(_do)

    # ---------- normalize input ----------

    def normalize_payload(self, raw: Dict[str, Any], *, platform: str) -> NovelPayload:
        platform_novel_id = (raw.get("novel_id") or raw.get("platform_novel_id") or "").strip()
        title = (raw.get("title") or "").strip()
        if not platform_novel_id or not title:
            raise ValueError(f"Missing platform novel_id/title: {raw}")

        author = (raw.get("author") or "").strip()
        intro = (raw.get("introduction") or raw.get("intro") or "").strip()
        main_category = (raw.get("main_category") or raw.get("category") or "").strip()

        tags = raw.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in re.split(r"[,\|/；;、\s]+", tags) if t.strip()]
        elif isinstance(tags, list):
            tags = [str(t).strip() for t in tags if str(t).strip()]
        else:
            tags = []

        # still allow passing qidian sub_category; treat as tag
        sub_cat = (raw.get("sub_category") or "").strip()
        if sub_cat and sub_cat not in tags:
            tags.append(sub_cat)

        url = (raw.get("url") or raw.get("novel_url") or "").strip()

        raw_status = (raw.get("status") or "").strip()
        status = normalize_status(platform, raw_status)

        total_words = safe_int(raw.get("total_words"), default=0)

        rank = safe_int(raw.get("rank"), default=-1)

        # platform-specific ranking metric
        total_recommend = raw.get("total_recommend")
        if total_recommend is not None:
            total_recommend = safe_int(total_recommend, default=0)

        reading_count = raw.get("reading_count")
        if reading_count is not None:
            reading_count = safe_int(reading_count, default=0)

        extra = raw.get("extra") if isinstance(raw.get("extra"), dict) else {}

        opening_chapters = raw.get("opening_chapters")
        if opening_chapters is not None and not isinstance(opening_chapters, list):
            opening_chapters = None

        return NovelPayload(
            platform=platform,
            platform_novel_id=platform_novel_id,
            title=title,
            author=author,
            intro=intro,
            main_category=main_category,
            tags=tags,
            url=url,
            status=status,
            total_words=total_words,
            rank=rank,
            total_recommend=total_recommend,
            reading_count=reading_count,
            extra=extra,
            opening_chapters=opening_chapters,
        )

    # --------------------------
    # canonical novel resolution
    # --------------------------

    @staticmethod
    def _intro_similarity(a_norm: str, b_norm: str) -> float:
        ta, tb = tokenize(a_norm), tokenize(b_norm)
        jac = jaccard(ta, tb)
        seq = SequenceMatcher(None, a_norm, b_norm).ratio() if a_norm and b_norm else 0.0
        return 0.65 * jac + 0.35 * seq

    @staticmethod
    def _title_similarity(title: str, existing_title_norms: List[str]) -> float:
        if not title or not existing_title_norms:
            return 0.0
        tn = normalize_text(title)
        best = 0.0
        for t in existing_title_norms:
            best = max(best, SequenceMatcher(None, tn, t).ratio())
        return best

    @staticmethod
    def _chapter_title_sig(opening_chapters: Optional[List[Dict[str, Any]]], k: int = 5) -> List[str]:
        if not opening_chapters:
            return []
        out = []
        for ch in opening_chapters[:k]:
            t = (ch.get("chapter_title") or "").strip()
            if t:
                out.append(normalize_text(t))
        return out

    @staticmethod
    def _chapter_sig_similarity(sig_a: List[str], sig_b: List[str]) -> float:
        if not sig_a or not sig_b:
            return 0.0
        return jaccard(sig_a, sig_b)

    def _candidate_novels(self, conn: sqlite3.Connection, platform: str, author_norm: str) -> List[sqlite3.Row]:
        # Keep candidates inside same platform (since cross-platform cannot be same novel)
        if not author_norm:
            return conn.execute(
                "SELECT * FROM novels WHERE platform=? ORDER BY last_seen_date DESC LIMIT 200",
                (platform,),
            ).fetchall()

        return conn.execute(
            "SELECT * FROM novels WHERE platform=? AND author_norm=? ORDER BY last_seen_date DESC LIMIT 300",
            (platform, author_norm),
        ).fetchall()

    def resolve_novel_uid(
        self,
        conn: sqlite3.Connection,
        *,
        payload: NovelPayload,
        snapshot_date: str,
        threshold: float = 0.84,
    ) -> int:
        """
        Resolution order:
          1) If (platform, platform_novel_id) already exists -> same novel_uid (hard identity).
          2) Otherwise, try to match within the same platform using author+intro+optional chapters/titles.
             If best score >= threshold -> merge (treat as same novel, likely due to platform ID change edge-case).
          3) Else create new novel.
        """
        # 1) hard identity
        row = conn.execute(
            "SELECT novel_uid FROM novels WHERE platform=? AND platform_novel_id=?",
            (payload.platform, payload.platform_novel_id),
        ).fetchone()
        if row:
            novel_uid = int(row["novel_uid"])
            self._update_novel_core(conn, novel_uid=novel_uid, payload=payload, last_seen_date=snapshot_date)
            return novel_uid

        # 2) similarity match (rare but keep for safety: platform id changes / scrape inconsistency)
        author_norm = normalize_text(payload.author)
        intro_norm = normalize_text(payload.intro)
        incoming_sig = self._chapter_title_sig(payload.opening_chapters, k=5)

        best_uid = None
        best_score = -1.0

        for n in self._candidate_novels(conn, payload.platform, author_norm):
            n_intro_norm = n["intro_norm"] or ""
            n_main = n["main_category"] or ""

            title_rows = conn.execute(
                "SELECT title_norm FROM novel_titles WHERE novel_uid=? LIMIT 25",
                (n["novel_uid"],),
            ).fetchall()
            existing_title_norms = [r["title_norm"] for r in title_rows]

            sig = {}
            try:
                sig = json.loads(n["signature_json"] or "{}")
            except Exception:
                sig = {}
            stored_sig = sig.get("chapter_title_sig") or []

            intro_sim = self._intro_similarity(intro_norm, n_intro_norm)
            ch_sim = self._chapter_sig_similarity(incoming_sig, stored_sig)
            title_sim = self._title_similarity(payload.title, existing_title_norms)

            score = 0.65 * intro_sim + 0.25 * ch_sim + 0.10 * title_sim
            if payload.main_category and n_main and payload.main_category == n_main:
                score += 0.03

            if score > best_score:
                best_score = score
                best_uid = int(n["novel_uid"])

        if best_uid is not None and best_score >= threshold:
            # merge: update core; BUT keep platform_novel_id from payload (new identity)
            self._adopt_new_platform_id(conn, novel_uid=best_uid, platform_novel_id=payload.platform_novel_id)
            self._update_novel_core(conn, novel_uid=best_uid, payload=payload, last_seen_date=snapshot_date)
            return best_uid

        # 3) create
        return self._create_novel(conn, payload=payload, snapshot_date=snapshot_date)

    def _create_novel(self, conn: sqlite3.Connection, *, payload: NovelPayload, snapshot_date: str) -> int:
        author_norm = normalize_text(payload.author)
        intro_norm = normalize_text(payload.intro)
        ch_sig = self._chapter_title_sig(payload.opening_chapters, k=5)

        sig = {
            "intro_sha1": sha1_hex(intro_norm) if intro_norm else "",
            "chapter_title_sig": ch_sig[:5] if ch_sig else [],
        }

        cur = conn.execute(
            """
            INSERT INTO novels(
                platform, platform_novel_id,
                author, author_norm, intro, intro_norm,
                main_category, status, total_words, url,
                signature_json,
                created_date, last_seen_date
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.platform,
                payload.platform_novel_id,
                payload.author,
                author_norm,
                payload.intro,
                intro_norm,
                payload.main_category,
                payload.status,
                payload.total_words,
                payload.url or "",
                json.dumps(sig, ensure_ascii=False),
                snapshot_date,
                snapshot_date,
            ),
        )
        return int(cur.lastrowid)

    def _update_novel_core(self, conn: sqlite3.Connection, *, novel_uid: int, payload: NovelPayload, last_seen_date: str) -> None:
        # Merge signatures conservatively
        row = conn.execute("SELECT signature_json FROM novels WHERE novel_uid=?", (novel_uid,)).fetchone()
        sig = {}
        if row:
            try:
                sig = json.loads(row["signature_json"] or "{}")
            except Exception:
                sig = {}

        intro_norm = normalize_text(payload.intro)
        if intro_norm and not sig.get("intro_sha1"):
            sig["intro_sha1"] = sha1_hex(intro_norm)

        incoming_ch_sig = self._chapter_title_sig(payload.opening_chapters, k=5)
        if incoming_ch_sig and not sig.get("chapter_title_sig"):
            sig["chapter_title_sig"] = incoming_ch_sig[:5]

        conn.execute(
            """
            UPDATE novels
            SET author=?,
                author_norm=?,
                intro=?,
                intro_norm=?,
                main_category=CASE WHEN ?!='' THEN ? ELSE main_category END,
                status=?,
                total_words=CASE WHEN ?>0 THEN ? ELSE total_words END,
                url=CASE WHEN ?!='' THEN ? ELSE url END,
                signature_json=?,
                last_seen_date=?
            WHERE novel_uid=?
            """,
            (
                payload.author,
                normalize_text(payload.author),
                payload.intro,
                intro_norm,
                payload.main_category,
                payload.main_category,
                payload.status,
                payload.total_words,
                payload.total_words,
                payload.url,
                payload.url,
                json.dumps(sig, ensure_ascii=False),
                last_seen_date,
                novel_uid,
            ),
        )

    def _adopt_new_platform_id(self, conn: sqlite3.Connection, *, novel_uid: int, platform_novel_id: str) -> None:
        """
        Edge-case support: platform changes internal id (rare).
        Since (platform, platform_novel_id) is UNIQUE, we update the canonical row.
        """
        conn.execute(
            "UPDATE novels SET platform_novel_id=? WHERE novel_uid=?",
            (platform_novel_id, novel_uid),
        )

    # --------------------------
    # titles + tags upserts
    # --------------------------

    def _upsert_novel_title(self, conn: sqlite3.Connection, novel_uid: int, title: str, snapshot_date: str, *, make_primary: bool) -> None:
        title = (title or "").strip()
        if not title:
            return
        tn = normalize_text(title)

        conn.execute(
            """
            INSERT INTO novel_titles(novel_uid, title, title_norm, is_primary, first_seen_date, last_seen_date)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(novel_uid, title_norm) DO UPDATE SET
                last_seen_date=excluded.last_seen_date
            """,
            (novel_uid, title, tn, 1 if make_primary else 0, snapshot_date, snapshot_date),
        )

        if make_primary:
            conn.execute("UPDATE novel_titles SET is_primary=0 WHERE novel_uid=? AND title_norm!=?", (novel_uid, tn))
            conn.execute("UPDATE novel_titles SET is_primary=1 WHERE novel_uid=? AND title_norm=?", (novel_uid, tn))

    def _bulk_upsert_tags(self, conn: sqlite3.Connection, tag_names: Sequence[str]) -> Dict[str, int]:
        pairs = []
        for t in tag_names:
            t = (t or "").strip()
            if not t:
                continue
            tn = normalize_text(t)
            if tn:
                pairs.append((t, tn))

        norm_to_orig: Dict[str, str] = {}
        for orig, tn in pairs:
            if tn not in norm_to_orig:
                norm_to_orig[tn] = orig

        if not norm_to_orig:
            return {}

        conn.executemany(
            """
            INSERT INTO tags(tag_name, tag_norm)
            VALUES(?, ?)
            ON CONFLICT(tag_norm) DO UPDATE SET
                tag_name=excluded.tag_name
            """,
            [(orig, tn) for tn, orig in norm_to_orig.items()],
        )

        rows = conn.execute(
            f"SELECT tag_id, tag_norm FROM tags WHERE tag_norm IN ({','.join(['?']*len(norm_to_orig))})",
            tuple(norm_to_orig.keys()),
        ).fetchall()

        return {r["tag_norm"]: int(r["tag_id"]) for r in rows}

    def _upsert_novel_tags(self, conn: sqlite3.Connection, novel_uid: int, tag_ids: Sequence[int]) -> None:
        if not tag_ids:
            return
        conn.executemany(
            "INSERT OR IGNORE INTO novel_tag_map(novel_uid, tag_id) VALUES(?, ?)",
            [(novel_uid, tid) for tid in set(tag_ids)],
        )

    # --------------------------
    # rank list + snapshot upserts
    # --------------------------

    def _get_or_create_rank_list_id(
            self,
            conn: sqlite3.Connection,
            *,
            platform: str,
            rank_family: str,
            rank_sub_cat: str,
            source_url: str,
    ) -> int:
        conn.execute(
            """
            INSERT INTO rank_lists(platform, rank_family, rank_sub_cat, source_url)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(platform, rank_family, rank_sub_cat) DO UPDATE SET
                source_url=CASE WHEN excluded.source_url!='' THEN excluded.source_url ELSE rank_lists.source_url END
            """,
            (platform, rank_family, rank_sub_cat or "", source_url or ""),
        )
        row = conn.execute(
            """
            SELECT rank_list_id FROM rank_lists
            WHERE platform=? AND rank_family=? AND rank_sub_cat=?
            """,
            (platform, rank_family, rank_sub_cat or ""),
        ).fetchone()
        return int(row["rank_list_id"])

    def _get_or_create_snapshot_id(self, conn: sqlite3.Connection, rank_list_id: int, snapshot_date: str, item_count: int) -> int:
        conn.execute(
            """
            INSERT INTO rank_snapshots(rank_list_id, snapshot_date, item_count)
            VALUES(?, ?, ?)
            ON CONFLICT(rank_list_id, snapshot_date) DO UPDATE SET
                item_count=excluded.item_count
            """,
            (rank_list_id, snapshot_date, item_count),
        )
        row = conn.execute(
            "SELECT snapshot_id FROM rank_snapshots WHERE rank_list_id=? AND snapshot_date=?",
            (rank_list_id, snapshot_date),
        ).fetchone()
        return int(row["snapshot_id"])

    # --------------------------
    # public: save rank snapshot
    # --------------------------

    def save_rank_snapshot(
            self,
            *,
            platform: str,
            rank_family: str,
            snapshot_date: Any,
            items: Sequence[Dict[str, Any]],
            rank_sub_cat: str = "",  # only used for qidian 新书榜四类
            source_url: str = "",
            resolve_threshold: float = 0.84,
            make_title_primary: bool = False,
    ) -> int:
        """
        Save ONE daily snapshot for ONE rank list.

        Platform metric:
          - qidian uses total_recommend
          - fanqie uses reading_count
        """

        """番茄小说的rank_sub_cat必须为空"""
        if platform == "fanqie":
            rank_sub_cat = ""

        snap = _date_str(snapshot_date)

        payloads: List[NovelPayload] = []
        for it in items:
            payloads.append(self.normalize_payload(it, platform=platform))

        def _do() -> int:
            with self._tx(immediate=True) as conn:
                rank_list_id = self._get_or_create_rank_list_id(
                    conn,
                    platform=platform,
                    rank_family=rank_family,
                    rank_sub_cat=rank_sub_cat,
                    source_url=source_url,
                )

                snapshot_id = self._get_or_create_snapshot_id(conn, rank_list_id, snap, item_count=len(payloads))

                # bulk upsert all tags first
                all_tags: List[str] = []
                for p in payloads:
                    all_tags.extend(p.tags or [])
                tag_map = self._bulk_upsert_tags(conn, all_tags)  # tag_norm -> tag_id

                entry_rows = []

                for p in payloads:
                    novel_uid = self.resolve_novel_uid(
                        conn,
                        payload=p,
                        snapshot_date=snap,
                        threshold=resolve_threshold,
                    )

                    # title aliases
                    self._upsert_novel_title(conn, novel_uid, p.title, snap, make_primary=make_title_primary)

                    # tags
                    tag_ids = []
                    for t in (p.tags or []):
                        tid = tag_map.get(normalize_text(t))
                        if tid:
                            tag_ids.append(tid)
                    self._upsert_novel_tags(conn, novel_uid, tag_ids)

                    # rank entry row
                    if p.rank >= 0:
                        # set platform-specific metric columns
                        total_recommend = p.total_recommend if platform == "qidian" else None
                        reading_count = p.reading_count if platform == "fanqie" else None

                        entry_rows.append(
                            (
                                snapshot_id,
                                novel_uid,
                                p.rank,
                                total_recommend,
                                reading_count,
                                json.dumps(p.extra or {}, ensure_ascii=False),
                            )
                        )

                if entry_rows:
                    conn.executemany(
                        """
                        INSERT INTO rank_entries(snapshot_id, novel_uid, rank, total_recommend, reading_count, extra_json)
                        VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(snapshot_id, novel_uid) DO UPDATE SET
                            rank=excluded.rank,
                            total_recommend=excluded.total_recommend,
                            reading_count=excluded.reading_count,
                            extra_json=excluded.extra_json
                        """,
                        entry_rows,
                    )

                return len(entry_rows)

        return int(self._run_with_retry(_do))

    # --------------------------
    # public: upsert First_N_chapters
    # --------------------------

    def upsert_first_n_chapters(
        self,
        *,
        platform: str,
        platform_novel_id: str,
        publish_date: Any,
        chapters: Sequence[Dict[str, Any]],
        novel_fallback_fields: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Store First_N_chapters for a novel.
        If the novel row doesn't exist yet, we create a minimal novel record.

        novel_fallback_fields (optional):
          {title, author, intro, main_category, status, total_words, url, tags}
        """
        src = _date_str(publish_date)

        def _do() -> int:
            with self._tx(immediate=True) as conn:
                row = conn.execute(
                    "SELECT novel_uid FROM novels WHERE platform=? AND platform_novel_id=?",
                    (platform, platform_novel_id),
                ).fetchone()

                if row:
                    novel_uid = int(row["novel_uid"])
                else:
                    # create minimal novel if not exists
                    fb = novel_fallback_fields or {}
                    payload = self.normalize_payload(
                        {
                            "novel_id": platform_novel_id,
                            "title": fb.get("title") or "unknown",
                            "author": fb.get("author") or "",
                            "intro": fb.get("intro") or "",
                            "main_category": fb.get("main_category") or "",
                            "status": fb.get("status") or "",
                            "total_words": fb.get("total_words") or 0,
                            "url": fb.get("url") or "",
                            "tags": fb.get("tags") or [],
                            "opening_chapters": chapters[:5],  # help signature
                            "rank": -1,
                        },
                        platform=platform,
                    )
                    novel_uid = self._create_novel(conn, payload=payload, snapshot_date=src)
                    self._upsert_novel_title(conn, novel_uid, payload.title, src, make_primary=True)

                rows_to_insert = []
                for ch in chapters:
                    num = safe_int(ch.get("chapter_num"), default=-1)
                    title = (ch.get("chapter_title") or "").strip()
                    if num < 0 or not title:
                        continue
                    content = (ch.get("chapter_content") or "").strip()
                    url = (ch.get("chapter_url") or ch.get("url") or "").strip()
                    wc = safe_int(ch.get("word_count"), default=len(content))

                    h = sha1_hex(normalize_text(title) + "\n" + normalize_text(content[:4000])) if (title or content) else ""
                    rows_to_insert.append((novel_uid, num, title, content, url, wc, h, src))

                if not rows_to_insert:
                    return 0

                conn.executemany(
                    """
                    INSERT INTO first_n_chapters(
                        novel_uid, chapter_num, chapter_title, chapter_content, chapter_url,
                        word_count, content_hash, publish_date
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(novel_uid, chapter_num) DO UPDATE SET
                        chapter_title=excluded.chapter_title,
                        chapter_content=excluded.chapter_content,
                        chapter_url=excluded.chapter_url,
                        word_count=excluded.word_count,
                        content_hash=excluded.content_hash,
                        publish_date=excluded.publish_date
                    """,
                    rows_to_insert,
                )
                return len(rows_to_insert)

        return int(self._run_with_retry(_do))

    # --------------------------
    # public: query First_N_chapters
    # --------------------------

    def get_first_n_chapter_count(self, *, platform: str, platform_novel_id: str) -> int:
        """Return COUNT(*) of stored opening chapters for a novel."""
        if not platform or not platform_novel_id:
            return 0

        def _do() -> int:
            with self._tx(immediate=False) as conn:
                row = conn.execute(
                    "SELECT novel_uid FROM novels WHERE platform=? AND platform_novel_id=?",
                    (platform, platform_novel_id),
                ).fetchone()
                if not row:
                    return 0
                novel_uid = int(row["novel_uid"])
                c = conn.execute(
                    "SELECT COUNT(*) AS c FROM first_n_chapters WHERE novel_uid=?",
                    (novel_uid,),
                ).fetchone()
                return int(c["c"] or 0)

        return int(self._run_with_retry(_do))

    def get_first_n_chapter_max_num(self, *, platform: str, platform_novel_id: str) -> int:
        """Return MAX(chapter_num) stored for a novel."""
        if not platform or not platform_novel_id:
            return 0

        def _do() -> int:
            with self._tx(immediate=False) as conn:
                row = conn.execute(
                    "SELECT novel_uid FROM novels WHERE platform=? AND platform_novel_id=?",
                    (platform, platform_novel_id),
                ).fetchone()
                if not row:
                    return 0
                novel_uid = int(row["novel_uid"])
                r = conn.execute(
                    "SELECT MAX(chapter_num) AS m FROM first_n_chapters WHERE novel_uid=?",
                    (novel_uid,),
                ).fetchone()
                return int(r["m"] or 0)

        return int(self._run_with_retry(_do))


    # --------------------------
    # inspection
    # --------------------------

    def get_table_counts(self) -> Dict[str, int]:
        tables = [
            "novels",
            "novel_titles",
            "tags",
            "novel_tag_map",
            "rank_lists",
            "rank_snapshots",
            "rank_entries",
            "first_n_chapters",
        ]

        def _do():
            with self._lock:
                conn = self._connect()
                try:
                    return {t: int(conn.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"]) for t in tables}
                finally:
                    conn.close()

        return self._run_with_retry(_do)

    """获取小说的归一化标题"""
    def get_novel_title_norm(self, platform: str, platform_novel_id: str) -> Optional[str]:
        try:
            # 使用_tx上下文管理器来执行查询
            def query_func():
                with self._tx(immediate=False) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT nt.title_norm
                        FROM novels n
                        JOIN novel_titles nt ON n.novel_uid = nt.novel_uid
                        WHERE n.platform = ? 
                          AND n.platform_novel_id = ?
                          AND nt.is_primary = 1
                        LIMIT 1
                    """, (platform, platform_novel_id))
                    result = cursor.fetchone()
                    return result['title_norm'] if result else None

            # 使用重试机制执行查询
            title_norm = self._run_with_retry(query_func)

            if title_norm:
                self.logger.debug(f"获取归一化标题成功: {title_norm} (平台: {platform}, ID: {platform_novel_id})")
            else:
                self.logger.debug(f"未找到归一化标题 (平台: {platform}, ID: {platform_novel_id})")

            return title_norm

        except Exception as e:
            self.logger.error(f"获取归一化标题失败: {e}")
            import traceback
            self.logger.error(f"详细错误: {traceback.format_exc()}")
            return None