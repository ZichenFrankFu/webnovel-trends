"""
Fanqie Spider Test Suite (Refined)

Only 4 test modes are kept:
1) decryption   : font decryption module only (Fanqie-specific)
2) quick        : one rank -> first book -> detail metadata + first chapter (NO DB)
3) full         : one rank -> top N books -> metadata + first K chapters each (WRITE DB)
4) multi_ranks  : multiple ranks -> per-rank pipeline (WRITE DB)

Timing is recorded per major step for performance tuning:
- fetch_rank_list
- enrich_rank_items (detail metadata)
- fetch_first_n_chapters
- DB write (rank snapshot + chapters upsert)
"""

import os
import sys
import time
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import WEBSITES

# Shared test utilities
from base_test import (
    Timer,
    fmt_sec,
    print_header,
    print_hr,
    ensure_clean_dirs,
    init_db,
    print_db_counts,
    pick_first,
    safe_pid,
    safe_trunc,
    db_get_chapter_count,
    db_get_max_chapter_index,
)

# ------------------------------------------------------------------
# Config helpers (from config.py)
# ------------------------------------------------------------------

def _get_fanqie_site_cfg() -> Dict[str, Any]:
    try:
        cfg = WEBSITES.get("fanqie", {}) or {}
        return dict(cfg)
    except Exception:
        return {}

def _get_rank_choices() -> List[str]:
    cfg = _get_fanqie_site_cfg()
    rank_urls = cfg.get("rank_urls") or {}
    if isinstance(rank_urls, dict):
        return list(rank_urls.keys())
    return []

def _default_rank_key(choices: List[str]) -> str:
    prefer = ["阅读榜科幻末世", "阅读榜西方奇幻", "阅读榜都市高武", "新书榜科幻末世", "新书榜西方奇幻"]
    for k in prefer:
        if k in choices:
            return k
    return choices[0] if choices else ""

def _default_multirank_keys(choices: List[str]) -> str:
    prefer = ["阅读榜西方奇幻", "阅读榜科幻末世", "阅读榜都市高武"]
    picked = [k for k in prefer if k in choices]
    if not picked and choices:
        picked = choices[:3]
    return ",".join(picked)


# ------------------------------------------------------------------
# DB / Spider init
# ------------------------------------------------------------------

def _ensure_clean_dirs() -> str:
    """Remove only fanqie_test.db and ensure output dirs exist. Return db_path."""
    return ensure_clean_dirs(db_relpath=os.path.join("test_output", "fanqie_test.db"), remove_db=True)


def _init_test_db(db_path: str):
    return init_db(db_path, is_test=True)


def _init_spider(db: Any):
    from spiders.fanqie_spider import FanqieSpider
    site_cfg = _get_fanqie_site_cfg()
    return FanqieSpider(site_cfg, db)


# ------------------------------------------------------------------
# Core steps (timed)
# ------------------------------------------------------------------

def _step_fetch_rank(spider: Any, *, rank_key: str, pages: int) -> Tuple[List[Dict[str, Any]], float]:
    with Timer("fetch_rank_list") as t:
        items = spider.fetch_rank_list(rank_key, pages=pages)
    return items or [], t.elapsed


def _step_enrich(spider: Any, items: List[Dict[str, Any]], *, max_books: int) -> Tuple[List[Dict[str, Any]], float]:
    with Timer("enrich_rank_items") as t:
        enriched = spider.enrich_rank_items(
            items,
            max_books=max_books,
            fetch_detail=True,
            fetch_chapters=False,
            chapter_count=0,
        )
    return enriched or [], t.elapsed


def _step_fetch_chapters(spider: Any, *, novel_url: str, target_chapter_count: int) -> Tuple[List[Dict[str, Any]], float]:
    with Timer("fetch_first_n_chapters") as t:
        chapters = spider.fetch_first_n_chapters(novel_url, target_chapter_count=target_chapter_count)
    return chapters or [], t.elapsed


def _step_db_write_snapshot(db: Any, *, rank_key: str, snapshot_date: str, items: List[Dict[str, Any]]) -> Tuple[Optional[int], float]:
    with Timer("db_write_snapshot") as t:
        snapshot_id = db.save_rank_snapshot(
            platform="fanqie",
            rank_family="fanqie_rank",
            rank_sub_cat=rank_key,
            snapshot_date=snapshot_date,
            items=items,
            source_url=((_get_fanqie_site_cfg().get("rank_urls") or {}).get(rank_key, "")),
            make_title_primary=True,
        )
    return snapshot_id, t.elapsed


def _step_db_upsert_chapters(
    db: Any,
    *,
    book: Dict[str, Any],
    chapters: List[Dict[str, Any]],
    snapshot_date: str
) -> float:
    if not chapters:
        return 0.0
    if not hasattr(db, "upsert_first_n_chapters"):
        return 0.0

    pid = safe_pid(book)
    if not pid:
        return 0.0

    publish_date = chapters[0].get("publish_date") or snapshot_date

    with Timer("db_upsert_first_n_chapters") as t:
        db.upsert_first_n_chapters(
            platform="fanqie",
            platform_novel_id=pid,
            publish_date=publish_date,
            chapters=chapters,
            novel_fallback_fields={
                "title": book.get("title", ""),
                "author": book.get("author", ""),
                "intro": book.get("intro", ""),
                "main_category": book.get("main_category", ""),
                "status": book.get("status", ""),
                "total_words": book.get("total_words", 0),
                "url": book.get("url", ""),
                "tags": book.get("tags", []),
            },
        )
    return t.elapsed


# ------------------------------------------------------------------
# Test modes
# ------------------------------------------------------------------

def run_decryption(*, verbose: bool = False) -> None:
    print_header("[Test] decryption - 字体解密模块")
    spider = None
    try:
        spider = _init_spider(db=None)
        if hasattr(spider, "_test_decryption_module"):
            spider._test_decryption_module(verbose=verbose)  # optional hook
            return

        # Fallback: call internal decrypt on a known snippet if available
        sample = "12345"
        if hasattr(spider, "_decrypt_text"):
            out = spider._decrypt_text(sample)
            print(f"[decrypt] '{sample}' -> '{out}'")
        else:
            print("[warn] spider has no _decrypt_text / _test_decryption_module")
    finally:
        try:
            if spider:
                spider.close()
        except Exception:
            pass


def run_quick(*, rank_key: str, pages: int, top_n: int, chapter_n: int, verbose: bool = False) -> None:
    """
    Quick smoke test (NO DB):
    - fetch one rank
    - pick first book
    - fetch detail metadata
    - fetch first chapter
    """
    print_header(
        "[Test] quick - 单榜单第1本 + detail + 第1章（不写入 fanqie_test DB）",
        params={"rank_key": rank_key, "pages": pages, "top_n": top_n, "chapter_n": chapter_n},
    )

    spider = None
    try:
        spider = _init_spider(db=None)

        items, t_rank = _step_fetch_rank(spider, rank_key=rank_key, pages=pages)
        print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | items={len(items)}")

        book = pick_first(items[:top_n])
        if not book:
            print("[结果] 未获取到榜单作品")
            return

        pid = safe_pid(book)
        url = book.get("url", "")
        print(f"\n[样本] 《{book.get('title','') }》 pid={pid} url={url}")

        with Timer("fetch_novel_detail") as t:
            detail = spider.fetch_novel_detail(url, pid, seed=book)
        print(f"[计时] fetch_novel_detail: {fmt_sec(t.elapsed)}")

        chapters, t_ch = _step_fetch_chapters(spider, novel_url=url, target_chapter_count=chapter_n)
        print(f"[计时] fetch_first_n_chapters: {fmt_sec(t_ch)} | chapters={len(chapters)}")

        # Preview
        print("\n[结果预览] detail metadata")
        for k in ["title", "author", "main_category", "status", "total_words", "intro"]:
            v = detail.get(k)
            if k == "intro":
                v = safe_trunc(v, 120)
            print(f"  - {k}: {v}")

        if chapters:
            c0 = chapters[0]
            print("\n[结果预览] chapter #1")
            print(f"  - title: {c0.get('title')}")
            txt = (c0.get("content") or "")
            print(f"  - content(sample): {safe_trunc(txt, 180)}")

    finally:
        try:
            if spider:
                spider.close()
        except Exception:
            pass


def run_full(*, rank_key: str, pages: int, top_n: int, chapter_n: int, verbose: bool = False) -> None:
    """
    Full DB-backed test:
    - fetch rank list
    - enrich metadata for top_n
    - write rank snapshot
    - fetch first chapter_n chapters per book and upsert into DB
    """
    print_header(
        "[Test] full - 单榜单 top_n 本 + metadata + 前 chapter_n 章（写入 fanqie_test DB）",
        params={"rank_key": rank_key, "pages": pages, "top_n": top_n, "chapter_n": chapter_n},
    )

    db_path = _ensure_clean_dirs()
    db = _init_test_db(db_path)
    spider = None
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    try:
        spider = _init_spider(db=db)

        # 1) fetch rank list
        items, t_rank = _step_fetch_rank(spider, rank_key=rank_key, pages=pages)
        items = (items or [])[:top_n]
        print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | picked={len(items)}")

        if not items:
            print("[结果] 未获取到榜单作品")
            return

        # 2) enrich metadata
        enriched, t_enrich = _step_enrich(spider, items, max_books=top_n)
        print(f"[计时] enrich_rank_items: {fmt_sec(t_enrich)} | enriched={len(enriched)}")

        # 3) write snapshot
        snapshot_id, t_db_snap = _step_db_write_snapshot(db, rank_key=rank_key, snapshot_date=snapshot_date, items=enriched)
        print(f"[计时] db_write_snapshot: {fmt_sec(t_db_snap)} | snapshot_id={snapshot_id}")

        # 4) fetch chapters + upsert
        total_ch_fetch = 0.0
        total_ch_db = 0.0
        for i, b in enumerate(enriched, 1):
            url = b.get("url", "")
            title = (b.get("title") or "")[:40]
            if not url:
                print(f"[跳过] #{i} 《{title}》缺少 url")
                continue

            chapters, t_ch = _step_fetch_chapters(spider, novel_url=url, target_chapter_count=chapter_n)
            total_ch_fetch += t_ch
            print(f"[计时] fetch_first_n_chapters #{i}: {fmt_sec(t_ch)} | chapters={len(chapters)} | 《{title}》")

            t_db_ch = _step_db_upsert_chapters(db, book=b, chapters=chapters, snapshot_date=snapshot_date)
            total_ch_db += t_db_ch
            if t_db_ch > 0:
                print(f"[计时] db_upsert_first_n_chapters #{i}: {fmt_sec(t_db_ch)} | 《{title}》")

            if i < len(enriched):
                time.sleep(1.5)

        print("\n[计时汇总]")
        print(f"  - fetch_rank_list        : {fmt_sec(t_rank)}")
        print(f"  - enrich_rank_items      : {fmt_sec(t_enrich)}")
        print(f"  - db_write_snapshot      : {fmt_sec(t_db_snap)}")
        print(f"  - fetch_first_n_chapters : {fmt_sec(total_ch_fetch)} (sum)")
        print(f"  - db_upsert_chapters     : {fmt_sec(total_ch_db)} (sum)")

        print_db_counts(db)
        print(f"\n[输出] 数据库文件: {db_path}")

    finally:
        try:
            if spider:
                spider.close()
        except Exception:
            pass


def run_multi_ranks(*, rank_keys: List[str], pages: int, top_n: int, chapter_n: int, verbose: bool = False) -> None:
    """
    Multi-rank DB-backed test:
    - loop ranks -> fetch + enrich + snapshot write (+ optional chapters if chapter_n>0)
    """
    print_header(
        "[Test] multi_ranks - 多榜单循环（写入 fanqie_test DB）",
        params={"rank_keys": rank_keys, "pages": pages, "top_n(per rank)": top_n, "chapter_n": chapter_n},
    )

    db_path = _ensure_clean_dirs()
    db = _init_test_db(db_path)
    spider = None
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    grand = {
        "fetch_rank": 0.0,
        "enrich": 0.0,
        "db_snapshot": 0.0,
        "chapters_fetch": 0.0,
        "chapters_db": 0.0,
    }

    try:
        spider = _init_spider(db=db)

        for idx, rk in enumerate(rank_keys, 1):
            print("\n" + "-" * 80)
            print(f"[{idx}/{len(rank_keys)}] rank_key={rk}")

            items, t_rank = _step_fetch_rank(spider, rank_key=rk, pages=pages)
            items = (items or [])[:top_n]
            grand["fetch_rank"] += t_rank
            print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | picked={len(items)}")

            if not items:
                print("[结果] empty")
                continue

            enriched, t_enrich = _step_enrich(spider, items, max_books=top_n)
            grand["enrich"] += t_enrich
            print(f"[计时] enrich_rank_items: {fmt_sec(t_enrich)} | enriched={len(enriched)}")

            snapshot_id, t_db_snap = _step_db_write_snapshot(db, rank_key=rk, snapshot_date=snapshot_date, items=enriched)
            grand["db_snapshot"] += t_db_snap
            print(f"[计时] db_write_snapshot: {fmt_sec(t_db_snap)} | snapshot_id={snapshot_id}")

            if chapter_n > 0:
                for i, b in enumerate(enriched, 1):
                    url = b.get("url", "")
                    if not url:
                        continue
                    chapters, t_ch = _step_fetch_chapters(spider, novel_url=url, target_chapter_count=chapter_n)
                    grand["chapters_fetch"] += t_ch

                    t_db_ch = _step_db_upsert_chapters(db, book=b, chapters=chapters, snapshot_date=snapshot_date)
                    grand["chapters_db"] += t_db_ch

                    if i < len(enriched):
                        time.sleep(1.2)

        print("\n[计时汇总 - multi_ranks]")
        for k, v in grand.items():
            print(f"  - {k:<16}: {fmt_sec(v)}")

        print_db_counts(db)
        print(f"\n[输出] 数据库文件: {db_path}")

    finally:
        try:
            if spider:
                spider.close()
        except Exception:
            pass


def run_smart_fetch(*, rank_key: str, pages: int, chapter_n1: int, chapter_n2: int, verbose: bool = False) -> None:
    """
    Smart-fetch experiment (DB-backed) with clearer diagnostics:

    What we want to know:
    1) run#2 是否只“补抓 delta = chapter_n2 - chapter_n1”？
    2) run#2 是否仍然发生“重复网络抓取 1..chapter_n1”，导致耗时接近从零抓 chapter_n2？
    3) 用 baseline（从零抓 chapter_n2）对照 run#2，判断是否真正 skip 了重复抓取。

    Output focuses on:
    - DB before/after counts
    - DB max chapter index delta (if schema supports)
    - baseline scratch fetch time
    """
    print_header(
        "[Test] smart_fetch - 同一本书两次抓取不同 chapter_n（验证智能补全/去重抓取）",
        params={
            "rank_key": rank_key,
            "pages": pages,
            "chapter_n1": chapter_n1,
            "chapter_n2": chapter_n2,
        },
    )

    if chapter_n2 < chapter_n1:
        print(f"[WARN] chapter_n2 ({chapter_n2}) < chapter_n1 ({chapter_n1})，自动交换以保证递增")
        chapter_n1, chapter_n2 = chapter_n2, chapter_n1

    expected_delta = chapter_n2 - chapter_n1

    # Main DB (clean)
    db_path = _ensure_clean_dirs()
    db = init_db(db_path, is_test=True)
    spider = None
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    try:
        spider = _init_spider(db=db)

        # pick one sample
        items, t_rank = _step_fetch_rank(spider, rank_key=rank_key, pages=pages)
        items = (items or [])[:1]
        print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | picked={len(items)}")

        book = pick_first(items)
        if not book:
            print("[结果] 未获取到榜单作品")
            return

        pid = safe_pid(book)
        url = book.get("url", "")
        print(f"\n[样本] 《{safe_trunc(book.get('title',''),40)}》 pid={pid}")

        # Detail (optional for DB row)
        with Timer("fetch_novel_detail") as t_det:
            detail = spider.fetch_novel_detail(url, pid, seed=book)
        print(f"[计时] fetch_novel_detail: {fmt_sec(t_det.elapsed)}")

        platform = "fanqie"

        # --------------------------
        # run#1
        # --------------------------
        before1 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max1 = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"[DB] before run#1: count={before1} max_idx={max1}")

        chapters1, t_ch1 = _step_fetch_chapters(spider, novel_url=url, target_chapter_count=chapter_n1)
        print(f"[计时] fetch_first_n_chapters #1: {fmt_sec(t_ch1)} | returned={len(chapters1)} | target={chapter_n1}")

        t_db1 = _step_db_upsert_chapters(db, book=detail or book, chapters=chapters1, snapshot_date=snapshot_date)
        if t_db1 > 0:
            print(f"[计时] db_upsert_first_n_chapters #1: {fmt_sec(t_db1)}")

        after1 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max1b = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"[DB] after  run#1: count={after1} max_idx={max1b} | count_delta={after1 - before1} | max_delta={max1b - max1}")

        # --------------------------
        # run#2 (delta)
        # --------------------------
        before2 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max2 = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"\n[DB] before run#2: count={before2} max_idx={max2} | expected_delta={expected_delta}")

        chapters2, t_ch2 = _step_fetch_chapters(spider, novel_url=url, target_chapter_count=chapter_n2)
        print(f"[计时] fetch_first_n_chapters #2: {fmt_sec(t_ch2)} | returned={len(chapters2)} | target={chapter_n2}")

        t_db2 = _step_db_upsert_chapters(db, book=detail or book, chapters=chapters2, snapshot_date=snapshot_date)
        if t_db2 > 0:
            print(f"[计时] db_upsert_first_n_chapters #2: {fmt_sec(t_db2)}")

        after2 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max2b = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"[DB] after  run#2: count={after2} max_idx={max2b} | count_delta={after2 - before2} | max_delta={max2b - max2}")

        # --------------------------
        # baseline: scratch fetch chapter_n2 on a fresh DB
        # --------------------------
        baseline_rel = os.path.join("test_output", "fanqie_test_baseline.db")
        baseline_path = ensure_clean_dirs(db_relpath=baseline_rel, remove_db=True)
        baseline_db = init_db(baseline_path, is_test=True)
        baseline_spider = None
        t_scratch = None

        try:
            baseline_spider = _init_spider(db=baseline_db)
            # ensure novel exists (optional)
            try:
                baseline_spider.fetch_novel_detail(url, pid, seed=book)
            except Exception:
                pass

            with Timer("baseline_scratch_fetch") as t0:
                _ = baseline_spider.fetch_first_n_chapters(url, target_chapter_count=int(chapter_n2))
            t_scratch = t0.elapsed
            print(f"\n[Baseline] scratch fetch target={chapter_n2}: {fmt_sec(t_scratch)} | db={baseline_path}")
        finally:
            try:
                if baseline_spider:
                    baseline_spider.close()
            except Exception:
                pass

        # --------------------------
        # conclusion
        # --------------------------
        print("\n[判定结论]")
        print(f"  - run#1 fetch time: {fmt_sec(t_ch1)} (target={chapter_n1})")
        print(f"  - run#2 fetch time: {fmt_sec(t_ch2)} (target={chapter_n2})")
        if t_scratch is not None:
            print(f"  - baseline scratch : {fmt_sec(t_scratch)} (target={chapter_n2})")

        max_delta = max2b - max2
        count_delta = after2 - before2

        print("\n[DB增量检查]")
        print(f"  - expected_delta = {expected_delta}")
        print(f"  - db count_delta = {count_delta}")
        print(f"  - db max_delta   = {max_delta}")

        # DB delta inference
        db_delta_ok = False
        if expected_delta == 0:
            db_delta_ok = True
        else:
            if max_delta == expected_delta:
                db_delta_ok = True
            elif max_delta == 0 and count_delta == expected_delta:
                # schema may not support max idx but count increased
                db_delta_ok = True

        if db_delta_ok:
            print("  - 结果: DB 侧看起来只新增了缺失章节（去重/补全逻辑可能有效）")
        else:
            print("  - 结果: DB 增量异常（可能重复写入、索引字段未写入、或补全判断依赖字段缺失）")

        if t_scratch is not None and t_scratch > 0:
            ratio = t_ch2 / t_scratch
            print("\n[网络重复抓取倾向]")
            print(f"  - run#2 / scratch ratio = {ratio:.2f}")
            if ratio < 0.65 and expected_delta > 0:
                print("  - 结果: run#2 明显小于从零抓取，倾向于 spider 真的跳过了重复抓取")
            elif ratio > 0.85 and expected_delta > 0:
                print("  - 结果: run#2 接近从零抓取，倾向于 spider 仍在重复抓前面章节（spider问题）")
            else:
                print("  - 结果: 介于两者之间；建议再跑一次或提高 chapter_n2 让差异更明显")

        print_db_counts(db)
        print(f"\n[输出] 主数据库文件: {db_path}")

    finally:
        try:
            if spider:
                spider.close()
        except Exception:
            pass

def run_fake_same_novel_rename_test():
    """
    Fake input test:
    - same platform_novel_id (and same author/intro/url), but different title
    - expect: novels=1, novel_titles=2, and latest title becomes primary
    - also test chapters upsert: first 2 then 4 => count grows to 4, max=4
    """
    print("\n================================================================================")
    print("[Test] fake_rename - 同小说不同书名（不访问网站，纯 DB fake input）")
    print("================================================================================")

    db_path = _ensure_clean_dirs()
    db = _init_test_db(db_path)

    snapshot_date = "2026-02-08"
    platform_novel_id = "fanqie_123456"
    url = "https://fanqie.example/book/123456"
    author = "某作者"
    intro = "这是一本会改名的书，简介保持不变，用于测试去重与书名别名。"

    # 第一次：旧书名
    items_v1 = [{
        "platform": "fanqie",
        "platform_novel_id": platform_novel_id,
        "title": "旧书名：风起云涌",
        "author": author,
        "intro": intro,
        "main_category": "科幻",
        "tags": ["科幻", "末世"],
        "status": "ongoing",
        "total_words": 123456,
        "url": url,
        "rank": 1,
        "reading_count": 1111,
    }]

    # 第二次：新书名（同一本书）
    items_v2 = [{
        "platform": "fanqie",
        "platform_novel_id": platform_novel_id,
        "title": "新书名：云涌风起",
        "author": author,
        "intro": intro,
        "main_category": "科幻",
        "tags": ["科幻", "末世"],
        "status": "ongoing",
        "total_words": 123999,
        "url": url,
        "rank": 1,
        "reading_count": 2222,
    }]

    # 写入两次 snapshot（会触发 novels upsert + novel_titles upsert）
    db.save_rank_snapshot(
        platform="fanqie",
        rank_family="fake_rank",
        rank_sub_cat="",
        snapshot_date=snapshot_date,
        items=items_v1,
        source_url="",
        make_title_primary=True,
    )

    db.save_rank_snapshot(
        platform="fanqie",
        rank_family="fake_rank",
        rank_sub_cat="",
        snapshot_date=snapshot_date,
        items=items_v2,
        source_url="",
        make_title_primary=True,
    )

    # 断言：novels=1，novel_titles=2
    counts = db.get_table_counts()
    print("\n[db] table counts after rename writes:")
    for k, v in counts.items():
        print(f"  - {k}: {v}")

    assert counts["novels"] == 1, f"Expected novels=1, got {counts['novels']}"
    assert counts["novel_titles"] == 2, f"Expected novel_titles=2, got {counts['novel_titles']}"

    # 进一步断言：primary title 应该是第二次的新书名（make_title_primary=True）
    with db._tx(immediate=False) as conn:
        row = conn.execute(
            """
            SELECT nt.title
            FROM novels n
            JOIN novel_titles nt ON n.novel_uid = nt.novel_uid
            WHERE n.platform=? AND n.platform_novel_id=? AND nt.is_primary=1
            LIMIT 1
            """,
            ("fanqie", platform_novel_id),
        ).fetchone()

        primary_title = row["title"] if row else None

        titles = conn.execute(
            """
            SELECT nt.title, nt.is_primary
            FROM novels n
            JOIN novel_titles nt ON n.novel_uid = nt.novel_uid
            WHERE n.platform=? AND n.platform_novel_id=?
            ORDER BY nt.title_id ASC
            """,
            ("fanqie", platform_novel_id),
        ).fetchall()

    print("\n[titles] stored titles:")
    for r in titles:
        print(f"  - title={r['title']} | is_primary={r['is_primary']}")

    assert primary_title == "新书名：云涌风起", f"Expected primary title to be new title, got {primary_title}"

    # ------------------------------------------------------------
    # 章节 fake upsert：先 2 章，再 4 章
    # ------------------------------------------------------------
    chapters_2 = [
        {"chapter_num": 1, "chapter_title": "第1章", "chapter_content": "内容1", "chapter_url": url + "/c1", "word_count": 3},
        {"chapter_num": 2, "chapter_title": "第2章", "chapter_content": "内容2", "chapter_url": url + "/c2", "word_count": 3},
    ]
    chapters_4 = [
        {"chapter_num": 1, "chapter_title": "第1章", "chapter_content": "内容1(更新版)", "chapter_url": url + "/c1", "word_count": 6},
        {"chapter_num": 2, "chapter_title": "第2章", "chapter_content": "内容2", "chapter_url": url + "/c2", "word_count": 3},
        {"chapter_num": 3, "chapter_title": "第3章", "chapter_content": "内容3", "chapter_url": url + "/c3", "word_count": 3},
        {"chapter_num": 4, "chapter_title": "第4章", "chapter_content": "内容4", "chapter_url": url + "/c4", "word_count": 3},
    ]

    # 第一次写 2 章
    db.upsert_first_n_chapters(
        platform="fanqie",
        platform_novel_id=platform_novel_id,
        publish_date=snapshot_date,
        chapters=chapters_2,
        novel_fallback_fields={
            "title": "旧书名：风起云涌",
            "author": author,
            "intro": intro,
            "main_category": "科幻",
            "status": "ongoing",
            "total_words": 0,
            "url": url,
            "tags": ["科幻", "末世"],
        },
    )

    c1 = db.get_first_n_chapter_count(platform="fanqie", platform_novel_id=platform_novel_id)
    m1 = db.get_first_n_chapter_max_num(platform="fanqie", platform_novel_id=platform_novel_id)
    print(f"\n[chapters] after first upsert: count={c1}, max={m1}")
    assert c1 == 2 and m1 == 2, f"Expected count=2,max=2; got count={c1},max={m1}"

    # 第二次写 4 章（包含前两章，且第1章内容更新）
    db.upsert_first_n_chapters(
        platform="fanqie",
        platform_novel_id=platform_novel_id,
        publish_date=snapshot_date,
        chapters=chapters_4,
        novel_fallback_fields={
            "title": "新书名：云涌风起",
            "author": author,
            "intro": intro,
            "main_category": "科幻",
            "status": "ongoing",
            "total_words": 0,
            "url": url,
            "tags": ["科幻", "末世"],
        },
    )

    c2 = db.get_first_n_chapter_count(platform="fanqie", platform_novel_id=platform_novel_id)
    m2 = db.get_first_n_chapter_max_num(platform="fanqie", platform_novel_id=platform_novel_id)
    print(f"[chapters] after second upsert: count={c2}, max={m2}")
    assert c2 == 4 and m2 == 4, f"Expected count=4,max=4; got count={c2},max={m2}"

    print("\n fake_rename test passed.")


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def main():
    rank_choices = _get_rank_choices()

    parser = argparse.ArgumentParser(description="Fanqie Spider Test Suite")
    parser.add_argument(
        "--test",
        required=True,
        choices=["decryption", "quick", "full", "multi_ranks", "smart_fetch", "fake_rename"],
        help="Test mode: decryption | quick | full | multi_ranks",
    )

    parser.add_argument("--pages", type=int, default=1, help="Pages to fetch for rank list (default: 1)")
    parser.add_argument("--top_n", type=int, default=None, help="Top N books to process per rank (default depends on --test)")
    parser.add_argument("--chapter_n", type=int, default=None, help="Chapters to fetch per book (default depends on --test)")

    # Smart fetch experiment
    parser.add_argument("--chapter_n1", type=int, default=3, help="Smart fetch: first run target chapters (default: 3)")
    parser.add_argument("--chapter_n2", type=int, default=4, help="Smart fetch: second run target chapters (default: 4)")

    if rank_choices:
        parser.add_argument("--rank_key", type=str, default=_default_rank_key(rank_choices), choices=rank_choices,
                            help="Single rank key (from config.WEBSITES['fanqie'].rank_urls)")
    else:
        parser.add_argument("--rank_key", type=str, default="", help="Single rank key (config.WEBSITES['fanqie'].rank_urls is empty)")

    parser.add_argument("--rank_keys", type=str, default=_default_multirank_keys(rank_choices),
                        help="Multiple rank keys CSV (default from config)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Mode defaults (only if user did not pass)
    if args.test == "quick":
        top_n = args.top_n if args.top_n is not None else 1
        chapter_n = args.chapter_n if args.chapter_n is not None else 1
        run_quick(rank_key=args.rank_key, pages=args.pages, top_n=top_n, chapter_n=chapter_n, verbose=args.verbose)
        return

    if args.test == "full":
        top_n = args.top_n if args.top_n is not None else 3
        chapter_n = args.chapter_n if args.chapter_n is not None else 5
        run_full(rank_key=args.rank_key, pages=args.pages, top_n=top_n, chapter_n=chapter_n, verbose=args.verbose)
        return

    if args.test == "multi_ranks":
        top_n = args.top_n if args.top_n is not None else 1
        # default no chapters for multi-ranks unless explicitly requested
        chapter_n = args.chapter_n if args.chapter_n is not None else 3
        rank_keys = [x.strip() for x in (args.rank_keys or "").split(",") if x.strip()]
        run_multi_ranks(rank_keys=rank_keys, pages=args.pages, top_n=top_n, chapter_n=chapter_n, verbose=args.verbose)
        return


    if args.test == "smart_fetch":
        run_smart_fetch(rank_key=args.rank_key, pages=args.pages, chapter_n1=args.chapter_n1, chapter_n2=args.chapter_n2, verbose=args.verbose)
        return

    if args.test == "decryption":
        run_decryption(verbose=args.verbose)
        return

    if args.test == "fake_rename":
        run_fake_same_novel_rename_test()
        return


if __name__ == "__main__":
    main()
