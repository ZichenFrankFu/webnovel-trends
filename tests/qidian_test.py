"""
Qidian Spider Test Suite (Refined)

Aligned with fanqie_test style:
Only 4 test modes are kept:
1) decryption   : (N/A for Qidian) kept for CLI parity
2) quick        : one rank -> first book -> detail metadata + first chapter (NO DB)
3) full         : one rank -> top N books -> metadata + first K chapters each (WRITE DB)
4) multi_ranks  : multiple ranks -> per-rank pipeline (WRITE DB)

Timing is recorded per major step for performance tuning:
- fetch_rank_list
- enrich_rank_items (detail metadata)
- fetch_first_n_chapters
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Ensure project root is on sys.path so `spiders/` and `database/` can be imported
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import WEBSITES
from spiders.qidian_spider import QidianSpider

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
    safe_trunc,
)

# ============================================================
# Config helpers (from config.py)
# ============================================================

def _get_qidian_site_cfg() -> Dict[str, Any]:
    return WEBSITES.get("qidian", {}) or {}


def _get_rank_choices() -> List[str]:
    rank_urls = _get_qidian_site_cfg().get("rank_urls") or {}
    return list(rank_urls.keys()) if isinstance(rank_urls, dict) else []


def _default_rank_key(choices: List[str]) -> str:
    prefer = ["月票榜", "畅销榜", "推荐榜", "收藏榜", "阅读指数榜"]
    for k in prefer:
        if k in choices:
            return k
    return choices[0] if choices else ""


def _default_multirank_keys(choices: List[str]) -> str:
    prefer = ["月票榜", "畅销榜", "推荐榜"]
    picked = [k for k in prefer if k in choices]
    if not picked and choices:
        picked = choices[:3]
    return ",".join(picked)


# ============================================================
# Spider init + call adapters (avoid signature drift)
# ============================================================

def _init_spider(site_cfg: Dict[str, Any], db: Any):
    """
    QidianSpider signature may vary across refactors.
    Try common constructor patterns.
    """
    # pattern A: QidianSpider(site_cfg, db_handler=db)
    try:
        return QidianSpider(site_cfg, db_handler=db)
    except TypeError:
        pass

    # pattern B: QidianSpider(site_cfg, db)
    try:
        return QidianSpider(site_cfg, db)
    except TypeError:
        pass

    # pattern C: QidianSpider(site_cfg)
    try:
        spider = QidianSpider(site_cfg)
        # attach db if supported
        if hasattr(spider, "db_handler"):
            setattr(spider, "db_handler", db)
        elif hasattr(spider, "db"):
            setattr(spider, "db", db)
        return spider
    except TypeError as e:
        raise TypeError(f"Failed to initialize QidianSpider with known signatures. Last error: {e}")


def _close_spider(spider: Any) -> None:
    for attr in ("close", "quit", "shutdown"):
        fn = getattr(spider, attr, None)
        if callable(fn):
            try:
                fn()
            except Exception:
                pass
            return


def _call_fetch_rank_list(spider: Any, *, rank_key: str, pages: int, top_n: int) -> List[Dict[str, Any]]:
    """
    Adapter for QidianSpider.fetch_rank_list.

    Current QidianSpider implementation:
      - fetch_rank_list(rank_type: str = "畅销榜", page=5)  (params may be ignored)
      - page count is driven by spider.site_config["pages_per_rank"]

    So in tests we:
      1) set spider.site_config["pages_per_rank"] = pages
      2) call fetch_rank_list with rank_type only
      3) apply top_n slicing on the returned list
    """
    # Respect CLI pages by overriding site config (spider reads this internally)
    try:
        if hasattr(spider, "site_config") and isinstance(spider.site_config, dict):
            spider.site_config["pages_per_rank"] = int(pages)
        elif hasattr(spider, "config") and isinstance(spider.config, dict):
            spider.config["pages_per_rank"] = int(pages)
    except Exception:
        pass

    last_err: Optional[Exception] = None

    try:
        novels = spider.fetch_rank_list(rank_type=rank_key)
        return (novels or [])[:top_n] if top_n else (novels or [])
    except TypeError as e:
        last_err = e

    try:
        novels = spider.fetch_rank_list(rank_key)
        return (novels or [])[:top_n] if top_n else (novels or [])
    except TypeError as e:
        last_err = e

    raise TypeError(
        "QidianSpider.fetch_rank_list signature mismatch. "
        f"Expected a single rank_type/rank_key argument. Last error: {last_err}"
    )


def _call_fetch_novel_detail(spider: Any, *, novel: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adapter for QidianSpider.fetch_novel_detail.

    Current signature:
        fetch_novel_detail(novel_url: str, pid: str, seed: Optional[dict]=None)

    We pass (url, pid, seed=novel).
    """
    url = (novel or {}).get("url") or ""
    pid = (novel or {}).get("platform_novel_id") or (novel or {}).get("pid") or ""

    last_err: Optional[Exception] = None

    try:
        return spider.fetch_novel_detail(url, pid, seed=novel) or {}
    except TypeError as e:
        last_err = e

    try:
        return spider.fetch_novel_detail(url, pid) or {}
    except TypeError as e:
        last_err = e

    try:
        return spider.fetch_novel_detail(novel) or {}
    except TypeError as e:
        last_err = e

    raise TypeError(
        "QidianSpider.fetch_novel_detail signature mismatch. "
        f"Tried (url, pid, seed) variants. Last error: {last_err}"
    )


def _call_fetch_first_n_chapters(spider: Any, *, novel: Dict[str, Any], chapter_n: int) -> List[Dict[str, Any]]:
    """
    Adapter for QidianSpider.fetch_first_n_chapters.

    Current signature:
        fetch_first_n_chapters(novel_url: str, target_chapter_count: int = 5, *, fallback_title: str = "")
    """
    url = (novel or {}).get("url") or ""
    title = (novel or {}).get("title") or ""

    last_err: Optional[Exception] = None

    try:
        return spider.fetch_first_n_chapters(url, int(chapter_n), fallback_title=title) or []
    except TypeError as e:
        last_err = e

    try:
        return spider.fetch_first_n_chapters(url, int(chapter_n)) or []
    except TypeError as e:
        last_err = e

    try:
        return spider.fetch_first_n_chapters(novel, int(chapter_n)) or []
    except TypeError as e:
        last_err = e

    raise TypeError(
        "QidianSpider.fetch_first_n_chapters signature mismatch. "
        f"Tried (url, n) variants. Last error: {last_err}"
    )


# ============================================================
# Core steps (timed)
# ============================================================

def _step_fetch_rank(spider: Any, *, rank_key: str, pages: int, top_n: int) -> Tuple[List[Dict[str, Any]], float]:
    with Timer("fetch_rank_list") as t:
        novels = _call_fetch_rank_list(spider, rank_key=rank_key, pages=pages, top_n=top_n)
    return novels or [], t.elapsed


def _step_enrich_details(spider: Any, novels: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], float]:
    enriched: List[Dict[str, Any]] = []
    with Timer("enrich_rank_items") as t:
        for n in novels or []:
            d = _call_fetch_novel_detail(spider, novel=n)
            # merge detail back (seed may already be in-place, but be defensive)
            merged = dict(n or {})
            merged.update(d or {})
            enriched.append(merged)
    return enriched, t.elapsed


def _step_fetch_chapters_per_book(spider: Any, novels: List[Dict[str, Any]], *, chapter_n: int) -> Tuple[float, int]:
    total_sec = 0.0
    total_ch = 0
    for i, n in enumerate(novels or [], 1):
        title = (n.get("title") or "")[:40]
        with Timer(f"fetch_first_n_chapters #{i}") as t:
            chapters = _call_fetch_first_n_chapters(spider, novel=n, chapter_n=chapter_n)
        total_sec += t.elapsed
        total_ch += len(chapters or [])
        print(f"[计时] fetch_first_n_chapters #{i}: {fmt_sec(t.elapsed)} | chapters={len(chapters or [])} | 《{title}》")
        if i < len(novels):
            time.sleep(1.2)
    return total_sec, total_ch


# ============================================================
# Test modes (4 modes only)
# ============================================================

def run_decryption(*, verbose: bool = False) -> None:
    print_header("[Test] decryption - 字体解密模块", "(Qidian N/A - skipped)")
    print("[SKIP] Qidian does not require font decryption. Test skipped.")


def run_quick(*, rank_key: str, pages: int, top_n: int, chapter_n: int, verbose: bool = False) -> None:
    """
    Quick smoke test (NO DB):
    - fetch one rank
    - pick first book
    - fetch detail metadata
    - fetch first chapter
    """
    print_header(
        "[Test] quick - 单榜单第1本 + detail + 第1章（不写入 qidian_test DB）",
        params={"rank_key": rank_key, "pages": pages, "top_n": top_n, "chapter_n": chapter_n},
    )

    spider = None
    try:
        site_cfg = _get_qidian_site_cfg()
        spider = _init_spider(site_cfg, db=None)

        items, t_rank = _step_fetch_rank(spider, rank_key=rank_key, pages=pages, top_n=max(1, top_n))
        print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | items={len(items)}")

        book = pick_first(items[:top_n])
        if not book:
            print("[结果] 未获取到榜单作品")
            return

        url = book.get("url", "")
        pid = book.get("platform_novel_id") or book.get("pid") or ""
        print(f"\n[样本] 《{book.get('title','')}》 pid={pid} url={url}")

        with Timer("fetch_novel_detail") as t:
            detail = _call_fetch_novel_detail(spider, novel=book)
        print(f"[计时] fetch_novel_detail: {fmt_sec(t.elapsed)}")

        with Timer("fetch_first_n_chapters") as t:
            chapters = _call_fetch_first_n_chapters(spider, novel=book, chapter_n=max(1, chapter_n))
        print(f"[计时] fetch_first_n_chapters: {fmt_sec(t.elapsed)} | chapters={len(chapters or [])}")

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
            print(f"  - content(sample): {safe_trunc(c0.get('content') or '', 180)}")

    finally:
        if spider:
            _close_spider(spider)


def run_full(*, rank_key: str, pages: int, top_n: int, chapter_n: int, db_relpath: str, verbose: bool = False) -> None:
    """
    Full DB-backed test (single rank):
    - fetch rank list
    - fetch detail metadata for top_n
    - fetch first chapter_n chapters per book
    """
    print_header(
        "[Test] full - 单榜单 top_n 本 + metadata + 前 chapter_n 章（写入 qidian_test DB）",
        params={"rank_key": rank_key, "pages": pages, "top_n": top_n, "chapter_n": chapter_n, "db": db_relpath},
    )

    db_path = ensure_clean_dirs(db_relpath=db_relpath, remove_db=True)
    db = init_db(db_path, is_test=True)

    spider = None
    snapshot_date = datetime.now().strftime("%Y-%m-%d")

    try:
        site_cfg = _get_qidian_site_cfg()
        spider = _init_spider(site_cfg, db=db)

        novels, t_rank = _step_fetch_rank(spider, rank_key=rank_key, pages=pages, top_n=top_n)
        print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | picked={len(novels)}")
        if not novels:
            print("[结果] 未获取到榜单作品")
            return

        enriched, t_enrich = _step_enrich_details(spider, novels)
        print(f"[计时] enrich_rank_items: {fmt_sec(t_enrich)} | enriched={len(enriched)}")

        total_ch_sec, total_ch = _step_fetch_chapters_per_book(spider, enriched, chapter_n=chapter_n)

        print("\n[计时汇总]")
        print(f"  - fetch_rank_list        : {fmt_sec(t_rank)}")
        print(f"  - enrich_rank_items      : {fmt_sec(t_enrich)}")
        print(f"  - fetch_first_n_chapters : {fmt_sec(total_ch_sec)} (sum) | chapters={total_ch}")

        print_db_counts(db)
        print(f"\n[输出] 数据库文件: {db_path}")

    finally:
        if spider:
            _close_spider(spider)


def run_multi_ranks(
    *,
    rank_keys: List[str],
    pages: int,
    top_n: int,
    chapter_n: int,
    db_relpath: str,
    verbose: bool = False,
) -> None:
    """
    Multi-rank DB-backed test:
    - loop ranks -> fetch + enrich + optional chapters
    """
    print_header(
        "[Test] multi_ranks - 多榜单循环（写入 qidian_test DB）",
        params={"rank_keys": rank_keys, "pages": pages, "top_n(per rank)": top_n, "chapter_n": chapter_n, "db": db_relpath},
    )

    db_path = ensure_clean_dirs(db_relpath=db_relpath, remove_db=True)
    db = init_db(db_path, is_test=True)

    spider = None
    grand = {"fetch_rank": 0.0, "enrich": 0.0, "chapters_fetch": 0.0, "chapters_total": 0}

    try:
        site_cfg = _get_qidian_site_cfg()
        spider = _init_spider(site_cfg, db=db)

        for idx, rk in enumerate(rank_keys, 1):
            print("\n" + "-" * 80)
            print(f"[{idx}/{len(rank_keys)}] rank_key={rk}")

            novels, t_rank = _step_fetch_rank(spider, rank_key=rk, pages=pages, top_n=top_n)
            grand["fetch_rank"] += t_rank
            print(f"[计时] fetch_rank_list: {fmt_sec(t_rank)} | picked={len(novels)}")
            if not novels:
                print("[结果] empty")
                continue

            enriched, t_enrich = _step_enrich_details(spider, novels)
            grand["enrich"] += t_enrich
            print(f"[计时] enrich_rank_items: {fmt_sec(t_enrich)} | enriched={len(enriched)}")

            if chapter_n > 0:
                total_ch_sec, total_ch = _step_fetch_chapters_per_book(spider, enriched, chapter_n=chapter_n)
                grand["chapters_fetch"] += total_ch_sec
                grand["chapters_total"] += total_ch

        print("\n[计时汇总 - multi_ranks]")
        print(f"  - fetch_rank_list        : {fmt_sec(grand['fetch_rank'])}")
        print(f"  - enrich_rank_items      : {fmt_sec(grand['enrich'])}")
        if chapter_n > 0:
            print(f"  - fetch_first_n_chapters : {fmt_sec(grand['chapters_fetch'])} (sum) | chapters={grand['chapters_total']}")

        print_db_counts(db)
        print(f"\n[输出] 数据库文件: {db_path}")

    finally:
        if spider:
            _close_spider(spider)


# ============================================================
# CLI
# ============================================================

def main():
    rank_choices = _get_rank_choices()

    parser = argparse.ArgumentParser(description="Qidian Spider Test Suite (refined 4 modes)")
    parser.add_argument(
        "--test",
        required=True,
        choices=["decryption", "quick", "full", "multi_ranks"],
        help="Test mode: decryption | quick | full | multi_ranks",
    )

    parser.add_argument("--pages", type=int, default=1, help="Pages to fetch for rank list (default: 1)")
    parser.add_argument("--top_n", type=int, default=None, help="Top N books to process per rank (default depends on --test)")
    parser.add_argument("--chapter_n", type=int, default=None, help="Chapters to fetch per book (default depends on --test)")

    if rank_choices:
        parser.add_argument("--rank_key", type=str, default=_default_rank_key(rank_choices), choices=rank_choices,
                            help="Single rank key (from config.WEBSITES['qidian'].rank_urls)")
    else:
        parser.add_argument("--rank_key", type=str, default="", help="Single rank key (rank_urls empty)")

    parser.add_argument("--rank_keys", type=str, default=_default_multirank_keys(rank_choices),
                        help="Multiple rank keys CSV (default from config)")
    parser.add_argument("--db_path", type=str, default="test_output/qidian_test.db", help="DB path (relative to project root)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    # Mode defaults (only if user did not pass)
    if args.test == "decryption":
        run_decryption(verbose=args.verbose)
        return

    if args.test == "quick":
        top_n = args.top_n if args.top_n is not None else 1
        chapter_n = args.chapter_n if args.chapter_n is not None else 1
        run_quick(rank_key=args.rank_key, pages=args.pages, top_n=top_n, chapter_n=chapter_n, verbose=args.verbose)
        return

    if args.test == "full":
        top_n = args.top_n if args.top_n is not None else 3
        chapter_n = args.chapter_n if args.chapter_n is not None else 5
        run_full(
            rank_key=args.rank_key,
            pages=args.pages,
            top_n=top_n,
            chapter_n=chapter_n,
            db_relpath=args.db_path,
            verbose=args.verbose,
        )
        return

    if args.test == "multi_ranks":
        top_n = args.top_n if args.top_n is not None else 1
        chapter_n = args.chapter_n if args.chapter_n is not None else 3
        rank_keys = [x.strip() for x in (args.rank_keys or "").split(",") if x.strip()]
        run_multi_ranks(
            rank_keys=rank_keys,
            pages=args.pages,
            top_n=top_n,
            chapter_n=chapter_n,
            db_relpath=args.db_path,
            verbose=args.verbose,
        )
        return


if __name__ == "__main__":
    main()
