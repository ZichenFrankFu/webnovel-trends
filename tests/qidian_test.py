import argparse
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

# Ensure project root is on sys.path so `spiders/` and `database/` can be imported
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import WEBSITES
from database.db_handler import DatabaseHandler
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
    db_get_chapter_count,
    db_get_max_chapter_index,
)

# ============================================================
# Helpers: defaults from config
# ============================================================

def _get_qidian_cfg() -> Dict[str, Any]:
    return WEBSITES.get("qidian", {}) or {}


def _get_rank_choices() -> List[str]:
    rank_urls = _get_qidian_cfg().get("rank_urls") or {}
    return list(rank_urls.keys()) if isinstance(rank_urls, dict) else []


def _default_rank_key(choices: List[str]) -> str:
    # Prefer common ranks if present, else first
    prefer = ["月票榜", "畅销榜", "推荐榜", "收藏榜", "阅读指数榜"]
    for k in prefer:
        if k in choices:
            return k
    return choices[0] if choices else ""


def _default_rank_keys_csv(choices: List[str]) -> str:
    prefer = ["月票榜", "畅销榜", "收藏榜"]
    picked = [k for k in prefer if k in choices]
    if not picked and choices:
        picked = choices[:3]
    return ",".join(picked)

# ============================================================
# Spider call adapters (avoid signature drift)
# ============================================================

def _init_spider(site_cfg: Dict[str, Any], db: Optional[DatabaseHandler]):
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
        raise TypeError(
            f"Failed to initialize QidianSpider with known signatures. Last error: {e}"
        )


def _call_fetch_rank_list(spider, rank_key: str, pages: int, top_n: int, write_db: bool):
    """Adapter for QidianSpider.fetch_rank_list.

    Current QidianSpider implementation uses:
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

    # write_db is controlled by whether db_handler exists; keep arg for CLI parity
    last_err: Optional[Exception] = None

    # Prefer keyword in case signature is (rank_type=...)
    try:
        novels = spider.fetch_rank_list(rank_type=rank_key)
        return novels[:top_n] if top_n else novels
    except TypeError as e:
        last_err = e

    # Fallback: positional (rank_key)
    try:
        novels = spider.fetch_rank_list(rank_key)
        return novels[:top_n] if top_n else novels
    except TypeError as e:
        last_err = e

    raise TypeError(
        "QidianSpider.fetch_rank_list signature mismatch. "
        f"Expected a single rank_type/rank_key argument. Last error: {last_err}"
    )


def _call_fetch_novel_detail(spider, novel: Dict[str, Any], write_db: bool):
    """Adapter for QidianSpider.fetch_novel_detail.

    Current QidianSpider signature:
        fetch_novel_detail(novel_url: str, pid: str, seed: Optional[dict]=None)

    The rank item dict already contains:
      - novel["url"]
      - novel["platform_novel_id"]

    So we pass (url, pid, seed=novel). If your spider later changes again,
    this adapter is the only place you need to touch.
    """
    url = (novel or {}).get("url") or ""
    pid = (novel or {}).get("platform_novel_id") or (novel or {}).get("pid") or ""

    last_err: Optional[Exception] = None

    # Preferred call
    try:
        return spider.fetch_novel_detail(url, pid, seed=novel)
    except TypeError as e:
        last_err = e

    # Fallback: some versions might not accept seed
    try:
        return spider.fetch_novel_detail(url, pid)
    except TypeError as e:
        last_err = e

    # Fallback: some very old versions used dict input
    try:
        return spider.fetch_novel_detail(novel)
    except TypeError as e:
        last_err = e

    raise TypeError(
        "QidianSpider.fetch_novel_detail signature mismatch. "
        f"Tried (url, pid, seed) variants. Last error: {last_err}"
    )


def _call_fetch_first_n_chapters(spider, novel: Dict[str, Any], chapter_n: int, write_db: bool):
    """Adapter for QidianSpider.fetch_first_n_chapters.

    Current QidianSpider signature:
        fetch_first_n_chapters(novel_url: str, target_chapter_count: int = 5, *, fallback_title: str = "")

    So we pass novel_url and chapter_n, and optionally fallback_title.
    """
    url = (novel or {}).get("url") or ""
    title = (novel or {}).get("title") or ""
    last_err: Optional[Exception] = None

    try:
        return spider.fetch_first_n_chapters(url, int(chapter_n), fallback_title=title)
    except TypeError as e:
        last_err = e

    try:
        return spider.fetch_first_n_chapters(url, int(chapter_n))
    except TypeError as e:
        last_err = e

    # Legacy: dict-based call
    try:
        return spider.fetch_first_n_chapters(novel, int(chapter_n))
    except TypeError as e:
        last_err = e

    raise TypeError(
        "QidianSpider.fetch_first_n_chapters signature mismatch. "
        f"Tried (url, n) variants. Last error: {last_err}"
    )


def _close_spider(spider):
    for attr in ("close", "quit", "shutdown"):
        fn = getattr(spider, attr, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception:
                return

def run_smart_fetch(*, rank_key: str, pages: int, chapter_n1: int, chapter_n2: int, verbose: bool = False) -> None:
    """
    Smart-fetch experiment (DB-backed) with clearer diagnostics + baseline.

    We do:
    - fresh DB
    - run#1 fetch chapters target=chapter_n1 (write_db=True)
    - run#2 fetch chapters target=chapter_n2 (write_db=True)
    - baseline: scratch fetch target=chapter_n2 on a separate fresh DB

    Then we judge:
    - DB delta matches expected_delta?
    - run#2 time vs baseline scratch time (is it close or much smaller)
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
    db_relpath = os.path.join("test_output", "qidian_test.db")
    db_path = ensure_clean_dirs(db_relpath=db_relpath, remove_db=True)
    db = init_db(db_path, is_test=True)

    spider = None
    try:
        site_cfg = _get_qidian_cfg()
        spider = _init_spider(site_cfg, db=db)

        # 1) fetch rank list (top 1)
        with Timer("fetch_rank_list") as t_rank:
            novels = _call_fetch_rank_list(spider, rank_key=rank_key, pages=pages, top_n=1, write_db=True)
        print(f"[计时] fetch_rank_list: {fmt_sec(t_rank.elapsed)} | picked={len(novels)}")

        book = pick_first(novels)
        if not book:
            print("[结果] 未获取到榜单作品")
            return

        pid = (book.get("platform_novel_id") or safe_pid(book)).strip()
        title = safe_trunc(book.get("title", ""), 40)
        print(f"\n[样本] 《{title}》 pid={pid}")

        # 2) enrich metadata (ensure novel row exists)
        with Timer("enrich_rank_items") as t_enrich:
            detail = _call_fetch_novel_detail(spider, book, write_db=True)
        print(f"[计时] enrich_rank_items: {fmt_sec(t_enrich.elapsed)} | enriched=1")

        platform = "qidian"

        # --------------------------
        # run#1
        # --------------------------
        before1 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max1 = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"[DB] before run#1: count={before1} max_idx={max1}")

        with Timer(f"fetch_first_n_chapters (chapter_n={chapter_n1})") as t_ch1:
            ch1 = _call_fetch_first_n_chapters(spider, detail or book, chapter_n=chapter_n1, write_db=True)
        print(f"[计时] fetch_first_n_chapters #1: {fmt_sec(t_ch1.elapsed)} | returned={len(ch1 or [])} | target={chapter_n1}")

        after1 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max1b = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"[DB] after  run#1: count={after1} max_idx={max1b} | count_delta={after1 - before1} | max_delta={max1b - max1}")

        # --------------------------
        # run#2 (delta)
        # --------------------------
        before2 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max2 = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"\n[DB] before run#2: count={before2} max_idx={max2} | expected_delta={expected_delta}")

        with Timer(f"fetch_first_n_chapters (chapter_n={chapter_n2})") as t_ch2:
            ch2 = _call_fetch_first_n_chapters(spider, detail or book, chapter_n=chapter_n2, write_db=True)
        print(f"[计时] fetch_first_n_chapters #2: {fmt_sec(t_ch2.elapsed)} | returned={len(ch2 or [])} | target={chapter_n2}")

        after2 = db_get_chapter_count(db, platform=platform, platform_novel_id=pid)
        max2b = db_get_max_chapter_index(db, platform=platform, platform_novel_id=pid)
        print(f"[DB] after  run#2: count={after2} max_idx={max2b} | count_delta={after2 - before2} | max_delta={max2b - max2}")

        # --------------------------
        # baseline: scratch fetch chapter_n2 on a separate fresh DB
        # --------------------------
        baseline_relpath = os.path.join("test_output", "qidian_test_baseline.db")
        baseline_path = ensure_clean_dirs(db_relpath=baseline_relpath, remove_db=True)
        baseline_db = init_db(baseline_path, is_test=True)
        baseline_spider = None
        t_scratch = None

        try:
            baseline_spider = _init_spider(site_cfg, db=baseline_db)

            # reuse same detail call so novel row exists
            try:
                _ = _call_fetch_novel_detail(baseline_spider, book, write_db=True)
            except Exception:
                pass

            with Timer("baseline_scratch_fetch") as t0:
                _ = _call_fetch_first_n_chapters(baseline_spider, detail or book, chapter_n=chapter_n2, write_db=True)
            t_scratch = t0.elapsed
            print(f"\n[Baseline] scratch fetch target={chapter_n2}: {fmt_sec(t_scratch)} | db={baseline_path}")
        finally:
            try:
                if baseline_spider:
                    _close_spider(baseline_spider)
            except Exception:
                pass

        # --------------------------
        # conclusion
        # --------------------------
        print("\n[判定结论]")
        print(f"  - run#1 fetch time: {fmt_sec(t_ch1.elapsed)} (target={chapter_n1})")
        print(f"  - run#2 fetch time: {fmt_sec(t_ch2.elapsed)} (target={chapter_n2})")
        if t_scratch is not None:
            print(f"  - baseline scratch : {fmt_sec(t_scratch)} (target={chapter_n2})")

        max_delta = max2b - max2
        count_delta = after2 - before2

        print("\n[DB增量检查]")
        print(f"  - expected_delta = {expected_delta}")
        print(f"  - db count_delta = {count_delta}")
        print(f"  - db max_delta   = {max_delta}")

        db_delta_ok = False
        if expected_delta == 0:
            db_delta_ok = True
        else:
            if max_delta == expected_delta:
                db_delta_ok = True
            elif max_delta == 0 and count_delta == expected_delta:
                db_delta_ok = True

        if db_delta_ok:
            print("  - 结果: DB 侧看起来只新增了缺失章节（去重/补全逻辑可能有效）")
        else:
            print("  - 结果: DB 增量异常（可能重复写入、索引字段未写入、或补全判断依赖字段缺失）")

        if t_scratch is not None and t_scratch > 0:
            ratio = t_ch2.elapsed / t_scratch
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
                _close_spider(spider)
        except Exception:
            pass


# ============================================================
# Test implementations (4 modes only)
# ============================================================

def test_decryption(_: argparse.Namespace):
    """
    Qidian has no font decryption.
    This mode exists only to keep CLI parity with fanqie_test.
    """
    print("[SKIP] Qidian does not require font decryption. Test skipped.")


def test_quick(args: argparse.Namespace):
    """
    Quick HTML sanity check (NO DB):
    - single rank
    - first novel only
    - fetch detail + first chapter
    """
    choices = _get_rank_choices()
    rank_key = (args.rank_key or _default_rank_key(choices)).strip()

    site_cfg = _get_qidian_cfg()
    spider = _init_spider(site_cfg, db=None)

    try:
        with Timer("fetch_rank_list"):
            novels = _call_fetch_rank_list(
                spider=spider,
                rank_key=rank_key,
                pages=args.pages,
                top_n=1,
                write_db=False,
            )

        if not novels:
            print("[WARN] No novels fetched.")
            return

        novel = novels[0]

        with Timer("fetch_detail"):
            _call_fetch_novel_detail(spider, novel, write_db=False)

        with Timer("fetch_first_chapter"):
            _call_fetch_first_n_chapters(spider, novel, chapter_n=1, write_db=False)
    finally:
        _close_spider(spider)


def test_full(args: argparse.Namespace):
    """
    Full pipeline (single rank, write DB):
    - top 3 novels (default)
    - full metadata
    - first 5 chapters each (default)
    """
    choices = _get_rank_choices()
    rank_key = (args.rank_key or _default_rank_key(choices)).strip()
    top_n = args.top_n if args.top_n is not None else 3
    chapter_n = args.chapter_n if args.chapter_n is not None else 5

    db = DatabaseHandler(db_path=args.db_path)
    site_cfg = _get_qidian_cfg()
    spider = _init_spider(site_cfg, db=db)

    try:
        with Timer("fetch_rank_list (+db_write)"):
            novels = _call_fetch_rank_list(
                spider=spider,
                rank_key=rank_key,
                pages=args.pages,
                top_n=top_n,
                write_db=True,
            )

        with Timer("enrich_rank_items (+db_write)"):
            for novel in novels or []:
                _call_fetch_novel_detail(spider, novel, write_db=True)

        with Timer("fetch_first_n_chapters (+db_write)"):
            for novel in novels or []:
                _call_fetch_first_n_chapters(spider, novel, chapter_n=chapter_n, write_db=True)
    finally:
        _close_spider(spider)


def test_multi_ranks(args: argparse.Namespace):
    """
    Multiple ranks test (write DB):
    - iterate rank_keys
    - small sample per rank (default top_n=1)
    - save first N chapters per book (default chapter_n=3)
    """
    choices = _get_rank_choices()
    default_csv = _default_rank_keys_csv(choices)

    rank_keys = [x.strip() for x in (args.rank_keys or default_csv).split(",") if x.strip()]
    top_n = args.top_n if args.top_n is not None else 1
    chapter_n = args.chapter_n if args.chapter_n is not None else 3

    db = DatabaseHandler(db_path=args.db_path)
    site_cfg = _get_qidian_cfg()
    spider = _init_spider(site_cfg, db=db)

    try:
        for rank_key in rank_keys:
            print(f"\n=== Rank: {rank_key} ===")

            with Timer(f"fetch_rank_list [{rank_key}] (+db_write)"):
                novels = _call_fetch_rank_list(
                    spider=spider,
                    rank_key=rank_key,
                    pages=args.pages,
                    top_n=top_n,
                    write_db=True,
                )

            with Timer(f"enrich_rank_items [{rank_key}] (+db_write)"):
                for novel in novels or []:
                    _call_fetch_novel_detail(spider, novel, write_db=True)

            if chapter_n > 0:
                with Timer(f"fetch_first_n_chapters [{rank_key}] (+db_write)"):
                    for novel in novels or []:
                        _call_fetch_first_n_chapters(
                            spider,
                            novel,
                            chapter_n=chapter_n,
                            write_db=True,
                        )
    finally:
        _close_spider(spider)

def test_smart_fetch(args: argparse.Namespace):
    choices = _get_rank_choices()
    rank_key = (args.rank_key or _default_rank_key(choices)).strip()
    run_smart_fetch(
        rank_key=rank_key,
        pages=args.pages,
        chapter_n1=args.chapter_n1,
        chapter_n2=args.chapter_n2,
        verbose=args.verbose,
    )

# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser("qidian_test")

    parser.add_argument(
        "--test",
        required=True,
        choices=["decryption", "quick", "full", "multi_ranks", "smart_fetch"],
        help="Test mode",
    )

    rank_choices = _get_rank_choices()

    parser.add_argument(
        "--rank_key",
        type=str,
        default=_default_rank_key(rank_choices),
        help="Single rank key (defaults from config.WEBSITES['qidian'].rank_urls)",
    )
    parser.add_argument(
        "--rank_keys",
        type=str,
        default=_default_rank_keys_csv(rank_choices),
        help="Multiple rank keys CSV (defaults from config.WEBSITES['qidian'].rank_urls)",
    )
    parser.add_argument("--top_n", type=int, help="Top N novels per rank (overrides defaults per test mode)")
    parser.add_argument("--chapter_n", type=int, help="First N chapters per novel (overrides defaults per test mode)")
    parser.add_argument("--pages", type=int, default=1, help="Rank pages")
    parser.add_argument("--db_path", type=str, default="test_output/qidian_test.db")
    parser.add_argument("--verbose", action="store_true")
    # Smart fetch experiment
    parser.add_argument("--chapter_n1", type=int, default=3, help="Smart fetch: first run target chapters (default: 3)")
    parser.add_argument("--chapter_n2", type=int, default=4,
                        help="Smart fetch: second run target chapters (default: 4)")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    dispatch = {
        "decryption": test_decryption,
        "quick": test_quick,
        "full": test_full,
        "multi_ranks": test_multi_ranks,
        "smart_fetch": test_smart_fetch,
    }

    dispatch[args.test](args)


if __name__ == "__main__":
    main()