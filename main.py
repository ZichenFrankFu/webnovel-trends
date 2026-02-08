# main.py
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import config
from tasks.scheduler import TaskScheduler

# --- Debug prints (keep your current behavior) ---
print("CONFIG FILE:", config.__file__)
print("max_page_retries:", config.CRAWLER_CONFIG["page_fetch"].get("max_page_retries"))

PROJECT_ROOT = Path(__file__).resolve().parent


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _split_csv(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _get_site_cfg(platform: str) -> Dict[str, Any]:
    # Your tests use: from config import WEBSITES
    # Main uses config.WEBSITES in your codebase as well.
    websites = getattr(config, "WEBSITES", {}) or {}
    return dict(websites.get(platform, {}) or {})


def _get_rank_urls(site_cfg: Dict[str, Any]) -> Dict[str, str]:
    ru = site_cfg.get("rank_urls") or {}
    return ru if isinstance(ru, dict) else {}


def _get_rank_type_map(site_cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    m = site_cfg.get("rank_type_map") or {}
    return m if isinstance(m, dict) else {}


def _resolve_rank_keys(
    *,
    platform: str,
    site_cfg: Dict[str, Any],
    rank_key: str,
    rank_csv: str,
) -> List[str]:
    rank_key = (rank_key or "").strip()
    if rank_key:
        return [rank_key]

    keys = _split_csv(rank_csv)
    if keys:
        return keys

    # default = all configured ranks
    return list(_get_rank_urls(site_cfg).keys())


def _validate_rank_keys(*, platform: str, site_cfg: Dict[str, Any], keys: List[str]) -> None:
    valid = set(_get_rank_urls(site_cfg).keys())
    bad = [k for k in keys if k not in valid]
    if bad:
        raise ValueError(
            f"[{platform}] invalid rank key(s): {bad}. "
            f"Valid keys are: {sorted(valid)}"
        )


def _init_db(db_path: Optional[str] = None):
    """
    Initialize DatabaseHandler with signature compatibility.
    If db_path is None, default to <project_root>/outputs/data/novels.db
    """
    from database.db_handler import DatabaseHandler

    if not db_path:
        default_path = PROJECT_ROOT / "outputs" / "data" / "novels.db"
        default_path.parent.mkdir(parents=True, exist_ok=True)
        db_path = str(default_path)

    # Try new signature first
    try:
        return DatabaseHandler(db_path, is_test=False)
    except TypeError:
        # Older signature
        return DatabaseHandler(db_path=db_path)


def _close_spider(spider: Any) -> None:
    for attr in ("close", "quit", "shutdown"):
        fn = getattr(spider, attr, None)
        if callable(fn):
            try:
                fn()
            except Exception:
                pass
            return


# ------------------------------------------------------------------
# Spider init adapters (signature drift safe)
# ------------------------------------------------------------------
def _init_qidian_spider(site_cfg: Dict[str, Any], db: Any):
    from spiders.qidian_spider import QidianSpider

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

    # pattern C: QidianSpider(site_cfg) then attach
    spider = QidianSpider(site_cfg)
    if hasattr(spider, "db_handler"):
        setattr(spider, "db_handler", db)
    elif hasattr(spider, "db"):
        setattr(spider, "db", db)
    return spider


def _init_fanqie_spider(site_cfg: Dict[str, Any], db: Any):
    from spiders.fanqie_spider import FanqieSpider

    # pattern A: FanqieSpider(site_cfg, db_handler=db)
    try:
        return FanqieSpider(site_cfg, db_handler=db)
    except TypeError:
        pass

    # pattern B: FanqieSpider(site_cfg, db)
    try:
        return FanqieSpider(site_cfg, db)
    except TypeError:
        pass

    # pattern C: FanqieSpider(site_cfg) then attach
    spider = FanqieSpider(site_cfg)
    if hasattr(spider, "db_handler"):
        setattr(spider, "db_handler", db)
    elif hasattr(spider, "db"):
        setattr(spider, "db", db)
    return spider


# ------------------------------------------------------------------
# Core runners (test-like interface)
# ------------------------------------------------------------------
def _run_one_platform(
    *,
    platform: str,
    rank_keys: List[str],
    qidian_pages: int,
    fanqie_pages: int,
    chapter_count: int,
    enrich_detail: bool,
    enrich_chapters: bool,
    snapshot_date: str,
    db: Any,
) -> None:
    site_cfg = _get_site_cfg(platform)
    rank_urls = _get_rank_urls(site_cfg)
    rank_type_map = _get_rank_type_map(site_cfg)

    _validate_rank_keys(platform=platform, site_cfg=site_cfg, keys=rank_keys)

    # init spider
    spider = None
    try:
        if platform == "qidian":
            spider = _init_qidian_spider(site_cfg, db=db)
        else:
            spider = _init_fanqie_spider(site_cfg, db=db)

        print("\n" + "=" * 70)
        print(f"[{platform}] ranks={len(rank_keys)} pages={(qidian_pages if platform=='qidian' else fanqie_pages)} chapters={chapter_count}")
        print("=" * 70)

        # qidian pages are read from site_config["pages_per_rank"] in your test adapter
        if platform == "qidian":
            try:
                if hasattr(spider, "site_config") and isinstance(spider.site_config, dict):
                    spider.site_config["pages_per_rank"] = int(qidian_pages)
            except Exception:
                pass

        for rk in rank_keys:
            ident = rank_type_map.get(rk, {}) or {}
            rank_family = ident.get("rank_family") or rk
            rank_sub_cat = ident.get("rank_sub_cat") or ""

            print(f"\n[{platform}] rank={rk} | rank_family={rank_family} | rank_sub_cat={rank_sub_cat or '-'} | chapters={chapter_count}")

            # 1) fetch rank list
            if platform == "qidian":
                # qidian spider signature might be fetch_rank_list(rank_type=...)
                try:
                    items = spider.fetch_rank_list(rank_type=rk)
                except TypeError:
                    items = spider.fetch_rank_list(rk)
            else:
                # fanqie test uses fetch_rank_list(rank_key, pages=pages)
                try:
                    items = spider.fetch_rank_list(rk, pages=int(fanqie_pages))
                except TypeError:
                    # fallback if pages not supported
                    items = spider.fetch_rank_list(rk)

            items = items or []
            if not items:
                print(f"[{platform}] rank={rk} empty; skip.")
                continue

            # 2) enrich metadata (details only; do NOT auto-fetch chapters here)
            enriched = items
            if enrich_detail:
                try:
                    enriched = spider.enrich_rank_items(
                        items,
                        max_books=len(items),
                        fetch_detail=True,
                        fetch_chapters=False,
                        chapter_count=0,
                    ) or []
                except TypeError:
                    # older signature fallback
                    enriched = spider.enrich_rank_items(items) or []

            # 3) save rank snapshot to DB (same idea as fanqie_test)
            if hasattr(db, "save_rank_snapshot"):
                try:
                    db.save_rank_snapshot(
                        platform=platform,
                        rank_family=rank_family,
                        rank_sub_cat=(rank_sub_cat or rk),
                        snapshot_date=snapshot_date,
                        items=enriched,
                        source_url=rank_urls.get(rk, ""),
                        make_title_primary=True,
                    )
                except TypeError:
                    # older db signature fallback (ignore extras)
                    db.save_rank_snapshot(
                        platform=platform,
                        rank_family=rank_family,
                        rank_sub_cat=(rank_sub_cat or rk),
                        snapshot_date=snapshot_date,
                        items=enriched,
                    )

            # 4) chapters
            if enrich_chapters and chapter_count > 0:
                # For qidian: spider likely writes chapters to db internally if db_handler present.
                # For fanqie: to be safe, we also upsert via db if method exists.
                for i, book in enumerate(enriched, 1):
                    url = (book.get("url") or "").strip()
                    pid = (book.get("platform_novel_id") or book.get("novel_id") or "").strip()
                    if not url or not pid:
                        continue

                    # fetch chapters
                    chapters = None
                    if platform == "qidian":
                        title = book.get("title") or ""
                        try:
                            chapters = spider.fetch_first_n_chapters(url, int(chapter_count), fallback_title=title)
                        except TypeError:
                            chapters = spider.fetch_first_n_chapters(url, int(chapter_count))
                    else:
                        try:
                            chapters = spider.fetch_first_n_chapters(url, target_chapter_count=int(chapter_count))
                        except TypeError:
                            chapters = spider.fetch_first_n_chapters(url, int(chapter_count))

                    chapters = chapters or []

                    # fanqie: ensure DB write even if spider doesn't auto-upsert
                    if platform == "fanqie" and chapters and hasattr(db, "upsert_first_n_chapters"):
                        publish_date = chapters[0].get("publish_date") or snapshot_date
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

    finally:
        if spider:
            _close_spider(spider)


def run_scheduler():
    print("[scheduler] starting TaskScheduler...")
    scheduler = TaskScheduler()
    scheduler.run_forever(interval_minutes=60)


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
def main():
    epilog = r"""
Examples:

1) 默认全量（起点+番茄，全部 config 榜单）
   python main.py once

2) 单平台：只跑番茄全部榜单
   python main.py once --platform fanqie --chapter_count 5

3) 单平台单榜单：番茄（rank_key 必须是 config.WEBSITES['fanqie']['rank_urls'] 的完整键名）
   python main.py once --platform fanqie --rank_key 阅读榜西方奇幻 --chapter_count 5
   python main.py once --platform fanqie --rank_key 新书榜科幻末世 --chapter_count 5

4) 单平台单榜单：起点
   python main.py once --platform qidian --rank_key 月票榜 --qidian_pages 5 --chapter_count 5

5) 多榜单 CSV（仍支持）
   python main.py once --platform fanqie --fanqie_ranks 阅读榜科幻末世,新书榜科幻末世 --chapter_count 5
   python main.py once --platform qidian --qidian_ranks 月票榜,畅销榜 --qidian_pages 3 --chapter_count 5
"""

    parser = argparse.ArgumentParser(
        description="WebNovel Trends - 小说热点分析系统（绕开 run_once，直接用 spider 接口跑单平台/单榜单）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )

    parser.add_argument("mode", choices=["once", "scheduler"], help="运行模式: once | scheduler")

    parser.add_argument(
        "--platform",
        choices=["all", "qidian", "fanqie"],
        default="all",
        help="选择运行平台：all(默认), qidian, fanqie",
    )

    parser.add_argument(
        "--rank_key",
        default="",
        help="只跑一个榜单（需配合 --platform qidian/fanqie；platform=all 时不允许）",
    )

    parser.add_argument("--qidian_ranks", default="", help="起点榜单 key（逗号分隔）。默认=全部")
    parser.add_argument("--fanqie_ranks", default="", help="番茄榜单 key（逗号分隔，必须为完整键名如：阅读榜科幻末世）。默认=全部")

    parser.add_argument("--qidian_pages", type=int, default=2, help="起点每榜抓取页数（默认2）")
    parser.add_argument("--fanqie_pages", type=int, default=1, help="番茄榜单页数（默认1；多数榜单无分页或固定30本）")

    parser.add_argument("--chapter_count", type=int, default=5, help="每本书抓取前 N 章（默认5）")
    parser.add_argument("--no_detail", action="store_true", help="禁用详情补全")
    parser.add_argument("--no_chapters", action="store_true", help="禁用章节抓取")

    parser.add_argument("--snapshot_date", type=str, default="", help="快照日期 YYYY-MM-DD（默认今天）")
    parser.add_argument("--db_path", type=str, default="", help="数据库路径（默认 outputs/data/novels.db）")

    args = parser.parse_args()

    print("WebNovel Trends 小说热点分析系统")
    print("当前时间:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)

    if args.mode == "scheduler":
        run_scheduler()
        return

    # once mode
    if args.platform == "all" and (args.rank_key or "").strip():
        parser.error("--platform all 时不允许使用 --rank_key（请指定 qidian 或 fanqie）")

    snapshot_date = (args.snapshot_date or "").strip() or _today()

    db = _init_db(args.db_path)

    # Resolve ranks per platform
    if args.platform in ("all", "qidian"):
        q_cfg = _get_site_cfg("qidian")
        q_rank_keys = _resolve_rank_keys(
            platform="qidian",
            site_cfg=q_cfg,
            rank_key=(args.rank_key if args.platform == "qidian" else ""),
            rank_csv=args.qidian_ranks,
        )
    else:
        q_rank_keys = []

    if args.platform in ("all", "fanqie"):
        f_cfg = _get_site_cfg("fanqie")
        f_rank_keys = _resolve_rank_keys(
            platform="fanqie",
            site_cfg=f_cfg,
            rank_key=(args.rank_key if args.platform == "fanqie" else ""),
            rank_csv=args.fanqie_ranks,
        )
    else:
        f_rank_keys = []

    enrich_detail = (not args.no_detail)
    enrich_chapters = (not args.no_chapters)

    # Run requested platforms
    if args.platform in ("all", "qidian") and q_rank_keys:
        _run_one_platform(
            platform="qidian",
            rank_keys=q_rank_keys,
            qidian_pages=int(args.qidian_pages),
            fanqie_pages=int(args.fanqie_pages),
            chapter_count=int(args.chapter_count),
            enrich_detail=enrich_detail,
            enrich_chapters=enrich_chapters,
            snapshot_date=snapshot_date,
            db=db,
        )

    if args.platform in ("all", "fanqie") and f_rank_keys:
        _run_one_platform(
            platform="fanqie",
            rank_keys=f_rank_keys,
            qidian_pages=int(args.qidian_pages),
            fanqie_pages=int(args.fanqie_pages),
            chapter_count=int(args.chapter_count),
            enrich_detail=enrich_detail,
            enrich_chapters=enrich_chapters,
            snapshot_date=snapshot_date,
            db=db,
        )


if __name__ == "__main__":
    main()
