# main.py
"""WebNovel Trends - 小说热点分析系统 CLI

保持 run_once 不改动，同时在 main 里增加：
1) 单平台 + 单榜单（--platform + --rank_key）
2) 单平台 + 全部榜单（--platform，不提供 --rank_key）
   - 新书榜（key 以“新书榜”开头）章节数可单独控制：--newbook_chapter_count（默认 2）
   - 其他榜单使用 --chapter_count（默认 5）

备注：
- 默认会抓详情页与章节；可以用 --no_detail / --no_chapters 关闭。
- 番茄 pages 固定为 1（单页滚动），--pages 会被忽略。
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import config

# 现有能力（不修改 run_once）
from tasks.run_spiders_once import run_once
from tasks.scheduler import TaskScheduler

PROJECT_ROOT = Path(__file__).resolve().parent


def _split_csv(s: str) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _resolve_db_path() -> Path:
    """Resolve novels.db path.

    兼容不同 config 写法：
    - config.DB_PATH / config.DB_FILE / config.SQLITE_PATH
    - config.OUTPUT_PATHS['data']
    - 默认 outputs/data/novels.db
    """
    for attr in ("DB_PATH", "DB_FILE", "SQLITE_PATH"):
        p = getattr(config, attr, None)
        if isinstance(p, str) and p.strip():
            pp = Path(p)
            return pp if pp.is_absolute() else (PROJECT_ROOT / pp)

    out = getattr(config, "OUTPUT_PATHS", {}) or {}
    data_dir = out.get("data") or out.get("db") or "outputs/data"
    return (PROJECT_ROOT / data_dir / "novels.db")


def _init_db(db_path: Path):
    """Init DatabaseHandler (compatible with both old/new signatures)."""
    from database.db_handler import DatabaseHandler

    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        return DatabaseHandler(str(db_path), is_test=False)
    except TypeError:
        return DatabaseHandler(db_path=str(db_path))


def _print_rank_keys(platform: str) -> str:
    websites = getattr(config, "WEBSITES", {}) or {}
    cfg = (websites.get(platform) or {})
    rank_urls = (cfg.get("rank_urls") or {})
    if not rank_urls:
        return "(none)"
    return ", ".join(list(rank_urls.keys()))


def _get_site_cfg(platform: str) -> Dict[str, Any]:
    """Return a copy of config.WEBSITES[platform]."""
    websites = getattr(config, "WEBSITES", {}) or {}
    cfg = websites.get(platform, {}) or {}
    return dict(cfg)


def _is_newbook_rank(rank_key: str) -> bool:
    return (rank_key or "").startswith("新书榜")


def _chapter_count_for_rank(*, rank_key: str, normal_count: int, newbook_count: int) -> int:
    return int(newbook_count) if _is_newbook_rank(rank_key) else int(normal_count)


def _run_single_rank(
    *,
    platform: str,
    rank_key: str,
    pages: Optional[int],
    chapter_count: int,
    newbook_chapter_count: int,
    enrich_detail: bool,
    enrich_chapters: bool,
) -> None:
    """Single platform + single rank pipeline.

    调 spider.fetch_and_save_rank()：抓取+补全+（可选）章节抓取+写库。
    """
    db_path = _resolve_db_path()
    db = _init_db(db_path)
    print(f"[db] initialized: {db_path}")

    platform = (platform or "").strip().lower()

    if platform == "qidian":
        from spiders.qidian_spider import QidianSpider

        site_cfg: Dict[str, Any] = _get_site_cfg("qidian")
        spider = QidianSpider(site_cfg, db)

        rank_urls = (spider.site_config.get("rank_urls") or {})
        if rank_key not in rank_urls:
            raise SystemExit(
                f"[qidian] unknown rank_key: {rank_key}.\n"
                f"Available: {_print_rank_keys('qidian')}"
            )

        cc = _chapter_count_for_rank(
            rank_key=rank_key, normal_count=chapter_count, newbook_count=newbook_chapter_count
        )

        print(
            f"\n[{platform}] ===== Start single rank =====\n"
            f"rank_key={rank_key}\n"
            f"chapter_count={cc}\n"
        )

        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=pages,
            enrich_detail=enrich_detail,
            enrich_chapters=enrich_chapters,
            chapter_count=cc,
        )
        ident = f"{result.get('rank_family','')}|{result.get('rank_sub_cat','')}".strip("|")
        print(
            f"\n[qidian] done: rank={rank_key} ({ident}) "
            f"items={len(result.get('items') or [])} snapshot_id={result.get('snapshot_id')}"
        )
        spider.close()
        return

    if platform == "fanqie":
        from spiders.fanqie_spider import FanqieSpider

        site_cfg: Dict[str, Any] = _get_site_cfg("fanqie")
        spider = FanqieSpider(site_cfg, db)

        rank_urls = (spider.site_config.get("rank_urls") or {})
        if rank_key not in rank_urls:
            raise SystemExit(
                f"[fanqie] unknown rank_key: {rank_key}.\n"
                f"Available: {_print_rank_keys('fanqie')}"
            )

        cc = _chapter_count_for_rank(
            rank_key=rank_key, normal_count=chapter_count, newbook_count=newbook_chapter_count
        )

        print(
            f"\n[{platform}] ===== Start single rank =====\n"
            f"rank_key={rank_key}\n"
            f"chapter_count={cc}\n"
        )

        # 番茄榜单只有 1 页（滚动加载），pages 参数无意义
        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=None,
            enrich_detail=enrich_detail,
            enrich_chapters=enrich_chapters,
            chapter_count=cc,
        )
        ident = f"{result.get('rank_family','')}|{result.get('rank_sub_cat','')}".strip("|")
        print(
            f"\n[fanqie] done: rank={rank_key} ({ident}) "
            f"items={len(result.get('items') or [])} snapshot_id={result.get('snapshot_id')}"
        )
        spider.close()
        return

    raise SystemExit("--platform must be one of: qidian, fanqie")


def _run_platform_all_ranks(
    *,
    platform: str,
    pages: Optional[int],
    chapter_count: int,
    newbook_chapter_count: int,
    enrich_detail: bool,
    enrich_chapters: bool,
) -> None:
    """Single platform + ALL ranks.

    逐榜单调用 fetch_and_save_rank()，从而对“新书榜 vs 非新书榜”使用不同章节数。
    """
    db_path = _resolve_db_path()
    db = _init_db(db_path)
    print(f"[db] initialized: {db_path}")

    platform = (platform or "").strip().lower()

    if platform == "qidian":
        from spiders.qidian_spider import QidianSpider

        site_cfg: Dict[str, Any] = _get_site_cfg("qidian")
        spider = QidianSpider(site_cfg, db)
        rank_urls = (spider.site_config.get("rank_urls") or {})
        if not rank_urls:
            raise SystemExit("[qidian] rank_urls is empty in config.WEBSITES['qidian'].")

        # pages: if user doesn't pass --pages, fall back to --qidian_pages in legacy args handled by CLI;
        # here we accept pages as already resolved.
        total = len(rank_urls)
        for idx, rk in enumerate(rank_urls.keys(), 1):
            cc = _chapter_count_for_rank(
                rank_key=rk,
                normal_count=chapter_count,
                newbook_count=newbook_chapter_count,
            )

            print(
                f"\n[qidian] ===== Switching rank ({idx}/{total}) =====\n"
                f"rank_key={rk}\n"
                f"pages={pages}\n"
                f"chapter_count={cc}\n"
            )

            spider.fetch_and_save_rank(
                rank_type=rk,
                pages=pages,
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
                chapter_count=cc,
            )

        spider.close()
        return

    if platform == "fanqie":
        from spiders.fanqie_spider import FanqieSpider

        site_cfg: Dict[str, Any] = _get_site_cfg("fanqie")
        spider = FanqieSpider(site_cfg, db)
        rank_urls = (spider.site_config.get("rank_urls") or {})
        if not rank_urls:
            raise SystemExit("[fanqie] rank_urls is empty in config.WEBSITES['fanqie'].")

        # 分开新书榜 / 其他榜单（便于日志清晰，也符合你的需求）
        newbook_ranks = [k for k in rank_urls.keys() if _is_newbook_rank(k)]
        other_ranks = [k for k in rank_urls.keys() if not _is_newbook_rank(k)]

        if other_ranks:
            print(f"[fanqie] other ranks: {len(other_ranks)} | chapter_count={chapter_count}")
        if newbook_ranks:
            print(f"[fanqie] newbook ranks: {len(newbook_ranks)} | newbook_chapter_count={newbook_chapter_count}")

        # 其他榜单：使用 chapter_count
        for idx, rk in enumerate(other_ranks, 1):
            print(
                f"\n[fanqie] ===== Switching rank ({idx}/{len(other_ranks)}): {rk} "
                f"(chapter_count={chapter_count}) ====="
            )
            spider.fetch_and_save_rank(
                rank_type=rk,
                pages=None,
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
                chapter_count=int(chapter_count),
            )

        # 新书榜：使用 newbook_chapter_count（默认 2）
        for idx, rk in enumerate(newbook_ranks, 1):
            print(
                f"\n[fanqie] ===== Switching rank ({idx}/{len(other_ranks)}): {rk} "
                f"(chapter_count={chapter_count}) ====="
            )
            spider.fetch_and_save_rank(
                rank_type=rk,
                pages=None,
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
                chapter_count=int(newbook_chapter_count),
            )

        spider.close()
        return

    raise SystemExit("--platform must be one of: qidian, fanqie")


def run_scheduler() -> None:
    print("[scheduler] starting TaskScheduler...")
    scheduler = TaskScheduler()
    scheduler.run_forever(interval_minutes=60)


def build_parser() -> argparse.ArgumentParser:
    websites = getattr(config, "WEBSITES", {}) or {}
    fanqie_keys = ", ".join((websites.get("fanqie", {}) or {}).get("rank_urls", {}).keys())
    qidian_keys = ", ".join((websites.get("qidian", {}) or {}).get("rank_urls", {}).keys())

    epilog_lines = [
        "Examples:",
        "  # 全量抓取（run_once，不变）",
        "  python main.py once --qidian_pages 2 --chapter_count 5",
        "",
        "  # 单平台全榜（新书榜章节数可单独控制）",
        "  python main.py once --platform fanqie --chapter_count 5 --newbook_chapter_count 2",
        "  python main.py once --platform qidian --pages 2 --chapter_count 5",
        "",
        "  # 单平台单榜（推荐；fetch_and_save_rank，抓取+保存由 spider 完成）",
        "  python main.py once --platform fanqie --rank_key \"新书榜科幻末世\" --chapter_count 5 --newbook_chapter_count 2",
        "  python main.py once --platform qidian --rank_key \"月票榜\" --pages 2 --chapter_count 5",
        "",
        "Available rank keys (from config.py):",
        f"  qidian: {qidian_keys or '(none)'}",
        f"  fanqie: {fanqie_keys or '(none)'}",
    ]

    parser = argparse.ArgumentParser(
        description="WebNovel Trends - 小说热点分析系统",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="\n".join(epilog_lines),
    )

    parser.add_argument(
        "mode",
        choices=["once", "scheduler"],
        help="运行模式: once(单次运行), scheduler(定时任务)",
    )

    # --- Single platform (all ranks or single rank) ---
    parser.add_argument(
        "--platform",
        choices=["qidian", "fanqie"],
        default=None,
        help="只运行指定平台：不提供 --rank_key 时=跑该平台全部榜单；提供 --rank_key 时=只跑该榜单。",
    )
    parser.add_argument(
        "--rank_key",
        default=None,
        help="指定平台的榜单 key（必须与 config.py 中的 rank_urls key 完全一致）。",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=None,
        help="仅起点有效：每个榜单抓取页数。番茄忽略（固定 1 页滚动加载）。",
    )

    # --- run_once controls (keep) ---
    parser.add_argument(
        "--qidian_ranks",
        default="",
        help="起点榜单类型（逗号分隔）。默认=运行 config 里 rank_type_map 的所有榜单",
    )
    parser.add_argument(
        "--qidian_pages",
        type=int,
        default=2,
        help="起点每个榜单抓取页数，默认=2（约40本书）",
    )
    parser.add_argument(
        "--fanqie_ranks",
        default="",
        help="番茄榜单类型（逗号分隔）。默认=运行 config 里 rank_type_map 的所有榜单",
    )

    # Chapters
    parser.add_argument(
        "--chapter_count",
        type=int,
        default=5,
        help="非新书榜：每本书抓取并存储的前N章，默认=5",
    )
    parser.add_argument(
        "--newbook_chapter_count",
        type=int,
        default=2,
        help="新书榜（rank_key 以“新书榜”开头）：每本书抓取并存储的前N章，默认=2",
    )

    # Optional switches
    parser.add_argument("--no_detail", action="store_true", help="禁用详情页补全")
    parser.add_argument("--no_chapters", action="store_true", help="禁用章节抓取")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    print("WebNovel Trends 小说热点分析系统")
    print("当前时间:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)

    if args.mode == "scheduler":
        run_scheduler()
        return

    enrich_detail = (not args.no_detail)
    enrich_chapters = (not args.no_chapters)

    # 单平台模式（优先）：不传 rank_key => 全榜；传 rank_key => 单榜
    if args.platform:
        if args.rank_key:
            _run_single_rank(
                platform=args.platform,
                rank_key=args.rank_key,
                pages=args.pages if args.platform == "qidian" else None,
                chapter_count=int(args.chapter_count),
                newbook_chapter_count=int(args.newbook_chapter_count),
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
            )
        else:
            # 平台全榜：起点 pages 优先用 --pages，否则退回 legacy 的 --qidian_pages
            pages = None
            if args.platform == "qidian":
                pages = int(args.pages) if args.pages is not None else int(args.qidian_pages)

            _run_platform_all_ranks(
                platform=args.platform,
                pages=pages,
                chapter_count=int(args.chapter_count),
                newbook_chapter_count=int(args.newbook_chapter_count),
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
            )
        return

    # 否则：走原 run_once（不改动）
    run_once(
        qidian_rank_types=_split_csv(args.qidian_ranks) or None,
        qidian_pages=int(args.qidian_pages),
        fanqie_rank_types=_split_csv(args.fanqie_ranks) or None,
        chapter_count=int(args.chapter_count),
        enrich_detail=enrich_detail,
        enrich_chapters=enrich_chapters,
    )


if __name__ == "__main__":
    main()
