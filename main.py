# main.py
"""WebNovel Trends - 小说热点分析系统 CLI

保持 run_once 不改动，同时在 main 里增加“单平台 + 单榜单”抓取入口。

用法示例：

1) 全量抓取（与你之前一致；会跑两个平台的默认榜单集合）
   python main.py once --qidian_pages 2 --chapter_count 5

2) 单平台 + 单榜单（复用 test 同款接口 fetch_and_save_rank；只跑指定平台/榜单）
   - 番茄：
     python main.py once --platform fanqie --rank_key "新书榜科幻末世" --chapter_count 5

   - 起点：
     python main.py once --platform qidian --rank_key "月票榜" --pages 2 --chapter_count 5

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
    # 1) explicit path
    for attr in ("DB_PATH", "DB_FILE", "SQLITE_PATH"):
        p = getattr(config, attr, None)
        if isinstance(p, str) and p.strip():
            pp = Path(p)
            return pp if pp.is_absolute() else (PROJECT_ROOT / pp)

    # 2) OUTPUT_PATHS
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
    keys = list(rank_urls.keys())
    return ", ".join(keys)


def _run_single_rank(
    *,
    platform: str,
    rank_key: str,
    pages: Optional[int],
    chapter_count: int,
    enrich_detail: bool,
    enrich_chapters: bool,
) -> None:
    """Single platform + single rank pipeline.

    关键：调用 spider.fetch_and_save_rank()，让 spider 自己负责：
    - rank 抓取
    - detail 补全
    - 章节抓取
    - rank snapshot / chapters 写库
    """

    db_path = _resolve_db_path()
    db = _init_db(db_path)
    print(f"[db] initialized: {db_path}")

    platform = (platform or "").strip().lower()

    if platform == "qidian":
        from spiders.qidian_spider import QidianSpider

        site_cfg: Dict[str, Any] = {}  # 让 spider 自己 merge config.WEBSITES['qidian']
        spider = QidianSpider(site_cfg, db)

        rank_urls = (spider.site_config.get("rank_urls") or {})
        if rank_key not in rank_urls:
            raise SystemExit(
                f"[qidian] unknown rank_key: {rank_key}.\n"
                f"Available: {_print_rank_keys('qidian')}"
            )

        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=pages,
            enrich_detail=enrich_detail,
            enrich_chapters=enrich_chapters,
            chapter_count=chapter_count,
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

        site_cfg: Dict[str, Any] = {}
        spider = FanqieSpider(site_cfg, db)

        rank_urls = (spider.site_config.get("rank_urls") or {})
        if rank_key not in rank_urls:
            raise SystemExit(
                f"[fanqie] unknown rank_key: {rank_key}.\n"
                f"Available: {_print_rank_keys('fanqie')}"
            )

        # 番茄榜单只有 1 页（滚动加载），pages 参数无意义
        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=None,
            enrich_detail=enrich_detail,
            enrich_chapters=enrich_chapters,
            chapter_count=chapter_count,
        )
        ident = f"{result.get('rank_family','')}|{result.get('rank_sub_cat','')}".strip("|")
        print(
            f"\n[fanqie] done: rank={rank_key} ({ident}) "
            f"items={len(result.get('items') or [])} snapshot_id={result.get('snapshot_id')}"
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
        "  # 单平台单榜（推荐；和 tests 同款接口）",
        "  python main.py once --platform fanqie --rank_key \"新书榜科幻末世\" --chapter_count 5",
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

    # --- Single platform / single rank ---
    parser.add_argument(
        "--platform",
        choices=["qidian", "fanqie"],
        default=None,
        help="只运行指定平台（与 --rank_key 配合使用）。不提供则走 run_once（全量）。",
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
        help="起点/番茄：每本书抓取并存储的前N章，默认=5",
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

    # mode == once
    enrich_detail = (not args.no_detail)
    enrich_chapters = (not args.no_chapters)

    # 单平台单榜：优先
    if args.platform or args.rank_key:
        if not args.platform or not args.rank_key:
            raise SystemExit("单平台单榜模式必须同时提供 --platform 和 --rank_key")

        _run_single_rank(
            platform=args.platform,
            rank_key=args.rank_key,
            pages=args.pages,
            chapter_count=int(args.chapter_count),
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
