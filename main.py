# main.py
import argparse
from datetime import datetime
from tasks.scheduler import TaskScheduler

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
from tasks.run_spiders_once import run_once
from tasks.scheduler import TaskScheduler
import config
print("CONFIG FILE:", config.__file__)
print("max_page_retries:", config.CRAWLER_CONFIG["page_fetch"].get("max_page_retries"))


def _split_csv(s: str) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]



def run_scheduler():
    print("[scheduler] starting TaskScheduler...")
    scheduler = TaskScheduler()
    scheduler.run_forever(interval_minutes=60)



def main():
    parser = argparse.ArgumentParser(description="WebNovel Trends - 小说热点分析系统")

    parser.add_argument(
        "mode",
        choices=["once", "scheduler"],
        help="运行模式: once(单次运行), scheduler(定时任务)",
    )

    # Rank controls (exactly as you requested)
    parser.add_argument(
        "--qidian_ranks",
        default="",
        help="起点榜单类型（逗号分隔）。默认=运行 config 里 rank_type_map 的所有榜单",
    )
    parser.add_argument(
        "--qidian_pages",
        type=int,
        default=3,
        help="起点每个榜单抓取页数，默认=3（约60本书）",
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

    args = parser.parse_args()

    print("WebNovel Trends 小说热点分析系统")
    print("当前时间:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)

    if args.mode == "once":
        run_once(
            qidian_rank_types=_split_csv(args.qidian_ranks) or None,
            qidian_pages=int(args.qidian_pages),
            fanqie_rank_types=_split_csv(args.fanqie_ranks) or None,
            chapter_count=int(args.chapter_count),
            enrich_detail=(not args.no_detail),
            enrich_chapters=(not args.no_chapters),
        )
    elif args.mode == "scheduler":
        run_scheduler()


if __name__ == "__main__":
    main()
