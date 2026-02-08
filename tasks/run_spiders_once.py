# tasks/jobs.py
from __future__ import annotations
from datetime import datetime

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent

import config
print("CONFIG FILE:", config.__file__)
print("max_page_retries:", config.CRAWLER_CONFIG["page_fetch"].get("max_page_retries"))

from spiders.qidian_spider import QidianSpider
from spiders.fanqie_spider import FanqieSpider
from database.db_handler import DatabaseHandler
import config


def run_once(
    *,
    qidian_rank_types: list[str] | None = None,
    qidian_pages: int = 5,
    fanqie_rank_types: list[str] | None = None,
    chapter_count: int = 5,
    enrich_detail: bool = True,
    enrich_chapters: bool = True,
):
    """Run a single full crawl for Qidian + Fanqie and persist results into SQLite."""

    # ------------------------------------------------------------------
    # Init DB
    # ------------------------------------------------------------------
    print("[DEBUG] run_once started")

    db_cfg_path = Path(config.DATABASE["path"])

    if db_cfg_path.is_absolute():
        db_path = db_cfg_path
    else:
        db_path = (PROJECT_ROOT / db_cfg_path).resolve()

    db_path.parent.mkdir(parents=True, exist_ok=True)

    db = DatabaseHandler(str(db_path))
    print(f"[db] initialized: {db_path}")

    # ------------------------------------------------------------------
    # Init spiders
    # ------------------------------------------------------------------
    qidian_config = config.WEBSITES["qidian"]
    fanqie_config = config.WEBSITES["fanqie"]

    qidian_spider = QidianSpider(qidian_config, db_handler=db)
    fanqie_spider = FanqieSpider(fanqie_config, db_handler=db)

    print("[spider] initialized: qidian + fanqie")

    # ------------------------------------------------------------------
    # Rank types defaults: run all rank_type_map keys (as requested)
    # ------------------------------------------------------------------
    if not qidian_rank_types:
        qidian_rank_types = list((qidian_config.get("rank_type_map") or {}).keys())
        if not qidian_rank_types:
            # fallback (should rarely happen)
            qidian_rank_types = list((qidian_config.get("rank_urls") or {}).keys())

    if not fanqie_rank_types:
        fanqie_rank_types = list((fanqie_config.get("rank_type_map") or {}).keys())
        if not fanqie_rank_types:
            fanqie_rank_types = list((fanqie_config.get("rank_urls") or {}).keys())

    # ------------------------------------------------------------------
    # Crawl Qidian
    # Default: 3 pages ~ 60 books (20/page) + first 5 chapters
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"[qidian] ranks={len(qidian_rank_types)} pages={qidian_pages} chapters={chapter_count}")
    print("=" * 70)

    qidian_ok = 0
    for rank_type in qidian_rank_types:
        print(f"  -> rank: {rank_type}")
        try:
            result = qidian_spider.fetch_and_save_rank(
                rank_type=rank_type,
                pages=int(qidian_pages),
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
                chapter_count=int(chapter_count),
                # 让“页数”主导数量：5页≈100本
                max_books=int(qidian_pages) * 20,
            )
            qidian_ok += 1
            print(f"     done: items={len(result.get('items', []))}")
        except Exception as e:
            print(f"     failed: {e}")

    # ------------------------------------------------------------------
    # Crawl Fanqie
    # Default: each rank ~ 30 books + first 5 chapters
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print(f"[fanqie] ranks={len(fanqie_rank_types)} chapters={chapter_count}")
    print("=" * 70)

    fanqie_ok = 0
    for rank_type in fanqie_rank_types:
        print(f"  -> rank: {rank_type}")
        try:
            result = fanqie_spider.fetch_and_save_rank(
                rank_type=rank_type,
                pages=1,  # 番茄榜单固定单页
                enrich_detail=enrich_detail,
                enrich_chapters=enrich_chapters,
                chapter_count=int(chapter_count),
                max_books=30,  # 抓满 30 本
            )
            fanqie_ok += 1
            print(f"     done: items={len(result.get('items', []))}")
        except Exception as e:
            print(f"     failed: {e}")

    # ------------------------------------------------------------------
    # Close spiders
    # ------------------------------------------------------------------
    try:
        qidian_spider.close()
    except Exception:
        pass
    try:
        fanqie_spider.close()
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    today = datetime.now().strftime("%Y-%m-%d")
    print("\n" + "=" * 70)
    print(f"[summary] {today}")
    print(f"  qidian ranks finished: {qidian_ok}/{len(qidian_rank_types)}")
    print(f"  fanqie ranks finished: {fanqie_ok}/{len(fanqie_rank_types)}")

    try:
        q_today = db.get_today_rankings(platform="qidian")
        f_today = db.get_today_rankings(platform="fanqie")
        print(f"  qidian books today: {len(q_today)}")
        print(f"  fanqie books today: {len(f_today)}")
    except Exception as e:
        print(f"  db stats failed: {e}")
    print("=" * 70)
    print("[done] crawl finished.")

    print("[DEBUG] run_once finished")
