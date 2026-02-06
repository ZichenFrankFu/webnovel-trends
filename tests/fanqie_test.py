# tests/fanqie_test.py
"""
Fanqie Spider Comprehensive Test (DB-backed)

This test is aligned with the current FanqieSpider implementation:
- FanqieSpider.fetch_rank_list(rank_type)  (pages controlled by site_config["pages_per_rank"])
- FanqieSpider.fetch_novel_detail(novel_url, novel_id="")
- FanqieSpider.enrich_books_with_details(books, max_books=...)
- FanqieSpider.fetch_first_n_chapters(novel_url, n=...)
- FanqieSpider.fetch_and_save_rank(...)  (one-stop pipeline)
- FanqieSpider.fetch_whole_rank()
- FanqieSpider._decrypt_text / _decrypt_html

It also validates DB inserts via DatabaseHandler.save_rank_snapshot and FIRST_N_CHAPTERS tables.
"""

import os
import sys
import shutil
import time
import sqlite3
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional


def _project_root() -> str:
    """Return project root path (repo root)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _ensure_clean_dirs(project_root: str):
    """Clean old test artifacts and ensure required directories exist."""
    test_output_dir = os.path.join(project_root, "test_output")
    os.makedirs(test_output_dir, exist_ok=True)

    # remove test db
    db_path = os.path.join(test_output_dir, "fanqie_test.db")
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"[cleanup] removed: {db_path}")
        except Exception as e:
            print(f"[cleanup] failed to remove {db_path}: {e}")

    # move outputs/debug -> test_output/debug (if exists in repo root)
    src_debug = os.path.join(project_root, "outputs", "debug")
    dst_debug = os.path.join(test_output_dir, "debug")
    if os.path.exists(src_debug):
        try:
            if os.path.exists(dst_debug):
                shutil.rmtree(dst_debug)
            os.makedirs(test_output_dir, exist_ok=True)
            shutil.move(src_debug, dst_debug)
            print(f"[move] {src_debug} -> {dst_debug}")
        except Exception as e:
            print(f"[move] failed to move debug dir: {e}")

    os.makedirs(dst_debug, exist_ok=True)


def _open_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _print_table_counts(db: Any):
    counts = db.get_table_counts()
    print("\n[db] table counts:")
    for k in [
        "novels",
        "novel_titles",
        "tags",
        "novel_tag_map",
        "rank_lists",
        "rank_snapshots",
        "rank_entries",
        "first_n_chapters",
    ]:
        print(f"  - {k}: {counts.get(k, 0)}")


def _peek_some_rows(db_path: str):
    conn = _open_sqlite(db_path)
    cur = conn.cursor()

    print("\n[db] rank_lists (fanqie) latest 5:")
    cur.execute(
        """
        SELECT rank_list_id, platform, rank_family, rank_sub_cat, source_url
        FROM rank_lists
        WHERE platform='fanqie'
        ORDER BY rank_list_id DESC
        LIMIT 5
        """
    )
    for r in cur.fetchall():
        print(f"  - #{r['rank_list_id']} {r['rank_family']} / {r['rank_sub_cat']}")

    print("\n[db] rank_snapshots (fanqie) latest 5:")
    cur.execute(
        """
        SELECT rs.snapshot_id, rs.rank_list_id, rs.snapshot_date, rs.item_count
        FROM rank_snapshots rs
        JOIN rank_lists rl ON rl.rank_list_id = rs.rank_list_id
        WHERE rl.platform='fanqie'
        ORDER BY rs.snapshot_id DESC
        LIMIT 5
        """
    )
    for r in cur.fetchall():
        print(f"  - snapshot#{r['snapshot_id']} list#{r['rank_list_id']} {r['snapshot_date']} items={r['item_count']}")

    print("\n[db] rank_entries (latest fanqie snapshot) top 5:")
    cur.execute(
        """
        WITH latest AS (
          SELECT MAX(rs.snapshot_id) AS sid
          FROM rank_snapshots rs
          JOIN rank_lists rl ON rl.rank_list_id=rs.rank_list_id
          WHERE rl.platform='fanqie'
        )
        SELECT re.rank, n.platform_novel_id, COALESCE(nt.title, '') AS title, n.author, n.main_category
        FROM rank_entries re
        JOIN latest l ON l.sid=re.snapshot_id
        JOIN novels n ON n.novel_uid=re.novel_uid
        LEFT JOIN novel_titles nt ON nt.novel_uid=n.novel_uid AND nt.is_primary=1
        ORDER BY re.rank ASC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)")
    for r in rows:
        t = r["title"] or "(no primary title)"
        print(f"  - #{r['rank']:>3} 《{t[:20]}》 / {r['author']} / {r['main_category']}")

    print("\n[db] first_n_chapters (fanqie) latest 5:")
    cur.execute(
        """
        SELECT fnc.novel_uid, n.platform_novel_id, fnc.chapter_num, fnc.chapter_title, fnc.word_count, fnc.publish_date
        FROM first_n_chapters fnc
        JOIN novels n ON n.novel_uid=fnc.novel_uid
        WHERE n.platform='fanqie'
        ORDER BY fnc.chapter_id DESC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)")
    else:
        for r in rows:
            print(
                f"  - {r['platform_novel_id']} ch{r['chapter_num']} {r['chapter_title'][:18]}... words={r['word_count']} date={r['publish_date']}")

    conn.close()


def _choose_sample_book(books: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pick first book with a usable url/id."""
    for b in books or []:
        if b.get("url") and b.get("platform_novel_id"):
            return b
    return books[0] if books else None


def _test_rank_pipeline_fast(spider: Any, db: Any, *, rank_key: str, pages: int, limit_books: int, top_n: int,
                             chapter_n: int):
    """
    优化的测试流程：
    1. 获取排行榜前limit_books本小说的metadata
    2. 只为前top_n本小说获取前chapter_n章内容

    Args:
        limit_books: 限制获取的小说数量（从榜单中取前limit_books本）
        top_n: 为前top_n本小说获取章节内容
        chapter_n: 每本小说获取的章节数
    """
    print("\n" + "=" * 80)
    print(
        f"[case] rank_pipeline_fast: rank_key={rank_key} pages={pages} limit_books={limit_books} top_n={top_n} chapter_n={chapter_n}")
    print("=" * 80)
    print(f"[info] 优化模式: 获取前{limit_books}本小说metadata, 只给前{top_n}本获取{chapter_n}章内容")

    # control pages by config
    spider.site_config["pages_per_rank"] = int(pages)

    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    source_url = (spider.site_config.get("rank_urls") or {}).get(rank_key, "")

    try:
        # 第一步：获取排行榜前limit_books本小说并丰富metadata（不获取章节）
        print(f"[step 1] 获取排行榜前{limit_books}本小说并丰富metadata...")

        # 直接使用fetch_and_save_rank，限制max_books参数
        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=pages,
            enrich_detail=True,
            enrich_chapters=False,  # 不获取章节
            chapter_count=0,
            snapshot_date=snapshot_date,
            max_books=limit_books,  # 限制处理的小说数量
        )
        items = result.get("items") or []
        snapshot_id = result.get("snapshot_id")
        print(f"[ok] fetch_and_save_rank -> items={len(items)} snapshot_id={snapshot_id}")

        if not items:
            print("[warning] 没有获取到任何小说数据")
            return {"items": [], "snapshot_id": None}

        # 第二步：只为前top_n本小说获取章节
        if items and top_n > 0 and chapter_n > 0:
            # 确保top_n不超过limit_books
            actual_top_n = min(top_n, len(items))
            print(f"\n[step 2] 为前{actual_top_n}本小说获取前{chapter_n}章内容...")
            for i, book in enumerate(items[:actual_top_n], 1):
                title = book.get('title', '未知')
                novel_id = book.get('platform_novel_id', '')
                novel_url = book.get('url', '')

                if not novel_url or not novel_id:
                    print(f"  [{i}] 跳过: 缺少URL或ID")
                    continue

                print(f"  [{i}] 《{title[:30]}》 - 获取{chapter_n}章内容")

                try:
                    chapters = spider.fetch_first_n_chapters(novel_url, n=chapter_n)
                    if chapters:
                        print(f"     成功获取{len(chapters)}章内容")

                        # 保存章节到数据库
                        if hasattr(db, "upsert_first_n_chapters"):
                            # 获取小说详情用于保存
                            detail = spider.fetch_novel_detail(novel_url, novel_id)

                            db.upsert_first_n_chapters(
                                platform="fanqie",
                                platform_novel_id=novel_id,
                                publish_date=chapters[0].get("publish_date", snapshot_date),
                                chapters=chapters,
                                novel_fallback_fields={
                                    "title": detail.get("title", title),
                                    "author": detail.get("author", book.get("author", "")),
                                    "intro": detail.get("intro", book.get("intro", "")),
                                    "main_category": detail.get("main_category", book.get("main_category", "")),
                                    "status": detail.get("status", book.get("status", "")),
                                    "total_words": detail.get("total_words", book.get("total_words", 0)),
                                    "url": novel_url,
                                    "tags": detail.get("tags", book.get("tags", [])),
                                },
                            )
                            print(f"     已保存到数据库")
                        else:
                            print(f"     [debug] 数据库不支持upsert_first_n_chapters")
                    else:
                        print(f"     获取章节失败或没有章节")

                    # 在获取章节之间添加延迟
                    if i < actual_top_n:
                        print(f"     等待2秒...")
                        time.sleep(2)

                except Exception as e:
                    print(f"     获取章节时出错: {e}")
                    continue

        # 预览结果
        if items:
            print(f"\n[preview] 前{min(3, len(items))}本小说metadata:")
            for i, b in enumerate(items[:3], 1):
                print(f"  {i}. 《{(b.get('title') or '')[:30]}》")
                print(
                    f"     author={b.get('author', '')} cat={b.get('main_category', '')} status={b.get('status', '')}")
                print(f"     rank={b.get('rank')} reading={b.get('reading_count')} words={b.get('total_words')}")
                tgs = b.get("tags") or []
                if tgs:
                    print(f"     tags={', '.join(tgs[:4])}{'...' if len(tgs) > 4 else ''}")
                print(f"     url={b.get('url', '')[:80]}")

        _print_table_counts(db)
        return {"items": items, "snapshot_id": snapshot_id}

    except Exception as e:
        print(f"[error] rank_pipeline_fast failed: {e}")
        return {"items": [], "snapshot_id": None}


def _test_rank_pipeline(spider: Any, db: Any, *, rank_key: str, pages: int, limit_books: int, top_n: int,
                        fetch_chapters: bool,
                        chapter_n: int):
    """
    Primary integration test:
    - fetch_and_save_rank (preferred)
    - otherwise manual: fetch_rank_list -> enrich_books_with_details -> db.save_rank_snapshot
    - optionally fetch chapters and upsert (if db supports upsert_first_n_chapters)
    """
    print("\n" + "=" * 80)
    print(
        f"[case] rank_pipeline: rank_key={rank_key} pages={pages} limit_books={limit_books} top_n={top_n} fetch_chapters={fetch_chapters} chapter_n={chapter_n}")
    print("=" * 80)

    # 使用快速模式
    return _test_rank_pipeline_fast(spider, db, rank_key=rank_key, pages=pages, limit_books=limit_books, top_n=top_n,
                                    chapter_n=chapter_n)


def _test_novel_detail(spider: Any, *, novel_url: str, novel_id: str):
    print("\n" + "=" * 80)
    print("[case] novel_detail")
    print("=" * 80)
    try:
        detail = spider.fetch_novel_detail(novel_url, novel_id or "")
        print(
            f"[ok] title=《{detail.get('title', '')[:30]}》 author={detail.get('author', '')} cat={detail.get('main_category', '')}")
        print(
            f"     status={detail.get('status', '')} words={detail.get('total_words', 0)} first_upload_date={detail.get('first_upload_date', '')}")
        print(f"     tags={detail.get('tags', [])}")
        return detail
    except Exception as e:
        print(f"[error] fetch_novel_detail failed: {e}")
        return {}


def _test_chapters(spider: Any, db: Any, *, novel_url: str, platform_novel_id: str, n: int):
    print("\n" + "=" * 80)
    print(f"[case] chapters n={n}")
    print("=" * 80)
    try:
        chapters = spider.fetch_first_n_chapters(novel_url, n=n)
        print(f"[ok] fetched chapters: {len(chapters)}")

        if chapters:
            # 打印章节详情
            for i, ch in enumerate(chapters[:min(5, len(chapters))], 1):
                print(
                    f"  {i}. {ch.get('chapter_title', '')[:30]}... words={ch.get('word_count', 0)} date={ch.get('publish_date', '')}")
                preview = (ch.get("chapter_content") or "")[:100].replace("\n", " ")
                if preview:
                    print(f"     preview: {preview}...")

            # 保存到数据库
            if hasattr(db, "upsert_first_n_chapters"):
                print(f"[debug] Saving {len(chapters)} chapters to database...")

                # 获取小说信息以便保存
                novel_info = {}
                try:
                    detail = spider.fetch_novel_detail(novel_url, platform_novel_id)
                    novel_info = {
                        "title": detail.get("title", ""),
                        "author": detail.get("author", ""),
                        "intro": detail.get("intro", ""),
                        "main_category": detail.get("main_category", ""),
                        "status": detail.get("status", ""),
                        "total_words": detail.get("total_words", 0),
                        "tags": detail.get("tags", []),
                    }
                except:
                    novel_info = {
                        "title": "",
                        "author": "",
                        "intro": "",
                        "main_category": "",
                        "status": "",
                        "total_words": 0,
                        "tags": [],
                    }

                # 使用第一个章节的发布日期，如果没有则使用当前日期
                publish_date = chapters[0].get("publish_date", datetime.now().strftime("%Y-%m-%d"))
                print(f"[debug] Using publish_date: {publish_date}")

                # 调用数据库保存方法
                result = db.upsert_first_n_chapters(
                    platform="fanqie",
                    platform_novel_id=platform_novel_id,
                    publish_date=publish_date,
                    chapters=chapters,
                    novel_fallback_fields={
                        "title": novel_info["title"],
                        "author": novel_info["author"],
                        "intro": novel_info["intro"],
                        "main_category": novel_info["main_category"],
                        "status": novel_info["status"],
                        "total_words": novel_info["total_words"],
                        "url": novel_url,
                        "tags": novel_info["tags"],
                    },
                )
                print(f"[db] upsert_first_n_chapters -> saved {len(chapters)} chapters")
        else:
            print("[warning] No chapters fetched")

        _print_table_counts(db)
        return chapters
    except Exception as e:
        print(f"[error] fetch_first_n_chapters failed: {e}")
        return []


def _test_decryption(spider: Any):
    print("\n" + "=" * 80)
    print("[case] decryption")
    print("=" * 80)

    # Note: FANQIE_CHAR_MAP depends on actual font mapping; this is a smoke test.
    samples = [
        "这是一段普通文本",
        "<div>普通HTML文本</div>",
    ]
    for s in samples:
        try:
            if "<" in s:
                out = spider._decrypt_html(s)
                print(f"  html: {s} -> {out}")
            else:
                out = spider._decrypt_text(s)
                print(f"  text: {s} -> {out}")
        except Exception as e:
            print(f"  error decrypting {s}: {e}")


def run_comprehensive_fanqie_test(
        *,
        test_cases: Optional[List[str]] = None,
        pages: int = 1,
        limit_books: int = 5,  # 新增：限制抓取的小说数量，默认5本
        top_n: int = 2,  # 默认只给前2本获取章节
        fetch_chapters: bool = True,  # 默认启用章节获取
        chapter_n: int = 2,  # 默认每本获取2章
        rank_key: str = "read_western_fantasy",
):
    print("=" * 80)
    print("Fanqie Spider + DB Schema Test (优化模式)")
    print("=" * 80)
    print(f"[config] 获取前{limit_books}本小说metadata，只给前{top_n}本获取{chapter_n}章内容")

    if not test_cases or "all" in test_cases:
        test_cases = ["rank_pipeline", "novel_detail", "chapters", "decryption"]

    project_root = _project_root()
    sys.path.insert(0, project_root)
    sys.path.insert(0, os.path.join(project_root, "spiders"))

    _ensure_clean_dirs(project_root)

    print("\n[1] init test database ...")
    # 尝试导入 DatabaseHandler，如果失败则使用模拟的
    try:
        from database.db_handler import DatabaseHandler
        db_path = os.path.join(project_root, "test_output", "fanqie_test.db")
        db = DatabaseHandler(db_path, is_test=True)
        print(f"[db] created: {db_path}")
    except ImportError:
        print("[warning] DatabaseHandler not found, using mock")

        # 创建模拟的数据库处理器
        class MockDatabaseHandler:
            def __init__(self, db_path, is_test=False):
                self.db_path = db_path
                self.is_test = is_test

            def get_table_counts(self):
                return {}

            def save_rank_snapshot(self, **kwargs):
                print(f"[mock] save_rank_snapshot called with {len(kwargs.get('items', []))} items")
                return 1

            def upsert_first_n_chapters(self, **kwargs):
                print(f"[mock] upsert_first_n_chapters called with {len(kwargs.get('chapters', []))} chapters")
                return True

            def get_chapters_count(self, novel_id):
                return 0

            def get_novel_chapters(self, novel_id, limit):
                return []

            def save_novel(self, novel_data, chapters):
                print(f"[mock] save_novel called for {novel_data.get('title')}")

        db_path = os.path.join(project_root, "test_output", "fanqie_test.db")
        db = MockDatabaseHandler(db_path, is_test=True)
        print(f"[mock db] created: {db_path}")

    _print_table_counts(db)

    print("\n[2] init spider ...")
    try:
        from spiders.fanqie_spider import FanqieSpider
    except ImportError as e:
        print(f"[error] Failed to import FanqieSpider: {e}")
        print("[info] Make sure you're in the correct directory and spiders module is accessible")
        return

    # 使用更通用的番茄小说配置
    fanqie_config: Dict[str, Any] = {
        "name": "番茄小说",
        "base_url": "https://fanqienovel.com",
        "request_delay": 2,
        "max_retries": 2,  # 测试时减少重试次数
        "pages_per_rank": int(pages),
        "chapter_extraction_goal": int(chapter_n),
        "rank_urls": {
            # 使用更常见的榜单URL - 根据测试输出，这个URL是有效的
            "read_western_fantasy": "https://fanqienovel.com/rank/1_2_1141",
        },
        "rank_type_map": {
            "read_western_fantasy": {"rank_family": "阅读榜", "rank_sub_cat": "西方奇幻"},
        },
        "selenium_specific": {
            "options": {
                "headless": True,
                "window_size": "1920,1080",
                "disable_gpu": True,
            },
            "stealth_mode": True,
            "timeout": 15,
            "implicit_wait": 5,
            "page_load_timeout": 20,
        },
    }

    try:
        spider = FanqieSpider(fanqie_config, db)
        # 检查driver是否初始化成功
        if spider.driver is None:
            print("[warning] Selenium driver failed to initialize, some tests may fail")
        print("[spider] ready")
    except Exception as e:
        print(f"[error] Failed to initialize FanqieSpider: {e}")
        return

    # shared state
    state: Dict[str, Any] = {}

    for tc in test_cases:
        print(f"\n=== Running test case: {tc} ===")
        if tc == "rank_pipeline":
            state["rank_pipeline"] = _test_rank_pipeline(
                spider, db,
                rank_key=rank_key,
                pages=pages,
                limit_books=limit_books,  # 传入limit_books参数
                top_n=top_n,
                fetch_chapters=fetch_chapters,  # 传入fetch_chapters参数
                chapter_n=chapter_n,
            )
            sample = _choose_sample_book(state["rank_pipeline"]["items"])
            if sample:
                state["sample_book"] = sample
                print(f"[info] Selected sample book: {sample.get('title', 'Unknown')}")

        elif tc == "novel_detail":
            sample = state.get("sample_book")
            if not sample:
                # 如果没有样本，使用排行榜中的第一个
                items = state.get("rank_pipeline", {}).get("items", [])
                if items:
                    sample = items[0]
                    print(f"[info] Using first book from rank: {sample.get('title', 'Unknown')}")

            if sample:
                state["novel_detail"] = _test_novel_detail(
                    spider,
                    novel_url=sample.get("url", ""),
                    novel_id=sample.get("platform_novel_id", ""),
                )
            else:
                print("[skip] novel_detail: no sample book available")

        elif tc == "chapters":
            if not fetch_chapters:
                print("[skip] chapters test requires --fetch_chapters")
                continue

            sample = state.get("sample_book")
            if not sample:
                # 如果没有样本，使用排行榜中的第一个
                items = state.get("rank_pipeline", {}).get("items", [])
                if items:
                    sample = items[0]
                    print(f"[info] Using first book from rank for chapters: {sample.get('title', 'Unknown')}")

            if sample:
                _test_chapters(
                    spider, db,
                    novel_url=sample.get("url", ""),
                    platform_novel_id=sample.get("platform_novel_id", ""),
                    n=chapter_n,
                )
            else:
                print("[skip] chapters: no sample book available")

        elif tc == "decryption":
            _test_decryption(spider)

        else:
            print(f"[warn] unknown test case: {tc}")

        if tc != test_cases[-1]:
            time.sleep(2)  # 避免请求过快

    print("\n" + "=" * 80)
    print("[final] db summary")
    print("=" * 80)
    _print_table_counts(db)

    # 尝试查看数据库内容，如果数据库文件存在
    db_path = os.path.join(project_root, "test_output", "fanqie_test.db")
    if os.path.exists(db_path):
        _peek_some_rows(db_path)
    else:
        print("[info] Database file not created")

    try:
        spider.close()
        print("[spider] closed")
    except Exception as e:
        print(f"[spider] close error: {e}")

    print(f"\n[done] test completed")


def run_quick_test():
    """快速测试，只测试基本功能"""
    run_comprehensive_fanqie_test(
        test_cases=["rank_pipeline", "novel_detail", "chapters", "decryption"],
        pages=1,
        limit_books=5,  # 快速测试限制5本
        top_n=2,
        fetch_chapters=True,  # 快速测试也包含章节
        chapter_n=2,
        rank_key="read_western_fantasy",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fanqie spider comprehensive test (DB-backed)")
    parser.add_argument(
        "--test",
        type=str,
        default="all",
        choices=["rank_pipeline", "novel_detail", "chapters", "decryption", "all", "quick"],
        help="Which test to run",
    )
    parser.add_argument("--pages", type=int, default=1, help="pages_per_rank")
    parser.add_argument("--limit_books", type=int, default=5, help="限制抓取的小说数量（默认5本）")
    parser.add_argument("--top_n", type=int, default=2, help="只给前top_n本小说获取章节")
    parser.add_argument("--fetch_chapters", action="store_true", default=True,
                        help="enable chapter fetch test (default: True)")
    parser.add_argument("--no_fetch_chapters", action="store_false", dest="fetch_chapters",
                        help="disable chapter fetch test")
    parser.add_argument("--chapter_n", type=int, default=2, help="number of chapters to fetch per novel")
    parser.add_argument("--rank_key", type=str, default="read_western_fantasy", help="rank key in rank_urls")

    args = parser.parse_args()

    if args.test == "quick":
        run_quick_test()
    else:
        tcs = None if args.test == "all" else [args.test]
        run_comprehensive_fanqie_test(
            test_cases=tcs,
            pages=args.pages,
            limit_books=args.limit_books,
            top_n=args.top_n,
            fetch_chapters=args.fetch_chapters,
            chapter_n=args.chapter_n,
            rank_key=args.rank_key,
        )