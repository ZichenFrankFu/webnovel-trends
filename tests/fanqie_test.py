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
import time
import sqlite3
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional
from functools import wraps
from typing import Callable, Any

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import WEBSITES

def _ensure_clean_dirs():
    """清理旧的测试产物并创建必要的输出目录"""
    print("\n" + "=" * 80)
    print("测试环境准备")
    print("=" * 80)

    # 1. 只清除 fanqie_test.db，保留其他数据库文件
    fanqie_test_path = "test_output/fanqie_test.db"
    if os.path.exists(fanqie_test_path):
        try:
            os.remove(fanqie_test_path)
            print(f"[清理] 已删除测试数据库: {fanqie_test_path}")
        except Exception as e:
            print(f"[清理] 删除 {fanqie_test_path} 失败: {e}")
    else:
        print(f"[清理] 测试数据库不存在: {fanqie_test_path}")

   # 2. 确保必要的目录存在
    os.makedirs("test_output", exist_ok=True)
    os.makedirs("test_output/debug", exist_ok=True)
    print(f"[目录] 确保目录存在: test_output/, test_output/debug/")

# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------

def _open_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def timeit(func: Callable) -> Callable:
    """计时装饰器，用于测量函数执行时间"""
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"[计时] {func.__name__}: {elapsed:.2f}秒")
        return result
    return wrapper

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
    """查看数据库中的一些记录"""
    if not os.path.exists(db_path):
        print("[信息] 数据库文件不存在")
        return

    try:
        conn = _open_sqlite(db_path)
        cur = conn.cursor()

        print("\n[数据库预览] 番茄小说相关记录")
        print("=" * 80)

        # 1. 最新的榜单列表
        print("\n1. 最新榜单列表 (fanqie):")
        # 首先检查表结构
        cur.execute("PRAGMA table_info(rank_lists)")
        columns = [row[1] for row in cur.fetchall()]
        print(f"[调试] rank_lists表结构: {columns}")

        cur.execute(
            """
            SELECT rank_list_id, platform, rank_family, rank_sub_cat, source_url
            FROM rank_lists
            ORDER BY rank_list_id DESC
            LIMIT 5
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("(暂无数据)")
        for r in rows:
            print(f"  - #{r['rank_list_id']} {r['platform']} / {r['rank_family']} / {r['rank_sub_cat']}")
            print(f"    来源URL: {r['source_url'][:80]}...")

        # 2. 最新的快照
        print("\n2. 最新榜单快照:")
        cur.execute("""
            SELECT rs.snapshot_id, rs.rank_list_id, rs.snapshot_date, rs.item_count, rl.rank_family
            FROM rank_snapshots rs
            JOIN rank_lists rl ON rl.rank_list_id = rs.rank_list_id
            WHERE rl.platform='fanqie'
            ORDER BY rs.snapshot_id DESC
            LIMIT 3
        """)
        for r in cur.fetchall():
            print(f"   快照ID: {r['snapshot_id']} | 榜单ID: {r['rank_list_id']}")
            print(f"   快照日期: {r['snapshot_date']} | 项目数: {r['item_count']}")
            print(f"   榜单类型: {r['rank_family']}")
            print()

        # 3. 最新的榜单条目
        print("\n3. 最新榜单条目 (前5名):")
        cur.execute("""
            WITH latest_snapshot AS (
                SELECT MAX(rs.snapshot_id) AS sid
                FROM rank_snapshots rs
                JOIN rank_lists rl ON rl.rank_list_id=rs.rank_list_id
                WHERE rl.platform='fanqie'
            )
            SELECT re.rank, n.platform_novel_id, COALESCE(nt.title, '') AS title, 
                   n.author, n.main_category, n.status, n.total_words
            FROM rank_entries re
            JOIN latest_snapshot ls ON ls.sid=re.snapshot_id
            JOIN novels n ON n.novel_uid=re.novel_uid
            LEFT JOIN novel_titles nt ON nt.novel_uid=n.novel_uid AND nt.is_primary=1
            ORDER BY re.rank ASC
            LIMIT 5
        """)
        rows = cur.fetchall()
        if not rows:
            print("   (无数据)")
        for r in rows:
            t = r["title"] or "(无标题)"
            print(f"   排名: #{r['rank']:3d} | 《{t[:20]}...》")
            print(f"   作者: {r['author']} | 分类: {r['main_category']}")
            print(f"   状态: {r['status']} | 字数: {r['total_words']}")
            print()

        # 4. 最新的章节
        print("\n4. 最新抓取的章节:")
        cur.execute("""
            SELECT fnc.novel_uid, n.platform_novel_id, fnc.chapter_num, 
                   fnc.chapter_title, fnc.word_count, fnc.publish_date
            FROM first_n_chapters fnc
            JOIN novels n ON n.novel_uid=fnc.novel_uid
            WHERE n.platform='fanqie'
            ORDER BY fnc.chapter_id DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        if not rows:
            print("   (无数据)")
        else:
            for r in rows:
                print(f"   小说ID: {r['platform_novel_id']}")
                print(f"   章节: #{r['chapter_num']} 《{r['chapter_title'][:20]}...》")
                print(f"   字数: {r['word_count']} | 发布日期: {r['publish_date']}")
                print()

        conn.close()

    except Exception as e:
        print(f"[错误] 数据库预览失败: {e}")
        import traceback
        traceback.print_exc()

def _choose_sample_book(books: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """选择第一本有可用URL/ID的书作为样本"""
    for b in books or []:
        if b.get("url") and b.get("platform_novel_id"):
            return b
    return books[0] if books else None

# ------------------------------------------------------------------
# Test Cases
# ------------------------------------------------------------------
@timeit
def _test_novel_detail(spider: Any, *, novel_url: str, novel_id: str):
    print("\n" + "=" * 80)
    print("[测试用例] 小说详情获取测试")
    print("=" * 80)
    try:
        print(f"[参数] 小说URL: {novel_url}")
        print(f"       小说ID: {novel_id}")

        start_time = time.time()
        detail = spider.fetch_novel_detail(novel_url, novel_id or "")
        elapsed_time = time.time() - start_time

        print(f"\n[结果] 获取成功 (耗时: {elapsed_time:.2f}秒)")
        print(f"  标题: 《{detail.get('title', '')}》")
        print(f"  作者: {detail.get('author', '')}")
        print(f"  分类: {detail.get('main_category', '')}")
        print(f"  状态: {detail.get('status', '')}")
        print(f"  字数: {detail.get('total_words', 0)}")
        print(f"  首传日期: {detail.get('first_upload_date', '')}")
        print(f"  在读人数: {detail.get('reading_count', 0)}")
        print(f"  标签: {detail.get('tags', [])}")

        return detail
    except Exception as e:
        print(f"[错误] fetch_novel_detail 失败: {e}")
        return {}

@timeit
def _test_chapters(spider: Any, db: Any, *, novel_url: str, platform_novel_id: str, n: int):
    print("\n" + "=" * 80)
    print(f"[测试用例] 章节获取测试 (n={n})")
    print("=" * 80)
    try:
        print(f"[参数] 小说URL: {novel_url}")
        print(f"       小说ID: {platform_novel_id}")
        print(f"       目标章节数: {n}")

        start_time = time.time()
        chapters = spider.fetch_first_n_chapters(novel_url, n=n)
        elapsed_time = time.time() - start_time

        print(f"\n[结果] 获取成功 (耗时: {elapsed_time:.2f}秒)")
        print(f"  获取章节数: {len(chapters)}")

        if chapters:
            # 打印章节详情
            print(f"\n[章节预览] 前{min(3, len(chapters))}章:")
            for i, ch in enumerate(chapters[:min(5, len(chapters))], 1):
                print(f"  {i}. 《{ch.get('chapter_title', '')[:30]}...》")
                print(f"     字数: {ch.get('word_count', 0)} | 发布日期: {ch.get('publish_date', '')}")
                preview = (ch.get("chapter_content") or "")[:100].replace("\n", " ")
                if preview:
                    print(f"     预览: {preview}...")
                print()

            # 保存到数据库
            if hasattr(db, "upsert_first_n_chapters"):
                print(f"[数据库] 保存 {len(chapters)} 个章节到数据库...")

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
                except Exception as e:
                    print(f"[警告] 获取小说详情失败: {e}")
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
                print(f"[数据库] 使用发布日期: {publish_date}")

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
                print(f"[数据库] upsert_first_n_chapters 完成，保存了 {len(chapters)} 个章节")
        else:
            print("[警告] 没有获取到章节")

        _print_table_counts(db)
        return chapters
    except Exception as e:
        print(f"[错误] fetch_first_n_chapters 失败: {e}")
        import traceback
        traceback.print_exc()
        return []

@timeit
def _test_decryption(spider: Any):
    print("\n" + "=" * 80)
    print("[测试用例] 字体解密功能测试")
    print("=" * 80)

    # 测试样本
    samples = [
        "这是一段普通文本",
        "<div>普通HTML文本</div>",
        "测试加密字符",  # 假设这是加密的
    ]

    for s in samples:
        try:
            if "<" in s:
                out = spider._decrypt_html(s)
                print(f"  HTML解密: {s} -> {out}")
            else:
                out = spider._decrypt_text(s)
                print(f"  文本解密: {s} -> {out}")
        except Exception as e:
            print(f"  解密 {s} 时出错: {e}")

# ------------------------------------------------------------------
# Test Modes
# ------------------------------------------------------------------

def _test_rank_pipeline_fast(spider: Any, db: Any, *, rank_key: str, pages: int, limit_books: int, top_n: int,
                             chapter_n: int):
    """
    优化的测试流程：
    1. 获取排行榜前limit_books本小说的metadata
    2. 只为前top_n本小说获取前chapter_n章内容

    优化特性:
    - 智能章节补全: 如果数据库中已有章节，只抓取缺失的部分
    - 小说去重: 通过作者和简介判断是否为同一本小说

    Args:
        limit_books: 限制获取的小说数量（从榜单中取前limit_books本）
        top_n: 为前top_n本小说获取章节内容
        chapter_n: 每本小说获取的章节数
    """
    print("\n" + "=" * 80)
    print(f"[测试用例] 榜单流程快速测试")
    print("=" * 80)
    print(f"[参数配置]")
    print(f"  榜单类型: {rank_key}")
    print(f"  页数: {pages}")
    print(f"  限制小说数: {limit_books}")
    print(f"  获取章节的小说数: {top_n}")
    print(f"  每本章节数: {chapter_n}")
    print("-" * 80)

    # 记录各部分时间
    timings: Dict[str, Any] = {
        "fetch_rank": 0.0,
        "chapters": [],
        "total": 0.0
    }

    start_total_time = time.time()

    # 通过配置控制页数
    spider.site_config["pages_per_rank"] = int(pages)

    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    source_url = (spider.site_config.get("rank_urls") or {}).get(rank_key, "")

    print(f"[步骤1] 获取榜单页面: {source_url}")
    print(f"        配置: pages_per_rank={pages}")

    try:
        # 第一步：获取排行榜前limit_books本小说并丰富metadata（不获取章节）
        print(f"\n[步骤2] 获取排行榜前{limit_books}本小说并丰富metadata...")

        start_fetch_time = time.time()
        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=pages,
            enrich_detail=True,
            enrich_chapters=False,  # 不获取章节
            chapter_count=0,
            snapshot_date=snapshot_date,
            max_books=limit_books,  # 限制处理的小说数量
        )
        fetch_time = time.time() - start_fetch_time
        timings["fetch_rank"] = fetch_time

        items = result.get("items") or []
        snapshot_id = result.get("snapshot_id")

        print(f"[结果] 获取完成")
        print(f"  耗时: {fetch_time:.2f}秒")
        print(f"  获取小说数: {len(items)}")
        print(f"  快照ID: {snapshot_id}")

        if not items:
            print("[警告] 没有获取到任何小说数据")
            total_time = time.time() - start_total_time
            timings["total"] = total_time
            return {"items": [], "snapshot_id": None, "timings": timings}

        # 第二步：只为前top_n本小说获取章节
        if items and top_n > 0 and chapter_n > 0:
            # 确保top_n不超过limit_books
            actual_top_n = min(top_n, len(items))
            print(f"\n[步骤3] 为前{actual_top_n}本小说获取前{chapter_n}章内容...")
            print(f"[章节获取策略] 智能补全：如果数据库中已有章节，只获取缺失的部分")

            for i, book in enumerate(items[:actual_top_n], 1):
                title = book.get('title', '未知')
                novel_id = book.get('platform_novel_id', '')
                novel_url = book.get('url', '')

                if not novel_url or not novel_id:
                    print(f"  [{i:2d}] 跳过: 缺少URL或ID")
                    continue

                print(f"\n  [{i:2d}] 《{title[:30]}》")
                print(f"       小说ID: {novel_id}")
                print(f"       目标章节数: {chapter_n}")

                try:
                    # 先检查数据库中已有的章节数
                    existing_chapters = 0
                    if hasattr(db, "get_chapters_count"):
                        existing_chapters = db.get_chapters_count(novel_id)
                        print(f"       数据库已有章节: {existing_chapters}")

                    # 计算需要获取的新章节数
                    need_chapters = max(0, chapter_n - existing_chapters)

                    if need_chapters == 0:
                        print(f"       智能跳过: 已有{existing_chapters}章 >= 目标{chapter_n}章")
                        continue

                    print(f"       需要获取新章节: {need_chapters}章")

                    start_chapter_time = time.time()
                    chapters = spider.fetch_first_n_chapters(novel_url, n=chapter_n)
                    chapter_elapsed = time.time() - start_chapter_time
                    timings["chapters"].append(chapter_elapsed)

                    if chapters:
                        print(f"       成功获取: {len(chapters)}章")
                        print(f"       耗时: {chapter_elapsed:.2f}秒")

                        # 保存章节到数据库
                        if hasattr(db, "upsert_first_n_chapters"):
                            # 获取小说详情用于保存
                            detail = spider.fetch_novel_detail(novel_url, novel_id)

                            # 智能去重：通过作者和简介查找已存在的小说
                            existing_novel = None
                            if hasattr(db, "find_novel_by_author_and_intro"):
                                existing_novel = db.find_novel_by_author_and_intro(
                                    author=detail.get('author', ''),
                                    intro=detail.get('intro', '')[:200]
                                )

                            if existing_novel:
                                print(f"       发现已存在小说: {existing_novel.get('title', '')}")
                                print(f"       使用已有小说ID: {existing_novel.get('platform_novel_id')}")
                                novel_id = existing_novel.get('platform_novel_id', novel_id)

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
                            print(f"       已保存到数据库")
                        else:
                            print(f"       [调试] 数据库不支持upsert_first_n_chapters")
                    else:
                        print(f"       获取章节失败或没有章节")

                    # 在获取章节之间添加延迟
                    if i < actual_top_n:
                        print(f"       等待3秒...")
                        time.sleep(3)

                except Exception as e:
                    print(f"       获取章节时出错: {e}")
                    continue

        # 预览结果
        if items:
            print(f"\n[结果预览] 前{min(3, len(items))}本小说metadata:")
            for i, b in enumerate(items[:3], 1):
                print(f"\n  {i}. 《{(b.get('title') or '')[:30]}》")
                print(f"     作者: {b.get('author', '')}")
                print(f"     分类: {b.get('main_category', '')}")
                print(f"     状态: {b.get('status', '')}")
                print(f"     排名: {b.get('rank')}")
                print(f"     在读: {b.get('reading_count')}")
                print(f"     字数: {b.get('total_words')}")
                tgs = b.get("tags") or []
                if tgs:
                    print(f"     标签: {', '.join(tgs[:4])}{'...' if len(tgs) > 4 else ''}")
                print(f"     URL: {b.get('url', '')[:80]}")

        _print_table_counts(db)

        total_time = time.time() - start_total_time
        timings["total"] = total_time

        # 打印详细计时
        print(f"\n[详细计时] 榜单流程测试:")
        print(f"  - 榜单获取与丰富: {timings['fetch_rank']:.2f}秒")
        if timings["chapters"]:
            total_chapter_time = sum(timings["chapters"])
            avg_chapter_time = total_chapter_time / len(timings["chapters"])
            print(f"  - 章节获取:")
            print(f"     总耗时: {total_chapter_time:.2f}秒")
            print(f"     平均每本: {avg_chapter_time:.2f}秒")
            print(f"     每本耗时: {', '.join([f'{t:.2f}' for t in timings['chapters']])}秒")
        print(f"  - 总计: {total_time:.2f}秒")

        return {"items": items, "snapshot_id": snapshot_id, "timings": timings}

    except Exception as e:
        print(f"[错误] 榜单流程快速测试失败: {e}")
        import traceback
        traceback.print_exc()

        total_time = time.time() - start_total_time
        timings["total"] = total_time
        print(f"[计时] 测试失败，总耗时: {total_time:.2f}秒")

        return {"items": [], "snapshot_id": None, "timings": timings}


def _test_rank_pipeline(spider: Any, db: Any, *, rank_key: str, pages: int, limit_books: int, top_n: int,
                        fetch_chapters: bool,
                        chapter_n: int):
    """
    主要集成测试:
    - fetch_and_save_rank (首选)
    - 可选手动: fetch_rank_list -> enrich_books_with_details -> db.save_rank_snapshot
    - 可选获取章节并upsert (如果db支持upsert_first_n_chapters)
    """
    print("\n" + "=" * 80)
    print(f"[测试用例] 榜单完整流程测试")
    print("=" * 80)
    print(f"[参数配置]")
    print(f"  榜单类型: {rank_key}")
    print(f"  页数: {pages}")
    print(f"  限制小说数: {limit_books}")
    print(f"  获取章节的小说数: {top_n}")
    print(f"  是否获取章节: {fetch_chapters}")
    print(f"  每本章节数: {chapter_n}")
    print("-" * 80)

    # 使用快速模式
    result = _test_rank_pipeline_fast(spider, db, rank_key=rank_key, pages=pages, limit_books=limit_books, top_n=top_n,
                                      chapter_n=chapter_n)

    # 添加一个总计时输出
    if "timings" in result:
        timings = result["timings"]
        print(f"\n[测试小结] 榜单 '{rank_key}' 测试完成:")
        print(f"  获取小说数: {len(result.get('items', []))}")
        print(f"  总耗时: {timings.get('total', 0):.2f}秒")

    return result

def run_comprehensive_fanqie_test(
        *,
        test_cases: Optional[List[str]] = None,
        pages: int = 1,
        limit_books: int = 5,
        top_n: int = 2,
        fetch_chapters: bool = True,
        chapter_n: int = 2,
        rank_key: str = "read_western_fantasy",
        rank_keys: Optional[List[str]] = None,
):
    print("=" * 80)
    print("番茄小说爬虫 + 数据库架构测试 (优化模式)")
    print("=" * 80)
    print(f"[测试配置]")
    print(f"  测试用例: {test_cases or ['all']}")
    print(f"  榜单页数: {pages}")
    print(f"  限制小说数量: {limit_books}")
    print(f"  获取章节的小说数: {top_n}")
    print(f"  是否获取章节: {fetch_chapters}")
    print(f"  每本小说章节数: {chapter_n}")
    print(f"  榜单类型: {rank_key}")
    if rank_keys:
        print(f"  多个榜单测试: {rank_keys}")
    print("-" * 80)
    print(f"[优化特性]")
    print(f"  智能章节补全: 如果数据库中已有章节，只获取缺失的部分")
    print(f"  小说去重: 通过作者和简介判断是否为同一本小说")
    print(f"  资源优化: 限制抓取数量，避免不必要的请求")
    print("=" * 80)

    if not test_cases or "all" in test_cases:
        test_cases = ["rank_pipeline", "novel_detail", "chapters", "decryption"]

    sys.path.insert(0, project_root)
    sys.path.insert(0, os.path.join(project_root, "spiders"))

    _ensure_clean_dirs()

    print("\n[1] 初始化测试数据库...")
    try:
        from database.db_handler import DatabaseHandler
        db_path = os.path.join(project_root, "test_output", "fanqie_test.db")
        db = DatabaseHandler(db_path, is_test=True)
        print(f"[数据库] 已创建: {db_path}")

        # 测试数据库连接
        counts = db.get_table_counts()
        print(f"[数据库] 连接成功，表数量: {len(counts)}")

    except ImportError as e:
        print(f"[错误] 无法导入 DatabaseHandler: {e}")
        print("[提示] 请确保 database 模块在 Python 路径中")
        print("[提示] 可以在项目根目录运行: pip install -e .")
        return
    except Exception as e:
        print(f"[错误] 数据库初始化失败: {e}")
        print("[提示] 检查数据库配置和依赖")
        import traceback
        traceback.print_exc()
        return

    _print_table_counts(db)

    print("\n[2] 初始化爬虫...")
    try:
        from spiders.fanqie_spider import FanqieSpider
    except ImportError as e:
        print(f"[错误] 无法导入 FanqieSpider: {e}")
        print("[提示] 确保在正确的目录下，并且 spiders 模块可访问")
        return

    # 番茄小说配置（从global config引入）
    fanqie_config = WEBSITES.get("fanqie")

    try:
        spider = FanqieSpider(fanqie_config, db)
        # 检查driver是否初始化成功
        if spider.driver is None:
            print("[警告] Selenium driver 初始化失败，某些测试可能会失败")
        else:
            print("[爬虫] 初始化成功")
    except Exception as e:
        print(f"[错误] FanqieSpider 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return

    # 共享状态
    state: Dict[str, Any] = {}

    # 如果有多个榜单需要测试
    test_rank_keys = rank_keys or [rank_key]

    for rank_key_to_test in test_rank_keys:
        print(f"\n{'#' * 80}")
        print(f"开始测试榜单: {rank_key_to_test}")
        print(f"{'#' * 80}")

        for tc in test_cases:
            print(f"\n=== 运行测试用例: {tc} ===")

            if tc == "rank_pipeline":
                state["rank_pipeline"] = _test_rank_pipeline(
                    spider, db,
                    rank_key=rank_key_to_test,
                    pages=pages,
                    limit_books=limit_books,
                    top_n=top_n,
                    fetch_chapters=fetch_chapters,
                    chapter_n=chapter_n,
                )
                sample = _choose_sample_book(state["rank_pipeline"]["items"])
                if sample:
                    state["sample_book"] = sample
                    print(f"[信息] 选择样本小说: {sample.get('title', '未知')}")

            elif tc == "decryption":
                _test_decryption(spider)

            elif tc == "novel_detail":
                sample = state.get("sample_book")
                if not sample:
                    # 如果没有样本，使用排行榜中的第一个
                    items = state.get("rank_pipeline", {}).get("items", [])
                    if items:
                        sample = items[0]
                        print(f"[信息] 使用排行榜第一本小说: {sample.get('title', '未知')}")

                if sample:
                    state["novel_detail"] = _test_novel_detail(
                        spider,
                        novel_url=sample.get("url", ""),
                        novel_id=sample.get("platform_novel_id", ""),
                    )
                else:
                    print("[跳过] novel_detail: 没有可用的样本小说")

            elif tc == "chapters":
                if not fetch_chapters:
                    print("[跳过] chapters: 需要 --fetch_chapters 参数")
                    continue

                sample = state.get("sample_book")
                if not sample:
                    # 如果没有样本，使用排行榜中的第一个
                    items = state.get("rank_pipeline", {}).get("items", [])
                    if items:
                        sample = items[0]
                        print(f"[信息] 使用排行榜第一本小说作为章节测试样本: {sample.get('title', '未知')}")

                if sample:
                    _test_chapters(
                        spider, db,
                        novel_url=sample.get("url", ""),
                        platform_novel_id=sample.get("platform_novel_id", ""),
                        n=chapter_n,
                    )
                else:
                    print("[跳过] chapters: 没有可用的样本小说")

            else:
                print(f"[警告] 未知测试用例: {tc}")

            if tc != test_cases[-1]:
                time.sleep(2)  # 避免请求过快

        # 如果不是最后一个榜单，等待一下再测试下一个
        if rank_key_to_test != test_rank_keys[-1]:
            print(f"\n等待3秒后测试下一个榜单...")
            time.sleep(3)

    print("\n" + "=" * 80)
    print("[最终结果] 数据库总结")
    print("=" * 80)
    _print_table_counts(db)

    # 查看数据库内容
    db_path = os.path.join(project_root, "test_output", "fanqie_test.db")
    if os.path.exists(db_path):
        _peek_some_rows(db_path)
    else:
        print("[信息] 数据库文件未创建")

    try:
        spider.close()
        print("[爬虫] 已关闭")
    except Exception as e:
        print(f"[爬虫] 关闭错误: {e}")

    print(f"\n[完成] 测试完成")


def run_quick_test(rank_keys=None):
    """快速测试，只测试基本功能"""
    print("=" * 80)
    print("番茄小说爬虫快速测试")
    print("=" * 80)
    print("[说明] 快速测试将使用默认参数测试基本功能")
    print("       耗时较短，适合快速验证爬虫是否正常工作")
    print("-" * 80)

    # 处理rank_keys参数
    if rank_keys:
        print(f"[配置] 测试多个榜单: {rank_keys}")
    else:
        rank_keys = ["read_western_fantasy"]

    run_comprehensive_fanqie_test(
        test_cases=["rank_pipeline", "novel_detail", "chapters", "decryption"],
        pages=1,
        limit_books=3,
        top_n=1,
        fetch_chapters=True,
        chapter_n=1,
        rank_key=rank_keys[0],  # 使用第一个榜单作为默认
        rank_keys=rank_keys,  # 传入所有榜单
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="番茄小说爬虫综合测试")
    parser.add_argument(
        "--test",
        type=str,
        default="all",
        choices=["rank_pipeline", "novel_detail", "chapters", "decryption", "all", "quick"],
        help="要运行的测试",
    )
    parser.add_argument("--pages", type=int, default=1, help="每榜单页数")
    parser.add_argument("--limit_books", type=int, default=5, help="限制抓取的小说数量（默认5本）")
    parser.add_argument("--top_n", type=int, default=2, help="只给前top_n本小说获取章节")
    parser.add_argument("--fetch_chapters", action="store_true", default=True,
                        help="启用章节获取测试（默认：启用）")
    parser.add_argument("--no_fetch_chapters", action="store_false", dest="fetch_chapters",
                        help="禁用章节获取测试")
    parser.add_argument("--chapter_n", type=int, default=2, help="每本小说获取的章节数（默认：2）")
    parser.add_argument("--rank_key", type=str, default="read_western_fantasy", help="rank_urls中的榜单键")
    parser.add_argument("--rank_keys", type=str, default="read_western_fantasy",
                        help="多个榜单键，用逗号分隔（默认：阅读榜·西方奇幻）")
    parser.add_argument("--verbose", action="store_true", help="详细输出模式")

    args = parser.parse_args()

    # 处理多个榜单
    rank_keys = []
    if args.rank_keys:
        rank_keys = [k.strip() for k in args.rank_keys.split(",") if k.strip()]

    if args.test == "quick":
        run_quick_test(rank_keys)  # 传入rank_keys参数
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
            rank_keys=rank_keys,
        )