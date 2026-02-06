# tests/qidian_test.py
import os
import sys
import time
import sqlite3
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any
from functools import wraps
from typing import Callable, Any
# 添加项目路径
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config import WEBSITES


def _ensure_clean_dirs():
    """清理旧的测试产物并创建必要的输出目录"""
    print("\n" + "=" * 80)
    print("测试环境准备")
    print("=" * 80)

    # 1. 只清除 qidian_test.db，保留其他数据库文件
    qidian_db_path = "test_output/qidian_test.db"
    if os.path.exists(qidian_db_path):
        try:
            os.remove(qidian_db_path)
            print(f"[清理] 已删除测试数据库: {qidian_db_path}")
        except Exception as e:
            print(f"[清理] 删除 {qidian_db_path} 失败: {e}")
    else:
        print(f"[清理] 测试数据库不存在: {qidian_db_path}")

   # 2. 确保必要的目录存在
    os.makedirs("test_output", exist_ok=True)
    os.makedirs("test_output/debug", exist_ok=True)
    print(f"[目录] 确保目录存在: test_output/, test_output/debug/")

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

def _open_sqlite(db_path: str) -> sqlite3.Connection:
    """Open sqlite connection for verification queries."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def _print_table_counts(db):
    """Print all table counts using db.get_table_counts()."""
    print("\n" + "-" * 60)
    print("数据库统计")
    print("-" * 60)

    try:
        counts = db.get_table_counts()
        table_mapping = {
            "novels": "小说基本信息",
            "novel_titles": "小说标题记录",
            "tags": "标签信息",
            "novel_tag_map": "小说标签映射",
            "rank_lists": "榜单列表",
            "rank_snapshots": "榜单快照",
            "rank_entries": "榜单条目",
            "first_n_chapters": "前N章内容",
        }

        for k, display_name in table_mapping.items():
            count = counts.get(k, 0)
            status = "EMPTY" if count > 0 else ""
            print(f"  {status} {display_name:<15}: {count:>4} 条记录")

        total = sum(counts.get(k, 0) for k in table_mapping.keys())
        print(f"\n 总计: {total} 条记录")

    except Exception as e:
        print(f"获取数据库统计失败: {e}")

def _peek_some_rows(db_path: str):
    """Show a few rows from key tables to verify inserts."""
    print("\n" + "-" * 80)
    print("数据库内容预览")
    print("-" * 80)

    conn = _open_sqlite(db_path)
    cur = conn.cursor()

    print("\n榜单列表 (rank_lists):")
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

    print("\n榜单快照 (rank_snapshots):")
    cur.execute(
        """
        SELECT snapshot_id, rank_list_id, snapshot_date, item_count
        FROM rank_snapshots
        ORDER BY snapshot_id DESC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(暂无数据)")
    for r in rows:
        print(f"  - 快照#{r['snapshot_id']} 榜单#{r['rank_list_id']} {r['snapshot_date']} 书籍数={r['item_count']}")

    print("\n 榜单条目 (最新快照的前5名):")
    cur.execute(
        """
        SELECT re.rank, n.platform, n.platform_novel_id, nt.title, n.author, n.main_category, re.total_recommend
        FROM rank_entries re
        JOIN rank_snapshots rs ON rs.snapshot_id = re.snapshot_id
        JOIN novels n ON n.novel_uid = re.novel_uid
        LEFT JOIN novel_titles nt ON nt.novel_uid = n.novel_uid AND nt.is_primary=1
        WHERE rs.snapshot_id = (SELECT MAX(snapshot_id) FROM rank_snapshots)
        ORDER BY re.rank ASC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(暂无数据)")
    for r in rows:
        title = r["title"] or "(无主标题)"
        print(f"  - 第{r['rank']:2}名: 《{title[:20]}...》")
        print(f"     作者: {r['author']} | 分类: {r['main_category']} | 总推荐: {r['total_recommend'] or 'N/A'}")

    print("\n小说基本信息 (最新5本):")
    cur.execute(
        """
        SELECT novel_uid, platform, platform_novel_id, author, main_category, status, total_words
        FROM novels
        ORDER BY novel_uid DESC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(暂无数据)")
    for r in rows:
        words = f"{r['total_words']:,}" if r['total_words'] else "未知"
        print(f"  - #{r['novel_uid']} {r['platform']}:{r['platform_novel_id']}")
        print(f"     作者: {r['author']} | 分类: {r['main_category']} | 状态: {r['status']}")
        print(f"     字数: {words}")

    print("\n 标签信息 (最新10个):")
    cur.execute(
        """
        SELECT tag_id, tag_name
        FROM tags
        ORDER BY tag_id DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(暂无数据)")
    for r in rows:
        print(f"  - #{r['tag_id']}: {r['tag_name']}")

    print("\n章节内容 (最新5章):")
    cur.execute(
        """
        SELECT fc.novel_uid, fc.chapter_num, fc.chapter_title, fc.word_count, fc.publish_date
        FROM first_n_chapters fc
        JOIN novels n ON n.novel_uid = fc.novel_uid
        WHERE n.platform = 'qidian'
        ORDER BY fc.chapter_id DESC
        LIMIT 5
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("(暂无数据)")
    for r in rows:
        title = r["chapter_title"][:30] + "..." if len(r["chapter_title"]) > 30 else r["chapter_title"]
        print(f"  - 小说#{r['novel_uid']} 第{r['chapter_num']}章: {title}")
        print(f"     字数: {r['word_count']} | 发布时间: {r['publish_date']}")

    conn.close()
    print("\n" + "-" * 80)

"""测试抓取榜单并保存到数据库"""
def _test_fetch_rank_list(spider, db, rank_key="hotsales", snapshot_date=None, pages=1, top_n=None):
    print(f"\n{'=' * 80}")
    print(f"测试: 抓取榜单数据")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 榜单类型: {rank_key}")
    print(f"  - 抓取页数: {pages}")
    print(f"  - 快照日期: {snapshot_date or '今天'}")
    print(f"  - 限制数量: {top_n or '无限制'}")
    print(f"{'=' * 80}")

    try:
        # 临时修改页数配置
        original_pages = spider.site_config.get("pages_per_rank", 5)
        spider.site_config["pages_per_rank"] = pages

        print(f"开始抓取 {rank_key} 榜 (共{pages}页)...")
        books = spider.fetch_rank_list(rank_type=rank_key)

        # 恢复原始配置
        spider.site_config["pages_per_rank"] = original_pages

        print(f"成功抓取了 {len(books)} 本书籍")

        # 限制数量
        if top_n and top_n > 0:
            books = books[:top_n]
            print(f"限制为前 {top_n} 本书籍")

        if books:
            print(f"\n前3本书籍信息:")
            for i, book in enumerate(books[:3], 1):
                print(f"\n{i}. 《{book.get('title', '无标题')}》")
                print(f"   作者: {book.get('author', '未知')}")
                print(f"   分类: {book.get('main_category', '未知')}")
                print(f"   标签: {', '.join(book.get('tags', []))}")
                print(f"   排名: {book.get('rank', 'N/A')}")
                print(f"   状态: {book.get('status', '未知')}")
                print(f"   字数: {book.get('total_words', 0)}")
                print(f"   总推荐: {book.get('total_recommend', 'N/A')}")
                print(f"   URL: {book.get('url', 'N/A')[:80]}...")

        # 保存到数据库
        if db and books:
            snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
            source_url = spider.site_config.get("rank_urls", {}).get(rank_key, "")

            # 获取榜单身份信息
            rank_identity = spider.rank_type_map.get(rank_key)
            if rank_identity:
                rank_family = rank_identity.rank_family
                rank_sub_cat = rank_identity.rank_sub_cat
            else:
                rank_family = "未知榜单"
                rank_sub_cat = ""

            print(f"\n保存到数据库...")
            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family=rank_family,
                rank_sub_cat=rank_sub_cat,
                snapshot_date=snapshot_date,
                items=books,
                source_url=source_url,
                make_title_primary=True,
            )
            print(f"保存了 {inserted} 条榜单记录到数据库")
            _print_table_counts(db)

        return books
    except Exception as e:
        print(f"抓取榜单失败: {e}")
        import traceback
        traceback.print_exc()
        return []

"""测试抓取小说详情并保存到数据库"""
def _test_fetch_novel_detail(spider, db, novel_url=None, novel_id=None):
    print(f"\n{'=' * 80}")
    print("测试: 抓取小说详情")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 小说URL: {novel_url or 'N/A'}")
    print(f"  - 小说ID: {novel_id or 'N/A'}")
    print(f"{'=' * 80}")

    if not novel_url and not novel_id:
        print("没有提供小说URL或ID，跳过测试")
        return None

    try:
        print(f"开始抓取小说详情...")
        detail = spider.fetch_novel_detail(novel_url or "", novel_id or "")
        print(f"成功抓取了小说详情")

        print(f"\n小说详情信息:")
        print(f"   标题: 《{detail.get('title', '无标题')}》")
        print(f"   作者: {detail.get('author', '未知')}")
        print(f"   分类: {detail.get('main_category', '未知')}")
        print(f"   标签: {', '.join(detail.get('tags', []))}")
        print(f"   状态: {detail.get('status', '未知')}")
        print(f"   字数: {detail.get('total_words', 0)}")
        print(f"   总推荐: {detail.get('total_recommend', 'N/A')}")
        print(f"   上架时间: {detail.get('first_upload_date', '未知')}")
        print(f"   简介: {detail.get('intro', '无简介')[:200]}...")

        # 保存到数据库（通过创建一个虚拟榜单的方式）
        if db and detail.get('platform_novel_id'):
            print(f"\n通过测试榜单保存小说详情到数据库...")
            # 创建一个单本书籍的榜单快照来保存小说信息
            book_item = {
                "novel_id": detail.get('platform_novel_id'),
                "title": detail.get('title', ''),
                "author": detail.get('author', ''),
                "platform": "qidian",
                "url": detail.get('url', ''),
                "introduction": detail.get('intro', ''),
                "main_category": detail.get('main_category', ''),
                "tags": detail.get('tags', []),
                "status": detail.get('status', ''),
                "total_words": detail.get('total_words', 0),
                "rank": 1,
                "total_recommend": detail.get('total_recommend'),
                "extra": {
                    "source": "novel_detail_test",
                    "first_upload_date": detail.get('first_upload_date', '')
                }
            }

            snapshot_date = datetime.now().strftime("%Y-%m-%d")
            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family="单本测试",
                rank_sub_cat="详情页测试",
                snapshot_date=snapshot_date,
                items=[book_item],
                source_url=detail.get('url', ''),
                make_title_primary=True,
            )
            print(f"通过测试榜单保存了小说详情")

        return detail
    except Exception as e:
        print(f"抓取详情失败: {e}")
        import traceback
        traceback.print_exc()
        return None

"""测试抓取前N章内容并保存到数据库"""
def _test_fetch_first_n_chapters(spider, db, novel_url=None, novel_id=None, chapter_n=3):
    print(f"\n{'=' * 80}")
    print(f"测试: 抓取前{chapter_n}章内容")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 小说URL: {novel_url or 'N/A'}")
    print(f"  - 小说ID: {novel_id or 'N/A'}")
    print(f"  - 章节数: {chapter_n}")
    print(f"{'=' * 80}")

    if not novel_url:
        print("没有提供小说URL，跳过测试")
        return []

    try:
        print(f"开始抓取前{chapter_n}章内容...")
        chapters = spider.fetch_first_n_chapters(novel_url, n=chapter_n)
        print(f"成功抓取了 {len(chapters)} 章内容")

        if chapters:
            print(f"\n📖 前{min(3, len(chapters))}章信息:")
            for i, chapter in enumerate(chapters[:3], 1):
                print(f"\n{i}. {chapter.get('chapter_title', '无标题')}")
                print(f"   字数: {chapter.get('word_count', 0)}")
                print(f"   发布日期: {chapter.get('publish_date', '未知')}")
                content_preview = chapter.get('chapter_content', '')
                if content_preview:
                    print(f"   内容预览: {content_preview[:100]}...")

        # 保存章节到数据库
        if db and chapters:
            # 需要小说ID
            if not novel_id:
                # 从URL提取小说ID
                import re
                match = re.search(r'/book/(\d+)/', novel_url)
                if match:
                    novel_id = match.group(1)

            if novel_id:
                # 获取小说基本信息
                print(f"\n获取小说基本信息并保存章节...")
                detail = spider.fetch_novel_detail(novel_url, novel_id)

                inserted = db.upsert_first_n_chapters(
                    platform="qidian",
                    platform_novel_id=novel_id,
                    publish_date=chapters[0].get('publish_date') if chapters else datetime.now().strftime("%Y-%m-%d"),
                    chapters=chapters,
                    novel_fallback_fields={
                        "title": detail.get('title', '未知'),
                        "author": detail.get('author', '未知'),
                        "intro": detail.get('intro', ''),
                        "main_category": detail.get('main_category', '未知'),
                        "status": detail.get('status', 'ongoing'),
                        "total_words": detail.get('total_words', 0),
                        "url": novel_url,
                        "tags": detail.get('tags', []),
                    },
                )
                print(f"保存了 {inserted} 章内容到数据库")

        return chapters
    except Exception as e:
        print(f"抓取章节失败: {e}")
        import traceback
        traceback.print_exc()
        return []

"""测试补全榜单数据并保存到数据库"""
def _test_enrich_rank_items(spider, db, books, max_books=3, snapshot_date=None,
                            fetch_detail=True, fetch_chapters=False, chapter_n=3):
    print(f"\n{'=' * 80}")
    print(f"测试: 补全榜单数据")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 书籍数量: {len(books)} 本")
    print(f"  - 最大处理: {max_books} 本")
    print(f"  - 获取详情: {'是' if fetch_detail else '否'}")
    print(f"  - 获取章节: {'是' if fetch_chapters else '否'} (章节数: {chapter_n})")
    print(f"  - 章节智能补全: {'是' if fetch_chapters else '不适用'}")
    print(f"{'=' * 80}")

    if not books:
        print("没有书籍数据，跳过测试")
        return []

    try:
        print(f"开始补全 {min(max_books, len(books))} 本书籍的数据...")
        enriched_books = spider.enrich_rank_items(
            books,
            max_books=max_books,
            fetch_detail=fetch_detail,
            fetch_chapters=fetch_chapters,
            chapter_count=chapter_n,
        )
        print(f"成功补全了 {len(enriched_books)} 本书籍的数据")

        if enriched_books:
            print(f"\n补全后的数据对比:")
            for i, book in enumerate(enriched_books[:2], 1):
                original = books[i - 1] if i - 1 < len(books) else {}
                print(f"\n{i}. 《{book.get('title', '无标题')}》")
                print(f"   分类变化: {original.get('main_category', '未知')} -> {book.get('main_category', '未知')}")
                print(f"   状态: {book.get('status', '未知')}")
                print(f"   字数: {book.get('total_words', 0)}")
                print(f"   总推荐: {book.get('total_recommend', 'N/A')}")
                print(f"   上架时间: {book.get('first_upload_date', '未知')}")
                if fetch_chapters:
                    chapters = book.get('first_n_chapters', [])
                    print(f"   章节数: {len(chapters)}")

        # 保存补全后的数据到数据库
        if db and enriched_books:
            snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
            source_url = spider.site_config.get("rank_urls", {}).get("hotsales", "")

            print(f"\n保存补全后的数据到数据库...")
            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family="补全测试榜",
                rank_sub_cat="",
                snapshot_date=snapshot_date,
                items=enriched_books,
                source_url=source_url,
                make_title_primary=True,
            )
            print(f"保存了 {inserted} 条补全后的榜单记录到数据库")

        return enriched_books
    except Exception as e:
        print(f"补全数据失败: {e}")
        import traceback
        traceback.print_exc()
        return books[:max_books]

"""测试完整流程：抓取榜单 -> 补全数据 -> 保存到数据库"""
def _test_fetch_and_save_rank(spider, db, rank_key="hotsales", pages=1, top_n=3,
                              fetch_detail=True, fetch_chapters=False, chapter_n=3):
    print(f"\n{'=' * 80}")
    print(f"测试: 完整流程")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 榜单类型: {rank_key}")
    print(f"  - 抓取页数: {pages}")
    print(f"  - 处理数量: {top_n}")
    print(f"  - 获取详情: {'是' if fetch_detail else '否'}")
    print(f"  - 获取章节: {'是' if fetch_chapters else '否'} (章节数: {chapter_n})")
    print(f"  - 章节智能补全: {'是' if fetch_chapters else '不适用'}")
    print(f"{'=' * 80}")

    try:
        print(f"开始完整流程测试...")
        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=pages,
            enrich_detail=fetch_detail,
            enrich_chapters=fetch_chapters,
            chapter_count=chapter_n,
            snapshot_date=datetime.now().strftime("%Y-%m-%d"),
            max_books=top_n,
        )

        print(f"\n完整流程完成")
        print(f"   榜单类型: {result.get('rank_type')}")
        print(f"   榜单大类: {result.get('rank_family')}")
        print(f"   子分类: {result.get('rank_sub_cat')}")
        print(f"   快照ID: {result.get('snapshot_id')}")
        print(f"   书籍数量: {len(result.get('items', []))}")

        if result.get('items'):
            print(f"\n保存的书籍数据:")
            for i, book in enumerate(result.get('items', [])[:2], 1):
                print(f"\n{i}. 《{book.get('title', '无标题')}》")
                print(f"   分类: {book.get('main_category', '未知')}")
                print(f"   状态: {book.get('status', '未知')}")
                print(f"   字数: {book.get('total_words', 0)}")
                print(f"   总推荐: {book.get('total_recommend', 'N/A')}")
                if fetch_chapters:
                    chapters = book.get('first_n_chapters', [])
                    print(f"   章节数: {len(chapters)}")
                    if chapters:
                        existing_count = spider._get_existing_chapter_count(book.get('platform_novel_id', ''))
                        new_count = len(chapters) - existing_count
                        if new_count > 0:
                            print(f"   新抓取章节: {new_count}")

        return result
    except Exception as e:
        print(f"完整流程失败: {e}")
        import traceback
        traceback.print_exc()
        return {}

"""测试抓取所有config中的目标榜单并保存到数据库"""
def _test_all_ranks(spider, db, pages=1, max_books_per_rank=20):
    print(f"\n{'=' * 80}")
    print("测试: 抓取所有目标榜单")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 抓取页数: {pages}")
    print(f"  - 每榜数量: {max_books_per_rank}")
    print(f"  - 榜单列表: {list(spider.site_config.get('rank_urls', {}).keys())}")
    print(f"{'=' * 80}")

    try:
        print(f"开始抓取所有榜单...")
        all_books = spider.fetch_whole_rank()
        print(f"抓取了所有榜单，共 {len(all_books)} 本书籍")

        # 统计分类信息
        categories = {}
        for book in all_books:
            cat = book.get('main_category', '未知')
            categories[cat] = categories.get(cat, 0) + 1

        print(f"\n分类统计:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(all_books)) * 100 if all_books else 0
            print(f"   {cat:<8}: {count:>3}本 ({percentage:.1f}%)")

        # 保存所有数据到数据库
        if db and all_books:
            snapshot_date = datetime.now().strftime("%Y-%m-%d")

            # 限制数量避免数据太多
            books_to_save = all_books[:max_books_per_rank * 3]
            print(f"\n保存 {len(books_to_save)} 本书籍到数据库 (限制: {max_books_per_rank * 3}本)...")

            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family="综合榜单",
                rank_sub_cat="所有榜单合并",
                snapshot_date=snapshot_date,
                items=books_to_save,
                source_url="https://www.qidian.com",
                make_title_primary=True,
            )
            print(f"保存了 {inserted} 条综合榜单记录到数据库")

        return all_books
    except Exception as e:
        print(f"抓取所有榜单失败: {e}")
        import traceback
        traceback.print_exc()
        return []

"""测试多个指定榜单"""
def _test_multiple_ranks(
    spider,
    db,
    rank_keys=None,
    pages=1,
    top_n=5,
    fetch_detail=False,
    fetch_chapters=False,
    chapter_n=3,
):
    """测试多个指定榜单（修复：多榜时章节存储混乱）

    修复思路：
    - 多榜时不要在测试里“提前/重复”做 novel_uid 推断、existing_count 判断等逻辑
    - 每个榜单：抓取 ->（可选）补全（含章节）-> 先保存榜单快照（确保 novel 已入库）-> 再幂等 upsert 章节
    - upsert_first_n_chapters 自带 ON CONFLICT(novel_uid, chapter_num) 幂等更新，不怕重复写
    """
    if rank_keys is None:
        rank_keys = ["hotsales", "yuepiao", "recom"]

    print(f"\n{'=' * 80}")
    print("测试: 多个榜单抓取")
    print(f"{'=' * 80}")
    print(f"测试配置:")
    print(f"  - 榜单列表: {rank_keys}")
    print(f"  - 每榜页数: {pages}")
    print(f"  - 每榜数量: {top_n}")
    print(f"  - 获取详情: {'是' if fetch_detail else '否'}")
    print(f"  - 获取章节: {'是' if fetch_chapters else '否'} (章节数: {chapter_n})")
    print(f"{'=' * 80}")

    rank_urls = spider.site_config.get("rank_urls", {})
    print(f"\n爬虫配置中的榜单URLs:")
    for key, url in rank_urls.items():
        print(f"  {key}: {url}")

    valid_rank_keys = [key for key in rank_keys if key in rank_urls]
    invalid_rank_keys = [key for key in rank_keys if key not in rank_urls]

    if invalid_rank_keys:
        print(f"\n警告: 以下榜单不在配置中，将被跳过: {invalid_rank_keys}")

    if not valid_rank_keys:
        print("错误: 没有有效的榜单可测试")
        return {}

    print(f"\n开始测试以下榜单: {valid_rank_keys}")

    all_results = {}

    for rank_key in valid_rank_keys:
        print(f"\n{'=' * 60}")
        print(f"开始抓取榜单: {rank_key}")
        print(f"{'=' * 60}")

        try:
            original_pages = spider.site_config.get("pages_per_rank", 5)
            spider.site_config["pages_per_rank"] = pages

            # 1) 抓榜单
            books = spider.fetch_rank_list(rank_type=rank_key)

            spider.site_config["pages_per_rank"] = original_pages

            if top_n and top_n > 0:
                books = books[:top_n]

            print(f"抓取到 {len(books)} 本书籍")

            if not books:
                all_results[rank_key] = {"success": False, "error": "未抓取到数据"}
                print(f"榜单 '{rank_key}': 未抓取到数据")
                continue

            # 2) 可选补全（包含章节）
            if fetch_detail:
                print("开始补全数据...")
                books = spider.enrich_rank_items(
                    books,
                    max_books=top_n,
                    fetch_detail=True,
                    fetch_chapters=fetch_chapters,
                    chapter_count=chapter_n,
                )
                print("补全完成")

            # 3) 先保存榜单快照（确保 novel / titles / tags / rank_entries 入库）
            snapshot_date = datetime.now().strftime("%Y-%m-%d")
            source_url = rank_urls.get(rank_key, "")
            rank_identity = spider.rank_type_map.get(rank_key)

            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family=rank_identity.rank_family if rank_identity else "未知榜单",
                rank_sub_cat=rank_identity.rank_sub_cat if rank_identity else "",
                snapshot_date=snapshot_date,
                items=books,
                source_url=source_url,
                make_title_primary=True,
            )
            print(f"榜单 '{rank_key}': 保存 {inserted} 条榜单记录")

            # 4) 再幂等保存章节（仅当补全产出了 first_n_chapters）
            chapters_saved_total = 0
            novels_with_chapters = 0

            if fetch_chapters:
                for book in books:
                    chapters = book.get("first_n_chapters") or []
                    if not chapters:
                        continue

                    # 兼容字段名：platform_novel_id / novel_id
                    novel_id = (book.get("platform_novel_id") or book.get("novel_id") or "").strip()
                    if not novel_id:
                        continue

                    fallback_intro = (book.get("introduction") or book.get("intro") or "").strip()

                    inserted_ch = db.upsert_first_n_chapters(
                        platform="qidian",
                        platform_novel_id=novel_id,
                        publish_date=chapters[0].get("publish_date") if chapters else snapshot_date,
                        chapters=chapters,
                        novel_fallback_fields={
                            "title": book.get("title", "未知"),
                            "author": book.get("author", "未知"),
                            "intro": fallback_intro,
                            "main_category": book.get("main_category", "未知"),
                            "status": book.get("status", "ongoing"),
                            "total_words": book.get("total_words", 0),
                            "url": book.get("url", ""),
                            "tags": book.get("tags", []),
                        },
                    )

                    if inserted_ch > 0:
                        novels_with_chapters += 1
                        chapters_saved_total += inserted_ch

                print(
                    f"榜单 '{rank_key}': 章节写入完成，涉及 {novels_with_chapters} 本书，共 upsert {chapters_saved_total} 章"
                )

            all_results[rank_key] = {
                "success": True,
                "books_count": len(books),
                "inserted_count": inserted,
                "novels_with_chapters": novels_with_chapters if fetch_chapters else 0,
                "chapters_upserted": chapters_saved_total if fetch_chapters else 0,
            }

        except Exception as e:
            all_results[rank_key] = {"success": False, "error": str(e)}
            print(f"榜单 '{rank_key}' 异常: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if rank_key != valid_rank_keys[-1]:
                delay = 2
                print(f"\n等待{delay}秒后继续下一个榜单...")
                time.sleep(delay)

    return all_results


"""验证章节是否被正确存储到数据库"""
def _verify_chapter_storage(db_path: str) -> Dict[str, Any]:
    conn = _open_sqlite(db_path)
    cur = conn.cursor()

    results = {
        "total_chapters": 0,
        "novels_with_chapters": 0,
        "novels_details": [],
        "errors": []
    }

    try:
        # 1. 直接查询所有章节
        cur.execute("SELECT COUNT(*) as count FROM first_n_chapters")
        total = cur.fetchone()['count']
        print(f"first_n_chapters表总记录数: {total}")
        results["total_chapters"] = total

        # 2. 查询每个小说的章节数
        cur.execute("""
                SELECT 
                    n.novel_uid,
                    n.platform_novel_id,
                    n.author,
                    COUNT(fc.chapter_id) as chapter_count
                FROM novels n
                LEFT JOIN first_n_chapters fc ON fc.novel_uid = n.novel_uid
                WHERE n.platform = 'qidian'
                GROUP BY n.novel_uid, n.platform_novel_id
                ORDER BY n.novel_uid DESC
            """)

        novels = cur.fetchall()

        if not novels:
            print("数据库中没有小说记录")
        else:
            print(f"找到 {len(novels)} 本小说")

            for novel in novels:
                novel_id = novel['platform_novel_id']
                novel_uid = novel['novel_uid']
                author = novel['author']
                count = novel['chapter_count']

                if count > 0:
                    results["novels_with_chapters"] += 1

                # 获取小说标题
                cur.execute("""
                        SELECT title FROM novel_titles 
                        WHERE novel_uid = ? AND is_primary = 1
                        LIMIT 1
                    """, (novel_uid,))
                title_row = cur.fetchone()
                title = title_row['title'] if title_row else "未知"

                novel_details = {
                    "novel_id": novel_id,
                    "title": title,
                    "author": author,
                    "chapter_count": count,
                }

                # 获取章节详情
                if count > 0:
                    cur.execute("""
                            SELECT chapter_num, chapter_title, word_count, publish_date
                            FROM first_n_chapters
                            WHERE novel_uid = ?
                            ORDER BY chapter_num ASC
                        """, (novel_uid,))
                    chapters = cur.fetchall()
                    novel_details["chapters"] = [dict(ch) for ch in chapters[:5]]

                results["novels_details"].append(novel_details)

                status = "SUCCESS" if count > 0 else "FAIL"
                print(f"  {status} 小说ID: {novel_id}")
                print(f"      标题: {title}")
                print(f"      作者: {author}")
                print(f"      章节数: {count}")

        # 3. 显示最近的章节
        if total > 0:
            print(f"\n最近5章记录:")
            cur.execute("""
                    SELECT 
                        fc.novel_uid,
                        n.platform_novel_id,
                        fc.chapter_num,
                        fc.chapter_title,
                        fc.word_count,
                        fc.publish_date
                    FROM first_n_chapters fc
                    JOIN novels n ON n.novel_uid = fc.novel_uid
                    ORDER BY fc.chapter_id DESC
                    LIMIT 5
                """)
            recent_chapters = cur.fetchall()

            for ch in recent_chapters:
                ch_title = ch['chapter_title']
                if len(ch_title) > 30:
                    ch_title = ch_title[:27] + "..."
                print(f"    - 小说ID:{ch['platform_novel_id']} 第{ch['chapter_num']}章: {ch_title}")
                print(f"        字数: {ch['word_count']}, 发布时间: {ch['publish_date']}")

        # 4. 统计信息
        print(f"\n统计摘要:")
        print(f"  总章节数: {results['total_chapters']}")
        print(f"  有章节的小说数: {results['novels_with_chapters']}")
        print(f"  总小说数: {len(novels) if novels else 0}")

    except Exception as e:
        print(f"验证过程中出错: {e}")
        import traceback
        print(f"详细错误: {traceback.format_exc()}")
        results["errors"].append(f"验证错误: {e}")

    finally:
        conn.close()

    return results

"""全面的起点爬虫测试，所有数据保存到数据库"""
def run_comprehensive_qidian_test(
        *,
        test_cases: list = None,
        pages: int = 1,
        top_n: int = 3,
        fetch_detail: bool = True,
        fetch_chapters: bool = False,
        chapter_n: int = 3,
        rank_key: str = "hotsales",
        rank_keys: list = None,
        max_books_per_test: int = None,
        verbose: bool = True,
):
    """
    全面的起点爬虫测试，所有数据保存到数据库。

    Args:
        test_cases: 要运行的测试用例列表，可选值:
                   ['rank_list', 'novel_detail', 'chapters',
                    'enrich', 'full_pipeline', 'all_ranks', 'all']
        pages: 抓取榜单页数
        top_n: 测试书籍数量
        fetch_detail: 是否获取详情
        fetch_chapters: 是否获取章节
        chapter_n: 抓取章节数
        rank_key: 榜单类型
        max_books_per_test: 每个测试最大处理书籍数
        verbose: 是否显示详细日志
    """
    print("\n" + "=" * 100)
    print("起点中文网爬虫 - 全面功能测试 (数据保存到数据库)")
    print("=" * 100)

    # 确保rank_keys是有效的列表
    if rank_keys is None:
        rank_keys = ["hotsales", "yuepiao", "recom", "collect"]
    elif isinstance(rank_keys, str):
        # 如果是字符串，则解析为列表
        rank_keys = [key.strip() for key in rank_keys.split(',') if key.strip()]


    # 显示测试配置
    print(f"\n测试配置:")
    print(f"  - 测试用例: {test_cases or ['all']}")
    print(f"  - 榜单类型: {rank_key}")
    print(f"  - 多榜单测试: {rank_keys}")
    print(f"  - 抓取页数: {pages}")
    print(f"  - 处理数量: {top_n}")
    print(f"  - 获取详情: {'是' if fetch_detail else '否'}")
    print(f"  - 获取章节: {'是' if fetch_chapters else '否'}")
    print(f"  - 章节数量: {chapter_n}")
    print(f"  - 详细日志: {'是' if verbose else '否'}")
    print(f"{'=' * 100}")

    # 默认测试所有用例
    if test_cases is None or 'all' in test_cases:
        test_cases = ['rank_list', 'novel_detail', 'chapters', 'enrich', 'full_pipeline', 'all_ranks']

    # 添加项目路径
    sys.path.insert(0, project_root)

    # 清理旧文件
    _ensure_clean_dirs()

    # 初始化数据库
    print(f"\n[1/4] 初始化测试数据库...")
    from database.db_handler import DatabaseHandler

    db_path = os.path.join(project_root, "test_output", "qidian_test.db")
    db = DatabaseHandler(db_path, is_test=True)
    print(f"  数据库路径: {db_path}")
    print(f"  测试模式: 是")

    # 初始化爬虫
    print(f"\n[2/4] 初始化起点爬虫...")
    from spiders.qidian_spider import QidianSpider

    qidian_config = WEBSITES.get('qidian', {}).copy()
    if qidian_config:
        qidian_config["pages_per_rank"] = pages
        qidian_config["chapter_extraction_goal"] = chapter_n
    else:
        # 备用配置（如果config中没有）
        qidian_config = {
            "name": "起点中文网",
            "base_url": "https://www.qidian.com",
            "request_delay": 2,
            "pages_per_rank": pages,
            "chapter_extraction_goal": chapter_n,
            "rank_urls": {
                "hotsales": "https://www.qidian.com/rank/hotsales/page{page}/",
                "yuepiao": "https://www.qidian.com/rank/yuepiao/page{page}/",
                "recom": "https://www.qidian.com/rank/recom/page{page}/",
                "collect": "https://www.qidian.com/rank/collect/page{page}/",
                "newbook": "https://www.qidian.com/rank/newbook/page{page}/",
            },
            "novel_types": ["玄幻", "奇幻", "武侠", "仙侠", "都市", "现实",
                            "军事", "历史", "游戏", "体育", "科幻", "诸天无限",
                            "悬疑", "轻小说", "短篇"]
        }

    spider = QidianSpider(qidian_config, db)
    print(f"  爬虫初始化完成")
    print(f"  默认章节数: {spider.default_chapter_count}")
    print(f"  榜单类型映射: {list(spider.rank_type_map.keys())}")

    # 显示初始数据库状态
    print(f"\n[3/4] 初始数据库状态:")
    _print_table_counts(db)

    # 存储测试数据
    test_data = {}
    test_results = {}

    # 运行测试用例
    print(f"\n[4/4] 运行测试用例 ({len(test_cases)}个):")
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}/{len(test_cases)}. 测试用例: {test_case}")

        start_time = time.time()

        if test_case == 'rank_list':
            # 测试抓取榜单
            books = _test_fetch_rank_list(spider, db, rank_key, pages=pages, top_n=top_n)
            test_data['rank_list_books'] = books
            test_results['rank_list'] = {
                'success': len(books) > 0,
                'count': len(books),
                'time': time.time() - start_time
            }

            if books:
                # 保存第一本书的信息用于后续测试
                test_data['sample_book'] = books[0]
                test_data['sample_url'] = books[0].get('url', '')
                test_data['sample_id'] = books[0].get('platform_novel_id', '')
                print(f"  样本书籍: 《{books[0].get('title', '')}》 (ID: {test_data['sample_id']})")

        elif test_case == 'novel_detail':
            # 检查是否有样本数据，如果没有则先抓取榜单
            if 'sample_url' not in test_data:
                print("  先获取榜单数据以获取样本小说...")
                books = _test_fetch_rank_list(spider, db, rank_key, pages=pages, top_n=1)
                if books:
                    test_data['sample_url'] = books[0].get('url', '')
                    test_data['sample_id'] = books[0].get('platform_novel_id', '')

            # 测试抓取小说详情
            if 'sample_url' in test_data:
                detail = _test_fetch_novel_detail(
                    spider, db,
                    novel_url=test_data.get('sample_url'),
                    novel_id=test_data.get('sample_id')
                )
                test_data['novel_detail'] = detail
                test_results['novel_detail'] = {
                    'success': detail is not None,
                    'time': time.time() - start_time
                }
            else:
                print("  跳过 - 没有可用的样本小说URL")

        elif test_case == 'chapters':
            # 检查是否有样本数据，如果没有则先抓取榜单
            if 'sample_url' not in test_data:
                print("  先获取榜单数据以获取样本小说...")
                books = _test_fetch_rank_list(spider, db, rank_key, pages=pages, top_n=1)
                if books:
                    test_data['sample_url'] = books[0].get('url', '')
                    test_data['sample_id'] = books[0].get('platform_novel_id', '')

            # 测试抓取章节
            if 'sample_url' in test_data:
                chapters = _test_fetch_first_n_chapters(
                    spider, db,
                    novel_url=test_data.get('sample_url'),
                    novel_id=test_data.get('sample_id'),
                    chapter_n=chapter_n
                )
                test_data['chapters'] = chapters
                test_results['chapters'] = {
                    'success': len(chapters) > 0,
                    'count': len(chapters),
                    'time': time.time() - start_time
                }

        elif test_case == 'enrich':
            # 检查是否有榜单数据，如果没有则先抓取
            if 'rank_list_books' not in test_data:
                print("  先获取榜单数据...")
                books = _test_fetch_rank_list(spider, db, rank_key, pages=pages, top_n=top_n)
                if books:
                    test_data['rank_list_books'] = books

            # 测试补全数据
            if 'rank_list_books' in test_data and test_data['rank_list_books']:
                max_books = max_books_per_test or top_n
                enriched = _test_enrich_rank_items(
                    spider, db,
                    test_data['rank_list_books'],
                    max_books=min(max_books, len(test_data['rank_list_books'])),
                    fetch_detail=fetch_detail,
                    fetch_chapters=fetch_chapters,
                    chapter_n=chapter_n
                )
                test_data['enriched_books'] = enriched
                test_results['enrich'] = {
                    'success': len(enriched) > 0,
                    'count': len(enriched),
                    'time': time.time() - start_time
                }
            else:
                print("  跳过 - 没有可用的榜单数据")

        elif test_case == 'full_pipeline':
            # 测试完整流程
            result = _test_fetch_and_save_rank(
                spider, db, rank_key, pages, top_n, fetch_detail, fetch_chapters, chapter_n
            )
            test_data['full_pipeline_result'] = result
            test_results['full_pipeline'] = {
                'success': result.get('snapshot_id') is not None,
                'snapshot_id': result.get('snapshot_id'),
                'count': len(result.get('items', [])),
                'time': time.time() - start_time
            }

        elif test_case == 'all_ranks':
            # 测试抓取所有榜单
            all_books = _test_all_ranks(spider, db, pages=pages, max_books_per_rank=top_n)
            test_data['all_ranks_books'] = all_books
            test_results['all_ranks'] = {
                'success': len(all_books) > 0,
                'count': len(all_books),
                'time': time.time() - start_time
            }

        elif test_case == 'multiple_ranks':
            # 测试多个榜单 - 使用传入的rank_keys参数
            multi_results = _test_multiple_ranks(
                spider, db,
                rank_keys=rank_keys,
                pages=pages,
                top_n=top_n,
                fetch_detail=fetch_detail,
                fetch_chapters=fetch_chapters,
                chapter_n=chapter_n,
            )
            test_data['multiple_ranks_results'] = multi_results
            success_count = sum(1 for r in multi_results.values() if r.get('success', False))
            test_results['multiple_ranks'] = {
                'success': success_count > 0,
                'ranks_tested': len(multi_results),
                'success_count': success_count,
                'results': multi_results
            }


        else:
            print(f"  跳过测试用例 {test_case} (未知的测试类型)")

        # 记录每个测试用例的耗时
        elapsed = time.time() - start_time
        test_results[test_case]['elapsed'] = elapsed
        print(f"  测试用例 '{test_case}' 总耗时: {elapsed:.2f}秒")

        # 测试间隔（不是最后一个测试）
        if test_case != test_cases[-1]:
            delay = 3
            print(f"  等待{delay}秒后继续下一个测试...")
            time.sleep(delay)

    # 最终数据库状态
    print(f"\n{'=' * 100}")
    print("最终数据库状态:")
    print(f"{'=' * 100}")
    _print_table_counts(db)

    if verbose:
        _peek_some_rows(db_path)

    # 生成测试报告
    print(f"\n{'=' * 100}")
    print("测试报告:")
    print(f"{'=' * 100}")

    # 统计测试结果
    total_tests = len(test_results)
    passed_tests = sum(1 for result in test_results.values() if result.get('success', False))

    print(f"\n测试用例执行情况 ({passed_tests}/{total_tests} 通过):")
    for test_name, result in test_results.items():
        status = "通过" if result.get('success', False) else "失败"
        time_taken = f"{result.get('time', 0):.1f}s"

        if test_name == 'rank_list':
            count = result.get('count', 0)
            print(f"  {status} {test_name:<15}: 抓取{count}本书籍, 耗时{time_taken}")
        elif test_name == 'novel_detail':
            print(f"  {status} {test_name:<15}: 详情页抓取, 耗时{time_taken}")
        elif test_name == 'chapters':
            count = result.get('count', 0)
            print(f"  {status} {test_name:<15}: 抓取{count}个章节, 耗时{time_taken}")
        elif test_name == 'enrich':
            count = result.get('count', 0)
            print(f"  {status} {test_name:<15}: 补全{count}本书籍, 耗时{time_taken}")
        elif test_name == 'full_pipeline':
            snapshot_id = result.get('snapshot_id', 'N/A')
            count = result.get('count', 0)
            print(f"  {status} {test_name:<15}: 快照ID={snapshot_id}, {count}本书籍, 耗时{time_taken}")
        elif test_name == 'all_ranks':
            count = result.get('count', 0)
            print(f"  {status} {test_name:<15}: 抓取{count}本书籍, 耗时{time_taken}")

    # 数据库统计详情
    print(f"\n数据库统计详情:")
    conn = _open_sqlite(db_path)
    cur = conn.cursor()

    # 统计各表记录数
    tables = [
        ('novels', '小说基本信息'),
        ('novel_titles', '小说标题记录'),
        ('tags', '标签信息'),
        ('novel_tag_map', '小说标签映射'),
        ('rank_lists', '榜单列表'),
        ('rank_snapshots', '榜单快照'),
        ('rank_entries', '榜单条目'),
        ('first_n_chapters', '前N章内容'),
    ]

    total_records = 0
    for table, description in tables:
        cur.execute(f"SELECT COUNT(*) as count FROM {table}")
        count = cur.fetchone()['count']
        total_records += count
        status = "有数据" if count > 0 else "无数据"
        print(f"  {status} {description:<15}: {count:>5} 条记录")

    print(f"\n  总计: {total_records} 条记录")

    conn.close()

    # 验证章节存储
    print("章节存储验证")
    _verify_chapter_storage(db_path)

    # 智能抓取统计
    if 'enrich' in test_results and fetch_chapters:
        print(f"\n智能章节抓取统计:")
        print(f"  启用了智能抓取逻辑")
        print(f"  目标章节数: {chapter_n}")
        print(f"  数据库检查: 已实现")

    # 关闭爬虫
    print(f"\n{'=' * 100}")
    print("测试完成")
    print(f"{'=' * 100}")

    try:
        spider.close()
        print(f"爬虫已关闭")
    except Exception as e:
        print(f"关闭爬虫时出错: {e}")

    print(f"\n输出文件:")
    print(f"  数据库文件: {db_path}")
    print(f"  调试目录: test_output/debug/")



def run_quick_test():
    """快速测试：只测试基本功能"""
    print("\n" + "=" * 100)
    print("⚡ 快速测试模式")
    print("=" * 100)

    print(f"\n测试配置:")
    print(f"  - 测试用例: rank_list, novel_detail")
    print(f"  - 抓取页数: 1")
    print(f"  - 处理数量: 2")
    print(f"  - 获取详情: 是")
    print(f"  - 获取章节: 否")
    print(f"{'=' * 100}")

    run_comprehensive_qidian_test(
        test_cases=['rank_list', 'novel_detail'],
        pages=1,
        top_n=2,
        fetch_detail=True,
        fetch_chapters=False,
        rank_key="hotsales",
        verbose=False,
    )


def run_custom_test(args):
    """自定义测试"""
    test_cases = [args.test] if args.test != 'all' else None

    # 修改这里的解析逻辑
    rank_keys = []
    if args.rank_keys:
        # 首先去除空格，然后按逗号分割，再过滤空字符串
        rank_keys = [key.strip() for key in args.rank_keys.split(',') if key.strip()]

    if not rank_keys:  # 如果为空，使用默认值
        rank_keys = ["hotsales", "yuepiao", "recom", "collect"]

    print(f"解析到的rank_keys: {rank_keys}")  # 添加调试信息

    if args.test == 'quick':
        run_quick_test()
    else:
        run_comprehensive_qidian_test(
            test_cases=test_cases,
            pages=args.pages,
            top_n=args.top_n,
            fetch_detail=args.fetch_detail,
            fetch_chapters=args.fetch_chapters,
            chapter_n=args.chapter_n,
            rank_key=args.rank_key,
            rank_keys=rank_keys,  # 传递处理后的列表
            max_books_per_test=args.max_books,
            verbose=args.verbose,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="起点中文网爬虫综合测试",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # 测试用例选择
    parser.add_argument("--test", type=str, default="all",
                        choices=['rank_list', 'novel_detail', 'chapters',
                                 'enrich', 'full_pipeline', 'all_ranks', 'multiple_ranks', 'all', 'quick'],
                        help="要运行的测试用例")

    # 测试参数
    parser.add_argument("--pages", type=int, default=1,
                        help="抓取榜单页数 (默认: 1)")
    parser.add_argument("--top_n", type=int, default=3,
                        help="测试书籍数量 (默认: 3)")
    parser.add_argument("--fetch_detail", action="store_true", default=True,
                        help="是否获取详情 (默认: 是)")
    parser.add_argument("--no_fetch_detail", action="store_false", dest="fetch_detail",
                        help="不获取详情")
    parser.add_argument("--fetch_chapters", action="store_true",
                        help="是否抓取章节 (默认: 否)")
    parser.add_argument("--chapter_n", type=int, default=3,
                        help="抓取章节数 (默认: 3)")
    parser.add_argument("--rank_key", type=str, default="hotsales",
                        choices=['hotsales', 'yuepiao', 'recom', 'collect', 'newbook'],
                        help="榜单类型 (默认: hotsales)")
    parser.add_argument("--rank_keys", type=str, default="hotsales,yuepiao,recom,collect",
                        help="多个榜单的键，用逗号分隔 (默认: hotsales,yuepiao,recom,collect)")
    parser.add_argument("--max_books", type=int, default=None,
                        help="每个测试最大处理书籍数 (默认: 使用top_n)")
    parser.add_argument("--verbose", action="store_true",
                        help="显示详细日志和数据库内容")

    args = parser.parse_args()

    # 运行测试
    run_custom_test(args)