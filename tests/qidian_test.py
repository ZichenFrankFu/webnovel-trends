# test/qidian_test.py
import os
import sys
import shutil
import time
import sqlite3
import argparse
from datetime import datetime, timedelta


def _project_root() -> str:
    """Return project root path (webnovel_trends/)."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _ensure_clean_dirs():
    """Remove old test artifacts and create required output dirs."""
    test_dirs = ["test_output", "outputs/debug"]
    for d in test_dirs:
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                print(f"[cleanup] removed: {d}")
            except Exception as e:
                print(f"[cleanup] failed to remove {d}: {e}")

    os.makedirs("test_output", exist_ok=True)
    os.makedirs("outputs/debug", exist_ok=True)


def _open_sqlite(db_path: str) -> sqlite3.Connection:
    """Open sqlite connection for verification queries."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _print_table_counts(db):
    """Print all table counts using db.get_table_counts()."""
    counts = db.get_table_counts()
    print("\n[db] 数据库统计:")
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
    """Show a few rows from key tables to verify inserts."""
    conn = _open_sqlite(db_path)
    cur = conn.cursor()

    print("\n[db] 榜单列表 (rank_lists):")
    cur.execute(
        """
        SELECT rank_list_id, platform, rank_family, rank_sub_cat, source_url
        FROM rank_lists
        ORDER BY rank_list_id DESC
        LIMIT 5
        """
    )
    for r in cur.fetchall():
        print(f"  - #{r['rank_list_id']} {r['platform']} / {r['rank_family']} / {r['rank_sub_cat']}")

    print("\n[db] 榜单快照 (rank_snapshots):")
    cur.execute(
        """
        SELECT snapshot_id, rank_list_id, snapshot_date, item_count
        FROM rank_snapshots
        ORDER BY snapshot_id DESC
        LIMIT 5
        """
    )
    for r in cur.fetchall():
        print(f"  - snapshot#{r['snapshot_id']} list#{r['rank_list_id']} {r['snapshot_date']} items={r['item_count']}")

    print("\n[db] 榜单条目 (rank_entries - top 5):")
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
        print("  (暂无数据)")
    for r in rows:
        title = r["title"] or "(无主标题)"
        print(f"  - 排名 {r['rank']}: 《{title}》 / {r['author']} / {r['main_category']} / 总推荐={r['total_recommend']}")

    print("\n[db] 小说 (novels):")
    cur.execute(
        """
        SELECT novel_uid, platform, platform_novel_id, author, main_category, status, total_words
        FROM novels
        ORDER BY novel_uid DESC
        LIMIT 5
        """
    )
    for r in cur.fetchall():
        print(
            f"  - #{r['novel_uid']} {r['platform']}:{r['platform_novel_id']} / {r['author']} / {r['main_category']} / {r['status']} / {r['total_words']}字")

    print("\n[db] 标签 (tags):")
    cur.execute(
        """
        SELECT tag_id, tag_name
        FROM tags
        ORDER BY tag_id DESC
        LIMIT 10
        """
    )
    for r in cur.fetchall():
        print(f"  - #{r['tag_id']}: {r['tag_name']}")

    print("\n[db] 章节 (first_n_chapters):")
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
    for r in cur.fetchall():
        print(
            f"  - novel#{r['novel_uid']} 第{r['chapter_num']}章: {r['chapter_title'][:30]}... / {r['word_count']}字 / {r['publish_date']}")

    conn.close()


def _test_fetch_rank_list(spider, db, rank_key="hotsales", snapshot_date=None):
    """测试抓取榜单并保存到数据库"""
    print(f"\n{'=' * 60}")
    print(f"测试：抓取榜单数据并保存到数据库 (rank_type={rank_key})")
    print(f"{'=' * 60}")

    try:
        books = spider.fetch_rank_list(rank_type=rank_key)
        print(f"[成功] 抓取了 {len(books)} 本书籍")

        if books:
            print("\n前3本书籍信息:")
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

            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family=rank_family,
                rank_sub_cat=rank_sub_cat,
                snapshot_date=snapshot_date,
                items=books,
                source_url=source_url,
                make_title_primary=True,
            )
            print(f"[数据库] 保存了 {inserted} 条榜单记录")
            _print_table_counts(db)

        return books
    except Exception as e:
        print(f"[失败] 抓取榜单失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def _test_fetch_novel_detail(spider, db, novel_url=None, novel_id=None):
    """测试抓取小说详情并保存到数据库"""
    print(f"\n{'=' * 60}")
    print("测试：抓取小说详情")
    print(f"{'=' * 60}")

    if not novel_url and not novel_id:
        print("[跳过] 没有提供小说URL或ID")
        return None

    try:
        detail = spider.fetch_novel_detail(novel_url or "", novel_id or "")
        print("[成功] 抓取了小说详情")

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
            print(f"[数据库] 通过测试榜单保存了小说详情")

        return detail
    except Exception as e:
        print(f"[失败] 抓取详情失败: {e}")
        import traceback
        traceback.print_exc()
        return None


def _test_fetch_first_n_chapters(spider, db, novel_url=None, novel_id=None, chapter_n=3):
    """测试抓取前N章内容并保存到数据库"""
    print(f"\n{'=' * 60}")
    print(f"测试：抓取前{chapter_n}章内容并保存到数据库")
    print(f"{'=' * 60}")

    if not novel_url:
        print("[跳过] 没有提供小说URL")
        return []

    try:
        chapters = spider.fetch_first_n_chapters(novel_url, n=chapter_n)
        print(f"[成功] 抓取了 {len(chapters)} 章内容")

        if chapters:
            print(f"\n前{min(3, len(chapters))}章信息:")
            for i, chapter in enumerate(chapters[:3], 1):
                print(f"\n{i}. {chapter.get('chapter_title', '无标题')}")
                print(f"   字数: {chapter.get('word_count', 0)}")
                print(f"   发布日期: {chapter.get('publish_date', '未知')}")
                print(f"   内容预览: {chapter.get('chapter_content', '')[:100]}...")

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
                print(f"[数据库] 保存了 {inserted} 章内容")

        return chapters
    except Exception as e:
        print(f"[失败] 抓取章节失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def _test_enrich_rank_items(spider, db, books, max_books=3, snapshot_date=None):
    """测试丰富榜单数据并保存到数据库"""
    print(f"\n{'=' * 60}")
    print(f"测试：丰富榜单数据并保存到数据库 (最多{max_books}本)")
    print(f"{'=' * 60}")

    if not books:
        print("[跳过] 没有书籍数据")
        return []

    try:
        enriched_books = spider.enrich_rank_items(
            books,
            max_books=max_books,
            fetch_detail=True,
            fetch_chapters=False,
        )
        print(f"[成功] 丰富了 {len(enriched_books)} 本书籍的数据")

        if enriched_books:
            print(f"\n丰富后的数据:")
            for i, book in enumerate(enriched_books[:2], 1):
                print(f"\n{i}. 《{book.get('title', '无标题')}》")
                print(
                    f"   分类变化: {books[i - 1].get('main_category', '未知')} -> {book.get('main_category', '未知')}")
                print(f"   状态: {book.get('status', '未知')}")
                print(f"   字数: {book.get('total_words', 0)}")
                print(f"   总推荐: {book.get('total_recommend', 'N/A')}")
                print(f"   上架时间: {book.get('first_upload_date', '未知')}")

        # 保存丰富后的数据到数据库
        if db and enriched_books:
            snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
            source_url = spider.site_config.get("rank_urls", {}).get("hotsales", "")

            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family="丰富测试榜",
                rank_sub_cat="",
                snapshot_date=snapshot_date,
                items=enriched_books,
                source_url=source_url,
                make_title_primary=True,
            )
            print(f"[数据库] 保存了 {inserted} 条丰富后的榜单记录")

        return enriched_books
    except Exception as e:
        print(f"[失败] 丰富数据失败: {e}")
        import traceback
        traceback.print_exc()
        return books[:max_books]


def _test_fetch_and_save_rank(spider, db, rank_key="hotsales", pages=1, top_n=3, fetch_chapters=False, chapter_n=3):
    """测试完整流程：抓取榜单 -> 丰富数据 -> 保存到数据库"""
    print(f"\n{'=' * 60}")
    print(f"测试：完整流程 (抓取榜单 -> 丰富数据 -> 保存到数据库)")
    print(f"{'=' * 60}")

    try:
        result = spider.fetch_and_save_rank(
            rank_type=rank_key,
            pages=pages,
            enrich_detail=True,
            enrich_chapters=fetch_chapters,
            chapter_count=chapter_n,
            snapshot_date=datetime.now().strftime("%Y-%m-%d"),
            max_books=top_n,
        )

        print(f"[成功] 完成完整流程")
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

        return result
    except Exception as e:
        print(f"[失败] 完整流程失败: {e}")
        import traceback
        traceback.print_exc()
        return {}


def _test_all_ranks(spider, db):
    """测试抓取所有配置的榜单并保存到数据库"""
    print(f"\n{'=' * 60}")
    print("测试：抓取所有配置的榜单并保存到数据库")
    print(f"{'=' * 60}")

    try:
        all_books = spider.fetch_all_ranks()
        print(f"[成功] 抓取了所有榜单，共 {len(all_books)} 本书籍")

        # 统计分类信息
        categories = {}
        for book in all_books:
            cat = book.get('main_category', '未知')
            categories[cat] = categories.get(cat, 0) + 1

        print(f"\n分类统计:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            print(f"   {cat}: {count}本")

        # 保存所有数据到数据库
        if db and all_books:
            snapshot_date = datetime.now().strftime("%Y-%m-%d")

            inserted = db.save_rank_snapshot(
                platform="qidian",
                rank_family="综合榜单",
                rank_sub_cat="所有榜单合并",
                snapshot_date=snapshot_date,
                items=all_books[:50],  # 最多保存50本，避免数据太多
                source_url="https://www.qidian.com",
                make_title_primary=True,
            )
            print(f"[数据库] 保存了 {inserted} 条综合榜单记录")

        return all_books
    except Exception as e:
        print(f"[失败] 抓取所有榜单失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def run_comprehensive_qidian_test(
        *,
        test_cases: list = None,
        pages: int = 1,
        top_n: int = 3,
        fetch_chapters: bool = False,
        chapter_n: int = 3,
        rank_key: str = "hotsales",
):
    """
    全面的起点爬虫测试，所有数据保存到数据库。

    Args:
        test_cases: 要运行的测试用例列表，可选值:
                   ['rank_list', 'novel_detail', 'chapters',
                    'enrich', 'full_pipeline', 'all_ranks', 'all']
        pages: 抓取榜单页数
        top_n: 测试书籍数量
        fetch_chapters: 是否抓取章节
        chapter_n: 抓取章节数
        rank_key: 榜单类型
    """
    print("=" * 80)
    print("起点中文网爬虫 - 全面功能测试 (数据保存到数据库)")
    print("=" * 80)

    # 默认测试所有用例
    if test_cases is None or 'all' in test_cases:
        test_cases = ['rank_list', 'novel_detail', 'chapters', 'enrich', 'full_pipeline', 'all_ranks']

    # 添加项目路径
    project_root = _project_root()
    sys.path.insert(0, project_root)

    # 清理旧文件
    _ensure_clean_dirs()

    # 初始化数据库
    print("\n[1] 初始化测试数据库 ...")
    from database.db_handler import DatabaseHandler

    db_path = os.path.join(project_root, "test_output", "qidian_test.db")
    db = DatabaseHandler(db_path, is_test=True)
    print(f"[db] 数据库路径: {db_path}")

    # 初始化爬虫
    print("\n[2] 初始化起点爬虫 ...")
    from spiders.qidian_spider import QidianSpider

    qidian_config = {
        "name": "起点中文网",
        "base_url": "https://www.qidian.com",
        "request_delay": 2,
        "pages_per_rank": pages,
        "chapter_extraction_goal": chapter_n,
        "rank_urls": {
            "hotsales": "https://www.qidian.com/rank/hotsales/page{page}/",
            "yuepiao": "https://www.qidian.com/rank/yuepiao/page{page}/",
            "recommend": "https://www.qidian.com/rank/recommend/page{page}/",
            "collect": "https://www.qidian.com/rank/collect/page{page}/",
            "newbook": "https://www.qidian.com/rank/newbook/page{page}/",
        },
        "novel_types": ["玄幻", "奇幻", "武侠", "仙侠", "都市", "现实",
                        "军事", "历史", "游戏", "体育", "科幻", "诸天无限",
                        "悬疑", "轻小说", "短篇"]
    }

    spider = QidianSpider(qidian_config, db)
    print(f"[spider] 爬虫初始化完成")

    # 显示初始数据库状态
    _print_table_counts(db)

    # 存储测试数据
    test_data = {}

    # 运行测试用例
    for test_case in test_cases:
        if test_case == 'rank_list':
            # 测试抓取榜单
            books = _test_fetch_rank_list(spider, db, rank_key)
            test_data['rank_list_books'] = books

            if books:
                # 保存第一本书的信息用于后续测试
                test_data['sample_book'] = books[0]
                test_data['sample_url'] = books[0].get('url', '')
                test_data['sample_id'] = books[0].get('platform_novel_id', '')

        elif test_case == 'novel_detail' and 'sample_url' in test_data:
            # 测试抓取小说详情
            detail = _test_fetch_novel_detail(
                spider, db,
                novel_url=test_data.get('sample_url'),
                novel_id=test_data.get('sample_id')
            )
            test_data['novel_detail'] = detail

        elif test_case == 'chapters' and 'sample_url' in test_data:
            # 测试抓取章节
            chapters = _test_fetch_first_n_chapters(
                spider, db,
                novel_url=test_data.get('sample_url'),
                novel_id=test_data.get('sample_id'),
                chapter_n=chapter_n
            )
            test_data['chapters'] = chapters

        elif test_case == 'enrich' and 'rank_list_books' in test_data:
            # 测试丰富数据
            enriched = _test_enrich_rank_items(
                spider, db,
                test_data['rank_list_books'],
                max_books=min(top_n, len(test_data['rank_list_books']))
            )
            test_data['enriched_books'] = enriched

        elif test_case == 'full_pipeline':
            # 测试完整流程
            result = _test_fetch_and_save_rank(
                spider, db, rank_key, pages, top_n, fetch_chapters, chapter_n
            )
            test_data['full_pipeline_result'] = result

        elif test_case == 'all_ranks':
            # 测试抓取所有榜单
            all_books = _test_all_ranks(spider, db)
            test_data['all_ranks_books'] = all_books

        # 测试间隔
        if test_case != test_cases[-1]:
            print("\n" + "=" * 60)
            print("等待3秒后继续下一个测试...")
            print("=" * 60 + "\n")
            time.sleep(3)

    # 最终数据库状态
    print("\n" + "=" * 80)
    print("最终数据库状态:")
    print("=" * 80)
    _print_table_counts(db)
    _peek_some_rows(db_path)

    # 生成测试报告
    print("\n" + "=" * 80)
    print("测试报告:")
    print("=" * 80)

    # 统计测试结果
    total_tests = len(test_cases)
    passed_tests = 0

    test_results = []
    for test_case in test_cases:
        if test_case in test_data and test_data.get(test_case + '_books') or test_data.get(test_case + '_result'):
            passed_tests += 1
            test_results.append(f"✓ {test_case}: 成功")
        else:
            test_results.append(f"✗ {test_case}: 失败或未执行")

    print(f"\n测试用例 ({passed_tests}/{total_tests} 通过):")
    for result in test_results:
        print(f"  {result}")

    # 数据库统计详情
    print(f"\n数据库统计详情:")
    conn = _open_sqlite(db_path)
    cur = conn.cursor()

    # 统计各表记录数
    tables = ['novels', 'novel_titles', 'tags', 'novel_tag_map',
              'rank_lists', 'rank_snapshots', 'rank_entries', 'first_n_chapters']

    for table in tables:
        cur.execute(f"SELECT COUNT(*) as count FROM {table}")
        count = cur.fetchone()['count']
        print(f"  {table}: {count} 条记录")

    conn.close()

    # 关闭爬虫
    print("\n[完成] 所有测试完成，所有数据已保存到数据库")
    try:
        spider.close()
        print("[spider] 爬虫已关闭")
    except Exception as e:
        print(f"[spider] 关闭爬虫时出错: {e}")

    print(f"\n数据库文件位置: {db_path}")
    print("可以使用SQLite工具查看详细数据")


def run_quick_test():
    """快速测试：只测试基本功能"""
    print("=" * 80)
    print("快速测试模式")
    print("=" * 80)

    run_comprehensive_qidian_test(
        test_cases=['rank_list', 'novel_detail'],
        pages=1,
        top_n=2,
        fetch_chapters=False,
        rank_key="hotsales",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="起点中文网爬虫全面功能测试 - 数据保存到数据库")

    # 测试用例选择
    parser.add_argument("--test", type=str, default="all",
                        choices=['rank_list', 'novel_detail', 'chapters',
                                 'enrich', 'full_pipeline', 'all_ranks', 'all', 'quick'],
                        help="要运行的测试用例")

    # 测试参数
    parser.add_argument("--pages", type=int, default=1, help="抓取榜单页数")
    parser.add_argument("--top_n", type=int, default=3, help="测试书籍数量")
    parser.add_argument("--fetch_chapters", action="store_true", help="是否抓取章节")
    parser.add_argument("--chapter_n", type=int, default=3, help="抓取章节数")
    parser.add_argument("--rank_key", type=str, default="hotsales",
                        choices=['hotsales', 'yuepiao', 'recommend', 'collect', 'newbook'],
                        help="榜单类型")

    args = parser.parse_args()

    # 根据参数决定测试内容
    if args.test == 'quick':
        run_quick_test()
    else:
        test_cases = [args.test] if args.test != 'all' else None

        run_comprehensive_qidian_test(
            test_cases=test_cases,
            pages=args.pages,
            top_n=args.top_n,
            fetch_chapters=args.fetch_chapters,
            chapter_n=args.chapter_n,
            rank_key=args.rank_key,
        )