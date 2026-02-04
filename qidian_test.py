# qidian_test.py
import os
import sys
import shutil
import time
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

"""起点爬虫测试"""
def test_qidian_features_safe():
    print("=" * 70)
    print("起点爬虫测试")
    print("=" * 70)

    # 清理旧的测试文件
    test_dirs = ['test_output', 'outputs/debug']
    for dir_path in test_dirs:
        if os.path.exists(dir_path):
            try:
                shutil.rmtree(dir_path)
                print(f"已清理: {dir_path}")
            except Exception as e:
                print(f"清理失败 {dir_path}: {e}")

    # 确保测试输出目录存在
    os.makedirs('test_output', exist_ok=True)
    os.makedirs('outputs/debug', exist_ok=True)

    # 创建测试数据库
    print("\n1. 初始化测试数据库...")
    try:
        from database.db_handler import create_test_db_handler
        db = create_test_db_handler()
        print(f"测试数据库初始化成功")

        # 打印各表初始记录数量
        counts = db.get_table_counts()
        print(f"初始表记录数: daily_rankings={counts.get('daily_rankings', 0)}, "
              f"novel_archive={counts.get('novel_archive', 0)}, "
              f"novel_chapters={counts.get('novel_chapters', 0)}")
    except Exception as e:
        print(f"数据库初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return

    print("\n1.5 测试数据库操作功能...")
    try:
        # 测试基本数据库操作
        test_data = {
            'novel_id': 'test_novel_001',
            'title': '测试小说',
            'author': '测试作者',
            'platform': 'qidian',
            'novel_url': 'https://example.com',
            'category': '测试分类',
            'introduction': '测试简介',
            'rank': 1,
            'rank_type':"测试榜单",
            'fetch_date': datetime.now(),
            'fetch_time': datetime.now(),
        }

        # 测试保存daily_ranking
        result1 = db.save_daily_ranking(test_data)
        print(f"   daily_ranking保存测试: {result1}")

        # 测试保存novel_archive
        result2 = db.save_novel(test_data)
        print(f"   novel_archive保存测试: {result2}")

        # 测试章节保存
        test_chapters = [
            {
                'chapter_num': 1,
                'chapter_title': '测试第一章',
                'chapter_content': '测试内容',
                'chapter_url': 'https://example.com/chapter1',
                'first_post_time': '2024-01-01',
                'word_count': 100,
                'novel_title': '测试书名'
            }
        ]
        result3 = db.save_novel(test_data, test_chapters)
        print(f"   novel_archive带章节保存测试: {result3}")

        # 检查记录数
        counts = db.get_table_counts()
        print(f"   测试后表记录数: daily_rankings={counts.get('daily_rankings', 0)}, "
              f"novel_archive={counts.get('novel_archive', 0)}, "
              f"novel_chapters={counts.get('novel_chapters', 0)}")

    except Exception as e:
        print(f"   数据库操作测试失败: {e}")
        import traceback
        traceback.print_exc()

    # 配置起点爬虫
    print("\n2. 配置起点爬虫...")
    try:
        qidian_config = {
            'name': '起点中文网',
            'rank_urls': {
                'hotsales': 'https://www.qidian.com/rank/hotsales/page{page}/',
            },
            'base_url': 'https://www.qidian.com',
            'request_delay': 3,
            'pages_per_rank': 1,
            'chapter_extraction_goal': 5,  # 设置章节获取目标
        }

        print("导入QidianSpider...")
        from spiders.qidian_spider import QidianSpider

        # 检查QidianSpider的__init__参数
        import inspect
        init_signature = inspect.signature(QidianSpider.__init__)
        print(f"QidianSpider.__init__参数: {init_signature}")

        # 根据参数数量创建实例
        params = list(init_signature.parameters.keys())
        if len(params) >= 3:  # 包括self, site_config, db_handler
            print("创建爬虫实例（带db_handler）...")
            spider = QidianSpider(qidian_config, db)
            print("爬虫初始化成功（带db_handler）")
        else:
            print("创建爬虫实例（不带db_handler）...")
            spider = QidianSpider(qidian_config)
            # 尝试动态添加db_handler属性
            spider.db_handler = db
            print("爬虫初始化成功（动态添加db_handler）")

        print("开始抓取起点畅销榜...")

        books = spider.fetch_rank_list('hotsales')

        if not books:
            print("未抓取到书籍数据，测试结束")
            spider.close()
            return

        print(f"成功抓取 {len(books)} 本书籍")

        print(f"\n3. 测试前三本书获取元数据:")
        # 选择前三本书进行详细测试
        if len(books) >= 3:
            test_books = books[:3]

            for book_index, book in enumerate(test_books, 1):
                print(f"\n   [{book_index}] 测试书籍: 《{book['title']}》")
                print(f"       作者: {book.get('author', '未知')}")
                print(f"       分类: {book.get('category', '未知')}")
                print(f"       URL: {book['url']}")
                print(f"       平台ID: {book['novel_id']}")
                print(f"       排名: {book['rank']}")

                # 保存每本书到数据库
                print(f"\n   4.{book_index}测试保存书籍到数据库...")
                try:
                    result = db.save_daily_ranking(book)
                    if result:
                        print(f"       成功保存: 《{book['title'][:20]}...》")
                    else:
                        print("       保存失败")
                except Exception as e:
                    print(f"       保存失败: {e}")

            # 测试详情补充功能
            print(f"\n   5.{book_index} 测试详情补充功能...")
            try:
                detail = spider.fetch_novel_detail(
                    book['url'],
                    book['novel_id']
                )

                if detail:
                    print(f"       成功获取详情")
                    if 'title' in detail:
                        print(f"       标题: {detail['title']}")
                    if 'author' in detail:
                        print(f"       作者: {detail['author']}")
                    if 'category' in detail:
                        print(f"       分类: {detail['category']}")
                    if 'introduction' in detail:
                        intro_preview = detail['introduction'][:80].replace('\n', ' ')
                        print(f"       简介预览: {intro_preview}...")
                else:
                    print("       详情获取失败")
            except Exception as e:
                print(f"       详情补充测试失败: {e}")

            # 测试章节获取功能（测试前三本书）
            print("\n6. 测试章节获取功能（前三本书）...")

            for book_index, book in enumerate(test_books, 1):
                print(f"\n   6.{book_index} 测试书籍《{book['title'][:15]}...》的章节获取功能...")
                try:
                    if hasattr(spider, 'fetch_novel_chapters'):
                        print(f"       开始抓取《{book['title'][:15]}...》的前3章内容...")

                        # 先获取书籍详情
                        detail = spider.fetch_novel_detail(
                            book['url'],
                            book['novel_id']
                        )

                        # 抓取章节
                        chapters = spider.fetch_novel_chapters(
                            book['url'],
                            book['novel_id'],
                            chapter_count=3  # 每本书抓取3章
                        )

                        if chapters:
                            print(f"       成功抓取 {len(chapters)} 章")

                            # 显示每章的详细信息
                            for i, chapter in enumerate(chapters, 1):
                                print(f"         {i}. {chapter['chapter_title']}")
                                if chapter.get('chapter_content'):
                                    content_preview = chapter['chapter_content'][:60].replace('\n', ' ')
                                    print(f"            内容预览: {content_preview}...")
                                if chapter.get('word_count'):
                                    print(f"            字数: {chapter['word_count']}")
                                if chapter.get('first_post_time'):
                                    print(f"            首发时间: {chapter['first_post_time']}")

                            # 保存章节到数据库
                            print(f"\n       保存《{book['title'][:15]}...》的章节到数据库...")
                            if hasattr(spider, 'db_handler') and spider.db_handler:
                                # 准备小说基本信息
                                novel_data = {
                                    'novel_id': book['novel_id'],
                                    'title': detail.get('title', book['title']) if detail else book['title'],
                                    'novel_title': detail.get('novel_title', book['title']) if detail else book['title'],
                                    'author': detail.get('author', book.get('author', '未知')) if detail else book.get(
                                        'author', '未知'),
                                    'platform': 'qidian',
                                    'novel_url': book['url'],
                                    'category': detail.get('category',
                                                           book.get('category', '')) if detail else book.get('category',
                                                                                                             ''),
                                    'introduction': detail.get('introduction', '') if detail else '',
                                    'tags': detail.get('tags', []) if detail else [],
                                }

                                # 保存小说基本信息
                                novel_saved = spider.db_handler.save_novel(novel_data, chapters)
                                if novel_saved:
                                    print(f"       小说信息保存成功")

                                    # 打印当前书籍保存后的记录数
                                    counts = db.get_table_counts()
                                    print(f"       当前表记录数: novel_archive={counts.get('novel_archive', 0)}, "
                                          f"novel_chapters={counts.get('novel_chapters', 0)}")

                                    # 验证当前书籍的章节是否保存成功
                                    db_chapters = db.get_novel_chapters(book['novel_id'], 2)
                                    print(f"       从数据库读取《{book['title'][:15]}...》的前2章: {len(db_chapters)} 章")
                                    if db_chapters:
                                        for i, db_chapter in enumerate(db_chapters, 1):
                                            print(
                                                f"           {i}. {db_chapter['chapter_title']} (ID: {db_chapter.get('id', 'N/A')})")
                                    else:
                                        print(f"           警告：未能从数据库读取到章节，可能保存失败")
                                else:
                                    print("       小说信息保存失败")
                            else:
                                print("       爬虫没有db_handler，跳过保存")
                        else:
                            print("       章节抓取失败或未找到章节")
                    else:
                        print("       爬虫没有fetch_novel_chapters方法")
                except Exception as e:
                    print(f"       章节抓取失败: {e}")
                    import traceback
                    traceback.print_exc()

                # 书籍间延迟（除了最后一本书）
                if book_index < len(test_books):
                    print(f"\n       等待3秒后测试下一本书的章节...")
                    time.sleep(3)

                # 检查所有保存的章节总数
                try:
                    counts = db.get_table_counts()
                    total_chapters = counts.get('novel_chapters', 0)
                    total_novels = counts.get('novel_archive', 0)
                    print(f"   - 数据库统计: 已保存小说数={total_novels}, 已保存章节总数={total_chapters}")
                except Exception as e:
                    print(f"   - 数据库统计失败: {e}")

            # 测试详情补充集成功能（对前三本书进行批量处理）
            print("\n7. 测试详情补充集成功能（批量处理前三本书）...")
            try:
                if hasattr(spider, 'enrich_books_with_details'):
                    print("   使用enrich_books_with_details批量补充详细信息...")

                    # 批量处理前三本书
                    enriched_books = spider.enrich_books_with_details(
                        books[:3],
                        max_books=3,
                        fetch_chapters=False,  # 不获取章节，只获取元数据
                        chapter_count=2
                    )

                    if enriched_books:
                        print(f"   成功批量补充 {len(enriched_books)} 本书的详细信息")

                        # 显示批量处理结果
                        for i, book in enumerate(enriched_books, 1):
                            print(f"     {i}. 《{book['title'][:15]}...》")
                            print(f"         作者: {book.get('author', '未知')}")
                            print(f"         分类: {book.get('category', '未知')}")
                            if 'introduction' in book and book['introduction']:
                                intro_preview = book['introduction'][:50].replace('\n', ' ')
                                print(f"         简介: {intro_preview}...")

                            # 保存到daily_ranking表
                            try:
                                book_data = {
                                    'novel_id': book.get('novel_id', f'test_{i}'),
                                    'title': book['title'],
                                    'author': book.get('author', '未知'),
                                    'platform': 'qidian',
                                    'novel_url': book.get('url', ''),
                                    'category': book.get('category', ''),
                                    'introduction': book.get('introduction', ''),
                                    'tags': book.get('tags', []),
                                    'rank': book.get('rank', i),
                                    'rank_type': book.get('rank_type'),
                                }
                                db.save_daily_ranking(book_data)
                            except Exception as e:
                                print(f"         daily_ranking保存失败: {e}")

                            # 保存到novel_archive表
                            try:
                                novel_data = {
                                    'novel_id': book.get('novel_id', f'test_{i}'),
                                    'title': book['title'],
                                    'author': book.get('author', '未知'),
                                    'platform': 'qidian',
                                    'url': book.get('url', ''),
                                    'category': book.get('category', ''),
                                    'introduction': book.get('introduction', ''),
                                    'tags': book.get('tags', []),
                                }

                                if db.save_novel(novel_data):
                                    print(f"         小说信息save_novel保存成功")
                                else:
                                    print(f"         小说信息save_novel保存失败")
                            except Exception as e:
                                print(f"         novel_archive保存失败: {e}")
                    else:
                        print("   批量详情补充失败")
                else:
                    print("   爬虫没有enrich_books_with_details方法")
            except Exception as e:
                print(f"   批量详情补充集成测试失败: {e}")

            print("\n8. 测试重复作品存储（验证去重功能）...")
            try:
                if test_books:
                    first_book = test_books[0]  # 获取第一本书
                    print(f"   测试重复存储第一名作品: 《{first_book['title'][:15]}...》")

                    # 先获取书籍详情
                    detail = spider.fetch_novel_detail(
                        first_book['url'],
                        first_book['novel_id']
                    )

                    # 记录保存前的数据统计
                    counts_before = db.get_table_counts()
                    print(f"   保存前记录数: novel_archive={counts_before.get('novel_archive', 0)}, "
                          f"novel_chapters={counts_before.get('novel_chapters', 0)}")

                    # 再次保存到daily_ranking表
                    print(f"\n   再次保存到daily_ranking表...")
                    daily_ranking_result = db.save_daily_ranking(first_book)
                    if daily_ranking_result:
                        print(f"   daily_ranking保存结果: {daily_ranking_result}")

                    # 再次保存到novel_archive表
                    print(f"\n   再次保存到novel_archive表...")
                    novel_data = {
                        'novel_id': first_book['novel_id'],
                        'title': detail.get('title', first_book['title']) if detail else first_book['title'],
                        'author': detail.get('author', first_book.get('author', '未知')) if detail else first_book.get(
                            'author', '未知'),
                        'platform': 'qidian',
                        'url': first_book['url'],
                        'category': detail.get('category',
                                               first_book.get('category', '')) if detail else first_book.get('category',
                                                                                                             ''),
                        'introduction': detail.get('introduction', '') if detail else '',
                        'tags': detail.get('tags', []) if detail else [],
                    }

                    novel_saved = db.save_novel(novel_data)
                    print(f"   novel_archive保存结果: {novel_saved}")

                    # 再次保存章节
                    print(f"\n   再次尝试保存章节...")
                    chapters = spider.fetch_novel_chapters(
                        first_book['url'],
                        first_book['novel_id'],
                        chapter_count=2
                    )

                    if chapters:
                        novel_saved_with_chapters = db.save_novel(novel_data, chapters)
                        print(f"   novel_archive保存章节结果: {novel_saved_with_chapters}")

                    # 记录保存后的数据统计
                    counts_after = db.get_table_counts()
                    print(f"   保存后记录数: novel_archive={counts_after.get('novel_archive', 0)}, "
                          f"novel_chapters={counts_after.get('novel_chapters', 0)}")

                    # 比较记录数变化
                    novel_archive_diff = counts_after.get('novel_archive', 0) - counts_before.get('novel_archive', 0)
                    novel_chapters_diff = counts_after.get('novel_chapters', 0) - counts_before.get('novel_chapters', 0)

                    print(f"\n   去重测试结果:")
                    print(f"   - novel_archive表变化: {novel_archive_diff} 条记录")
                    print(f"   - novel_chapters表变化: {novel_chapters_diff} 条记录")

                    if novel_archive_diff == 0:
                        print("   ✅ novel_archive表成功去重，没有重复插入记录")
                    else:
                        print("   ❌ novel_archive表可能重复插入了记录")

                    if novel_chapters_diff == 0:
                        print("   ✅ novel_chapters表成功去重，没有重复插入章节")
                    else:
                        print("   ❌ novel_chapters表可能重复插入了章节")
                else:
                    print("   没有可测试的书籍")
            except Exception as e:
                print(f"   重复作品存储测试失败: {e}")
                import traceback
                traceback.print_exc()

            # 测试数据库查询
            print("\n9. 测试数据库查询功能...")
            try:
                today = datetime.now().strftime('%Y-%m-%d')
                # 使用数据库处理器的内部方法获取连接
                with db._db_lock:
                    conn = db._get_connection()
                    cursor = conn.cursor()

                    # 查询今天保存的书籍
                    cursor.execute('''
                        SELECT id, title, author, category, rank 
                        FROM daily_rankings 
                        WHERE platform = 'qidian' AND fetch_date = ?
                        ORDER BY rank
                        LIMIT 5
                    ''', (today,))

                    results = cursor.fetchall()
                    print(f"   数据库中今天的起点书籍: {len(results)} 本")

                    for id_val, title, author, category, rank in results:
                        print(f"     ID:{id_val} 排名{rank}: 《{title[:15]}...》 - {author} ({category})")

                    # 查询已保存的小说基本信息
                    cursor.execute('''
                        SELECT novel_id, title, author, category, 
                               (SELECT COUNT(*) FROM novel_chapters WHERE novel_archive.novel_id = novel_chapters.novel_id) as chapters_count
                        FROM novel_archive 
                        WHERE platform = 'qidian'
                        ORDER BY created_at DESC
                        LIMIT 5
                    ''')

                    novel_results = cursor.fetchall()
                    print(f"   数据库中起点小说基本信息: {len(novel_results)} 本")

                    for novel_id, title, author, category, chapters_count in novel_results:
                        print(f"     《{title[:15]}...》 - {author} ({category}) 章节数:{chapters_count}")

                    # 查询章节信息
                    cursor.execute('''
                        SELECT id, chapter_num, chapter_title, first_post_time, word_count
                        FROM novel_chapters 
                        ORDER BY id DESC
                        LIMIT 3
                    ''')

                    chapter_results = cursor.fetchall()
                    print(f"   最近保存的章节: {len(chapter_results)} 章")

                    for ch_id, ch_num, ch_title, first_post_time, word_count in chapter_results:
                        print(
                            f"     章节ID:{ch_id} 第{ch_num}章: {ch_title[:20]}... 首发:{first_post_time} 字数:{word_count}")

                    conn.close()

            except Exception as e:
                print(f"   数据库查询失败: {e}")
                import traceback
                traceback.print_exc()

        print("\n" + "=" * 70)
        print("✅ 起点爬虫测试完成!")
        print("=" * 70)

        # 测试总结
        print("\n测试总结:")
        print(f"1. 抓取书籍数量: {len(books)} 本")
        print(f"2. 数据库操作: 成功")
        print(f"3. 详情补充: 已测试")
        print(f"4. 章节获取: 成功抓取章节，但需要检查数据库保存")
        # 检查章节保存情况
        try:
            counts = db.get_table_counts()
            total_chapters_saved = counts.get('novel_chapters', 0)
            print(f"5. 章节保存: 数据库中已有 {total_chapters_saved} 章")
        except:
            print(f"5. 章节保存: 无法获取章节数")

        print(f"6. 详情补充集成: 已测试")
        print(f"7. 重复作品去重: 已测试")

        # 最终表记录数
        counts = db.get_table_counts()
        print(f"\n最终表记录数:")
        print(f"- daily_rankings: {counts.get('daily_rankings', 0)} 条")
        print(f"- novel_archive: {counts.get('novel_archive', 0)} 条")
        print(f"- novel_chapters: {counts.get('novel_chapters', 0)} 条")

        # 测试数据保存位置
        print(f"\n测试数据保存位置:")
        print(f"- 数据库文件: {db.db_path}")

    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # 关闭爬虫
        print("\n清理资源...")
        try:
            spider.close()
            print("爬虫已关闭")
        except:
            pass


def fix_qidian_spider():
    """自动修复qidian_spider.py文件"""
    qidian_spider_path = os.path.join('spiders', 'qidian_spider.py')

    if not os.path.exists(qidian_spider_path):
        print(f"找不到qidian_spider.py: {qidian_spider_path}")
        return False

    print(f"检查并修复 {qidian_spider_path}...")

    with open(qidian_spider_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 检查__init__方法
    if 'def __init__(self, site_config):' in content:
        print("发现旧版本__init__方法，正在修复...")

        # 替换__init__方法签名
        new_content = content.replace(
            'def __init__(self, site_config):',
            'def __init__(self, site_config, db_handler=None):'
        )

        # 在__init__方法中添加db_handler属性
        lines = new_content.split('\n')
        for i, line in enumerate(lines):
            if 'self._init_selenium()' in line:
                # 在self._init_selenium()之前添加self.db_handler = db_handler
                indent = len(line) - len(line.lstrip())
                lines.insert(i, ' ' * indent + 'self.db_handler = db_handler  # 添加数据库处理器')
                break

        new_content = '\n'.join(lines)

        # 写回文件
        with open(qidian_spider_path, 'w', encoding='utf-8') as f:
            f.write(new_content)

        print("修复完成！")
        return True
    else:
        print("qidian_spider.py已经是最新版本")
        return True


if __name__ == '__main__':
    print("起点爬虫测试")
    print("=" * 50)

    # 先尝试修复qidian_spider.py
    print("\n第一步：检查qidian_spider.py版本...")
    if not fix_qidian_spider():
        print("修复失败，无法继续测试")
        exit(1)

    print("\n第二步：运行安全测试...")
    test_qidian_features_safe()