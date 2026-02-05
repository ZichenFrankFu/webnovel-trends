# main.py

import argparse
import os
import sys
from datetime import datetime
import config
from tasks.scheduler import TaskScheduler
from spiders.qidian_spider import QidianSpider
from spiders.fanqie_spider import FanqieSpider
from database.db_handler import DatabaseHandler

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def run_once():
    # 初始化数据库
    db_path = config.DATABASE['path']
    db = DatabaseHandler(db_path)
    print(f"数据库已初始化: {db_path}")

    # 初始化爬虫
    print("\n初始化爬虫...")

    # 起点爬虫
    qidian_config = config.WEBSITES['qidian']
    qidian_spider = QidianSpider(qidian_config)
    print(f"✓ 起点爬虫初始化完成")

    # 番茄爬虫
    fanqie_config = config.WEBSITES['fanqie']
    fanqie_spider = FanqieSpider(fanqie_config)
    print(f"✓ 番茄爬虫初始化完成")

    # 抓取所有榜单
    print("\n开始抓取榜单数据...")

    # 抓取起点数据
    print("\n抓取起点榜单...")
    try:
        qidian_books = qidian_spider.fetch_whole_rank()
        print(f"起点抓取完成: {len(qidian_books)} 本书")

        # 补充详情并抓取章节
        print("补充详情信息并抓取章节...")
        enriched_qidian_books = qidian_spider.enrich_books_with_details(
            qidian_books,
            max_books=50,  # 处理前50本书
            fetch_chapters=True,  # 抓取章节
            chapters_per_book=5   # 每本书抓取5章
        )

        # 保存到数据库
        saved_count = 0
        chapter_count = 0
        for book in enriched_qidian_books:
            # 保存书籍信息
            if db.save_daily_ranking(book):
                saved_count += 1

            # 保存章节内容
            if 'chapters' in book:
                for chapter in book['chapters']:
                    if db.save_chapter_content(chapter):
                        chapter_count += 1

        print(f"起点数据保存完成: {saved_count} 本书, {chapter_count} 个章节")

    except Exception as e:
        print(f"起点数据抓取失败: {e}")

    # 抓取番茄数据
    print("\n抓取番茄榜单...")
    try:
        fanqie_books = fanqie_spider.fetch_whole_rank()
        print(f"番茄抓取完成: {len(fanqie_books)} 本书")

        # 补充详情并抓取章节
        print("补充详情信息并抓取章节...")
        enriched_fanqie_books = fanqie_spider.enrich_books_with_details(
            fanqie_books,
            max_books=50,  # 处理前50本书
            fetch_chapters=True,  # 抓取章节
            chapters_per_book=5   # 每本书抓取5章
        )

        # 保存到数据库
        saved_count = 0
        chapter_count = 0
        for book in enriched_fanqie_books:
            # 保存书籍信息
            if db.save_daily_ranking(book):
                saved_count += 1

            # 保存章节内容
            if 'chapters' in book:
                for chapter in book['chapters']:
                    if db.save_chapter_content(chapter):
                        chapter_count += 1

        print(f"番茄数据保存完成: {saved_count} 本书, {chapter_count} 个章节")

    except Exception as e:
        print(f"番茄数据抓取失败: {e}")

    # 关闭爬虫
    print("\n关闭爬虫...")
    qidian_spider.close()
    fanqie_spider.close()
    print("爬虫已关闭")

    # 统计数据
    print("\n" + "=" * 60)
    print("数据抓取完成")
    print("=" * 60)

    # 获取今日数据统计
    today = datetime.now().strftime('%Y-%m-%d')
    print(f"\n今日数据统计 ({today}):")

    # 起点数据
    qidian_books_today = db.get_today_rankings(platform='qidian')
    print(f"起点中文网: {len(qidian_books_today)} 本书")

    # 番茄数据
    fanqie_books_today = db.get_today_rankings(platform='fanqie')
    print(f"番茄小说: {len(fanqie_books_today)} 本书")

    print("\n抓取完成!")




def run_scheduler():
    """启动定时任务调度器"""
    print("启动定时任务调度器...")
    scheduler = TaskScheduler()
    scheduler.start()


def main():
    parser = argparse.ArgumentParser(description='WebNovel Trends - 小说热点分析系统')
    parser.add_argument('mode', choices=['once', 'scheduler', 'tests', 'analyze'],
                        help='运行模式: once(单次运行), scheduler(定时任务), tests(测试), analyze(仅分析)')

    args = parser.parse_args()

    print("WebNovel Trends 小说热点分析系统")
    print("版本: 1.0.0")
    print("当前时间:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 60)

    if args.mode == 'once':
        run_once()
    elif args.mode == 'scheduler':
        run_scheduler()
    elif args.mode == 'analyze':
        pass

if __name__ == '__main__':
    main()