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

    # 起点爬虫 - 注入数据库处理器
    qidian_config = config.WEBSITES['qidian']
    qidian_spider = QidianSpider(qidian_config, db_handler=db)
    print(f"✓ 起点爬虫初始化完成")

    # 番茄爬虫 - 注入数据库处理器
    fanqie_config = config.WEBSITES['fanqie']
    fanqie_spider = FanqieSpider(fanqie_config, db_handler=db)
    print(f"✓ 番茄爬虫初始化完成")

    # 抓取所有榜单
    print("\n开始抓取榜单数据...")

    # 获取配置中的榜单类型
    qidian_rank_types = list(qidian_config.get("rank_urls", {}).keys())
    fanqie_rank_types = list(fanqie_config.get("rank_urls", {}).keys())

    print(f"起点榜单类型: {qidian_rank_types}")
    print(f"番茄榜单类型: {fanqie_rank_types}")

    # 抓取起点数据
    print("\n抓取起点榜单...")
    try:
        all_qidian_results = []

        # 遍历所有榜单类型
        for rank_type in qidian_rank_types[:3]:  # 先测试前3个榜单
            print(f"处理起点榜单: {rank_type}")

            try:
                # 使用新的一站式方法抓取榜单
                result = qidian_spider.fetch_and_save_rank(
                    rank_type=rank_type,
                    pages=5,  # 每榜抓取2页（可根据需要调整）
                    enrich_detail=True,
                    enrich_chapters=True,  # 抓取章节
                    chapter_count=5,  # 每本书抓取5章
                    max_books=20,  # 每榜处理前20本书
                )

                all_qidian_results.append(result)
                print(f"  完成: {rank_type}, 处理了 {len(result.get('items', []))} 本书")

            except Exception as e:
                print(f"  处理榜单 {rank_type} 失败: {e}")
                continue

        # 统计起点数据
        total_qidian_books = 0
        total_qidian_chapters = 0
        for result in all_qidian_results:
            items = result.get('items', [])
            total_qidian_books += len(items)
            for book in items:
                chapters = book.get('first_n_chapters', [])
                total_qidian_chapters += len(chapters)

        print(f"\n起点数据统计: {total_qidian_books} 本书, {total_qidian_chapters} 个章节")

    except Exception as e:
        print(f"起点数据抓取失败: {e}")
        import traceback
        traceback.print_exc()

    # 抓取番茄数据
    print("\n抓取番茄榜单...")
    try:
        all_fanqie_results = []

        # 遍历所有榜单类型
        for rank_type in fanqie_rank_types[:2]:  # 先测试前2个榜单
            print(f"处理番茄榜单: {rank_type}")

            try:
                # 使用新的一站式方法抓取榜单
                result = fanqie_spider.fetch_and_save_rank(
                    rank_type=rank_type,
                    pages=2,  # 每榜抓取2页（可根据需要调整）
                    enrich_detail=True,
                    enrich_chapters=True,  # 抓取章节
                    chapter_count=5,  # 每本书抓取5章
                    max_books=20,  # 每榜处理前20本书
                )

                all_fanqie_results.append(result)
                print(f"  完成: {rank_type}, 处理了 {len(result.get('items', []))} 本书")

            except Exception as e:
                print(f"  处理榜单 {rank_type} 失败: {e}")
                continue

        # 统计番茄数据
        total_fanqie_books = 0
        total_fanqie_chapters = 0
        for result in all_fanqie_results:
            items = result.get('items', [])
            total_fanqie_books += len(items)
            for book in items:
                chapters = book.get('first_n_chapters', [])
                total_fanqie_chapters += len(chapters)

        print(f"\n番茄数据统计: {total_fanqie_books} 本书, {total_fanqie_chapters} 个章节")

    except Exception as e:
        print(f"番茄数据抓取失败: {e}")
        import traceback
        traceback.print_exc()

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