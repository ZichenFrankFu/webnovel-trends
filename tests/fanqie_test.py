import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_handler import DatabaseHandler
from spiders.fanqie_spider import FanqieSpider


def test_all_features():
    """测试所有新功能"""
    print("=" * 60)
    print("测试完整系统功能")
    print("=" * 60)

    # 测试数据库
    print("\n1. 测试数据库连接...")
    try:
        # 使用完整路径避免问题
        test_db_path = os.path.join(os.getcwd(), 'test_output', 'tests.db')
        db = DatabaseHandler(test_db_path)
        print(f"   数据库初始化成功: {test_db_path}")

        # 测试保存数据
        test_data = {
            'novel_id': 'test_123',
            'platform': 'tests',
            'title': '测试小说',
            'author': '测试作者',
            'rank': 1,
            'rank_type': 'hot',
            'category': '玄幻·东方玄幻',
            'tags': ['玄幻', '东方玄幻'],
            'introduction': '测试简介',
            'url': 'http://example.com',
            'fetch_date': '2026-02-03',
            'fetch_time': '12:00:00'
        }

        result = db.save_daily_ranking(test_data)
        print(f"   保存测试数据: {'成功' if result else '失败'}")

    except Exception as e:
        print(f"   数据库测试失败: {e}")
        import traceback
        traceback.print_exc()
        return

    # 测试番茄爬虫
    print("\n测试番茄爬虫...")
    try:
        fanqie_config = {
            'name': '番茄小说',
            'rank_urls': {
                'scifi_apocalypse': 'https://fanqienovel.com/rank/1_2_8',
            },
            'base_url': 'https://fanqienovel.com',
            'request_delay': 2,
        }

        print("   初始化番茄爬虫...")
        fanqie_spider = FanqieSpider(fanqie_config)

        print("   开始抓取科幻末世榜单...")
        books = fanqie_spider.fetch_rank_list('scifi_apocalypse')

        if books:
            print(f"   抓取到 {len(books)} 本书")
            print("   示例标题（已解密）:")
            for i, book in enumerate(books[:3], 1):
                print(f"     {i}. {book['title'][:20]}...")

        if hasattr(fanqie_spider, 'close'):
            fanqie_spider.close()

        print("   番茄爬虫测试完成")

    except Exception as e:
        print(f"   番茄爬虫测试失败: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 60)
    print("✅ 所有功能测试完成!")
    print("=" * 60)


if __name__ == '__main__':
    test_all_features()