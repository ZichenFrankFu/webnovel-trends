# tasks/scheduler.py

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import time
from datetime import datetime
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import WEBSITES, DATABASE, OUTPUT_PATHS
from spiders.qidian_spider import QidianSpider
from spiders.fanqie_spider import FanqieSpider
from database.db_handler import DatabaseHandler
from analysis.trend_analyzer import TrendAnalyzer
from analysis.visualizer import DataVisualizer


class TaskScheduler:
    def __init__(self):
        self.setup_logging()
        self.db_handler = DatabaseHandler(DATABASE['path'])
        self.scheduler = BlockingScheduler()

    def setup_logging(self):
        """设置日志"""
        log_file = os.path.join(OUTPUT_PATHS['logs'], 'scheduler.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

        self.logger = logging.getLogger('TaskScheduler')

    def daily_crawl_task(self):
        """每日爬取任务"""
        self.logger.info("开始执行每日爬取任务")
        start_time = time.time()

        try:
            # 1. 爬取起点数据
            self.logger.info("爬取起点中文网...")
            qidian_spider = QidianSpider(WEBSITES['qidian'])
            qidian_books = qidian_spider.fetch_all_ranks()

            # 为前20本书补充详情
            enriched_qidian = qidian_spider.enrich_books_with_details(qidian_books, max_books=20)

            # 保存到数据库
            for book in enriched_qidian:
                self.db_handler.save_daily_ranking(book)

            self.logger.info(f"起点数据爬取完成: {len(enriched_qidian)} 本书")

            # 2. 爬取番茄数据
            self.logger.info("爬取番茄小说...")
            fanqie_spider = FanqieSpider(WEBSITES['fanqie'])
            fanqie_books = fanqie_spider.fetch_all_ranks()

            # 为前15本书补充详情
            enriched_fanqie = fanqie_spider.enrich_books_with_details(fanqie_books, max_books=15)

            # 保存到数据库
            for book in enriched_fanqie:
                self.db_handler.save_daily_ranking(book)

            self.logger.info(f"番茄数据爬取完成: {len(enriched_fanqie)} 本书")

            # 3. 分析数据
            self.logger.info("开始数据分析...")
            analyzer = TrendAnalyzer(self.db_handler)
            trends_report = analyzer.analyze_daily_trends()

            # 4. 生成可视化
            self.logger.info("生成可视化报告...")
            visualizer = DataVisualizer()
            date_str = datetime.now().strftime('%Y%m%d')
            visuals = visualizer.create_daily_report(trends_report, date_str)

            # 5. 保存报告
            report_file = os.path.join(OUTPUT_PATHS['reports'], f'daily_report_{date_str}.json')
            import json
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'report_date': datetime.now().strftime('%Y-%m-%d'),
                    'summary': trends_report.get('summary', {}),
                    'platforms': trends_report.get('platforms', {}),
                    'visualizations': list(visuals.values()) if visuals else []
                }, f, ensure_ascii=False, indent=2)

            elapsed_time = time.time() - start_time
            self.logger.info(f"每日任务完成! 耗时: {elapsed_time:.2f}秒")

        except Exception as e:
            self.logger.error(f"每日任务执行失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def monthly_analysis_task(self):
        """月度分析任务（每月1号执行）"""
        self.logger.info("开始执行月度分析任务")

        try:
            analyzer = TrendAnalyzer(self.db_handler)

            # 分析上月数据
            last_month = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
            monthly_report = analyzer.analyze_monthly_trends(last_month)

            # 保存月度报告
            report_file = os.path.join(OUTPUT_PATHS['reports'], f'monthly_report_{last_month}.json')
            import json
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(monthly_report, f, ensure_ascii=False, indent=2)

            # 生成月度图表
            visualizer = DataVisualizer()
            chart_file = visualizer.create_monthly_trend_chart(monthly_report, last_month)

            self.logger.info(f"月度分析完成: {report_file}")

        except Exception as e:
            self.logger.error(f"月度任务执行失败: {e}")

    def quarterly_analysis_task(self):
        """季度分析任务"""
        self.logger.info("开始执行季度分析任务")

        try:
            # 这里可以添加季度分析逻辑
            # 例如：比较三个月的数据，识别长期趋势

            self.logger.info("季度分析完成")

        except Exception as e:
            self.logger.error(f"季度任务执行失败: {e}")

    def start(self):
        """启动调度器"""
        self.logger.info("启动任务调度器")

        # 每日凌晨2点执行爬取任务（避免高峰时段）
        self.scheduler.add_job(
            self.daily_crawl_task,
            CronTrigger(hour=2, minute=0),
            id='daily_crawl',
            name='每日爬取任务',
            replace_existing=True
        )

        # 每月1号凌晨3点执行月度分析
        self.scheduler.add_job(
            self.monthly_analysis_task,
            CronTrigger(day=1, hour=3, minute=0),
            id='monthly_analysis',
            name='月度分析任务',
            replace_existing=True
        )

        # 每季度第一天凌晨4点执行季度分析
        self.scheduler.add_job(
            self.quarterly_analysis_task,
            CronTrigger(month='1,4,7,10', day=1, hour=4, minute=0),
            id='quarterly_analysis',
            name='季度分析任务',
            replace_existing=True
        )

        self.logger.info("任务调度器已启动")
        self.logger.info("当前计划任务:")
        for job in self.scheduler.get_jobs():
            self.logger.info(f"  - {job.name}: {job.next_run_time}")

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("任务调度器已停止")