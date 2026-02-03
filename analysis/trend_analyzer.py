# analysis/trend_analyzer.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from collections import Counter
import jieba
import jieba.analyse


class TrendAnalyzer:
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.logger = logging.getLogger('TrendAnalyzer')

        # 初始化jieba
        jieba.initialize()

    def analyze_daily_trends(self, date_str=None):
        """分析每日趋势"""
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')

        yesterday = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

        trends_report = {
            'analysis_date': date_str,
            'platforms': {},
            'summary': {}
        }

        # 分析各平台
        for platform in ['qidian', 'fanqie']:
            platform_trends = self._analyze_platform_trends(date_str, yesterday, platform)
            trends_report['platforms'][platform] = platform_trends

        # 生成总结
        trends_report['summary'] = self._generate_summary(trends_report['platforms'])

        return trends_report

    def _analyze_platform_trends(self, today, yesterday, platform):
        """分析单个平台的趋势"""
        platform_data = {
            'platform': platform,
            'date': today,
            'rank_types': {},
            'top_tags': [],
            'rising_stars': [],
            'falling_stars': []
        }

        # 获取今日榜单
        today_df = self.db_handler.get_daily_ranking(today, platform=platform)

        if today_df.empty:
            self.logger.warning(f"没有找到{platform}在{today}的数据")
            return platform_data

        # 分析各榜单类型
        for rank_type in today_df['rank_type'].unique():
            rank_data = self._analyze_rank_type(today, yesterday, platform, rank_type)
            platform_data['rank_types'][rank_type] = rank_data

        # 提取热门标签
        all_tags = self._extract_tags_from_dataframe(today_df)
        top_tags = Counter(all_tags).most_common(10)
        platform_data['top_tags'] = [{'tag': tag, 'count': count} for tag, count in top_tags]

        # 计算排名变动
        if yesterday:
            rank_changes = self.db_handler.calculate_rank_change(today, yesterday, platform, 'hot', 100)
            if not rank_changes.empty:
                # 上升最快
                rising = rank_changes[rank_changes['rank_change'] > 0].nlargest(5, 'rank_change')
                platform_data['rising_stars'] = rising.to_dict('records')

                # 下降最快
                falling = rank_changes[rank_changes['rank_change'] < 0].nsmallest(5, 'rank_change')
                platform_data['falling_stars'] = falling.to_dict('records')

        return platform_data

    def _analyze_rank_type(self, today, yesterday, platform, rank_type):
        """分析特定榜单类型"""
        today_df = self.db_handler.get_daily_ranking(today, platform, rank_type)

        if today_df.empty:
            return {}

        # 标签分析
        tag_stats = self._analyze_tags(today_df)

        # 保存到数据库
        self.db_handler.save_trend_statistics(today, platform, rank_type, tag_stats)

        # 统计信息
        stats = {
            'total_books': len(today_df),
            'top_tags': [],
            'tag_diversity': len(tag_stats),
            'avg_rank_change': 0
        }

        # 热门标签
        sorted_tags = sorted(tag_stats.items(), key=lambda x: x[1][0], reverse=True)[:5]
        stats['top_tags'] = [{'tag': tag, 'count': count, 'percentage': count / total * 100}
                             for tag, (count, total) in sorted_tags]

        return stats

    def _analyze_tags(self, df):
        """分析标签统计"""
        all_tags = []

        for tags_json in df['tags'].dropna():
            try:
                tags = json.loads(tags_json)
                all_tags.extend(tags)
            except:
                continue

        tag_counter = Counter(all_tags)
        total_books = len(df)

        # 计算每个标签的统计
        tag_stats = {}
        for tag, count in tag_counter.items():
            tag_stats[tag] = (count, total_books)

        return tag_stats

    def _extract_tags_from_dataframe(self, df):
        """从DataFrame中提取所有标签"""
        all_tags = []

        for tags_json in df['tags'].dropna():
            try:
                tags = json.loads(tags_json)
                all_tags.extend(tags)
            except:
                continue

        return all_tags

    def _generate_summary(self, platforms_data):
        """生成趋势总结"""
        summary = {
            'overall_top_tags': [],
            'cross_platform_trends': [],
            'recommendations': []
        }

        # 合并所有平台的标签
        all_tags = []
        for platform, data in platforms_data.items():
            for tag_info in data.get('top_tags', []):
                all_tags.append((tag_info['tag'], tag_info['count'], platform))

        # 计算总体热门标签
        tag_counter = Counter()
        for tag, count, _ in all_tags:
            tag_counter[tag] += count

        summary['overall_top_tags'] = [{'tag': tag, 'count': count}
                                       for tag, count in tag_counter.most_common(10)]

        # 生成写作建议
        summary['recommendations'] = self._generate_writing_recommendations(tag_counter)

        return summary

    def _generate_writing_recommendations(self, tag_counter):
        """基于标签分析生成写作建议"""
        recommendations = []

        top_tags = [tag for tag, _ in tag_counter.most_common(5)]

        if '玄幻' in top_tags or '仙侠' in top_tags:
            recommendations.append({
                'type': '题材建议',
                'content': '当前玄幻/仙侠题材热度较高，可以考虑创作相关题材，注意创新世界观设定'
            })

        if '都市' in top_tags or '言情' in top_tags:
            recommendations.append({
                'type': '题材建议',
                'content': '现实题材依然受欢迎，可以考虑加入职业元素、甜宠或职场情节'
            })

        if '科幻' in top_tags:
            recommendations.append({
                'type': '题材建议',
                'content': '科幻题材有稳定受众，可以考虑软科幻或近未来设定，降低阅读门槛'
            })

        if len(top_tags) > 0:
            tag_combinations = []
            for i in range(len(top_tags)):
                for j in range(i + 1, len(top_tags)):
                    tag_combinations.append(f"{top_tags[i]}+{top_tags[j]}")

            if tag_combinations:
                recommendations.append({
                    'type': '融合建议',
                    'content': f'可以考虑题材融合，如：{", ".join(tag_combinations[:3])}等组合'
                })

        recommendations.append({
            'type': '通用建议',
            'content': '无论选择什么题材，人物塑造和故事情节是关键，建议前3章快速建立冲突'
        })

        return recommendations

    def analyze_monthly_trends(self, year_month=None):
        """分析月度趋势"""
        if not year_month:
            year_month = datetime.now().strftime('%Y-%m')

        start_date = f"{year_month}-01"

        # 计算结束日期
        year, month = map(int, year_month.split('-'))
        if month == 12:
            end_date = f"{year}-12-31"
        else:
            end_date = f"{year}-{month + 1:02d}-01"
            end_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

        monthly_report = {
            'period': year_month,
            'date_range': f"{start_date} 至 {end_date}",
            'platform_analysis': {},
            'trend_changes': []
        }

        # 获取月度数据
        for platform in ['qidian', 'fanqie']:
            trend_data = self.db_handler.get_trend_data(start_date, end_date, platform)

            if not trend_data.empty:
                platform_analysis = self._analyze_monthly_platform(trend_data, platform)
                monthly_report['platform_analysis'][platform] = platform_analysis

        # 分析趋势变化
        monthly_report['trend_changes'] = self._analyze_trend_changes(monthly_report['platform_analysis'])

        return monthly_report

    def _analyze_monthly_platform(self, trend_data, platform):
        """分析平台的月度数据"""
        analysis = {
            'platform': platform,
            'total_days': trend_data['stat_date'].nunique(),
            'tag_trends': {},
            'stability_analysis': {}
        }

        # 分析标签趋势
        pivot_data = trend_data.pivot_table(
            index='stat_date',
            columns='tag_name',
            values='tag_percentage',
            aggfunc='mean'
        ).fillna(0)

        # 计算标签的月度趋势
        if not pivot_data.empty:
            tag_trends = {}
            for tag in pivot_data.columns:
                tag_series = pivot_data[tag]
                if tag_series.mean() > 0.5:  # 至少平均占0.5%
                    trend = '上升' if tag_series.iloc[-1] > tag_series.iloc[0] else '下降'
                    volatility = tag_series.std() / tag_series.mean() if tag_series.mean() > 0 else 0

                    tag_trends[tag] = {
                        'avg_percentage': float(tag_series.mean()),
                        'trend': trend,
                        'volatility': float(volatility),
                        'peak': float(tag_series.max()),
                        'trough': float(tag_series.min())
                    }

            analysis['tag_trends'] = dict(sorted(tag_trends.items(),
                                                 key=lambda x: x[1]['avg_percentage'],
                                                 reverse=True)[:15])

        return analysis

    def _analyze_trend_changes(self, platform_analysis):
        """分析趋势变化"""
        changes = []

        # 这里可以添加更复杂的趋势变化分析逻辑
        # 例如：识别新兴标签、衰退标签等

        return changes