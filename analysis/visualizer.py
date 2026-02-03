# analysis/visualizer.py

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd
import numpy as np
from datetime import datetime
import seaborn as sns
from wordcloud import WordCloud
import json
import os

# 设置中文字体（需要根据你的系统调整）
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


class DataVisualizer:
    def __init__(self, output_dir='outputs/reports/visualizations'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def create_daily_report(self, trends_report, date_str=None):
        """创建每日报告可视化"""
        if not date_str:
            date_str = datetime.now().strftime('%Y%m%d')

        visuals = {}

        # 1. 热门标签饼图
        tag_fig = self._create_tag_pie_chart(trends_report, date_str)
        visuals['tag_pie_chart'] = tag_fig

        # 2. 平台对比柱状图
        platform_fig = self._create_platform_comparison(trends_report, date_str)
        visuals['platform_comparison'] = platform_fig

        # 3. 排名变动图
        if any('rising_stars' in p for p in trends_report.get('platforms', {}).values()):
            rank_fig = self._create_rank_change_chart(trends_report, date_str)
            visuals['rank_change_chart'] = rank_fig

        # 4. 标签词云
        wordcloud_fig = self._create_tag_wordcloud(trends_report, date_str)
        visuals['tag_wordcloud'] = wordcloud_fig

        return visuals

    def _create_tag_pie_chart(self, trends_report, date_str):
        """创建标签饼图"""
        fig, axes = plt.subplots(1, 2, figsize=(15, 7))

        for idx, (platform, data) in enumerate(trends_report.get('platforms', {}).items()):
            if idx >= 2:
                break

            tags = data.get('top_tags', [])[:8]  # 取前8个标签
            if tags:
                tag_names = [t['tag'] for t in tags]
                tag_counts = [t['count'] for t in tags]

                # 计算百分比
                total = sum(tag_counts)
                percentages = [count / total * 100 for count in tag_counts]

                # 创建饼图
                wedges, texts, autotexts = axes[idx].pie(
                    tag_counts,
                    labels=tag_names,
                    autopct='%1.1f%%',
                    startangle=90,
                    textprops={'fontsize': 10}
                )

                axes[idx].set_title(f'{data.get("platform", platform)} - 热门标签分布', fontsize=14)

                # 美化文本
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontsize(9)

        plt.tight_layout()
        filename = f'{self.output_dir}/tag_pie_{date_str}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        plt.close()

        return filename

    def _create_platform_comparison(self, trends_report, date_str):
        """创建平台对比柱状图"""
        platforms_data = trends_report.get('platforms', {})

        if len(platforms_data) < 2:
            return None

        # 提取数据
        platform_names = []
        tag_diversities = []
        avg_tag_counts = []

        for platform, data in platforms_data.items():
            platform_names.append(data.get('platform', platform))

            # 计算标签多样性
            diversity = 0
            total_tags = 0
            tag_count = 0

            for rank_type, stats in data.get('rank_types', {}).items():
                diversity += stats.get('tag_diversity', 0)
                for tag_info in stats.get('top_tags', []):
                    total_tags += tag_info.get('count', 0)
                    tag_count += 1

            avg_tags = total_tags / tag_count if tag_count > 0 else 0

            tag_diversities.append(diversity)
            avg_tag_counts.append(avg_tags)

        # 创建柱状图
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

        x = np.arange(len(platform_names))
        width = 0.35

        # 标签多样性
        ax1.bar(x, tag_diversities, width, label='标签多样性', color='skyblue')
        ax1.set_ylabel('标签数量', fontsize=12)
        ax1.set_title('各平台标签多样性对比', fontsize=14)
        ax1.set_xticks(x)
        ax1.set_xticklabels(platform_names)
        ax1.legend()

        # 平均标签数
        ax2.bar(x, avg_tag_counts, width, label='平均标签数/书', color='lightcoral')
        ax2.set_ylabel('平均数量', fontsize=12)
        ax2.set_title('各平台平均标签数量对比', fontsize=14)
        ax2.set_xticks(x)
        ax2.set_xticklabels(platform_names)
        ax2.legend()

        plt.tight_layout()
        filename = f'{self.output_dir}/platform_comparison_{date_str}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        plt.close()

        return filename

    def _create_rank_change_chart(self, trends_report, date_str):
        """创建排名变动图"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        for idx, (platform, data) in enumerate(trends_report.get('platforms', {}).items()):
            if idx >= 2:
                break

            rising_stars = data.get('rising_stars', [])[:5]
            falling_stars = data.get('falling_stars', [])[:5]

            if not rising_stars and not falling_stars:
                continue

            # 准备数据
            all_stars = rising_stars + falling_stars
            titles = [star.get('title', '未知')[:10] + '...' for star in all_stars]
            changes = [star.get('rank_change', 0) for star in all_stars]
            colors = ['green' if change > 0 else 'red' for change in changes]

            # 创建水平条形图
            y_pos = np.arange(len(all_stars))
            axes[idx].barh(y_pos, changes, color=colors)
            axes[idx].set_yticks(y_pos)
            axes[idx].set_yticklabels(titles, fontsize=9)
            axes[idx].set_xlabel('排名变化（正数=上升）', fontsize=11)
            axes[idx].set_title(f'{platform} - 排名变动最大的作品', fontsize=13)

            # 添加数值标签
            for i, v in enumerate(changes):
                axes[idx].text(v + (0.5 if v > 0 else -0.5), i, str(v),
                               color='black', va='center', fontsize=9)

        plt.tight_layout()
        filename = f'{self.output_dir}/rank_change_{date_str}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        plt.close()

        return filename

    def _create_tag_wordcloud(self, trends_report, date_str):
        """创建标签词云"""
        # 收集所有标签
        all_tags = []
        tag_weights = {}

        for platform, data in trends_report.get('platforms', {}).items():
            for tag_info in data.get('top_tags', []):
                tag = tag_info['tag']
                count = tag_info['count']
                all_tags.extend([tag] * count)
                tag_weights[tag] = tag_weights.get(tag, 0) + count

        if not all_tags:
            return None

        # 创建词云
        wordcloud = WordCloud(
            font_path=self._get_chinese_font(),
            width=800,
            height=400,
            background_color='white',
            max_words=50,
            contour_width=1,
            contour_color='steelblue'
        ).generate_from_frequencies(tag_weights)

        # 绘制词云
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('热门题材标签词云', fontsize=16)

        filename = f'{self.output_dir}/wordcloud_{date_str}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        plt.close()

        return filename

    def _get_chinese_font(self):
        """获取中文字体路径"""
        # 尝试多种常见的中文字体
        font_candidates = [
            'SimHei.ttf',
            'msyh.ttc',  # 微软雅黑
            'Arial Unicode.ttf',
            '/System/Library/Fonts/PingFang.ttc',  # macOS
            '/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf'  # Linux
        ]

        for font in font_candidates:
            if os.path.exists(font):
                return font

        # 如果找不到，使用默认字体
        return None

    def create_monthly_trend_chart(self, monthly_report, year_month):
        """创建月度趋势图表"""
        filename = f'{self.output_dir}/monthly_trend_{year_month}.png'

        # 这里可以添加更复杂的月度趋势可视化
        # 例如：标签趋势线图、热度变化图等

        return filename