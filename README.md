# 📚 WebNovel Trends - 小说热点分析系统

## LLM Prompts
### 1. 项目简介
一个每日自动爬取起点中文网、番茄小说榜单数据的系统，为作者提供市场趋势参考，非商业用途，并且总结每日、每月以及每季度的热点题材分布。
### 2. Tech Stack
 - Language: Python
 - Database: SQLite
### 3. Structure
```text
webnovel_trends/
├── outputs/
│   ├── data
│   ├── logs
├── config.py                # 配置文件
├── spiders/
│   ├── base_spider.py       # 爬虫基类
│   ├── qidian_spider.py     # 起点爬虫
│   ├── fanqie_spider.py     # 番茄爬虫
│   ├── fanqie_font_decoder  # 番茄解码
│   ├── request_handler      # Selenium 请求
├── database/
│   └── db_handler.py        # 数据库操作
├── analysis/
│   ├── trend_analyzer.py    # 趋势分析器
│   └── visualizer.py        # 可视化模块
├── tasks/
│   └── scheduler.py         # 任务调度器
├── outputs/
│   ├── logs/                # 日志文件
│   ├── data/                # 数据存储
│   └── reports/             # 分析报告
├── requirements.txt         # 依赖列表
├── main.py                  # 主程序入口
└── README.md                # 项目说明
```


### 1. 安装依赖

```bash
pip install -r requirements.txt