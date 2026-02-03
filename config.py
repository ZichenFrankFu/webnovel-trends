import os

# Root Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WEBSITES = {
    'qidian': {
        'name': '起点中文网',
        'rank_urls': {
            'recom': 'https://www.qidian.com/rank/recom/page{page}/',              # 推荐榜
            'hotsales': 'https://www.qidian.com/rank/hotsales/page{page}/',        # 畅销榜
            'signnewbook': 'https://www.qidian.com/rank/signnewbook/page{page}/',  # 签约作者新书榜
            'pubnewbook': 'https://www.qidian.com/rank/pubnewbook/page{page}/',    # 公众作者新书榜
            'newsign': 'https://www.qidian.com/rank/newsign/page{page}/',          # 新人签约新书榜
            'newauthor': 'https://www.qidian.com/rank/newauthor/page{page}/',      # 新人作者新书榜
        },
        'base_url': 'https://www.qidian.com',
        'request_delay': 5,
        'pages_per_rank': 1,
        'chapter_extraction_goal':5,
        'selenium_specific': {
            'method': 'dynamic',  # 动态加载方式
        }
    },
    'fanqie': {
        'name': '番茄小说',
        'rank_urls': {
            'scifi_apocalypse': 'https://fanqienovel.com/rank/1_2_8',      # 科幻末世
            'urban_martial': 'https://fanqienovel.com/rank/1_2_1014',      # 都市高武
            'suspense_brainhole': 'https://fanqienovel.com/rank/1_2_539',  # 悬疑脑洞
            'urban_brainhole': 'https://fanqienovel.com/rank/1_2_262',     # 都市脑洞
            'anime_derivation': 'https://fanqienovel.com/rank/1_2_718',    # 动漫衍生
        },
        'base_url': 'https://fanqienovel.com',
        'request_delay': 3,
        'selenium_specific': {
            'method': 'scroll_load',  # 滚动加载方式
            'target_count': 30,        # 目标加载数量
            'scroll_delay': 2,         # 滚动延迟
            'max_scroll_attempts': 10, # 最大滚动尝试次数
        }
    }
}

DATABASE = {
    'path': os.path.join(BASE_DIR, 'outputs', 'data', 'novels.db'),
    'tables': {
        'daily_rank': 'daily_rankings',
        'novel_info': 'novel_infos',
        'trend_stats': 'trend_statistics'
    }
}

OUTPUT_PATHS = {
    'data': os.path.join(BASE_DIR, 'outputs', 'data'),
    'logs': os.path.join(BASE_DIR, 'outputs', 'logs'),
    'reports': os.path.join(BASE_DIR, 'outputs', 'reports'),
    'visualizations': os.path.join(BASE_DIR, 'outputs', 'reports', 'visualizations'),
    'screenshots': os.path.join(BASE_DIR, 'outputs', 'screenshots'),
}

for path in OUTPUT_PATHS.values():
    os.makedirs(path, exist_ok=True)

ANALYSIS = {
    'top_n': 50,
    'history_days': 30,
    'trend_window': 7,
}

# Selenium全局配置
SELENIUM_CONFIG = {
    'browser': 'chrome',  # 浏览器类型：chrome, firefox, edge

    # 浏览器选项
    'options': {
        'headless': True,  # 无头模式
        'no_sandbox': True,
        'disable_dev_shm_usage': True,
        'disable_gpu': True,
        'window_size': '1920,1080',
        'disable_blink_features': 'AutomationControlled',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    },

    # 实验性选项
    'experimental_options': {
        'excludeSwitches': ['enable-automation'],
        'useAutomationExtension': False,
    },

    # 超时设置
    'timeout': 15,  # 显式等待超时(秒)
    'implicit_wait': 10,  # 隐式等待时间(秒)
    'page_load_timeout': 30,  # 页面加载超时时间(秒)
    'script_timeout': 10,  # 脚本执行超时时间(秒)

    # 其他配置
    'stealth_mode': True,  # 启用反检测
    'save_screenshots': False,  # 是否保存截图
    'screenshot_on_error': True,  # 出错时保存截图
    'driver_path': None,  # ChromeDriver路径，None表示自动查找
}

# 爬虫通用配置
CRAWLER_CONFIG = {
    'max_retries': 3,
    'retry_delay': 2,
    'use_proxy': False,
    'proxy_pool': [],
    'log_level': 'INFO',
    'cache_enabled': True,
    'cache_expiry': 3600,  # 缓存过期时间(秒)
}