import os

# Root Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WEBSITES = {
    'qidian': {
        'name': '起点中文网',
        'rank_urls': {
            'yuepiao': 'https://www.qidian.com/rank/yuepiao/page{page}/',          # 月票榜
            'hotsales': 'https://www.qidian.com/rank/hotsales/page{page}/',        # 畅销榜
            'readIndex': 'https://www.qidian.com/rank/readIndex/page{page}/',      # 阅读指数榜
            'recom': 'https://www.qidian.com/rank/recom/page{page}/',              # 推荐榜
            'collect': 'https://www.qidian.com/rank/collect/page{page}/',          # 收藏榜
            'signnewbook': 'https://www.qidian.com/rank/signnewbook/page{page}/',  # 签约作者新书榜
            'pubnewbook': 'https://www.qidian.com/rank/pubnewbook/page{page}/',    # 公众作者新书榜
            'newsign': 'https://www.qidian.com/rank/newsign/page{page}/',          # 新人签约新书榜
            'newauthor': 'https://www.qidian.com/rank/newauthor/page{page}/',      # 新人作者新书榜
        },
        "rank_type_map": {
            # 常规榜单
            "yuepiao": {"rank_family": "月票榜", "rank_sub_cat": ""},
            "hotsales": {"rank_family": "畅销榜", "rank_sub_cat": ""},
            "readIndex": {"rank_family": "阅读指数榜", "rank_sub_cat": ""},
            "recom": {"rank_family": "推荐榜", "rank_sub_cat": ""},
            "collect": {"rank_family": "收藏榜", "rank_sub_cat": ""},

            # 新书榜
            "signnewbook": {"rank_family": "新书榜", "rank_sub_cat": "签约作者"},
            "pubnewbook": {"rank_family": "新书榜", "rank_sub_cat": "公众作者"},
            "newsign": {"rank_family": "新书榜", "rank_sub_cat": "新人签约"},
            "newauthor": {"rank_family": "新书榜", "rank_sub_cat": "新人作者"},
        },

        'base_url': 'https://www.qidian.com',
        'novel_types': ['玄幻','奇幻',
                        '武侠','仙侠',
                        '都市','现实',
                        '军事','历史',
                        '游戏','体育',
                        '科幻','诸天无限'
                        '悬疑','轻小说','短篇'],
        'request_delay': 2,
        'pages_per_rank': 1,
        'chapter_extraction_goal':5,
        'selenium_specific': {
            'method': 'dynamic',
        },
        'max_retries': 5,
    },
    'fanqie': {
        'name': '番茄小说',
        'rank_urls': {
            'read_western_fantasy': 'https://fanqienovel.com/rank/1_2_1141',    # 阅读榜·西方奇幻
            'read_scifi_apocalypse': 'https://fanqienovel.com/rank/1_2_8',      # 阅读榜·科幻末世
            'read_urban_highmartial': 'https://fanqienovel.com/rank/1_2_1014',  # 阅读榜·都市高武
            'read_suspense_brainhole': 'https://fanqienovel.com/rank/1_2_539',  # 阅读榜·悬疑脑洞
            'read_urban_brainhole': 'https://fanqienovel.com/rank/1_2_262',     # 阅读榜·都市脑洞
            'read_anime': 'https://fanqienovel.com/rank/1_2_718',               # 阅读榜·动漫衍生
            'new_western_fantasy': 'https://fanqienovel.com/rank/1_1_1141',     # 新书榜·西方奇幻
            'new_scifi_apocalypse': 'https://fanqienovel.com/rank/1_1_8',       # 新书榜·科幻末世
            'new_urban_highmartial': 'https://fanqienovel.com/rank/1_1_1014',   # 新书榜·都市高武
            'new_suspense_brainhole': 'https://fanqienovel.com/rank/1_1_539',   # 新书榜·悬疑脑洞
            'new_urban_brainhole': 'https://fanqienovel.com/rank/1_1_262',      # 新书榜·都市脑洞
            'new_anime': 'https://fanqienovel.com/rank/1_1_718',                # 新书榜·动漫衍生
        },
        "rank_type_map": {
            "read_western_fantasy": {"rank_family": "阅读榜", "rank_sub_cat": "西方奇幻"},
            "read_scifi_apocalypse": {"rank_family": "阅读榜", "rank_sub_cat": "科幻末世"},
            "read_urban_highmartial": {"rank_family": "阅读榜", "rank_sub_cat": "都市高武"},
            "read_suspense_brainhole": {"rank_family": "阅读榜", "rank_sub_cat": "悬疑脑洞"},
            "read_urban_brainhole": {"rank_family": "阅读榜", "rank_sub_cat": "都市脑洞"},
            "read_anime": {"rank_family": "阅读榜", "rank_sub_cat": "动漫衍生"},
            "new_western_fantasy": {"rank_family": "新书榜", "rank_sub_cat": "西方奇幻"},
            "new_scifi_apocalypse": {"rank_family": "新书榜", "rank_sub_cat": "科幻末世"},
            "new_urban_highmartial": {"rank_family": "新书榜", "rank_sub_cat": "都市高武"},
            "new_suspense_brainhole": {"rank_family": "新书榜", "rank_sub_cat": "悬疑脑洞"},
            "new_urban_brainhole": {"rank_family": "新书榜", "rank_sub_cat": "都市脑洞"},
            "new_anime": {"rank_family": "新书榜", "rank_sub_cat": "动漫衍生"},
        },
        'base_url': 'https://fanqienovel.com',
        'request_delay': 2,
        'selenium_specific': {
            'method': 'scroll_load',  # 滚动加载方式
            'target_count': 30,        # 目标加载数量
            'scroll_delay': 2,         # 滚动延迟
            'max_scroll_attempts': 10, # 最大滚动尝试次数
        },
        'max_retries': 5,
        'chapter_extraction_goal':5,
        'pages_per_rank': 1,
    }
}

DATABASE = {
    'path': os.path.join(BASE_DIR, 'outputs', 'data', 'novels.db'),
    'tables': {
        'novels': 'novels',
        'novel_titles': 'novel_titles',
        'tags': 'tags',
        'novel_tag_map': 'novel_tag_map',
        'rank_lists': 'rank_lists',
        'rank_snapshots': 'rank_snapshots',
        'rank_entries': 'rank_entries',
        'first_n_chapters': 'first_n_chapters'
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

    # 性能优化配置
    'disable_images': True,  # 禁用图片加载
    'disable_css': True,  # 禁用CSS加载
    'enable_javascript': True,  # 启用JavaScript
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

    # 通用页面爬虫配置
    'page_fetch': {
        'max_page_retries': 3,  # 页面获取最大重试次数
        'page_retry_delay': 2,  # 页面重试延迟(秒)
        'default_wait_sec': 12,  # 默认等待时间
    },

    # 章节抓取配置
    'chapter_fetch': {
        'max_retries': 3,
        'delay_between_chapters': (1.0, 3.0),
        'wait_for_content_sec': 15,
    },
}