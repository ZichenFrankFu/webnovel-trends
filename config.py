import os

# Root Directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WEBSITES = {
    'qidian': {
        'name': '起点中文网',
        'rank_urls': {
            '月票榜': 'https://www.qidian.com/rank/yuepiao/page{page}/',          # 月票榜
            '畅销榜': 'https://www.qidian.com/rank/hotsales/page{page}/',        # 畅销榜
            '阅读指数榜': 'https://www.qidian.com/rank/readIndex/page{page}/',      # 阅读指数榜
            '推荐榜': 'https://www.qidian.com/rank/recom/page{page}/',              # 推荐榜
            '收藏榜': 'https://www.qidian.com/rank/collect/page{page}/',          # 收藏榜
            '签约作者新书榜': 'https://www.qidian.com/rank/signnewbook/page{page}/',  # 签约作者新书榜
            '公众作者新书榜': 'https://www.qidian.com/rank/pubnewbook/page{page}/',    # 公众作者新书榜
            '新人签约新书榜': 'https://www.qidian.com/rank/newsign/page{page}/',          # 新人签约新书榜
            '新人作者新书榜': 'https://www.qidian.com/rank/newauthor/page{page}/',      # 新人作者新书榜
        },
        "rank_type_map": {
            # 常规榜单
            "月票榜": {"rank_family": "月票榜", "rank_sub_cat": ""},
            "畅销榜": {"rank_family": "畅销榜", "rank_sub_cat": ""},
            "阅读指数榜": {"rank_family": "阅读指数榜", "rank_sub_cat": ""},
            "推荐榜": {"rank_family": "推荐榜", "rank_sub_cat": ""},
            "收藏榜": {"rank_family": "收藏榜", "rank_sub_cat": ""},

            # 新书榜
            "签约作者新书榜": {"rank_family": "新书榜", "rank_sub_cat": "签约作者"},
            "公众作者新书榜": {"rank_family": "新书榜", "rank_sub_cat": "公众作者"},
            "新人签约新书榜": {"rank_family": "新书榜", "rank_sub_cat": "新人签约"},
            "新人作者新书榜": {"rank_family": "新书榜", "rank_sub_cat": "新人作者"},
        },
        'base_url': 'https://www.qidian.com',
        'novel_types': ['玄幻','奇幻',
                        '武侠','仙侠',
                        '都市','现实',
                        '军事','历史',
                        '游戏','体育',
                        '科幻','诸天无限',
                        '悬疑','轻小说','短篇'],
        'sub_to_main_map' : {
            # 玄幻相关
            "东方玄幻": "玄幻",
            "异世大陆": "玄幻",
            "高武世界": "玄幻",
            "王朝争霸": "玄幻",

            # 奇幻相关
            "剑与魔法": "奇幻",
            "史诗奇幻": "奇幻",
            "神秘幻想": "奇幻",
            "现代魔法": "奇幻",
            "历史神话": "奇幻",
            "另类幻想": "奇幻",

            # 武侠相关
            "传统武侠": "武侠",
            "武侠幻想": "武侠",
            "国术无双": "武侠",
            "古武未来": "武侠",
            "武侠同人": "武侠",

            # 仙侠相关
            "修真文明": "仙侠",
            "幻想修仙": "仙侠",
            "现代修真": "仙侠",
            "神话修真": "仙侠",
            "古典仙侠": "仙侠",

            # 都市相关
            "都市生活": "都市",
            "娱乐明星": "都市",
            "商战职场": "都市",
            "异术超能": "都市",
            "都市异能": "都市",
            "青春校园": "都市",

            # 历史相关
            "架空历史": "历史",
            "两宋元明": "历史",
            "外国历史": "历史",
            "上古先秦": "历史",
            "秦汉三国": "历史",
            "两晋隋唐": "历史",
            "五代十国": "历史",
            "清史民国": "历史",
            "历史传记": "历史",
            "民间传说": "历史",

            # 军事相关
            "战争幻想": "军事",
            "谍战特工": "军事",
            "军旅生涯": "军事",
            "抗战烽火": "军事",
            "军事战争": "军事",

            # 悬疑相关
            "悬疑侦探": "悬疑",
            "诡秘悬疑": "悬疑",
            "探险生存": "悬疑",
            "奇妙世界": "悬疑",
            "古今传奇": "悬疑",

            # 科幻相关
            "星际文明": "科幻",
            "时空穿梭": "科幻",
            "未来世界": "科幻",
            "古武机甲": "科幻",
            "超级科技": "科幻",
            "进化变异": "科幻",
            "末世危机": "科幻",

            # 游戏相关
            "电子竞技": "游戏",
            "虚拟网游": "游戏",
            "游戏异界": "游戏",
            "游戏系统": "游戏",
            "游戏主播": "游戏",

            # 体育相关
            "体育赛事": "体育",
            "篮球运动": "体育",
            "足球运动": "体育",

            # 轻小说相关
            "原生幻想": "轻小说",
            "衍生同人": "轻小说",
            "搞笑吐槽": "轻小说",
            "恋爱日常": "轻小说",
        },
        'pages_per_rank': 1,
        'chapter_extraction_goal': 5,
        'max_log_chapters': 5,
        'selenium_specific': {
            'method': 'dynamic',
        },
        # Rank 页是主来源的字段
        "rank_fields_primary": [
            "title",
            "author",
            "intro",
            "main_category",
            "tags",
        ],
        # Detail 页是主来源的字段
        "detail_fields_primary": [
            "status",
            "total_words",
            "total_recommend",
        ],
        # Detail 页 fallback 触发规则
        "detail_fallback_rules": {
            "title": {"when_empty": True,},
            "author": {"when_empty": True,},
            "intro": {"when_empty": True,
                      "min_len": 5,},
            "main_category": {"when_unknown": True,},
            "tags": {"when_empty": True,},
        },


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
        'selenium_specific': {
            'method': 'scroll_load',   # 滚动加载方式
            'target_count': 30,        # 目标加载数量
            'scroll_delay': 2,         # 滚动延迟
            'max_scroll_attempts': 10, # 最大滚动尝试次数
        },
        'chapter_extraction_goal': 5,
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
    "enabled": True,
    'browser': 'chrome',

    # 浏览器选项
    'options': {
        'headless': True,           # 不直接展示browser
        'no_sandbox': True,
        'disable_dev_shm_usage': True,
        'disable_gpu': True,
        'window_size': '1920,1080',
        'disable_blink_features': 'AutomationControlled',
        'user_agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
    },

    # 实验性选项
    'experimental_options': {
        'excludeSwitches': ['enable-automation'],
        'useAutomationExtension': False,
    },

    # 超时设置
    'timeout': 15,
    'implicit_wait': 10,
    'page_load_timeout': 30,
    'script_timeout': 10,

    # 重连设置
    "retry": {
        "enabled": True,
        "max_retries": 3,
        "backoff_seconds": 2,
    },

    # 性能优化设置
    "prefs": {
        "profile.default_content_setting_values": {
            "images": 2,
            "stylesheet": 2,
            "javascript": 1,
        }
    },

    # 反爬设置
    "stealth": {
        "enabled": True,
        "disable_blink_features": "AutomationControlled",
        "excludeSwitches": ["enable-automation"],
        "useAutomationExtension": False,
        "webdriver_undefined_script": (
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        ),
    },
}

# 爬虫通用配置
CRAWLER_CONFIG = {
    'max_retries': 4,
    'retry_delay': 2,
    'use_proxy': False,
    'proxy_pool': [],
    'log_level': 'INFO',
    'cache_enabled': True,
    'cache_expiry': 3600,

    # 通用页面爬虫配置
    'page_fetch': {
        'max_page_retries': 3,  # 页面获取最大重试次数
        'page_retry_delay': 2,  # 页面重试延迟(秒)
        'default_wait_sec': 12,  # 默认等待时间
        'post_load_delay_range': {0.8, 1.6},
        'min_html_length': 1000,
        'bad_title_keywords': {"404", "无法访问", "出错了"},
    },

    # 章节抓取配置
    'chapter_fetch': {
        'max_retries': 3,
        'delay_between_chapters': (1.0, 3.0),
        'wait_for_content_sec': 15,
    },

}