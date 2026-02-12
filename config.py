import os

# Root Directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
        'pages_per_rank': 5,
        'chapter_extraction_goal': 5,
        'max_log_chapters': 5,
        'selenium_specific': {
            'method': 'dynamic',
            # 榜单页：等榜单列表结构
            "rank_wait_css": "div.rank-list, div.rank-body, ul.rank-list, li[data-rid], body",
            # 详情页：等书籍信息
            "detail_wait_css": ".info-label, .info-count-item, h1",
            # 目录页：等章节列表或章节链接
            "catalog_wait_css": ".catalog-content-wrap, .volume-wrap, .catalog-content, a[href*='/chapter/']",
            'page_fetch_overrides': {
                'page_load_sec': 10,
                'ready_state_sec': 8,
                'wait_css_sec': 8,
                'wait_css_required': True,
            },
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
            '阅读榜西方奇幻': 'https://fanqienovel.com/rank/1_2_1141',     # 阅读榜西方奇幻
            '阅读榜科幻末世': 'https://fanqienovel.com/rank/1_2_8',        # 阅读榜科幻末世
            '阅读榜都市高武': 'https://fanqienovel.com/rank/1_2_1014',     # 阅读榜都市高武
            '阅读榜悬疑脑洞': 'https://fanqienovel.com/rank/1_2_539',      # 阅读榜悬疑脑洞
            '阅读榜都市脑洞': 'https://fanqienovel.com/rank/1_2_262',      # 阅读榜都市脑洞
            '阅读榜动漫衍生': 'https://fanqienovel.com/rank/1_2_718',      # 阅读榜动漫衍生
            '新书榜西方奇幻': 'https://fanqienovel.com/rank/1_1_1141',     # 新书榜西方奇幻
            '新书榜科幻末世': 'https://fanqienovel.com/rank/1_1_8',        # 新书榜科幻末世
            '新书榜都市高武': 'https://fanqienovel.com/rank/1_1_1014',     # 新书榜都市高武
            '新书榜悬疑脑洞': 'https://fanqienovel.com/rank/1_1_539',      # 新书榜悬疑脑洞
            '新书榜都市脑洞': 'https://fanqienovel.com/rank/1_1_262',      # 新书榜都市脑洞
            '新书榜动漫衍生': 'https://fanqienovel.com/rank/1_1_718',      # 新书榜动漫衍生
        },
        "rank_type_map": {
            "阅读榜西方奇幻": {"rank_family": "阅读榜", "rank_sub_cat": "西方奇幻"},
            "阅读榜科幻末世": {"rank_family": "阅读榜", "rank_sub_cat": "科幻末世"},
            "阅读榜都市高武": {"rank_family": "阅读榜", "rank_sub_cat": "都市高武"},
            "阅读榜悬疑脑洞": {"rank_family": "阅读榜", "rank_sub_cat": "悬疑脑洞"},
            "阅读榜都市脑洞": {"rank_family": "阅读榜", "rank_sub_cat": "都市脑洞"},
            "阅读榜动漫衍生": {"rank_family": "阅读榜", "rank_sub_cat": "动漫衍生"},
            # 新书榜
            "新书榜西方奇幻": {"rank_family": "新书榜", "rank_sub_cat": "西方奇幻"},
            "新书榜科幻末世": {"rank_family": "新书榜", "rank_sub_cat": "科幻末世"},
            "新书榜都市高武": {"rank_family": "新书榜", "rank_sub_cat": "都市高武"},
            "新书榜悬疑脑洞": {"rank_family": "新书榜", "rank_sub_cat": "悬疑脑洞"},
            "新书榜都市脑洞": {"rank_family": "新书榜", "rank_sub_cat": "都市脑洞"},
            "新书榜动漫衍生": {"rank_family": "新书榜", "rank_sub_cat": "动漫衍生"},
        },
        'base_url': 'https://fanqienovel.com',
        'chapter_extraction_goal': 5,
        'pages_per_rank': 1,
        'selenium_specific': {
            'method': 'scroll_load',   # 滚动加载方式
            'target_count': 30,        # 目标加载数量
            'scroll_delay': 2,         # 滚动延迟
            'max_scroll_attempts': 10, # 最大滚动尝试次数
            "wait_css": ".rank-book-item, .book-item, a[href*='/page/'], a[href*='/book/']",
            # detail 页单独的 wait_css
            "detail_wait_css": ".info-label, .info-count-item, meta[property='og:title'], h1",
            # detail 页是否滚动（默认不滚）
            "detail_is_scrolling": False,
            "chapter_wait_css": ".muye-reader-content, .reader-content, h1.muye-reader-title, h1",
            "chapter_is_scrolling": False,
            'page_fetch_overrides': {
                'page_load_sec': 8,
                'ready_state_sec': 6,
                'wait_css_sec': 10,
                'wait_css_required': True,
                'min_html_length': 1200,
            },
            'use_proxy': False,
            'headless': False,
        },
        # Fanqie Rank 页主来源字段
        "rank_fields_primary": [
            "platform_novel_id",
            "title",
            "author",
            "intro",
            "reading_count",
            "status",
            "total_words",
        ],
        # Fanqie Detail 页主来源字段
        "detail_fields_primary": [
            "main_category",
            "tags",
        ],
        # Detail 页 fallback 触发规则（Fanqie rank 页通常缺分类/标签）
        "detail_fallback_rules": {
            "title": {"when_empty": True},
            "author": {"when_empty": True},
            "intro": {"when_empty": True},
            "main_category": {"when_empty": True},
            "tags": {"when_empty": True},
            "status": {"when_empty": True},
            "total_words": {"when_zero": True},
        },
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
        'user_agent': [
            # 现代Chrome (Windows)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",

            # 现代Chrome (Mac)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",

            # 现代Firefox
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",

            # Edge
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",

            # Safari
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",

            # 移动端
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 14; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
        ],
    },

    # 实验性选项
    'experimental_options': {
        'excludeSwitches': ['enable-automation'],
        'useAutomationExtension': False,
    },

    # 超时设置
    'timeout': 25,
    'implicit_wait': 10,
    'page_load_timeout': 30,
    'script_timeout': 10,

    # 重连设置
    "retry": {
        "enabled": True,
        "max_retries": 5,
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

    # 反爬代理
    'use_proxy': True,  # 启用代理
    'proxy_pool': [
        # === 免费代理（稳定性较差）===
        'http://103.156.17.69:8080',    # 印尼
        'http://45.182.115.148:999',    # 哥伦比亚
        'http://177.87.168.6:53281',    # 巴西
        'http://103.155.54.245:83',     # 印度
        'http://201.150.100.34:999',    # 秘鲁
        'http://45.77.56.113:3128',     # 美国
        'http://103.76.12.42:8181',     # 巴基斯坦
        'http://103.159.46.14:83',      # 菲律宾
        'http://45.225.184.177:999',    # 秘鲁
        'http://103.168.44.153:8080',   # 孟加拉
    ],

    'proxy_settings': {
        'rotate_after_failures': 2,  # 失败2次后轮换
        'test_url': 'http://httpbin.org/ip',  # 代理测试URL
        'timeout': 10,  # 代理超时
        'retry_on_proxy_fail': True,  # 代理失败重试
    },

    # 通用页面爬虫配置
    'page_fetch': {
        'max_page_retries': 5,  # 页面获取最大重试次数
        'page_retry_delay': 2,  # 页面重试延迟(秒)
        'default_wait_sec': 12,  # 默认等待时间
        'post_load_delay_range': (0.8, 1.6),
        'bad_title_keywords': ["404", "无法访问", "出错了"],
        'min_html_length': 1000,
        # 拆分不同阶段的超时，避免互相绑死
        'page_load_sec': 10,          # driver.get 的超时（建议小一点）
        'ready_state_sec': 8,         # 等 document.readyState 的超时
        'wait_css_sec': 8,            # 等 wait_css 的超时
        'stop_loading_on_timeout': True,  # page load timeout 时执行 window.stop()
        'wait_css_required': True,    # wait_css 超时是否视为失败（建议 True）
        'antibot_consecutive_threshold': 3,
        'antibot_cooldown_range': (60, 180),

    },

    # 章节抓取配置
    'chapter_fetch': {
        'max_retries': 3,
        'delay_between_chapters': (1.0, 3.0),
        'wait_for_content_sec': 15,
    },
    # 章节抓取策略
    "chapter_policy": {
        "default": 5,
        # 新书榜默认只抓前 N 章
        "new_book_chapter_count": 2,
    },
    "page_fetch_overrides": {
        # 每 N 次 driver.get 重启一次
        "restart_driver_every_n_get": 200,
        # 每个榜单跑完就重启一次
        "restart_driver_each_rank": True,
    },

}

# config.py 中添加
ANTI_BLOCK_CONFIG = {
    'proxy': {
        'enabled': True,  # 根据需求开启
        'type': 'file',
        'path': 'proxies.txt',
        'rotate_after': 10,
        'timeout': 10
    },
    'user_agent': {
        'enabled': True,
        'rotate_after': 5
    },
    'throttler': {
        'min_delay': 1.5,
        'max_delay': 3.5,
        'random_factor': 0.3,
        'burst_detection': True,
        'burst_threshold': 5,
        'burst_cooldown': 15.0
    },
    'block_detector': {
        'enabled': True,
        'block_keywords': [
            "验证码", "访问频繁", "禁止访问", "403", "404", "出错了",
            "访问受限", "安全验证", "请输入验证码", "账号异常",
            "您的访问过于频繁", "请稍后再试"
        ],
        'status_codes': [403, 429, 503, 418],
        'retry_on_block': True,
        'max_block_retries': 5,
        'cooldown_on_block': 30.0
    }
}

# ------------------------------------------------------------------
# Scheduler Configuration
# ------------------------------------------------------------------
SCHEDULER = {
    # 每天运行时间（本地时间）
    # 格式: "HH:MM"
    "run_time": "19:30",

    # 失败后的重试次数（不含首次）
    "retry_attempts": 2,

    # 重试退避时间（秒，线性递增）
    "retry_backoff_sec": 120,

    # 触发前的轻微抖动（避免卡在整点）
    "jitter_sec": 5,
}

ANALYSIS_CONFIG = {
    "top_k": 30,
    "rank_scale": 1.0,
    "metric_scale": 1.0,
    "metric_transform": "log1p",
    "prefer_primary_title": True,

    # key: "{platform}:{rank_family}:{rank_sub_cat}"
    # rank_sub_cat 为空时写空字符串即可（末尾保留冒号）
    "list_weights": {
        "qidian:月票榜:": 1.2,
        "qidian:畅销榜:": 1.0,
        "qidian:推荐榜:": 0.9,

        "fanqie:阅读榜:科幻末世": 1.1,
        "fanqie:阅读榜:都市高武": 1.0,
        "fanqie:阅读榜:悬疑脑洞": 1.0,
        "fanqie:新书榜:": 0.9,

        # fallback
        "fanqie:阅读榜": 1.0,
        "qidian": 1.0,
    },
}

