# spiders/base_spider.py
from __future__ import annotations
import copy
import os
import time
import json
import logging
import random
from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Union, Sequence
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException
from bs4 import BeautifulSoup
import config
import undetected_chromedriver as uc
from .antibot import (
    AntiBotConfig,
    AntiBotDetector,
    AntiBotHandler,
    AntiBotDetectedException,
    FatalAntiBotException,
)

# --- patch uc destructor ---
try:
    _uc_orig_del = uc.Chrome.__del__

    def _uc_safe_del(self):
        try:
            _uc_orig_del(self)
        except OSError as e:
            if getattr(e, "winerror", None) == 6 or "WinError 6" in str(e):
                return
            raise
        except Exception:
            return

    uc.Chrome.__del__ = _uc_safe_del
except Exception:
    pass

class BaseSpider(ABC):
    def __init__(self, site_config: Dict[str, Any], db_handler: Any = None):
        """
        Args:
            site_config: 站点配置字典，包含base_url、rank_urls等
            db_handler: 数据库处理器实例，用于直接存储数据
        """
        self.config = config
        self.site_config = site_config
        self.name = site_config.get('name', 'unknown')
        self.base_url = site_config.get('base_url', '')

        # spider共用config参数
        crawler = getattr(self.config, "CRAWLER_CONFIG", {}) or {}
        self.request_delay = float(site_config.get("request_delay", crawler.get("retry_delay", 2.0)))
        self.max_retries = int(site_config.get("max_retries", crawler.get("max_retries", 3)))

        # 数据库处理器
        self.db_handler = db_handler

        # Selenium驱动
        self.driver: Optional[webdriver.Chrome] = None

        # 日志记录器
        self.logger = self._setup_logger()

        # 代理池相关
        self.proxy_pool: List[str] = []
        self.current_proxy_index: int = 0
        self.current_proxy: Optional[str] = None

        self._init_proxy_pool()
        if self.proxy_pool:
            self.current_proxy = self.proxy_pool[0]

        # 反爬检测相关
        self.antibot_keywords = ['验证码', 'captcha', '访问限制', '安全验证']

        # 缓存
        self.book_cache: Dict[str, Dict[str, Any]] = {}
        self.retry_count = 0

        # Selenium配置
        self.selenium_config = self._build_selenium_config()

        # 初始化Selenium驱动
        if self.selenium_config.get('enabled', True):
            self._init_driver()

        # anti-bot module
        ab_cfg_dict = ((self.site_config or {}).get("antibot") or {})
        crawler_cfg = getattr(self.config, "CRAWLER_CONFIG", {}) or {}
        global_ab = (crawler_cfg.get("antibot") or {})  # 可选：全局默认
        merged_ab = {**global_ab, **ab_cfg_dict}

        self.antibot_cfg = AntiBotConfig(
            min_html_length=int(merged_ab.get("min_html_length", 800)),
            consecutive_threshold=int(merged_ab.get("consecutive_threshold", 3)),
            cooldown_range=tuple(merged_ab.get("cooldown_range", (60, 180))),
            mode=str(merged_ab.get("mode", "cooldown")),
        )

        self.antibot_detector = AntiBotDetector(self.antibot_cfg)
        self.antibot_handler = AntiBotHandler(self.antibot_cfg)

    """设置日志记录器"""
    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(f'{self.name}_spider')
        logger.setLevel(logging.INFO)

        # 如果已经配置过处理器，则直接返回
        if logger.handlers:
            return logger

        # 确保日志目录存在
        log_dir = (getattr(self.config, "OUTPUT_PATHS", {}) or {}).get("logs", "outputs/logs")
        os.makedirs(log_dir, exist_ok=True)

        # 文件处理器
        file_handler = logging.FileHandler(
            f'{log_dir}/{self.name}_spider.log',
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # 格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # 添加处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    # ------------------------------------------------------------------
    # Anti-Block
    # ------------------------------------------------------------------
    """应用 stealth JavaScript 隐藏自动化特征"""
    def _apply_stealth_js(self):
        try:
            stealth_js = """
            // 隐藏 webdriver 属性
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // 覆盖 plugins 属性
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // 覆盖 languages 属性
            Object.defineProperty(navigator, 'languages', {
                get: () => ['zh-CN', 'zh', 'en']
            });

            // 添加 Chrome 特性
            window.chrome = {
                runtime: {},
                loadTimes: function() {},
                csi: function() {},
                app: {}
            };

            // 覆盖 permissions 属性
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );

            // 覆盖 navigator 属性
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });

            // 模拟真实的屏幕属性
            Object.defineProperty(screen, 'orientation', {
                get: () => ({ type: 'landscape-primary' })
            });
            """

            if hasattr(self, 'driver') and self.driver:
                self.driver.execute_script(stealth_js)

        except Exception as e:
            self.logger.debug(f"注入 stealth JS 失败: {e}")

    """从配置加载代理池"""
    def _init_proxy_pool(self):
        try:
            proxy_config = (self.site_config or {}).get("proxy_pool", [])
            if proxy_config:
                self.proxy_pool = proxy_config
                self.logger.info(f"初始化代理池，共 {len(self.proxy_pool)} 个代理")
            else:
                proxy_env = os.environ.get('PROXY_POOL', '')
                if proxy_env:
                    self.proxy_pool = [p.strip() for p in proxy_env.split(',') if p.strip()]
        except Exception as e:
            self.logger.warning(f"初始化代理池失败: {e}")

    """代理轮换"""
    def _rotate_proxy(self) -> bool:
        if not self.proxy_pool:
            return False
        self.current_proxy_index += 1
        self.current_proxy = self.proxy_pool[self.current_proxy_index % len(self.proxy_pool)]
        self.logger.info(f"轮换代理: {self.current_proxy}")
        return True

    """检查是否被反爬虫检测到，包含空白页面检测"""
    def _check_antibot_detected(self, soup: BeautifulSoup, html_length: int = 0) -> bool:
        try:
            # 1. 首先检查页面是否过短（空白页面检测）
            min_content_length = 200
            if html_length < min_content_length:
                self.logger.warning(f"页面过短 ({html_length} 字符)，疑似反爬空白页面")
                return True

            # 2. 检查页面文本内容
            page_text = str(soup).lower()

            # 检查是否包含反爬关键词
            antibot_patterns = [
                '验证码', 'captcha', '访问限制', 'rate limit', '访问异常',
                '安全验证', '请完成验证', 'human verification', 'robot check',
                'security check', 'access denied', 'denied access',
                'anti-spam', '反爬虫', '防采集'
            ]

            for pattern in antibot_patterns:
                if pattern.lower() in page_text:
                    self.logger.warning(f"检测到反爬关键词: {pattern}")
                    return True

            # 3. 检查页面标题
            title = soup.title.string.lower() if soup.title else ""
            antibot_titles = []
            for antibot_title in antibot_titles:
                if antibot_title in title:
                    self.logger.warning(f"检测到反爬标题: {title}")
                    return True

            # 4. 检查是否有验证码元素
            captcha_selectors = [
                '.captcha', '.verification-code', '.security-check', '#captcha',
                '.recaptcha', '.h-captcha', '.g-recaptcha', '.verify-code',
                '.verification', '.verification-modal', '.antibot-modal',
                '.antispam', '.human-verification', '.robot-check'
            ]
            for selector in captcha_selectors:
                if soup.select_one(selector):
                    self.logger.warning(f"检测到验证码元素: {selector}")
                    return True

            # 5. 检查页面是否只包含基础HTML结构（无实际内容）
            body_content = soup.find('body')
            if body_content:
                body_text = body_content.get_text(strip=True)
                if len(body_text) < 50:  # 页面body内容过少
                    self.logger.warning(f"页面内容过少 ({len(body_text)} 字符)，疑似反爬")
                    return True

            # 6. 检查是否有反爬警告信息
            warning_messages = [
                '为了保障您的访问安全', '检测到异常访问', '请完成下方验证后继续',
                '您的请求过于频繁', '请稍后再试', '请输入验证码继续访问'
            ]
            for warning in warning_messages:
                if warning in page_text:
                    self.logger.warning(f"检测到反爬警告: {warning}")
                    return True

            # 7. 检查是否有反爬重定向相关的meta标签
            meta_refresh = soup.find('meta', {'http-equiv': 'refresh'})
            if meta_refresh and ('url=' in str(meta_refresh.get('content', '')).lower()):
                self.logger.warning("检测到页面重定向meta标签，疑似反爬")
                return True

            # 8. 检查是否有iframe指向验证码页面
            iframes = soup.find_all('iframe')
            for iframe in iframes:
                src = iframe.get('src', '')
                if any(keyword in src.lower() for keyword in ['captcha', 'verify', 'verification', 'challenge']):
                    self.logger.warning(f"检测到验证码iframe: {src}")
                    return True

            return False

        except Exception as e:
            self.logger.debug(f"反爬检测失败: {e}")
            # 如果反爬检测失败，保守起见认为检测到了反爬
            return True

    # ------------------------------------------------------------------
    # Selenium and webdriver
    # ------------------------------------------------------------------
    """合并global config和specific config"""
    def _build_selenium_config(self) -> Dict[str, Any]:
        base = getattr(config, "SELENIUM_CONFIG", {}) or {}
        site_specific = (self.site_config or {}).get("selenium_specific", {}) or {}
        return self._deep_merge_dict(base, site_specific)

    """使用 undetected_chromedriver 初始化驱动"""
    def _init_driver(self) -> bool:
        try:
            cfg = self.selenium_config or {}
            if not cfg.get("enabled", True):
                self.logger.info("Selenium disabled by config.")
                return False

            # 1. 创建 uc 专用的 ChromeOptions
            options = uc.ChromeOptions()
            opt_cfg = cfg.get("options", {}) or {}

            # headless 模式（强烈建议 False）
            headless = bool(opt_cfg.get("headless", False))
            if headless:
                options.add_argument("--headless=new")

            # 窗口大小
            if opt_cfg.get("window_size"):
                options.add_argument(f"--window-size={opt_cfg['window_size']}")

            # User-Agent
            if opt_cfg.get("user_agent"):
                ua = opt_cfg['user_agent']
                if isinstance(ua, (tuple, list)):
                    selected_ua = random.choice(ua)
                    options.add_argument(f"user-agent={selected_ua}")
                elif isinstance(ua, str):
                    options.add_argument(f"user-agent={ua}")

            # 其他命令行参数
            for k, v in opt_cfg.items():
                if k in {"headless", "window_size", "user_agent"}:
                    continue
                flag = f"--{k.replace('_', '-')}"
                if isinstance(v, bool):
                    if v:
                        options.add_argument(flag)
                elif isinstance(v, str):
                    options.add_argument(f"{flag}={v}")
                elif v is not None:
                    options.add_argument(f"{flag}={v}")

            # 2. 实验性选项（跳过与 uc 冲突的项）
            for k, v in (cfg.get("experimental_options", {}) or {}).items():
                if k in ("excludeSwitches", "useAutomationExtension"):
                    self.logger.debug(f"跳过 experimental_option '{k}' (与 uc 不兼容)")
                    continue
                options.add_experimental_option(k, v)

            # 3. 用户偏好设置
            prefs = cfg.get("prefs")
            if isinstance(prefs, dict) and prefs:
                options.add_experimental_option("prefs", prefs)

            # 4. 代理配置
            use_proxy = bool((getattr(self.config, "CRAWLER_CONFIG", {}) or {}).get("use_proxy", False))
            if use_proxy and self.current_proxy:
                options.add_argument(f"--proxy-server={self.current_proxy}")

            # 5. 启动 undetected_chromedriver（兼容版本处理）
            try:
                self.driver = uc.Chrome(
                    options=options,
                    version_main=144
                )
            except Exception as e:
                self.logger.warning(f"指定 version_main=144 失败，尝试自动匹配: {e}")
                self.driver = uc.Chrome(options=options)

            # 6. 设置超时
            self.driver.set_page_load_timeout(int(cfg.get("page_load_timeout", 30)))
            self.driver.implicitly_wait(int(cfg.get("implicit_wait", 10)))

            self.logger.info(f"{self.__class__.__name__} undetected_chromedriver 初始化成功")
            return True

        except Exception as e:
            self.logger.error(f"undetected_chromedriver 初始化失败: {e}")
            # 重试逻辑
            retry_cfg = (self.selenium_config or {}).get("retry", {}) or {}
            if not retry_cfg.get("enabled", True):
                return False
            max_retries = int(retry_cfg.get("max_retries", 3))
            backoff = float(retry_cfg.get("backoff_seconds", 2))
            if getattr(self, "retry_count", 0) < max_retries:
                self.retry_count = getattr(self, "retry_count", 0) + 1
                time.sleep(backoff)
                return self._init_driver()
            return False

    # ------------------------------------------------------------------
    # Fetch Webpage via BeautifulSoup
    # ------------------------------------------------------------------
    def _get_page_fetch_cfg(self) -> dict:
        """
        Merge page fetch configs in this order (later overrides earlier):
        1) CRAWLER_CONFIG.page_fetch (global defaults)
        2) CRAWLER_CONFIG.page_fetch_overrides (global overrides)
        3) site_config.selenium_specific.page_fetch_overrides (site overrides)
        4) site_config.page_fetch_overrides (backward compatible)
        """
        crawler_cfg = getattr(self.config, "CRAWLER_CONFIG", {}) or {}

        global_fetch = (crawler_cfg.get("page_fetch", {}) or {})
        global_overrides = (crawler_cfg.get("page_fetch_overrides", {}) or {})

        selenium_specific = (self.site_config or {}).get("selenium_specific", {}) or {}
        site_overrides = (selenium_specific.get("page_fetch_overrides", {}) or {})

        legacy_site_overrides = (self.site_config or {}).get("page_fetch_overrides", {}) or {}

        return {**global_fetch, **global_overrides, **site_overrides, **legacy_site_overrides}

    def _get_soup(
            self,
            url: str,
            wait_css: Optional[str] = None,
            wait_sec: Optional[int] = None,
            max_retries: Optional[int] = None,
            retry_delay: Optional[int] = None,
            is_scrolling: bool = False,
            # scrolling params (only used when is_scrolling=True)
            target_count: Optional[int] = None,
            max_scroll_attempts: Optional[int] = None,
            item_css: Optional[str] = None,
            scroll_pause_sec: Optional[float] = None,
            no_change_limit: int = 3,
    ) -> Optional[Any]:
        """
        Unified selenium fetch -> optional wait_css -> optional scroll -> optional html postprocess -> soup.

        - qidian: is_scrolling=False (default)
        - fanqie: is_scrolling=True, subclass overrides _scroll_load() and _postprocess_html()

        Config defaults read from:
          CRAWLER_CONFIG.page_fetch + site_config.page_fetch_overrides

        New:
          - driver.get counter + periodic restart (restart_driver_every_n_get)
          - auto restart on invalid session id
        """
        # ---- config defaults (global + site override) ----
        cfg = self._get_page_fetch_cfg()

        _wait_sec = int(wait_sec if wait_sec is not None else cfg.get("default_wait_sec", 10))
        _max_retries = int(max_retries if max_retries is not None else cfg.get("max_page_retries", 3))
        _retry_delay = float(retry_delay if retry_delay is not None else cfg.get("page_retry_delay", 3))

        post_load_delay = cfg.get("post_load_delay_range", (1, 2))
        if not isinstance(post_load_delay, (list, tuple)) or len(post_load_delay) != 2:
            post_load_delay = (1, 2)

        min_html_length = int(cfg.get("min_html_length", 800))
        bad_title_keywords = cfg.get("bad_title_keywords", ["404", "无法访问", "出错了"])

        # periodic restart knobs (default: off)
        restart_every_n_get = int(cfg.get("restart_driver_every_n_get", 0) or 0)

        # -------- driver lifecycle helpers (local closure, no need to modify other functions) --------
        def _driver_is_alive() -> bool:
            try:
                d = getattr(self, "driver", None)
                if d is None:
                    return False
                sid = getattr(d, "session_id", None)
                if not sid:
                    return False
                _ = d.current_url  # light ping
                return True
            except Exception:
                return False

        def _restart_driver(reason: str) -> None:
            try:
                if getattr(self, "driver", None) is not None:
                    try:
                        self.driver.quit()
                    except Exception:
                        pass
            finally:
                self.driver = None

            self.logger.warning(f"[Selenium] restart driver. reason={reason}")
            try:
                self._init_driver()
            except Exception as e:
                self.logger.error(f"[Selenium] driver init failed after restart. error={e}")
                # init 失败就让后续尝试继续触发重试逻辑
                self.driver = None

        def _ensure_driver_ready(reason: str) -> bool:
            if _driver_is_alive():
                return True
            _restart_driver(reason=reason)
            return _driver_is_alive()

        # -------- initial ensure --------
        if not _ensure_driver_ready(reason="driver not initialized / not alive before _get_soup"):
            self.logger.error("Selenium driver not initialized (restart failed)")
            return None

        total_attempts = _max_retries + 1

        for attempt in range(1, total_attempts + 1):
            try:
                # periodic restart BEFORE navigation (avoid dying mid-get)
                if restart_every_n_get > 0:
                    counter = int(getattr(self, "_driver_get_counter", 0) or 0)
                    if counter > 0 and (counter % restart_every_n_get == 0):
                        _restart_driver(
                            reason=f"periodic restart: every {restart_every_n_get} gets (counter={counter})")
                        if not _ensure_driver_ready(reason="driver not alive after periodic restart"):
                            raise RuntimeError("driver not available after periodic restart")

                self.logger.info(f"[页面获取] 尝试 {attempt}/{total_attempts}: {url}")

                # page load
                try:
                    page_load_sec = int(cfg.get("page_load_sec", _wait_sec))
                    self.driver.set_page_load_timeout(page_load_sec)
                except Exception:
                    pass

                # ---- critical: driver.get ----
                self.driver.get(url)

                # get counter increments ONLY after get() succeeds (no exception thrown)
                self._driver_get_counter = int(getattr(self, "_driver_get_counter", 0) or 0) + 1

                self._humanlike_sleep(post_load_delay[0], post_load_delay[1])

                # optional wait css
                if wait_css:
                    try:
                        WebDriverWait(self.driver, _wait_sec).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                        )
                    except Exception as e:
                        self.logger.debug(f"Wait CSS timeout: {wait_css}, error={e}")

                # optional scroll (fanqie)
                if is_scrolling:
                    self._scroll_load(
                        target_count=target_count,
                        max_scroll_attempts=max_scroll_attempts,
                        item_css=item_css,
                        scroll_pause_sec=scroll_pause_sec,
                        no_change_limit=no_change_limit,
                    )

                html = self.driver.page_source or ""
                html = self._postprocess_html(html)

                soup = BeautifulSoup(html, "html.parser") if html else None

                # anti-bot detect (module)
                try:
                    self.antibot_detector.detect(soup=soup, html=html, html_length=len(html))
                    self._consecutive_short_pages = 0
                except AntiBotDetectedException:
                    self._consecutive_short_pages = int(getattr(self, "_consecutive_short_pages", 0) or 0) + 1
                    raise  # 交给外层 except AntiBotDetectedException 统一处理

                # title bad keywords / other checks 也可以继续保留在 detector，或留在这
                title = ""
                try:
                    title = (soup.title.string if soup and soup.title else "") or ""
                except Exception:
                    title = ""

                if any(k in title for k in bad_title_keywords):
                    raise ValueError(f"Bad page title: {title}")

                return soup

            except (InvalidSessionIdException, WebDriverException) as e:
                msg = str(e).lower()
                # invalid session 是“driver 死亡”，必须重启而不是 refresh
                if "invalid session id" in msg or isinstance(e, InvalidSessionIdException):
                    self.logger.warning(
                        f"[页面获取] 失败 {attempt}/{total_attempts}: {url} ; error=invalid session id -> restart driver"
                    )
                    _restart_driver(reason="invalid session id during fetch")
                    time.sleep(_retry_delay)
                    continue

                # 其他 webdriver 异常：按原逻辑退避 + refresh（尽量保持你的行为不变）
                self.logger.warning(f"[页面获取] 失败 {attempt}/{total_attempts}: {url} ; error={e}")

                if attempt >= total_attempts:
                    self.logger.error(f"[页面获取] 所有尝试都失败: {url}")
                    return None

                time.sleep(_retry_delay)
                try:
                    self.driver.refresh()
                except Exception:
                    pass
                continue



            except AntiBotDetectedException as e:
                self.logger.warning(f"[页面获取] 失败 {attempt}/{total_attempts}: {url} ; error={e}")
                try:
                    self.antibot_handler.handle(
                        logger=self.logger,
                        url=url,
                        consecutive_count=int(getattr(self, "_consecutive_short_pages", 0) or 0),
                        rotate_proxy_fn=(self._rotate_proxy if self.proxy_pool else None),
                        restart_driver_fn=self.restart_driver,
                        close_driver_fn=self.close,
                    )
                except FatalAntiBotException:
                    raise
                if int(getattr(self, "_consecutive_short_pages", 0) or 0) >= int(
                        self.antibot_cfg.consecutive_threshold):
                    return None
                time.sleep(_retry_delay)
                continue



            except Exception as e:
                self.logger.warning(f"[页面获取] 失败 {attempt}/{total_attempts}: {url} ; error={e}")

                # 每次失败后增加延迟
                time.sleep(random.uniform(2, 6))

                # 每两次失败轮换一次代理
                if attempt % 2 == 0 and self.proxy_pool:
                    self._rotate_proxy()
                    self.restart_driver()

                if attempt >= total_attempts:
                    self.logger.error(f"[页面获取] 所有尝试都失败: {url}")
                    return None

                time.sleep(_retry_delay)

                # 简单恢复：refresh（不强制 reinit，保持简单）
                try:
                    self.driver.refresh()
                except Exception:
                    pass

        return None

    """_get_soup Hook: scrolling logic for infinite-scroll pages"""
    def _scroll_load(
            self,
            target_count: Optional[int] = None,
            max_scroll_attempts: Optional[int] = None,
            item_css: Optional[str] = None,
            scroll_pause_sec: Optional[float] = None,
            no_change_limit: int = 3,
    ) -> None:
        sel_cfg = (self.site_config or {}).get("selenium_specific", {}) or {}
        target_count = int(target_count or sel_cfg.get("target_count") or 0) or None
        max_scroll_attempts = int(max_scroll_attempts or sel_cfg.get("max_scroll_attempts") or 10)
        item_css = item_css or sel_cfg.get("item_css")  # 允许为空：为空就只按高度变化判断
        scroll_pause_sec = float(scroll_pause_sec or sel_cfg.get("scroll_pause_sec") or 0.8)

        last_cnt = -1
        no_change = 0

        for _ in range(max_scroll_attempts):
            # 统计当前元素数
            cnt = None
            if item_css:
                try:
                    cnt = len(self.driver.find_elements("css selector", item_css))
                except Exception:
                    cnt = None

            # 达标直接停
            if target_count and cnt is not None and cnt >= target_count:
                break

            # 连续不增长则停
            if cnt is not None:
                if cnt == last_cnt:
                    no_change += 1
                else:
                    no_change = 0
                last_cnt = cnt
                if no_change >= no_change_limit:
                    break

            # 执行滚动
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                break
            time.sleep(scroll_pause_sec)

    """_get_soup Hook: site-specific html processing (e.g., decrypt)"""
    def _postprocess_html(self, html: str) -> str:
        return html

    """重启driver并应用新配置"""
    def restart_driver(self, reason: str = "") -> bool:
        try:
            if hasattr(self, 'driver') and self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
            time.sleep(random.uniform(2, 5))  # 随机延迟

            # 重新初始化driver
            success = self._init_driver()

            if success:
                # 应用stealth脚本
                self._apply_stealth_js()

                if reason:
                    self.logger.info(f"Driver已重启: {reason}")
                else:
                    self.logger.info("Driver已重启")

            return success

        except Exception as e:
            self.logger.error(f"重启driver失败: {e}")
            return False

    """在爬取完一个榜单之后重启driver"""
    def restart_driver_after_rank(self, rank_type: str = "") -> None:
        cfg = self._get_page_fetch_cfg()
        restart_each_rank = bool(cfg.get("restart_driver_each_rank", False))

        if not restart_each_rank:
            return

        try:
            if getattr(self, "driver", None) is not None:
                try:
                    self.driver.quit()
                except Exception:
                    pass
        finally:
            self.driver = None

        self.logger.warning(f"[Selenium] restart driver after rank. rank_type={rank_type}")
        self._init_driver()

    def _driver_is_alive(self) -> bool:
        try:
            d = getattr(self, "driver", None)
            if d is None:
                return False
            # session_id 为空通常意味着 quit 过
            sid = getattr(d, "session_id", None)
            if not sid:
                return False
            # 轻量探测：访问 current_url 会触发与 driver 的通信
            _ = d.current_url
            return True
        except Exception:
            return False

    def _restart_driver(self, reason: str = "") -> None:
        try:
            if getattr(self, "driver", None) is not None:
                try:
                    self.driver.quit()
                except Exception:
                    pass
        finally:
            self.driver = None
        self.logger.warning(f"[Selenium] restart driver. reason={reason}".strip())
        self._init_driver()

    def close(self) -> None:
        """关闭爬虫，释放资源"""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info(f"{self.name} Selenium driver closed")
            except Exception as e:
                self.logger.error(f"Failed to close driver: {e}")
            finally:
                self.driver = None

        # 清理代理池引用
        self.proxy_pool = []
        self.current_proxy_index = 0

    # ------------------------------------------------------------------
    # Fetch Fallback Logic (当rank page获取信息失败时，在detail page补全信息）
    # ------------------------------------------------------------------
    def _need_fallback_scalar(
            self,
            cur: object,
            *,
            when_empty: bool = False,
            when_unknown: bool = False,
            min_len: int = 0,
            unknown_set: set[str] | None = None,
    ) -> bool:
        unknown_set = unknown_set or {"", "未知"}

        if when_empty:
            if cur is None:
                return True
            if isinstance(cur, str) and cur.strip() == "":
                return True

        if when_unknown:
            if cur is None:
                return True
            if isinstance(cur, str) and cur.strip() in unknown_set:
                return True

        if min_len and isinstance(cur, str):
            if len(cur.strip()) < min_len:
                return True

        return False

    def _need_fallback_tags(
            self,
            tags: object,
            *,
            unknown_set: set[str] | None = None,
    ) -> bool:
        unknown_set = unknown_set or {"", "未知"}

        if tags is None:
            return True
        if not isinstance(tags, list):
            return True

        valid = [
            t for t in tags
            if isinstance(t, str) and t.strip() and t.strip() not in unknown_set
        ]
        return len(valid) == 0

    def _slice_chapter_infos_to_fetch(
            self,
            chapter_infos: Sequence[Any],
            existing_count: int,
            need_count: int,
    ) -> List[Any]:
        """
        Generic slicing helper:
        - skip first `existing_count` items
        - take next `need_count` items
        """
        if not chapter_infos or need_count <= 0:
            return []
        start = max(0, int(existing_count))
        end = start + int(need_count)
        return list(chapter_infos[start:end])

    """决定是否值得进详情页（避免重复提取）"""
    def _needs_detail(self, item: Dict[str, Any]) -> bool:
        # detail-only 字段缺失时才进
        if not item.get("status"):
            return True
        if not item.get("total_words"):
            return True
        if item.get("total_recommend") is None:
            return True

        # 分类策略：rank 提取到主分类就不进详情页补分类
        main_cat = (item.get("main_category") or "").strip()
        if not main_cat or main_cat == "未知":
            return True

        return False

    # ------------------------------------------------------------------
    # Spider Functions
    # ------------------------------------------------------------------
    """获取榜单数据"""
    @abstractmethod
    def fetch_rank_list(self, rank_type: str = '', pages: int = 5) -> List[Dict[str, Any]]:
        """
        Args:
            rank_type: 榜单类型
            pages: 爬取页数
        Returns:
            List[Dict[str, Any]]: 榜单数据列表
        """
        pass

    """获取小说详情"""
    @abstractmethod
    def fetch_novel_detail(self, novel_url: str, pid: str, seed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Args:
            novel_url: 小说URL
            novel_id: 小说ID
        Returns:
            Dict[str, Any]: 小说详情数据
        """
        pass

    """使用详情数据补完小说信息"""
    @abstractmethod
    def enrich_books_with_details(self, books: List[Dict[str, Any]],
                                  max_books: int = 20) -> List[Dict[str, Any]]:
        """
        Args:
            books: 书籍列表
            max_books: 最大处理书籍数
        Returns:
            List[Dict[str, Any]]: 补完后的书籍列表
        """
        pass

    """获取小说前N章内容"""
    @abstractmethod
    def fetch_first_n_chapters(self, novel_url: str, target_chapter_count: int = 5) -> List[Dict[str, Any]]:
        """
        Args:
            novel_url: 小说URL
            n: 章节数量
        Returns:
            List[Dict[str, Any]]: 章节数据列表
        """
        pass

    """获取整个榜单数据"""
    @abstractmethod
    def fetch_whole_rank(self) -> List[Dict[str, Any]]:
        """
        Returns:
            List[Dict[str, Any]]: 所有榜单数据
        """
        pass

    def run_daily_task(self, rank_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """执行每日爬取任务

        Args:
            rank_types: 需要爬取的榜单类型列表，为None则爬取所有配置的榜单

        Returns:
            Dict[str, Any]: 任务执行结果
        """
        self.logger.info(f"Starting daily task for {self.name}")

        results = {
            "platform": self.name,
            "date": self._today_str(),
            "rank_snapshots": [],
            "total_novels": 0,
            "total_chapters": 0,
        }

        # 确定要爬取的榜单类型
        if rank_types is None:
            rank_types = list(self.site_config.get("rank_urls", {}).keys())

        # 爬取每个榜单
        for rank_type in rank_types:
            try:
                self.logger.info(f"Fetching rank: {rank_type}")

                # 获取榜单数据
                rank_items = self.fetch_rank_list(rank_type)

                # 丰富数据（获取详情）
                enriched_items = self.enrich_books_with_details(
                    rank_items,
                    max_books=min(20, len(rank_items))
                )

                # 保存到数据库（如果有数据库处理器）
                if self.db_handler and hasattr(self.db_handler, "save_rank_snapshot"):
                    rank_type_map = self.site_config.get("rank_type_map", {})
                    ident = rank_type_map.get(rank_type)

                    if not ident:
                        raise ValueError(f"rank_type '{rank_type}' not found in rank_type_map")

                    snapshot_id = self.db_handler.save_rank_snapshot(
                        platform=self.name,
                        rank_family=ident["rank_family"],
                        rank_sub_cat=ident["rank_sub_cat"],
                        snapshot_date=self._today_str(),
                        items=enriched_items,
                        source_url=self.site_config.get("rank_urls", {}).get(rank_type, ""),
                    )

                    results["rank_snapshots"].append({
                        "rank_type": rank_type,
                        "snapshot_id": snapshot_id,
                        "novel_count": len(enriched_items),
                    })

                # 保存原始数据
                self._save_raw_data(
                    enriched_items,
                    f"{self.name}_{rank_type}_{self._today_str()}.json"
                )

                results["total_novels"] += len(enriched_items)

                # 随机延迟，避免请求过快
                self._humanlike_sleep(3, 5)

            except Exception as e:
                self.logger.error(f"Failed to fetch rank {rank_type}: {e}")
                continue

        self.logger.info(f"Daily task completed for {self.name}")
        return results


    # ------------------------------------------------------------------
    # Common Utils
    # ------------------------------------------------------------------
    @staticmethod
    def _deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge override into base and return a NEW dict (base not mutated).
        """
        out = copy.deepcopy(base) if base else {}
        for k, v in (override or {}).items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = BaseSpider._deep_merge_dict(out[k], v)
            else:
                out[k] = v
        return out

    def _today_str(self) -> str:
        """获取今日日期字符串 (YYYY-MM-DD)"""
        return date.today().strftime("%Y-%m-%d")

    def _to_abs_url(self, href: str) -> str:
        """将相对URL转换为绝对URL"""
        if not href:
            return ""
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("http"):
            return href
        return urljoin(self.base_url, href)

    def _normalize_text(self, text: str) -> str:
        """标准化文本：去除多余空白字符"""
        if not text:
            return ""
        return ' '.join(text.split())

    def normalize_novel_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """标准化小说数据格式

        Args:
            raw_data: 原始小说数据

        Returns:
            Dict[str, Any]: 标准化后的数据
        """
        return {
            "platform": self.name,
            "platform_novel_id": raw_data.get("novel_id", ""),
            "title": raw_data.get("title", ""),
            "author": raw_data.get("author", ""),
            "intro": raw_data.get("intro", ""),
            "main_category": raw_data.get("main_category", ""),
            "sub_category": raw_data.get("sub_category", ""),
            "tags": raw_data.get("tags", []),
            "status": raw_data.get("status", "ongoing"),
            "total_words": raw_data.get("total_words", 0),
            "url": raw_data.get("url", ""),
            "rank": raw_data.get("rank", -1),
            "extra": raw_data.get("extra", {}),
        }

    def _parse_cn_number(self, text: str) -> Optional[int]:
        """解析中文数字表示（如'12.3万'、'1.2亿'）为整数

        Args:
            text: 包含中文数字的文本

        Returns:
            Optional[int]: 解析后的整数，解析失败返回None
        """
        if not text:
            return None

        text = text.strip().replace(',', '')

        # 匹配数字和单位
        import re
        pattern = r'([0-9]+(?:\.[0-9]+)?)\s*([万亿]?)'
        match = re.search(pattern, text)

        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2)

        if unit == '万':
            value *= 10000
        elif unit == '亿':
            value *= 100000000

        return int(value)

    def _dedupe_keep_order(self, xs: Sequence[str]) -> List[str]:
        """Dedupe strings while keeping original order."""
        seen = set()
        out: List[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    """模拟人类操作的随机延迟"""
    def _humanlike_sleep(self, min_time: Optional[float] = None, max_time: Optional[float] = None) -> None:
        """
        Human-like delay. If min_time/max_time not provided, use config defaults.
        Priority: explicit args > site_config.request_delay > CRAWLER_CONFIG.retry_delay
        """
        if min_time is None or max_time is None:
            # 站点如果是单值 request_delay，就扩展为一个小区间
            site_delay = (self.site_config or {}).get("request_delay", None)

            if isinstance(site_delay, (list, tuple)) and len(site_delay) == 2:
                min_time, max_time = float(site_delay[0]), float(site_delay[1])
            else:
                base = float(site_delay) if site_delay is not None else float(
                    (getattr(self.config, "CRAWLER_CONFIG", {}) or {}).get("retry_delay", 2.0)
                )
                # 默认给一个窄随机范围，避免完全固定
                min_time, max_time = max(0.0, base * 0.7), base * 1.3

        time.sleep(random.uniform(float(min_time), float(max_time)))

    # ------------------------------------------------------------------
    # Database Operations
    # ------------------------------------------------------------------
    """保存原始数据到文件"""
    def _save_raw_data(self, data: Any, filename: str) -> None:
        """
        Args:
            data: 要保存的数据
            filename: 文件名
        """
        raw_data_dir = 'outputs/data/raw'
        os.makedirs(raw_data_dir, exist_ok=True)

        filepath = os.path.join(raw_data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            if isinstance(data, (dict, list)):
                json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                f.write(str(data))

        self.logger.debug(f"Raw data saved: {filepath}")

    """获取用于显示的标题，优先使用normalized标题"""
    def _get_display_title(self, novel_id: str, fallback_title: str = "", platform: str = "") -> Tuple[str, str]:
        """
        Args:
            novel_id: 小说平台ID
            fallback_title: 后备标题（当无法从数据库获取时使用）

        Returns:
            Tuple[显示标题, 标题来源]
        """
        # 首先尝试从数据库获取归一化标题
        if not self.db_handler:
            return None

        title_norm = ""

        try:
            # 使用db_handler的get_novel_title_norm方法
            if hasattr(self.db_handler, 'get_novel_title_norm'):
                title_norm = self.db_handler.get_novel_title_norm(platform, novel_id)
                if title_norm:
                    self.logger.debug("[标题查询] 从数据库获取到归一化标题: %s (小说ID: %s)", title_norm, novel_id)
                else:
                    self.logger.debug("[标题查询] 数据库中未找到归一化标题 (小说ID: %s)", novel_id)
            else:
                self.logger.warning("[标题查询] db_handler没有get_novel_title_norm方法")
                return None

        except Exception as e:
            self.logger.debug("[标题查询] 获取归一化标题失败 (小说ID: %s): %s", novel_id, e)

        if title_norm:
            return title_norm, "norm标题"
        elif fallback_title:
            return fallback_title, "fallback标题"
        else:
            # 如果都没有，尝试从数据库中查询
            try:
                if self.db_handler and hasattr(self.db_handler, 'get_novel_title'):
                    title = self.db_handler.get_novel_title(platform, novel_id)
                    if title:
                        return title, "数据库标题"
            except Exception as e:
                self.logger.debug("[标题显示] 获取数据库标题失败: %s", e)

            return f"小说ID:{novel_id}", "ID"

    """获取数据库中已有章节数量"""
    def _get_existing_chapter_count(self, novel_id: str) -> int:
        """Return stored chapter count for (platform, platform_novel_id) using db_handler API."""
        if not self.db_handler or not novel_id:
            return 0

        # 统一平台 key：qidian / fanqie
        platform = getattr(self, "site_key", None) or "qidian"
        platform = str(platform).lower()

        # 优先走 DatabaseHandler 的 public 方法
        if hasattr(self.db_handler, "get_first_n_chapter_count"):
            try:
                count = self.db_handler.get_first_n_chapter_count(platform=platform, platform_novel_id=novel_id)
                self.logger.debug(f"[章节数查询][FINAL] platform={platform} novel_id={novel_id} -> {count}")
                return int(count or 0)
            except Exception:
                return 0

        # 兼容旧 handler（如果你未来换实现）
        for method_name in ["get_chapter_count", "count_first_n_chapters", "get_first_n_chapters_count"]:
            if hasattr(self.db_handler, method_name):
                try:
                    method = getattr(self.db_handler, method_name)
                    try:
                        return int(method(platform, novel_id) or 0)
                    except TypeError:
                        return int(method(novel_id) or 0)
                except Exception:
                    continue

        return 0

    """获取数据库中已有章节"""
    def _get_existing_chapters(self, novel_id: str, limit: int):
        if not self.db_handler:
            return []

        platform = getattr(self, "platform", None) or getattr(self, "site_key", None) or "qidian"

        for name in ("get_first_n_chapters", "get_novel_chapters"):
            fn = getattr(self.db_handler, name, None)
            if not fn:
                continue
            try:
                return fn(platform, novel_id, limit)
            except TypeError:
                try:
                    return fn(novel_id, limit)
                except Exception:
                    pass
            except Exception:
                pass

        return []

    """格式化已有章节数据"""
    def _format_existing_chapters(
            self,
            existing_chapters: List[Dict[str, Any]],
            target_count: int,
            publish_date: str = "",
    ) -> List[Dict[str, Any]]:
        chapters: List[Dict[str, Any]] = []
        for i, ch in enumerate(existing_chapters[:target_count], 1):
            chapters.append({
                "chapter_num": int(ch.get("chapter_num") or i),
                "chapter_title": ch.get("chapter_title", f"第{i}章"),
                "chapter_content": ch.get("chapter_content", ""),
                "chapter_url": ch.get("chapter_url", ""),
                "word_count": int(ch.get("word_count") or 0),
                "publish_date": ch.get("publish_date") or publish_date or "",
            })
        # 按 chapter_num 排一下，避免 DB 返回乱序
        chapters.sort(key=lambda x: x["chapter_num"])

        return chapters

    """合并已有章节和新章节"""
    def _merge_chapters(
            self,
            existing_chapters: List[Dict[str, Any]],
            new_chapters: List[Dict[str, Any]],
            target_count: int,
            publish_date: str = "",
    ) -> List[Dict[str, Any]]:
        all_chapters: List[Dict[str, Any]] = []

        # 先把已有章节标准化（如果外面已经标准化了也不影响）
        formatted_existing = self._format_existing_chapters(existing_chapters, target_count, publish_date)

        # new_chapters 假设已是标准 schema；若 publish_date 为空也兜底
        formatted_new: List[Dict[str, Any]] = []
        for ch in new_chapters:
            formatted_new.append({
                "chapter_num": int(ch.get("chapter_num") or 0),
                "chapter_title": ch.get("chapter_title", ""),
                "chapter_content": ch.get("chapter_content", ""),
                "chapter_url": ch.get("chapter_url", ""),
                "word_count": int(ch.get("word_count") or 0),
                "publish_date": ch.get("publish_date") or publish_date or "",
            })

        all_chapters.extend(formatted_existing)
        all_chapters.extend(formatted_new)

        # 排序 + 只取 target_count + 重编号
        all_chapters.sort(key=lambda x: x["chapter_num"])
        all_chapters = all_chapters[:target_count]
        for idx, ch in enumerate(all_chapters, 1):
            ch["chapter_num"] = idx
        return all_chapters

    def db_get_chapter_count(db: Any, *, platform: str, platform_novel_id: str) -> int:
        """
        Return number of stored chapters for (platform, platform_novel_id) in first_n_chapters.
        Works for SQLite handler exposing .conn or .cursor().
        """
        if not db or not platform_novel_id:
            return 0
        try:
            conn = getattr(db, "conn", None)
            if conn is None and hasattr(db, "get_connection"):
                conn = db.get_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(1) FROM first_n_chapters WHERE platform=? AND platform_novel_id=?",
                (platform, platform_novel_id),
            )
            row = cur.fetchone()
            return int(row[0] or 0)
        except Exception:
            return 0

    def db_get_max_chapter_index(db: Any, *, platform: str, platform_novel_id: str) -> int:
        """
        Return max chapter_index (or chapter_no) stored, if column exists.
        If schema differs, returns 0 (safe fallback).
        """
        if not db or not platform_novel_id:
            return 0
        try:
            conn = getattr(db, "conn", None)
            if conn is None and hasattr(db, "get_connection"):
                conn = db.get_connection()
            cur = conn.cursor()

            # try common column names
            for col in ("chapter_index", "chapter_no", "idx", "chapter_order"):
                try:
                    cur.execute(
                        f"SELECT MAX({col}) FROM first_n_chapters WHERE platform=? AND platform_novel_id=?",
                        (platform, platform_novel_id),
                    )
                    row = cur.fetchone()
                    if row and row[0] is not None:
                        return int(row[0])
                except Exception:
                    continue
            return 0
        except Exception:
            return 0

    def _db_has_enough_opening_chapters(self, platform_novel_id: str, target_chapter_count: int) -> bool:
        """
        若 DB 中该书已存的 first_n_chapters 数量 >= target，则返回 True。
        用于在 rank 页直接短路，跳过 detail page。
        """
        if not platform_novel_id or not self.db_handler:
            return False
        if not hasattr(self.db_handler, "get_first_n_chapter_count"):
            return False

        try:
            cnt = int(self.db_handler.get_first_n_chapter_count(
                platform="fanqie",
                platform_novel_id=str(platform_novel_id).strip()
            ) or 0)
            return cnt >= int(target_chapter_count or 0)
        except Exception as e:
            # DB 查询失败时，不做短路，继续走原逻辑（更安全）
            self.logger.debug(f"[prefetch-skip] get_first_n_chapter_count failed id={platform_novel_id}: {e}")
            return False


class MockResponse:
    """模拟requests.Response对象，用于测试"""
    def __init__(self, html: str, encoding: str = 'utf-8'):
        self.text = html
        self.content = html.encode(encoding) if isinstance(html, str) else html
        self.status_code = 200
        self.encoding = encoding