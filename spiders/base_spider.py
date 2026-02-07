# spiders/base_spider.py
from __future__ import annotations

import copy
import os
import time
import json
import logging
import random
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union, Sequence
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import config


class BaseSpider(ABC):
    """网络小说平台爬虫基类

    为起点中文网、番茄小说等平台提供统一的爬虫接口和基础功能。
    支持Selenium自动化爬取，处理反爬机制，日志记录等功能。
    """

    def __init__(self, site_config: Dict[str, Any], db_handler: Any = None):
        """初始化爬虫

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

        # 缓存
        self.book_cache: Dict[str, Dict[str, Any]] = {}
        self.retry_count = 0

        # Selenium配置
        self.selenium_config = self._build_selenium_config()

        # 初始化Selenium驱动
        if self.selenium_config.get('enabled', True):
            self._init_driver()

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
    # Selenium and webdriver 初始化
    # ------------------------------------------------------------------
    def _build_selenium_config(self) -> Dict[str, Any]:
        """
        Merge: config.SELENIUM_CONFIG (global default)
             + self.site_config['selenium_specific'] (site override)
        """
        base = getattr(config, "SELENIUM_CONFIG", {}) or {}
        site_specific = (self.site_config or {}).get("selenium_specific", {}) or {}
        return self._deep_merge_dict(base, site_specific)

    def _init_driver(self) -> bool:
        try:
            cfg = self.selenium_config or {}
            if not cfg.get("enabled", True):
                self.logger.info("Selenium disabled by config.")
                return False

            browser = (cfg.get("browser") or "chrome").lower()
            if browser != "chrome":
                raise ValueError(f"Unsupported browser: {browser}")

            options = Options()

            # ----- chrome arguments from cfg["options"] -----
            opt_cfg = cfg.get("options", {}) or {}

            headless = bool(opt_cfg.get("headless", True))
            if headless:
                options.add_argument("--headless=new")

            if opt_cfg.get("window_size"):
                options.add_argument(f"--window-size={opt_cfg['window_size']}")

            if opt_cfg.get("user_agent"):
                options.add_argument(f"user-agent={opt_cfg['user_agent']}")

            # add all remaining items as flags
            for k, v in opt_cfg.items():
                if k in {"headless", "window_size", "user_agent"}:
                    continue
                flag = f"--{k.replace('_', '-')}"
                if isinstance(v, bool):
                    if v:
                        options.add_argument(flag)
                elif isinstance(v, str):
                    # allow either "--k=v" style or special cases
                    options.add_argument(f"{flag}={v}")
                elif v is not None:
                    options.add_argument(f"{flag}={v}")

            # ----- experimental options -----
            for k, v in (cfg.get("experimental_options", {}) or {}).items():
                options.add_experimental_option(k, v)

            # ----- prefs (perf etc.) -----
            prefs = cfg.get("prefs")
            if isinstance(prefs, dict) and prefs:
                options.add_experimental_option("prefs", prefs)

            # ----- stealth -----
            stealth = cfg.get("stealth", {}) or {}
            if stealth.get("enabled", True):
                if stealth.get("disable_blink_features"):
                    options.add_argument(
                        f"--disable-blink-features={stealth['disable_blink_features']}"
                    )
                # allow overriding these in stealth block
                if stealth.get("excludeSwitches"):
                    options.add_experimental_option("excludeSwitches", stealth["excludeSwitches"])
                if "useAutomationExtension" in stealth:
                    options.add_experimental_option("useAutomationExtension", stealth["useAutomationExtension"])

            # ----- driver service -----
            driver_path = cfg.get("driver_path")
            auto_install = bool(cfg.get("auto_install_driver", True))

            if driver_path and os.path.exists(driver_path):
                service = Service(driver_path)
            else:
                if not auto_install:
                    raise FileNotFoundError(
                        "driver_path not found and auto_install_driver is False"
                    )
                if ChromeDriverManager is None:
                    raise RuntimeError(
                        "webdriver_manager not installed, cannot auto install chromedriver"
                    )
                service = Service(ChromeDriverManager().install())

            self.driver = webdriver.Chrome(service=service, options=options)

            self.driver.set_page_load_timeout(int(cfg.get("page_load_timeout", 30)))
            self.driver.implicitly_wait(int(cfg.get("implicit_wait", 10)))

            # webdriver undefined script
            script = stealth.get("webdriver_undefined_script")
            if stealth.get("enabled", True) and script:
                self.driver.execute_script(script)

            self.logger.info(f"{self.__class__.__name__} Selenium driver initialized.")
            return True

        except Exception as e:
            self.logger.error(f"Selenium init failed: {e}")

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
    # Fetch Webpage vis BeautifulSoup
    # ------------------------------------------------------------------
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
        """
        if not self.driver:
            self.logger.error("Selenium driver not initialized")
            return None

        # ---- config defaults (global + site override) ----
        crawler_cfg = getattr(self.config, "CRAWLER_CONFIG", {}) or {}
        global_fetch = (crawler_cfg.get("page_fetch", {}) or {})
        site_fetch = (self.site_config or {}).get("page_fetch_overrides", {}) or {}
        cfg = {**global_fetch, **site_fetch}

        _wait_sec = int(wait_sec if wait_sec is not None else cfg.get("default_wait_sec", 10))
        _max_retries = int(max_retries if max_retries is not None else cfg.get("max_page_retries", 2))
        _retry_delay = float(retry_delay if retry_delay is not None else cfg.get("page_retry_delay", 3))

        post_load_delay = cfg.get("post_load_delay_range", (1, 2))
        if not isinstance(post_load_delay, (list, tuple)) or len(post_load_delay) != 2:
            post_load_delay = (1, 2)

        min_html_length = int(cfg.get("min_html_length", 800))
        bad_title_keywords = cfg.get("bad_title_keywords", ["404", "无法访问", "出错了"])

        total_attempts = _max_retries + 1

        for attempt in range(1, total_attempts + 1):
            try:
                self.logger.info(f"[页面获取] 尝试 {attempt}/{total_attempts}: {url}")

                # page load
                try:
                    self.driver.set_page_load_timeout(_wait_sec)
                except Exception:
                    pass

                self.driver.get(url)
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

                if len(html) < min_html_length:
                    raise ValueError(f"Page source too short: {len(html)}")

                soup = BeautifulSoup(html, "html.parser")

                title = ""
                try:
                    title = (soup.title.string if soup.title else "") or ""
                except Exception:
                    title = ""

                if any(k in title for k in bad_title_keywords):
                    raise ValueError(f"Bad page title: {title}")

                return soup

            except Exception as e:
                self.logger.warning(f"[页面获取] 失败 {attempt}/{total_attempts}: {url} ; error={e}")

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
        return

    """_get_soup Hook: site-specific html processing (e.g., decrypt)"""
    def _postprocess_html(self, html: str) -> str:
        return html

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
                    snapshot_id = self.db_handler.save_rank_snapshot(
                        platform=self.name,
                        rank_family=rank_type,
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
        """
        Args:
            novel_id: 小说平台ID

        Returns:
            数据库中已有的章节数量，如果查询失败返回0
        """
        if not self.db_handler or not hasattr(self.db_handler, 'get_chapters_count'):
            return 0

        try:
            count = self.db_handler.get_chapters_count(novel_id)
            self.logger.info(f"[章节智能补全] 小说 {novel_id} 在数据库中已有 {count} 章")
            return count
        except Exception as e:
            self.logger.warning(f"[章节智能补全] 查询已有章节数失败: {e}")
            return 0

    """获取数据库中已有章节"""
    def _get_existing_chapters(self, novel_id: str, limit: int) -> List[Dict[str, Any]]:
        """获取数据库中已有章节

        Args:
            novel_id: 小说平台ID
            limit: 最大获取章节数

        Returns:
            已有章节列表
        """
        if not self.db_handler or not hasattr(self.db_handler, 'get_novel_chapters'):
            return []

        try:
            chapters = self.db_handler.get_novel_chapters(novel_id, limit)
            self.logger.info(f"[章节智能补全] 从数据库获取小说 {novel_id} 的 {len(chapters)} 个章节")
            return chapters
        except Exception as e:
            self.logger.warning(f"[章节智能补全] 获取已有章节失败: {e}")
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

class MockResponse:
    """模拟requests.Response对象，用于测试"""
    def __init__(self, html: str, encoding: str = 'utf-8'):
        self.text = html
        self.content = html.encode(encoding) if isinstance(html, str) else html
        self.status_code = 200
        self.encoding = encoding