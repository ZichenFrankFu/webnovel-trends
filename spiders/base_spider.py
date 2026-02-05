# spiders/base_spider.py
from __future__ import annotations

import os
import time
import json
import logging
import random
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


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
        self.site_config = site_config
        self.name = site_config.get('name', 'unknown')
        self.base_url = site_config.get('base_url', '')
        self.request_delay = site_config.get('request_delay', 2.0)
        self.max_retries = site_config.get('max_retries', 3)

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

    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger(f'{self.name}_spider')
        logger.setLevel(logging.INFO)

        # 如果已经配置过处理器，则直接返回
        if logger.handlers:
            return logger

        # 确保日志目录存在
        log_dir = 'outputs/logs'
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

    def _build_selenium_config(self) -> Dict[str, Any]:
        """构建Selenium配置

        合并默认配置和站点特定配置
        """
        # 默认配置
        config = {
            "enabled": True,
            "browser": "chrome",
            "options": {
                "headless": True,
                "no_sandbox": True,
                "disable_dev_shm_usage": True,
                "disable_gpu": True,
                "window_size": "1920,1080",
                "disable_blink_features": "AutomationControlled",
                "user_agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
            "experimental_options": {
                "excludeSwitches": ["enable-automation"],
                "useAutomationExtension": False,
            },
            "timeout": 15,
            "implicit_wait": 10,
            "page_load_timeout": 30,
            "stealth_mode": True,
            "driver_path": None,
        }

        # 更新站点特定配置
        site_specific = self.site_config.get("selenium_specific", {}) or {}
        for k, v in site_specific.items():
            if k in config and isinstance(config[k], dict) and isinstance(v, dict):
                config[k].update(v)
            else:
                config[k] = v

        return config

    def _init_driver(self) -> bool:
        """初始化Chrome WebDriver

        Returns:
            bool: 是否初始化成功
        """
        try:
            options = Options()
            cfg_options = self.selenium_config.get("options", {})

            # 设置Chrome选项
            if cfg_options.get("headless", True):
                options.add_argument("--headless=new")

            if "window_size" in cfg_options:
                options.add_argument(f"--window-size={cfg_options['window_size']}")

            if cfg_options.get("user_agent"):
                options.add_argument(f"user-agent={cfg_options['user_agent']}")

            # 添加其他选项
            for k, v in cfg_options.items():
                if k in {"headless", "window_size", "user_agent"}:
                    continue
                if isinstance(v, bool) and v:
                    options.add_argument(f"--{k.replace('_', '-')}")
                elif isinstance(v, str):
                    options.add_argument(f"--{k.replace('_', '-')}={v}")

            # 实验性选项
            for k, v in (self.selenium_config.get("experimental_options", {}) or {}).items():
                options.add_experimental_option(k, v)

            # 性能优化：禁用图片和CSS
            prefs = {
                "profile.default_content_setting_values": {
                    "images": 2,
                    "stylesheet": 2,
                    "javascript": 1,
                }
            }
            options.add_experimental_option("prefs", prefs)

            # 反检测
            if self.selenium_config.get("stealth_mode", True):
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option("useAutomationExtension", False)

            # 驱动路径
            driver_path = self.selenium_config.get("driver_path")
            if driver_path and os.path.exists(driver_path):
                service = Service(driver_path)
            else:
                service = Service(ChromeDriverManager().install())

            # 创建驱动
            self.driver = webdriver.Chrome(service=service, options=options)

            # 设置超时
            self.driver.set_page_load_timeout(
                int(self.selenium_config.get("page_load_timeout", 30))
            )
            self.driver.implicitly_wait(
                int(self.selenium_config.get("implicit_wait", 10))
            )

            # 执行反检测脚本
            if self.selenium_config.get("stealth_mode", True):
                self.driver.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )

            self.logger.info(f"{self.name} Selenium driver initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Selenium driver initialization failed: {e}")
            if self.retry_count < self.max_retries:
                self.retry_count += 1
                time.sleep(2)
                return self._init_driver()
            return False

    def _sleep_human(self, min_time: float = 1.0, max_time: float = 3.0) -> None:
        """模拟人类操作的随机延迟"""
        time.sleep(random.uniform(min_time, max_time))

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

    def _save_raw_data(self, data: Any, filename: str) -> None:
        """保存原始数据到文件

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

    def _normalize_text(self, text: str) -> str:
        """标准化文本：去除多余空白字符"""
        if not text:
            return ""
        return ' '.join(text.split())

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

    def _get_soup(self, url: str, wait_css: Optional[str] = None,
                  wait_sec: int = 10) -> Optional[Any]:
        """使用Selenium获取页面并返回BeautifulSoup对象

        Args:
            url: 目标URL
            wait_css: 等待的CSS选择器
            wait_sec: 等待超时时间

        Returns:
            Optional[Any]: BeautifulSoup对象，失败返回None
        """
        if not self.driver:
            self.logger.error("Selenium driver not initialized")
            return None

        try:
            self.logger.debug(f"Fetching URL: {url}")
            self.driver.get(url)
            self._sleep_human(1, 2)

            # 等待特定元素加载
            if wait_css:
                try:
                    WebDriverWait(self.driver, wait_sec).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                    )
                except Exception as e:
                    self.logger.debug(f"Wait for element timeout: {wait_css}, error: {e}")

            # 导入BeautifulSoup
            from bs4 import BeautifulSoup
            return BeautifulSoup(self.driver.page_source, 'html.parser')

        except Exception as e:
            self.logger.error(f"Failed to fetch page {url}: {e}")
            return None

    @abstractmethod
    def fetch_rank_list(self, rank_type: str = 'hot', pages: int = 5) -> List[Dict[str, Any]]:
        """获取榜单数据

        Args:
            rank_type: 榜单类型
            pages: 爬取页数

        Returns:
            List[Dict[str, Any]]: 榜单数据列表
        """
        pass

    @abstractmethod
    def fetch_novel_detail(self, novel_url: str, novel_id: str = '') -> Dict[str, Any]:
        """获取小说详情

        Args:
            novel_url: 小说URL
            novel_id: 小说ID

        Returns:
            Dict[str, Any]: 小说详情数据
        """
        pass

    @abstractmethod
    def enrich_books_with_details(self, books: List[Dict[str, Any]],
                                  max_books: int = 20) -> List[Dict[str, Any]]:
        """使用详情数据丰富书籍信息

        Args:
            books: 书籍列表
            max_books: 最大处理书籍数

        Returns:
            List[Dict[str, Any]]: 丰富后的书籍列表
        """
        pass

    @abstractmethod
    def fetch_whole_rank(self) -> List[Dict[str, Any]]:
        """获取所有配置的榜单数据

        Returns:
            List[Dict[str, Any]]: 所有榜单数据
        """
        pass

    @abstractmethod
    def fetch_first_n_chapters(self, novel_url: str, n: int = 5) -> List[Dict[str, Any]]:
        """获取小说前N章内容

        Args:
            novel_url: 小说URL
            n: 章节数量

        Returns:
            List[Dict[str, Any]]: 章节数据列表
        """
        pass

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
                self._sleep_human(3, 5)

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


class MockResponse:
    """模拟requests.Response对象，用于测试"""

    def __init__(self, html: str, encoding: str = 'utf-8'):
        self.text = html
        self.content = html.encode(encoding) if isinstance(html, str) else html
        self.status_code = 200
        self.encoding = encoding