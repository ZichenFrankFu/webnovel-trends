# spiders/fanqie_spider.py
from __future__ import annotations

import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import config
from .base_spider import BaseSpider
from .fanqie_font_decoder import FANQIE_CHAR_MAP

GLOBAL_SELENIUM_CONFIG = getattr(config, "SELENIUM_CONFIG", {}) or {}


@dataclass(frozen=True)
class RankIdentity:
    """Normalized rank identity that maps to RANK_LISTS schema."""
    rank_family: str
    rank_sub_cat: str = ""  # 番茄小说目前没有子分类


class FanqieSpider(BaseSpider):
    """番茄小说爬虫

    该 Spider 主要负责：
    1) 抓取榜单页（可多页）
    2) 抓取详情页（补全元信息：分类/状态/字数/简介等）
    3) 可选抓取前 N 章（FIRST_N_CHAPTERS）
    4) 输出"可直接落库"的标准化结构；如注入 db_handler 可直接写入数据库

    重要接口（BaseSpider 抽象方法要求）：
    - fetch_rank_list
    - fetch_novel_detail
    - enrich_books_with_details
    - fetch_whole_rank
    """

    def __init__(self, site_config: Dict[str, Any], db_handler: Any = None):
        """Initialize Fanqie spider.

        Args:
            site_config: Site configuration dict. Key fields:
                - base_url: str
                - rank_urls: dict[str, str] (url template supports {page})
                - pages_per_rank: int
                - chapter_extraction_goal: int
                - selenium_specific: optional selenium overrides
            db_handler: Optional DB handler. If provided and exposes:
                - save_rank_snapshot(...)
                - upsert_first_n_chapters(...)
              then this spider can persist results directly.
        """
        super().__init__(site_config)

        self.driver: Optional[webdriver.Chrome] = None
        self.db_handler = db_handler

        self.book_cache: Dict[str, Dict[str, Any]] = {}
        self.retry_count = 0
        self.max_retries = int(site_config.get("max_retries", 3))

        self.selenium_config = self._build_selenium_config()
        self._init_driver()

        self.default_chapter_count = int(site_config.get("chapter_extraction_goal", 5))
        self.rank_type_map: Dict[str, RankIdentity] = self._build_rank_type_map()

        # 字体解密映射
        self.char_map = FANQIE_CHAR_MAP

    # ------------------------------------------------------------------
    # Selenium setup
    # ------------------------------------------------------------------
    def _build_selenium_config(self) -> Dict[str, Any]:
        """Merge default Selenium config + global config + site specific overrides."""
        cfg: Dict[str, Any] = {
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
            "timeout": 20,
            "implicit_wait": 10,
            "page_load_timeout": 30,
            "stealth_mode": True,
            "driver_path": None,
        }

        cfg.update(GLOBAL_SELENIUM_CONFIG)

        site_specific = self.site_config.get("selenium_specific", {}) or {}
        for k, v in site_specific.items():
            if k in cfg and isinstance(cfg[k], dict) and isinstance(v, dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
        return cfg

    def _init_driver(self) -> bool:
        """Initialize Chrome WebDriver with anti-bot friendly options.

        Returns:
            bool: True if driver is initialized, False otherwise.
        """
        try:
            options = Options()
            cfg_options = self.selenium_config.get("options", {}) or {}

            if cfg_options.get("headless", True):
                options.add_argument("--headless=new")

            if "window_size" in cfg_options:
                options.add_argument(f"--window-size={cfg_options['window_size']}")
            if cfg_options.get("user_agent"):
                options.add_argument(f"user-agent={cfg_options['user_agent']}")
            for k, v in cfg_options.items():
                if k in {"headless", "window_size", "user_agent"}:
                    continue
                if isinstance(v, bool) and v:
                    options.add_argument(f"--{k.replace('_', '-')}")
                elif isinstance(v, str):
                    options.add_argument(f"--{k.replace('_', '-')}={v}")

            for k, v in (self.selenium_config.get("experimental_options", {}) or {}).items():
                options.add_experimental_option(k, v)

            # perf: disable images/css
            prefs = {
                "profile.default_content_setting_values": {
                    "images": 2,
                    "stylesheet": 2,
                    "javascript": 1,
                }
            }
            options.add_experimental_option("prefs", prefs)

            if self.selenium_config.get("stealth_mode", True):
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option("useAutomationExtension", False)

            driver_path = self.selenium_config.get("driver_path")
            if driver_path and os.path.exists(driver_path):
                service = Service(driver_path)
            else:
                service = Service(ChromeDriverManager().install())

            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.set_page_load_timeout(int(self.selenium_config.get("page_load_timeout", 30)))
            self.driver.implicitly_wait(int(self.selenium_config.get("implicit_wait", 10)))

            if self.selenium_config.get("stealth_mode", True):
                self.driver.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )

            self.logger.info("FanqieSpider Selenium driver initialized.")
            return True
        except Exception as e:
            self.logger.error(f"Selenium init failed: {e}")
            if self.retry_count < self.max_retries:
                self.retry_count += 1
                time.sleep(1)
                return self._init_driver()
            return False

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------

    def _decrypt_text(self, text: str) -> str:
        """解密字体加密的文本"""
        if not text:
            return text

        result = []
        for char in text:
            if char in self.char_map:
                result.append(self.char_map[char])
            else:
                result.append(char)

        return ''.join(result)

    def _decrypt_html(self, html: str) -> str:
        """解密HTML中的所有加密文字"""
        for encrypted_char, real_char in self.char_map.items():
            if encrypted_char != real_char:
                html = html.replace(encrypted_char, real_char)

        return html

    def _extract_novel_id_from_url(self, url: str) -> str:
        """Extract Fanqie novel id (digits) from URL."""
        patterns = [
            r"/book/(\d+)",
            r"/page/(\d+)",
            r"/reader/(\d+)",
            r"fanqienovel\.com/page/(\d+)",
            r"fanqienovel\.com/book/(\d+)",
        ]
        for p in patterns:
            m = re.search(p, url or "")
            if m:
                return m.group(1)
        for part in (url or "").split("/"):
            if part.isdigit() and len(part) >= 6:
                return part
        return ""

    def _find_existing_novel_by_metadata(self, author: str, intro: str, title: str = "") -> Optional[Dict[str, Any]]:
        """通过作者和简介查找已存在的小说

        Args:
            author: 作者名
            intro: 简介（使用前200字符进行匹配）
            title: 小说标题（可选）

        Returns:
            找到的小说信息，或None
        """
        if not self.db_handler:
            return None

        try:
            # 简化的简介匹配（使用前200字符）
            intro_snippet = intro[:200] if intro else ""

            # 尝试通过作者和简介查找
            if hasattr(self.db_handler, 'find_novel_by_author_and_intro'):
                return self.db_handler.find_novel_by_author_and_intro(author, intro_snippet)

            # 如果找不到相应方法，返回None
            return None

        except Exception as e:
            self.logger.debug(f"通过元数据查找小说失败: {e}")
            return None

    def _add_enter_from_param(self, url: str) -> str:
        """给URL添加 enter_from=Rank 参数

        Args:
            url: 原始URL

        Returns:
            添加了参数的URL
        """
        if not url:
            return url

        # 检查URL是否已经有查询参数
        if '?' in url:
            # 检查是否已经包含 enter_from 参数
            if 'enter_from=' in url:
                # 如果已经包含，保持原样
                return url
            else:
                # 添加 enter_from=Rank 参数
                return f"{url}&enter_from=Rank"
        else:
            # 没有查询参数，直接添加
            return f"{url}?enter_from=Rank"

    # ------------------------------------------------------------------
    # Rank type mapping (rank_family / rank_sub_cat)
    # ------------------------------------------------------------------
    def _build_rank_type_map(self) -> Dict[str, RankIdentity]:
        """Build mapping from config rank_type to normalized RankIdentity."""
        custom = self.site_config.get("rank_type_map")
        if isinstance(custom, dict) and custom:
            out: Dict[str, RankIdentity] = {}
            for k, v in custom.items():
                out[k] = RankIdentity(
                    rank_family=v.get("rank_family", k),
                    rank_sub_cat=v.get("rank_sub_cat", "") or "",
                )
            return out

        # 番茄小说常见的榜单类型
        return {
            "hot": RankIdentity("热销榜"),
            "new": RankIdentity("新书榜"),
            "recommend": RankIdentity("推荐榜"),
            "collect": RankIdentity("收藏榜"),
            "vip": RankIdentity("VIP榜"),
            "finish": RankIdentity("完结榜"),
        }

    # ------------------------------------------------------------------
    # Page fetching with scroll loading
    # ------------------------------------------------------------------
    def _get_soup_with_scroll(
            self,
            url: str,
            wait_css: Optional[str] = None,
            wait_sec: Optional[int] = None,
            target_count: Optional[int] = None,
            max_scroll_attempts: Optional[int] = None,
            max_retries: Optional[int] = None,
    ) -> Optional[BeautifulSoup]:
        """Fetch URL using Selenium with scroll loading and return BeautifulSoup.

        Args:
            url: target url
            wait_css: optional CSS selector to wait for (presence)
            wait_sec: wait timeout seconds
            target_count: target number of items to load
            max_scroll_attempts: maximum scroll attempts
            max_retries: maximum retry times for page loading

        Returns:
            BeautifulSoup or None on failure.
        """
        if not self.driver:
            self.logger.error("Driver not initialized.")
            return None

        # 从 site_config 的 selenium_specific 中获取滚动配置
        scroll_config = self.site_config.get("selenium_specific", {})

        if wait_sec is None:
            wait_sec = 15  # 默认值
        if target_count is None:
            target_count = int(scroll_config.get("target_count", 30))
        if max_scroll_attempts is None:
            max_scroll_attempts = int(scroll_config.get("max_scroll_attempts", 10))
        if max_retries is None:
            max_retries = int(self.site_config.get("max_retries", 3))

        for retry in range(max_retries):
            try:
                self.logger.info(f"访问页面 (尝试 {retry + 1}/{max_retries}): {url}")

                # 增加页面加载超时时间
                current_timeout = self.driver.timeouts.page_load
                self.driver.set_page_load_timeout(30)  # 临时增加页面加载超时

                self.driver.get(url)
                self._sleep_human(2, 4)  # 增加初始加载等待时间

                # 恢复原始超时设置
                self.driver.set_page_load_timeout(current_timeout)

                if wait_css:
                    try:
                        WebDriverWait(self.driver, wait_sec).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                        )
                        self.logger.debug(f"成功等待到元素: {wait_css}")
                    except Exception as e:
                        self.logger.warning(f"等待元素超时: {wait_css}, 错误: {e}")

                # 滚动加载逻辑
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                no_change_count = 0
                loaded_items = 0

                for attempt in range(max_scroll_attempts):
                    # 滚动到底部
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2.5)  # 增加等待新内容加载的时间

                    # 计算新高度
                    new_height = self.driver.execute_script("return document.body.scrollHeight")

                    # 检查是否滚动到了底部
                    if new_height == last_height:
                        no_change_count += 1
                        if no_change_count >= 3:
                            self.logger.info(f"连续 {no_change_count} 次滚动高度未变化，停止滚动")
                            break
                    else:
                        no_change_count = 0
                        last_height = new_height

                    # 检查是否已达到目标数量
                    try:
                        current_items = self.driver.find_elements(By.CSS_SELECTOR, ".rank-book-item, .book-item")
                        if len(current_items) > loaded_items:
                            loaded_items = len(current_items)
                            self.logger.debug(f"滚动后加载项目数: {loaded_items}")

                        if len(current_items) >= target_count:
                            self.logger.info(f"已达到目标数量 {target_count}，停止滚动")
                            break
                    except Exception as e:
                        self.logger.debug(f"检查项目数时出错: {e}")

                    self.logger.debug(f"滚动第 {attempt + 1} 次，当前高度: {new_height}")
                    self._sleep_human(1, 2)

                # 获取最终页面源代码
                html = self.driver.page_source
                decrypted_html = self._decrypt_html(html)

                # 最终获取所有项目
                try:
                    final_items = self.driver.find_elements(By.CSS_SELECTOR, ".rank-book-item, .book-item")
                    self.logger.info(f"页面加载完成，共找到 {len(final_items)} 个项目")
                except:
                    final_items = []

                return BeautifulSoup(decrypted_html, "html.parser")

            except TimeoutException as e:
                self.logger.warning(f"页面加载超时 (尝试 {retry + 1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    self.logger.info(f"等待 {retry + 1} 秒后重试...")
                    time.sleep(retry + 1)
                    continue
                else:
                    self.logger.error(f"页面加载超时，已达到最大重试次数: {url}")
                    return None

            except Exception as e:
                self.logger.error(f"获取页面失败: {url} ; 错误: {e}")
                if retry < max_retries - 1:
                    self.logger.info(f"等待 {retry + 1} 秒后重试...")
                    time.sleep(retry + 1)
                    continue
                else:
                    return None

        return None

    # ------------------------------------------------------------------
    # Rank Page Parsing
    # ------------------------------------------------------------------
    def _parse_rank_page(self, soup: BeautifulSoup, *, rank_type: str, page: int) -> List[Dict[str, Any]]:
        """Parse one rank page soup into raw rank items."""
        selectors = [
            ".rank-book-item",
            ".book-item",
            ".book-list-item",
            ".rank-item",
        ]

        for sel in selectors:
            nodes = soup.select(sel)
            if not nodes or len(nodes) < 3:
                continue

            out: List[Dict[str, Any]] = []
            for idx, node in enumerate(nodes, 1):
                b = self._parse_rank_item(node, idx=idx, page=page, rank_type=rank_type)
                if b:
                    out.append(b)
            if out:
                return out

        return []

    def _parse_rank_item(self, node: Any, *, idx: int, page: int, rank_type: str) -> Optional[Dict[str, Any]]:
        """Parse one rank item node into a standardized dict."""
        try:
            # 提取标题和URL
            title_elem = node.select_one('.title a, h3 a, .book-title a')
            if not title_elem:
                return None

            title_raw = title_elem.text.strip()
            title = self._decrypt_text(title_raw)

            url = self._to_abs_url(title_elem.get("href", ""))
            if not url:
                return None

            pid = self._extract_novel_id_from_url(url) or f"fanqie_{page}_{idx}"

            # rank (assume 20/page typical)
            global_rank = (page - 1) * 20 + idx

            # 提取作者
            author = "未知"
            author_elem = node.select_one('.author a, .author-name a, .writer a')
            if author_elem:
                author_raw = author_elem.text.strip()
                author = self._decrypt_text(author_raw)

            # 提取简介
            intro = ""
            intro_elem = node.select_one('.desc.abstract, .intro, .description')
            if intro_elem:
                intro_raw = intro_elem.text.strip()
                intro = self._decrypt_text(intro_raw)
                intro = self._normalize_text(intro)

            # 提取状态
            status = "连载中"
            status_elem = node.select_one('.book-item-footer-status, .status, .state')
            if status_elem:
                status_raw = status_elem.text.strip()
                status_text = self._decrypt_text(status_raw)
                if "完结" in status_text or "完本" in status_text:
                    status = "完本"
                else:
                    status = "连载中"

            # 提取在读人数
            reading_count = 0
            reading_count_text = ""
            count_elem = node.select_one('.book-item-count, .read-count, .count')
            if count_elem:
                count_raw = count_elem.text.strip()
                reading_count_text = self._decrypt_text(count_raw)

                # 提取数字部分
                clean_text = reading_count_text.replace('在读：', '').replace('阅读：', '')
                reading_count = self._parse_cn_number(clean_text) or 0

            # 提取分类/标签
            tags = []
            tag_elements = node.select('.tag, .tags span, .label')
            for tag_elem in tag_elements:
                tag_raw = tag_elem.text.strip()
                tag = self._decrypt_text(tag_raw)
                if tag and tag not in tags:
                    tags.append(tag)

            # 主分类（第一个标签或默认）
            main_category = tags[0] if tags else ""

            # 提取封面
            cover_url = ""
            cover_elem = node.select_one('.book-cover-img, .cover img')
            if cover_elem:
                cover_url = cover_elem.get('src', '')

            # 提取最新章节
            last_chapter = ""
            last_chapter_elem = node.select_one('.chapter, .latest-chapter')
            if last_chapter_elem:
                chapter_raw = last_chapter_elem.text.strip()
                last_chapter = self._decrypt_text(chapter_raw)

            # 提取更新时间
            update_time = ""
            update_time_elem = node.select_one('.book-item-footer-time, .update-time')
            if update_time_elem:
                time_raw = update_time_elem.text.strip()
                update_time = self._decrypt_text(time_raw)

            return {
                "platform": "fanqie",
                "platform_novel_id": pid,
                "title": title,
                "author": author,
                "intro": intro,
                "main_category": main_category,
                "tags": tags,
                "status": status,
                "total_words": 0,  # detail page fills
                "url": url,
                "rank": global_rank,
                "reading_count": reading_count,
                "reading_count_text": reading_count_text,
                "rank_type": rank_type,
                "cover_url": cover_url,
                "last_chapter": last_chapter,
                "update_time": update_time,
                "fetch_date": self._today_str(),
                "fetch_time": time.strftime('%H:%M:%S'),
            }

        except Exception as e:
            self.logger.debug(f"parse rank item failed: {e}")
            return None

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_rank_list
    # ------------------------------------------------------------------
    def fetch_rank_list(self, rank_type: str = "", pages: int = 5) -> List[Dict[str, Any]]:
        """Fetch a rank list (multi-page) and return standardized items.

        Args:
            rank_type: key in site_config["rank_urls"]

        Returns:
            List of dicts (raw rank items). Each dict includes:
            - platform, platform_novel_id, title, author, intro
            - main_category, tags
            - status, total_words, url
            - rank, reading_count
        """
        url_template = (self.site_config.get("rank_urls") or {}).get(rank_type)
        if not url_template:
            self.logger.error(f"rank_type not configured in rank_urls: {rank_type}")
            return []

        pages = int(self.site_config.get("pages_per_rank", 5))
        all_items: List[Dict[str, Any]] = []

        for page in range(1, pages + 1):
            url = url_template.format(page=page) if "{page}" in url_template else url_template
            if page > 1 and "{page}" not in url_template:
                break  # 如果没有分页参数，只抓取第一页

            self.logger.info(f"Rank[{rank_type}] page {page}/{pages}: {url}")

            soup = self._get_soup_with_scroll(
                url,
                wait_css=".rank-book-item, .book-item",
                wait_sec=15,
                target_count=30,
                max_scroll_attempts=10,
            )

            if not soup:
                continue

            page_items = self._parse_rank_page(soup, rank_type=rank_type, page=page)

            # 去重
            seen_urls = set()
            unique_items = []
            for item in page_items:
                url = item.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_items.append(item)

            all_items.extend(unique_items)
            self.logger.info(f"Page {page}: found {len(page_items)} items, {len(unique_items)} unique")

            if page < pages:
                self._sleep_human(1, 3)

        return all_items

    # ------------------------------------------------------------------
    # Detail Page Parsing
    # ------------------------------------------------------------------
    def _fill_detail_title_author_intro(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        """Fill title/author/intro fields from detail page soup."""
        # 提取标题
        if not detail.get("title"):
            title_elem = soup.select_one('meta[property="og:title"]') or soup.select_one(
                'h1, .info-name h1, .book-title, header h1'
            )
            if title_elem:
                if title_elem.name == "meta":
                    t = (title_elem.get("content", "") or "").strip()
                    detail["title"] = self._decrypt_text(t)
                else:
                    title_raw = title_elem.text.strip()
                    detail["title"] = self._decrypt_text(title_raw)

        # 提取作者
        if not detail.get("author"):
            author_elem = soup.select_one('meta[property="og:novel:author"]') or soup.select_one(
                '.author-name:not(.author-desc), .author-name-text:not(.author-desc)'
            )
            if author_elem:
                if author_elem.name == "meta":
                    detail["author"] = (author_elem.get("content", "") or "").strip()
                else:
                    author_raw = author_elem.text.strip()
                    detail["author"] = self._decrypt_text(author_raw)

        # 提取简介
        if not detail.get("intro"):
            intro_elem = soup.select_one('meta[property="og:description"]') or soup.select_one(
                '.intro, .description, .book-intro, .content'
            )
            if intro_elem:
                if intro_elem.name == "meta":
                    intro = (intro_elem.get("content", "") or "").strip()
                    detail["intro"] = self._decrypt_text(intro)
                else:
                    intro_raw = intro_elem.text.strip()
                    detail["intro"] = self._decrypt_text(intro_raw)

    def _fill_detail_category_tags(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        """Fill main_category and tags from detail page soup."""
        # 如果已经有主分类且不是空，则跳过详情页分类提取
        current_main = detail.get("main_category", "")
        current_tags = detail.get("tags", [])

        if current_main:
            self.logger.info(f"[分类处理] 已有分类信息，跳过提取 - main='{current_main}', tags={current_tags}")
            return

        self.logger.info(f"[分类处理] 开始处理分类信息...")

        tags = []

        # 提取标签 - 番茄小说通常使用 info-label-grey 作为标签
        tag_elements = soup.select('.info-label-grey')

        # 如果找到了标签元素
        if tag_elements:
            for tag_elem in tag_elements:
                tag_raw = tag_elem.text.strip()
                tag = self._decrypt_text(tag_raw)
                if tag and tag not in tags and len(tag) < 20:
                    tags.append(tag)
        else:
            # 方法2: 尝试从info-label容器中提取
            info_label_container = soup.select_one('.info-label')
            if info_label_container:
                # 获取所有span，排除第一个info-label-yellow（状态）
                all_spans = info_label_container.find_all('span')
                for span in all_spans:
                    if 'info-label-yellow' not in span.get('class', []):
                        tag_raw = span.text.strip()
                        tag = self._decrypt_text(tag_raw)
                        if tag and tag not in tags and len(tag) < 20:
                            tags.append(tag)

        # 更新主分类（第一个标签）
        if tags:
            detail["main_category"] = tags[0]
        else:
            detail["main_category"] = ""

        # 合并标签
        existing_tags = set(detail.get("tags", []))
        new_tags = set(tags)
        merged_tags = list(existing_tags.union(new_tags))

        # 确保主分类不作为标签
        main_cat = detail.get("main_category", "")
        if main_cat in merged_tags:
            merged_tags.remove(main_cat)

        detail["tags"] = self._dedupe_keep_order(merged_tags)
        self.logger.info(f"[分类处理] 最终结果 - main='{detail['main_category']}', tags={detail['tags']}")

    def _fill_detail_status_words(self, soup: BeautifulSoup, detail: Dict[str, Any], page_url: str = "") -> None:
        """Fill normalized status (ongoing/completed) and total_words from detail page."""
        if page_url:
            self.logger.info(f"[数据补完] 正在从详情页获取完本/连载状态以及总字数: {page_url}")

        try:
            # 提取状态
            if not detail.get("status"):
                # 方法1: 从info-label-yellow提取
                status_elem = soup.select_one('.info-label-yellow')
                if status_elem:
                    status_raw = status_elem.text.strip()
                    status_text = self._decrypt_text(status_raw)
                    if '完结' in status_text:
                        detail["status"] = "完本"
                    elif '连载' in status_text:
                        detail["status"] = "连载中"
                    else:
                        detail["status"] = status_text
                else:
                    # 方法2: 回退到旧的选择器
                    status_selectors = ['.book-state, .status, .state']
                    for selector in status_selectors:
                        status_elem = soup.select_one(selector)
                        if status_elem:
                            status_raw = status_elem.text.strip()
                            status_text = self._decrypt_text(status_raw)
                            if '完结' in status_text:
                                detail["status"] = "完本"
                            elif '连载' in status_text:
                                detail["status"] = "连载中"
                            break

            # 提取总字数
            if not detail.get("total_words"):
                total_words = 0
                # 方法1: 使用新的HTML结构
                word_count_elem = soup.select_one('.info-count-word')
                if word_count_elem:
                    # 提取数字部分和单位
                    detail_elem = word_count_elem.select_one('.detail')
                    text_elem = word_count_elem.select_one('.text')

                    if detail_elem and text_elem:
                        detail_raw = detail_elem.text.strip()
                        detail_text = self._decrypt_text(detail_raw)
                        unit_raw = text_elem.text.strip()
                        unit_text = self._decrypt_text(unit_raw)

                        try:
                            num = float(detail_text)
                            # 根据单位转换
                            if '万' in unit_text:
                                total_words = int(num * 10000)
                            else:
                                total_words = int(num)
                        except ValueError:
                            self.logger.debug(f"无法转换字数: {detail_text}")

                if total_words == 0:
                    # 方法2: 回退到旧的选择器
                    words_selectors = ['.book-info-item, .info-item']
                    for selector in words_selectors:
                        info_elem = soup.select_one(selector)
                        if info_elem:
                            info_raw = info_elem.text.strip()
                            info_text = self._decrypt_text(info_raw)
                            if '字数' in info_text:
                                words_match = re.search(r'字数[：:]\s*([0-9.]+[万亿]?)', info_text)
                                if words_match:
                                    total_words = self._parse_cn_number(words_match.group(1)) or 0
                                break

                detail["total_words"] = total_words

            # 记录提取结果
            self.logger.info(f"小说状态: '{detail.get('status')}', 小说总字数: {detail.get('total_words')}")

        except Exception as e:
            self.logger.error(f"Error extracting status and word count from page: {e}")

    def _extract_first_upload_date(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        """从详情页提取上架时间（首发时间）"""
        try:
            self.logger.debug("开始提取上架时间...")

            # 方法1: 从meta标签提取
            meta_date = soup.select_one('meta[property="article:published_time"], meta[name="publish_date"]')
            if meta_date and meta_date.get('content'):
                date_str = meta_date.get('content')
                # 尝试解析日期格式
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
                if date_match:
                    detail["first_upload_date"] = date_match.group(1)
                    self.logger.info(f"从meta标签提取到上架时间: {date_match.group(1)}")
                    return

            # 方法2: 从页面文本中提取
            page_text = self._normalize_text(soup.get_text())

            # 查找常见日期格式
            date_patterns = [
                r'发布日期[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'更新时间[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'发表时间[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                r'(\d{4})-(\d{1,2})-(\d{1,2})',
            ]

            for pattern in date_patterns:
                match = re.search(pattern, page_text)
                if match:
                    if len(match.groups()) == 3:
                        year, month, day = match.groups()
                        detail["first_upload_date"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif len(match.groups()) == 1:
                        detail["first_upload_date"] = match.group(1)

                    self.logger.info(f"从页面文本提取到上架时间: {detail['first_upload_date']}")
                    return

            # 如果没有找到，返回空字符串
            detail["first_upload_date"] = ""
            self.logger.debug("未能提取到上架时间")

        except Exception as e:
            self.logger.error(f"提取上架时间失败: {e}")
            detail["first_upload_date"] = ""

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_novel_detail
    # ------------------------------------------------------------------
    def fetch_novel_detail(self, novel_url: str, novel_id: str = "") -> Dict[str, Any]:
        """Fetch novel detail page and extract normalized metadata.

        Args:
            novel_url: Fanqie book url
            novel_id: optional platform novel id; if empty, extracted from url

        Returns:
            dict with fields:
            - platform, platform_novel_id, url
            - title, author, intro
            - main_category, tags
            - status (ongoing/completed), total_words (int)
            - reading_count
            - first_upload_date
        """
        pid = novel_id or self._extract_novel_id_from_url(novel_url)
        if pid and pid in self.book_cache:
            return self.book_cache[pid]

        # 给URL添加 enter_from=Rank 参数
        detail_url = self._add_enter_from_param(novel_url)
        self.logger.info(f"访问详情页: {detail_url}")

        # 访问详情页
        soup = self._get_soup_with_scroll(
            detail_url,  # 使用添加了参数的URL
            wait_css=".info-name h1, h1, .title, .book-title",
            wait_sec=15,
            target_count=0,  # 详情页不需要滚动加载
            max_scroll_attempts=0,
        )

        if not soup:
            return {
                "platform": "fanqie",
                "platform_novel_id": pid,
                "url": novel_url,
                "title": "",
                "author": "",
                "intro": "",
                "main_category": "",
                "tags": [],
                "status": "",
                "total_words": 0,
                "reading_count": 0,
                "first_upload_date": "",
            }

        detail: Dict[str, Any] = {
            "platform": "fanqie",
            "platform_novel_id": pid,
            "url": novel_url,  # 原始URL
            "title": "",
            "author": "",
            "intro": "",
            "main_category": "",
            "tags": [],
            "status": "",
            "total_words": 0,
            "reading_count": 0,
            "first_upload_date": "",
        }

        self._fill_detail_title_author_intro(soup, detail)
        self._fill_detail_category_tags(soup, detail)
        self._fill_detail_status_words(soup, detail, page_url=detail_url)
        self._extract_first_upload_date(soup, detail)

        # 提取阅读数（如果详情页有）
        reading_count = 0
        count_elements = soup.select('.info-count-item')
        for elem in count_elements:
            text_raw = elem.text.strip()
            text = self._decrypt_text(text_raw)
            if '在读' in text or '阅读' in text:
                # 提取数字
                num_match = re.search(r'([\d\.]+)[万亿]?', text)
                if num_match:
                    reading_count = self._parse_cn_number(num_match.group(1)) or 0
                    break

        detail["reading_count"] = reading_count

        # last-resort: avoid empty title
        if not detail.get("title") and pid:
            detail["title"] = f"{pid}"

        if pid:
            self.book_cache[pid] = detail

        return detail

    # ------------------------------------------------------------------
    # Chapter Page Parsing
    # ------------------------------------------------------------------
    def _extract_chapter_links(self, soup: BeautifulSoup, book_id: str, max_chapters: int = 5) -> List[
        Tuple[str, str, str, int]]:
        """从目录页提取章节链接和基本信息"""
        chapter_links = []

        try:
            self.logger.info("开始提取章节链接...")

            # 尝试多种选择器
            chapter_selectors = [
                '.page-directory-content .chapter-item',
                '.chapter-item-list .chapter-item',
                'li[data-chapter-id]',
                '.chapter-list li',
                '.catalog-list li'
            ]

            chapter_items = None
            for selector in chapter_selectors:
                items = soup.select(selector)
                if items:
                    chapter_items = items
                    self.logger.info(f"使用选择器 '{selector}' 找到 {len(items)} 个章节项")
                    break

            # 如果以上选择器都没找到，尝试查找所有可能的章节链接
            if not chapter_items:
                all_links = soup.find_all('a', href=re.compile(r'chapter|read'))
                chapter_items = []
                for link in all_links:
                    if 'chapter' in link.text.lower() or '章' in link.text:
                        chapter_items.append(link.parent)

            self.logger.info(f"找到 {len(chapter_items)} 个可能的章节项")

            # 提取前max_chapters章
            for idx, item in enumerate(chapter_items[:max_chapters * 2]):  # 多提取一些，以防有锁定章节
                try:
                    # 提取章节标题和URL
                    chapter_link = item.select_one('a.chapter-item-title, a.chapter-title, a[href*="chapter"]')
                    if not chapter_link:
                        continue

                    href = chapter_link.get('href', '')
                    chapter_title_raw = chapter_link.text.strip()
                    chapter_title = self._decrypt_text(chapter_title_raw)

                    if not href:
                        continue

                    # 构建完整URL
                    chapter_url = self._to_abs_url(href)

                    # 提取是否锁定
                    is_locked = item.select_one('.chapter-item-lock, .locked') is not None
                    if is_locked:
                        self.logger.debug(f"章节 {chapter_title} 已锁定，跳过")
                        continue

                    # 提取章节发布日期
                    publish_date = ""
                    # 尝试从data属性提取
                    if item.get('data-publish-time'):
                        publish_date = item.get('data-publish-time')
                    elif item.get('data-update-time'):
                        publish_date = item.get('data-update-time')

                    # 提取章节字数
                    word_count = 0
                    word_elem = item.select_one('.chapter-item-word, .word-count')
                    if word_elem:
                        word_raw = word_elem.text.strip()
                        word_text = self._decrypt_text(word_raw)
                        num_match = re.search(r'(\d+)', word_text)
                        if num_match:
                            word_count = int(num_match.group(1))
                    else:
                        # 默认字数
                        word_count = 3000

                    chapter_links.append((
                        chapter_title,
                        chapter_url,
                        publish_date,
                        word_count
                    ))

                    self.logger.debug(f"提取到章节 {idx + 1}: {chapter_title} - {chapter_url}")

                except Exception as e:
                    self.logger.debug(f"解析章节项失败: {e}")
                    continue

            return chapter_links

        except Exception as e:
            self.logger.error(f"提取章节链接失败: {e}")
            return []

    def _parse_chapter_content(self, soup: BeautifulSoup) -> str:
        """提取章节正文内容"""
        try:
            # 尝试不同的选择器
            selectors = [
                '.muye-reader-content',
                '.reader-content',
                '.chapter-content',
                '.content-text',
                '.chapter-entity',
                '.read-content',
                '.novel-content',
                '.article-content',
            ]

            for selector in selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # 提取所有段落
                    paragraphs = content_elem.select('p, div.text')
                    if not paragraphs:
                        # 如果没有段落，直接获取文本
                        content_raw = content_elem.get_text(strip=True)
                        content = self._decrypt_text(content_raw)
                    else:
                        # 合并段落
                        content_parts = []
                        for p in paragraphs:
                            text_raw = p.get_text(strip=True)
                            text = self._decrypt_text(text_raw)
                            if text:
                                content_parts.append(text)
                        content = '\n\n'.join(content_parts)

                    # 清理内容
                    content = re.sub(r'\s+', ' ', content).strip()

                    if content:
                        self.logger.debug(f"使用选择器 '{selector}' 找到内容，长度: {len(content)}")
                        return content

            # 如果上述选择器都失败，尝试查找所有p标签
            all_paragraphs = soup.find_all('p')
            if all_paragraphs:
                content_parts = []
                for p in all_paragraphs:
                    text_raw = p.get_text(strip=True)
                    text = self._decrypt_text(text_raw)
                    if text and len(text) > 10:  # 过滤过短的文本
                        content_parts.append(text)

                if content_parts:
                    content = '\n\n'.join(content_parts)
                    self.logger.debug(f"从段落中找到内容，长度: {len(content)}")
                    return content

            self.logger.warning("未找到章节正文内容")
            return ""

        except Exception as e:
            self.logger.error(f"提取章节内容失败: {e}")
            return ""

    def _extract_publish_date_from_chapter(self, soup: BeautifulSoup) -> str:
        """从章节页面提取发布日期"""
        try:
            # 方法1：从meta标签提取
            meta_date = soup.select_one('meta[property="article:published_time"], meta[name="publish_date"]')
            if meta_date and meta_date.get('content'):
                date_str = meta_date.get('content')
                # 尝试解析日期格式
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
                if date_match:
                    return date_match.group(1)

            # 方法2：从页面文本中提取
            page_text = self._normalize_text(soup.get_text())

            # 查找常见日期格式
            date_patterns = [
                r'发布日期[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'更新时间[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'发表时间[:：]\s*(\d{4}-\d{2}-\d{2})',
                r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                r'(\d{4})-(\d{1,2})-(\d{1,2})',
            ]

            for pattern in date_patterns:
                match = re.search(pattern, page_text)
                if match:
                    if len(match.groups()) == 3:
                        year, month, day = match.groups()
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    elif len(match.groups()) == 1:
                        return match.group(1)

            # 方法3：查找包含日期的元素
            date_elements = soup.find_all(string=re.compile(r'\d{4}[-年]\d{1,2}[-月]\d{1,2}'))
            for element in date_elements:
                if isinstance(element, str):
                    date_match = re.search(r'(\d{4})[-年](\d{1,2})[-月](\d{1,2})', element)
                    if date_match:
                        year, month, day = date_match.groups()
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            # 如果没有找到，返回当前日期
            from datetime import datetime
            return datetime.now().strftime('%Y-%m-%d')

        except Exception as e:
            self.logger.error(f"提取发布日期失败: {e}")
            from datetime import datetime
            return datetime.now().strftime('%Y-%m-%d')

    def _fetch_single_chapter(self, chapter_url: str) -> Optional[Dict[str, Any]]:
        """获取单章内容"""
        try:
            # 访问章节页面
            self.logger.info(f'访问章节页面: {chapter_url}')

            # 使用 _get_soup_with_scroll 方法获取页面
            soup = self._get_soup_with_scroll(
                chapter_url,
                wait_css=".muye-reader-content, .reader-content, .chapter-content",
                wait_sec=15,
                target_count=0,
                max_scroll_attempts=0,
            )

            if not soup:
                return None

            # 提取章节内容
            chapter_content = self._parse_chapter_content(soup)

            # 提取发布日期
            publish_date = self._extract_publish_date_from_chapter(soup)

            # 计算字数
            word_count = 0
            if chapter_content:
                # 去除空白字符后计算中文字符数
                clean_content = re.sub(r'\s+', '', chapter_content)
                # 统计中文字符（包括中文标点）
                chinese_chars = re.findall(r'[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef]', clean_content)
                word_count = len(chinese_chars)

            # 提取章节标题
            chapter_title = ""
            # 优先选择 muye_reader_title（章节标题）
            for elem in soup.select(
                    'h1.muye-reader-title, .muye-reader-box-header .muye-reader-title, .muye-reader-title'):
                if not elem:
                    continue
                # 确保不是导航栏里的标题
                if elem.find_parent(class_='muye-reader-nav') is not None:
                    continue
                chapter_title = self._decrypt_text(elem.get_text(strip=True))
                if chapter_title:
                    break


            return {
                'content': chapter_content,
                'title': chapter_title,
                'publish_date': publish_date,
                'word_count': word_count,
                'url': chapter_url
            }

        except Exception as e:
            self.logger.error(f'获取章节内容失败 {chapter_url}: {e}')
            return None

    def fetch_first_n_chapters(self, novel_url: str, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取小说前N章内容（智能补全：只获取缺失的章节）

        Args:
            novel_url: 小说详情页URL
            n: 要获取的章节数，默认为配置中的default_chapter_count

        Returns:
            list: 章节内容列表，每个元素包含章节信息
        """
        # 如果没有指定章节数，使用配置中的默认值
        if n is None:
            n = self.default_chapter_count

        # 从URL中提取小说ID
        novel_id = self._extract_novel_id_from_url(novel_url)

        self.logger.info(f'开始获取小说章节内容: {novel_url} (小说ID: {novel_id}, 目标章节数: {n})')

        try:
            # 先获取书籍详情（用于获取标题和上架时间）
            detail = self.fetch_novel_detail(novel_url, novel_id)

            # 从详情中提取书名和上架时间
            novel_title = detail.get('title', '')
            first_upload_date = detail.get('first_upload_date', '')

            self.logger.info(f'小说详情已获取: 《{novel_title}》 by {detail.get("author", "未知")}')
            self.logger.info(f'小说上架时间: {first_upload_date}')

            # 检查数据库中已有的章节
            existing_chapter_count = 0
            existing_chapters = []

            if self.db_handler and hasattr(self.db_handler, 'get_chapters_count'):
                # 获取数据库中已有的章节数量
                existing_chapter_count = self.db_handler.get_chapters_count(novel_id)
                self.logger.info(f'数据库查询: 小说ID {novel_id} 已有章节数: {existing_chapter_count}')

                # 如果已有章节数 >= 目标章节数，直接从数据库加载
                if existing_chapter_count >= n:
                    self.logger.info(
                        f'章节智能补全: 数据库已有{existing_chapter_count}章 >= 目标{n}章，直接从数据库加载')
                    db_chapters = self.db_handler.get_novel_chapters(novel_id, n)

                    # 格式化数据库章节数据
                    chapters = []
                    for db_chapter in db_chapters:
                        chapters.append({
                            'chapter_num': db_chapter.get('chapter_num', 0),
                            'chapter_title': db_chapter.get('chapter_title', ''),
                            'chapter_content': db_chapter.get('chapter_content', ''),
                            'chapter_url': db_chapter.get('chapter_url', ''),
                            'word_count': db_chapter.get('word_count', 0),
                            'publish_date': db_chapter.get('publish_date', first_upload_date),
                        })
                    return chapters

                # 如果已有章节，获取已存在的章节信息
                if existing_chapter_count > 0:
                    existing_chapters = self.db_handler.get_novel_chapters(novel_id, existing_chapter_count)
                    self.logger.info(f'章节智能补全: 从数据库加载了{len(existing_chapters)}个现有章节')

            # 需要从网站抓取的新章节数
            need_chapter_count = n - existing_chapter_count
            if need_chapter_count <= 0:
                self.logger.info('章节智能补全: 不需要抓取新章节')
                # 格式化现有章节数据
                chapters = []
                for existing_chapter in existing_chapters[:n]:
                    chapters.append({
                        'chapter_num': existing_chapter.get('chapter_num', 0),
                        'chapter_title': existing_chapter.get('chapter_title', ''),
                        'chapter_content': existing_chapter.get('chapter_content', ''),
                        'chapter_url': existing_chapter.get('chapter_url', ''),
                        'word_count': existing_chapter.get('word_count', 0),
                        'publish_date': existing_chapter.get('publish_date', first_upload_date),
                    })
                return chapters

            self.logger.info(
                f'章节智能补全: 需要抓取{need_chapter_count}个新章节（已有{existing_chapter_count}章，目标{n}章）'
            )

            # 获取现有的最大章节号
            max_existing_chapter_num = 0
            if existing_chapters:
                max_existing_chapter_num = max([ch.get('chapter_num', 0) for ch in existing_chapters])
                self.logger.info(f'现有最大章节号: {max_existing_chapter_num}')

            # 构建目录页URL
            catalog_url = f"{novel_url}#Catalog" if "#" not in novel_url else novel_url

            self.logger.info(f'访问目录页: {catalog_url}')
            # 访问目录页获取章节链接
            catalog_soup = self._get_soup_with_scroll(
                catalog_url,
                wait_css=".page-directory-content, .chapter-list, .catalog-list",
                wait_sec=20,  # 增加等待时间
                target_count=0,
                max_scroll_attempts=0,
            )

            if not catalog_soup:
                self.logger.warning("无法访问目录页，返回现有章节")
                # 格式化现有章节数据
                chapters = []
                for existing_chapter in existing_chapters[:n]:
                    chapters.append({
                        'chapter_num': existing_chapter.get('chapter_num', 0),
                        'chapter_title': existing_chapter.get('chapter_title', ''),
                        'chapter_content': existing_chapter.get('chapter_content', ''),
                        'chapter_url': existing_chapter.get('chapter_url', ''),
                        'word_count': existing_chapter.get('word_count', 0),
                        'publish_date': existing_chapter.get('publish_date', first_upload_date),
                    })
                return chapters

            # 提取章节链接
            chapter_infos = self._extract_chapter_links(catalog_soup, novel_id, need_chapter_count * 2)

            if not chapter_infos:
                self.logger.warning("未找到章节链接，返回现有章节")
                # 格式化现有章节数据
                chapters = []
                for existing_chapter in existing_chapters[:n]:
                    chapters.append({
                        'chapter_num': existing_chapter.get('chapter_num', 0),
                        'chapter_title': existing_chapter.get('chapter_title', ''),
                        'chapter_content': existing_chapter.get('chapter_content', ''),
                        'chapter_url': existing_chapter.get('chapter_url', ''),
                        'word_count': existing_chapter.get('word_count', 0),
                        'publish_date': existing_chapter.get('publish_date', first_upload_date),
                    })
                return chapters

            self.logger.info(f'从目录页提取到 {len(chapter_infos)} 个章节链接')

            # 获取前need_chapter_count个非锁定章节
            new_chapters = []

            for i, (chapter_title, chapter_url, publish_date, word_count) in enumerate(chapter_infos):
                if len(new_chapters) >= need_chapter_count:
                    break

                self.logger.info(f'获取第{len(new_chapters) + 1}/{need_chapter_count}章: {chapter_title}')

                chapter_data = self._fetch_single_chapter(chapter_url)
                if chapter_data:
                    # 使用章节链接中的标题，如果章节页面没找到标题
                    if not chapter_data['title']:
                        chapter_data['title'] = chapter_title

                    # 使用章节链接中的发布日期，如果章节页面没找到
                    if not chapter_data['publish_date'] and publish_date:
                        chapter_data['publish_date'] = publish_date

                    # 使用章节链接中的字数，如果章节页面没计算出来
                    if chapter_data['word_count'] == 0 and word_count > 0:
                        chapter_data['word_count'] = word_count

                    chapter_data['chapter_num'] = max_existing_chapter_num + len(new_chapters) + 1
                    chapter_data['chapter_title'] = chapter_data['title']
                    chapter_data['chapter_content'] = chapter_data['content']
                    chapter_data['chapter_url'] = chapter_url

                    new_chapters.append(chapter_data)
                    self.logger.info(f'成功获取章节 {chapter_data["chapter_num"]}: {chapter_data["chapter_title"]}')

                    # 章节间延迟
                    if len(new_chapters) < need_chapter_count:
                        self.logger.info(f'章节间延迟 1-2 秒...')
                        self._sleep_human(1, 2)
                else:
                    self.logger.warning(f"未能获取章节内容: {chapter_title}")

            # 合并现有章节和新章节
            all_chapters = []

            # 添加现有章节
            for existing_chapter in existing_chapters:
                all_chapters.append({
                    'chapter_num': existing_chapter.get('chapter_num', 0),
                    'chapter_title': existing_chapter.get('chapter_title', ''),
                    'chapter_content': existing_chapter.get('chapter_content', ''),
                    'chapter_url': existing_chapter.get('chapter_url', ''),
                    'word_count': existing_chapter.get('word_count', 0),
                    'publish_date': existing_chapter.get('publish_date', first_upload_date),
                })

            # 添加新章节
            for chapter in new_chapters:
                all_chapters.append({
                    'chapter_num': chapter.get('chapter_num', 0),
                    'chapter_title': chapter.get('chapter_title', ''),
                    'chapter_content': chapter.get('chapter_content', ''),
                    'chapter_url': chapter.get('chapter_url', ''),
                    'word_count': chapter.get('word_count', 0),
                    'publish_date': chapter.get('publish_date', first_upload_date if first_upload_date else ""),
                })

            # 确保章节号连续并重新排序
            all_chapters.sort(key=lambda x: x['chapter_num'])
            for i, chapter in enumerate(all_chapters, 1):
                chapter['chapter_num'] = i

            self.logger.info(
                f'章节智能补全完成: 现有{len(existing_chapters)}章 + 新增{len(new_chapters)}章 = 总计{len(all_chapters)}章')

            # 保存新章节到数据库
            if self.db_handler and hasattr(self.db_handler, 'save_novel'):
                self.logger.info(f'保存{len(new_chapters)}个新章节到数据库')
                # 准备小说基本信息
                novel_data = {
                    'novel_id': novel_id,
                    'title': novel_title,
                    'author': detail.get('author', '未知'),
                    'platform': 'fanqie',
                    'novel_url': novel_url,
                    'category': detail.get('main_category', ''),
                    'introduction': detail.get('intro', ''),
                    'tags': detail.get('tags', []),
                    'status': detail.get('status', ''),
                    'total_words': detail.get('total_words', 0),
                    'first_upload_date': first_upload_date,
                }

                # 记录每个章节的发布时间用于调试
                for i, chapter in enumerate(new_chapters, 1):
                    chapter_publish_date = chapter.get('publish_date', '')
                    self.logger.debug(f'章节{i}的发布时间: {chapter_publish_date}')

                # 只保存新章节
                self.db_handler.save_novel(novel_data, new_chapters)

            # 只返回需要的数量
            return all_chapters[:n]

        except Exception as e:
            self.logger.error(f'获取小说章节内容失败 {novel_url}: {e}')
            return []

    # ------------------------------------------------------------------
    # Enrichment / persistence
    # ------------------------------------------------------------------
    def enrich_rank_items(
            self,
            items: Sequence[Dict[str, Any]],
            *,
            max_books: int = 20,
            fetch_detail: bool = True,
            fetch_chapters: bool = False,
            chapter_count: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Enrich rank items with detail metadata and optional First_N_chapters."""
        if chapter_count is None:
            chapter_count = self.default_chapter_count

        out: List[Dict[str, Any]] = []
        for i, book in enumerate(items[:max_books], 1):
            title = book.get('title', '未知')
            self.logger.info(f"[数据补完] 处理第{i}/{min(len(items), max_books)}本书: 《{title}》")
            enriched = dict(book)

            if fetch_detail:
                detail = self.fetch_novel_detail(enriched.get("url", ""), enriched.get("platform_novel_id", ""))

                # 记录处理前的分类信息
                original_main = enriched.get("main_category", "")
                original_tags = enriched.get("tags", [])
                self.logger.info(f"[数据补完] 《{title}》 原有分类 - 主分类: '{original_main}', 标签: {original_tags}")

                # 更新所有字段
                update_fields = ["title", "author", "intro", "status", "total_words", "first_upload_date"]

                for k in update_fields:
                    dv = detail.get(k)
                    if dv is not None:
                        # 只在为空时才更新
                        if k not in enriched or not enriched[k]:
                            enriched[k] = dv

                # 更新阅读数（使用详情页的，如果详情页有）
                if detail.get("reading_count", 0) > 0:
                    enriched["reading_count"] = detail["reading_count"]

                # 分类处理：优先使用详情页的分类，但避免用空值覆盖正确的分类
                detail_main_cat = detail.get("main_category")
                detail_tags = detail.get("tags", [])

                if detail_main_cat:
                    self.logger.info(f"[数据补完] 使用详情页主分类: '{detail_main_cat}' (替换原有: '{original_main}')")
                    enriched["main_category"] = detail_main_cat
                else:
                    self.logger.info(f"[数据补完] 保留原有主分类: '{original_main}'")

                # 合并标签
                existing_tags = set(enriched.get("tags", []))
                new_tags = set(detail.get("tags", []))
                merged_tags = list(existing_tags.union(new_tags))

                if merged_tags != original_tags:
                    self.logger.info(f"[数据补完] 合并标签: {original_tags} + {list(new_tags)} = {merged_tags}")
                enriched["tags"] = merged_tags

                self.logger.info(
                    f"[数据补完] 《{title}》 最终分类 - 主分类: '{enriched.get('main_category')}', 标签: {enriched.get('tags', [])}")

            if fetch_chapters:
                chapters = self.fetch_first_n_chapters(enriched.get("url", ""), n=chapter_count)
                if chapters:
                    enriched["first_n_chapters"] = chapters
                    self.logger.info(f"[数据补完] 《{title}》 获取到 {len(chapters)} 章内容")

            out.append(enriched)
            self._sleep_human(1, 3)

        return out

    # ------------------------------------------------------------------
    # BaseSpider API: enrich_books_with_details
    # ------------------------------------------------------------------
    def enrich_books_with_details(self, books, max_books: int = 20):
        """Enrich rank books with detail-page metadata."""
        return self.enrich_rank_items(
            books,
            max_books=max_books,
            fetch_detail=True,
            fetch_chapters=False,
            chapter_count=None,
        )

    # ------------------------------------------------------------------
    # Database Operations
    # ------------------------------------------------------------------
    def save_rank_snapshot(
            self,
            *,
            rank_type: str,
            items: Sequence[Dict[str, Any]],
            snapshot_date: Optional[str] = None,
            source_url: str = "",
            make_title_primary: bool = True,
    ) -> Optional[int]:
        """Persist a rank snapshot via db_handler if available.

        Args:
            rank_type: key in rank_urls and rank_type_map.
            items: enriched or raw items list.
            snapshot_date: YYYY-MM-DD; defaults to today.
            source_url: optional rank page url.

        Returns:
            snapshot_id (int) if db_handler returns it; otherwise None.
        """
        if not self.db_handler or not hasattr(self.db_handler, "save_rank_snapshot"):
            self.logger.warning("db_handler missing or lacks save_rank_snapshot; skip saving.")
            return None

        ident = self.rank_type_map.get(rank_type, RankIdentity(rank_family=rank_type))
        snapshot_date = snapshot_date or self._today_str()

        return self.db_handler.save_rank_snapshot(
            platform="fanqie",
            rank_family=ident.rank_family,
            rank_sub_cat=ident.rank_sub_cat,
            snapshot_date=snapshot_date,
            items=list(items),
            source_url=source_url or "",
            make_title_primary=make_title_primary,
        )

    def fetch_and_save_rank(
            self,
            rank_type: str,
            *,
            pages: Optional[int] = None,
            enrich_detail: bool = True,
            enrich_chapters: bool = False,
            chapter_count: Optional[int] = None,
            snapshot_date: Optional[str] = None,
            max_books: int = 200,
    ) -> Dict[str, Any]:
        """One-stop pipeline: fetch rank -> enrich -> save (optional)."""
        if pages is not None:
            self.site_config["pages_per_rank"] = pages

        raw = self.fetch_rank_list(rank_type=rank_type)[:max_books]

        enriched = self.enrich_rank_items(
            raw,
            max_books=max_books,
            fetch_detail=enrich_detail,
            fetch_chapters=enrich_chapters,
            chapter_count=chapter_count,
        )

        ident = self.rank_type_map.get(rank_type, RankIdentity(rank_family=rank_type))
        snapshot_id = self.save_rank_snapshot(
            rank_type=rank_type,
            items=enriched,
            snapshot_date=snapshot_date,
            source_url=(self.site_config.get("rank_urls") or {}).get(rank_type, ""),
            make_title_primary=True,
        )

        # 可选保存章节
        if enrich_chapters and self.db_handler and hasattr(self.db_handler, "upsert_first_n_chapters"):
            for b in enriched:
                chapters = b.get("first_n_chapters") or []
                if not chapters:
                    continue

                # 调试：检查章节发布时间
                self.logger.info(f"准备保存小说 {b.get('title')} 的章节")
                for i, chapter in enumerate(chapters, 1):
                    publish_date = chapter.get('publish_date', '')
                    self.logger.info(f"章节{i}发布时间: {publish_date}")

                first_chapter_publish_date = ""
                if chapters:
                    first_chapter_publish_date = chapters[0].get('publish_date', snapshot_date or self._today_str())

                self.db_handler.upsert_first_n_chapters(
                    platform="fanqie",
                    platform_novel_id=b.get("platform_novel_id", ""),
                    publish_date=first_chapter_publish_date,
                    chapters=chapters,
                    novel_fallback_fields={
                        "title": b.get("title", ""),
                        "author": b.get("author", ""),
                        "intro": b.get("intro", ""),
                        "main_category": b.get("main_category", ""),
                        "status": b.get("status", ""),
                        "total_words": b.get("total_words", 0),
                        "url": b.get("url", ""),
                        "tags": b.get("tags", []),
                    },
                )

        return {
            "rank_type": rank_type,
            "rank_family": ident.rank_family,
            "rank_sub_cat": ident.rank_sub_cat,
            "snapshot_id": snapshot_id,
            "items": enriched,
        }

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_whole_ranks, 一键启动
    # ------------------------------------------------------------------
    def fetch_whole_rank(self) -> List[Dict[str, Any]]:
        """Fetch all configured rank lists and return a flattened list of items."""
        all_books: List[Dict[str, Any]] = []
        for rank_type in (self.site_config.get("rank_urls") or {}):
            try:
                books = self.fetch_rank_list(rank_type)
                all_books.extend(books)
                if hasattr(self, "_save_raw_data"):
                    self._save_raw_data(books, f"{self.name}_{rank_type}_{time.strftime('%Y%m%d')}.json")
            except Exception as e:
                self.logger.error(f"抓取{rank_type}榜失败: {e}")
        return all_books

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close Selenium driver."""
        if self.driver:
            try:
                self.driver.quit()
            finally:
                self.driver = None
        self.logger.info("FanqieSpider closed.")