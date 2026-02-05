# spiders/qidian_spider.py
"""QidianSpider (起点中文网爬虫)

Phase 1 目标：
- 使用 Selenium 抓取起点各类榜单（畅销榜/月票榜/推荐榜/收藏榜/新书榜等）
- 抽取小说元信息（书名、作者、简介、主分类、细分题材 tag、状态、总字数）
- 可选：抓取前 N 章正文用于后续开篇分析（FIRST_N_CHAPTERS）
- 将抓取结果以"可直接写入数据库的标准化结构"返回；如提供 db_handler，则可直接落库

数据库对齐（你当前 schema 的关键点）：
- NOVELS.main_category：只存主分类（如"都市""玄幻"）
- 起点"副分类"当作一个 tag 进入 TAGS / NOVEL_TAG_MAP
- RANK_LISTS：rank_family 存大榜（畅销榜/月票榜/推荐榜/收藏榜/新书榜）
  rank_sub_cat 仅用于起点新书榜四小类，其它为空
- RANK_ENTRIES：起点指标使用 total_recommend（总推荐）

注意：
- 起点页面结构会变化；选择器做了多层 fallback。
"""

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
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

import config
from .base_spider import BaseSpider

GLOBAL_SELENIUM_CONFIG = getattr(config, "SELENIUM_CONFIG", {}) or {}


@dataclass(frozen=True)
class RankIdentity:
    """Normalized rank identity that maps to RANK_LISTS schema."""

    rank_family: str
    rank_sub_cat: str = ""  # only for 起点新书榜四小类; otherwise ""


class QidianSpider(BaseSpider):
    """起点中文网 Selenium 爬虫。

    该 Spider 在 Phase 1 主要负责：
    1) 抓取榜单页（可多页）
    2) 抓取详情页（补全元信息：分类/状态/字数/简介等）
    3) 可选抓取前 N 章（FIRST_N_CHAPTERS）
    4) 输出"可直接落库"的标准化结构；如注入 db_handler 可直接写入数据库

    重要接口（BaseSpider 抽象方法要求）：
    - fetch_rank_list
    - fetch_novel_detail
    - enrich_books_with_details
    - fetch_all_ranks
    """

    def __init__(self, site_config: Dict[str, Any], db_handler: Any = None):
        """Initialize Qidian spider.

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
            "timeout": 15,
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

            self.logger.info("QidianSpider Selenium driver initialized.")
            return True
        except Exception as e:
            self.logger.error(f"Selenium init failed: {e}")
            if self.retry_count < self.max_retries:
                self.retry_count += 1
                time.sleep(2)
                return self._init_driver()
            return False

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------
    def _today_str(self) -> str:
        """Return today's date string (YYYY-MM-DD)."""
        return date.today().strftime("%Y-%m-%d")

    def _sleep_human(self, a: float = 2.0, b: float = 4.0) -> None:
        """Sleep a random duration to reduce bot-like behaviour."""
        time.sleep(random.uniform(a, b))

    def _to_abs_url(self, href: str) -> str:
        """Convert href to absolute URL."""
        if not href:
            return ""
        if href.startswith("//"):
            return "https:" + href
        if href.startswith("http"):
            return href
        return urljoin(self.base_url, href)

    def _normalize_text(self, s: str) -> str:
        """Normalize whitespace for consistent parsing."""
        s = (s or "").strip()
        return re.sub(r"\s+", " ", s)

    def _dedupe_keep_order(self, xs: Sequence[str]) -> List[str]:
        """Dedupe strings while keeping original order."""
        seen = set()
        out: List[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def _parse_cn_number(self, text: str) -> Optional[int]:
        """Parse Chinese compact number like '12.3万' / '1.2亿' -> int."""
        if not text:
            return None
        t = text.strip().replace(",", "")
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([万亿]?)", t)
        if not m:
            return None
        val = float(m.group(1))
        unit = m.group(2)
        if unit == "万":
            val *= 10_000
        elif unit == "亿":
            val *= 100_000_000
        return int(val)

    # ------------------------------------------------------------------
    # Rank type mapping (rank_family / rank_sub_cat)
    # ------------------------------------------------------------------
    def _build_rank_type_map(self) -> Dict[str, RankIdentity]:
        """Build mapping from config rank_type to normalized RankIdentity.

        You can override via site_config["rank_type_map"] like:
        {
            "hotsales": {"rank_family": "畅销榜", "rank_sub_cat": ""},
            "newbook_signed_author": {"rank_family": "新书榜", "rank_sub_cat": "签约作者新书榜"}
        }
        """
        custom = self.site_config.get("rank_type_map")
        if isinstance(custom, dict) and custom:
            out: Dict[str, RankIdentity] = {}
            for k, v in custom.items():
                out[k] = RankIdentity(
                    rank_family=v.get("rank_family", k),
                    rank_sub_cat=v.get("rank_sub_cat", "") or "",
                )
            return out

        return {
            "hotsales": RankIdentity("畅销榜"),
            "yuepiao": RankIdentity("月票榜"),
            "recommend": RankIdentity("推荐榜"),
            "collect": RankIdentity("收藏榜"),
            "newbook": RankIdentity("新书榜"),
            "newbook_signed_author": RankIdentity("新书榜", "签约作者新书榜"),
            "newbook_public_author": RankIdentity("新书榜", "公众作者新书榜"),
            "newbook_new_signed": RankIdentity("新书榜", "新人签约新书榜"),
            "newbook_new_author": RankIdentity("新书榜", "新人作者新书榜"),
        }

    # ------------------------------------------------------------------
    # Page fetching
    # ------------------------------------------------------------------
    def _get_soup(
            self, url: str, wait_css: Optional[str] = None, wait_sec: int = 12
    ) -> Optional[BeautifulSoup]:
        """Fetch URL using Selenium and return BeautifulSoup.

        Args:
            url: target url
            wait_css: optional CSS selector to wait for (presence)
            wait_sec: wait timeout seconds

        Returns:
            BeautifulSoup or None on failure.
        """
        if not self.driver:
            self.logger.error("Driver not initialized.")
            return None
        try:
            self.driver.get(url)
            self._sleep_human(2, 4)

            if wait_css:
                try:
                    WebDriverWait(self.driver, wait_sec).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                    )
                except Exception:
                    self.logger.debug(f"Wait selector timeout: {wait_css}")

            return BeautifulSoup(self.driver.page_source, "html.parser")
        except Exception as e:
            self.logger.error(f"Failed to fetch page: {url} ; err={e}")
            return None

    # ------------------------------------------------------------------
    # Category normalization (主类 + 副类->tag)
    # ------------------------------------------------------------------
    def _extract_novel_id_from_url(self, url: str) -> str:
        """Extract Qidian novel id (digits) from URL."""
        patterns = [
            r"/book/(\d+)/",
            r"/info/(\d+)/",
            r"book\.qidian\.com/info/(\d+)",
            r"www\.qidian\.com/book/(\d+)",
        ]
        for p in patterns:
            m = re.search(p, url or "")
            if m:
                return m.group(1)
        for part in (url or "").split("/"):
            if part.isdigit() and len(part) >= 6:
                return part
        return ""

    def _split_qidian_category(self, raw_category: str) -> Tuple[str, Optional[str]]:
        """Split '大类·副类' into (main_category, sub_as_tag)."""
        raw = self._normalize_text(raw_category)
        if not raw or raw in {"未知", "未知分类"}:
            return "未知", None

        # 从配置中获取起点主分类列表
        qidian_main_categories = set(self.site_config.get('novel_types', []))

        # 如果没有配置，使用默认的主分类列表作为后备
        if not qidian_main_categories:
            qidian_main_categories = {
                "玄幻", "奇幻", "武侠", "仙侠", "都市", "现实", "军事", "历史",
                "游戏", "体育", "科幻", "诸天无限", "悬疑", "轻小说", "短篇"
            }

        # 常见的副分类/标签列表（用于识别）
        common_sub_categories = {
            "修真文明", "异术超能", "东方玄幻", "高武世界", "都市异能",
            "都市生活", "进化变异", "末世危机", "时空穿梭", "未来世界",
            "历史架空", "架空历史", "上古先秦", "两晋隋唐", "两宋元明",
            "清史民国", "外国历史", "战争幻想", "军事战争", "军旅生涯",
            "游戏异界", "电子竞技", "虚拟网游", "游戏系统", "体育竞技",
            "篮球运动", "足球运动", "体育赛事", "古武机甲", "星际文明",
            "超级科技", "时空穿梭", "诡秘悬疑", "侦探推理", "奇妙世界",
            "原生幻想", "青春日常", "恋爱日常", "搞笑吐槽", "衍生同人",
            "史诗奇幻", "现代魔法", "黑暗幻想", "剑与魔法", "现代修真",
            "古典仙侠", "神话修真", "现代修仙", "历史神话", "国术无双"
        }

        # 处理有分隔符的情况
        if "·" in raw:
            parts = [part.strip() for part in raw.split("·") if part.strip()]

            if len(parts) >= 2:
                # 尝试识别主分类（通常在第一个位置）
                main_cat_candidate = parts[0]
                sub_cat_candidate = parts[1] if len(parts) > 1 else ""

                # 如果第一个部分是主分类
                if main_cat_candidate in qidian_main_categories:
                    main_cat = main_cat_candidate
                    sub_cat = "·".join(parts[1:]) if len(parts) > 1 else None
                else:
                    # 检查第二个部分是否可能是主分类
                    if len(parts) > 1 and parts[1] in qidian_main_categories:
                        main_cat = parts[1]
                        sub_cat = parts[0] if len(parts[0]) > 0 else None
                        # 如果还有其他部分，也加入到副分类
                        if len(parts) > 2:
                            sub_cat = sub_cat + "·" + "·".join(parts[2:]) if sub_cat else "·".join(parts[2:])
                    else:
                        # 都不是主分类，可能是副分类的组合
                        # 检查是否有部分匹配主分类
                        main_cat = "未知"
                        for part in parts:
                            if part in qidian_main_categories:
                                main_cat = part
                                break

                        # 剩余部分作为副分类
                        remaining_parts = [p for p in parts if p != main_cat]
                        sub_cat = "·".join(remaining_parts) if remaining_parts else None

                # 如果副分类是空字符串，设为None
                if sub_cat and len(sub_cat) == 0:
                    sub_cat = None

                return main_cat, sub_cat
            else:
                # 只有一个部分但有分隔符
                cleaned = raw.replace("·", "").strip()
                if cleaned in qidian_main_categories:
                    return cleaned, None
                else:
                    return "未知", cleaned if cleaned else None
        else:
            # 没有分隔符的情况
            if raw in qidian_main_categories:
                return raw, None
            elif raw in common_sub_categories:
                # 如果是常见的副分类，则主分类设为未知
                return "未知", raw
            else:
                # 检查是否可能是主分类的变体或别名
                for main_cat in qidian_main_categories:
                    if raw.startswith(main_cat) or main_cat.startswith(raw):
                        if len(raw) <= 4:  # 避免错误匹配长文本
                            return main_cat, None

                # 无法识别，设为未知
                return "未知", raw if raw else None

    def _parse_rank_page(self, soup: BeautifulSoup, *, rank_type: str, page: int) -> List[Dict[str, Any]]:
        """Parse one rank page soup into raw rank items."""
        selectors = [
            ".book-img-text li",
            ".rank-view-list li",
            "li[data-rid]",
            "div[data-bid]",
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
            title_elem = node.select_one("h2 a") or node.select_one("a[href*='/book/']")
            if not title_elem:
                return None

            title = self._normalize_text(title_elem.get_text(strip=True))
            url = self._to_abs_url(title_elem.get("href", ""))
            pid = (
                    self._extract_novel_id_from_url(url)
                    or str(node.get("data-bid") or node.get("data-rid") or "").strip()
            )
            if not pid:
                return None

            # rank (assume 20/page typical; still usable for trend)
            global_rank = (page - 1) * 20 + idx

            author = ""
            author_elem = node.select_one("a.author, .author a.name, .author-name, p.author a.name")
            if author_elem:
                author = self._normalize_text(author_elem.get_text(strip=True))

            raw_category = self._extract_category_from_rank_item(node)
            main_cat, sub_tag = self._split_qidian_category(raw_category)
            self.logger.debug(f"分类解析 - 原始: '{raw_category}', 主分类: '{main_cat}', 副分类: '{sub_tag}'")

            intro = ""
            intro_elem = node.select_one(".intro, .book-intro")
            if intro_elem:
                intro = self._normalize_text(intro_elem.get_text(strip=True))

            tags = self._extract_tags_from_rank_item(node)
            if sub_tag:
                # 确保副分类作为标签添加，且不重复
                if sub_tag not in tags:
                    tags.append(sub_tag)

            # 确保主分类不作为标签
            if main_cat in tags:
                tags.remove(main_cat)

            tags = self._dedupe_keep_order([t for t in tags if t])

            # 移除榜单页中的总推荐数提取，只在详情页获取
            total_recommend = None

            return {
                "platform": "qidian",
                "platform_novel_id": pid,
                "title": title,
                "author": author,
                "intro": intro,
                "main_category": main_cat,
                "tags": tags,
                "status": "",  # detail page fills
                "total_words": 0,  # detail page fills
                "url": url,
                "rank": global_rank,
                "total_recommend": total_recommend,  # 将在详情页填充
                "rank_type": rank_type,
            }
        except Exception as e:
            self.logger.debug(f"parse rank item failed: {e}")
            return None

    def _extract_category_from_rank_item(self, node: Any) -> str:
        """Extract raw category string (usually '大类·副类') from rank item."""
        try:
            # 从author段落中提取分类信息
            author_p = node.select_one("p.author")
            if author_p:
                # 获取所有链接文本
                all_text = self._normalize_text(author_p.get_text(" ", strip=True))
                self.logger.debug(f"author段落文本: {all_text}")

                # 查找分类链接
                category_links = []
                for a in author_p.find_all("a"):
                    # 跳过作者链接
                    if "name" in a.get("class", []):
                        continue

                    text = self._normalize_text(a.get_text(strip=True))
                    if text and text not in ["连载", "完本"]:  # 排除状态文本
                        category_links.append(text)

                if len(category_links) >= 2:
                    result = f"{category_links[0]}·{category_links[1]}"
                    self.logger.debug(f"从author段落提取分类: {result}")
                    return result
                elif len(category_links) == 1:
                    result = category_links[0]
                    self.logger.debug(f"从author段落提取分类: {result}")
                    return result

            # 尝试直接搜索常见主分类
            node_text = self._normalize_text(node.get_text(" ", strip=True))
            main_categories = ["玄幻", "奇幻", "武侠", "仙侠", "都市", "现实",
                               "军事", "历史", "游戏", "体育", "科幻", "诸天无限",
                               "悬疑", "轻小说", "短篇"]

            for cat in main_categories:
                if cat in node_text:
                    # 检查是否在标题中（避免误判）
                    title_elem = node.select_one("h2 a")
                    if title_elem:
                        title_text = title_elem.get_text(strip=True)
                        if cat not in title_text or node_text.count(cat) > 1:
                            self.logger.debug(f"从文本搜索找到分类: {cat}")
                            return cat

            return "未知"
        except Exception as e:
            self.logger.debug(f"提取分类失败: {e}")
            return "未知"

    def _extract_tags_from_rank_item(self, node: Any) -> List[str]:
        """Extract tag list from rank item (excluding Qidian sub-category)."""
        tags: List[str] = []
        for el in node.select(".tag span, .tags a, .tag-wrap a"):
            t = self._normalize_text(el.get_text(strip=True))
            if t and len(t) <= 16:
                tags.append(t)
        return self._dedupe_keep_order(tags)

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_rank_list
    # ------------------------------------------------------------------
    def fetch_rank_list(self, rank_type: str = "hotsales") -> List[Dict[str, Any]]:
        """Fetch a rank list (multi-page) and return standardized items.

        Args:
            rank_type: key in site_config["rank_urls"]

        Returns:
            List of dicts (raw rank items). Each dict includes:
            - platform, platform_novel_id, title, author, intro
            - main_category (only main), tags (sub-category included as tag)
            - status, total_words, url
            - rank (int), total_recommend (None - 将在详情页获取)
        """
        url_template = (self.site_config.get("rank_urls") or {}).get(rank_type)
        if not url_template:
            self.logger.error(f"rank_type not configured in rank_urls: {rank_type}")
            return []

        pages = int(self.site_config.get("pages_per_rank", 5))
        all_items: List[Dict[str, Any]] = []

        for page in range(1, pages + 1):
            url = url_template.format(page=page)
            self.logger.info(f"Rank[{rank_type}] page {page}/{pages}: {url}")

            soup = self._get_soup(
                url,
                wait_css="[data-bid], .book-img-text li, .rank-view-list li",
                wait_sec=10,
            )
            if not soup:
                continue

            all_items.extend(self._parse_rank_page(soup, rank_type=rank_type, page=page))
            self._sleep_human(2.5, 5.5)

        return all_items

    def _fill_detail_title_author_intro(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        """Fill title/author/intro fields from detail page soup."""
        # 只有在这些字段为空时才填充
        if not detail.get("title"):
            title_elem = soup.select_one("meta[property='og:title']") or soup.select_one(
                "h1.book-title, h1.works-title, .book-info h1"
            )
            if title_elem:
                if title_elem.name == "meta":
                    t = (title_elem.get("content", "") or "").strip()
                    if " - " in t:
                        t = t.split(" - ")[0].strip()
                    detail["title"] = t
                else:
                    detail["title"] = self._normalize_text(title_elem.get_text(strip=True))

        if not detail.get("author"):
            author_elem = soup.select_one("meta[property='og:novel:author']") or soup.select_one(
                "a.writer, .author-name, .writer a"
            )
            if author_elem:
                if author_elem.name == "meta":
                    detail["author"] = (author_elem.get("content", "") or "").strip()
                else:
                    detail["author"] = self._normalize_text(author_elem.get_text(strip=True))

        if not detail.get("intro"):
            intro_elem = soup.select_one("meta[property='og:description']") or soup.select_one(
                ".book-intro, .intro, .description"
            )
            if intro_elem:
                if intro_elem.name == "meta":
                    detail["intro"] = (intro_elem.get("content", "") or "").strip()
                else:
                    detail["intro"] = self._normalize_text(intro_elem.get_text(strip=True))

    def _fill_detail_category_tags(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        """Fill main_category and tags from detail page soup - 只有在没有分类信息时才填充"""
        # 如果已经有主分类且不是"未知"，则跳过详情页分类提取
        if detail.get("main_category") and detail.get("main_category") != "未知":
            self.logger.info(f"使用榜单页分类信息: main='{detail.get('main_category')}', tags={detail.get('tags', [])}")
            return

        raw_cat = self._extract_category_from_detail(soup)
        main_cat, sub_tag = self._split_qidian_category(raw_cat)

        detail["main_category"] = main_cat

        # 合并标签：已有的标签加上新提取的副分类（如果存在）
        tags = detail.get("tags", [])
        if sub_tag and sub_tag not in tags:
            tags.append(sub_tag)

        # 从详情页提取的标签
        for sel in [".tag-wrap a", ".tags a", ".book-tag a", ".tag-list a", ".book-tags a"]:
            for el in soup.select(sel):
                t = self._normalize_text(el.get_text(strip=True))
                if t and t not in tags:
                    # 过滤掉可能是主分类的标签和常见非标签文本
                    if (t != main_cat and
                            t not in ['VIP', '签约', '完结', '连载', '上架', '免费', '热门', '推荐']):
                        tags.append(t)

        detail["tags"] = self._dedupe_keep_order(tags)
        self.logger.info(f"最终分类: main='{main_cat}', tags={tags}")

    def _fill_detail_status_words(self, soup: BeautifulSoup, detail: Dict[str, Any], page_url: str = "") -> None:
        """Fill normalized status (ongoing/completed) and total_words from detail page."""
        if page_url:
            self.logger.info(f"Parsing status and word count from detail page: {page_url}")

        try:
            # 方法1: 精确提取状态信息
            author_p = soup.select_one("p.author")
            if author_p:
                status_span = author_p.select_one("span")
                if status_span:
                    status_text = self._normalize_text(status_span.get_text(strip=True))
                    self.logger.debug(f"Found status text in span: '{status_text}'")

                    if status_text == "连载":
                        detail["status"] = "ongoing"
                    elif status_text == "完本":
                        detail["status"] = "completed"
                    elif "连载" in status_text:
                        detail["status"] = "ongoing"
                    elif "完本" in status_text or "完结" in status_text:
                        detail["status"] = "completed"

            # 如果通过span没有找到状态，回退到正则搜索整个页面
            if not detail.get("status"):
                page_text = self._normalize_text(soup.get_text(" ", strip=True))
                if re.search(r"\b完本\b|\b完结\b", page_text):
                    detail["status"] = "completed"
                elif re.search(r"\b连载\b", page_text):
                    detail["status"] = "ongoing"

            # 提取字数信息
            word_count = None
            intro_p = soup.select_one("p.intro")
            if intro_p:
                intro_text = intro_p.get_text(" ", strip=True)
                self.logger.debug(f"Looking for word count in intro: {intro_text[:100]}...")

                # 尝试匹配 "字数 365.76万" 或 "总字数 123.45万"
                m = re.search(r"(?:字数|总字数)\s*([0-9]+(?:\.[0-9]+)?[万亿]?)", intro_text)
                if m:
                    word_count = self._parse_cn_number(m.group(1))

            # 如果intro段落没有，尝试其他常见位置
            if word_count is None:
                # 查找所有包含"字数"的元素
                word_elements = soup.find_all(text=re.compile(r"字数"))
                for elem in word_elements:
                    if isinstance(elem, str):
                        self.logger.debug(f"Found word count element: {elem[:50]}")
                        parent_text = self._normalize_text(elem)
                        m = re.search(r"(?:字数|总字数)\s*([0-9]+(?:\.[0-9]+)?[万亿]?)", parent_text)
                        if m:
                            word_count = self._parse_cn_number(m.group(1))
                            break

            # 如果以上方法都没有找到，使用正则搜索整个页面文本
            if word_count is None:
                page_text = self._normalize_text(soup.get_text(" ", strip=True))
                m = re.search(r"(?:字数|总字数)\s*([0-9]+(?:\.[0-9]+)?[万亿]?)", page_text)
                if m:
                    word_count = self._parse_cn_number(m.group(1))

            if word_count is not None:
                detail["total_words"] = word_count
                self.logger.debug(f"Extracted word count: {word_count}")
            else:
                self.logger.warning("Could not extract word count from page")

            # 记录提取结果
            self.logger.info(f"Extracted status: '{detail.get('status')}', word count: {detail.get('total_words')}")

        except Exception as e:
            self.logger.error(f"Error extracting status and word count from page: {e}")

    """从详情页提取总推荐数"""
    def _fill_total_recommend(self, soup: BeautifulSoup, detail: Dict[str, Any], page_url: str = "") -> None:
        """从详情页提取总推荐数 - 针对起点详情页特定结构，只获取总推荐"""
        if page_url:
            self.logger.debug(f"Parsing total recommend from detail page: {page_url}")

        try:
            total_recommend = None

            # 记录调试信息
            self.logger.debug(f"开始提取总推荐数，页面URL: {page_url}")

            # 首先尝试最直接的路径：查找包含"总推荐"的任何元素
            self.logger.debug("方法1: 查找包含'总推荐'的任何元素...")

            # 查找所有包含"总推荐"的元素
            for element in soup.find_all(lambda tag: tag.name and '总推荐' in tag.get_text()):
                self.logger.debug(f"找到包含'总推荐'的元素: {element.name} - {element.get_text(strip=True)[:100]}")

                # 检查是否在 p.count 元素内
                if element.name == 'cite' or '总推荐' in element.get_text():
                    # 尝试向上查找父元素中的数字
                    parent = element.parent
                    if parent:
                        self.logger.debug(f"父元素: {parent.name}, 类: {parent.get('class', [])}")

                        # 在父元素内查找 em 标签
                        em_element = parent.find('em')
                        if em_element:
                            em_text = self._normalize_text(em_element.get_text(strip=True))
                            self.logger.debug(f"找到em元素内容: '{em_text}'")
                            total_recommend = self._parse_cn_number(em_text)
                            if total_recommend:
                                self.logger.info(f"通过父元素em找到总推荐数: {em_text} -> {total_recommend}")
                                detail["total_recommend"] = total_recommend
                                return

                        # 如果没有em标签，尝试查找父元素的文本
                        parent_text = self._normalize_text(parent.get_text(" ", strip=True))
                        self.logger.debug(f"父元素文本: {parent_text[:200]}")

                        # 尝试从父元素文本中提取
                        import re
                        patterns = [
                            r'([0-9]+(?:\.[0-9]+)?[万亿]?)\s*总推荐',
                            r'总推荐\s*([0-9]+(?:\.[0-9]+)?[万亿]?)',
                        ]
                        for pattern in patterns:
                            m = re.search(pattern, parent_text)
                            if m:
                                num = self._parse_cn_number(m.group(1))
                                if num:
                                    total_recommend = num
                                    self.logger.info(f"从父元素文本提取总推荐: {m.group(1)} -> {num}")
                                    detail["total_recommend"] = total_recommend
                                    return

            # 方法2: 直接查找 p.count 元素
            self.logger.debug("方法2: 直接查找 p.count 元素...")
            count_elements = soup.find_all('p', class_='count')
            for p_element in count_elements:
                self.logger.debug(f"找到 p.count 元素: {p_element.prettify()[:200]}")

                # 查找内部的 em 和 cite 元素
                em_element = p_element.find('em')
                cite_element = p_element.find('cite')

                if em_element and cite_element:
                    em_text = self._normalize_text(em_element.get_text(strip=True))
                    cite_text = self._normalize_text(cite_element.get_text(strip=True))

                    self.logger.debug(f"em文本: '{em_text}', cite文本: '{cite_text}'")

                    if '总推荐' in cite_text:
                        total_recommend = self._parse_cn_number(em_text)
                        if total_recommend:
                            self.logger.info(f"通过p.count找到总推荐数: {em_text} -> {total_recommend}")
                            detail["total_recommend"] = total_recommend
                            return

            # 方法3: 查找所有 cite 元素
            self.logger.debug("方法3: 查找所有 cite 元素...")
            for cite_element in soup.find_all('cite'):
                cite_text = self._normalize_text(cite_element.get_text(strip=True))
                if '总推荐' in cite_text:
                    self.logger.debug(f"找到包含'总推荐'的cite元素: {cite_text}")

                    # 查找前一个兄弟元素（可能是em）
                    prev_sibling = cite_element.find_previous_sibling()
                    while prev_sibling:
                        if prev_sibling.name == 'em':
                            em_text = self._normalize_text(prev_sibling.get_text(strip=True))
                            total_recommend = self._parse_cn_number(em_text)
                            if total_recommend:
                                self.logger.info(f"通过cite的前一个兄弟em找到总推荐数: {em_text} -> {total_recommend}")
                                detail["total_recommend"] = total_recommend
                                return
                        # 继续向前查找
                        prev_sibling = prev_sibling.find_previous_sibling()

                    # 如果没有找到前一个兄弟em，查找父元素中的em
                    parent = cite_element.parent
                    if parent:
                        em_element = parent.find('em')
                        if em_element:
                            em_text = self._normalize_text(em_element.get_text(strip=True))
                            total_recommend = self._parse_cn_number(em_text)
                            if total_recommend:
                                self.logger.info(f"通过cite父元素中的em找到总推荐数: {em_text} -> {total_recommend}")
                                detail["total_recommend"] = total_recommend
                                return

            # 方法4: 正则搜索整个页面
            self.logger.debug("方法4: 正则搜索整个页面...")
            page_text = self._normalize_text(soup.get_text(" ", strip=True))

            # 只搜索包含"总推荐"的部分
            import re
            start_idx = 0
            while True:
                idx = page_text.find('总推荐', start_idx)
                if idx == -1:
                    break

                # 提取上下文（前后100个字符）
                start = max(0, idx - 100)
                end = min(len(page_text), idx + 100)
                context = page_text[start:end]
                self.logger.debug(f"找到'总推荐'上下文: {context}")

                # 在上下文中查找数字
                patterns = [
                    r'([0-9]+(?:\.[0-9]+)?[万亿]?)\s*总推荐',
                    r'总推荐\s*([0-9]+(?:\.[0-9]+)?[万亿]?)',
                ]

                for pattern in patterns:
                    m = re.search(pattern, context)
                    if m:
                        num = self._parse_cn_number(m.group(1))
                        if num:
                            total_recommend = num
                            self.logger.info(f"从页面文本提取总推荐: {m.group(1)} -> {num}")
                            detail["total_recommend"] = total_recommend
                            return

                start_idx = idx + 1

            # 如果所有方法都失败
            if total_recommend is None:
                self.logger.warning(f"未能从详情页提取总推荐数: {page_url}")

                # 记录页面片段以帮助调试
                page_snippet = soup.prettify()[:5000]  # 前5000字符
                self.logger.debug(f"页面片段: {page_snippet}")

                # 如果详情页没有，但榜单页有，就保持榜单页的值
                if "total_recommend" not in detail or detail["total_recommend"] is None:
                    detail["total_recommend"] = 0
                    self.logger.debug("设置总推荐数为默认值0")
            else:
                self.logger.info(f"成功提取总推荐数: {total_recommend}")

        except Exception as e:
            self.logger.error(f"提取总推荐数时出错: {e}")
            import traceback
            self.logger.error(f"详细错误: {traceback.format_exc()}")

            # 设置默认值
            if "total_recommend" not in detail or detail["total_recommend"] is None:
                detail["total_recommend"] = 0

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_novel_detail
    # ------------------------------------------------------------------
    def fetch_novel_detail(self, novel_url: str, novel_id: str = "") -> Dict[str, Any]:
        """Fetch novel detail page and extract normalized metadata.

        Args:
            novel_url: Qidian book url, e.g. https://www.qidian.com/book/123456/
            novel_id: optional platform novel id; if empty, extracted from url

        Returns:
            dict with fields:
            - platform, platform_novel_id, url
            - title, author, intro
            - main_category, tags
            - status (ongoing/completed), total_words (int)
            - total_recommend
            - first_upload_date
        """
        pid = novel_id or self._extract_novel_id_from_url(novel_url)
        if pid and pid in self.book_cache:
            return self.book_cache[pid]

        # Some Qidian pages (www/book vs book/info) render different DOM.
        candidate_urls: List[str] = []
        if novel_url:
            candidate_urls.append(novel_url)
        if pid:
            candidate_urls.extend(
                [
                    f"https://book.qidian.com/info/{pid}/",
                    f"https://www.qidian.com/book/{pid}/",
                ]
            )
        # de-dup while keeping order
        seen_url = set()
        candidate_urls = [u for u in candidate_urls if u and not (u in seen_url or seen_url.add(u))]

        soup = None
        final_url = novel_url
        for u in candidate_urls:
            s = self._get_soup(
                u,
                wait_css="h1, .book-info, meta[property='og:title'], meta[property='og:novel:book_name']",
                wait_sec=14,
            )
            if s is not None:
                soup = s
                final_url = u
                # if the page is a real book page, it should at least contain a title-ish marker
                if s.select_one("meta[property='og:title'], meta[property='og:novel:book_name'], h1"):
                    break

        if not soup:
            return {
                "platform": "qidian",
                "platform_novel_id": pid,
                "url": novel_url,
                "title": "",
                "author": "",
                "intro": "",
                "main_category": "未知",
                "tags": [],
                "status": "",
                "total_words": 0,
                "total_recommend": 0,
                "first_upload_date": "",
            }

        detail: Dict[str, Any] = {
            "platform": "qidian",
            "platform_novel_id": pid,
            "url": final_url,
            "title": "",
            "author": "",
            "intro": "",
            "main_category": "未知",
            "tags": [],
            "status": "",
            "total_words": 0,
            "total_recommend": 0,
            "first_upload_date": "",
        }

        self._fill_detail_title_author_intro(soup, detail)
        self._fill_detail_category_tags(soup, detail)
        self._fill_detail_status_words(soup, detail)
        self._fill_total_recommend(soup, detail, page_url=final_url)
        self._extract_first_upload_date(soup, detail)

        # last-resort: avoid empty title
        if not detail.get("title") and pid:
            detail["title"] = f"{pid}"

        if pid:
            self.book_cache[pid] = detail
        return detail

    def _extract_category_from_detail(self, soup: BeautifulSoup) -> str:
        """Extract raw category string from detail page with improved selectors."""
        try:
            # 方法1: 从meta标签提取
            meta_category = soup.select_one('meta[property="og:novel:category"]')
            if meta_category and meta_category.get('content'):
                category = meta_category.get('content')
                self.logger.info(f"从meta标签提取分类: {category}")
                return category

            # 方法2: 从面包屑导航提取
            breadcrumb_selectors = [
                '.crumb a', '.bread-crumb a', '.site-nav a',
                '.nav-bar a', '.breadcrumb a', '.path a',
                '.book-nav a', '.nav a[href*="qidian.com/"]'
            ]

            for selector in breadcrumb_selectors:
                breadcrumb_links = soup.select(selector)
                if len(breadcrumb_links) >= 2:
                    # 获取所有链接文本
                    link_texts = [self._normalize_text(link.get_text(strip=True)) for link in breadcrumb_links]
                    # 过滤掉非分类文本
                    exclude = ['首页', '我的书架', '排行榜', '书库', '小说', '起点中文网', '起点', '搜索',
                               '全部作品', '作品', '目录', '正文', '最新章节', '加入书架']
                    category_candidates = [text for text in link_texts if text and text not in exclude]

                    self.logger.debug(f"面包屑候选分类: {category_candidates}")

                    if len(category_candidates) >= 2:
                        # 通常倒数第二个是主分类，最后一个是副分类
                        for i in range(len(category_candidates) - 1, 0, -1):
                            main_cat = category_candidates[i - 1]
                            sub_cat = category_candidates[i]
                            if main_cat != sub_cat and len(main_cat) <= 4 and len(sub_cat) <= 6:
                                result = f"{main_cat}·{sub_cat}"
                                self.logger.info(f"从面包屑提取分类: {result}")
                                return result
                        # 如果没找到合适的组合，返回最后一个候选
                        if category_candidates:
                            self.logger.info(f"从面包屑返回最后一个候选: {category_candidates[-1]}")
                            return category_candidates[-1]

            # 方法3: 从小说标签区域提取
            tag_selectors = [
                '.book-info-detail .tag', '.book-information .tag',
                '.book-info .tag', '.book-tag', '.tags a',
                '.tag-wrap a', '.book-tags a', '.tag-list a'
            ]

            for selector in tag_selectors:
                tag_elements = soup.select(selector)
                category_tags = []
                for tag in tag_elements:
                    text = self._normalize_text(tag.get_text(strip=True))
                    href = tag.get('href', '')

                    # 常见的主分类列表
                    main_categories = ["玄幻", "奇幻", "武侠", "仙侠", "都市", "现实",
                                       "军事", "历史", "游戏", "体育", "科幻", "诸天无限",
                                       "悬疑", "轻小说", "短篇"]

                    # 检查是否是主分类
                    for cat in main_categories:
                        if cat in text:
                            category_tags.append(cat)
                            break
                    else:
                        # 如果不是主分类，检查长度和排除常见非分类文本
                        if (text and len(text) <= 8 and
                                text not in ['VIP', '签约', '完结', '连载', '上架', '免费', '热门', '推荐']):
                            category_tags.append(text)

                if category_tags:
                    # 去重
                    unique_tags = []
                    seen = set()
                    for tag in category_tags:
                        if tag not in seen:
                            seen.add(tag)
                            unique_tags.append(tag)

                    if len(unique_tags) >= 2:
                        result = f"{unique_tags[0]}·{unique_tags[1]}"
                        self.logger.info(f"从标签区域提取分类: {result}")
                        return result
                    elif unique_tags:
                        result = unique_tags[0]
                        self.logger.info(f"从标签区域提取分类: {result}")
                        return result

            # 方法4: 从页面URL或特殊属性中提取
            # 查找可能有data-category属性的元素
            for elem in soup.select('[data-category], [data-type], [data-cate]'):
                cat = elem.get('data-category') or elem.get('data-type') or elem.get('data-cate')
                if cat:
                    self.logger.info(f"从data属性提取分类: {cat}")
                    return cat

            # 方法5: 从页面标题或关键词中提取
            title_elem = soup.select_one('title')
            if title_elem:
                title_text = title_elem.get_text(strip=True)
                # 常见分类关键词
                common_categories = ['玄幻', '奇幻', '武侠', '仙侠', '都市', '现实',
                                     '军事', '历史', '游戏', '体育', '科幻', '悬疑',
                                     '轻小说', '二次元', '古代言情', '现代言情']
                for cat in common_categories:
                    if cat in title_text:
                        self.logger.info(f"从标题提取分类: {cat}")
                        return cat

            # 方法6: 从页面文本中搜索
            page_text = self._normalize_text(soup.get_text(" ", strip=True))
            for cat in ['玄幻', '奇幻', '武侠', '仙侠', '都市', '现实',
                        '军事', '历史', '游戏', '体育', '科幻', '悬疑']:
                if cat in page_text:
                    # 检查是否在小说标题中（避免误判）
                    title_elem = soup.select_one('h1, .book-title')
                    if title_elem:
                        title_text = title_elem.get_text(strip=True)
                        if cat not in title_text or page_text.count(cat) > 1:
                            self.logger.info(f"从页面文本提取分类: {cat}")
                            return cat

            self.logger.warning("未能提取分类信息，返回'未知'")
            return "未知"

        except Exception as e:
            self.logger.error(f"提取分类失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return "未知"

    def _extract_first_upload_date(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        """从详情页提取上架时间（首发时间）"""
        try:
            self.logger.debug("开始提取上架时间...")

            # 方法0：直接查找新的HTML结构（针对你提供的HTML示例）
            # 查找所有包含"上架"文本的元素
            upload_elements = soup.find_all(string=re.compile(r'上架'))

            for upload_text in upload_elements:
                # 获取包含"上架"的元素
                upload_elem = upload_text.parent
                if upload_elem:
                    self.logger.debug(f"找到'上架'元素: {upload_elem}")

                    # 查找前一个兄弟元素（应该是日期元素）
                    prev_sibling = upload_elem.previous_sibling

                    # 可能需要跳过空白节点
                    while prev_sibling and not isinstance(prev_sibling, str) and getattr(prev_sibling, 'name',
                                                                                         None) is None:
                        prev_sibling = prev_sibling.previous_sibling

                    if prev_sibling and hasattr(prev_sibling, 'get_text'):
                        # 检查前一个元素是否包含日期
                        date_text = self._normalize_text(prev_sibling.get_text(strip=True))
                        self.logger.debug(f"前一个兄弟元素文本: {date_text}")

                        # 尝试解析日期格式
                        date_patterns = [
                            r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                            r'(\d{4})-(\d{1,2})-(\d{1,2})',
                            r'(\d{4})\.(\d{1,2})\.(\d{1,2})',
                            r'(\d{4})/(\d{1,2})/(\d{1,2})'
                        ]

                        for pattern in date_patterns:
                            date_match = re.search(pattern, date_text)
                            if date_match:
                                year = date_match.group(1)
                                month = date_match.group(2).zfill(2)
                                day = date_match.group(3).zfill(2)
                                first_upload_date = f"{year}-{month}-{day}"
                                detail["first_upload_date"] = first_upload_date
                                self.logger.info(f"从新结构提取上架时间: {first_upload_date}")
                                return

            # 方法1：查找包含"上架"文本的元素（原始方法）
            upload_elements = soup.find_all(string=re.compile(r'上架|首发时间'))

            for upload_text in upload_elements:
                parent = upload_text.parent
                if parent:
                    # 获取父元素的所有文本
                    parent_text = self._normalize_text(parent.get_text(" ", strip=True))
                    self.logger.debug(f"找到上架相关信息: {parent_text[:100]}")

                    # 尝试解析日期格式
                    date_patterns = [
                        r'(\d{4})年(\d{1,2})月(\d{1,2})日',
                        r'(\d{4})-(\d{1,2})-(\d{1,2})',
                        r'(\d{4})\.(\d{1,2})\.(\d{1,2})',
                        r'(\d{4})/(\d{1,2})/(\d{1,2})'
                    ]

                    for pattern in date_patterns:
                        date_match = re.search(pattern, parent_text)
                        if date_match:
                            year = date_match.group(1)
                            month = date_match.group(2).zfill(2)
                            day = date_match.group(3).zfill(2)
                            first_upload_date = f"{year}-{month}-{day}"
                            detail["first_upload_date"] = first_upload_date
                            self.logger.info(f"从文本提取上架时间: {first_upload_date}")
                            return

            # 方法2：查找meta标签
            meta_date = soup.select_one('meta[property="og:novel:update_time"]') or \
                        soup.select_one('meta[property="og:novel:create_time"]')
            if meta_date and meta_date.get('content'):
                date_str = meta_date.get('content')
                # 尝试解析各种日期格式
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    first_upload_date = dt.strftime("%Y-%m-%d")
                    detail["first_upload_date"] = first_upload_date
                    self.logger.info(f"从meta标签提取上架时间: {first_upload_date}")
                    return
                except:
                    pass

            # 方法3：查找页面中的时间元素
            time_elements = soup.select('time, .time, .date, .publish-date')
            for time_elem in time_elements:
                time_text = self._normalize_text(time_elem.get_text(strip=True))
                if time_text and ('上架' in time_text or '首发' in time_text):
                    for pattern in [r'(\d{4})年(\d{1,2})月(\d{1,2})日', r'(\d{4})-(\d{1,2})-(\d{1,2})']:
                        date_match = re.search(pattern, time_text)
                        if date_match:
                            year = date_match.group(1)
                            month = date_match.group(2).zfill(2)
                            day = date_match.group(3).zfill(2)
                            first_upload_date = f"{year}-{month}-{day}"
                            detail["first_upload_date"] = first_upload_date
                            self.logger.info(f"从时间元素提取上架时间: {first_upload_date}")
                            return

            # 方法4：查找特定的CSS类（针对新的起点页面结构）
            # 查找包含"上架"文本的元素，然后查找其附近包含日期格式的元素
            for elem in soup.select('.text-c12.text-s-gray-500.mt-4px'):
                if '上架' in elem.get_text(strip=True):
                    # 查找前一个兄弟元素
                    prev_sibling = elem.previous_sibling
                    while prev_sibling and (not hasattr(prev_sibling, 'get_text') or
                                            not self._normalize_text(prev_sibling.get_text(strip=True))):
                        prev_sibling = prev_sibling.previous_sibling

                    if prev_sibling and hasattr(prev_sibling, 'get_text'):
                        date_text = self._normalize_text(prev_sibling.get_text(strip=True))
                        # 尝试解析日期
                        date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
                        if date_match:
                            year = date_match.group(1)
                            month = date_match.group(2).zfill(2)
                            day = date_match.group(3).zfill(2)
                            first_upload_date = f"{year}-{month}-{day}"
                            detail["first_upload_date"] = first_upload_date
                            self.logger.info(f"从CSS类提取上架时间: {first_upload_date}")
                            return

            self.logger.debug("未能在详情页提取到上架时间，将使用空值")
            detail["first_upload_date"] = ""

        except Exception as e:
            self.logger.error(f"提取上架时间失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            detail["first_upload_date"] = ""

    # ------------------------------------------------------------------
    # Chapters (FIRST_N_CHAPTERS)
    # ------------------------------------------------------------------

    def _extract_chapter_links(self, soup: BeautifulSoup, book_id: str) -> List[Tuple[str, str, str, int]]:
        """从目录页面提取章节链接"""
        chapter_links = []

        try:
            self.logger.info("开始提取章节链接...")

            # 方法1：使用精确路径
            # div.book-detail-mid -> div.book-info-outer -> div.book-catalog.jsAutoReport ->
            # div.catalog-all -> div.catalog-volume -> ul.volume-chapters -> li.chapter-item -> a

            # 找到目录区域
            catalog_all = soup.select_one('div.catalog-all')
            if catalog_all:
                # 找到所有的catalog-volume
                catalog_volumes = catalog_all.select('div.catalog-volume')
                self.logger.info(f'找到 {len(catalog_volumes)} 个分卷')

                # 寻找第一个有效分卷（章节数>=15的正文卷）
                target_volume = None
                for volume_index, volume in enumerate(catalog_volumes):
                    volume_title_elem = volume.select_one('h3.volume-title')
                    volume_title = volume_title_elem.text.strip() if volume_title_elem else f'分卷{volume_index + 1}'

                    # 跳过"作品相关"卷
                    if '作品相关' in volume_title:
                        self.logger.info(f'跳过作品相关卷: {volume_title}')
                        continue

                    # 查找章节列表
                    chapters_list = volume.select_one('ul.volume-chapters')
                    if not chapters_list:
                        self.logger.warning(f'分卷 {volume_index + 1} ({volume_title}) 没有找到volume-chapters')
                        continue

                    # 获取所有章节项
                    chapter_items = chapters_list.select('li.chapter-item')
                    chapter_count = len(chapter_items)
                    self.logger.info(f'分卷 {volume_index + 1} ({volume_title}) 有 {chapter_count} 个章节')

                    # 判断分卷是否有效：章节数>=15
                    if chapter_count < 15:
                        self.logger.warning(
                            f'分卷 {volume_index + 1} ({volume_title}) 章节数{chapter_count} < 10，跳过此分卷')
                        continue
                    else:
                        target_volume = volume
                        self.logger.info(f'找到有效分卷: {volume_title} (章节数: {chapter_count})')
                        break

                if not target_volume:
                    self.logger.warning('未找到有效分卷（章节数>=15）')
                    return []

                # 查找目标卷的章节列表
                chapters_list = target_volume.select_one('ul.volume-chapters')
                if not chapters_list:
                    self.logger.warning('正文卷没有找到volume-chapters')
                    return []

                # 获取所有章节项
                chapter_items = chapters_list.select('li.chapter-item')
                self.logger.info(f'正文卷有 {len(chapter_items)} 个章节')

                for item_index, item in enumerate(chapter_items):
                    try:
                        # 提取章节链接和标题
                        chapter_link = item.select_one('a')
                        if not chapter_link:
                            continue

                        href = chapter_link.get('href', '')
                        link_text = chapter_link.text.strip()  # 可能包含章节名和其他信息

                        # 提取title属性中的信息
                        title_attr = chapter_link.get('title', '')

                        # 如果没有href，尝试其他方式
                        if not href:
                            # 尝试从data-chapterid属性获取
                            data_chapterid = item.get('data-chapterid')
                            if data_chapterid:
                                href = f'/chapter/{book_id}/{data_chapterid}/'

                        if href:
                            # 处理相对URL
                            chapter_url = self._to_abs_url(href)

                            # 从title属性中提取首发时间、字数和章节名
                            first_post_time = ""
                            word_count = 0
                            chapter_name = link_text

                            if title_attr:
                                # title_attr格式可能为："首发时间：2023-01-01 字数：3000 章节名：第一章"
                                import re
                                time_match = re.search(r'首发时间[：:]?\s*(\d{4}-\d{2}-\d{2})', title_attr)
                                word_match = re.search(r'字数[：:]?\s*(\d+)', title_attr)
                                chapter_match = re.search(r'章节名[：:]?\s*(.+)', title_attr)

                                if time_match:
                                    first_post_time = time_match.group(1)
                                if word_match:
                                    word_count = int(word_match.group(1))
                                if chapter_match:
                                    chapter_name = chapter_match.group(1)

                            # 如果没有从title属性提取到章节名，使用link_text
                            if not chapter_name or chapter_name == link_text:
                                # 清理link_text，移除多余信息
                                chapter_name = link_text
                                # 移除可能的时间、字数信息
                                chapter_name = re.sub(r'\d{4}-\d{2}-\d{2}\s*', '', chapter_name)
                                chapter_name = re.sub(r'\d+字\s*', '', chapter_name)
                                chapter_name = chapter_name.strip()

                            # 确保有章节名
                            if not chapter_name:
                                # 使用默认章节名
                                chapter_name = f'第{item_index + 1}章'

                            # 如果还没有提取到字数，尝试从link_text中提取
                            if word_count == 0:
                                word_match = re.search(r'(\d+)字', link_text)
                                if word_match:
                                    word_count = int(word_match.group(1))
                                else:
                                    # 使用默认字数
                                    word_count = 3000

                            # 如果没有提取到发布时间，使用上架时间或当前日期
                            if not first_post_time:
                                first_post_time = ""

                            chapter_links.append((
                                chapter_name,
                                chapter_url,
                                first_post_time,
                                word_count
                            ))

                            # 如果已经找到足够多的章节，可以提前退出
                            if len(chapter_links) >= 25:  # 多找一些，以防后面有无效链接
                                self.logger.info(f'已找到足够章节，停止在当前分卷搜索')
                                break

                    except Exception as e:
                        self.logger.debug(f'解析章节链接失败: {e}')
                        continue

                    # 如果已经找到足够章节，跳出循环
                    if len(chapter_links) >= 30:
                        break

            # 方法2：如果新路径没找到，尝试其他选择器
            if not chapter_links:
                self.logger.warning('新路径未找到章节链接，尝试其他方法')

                # 尝试直接查找所有包含chapter的链接
                all_links = soup.find_all('a', href=re.compile(r'/chapter/\d+/\d+/'))

                if all_links:
                    self.logger.info(f'找到 {len(all_links)} 个章节链接')

                    for link in all_links:
                        try:
                            href = link.get('href', '')
                            link_text = link.text.strip()

                            if href:
                                # 处理相对URL
                                chapter_url = self._to_abs_url(href)

                                # 尝试从href中提取章节信息
                                match = re.search(r'/chapter/\d+/(\d+)/', href)

                                # 如果没有章节名，使用link_text
                                if link_text:
                                    chapter_name = link_text.strip()
                                else:
                                    chapter_name = f'章节{len(chapter_links) + 1}'

                                # 提取字数
                                word_count = 0
                                word_match = re.search(r'(\d+)字', link_text)
                                if word_match:
                                    word_count = int(word_match.group(1))
                                else:
                                    word_count = 3000  # 默认值

                                chapter_links.append((
                                    chapter_name,
                                    chapter_url,
                                    "",  # 发布时间
                                    word_count
                                ))

                        except Exception as e:
                            self.logger.debug(f'解析备用章节链接失败: {e}')
                            continue

        except Exception as e:
            self.logger.error(f'提取章节链接时发生错误: {e}')

        # 去重
        unique_chapters = []
        seen_urls = set()

        for chapter_title, chapter_url, publish_date, word_count in chapter_links:
            if chapter_url and chapter_url not in seen_urls:
                seen_urls.add(chapter_url)
                unique_chapters.append((chapter_title, chapter_url, publish_date, word_count))

        self.logger.info(f'最终提取到 {len(unique_chapters)} 个唯一章节链接')

        return unique_chapters

    def _parse_chapter_content(self, soup: BeautifulSoup) -> Optional[str]:
        """提取章节正文内容"""
        try:
            # 方法1: 使用新路径 div.app -> div.reader -> div.reader-content -> div.chapter-wrapper -> .relative -> .print -> .content -> .content-text
            content_text_elements = soup.select(
                'div.app div.reader div.reader-content div.chapter-wrapper .relative .print .content .content-text')

            if content_text_elements:
                self.logger.info(f'使用新路径找到 {len(content_text_elements)} 个 content-text 元素')

                # 提取每个 content-text 的文本
                paragraphs = []
                for elem in content_text_elements:
                    text = elem.get_text(strip=True)
                    if text:  # 只保留非空文本
                        paragraphs.append(text)

                # 将所有段落合并成一整章
                if paragraphs:
                    full_content = '\n'.join(paragraphs)
                    self.logger.debug(f'合并后章节内容长度: {len(full_content)} 字符')
                    return full_content

            # 方法2: 尝试简化的路径
            if not content_text_elements:
                content_text_elements = soup.select('.content-text')
                if content_text_elements:
                    self.logger.info(f'使用简化路径找到 {len(content_text_elements)} 个 content-text 元素')

                    paragraphs = []
                    for elem in content_text_elements:
                        text = elem.get_text(strip=True)
                        if text:
                            paragraphs.append(text)

                    if paragraphs:
                        full_content = '\n'.join(paragraphs)
                        self.logger.debug(f'合并后章节内容长度: {len(full_content)} 字符')
                        return full_content

            # 方法3: 回退到原来的选择器
            if not content_text_elements:
                self.logger.warning('新路径未找到内容，尝试原有选择器')
                content_selectors = [
                    '.chapter-content',
                    '.read-content',
                    '.chapter-entity',
                    '.chapter-text',
                    '.content',
                ]

                for selector in content_selectors:
                    content_elem = soup.select_one(selector)
                    if content_elem:
                        content = content_elem.get_text(strip=True)
                        if content:
                            self.logger.info(f'使用选择器 {selector} 找到内容，长度: {len(content)} 字符')
                            return content

            # 方法4: 尝试查找所有段落
            if not content_text_elements:
                paragraphs = soup.select('p')
                if paragraphs:
                    self.logger.info(f'找到 {len(paragraphs)} 个段落元素')

                    paragraph_texts = []
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if text and len(text) > 3:  # 过滤过短的文本
                            paragraph_texts.append(text)

                    if paragraph_texts:
                        full_content = '\n'.join(paragraph_texts)
                        self.logger.info(f'从段落合并内容，长度: {len(full_content)} 字符')
                        return full_content

            self.logger.warning('未找到章节正文内容')
            return None

        except Exception as e:
            self.logger.error(f'提取章节内容失败: {e}')
            return None

    def _fetch_single_chapter(self, chapter_url: str) -> Optional[str]:
        """获取单章内容"""
        try:
            # 访问章节页面
            self.logger.info(f'访问章节页面: {chapter_url}')

            # 使用 _get_soup 方法获取页面
            soup = self._get_soup(
                chapter_url,
                wait_css="div.reader-content .content-text, div.chapter-wrapper .content-text, .read-content, .chapter-entity",
                wait_sec=15,
            )

            if not soup:
                return None

            # 提取章节内容（使用新的路径）
            chapter_content = self._parse_chapter_content(soup)

            if chapter_content:
                # 清理内容，移除HTML标签
                content_text = chapter_content

                # 清理多余的空格和换行
                content_text = re.sub(r'\s+', ' ', content_text)

                return content_text

            return None

        except Exception as e:
            self.logger.error(f'获取章节内容失败 {chapter_url}: {e}')
            return None

    def _extract_novel_title_from_catalog(self, soup: BeautifulSoup, book_id: str) -> str:
        """从目录页提取小说标题"""
        try:
            # 方法1: 从meta标签提取
            title_meta = soup.select_one('meta[property="og:title"]')
            if title_meta:
                novel_title = title_meta.get('content', '').strip()
                # 清理标题，移除可能的后缀
                if ' - ' in novel_title:
                    novel_title = novel_title.split(' - ')[0]
                return novel_title

            # 方法2: 从h1标签提取
            h1_title = soup.select_one('h1.book-title, h1.works-title, .book-info h1')
            if h1_title and h1_title.text.strip():
                return h1_title.text.strip()

            # 方法3: 尝试从面包屑导航提取
            breadcrumb = soup.select_one('.crumb, .breadcrumb, .site-nav')
            if breadcrumb:
                breadcrumb_text = breadcrumb.get_text()
                if '>' in breadcrumb_text:
                    parts = breadcrumb_text.split('>')
                    if parts:
                        return parts[-1].strip()

            # 方法4: 从URL中获取的book_id构建
            # 如果没有找到标题，返回一个默认标题
            return f'小说_{book_id}'

        except Exception as e:
            self.logger.debug(f'从目录页提取标题失败: {e}')
            return f'小说_{book_id}'

    def _fetch_novel_chapters_from_website(self, novel_url: str, novel_id: str, chapter_count: int) -> List[Dict[str, Any]]:
        """从网站抓取小说章节内容"""
        try:
            # 从小说URL提取book_id
            book_id = self._extract_novel_id_from_url(novel_url)
            if not book_id:
                self.logger.warning(f'无法从URL提取book_id: {novel_url}')
                return []

            # 构建目录页面URL
            catalog_url = f'https://book.qidian.com/info/{book_id}/#Catalog'
            self.logger.info(f'访问目录页: {catalog_url}')

            # 访问目录页面
            soup = self._get_soup(
                catalog_url,
                wait_css="div.catalog-all, div.catalog-volume, ul.volume-chapters",
                wait_sec=15,
            )

            if not soup:
                return []

            # 从目录页提取书籍标题
            novel_title = self._extract_novel_title_from_catalog(soup, book_id)

            # 提取章节链接
            chapter_infos = self._extract_chapter_links(soup, book_id)

            if not chapter_infos:
                self.logger.warning(f'未找到章节链接: {novel_url}')
                return []

            # 只取前chapter_count章
            chapter_infos = chapter_infos[:chapter_count]

            chapters = []

            for i, (chapter_title, chapter_url, first_post_time, word_count) in enumerate(chapter_infos, 1):
                self.logger.info(f'获取第{i}章: {chapter_title}')

                try:
                    chapter_content = self._fetch_single_chapter(chapter_url)

                    if chapter_content:
                        chapter_data = {
                            'chapter_num': i,
                            'chapter_title': chapter_title,
                            'chapter_content': chapter_content,
                            'chapter_url': chapter_url,
                            'publish_date': first_post_time if first_post_time else "",
                            'word_count': word_count,
                            'novel_title': novel_title
                        }

                        chapters.append(chapter_data)

                    # 章节间延迟
                    if i < len(chapter_infos):
                        self._sleep_human(2, 4)

                except Exception as e:
                    self.logger.error(f'获取章节失败 {chapter_title}: {e}')
                    continue

            self.logger.info(f'成功获取 {len(chapters)} 章内容')
            return chapters

        except Exception as e:
            self.logger.error(f'获取章节列表失败: {e}')
            return []

    # 更新 fetch_first_n_chapters 方法，使用新的章节获取逻辑
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

        self.logger.info(f'获取小说章节内容: {novel_url} (小说ID: {novel_id}, 章节数: {n})')

        try:
            # 先获取书籍详情（用于获取标题和上架时间）
            detail = self.fetch_novel_detail(novel_url, novel_id)

            # 从详情中提取书名和上架时间
            novel_title = detail.get('title', '')
            first_upload_date = detail.get('first_upload_date', '')

            # 检查数据库中已有的章节
            existing_chapter_count = 0
            existing_chapters = []

            if self.db_handler and hasattr(self.db_handler, 'get_chapters_count'):
                # 获取数据库中已有的章节数量
                existing_chapter_count = self.db_handler.get_chapters_count(novel_id)
                self.logger.info(f'数据库中已有章节数: {existing_chapter_count}')

                # 如果已有章节数 >= 目标章节数，直接从数据库加载
                if existing_chapter_count >= n:
                    self.logger.info(f'数据库已有{existing_chapter_count}章，足够，直接从数据库加载')
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
                    self.logger.info(f'从数据库加载了{len(existing_chapters)}个现有章节')

            # 需要从网站抓取的新章节数
            need_chapter_count = n - existing_chapter_count
            if need_chapter_count <= 0:
                self.logger.info('不需要抓取新章节')
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
                f'需要抓取{need_chapter_count}个新章节（已有{existing_chapter_count}章，目标{n}章）')

            # 获取现有的最大章节号
            max_existing_chapter_num = 0
            if existing_chapters:
                max_existing_chapter_num = max([ch.get('chapter_num', 0) for ch in existing_chapters])

            # 从网站抓取章节
            new_chapters = self._fetch_novel_chapters_from_website(novel_url, novel_id, need_chapter_count)

            if not new_chapters:
                self.logger.warning('未能获取到新章节')
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

            # 调整新章节的章节号
            for i, chapter in enumerate(new_chapters, 1):
                chapter['chapter_num'] = max_existing_chapter_num + i

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

            # 保存新章节到数据库
            if self.db_handler and hasattr(self.db_handler, 'save_novel'):
                self.logger.info(f'保存{len(new_chapters)}个新章节到数据库')
                # 准备小说基本信息
                novel_data = {
                    'novel_id': novel_id,
                    'title': novel_title,
                    'author': detail.get('author', '未知'),
                    'platform': 'qidian',
                    'novel_url': novel_url,
                    'category': detail.get('main_category', ''),
                    'introduction': detail.get('intro', ''),
                    'tags': detail.get('tags', []),
                    'status': detail.get('status', ''),
                    'total_words': detail.get('total_words', 0),
                }

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
            self.logger.info(f"Enrich {i}/{min(len(items), max_books)}: {book.get('title', '')}")
            enriched = dict(book)

            if fetch_detail:
                detail = self.fetch_novel_detail(enriched.get("url", ""), enriched.get("platform_novel_id", ""))

                # 更新所有字段，包括总推荐数
                update_fields = ["title", "author", "intro", "status", "total_words",
                                 "total_recommend", "first_upload_date"]

                for k in update_fields:
                    dv = detail.get(k)
                    if dv is not None:
                        # 对于总推荐数，总是更新（因为榜单页没有这个数据）
                        if k == "total_recommend":
                            enriched[k] = dv
                        # 对于其他字段，只在为空时才更新
                        elif k not in enriched or not enriched[k]:
                            enriched[k] = dv

                # 分类处理：优先使用详情页的分类，但避免用"未知"覆盖正确的分类
                detail_main_cat = detail.get("main_category")
                if detail_main_cat and detail_main_cat != "未知":
                    enriched["main_category"] = detail_main_cat

                # 合并标签
                existing_tags = set(enriched.get("tags", []))
                new_tags = set(detail.get("tags", []))
                enriched["tags"] = list(existing_tags.union(new_tags))

            if fetch_chapters:
                chapters = self.fetch_first_n_chapters(enriched.get("url", ""), n=chapter_count)
                if chapters:
                    enriched["first_n_chapters"] = chapters

            out.append(enriched)
            self._sleep_human(2, 4)

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
            :param make_title_primary:
        """
        if not self.db_handler or not hasattr(self.db_handler, "save_rank_snapshot"):
            self.logger.warning("db_handler missing or lacks save_rank_snapshot; skip saving.")
            return None

        ident = self.rank_type_map.get(rank_type, RankIdentity(rank_family=rank_type))
        snapshot_date = snapshot_date or self._today_str()

        return self.db_handler.save_rank_snapshot(
            platform="qidian",
            rank_family=ident.rank_family,
            rank_sub_cat=ident.rank_sub_cat,
            snapshot_date=snapshot_date,
            items=list(items),
            source_url=source_url or "",
            make_title_primary=make_title_primary,  # 传递参数
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
        )

        # 设置标题为主标题
        if snapshot_id and self.db_handler:
            # 重新保存，确保标题被设置为主标题
            snapshot_id = self.save_rank_snapshot(
                rank_type=rank_type,
                items=enriched,
                snapshot_date=snapshot_date,
                source_url=(self.site_config.get("rank_urls") or {}).get(rank_type, ""),
                make_title_primary=True,  # 添加这个参数
            )

        # 可选保存章节
        if enrich_chapters and self.db_handler and hasattr(self.db_handler, "upsert_first_n_chapters"):
            for b in enriched:
                chapters = b.get("first_n_chapters") or []
                if not chapters:
                    continue

                self.db_handler.upsert_first_n_chapters(
                    platform="qidian",
                    platform_novel_id=b.get("platform_novel_id", ""),
                    publish_date=(snapshot_date or self._today_str()),
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
    # BaseSpider API: fetch_all_ranks
    # ------------------------------------------------------------------
    def fetch_all_ranks(self):
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
        self.logger.info("QidianSpider closed.")