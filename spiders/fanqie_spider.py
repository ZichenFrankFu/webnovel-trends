# spiders/fanqie_spider.py
"""FanqieSpider (番茄小说爬虫)

Phase 1 目标：
- 使用 Selenium 抓取起点各类榜单
- 抽取小说元信息（书名、作者、简介、主分类、细分题材 tag、状态、总字数）
- 抓取前 N 章正文用于后续开篇分析（FIRST_N_CHAPTERS）

数据库相关
- NOVELS.main_category：只存主分类（如"西方奇幻""科幻末世"）
- 起点"副分类"当作一个 tag 进入 TAGS / NOVEL_TAG_MAP （如"奇幻""穿越"）
- RANK_LISTS：rank_family 存大榜（阅读榜/新书榜）
  rank_sub_cat 番茄小说无副分类
- RANK_ENTRIES：番茄使用 reading_count（在读）作为热度参考
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

import config
from .base_spider import BaseSpider
from .fanqie_font_decoder import FANQIE_CHAR_MAP

"""Normalized rank identity that maps to RANK_LISTS schema."""
@dataclass(frozen=True)
class RankIdentity:
    rank_family: str
    rank_sub_cat: str = ""

"""番茄小说 Selenium 爬虫"""
class FanqieSpider(BaseSpider):
    """
    功能：
    1) 抓取榜单页（scroll load）（每个榜单仅一页），获取：排名/书名/作者/简介/在读/状态等
    2) 抓取详情页（补全：分类/标签/字数/等）
    3) 抓取前 N 章免费章节（智能：只抓数据库缺失的）
    4) 将抓取到的数据写入数据库
    """

    def __init__(self, site_config: Dict[str, Any], db_handler: Any = None):
        """
        Args:
            site_config: 可覆盖 config.WEBSITES['fanqie'] 的部分配置
            db_handler: 你的 DBHandler 实例（可选）
        """
        root_cfg = (getattr(config, "WEBSITES", {}) or {}).get("fanqie", {}) or {}
        merged_cfg = self._deep_merge_dict(root_cfg, site_config or {})

        super().__init__(merged_cfg, db_handler=db_handler)
        self.site_key = "fanqie"
        self.platform = "fanqie"
        self.site_key = "fanqie"
        self.default_chapter_count = int(self.site_config.get("chapter_extraction_goal", 5))
        self.rank_type_map: Dict[str, RankIdentity] = self._build_rank_type_map()
        self.char_map = FANQIE_CHAR_MAP

    # ------------------------------------------------------------------
    # Utils: decrypt
    # ------------------------------------------------------------------
    """解码text"""
    def _decrypt_text(self, text: str) -> str:
        if not text:
            return text
        return "".join(self.char_map.get(ch, ch) for ch in text)

    """解码html"""
    def _decrypt_html(self, html: str) -> str:
        if not html:
            return html
        for encrypted_char, real_char in self.char_map.items():
            if encrypted_char != real_char:
                html = html.replace(encrypted_char, real_char)
        return html

    """normalize 解码后的text"""
    def _clean_decrypt(self, text: str) -> str:
        return self._normalize_text(self._decrypt_text(text or ""))

    # ------------------------------------------------------------------
    # Utils: ids / urls
    # ------------------------------------------------------------------
    """从番茄的小说url中获取番茄的uid"""
    def _extract_novel_id_from_url(self, url: str) -> str:
        patterns = [
            r"/book/(\d+)",
            r"/page/(\d+)",
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

    """番茄特有的url格式"""
    def _add_enter_from_param(self, url: str) -> str:
        if not url:
            return url
        if "?" in url:
            return url if "enter_from=" in url else f"{url}&enter_from=Rank"
        return f"{url}?enter_from=Rank"

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------
    """Build mapping from config rank_type to normalized RankIdentity."""
    def _build_rank_type_map(self) -> Dict[str, RankIdentity]:
        """
        优先级：
          1) site_config.rank_type_map（外部覆盖）
          2) config.WEBSITES['fanqie'].rank_type_map
          3) fallback：对 rank_urls 的每个 key，用 key 本身作为 rank_family
        """
        custom = (self.site_config or {}).get("rank_type_map")
        if isinstance(custom, dict) and custom:
            out: Dict[str, RankIdentity] = {}
            for k, v in custom.items():
                out[k] = RankIdentity(
                    rank_family=(v or {}).get("rank_family", k),
                    rank_sub_cat=(v or {}).get("rank_sub_cat", "") or "",
                )
            return out

        fanqie_cfg = (getattr(config, "WEBSITES", {}) or {}).get("fanqie", {}) or {}
        cfg_map = fanqie_cfg.get("rank_type_map")
        if isinstance(cfg_map, dict) and cfg_map:
            out: Dict[str, RankIdentity] = {}
            for k, v in cfg_map.items():
                out[k] = RankIdentity(
                    rank_family=(v or {}).get("rank_family", k),
                    rank_sub_cat=(v or {}).get("rank_sub_cat", "") or "",
                )
            return out

        # fallback：把 rank_urls 的 key 当 family（保证不会返回 None 导致后续崩）
        out: Dict[str, RankIdentity] = {}
        rank_urls = (self.site_config or {}).get("rank_urls", {}) or {}
        for k in rank_urls.keys():
            out[k] = RankIdentity(rank_family=k, rank_sub_cat="")
        return out

    def _cfg_detail_rules(self) -> Dict[str, Any]:
        return (self.site_config or {}).get("detail_fallback_rules", {}) or {}

    def _should_fill_when_empty(self, field: str, current_value: Any) -> bool:
        """
        fanqie 用 detail_fallback_rules 控制补全行为
        """
        rules = self._cfg_detail_rules().get(field, {}) or {}
        when_empty = bool(rules.get("when_empty", True))
        when_zero = bool(rules.get("when_zero", True))

        if current_value is None:
            return True
        if isinstance(current_value, str):
            return when_empty and (current_value.strip() == "")
        if isinstance(current_value, (list, tuple, set, dict)):
            return when_empty and (len(current_value) == 0)
        if isinstance(current_value, (int, float)):
            return when_zero and (current_value == 0)
        return False

    def _set_if_needed(self, detail: Dict[str, Any], field: str, value: Any) -> None:
        if value is None:
            return
        cur = detail.get(field)
        if self._should_fill_when_empty(field, cur):
            detail[field] = value

    def _meta_content(self, soup: BeautifulSoup, selector: str) -> str:
        meta = soup.select_one(selector)
        if meta and meta.get("content"):
            return (meta.get("content") or "").strip()
        return ""

    def _text_of(self, elem: Any) -> str:
        if not elem:
            return ""
        return (elem.get_text(strip=True) or "").strip()

    # ------------------------------------------------------------------
    # Publish date extraction (YYYY-MM-DD)
    # ------------------------------------------------------------------
    def _extract_date_ymd_from_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        s = self._normalize_text(text)

        m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            return f"{y}-{mo}-{d}"

        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", s)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            return f"{y}-{mo}-{d}"

        m = re.search(r"(\d{4})[./](\d{1,2})[./](\d{1,2})", s)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            return f"{y}-{mo}-{d}"

        return None

    def _extract_publish_date_ymd(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            meta = soup.select_one(
                'meta[property="article:published_time"], meta[name="publish_date"], meta[name="date"], meta[itemprop="datePublished"]'
            )
            if meta and meta.get("content"):
                parsed = self._extract_date_ymd_from_text(meta.get("content", ""))
                if parsed:
                    return parsed

            page_text = self._normalize_text(soup.get_text(" ", strip=True))
            for pat in [
                r"发布日期[:：]\s*([^\s]+)",
                r"更新时间[:：]\s*([^\s]+)",
                r"发表时间[:：]\s*([^\s]+)",
                r"首发时间[:：]\s*([^\s]+)",
                r"上架时间[:：]\s*([^\s]+)",
            ]:
                m = re.search(pat, page_text)
                if m:
                    parsed = self._extract_date_ymd_from_text(m.group(1))
                    if parsed:
                        return parsed

            candidates = soup.find_all(string=re.compile(r"\d{4}([年./-])\d{1,2}([月./-])\d{1,2}"))
            for t in candidates:
                if isinstance(t, str):
                    parsed = self._extract_date_ymd_from_text(t)
                    if parsed:
                        return parsed

            return None
        except Exception as e:
            self.logger.debug(f"extract publish date ymd failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Hooks for BaseSpider._get_soup
    # ------------------------------------------------------------------
    def _postprocess_html(self, html: str) -> str:
        return self._decrypt_html(html)

    def _scroll_load(
        self,
        target_count: Optional[int] = None,
        max_scroll_attempts: Optional[int] = None,
        item_css: Optional[str] = None,
        scroll_pause_sec: Optional[float] = None,
        no_change_limit: int = 3,
    ) -> None:
        """
        番茄榜单页：滚动加载更多卡片
        """
        sel_cfg = (self.site_config or {}).get("selenium_specific", {}) or {}

        if target_count is None:
            target_count = int(sel_cfg.get("target_count", 30))
        if max_scroll_attempts is None:
            max_scroll_attempts = int(sel_cfg.get("max_scroll_attempts", 10))

        if item_css is None:
            item_css = sel_cfg.get("item_css", ".rank-book-item, .book-item, .book-list-item, .rank-item")

        # 兼容 config 里叫 scroll_delay（你现在 config.py 就是这个字段名）
        if scroll_pause_sec is None:
            scroll_pause_sec = float(sel_cfg.get("scroll_pause_sec", sel_cfg.get("scroll_delay", 2.5)))

        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_change_count = 0
        loaded_items = 0

        for _ in range(max_scroll_attempts):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause_sec)

            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_change_count += 1
                if no_change_count >= no_change_limit:
                    self.logger.info(f"[滚动加载]滚动高度连续 {no_change_count} 次未变化，停止滚动")
                    break
            else:
                no_change_count = 0
                last_height = new_height

            try:
                current_items = self.driver.find_elements(By.CSS_SELECTOR, item_css)
                if len(current_items) > loaded_items:
                    loaded_items = len(current_items)
                if loaded_items >= target_count:
                    self.logger.info(f"[滚动加载] 已达到目标数量 {target_count}，停止滚动")
                    break
            except Exception:
                pass

            self._humanlike_sleep(1, 3)

    # ------------------------------------------------------------------
    # Rank parsing
    # ------------------------------------------------------------------
    def _parse_rank_page(self, soup: BeautifulSoup, *, rank_type: str, page: int) -> List[Dict[str, Any]]:
        selectors = [
            ".rank-book-item",
            ".book-item",
            ".book-list-item",
            ".rank-item",
        ]

        for sel in selectors:
            nodes = soup.select(sel)
            if not nodes:
                continue

            out: List[Dict[str, Any]] = []
            for idx, node in enumerate(nodes, 1):
                b = self._parse_rank_item(node, idx=idx, page=page, rank_type=rank_type)
                if b:
                    out.append(b)

            if len(out) >= 3:
                return out

        return []

    def _parse_rank_item(self, node: Any, *, idx: int, page: int, rank_type: str) -> Optional[Dict[str, Any]]:
        try:
            title_elem = node.select_one('.title a, h3 a, .book-title a, a[href*="/page/"], a[href*="/book/"]')
            if not title_elem:
                return None

            title = self._decrypt_text(title_elem.get_text(strip=True)).strip()
            url = self._to_abs_url(title_elem.get("href", ""))
            if not url:
                return None

            pid = self._extract_novel_id_from_url(url)
            if not pid:
                return None

            per_page = int((self.site_config or {}).get("selenium_specific", {}).get("target_count", 30) or 30)
            global_rank = (page - 1) * per_page + idx

            author = "未知"
            author_elem = node.select_one('.author a, .author-name a, .writer a, .author-name, .author')
            if author_elem:
                author = self._decrypt_text(author_elem.get_text(strip=True)).strip() or author

            intro = ""
            intro_elem = node.select_one('.desc.abstract, .intro, .description, .book-desc, .desc')
            if intro_elem:
                intro = self._normalize_text(self._decrypt_text(intro_elem.get_text(strip=True)))

            status = "连载中"
            status_elem = node.select_one('.book-item-footer-status, .status, .state, .book-status')
            if status_elem:
                st = self._decrypt_text(status_elem.get_text(strip=True))
                if "完结" in st or "完本" in st:
                    status = "完本"
                elif "连载" in st:
                    status = "连载中"

            reading_count = 0
            reading_count_text = ""
            count_elem = node.select_one('.book-item-count, .read-count, .count, .reading-count, .hot-num, .popularity')
            if count_elem:
                reading_count_text = self._decrypt_text(count_elem.get_text(strip=True))
                clean = (
                    reading_count_text.replace("在读：", "")
                    .replace("在读", "")
                    .replace("阅读：", "")
                    .replace("阅读", "")
                    .replace("人", "")
                    .strip()
                )
                reading_count = self._parse_cn_number(clean) or 0
            else:
                text_blob = self._normalize_text(node.get_text(" ", strip=True))
                m = re.search(r"(在读|阅读)[:：]?\s*([\d\.万亿]+)", text_blob)
                if m:
                    reading_count_text = m.group(0)
                    reading_count = self._parse_cn_number(m.group(2)) or 0

            return {
                "platform": "fanqie",
                "platform_novel_id": pid,
                "novel_id": pid,

                "title": title,
                "author": author,
                "intro": intro,
                "status": status,
                "reading_count": int(reading_count),
                "reading_count_text": reading_count_text,

                "url": url,
                "rank": int(global_rank),
                "rank_type": rank_type,
                "fetch_date": self._today_str(),
                "fetch_time": time.strftime("%H:%M:%S"),

                "main_category": "",
                "tags": [],
                "total_words": 0,
            }

        except Exception as e:
            self.logger.debug(f"parse rank item failed: {e}")
            return None

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_rank_list
    # ------------------------------------------------------------------
    def fetch_rank_list(self, rank_type: str = "", pages: int = 1) -> List[Dict[str, Any]]:
        url_template = (self.site_config.get("rank_urls") or {}).get(rank_type)
        if not url_template:
            self.logger.error(f"rank_type not configured in rank_urls: {rank_type}")
            return []

        if pages is None or int(pages) <= 0:
            pages = int(self.site_config.get("pages_per_rank", 1))
        else:
            pages = int(pages)

        sel_cfg = (self.site_config or {}).get("selenium_specific", {}) or {}
        wait_css = sel_cfg.get(
            "wait_css",
            ".rank-book-item, .book-item, .book-list-item, .rank-item, a[href*='/page/'], a[href*='/book/']"
        )

        all_items: List[Dict[str, Any]] = []
        seen_pid: set[str] = set()
        seen_url: set[str] = set()

        for page in range(1, pages + 1):
            url = url_template.format(page=page) if "{page}" in url_template else url_template
            if page > 1 and "{page}" not in url_template:
                break

            self.logger.info(f"当前榜单[{rank_type}] page {page}/{pages}: {url}")

            soup = self._get_soup(url, wait_css=wait_css, is_scrolling=True)
            if not soup:
                continue

            page_items = self._parse_rank_page(soup, rank_type=rank_type, page=page)

            unique_items: List[Dict[str, Any]] = []
            for item in page_items:
                pid = (item.get("platform_novel_id") or item.get("novel_id") or "").strip()
                u = (item.get("url") or "").strip()

                if pid:
                    if pid in seen_pid:
                        continue
                    seen_pid.add(pid)
                    unique_items.append(item)
                    continue

                if u:
                    if u in seen_url:
                        continue
                    seen_url.add(u)
                    unique_items.append(item)

            all_items.extend(unique_items)
            self.logger.info(f"Page {page}: found {len(page_items)} items, {len(unique_items)} unique (global)")

            if page < pages:
                self._humanlike_sleep(1, 3)

        return all_items

    # ------------------------------------------------------------------
    # Detail parsing helpers
    # ------------------------------------------------------------------
    def _fill_detail_title_author_intro(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        if self._should_fill_when_empty("title", detail.get("title")):
            t = self._meta_content(soup, 'meta[property="og:title"]')
            if not t:
                t = self._text_of(soup.select_one("h1, .info-name h1, .book-title, header h1"))
            self._set_if_needed(detail, "title", self._clean_decrypt(t))

        if self._should_fill_when_empty("author", detail.get("author")):
            a = self._meta_content(soup, 'meta[property="og:novel:author"]')
            if not a:
                a = self._text_of(soup.select_one('.author-name:not(.author-desc), .author-name-text:not(.author-desc), .author-name, .author'))
            self._set_if_needed(detail, "author", self._clean_decrypt(a))

        if self._should_fill_when_empty("intro", detail.get("intro")):
            it = self._meta_content(soup, 'meta[property="og:description"]')
            if not it:
                it = self._text_of(soup.select_one(".intro, .description, .book-intro, .content, .book-desc, .desc"))
            self._set_if_needed(detail, "intro", self._clean_decrypt(it))

    def _fill_detail_category_tags(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        current_main = (detail.get("main_category") or "").strip()
        current_tags = detail.get("tags") or []

        need_main = self._should_fill_when_empty("main_category", current_main)
        need_tags = self._should_fill_when_empty("tags", current_tags)
        if not need_main and not need_tags:
            return

        tags: List[str] = []

        tag_elements = soup.select(".info-label-grey")
        if tag_elements:
            for tag_elem in tag_elements:
                tag = self._clean_decrypt(self._text_of(tag_elem))
                if tag and tag not in tags and len(tag) < 20:
                    tags.append(tag)
        else:
            container = soup.select_one(".info-label")
            if container:
                for span in container.find_all("span"):
                    cls = span.get("class", []) or []
                    if "info-label-yellow" in cls:
                        continue
                    tag = self._clean_decrypt(self._text_of(span))
                    if tag and tag not in tags and len(tag) < 20:
                        tags.append(tag)

        inferred_main = tags[0] if tags else ""
        if need_main:
            self._set_if_needed(detail, "main_category", inferred_main)

        if need_tags:
            existing_tags = list(detail.get("tags") or [])
            merged = self._dedupe_keep_order(existing_tags + tags)

            main_cat = (detail.get("main_category") or "").strip()
            if main_cat and main_cat in merged:
                merged = [t for t in merged if t != main_cat]
            detail["tags"] = merged

    def _fill_detail_status_words(self, soup: BeautifulSoup, detail: Dict[str, Any], page_url: str = "") -> None:
        try:
            need_status = self._should_fill_when_empty("status", detail.get("status"))
            need_words = self._should_fill_when_empty("total_words", detail.get("total_words"))
            if not need_status and not need_words:
                return

            if need_status:
                status_val = ""
                status_elem = soup.select_one(".info-label-yellow")
                if status_elem:
                    st = self._clean_decrypt(self._text_of(status_elem))
                    if "完结" in st or "完本" in st:
                        status_val = "完本"
                    elif "连载" in st:
                        status_val = "连载中"

                if not status_val:
                    e = soup.select_one(".book-state, .status, .state, .book-status")
                    if e:
                        st = self._clean_decrypt(self._text_of(e))
                        if "完结" in st or "完本" in st:
                            status_val = "完本"
                        elif "连载" in st:
                            status_val = "连载中"

                if status_val:
                    detail["status"] = status_val

            if need_words:
                total_words = 0
                word_count_elem = soup.select_one(".info-count-word")
                if word_count_elem:
                    detail_elem = word_count_elem.select_one(".detail")
                    unit_elem = word_count_elem.select_one(".text")
                    if detail_elem and unit_elem:
                        num_text = self._clean_decrypt(self._text_of(detail_elem))
                        unit_text = self._clean_decrypt(self._text_of(unit_elem))
                        try:
                            num = float(num_text)
                            total_words = int(num * 10000) if ("万" in unit_text) else int(num)
                        except Exception:
                            pass

                if total_words == 0:
                    page_text = self._clean_decrypt(soup.get_text(" ", strip=True))
                    m = re.search(r"字数[：:]\s*([0-9.]+[万亿]?)", page_text)
                    if m:
                        total_words = self._parse_cn_number(m.group(1)) or 0

                if total_words > 0:
                    detail["total_words"] = int(total_words)

        except Exception as e:
            self.logger.error(f"Error extracting status/word count: {e}")

    def _extract_publish_date(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        try:
            d = self._extract_publish_date_ymd(soup)
            detail["publish_date"] = d or ""
        except Exception as e:
            self.logger.error(f"提取上架时间失败: {e}")
            detail["publish_date"] = ""

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_novel_detail
    # ------------------------------------------------------------------
    def fetch_novel_detail(self, novel_url: str, pid: str, seed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        pid = (pid or self._extract_novel_id_from_url(novel_url) or "").strip()
        if pid and pid in self.book_cache:
            return self.book_cache[pid]

        seed = seed or {}

        def _empty_payload() -> Dict[str, Any]:
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
                "publish_date": "",
            }

        # helper 先定义，避免 not soup 分支里“先用后定义”崩溃
        def _apply_seed_to_detail(detail_dict: Dict[str, Any], seed_dict: Dict[str, Any]) -> None:
            if not seed_dict:
                return

            rank_primary = (self.site_config or {}).get("rank_fields_primary", []) or []
            if not rank_primary:
                rank_primary = ["platform_novel_id", "title", "author", "intro", "reading_count", "status"]

            for k in rank_primary:
                if k in ("platform_novel_id", "platform", "url"):
                    continue
                if seed_dict.get(k) is None:
                    continue
                if k == "tags":
                    detail_dict["tags"] = list(seed_dict.get("tags") or [])
                else:
                    detail_dict[k] = seed_dict.get(k)

            if not detail_dict.get("platform_novel_id") and seed_dict.get("novel_id"):
                detail_dict["platform_novel_id"] = seed_dict.get("novel_id")

        sel_cfg = (self.site_config or {}).get("selenium_specific", {}) or {}
        wait_css = sel_cfg.get("detail_wait_css", sel_cfg.get("wait_css", ".info-label, .info-count-item, meta[property='og:title'], h1"))
        is_scrolling = bool(sel_cfg.get("detail_is_scrolling", False))

        detail_url = self._add_enter_from_param(novel_url)
        self.logger.info(f"访问详情页: {detail_url}")

        soup = self._get_soup(detail_url, wait_css=wait_css, is_scrolling=is_scrolling)

        if not soup:
            out = _empty_payload()
            _apply_seed_to_detail(out, seed)
            if pid:
                self.book_cache[pid] = out
            return out

        detail = _empty_payload()

        # 1) rank seed 先写入（rank 为主来源字段）
        _apply_seed_to_detail(detail, seed)

        # 2) detail 页补全（按规则只补缺失）
        self._fill_detail_title_author_intro(soup, detail)
        self._fill_detail_category_tags(soup, detail)
        self._fill_detail_status_words(soup, detail, page_url=detail_url)
        self._extract_publish_date(soup, detail)

        # 3) reading_count：仅当 rank 没拿到才用 detail 扫描补（避免覆盖 rank）
        if self._should_fill_when_empty("reading_count", detail.get("reading_count")):
            reading_count = 0
            for elem in soup.select(".info-count-item"):
                tx = self._clean_decrypt(elem.get_text(strip=True))
                if ("在读" in tx) or ("阅读" in tx):
                    m = re.search(r"([0-9.]+[万亿]?)", tx)
                    if m:
                        reading_count = self._parse_cn_number(m.group(1)) or 0
                        break
            if reading_count:
                detail["reading_count"] = int(reading_count)

        if not (detail.get("title") or "").strip() and pid:
            detail["title"] = pid

        if pid:
            self.book_cache[pid] = detail
        return detail

    # ------------------------------------------------------------------
    # Chapters
    # ------------------------------------------------------------------
    def _extract_chapter_links(self, soup: BeautifulSoup, book_id: str, max_chapters: int = 5) -> List[Tuple[str, str, str, int]]:
        chapter_links: List[Tuple[str, str, str, int]] = []
        base_url = (self.site_config or {}).get("base_url", "https://fanqienovel.com").rstrip("/")

        chapter_selectors = [
            ".page-directory-content .chapter-item",
            ".chapter-item-list .chapter-item",
            "li[data-chapter-id]",
            ".chapter-list li",
            ".directory-list li",
            ".chapter-list-item",
        ]

        chapter_items = []
        for selector in chapter_selectors:
            items = soup.select(selector)
            if items:
                chapter_items = items
                break

        if not chapter_items:
            links = soup.find_all("a", href=True)
            candidates = [
                link for link in links
                if "/reader/" in (link.get("href", "") or "") and "#detail" not in (link.get("href", "") or "")
            ]
            chapter_items = candidates

        if not chapter_items:
            return chapter_links

        chapter_items = chapter_items[:max_chapters]

        for idx, item in enumerate(chapter_items, 1):
            try:
                chapter_link = item if getattr(item, "name", None) == "a" else (item.find("a", href=True) or item.select_one("a[href]"))
                if not chapter_link:
                    continue

                chapter_url = (chapter_link.get("href", "") or "").strip()
                chapter_title = self._decrypt_text(chapter_link.get_text(strip=True)).strip()

                if chapter_url.startswith("/"):
                    chapter_url = f"{base_url}{chapter_url}"
                elif not chapter_url.startswith("http"):
                    chapter_url = f"{base_url}/{chapter_url.lstrip('/')}"

                chapter_id = ""
                if "/reader/" in chapter_url:
                    chapter_id = chapter_url.split("/reader/")[-1].split("?")[0].split("#")[0]
                if not chapter_id:
                    continue

                chapter_index = idx
                if getattr(item, "name", None) != "a":
                    index_elem = item.select_one(".chapter-num, .chapter-index, .num")
                    if index_elem:
                        nums = re.findall(r"\d+", index_elem.get_text(strip=True))
                        if nums:
                            try:
                                chapter_index = int(nums[0])
                            except Exception:
                                pass

                chapter_links.append((chapter_title, chapter_url, chapter_id, chapter_index))
            except Exception:
                continue

        return chapter_links

    def _parse_chapter_content(self, soup: BeautifulSoup) -> str:
        try:
            selectors = [
                ".muye-reader-content",
                ".reader-content",
                ".chapter-content",
                ".content-text",
                ".chapter-entity",
                ".read-content",
                ".novel-content",
                ".article-content",
            ]

            def _cleanup(text: str) -> str:
                if not text:
                    return ""
                lines = [ln.strip() for ln in text.splitlines()]
                lines = [ln for ln in lines if ln]
                return "\n\n".join(lines).strip()

            for selector in selectors:
                content_elem = soup.select_one(selector)
                if not content_elem:
                    continue

                for x in content_elem(["script", "style"]):
                    x.decompose()

                paragraphs = content_elem.select("p, div.text")
                if paragraphs:
                    parts = []
                    for p in paragraphs:
                        t = self._decrypt_text(p.get_text(" ", strip=True)).strip()
                        if t:
                            parts.append(t)
                    content = _cleanup("\n\n".join(parts))
                else:
                    content = _cleanup(self._decrypt_text(content_elem.get_text("\n", strip=True)))

                if content:
                    return content

            ps = soup.find_all("p")
            if ps:
                parts = []
                for p in ps:
                    t = self._decrypt_text(p.get_text(" ", strip=True)).strip()
                    if t and len(t) > 10:
                        parts.append(t)
                content = _cleanup("\n\n".join(parts))
                if content:
                    return content

            return ""
        except Exception as e:
            self.logger.error(f"提取章节内容失败: {e}")
            return ""

    def _extract_publish_date_from_chapter(self, soup: BeautifulSoup) -> Optional[str]:
        return self._extract_publish_date_ymd(soup)

    def _fetch_single_chapter(self, chapter_url: str) -> Optional[Dict[str, Any]]:
        try:
            self.logger.info(f"访问章节页面: {chapter_url}")

            sel_cfg = (self.site_config or {}).get("selenium_specific", {}) or {}
            wait_css = sel_cfg.get("chapter_wait_css", sel_cfg.get("wait_css", ".muye-reader-content, .reader-content, h1.muye-reader-title, h1"))
            is_scrolling = bool(sel_cfg.get("chapter_is_scrolling", False))

            soup = self._get_soup(chapter_url, wait_css=wait_css, is_scrolling=is_scrolling)
            if not soup:
                return None

            chapter_content = self._parse_chapter_content(soup)
            publish_date = self._extract_publish_date_from_chapter(soup)

            word_count = 0
            if chapter_content:
                clean_content = re.sub(r"\s+", "", chapter_content)
                chinese_chars = re.findall(r"[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef]", clean_content)
                word_count = len(chinese_chars)

            chapter_title = ""
            for elem in soup.select("h1.muye-reader-title, .muye-reader-box-header .muye-reader-title, .muye-reader-title, h1"):
                if elem.find_parent(class_="muye-reader-nav") is not None:
                    continue
                chapter_title = self._decrypt_text(elem.get_text(strip=True)).strip()
                if chapter_title:
                    break

            return {
                "content": chapter_content,
                "title": chapter_title,
                "publish_date": publish_date or "",
                "word_count": int(word_count),
                "url": chapter_url,
            }

        except Exception as e:
            self.logger.error(f"获取章节内容失败 {chapter_url}: {e}")
            return None

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_first_n_chapters (智能增量)
    # ------------------------------------------------------------------
    def fetch_first_n_chapters(self, novel_url: str, target_chapter_count: int = 5) -> List[Dict[str, Any]]:
        if not target_chapter_count or int(target_chapter_count) <= 0:
            target_chapter_count = int(self.site_config.get("chapter_extraction_goal", 5))

        novel_id = self._extract_novel_id_from_url(novel_url)
        self.logger.info(f"[章节获取] 开始获取小说章节内容: {novel_url} (小说ID: {novel_id}, 目标章节数: {target_chapter_count})")

        if not novel_id:
            self.logger.warning("[章节获取] 无法从URL提取小说ID，跳过章节抓取")
            return []

        try:
            existing_count = self._get_existing_chapter_count(novel_id)

            if existing_count >= target_chapter_count:
                existing = self._get_existing_chapters(novel_id, target_chapter_count)
                return self._format_existing_chapters(existing, target_chapter_count, publish_date="")

            # 只有需要补全时才访问详情页
            detail = self.fetch_novel_detail(novel_url, novel_id, seed=None)
            display_title = detail.get("title") or f"小说ID:{novel_id}"
            publish_date = detail.get("publish_date", "")

            if existing_count >= target_chapter_count:
                self.logger.info(f"[章节智能补全] 《{display_title}》 DB已有{existing_count}章 ≥ 目标{target_chapter_count}章，直接从数据库加载")
                existing = self._get_existing_chapters(novel_id, target_chapter_count)
                return self._format_existing_chapters(existing, target_chapter_count, publish_date=publish_date)

            existing_chapters: List[Dict[str, Any]] = []
            if existing_count > 0:
                existing_chapters = self._get_existing_chapters(novel_id, existing_count)

            need_count = target_chapter_count - existing_count
            self.logger.info(f"[章节智能补全] 《{display_title}》 需要抓取{need_count}章（已有{existing_count}章，目标{target_chapter_count}章）")

            # 目录页：你原来写的 wait_css="..." 会导致等待逻辑无意义甚至超时，这里改成可用 selector
            catalog_url = f"{novel_url}#Catalog" if "#" not in novel_url else novel_url
            self.logger.info(f"[章节智能补全] 《{display_title}》 访问目录页: {catalog_url}")

            sel_cfg = (self.site_config or {}).get("selenium_specific", {}) or {}
            catalog_wait_css = sel_cfg.get(
                "catalog_wait_css",
                ".page-directory-content, .chapter-item, a[href*='/reader/'], .directory-list, .chapter-list"
            )

            soup = self._get_soup(catalog_url, wait_css=catalog_wait_css, is_scrolling=False)
            if not soup:
                self.logger.warning(f"[章节智能补全] 《{display_title}》 无法访问目录页，返回已有章节")
                return self._format_existing_chapters(existing_chapters, target_chapter_count, publish_date=publish_date)

            chapter_infos = self._extract_chapter_links(
                soup, novel_id, max_chapters=max(target_chapter_count, existing_count + need_count)
            )
            if not chapter_infos:
                self.logger.warning(f"[章节智能补全] 《{display_title}》 未找到章节链接，返回已有章节")
                return self._format_existing_chapters(existing_chapters, target_chapter_count, publish_date=publish_date)

            chapter_infos_to_fetch = self._slice_chapter_infos_to_fetch(chapter_infos, existing_count, need_count)
            if not chapter_infos_to_fetch:
                self.logger.info(f"[章节智能补全] 《{display_title}》 无需抓取新章节（目录不足或need_count=0）")
                return self._format_existing_chapters(existing_chapters, target_chapter_count, publish_date=publish_date)

            new_chapters: List[Dict[str, Any]] = []
            for i, (chapter_title, chapter_url, chapter_id, chapter_index) in enumerate(chapter_infos_to_fetch, 1):
                chapter_num = existing_count + i
                self.logger.info(f"[章节智能补全] 《{display_title}》 - 抓取第{chapter_num}章: {chapter_title}")

                chapter_data = self._fetch_single_chapter(chapter_url)
                if not chapter_data:
                    continue

                chapter_publish_date = (chapter_data.get("publish_date") or publish_date or "")
                new_chapters.append({
                    "chapter_num": chapter_num,
                    "chapter_title": chapter_title or chapter_data.get("title", ""),
                    "chapter_content": chapter_data.get("content", ""),
                    "chapter_url": chapter_url,
                    "publish_date": chapter_publish_date,
                    "word_count": int(chapter_data.get("word_count") or 0),
                    "platform_chapter_id": chapter_id,
                    "chapter_index": int(chapter_index or chapter_num),
                })

                if i < len(chapter_infos_to_fetch):
                    self._humanlike_sleep(1.0, 2.5)

            self.logger.info(f"[章节智能补全] 《{display_title}》 成功抓取 {len(new_chapters)} 章新章节")
            if new_chapters and self.db_handler and hasattr(self.db_handler, "upsert_first_n_chapters"):
                first_publish_date = (
                        (new_chapters[0].get("publish_date") or "").strip()
                        or (publish_date or "").strip()
                        or self._today_str()
                )

                self.logger.info(f"[章节智能补全] 准备保存 {len(new_chapters)} 个新章节到数据库")
                try:
                    upsert_n = self.db_handler.upsert_first_n_chapters(
                        platform="fanqie",
                        platform_novel_id=novel_id,
                        publish_date=first_publish_date,
                        chapters=new_chapters,
                        novel_fallback_fields={
                            "title": detail.get("title", ""),
                            "author": detail.get("author", ""),
                            "intro": detail.get("intro", ""),
                            "main_category": detail.get("main_category", ""),
                            "status": detail.get("status", ""),
                            "total_words": detail.get("total_words", 0),
                            "url": detail.get("url", novel_url),
                            "tags": detail.get("tags", []),
                        },
                    )
                    self.logger.info(f"[章节智能补全] upsert_first_n_chapters 返回结果: {upsert_n}")
                except Exception as e:
                    self.logger.error(f"[章节智能补全] 保存章节失败: {e}")

            return self._merge_chapters(existing_chapters, new_chapters, target_chapter_count,
                                        publish_date=publish_date)
        except Exception as e:
            self.logger.error(f"[章节获取] 获取小说章节内容失败 {novel_url}: {e}")
            return []

    # ------------------------------------------------------------------
    # Enrichment
    # ------------------------------------------------------------------
    def _reconcile_same_book_and_title(self, book: Dict[str, Any]) -> Optional[int]:
        """
        番茄改名常见：用作者/简介/url(platform_id) 判定是否已入库。
        若已入库且 title 是新名字：追加到 novel_titles（不强制设为 primary）。
        返回已存在的 novel_uid（若能找到），否则 None。
        """
        if not self.db_handler or not hasattr(self.db_handler, "find_existing_novel_uid"):
            return None

        pid = (book.get("platform_novel_id") or book.get("novel_id") or "").strip()
        url = (book.get("url") or "").strip()
        author = (book.get("author") or "").strip()
        intro = (book.get("intro") or "").strip()
        title = (book.get("title") or "").strip()

        novel_uid = None
        try:
            novel_uid = self.db_handler.find_existing_novel_uid(
                platform="fanqie",
                platform_novel_id=pid,
                url=url,
                author=author,
                intro=intro,
            )
        except Exception as e:
            self.logger.debug(f"[dedup] find_existing_novel_uid failed: {e}")
            return None

        if not novel_uid:
            return None

        # 已存在：追加新 title（只追加，不抢 primary；primary 仍由 snapshot 的 make_title_primary 决定）
        if title and hasattr(self.db_handler, "upsert_title_alias_by_uid"):
            try:
                self.db_handler.upsert_title_alias_by_uid(
                    novel_uid=int(novel_uid),
                    title=title,
                    snapshot_date=self._today_str(),
                    make_primary=False,
                )
                self.logger.info(f"[dedup] same book detected uid={novel_uid}; title alias upserted: {title}")
            except Exception as e:
                self.logger.debug(f"[dedup] upsert_title_alias_by_uid failed uid={novel_uid}: {e}")

        return int(novel_uid)

    def enrich_rank_items(
        self,
        items: Sequence[Dict[str, Any]],
        *,
        max_books: int = 20,
        fetch_detail: bool = True,
        fetch_chapters: bool = False,
        chapter_count: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if chapter_count is None:
            chapter_count = self.default_chapter_count

        site_cfg = self.site_config or {}
        rank_primary = site_cfg.get("rank_fields_primary") or []
        detail_primary = site_cfg.get("detail_fields_primary") or []

        if not rank_primary:
            rank_primary = ["platform_novel_id", "title", "author", "intro", "reading_count", "status"]
        if not detail_primary:
            detail_primary = ["total_words", "main_category", "tags", "publish_date"]

        out: List[Dict[str, Any]] = []

        for i, book in enumerate(list(items)[:max_books], 1):
            title = book.get("title", "未知")
            self.logger.info(f"[数据补完] 处理第{i}/{min(len(items), max_books)}本书: 《{title}》")

            enriched = dict(book)

            if fetch_detail:
                detail = self.fetch_novel_detail(
                    enriched.get("url", ""),
                    enriched.get("platform_novel_id", ""),
                    seed=enriched,
                )

                for k, dv in detail.items():
                    if dv is None:
                        continue
                    if k in ("platform", "platform_novel_id", "url", "rank", "rank_type", "fetch_date", "fetch_time"):
                        continue

                    if k in rank_primary:
                        if self._should_fill_when_empty(k, enriched.get(k)):
                            enriched[k] = dv
                        continue

                    if k in detail_primary:
                        if self._should_fill_when_empty(k, enriched.get(k)):
                            enriched[k] = dv
                        else:
                            if k == "tags":
                                existing = list(enriched.get("tags") or [])
                                incoming = list(detail.get("tags") or [])
                                merged = self._dedupe_keep_order(existing + incoming)
                                main_cat = (enriched.get("main_category") or "").strip()
                                if main_cat and main_cat in merged:
                                    merged = [t for t in merged if t != main_cat]
                                enriched["tags"] = merged
                        continue

                    if self._should_fill_when_empty(k, enriched.get(k)):
                        enriched[k] = dv

                # tags list 强制规范化
                tags_val = enriched.get("tags")
                if tags_val is None:
                    enriched["tags"] = []
                elif not isinstance(tags_val, list):
                    enriched["tags"] = list(tags_val) if isinstance(tags_val, (set, tuple)) else [str(tags_val)]
                enriched["tags"] = self._dedupe_keep_order(enriched.get("tags") or [])

                main_cat = (enriched.get("main_category") or "").strip()
                if main_cat and main_cat in enriched["tags"]:
                    enriched["tags"] = [t for t in enriched["tags"] if t != main_cat]

                # reading_count：rank 主来源；只在缺失时用 detail 值兜底
                if self._should_fill_when_empty("reading_count", enriched.get("reading_count")):
                    if int(detail.get("reading_count") or 0) > 0:
                        enriched["reading_count"] = int(detail["reading_count"])

            if fetch_chapters:
                _ = self._reconcile_same_book_and_title(enriched)
                chapters = self.fetch_first_n_chapters(enriched.get("url", ""), target_chapter_count=chapter_count)
                if chapters:
                    enriched["first_n_chapters"] = chapters
                    self.logger.info(f"[数据补完] 《{title}》 获取到 {len(chapters)} 章内容")

            out.append(enriched)
            self._humanlike_sleep(1, 3)

        return out

    def enrich_books_with_details(self, books, max_books: int = 20):
        return self.enrich_rank_items(
            books,
            max_books=max_books,
            fetch_detail=True,
            fetch_chapters=False,
            chapter_count=None,
        )

    # ------------------------------------------------------------------
    # Database: snapshot + one-stop pipeline
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

                normalized: List[Dict[str, Any]] = []
                for ch in chapters:
                    content = ch.get("chapter_content") or ch.get("content") or ""
                    title = ch.get("chapter_title") or ch.get("title") or ""
                    url = ch.get("chapter_url") or ch.get("url") or ""
                    num = ch.get("chapter_num") or ch.get("chapter_index") or 0
                    wc = ch.get("word_count") or 0
                    pd = ch.get("publish_date") or ""

                    normalized.append({
                        "chapter_num": int(num) if str(num).isdigit() else 0,
                        "chapter_title": str(title),
                        "chapter_content": str(content),
                        "chapter_url": str(url),
                        "publish_date": str(pd),
                        "word_count": int(wc) if str(wc).replace(".", "", 1).isdigit() else 0,
                        "platform_chapter_id": ch.get("platform_chapter_id", "") or ch.get("chapter_id", ""),
                        "chapter_index": ch.get("chapter_index", 0),
                    })

                first_chapter_publish_date = (
                    (normalized[0].get("publish_date") or "").strip()
                    or (b.get("publish_date") or "").strip()
                    or (snapshot_date or self._today_str())
                )

                self.db_handler.upsert_first_n_chapters(
                    platform="fanqie",
                    platform_novel_id=b.get("platform_novel_id", ""),
                    publish_date=first_chapter_publish_date,
                    chapters=normalized,
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

    def fetch_whole_rank(
            self,
            *,
            pages: Optional[int] = None,
            enrich_detail: bool = True,
            enrich_chapters: bool = False,
            chapter_count: Optional[int] = None,
            snapshot_date: Optional[str] = None,
            max_books: int = 200,
    ) -> List[Dict[str, Any]]:
        all_books: List[Dict[str, Any]] = []

        rank_urls = (self.site_config.get("rank_urls") or {})
        if not rank_urls:
            self.logger.warning("site_config.rank_urls is empty; nothing to fetch.")
            return all_books

        crawler_cfg = getattr(self.config, "CRAWLER_CONFIG", {}) or {}
        global_fetch = (crawler_cfg.get("page_fetch", {}) or {})
        site_fetch = (self.site_config or {}).get("page_fetch_overrides", {}) or {}
        cfg = {**global_fetch, **site_fetch}

        between_rank_sleep = cfg.get("between_rank_sleep_range", None)
        if not isinstance(between_rank_sleep, (list, tuple)) or len(between_rank_sleep) != 2:
            # 保留你原来的默认值（不强求你改 config）
            between_rank_sleep = (1, 3)

        for rank_type in rank_urls.keys():
            try:
                self.logger.info(f"[一键启动] 开始处理榜单: {rank_type}")

                result = self.fetch_and_save_rank(
                    rank_type=rank_type,
                    pages=pages,
                    enrich_detail=enrich_detail,
                    enrich_chapters=enrich_chapters,
                    chapter_count=chapter_count,
                    snapshot_date=snapshot_date,
                    max_books=max_books,
                )

                items = result.get("items") or []
                all_books.extend(items)

                if hasattr(self, "_save_raw_data"):
                    try:
                        fname = f"{self.name}_{rank_type}_{time.strftime('%Y%m%d')}.json"
                        self._save_raw_data(items, fname)
                    except Exception as e:
                        self.logger.warning(f"[一键启动] 保存 raw 数据失败 rank={rank_type}: {e}")

            except Exception as e:
                self.logger.error(f"[一键启动] 处理榜单 {rank_type} 失败: {e}")

            finally:
                # 无论成功/失败，都执行 driver 生命周期策略（由 config 控制是否真的重启）
                try:
                    if hasattr(self, "restart_driver_after_rank"):
                        self.restart_driver_after_rank(rank_type)
                except Exception as e:
                    self.logger.warning(f"[一键启动] restart_driver_after_rank failed rank={rank_type}: {e}")

                # 榜单间隔（降低风控/降低 driver 压力）
                try:
                    self._humanlike_sleep(float(between_rank_sleep[0]), float(between_rank_sleep[1]))
                except Exception:
                    pass

        return all_books
