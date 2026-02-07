# spiders/qidian_spider.py
"""QidianSpider (起点中文网爬虫)

Phase 1 目标：
- 使用 Selenium 抓取起点各类榜单
- 抽取小说元信息（书名、作者、简介、主分类、细分题材 tag、状态、总字数）
- 抓取前 N 章正文用于后续开篇分析（FIRST_N_CHAPTERS）

数据库相关
- NOVELS.main_category：只存主分类（如"都市""玄幻"）
- 起点"副分类"当作一个 tag 进入 TAGS / NOVEL_TAG_MAP （如"异术超能"）
- RANK_LISTS：rank_family 存大榜（畅销榜/月票榜/推荐榜/收藏榜/新书榜）
  rank_sub_cat 仅用于起点新书榜四小类（签约作者，公众作者，新人签约，新人作者），其它为空
- RANK_ENTRIES：起点指标使用 total_recommend（总推荐）作为热度参考
"""

from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from bs4 import BeautifulSoup
from selenium import webdriver
import config
from .base_spider import BaseSpider

# Load global config
GLOBAL_SELENIUM_CONFIG = getattr(config, "SELENIUM_CONFIG", {}) or {}

"""Normalized rank identity that maps to RANK_LISTS schema."""
@dataclass(frozen=True)
class RankIdentity:
    rank_family: str
    rank_sub_cat: str = ""  # only for 起点新书榜四小类; otherwise ""

"""起点中文网 Selenium 爬虫"""
class QidianSpider(BaseSpider):
    """
    功能：
    1) 抓取榜单页（可多页，获取榜上书籍信息：排名/书名/分类/作者等信息）
    2) 抓取详情页（补全元信息：分类/状态/字数/简介等）
    3) 抓取前 N 章免费章节
    4) 将抓取到的数据写入数据库
    """

    """初始化起点爬虫"""
    def __init__(self, site_config: Dict[str, Any], db_handler: Any = None):
        """
        Args:
            site_config: 站点配置字典，关键字段：
            - base_url: str
            - rank_urls: dict[str, str] (url模板支持{page})
            - pages_per_rank: int
            - chapter_extraction_goal: int
            - selenium_specific: 可选的selenium覆盖配置
            - max_retries: 最大重试次数，默认3
        db_handler: 数据库处理器，负责将抓取到的内容持久化，加入数据库
            - save_rank_snapshot(...)
            - upsert_first_n_chapters(...)
        """
        root_cfg = (getattr(config, "WEBSITES", {}) or {}).get("qidian", {}) or {}
        merged_cfg = self._deep_merge_dict(root_cfg, site_config or {})

        super().__init__(merged_cfg, db_handler=db_handler)
        self.config = merged_cfg

        self.default_chapter_count = int(site_config.get("chapter_extraction_goal", 5))
        self.rank_type_map: Dict[str, RankIdentity] = self._build_rank_type_map()

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------
    """Split '大类·副类' into (main_category, sub_as_tag)"""
    def _split_qidian_category(self, raw_category: str) -> Tuple[str, Optional[str]]:
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

        # 子分类到主分类的映射（常见的副分类/题材标签）
        sub_to_main_map = self.site_config.get('sub_to_main_map', {})
        if not qidian_main_categories:
            sub_to_main_map = {
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
            }
        self.logger.debug(f"[分类解析] 开始解析分类: '{raw}'")

        # 处理有分隔符的情况
        if "·" in raw:
            parts = [part.strip() for part in raw.split("·") if part.strip()]

            self.logger.debug(f"[分类解析] 分隔符分割结果: {parts}")

            if len(parts) >= 2:
                # 尝试识别主分类
                main_cat = parts[0]
                sub_cat = "·".join(parts[1:])

                # 检查第一个部分是否为主分类
                if main_cat in qidian_main_categories:
                    self.logger.debug(f"[分类解析] 识别到主分类: '{main_cat}'")
                    return main_cat, sub_cat if sub_cat else None
                else:
                    # 检查第一个部分是否是子分类（有映射关系）
                    if main_cat in sub_to_main_map:
                        mapped_main = sub_to_main_map[main_cat]
                        self.logger.debug(f"[分类解析] 通过映射识别主分类: '{main_cat}' -> '{mapped_main}'")
                        return mapped_main, "·".join(parts)
                    # 检查是否有其他部分可能是主分类
                    for i, part in enumerate(parts):
                        if part in qidian_main_categories:
                            main_cat = part
                            sub_cat = "·".join([p for j, p in enumerate(parts) if j != i])
                            self.logger.debug(f"[分类解析] 在第{i + 1}部分找到主分类: '{main_cat}'")
                            return main_cat, sub_cat if sub_cat else None

                # 如果没有找到主分类，返回第一个部分作为主分类（可能不正确）
                self.logger.warning(f"[分类解析] 未识别到主分类，返回第一部分作为主分类: '{main_cat}'")
                return main_cat, "·".join(parts[1:]) if len(parts) > 1 else None
            else:
                # 只有一个部分但有分隔符
                cleaned = raw.replace("·", "").strip()
                self.logger.debug(f"[分类解析] 清理后的分类: '{cleaned}'")

                if cleaned in qidian_main_categories:
                    self.logger.debug(f"[分类解析] 识别为纯主分类: '{cleaned}'")
                    return cleaned, None
                elif cleaned in sub_to_main_map:
                    mapped_main = sub_to_main_map[cleaned]
                    self.logger.info(f"[分类解析] 通过映射识别主分类: '{cleaned}' -> '{mapped_main}'")
                    return mapped_main, cleaned
                else:
                    self.logger.warning(f"[分类解析] 无法识别的分类: '{cleaned}'")
                    return "未知", cleaned if cleaned else None
        else:
            # 没有分隔符的情况
            if raw in qidian_main_categories:
                self.logger.info(f"[分类解析] 识别为纯主分类: '{raw}'")
                return raw, None
            elif raw in sub_to_main_map:
                mapped_main = sub_to_main_map[raw]
                self.logger.info(f"[分类解析] 通过映射识别主分类: '{raw}' -> '{mapped_main}'")
                return mapped_main, raw
            else:
                # 检查是否包含主分类关键词
                for main_cat in qidian_main_categories:
                    if raw.startswith(main_cat) or main_cat in raw:
                        self.logger.info(f"[分类解析] 文本中包含主分类关键词: '{main_cat}'")
                        return main_cat, raw.replace(main_cat, "").strip() if raw != main_cat else None

                self.logger.warning(f"[分类解析] 无法识别的分类: '{raw}'")
                return "未知", raw if raw else None

    """从数据库novel_titles表中获取normalized标题(title_norm)"""
    def _get_novel_title_norm_from_db(self, novel_id: str) -> Optional[str]:
        if not self.db_handler:
            return None

        try:
            # 使用db_handler的get_novel_title_norm方法
            if hasattr(self.db_handler, 'get_novel_title_norm'):
                title_norm = self.db_handler.get_novel_title_norm("qidian", novel_id)
                if title_norm:
                    self.logger.debug("[标题查询] 从数据库获取到归一化标题: %s (小说ID: %s)", title_norm, novel_id)
                else:
                    self.logger.debug("[标题查询] 数据库中未找到归一化标题 (小说ID: %s)", novel_id)
                return title_norm
            else:
                self.logger.warning("[标题查询] db_handler没有get_novel_title_norm方法")
                return None

        except Exception as e:
            self.logger.debug("[标题查询] 获取归一化标题失败 (小说ID: %s): %s", novel_id, e)

        return None

    """获取用于显示的标题，优先使用normalized标题"""
    def _get_display_title(self, novel_id: str, fallback_title: str = "") -> Tuple[str, str]:
        """
        Args:
            novel_id: 小说平台ID
            fallback_title: 后备标题（当无法从数据库获取时使用）

        Returns:
            Tuple[显示标题, 标题来源]
        """
        # 首先尝试从数据库获取归一化标题
        title_norm = self._get_novel_title_norm_from_db(novel_id)

        if title_norm:
            return title_norm, "norm标题"
        elif fallback_title:
            return fallback_title, "fallback标题"
        else:
            # 如果都没有，尝试从数据库中查询
            try:
                if self.db_handler and hasattr(self.db_handler, 'get_novel_title'):
                    title = self.db_handler.get_novel_title("qidian", novel_id)
                    if title:
                        return title, "数据库标题"
            except Exception as e:
                self.logger.debug("[标题显示] 获取数据库标题失败: %s", e)

            return f"小说ID:{novel_id}", "ID"

    """从起点的小说url中获取起点的uid"""
    def _extract_novel_id_from_url(self, url: str) -> str:
        patterns = [
            r"/book/(\d+)/",
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

    # ------------------------------------------------------------------
    # Rank type mapping (rank_family / rank_sub_cat)
    # ------------------------------------------------------------------
    """Build mapping from config rank_type to normalized RankIdentity."""
    def _build_rank_type_map(self) -> Dict[str, RankIdentity]:
        # 首先检查 site_config 中的自定义映射
        custom = self.site_config.get("rank_type_map")
        if isinstance(custom, dict) and custom:
            out: Dict[str, RankIdentity] = {}
            for k, v in custom.items():
                out[k] = RankIdentity(
                    rank_family=v.get("rank_family", k),
                    rank_sub_cat=v.get("rank_sub_cat", "") or "",
                )
            return out

        # 然后尝试从 config.WEBSITES 中获取
        try:
            from config import WEBSITES
            qidian_config = WEBSITES.get("qidian", {})
            if qidian_config:
                config_rank_type_map = qidian_config.get("rank_type_map")
                if isinstance(config_rank_type_map, dict) and config_rank_type_map:
                    out: Dict[str, RankIdentity] = {}
                    for k, v in config_rank_type_map.items():
                        out[k] = RankIdentity(
                            rank_family=v.get("rank_family", k),
                            rank_sub_cat=v.get("rank_sub_cat", "") or "",
                        )
                    return out
        except ImportError:
            # 如果无法导入 config，则继续使用默认映射
            pass
        except Exception as e:
            self.logger.warning(f"从 config 获取 rank_type_map 失败: {e}")

    # ------------------------------------------------------------------
    # Rank Page Parsing -> novel_id, category, tag
    # ------------------------------------------------------------------
    """Parse Rank Page soup into raw rank items"""
    def _parse_rank_page(self, soup: BeautifulSoup, *, rank_type: str, page: int) -> List[Dict[str, Any]]:
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

    """Parse one rank item node into a standardized dict"""
    def _parse_rank_item(self, node: Any, *, idx: int, page: int, rank_type: str) -> Optional[Dict[str, Any]]:
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
                "status": None,         # wait for detail page fill
                "total_words": None,    # wait for detail page fill
                "url": url,
                "rank": global_rank,
                "total_recommend": total_recommend,  # 将在详情页填充
                "rank_type": rank_type,
            }
        except Exception as e:
            self.logger.debug(f"parse rank item failed: {e}")
            return None

    """Extract raw category string (usually '大类·副类') from rank item"""
    def _extract_category_from_rank_item(self, node: Any) -> str:
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

    """Extract tag list from rank item (excluding Qidian sub-category)"""
    def _extract_tags_from_rank_item(self, node: Any) -> List[str]:
        tags: List[str] = []
        for el in node.select(".tag span, .tags a, .tag-wrap a"):
            t = self._normalize_text(el.get_text(strip=True))
            if t and len(t) <= 16:
                tags.append(t)
        return self._dedupe_keep_order(tags)

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_rank_list
    # ------------------------------------------------------------------
    """Fetch a rank list (multi-page) and return standardized items"""
    def fetch_rank_list(self, rank_type: str = "畅销榜", page = 5) -> List[Dict[str, Any]]:
        """
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
            self.logger.info(f"当前榜单[{rank_type}] page {page}/{pages}: {url}")

            soup = self._get_soup(url, wait_css="...", is_scrolling=False)

            if not soup:
                continue

            all_items.extend(self._parse_rank_page(soup, rank_type=rank_type, page=page))
            self._humanlike_sleep(2.5, 5.5)

        return all_items

    # ------------------------------------------------------------------
    # Detail Page -> title, author, intro, status, total_recommend
    # ------------------------------------------------------------------
    """从 Detail Page soup中获取书名，作者，和简介并填充输入db的dict"""
    def _fill_detail_title_author_intro(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
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

    """从Detail Page soup中提取完整的分类信息（主分类·副分类）"""
    def _extract_category_from_detail(self, soup: BeautifulSoup) -> str:
        try:
            self.logger.info("[详情页]开始提取分类信息...")

            # 方法1：优先从 book-attribute 中提取分类信息
            # 根据HTML结构：<p class="book-attribute"> ... <a>都市</a><span class="dot">·</span><a>异术超能</a> ...
            book_attribute = soup.select_one('.book-attribute')

            if book_attribute:
                self.logger.debug(f"找到book-attribute元素: {book_attribute.prettify()[:500]}")

                # 查找所有的a标签（分类链接）
                category_links = []
                for link in book_attribute.find_all('a', href=True):
                    link_text = self._normalize_text(link.get_text(strip=True))
                    link_href = link.get('href', '')
                    link_title = link.get('title', '')

                    # 过滤掉非分类链接（可能包含其他信息）
                    # 分类链接通常有特定的href模式或title
                    if link_text and (link_href and ('chanId' in link_href or link_href.endswith('//'))):
                        # 检查是否可能是分类
                        if any(keyword in link_title for keyword in ['小说', '作品']):
                            category_links.append((link_text, link_title))
                        else:
                            # 如果title没有关键词，但文本长度合适，也可能是分类
                            if len(link_text) <= 4:  # 主分类通常1-2个字
                                category_links.append((link_text, link_title))
                            elif len(link_text) <= 8:  # 子分类可能稍长
                                category_links.append((link_text, link_title))

                self.logger.debug(f"从book-attribute找到的分类链接: {category_links}")

                if len(category_links) >= 2:
                    # 尝试组合主分类和子分类
                    main_cat = category_links[0][0]
                    sub_cat = category_links[1][0]

                    # 确保不是重复的
                    if main_cat != sub_cat:
                        result = f"{main_cat}·{sub_cat}"
                        self.logger.info(f"从book-attribute组合分类: '{result}'")
                        return result
                elif len(category_links) == 1:
                    result = category_links[0][0]
                    self.logger.info(f"从book-attribute提取单一分类: '{result}'")
                    return result

            # 方法2：使用正则表达式查找 book-attribute 中的分类模式
            if book_attribute:
                # 提取所有文本，包括分隔符
                full_text = self._normalize_text(book_attribute.get_text(" ", strip=True))
                self.logger.debug(f"book-attribute完整文本: '{full_text}'")

                # 尝试匹配 "都市 · 异术超能" 这种模式
                # 使用正则表达式查找可能的分类组合
                import re
                pattern = r'([\u4e00-\u9fff]{1,4})\s*[·•]\s*([\u4e00-\u9fff]{2,8})'
                match = re.search(pattern, full_text)

                if match:
                    main_cat = match.group(1)
                    sub_cat = match.group(2)
                    result = f"{main_cat}·{sub_cat}"
                    self.logger.info(f"通过正则匹配分类: '{result}'")
                    return result

                # 如果正则没匹配到，尝试查找所有中文字符段
                chinese_parts = re.findall(r'[\u4e00-\u9fff]+', full_text)
                self.logger.debug(f"中文部分: {chinese_parts}")

                # 过滤掉常见非分类词汇
                exclude_words = ['连载', '签约', 'VIP', '完本', '上架', '免费']
                category_candidates = [part for part in chinese_parts if part not in exclude_words]

                if len(category_candidates) >= 2:
                    main_cat = category_candidates[0]
                    sub_cat = category_candidates[1]
                    if main_cat != sub_cat:
                        result = f"{main_cat}·{sub_cat}"
                        self.logger.info(f"从文本中提取分类组合: '{result}'")
                        return result
                elif category_candidates:
                    result = category_candidates[0]
                    self.logger.info(f"从文本中提取单一分类: '{result}'")
                    return result

            # 方法3：从 meta 标签提取（备用方案）
            meta_category = soup.select_one('meta[property="og:novel:category"]')
            if meta_category and meta_category.get('content'):
                category = meta_category.get('content')
                self.logger.info(f"从meta标签提取分类: '{category}'")
                return category

            # 方法4：尝试从面包屑导航提取
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

                    if category_candidates:
                        # 通常最后一个或倒数第二个是分类
                        for i in range(len(category_candidates) - 1, -1, -1):
                            cat = category_candidates[i]
                            if len(cat) <= 4:  # 主分类通常较短
                                # 尝试与下一个候选组合
                                if i + 1 < len(category_candidates):
                                    next_cat = category_candidates[i + 1]
                                    if len(next_cat) <= 6:  # 子分类通常也较短
                                        result = f"{cat}·{next_cat}"
                                        self.logger.info(f"从面包屑组合分类: '{result}'")
                                        return result
                                return cat

            self.logger.warning("未能提取分类信息，返回'未知'")
            return "未知"

        except Exception as e:
            self.logger.error(f"提取分类失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return "未知"

    """Detail Pagesoup提取主分类和副分类并填充并填充输入db的dict"""
    def _fill_detail_category_tags(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        # 如果已经有主分类且不是"未知"，则跳过详情页分类提取
        current_main = detail.get("main_category", "")
        current_tags = detail.get("tags", [])

        if current_main and current_main != "未知":
            self.logger.info(f"[分类处理] 已有分类信息，跳过提取 - main='{current_main}', tags={current_tags}")
            return

        self.logger.info(f"[分类处理] 开始处理分类信息...")
        raw_cat = self._extract_category_from_detail(soup)
        main_cat, sub_tag = self._split_qidian_category(raw_cat)

        self.logger.info(f"[分类处理] 解析结果 - 原始: '{raw_cat}', 主分类: '{main_cat}', 副分类: '{sub_tag}'")

        # 更新主分类
        detail["main_category"] = main_cat

        # 合并标签：已有的标签加上新提取的副分类（如果存在）
        tags = detail.get("tags", [])

        if sub_tag and sub_tag not in tags:
            self.logger.info(f"[分类处理] 添加副分类作为标签: '{sub_tag}'")
            tags.append(sub_tag)

        # 从详情页提取的标签
        tag_count = 0
        for sel in [".tag-wrap a", ".tags a", ".book-tag a", ".tag-list a", ".book-tags a"]:
            for el in soup.select(sel):
                t = self._normalize_text(el.get_text(strip=True))
                if t and t not in tags:
                    # 过滤掉可能是主分类的标签和常见非标签文本
                    exclude_words = ['VIP', '签约', '完结', '连载', '上架', '免费', '热门', '推荐']
                    if (t != main_cat and t not in exclude_words):
                        self.logger.debug(f"[分类处理] 发现标签: '{t}'")
                        tags.append(t)
                        tag_count += 1

        # 确保主分类不作为标签
        if main_cat in tags:
            self.logger.info(f"[分类处理] 从标签中移除主分类: '{main_cat}'")
            tags.remove(main_cat)

        detail["tags"] = self._dedupe_keep_order(tags)
        self.logger.info(f"[分类处理] 最终结果 - main='{detail['main_category']}', tags={tags} (新增{tag_count}个标签)")

    """详情页soup提取是否完结并填充并填充输入db的dict"""
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
                        detail["status"] = "连载"
                    elif status_text == "完本":
                        detail["status"] = "完本"
                    elif "连载" in status_text:
                        detail["status"] = "连载"
                    elif "完本" in status_text or "完结" in status_text:
                        detail["status"] = "完本"

            # 如果通过span没有找到状态，回退到正则搜索整个页面
            if not detail.get("status"):
                page_text = self._normalize_text(soup.get_text(" ", strip=True))
                if re.search(r"\b完本\b|\b完结\b", page_text):
                    detail["status"] = "完本"
                elif re.search(r"\b连载\b", page_text):
                    detail["status"] = "连载"

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
                self.logger.debug(f"Extracted 小说总字数: {word_count}")
            else:
                self.logger.warning("Could not extract word count from page")

            # 记录提取结果
            self.logger.info(f"[详情页补充] 小说状态: '{detail.get('status')}', 小说总字数: {detail.get('total_words')}")

        except Exception as e:
            self.logger.error(f"Error extracting status and word count from page: {e}")

    """详情页soup提取总推荐数并填充并填充输入db的dict"""
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
                                    self.logger.info(f"[详情页补充]从parent提取总推荐: {m.group(1)} -> {num}")
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
    """获取小说metadata and normalize"""
    def fetch_novel_detail(self, novel_url: str, pid: str, seed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
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
            - publish_date
        """

        # 先确保 pid 正确（避免把 URL 当作 novel_id 用于标题显示）
        pid = pid or self._extract_novel_id_from_url(novel_url)
        seed = seed or {}
        seed_title = seed.get("title", "") or ""

        # 获取标题以更直观地在 log 中展示目标小说（优先 DB 归一化标题，其次用 rank seed）
        display_title, title_source = self._get_display_title(pid, seed_title)

        if title_source == "fallback 标题":
            # 说明 DB 里还没有该书标题，此时使用 Rank Page 作为标题来源更友好
            self.logger.info("[详情抓取] 小说《%s》的书名已从Rank Page获取", display_title)

        if display_title and title_source != "ID":
            self.logger.info("[详情抓取] 开始获取小说详情: 《%s》 (ID: %s)", display_title, pid)
        else:
            self.logger.info("[详情抓取] 开始获取小说详情: %s", novel_url)

        if pid and pid in self.book_cache:
            return self.book_cache[pid]
        if pid and pid in self.book_cache:
            return self.book_cache[pid]

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
            s = self._get_soup(u, wait_css="...", is_scrolling=False)

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
                "publish_date": "",
            }


        detail: Dict[str, Any] = {
            "platform": "qidian",
            "platform_novel_id": pid,
            "url": final_url,

            # rank 作为种子
            "title": seed.get("title", "") or "",
            "author": seed.get("author", "") or "",
            "intro": seed.get("intro", "") or "",
            "main_category": seed.get("main_category", "未知") or "未知",
            "tags": seed.get("tags", []) or [],

            # detail 负责字段
            "status": "",
            "total_words": 0,
            "total_recommend": 0,
            "publish_date": "",
        }

        if self._need_fallback_scalar(detail.get("main_category"), when_unknown=True) or self._need_fallback_tags(
                detail.get("tags")):
            self._fill_detail_title_author_intro(soup, detail)
            self._fill_detail_category_tags(soup, detail)

        self._fill_detail_status_words(soup, detail)
        self._fill_total_recommend(soup, detail, page_url=final_url)
        # publish_date 将在抓取章节时一并从章节页获取，避免在详情抓取阶段重复访问目录/章节页

        # last-resort: avoid empty title
        if not detail.get("title") and pid:
            detail["title"] = f"{pid}"

        if pid:
            self.book_cache[pid] = detail
        return detail

    # ------------------------------------------------------------------
    # Chapter Page -> FIRST_N_CHAPTERS, each w/ content, word count, publish date
    # ------------------------------------------------------------------
    """从Detail Page提取章节链接"""
    def _extract_chapter_links(self, soup: BeautifulSoup, book_id: str) -> List[Tuple[str, str, str, int]]:
        chapter_links = []
        try:
            self.logger.info("[章节获取]开始提取章节链接...")

            # 方法1：使用精确路径
            catalog_all = soup.select_one('div.catalog-all')
            if catalog_all:
                # 找到所有的catalog-volume
                catalog_volumes = catalog_all.select('div.catalog-volume')
                self.logger.info(f'[章节获取]找到 {len(catalog_volumes)} 个分卷')

                # 寻找第一个有效分卷（章节数>=10的正文卷）
                target_volume = None
                for volume_index, volume in enumerate(catalog_volumes):
                    volume_title_elem = volume.select_one('h3.volume-name, h3.volume-tittle')
                    volume_title = volume_title_elem.text.strip() if volume_title_elem else f'分卷{volume_index + 1}'

                    # 跳过"作品相关"卷
                    if '作品相关' in volume_title:
                        self.logger.info(f'[章节获取]跳过作品相关卷: {volume_title}')
                        continue

                    # 查找章节列表
                    chapters_list = volume.select_one('ul.volume-chapters')
                    if not chapters_list:
                        self.logger.warning(f'[章节获取]分卷[{volume_index + 1}] ({volume_title}) 没有找到volume-chapters')
                        continue

                    # 获取所有章节项
                    chapter_items = chapters_list.select('li.chapter-item')
                    chapter_count = len(chapter_items)
                    self.logger.info(f'[章节获取] ({volume_title}) 有 {chapter_count} 个章节')

                    # 判断分卷是否有效：章节数>=10
                    if chapter_count < 10:
                        self.logger.warning(
                            f'[章节获取]分卷 {volume_index + 1} ({volume_title}) 章节数{chapter_count} < 10，跳过此分卷')
                        continue
                    else:
                        target_volume = volume
                        self.logger.info(f'[章节获取]找到有效分卷: {volume_title} (章节数: {chapter_count})')
                        break

                if not target_volume:
                    self.logger.warning('[章节获取]未找到有效分卷（章节数>=10）')
                    return []

                # 查找目标卷的章节列表
                chapters_list = target_volume.select_one('ul.volume-chapters')
                if not chapters_list:
                    self.logger.warning('[章节获取]正文卷没有找到volume-chapters')
                    return []

                # 获取所有章节项
                chapter_items = chapters_list.select('li.chapter-item')
                self.logger.info(f'[章节获取]正文卷有 {len(chapter_items)} 个章节')

                for item_index, item in enumerate(chapter_items):
                    try:
                        # 提取章节链接和标题
                        chapter_link = item.select_one('a')
                        if not chapter_link:
                            continue

                        href = chapter_link.get('href', '')
                        link_text = chapter_link.text.strip()

                        # 提取title属性中的信息
                        title_attr = chapter_link.get('title', '')
                        self.logger.debug(f'[章节获取]章节链接title属性: {title_attr}')

                        # 如果没有href，尝试其他方式
                        if not href:
                            # 尝试从data-chapterid属性获取
                            data_chapterid = item.get('data-chapterid')
                            if data_chapterid:
                                href = f'/chapter/{book_id}/{data_chapterid}/'

                        if href:
                            # 处理相对URL
                            chapter_url = self._to_abs_url(href)

                            # 从title属性中提取时间信息 - 修复模式匹配
                            first_post_time = ""
                            word_count = 0
                            chapter_name = link_text

                            if title_attr:
                                # 尝试不同的时间模式
                                time_patterns = [
                                    r'首发时间[：:]?\s*(\d{4}-\d{2}-\d{2})',
                                    r'更新时间[：:]?\s*(\d{4}-\d{2}-\d{2})',
                                    r'发表时间[：:]?\s*(\d{4}-\d{2}-\d{2})',
                                    r'(\d{4}-\d{2}-\d{2})\s*(?:首发|更新|发表)'
                                ]

                                for pattern in time_patterns:
                                    time_match = re.search(pattern, title_attr)
                                    if time_match:
                                        # 提取日期部分
                                        date_str = time_match.group(1) if time_match.group(1) else time_match.group(0)
                                        # 清理日期字符串
                                        date_str = re.sub(r'[^\d-]', '', date_str)
                                        if len(date_str) >= 10:  # YYYY-MM-DD
                                            first_post_time = date_str[:10]
                                            self.logger.debug(f'[章节获取]从title属性提取到发布时间: {first_post_time}')
                                            break

                                # 提取字数
                                word_patterns = [
                                    r'字数[：:]?\s*(\d+)',
                                    r'(\d+)\s*字'
                                ]

                                for pattern in word_patterns:
                                    word_match = re.search(pattern, title_attr)
                                    if word_match:
                                        try:
                                            word_count = int(word_match.group(1))
                                            self.logger.debug(f'[章节获取]从title属性提取到字数: {word_count}')
                                            break
                                        except ValueError:
                                            continue

                                # 提取章节名
                                chapter_patterns = [
                                    r'章节名[：:]?\s*(.+)',
                                    r'章节标题[：:]?\s*(.+)',
                                    r'标题[：:]?\s*(.+)'
                                ]

                                for pattern in chapter_patterns:
                                    chapter_match = re.search(pattern, title_attr)
                                    if chapter_match:
                                        extracted_name = chapter_match.group(1).strip()
                                        # 清理可能的额外信息
                                        extracted_name = re.sub(r'\d{4}-\d{2}-\d{2}', '', extracted_name)
                                        extracted_name = re.sub(r'\d+字', '', extracted_name)
                                        extracted_name = extracted_name.strip()
                                        if extracted_name and len(extracted_name) > 1:
                                            chapter_name = extracted_name
                                            self.logger.debug(f'[章节获取]从title属性提取到章节名: {chapter_name}')
                                            break

                            # 如果没有从title属性提取到章节名，使用link_text
                            if not chapter_name or chapter_name == link_text:
                                # 清理link_text，移除多余信息
                                chapter_name = link_text
                                # 移除可能的时间、字数信息
                                chapter_name = re.sub(r'\d{4}-\d{2}-\d{2}', '', chapter_name)
                                chapter_name = re.sub(r'\d+字', '', chapter_name)
                                chapter_name = chapter_name.strip()

                            # 确保有章节名
                            if not chapter_name:
                                # 使用默认章节名
                                chapter_name = f'第{item_index + 1}章'

                            # 如果还没有提取到字数，尝试从link_text中提取或使用默认值
                            if word_count == 0:
                                word_match = re.search(r'(\d+)字', link_text)
                                if word_match:
                                    try:
                                        word_count = int(word_match.group(1))
                                    except ValueError:
                                        word_count = 0  # 默认字数
                                else:
                                    # 使用默认字数
                                    word_count = 0

                            chapter_links.append((
                                chapter_name,
                                chapter_url,
                                first_post_time,
                                word_count
                            ))

                            # 如果已经找到足够多的章节，可以提前退出
                            if len(chapter_links) >= 30:  # 多找一些，以防后面有无效链接
                                self.logger.info(f'[章节获取]已找到足够章节，停止在当前分卷搜索')
                                break

                    except Exception as e:
                        self.logger.debug(f'[章节获取]解析章节链接失败: {e}')
                        continue

                    # 如果已经找到足够章节，跳出循环
                    if len(chapter_links) >= 30:
                        break

            # 如果新路径没找到，尝试其他选择器
            if not chapter_links:
                self.logger.warning('新路径未找到章节链接，尝试其他方法')
                # ... 其他方法保持不变 ...

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

        # 记录前3个章节的信息用于调试
        for i, (title, url, date, words) in enumerate(unique_chapters[:3], 1):
            self.logger.info(f'[章节获取]章节{i}: 标题="{title}", 时间={date}, 字数={words}')

        return unique_chapters

    """Parse解析章节正文内容"""
    def _parse_chapter_content(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            # 方法1: 使用具体路径 div.app -> div.reader -> div.reader-content -> div.chapter-wrapper -> .relative -> .print -> .content -> .content-text
            content_text_elements = soup.select(
                'div.app div.reader div.reader-content div.chapter-wrapper .relative .print .content .content-text')

            if content_text_elements:
                self.logger.info(f'[章节正文parse]使用具体路径找到 {len(content_text_elements)} 个 content-text 元素')

                # 提取每个 content-text 的文本
                paragraphs = []
                for elem in content_text_elements:
                    text = elem.get_text(strip=True)
                    if text:  # 只保留非空文本
                        paragraphs.append(text)

                # 将所有段落合并成一整章
                if paragraphs:
                    full_content = '\n'.join(paragraphs)
                    self.logger.debug(f'[章节文本parse]合并后章节内容长度: {len(full_content)} 字符')
                    return full_content

            # 方法2: 尝试简化的路径
            if not content_text_elements:
                content_text_elements = soup.select('.content-text')
                if content_text_elements:
                    self.logger.info(f'[章节文本parse]使用简化路径找到 {len(content_text_elements)} 个 content-text 元素')

                    paragraphs = []
                    for elem in content_text_elements:
                        text = elem.get_text(strip=True)
                        if text:
                            paragraphs.append(text)

                    if paragraphs:
                        full_content = '\n'.join(paragraphs)
                        self.logger.debug(f'[章节文本parse]合并后章节内容长度: {len(full_content)} 字符')
                        return full_content

            # 方法4: 尝试查找所有段落
            if not content_text_elements:
                paragraphs = soup.select('p')
                if paragraphs:
                    self.logger.info(f'[章节文本parse]查找所有段落后找到 {len(paragraphs)} 个段落元素')

                    paragraph_texts = []
                    for p in paragraphs:
                        text = p.get_text(strip=True)
                        if text and len(text) > 3:          # 过滤过短的文本
                            paragraph_texts.append(text)

                    if paragraph_texts:
                        full_content = '\n'.join(paragraph_texts)
                        self.logger.info(f'[章节文本parse]从段落合并内容，长度: {len(full_content)} 字符')
                        return full_content

            self.logger.warning('[章节文本parse]未找到章节正文内容')
            return None

        except Exception as e:
            self.logger.error(f'提取章节内容失败: {e}')
            return None

    """从Chapter Page提取发布时间"""
    def _extract_publish_date_from_chapter(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            # 方法1：查找包含发布时间信息的元素
            # 常见的选择器
            selectors = [
                '.chapter-update', '.update-time', '.publish-time',
                '.chapter-info', '.info', '.chapter-meta',
                '.time', '.date', '.chapter-date'
            ]

            for selector in selectors:
                elements = soup.select(selector)
                for elem in elements:
                    text = self._normalize_text(elem.get_text(strip=True))
                    if text:
                        # 尝试提取日期
                        date_match = re.search(r'(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?', text)
                        if date_match:
                            year = date_match.group(1)
                            month = date_match.group(2).zfill(2)
                            day = date_match.group(3).zfill(2) if date_match.group(3) else "01"
                            return f"{year}-{month}-{day}"

            # 方法2：在页面文本中搜索日期
            page_text = self._normalize_text(soup.get_text(" ", strip=True))

            # 常见的关键词
            keywords = ['首发', '发布', '更新', '发表', '上架']
            for keyword in keywords:
                # 找到关键词的位置
                idx = page_text.find(keyword)
                if idx != -1:
                    # 提取关键词周围的文本
                    start = max(0, idx - 50)
                    end = min(len(page_text), idx + 50)
                    context = page_text[start:end]

                    # 在上下文中搜索日期
                    date_patterns = [
                        r'(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?',
                        r'(\d{4})-(\d{1,2})-(\d{1,2})',
                        r'(\d{4})\.(\d{1,2})\.(\d{1,2})',
                    ]

                    for pattern in date_patterns:
                        date_match = re.search(pattern, context)
                        if date_match:
                            groups = date_match.groups()
                            if len(groups) >= 3:
                                year = groups[0]
                                month = groups[1].zfill(2)
                                day = groups[2].zfill(2) if groups[2] else "01"
                                return f"{year}-{month}-{day}"

            # 方法3：查找所有可能包含日期的元素
            all_elements = soup.find_all(string=re.compile(r'\d{4}[年.-]\d{1,2}[月.-]\d{1,2}'))
            for text in all_elements:
                if isinstance(text, str):
                    date_match = re.search(r'(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?', text)
                    if date_match:
                        year = date_match.group(1)
                        month = date_match.group(2).zfill(2)
                        day = date_match.group(3).zfill(2) if date_match.group(3) else "01"
                        # 检查是否是合理的日期（排除页码等）
                        if int(year) > 2000 and int(year) < 2100:
                            return f"{year}-{month}-{day}"

            return None

        except Exception as e:
            self.logger.error(f"从章节页面提取发布时间失败: {e}")
            return None

    """填入、补完章节的发布时间"""
    def _enrich_publish_date(self, soup: BeautifulSoup, detail: Dict[str, Any]) -> None:
        try:
            self.logger.debug("开始提取上架时间（从章节页面）...")

            # 首先从URL中提取小说ID
            novel_id = (
                detail.get("platform_novel_id", ""))
            if not novel_id:
                self.logger.warning("没有小说ID，无法获取章节")
                detail["publish_date"] = ""
                return

            # 构建目录页面URL
            catalog_url = f'https://book.qidian.com/info/{novel_id}/#Catalog'
            self.logger.info(f'访问目录页以获取第一章: {catalog_url}')

            # 访问目录页面
            catalog_soup = self._get_soup(catalog_url, wait_css="...", is_scrolling=False)

            if not catalog_soup:
                self.logger.warning("无法访问目录页")
                detail["publish_date"] = ""
                return

            # 提取章节链接
            chapter_infos = self._extract_chapter_links(catalog_soup, novel_id)

            if not chapter_infos:
                self.logger.warning("未找到章节链接")
                detail["publish_date"] = ""
                return

            # 获取第一章的URL
            if len(chapter_infos) > 0:
                first_chapter_info = chapter_infos[0]
                chapter_url = first_chapter_info[1]         # 第二个元素是URL

                # 访问第一章页面
                chapter_soup = self._get_soup(chapter_url, wait_css="...", is_scrolling=False)

                if not chapter_soup:
                    self.logger.warning("无法访问第一章页面")
                    detail["publish_date"] = ""
                    return

                # 从章节页面提取发布时间
                publish_date = self._extract_publish_date_from_chapter(chapter_soup)

                if publish_date:
                    detail["publish_date"] = publish_date
                    self.logger.info(f"从第一章提取到上架时间: {publish_date}")
                    return

            # 如果没有找到，尝试使用其他方法
            self.logger.debug("未能从章节页面提取上架时间，尝试其他方法")

            # 方法1：查找包含"首发"、"发布"或"更新"的文本
            chapter_text = ""
            if chapter_soup:
                chapter_text = self._normalize_text(chapter_soup.get_text(" ", strip=True))

            # 在章节文本中搜索日期
            date_patterns = [
                r'首发时间[：:]?\s*(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?',
                r'发布时间[：:]?\s*(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?',
                r'更新时间[：:]?\s*(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?',
                r'(\d{4})年(\d{1,2})月(\d{1,2})日[^\d]',  # 前面不能是数字
                r'(\d{4})-(\d{1,2})-(\d{1,2})\b',
                r'(\d{4})\.(\d{1,2})\.(\d{1,2})\b',
            ]

            for pattern in date_patterns:
                date_match = re.search(pattern, chapter_text)
                if date_match:
                    groups = date_match.groups()
                    if len(groups) >= 3:
                        year = groups[0]
                        month = groups[1].zfill(2)
                        day = groups[2].zfill(2)
                        publish_date = f"{year}-{month}-{day}"
                        detail["publish_date"] = publish_date
                        self.logger.info(f"从章节文本提取到上架时间: {publish_date}")
                        return

            # 方法2：查找时间元素
            time_elements = []
            if chapter_soup:
                time_elements = chapter_soup.select('time, .time, .date, .publish-time')

            for time_elem in time_elements:
                time_text = self._normalize_text(time_elem.get_text(strip=True))

                # 尝试解析日期格式
                date_match = re.search(r'(\d{4})[年.-](\d{1,2})[月.-](\d{1,2})[日]?', time_text)
                if date_match:
                    year = date_match.group(1)
                    month = date_match.group(2).zfill(2)
                    day = date_match.group(3).zfill(2) if date_match.group(3) else "01"
                    publish_date = f"{year}-{month}-{day}"
                    detail["publish_date"] = publish_date
                    self.logger.info(f"从时间元素提取到上架时间: {publish_date}")
                    return

            self.logger.debug("未能提取到上架时间")
            detail["publish_date"] = ""

        except Exception as e:
            self.logger.error(f"[章节获取]提取上架时间失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            detail["publish_date"] = ""

    """获取、清洗单章正文内容"""
    def _fetch_single_chapter(self, chapter_url: str, *, display_title: str = "") -> Optional[str]:
        novel_id = self._extract_novel_id_from_url(chapter_url)

        if not display_title:
            display_title, _ = self._get_display_title(novel_id, "") if novel_id else ("未知", "ID")

        try:
            self.logger.info("[章节抓取] 访问章节页面: 《%s》 - %s", display_title, chapter_url)

            soup = self._get_soup(chapter_url, wait_css="...", is_scrolling=False)

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
            self.logger.error("[章节抓取] 获取《%s》的章节内容失败 %s: %s", display_title, chapter_url, e)
            return None


    """抓取单章正文，并尽可能从同一章节页中解析发布时间"""
    def _fetch_single_chapter_with_meta(self, chapter_url: str, *, display_title: str = "") -> Tuple[Optional[str], str]:
        """
        Returns:
            (chapter_content, publish_date)
        """
        novel_id = self._extract_novel_id_from_url(chapter_url)

        if not display_title:
            display_title, _ = self._get_display_title(novel_id, "") if novel_id else ("未知", "ID")

        try:
            self.logger.info("[章节抓取] 访问章节页面: 《%s》 - %s", display_title, chapter_url)

            soup = self._get_soup(chapter_url, wait_css="...", is_scrolling=False)
            if not soup:
                return None, ""

            # 正文
            chapter_content = self._parse_chapter_content(soup)
            content_text: Optional[str] = None
            if chapter_content:
                content_text = re.sub(r'\s+', ' ', chapter_content)

            # 发布时间（尽量从同一页面解析）
            publish_date = self._extract_publish_date_from_chapter(soup) or ""

            return content_text, publish_date

        except Exception as e:
            self.logger.error("[章节抓取] 获取《%s》的章节内容失败 %s: %s", display_title, chapter_url, e)
            return None, ""

    """从网站抓取小说章节内容"""
    def _fetch_novel_chapters(
            self,
              novel_url: str,
              novel_id: str,
              chapter_count: int,
              start_index: int = 0,
              publish_date: str = ""
    ) -> List[Dict[str, Any]]:
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
            soup = self._get_soup(catalog_url, wait_css="...", is_scrolling=False)

            if not soup:
                return []

            # 提取章节链接
            chapter_infos = self._extract_chapter_links(soup, book_id)

            if not chapter_infos:
                self.logger.warning(f'未找到章节链接: {novel_url}')
                return []

            # 只取前chapter_count章
            chapter_infos = self._slice_chapter_infos_to_fetch(
                chapter_infos,
                existing_count=start_index,
                need_count=chapter_count,
            )
            chapters = []

            for i, (chapter_title, chapter_url, first_post_time, word_count) in enumerate(chapter_infos, 1):
                self.logger.info(f'获取第{i}章: {chapter_title}')

                try:
                    chapter_content, chapter_publish_date = self._fetch_single_chapter_with_meta(chapter_url)

                    if chapter_content:
                        # 优先使用目录页提取的发布时间；否则用章节页解析的；再否则用已知 publish_date 兜底
                        publish_date = first_post_time or chapter_publish_date or publish_date

                        chapter_data = {
                            'chapter_num': i,
                            'chapter_title': chapter_title,
                            'chapter_content': chapter_content,
                            'chapter_url': chapter_url,
                            'publish_date': publish_date,  # 使用正确的发布时间
                            'word_count': word_count,
                        }

                        chapters.append(chapter_data)

                    # 章节间延迟
                    if i < len(chapter_infos):
                        self._humanlike_sleep(2, 4)

                except Exception as e:
                    self.logger.error(f'获取章节失败 {chapter_title}: {e}')
                    continue

            self.logger.info(f'成功获取 {len(chapters)} 章内容')
            return chapters

        except Exception as e:
            self.logger.error(f'获取章节列表失败: {e}')
            return []

    """智能获取小说前N章内容:只抓取缺失的章节"""
    def fetch_first_n_chapters(self, novel_url: str, target_chapter_count: int = 5, *, fallback_title: str = "") -> List[Dict[str, Any]]:
        # 使用辅助方法获取显示用标题
        novel_id = self._extract_novel_id_from_url(novel_url)
        display_title, title_source = self._get_display_title(novel_id, fallback_title)

        if title_source == "原始标题":
            self.logger.info("小说《%s》的书名已从Rank Page获取", display_title)

        self.logger.info("[章节智能补全] 开始智能获取数据库中缺少的章节: 《%s》 (ID: %s), 目标%d章",
                         display_title, novel_id, target_chapter_count)

        # 1. 检查数据库中已有章节数
        existing_count = self._get_existing_chapter_count(novel_id)

        # 2. 如果已有章节数 >= 目标章节数，直接从数据库获取
        if existing_count >= target_chapter_count:
            self.logger.info(f"[章节智能补全] 已有{existing_count}章 ≥ 目标{target_chapter_count}章，直接从数据库获取")
            existing_chapters = self._get_existing_chapters(novel_id, target_chapter_count)

            # 转换为标准格式
            chapters = []
            for i, ch in enumerate(existing_chapters, 1):
                chapters.append({
                    'chapter_num': i,
                    'chapter_title': ch.get('chapter_title', f'第{i}章'),
                    'chapter_content': ch.get('chapter_content', ''),
                    'chapter_url': ch.get('chapter_url', ''),
                    'word_count': ch.get('word_count', 0),
                    'publish_date': ch.get('publish_date', ''),
                })
            return chapters

        # 3. 获取已有章节（如果有）
        existing_chapters = []
        if existing_count > 0:
            existing_chapters = self._get_existing_chapters(novel_id, existing_count)
            self.logger.info(f"[章节智能补全] 已有{existing_count}章，需要补充{target_chapter_count - existing_count}章")

        # 4. publish_date 将在抓取章节时一并从章节页提取（避免和 fetch_novel_detail 重复访问）
        detail = self.fetch_novel_detail(novel_url, novel_id, seed={"title": fallback_title} if fallback_title else None)
        publish_date = ""

        # 5. 计算需要抓取的新章节数
        new_chapter_count = target_chapter_count - existing_count

        # 6. 从网站抓取新章节（跳过已有章节）
        self.logger.info(f"[章节智能补全] 开始抓取{new_chapter_count}个新章节")

        # 构建目录页面URL
        catalog_url = f'https://book.qidian.com/info/{novel_id}/#Catalog'
        self.logger.info(f"[章节智能补全] 访问目录页: {catalog_url}")

        # 访问目录页面
        soup = self._get_soup(catalog_url, wait_css="...", is_scrolling=False)

        if not soup:
            self.logger.warning("[章节智能补全] 无法访问目录页")
            return self._format_existing_chapters(existing_chapters, target_chapter_count)

        # 提取章节链接
        chapter_infos = self._extract_chapter_links(soup, novel_id)

        if not chapter_infos:
            self.logger.warning("[章节智能补全] 未找到章节链接")
            return self._format_existing_chapters(existing_chapters, target_chapter_count)

        # 跳过已有的章节，从第(existing_count+1)章开始抓取
        start_index = existing_count
        chapter_infos_to_fetch = chapter_infos[start_index:start_index + new_chapter_count]

        if existing_count > 0:
            self.logger.info("[章节智能补全] 已有%d章，跳过前%d章，仅抓取第%d章到第%d章",
                             existing_count, existing_count, existing_count + 1, existing_count + new_chapter_count)

        if not chapter_infos_to_fetch:
            self.logger.warning(f"[章节智能补全] 没有更多章节可抓取，已有{existing_count}章")
            return self._format_existing_chapters(existing_chapters, target_chapter_count)

        # 7. 抓取新章节
        new_chapters = []
        for i, (chapter_title, chapter_url, first_post_time, word_count) in enumerate(chapter_infos_to_fetch, 1):
            chapter_num = start_index + i

            self.logger.info("[章节智能补全] 《%s》 - 抓取第%d章: %s", display_title, chapter_num, chapter_title)

            try:
                chapter_content, chapter_publish_date = self._fetch_single_chapter_with_meta(chapter_url, display_title=display_title)

                if chapter_content:
                    # 优先使用目录页提取的发布时间；否则用章节页解析的；再否则用已知 publish_date 兜底
                    publish_date = first_post_time or chapter_publish_date or publish_date

                    chapter_data = {
                        'chapter_num': chapter_num,
                        'chapter_title': chapter_title,
                        'chapter_content': chapter_content,
                        'chapter_url': chapter_url,
                        'publish_date': publish_date,
                        'word_count': word_count,
                    }

                    new_chapters.append(chapter_data)
                    self.logger.info(
                        f"[章节正文获取] 成功获取第{chapter_num}章，字数: {word_count}, 发布时间: {publish_date}")

                # 章节间延迟
                if i < len(chapter_infos_to_fetch):
                    self._humanlike_sleep(2, 4)

            except Exception as e:
                self.logger.error(f"[章节智能补全] 获取章节失败 {chapter_title}: {e}")
                import traceback
                self.logger.error(f"详细错误: {traceback.format_exc()}")
                continue

        self.logger.info(f"[章节智能补全] 成功抓取 {len(new_chapters)} 个新章节")

        # 8. 合并已有章节和新章节
        all_chapters = self._merge_chapters(existing_chapters, new_chapters, target_chapter_count)

        # 9. 保存新章节到数据库 - 关键修复点
        if new_chapters and self.db_handler:
            self.logger.info(f"[章节智能补全] 准备保存 {len(new_chapters)} 个新章节到数据库")

            try:
                # 准备小说基本信息
                novel_data = {
                    'novel_id': novel_id,
                    'title': detail.get('title', ''),
                    'author': detail.get('author', '未知'),
                    'platform': 'qidian',
                    'novel_url': novel_url,
                    'category': detail.get('main_category', ''),
                    'introduction': detail.get('intro', ''),
                    'tags': detail.get('tags', []),
                    'status': detail.get('status', ''),
                    'total_words': detail.get('total_words', 0),
                    'publish_date': publish_date,
                }

                # 尝试直接保存章节
                if hasattr(self.db_handler, 'upsert_first_n_chapters'):
                    self.logger.info("[章节保存] 使用 upsert_first_n_chapters 方法保存章节")

                    # 获取第一个章节的发布时间
                    first_chapter_publish_date = ""
                    if new_chapters:
                        first_chapter_publish_date = new_chapters[0].get('publish_date', publish_date)

                    result = self.db_handler.upsert_first_n_chapters(
                        platform="qidian",
                        platform_novel_id=novel_id,
                        publish_date=first_chapter_publish_date,
                        chapters=new_chapters,
                        novel_fallback_fields={
                            "title": detail.get('title', ''),
                            "author": detail.get('author', ''),
                            "intro": detail.get('intro', ''),
                            "main_category": detail.get('main_category', ''),
                            "status": detail.get('status', ''),
                            "total_words": detail.get('total_words', 0),
                            "url": novel_url,
                            "tags": detail.get('tags', []),
                        },
                    )
                    self.logger.info(f"[章节智能补全] upsert_first_n_chapters 返回结果: {result}")
                else:
                    self.logger.error("[章节智能补全] db_handler 没有可用的章节保存方法")

            except Exception as e:
                self.logger.error(f"[章节智能补全] 保存章节到数据库失败: {e}")
                import traceback
                self.logger.error(f"详细错误: {traceback.format_exc()}")

        return all_chapters

    # ------------------------------------------------------------------
    # Enrichment / persistence
    # ------------------------------------------------------------------
    """补全榜单上小说的metadata并且抓取其前N章"""
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

        out: List[Dict[str, Any]] = []
        processed_count = 0

        self.logger.info(f"[数据补完] 开始处理 {len(items)} 本书籍，最大处理 {max_books} 本")
        self.logger.info(
            f"配置参数: fetch_detail={fetch_detail}, fetch_chapters={fetch_chapters}, chapter_count={chapter_count}")

        for i, book in enumerate(items[:max_books], 1):
            title = book.get('title', '未知')
            novel_id = book.get('platform_novel_id', '')
            display_title, title_source = self._get_display_title(novel_id, book.get('title', '未知'))
            processed_count += 1

            self.logger.info("[%d/%d] 处理书籍: 《%s》 (ID: %s, 标题来源: %s)",
                             i, min(len(items), max_books), display_title, novel_id, title_source)
            enriched = dict(book)

            # 记录 rank 的分类/标签（作为主来源）
            original_main = enriched.get("main_category", "")
            original_tags = enriched.get("tags", []) or []
            self.logger.info(f"[Rank Page获取信息] 《{display_title}》 - 主分类='{original_main}', 标签={original_tags}")

            if fetch_detail:
                self.logger.info("[数据补完] 《%s》 - 开始获取详情信息", display_title)
                # 先fetch rank page的信息
                detail = self.fetch_novel_detail(
                    enriched.get("url", ""),
                    enriched.get("platform_novel_id", ""),
                    seed=enriched,
                )

                # 更新所有字段，包括总推荐数
                update_fields = ["title", "author", "intro", "status", "total_words",
                                 "total_recommend", "publish_date"]

                # -----------------------------
                # 1) Detail 主字段：永远更新
                # -----------------------------
                # total_recommend：你明确只从详情页取（榜单页没有）
                if detail.get("total_recommend") is not None:
                    enriched["total_recommend"] = detail.get("total_recommend")
                    self.logger.debug(f"[数据补完] 《{display_title}》 - 更新总推荐数: {enriched['total_recommend']}")

                # status / total_words / publish_date：由详情页负责
                for k in ["status", "total_words", "publish_date"]:
                    dv = detail.get(k)
                    if dv is not None:
                        enriched[k] = dv
                        self.logger.debug(f"[数据补完] 《{display_title}》 - 更新字段 {k}: {dv}")

                # -----------------------------
                # 2) Rank 主字段：仅在缺失/异常时兜底补全
                # -----------------------------
                # title/author：为空才补
                if self._need_fallback_scalar(enriched.get("title"),
                                              when_empty=True) and not self._need_fallback_scalar(detail.get("title"),
                                                                                                  when_empty=True):
                    enriched["title"] = detail.get("title")
                    self.logger.info(f"[数据补完] 《{display_title}》 - fallback标题: {enriched['title']}")

                if self._need_fallback_scalar(enriched.get("author"),
                                              when_empty=True) and not self._need_fallback_scalar(detail.get("author"),
                                                                                                  when_empty=True):
                    enriched["author"] = detail.get("author")
                    self.logger.info(f"[数据补完] 《{display_title}》 - fallback作者: {enriched['author']}")

                # intro：为空 or 太短才补（按你之前建议的 min_len=15）
                if self._need_fallback_scalar(enriched.get("intro"), when_empty=True,
                                              min_len=15) and not self._need_fallback_scalar(detail.get("intro"),
                                                                                             when_empty=True,
                                                                                             min_len=15):
                    enriched["intro"] = detail.get("intro")
                    self.logger.info(
                        f"[数据补完] 《{display_title}》 - fallback简介(长度不足): {enriched['intro'][:40]}...")

                # -----------------------------
                # 3) 分类/标签：Rank 为主，仅在 Rank 缺失时用 Detail 兜底（不 merge）
                # -----------------------------
                rank_need_cat = self._need_fallback_scalar(enriched.get("main_category"), when_unknown=True)
                rank_need_tags = self._need_fallback_tags(enriched.get("tags"))

                if rank_need_cat:
                    detail_cat = detail.get("main_category")
                    if detail_cat and not self._need_fallback_scalar(detail_cat, when_unknown=True):
                        self.logger.info(
                            f"[数据补完] 《{display_title}》 - fallback主分类: '{original_main}' -> '{detail_cat}'"
                        )
                        enriched["main_category"] = detail_cat
                    else:
                        self.logger.info(f"[数据补完] 《{display_title}》 - 主分类兜底失败，保持Rank值: '{original_main}'")
                else:
                    self.logger.info(f"[数据补完] 《{display_title}》 - 主分类保持Rank: '{original_main}'")

                if rank_need_tags:
                    detail_tags = detail.get("tags", []) or []
                    if not self._need_fallback_tags(detail_tags):
                        self.logger.info(
                            f"[数据补完] 《{display_title}》 - fallback标签: {original_tags} -> {detail_tags}"
                        )
                        enriched["tags"] = detail_tags
                    else:
                        self.logger.info(f"[数据补完] 《{display_title}》 - 标签兜底失败，保持Rank标签: {original_tags}")
                else:
                    # 不 merge：保持 rank 标签
                    enriched["tags"] = original_tags

                # 最终去重（保持顺序）
                enriched["tags"] = self._dedupe_keep_order([t for t in (enriched.get("tags") or []) if t])

                self.logger.info(
                    f"[数据补完] 《{display_title}》 - 详情处理完成: 主分类='{enriched.get('main_category')}', 标签={enriched.get('tags', [])}"
                )

            if fetch_chapters:
                self.logger.info(f"[数据补完] 《{title}》 - 开始获取章节内容 (目标{chapter_count}章)")

                # 使用智能章节补全方法
                chapters = self.fetch_first_n_chapters(
                    enriched.get("url", ""),
                    chapter_count,
                    fallback_title=enriched.get("title", "")
                )

                if chapters:
                    enriched["first_n_chapters"] = chapters
                    existing_chapters = self._get_existing_chapter_count(novel_id)
                    new_chapters = len(chapters) - existing_chapters

                    if new_chapters > 0:
                        self.logger.info(
                            f"[数据补完] 《{title}》 - 获取到 {len(chapters)} 章内容 (其中 {new_chapters} 章为新抓取)")
                    else:
                        self.logger.info(f"[数据补完] 《{title}》 - 已有 {len(chapters)} 章内容，无需抓取新章节")
                else:
                    self.logger.warning(f"[数据补完] 《{title}》 - 未能获取章节内容")

            out.append(enriched)
            self.logger.info(f"[数据补完] 《{title}》 - 处理完成 [{i}/{min(len(items), max_books)}]")

            # 书籍间延迟（最后一本书不延迟）
            if i < min(len(items), max_books):
                delay = random.uniform(2, 4)
                self.logger.debug(f"[数据补完] 等待 {delay:.1f} 秒后处理下一本书...")
                self._humanlike_sleep(2, 4)

        self.logger.info(f"[数据补完] 全部完成！共处理 {processed_count} 本书籍")
        return out

    # ------------------------------------------------------------------
    # BaseSpider API: enrich_books_with_details
    # ------------------------------------------------------------------
    def enrich_books_with_details(self, books, max_books: int = 20):
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

                # 调试：检查章节发布时间
                self.logger.info(f"[章节保存]准备保存小说 {b.get('title')} 的章节")
                max_log_chapters = self.config.get("max_log_chapters", 5)
                for i, chapter in enumerate(chapters[:max_log_chapters], 1):
                    publish_date = chapter.get('publish_date', '')
                    self.logger.info(f"[章节保存] 章节{i}发布时间: {publish_date}")

                if len(chapters) > max_log_chapters:
                    self.logger.info(
                        f"[章节保存] 共 {len(chapters)} 章，仅显示前 {max_log_chapters} 章用于验证"
                    )

                first_chapter_publish_date = ""
                if chapters:
                    first_chapter_publish_date = chapters[0].get('publish_date', snapshot_date or self._today_str())

                self.db_handler.upsert_first_n_chapters(
                    platform="qidian",
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

    """保存新章节到数据库"""
    def _save_chapters(self, novel_id: str, detail: Dict[str, Any],
                       new_chapters: List[Dict[str, Any]]) -> None:
        try:
            display_title, title_source = self._get_display_title(novel_id, detail.get('title', ''))

            self.logger.info("[章节保存] 准备保存《%s》的 %d 个新章节到数据库", display_title, len(new_chapters))

            # 检查数据库handler状态
            self.logger.info("[数据库调试] db_handler状态: %s", "存在" if self.db_handler else "不存在")

            if not self.db_handler:
                self.logger.error("[数据库调试] 数据库handler不存在，无法保存章节")
                return

            if not new_chapters:
                self.logger.warning("[数据库调试] 没有新章节需要保存")
                return

            # 检查数据库handler的方法
            available_methods = [method for method in dir(self.db_handler) if not method.startswith('_')]
            self.logger.info("[数据库调试] db_handler可用方法: %s", ", ".join(available_methods[:10]))

            # 检查是否有保存章节的方法
            if hasattr(self.db_handler, 'upsert_first_n_chapters'):
                self.logger.info("[数据库调试] 找到 upsert_first_n_chapters 方法")

                # 准备小说基本信息
                novel_fallback_fields = {
                    "title": detail.get('title', ''),
                    "author": detail.get('author', '未知'),
                    "intro": detail.get('intro', ''),
                    "main_category": detail.get('main_category', ''),
                    "status": detail.get('status', ''),
                    "total_words": detail.get('total_words', 0),
                    "url": detail.get('url', ''),
                    "tags": detail.get('tags', []),
                }

                self.logger.info("[数据库调试] novel_fallback_fields准备完成")

                # 使用第一个章节的发布时间，如果没有则使用当前日期
                first_chapter_publish_date = ""
                if new_chapters:
                    first_chapter_publish_date = new_chapters[0].get('publish_date', '')
                    self.logger.info("[数据库调试] 第一个章节发布时间: %s", first_chapter_publish_date)

                # 显示将要保存的章节信息
                self.logger.info("[数据库调试] 将要保存的章节信息:")
                for i, chapter in enumerate(new_chapters[:3], 1):
                    self.logger.info("[数据库章节保存]章节%d: 标题='%s', 字数=%d, 发布时间=%s",
                                     i, chapter.get('chapter_title', ''),
                                     chapter.get('word_count', 0),
                                     chapter.get('publish_date', ''))

                try:
                    # 保存章节到数据库
                    self.logger.info("[数据库调试] 调用 upsert_first_n_chapters...")
                    result = self.db_handler.upsert_first_n_chapters(
                        platform="qidian",
                        platform_novel_id=novel_id,
                        publish_date=first_chapter_publish_date,
                        chapters=new_chapters,
                        novel_fallback_fields=novel_fallback_fields,
                    )
                    self.logger.info("[数据库调试] upsert_first_n_chapters 返回结果: %s", result)

                    if result:
                        self.logger.info("[章节保存] 成功保存 %d 个章节到数据库", len(new_chapters))
                    else:
                        self.logger.error("[章节保存] 保存章节到数据库失败，返回结果为: %s", result)

                except Exception as e:
                    self.logger.error("[数据库调试] 调用 upsert_first_n_chapters 失败: %s", e)
                    import traceback
                    self.logger.error("[数据库调试] 详细错误堆栈: %s", traceback.format_exc())
            else:
                self.logger.error("[数据库调试] db_handler没有upsert_first_n_chapters方法")

                # 检查是否有其他保存方法
                if hasattr(self.db_handler, 'save_novel'):
                    self.logger.info("[数据库调试] 找到 save_novel 方法，尝试使用它")
                    try:
                        # 准备小说数据
                        novel_data = {
                            'novel_id': novel_id,
                            'title': detail.get('title', ''),
                            'author': detail.get('author', '未知'),
                            'platform': 'qidian',
                            'novel_url': detail.get('url', ''),
                            'category': detail.get('main_category', ''),
                            'introduction': detail.get('intro', ''),
                            'tags': detail.get('tags', []),
                            'status': detail.get('status', ''),
                            'total_words': detail.get('total_words', 0),
                            'publish_date': detail.get('publish_date', ''),
                        }

                        result = self.db_handler.save_novel(novel_data, new_chapters)
                        self.logger.info("[数据库调试] save_novel 返回结果: %s", result)
                    except Exception as e:
                        self.logger.error("[数据库调试] 调用 save_novel 失败: %s", e)

            self.logger.info("[章节保存] 章节保存流程完成")

        except Exception as e:
            self.logger.error("[章节保存] 保存新章节到数据库失败: %s", e)
            import traceback
            self.logger.error("[数据库调试] 完整错误堆栈: %s", traceback.format_exc())

    # ------------------------------------------------------------------
    # BaseSpider API: fetch_whole_ranks, 一键启动
    # ------------------------------------------------------------------
    """Fetch all configured rank lists and return a flattened list of items"""
    def fetch_whole_rank(self):
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