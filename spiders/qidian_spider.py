# spiders/qidian_spider.py
from selenium.common import TimeoutException
import config
import time
import re
import random
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from .base_spider import BaseSpider
GLOBAL_SELENIUM_CONFIG = getattr(config, 'SELENIUM_CONFIG', {})

"""使用Selenium处理起点反爬的爬虫"""
class QidianSpider(BaseSpider):
    def __init__(self, site_config, db_handler=None):
        super().__init__(site_config)
        # Selenium
        self.driver = None
        self.book_cache = {}
        self.retry_count = 0
        self.max_retries = site_config.get('max_retries', 3)
        self.selenium_config = self._get_selenium_config()
        self._init_selenium()
        # DB
        self.db_handler = db_handler
        # Spider
        self.default_chapter_count = site_config.get('chapter_extraction_goal', 5)

    """获取Selenium配置（合并全局和站点特定配置）"""
    def _get_selenium_config(self):
        # 基础配置
        config = {
            'enabled': True,
            'browser': 'chrome',
            'options': {
                'headless': True,
                'no_sandbox': True,
                'disable_dev_shm_usage': True,
                'disable_gpu': True,
                'window_size': '1920,1080',
                'disable_blink_features': 'AutomationControlled',
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            },
            'experimental_options': {
                'excludeSwitches': ['enable-automation'],
                'useAutomationExtension': False,
            },
            'timeout': 15,
            'implicit_wait': 10,
            'page_load_timeout': 30,
            'script_timeout': 10,
            'stealth_mode': True,
            'save_screenshots': False,
            'screenshot_on_error': True,
            'driver_path': None,
        }

        # 用全局配置更新
        config.update(GLOBAL_SELENIUM_CONFIG)

        # 用站点特定配置更新
        site_specific = self.site_config.get('selenium_specific', {})
        if site_specific:
            for key, value in site_specific.items():
                if key in config and isinstance(config[key], dict) and isinstance(value, dict):
                    config[key].update(value)
                else:
                    config[key] = value

        return config

    """初始化Selenium WebDriver"""
    def _init_selenium(self):
        try:
            options = Options()

            # 应用配置选项
            config_options = self.selenium_config.get('options', {})

            # 无头模式
            if config_options.get('headless', True):
                options.add_argument('--headless=new')

            # 其他参数
            for key, value in config_options.items():
                if key == 'headless' or key == 'window_size':
                    continue
                elif isinstance(value, bool) and value:
                    options.add_argument(f'--{key.replace("_", "-")}')
                elif isinstance(value, str) and key != 'user_agent':
                    options.add_argument(f'--{key.replace("_", "-")}={value}')

            # 用户代理
            if 'user_agent' in config_options:
                options.add_argument(f'user-agent={config_options["user_agent"]}')

            # 实验性选项
            experimental_options = self.selenium_config.get('experimental_options', {})
            for key, value in experimental_options.items():
                options.add_experimental_option(key, value)

            # 性能优化：禁用图片和CSS加载
            prefs = {
                'profile.default_content_setting_values': {
                    'images': 2,  # 不加载图片
                    'javascript': 1,  # 启用JavaScript（必须）
                    'stylesheet': 2,  # 不加载CSS
                }
            }
            options.add_experimental_option('prefs', prefs)

            # 隐藏自动化特征
            if self.selenium_config.get('stealth_mode', True):
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)

            # 创建驱动
            driver_path = self.selenium_config.get('driver_path')
            if driver_path and os.path.exists(driver_path):
                service = Service(driver_path)
            else:
                service = Service(ChromeDriverManager().install())

            self.driver = webdriver.Chrome(service=service, options=options)

            # 设置超时
            timeout = self.selenium_config.get('timeout', 15)
            implicit_wait = self.selenium_config.get('implicit_wait', 10)
            page_load_timeout = self.selenium_config.get('page_load_timeout', 30)

            self.driver.set_page_load_timeout(page_load_timeout)
            self.driver.implicitly_wait(implicit_wait)

            # 隐藏自动化特征
            if self.selenium_config.get('stealth_mode', True):
                self.driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)

            self.logger.info("Selenium WebDriver初始化成功")
            return True

        except Exception as e:
            self.logger.error(f"Selenium初始化失败: {e}")
            if self.retry_count < self.max_retries:
                self.retry_count += 1
                self.logger.info(f"重试初始化Selenium ({self.retry_count}/{self.max_retries})")
                time.sleep(2)
                return self._init_selenium()
            else:
                self.logger.error("达到最大重试次数，Selenium初始化失败")
                return False

    """
    获取元数据（作者，书名，简介）
    """

    """从URL中提取小说ID"""
    def _extract_novel_id_from_url(self, url):
        """从小说URL中提取数字ID

        Args:
            url: 小说URL，如 https://www.qidian.com/book/1035420986/

        Returns:
            str: 小说ID，如 '1035420986'
        """
        try:
            # 匹配URL中的数字ID
            # 支持多种格式：/book/1035420986/ 或 /info/1035420986/
            patterns = [
                r'/book/(\d+)/',
                r'/info/(\d+)/',
                r'book\.qidian\.com/info/(\d+)',
                r'www\.qidian\.com/book/(\d+)'
            ]

            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    novel_id = match.group(1)
                    self.logger.info(f'从URL提取到小说ID: {novel_id} (URL: {url})')  # 添加详细日志
                    return novel_id

            # 如果没有匹配到，尝试其他可能的模式
            if 'book.qidian.com' in url:
                # 尝试从路径中提取
                path_parts = url.split('/')
                for part in path_parts:
                    if part.isdigit() and len(part) >= 6:  # 起点小说ID通常至少6位
                        self.logger.info(f'从URL路径提取到小说ID: {part} (URL: {url})')  # 添加详细日志
                        return part

            self.logger.warning(f'无法从URL提取小说ID: {url}')  # 添加警告日志
            return ''

        except Exception as e:
            self.logger.error(f'从URL提取小说ID失败: {e} (URL: {url})')
            return ''

    """提取‘大分类·副分类’"""
    def _extract_category(self, item):
        try:
            # 1. 首先定位到包含作者和分类的核心区域
            author_p = item.select_one('p.author')
            if not author_p:
                return '未知分类'

            # 2. 定位所有作为分类的 <a> 标签
            # 规则：href属性包含特定分类路径，且不是作者链接（作者链接有 class='name'）
            category_links = []
            for link in author_p.find_all('a'):
                # 排除作者链接
                if 'name' in link.get('class', []):
                    continue
                # 提取可能是分类的链接
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                # 关键过滤：只保留可能是分类的文本
                if link_text and len(link_text) < 8 and '更新' not in link_text and '章' not in link_text:
                    category_links.append(link_text)

            # 3. 组装分类信息
            if len(category_links) >= 2:
                # 通常第一个是大分类，第二个是副分类
                return f'{category_links[0]}·{category_links[1]}'
            elif len(category_links) == 1:
                # 只有大分类
                return f'{category_links[0]}'
            else:
                # 没有找到分类链接，尝试备用方案
                main_category = author_p.select_one('a[href*="qidian.com/"]:not(.name)')
                sub_category = author_p.select_one('a.go-sub-type')

                if main_category and sub_category:
                    return f'{main_category.get_text(strip=True)}·{sub_category.get_text(strip=True)}'
                elif main_category:
                    return main_category.get_text(strip=True)
                else:
                    return '未知分类'

        except Exception as e:
            self.logger.debug(f'提取分类失败: {e}')
            return '未知分类'

    """解析单个书籍项目（提取分类）"""
    def _parse_book_item(self, item, index, rank_type, page):
        try:
            # 获取书籍ID
            book_id = item.get('data-bid') or item.get('data-rid')
            if not book_id:
                # 尝试从链接中提取
                link_elem = item.select_one('a[href*="/book/"]')
                if link_elem:
                    href = link_elem['href']
                    match = re.search(r'/book/(\d+)', href)
                    if match:
                        book_id = match.group(1)

            # 获取标题和链接
            title_elem = item.select_one('h2 a')
            link_elem = item.select_one('a[href*="/book/"]')

            if not title_elem or not link_elem:
                return None

            # 获取完整的URL
            full_url = urljoin(self.base_url, link_elem['href'])

            # 从URL中提取小说ID - 直接使用数字ID
            novel_id = self._extract_novel_id_from_url(full_url)

            if not novel_id:
                # 如果提取失败，尝试使用book_id
                novel_id = book_id if book_id else ''

            if not novel_id:
                return None

            # 计算全局排名
            global_rank = (page - 1) * 20 + index

            book_info = {
                'novel_id': novel_id,  # 修改这里：直接使用数字ID，不加前缀
                'platform': 'qidian',
                'title': title_elem.text.strip(),
                'rank': global_rank,
                'rank_type': rank_type,
                'url': full_url,
                'fetch_date': time.strftime('%Y-%m-%d'),
                'fetch_time': time.strftime('%H:%M:%S'),
                'page': page,
            }

            # 提取作者
            author_elem = item.select_one('a.author, .author a.name, .author-name')
            if author_elem:
                book_info['author'] = author_elem.text.strip()

            # 提取分类
            category = self._extract_category(item)
            if category:
                book_info['category'] = category

            # 提取简介
            intro_elem = item.select_one('.intro, .book-intro')
            if intro_elem:
                book_info['brief_intro'] = intro_elem.text.strip()[:100]

            # 提取标签
            tags = []
            tag_elements = item.select('.tag span, .tags a')
            for tag in tag_elements:
                tag_text = tag.text.strip()
                if tag_text and len(tag_text) < 10:
                    tags.append(tag_text)

            if tags:
                book_info['tags'] = tags

            return book_info

        except Exception as e:
            self.logger.debug(f'解析书籍失败: {e}')
            return None

    """解析起点页面的规则"""
    def _parse_qidian_page(self, soup, rank_type, page):
        books = []

        # 尝试多种选择器
        for selector in ['.book-img-text li', '.rank-view-list li', 'li[data-rid]', 'div[data-bid]']:
            items = soup.select(selector)
            if items and len(items) > 5:
                self.logger.info(f'找到 {len(items)} 个项目')

                for index, item in enumerate(items, 1):
                    try:
                        book_info = self._parse_book_item(item, index, rank_type, page)
                        if book_info:
                            books.append(book_info)
                    except Exception as e:
                        self.logger.debug(f'解析书籍失败: {e}')
                        continue

                if books:
                    break

        return books

    """使用Selenium抓取网页"""
    def _fetch_single_page_with_selenium(self, url, rank_type, page):
        try:
            # 访问页面
            self.driver.get(url)

            # 等待页面加载
            time.sleep(random.uniform(2, 4))

            # 等待内容加载
            try:
                # 等待书籍元素出现
                wait = WebDriverWait(self.driver, 10)
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "[data-bid], .book-img-text li, .rank-view-list li"))
                )
            except:
                self.logger.warning("等待超时")

            # 获取页面源码
            html = self.driver.page_source

            # 使用BeautifulSoup解析
            soup = BeautifulSoup(html, 'html.parser')

            # 分析页面结构并提取数据
            books = self._parse_qidian_page(soup, rank_type, page)
            return books

        except Exception as e:
            self.logger.error(f"Selenium抓取页面失败 {url}: {e}")
            return []

    """使用Selenium从网页中抓取排行榜"""
    def fetch_rank_list(self, rank_type='hotsales'):
        self.logger.info(f'开始抓取{self.name} {rank_type}榜...')

        books = []
        pages = self.site_config.get('pages_per_rank', 5)

        for page in range(1, pages + 1):
            url_template = self.site_config['rank_urls'].get(rank_type)
            url = url_template.format(page=page)
            self.logger.info(f'正在抓取{rank_type}榜 第{page}页')

            page_books = self._fetch_single_page_with_selenium(url, rank_type, page)
            books.extend(page_books)

            # 页间随机延迟，模拟人类行为
            time.sleep(random.uniform(3, 6))

        self.logger.info(f'共抓取 {len(books)} 本小说')
        return books

    """从详情页提取分类"""
    def _extract_category_from_detail(self, soup):

        try:
            # 方法1：从面包屑导航提取
            breadcrumb = soup.select_one('.crumb, .breadcrumb, .site-nav')
            if breadcrumb:
                breadcrumb_text = breadcrumb.get_text()
                if '·' in breadcrumb_text:
                    parts = breadcrumb_text.split('·')
                    if len(parts) >= 2:
                        return parts[-2].strip() + '·' + parts[-1].strip()

            # 方法2：从分类链接提取
            category_links = soup.select('a[href*="chanId"], a[href*="category"]')
            for link in category_links:
                link_text = link.text.strip()
                if '·' in link_text or link_text in ['都市', '玄幻', '奇幻', '仙侠', '武侠']:
                    return link_text

            # 方法3：从meta信息提取
            meta_category = soup.select_one('meta[name="keywords"], meta[name="category"]')
            if meta_category and meta_category.get('content'):
                content = meta_category['content']
                for keyword in ['都市', '玄幻', '奇幻', '仙侠', '武侠', '科幻']:
                    if keyword in content:
                        return keyword

            return '未知'

        except Exception as e:
            self.logger.debug(f'从详情页提取分类失败: {e}')
            return '未知'

    """提取元数据详情信息"""
    def _extract_metadata(self, soup, detail, novel_id):
        try:
            # 提取标题 - 使用更精确的选择器
            title_elem = soup.select_one('h1.book-title, h1.works-title, .book-info h1, meta[property="og:title"]')

            if title_elem:
                if title_elem.name == 'meta':
                    # 从meta标签提取
                    title = title_elem.get('content', '').strip()
                    # 清理标题（可能包含" - 起点中文网"等后缀）
                    if ' - ' in title:
                        title = title.split(' - ')[0]
                    detail['title'] = title
                else:
                    # 从普通元素提取
                    detail['title'] = title_elem.text.strip()
            else:
                # 备用方案
                for selector in ['h1', 'h2', '.title', '.book-title']:
                    elem = soup.select_one(selector)
                    if elem and elem.text.strip():
                        detail['title'] = elem.text.strip()
                        break

            # 提取作者 - 使用更精确的选择器
            author_elem = soup.select_one('a.writer, .author-name, .writer a, meta[property="og:novel:author"]')

            if author_elem:
                if author_elem.name == 'meta':
                    # 从meta标签提取
                    detail['author'] = author_elem.get('content', '').strip()
                else:
                    # 从普通元素提取
                    detail['author'] = author_elem.text.strip()
            else:
                # 备用方案
                for selector in ['.author', '.writer', 'a[href*="author"]']:
                    elem = soup.select_one(selector)
                    if elem and elem.text.strip():
                        detail['author'] = elem.text.strip()
                        break

            # 提取分类（从详情页）
            category = self._extract_category_from_detail(soup)
            if category and category != '未知':
                detail['category'] = category

            # 提取标签
            tags = []
            for selector in ['.tag-wrap a', '.tags a', '.book-tag a']:
                tag_elements = soup.select(selector)
                for tag in tag_elements:
                    tag_text = tag.text.strip()
                    if tag_text and tag_text not in tags:
                        tags.append(tag_text)

            if tags:
                detail['tags'] = tags

            # 提取简介 - 使用更精确的选择器
            intro_elem = soup.select_one('.book-intro, .intro, .description, meta[property="og:description"]')

            if intro_elem:
                if intro_elem.name == 'meta':
                    # 从meta标签提取
                    detail['introduction'] = intro_elem.get('content', '').strip()
                else:
                    # 从普通元素提取
                    detail['introduction'] = intro_elem.text.strip()
            else:
                # 备用方案
                for selector in ['.intro', '.summary', '.content']:
                    elem = soup.select_one(selector)
                    if elem and elem.text.strip():
                        detail['introduction'] = elem.text.strip()
                        break

            # 调试日志
            self.logger.debug(
                f"提取元数据结果: 标题={detail.get('title', '无')}, 作者={detail.get('author', '无')}, 分类={detail.get('category', '无')}")

        except Exception as e:
            self.logger.error(f'提取元数据失败: {e}')

    """提取详情信息"""
    def _extract_detail_info(self, soup, detail, novel_id):
        try:
            # 提取标题 - 使用更精确的选择器
            title_elem = soup.select_one('h1.book-title, h1.works-title, .book-info h1, meta[property="og:title"]')

            if title_elem:
                if title_elem.name == 'meta':
                    # 从meta标签提取
                    title = title_elem.get('content', '').strip()
                    # 清理标题（可能包含" - 起点中文网"等后缀）
                    if ' - ' in title:
                        title = title.split(' - ')[0]
                    detail['title'] = title
                else:
                    # 从普通元素提取
                    detail['title'] = title_elem.text.strip()
            else:
                # 备用方案
                for selector in ['h1', 'h2', '.title', '.book-title']:
                    elem = soup.select_one(selector)
                    if elem and elem.text.strip():
                        detail['title'] = elem.text.strip()
                        break

            # 提取作者 - 使用更精确的选择器
            author_elem = soup.select_one('a.writer, .author-name, .writer a, meta[property="og:novel:author"]')

            if author_elem:
                if author_elem.name == 'meta':
                    # 从meta标签提取
                    detail['author'] = author_elem.get('content', '').strip()
                else:
                    # 从普通元素提取
                    detail['author'] = author_elem.text.strip()
            else:
                # 备用方案
                for selector in ['.author', '.writer', 'a[href*="author"]']:
                    elem = soup.select_one(selector)
                    if elem and elem.text.strip():
                        detail['author'] = elem.text.strip()
                        break

            # 提取分类（从详情页）
            category = self._extract_category_from_detail(soup)
            if category and category != '未知':
                detail['category'] = category

            # 提取标签
            tags = []
            for selector in ['.tag-wrap a', '.tags a', '.book-tag a']:
                tag_elements = soup.select(selector)
                for tag in tag_elements:
                    tag_text = tag.text.strip()
                    if tag_text and tag_text not in tags:
                        tags.append(tag_text)

            if tags:
                detail['tags'] = tags

            # 提取简介 - 使用更精确的选择器
            intro_elem = soup.select_one('.book-intro, .intro, .description, meta[property="og:description"]')

            if intro_elem:
                if intro_elem.name == 'meta':
                    # 从meta标签提取
                    detail['introduction'] = intro_elem.get('content', '').strip()
                else:
                    # 从普通元素提取
                    detail['introduction'] = intro_elem.text.strip()
            else:
                # 备用方案
                for selector in ['.intro', '.summary', '.content']:
                    elem = soup.select_one(selector)
                    if elem and elem.text.strip():
                        detail['introduction'] = elem.text.strip()
                        break

            # 调试日志
            self.logger.debug(
                f"提取详情结果: 标题={detail.get('title', '无')}, 作者={detail.get('author', '无')}, 分类={detail.get('category', '无')}")

        except Exception as e:
            self.logger.error(f'提取详情信息失败: {e}')

    """抓取小说详情（仅元数据，不包含章节内容）"""
    def fetch_novel_detail(self, novel_url, novel_id=''):
        """抓取小说详情元数据（标题、作者、分类、简介等）

        Args:
            novel_url: 小说详情页URL
            novel_id: 小说ID（可选）

        Returns:
            dict: 包含小说元数据的字典
        """
        # 从URL中提取小说ID
        url_novel_id = self._extract_novel_id_from_url(novel_url)

        # 优先使用传入的novel_id，如果没有则使用从URL提取的
        final_novel_id = novel_id if novel_id else url_novel_id

        # 如果缓存中已有，直接返回
        if final_novel_id and final_novel_id in self.book_cache:
            return self.book_cache[final_novel_id]

        self.logger.info(f'抓取详情元数据: {novel_url}')

        try:
            # 使用Selenium访问详情页
            self.driver.get(novel_url)
            time.sleep(random.uniform(2, 4))

            # 获取页面源码
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            detail = {
                'novel_url': novel_url,
                'novel_id': novel_id,
                'platform': 'qidian'
            }

            # 提取元数据
            self._extract_metadata(soup, detail, novel_id)

            # 缓存结果
            if novel_id:
                self.book_cache[novel_id] = detail

            return detail

        except Exception as e:
            self.logger.error(f'抓取详情元数据失败 {novel_url}: {e}')
            return {'novel_url': novel_url, 'novel_id': novel_id}

    """
    获取章节数据
    """
    """从目录页面提取章节链接"""
    def _extract_chapter_links(self, soup, book_id):
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
                            if href.startswith('//'):
                                chapter_url = 'https:' + href
                            elif href.startswith('/'):
                                chapter_url = urljoin(self.base_url, href)
                            elif href.startswith('http'):
                                chapter_url = href
                            else:
                                # 尝试构建URL
                                match = re.search(r'/chapter/(\d+)/(\d+)', href)
                                if match:
                                    chapter_id = match.group(2)
                                    chapter_url = f'https://www.qidian.com/chapter/{book_id}/{chapter_id}/'
                                else:
                                    continue

                            # 提取章节信息
                            chapter_info = {
                                'href': href,
                                'url': chapter_url,
                                'link_text': link_text,
                                'title_attr': title_attr
                            }

                            # 从title属性中提取首发时间、字数和章节名
                            if title_attr:
                                # title_attr格式可能为："首发时间：2023-01-01 字数：3000 章节名：第一章"
                                import re
                                time_match = re.search(r'首发时间[：:]?\s*(\d{4}-\d{2}-\d{2})', title_attr)
                                word_match = re.search(r'字数[：:]?\s*(\d+)', title_attr)
                                chapter_match = re.search(r'章节名[：:]?\s*(.+)', title_attr)

                                if time_match:
                                    chapter_info['first_post_time'] = time_match.group(1)
                                if word_match:
                                    chapter_info['word_count'] = int(word_match.group(1))
                                if chapter_match:
                                    chapter_info['chapter_name'] = chapter_match.group(1)

                            # 如果没有从title属性提取到章节名，使用link_text
                            if 'chapter_name' not in chapter_info and link_text:
                                # 清理link_text，移除多余信息
                                chapter_name = link_text
                                # 移除可能的时间、字数信息
                                chapter_name = re.sub(r'\d{4}-\d{2}-\d{2}\s*', '', chapter_name)
                                chapter_name = re.sub(r'\d+字\s*', '', chapter_name)
                                chapter_info['chapter_name'] = chapter_name.strip()

                            # 确保有章节名
                            if 'chapter_name' not in chapter_info or not chapter_info['chapter_name']:
                                # 使用默认章节名
                                chapter_info['chapter_name'] = f'第{item_index + 1}章'

                            chapter_links.append(chapter_info)

                            # 如果已经找到足够多的章节，可以提前退出
                            if len(chapter_links) >= 20:  # 多找一些，以防后面有无效链接
                                self.logger.info(f'已找到足够章节，停止在当前分卷搜索')
                                break

                    except Exception as e:
                        self.logger.debug(f'解析章节链接失败: {e}')
                        continue

                    # 如果已经找到足够章节，跳出循环
                    if len(chapter_links) >= 25:
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
                                if href.startswith('//'):
                                    chapter_url = 'https:' + href
                                elif href.startswith('/'):
                                    chapter_url = urljoin(self.base_url, href)
                                elif href.startswith('http'):
                                    chapter_url = href
                                else:
                                    continue

                                chapter_info = {
                                    'href': href,
                                    'url': chapter_url,
                                    'link_text': link_text,
                                    'title_attr': link.get('title', '')
                                }

                                # 尝试从href中提取章节信息
                                match = re.search(r'/chapter/\d+/(\d+)/', href)
                                if match:
                                    chapter_info['chapter_id'] = match.group(1)

                                # 如果没有章节名，使用link_text
                                if link_text:
                                    chapter_info['chapter_name'] = link_text.strip()
                                else:
                                    chapter_info['chapter_name'] = f'章节{len(chapter_links) + 1}'

                                chapter_links.append(chapter_info)

                        except Exception as e:
                            self.logger.debug(f'解析备用章节链接失败: {e}')
                            continue

        except Exception as e:
            self.logger.error(f'提取章节链接时发生错误: {e}')

        # 去重
        unique_chapters = []
        seen_urls = set()

        for chapter in chapter_links:
            url = chapter.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_chapters.append(chapter)

        self.logger.info(f'最终提取到 {len(unique_chapters)} 个唯一章节链接')

        # 返回格式化的章节信息
        result = []
        for i, chapter in enumerate(unique_chapters, 1):
            result.append((
                chapter.get('chapter_name', f'第{i}章'),
                chapter['url'],
                chapter.get('first_post_time', ''),
                chapter.get('word_count', 0)
            ))

        return result

    """提取章节正文内容"""
    def _parse_chapter_content(self, soup):
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
                        if text and len(text) >3:  # 过滤过短的文本
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

    """获取单章内容"""
    def _fetch_single_chapter(self, chapter_url):
        try:
            # 访问章节页面
            self.logger.info(f'访问章节页面: {chapter_url}')
            self.driver.get(chapter_url)

            # 等待页面加载
            wait_time = random.uniform(3, 5)
            self.logger.debug(f'等待{wait_time:.1f}秒让页面加载')
            time.sleep(wait_time)

            # 等待正文内容加载
            try:
                wait = WebDriverWait(self.driver, 15)
                # 使用新的选择器路径等待正文出现
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.reader-content .content-text, div.chapter-wrapper .content-text"))
                )
                self.logger.debug('正文内容已加载')
            except TimeoutException:
                self.logger.warning(f'章节正文加载超时: {chapter_url}')

                # 尝试其他可能的选择器
                try:
                    wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, ".read-content, .chapter-entity, .chapter-text"))
                    )
                    self.logger.debug('使用备选选择器找到正文内容')
                except TimeoutException:
                    self.logger.warning('备选选择器也超时，继续尝试解析现有内容')

            # 获取章节页面源码
            chapter_html = self.driver.page_source
            chapter_soup = BeautifulSoup(chapter_html, 'html.parser')

            # 保存页面源码用于调试
            debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs', 'debug')
            os.makedirs(debug_dir, exist_ok=True)
            debug_file = os.path.join(debug_dir, f'chapter_{int(time.time())}.html')
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(chapter_html)
            self.logger.debug(f'章节页面已保存到: {debug_file}')

            # 提取章节内容（使用新的路径）
            chapter_content = self._parse_chapter_content(chapter_soup)

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

    """从目录页提取小说标题"""

    def _extract_novel_title_from_catalog(self, soup, book_id):
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

    """从网站抓取小说章节内容"""
    def _fetch_novel_chapters_from_website(self, novel_url, novel_id, chapter_count):
        try:
            # 从小说URL提取book_id
            book_id_match = re.search(r'/book/(\d+)', novel_url)
            if not book_id_match:
                self.logger.warning(f'无法从URL提取book_id: {novel_url}')
                return []

            book_id = book_id_match.group(1)

            # 构建目录页面URL
            catalog_url = f'https://book.qidian.com/info/{book_id}/#Catalog'
            self.logger.info(f'访问目录页: {catalog_url}')

            # 访问目录页面
            self.driver.get(catalog_url)
            time.sleep(random.uniform(3, 5))

            # 等待目录加载完成
            try:
                wait = WebDriverWait(self.driver, 15)
                # 等待目录区域出现
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.catalog-all, div.catalog-volume, ul.volume-chapters"))
                )
                time.sleep(2)

                # 检查是否有章节项
                chapter_elements = self.driver.find_elements(By.CSS_SELECTOR, "li.chapter-item, a[href*='/chapter/']")
                self.logger.info(f'找到 {len(chapter_elements)} 个章节元素')

            except TimeoutException:
                self.logger.warning("目录加载超时，尝试继续解析")

            # 获取目录页面源码
            catalog_html = self.driver.page_source

            # 保存页面源码用于调试
            debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'outputs', 'debug')
            os.makedirs(debug_dir, exist_ok=True)
            debug_file = os.path.join(debug_dir, f'catalog_{book_id}_{int(time.time())}.html')
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(catalog_html)
            self.logger.debug(f'目录页面已保存到: {debug_file}')

            catalog_soup = BeautifulSoup(catalog_html, 'html.parser')

            # 从目录页提取书籍标题
            novel_title = self._extract_novel_title_from_catalog(catalog_soup, book_id)

            # 提取章节链接
            chapter_infos = self._extract_chapter_links(catalog_soup, book_id)

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
                            'first_post_time': first_post_time,
                            'word_count': word_count,
                            'novel_title': novel_title
                        }

                        chapters.append(chapter_data)

                    # 章节间延迟
                    if i < len(chapter_infos):
                        delay = random.uniform(2, 4)
                        self.logger.debug(f'等待{delay:.1f}秒后获取下一章')
                        time.sleep(delay)

                except Exception as e:
                    self.logger.error(f'获取章节失败 {chapter_title}: {e}')
                    continue

            self.logger.info(f'成功获取 {len(chapters)} 章内容')
            return chapters

        except Exception as e:
            self.logger.error(f'获取章节列表失败: {e}')
            return []

    """先检查数据库是否已有，然后获取小说前N章内容"""
    def fetch_novel_chapters(self, novel_url, novel_id='', chapter_count=None):
        """获取小说前N章内容

        Args:
            novel_url: 小说详情页URL
            novel_id: 小说ID（可选）
            chapter_count: 要获取的章节数，默认为配置中的default_chapter_count

        Returns:
            list: 章节内容列表，每个元素包含章节信息
        """
        # 如果没有指定章节数，使用配置中的默认值
        if chapter_count is None:
            chapter_count = self.default_chapter_count

        # 从URL中提取小说ID（如果未提供）
        if not novel_id:
            novel_id = self._extract_novel_id_from_url(novel_url)

        self.logger.info(f'获取小说章节内容: {novel_url} (小说ID: {novel_id}, 章节数: {chapter_count})')

        try:
            # 先获取书籍详情（用于获取标题）
            detail = self.fetch_novel_detail(novel_url, novel_id)

            # 从详情中提取书名
            novel_title = detail.get('title', '') if detail else ''

            # 检查数据库是否已有章节
            should_extract = True
            chapters = []

            if self.db_handler:
                # 方法1：尝试使用 check_novel_exists_by_url 方法（如果存在）
                try:
                    if hasattr(self.db_handler, 'check_novel_exists_by_url'):
                        novel_check = self.db_handler.check_novel_exists_by_url(novel_url)
                        if novel_check['exists'] and novel_check['has_chapters']:
                            if novel_check['chapters_count'] >= chapter_count:
                                self.logger.info(f'数据库已有{novel_check["chapters_count"]}章，直接加载')
                                # 从数据库加载章节
                                db_chapters = self.db_handler.get_novel_chapters(novel_check['novel_id'], chapter_count)
                                if db_chapters:
                                    for db_chapter in db_chapters:
                                        chapters.append({
                                            'chapter_num': db_chapter['chapter_num'],
                                            'chapter_title': db_chapter['chapter_title'],
                                            'chapter_content': db_chapter['chapter_content'],
                                            'chapter_url': db_chapter['chapter_url']
                                        })
                                    return chapters
                                else:
                                    self.logger.info('数据库章节加载失败，重新抓取')
                            else:
                                self.logger.info(f'数据库只有{novel_check["chapters_count"]}章，需要补充')
                    else:
                        # 方法2：使用 check_novel_exists 方法
                        # 先获取小说详情以得到标题和作者
                        detail = self.fetch_novel_detail(novel_url, novel_id)
                        if detail and 'title' in detail and 'author' in detail:
                            novel_check = self.db_handler.check_novel_exists(detail['title'], detail['author'],
                                                                             'qidian')
                            if novel_check['exists']:
                                # 检查是否有章节
                                if novel_check['has_chapters'] and novel_check['chapters_count'] >= chapter_count:
                                    self.logger.info(f'数据库已有{novel_check["chapters_count"]}章，直接加载')
                                    # 从数据库加载章节
                                    db_chapters = self.db_handler.get_novel_chapters(novel_check['novel_id'],
                                                                                     chapter_count)
                                    if db_chapters:
                                        for db_chapter in db_chapters:
                                            chapters.append({
                                                'chapter_num': db_chapter['chapter_num'],
                                                'chapter_title': db_chapter['chapter_title'],
                                                'chapter_content': db_chapter['chapter_content'],
                                                'chapter_url': db_chapter['chapter_url']
                                            })
                                        return chapters
                                    else:
                                        self.logger.info('数据库章节加载失败，重新抓取')
                                else:
                                    self.logger.info(f'数据库只有{novel_check["chapters_count"]}章或没有章节，需要补充')
                        else:
                            self.logger.info('无法获取小说详情，直接抓取章节')
                except Exception as e:
                    self.logger.warning(f'数据库检查失败，将直接抓取章节: {e}')

            # 需要从网站抓取章节
            chapters = self._fetch_novel_chapters_from_website(novel_url, novel_id, chapter_count)

            # 确保每个章节都有novel_title
            for chapter in chapters:
                if not chapter.get('novel_title') and novel_title:
                    chapter['novel_title'] = novel_title

            return chapters

        except Exception as e:
            self.logger.error(f'获取小说章节内容失败 {novel_url}: {e}')
            return []



    """为书籍列表补充详情信息"""
    def enrich_books_with_details(self, books, max_books=20, fetch_chapters=False, chapter_count=None,
                                  check_existing=True):
        """为书籍列表补充详情信息

        Args:
            books: 书籍列表
            max_books: 最大处理书籍数
            fetch_chapters: 是否获取章节内容
            chapter_count: 获取的章节数，默认为配置中的default_chapter_count
            check_existing: 是否检查数据库中已有的记录
        """
        # 如果没有指定章节数，使用配置中的默认值
        if chapter_count is None:
            chapter_count = self.default_chapter_count

        enriched_books = []

        for i, book in enumerate(books[:max_books]):
            original_title = book['title']
            author = book.get('author', '未知')

            self.logger.info(f'正在补充详情 ({i + 1}/{min(len(books), max_books)}): {original_title}')

            try:
                # 获取元数据
                detail = self.fetch_novel_detail(book['url'], book.get('novel_id', ''))

                # 更新书籍信息
                if 'title' in detail:
                    if detail['title'] != original_title:
                        self.logger.debug(f"标题变化: '{original_title}' -> '{detail['title']}'")
                    book['title'] = detail['title']

                if 'author' in detail and detail['author'] != '未知':
                    book['author'] = detail['author']

                if 'category' in detail:
                    book['category'] = detail.get('category')

                if 'tags' in detail:
                    book['tags'] = detail.get('tags')

                if 'introduction' in detail:
                    book['introduction'] = detail.get('introduction')

                # 如果需要获取章节内容
                if fetch_chapters:
                    # 检查是否已有同名同作者的小说且已有章节
                    should_extract_chapters = True

                    if check_existing and self.db_handler:
                        # 检查数据库中是否存在同名同作者的小说
                        novel_check = self.db_handler.check_novel_exists(book['title'], book['author'], 'qidian')

                        if novel_check['exists']:
                            novel_id = novel_check['novel_id']

                            # 检查是否已有章节
                            if novel_check['has_chapters'] and novel_check['chapters_count'] >= chapter_count:
                                self.logger.info(f'小说已存在且已有章节，从数据库加载')
                                should_extract_chapters = False

                                # 从数据库加载现有章节
                                db_chapters = self.db_handler.get_novel_chapters(novel_id, chapter_count)
                                if db_chapters:
                                    book['chapters'] = []
                                    for db_chapter in db_chapters:
                                        book['chapters'].append({
                                            'chapter_num': db_chapter['chapter_num'],
                                            'chapter_title': db_chapter['chapter_title'],
                                            'chapter_content': db_chapter['chapter_content'],
                                            'chapter_url': db_chapter['chapter_url']
                                        })

                            # 更新novel_id为数据库中的ID
                            book['novel_id'] = novel_id

                    # 如果需要抓取章节
                    if should_extract_chapters:
                        chapters = self.fetch_novel_chapters(book['url'], book.get('novel_id', ''), chapter_count)
                        if chapters:
                            book['chapters'] = chapters

                enriched_books.append(book)

            except Exception as e:
                self.logger.error(f'补充详情失败 {original_title}: {e}')
                enriched_books.append(book)

            # 礼貌等待
            time.sleep(random.uniform(2, 4))

        return enriched_books

    """抓取所有榜单"""
    def fetch_all_ranks(self):
            all_books = []

            for rank_type in self.site_config['rank_urls']:
                try:
                    books = self.fetch_rank_list(rank_type)
                    all_books.extend(books)

                    # 保存原始数据
                    self._save_raw_data(books, f'{self.name}_{rank_type}_{time.strftime("%Y%m%d")}.json')

                except Exception as e:
                    self.logger.error(f'抓取{rank_type}榜失败: {e}')

            return all_books

    """关闭Selenium"""
    def close(self):
        """关闭Selenium驱动"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Selenium WebDriver已关闭")