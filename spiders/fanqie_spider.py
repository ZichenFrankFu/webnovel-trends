# spiders/fanqie_spider.py
import config
import time
import re
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from .base_spider import BaseSpider
from .fanqie_font_decoder import FANQIE_CHAR_MAP

try:
    GLOBAL_SELENIUM_CONFIG = getattr(config, 'SELENIUM_CONFIG', {})
except ImportError:
    GLOBAL_SELENIUM_CONFIG = {
        'enabled': True,
        'browser': 'chrome',
        'options': {
            'headless': True,
            'no_sandbox': True,
            'disable_dev_shm_usage': True,
            'disable_gpu': True,
            'window_size': '1920,1080',
        },
        'timeout': 15,
    }


class FanqieSpider(BaseSpider):
    def __init__(self, site_config):
        super().__init__(site_config)
        self.driver = None
        self.selenium_config = self._get_selenium_config()
        self.selenium_specific = site_config.get('selenium_specific', {})
        self._init_selenium()
        self.chapter_fetcher = None

    def _get_selenium_config(self):
        """获取Selenium配置"""
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

        config.update(GLOBAL_SELENIUM_CONFIG)

        site_specific = self.site_config.get('selenium_specific', {})
        if site_specific:
            for key, value in site_specific.items():
                if key in config and isinstance(config[key], dict) and isinstance(value, dict):
                    config[key].update(value)
                else:
                    config[key] = value

        return config

    def _init_selenium(self):
        """初始化Selenium WebDriver"""
        if not self.selenium_config.get('enabled', True):
            self.logger.warning("Selenium已禁用")
            return

        try:
            options = webdriver.ChromeOptions()

            config_options = self.selenium_config.get('options', {})

            if config_options.get('headless', True):
                options.add_argument('--headless=new')

            for key, value in config_options.items():
                if key == 'headless' or key == 'window_size' or key == 'user_agent':
                    continue
                elif isinstance(value, bool) and value:
                    options.add_argument(f'--{key.replace("_", "-")}')
                elif isinstance(value, str):
                    options.add_argument(f'--{key.replace("_", "-")}={value}')

            if 'user_agent' in config_options:
                options.add_argument(f'user-agent={config_options["user_agent"]}')

            experimental_options = self.selenium_config.get('experimental_options', {})
            for key, value in experimental_options.items():
                options.add_experimental_option(key, value)

            if self.selenium_config.get('stealth_mode', True):
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_experimental_option("excludeSwitches", ["enable-automation"])
                options.add_experimental_option('useAutomationExtension', False)

            self.driver = webdriver.Chrome(options=options)

            if self.selenium_config.get('stealth_mode', True):
                self.driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)

            self.logger.info("Selenium WebDriver初始化成功")
            if self.driver:
                self.chapter_fetcher = FanqieChapterFetcher(self.driver, self.logger)
            return True

        except Exception as e:
            self.logger.error(f"Selenium初始化失败: {e}")
            return False

    def fetch_rank_list(self, rank_type='scifi_apocalypse'):
        """抓取番茄排行榜"""
        url = self.site_config['rank_urls'].get(rank_type)
        if not url:
            raise ValueError(f"未知的榜单类型: {rank_type}")

        self.logger.info(f'开始抓取{self.name} {rank_type}榜...')

        # 使用Selenium滚动加载完整页面
        html = self._scroll_to_load_all(url)

        if html is None:
            self.logger.warning("Selenium加载失败")
            return []

        soup = BeautifulSoup(html, 'html.parser')
        books = []

        rank_items = soup.find_all('div', class_='rank-book-item')

        for index, item in enumerate(rank_items[:30], 1):
            try:
                title_elem = item.select_one('.title a')
                if title_elem:
                    title = self._decrypt_text(title_elem.text.strip())
                    href = title_elem.get('href', '')
                else:
                    title_elem = item.find('a', class_=re.compile('title|book-title', re.I))
                    if title_elem:
                        title = self._decrypt_text(title_elem.text.strip())
                        href = title_elem.get('href', '')
                    else:
                        title = f'番茄小说{index}'
                        href = ''

                if href:
                    if href.startswith('//'):
                        full_url = 'https:' + href
                    elif href.startswith('/'):
                        full_url = urljoin(self.base_url, href)
                    else:
                        full_url = href
                else:
                    full_url = urljoin(self.base_url, f'/page/{index}')

                book_id_match = re.search(r'/book/(\d+)', href) or re.search(r'/page/(\d+)', href)
                book_id = book_id_match.group(1) if book_id_match else str(index)

                author_elem = item.select_one('.author a') or item.select_one('.author span')
                author = '未知'
                if author_elem:
                    author = self._decrypt_text(author_elem.text.strip())

                desc_elem = item.select_one('.desc.abstract')
                introduction = ''
                if desc_elem:
                    introduction = self._decrypt_text(desc_elem.text.strip())

                count_elem = item.select_one('.book-item-count')
                read_count = ''
                if count_elem:
                    read_count = self._decrypt_text(count_elem.text.strip())

                book_info = {
                    'novel_id': f'fanqie_{book_id}',
                    'platform': 'fanqie',
                    'title': title[:100],
                    'author': author,
                    'introduction': introduction[:200] if introduction else '',
                    'read_count': read_count,
                    'rank': index,
                    'rank_type': rank_type,
                    'url': full_url,
                    'fetch_date': time.strftime('%Y-%m-%d'),
                    'fetch_time': time.strftime('%H:%M:%S'),
                }

                books.append(book_info)

            except Exception as e:
                self.logger.debug(f'解析番茄书籍失败: {e}')
                continue

        self.logger.info(f'共抓取{len(books)}本小说')
        return books

    def _scroll_to_load_all(self, url):
        """使用Selenium滚动页面加载所有项目"""
        if not self.driver:
            self.logger.error("Selenium驱动未初始化")
            return None

        try:
            target_count = self.selenium_specific.get('target_count', 30)
            scroll_delay = self.selenium_specific.get('scroll_delay', 2)
            max_scroll_attempts = self.selenium_specific.get('max_scroll_attempts', 20)

            self.driver.get(url)

            wait = WebDriverWait(self.driver, 15)

            try:
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "rank-book-item")))
            except TimeoutException:
                self.logger.warning("未找到rank-book-item元素")

            current_items = self.driver.find_elements(By.CLASS_NAME, "rank-book-item")
            self.logger.info(f"初始加载了 {len(current_items)} 个项目")

            if len(current_items) >= target_count:
                return self.driver.page_source

            last_count = len(current_items)
            no_change_count = 0

            for attempt in range(max_scroll_attempts):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_delay)

                current_items = self.driver.find_elements(By.CLASS_NAME, "rank-book-item")
                current_count = len(current_items)

                self.logger.info(f"滚动后第 {attempt + 1} 次尝试，当前项目数: {current_count}")

                if current_count >= target_count:
                    self.logger.info(f"已达到目标数量 {target_count}，停止滚动")
                    break

                if current_count == last_count:
                    no_change_count += 1
                    if no_change_count >= 3:
                        self.logger.info(f"连续 {no_change_count} 次滚动没有新内容，停止滚动")
                        break
                else:
                    no_change_count = 0
                    last_count = current_count

                time.sleep(random.uniform(1, 2))

            page_source = self.driver.page_source
            final_items = self.driver.find_elements(By.CLASS_NAME, "rank-book-item")
            self.logger.info(f"最终获取到 {len(final_items)} 个项目")

            return page_source

        except Exception as e:
            self.logger.error(f"Selenium滚动加载失败: {e}")
            return None

    def _decrypt_text(self, text):
        """解密字体加密的文本"""
        if not text:
            return text

        result = []
        for char in text:
            if char in FANQIE_CHAR_MAP:
                result.append(FANQIE_CHAR_MAP[char])
            else:
                result.append(char)

        return ''.join(result)

    def _decrypt_html(self, html):
        """解密HTML中的所有加密文字"""
        for encrypted_char, real_char in FANQIE_CHAR_MAP.items():
            if encrypted_char != real_char:
                html = html.replace(encrypted_char, real_char)

        return html

    def fetch_novel_detail(self, novel_url, novel_id=''):
        """抓取番茄小说详情"""
        if not self.driver:
            self.logger.error("Selenium驱动未初始化")
            return {}

        try:
            self.driver.get(novel_url)
            time.sleep(random.uniform(2, 4))

            html = self.driver.page_source
            decrypted_html = self._decrypt_html(html)

            soup = BeautifulSoup(decrypted_html, 'html.parser')
            detail = {'novel_url': novel_url}

            title_selectors = ['h1', '.title', '.book-title', 'header h1']
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    detail['title'] = title_elem.text.strip()
                    break

            author_selectors = ['.author', '.writer', '.author-name', 'a[href*="author"]']
            for selector in author_selectors:
                author_elem = soup.select_one(selector)
                if author_elem:
                    detail['author'] = author_elem.text.strip()
                    break

            intro_selectors = ['.intro', '.description', '.book-intro', '.content']
            for selector in intro_selectors:
                intro_elem = soup.select_one(selector)
                if intro_elem:
                    detail['introduction'] = intro_elem.text.strip()
                    break

            tags = []
            tag_selectors = ['.tags', '.tag-list', '.category', '.label']
            for selector in tag_selectors:
                tag_container = soup.select_one(selector)
                if tag_container:
                    tag_elements = tag_container.find_all(['a', 'span'])
                    for tag in tag_elements:
                        tag_text = tag.text.strip()
                        if tag_text and tag_text not in tags and len(tag_text) < 20:
                            tags.append(tag_text)

            if tags:
                detail['tags'] = tags

            return detail

        except Exception as e:
            self.logger.error(f'抓取详情失败 {novel_url}: {e}')
            return {}

    def fetch_chapters(self, novel_url, novel_id, max_chapters=5):
        """抓取小说前N章内容"""
        if not self.chapter_fetcher:
            self.logger.error("章节抓取器未初始化")
            return []

        return self.chapter_fetcher.fetch_chapters(novel_url, novel_id, 'fanqie', max_chapters)

    def enrich_books_with_details(self, books, max_books=20, fetch_chapters=False, chapters_per_book=5):
        """为书籍列表补充详情信息，可选择抓取章节"""
        enriched_books = []

        for i, book in enumerate(books[:max_books]):
            self.logger.info(f'正在补充详情 ({i + 1}/{min(len(books), max_books)}): {book["title"]}')

            try:
                detail = self.fetch_novel_detail(book['url'], book.get('novel_id', ''))

                # 更新书籍信息
                if 'title' in detail:
                    book['title'] = detail['title']
                if 'author' in detail and detail['author'] != '未知':
                    book['author'] = detail['author']
                if 'tags' in detail:
                    book['tags'] = detail.get('tags')
                if 'introduction' in detail:
                    book['introduction'] = detail.get('introduction')

                # 如果需要抓取章节
                if fetch_chapters:
                    chapters = self.fetch_chapters(book['url'], book['novel_id'], chapters_per_book)
                    book['chapters'] = chapters
                    self.logger.info(f"已抓取 {len(chapters)} 章内容")

                enriched_books.append(book)

            except Exception as e:
                self.logger.error(f'补充详情失败 {book["title"]}: {e}')
                enriched_books.append(book)

            time.sleep(random.uniform(1, 3))

        return enriched_books

    def fetch_all_ranks(self):
        """抓取所有榜单"""
        all_books = []

        for rank_type in self.site_config['rank_urls']:
            try:
                books = self.fetch_rank_list(rank_type)
                all_books.extend(books)

                self._save_raw_data(books, f'{self.name}_{rank_type}_{time.strftime("%Y%m%d")}.json')

            except Exception as e:
                self.logger.error(f'抓取{rank_type}榜失败: {e}')

        return all_books

    def close(self):
        """关闭Selenium驱动"""
        if self.driver:
            self.driver.quit()
            self.logger.info("Selenium WebDriver已关闭")