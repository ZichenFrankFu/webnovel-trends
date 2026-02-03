# spiders/request_handler.py
import requests
import time
import random
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class RequestHandler:
    """智能请求处理器，自动切换策略"""

    def __init__(self):
        self.logger = logging.getLogger('RequestHandler')
        self.session = requests.Session()
        self.driver = None

    def get_headers(self):
        """生成随机请求头"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]

        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.qidian.com/',
        }

    def request_with_retry(self, url, max_retries=3):
        """带重试的请求"""
        for i in range(max_retries):
            try:
                headers = self.get_headers()
                response = self.session.get(url, headers=headers, timeout=10)

                # 检查是否被反爬
                if response.status_code == 202 or 'probe.js' in response.text:
                    self.logger.warning(f"检测到反爬机制，尝试 {i + 1}/{max_retries}")

                    if i == max_retries - 1:
                        self.logger.info("切换到Selenium模式...")
                        return self.request_with_selenium(url)

                    time.sleep(random.uniform(2, 5))
                    continue

                return response

            except Exception as e:
                self.logger.error(f"请求失败 {i + 1}/{max_retries}: {e}")
                if i < max_retries - 1:
                    time.sleep(random.uniform(3, 6))

        # 所有重试都失败，使用Selenium
        return self.request_with_selenium(url)

    def request_with_selenium(self, url):
        """使用Selenium请求（处理懒加载页面）"""
        try:
            if not hasattr(self, 'driver') or not self.driver:
                self._init_selenium()

            self.logger.info(f"使用Selenium访问懒加载页面: {url}")
            self.driver.get(url)

            # 等待初始加载
            time.sleep(3)

            # 返回一个包含driver的响应对象
            class SeleniumResponse:
                def __init__(self, driver):
                    self.driver = driver
                    self.text = driver.page_source

            return SeleniumResponse(self.driver)

        except Exception as e:
            self.logger.error(f"Selenium请求失败: {e}")
            return None

    def _init_selenium(self):
        """初始化Selenium"""
        options = Options()
        options.add_argument('--headless')  # 无头模式
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # 隐藏自动化特征
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    def close(self):
        """关闭资源"""
        if self.driver:
            self.driver.quit()


class MockResponse:
    """模拟requests.Response对象"""
    def __init__(self, html, encoding='utf-8'):
        self.text = html
        self.content = html.encode(encoding) if isinstance(html, str) else html
        self.status_code = 200
        self.encoding = encoding