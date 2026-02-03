# spiders/base_spider.py
import time
import logging
from abc import ABC, abstractmethod
import json
import os


class BaseSpider(ABC):
    def __init__(self, site_config):
        self.site_config = site_config
        self.name = site_config['name']
        self.base_url = site_config['base_url']
        self.request_delay = site_config.get('request_delay', 2)
        self.logger = self._setup_logger()

    def _setup_logger(self):
        logger = logging.getLogger(f'{self.name}_spider')
        logger.setLevel(logging.INFO)

        # 确保日志目录存在
        log_dir = 'outputs/logs'
        os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(f'{log_dir}/{self.name}_spider.log', encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    @abstractmethod
    def fetch_rank_list(self, rank_type='hot'):
        pass

    @abstractmethod
    def fetch_novel_detail(self, novel_url, novel_id=''):
        pass

    @abstractmethod
    def enrich_books_with_details(self, books, max_books=20):
        pass

    @abstractmethod
    def fetch_all_ranks(self):
        pass

    def _save_raw_data(self, data, filename):
        raw_data_dir = 'outputs/data/raw'
        os.makedirs(raw_data_dir, exist_ok=True)

        filepath = os.path.join(raw_data_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        self.logger.debug(f'原始数据已保存: {filepath}')