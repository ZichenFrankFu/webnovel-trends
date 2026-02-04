# database/db_handler.py

import sqlite3
import json
from datetime import datetime, timedelta
import logging
import os
import threading
import time


class DatabaseHandler:
    def __init__(self, db_path, is_test=False):
        self.db_path = db_path
        self.is_test = is_test
        self.logger = logging.getLogger('DatabaseHandler')
        self._db_lock = threading.Lock()
        self._init_database()

    def _init_database(self):
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # 使用带锁的连接
        with self._db_lock:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 如果这是测试数据库，删除并重新创建所有表
            if self.is_test:
                self.logger.info("初始化测试数据库...")
                cursor.execute("DROP TABLE IF EXISTS daily_rankings")
                cursor.execute("DROP TABLE IF EXISTS novel_archive")
                cursor.execute("DROP TABLE IF EXISTS novel_chapters")
                conn.commit()

            # 创建每日榜单表（使用独立的id作为主键）
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                novel_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                title TEXT NOT NULL,
                author TEXT,
                rank INTEGER NOT NULL,
                rank_type TEXT NOT NULL,
                category TEXT,
                introduction TEXT,
                url TEXT,
                fetch_date DATE NOT NULL,
                fetch_time TIME NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(novel_id, platform, rank_type, fetch_date, fetch_time)
            )
            ''')

            # 创建小说档案表（使用novel_id作为主键）
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS novel_archive (
                novel_id TEXT PRIMARY KEY NOT NULL,
                platform TEXT NOT NULL,
                title TEXT NOT NULL,
                author TEXT,
                category TEXT,
                introduction TEXT,
                url TEXT NOT NULL,
                first_seen DATE,
                last_updated DATE,
                has_chapters BOOLEAN DEFAULT 0,
                chapters_count INTEGER DEFAULT 0,
                chapters_last_extracted DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # 创建章节内容表（使用独立的id作为主键）
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS novel_chapters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                novel_id TEXT NOT NULL,
                novel_title TEXT NOT NULL,
                chapter_num INTEGER NOT NULL,
                chapter_title TEXT NOT NULL,
                chapter_content TEXT,
                chapter_url TEXT,
                word_count INTEGER DEFAULT 0,
                first_post_time DATE,
                extract_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(novel_id, chapter_num)
            )
            ''')

            conn.commit()

            # 初始化id序列（确保从1开始）
            self._init_id_sequences(conn)

            # 创建索引
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_daily_rank_composite 
            ON daily_rankings(fetch_date, platform, rank_type, rank)
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_daily_novel_id ON daily_rankings(novel_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_novel_archive_url ON novel_archive(url)')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_novel_archive_platform_title ON novel_archive(platform, title)')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_novel_chapters_novel_id ON novel_chapters(novel_id, chapter_num)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chapters_first_post_time ON novel_chapters(first_post_time)')

            conn.commit()
            conn.close()

            self.logger.info(f"数据库初始化完成: {self.db_path}")

    def _init_id_sequences(self, conn):
        """初始化表的id序列，确保从1开始"""
        cursor = conn.cursor()

        # 重置daily_rankings表的自增序列
        try:
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='daily_rankings'")
            self.logger.info("已重置daily_rankings表的自增序列")
        except Exception as e:
            self.logger.debug(f"重置daily_rankings序列失败: {e}")

        # 重置novel_chapters表的自增序列
        try:
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='novel_chapters'")
            self.logger.info("已重置novel_chapters表的自增序列")
        except Exception as e:
            self.logger.debug(f"重置novel_chapters序列失败: {e}")

        conn.commit()

    def _get_connection(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 确保数据库文件所在目录存在
                db_dir = os.path.dirname(self.db_path)
                if db_dir:
                    os.makedirs(db_dir, exist_ok=True)

                conn = sqlite3.connect(self.db_path, timeout=30)
                # 启用外键约束
                conn.execute("PRAGMA foreign_keys = ON")
                # 启用WAL模式（Write-Ahead Logging）减少锁冲突
                conn.execute("PRAGMA journal_mode = WAL")
                return conn
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"数据库被锁，重试 {attempt + 1}/{max_retries}")
                    time.sleep(1)
                else:
                    raise
        return None

    def save_daily_ranking(self, book_data):
        """保存每日榜单数据（带锁和重试）"""
        max_retries = 3

        # 验证数据完整性
        if not self._validate_book_data(book_data):
            self.logger.warning(f"书籍数据验证失败，跳过保存: {book_data.get('title', '未知')}")
            return False

        for attempt in range(max_retries):
            try:
                with self._db_lock:  # 使用锁确保线程安全
                    conn = self._get_connection()
                    cursor = conn.cursor()

                    # 准备数据：合并tags到category
                    category = book_data.get('category', '未知')
                    tags = book_data.get('tags', [])

                    # 如果tags存在且不为空，将其合并到category
                    if tags and isinstance(tags, list) and len(tags) > 0:
                        tags_str = ' | '.join(tags)
                        if category and category != '未知':
                            category = f"{category} | {tags_str}"
                        else:
                            category = tags_str

                    # 获取简介
                    intro = book_data.get('introduction') or book_data.get('brief_intro', '')
                    intro = intro[:500] if intro else ''  # 限制长度

                    # 先检查是否已存在相同记录
                    cursor.execute('''
                    SELECT id FROM daily_rankings 
                    WHERE novel_id = ? AND platform = ? AND rank_type = ? AND fetch_date = ? AND fetch_time = ?
                    ''', (
                        book_data['novel_id'],
                        book_data['platform'],
                        book_data['rank_type'],
                        book_data['fetch_date'],
                        book_data['fetch_time']
                    ))

                    existing_record = cursor.fetchone()

                    if existing_record:
                        # 更新现有记录
                        cursor.execute('''
                        UPDATE daily_rankings 
                        SET title = ?, author = ?, rank = ?, category = ?, introduction = ?, url = ?
                        WHERE id = ?
                        ''', (
                            book_data['title'],
                            book_data.get('author', ''),
                            book_data['rank'],
                            category,
                            intro,
                            book_data.get('url', ''),
                            existing_record[0]
                        ))
                    else:
                        # 插入新记录
                        cursor.execute('''
                        INSERT INTO daily_rankings 
                        (novel_id, platform, title, author, rank, rank_type, category, introduction, url, fetch_date, fetch_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            book_data['novel_id'],
                            book_data['platform'],
                            book_data['title'],
                            book_data.get('author', ''),
                            book_data['rank'],
                            book_data['rank_type'],
                            category,  # 合并后的category
                            intro,
                            book_data.get('url', ''),
                            book_data['fetch_date'],
                            book_data['fetch_time']
                        ))

                    # 更新小说档案表
                    self._update_novel_archive(book_data, cursor, category)

                    conn.commit()
                    self.logger.debug(f"保存数据: {book_data['title']} (排名: {book_data['rank']})")

                    conn.close()
                    return True

            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"保存数据时数据库被锁，重试 {attempt + 1}/{max_retries}")
                    time.sleep(2)
                else:
                    self.logger.error(f"保存数据失败 {book_data.get('title')}: {e}")
                    return False
            except Exception as e:
                self.logger.error(f"保存数据失败 {book_data.get('title')}: {e}")
                return False

        return False

    def _validate_book_data(self, book_data):
        """验证书籍数据完整性"""
        required_fields = ['novel_id', 'platform', 'title', 'rank', 'rank_type', 'fetch_date', 'fetch_time']

        for field in required_fields:
            if field not in book_data or not book_data[field]:
                self.logger.warning(f"书籍数据缺少必要字段: {field}")
                return False

        # 验证 novel_id 格式
        novel_id = str(book_data['novel_id']).strip()
        if not novel_id or novel_id == '0' or len(novel_id) < 3:
            self.logger.warning(f"novel_id 格式无效: {novel_id}")
            return False

        # 验证标题
        title = str(book_data['title']).strip()
        if not title or len(title) < 1:
            self.logger.warning(f"标题无效: {title}")
            return False

        return True

    def _update_novel_archive(self, book_data, cursor, category):
        """更新小说档案表"""
        # 检查小说是否已存在
        cursor.execute('SELECT novel_id, first_seen FROM novel_archive WHERE novel_id = ?',
                    (book_data['novel_id'],))
        result = cursor.fetchone()

        # 获取简介
        intro = book_data.get('introduction') or book_data.get('brief_intro', '')
        intro = intro[:1000] if intro else ''  # 限制长度

        # 确保URL正确存储
        url = book_data.get('url', '')
        if not url and 'novel_url' in book_data:
            url = book_data['novel_url']

        # Calculate HAS_CHAPTERS and CHAPTERS_COUNT
        cursor.execute('''
        SELECT COUNT(*) FROM novel_chapters
        WHERE novel_id = ?
        ''', (book_data['novel_id'],))
        chapters_count = cursor.fetchone()[0]
        has_chapters = 1 if chapters_count > 0 else 0

        if result:
            # 更新现有记录
            novel_id, first_seen = result

            cursor.execute('''
            UPDATE novel_archive 
            SET title = ?, author = ?, category = ?, introduction = ?, 
                url = ?, last_updated = ?, has_chapters = ?, chapters_count = ?
            WHERE novel_id = ?
            ''', (
                book_data['title'],
                book_data.get('author', ''),
                category,  # 合并后的category
                intro,
                url,
                book_data['fetch_date'],
                has_chapters,
                chapters_count,
                book_data['novel_id']
            ))
        else:
            # 插入新记录
            cursor.execute('''
            INSERT OR REPLACE INTO novel_archive 
            (novel_id, platform, title, author, category, introduction, url, first_seen, last_updated, has_chapters, chapters_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                book_data['novel_id'],
                book_data['platform'],
                book_data['title'],
                book_data.get('author', ''),
                category,  # 合并后的category
                intro,
                url,
                book_data['fetch_date'],
                book_data['fetch_date'],
                has_chapters,
                chapters_count
            ))

    def check_novel_exists(self, title, author, platform='qidian'):
        """检查是否存在同名同作者的小说"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute('''
                SELECT novel_id, title, author, url, has_chapters, chapters_count, chapters_last_extracted 
                FROM novel_archive 
                WHERE platform = ? AND LOWER(title) = LOWER(?) AND LOWER(author) = LOWER(?)
                ''', (platform, title.strip(), author.strip() if author else ''))

                result = cursor.fetchone()
                conn.close()

                if result:
                    novel_id, db_title, db_author, db_url, has_chapters, chapters_count, last_extracted = result
                    return {
                        'exists': True,
                        'novel_id': novel_id,
                        'title': db_title,
                        'author': db_author,
                        'url': db_url,
                        'has_chapters': bool(has_chapters),
                        'chapters_count': chapters_count or 0,
                        'chapters_last_extracted': last_extracted
                    }
                return {'exists': False}

        except Exception as e:
            self.logger.error(f'检查小说存在失败: {e}')
            return {'exists': False}

    def check_novel_exists_by_url(self, novel_url):
        """通过URL检查小说是否存在"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute('''
                SELECT novel_id, title, author, has_chapters, chapters_count, chapters_last_extracted 
                FROM novel_archive 
                WHERE url = ?
                ''', (novel_url,))

                result = cursor.fetchone()
                conn.close()

                if result:
                    novel_id, db_title, db_author, has_chapters, chapters_count, last_extracted = result
                    return {
                        'exists': True,
                        'novel_id': novel_id,
                        'title': db_title,
                        'author': db_author,
                        'has_chapters': bool(has_chapters),
                        'chapters_count': chapters_count or 0,
                        'chapters_last_extracted': last_extracted
                    }
                return {'exists': False}

        except Exception as e:
            self.logger.error(f'通过URL检查小说存在失败: {e}')
            return {'exists': False}

    def check_novel_has_chapters(self, novel_id):
        """检查小说是否有章节"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                # 检查novel_archive表
                cursor.execute('''
                SELECT has_chapters, chapters_count 
                FROM novel_archive 
                WHERE novel_id = ?
                ''', (novel_id,))

                result = cursor.fetchone()
                if result and result[0]:  # has_chapters为True
                    return True

                # 检查novel_chapters表
                cursor.execute('''
                SELECT COUNT(*) 
                FROM novel_chapters 
                WHERE novel_id = ?
                ''', (novel_id,))

                chapter_count = cursor.fetchone()[0]
                conn.close()

                return chapter_count > 0

        except Exception as e:
            self.logger.error(f'检查小说章节失败: {e}')
            return False

    def get_chapters_count(self, novel_id):
        """获取小说的章节数量"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute('''
                SELECT COUNT(*) 
                FROM novel_chapters 
                WHERE novel_id = ?
                ''', (novel_id,))

                result = cursor.fetchone()
                conn.close()

                return result[0] if result else 0

        except Exception as e:
            self.logger.error(f'获取章节数量失败: {e}')
            return 0

    def save_novel(self, novel_data, chapters=None):
        """保存小说信息和章节内容"""
        # 验证数据
        if not self._validate_novel_data(novel_data):
            self.logger.warning(f"小说数据验证失败，跳过保存: {novel_data.get('title', '未知')}")
            return False

        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                # 合并tags到category
                category = novel_data.get('category', '未知')
                tags = novel_data.get('tags', [])

                if tags and isinstance(tags, list) and len(tags) > 0:
                    tags_str = ' | '.join(tags)
                    if category and category != '未知':
                        category = f"{category} | {tags_str}"
                    else:
                        category = tags_str

                # 确保URL正确存储
                url = novel_data.get('url', '')
                if not url and 'novel_url' in novel_data:
                    url = novel_data['novel_url']

                # 保存小说基本信息到novel_archive表
                # 检查是否已存在此小说
                cursor.execute('SELECT has_chapters, chapters_count FROM novel_archive WHERE novel_id = ?',
                              (novel_data['novel_id'],))
                existing_record = cursor.fetchone()
                
                # 如果记录已存在且没有传入新章节，保留现有的chapters信息
                if existing_record and not chapters:
                    existing_has_chapters, existing_chapters_count = existing_record
                    has_chap = existing_has_chapters
                    chap_count = existing_chapters_count
                else:
                    # 如果没有现有记录或传入了新章节，使用新的章节信息
                    has_chap = 1 if (chapters and len(chapters) > 0) else 0
                    chap_count = len(chapters) if chapters else 0
                
                cursor.execute('''
                INSERT OR REPLACE INTO novel_archive 
                (novel_id, platform, title, author, category, introduction, url, first_seen, last_updated, has_chapters, chapters_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    novel_data['novel_id'],
                    novel_data['platform'],
                    novel_data['title'],
                    novel_data.get('author', ''),
                    category,  # 合并后的category
                    novel_data.get('introduction', ''),
                    url,
                    novel_data.get('first_seen', datetime.now().strftime('%Y-%m-%d')),
                    datetime.now().strftime('%Y-%m-%d'),
                    has_chap,  # has_chapters
                    chap_count  # chapters_count
                ))

                # 如果有章节内容，保存章节
                if chapters and len(chapters) > 0:
                    self.save_chapters(novel_data['novel_id'], chapters, cursor)

                conn.commit()
                conn.close()

                self.logger.info(f"保存小说信息成功: {novel_data['title']}")
                return True

        except Exception as e:
            self.logger.error(f'保存小说信息失败: {e}')
            return False

    def _validate_novel_data(self, novel_data):
        """验证小说数据完整性"""
        required_fields = ['novel_id', 'platform', 'title']

        for field in required_fields:
            if field not in novel_data or not novel_data[field]:
                self.logger.warning(f"小说数据缺少必要字段: {field}")
                return False

        # 验证 novel_id 格式
        novel_id = str(novel_data['novel_id']).strip()
        if not novel_id or novel_id == '0' or len(novel_id) < 3:
            self.logger.warning(f"novel_id 格式无效: {novel_id}")
            return False

        # 验证标题
        title = str(novel_data['title']).strip()
        if not title or len(title) < 1:
            self.logger.warning(f"标题无效: {title}")
            return False

        return True

    def save_chapters(self, novel_id, chapters, cursor=None):
        """保存章节内容到数据库（包含首发时间）"""
        if not chapters:
            return False

        # 验证 novel_id
        if not novel_id or novel_id == '0' or len(str(novel_id).strip()) < 3:
            self.logger.warning(f"无效的 novel_id: {novel_id}")
            return False

        try:
            close_conn = False
            if cursor is None:
                # 如果没有传入cursor，自己创建连接
                with self._db_lock:
                    conn = self._get_connection()
                    cursor = conn.cursor()
                    close_conn = True
            else:
                # 如果传入了cursor，使用现有的连接
                conn = None

            today = datetime.now().strftime('%Y-%m-%d')
            saved_count = 0

            # 保存每个章节
            for chapter in chapters:
                try:
                    # 验证章节数据
                    if not self._validate_chapter_data(chapter):
                        self.logger.warning(f"章节数据验证失败，跳过: {chapter.get('chapter_title', '未知')}")
                        continue

                    word_count = len(chapter.get('chapter_content', ''))

                    # 处理首发时间
                    first_post_time = chapter.get('first_post_time', '')
                    if not first_post_time and 'first_post_time' not in chapter:
                        # 尝试从其他字段获取
                        first_post_time = chapter.get('post_time', '')

                    # 确保日期格式正确
                    if first_post_time:
                        try:
                            # 尝试解析日期格式
                            datetime.strptime(first_post_time, '%Y-%m-%d')
                        except ValueError:
                            # 如果格式不正确，设为空
                            first_post_time = ''

                    # 获取小说标题（从章节数据中获取）
                    novel_title = chapter.get('novel_title', '')

                    # 先检查是否已存在相同章节
                    cursor.execute('''
                    SELECT id FROM novel_chapters 
                    WHERE novel_id = ? AND chapter_num = ?
                    ''', (novel_id, chapter['chapter_num']))

                    existing_chapter = cursor.fetchone()

                    if existing_chapter:
                        # 更新现有章节
                        cursor.execute('''
                        UPDATE novel_chapters 
                        SET novel_title = ?, chapter_title = ?, chapter_content = ?, chapter_url = ?, 
                            word_count = ?, first_post_time = ?, extract_date = ?
                        WHERE id = ?
                        ''', (
                            novel_title,  # 添加小说标题
                            chapter['chapter_title'],
                            chapter.get('chapter_content', ''),
                            chapter.get('chapter_url', ''),
                            word_count,
                            first_post_time,  # 首发时间
                            today,
                            existing_chapter[0]
                        ))
                    else:
                        # 插入新章节 - 修复SQL语句参数顺序
                        cursor.execute('''
                        INSERT OR IGNORE INTO novel_chapters 
                        (novel_id, novel_title, chapter_num, chapter_title, chapter_content, chapter_url, word_count, first_post_time, extract_date) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            novel_id,
                            novel_title,  # 小说标题
                            chapter['chapter_num'],
                            chapter['chapter_title'],
                            chapter.get('chapter_content', ''),
                            chapter.get('chapter_url', ''),
                            word_count,
                            first_post_time,  # 首发时间
                            today
                        ))
                    saved_count += 1
                except Exception as e:
                    self.logger.warning(f'保存章节 {chapter.get("chapter_num")} 失败: {e}')
                    continue

            # 更新novel_archive表的章节信息
            cursor.execute('''
            SELECT COUNT(*) FROM novel_chapters WHERE novel_id = ?
            ''', (novel_id,))
            chapter_count = cursor.fetchone()[0]
            
            cursor.execute('''
            UPDATE novel_archive 
            SET has_chapters = ?, 
                chapters_count = ?,
                chapters_last_extracted = ?
            WHERE novel_id = ?
            ''', (1 if chapter_count > 0 else 0, chapter_count, today, novel_id))

            if close_conn and conn:
                conn.commit()
                conn.close()

            self.logger.info(f"成功保存了 {saved_count}/{len(chapters)} 个章节到数据库: {novel_id}")
            return saved_count > 0

        except Exception as e:
            self.logger.error(f'保存章节失败: {e}')
            return False

    def _validate_chapter_data(self, chapter_data):
        """验证章节数据完整性"""
        required_fields = ['chapter_num', 'chapter_title']

        for field in required_fields:
            if field not in chapter_data or chapter_data[field] is None:
                self.logger.warning(f"章节数据缺少必要字段: {field}")
                return False

        # 验证章节编号
        chapter_num = chapter_data['chapter_num']
        if not isinstance(chapter_num, int) or chapter_num < 1:
            self.logger.warning(f"章节编号无效: {chapter_num}")
            return False

        # 验证章节标题
        chapter_title = str(chapter_data['chapter_title']).strip()
        if not chapter_title or len(chapter_title) < 1:
            self.logger.warning(f"章节标题无效: {chapter_title}")
            return False

        # 验证小说标题（可选，但建议有）
        if 'novel_title' not in chapter_data:
            self.logger.warning(f"章节数据缺少novel_title字段，但将继续保存")
            # 不返回False，因为novel_title不是必需字段

        return True

    def get_novel_chapters(self, novel_id, limit=5):
        """获取小说的章节内容（包含首发时间）"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute('''
                SELECT id, chapter_num, chapter_title, chapter_content, chapter_url, word_count, first_post_time, extract_date
                FROM novel_chapters 
                WHERE novel_id = ?
                ORDER BY chapter_num
                LIMIT ?
                ''', (novel_id, limit))

                rows = cursor.fetchall()
                conn.close()

                chapters = []
                for row in rows:
                    chapters.append({
                        'id': row[0],  # novel_chapters表的独立id
                        'chapter_num': row[1],
                        'chapter_title': row[2],
                        'chapter_content': row[3],
                        'chapter_url': row[4],
                        'word_count': row[5],
                        'first_post_time': row[6],  # 首发时间
                        'extract_date': row[7]
                    })

                return chapters

        except Exception as e:
            self.logger.error(f'获取章节失败: {e}')
            return []

    def update_novel_chapter_info(self, novel_id):
        """更新小说的章节信息统计"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                # 获取章节数量
                cursor.execute('''
                SELECT COUNT(*) 
                FROM novel_chapters 
                WHERE novel_id = ?
                ''', (novel_id,))

                chapter_count = cursor.fetchone()[0]

                if chapter_count > 0:
                    # 更新章节信息
                    cursor.execute('''
                    UPDATE novel_archive 
                    SET has_chapters = 1, 
                        chapters_count = ?,
                        chapters_last_extracted = ?
                    WHERE novel_id = ?
                    ''', (chapter_count, datetime.now().strftime('%Y-%m-%d'), novel_id))
                else:
                    # 如果没有章节，重置状态
                    cursor.execute('''
                    UPDATE novel_archive 
                    SET has_chapters = 0, 
                        chapters_count = 0,
                        chapters_last_extracted = NULL
                    WHERE novel_id = ?
                    ''', (novel_id,))

                conn.commit()
                conn.close()

                self.logger.info(f"更新小说 {novel_id} 章节信息成功: {chapter_count} 章")
                return True

        except Exception as e:
            self.logger.error(f'更新章节信息失败: {e}')
            return False

    def get_novel_by_url(self, url):
        """通过URL获取小说信息"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                cursor.execute('''
                SELECT novel_id, title, author, category, introduction, url, 
                       has_chapters, chapters_count, chapters_last_extracted
                FROM novel_archive 
                WHERE url = ?
                ''', (url,))

                result = cursor.fetchone()
                conn.close()

                if result:
                    return {
                        'novel_id': result[0],
                        'title': result[1],
                        'author': result[2],
                        'category': result[3],
                        'introduction': result[4],
                        'url': result[5],
                        'has_chapters': bool(result[6]),
                        'chapters_count': result[7] or 0,
                        'chapters_last_extracted': result[8]
                    }
                return None

        except Exception as e:
            self.logger.error(f'通过URL获取小说信息失败: {e}')
            return None

    def batch_save_novels(self, novels_data):
        """批量保存小说数据"""
        if not novels_data:
            return 0

        saved_count = 0
        for novel_data in novels_data:
            if self.save_novel(novel_data):
                saved_count += 1

        self.logger.info(f"批量保存小说完成: {saved_count}/{len(novels_data)} 条记录")
        return saved_count

    def get_table_counts(self):
        """获取各表的记录数量"""
        try:
            with self._db_lock:
                conn = self._get_connection()
                cursor = conn.cursor()

                counts = {}

                # 获取daily_rankings表数量
                cursor.execute("SELECT COUNT(*) FROM daily_rankings")
                counts['daily_rankings'] = cursor.fetchone()[0]

                # 获取novel_archive表数量
                cursor.execute("SELECT COUNT(*) FROM novel_archive")
                counts['novel_archive'] = cursor.fetchone()[0]

                # 获取novel_chapters表数量
                cursor.execute("SELECT COUNT(*) FROM novel_chapters")
                counts['novel_chapters'] = cursor.fetchone()[0]

                conn.close()
                return counts

        except Exception as e:
            self.logger.error(f"获取表记录数量失败: {e}")
            return {}


# 创建测试数据库处理器的函数
def create_test_db_handler():
    """创建测试数据库处理器"""
    test_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_output', 'qidian_test.db')
    return DatabaseHandler(test_db_path, is_test=True)


# 创建正式数据库处理器的函数
def create_prod_db_handler():
    """创建正式数据库处理器"""
    prod_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'qidian.db')
    return DatabaseHandler(prod_db_path, is_test=False)