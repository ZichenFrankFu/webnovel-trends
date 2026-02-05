# database/db_schema.py
from __future__ import annotations

import sqlite3


DDL = [
    # =========================
    # Canonical Novel (one novel belongs to exactly one platform)
    # =========================
    """
    CREATE TABLE IF NOT EXISTS novels (
        novel_uid INTEGER PRIMARY KEY AUTOINCREMENT,

        platform TEXT NOT NULL,                   -- qidian / fanqie
        platform_novel_id TEXT NOT NULL,          -- platform internal id

        author TEXT NOT NULL DEFAULT '',
        author_norm TEXT NOT NULL DEFAULT '',

        intro TEXT NOT NULL DEFAULT '',
        intro_norm TEXT NOT NULL DEFAULT '',

        main_category TEXT NOT NULL DEFAULT '',

        status TEXT NOT NULL DEFAULT 'ongoing',   -- ongoing / completed
        total_words INTEGER NOT NULL DEFAULT 0,   -- current total word count

        url TEXT NOT NULL DEFAULT '',
        signature_json TEXT NOT NULL DEFAULT '{}',   -- intro/chapter-title signature for dedup

        created_date DATE DEFAULT NULL,
        last_seen_date DATE NOT NULL
    );
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_novels_platform_id ON novels(platform, platform_novel_id);",
    "CREATE INDEX IF NOT EXISTS idx_novels_author_norm ON novels(platform, author_norm);",
    "CREATE INDEX IF NOT EXISTS idx_novels_main_category ON novels(main_category);",
    "CREATE INDEX IF NOT EXISTS idx_novels_status ON novels(status);",
    "CREATE INDEX IF NOT EXISTS idx_novels_last_seen ON novels(last_seen_date);",

    # =========================
    # Multiple titles per novel (rename history / aliases)
    # =========================
    """
    CREATE TABLE IF NOT EXISTS novel_titles (
        title_id INTEGER PRIMARY KEY AUTOINCREMENT,
        novel_uid INTEGER NOT NULL,

        title TEXT NOT NULL,
        title_norm TEXT NOT NULL,
        is_primary INTEGER NOT NULL DEFAULT 0,

        first_seen_date DATE NOT NULL,
        last_seen_date DATE NOT NULL,

        FOREIGN KEY(novel_uid) REFERENCES novels(novel_uid) ON DELETE CASCADE,
        UNIQUE(novel_uid, title_norm)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_titles_novel_uid ON novel_titles(novel_uid);",
    "CREATE INDEX IF NOT EXISTS idx_titles_title_norm ON novel_titles(title_norm);",

    # =========================
    # Unified Tags Dictionary
    # =========================
    """
    CREATE TABLE IF NOT EXISTS tags (
        tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag_name TEXT NOT NULL,
        tag_norm TEXT NOT NULL,
        UNIQUE(tag_norm)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_tags_norm ON tags(tag_norm);",

    # =========================
    # Novel <-> Tag mapping (many-to-many)
    # =========================
    """
    CREATE TABLE IF NOT EXISTS novel_tag_map (
        novel_uid INTEGER NOT NULL,
        tag_id INTEGER NOT NULL,
        PRIMARY KEY(novel_uid, tag_id),
        FOREIGN KEY(novel_uid) REFERENCES novels(novel_uid) ON DELETE CASCADE,
        FOREIGN KEY(tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_novel_tag_map_tag ON novel_tag_map(tag_id);",

    # =========================
    # Rank list definition
    # =========================

"""
CREATE TABLE IF NOT EXISTS rank_lists (
    rank_list_id INTEGER PRIMARY KEY AUTOINCREMENT,

    platform TEXT NOT NULL,
    rank_family TEXT NOT NULL,                 -- fanqie: 阅读榜/新书榜 ; qidian: 畅销榜/月票榜/推荐榜/收藏榜/新书榜
    rank_sub_cat TEXT NOT NULL DEFAULT '',     -- only for qidian 新书榜四类；fanqie always ''

    source_url TEXT NOT NULL DEFAULT '',

    UNIQUE(platform, rank_family, rank_sub_cat)
);
""",
"CREATE INDEX IF NOT EXISTS idx_rank_lists_platform_family ON rank_lists(platform, rank_family);",
"CREATE INDEX IF NOT EXISTS idx_rank_lists_family_subcat ON rank_lists(rank_family, rank_sub_cat);",

    # =========================
    # Daily snapshot (date-only)
    # =========================
    """
    CREATE TABLE IF NOT EXISTS rank_snapshots (
        snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
        rank_list_id INTEGER NOT NULL,
        snapshot_date DATE NOT NULL,
        item_count INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(rank_list_id) REFERENCES rank_lists(rank_list_id) ON DELETE CASCADE,
        UNIQUE(rank_list_id, snapshot_date)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_snapshots_date ON rank_snapshots(snapshot_date);",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_list_date ON rank_snapshots(rank_list_id, snapshot_date);",

    # =========================
    # Rank entries (platform-specific metric columns)
    #   - Qidian: total_recommend
    #   - Fanqie: reading_count
    # =========================
    """
    CREATE TABLE IF NOT EXISTS rank_entries (
        snapshot_id INTEGER NOT NULL,
        novel_uid INTEGER NOT NULL,
        rank INTEGER NOT NULL,

        total_recommend INTEGER DEFAULT NULL,      -- qidian
        reading_count INTEGER DEFAULT NULL,        -- fanqie
        extra_json TEXT NOT NULL DEFAULT '{}',

        PRIMARY KEY(snapshot_id, novel_uid),
        FOREIGN KEY(snapshot_id) REFERENCES rank_snapshots(snapshot_id) ON DELETE CASCADE,
        FOREIGN KEY(novel_uid) REFERENCES novels(novel_uid) ON DELETE CASCADE
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_rank_entries_novel_uid ON rank_entries(novel_uid);",
    "CREATE INDEX IF NOT EXISTS idx_rank_entries_rank ON rank_entries(snapshot_id, rank);",

    # =========================
    # First_N_chapters stored per novel (since no cross-platform duplication)
    # =========================
    """
    CREATE TABLE IF NOT EXISTS first_n_chapters (
        chapter_id INTEGER PRIMARY KEY AUTOINCREMENT,
        novel_uid INTEGER NOT NULL,
        chapter_num INTEGER NOT NULL,
        chapter_title TEXT NOT NULL,
        chapter_content TEXT NOT NULL DEFAULT '',
        chapter_url TEXT NOT NULL DEFAULT '',

        word_count INTEGER NOT NULL DEFAULT 0,
        content_hash TEXT NOT NULL DEFAULT '',
        publish_date DATE NOT NULL,

        FOREIGN KEY(novel_uid) REFERENCES novels(novel_uid) ON DELETE CASCADE,
        UNIQUE(novel_uid, chapter_num)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_first_n_chapters_novel ON first_n_chapters(novel_uid);",
    "CREATE INDEX IF NOT EXISTS idx_first_n_chapters_publish_date ON first_n_chapters(publish_date);",
    "CREATE INDEX IF NOT EXISTS idx_first_n_chapters_hash ON first_n_chapters(content_hash);",
]


def create_all(conn: sqlite3.Connection, *, drop: bool = False) -> None:
    cur = conn.cursor()

    # perf / safety pragmas
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA journal_mode = WAL;")
    cur.execute("PRAGMA synchronous = NORMAL;")
    cur.execute("PRAGMA temp_store = MEMORY;")
    cur.execute("PRAGMA cache_size = -20000;")  # ~20MB

    if drop:
        for t in [
            "first_n_chapters",
            "rank_entries",
            "rank_snapshots",
            "rank_lists",
            "novel_tag_map",
            "tags",
            "novel_titles",
            "novels",
        ]:
            cur.execute(f"DROP TABLE IF EXISTS {t};")

    for stmt in DDL:
        cur.execute(stmt)
