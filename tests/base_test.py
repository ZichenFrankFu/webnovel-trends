"""
Common test utilities for webnovel_trends spider test suites.

Goal:
- Keep qidian_test.py and fanqie_test.py consistent in CLI UX and logging.
- Provide lightweight helpers only (no spider/site specific logic).

Usage (in tests/*.py):
1) Insert project root to sys.path (already done in both tests).
2) `from base_test import ...`
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# Project root is the directory containing this file (config.py should also live here).
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------
# Timing
# ------------------------------------------------------------------

@dataclass
class Timer:
    name: str
    start: float = 0.0
    elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.elapsed = time.perf_counter() - self.start


def fmt_sec(sec: float) -> str:
    return f"{sec:.2f}s"


# ------------------------------------------------------------------
# Printing helpers
# ------------------------------------------------------------------

def print_header(title: str, subtitle: str = "", params: Optional[Dict[str, Any]] = None) -> None:
    print("\n" + "=" * 80)
    print(title)
    if subtitle:
        print(subtitle)
    print("=" * 80)
    if params:
        parts = [f"{k}={v}" for k, v in params.items()]
        print("[参数] " + " | ".join(parts))


def print_hr(char: str = "-", width: int = 80) -> None:
    print(char * width)


def safe_trunc(s: Any, n: int = 120) -> str:
    if s is None:
        return ""
    txt = str(s)
    return txt[:n] + ("..." if len(txt) > n else "")


# ------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------

def ensure_clean_dirs(*, db_relpath: str, remove_db: bool = True) -> str:
    """
    Ensure test_output/ and test_output/debug exist.
    Optionally remove the db file.
    Return absolute db path.
    """
    db_path = os.path.join(PROJECT_ROOT, db_relpath)
    os.makedirs(os.path.join(PROJECT_ROOT, "test_output"), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, "test_output", "debug"), exist_ok=True)

    if remove_db and os.path.exists(db_path):
        try:
            os.remove(db_path)
            print(f"[cleanup] removed: {db_path}")
        except Exception as e:
            print(f"[cleanup] failed to remove {db_path}: {e}")

    return db_path


def init_db(db_path: str, *, is_test: bool = True):
    """
    Initialize DatabaseHandler. Compatible with both old/new signatures.
    """
    from database.db_handler import DatabaseHandler

    try:
        return DatabaseHandler(db_path, is_test=is_test)
    except TypeError:
        # Older signature: DatabaseHandler(db_path=...)
        return DatabaseHandler(db_path=db_path)


def print_db_counts(db: Any) -> None:
    """
    Pretty-print database table counts if db supports get_table_counts().
    """
    if not hasattr(db, "get_table_counts"):
        print("[db] get_table_counts() not available; skip counts.")
        return

    try:
        counts = db.get_table_counts()
    except Exception as e:
        print(f"[db] failed to read counts: {e}")
        return

    print("\n------------------------------------------------------------")
    print("数据库统计")
    print("------------------------------------------------------------")
    mapping = [
        ("novels", "小说基本信息"),
        ("novel_titles", "小说标题记录"),
        ("tags", "标签信息"),
        ("novel_tag_map", "小说标签映射"),
        ("rank_lists", "榜单列表"),
        ("rank_snapshots", "榜单快照"),
        ("rank_entries", "榜单条目"),
        ("first_n_chapters", "前N章内容"),
    ]
    for k, label in mapping:
        print(f"   {label:<14}: {int(counts.get(k, 0)):>4} 条记录")

def db_get_chapter_count(db: Any, *, platform: str, platform_novel_id: str) -> int:
    if not db:
        return 0
    if hasattr(db, "get_first_n_chapter_count"):
        try:
            return int(db.get_first_n_chapter_count(platform=platform, platform_novel_id=platform_novel_id) or 0)
        except Exception:
            return 0
    return 0


def db_get_max_chapter_index(db: Any, *, platform: str, platform_novel_id: str) -> int:
    if not db:
        return 0
    if hasattr(db, "get_first_n_chapter_max_num"):
        try:
            return int(db.get_first_n_chapter_max_num(platform=platform, platform_novel_id=platform_novel_id) or 0)
        except Exception:
            return 0
    return 0



# ------------------------------------------------------------------
# Misc
# ------------------------------------------------------------------

def pick_first(items: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return items[0] if items else None


def safe_pid(book: Dict[str, Any]) -> str:
    return (book.get("platform_novel_id") or book.get("novel_id") or book.get("pid") or "").strip()
