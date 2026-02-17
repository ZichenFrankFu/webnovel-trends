from __future__ import annotations
import sqlite3
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query

from ..settings import settings
from ..utils import load_repo_config, get_db_path

router = APIRouter(prefix="/api/db", tags=["db"])

def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con

@router.get("/info")
def db_info():
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    return {"db_path": db_path}

@router.get("/tables")
def list_tables():
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    with _connect(db_path) as con:
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    return {"tables": [r["name"] for r in rows]}

@router.get("/table/{name}")
def read_table(name: str, limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0)):
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)

    # whitelist by existing tables
    with _connect(db_path) as con:
        exists = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="table not found")

        rows = con.execute(f"SELECT * FROM {name} LIMIT ? OFFSET ?", (limit, offset)).fetchall()
        data = [dict(r) for r in rows]
    return {"rows": data, "limit": limit, "offset": offset}

@router.get("/rank_lists")
def rank_lists(platform: str | None = None):
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    q = "SELECT * FROM rank_lists"
    params = []
    if platform:
        q += " WHERE platform=?"
        params.append(platform)
    q += " ORDER BY rank_list_id DESC"
    with _connect(db_path) as con:
        rows = con.execute(q, params).fetchall()
    return {"rows": [dict(r) for r in rows]}

@router.get("/snapshots")
def snapshots(rank_list_id: int):
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    with _connect(db_path) as con:
        rows = con.execute(
            "SELECT * FROM rank_snapshots WHERE rank_list_id=? ORDER BY snapshot_date DESC, snapshot_id DESC",
            (rank_list_id,),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}

@router.get("/entries")
def entries(snapshot_id: int, limit: int = Query(default=200, ge=1, le=2000)):
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    with _connect(db_path) as con:
        rows = con.execute(
            "SELECT * FROM rank_entries WHERE snapshot_id=? ORDER BY rank ASC LIMIT ?",
            (snapshot_id, limit),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}

@router.get("/novel/{novel_uid}")
def novel_detail(novel_uid: int):
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    with _connect(db_path) as con:
        n = con.execute("SELECT * FROM novels WHERE novel_uid=?", (novel_uid,)).fetchone()
        if not n:
            raise HTTPException(status_code=404, detail="novel not found")

        titles = con.execute("SELECT * FROM novel_titles WHERE novel_uid=? ORDER BY last_seen_date DESC", (novel_uid,)).fetchall()
        tags = con.execute(
            "SELECT t.* FROM tags t JOIN novel_tag_map m ON m.tag_id=t.tag_id WHERE m.novel_uid=? ORDER BY t.tag_name",
            (novel_uid,),
        ).fetchall()
        history = con.execute(
            "SELECT e.*, s.snapshot_date, l.rank_family, l.rank_sub_cat, l.platform "
            "FROM rank_entries e "
            "JOIN rank_snapshots s ON s.snapshot_id=e.snapshot_id "
            "JOIN rank_lists l ON l.rank_list_id=s.rank_list_id "
            "WHERE e.novel_uid=? "
            "ORDER BY s.snapshot_date DESC, e.rank ASC",
            (novel_uid,),
        ).fetchall()
        chapters = con.execute(
            "SELECT * FROM first_n_chapters WHERE novel_uid=? ORDER BY chapter_index ASC",
            (novel_uid,),
        ).fetchall()

    return {
        "novel": dict(n),
        "titles": [dict(r) for r in titles],
        "tags": [dict(r) for r in tags],
        "rank_history": [dict(r) for r in history],
        "chapters": [dict(r) for r in chapters],
    }

@router.get("/diagnostics/item_count_mismatch")
def diag_item_count_mismatch(limit: int = Query(default=100, ge=1, le=500)):
    repo_cfg = load_repo_config(settings.repo_root)
    db_path = get_db_path(repo_cfg, settings.repo_root)
    sql = """
    SELECT
      s.snapshot_id,
      s.rank_list_id,
      s.snapshot_date,
      l.platform,
      l.rank_family,
      l.rank_sub_cat,
      s.item_count AS item_count_snapshot,
      (
        SELECT COUNT(*)
        FROM rank_entries e
        WHERE e.snapshot_id = s.snapshot_id
      ) AS item_count_entries
    FROM rank_snapshots s
    JOIN rank_lists l ON l.rank_list_id = s.rank_list_id
    WHERE s.item_count IS NOT NULL
      AND s.item_count != (
        SELECT COUNT(*)
        FROM rank_entries e
        WHERE e.snapshot_id = s.snapshot_id
      )
    ORDER BY s.snapshot_date DESC, s.snapshot_id DESC
    LIMIT ?
    """
    with _connect(db_path) as con:
        rows = con.execute(sql, (limit,)).fetchall()
    return {"rows": [dict(r) for r in rows]}
