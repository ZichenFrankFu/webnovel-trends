# analysis/data_access.py
from __future__ import annotations

import sqlite3
import pandas as pd
from typing import Optional

def connect_sqlite(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)

def load_rank_long_df(
    conn: sqlite3.Connection,
    *,
    start_date: str,
    end_date: str,
    platform: str = "both",               # qidian / fanqie / both
    rank_family: Optional[str] = None,    # e.g. 月票榜 / 阅读榜
    rank_sub_cat: Optional[str] = None,   # e.g. 科幻末世
) -> pd.DataFrame:
    """
    输出“长表”：每行=某天某榜某书（可带 tag，多 tag 会展开为多行）

    必含列（后续指标都用这些）：
      snapshot_date, platform, rank_family, rank_sub_cat, rank_list_id, snapshot_id,
      novel_uid, rank, total_recommend, reading_count,
      main_category, status, total_words, created_date, last_seen_date,
      tag_name（可空）
    """
    where = ["rs.snapshot_date >= :start_date", "rs.snapshot_date <= :end_date"]
    params = {"start_date": start_date, "end_date": end_date}

    if platform != "both":
        where.append("rl.platform = :platform")
        params["platform"] = platform
    if rank_family:
        where.append("rl.rank_family = :rank_family")
        params["rank_family"] = rank_family
    if rank_sub_cat:
        where.append("rl.rank_sub_cat = :rank_sub_cat")
        params["rank_sub_cat"] = rank_sub_cat

    where_sql = " AND ".join(where)

    sql = f"""
    SELECT
      rs.snapshot_date                    AS snapshot_date,
      rl.platform                         AS platform,
      rl.rank_family                      AS rank_family,
      rl.rank_sub_cat                     AS rank_sub_cat,
      rl.rank_list_id                     AS rank_list_id,
      rs.snapshot_id                      AS snapshot_id,

      re.novel_uid                        AS novel_uid,
      re.rank                             AS rank,
      re.total_recommend                  AS total_recommend,
      re.reading_count                    AS reading_count,

      n.main_category                     AS main_category,
      n.status                            AS status,
      n.total_words                       AS total_words,
      n.created_date                      AS created_date,
      n.last_seen_date                    AS last_seen_date,

      t.tag_name                          AS tag_name
    FROM rank_entries re
    JOIN rank_snapshots rs ON rs.snapshot_id = re.snapshot_id
    JOIN rank_lists rl     ON rl.rank_list_id = rs.rank_list_id
    JOIN novels n          ON n.novel_uid = re.novel_uid
    LEFT JOIN novel_tag_map ntm ON ntm.novel_uid = n.novel_uid
    LEFT JOIN tags t           ON t.tag_id = ntm.tag_id
    WHERE {where_sql}
    """

    df = pd.read_sql_query(sql, conn, params=params)

    # 类型清洗
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["total_recommend"] = pd.to_numeric(df["total_recommend"], errors="coerce")
    df["reading_count"] = pd.to_numeric(df["reading_count"], errors="coerce")
    return df
