# analysis/run_analysis.py
from __future__ import annotations

import argparse
from analysis.trend_analyzer import TrendAnalyzer, AnalyzerArgs
from datetime import date, timedelta
import sqlite3
import pandas as pd

def _get_db_date_range(db_path: str) -> tuple[date, date]:
    conn = sqlite3.connect(db_path)
    q = """
    SELECT MIN(snapshot_date) AS min_d, MAX(snapshot_date) AS max_d
    FROM rank_snapshots
    """
    r = pd.read_sql_query(q, conn).iloc[0]
    conn.close()
    min_d = pd.to_datetime(r["min_d"]).date()
    max_d = pd.to_datetime(r["max_d"]).date()
    return min_d, max_d

def _compute_window(db_path: str, lookback: str, asof: str | None) -> tuple[str, str]:
    min_d, max_d = _get_db_date_range(db_path)
    end_d = date.fromisoformat(asof) if asof else max_d

    if lookback == "all":
        start_d = min_d
    else:
        days = {"week": 7, "month": 30, "quarter": 90, "year": 365}[lookback]
        start_d = end_d - timedelta(days=days)

    return start_d.isoformat(), end_d.isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="outputs/data/novels.db")
    ap.add_argument("--platform", default="both", choices=["qidian", "fanqie", "both"])
    ap.add_argument("--rank_family", default=None)
    ap.add_argument("--rank_sub_cat", default=None)
    ap.add_argument("--top_k", type=int, default=20)
    # 下面两个参数让你可以控制输出位置（可选）
    ap.add_argument("--report_dir", default="../outputs/reports", help="Directory to write final report md")
    ap.add_argument("--report_id", default=None, help="Optional report id; default is start_date_end_date")
    ap.add_argument(
        "--lookback",
        default="month",
        choices=["week", "month", "quarter", "year", "all"],
        help="How far to look back from the latest snapshot_date in DB."
    )
    ap.add_argument(
        "--asof",
        default=None,
        help="Optional YYYY-MM-DD. If not provided, use MAX(snapshot_date) in DB."
    )

    args = ap.parse_args()
    start_date, end_date = _compute_window(args.db, args.lookback, args.asof)
    analyzer = TrendAnalyzer()

    md, report_path = analyzer.run(AnalyzerArgs(
        db_path=args.db,
        start_date=start_date,
        end_date=end_date,
        platform=args.platform,
        rank_family=args.rank_family,
        rank_sub_cat=args.rank_sub_cat,
        top_k=args.top_k,
        report_dir=args.report_dir,
        report_id=args.report_id,
    ))

    # 不再手动 write_report，避免把 tuple/路径写乱
    print(f"[final report] {report_path}")


if __name__ == "__main__":
    main()
