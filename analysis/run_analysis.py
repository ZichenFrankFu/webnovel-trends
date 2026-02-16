from __future__ import annotations

import argparse
import sqlite3
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from analysis.trend_analyzer import TrendAnalyzer, AnalyzerArgs


def _get_db_date_range(db_path: str) -> tuple[date, date]:
    conn = sqlite3.connect(db_path)
    q = """
    SELECT MIN(snapshot_date) AS min_d, MAX(snapshot_date) AS max_d
    FROM rank_snapshots
    """
    r = pd.read_sql_query(q, conn).iloc[0]
    conn.close()

    if pd.isna(r["min_d"]) or pd.isna(r["max_d"]):
        raise RuntimeError("No snapshot data found in DB (rank_snapshots is empty).")

    min_d = pd.to_datetime(r["min_d"]).date()
    max_d = pd.to_datetime(r["max_d"]).date()
    return min_d, max_d


def _today_ny() -> date:
    # Use America/New_York as the project's canonical timezone for date boundaries
    return datetime.now(ZoneInfo("America/New_York")).date()


def _compute_window(db_path: str, lookback: str) -> tuple[str, str]:
    """Compute (start_date, end_date) from --lookback, with end_date fixed to 'today' (NY time)."""
    min_d, _ = _get_db_date_range(db_path)
    end_d = _today_ny()  # ✅ end date is always today

    if lookback == "all":
        start_d = min_d
    else:
        days = {"week": 7, "month": 30, "quarter": 90, "year": 365}[lookback]
        start_d = end_d - timedelta(days=days)

        # Optional clamp so we don't request before DB coverage
        if start_d < min_d:
            start_d = min_d

    return start_d.isoformat(), end_d.isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="outputs/data/novels.db")
    ap.add_argument("--platform", default="both", choices=["qidian", "fanqie", "both"])
    ap.add_argument("--top_k", type=int, default=20)

    # output location
    ap.add_argument("--report_dir", default="../outputs/reports", help="Directory to write final report md")
    ap.add_argument("--report_id", default=None, help="Optional report id; default is start_date_end_date")

    # time window
    ap.add_argument(
        "--lookback",
        default="all",
        choices=["week", "month", "quarter", "year", "all"],
        help="How far to look back from TODAY (America/New_York). End date is always today."
    )

    args = ap.parse_args()

    start_date, end_date = _compute_window(args.db, args.lookback)

    analyzer = TrendAnalyzer()
    md, report_path = analyzer.run(AnalyzerArgs(
        db_path=args.db,
        start_date=start_date,
        end_date=end_date,
        platform=args.platform,
        rank_family= None,
        rank_sub_cat= None,
        top_k=args.top_k,
        report_dir=args.report_dir,
        report_id=args.report_id,
    ))

    print(f"[window] {start_date} ~ {end_date} (end_date=today, America/New_York)")
    print(f"[final report] {report_path}")


if __name__ == "__main__":
    main()
