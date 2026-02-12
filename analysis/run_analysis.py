# analysis/run_analysis.py
from __future__ import annotations

import argparse
from analysis.trend_analyzer import TrendAnalyzer, AnalyzerArgs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="outputs/data/novels.db")
    ap.add_argument("--start_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end_date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--platform", default="both", choices=["qidian", "fanqie", "both"])
    ap.add_argument("--rank_family", default=None)
    ap.add_argument("--rank_sub_cat", default=None)
    ap.add_argument("--top_k", type=int, default=20)

    # 下面两个参数让你可以控制输出位置（可选）
    ap.add_argument("--report_dir", default="../outputs/reports", help="Directory to write final report md")
    ap.add_argument("--report_id", default=None, help="Optional report id; default is start_date_end_date")

    args = ap.parse_args()

    analyzer = TrendAnalyzer()

    # TrendAnalyzer.run() 现在返回 (md_text, report_path) 并且内部已经写入 report_path
    md, report_path = analyzer.run(AnalyzerArgs(
        db_path=args.db,
        start_date=args.start_date,
        end_date=args.end_date,
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
