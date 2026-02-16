# analysis/trend_analyzer.py
from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd

from analysis.data_access import connect_sqlite, load_rank_long_df
from analysis.heat import HeatConfig, add_heat
from analysis.metrics import (
    MetricConfig,
    add_unified_columns,
    compute_weekly_tag_panel,
    compute_timewindow_rollup,
    compute_weekly_category_panel,
    compute_timewindow_category_rollup as compute_timewindow_rollup_category,
    compute_new_entry_ratio_compact,
    compute_opening_opportunities,
    compute_cooccurrence_pairs,
    compute_cooccurrence_triples,
)
from analysis.report import ReportConfig, build_final_report
from analysis.visualization import save_bar_topk, save_line_top_tags


@dataclass(frozen=True)
class AnalyzerArgs:
    db_path: str
    start_date: str
    end_date: str
    platform: str = "both"
    rank_family: str | None = None
    rank_sub_cat: str | None = None
    top_k: int = 20
    report_dir: str = "outputs/reports"
    report_id: str | None = None


class TrendAnalyzer:
    def __init__(self, heat_cfg: HeatConfig | None = None, metric_cfg: MetricConfig | None = None):
        self.heat_cfg = heat_cfg or HeatConfig(alpha=0.7, tanh_c=3.0)
        self.metric_cfg = metric_cfg or MetricConfig(
            top_n_for_top_appearance=10,
            entry_top_n=30,
            top_k_tags=20,
            top_k_pairs=30,
            top_k_triples=30,
        )

    def _make_assets(
        self,
        *,
        weekly,
        roll,
        report_assets_dir: str,
        topk: int,
    ) -> dict[str, dict[str, str]]:
        """Generate visualization images. Returns abs paths."""
        images: dict[str, dict[str, str]] = {"fanqie": {}, "qidian": {}}

        for p in ["fanqie", "qidian"]:
            r = roll[roll["platform"] == p].copy()
            w = weekly[weekly["platform"] == p].copy()
            if r.empty:
                continue

            path_share = os.path.join(report_assets_dir, f"{p}_share_top10.png")
            save_bar_topk(
                r,
                label_col="tag_u",
                value_col="mean_share",
                title=f"{p} - Top10 tags by mean_share",
                out_path=path_share,
                topk=min(10, topk),
            )
            images[p]["share_top10"] = os.path.abspath(path_share)

            path_heat = os.path.join(report_assets_dir, f"{p}_heat_top10.png")
            save_bar_topk(
                r,
                label_col="tag_u",
                value_col="avg_heat",
                title=f"{p} - Top10 tags by avg_heat",
                out_path=path_heat,
                topk=min(10, topk),
            )
            images[p]["heat_top10"] = os.path.abspath(path_heat)

            if not w.empty and "concentration_index" in w.columns:
                ww = w.groupby(["week"], dropna=False)["concentration_index"].mean().reset_index()
                path_conc = os.path.join(report_assets_dir, f"{p}_concentration_daily.png")
                save_line_top_tags(
                    ww.assign(dummy="concentration"),
                    x_col="week",
                    y_col="concentration_index",
                    tag_col="dummy",
                    title=f"{p} - concentration_index over days",
                    out_path=path_conc,
                    topk=1,
                )
                images[p]["concentration_daily"] = os.path.abspath(path_conc)

        return images

    def run(self, args: AnalyzerArgs):
        os.makedirs(args.report_dir, exist_ok=True)
        report_id = args.report_id or f"{args.start_date}_{args.end_date}"
        out_dir = os.path.join(args.report_dir, report_id)
        os.makedirs(out_dir, exist_ok=True)
        assets_dir = os.path.join(out_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)

        conn = connect_sqlite(args.db_path)
        try:
            df = load_rank_long_df(
                conn,
                start_date=args.start_date,
                end_date=args.end_date,
                platform=args.platform,
                rank_family=args.rank_family,
                rank_sub_cat=args.rank_sub_cat,
            )

            if df is None or df.empty:
                md = build_final_report(
                    start_date=args.start_date,
                    end_date=args.end_date,
                    weekly=pd.DataFrame(),
                    roll=pd.DataFrame(),
                    weekly_cat=pd.DataFrame(),
                    roll_cat=pd.DataFrame(),
                    new_entry_compact=pd.DataFrame(),
                    opening_opportunities=pd.DataFrame(),
                    pairs2=pd.DataFrame(),
                    triples3=pd.DataFrame(),
                    images={},
                    cfg=ReportConfig(top_k=args.top_k),
                    coverage=pd.DataFrame(),
                    ranklist_avg_daily=pd.DataFrame(),
                )
                report_path = os.path.join(out_dir, "final_report.md")
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(md)
                return md, report_path

            # heat + unified columns
            df = add_heat(df, self.heat_cfg)
            df = add_unified_columns(df)

            weekly = compute_weekly_tag_panel(df, self.metric_cfg)
            roll = compute_timewindow_rollup(weekly, self.metric_cfg)

            weekly_cat = compute_weekly_category_panel(df, self.metric_cfg)
            roll_cat = compute_timewindow_rollup_category(weekly_cat, self.metric_cfg)

            new_entry_compact = compute_new_entry_ratio_compact(df, args.start_date, args.end_date)
            opening_opportunities = compute_opening_opportunities(df, args.start_date, args.end_date)

            pairs2 = compute_cooccurrence_pairs(df, self.metric_cfg)
            triples3 = compute_cooccurrence_triples(df, self.metric_cfg)

            # ----------------------------
            # 数据覆盖（按平台：min/max/days_span/unique novels/unique snapshots）
            # ----------------------------
            d_cov = df.dropna(subset=["platform", "snapshot_date", "novel_uid"]).copy()
            d_cov["snapshot_date"] = pd.to_datetime(d_cov["snapshot_date"], errors="coerce").dt.date

            cov_books = (
                d_cov.drop_duplicates(["platform", "novel_uid"])
                .groupby("platform", dropna=False)["novel_uid"]
                .nunique()
                .reset_index(name="unique_novels_in_window")
            )
            cov_days = (
                d_cov.drop_duplicates(["platform", "snapshot_date"])
                .groupby("platform", dropna=False)["snapshot_date"]
                .agg(min_date="min", max_date="max", unique_snapshots="nunique")
                .reset_index()
            )
            cov_days["days_span"] = (
                (pd.to_datetime(cov_days["max_date"]) - pd.to_datetime(cov_days["min_date"]))
                .dt.days + 1
            )
            coverage = cov_days.merge(cov_books, on="platform", how="left")

            # ----------------------------
            # Rank-list coverage stats (dedup by novel_uid)
            # ----------------------------
            d0 = df.dropna(subset=["platform", "rank_family", "snapshot_date", "novel_uid"]).copy()
            d0["snapshot_date"] = pd.to_datetime(d0["snapshot_date"], errors="coerce").dt.date
            d0["rank_sub_cat"] = d0["rank_sub_cat"].fillna("").astype(str).str.strip()

            daily_rank_books = (
                d0.drop_duplicates(["platform", "rank_family", "rank_sub_cat", "snapshot_date", "novel_uid"])
                .groupby(["platform", "rank_family", "rank_sub_cat", "snapshot_date"], dropna=False)
                .agg(daily_books=("novel_uid", "nunique"))
                .reset_index()
            )

            ranklist_avg_daily = (
                daily_rank_books
                .groupby(["platform", "rank_family", "rank_sub_cat"], dropna=False)
                .agg(
                    avg_daily_books=("daily_books", "mean"),
                    min_date=("snapshot_date", "min"),
                    max_date=("snapshot_date", "max"),
                    days_seen=("snapshot_date", "nunique"),
                )
                .reset_index()
            )
            ranklist_avg_daily["days_span"] = (
                (pd.to_datetime(ranklist_avg_daily["max_date"]) - pd.to_datetime(ranklist_avg_daily["min_date"]))
                .dt.days + 1
            )

            ranklist_total_books = (
                d0.drop_duplicates(["platform", "rank_family", "rank_sub_cat", "novel_uid"])
                .groupby(["platform", "rank_family", "rank_sub_cat"], dropna=False)
                .agg(total_books=("novel_uid", "nunique"))
                .reset_index()
            )
            ranklist_avg_daily = ranklist_avg_daily.merge(
                ranklist_total_books,
                on=["platform", "rank_family", "rank_sub_cat"],
                how="left",
            )

            images = self._make_assets(
                weekly=weekly,
                roll=roll,
                report_assets_dir=assets_dir,
                topk=args.top_k,
            )
            for p in images:
                for k in list(images[p].keys()):
                    images[p][k] = os.path.relpath(images[p][k], out_dir)

            md = build_final_report(
                start_date=args.start_date,
                end_date=args.end_date,
                weekly=weekly,
                roll=roll,
                weekly_cat=weekly_cat,
                roll_cat=roll_cat,
                new_entry_compact=new_entry_compact,
                opening_opportunities=opening_opportunities,
                pairs2=pairs2,
                triples3=triples3,
                images=images,
                cfg=ReportConfig(top_k=args.top_k),
                coverage=coverage,
                ranklist_avg_daily=ranklist_avg_daily,
            )

            report_path = os.path.join(out_dir, "final_report.md")
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(md)

            return md, report_path
        finally:
            conn.close()
