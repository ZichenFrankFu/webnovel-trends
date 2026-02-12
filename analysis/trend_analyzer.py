# analysis/trend_analyzer.py
from __future__ import annotations

import os
from dataclasses import dataclass

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

    def _make_assets(self, *, weekly, roll, report_assets_dir: str, topk: int) -> dict[str, dict[str, str]]:
        """
        生成可视化图片，返回相对路径（相对于 report md 的目录）
        images[platform][key] = relative_path
        """
        images: dict[str, dict[str, str]] = {"fanqie": {}, "qidian": {}}

        # 平台拆开画图
        for p in ["fanqie", "qidian"]:
            r = roll[roll["platform"] == p].copy()
            w = weekly[weekly["platform"] == p].copy()

            if r.empty:
                continue

            # 1) Market share Top10 bar
            path_share = os.path.join(report_assets_dir, f"{p}_share_top10.png")
            save_bar_topk(
                r,
                label_col="tag_u",
                value_col="mean_share",
                title=f"{p} - Top10 tags by mean_share",
                out_path=path_share,
                topk=min(10, topk),
            )
            images[p]["share_top10"] = os.path.relpath(path_share, start=os.path.dirname(report_assets_dir))

            # 2) Heat Top10 bar
            path_heat = os.path.join(report_assets_dir, f"{p}_heat_top10.png")
            save_bar_topk(
                r,
                label_col="tag_u",
                value_col="avg_heat",
                title=f"{p} - Top10 tags by avg_heat",
                out_path=path_heat,
                topk=min(10, topk),
            )
            images[p]["heat_top10"] = os.path.relpath(path_heat, start=os.path.dirname(report_assets_dir))

            # 3) concentration_index line (按周)
            if not w.empty and "concentration_index" in w.columns:
                # 先在榜单层聚合到周
                ww = w.groupby(["week"])["concentration_index"].mean().reset_index()
                path_conc = os.path.join(report_assets_dir, f"{p}_concentration_weekly.png")
                save_line_top_tags(
                    ww.assign(dummy="concentration"),
                    x_col="week",
                    y_col="concentration_index",
                    tag_col="dummy",
                    title=f"{p} - concentration_index over weeks",
                    out_path=path_conc,
                    topk=1,
                )
                images[p]["concentration_weekly"] = os.path.relpath(path_conc, start=os.path.dirname(report_assets_dir))

            # 4) Chance tags heat/share trend（取 roll 里 chance_score 前5的 tag）
            if not w.empty:
                r2 = r.copy()
                r2["chance_score"] = (-r2["rank_slope"].fillna(0)) + r2["heat_slope"].fillna(0) + r2["share_slope"].fillna(0)
                top_tags = r2.sort_values("chance_score", ascending=False)["tag_u"].head(5).tolist()
                wt = w[w["tag_u"].isin(top_tags)].copy()
                if not wt.empty:
                    path_heat_tr = os.path.join(report_assets_dir, f"{p}_chance_heat_trend.png")
                    save_line_top_tags(
                        wt,
                        x_col="week",
                        y_col="avg_heat_raw",
                        tag_col="tag_u",
                        title=f"{p} - chance tags avg_heat trend",
                        out_path=path_heat_tr,
                        topk=min(5, len(top_tags)),
                    )
                    images[p]["chance_heat_trend"] = os.path.relpath(path_heat_tr, start=os.path.dirname(report_assets_dir))

                    path_share_tr = os.path.join(report_assets_dir, f"{p}_chance_share_trend.png")
                    save_line_top_tags(
                        wt,
                        x_col="week",
                        y_col="tag_share",
                        tag_col="tag_u",
                        title=f"{p} - chance tags share trend",
                        out_path=path_share_tr,
                        topk=min(5, len(top_tags)),
                    )
                    images[p]["chance_share_trend"] = os.path.relpath(path_share_tr, start=os.path.dirname(report_assets_dir))

        return images

    def run(self, args: AnalyzerArgs) -> tuple[str, str]:
        """
        返回 (md_text, report_path)
        """
        conn = connect_sqlite(args.db_path)

        df = load_rank_long_df(
            conn,
            start_date=args.start_date,
            end_date=args.end_date,
            platform=args.platform,
            rank_family=args.rank_family,
            rank_sub_cat=args.rank_sub_cat,
        )

        # heat + unify tag/week
        df = add_heat(df, self.heat_cfg)
        df = add_unified_columns(df)

        # weekly panel + rollup (tags)
        weekly = compute_weekly_tag_panel(df, self.metric_cfg)
        roll = compute_timewindow_rollup(weekly, self.metric_cfg)

        # weekly panel + rollup (categories; cross-platform comparable)
        weekly_cat = compute_weekly_category_panel(df, self.metric_cfg)
        roll_cat = compute_timewindow_rollup_category(weekly_cat, self.metric_cfg)


        # extra blocks
        new_entry_compact = compute_new_entry_ratio_compact(df, args.start_date, args.end_date)
        pairs2 = compute_cooccurrence_pairs(df, self.metric_cfg)
        triples3 = compute_cooccurrence_triples(df, self.metric_cfg)

        # report paths
        report_id = args.report_id or f"{args.start_date}_{args.end_date}"
        report_dir = args.report_dir
        os.makedirs(report_dir, exist_ok=True)

        report_path = os.path.join(report_dir, f"final_report_{report_id}.md")

        # assets dir: outputs/reports/assets/<report_id>/
        assets_dir = os.path.join(report_dir, "assets", report_id)
        os.makedirs(assets_dir, exist_ok=True)

        # generate images (relative paths for md)
        # 注意：report_path 在 report_dir 下，assets_dir 在 report_dir/assets/<id>
        # 我们在 report.py 里直接用 rel path 写入 md
        images = {"fanqie": {}, "qidian": {}}
        try:
            images = self._make_assets(
                weekly=weekly,
                roll=roll,
                report_assets_dir=assets_dir,
                topk=args.top_k,
            )
            # 修正 relpath 计算：以上函数用 dirname(assets_dir) 作为 base，
            # 这里统一改成相对于 report_dir（md 所在目录）更稳妥
            # => 重新计算一遍
            for p in ["fanqie", "qidian"]:
                fixed = {}
                for k, abs_or_rel in images[p].items():
                    # abs_or_rel 可能已是相对路径，转成绝对后再相对
                    ap = abs_or_rel
                    if not os.path.isabs(ap):
                        ap = os.path.join(os.path.dirname(assets_dir), abs_or_rel)
                    fixed[k] = os.path.relpath(ap, start=report_dir)
                images[p] = fixed
        except Exception:
            # 可视化失败不阻断报告生成
            images = None

        md = build_final_report(
            start_date=args.start_date,
            end_date=args.end_date,
            weekly=weekly,
            roll=roll,
            weekly_cat=weekly_cat,
            roll_cat=roll_cat,
            new_entry_compact=new_entry_compact,
            pairs2=pairs2,
            triples3=triples3,
            images=images,
            cfg=ReportConfig(top_k=args.top_k),
        )

        # write md
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(md)

        return md, report_path
