# analysis/report.py
from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ReportConfig:
    top_k: int = 20


def _nan_to_dash(df: pd.DataFrame) -> pd.DataFrame:
    """Avoid 'nan' in markdown output; show '-' instead."""
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_numeric_dtype(out[c]):
            out[c] = out[c].map(lambda x: "-" if pd.isna(x) else x)
        else:
            out[c] = out[c].fillna("-")
    return out


def md_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "\n（无数据）\n"
    d = df.head(max_rows).copy()
    d = _nan_to_dash(d)
    return "\n" + d.to_markdown(index=False) + "\n"


def _explain_sampling() -> str:
    return (
        "本报告按数据库内已抓取的榜单快照进行统计，口径为：\n"
        "- 起点 heat_raw=total_recommend；番茄 heat_raw=reading_count\n"
        "- tag_u：起点=sub_cat（从 main_category 拆分），番茄=tag_name\n"
        "- cat_u：均使用 main_category\n"
        "\n"
    )


def _explain_concentration() -> str:
    return "集中度（concentration_index）使用 HHI：同一榜单同一天的各标签 share^2 之和。越大表示越集中。\n"


def _explain_cross_platform_diff() -> str:
    return "跨平台差异仅展示两个平台共有的分类（presence=both），否则对比没有意义。\n"


def _norm_subcat(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def build_cross_platform_diff_by_category(roll_cat: pd.DataFrame) -> pd.DataFrame:
    agg = roll_cat.groupby(["platform", "cat_u"], dropna=False).agg(
        mean_share=("mean_share", "mean"),
        avg_heat=("avg_heat", "mean"),
        mean_rank=("mean_rank", "mean"),
    ).reset_index()

    q = agg[agg["platform"] == "qidian"].rename(columns={
        "mean_share": "share_qidian", "avg_heat": "heat_qidian", "mean_rank": "rank_qidian"
    }).drop(columns=["platform"])
    f = agg[agg["platform"] == "fanqie"].rename(columns={
        "mean_share": "share_fanqie", "avg_heat": "heat_fanqie", "mean_rank": "rank_fanqie"
    }).drop(columns=["platform"])

    diff = q.merge(f, on="cat_u", how="outer")

    diff["presence"] = "both"
    diff.loc[diff["share_qidian"].isna() & diff["share_fanqie"].notna(), "presence"] = "fanqie_only"
    diff.loc[diff["share_fanqie"].isna() & diff["share_qidian"].notna(), "presence"] = "qidian_only"

    diff["share_qidian"] = diff["share_qidian"].fillna(0.0)
    diff["share_fanqie"] = diff["share_fanqie"].fillna(0.0)
    diff["share_diff"] = diff["share_qidian"] - diff["share_fanqie"]

    diff["heat_diff"] = np.where(diff["presence"] == "both", diff["heat_qidian"] - diff["heat_fanqie"], np.nan)
    diff["rank_diff"] = np.where(diff["presence"] == "both", diff["rank_qidian"] - diff["rank_fanqie"], np.nan)
    return diff


def _section_images(images: dict[str, dict[str, str]]) -> str:
    if not images:
        return ""
    out = ["## 图表\n"]
    for p in ["qidian", "fanqie"]:
        if p not in images:
            continue
        for k, rel in images[p].items():
            out.append(f"### {p} - {k}\n")
            out.append(f"![{p}-{k}]({rel})\n")
    return "\n".join(out)


def _section_platform_topk(roll: pd.DataFrame, platform: str, top_k: int) -> str:
    r = roll[roll["platform"] == platform].copy()
    if r.empty:
        return f"## {platform}\n\n（无数据）\n"

    # roll keyed by (platform, rank_family, rank_sub_cat, tag_u)
    # aggregate to one row per tag_u to avoid duplicates
    r = r.dropna(subset=["tag_u"]).copy()

    agg = r.groupby(["tag_u"], dropna=False).agg(
        mean_share=("mean_share", "mean"),
        avg_heat=("avg_heat", "mean"),
        mean_rank=("mean_rank", "mean"),
    ).reset_index()

    top_share = agg.sort_values("mean_share", ascending=False)[
        ["tag_u", "mean_share", "avg_heat", "mean_rank"]
    ].head(top_k)

    top_heat = agg.sort_values("avg_heat", ascending=False)[
        ["tag_u", "avg_heat", "mean_share", "mean_rank"]
    ].head(top_k)

    md = []
    md.append(f"## {platform}\n")
    md.append("### Top tags by mean_share\n")
    md.append(md_table(top_share, max_rows=top_k))
    md.append("### Top tags by avg_heat\n")
    md.append(md_table(top_heat, max_rows=top_k))
    return "\n".join(md)


def _qidian_rank_structure(weekly: pd.DataFrame, ranklist_avg_daily: pd.DataFrame | None = None) -> pd.DataFrame:
    w = weekly[weekly["platform"] == "qidian"].copy()
    if w.empty:
        return pd.DataFrame()

    w["rank_sub_cat_norm"] = _norm_subcat(w["rank_sub_cat"])
    w["rank_sub_cat_for_struct"] = np.where(
        w["rank_family"].astype(str).str.contains("新书"),
        w["rank_sub_cat_norm"],
        ""
    )

    gb = w.groupby(["rank_family", "rank_sub_cat_for_struct", "week"], dropna=False).agg(
        avg_heat=("avg_heat_raw", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    out = gb.groupby(["rank_family", "rank_sub_cat_for_struct"], dropna=False).agg(
        avg_heat=("avg_heat", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index().rename(columns={"rank_sub_cat_for_struct": "rank_sub_cat"})

    if ranklist_avg_daily is not None and not ranklist_avg_daily.empty:
        ra = ranklist_avg_daily[ranklist_avg_daily["platform"] == "qidian"].copy()
        ra["rank_sub_cat_norm"] = _norm_subcat(ra["rank_sub_cat"])
        ra["rank_sub_cat_key"] = np.where(
            ra["rank_family"].astype(str).str.contains("新书"),
            ra["rank_sub_cat_norm"],
            ""
        )
        ra = ra.drop(columns=["platform", "rank_sub_cat", "rank_sub_cat_norm"], errors="ignore")

        ra = ra.groupby(["rank_family", "rank_sub_cat_key"], dropna=False).agg(
            total_books=("total_books", "max"),
            avg_daily_books=("avg_daily_books", "mean"),
        ).reset_index()

        out = out.merge(
            ra[["rank_family", "rank_sub_cat_key", "avg_daily_books", "total_books"]],
            left_on=["rank_family", "rank_sub_cat"],
            right_on=["rank_family", "rank_sub_cat_key"],
            how="left",
        ).drop(columns=["rank_sub_cat_key"])
    else:
        out["avg_daily_books"] = np.nan
        out["total_books"] = np.nan

    out["rank_sub_cat"] = out["rank_sub_cat"].astype(str)
    out.loc[out["rank_sub_cat"].str.strip().eq(""), "rank_sub_cat"] = "-"

    out = out.sort_values(["rank_family", "avg_daily_books", "avg_heat"], ascending=[True, False, False])
    return out


def _fanqie_rank_structure(weekly: pd.DataFrame, ranklist_avg_daily: pd.DataFrame | None = None) -> pd.DataFrame:
    w = weekly[weekly["platform"] == "fanqie"].copy()
    if w.empty:
        return pd.DataFrame()

    w["rank_sub_cat_norm"] = _norm_subcat(w["rank_sub_cat"])

    gb = w.groupby(["rank_family", "rank_sub_cat_norm", "week"], dropna=False).agg(
        books=("books_in_group", "max"),
        avg_heat=("avg_heat_raw", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    out = gb.groupby(["rank_family", "rank_sub_cat_norm"], dropna=False).agg(
        mean_books=("books", "mean"),
        avg_heat=("avg_heat", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    if ranklist_avg_daily is not None and not ranklist_avg_daily.empty:
        ra = ranklist_avg_daily[ranklist_avg_daily["platform"] == "fanqie"].copy()
        ra["rank_sub_cat_norm"] = _norm_subcat(ra["rank_sub_cat"])
        ra = ra.drop(columns=["platform"], errors="ignore")

        ra = ra.groupby(["rank_family", "rank_sub_cat_norm"], dropna=False).agg(
            total_books=("total_books", "max"),
            avg_daily_books=("avg_daily_books", "mean"),
        ).reset_index()

        out = out.merge(
            ra[["rank_family", "rank_sub_cat_norm", "avg_daily_books", "total_books"]],
            on=["rank_family", "rank_sub_cat_norm"],
            how="left",
        )
    else:
        out["avg_daily_books"] = np.nan
        out["total_books"] = np.nan

    out = out.rename(columns={"rank_sub_cat_norm": "rank_sub_cat"})
    out = out.sort_values(["rank_family", "avg_daily_books", "avg_heat"], ascending=[True, False, False])
    return out


def build_final_report(
    *,
    start_date: str,
    end_date: str,
    weekly: pd.DataFrame,
    roll: pd.DataFrame,
    weekly_cat: pd.DataFrame | None = None,
    roll_cat: pd.DataFrame | None = None,
    new_entry_compact: pd.DataFrame | None = None,
    opening_opportunities: pd.DataFrame | None = None,
    pairs2: pd.DataFrame | None = None,
    triples3: pd.DataFrame | None = None,
    images: dict[str, dict[str, str]] | None = None,
    cfg: ReportConfig = ReportConfig(),
    coverage: pd.DataFrame | None = None,
    ranklist_avg_daily: pd.DataFrame | None = None,
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    md: list[str] = []
    md.append(f"# WebNovel Trend Report ({start_date} ~ {end_date})\n")
    md.append(f"_Generated: {now}_\n")
    md.append(_explain_sampling())

    # 数据覆盖：集中展示 min/max/days_span（这里只保留一次）
    md.append("## 数据覆盖\n")
    if coverage is not None and not coverage.empty:
        # 只显示核心列（避免冗长）
        prefer = ["platform", "min_date", "max_date", "days_span", "unique_snapshots", "unique_novels_in_window"]
        cols = [c for c in prefer if c in coverage.columns]
        md.append(md_table(coverage[cols], max_rows=10))
    else:
        md.append("（无数据）\n")

    # Qidian
    md.append(_section_platform_topk(roll, "qidian", cfg.top_k))
    md.append("## 起点抓取数据总览\n")
    md.append(md_table(_qidian_rank_structure(weekly, ranklist_avg_daily), max_rows=30))

    # Fanqie
    md.append(_section_platform_topk(roll, "fanqie", cfg.top_k))
    md.append("## 番茄抓取数据总览\n")
    md.append(md_table(_fanqie_rank_structure(weekly, ranklist_avg_daily), max_rows=30))
    md.append(_explain_concentration())

    # Co-occurrence
    md.append("## Tag 共现（pairs）\n")
    md.append(md_table(pairs2, max_rows=30))
    md.append("## Tag 共现（triples）\n")
    md.append(md_table(triples3, max_rows=30))

    # Cross-platform diff: category only
    if roll_cat is not None and not roll_cat.empty:
        md.append("## 跨平台题材差异（仅 cat_u）\n")
        md.append(_explain_cross_platform_diff())
        diffc = build_cross_platform_diff_by_category(roll_cat)
        diffc = diffc[diffc["presence"] == "both"].copy()
        show = diffc.sort_values("share_diff", ascending=False)[
            ["cat_u", "share_qidian", "share_fanqie", "share_diff", "heat_diff", "rank_diff"]
        ]
        md.append(md_table(show, max_rows=30))
    else:
        md.append("## 跨平台题材差异（仅 cat_u）\n（无数据）\n")

    # Opening opportunities: remove days_span/min/max
    md.append("## 新书热点\n")
    md.append("### 开书机会榜（platform + main_category + tag_u）\n")
    if opening_opportunities is None or opening_opportunities.empty:
        md.append("（无数据）\n")
    else:
        show_cols = [
            "platform", "cat_u", "tag_u",
            "share_start", "share_end", "share_delta",
            "heat_start", "heat_end", "heat_delta",
            "new_entry_ratio", "total_books",
            "opportunity_score",
        ]
        cols = [c for c in show_cols if c in opening_opportunities.columns]
        md.append(md_table(opening_opportunities[cols], max_rows=80))

    md.append("### 新入榜占比概览（platform + tag_u）\n")
    if new_entry_compact is None or new_entry_compact.empty:
        md.append("（无数据）\n")
    else:
        md.append(md_table(new_entry_compact, max_rows=30))

    # Images
    img_sec = _section_images(images or {})
    if img_sec:
        md.append(img_sec)

    return "\n".join(md)
