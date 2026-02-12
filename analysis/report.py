# analysis/report.py
from __future__ import annotations

import os
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
    # Avoid duplicated rows in report tables (usually caused by upstream joins/rollups)
    # Only remove fully identical rows; keep distinct records.
    dd = df.drop_duplicates().reset_index(drop=True)
    return _nan_to_dash(dd.head(max_rows)).to_markdown(index=False)


def _safe_subcat(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)) or str(x).strip() == "":
        return "（未分组/ALL）"
    return str(x)


def _norm_subcat(series: pd.Series) -> pd.Series:
    """Normalize rank_sub_cat so empty-string and NaN collapse into the same label."""
    s = series.fillna("").astype(str).map(lambda x: x.strip())
    return s.replace({"": "（未分组/ALL）"})

def build_cross_platform_diff_by_tag(roll: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-platform diff for TAG view (tag_u).
    - share missing -> 0 (not present on that platform)
    - heat/rank diff only for presence=both
    """
    agg = roll.groupby(["platform", "tag_u"], dropna=False).agg(
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

    diff = q.merge(f, on="tag_u", how="outer")

    diff["presence"] = "both"
    diff.loc[diff["share_qidian"].isna() & diff["share_fanqie"].notna(), "presence"] = "fanqie_only"
    diff.loc[diff["share_fanqie"].isna() & diff["share_qidian"].notna(), "presence"] = "qidian_only"

    diff["share_qidian"] = diff["share_qidian"].fillna(0.0)
    diff["share_fanqie"] = diff["share_fanqie"].fillna(0.0)
    diff["share_diff"] = diff["share_qidian"] - diff["share_fanqie"]

    diff["heat_diff"] = np.where(
        diff["presence"] == "both",
        diff["heat_qidian"] - diff["heat_fanqie"],
        np.nan
    )
    diff["rank_diff"] = np.where(
        diff["presence"] == "both",
        diff["rank_qidian"] - diff["rank_fanqie"],
        np.nan
    )
    return diff


def build_cross_platform_diff_by_category(roll_cat: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-platform diff for CATEGORY view (cat_u) — comparable across platforms.
    Same behavior as tag diff.
    """
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

    diff["heat_diff"] = np.where(
        diff["presence"] == "both",
        diff["heat_qidian"] - diff["heat_fanqie"],
        np.nan
    )
    diff["rank_diff"] = np.where(
        diff["presence"] == "both",
        diff["rank_qidian"] - diff["rank_fanqie"],
        np.nan
    )
    return diff


def _explain_sampling() -> str:
    return (
        "### 抽样机制说明（非常重要）\n\n"
        "- **起点**：混合榜/全站竞争（榜单样本更接近“市场主赛道竞争结果”）。\n"
        "- **番茄**：分类榜/子类竞争（榜单样本更接近条件分布 `P(书 | 子分类榜)`）。\n\n"
        "因此：\n"
        "- 起点可以直接统计 **分类占比** 来描述“市场竞争结构”；\n"
        "- 番茄不应把不同子分类榜直接混在一起当“市场份额”，应优先做**分类榜结构**与**分类内 top tags**。\n"
    )

def _explain_cross_platform_diff() -> str:
    return (
        "### 如何解读“跨平台差异”\n"
        "- **presence**：该分类/标签在窗口内出现的平台范围：`both`/`qidian_only`/`fanqie_only`。\n"
        "- **share_qidian / share_fanqie**：窗口内平均占比（在各自平台内归一化后的份额），用于衡量“在平台内部有多常见”。\n"
        "- **share_diff = share_qidian - share_fanqie**：为正表示该分类/标签在起点相对更强，为负表示在番茄相对更强。\n"
        "- **heat_diff / rank_diff**：仅在 `presence=both` 时有意义。\n"
        "  - **heat_diff = heat_qidian - heat_fanqie**：为正表示起点侧平均热度更高。\n"
        "  - **rank_diff = rank_qidian - rank_fanqie**：注意 rank 越小越靠前；因此 **rank_diff 为负**通常表示起点侧排名更好。\n"
        "\n"
        "> 说明：起点榜单是混合竞争样本；番茄榜单是分类榜条件样本。\n"
        "> 因此跨平台对比优先使用 **cat_u（main_category）** 这一可比口径；`tag_u` 仅作为补充参考。\n"
    )


def _explain_concentration() -> str:
    return (
        "### 指标解释：concentration_index（集中度）\n\n"
        "本报告的 `concentration_index` 采用与 HHI 类似的定义：\n"
        "- 在同一周、同一榜单分组（rank_family + rank_sub_cat）内，先计算每个 tag（或分类）的占比 `s_i`；\n"
        "- 再计算 `Σ s_i^2`。\n\n"
        "含义：\n"
        "- **越接近 1**：越集中（少数 tag/分类占据大部分上榜位，同质化更强）；\n"
        "- **越接近 0**：越分散（题材更丰富，长尾更明显）。\n"
    )


def _section_platform_topk(roll: pd.DataFrame, platform: str, top_k: int) -> str:
    r = roll[roll["platform"] == platform].copy()
    if r.empty:
        return f"## {platform}\n\n（无数据）\n"

    # IMPORTANT:
    # roll is keyed by (platform, rank_family, rank_sub_cat, tag_u) in analyzer.
    # For mixed-rank platforms (qidian), the same tag_u can appear multiple times across rank lists.
    # We aggregate to one row per tag_u to avoid duplicated rows in Top tables.
    r = r.dropna(subset=["tag_u"]).copy()

    # collapse rank_family/rank_sub_cat so each tag_u appears once
    agg = r.groupby(["tag_u"], dropna=False).agg(
        mean_share=("mean_share", "mean"),
        avg_heat=("avg_heat", "mean"),
        mean_rank=("mean_rank", "mean"),
        weeks=("weeks", "max"),
    ).reset_index()

    # For reference/debugging, you can keep how many rank groups contributed:
    # agg["n_rank_groups"] = r.groupby("tag_u").size().values

    top_share = agg.sort_values("mean_share", ascending=False)[
        ["tag_u", "mean_share", "avg_heat", "mean_rank", "weeks"]
    ].head(top_k)

    top_heat = agg.sort_values("avg_heat", ascending=False)[
        ["tag_u", "avg_heat", "mean_share", "mean_rank", "weeks"]
    ].head(top_k)

    out = [f"## {platform}\n"]
    out.append("### Top tags by mean_share\n")
    out.append(md_table(top_share, max_rows=top_k))
    out.append("\n### Top tags by avg_heat\n")
    out.append(md_table(top_heat, max_rows=top_k))
    return "\n".join(out)
def _section_images(images: dict[str, dict[str, str]] | None) -> str:
    if not images:
        return ""
    out = ["## 可视化\n"]
    for p in ["qidian", "fanqie"]:
        if p not in images or not images[p]:
            continue
        out.append(f"### {p}\n")
        for k, path in images[p].items():
            out.append(f"- {k}: ![]({path})")
        out.append("")
    return "\n".join(out)


def _fanqie_rank_structure(weekly: pd.DataFrame) -> pd.DataFrame:
    """
    构造番茄“分类榜结构”表：
    每个 (rank_family, rank_sub_cat) 汇总：
      - mean_books: 每周平均上榜书数（去重后）
      - avg_heat: 每周平均热度
      - entry_threshold: 平均入榜门槛
      - concentration_index: 平均集中度
      - weeks: 覆盖周数
    """
    w = weekly[weekly["platform"] == "fanqie"].copy()
    if w.empty:
        return pd.DataFrame()

    # weekly 为 tag 面板：按榜单层聚合（去掉 tag_u 维）
    w["rank_sub_cat_norm"] = _norm_subcat(w["rank_sub_cat"])

    gb = w.groupby(["rank_family", "rank_sub_cat_norm", "week"], dropna=False).agg(
        books=("book_count", "sum"),
        avg_heat=("avg_heat_raw", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    out = gb.groupby(["rank_family", "rank_sub_cat_norm"], dropna=False).agg(
        mean_books=("books", "mean"),
        avg_heat=("avg_heat", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
        weeks=("week", "nunique"),
    ).reset_index()

    out = out.rename(columns={"rank_sub_cat_norm": "rank_sub_cat"})
    out = out.sort_values(["avg_heat", "mean_books"], ascending=False)
    return out


def _fanqie_top_tags_by_subcat(weekly: pd.DataFrame, top_groups: int = 3, top_k: int = 10) -> str:
    w = weekly[weekly["platform"] == "fanqie"].copy()
    if w.empty:
        return "（无数据）\n"

    # 选最“重”的榜单组（按 avg_heat & books）
    struct = _fanqie_rank_structure(weekly)
    if struct.empty:
        return "（无数据）\n"
    pick = struct.head(top_groups)[["rank_family", "rank_sub_cat"]].to_dict("records")

    out = []
    for g in pick:
        rf = g["rank_family"]
        sub = g["rank_sub_cat"]
        # 注意：rank_sub_cat 已在结构表中归一化为“（未分组/ALL）”等；这里用同样的归一化口径筛选
        ww = w.copy()
        ww["rank_sub_cat_norm"] = _norm_subcat(ww["rank_sub_cat"])
        wt = ww[(ww["rank_family"] == rf) & (ww["rank_sub_cat_norm"] == sub)].copy()
        if wt.empty:
            continue

        # 分类内 top tags：用 mean(tag_share)
        tt = wt.groupby("tag_u", dropna=False).agg(
            mean_share=("tag_share", "mean"),
            avg_heat=("avg_heat_raw", "mean"),
            mean_rank=("avg_rank", "mean"),
            weeks=("week", "nunique"),
        ).reset_index().dropna(subset=["tag_u"])

        tt = tt.sort_values("mean_share", ascending=False).head(top_k)

        out.append(f"#### {rf}·{sub} - Top tags\n")
        out.append(md_table(tt[["tag_u", "mean_share", "avg_heat", "mean_rank", "weeks"]], max_rows=top_k))
        out.append("")
    return "\n".join(out) if out else "（无数据）\n"


def build_final_report(
    *,
    start_date: str,
    end_date: str,
    weekly: pd.DataFrame,
    roll: pd.DataFrame,
    weekly_cat: pd.DataFrame | None = None,
    roll_cat: pd.DataFrame | None = None,
    new_entry_compact: pd.DataFrame | None = None,
    pairs2: pd.DataFrame | None = None,
    triples3: pd.DataFrame | None = None,
    images: dict[str, dict[str, str]] | None = None,   # images[platform][key] = relative_path
    cfg: ReportConfig = ReportConfig(),
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    md: list[str] = []
    md.append(f"# WebNovel Trend Report ({start_date} ~ {end_date})\n")
    md.append(f"_Generated: {now}_\n")
    md.append(_explain_sampling())

    # Quick stats
    md.append("## 数据覆盖\n")
    if roll is not None and not roll.empty:
        cov = roll.groupby("platform")["weeks"].max().reset_index().rename(columns={"weeks": "max_weeks_in_window"})
        md.append(md_table(cov, max_rows=10))
    else:
        md.append("（无数据）\n")

    # Qidian/Fanqie tag view
    md.append(_section_platform_topk(roll, "qidian", cfg.top_k))
    md.append(_section_platform_topk(roll, "fanqie", cfg.top_k))

    # Fanqie structure
    md.append("## 番茄：分类榜结构（按 rank_family + rank_sub_cat）\n")
    struct = _fanqie_rank_structure(weekly)
    md.append(md_table(struct, max_rows=30))
    md.append(_explain_concentration())

    md.append("## 番茄：分类内 Top tags（按最热 Top3 分类榜）\n")
    md.append(_fanqie_top_tags_by_subcat(weekly, top_groups=3, top_k=min(cfg.top_k, 10)))

    # Cross-platform diff: category (preferred)
    if roll_cat is not None and not roll_cat.empty:
        md.append("## 跨平台差异（分类口径：cat_u，可比）\n")
        md.append(_explain_cross_platform_diff())
        diffc = build_cross_platform_diff_by_category(roll_cat)
        show = diffc.sort_values("share_diff", ascending=False)[
            ["cat_u", "presence", "share_qidian", "share_fanqie", "share_diff", "heat_diff", "rank_diff"]
        ]
        md.append(md_table(show, max_rows=30))
    else:
        md.append("## 跨平台差异（分类口径：cat_u，可比）\n\n（无分类口径数据）\n")

    # Cross-platform diff: tag (supplementary)
    md.append("## 跨平台差异（tag_u，补充参考）\n")
    difft = build_cross_platform_diff_by_tag(roll)
    show2 = difft.sort_values("share_diff", ascending=False)[
        ["tag_u", "presence", "share_qidian", "share_fanqie", "share_diff", "heat_diff", "rank_diff"]
    ]
    md.append(md_table(show2, max_rows=30))

    # New entry compact
    md.append("## 新书驱动（created_date 落在窗口内）\n")
    if new_entry_compact is None or new_entry_compact.empty:
        md.append("（无数据）\n")
    else:
        md.append(md_table(new_entry_compact, max_rows=30))

    # Co-occurrence
    md.append("## Tag 共现（pairs）\n")
    md.append(md_table(pairs2, max_rows=30))
    md.append("## Tag 共现（triples）\n")
    md.append(md_table(triples3, max_rows=30))

    # Images
    img_sec = _section_images(images)
    if img_sec:
        md.append(img_sec)

    return "\n".join(md)
