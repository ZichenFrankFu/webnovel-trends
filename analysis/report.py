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
        return "ALL"
    return str(x)


def _norm_subcat(series: pd.Series) -> pd.Series:
    """Normalize rank_sub_cat so empty-string and NaN collapse into the same label."""
    s = series.fillna("").astype(str).map(lambda x: x.strip())
    return s.replace({"": "ALL"})

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
        "### 抽样机制说明\n\n"
        "- **起点**：混合榜/全站竞争（榜单样本更接近真实市场主赛道竞争结果）\n"
        "- **番茄**：分类榜/子类竞争（榜单样本更接近条件分布 `P(书 | 子分类榜)`）\n\n"
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


def _fanqie_rank_structure(weekly: pd.DataFrame, ranklist_avg_daily: pd.DataFrame | None = None) -> pd.DataFrame:
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
        books=("books_in_group", "max"),
        avg_heat=("avg_heat_raw", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    out = gb.groupby(["rank_family", "rank_sub_cat_norm"], dropna=False).agg(
        total_books=("books", "sum"),
        avg_heat=("avg_heat", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
        weeks=("week", "nunique"),
    ).reset_index()

    # merge avg_daily_books / days / total_books
    if ranklist_avg_daily is not None and not ranklist_avg_daily.empty:
        ra = ranklist_avg_daily[ranklist_avg_daily["platform"] == "fanqie"].copy()

        # 用单独的 key 列，避免把 norm 覆盖成重复的 rank_sub_cat
        ra["rank_sub_cat_key"] = _norm_subcat(ra["rank_sub_cat"])
        ra = ra.drop(columns=["platform"], errors="ignore")

        out = out.drop(columns=["total_books"], errors="ignore")

        out = out.merge(
            ra[["rank_family", "rank_sub_cat_key", "avg_daily_books", "days", "total_books"]],
            left_on=["rank_family", "rank_sub_cat_norm"],
            right_on=["rank_family", "rank_sub_cat_key"],
            how="left",
        ).drop(columns=["rank_sub_cat_key"])
    else:
        out["avg_daily_books"] = np.nan
        out["days"] = np.nan
        out["total_books"] = np.nan

    out = out.rename(columns={"rank_sub_cat_norm": "rank_sub_cat"})
    out["_is_all"] = out["rank_sub_cat"].eq("ALL")
    out = out.sort_values(
        ["rank_family", "_is_all", "avg_daily_books", "avg_heat"],
        ascending=[True, True, False, False]
    ).drop(columns=["_is_all"])

    return out

def _qidian_rank_structure(
    weekly: pd.DataFrame,
    ranklist_avg_daily: pd.DataFrame | None = None
) -> pd.DataFrame:
    w = weekly[weekly["platform"] == "qidian"].copy()
    if w.empty:
        return pd.DataFrame()

    # 只对“新书榜”保留 subcat；其他榜单不区分 subcat
    w["rank_sub_cat_norm"] = _norm_subcat(w["rank_sub_cat"])
    w["rank_sub_cat_for_struct"] = np.where(
        w["rank_family"].astype(str).str.contains("新书"),
        w["rank_sub_cat_norm"],
        ""  # 其它榜单统一一个分组（不显示 subcat）
    )

    # 从 weekly(tag 面板) 计算结构指标（注意：books_in_group 在 tag 面板里不是榜单总书数）
    gb = w.groupby(["rank_family", "rank_sub_cat_for_struct", "week"], dropna=False).agg(
        avg_heat=("avg_heat_raw", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    out = gb.groupby(["rank_family", "rank_sub_cat_for_struct"], dropna=False).agg(
        avg_heat=("avg_heat", "mean"),
        entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    # 先把分组列标准化成 rank_sub_cat（后面所有 merge 都用它）
    out = out.rename(columns={"rank_sub_cat_for_struct": "rank_sub_cat"})

    # =========================
    # merge avg_daily_books / days / total_books （来自 ranklist_avg_daily）
    # =========================
    if ranklist_avg_daily is not None and not ranklist_avg_daily.empty:
        ra = ranklist_avg_daily[ranklist_avg_daily["platform"] == "qidian"].copy()

        # 对齐结构口径：新书榜保留 subcat，其它榜单 subcat 统一为空字符串 ""
        ra["rank_sub_cat_norm"] = _norm_subcat(ra["rank_sub_cat"])
        ra["rank_sub_cat_key"] = np.where(
            ra["rank_family"].astype(str).str.contains("新书"),
            ra["rank_sub_cat_norm"],
            ""
        )

        # drop 原始列，避免重复列名
        ra = ra.drop(columns=["platform", "rank_sub_cat", "rank_sub_cat_norm"], errors="ignore")

        # ✅ 关键修复：把 ra 压成唯一 key（避免 merge 1->N 复制 out 行）
        ra = ra.groupby(["rank_family", "rank_sub_cat_key"], dropna=False).agg(
            # ✅ total_books 已经是 unique novels 数，不能再 sum
            total_books=("total_books", "max"),
            days=("days", "max"),
            avg_daily_books=("avg_daily_books", "mean"),
        ).reset_index()

        # ✅ 如果 out 里已经存在 total_books（旧口径），先删掉，避免 total_books_x/y
        out = out.drop(columns=["total_books"], errors="ignore")

        out = out.merge(
            ra[["rank_family", "rank_sub_cat_key", "avg_daily_books", "days", "total_books"]],
            left_on=["rank_family", "rank_sub_cat"],
            right_on=["rank_family", "rank_sub_cat_key"],
            how="left",
        ).drop(columns=["rank_sub_cat_key"])
    else:
        out["avg_daily_books"] = np.nan
        out["days"] = np.nan
        out["total_books"] = np.nan

    # 让“非新书榜”的 subcat 显示为 "-"
    out["rank_sub_cat"] = out["rank_sub_cat"].astype(str)
    out.loc[out["rank_sub_cat"].str.strip().eq(""), "rank_sub_cat"] = "-"

    # 排序：优先看抓取强度，其次热度
    out = out.sort_values(
        ["rank_family", "avg_daily_books", "avg_heat"],
        ascending=[True, False, False]
    )
    return out


def _fanqie_top_tags_by_subcat(weekly: pd.DataFrame, top_groups: int = 3, top_k: int = 10) -> str:
    w = weekly[weekly["platform"] == "fanqie"].copy()
    if w.empty:
        return "（无数据）\n"

    struct = _fanqie_rank_structure(weekly)
    if struct.empty:
        return "（无数据）\n"

    pick = struct.head(top_groups)[["rank_family", "rank_sub_cat"]].to_dict("records")

    # 合并所有选中的榜单
    ww = w.copy()
    ww["rank_sub_cat_norm"] = _norm_subcat(ww["rank_sub_cat"])

    mask = False
    for g in pick:
        mask |= (
            (ww["rank_family"] == g["rank_family"]) &
            (ww["rank_sub_cat_norm"] == g["rank_sub_cat"])
        )

    wt = ww[mask].copy()
    if wt.empty:
        return "（无数据）\n"

    tt = wt.groupby("tag_u", dropna=False).agg(
        mean_share=("tag_share", "mean"),
        avg_heat=("avg_heat_raw", "mean"),
        mean_rank=("avg_rank", "mean"),
        days=("week", "nunique"),
    ).reset_index().dropna(subset=["tag_u"])

    tt = tt.sort_values("mean_share", ascending=False).head(top_k)

    out = []
    out.append(f"#### Top tags（合并最热 {top_groups} 个分类榜）\n")
    out.append(md_table(tt[["tag_u", "mean_share", "avg_heat", "mean_rank", "days"]], max_rows=top_k))
    return "\n".join(out)


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
    coverage: pd.DataFrame | None = None,
    ranklist_avg_daily: pd.DataFrame | None = None,

) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    md: list[str] = []
    md.append(f"# WebNovel Trend Report ({start_date} ~ {end_date})\n")
    md.append(f"_Generated: {now}_\n")
    md.append(_explain_sampling())

    # Quick stats
    md.append("## 数据覆盖\n")
    if coverage is not None and not coverage.empty:
        md.append(md_table(coverage, max_rows=10))
    else:
        md.append("（无数据）\n")

    # Qidian
    md.append(_section_platform_topk(roll, "qidian", cfg.top_k))
    md.append("## 起点抓取数据总览 \n")
    struct_q = _qidian_rank_structure(weekly, ranklist_avg_daily)
    md.append(md_table(struct_q, max_rows=30))

    # Fanqie
    md.append(_section_platform_topk(roll, "fanqie", cfg.top_k))
    md.append("## 番茄抓取数据总览\n")
    struct = _fanqie_rank_structure(weekly, ranklist_avg_daily)
    md.append(md_table(struct, max_rows=30))
    md.append(_explain_concentration())

    md.append("## 番茄：分类内 Top tags（按最热 Top5 分类榜）\n")
    md.append(_fanqie_top_tags_by_subcat(weekly, top_groups=5, top_k=min(cfg.top_k, 10)))

    # Co-occurrence
    md.append("## Tag 共现（pairs）\n")
    md.append(md_table(pairs2, max_rows=30))
    md.append("## Tag 共现（triples）\n")
    md.append(md_table(triples3, max_rows=30))


    # Cross-platform diff: category (preferred)
    if roll_cat is not None and not roll_cat.empty:
        md.append("## 跨平台题材差异\n")
        md.append(_explain_cross_platform_diff())
        diffc = build_cross_platform_diff_by_category(roll_cat)
        diffc = diffc[diffc["presence"] == "both"]
        show = diffc.sort_values("share_diff", ascending=False)[
            ["cat_u", "presence", "share_qidian", "share_fanqie", "share_diff", "heat_diff", "rank_diff"]
        ]
        md.append(md_table(show, max_rows=30))

    # New entry compact
    md.append("## 新书热点\n")
    if new_entry_compact is None or new_entry_compact.empty:
        md.append("（无数据）\n")
    else:
        md.append(md_table(new_entry_compact, max_rows=30))


    # Images
    img_sec = _section_images(images)
    if img_sec:
        md.append(img_sec)

    return "\n".join(md)
