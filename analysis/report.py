# analysis/report.py
from __future__ import annotations

import os
import numpy as np
import pandas as pd
from datetime import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class ReportConfig:
    top_k: int = 20


def _nan_to_dash(df: pd.DataFrame) -> pd.DataFrame:
    """
    避免 markdown 输出 'nan'；把 NaN/None 统一显示为 '-'
    """
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
    return _nan_to_dash(df.head(max_rows)).to_markdown(index=False)


def build_cross_platform_diff(roll: pd.DataFrame) -> pd.DataFrame:
    """
    跨平台差异：
    - share 缺失视为 0（表示该平台没出现）
    - heat/rank diff 仅对 presence=both 计算，避免 qidian_only/fanqie_only 误导
    - 保留 presence 字段方便解释
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

    # share 缺失=0
    diff["share_qidian"] = diff["share_qidian"].fillna(0.0)
    diff["share_fanqie"] = diff["share_fanqie"].fillna(0.0)
    diff["share_diff"] = diff["share_qidian"] - diff["share_fanqie"]

    # heat/rank diff：只对 both 才算
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


def _topk_by_platform(df: pd.DataFrame, sort_col: str, topk: int, ascending: bool = False) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for p in ["fanqie", "qidian"]:
        sub = df[df["platform"] == p].sort_values(sort_col, ascending=ascending).head(topk)
        out[p] = sub
    return out


def _recompute_life_stage_tag_level(r: pd.DataFrame) -> pd.DataFrame:
    """
    roll 聚合到 (platform, tag_u) 后，重新判定 life_stage，避免同一 tag 在不同榜单产生多个 stage。
    """
    def stage_row(row) -> tuple[str, str]:
        weeks = row.get("weeks", np.nan)
        ms = row.get("mean_share", np.nan)
        mr = row.get("mean_rank", np.nan)
        ss = row.get("share_slope", np.nan)
        hs = row.get("heat_slope", np.nan)

        # 单周：用水平判断
        if pd.isna(weeks) or weeks < 2:
            if (pd.notna(ms) and ms >= 0.15) or (pd.notna(mr) and mr <= 10):
                return "单周快照-强势", "level"
            if pd.notna(ms) and ms >= 0.05:
                return "单周快照-中等", "level"
            return "单周快照-长尾", "level"

        # 多周：用趋势判断；缺 slope 仍给可读结果
        if pd.isna(ss) or pd.isna(hs):
            if pd.notna(ms) and ms >= 0.10:
                return "成熟(缺趋势)", "level"
            return "过渡(缺趋势)", "level"

        if ss > 0 and hs > 0:
            return "成长", "slope"
        if ss < 0 and hs < 0:
            return "衰退", "slope"
        if pd.notna(ms) and ms >= 0.10 and abs(ss) < 1e-3 and abs(hs) < 1e-3:
            return "成熟", "slope"
        return "过渡", "slope"

    out = r.copy()
    out[["life_stage", "stage_basis"]] = out.apply(lambda rr: pd.Series(stage_row(rr)), axis=1)
    return out


def _roll_tag_level(roll: pd.DataFrame) -> pd.DataFrame:
    """
    关键修复：
    原 roll 粒度是 (platform, rank_family, rank_sub_cat, tag_u)，导致同一 tag_u 会重复出现在 TopK。
    这里统一聚合成 (platform, tag_u) 粒度，用于所有 TopK 表与可视化。
    """
    # 用 mean 聚合大多数指标；weeks 用 max（窗口长度只要最大即可）
    cols = set(roll.columns)

    def _pick(col: str, default=np.nan):
        return col if col in cols else default

    r = roll.groupby(["platform", "tag_u"], dropna=False).agg(
        weeks=("weeks", "max") if "weeks" in cols else ("tag_u", "count"),
        mean_rank=("mean_rank", "mean") if "mean_rank" in cols else ("avg_rank", "mean"),
        rank_std=("rank_std", "mean") if "rank_std" in cols else ("rank", "std"),
        mean_share=("mean_share", "mean") if "mean_share" in cols else ("tag_share", "mean"),
        avg_heat=("avg_heat", "mean") if "avg_heat" in cols else ("avg_heat_raw", "mean"),
        median_heat=("median_heat", "median") if "median_heat" in cols else ("avg_heat_raw", "median"),
        heat_volatility=("heat_volatility", "mean") if "heat_volatility" in cols else ("avg_heat_raw", "std"),
        mean_efficiency=("mean_efficiency", "mean") if "mean_efficiency" in cols else ("efficiency", "mean"),
        mean_head_ratio=("mean_head_ratio", "mean") if "mean_head_ratio" in cols else ("head_ratio", "mean"),
        mean_entry_threshold=("mean_entry_threshold", "mean") if "mean_entry_threshold" in cols else ("entry_threshold", "mean"),
        rank_slope=("rank_slope", "mean") if "rank_slope" in cols else ("avg_rank", "mean"),
        share_slope=("share_slope", "mean") if "share_slope" in cols else ("tag_share", "mean"),
        heat_slope=("heat_slope", "mean") if "heat_slope" in cols else ("avg_heat_raw", "mean"),
        top_appearance_ratio=("top_appearance_ratio", "mean") if "top_appearance_ratio" in cols else ("tag_u", "count"),
    ).reset_index()

    # 重新计算生命周期（tag-level）
    r = _recompute_life_stage_tag_level(r)
    return r


def build_final_report(
    *,
    start_date: str,
    end_date: str,
    weekly: pd.DataFrame,
    roll: pd.DataFrame,
    new_entry_compact: pd.DataFrame | None,
    pairs2: pd.DataFrame | None,
    triples3: pd.DataFrame | None,
    images: dict[str, dict[str, str]] | None,   # images[platform][key] = relative_path
    cfg: ReportConfig,
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    topk = cfg.top_k

    # ✅ 关键修复：roll → (platform, tag_u) 聚合，避免 TopK 重复 tag
    r = _roll_tag_level(roll)

    # 综合分（平台内排序用）
    r["safe_score"] = (1.0 / r["mean_rank"].replace(0, np.nan)) + r["top_appearance_ratio"].fillna(0) - r["rank_std"].fillna(0)
    r["chance_score"] = (-r["rank_slope"].fillna(0)) + r["heat_slope"].fillna(0) + r["share_slope"].fillna(0)
    r["blue_score"] = r["mean_efficiency"].fillna(0) - r["mean_share"].fillna(0)
    r["risk_score"] = r["heat_volatility"].fillna(0) + r["rank_std"].fillna(0)

    # 关键榜单（列精简：保留解释所需的最少列）
    cols_safe = ["platform", "tag_u", "weeks", "mean_rank", "rank_std", "rank_slope", "top_appearance_ratio", "mean_share", "avg_heat", "mean_efficiency", "safe_score"]
    cols_chance = ["platform", "tag_u", "weeks", "mean_rank", "rank_slope", "share_slope", "heat_slope", "mean_share", "avg_heat", "chance_score"]
    cols_share = ["platform", "tag_u", "weeks", "mean_share", "mean_rank", "avg_heat", "mean_efficiency"]
    cols_heat = ["platform", "tag_u", "weeks", "avg_heat", "median_heat", "mean_rank", "mean_share"]
    cols_comp = ["platform", "tag_u", "weeks", "mean_efficiency", "mean_share", "mean_head_ratio", "mean_entry_threshold", "avg_heat"]
    cols_life = ["platform", "tag_u", "life_stage", "stage_basis", "mean_share", "share_slope", "heat_slope", "mean_rank", "weeks"]
    cols_risk = ["platform", "tag_u", "weeks", "heat_volatility", "rank_std", "risk_score", "mean_share", "avg_heat"]

    safe = r[cols_safe].sort_values("safe_score", ascending=False)
    chance = r[cols_chance].sort_values("chance_score", ascending=False)
    share = r[cols_share].sort_values("mean_share", ascending=False)
    heat = r[cols_heat].sort_values("avg_heat", ascending=False)
    comp = r[cols_comp].sort_values("mean_efficiency", ascending=False)
    life = r[cols_life].sort_values(["platform", "life_stage", "mean_share"], ascending=[True, True, False])
    risk = r[cols_risk].sort_values("risk_score", ascending=False)

    safe_p = _topk_by_platform(safe, "safe_score", topk, ascending=False)
    chance_p = _topk_by_platform(chance, "chance_score", topk, ascending=False)
    share_p = _topk_by_platform(share, "mean_share", topk, ascending=False)
    heat_p = _topk_by_platform(heat, "avg_heat", topk, ascending=False)
    comp_p = _topk_by_platform(comp, "mean_efficiency", topk, ascending=False)
    life_p = _topk_by_platform(life, "mean_share", topk, ascending=False)
    risk_p = _topk_by_platform(risk, "risk_score", topk, ascending=False)

    # 跨平台差异（这里传入 tag-level 的 r，避免重复）
    diff = build_cross_platform_diff(r).sort_values("share_diff", ascending=False).head(topk)

    parts: list[str] = []
    parts.append("# WebNovel Trends – Final Report\n")
    parts.append(f"- Generated at: {now}\n")
    parts.append(f"- Range: {start_date} ~ {end_date}\n")
    parts.append(f"- TopK: {topk}\n")
    parts.append("\n---\n")

    # 可视化（平台分开）
    if images:
        parts.append("## 0) 可视化速览\n")
        for p in ["fanqie", "qidian"]:
            if p not in images:
                continue
            parts.append(f"### {p}\n")
            for k, rel_path in images[p].items():
                parts.append(f"- {k}\n\n![]({rel_path})\n")
        parts.append("\n---\n")

    # Safe / Chance
    parts.append("## 1) Safe 题材（长期稳定高位）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(safe_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(safe_p["qidian"], topk))
    parts.append("\n---\n")

    parts.append("## 2) Chance 题材（上升最快）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(chance_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(chance_p["qidian"], topk))
    parts.append("\n---\n")

    # Market share / Heat
    parts.append("## 3) 题材占比 TopK（市场份额）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(share_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(share_p["qidian"], topk))
    parts.append("\n---\n")

    parts.append("## 4) 热度 TopK（平台内热度强度）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(heat_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(heat_p["qidian"], topk))
    parts.append("\n---\n")

    # Competition / Lifecycle / Risk
    parts.append("## 5) 竞争与效率 TopK（更适合新作者的赛道线索）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(comp_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(comp_p["qidian"], topk))
    parts.append("\n---\n")

    parts.append("## 6) 生命周期（tag-level：同一 tag 不重复）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(life_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(life_p["qidian"], topk))
    parts.append("\n---\n")

    parts.append("## 7) 高风险题材（波动大）\n")
    parts.append("### fanqie\n")
    parts.append(md_table(risk_p["fanqie"], topk))
    parts.append("\n### qidian\n")
    parts.append(md_table(risk_p["qidian"], topk))
    parts.append("\n---\n")

    # New entry（精简列）
    if new_entry_compact is not None and not new_entry_compact.empty:
        parts.append("## 8) 新书驱动（平台扶持信号，按 platform+tag 聚合）\n")
        for p in ["fanqie", "qidian"]:
            parts.append(f"### {p}\n")
            sub = new_entry_compact[new_entry_compact["platform"] == p].head(topk)
            parts.append(md_table(sub[["platform", "tag_u", "total_books", "new_books", "new_entry_ratio"]], topk))
        parts.append("\n---\n")

    # Co-occur 2 / 3
    if pairs2 is not None and not pairs2.empty:
        parts.append("## 9) Fanqie Tags 二标签共现 TopK（爆款组合线索）\n")
        parts.append(md_table(pairs2, topk))
        parts.append("\n---\n")

    if triples3 is not None and not triples3.empty:
        parts.append("## 10) Fanqie Tags 三标签共现 TopK（组合升级）\n")
        parts.append(md_table(triples3, topk))
        parts.append("\n---\n")

    parts.append("## 11) 跨平台差异（起点 vs 番茄题材偏好）\n")
    parts.append(md_table(diff[[
        "tag_u", "presence",
        "share_qidian", "share_fanqie", "share_diff",
        "heat_qidian", "heat_fanqie", "heat_diff",
        "rank_qidian", "rank_fanqie", "rank_diff"
    ]], topk))
    parts.append("\n---\n")

    parts.append("## 指标口径说明（摘要）\n")
    parts.append("- heat_raw：番茄 reading_count，起点 total_recommend（按 platform 映射）。\n")
    parts.append("- heat_mix：平台内 heat_pct（百分位） + robust z（MAD）压缩后混合，用于更稳健的排序。\n")
    parts.append("- 起点以 main_category 作为 tag；番茄以 tags 表中的 tag_name 作为 tag。\n")

    return "\n".join(parts)


def write_report(md: str, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
