# analysis/metrics.py
from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from itertools import combinations


@dataclass(frozen=True)
class MetricConfig:
    top_n_for_top_appearance: int = 10   # top_appearance_ratio 的 TopN
    entry_top_n: int = 30               # entry_threshold 的 TopN
    top_k_tags: int = 20
    top_k_pairs: int = 30
    top_k_triples: int = 30


def week_start_monday(d: pd.Series) -> pd.Series:
    dt = pd.to_datetime(d)
    return dt.dt.to_period("W-MON").dt.start_time.dt.date


def unify_tag(row) -> str:
    # qidian: main_category 当作 tag；fanqie: tag_name
    if row["platform"] == "qidian":
        return row["main_category"] if pd.notna(row["main_category"]) else "UNKNOWN"
    return row["tag_name"] if pd.notna(row["tag_name"]) else "UNKNOWN"


def linear_slope(y: np.ndarray) -> float:
    if len(y) < 2:
        return np.nan
    x = np.arange(len(y), dtype=float)
    y = y.astype(float)
    if np.all(np.isnan(y)):
        return np.nan
    xm = x.mean()
    ym = np.nanmean(y)
    num = np.nansum((x - xm) * (y - ym))
    den = np.nansum((x - xm) ** 2)
    return float(num / den) if den != 0 else np.nan


def add_unified_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["week"] = week_start_monday(out["snapshot_date"].astype(str))
    out["tag_u"] = out.apply(unify_tag, axis=1)
    return out


def compute_weekly_tag_panel(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, week, tag_u) 的周面板指标
    """
    d = df.copy()
    d = d.dropna(subset=["rank", "week", "platform", "rank_family"])

    keys = ["platform", "rank_family", "rank_sub_cat", "week", "tag_u"]

    # 周内该 tag 的作品数（去重）
    g = d.groupby(keys, dropna=False)
    weekly = g.agg(
        book_count=("novel_uid", lambda x: x.nunique(dropna=True)),
        avg_rank=("rank", "mean"),
        median_rank=("rank", "median"),
        avg_heat_raw=("heat_raw", "mean"),
        median_heat_raw=("heat_raw", "median"),
        total_heat_raw=("heat_raw", "sum"),
        avg_heat_mix=("heat_mix", "mean"),
        total_heat_mix=("heat_mix", "sum"),
        heat_volatility=("heat_raw", "std"),
    ).reset_index()

    weekly["efficiency"] = weekly["total_heat_raw"] / weekly["book_count"].replace(0, np.nan)

    # head_ratio：周内 top3 heat / total heat（按 novel_uid 去重）
    def head_ratio(sub: pd.DataFrame) -> float:
        x = sub.drop_duplicates("novel_uid")[["novel_uid", "heat_raw"]].dropna()
        if x.empty:
            return np.nan
        tot = x["heat_raw"].sum()
        if tot <= 0:
            return np.nan
        return float(x["heat_raw"].nlargest(3).sum() / tot)

    head = g.apply(head_ratio).reset_index(name="head_ratio")
    weekly = weekly.merge(head, on=keys, how="left")

    # tag_share & concentration_index：在 (platform, rank_family, rank_sub_cat, week) 内计算
    key2 = ["platform", "rank_family", "rank_sub_cat", "week"]
    total_books = weekly.groupby(key2)["book_count"].transform("sum").replace(0, np.nan)
    weekly["tag_share"] = weekly["book_count"] / total_books

    conc = (
        weekly.groupby(key2)["tag_share"]
        .apply(lambda s: float((s.fillna(0) ** 2).sum()))
        .reset_index(name="concentration_index")
    )
    weekly = weekly.merge(conc, on=key2, how="left")

    # entry_threshold：该周该榜 TopN 最低 heat_raw（和 tag 无关，回填给每个 tag）
    entry_n = cfg.entry_top_n
    d2 = (
        d.drop_duplicates(["platform", "rank_family", "rank_sub_cat", "week", "novel_uid"])
        .dropna(subset=["rank", "heat_raw"])
    )

    def entry_threshold(sub: pd.DataFrame) -> float:
        sub = sub.sort_values("rank").head(entry_n)
        return float(sub["heat_raw"].min()) if not sub.empty else np.nan

    th = d2.groupby(key2, dropna=False).apply(entry_threshold).reset_index(name="entry_threshold")
    weekly = weekly.merge(th, on=key2, how="left")

    # share_growth / heat_growth：按 tag_u 的周序列 pct_change
    weekly = weekly.sort_values(keys)
    weekly["share_growth"] = weekly.groupby(
        ["platform", "rank_family", "rank_sub_cat", "tag_u"]
    )["tag_share"].pct_change()

    weekly["heat_growth"] = weekly.groupby(
        ["platform", "rank_family", "rank_sub_cat", "tag_u"]
    )["avg_heat_raw"].pct_change()

    return weekly


def compute_timewindow_rollup(weekly: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, tag_u) 的窗口汇总（safe/chance 等用）
    """
    d = weekly.copy()
    keys = ["platform", "rank_family", "rank_sub_cat", "tag_u"]
    d = d.sort_values(keys + ["week"])

    roll = d.groupby(keys, dropna=False).agg(
        weeks=("week", "nunique"),
        mean_rank=("avg_rank", "mean"),
        rank_std=("avg_rank", "std"),
        mean_share=("tag_share", "mean"),
        avg_heat=("avg_heat_raw", "mean"),
        median_heat=("median_heat_raw", "median"),
        heat_volatility=("avg_heat_raw", "std"),
        mean_efficiency=("efficiency", "mean"),
        mean_head_ratio=("head_ratio", "mean"),
        mean_entry_threshold=("entry_threshold", "mean"),
    ).reset_index()

    # rank_slope / share_slope / heat_slope
    def slope_of(col: str):
        def _f(sub: pd.DataFrame) -> float:
            y = sub.sort_values("week")[col].to_numpy()
            return linear_slope(y)
        return _f

    roll = roll.merge(
        d.groupby(keys, dropna=False).apply(slope_of("avg_rank")).reset_index(name="rank_slope"),
        on=keys, how="left"
    )
    roll = roll.merge(
        d.groupby(keys, dropna=False).apply(slope_of("tag_share")).reset_index(name="share_slope"),
        on=keys, how="left"
    )
    roll = roll.merge(
        d.groupby(keys, dropna=False).apply(slope_of("avg_heat_raw")).reset_index(name="heat_slope"),
        on=keys, how="left"
    )

    # top_appearance_ratio：该 tag 每周 avg_rank <= TopN 的比例
    N = cfg.top_n_for_top_appearance
    top_ratio = (
        d.assign(is_top=lambda x: x["avg_rank"] <= N)
        .groupby(keys)["is_top"].mean()
        .reset_index(name="top_appearance_ratio")
    )
    roll = roll.merge(top_ratio, on=keys, how="left")

    # -------------------------
    # lifecycle stage（修复 UNKNOWN）
    # 核心：weeks < 2 时用“水平”打标签（单周快照-*），weeks>=2 用 slope
    # 并输出 stage_basis 便于解释
    # -------------------------
    def stage_row(row) -> tuple[str, str]:
        weeks = row["weeks"]
        ms = row["mean_share"]
        mr = row["mean_rank"]
        ss = row["share_slope"]
        hs = row["heat_slope"]

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

    roll[["life_stage", "stage_basis"]] = roll.apply(lambda r: pd.Series(stage_row(r)), axis=1)

    return roll


def compute_new_entry_ratio_compact(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    新书驱动（精简版）：
    - 只按 (platform, tag_u) 聚合，避免 rank_family/rank_sub_cat 重复刷屏
    - 新书定义：created_date 落在 [start_date, end_date]
    输出列：
      platform, tag_u, total_books, new_books, new_entry_ratio
    """
    d = df.copy()
    d["created_date"] = pd.to_datetime(d["created_date"], errors="coerce").dt.date
    s = pd.to_datetime(start_date).date()
    e = pd.to_datetime(end_date).date()

    d1 = d.drop_duplicates(["platform", "tag_u", "novel_uid"]).copy()
    d1["is_new"] = d1["created_date"].apply(lambda x: (pd.notna(x) and s <= x <= e))

    out = d1.groupby(["platform", "tag_u"]).agg(
        total_books=("novel_uid", "count"),
        new_books=("is_new", "sum"),
    ).reset_index()

    out["new_entry_ratio"] = out["new_books"] / out["total_books"].replace(0, np.nan)
    out = out.sort_values("new_entry_ratio", ascending=False)
    return out


def compute_cooccurrence_pairs(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    共现（2-tag）：仅对 fanqie tags 更有意义（起点 main_category 单值共现没有意义）
    统计：同一本 fanqie 书的 tag pair 共现次数，输出 TopK
    """
    d = df[df["platform"] == "fanqie"].copy()
    tag_map = (
        d.dropna(subset=["tag_name"])
        .groupby("novel_uid")["tag_name"]
        .apply(lambda s: sorted(set(s)))
        .reset_index()
    )

    counts: dict[tuple[str, str], int] = {}
    for tags in tag_map["tag_name"]:
        if len(tags) < 2:
            continue
        for a, b in combinations(tags, 2):
            counts[(a, b)] = counts.get((a, b), 0) + 1

    if not counts:
        return pd.DataFrame(columns=["tag_a", "tag_b", "cooccur2_count"])

    pairs = (
        pd.DataFrame([(a, b, c) for (a, b), c in counts.items()],
                     columns=["tag_a", "tag_b", "cooccur2_count"])
        .sort_values("cooccur2_count", ascending=False)
        .head(cfg.top_k_pairs)
    )
    return pairs


def compute_cooccurrence_triples(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    共现（3-tag）：fanqie 三标签组合共现次数，输出 TopK
    """
    d = df[df["platform"] == "fanqie"].copy()
    tag_map = (
        d.dropna(subset=["tag_name"])
        .groupby("novel_uid")["tag_name"]
        .apply(lambda s: sorted(set(s)))
        .reset_index()
    )

    counts: dict[tuple[str, str, str], int] = {}
    for tags in tag_map["tag_name"]:
        if len(tags) < 3:
            continue
        for a, b, c in combinations(tags, 3):
            key = (a, b, c)
            counts[key] = counts.get(key, 0) + 1

    if not counts:
        return pd.DataFrame(columns=["tag_a", "tag_b", "tag_c", "cooccur3_count"])

    triples = (
        pd.DataFrame([(a, b, c, n) for (a, b, c), n in counts.items()],
                     columns=["tag_a", "tag_b", "tag_c", "cooccur3_count"])
        .sort_values("cooccur3_count", ascending=False)
        .head(cfg.top_k_triples)
    )
    return triples
