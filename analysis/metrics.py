# analysis/metrics.py
from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass
from itertools import combinations


# ==========================
# Category merge rules
# ==========================
CATEGORY_RULES = {
    "都市": ["都市高武", "都市脑洞"],
    "科幻": ["科幻末世"],
    "奇幻": ["西方奇幻"],
    "悬疑": ["悬疑脑洞"],
    "玄幻": ["玄幻脑洞"],
}



@dataclass(frozen=True)
class MetricConfig:
    top_n_for_top_appearance: int = 10   # top_appearance_ratio 的 TopN
    entry_top_n: int = 30               # entry_threshold 的 TopN
    top_k_tags: int = 20
    top_k_pairs: int = 30
    top_k_triples: int = 30


def week_start_monday(d: pd.Series) -> pd.Series:
    # 不再按自然周分桶
    # 直接使用 snapshot_date 本身（按日聚合）
    return pd.to_datetime(d).dt.date


def unify_tag(row) -> str:
    # qidian: main_category 当作 tag；fanqie: tag_name
    if row["platform"] == "qidian":
        return row["main_category"] if pd.notna(row["main_category"]) else pd.NA
    return row["tag_name"] if pd.notna(row["tag_name"]) else pd.NA


def unify_category(row) -> str:
    cat = row["main_category"]
    if pd.isna(cat):
        return pd.NA

    cat = str(cat).strip()

    # 命中“子类”或“父类”都统一返回父类（只做合并，不保留层级）
    for parent, subs in CATEGORY_RULES.items():
        if parent in cat:
            return parent
        for sub in subs:
            if sub in cat:
                return parent

    return cat


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

    # 注意：缺失不要填 "UNKNOWN"，直接 NA，后续聚合/画图会 dropna 避免污染
    out["tag_u"] = out.apply(unify_tag, axis=1)
    out["cat_u"] = out.apply(unify_category, axis=1)
    return out


# -----------------------------
# Weekly panel: TAG 口径
# -----------------------------
def compute_weekly_tag_panel(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, week, tag_u) 的周面板指标
    """
    d = df.copy()
    d = d.dropna(subset=["rank", "week", "platform", "rank_family", "tag_u"])

    keys = ["platform", "rank_family", "rank_sub_cat", "week", "tag_u"]
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

    key2 = ["platform", "rank_family", "rank_sub_cat", "week"]

    # ✅ 口径修复：share 的分母必须是“该周该榜单 unique novels 数”，不能用 sum(book_count)
    # 否则同一本书在多个 tag 下会被重复计入分母，导致 share 失真、Top tags 表出现重复/异常。
    d2 = (
        d.drop_duplicates(["platform", "rank_family", "rank_sub_cat", "week", "novel_uid"])
        .dropna(subset=["rank", "heat_raw"])
    )
    books_grp = (
        d2.groupby(key2, dropna=False)["novel_uid"]
        .nunique(dropna=True)
        .reset_index(name="books_in_group")
    )
    weekly = weekly.merge(books_grp, on=key2, how="left")

    denom = weekly["books_in_group"].replace(0, np.nan)
    weekly["tag_share"] = weekly["book_count"] / denom

    # concentration: HHI over tag shares within the same (platform, rank_family, subcat, week)
    conc = (
        weekly.groupby(key2, dropna=False)["tag_share"]
        .apply(lambda s: float((s.fillna(0) ** 2).sum()))
        .reset_index(name="concentration_index")
    )
    weekly = weekly.merge(conc, on=key2, how="left")


    entry_n = cfg.entry_top_n



    def entry_threshold(sub: pd.DataFrame) -> float:
        sub = sub.sort_values("rank").head(entry_n)
        return float(sub["heat_raw"].min()) if not sub.empty else np.nan

    th = d2.groupby(key2, dropna=False).apply(entry_threshold).reset_index(name="entry_threshold")
    weekly = weekly.merge(th, on=key2, how="left")

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

    N = cfg.top_n_for_top_appearance
    top_ratio = (
        d.assign(is_top=lambda x: x["avg_rank"] <= N)
        .groupby(keys)["is_top"].mean()
        .reset_index(name="top_appearance_ratio")
    )
    roll = roll.merge(top_ratio, on=keys, how="left")

    # lifecycle stage（避免 UNKNOWN）
    def stage_row(row) -> tuple[str, str]:
        weeks = row["weeks"]
        ms = row["mean_share"]
        mr = row["mean_rank"]
        ss = row["share_slope"]
        hs = row["heat_slope"]

        if pd.isna(weeks) or weeks < 2:
            if (pd.notna(ms) and ms >= 0.15) or (pd.notna(mr) and mr <= 10):
                return "单周快照-强势", "level"
            if pd.notna(ms) and ms >= 0.05:
                return "单周快照-中等", "level"
            return "单周快照-长尾", "level"

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


# -----------------------------
# Weekly panel: CATEGORY 口径（你缺的就是这个）
# -----------------------------
def compute_weekly_category_panel(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, week, cat_u) 的周面板指标
    用于跨平台可比的“分类口径”
    """
    d = df.copy()
    d = d.dropna(subset=["rank", "week", "platform", "rank_family", "cat_u"])

    keys = ["platform", "rank_family", "rank_sub_cat", "week", "cat_u"]
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


    key2 = ["platform", "rank_family", "rank_sub_cat", "week"]

    # ✅ 口径修复：cat_share 的分母用该周该榜单 unique novels 数（去重 novel_uid）
    d2 = (
        d.drop_duplicates(["platform", "rank_family", "rank_sub_cat", "week", "novel_uid"])
        .dropna(subset=["rank", "heat_raw"])
    )
    books_grp = (
        d2.groupby(key2, dropna=False)["novel_uid"]
        .nunique(dropna=True)
        .reset_index(name="books_in_group")
    )
    weekly = weekly.merge(books_grp, on=key2, how="left")

    denom = weekly["books_in_group"].replace(0, np.nan)
    weekly["cat_share"] = weekly["book_count"] / denom

    conc = (
        weekly.groupby(key2, dropna=False)["cat_share"]
        .apply(lambda s: float((s.fillna(0) ** 2).sum()))
        .reset_index(name="concentration_index")
    )
    weekly = weekly.merge(conc, on=key2, how="left")

    entry_n = cfg.entry_top_n

    def entry_threshold(sub: pd.DataFrame) -> float:
        sub = sub.sort_values("rank").head(entry_n)
        return float(sub["heat_raw"].min()) if not sub.empty else np.nan

    th = d2.groupby(key2, dropna=False).apply(entry_threshold).reset_index(name="entry_threshold")
    weekly = weekly.merge(th, on=key2, how="left")

    weekly = weekly.sort_values(keys)
    weekly["share_growth"] = weekly.groupby(
        ["platform", "rank_family", "rank_sub_cat", "cat_u"]
    )["cat_share"].pct_change()

    weekly["heat_growth"] = weekly.groupby(
        ["platform", "rank_family", "rank_sub_cat", "cat_u"]
    )["avg_heat_raw"].pct_change()

    return weekly


def compute_timewindow_category_rollup(weekly_cat: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, cat_u) 的窗口汇总
    """
    d = weekly_cat.copy()
    keys = ["platform", "rank_family", "rank_sub_cat", "cat_u"]
    d = d.sort_values(keys + ["week"])

    roll = d.groupby(keys, dropna=False).agg(
        weeks=("week", "nunique"),
        mean_rank=("avg_rank", "mean"),
        rank_std=("avg_rank", "std"),
        mean_share=("cat_share", "mean"),
        avg_heat=("avg_heat_raw", "mean"),
        heat_volatility=("avg_heat_raw", "std"),
        mean_efficiency=("efficiency", "mean"),
        mean_entry_threshold=("entry_threshold", "mean"),
    ).reset_index()

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
        d.groupby(keys, dropna=False).apply(slope_of("cat_share")).reset_index(name="share_slope"),
        on=keys, how="left"
    )
    roll = roll.merge(
        d.groupby(keys, dropna=False).apply(slope_of("avg_heat_raw")).reset_index(name="heat_slope"),
        on=keys, how="left"
    )
    return roll


# -----------------------------
# Other blocks (co-occurrence / new entry)
# -----------------------------
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
    d1 = d1.dropna(subset=["tag_u"])
    d1["is_new"] = d1["created_date"].apply(lambda x: (pd.notna(x) and s <= x <= e))

    out = d1.groupby(["platform", "tag_u"]).agg(
        total_books=("novel_uid", "count"),
        new_books=("is_new", "sum"),
    ).reset_index()

    out["new_entry_ratio"] = out["new_books"] / out["total_books"].replace(0, np.nan)
    out = out.sort_values("new_entry_ratio", ascending=False)
    return out


def compute_cooccurrence_pairs(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    d = df.dropna(subset=["tag_u"]).copy()
    d = d.drop_duplicates(["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid", "tag_u"])

    keys = ["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid"]
    tags_by_book = d.groupby(keys)["tag_u"].apply(lambda s: sorted(set([x for x in s if pd.notna(x)]))).reset_index()

    counter = {}
    for _, row in tags_by_book.iterrows():
        tags = row["tag_u"]
        if not tags or len(tags) < 2:
            continue
        for a, b in combinations(tags, 2):
            counter[(a, b)] = counter.get((a, b), 0) + 1

    out = pd.DataFrame(
        [{"tag_a": k[0], "tag_b": k[1], "count": v} for k, v in counter.items()]
    ).sort_values("count", ascending=False).head(cfg.top_k_pairs)
    return out


def compute_cooccurrence_triples(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    d = df.dropna(subset=["tag_u"]).copy()
    d = d.drop_duplicates(["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid", "tag_u"])

    keys = ["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid"]
    tags_by_book = d.groupby(keys)["tag_u"].apply(lambda s: sorted(set([x for x in s if pd.notna(x)]))).reset_index()

    counter = {}
    for _, row in tags_by_book.iterrows():
        tags = row["tag_u"]
        if not tags or len(tags) < 3:
            continue
        for a, b, c in combinations(tags, 3):
            counter[(a, b, c)] = counter.get((a, b, c), 0) + 1

    out = pd.DataFrame(
        [{"tag_a": k[0], "tag_b": k[1], "tag_c": k[2], "count": v} for k, v in counter.items()]
    ).sort_values("count", ascending=False).head(cfg.top_k_triples)
    return out