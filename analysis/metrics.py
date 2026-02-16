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


def _derive_sub_cat(main_category: object) -> object:
    """Derive qidian sub_cat from NOVELS.main_category.

    Examples:
      - '仙侠·修真文明' -> '修真文明'
      - '科幻末世' -> '科幻末世'
    """
    if pd.isna(main_category):
        return pd.NA
    s = str(main_category).strip()
    if not s:
        return pd.NA
    if "·" in s:
        parts = [p.strip() for p in s.split("·") if p.strip()]
        return parts[-1] if parts else pd.NA
    return s


def unify_tag(row) -> object:
    """Unified tag (tag_u) per Analysis.md.

    | platform | tag_u    |
    |----------|----------|
    | qidian   | sub_cat  |
    | fanqie   | tag_name |
    """
    if row.get("platform") == "qidian":
        return _derive_sub_cat(row.get("main_category"))
    tn = row.get("tag_name")
    return str(tn).strip() if pd.notna(tn) and str(tn).strip() else pd.NA


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
        book_count=("novel_uid", lambda x: x.nunique()),
        avg_rank=("rank", "mean"),
        avg_heat_raw=("heat_raw", "mean"),
        median_heat_raw=("heat_raw", "median"),
        entry_threshold=("heat_raw", "min"),
    ).reset_index()

    # books_in_group: 每周该榜单 unique novels 数（分母）
    key2 = ["platform", "rank_family", "rank_sub_cat", "week"]
    books_in_group = (
        d.drop_duplicates(key2 + ["novel_uid"])
        .groupby(key2, dropna=False)["novel_uid"]
        .nunique()
        .reset_index(name="books_in_group")
    )
    weekly = weekly.merge(books_in_group, on=key2, how="left")

    weekly["tag_share"] = weekly["book_count"] / weekly["books_in_group"].replace(0, np.nan)

    # efficiency: heat per rank (rough)
    weekly["efficiency"] = weekly["avg_heat_raw"] / weekly["avg_rank"].replace(0, np.nan)

    # head ratio: fraction of top appearances
    top_n = cfg.top_n_for_top_appearance
    top_app = (
        d[d["rank"] <= top_n]
        .drop_duplicates(key2 + ["novel_uid", "tag_u"])
        .groupby(keys, dropna=False)["novel_uid"]
        .nunique()
        .reset_index(name="top_app_count")
    )
    weekly = weekly.merge(top_app, on=keys, how="left")
    weekly["top_app_count"] = weekly["top_app_count"].fillna(0)
    weekly["head_ratio"] = weekly["top_app_count"] / weekly["book_count"].replace(0, np.nan)

    # concentration_index (HHI-like) for each list-week using tag_share
    conc = weekly.groupby(key2, dropna=False).apply(
        lambda sub: float(np.nansum((sub["tag_share"].to_numpy(dtype=float)) ** 2))
    ).reset_index(name="concentration_index")
    weekly = weekly.merge(conc, on=key2, how="left")

    return weekly


def compute_timewindow_rollup(weekly: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, tag_u) 的窗口汇总（safe/chance 等用）
    """
    d = weekly.copy()
    keys = ["platform", "rank_family", "rank_sub_cat", "tag_u"]
    d = d.sort_values(keys + ["week"])

    roll = d.groupby(keys, dropna=False).agg(
        days_seen=("week", "nunique"),
        min_date=("week", "min"),
        max_date=("week", "max"),
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

    roll["days_span"] = (
        (pd.to_datetime(roll["max_date"]) - pd.to_datetime(roll["min_date"]))
        .dt.days + 1
    )

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
        d.groupby(keys, dropna=False).apply(slope_of("avg_heat_raw")).reset_index(name="heat_slope"),
        on=keys, how="left"
    )
    roll = roll.merge(
        d.groupby(keys, dropna=False).apply(slope_of("tag_share")).reset_index(name="share_slope"),
        on=keys, how="left"
    )

    # safe/chance heuristic
    def stage_row(row):
        days_seen = row["days_seen"]
        if pd.isna(days_seen) or days_seen < 2:
            return "unknown"
        if row["mean_share"] >= 0.08 and row["avg_heat"] >= row["median_heat"]:
            return "safe"
        if row["share_slope"] > 0 and row["heat_slope"] > 0:
            return "chance"
        return "stable"

    roll["stage"] = roll.apply(stage_row, axis=1)
    return roll


# -----------------------------
# Weekly panel: CATEGORY 口径
# -----------------------------
def compute_weekly_category_panel(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    d = df.copy()
    d = d.dropna(subset=["rank", "week", "platform", "rank_family", "cat_u"])

    keys = ["platform", "rank_family", "rank_sub_cat", "week", "cat_u"]
    g = d.groupby(keys, dropna=False)

    weekly = g.agg(
        book_count=("novel_uid", lambda x: x.nunique()),
        avg_rank=("rank", "mean"),
        avg_heat_raw=("heat_raw", "mean"),
        entry_threshold=("heat_raw", "min"),
    ).reset_index()

    key2 = ["platform", "rank_family", "rank_sub_cat", "week"]
    books_in_group = (
        d.drop_duplicates(key2 + ["novel_uid"])
        .groupby(key2, dropna=False)["novel_uid"]
        .nunique()
        .reset_index(name="books_in_group")
    )
    weekly = weekly.merge(books_in_group, on=key2, how="left")

    weekly["cat_share"] = weekly["book_count"] / weekly["books_in_group"].replace(0, np.nan)

    conc = weekly.groupby(key2, dropna=False).apply(
        lambda sub: float(np.nansum((sub["cat_share"].to_numpy(dtype=float)) ** 2))
    ).reset_index(name="concentration_index")
    weekly = weekly.merge(conc, on=key2, how="left")
    return weekly


def compute_timewindow_category_rollup(weekly_cat: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    """
    输出：按 (platform, rank_family, rank_sub_cat, cat_u) 的窗口汇总
    """
    d = weekly_cat.copy()
    keys = ["platform", "rank_family", "rank_sub_cat", "cat_u"]
    d = d.sort_values(keys + ["week"])

    roll = d.groupby(keys, dropna=False).agg(
        days_seen=("week", "nunique"),
        min_date=("week","min"),
        max_date=("week","max"),
        mean_rank=("avg_rank", "mean"),
        mean_share=("cat_share", "mean"),
        avg_heat=("avg_heat_raw", "mean"),
        mean_entry_threshold=("entry_threshold", "mean"),
        concentration_index=("concentration_index", "mean"),
    ).reset_index()

    roll["days_span"] = (
        (pd.to_datetime(roll["max_date"]) - pd.to_datetime(roll["min_date"]))
        .dt.days + 1
    )

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
        d.groupby(keys, dropna=False).apply(slope_of("avg_heat_raw")).reset_index(name="heat_slope"),
        on=keys, how="left"
    )
    roll = roll.merge(
        d.groupby(keys, dropna=False).apply(slope_of("cat_share")).reset_index(name="share_slope"),
        on=keys, how="left"
    )
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
    d1 = d1.dropna(subset=["tag_u"])

    def _is_new(row) -> bool:
        # fanqie：新书榜天然新入榜（不依赖 created_date）
        if row.get("platform") == "fanqie":
            rf = str(row.get("rank_family") or "")
            return "新书" in rf
        cd = row.get("created_date")
        return (pd.notna(cd) and s <= cd <= e)

    d1["is_new"] = d1.apply(_is_new, axis=1)

    out = d1.groupby(["platform", "tag_u"]).agg(
        total_books=("novel_uid", "count"),
        new_books=("is_new", "sum"),
    ).reset_index()

    out["new_entry_ratio"] = out["new_books"] / out["total_books"].replace(0, np.nan)
    out = out.sort_values("new_entry_ratio", ascending=False)
    return out


def compute_opening_opportunities(
    df: pd.DataFrame,
    start_date: str,
    end_date: str,
    *,
    top_n: int = 80,
    min_total_books: int = 5,
) -> pd.DataFrame:
    """机会榜：platform + cat_u + tag_u。

    目标：给“现在增长最快、适合开书”的题材组合提供可排序表。

    - share：在同一平台同一天，(cat_u, tag_u) 覆盖的 unique novels / 平台当天 unique novels
    - share_delta：窗口末日 - 首日
    - heat_delta：窗口末日 - 首日（avg_heat_raw）
    - new_entry_ratio：qidian=created_date in window；fanqie=新书榜天然新入榜
    """
    d = df.copy()
    d["snapshot_date"] = pd.to_datetime(d["snapshot_date"], errors="coerce").dt.date
    s = pd.to_datetime(start_date).date()
    e = pd.to_datetime(end_date).date()
    d = d[(d["snapshot_date"] >= s) & (d["snapshot_date"] <= e)].copy()
    d = d.dropna(subset=["platform", "snapshot_date", "novel_uid", "cat_u", "tag_u"])
    if d.empty:
        return pd.DataFrame()

    # 平台日分母：unique novels
    denom = (
        d.drop_duplicates(["platform", "snapshot_date", "novel_uid"])
        .groupby(["platform", "snapshot_date"], dropna=False)["novel_uid"]
        .nunique(dropna=True)
        .reset_index(name="platform_books")
    )

    # 组内日：unique novels + avg heat
    g = (
        d.drop_duplicates(["platform", "snapshot_date", "cat_u", "tag_u", "novel_uid"])
        .groupby(["platform", "snapshot_date", "cat_u", "tag_u"], dropna=False)
        .agg(
            books=("novel_uid", "nunique"),
            avg_heat=("heat_raw", "mean"),
        )
        .reset_index()
        .merge(denom, on=["platform", "snapshot_date"], how="left")
    )
    g["share"] = g["books"] / g["platform_books"].replace(0, np.nan)

    # 首末日（按该组实际出现的日期）
    span = g.groupby(["platform", "cat_u", "tag_u"], dropna=False)["snapshot_date"].agg(
        min_date="min", max_date="max"
    ).reset_index()
    span["days_span"] = (pd.to_datetime(span["max_date"]) - pd.to_datetime(span["min_date"])).dt.days + 1

    gf = g.merge(span, on=["platform", "cat_u", "tag_u"], how="inner")

    start_rows = gf[gf["snapshot_date"] == gf["min_date"]][
        ["platform", "cat_u", "tag_u", "share", "avg_heat"]
    ].rename(columns={"share": "share_start", "avg_heat": "heat_start"})

    end_rows = gf[gf["snapshot_date"] == gf["max_date"]][
        ["platform", "cat_u", "tag_u", "share", "avg_heat"]
    ].rename(columns={"share": "share_end", "avg_heat": "heat_end"})

    out = span.merge(start_rows, on=["platform", "cat_u", "tag_u"], how="left") \
              .merge(end_rows, on=["platform", "cat_u", "tag_u"], how="left")

    out["share_delta"] = out["share_end"] - out["share_start"]
    out["heat_delta"] = out["heat_end"] - out["heat_start"]

    # 新书占比：窗口内 unique novels
    d2 = d.drop_duplicates(["platform", "cat_u", "tag_u", "novel_uid"]).copy()
    d2["created_date"] = pd.to_datetime(d2.get("created_date"), errors="coerce").dt.date

    def _is_new_row(row) -> bool:
        if row.get("platform") == "fanqie":
            rf = str(row.get("rank_family") or "")
            return "新书" in rf
        cd = row.get("created_date")
        return (pd.notna(cd) and s <= cd <= e)

    d2["is_new"] = d2.apply(_is_new_row, axis=1)
    nb = d2.groupby(["platform", "cat_u", "tag_u"], dropna=False).agg(
        total_books=("novel_uid", "count"),
        new_books=("is_new", "sum"),
    ).reset_index()
    nb["new_entry_ratio"] = nb["new_books"] / nb["total_books"].replace(0, np.nan)

    out = out.merge(nb, on=["platform", "cat_u", "tag_u"], how="left")
    out = out[out["total_books"].fillna(0) >= min_total_books].copy()

    # 分平台标准化打分（简单可用）
    def _z(s: pd.Series) -> pd.Series:
        s = s.astype(float)
        mu = np.nanmean(s)
        sd = np.nanstd(s)
        return (s - mu) / (sd + 1e-12)

    out["score_share"] = out.groupby("platform")["share_delta"].transform(_z)
    out["score_heat"] = out.groupby("platform")["heat_delta"].transform(_z)
    out["score_new"] = out.groupby("platform")["new_entry_ratio"].transform(_z)
    out["opportunity_score"] = out["score_share"].fillna(0) + out["score_heat"].fillna(0) + 0.8 * out["score_new"].fillna(0)

    out = out.sort_values(["platform", "opportunity_score"], ascending=[True, False]).head(top_n)
    return out


def compute_cooccurrence_pairs(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    d = df.dropna(subset=["tag_u"]).copy()
    d = d.drop_duplicates(["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid", "tag_u"])

    keys = ["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid"]
    tags_by_book = d.groupby(keys)["tag_u"].apply(lambda s: sorted(set([x for x in s if pd.notna(x)]))).reset_index()

    counter = {}
    for _, row in tags_by_book.iterrows():
        tags = row["tag_u"]
        if len(tags) < 2:
            continue
        for a, b in combinations(tags, 2):
            counter[(a, b)] = counter.get((a, b), 0) + 1

    out = pd.DataFrame(
        [{"tag_a": k[0], "tag_b": k[1], "count": v} for k, v in counter.items()]
    )
    if out.empty:
        return out
    out = out.sort_values("count", ascending=False).head(cfg.top_k_pairs)
    return out


def compute_cooccurrence_triples(df: pd.DataFrame, cfg: MetricConfig) -> pd.DataFrame:
    d = df.dropna(subset=["tag_u"]).copy()
    d = d.drop_duplicates(["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid", "tag_u"])

    keys = ["platform", "snapshot_date", "rank_family", "rank_sub_cat", "novel_uid"]
    tags_by_book = d.groupby(keys)["tag_u"].apply(lambda s: sorted(set([x for x in s if pd.notna(x)]))).reset_index()

    counter = {}
    for _, row in tags_by_book.iterrows():
        tags = row["tag_u"]
        if len(tags) < 3:
            continue
        for a, b, c in combinations(tags, 3):
            counter[(a, b, c)] = counter.get((a, b, c), 0) + 1

    out = pd.DataFrame(
        [{"tag_a": k[0], "tag_b": k[1], "tag_c": k[2], "count": v} for k, v in counter.items()]
    )
    if out.empty:
        return out
    out = out.sort_values("count", ascending=False).head(cfg.top_k_triples)
    return out
