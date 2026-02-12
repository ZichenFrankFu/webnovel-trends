# analysis/visualization.py
from __future__ import annotations

import os
import re
import matplotlib.pyplot as plt
import pandas as pd

# 解决中文字体 warning（Windows 常见字体）
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


def _ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _fix_title_topn(title: str, n: int) -> str:
    """
    把 title 中的 'Top 10' / 'Top10' / 'top 10' 替换成真实 Top{n}。
    如果没写 TopN，则在末尾追加 (Top{n})。
    """
    if not isinstance(title, str) or not title:
        return f"Top{n}"

    # 替换 Top 10/Top10 等
    new_title, cnt = re.subn(r"(?i)top\s*\d+", f"Top{n}", title)
    if cnt > 0:
        return new_title
    return f"{title} (Top{n})"


def save_bar_topk(
    df: pd.DataFrame,
    *,
    label_col: str,
    value_col: str,
    title: str,
    out_path: str,
    topk: int = 10,
) -> str:
    """
    修复点：
    1) 先对 label_col 聚合（避免同名 tag 多行导致 x 轴标签重复，柱子叠加 -> 看起来只有2个）
    2) 标题 TopN 自动用真实 n 修正
    """
    _ensure_dir(out_path)
    d = df.dropna(subset=[label_col, value_col]).copy()

    # ✅ 关键：同名 label 去重聚合（来自不同榜单/子榜的重复行）
    d = d.groupby(label_col, dropna=False)[value_col].mean().reset_index()

    d = d.sort_values(value_col, ascending=False).head(topk)
    n = int(len(d))

    plt.figure()
    plt.bar(d[label_col].astype(str), d[value_col])
    plt.title(_fix_title_topn(title, n))
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path


def save_line_top_tags(
    df: pd.DataFrame,
    *,
    x_col: str,
    y_col: str,
    tag_col: str,
    title: str,
    out_path: str,
    topk: int = 5,
) -> str:
    """
    修复点：
    - 同一 (x, tag) 多行时先聚合 mean，避免折线重复点造成异常
    - top tags 的选择依旧用 y 的均值
    """
    _ensure_dir(out_path)
    d = df.dropna(subset=[x_col, y_col, tag_col]).copy()

    # 先聚合到 (x, tag) 粒度，避免重复点
    d = d.groupby([x_col, tag_col], dropna=False)[y_col].mean().reset_index()

    top_tags = (
        d.groupby(tag_col)[y_col].mean()
        .sort_values(ascending=False)
        .head(topk)
        .index
        .tolist()
    )
    d = d[d[tag_col].isin(top_tags)].sort_values(x_col)

    plt.figure()
    for tag, sub in d.groupby(tag_col):
        plt.plot(sub[x_col].astype(str), sub[y_col], label=str(tag))
    plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    return out_path
