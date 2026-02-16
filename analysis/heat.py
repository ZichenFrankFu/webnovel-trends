# analysis/heat.py
from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass

@dataclass(frozen=True)
class HeatConfig:
    alpha: float = 0.7
    tanh_c: float = 3.0

def add_heat(df: pd.DataFrame, cfg: HeatConfig) -> pd.DataFrame:
    """
    输入 df 必须含：platform, reading_count, total_recommend
    输出新增：
      heat_raw, heat_pct, heat_rz, heat_rz01, heat_mix

    性能优化：
    - 用向量化替代 apply(axis=1)
    """
    out = df.copy()

    # 向量化 heat_raw（避免 apply 性能瓶颈）
    out["heat_raw"] = np.where(
        out["platform"] == "fanqie",
        out["reading_count"],
        out["total_recommend"],
    )
    out["heat_raw"] = pd.to_numeric(out["heat_raw"], errors="coerce")

    # 平台内 percentile（鲁棒）
    out["heat_pct"] = out.groupby("platform")["heat_raw"].rank(pct=True)

    # Robust Z：median + MAD
    def robust_z(x: pd.Series) -> pd.Series:
        x = x.astype(float)
        med = np.nanmedian(x)
        mad = np.nanmedian(np.abs(x - med))
        if mad == 0 or np.isnan(mad):
            return pd.Series(np.zeros(len(x)), index=x.index)
        return (x - med) / (1.4826 * mad)

    out["heat_rz"] = out.groupby("platform")["heat_raw"].transform(robust_z)

    # tanh 压缩到 (-1,1)，再映射到 (0,1)
    c = float(cfg.tanh_c)
    out["heat_rz01"] = (np.tanh(out["heat_rz"] / c) + 1.0) / 2.0

    # 混合
    alpha = float(cfg.alpha)
    out["heat_mix"] = alpha * out["heat_pct"] + (1 - alpha) * out["heat_rz01"]

    return out
