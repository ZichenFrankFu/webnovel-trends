# WebNovel Trends 分析文档（Analysis.md）

本系统用于对起点中文网与番茄小说榜单数据进行结构化分析， 构建统一的题材趋势与竞争结构量化体系。

一键启动
```bash
python run_analysis.py --db ../outputs/data/novels.db --platform both --top_k 10 --lookback all
```

---

# 1. System Overview

分析流程如下：

run_analysis.py  
→ TrendAnalyzer.run()  
→ data_access → heat → metrics → visualization → report  

核心模块职责：

| 文件 | 功能 |
|------|------|
| data_access.py | 从 SQLite 拉取长表数据 |
| heat.py | 构建跨平台统一热度体系 |
| metrics.py | 计算周度指标与时间窗口汇总指标 |
| trend_analyzer.py | 串联分析流程 |
| visualization.py | 生成图表 |
| report.py | 生成 Markdown 报告 |
| run_analysis.py | 控制时间窗口与参数 |

---

# 2. Args
## 2.1 时间窗口由 run_analysis.py 中的参数控制：
> --lookback {week, month, quarter, year, all}

规则如下：

$$
end\_date = today（America/New\_York）
$$

$$
start\_date = today - lookback\_days
$$

其中：

- week = 7 天
- month = 30 天
- quarter = 90 天
- year = 365 天
- all = 数据库最早日期

若计算出的 start_date 早于数据库最早 snapshot_date，
则自动修正为数据库最早日期。

## 2.2 平台
> --platform {qidian, fanqie, both}

## 2.3 topK数量
> --top_k 

---

# 3. 统一热度计算

为实现跨平台可比性，构建 heat_mix 体系

## 3.1 原始热度 heat_raw

$$
heat\_raw =
\begin{cases}
reading\_count & \text{番茄} \\
total\_recommend & \text{起点}
\end{cases}
$$

---

## 3.2 平台内百分位 heat_pct

$$
heat\_{pct} = rank\_percentile(heat\_raw \mid platform), (0,1]
$$


---

## 3.3 Robust Z 分数 heat_rz

$$
heat_{rz} =
\frac{heat\_raw - median(heat\_raw)}
{1.4826 \cdot MAD}
$$

其中：

$$
MAD = median(|x - median(x)|)
$$

用于抵抗爆款极端值

---

## 3.4 非线性压缩 heat_rz01

$$
heat_{rz01} =
\frac{\tanh\left(\frac{heat_{rz}}{c}\right) + 1}{2}
$$

默认：

$$
c = 3
$$

---

## 3.5 混合热度 heat_mix

$$
heat_{mix} = \alpha \cdot heat_{pct} + (1-\alpha) \cdot heat_{rz01}
$$

默认：

$$
\alpha = 0.7
$$

---

# 4. 统一分类体系
起点榜单是所有类别小说一起竞争， 主分类 main_category 具有市场对于分类选择的代表性。
番茄榜单是已分类之后的榜单，通过更细分的标签tag来判断其在本分类内部的趋势。

规则：

| 平台 | tag_u    | cat_u |
|------|----------|--------|
| 起点 | sub_cat  | main_category |
| 番茄 | tag_name | main_category |

跨平台比较优先使用 cat_u

---

# 5. 周度指标（Weekly Panel）

## 5.1 tag_share

$$
tag\_share =
\frac{\text{含该标签的 unique novels 数}}
{\text{该榜单的 unique novels 总数}}
$$


---

## 5.2 avg_heat_raw

$$
\frac{1}{N} \sum heat_{raw}
$$

---

## 5.3 avg_rank

$$
\frac{1}{N} \sum rank
$$

数值越小表示排名越好。

---

## 5.4 entry_threshold

$$
\max(rank)
$$

表示进入榜单所需最低总推荐/在读人数

---

## 5.5 books_in_group

某周某榜单的 unique novels 数量。

---

## 5.6 concentration_index（集中度）

$$
H = \sum_i p_i^2
$$

其中：

$$
p_i = tag\_share_i
$$

解释：

- 接近 1 → 高度垄断
- 接近 0 → 长尾分散

---

# 6. Stats over time

## 6.1 mean_share

$$
mean\_share =
\frac{1}{T} \sum tag\_share_t
$$

---

## 6.2 avg_heat

$$
avg\_heat =
\frac{1}{T} \sum avg\_heat_t
$$

---

## 6.3 mean_rank

$$
mean\_rank =
\frac{1}{T} \sum avg\_rank_t
$$

---

## 6.4 share_slope

对时间做线性回归：

$$
tag\_share_t = a t + b
$$

$$
share\_slope = a
$$

---

## 6.5 heat_slope

$$
avg\_heat_t = a t + b
$$

---

## 6.6 rank_slope

$$
avg\_rank_t = a t + b
$$

若 slope < 0 表示排名改善。

---

## 6.7 total_books

$$
total\_books =
\text{窗口期内出现过的 unique novel\_uid 数}
$$

---

# 7. Chance Score（潜在风口）

$$
chance = (-rank\_slope) + heat\_slope + share\_slope
$$

用于识别潜在增长题材。

## 7.1 new_entry_ratio

$$
\frac{\text{新进入榜单的小说数}}
{\text{总小说数}}
$$

---

## 7.2 新书平均热度

$$
\frac{1}{N} \sum heat_{new}
$$

---

# 8. 跨平台差异

## 8.1 share_diff

$$
share_{qidian} - share_{fanqie}
$$

---

## 8.2 heat_diff

$$
avg\_heat_{qidian} - avg\_heat_{fanqie}
$$

---

## 8.3 rank_diff

$$
mean\_rank_{qidian} - mean\_rank_{fanqie}
$$

---

