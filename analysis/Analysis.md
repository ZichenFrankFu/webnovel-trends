# WebNovel Trends Analysis 说明文档
版本：Phase 1  
目标：构建小说题材量化分析与决策系统  
适用平台：起点中文网 / 番茄小说  

一键启动
```bash
python run_analysis.py --db ../outputs/data/novels.db --start_date 2026-02-01 --end_date 2026-02-12 --platform both --top_k 10
```

---
# 一、时间窗口说明
所有指标均基于可调时间窗口计算。
推荐参数：

```
--start_date YYYY-MM-DD
--end_date YYYY-MM-DD
--window daily | weekly | monthly
--top_k 数量
--platform qidian | fanqie | both
--rank_family 可选
```
默认建议使用 **weekly（按周统计）**，以减少日波动影响。
---


# 0. 抽样机制差异（必须读）
- **起点中文网**：混合榜（全站竞争），因此 `main_category` 的占比更接近“市场竞争结构”。
- **番茄小说**：分类榜（每个 `rank_sub_cat` 单独竞争），跨 `rank_sub_cat` 聚合后的占比是“抽样设计结果”，不能直接当作市场份额。

本项目为此做了两套口径：
- `tag_u`：题材/标签口径（起点=main_category；番茄=tag_name），用于平台内选题与组合分析。
- `cat_u`：跨平台对齐口径（两平台都取 novels.main_category），用于解释与跨平台差异表。

报告中会额外输出番茄 `rank_sub_cat` 结构摘要，以及“分类内 Top tags”。
---
# 二、排名类指标（Ranking Metrics）

排名指标用于分析题材在竞争结构中的地位。 它们回答的是：

> 在有限榜单资源下，这个题材竞争力如何？

---

## 1. 平均排名（mean_rank）

$$
mean\_rank = \frac{1}{T} \sum rank_t
$$

### 含义：

- 反映题材长期结构地位
- 数值越小越好

### 用途：

- 判断是否为长期主赛道
- 构建 Safe 题材指数

---

## 2. 排名标准差（rank_std）

$$
rank\_std = std(rank_t)
$$

### 含义：

- 衡量排名波动程度
- 越小越稳定

### 用途：

- 判断风险
- 区分稳定赛道与情绪驱动型赛道

---

## 3. 排名趋势斜率（rank_slope）

$$
rank_t = a t + b
$$

- slope < 0 → 排名上升
- slope > 0 → 排名下降

### 含义：

- 判断题材趋势方向

### 用途：

- 识别机会型题材
- 预测趋势变化

---

## 4. 头部出现比例（top_appearance_ratio）

$$
\frac{\text{进入前N次数}}{\text{总时间单位}}
$$

### 含义：

- 衡量题材持续占据头部的能力

### 用途：

- 判断是否为平台常驻主力赛道

---

# 三、市场份额指标（Market Share Metrics）

市场份额指标分析题材结构分布。

它们回答的是：

> 市场资源如何在不同题材之间分配？

---

## 1. 题材占比（tag_share）

$$
\frac{\text{该题材书数量}}{\text{总书数量}}
$$

### 含义：

- 反映市场规模

### 用途：

- 判断主流与小众赛道
- 判断赛道拥挤程度

---

## 2. 占比增长率（share_growth）

$$
\frac{share_t - share_{t-1}}{share_{t-1}}
$$

### 含义：

- 衡量题材是否扩张或收缩

### 用途：

- 判断结构性趋势
- 识别新兴赛道

---

## 3. 市场集中度（Herfindahl指数）

$$
H = \sum p_i^2
$$

### 含义：

- 衡量市场是否集中在少数题材

### 用途：

- 判断是否为垄断型结构
- 判断市场是否多元竞争

---

# 四、热度类指标（Heat Metrics）

本项目中，不同平台的“热度”原始含义不同：

- **番茄小说**：`reading_count`（阅读量，行为驱动）
- **起点中文网**：`total_recommend`（推荐票，投票驱动）

为支持**跨平台比较**与**综合排序**，在 analysis 层统一抽象出 `heat` 相关字段，并采用 **“percentile + robust z” 混合指标**。

---

## 1. 字段定义

### 1.1 原始热度：`heat_raw`

按平台映射：

- 若 `platform == "fanqie"`：`heat_raw = reading_count`
- 若 `platform == "qidian"`：`heat_raw = total_recommend`

> `heat_raw` 用于平台内展示与原始数据保留，不直接用于跨平台数值对比。

---

### 1.2 平台内百分位热度：`heat_pct`

在**各平台内部**对 `heat_raw` 做百分位排名：

$$
heat\_pct = \text{rank\_pct}(heat\_raw) \in (0,1]
$$

说明：
- 只保留相对顺序（极其稳健）
- 对爆款极端值不敏感
- **推荐作为跨平台对比的默认热度刻度**

---

### 1.3 平台内稳健标准化：`heat_rz`

传统 z-score 对长尾/爆款分布敏感，因此采用 **Robust Z**（中位数 + MAD）：

设：

- $$m = \text{median}(heat\_raw)$$
- $$MAD = \text{median}(|heat\_raw - m|)$$

则：

$$
heat\_{rz} = \frac{heat\_raw - m}{1.4826 \cdot MAD}
$$

说明：
- 使用 MAD 替代标准差，显著降低极端值影响
- 比普通 z-score 更适合网络小说热度这类重尾分布

---

## 2. 稳定压缩（将 robust z 限幅）

由于 `heat_rz` 可能仍存在较大幅度，为避免混合后被极端值主导，对其进行平滑压缩：

$$
heat\_{rz\_clip} = \tanh\left(\frac{heat\_{rz}}{c}\right)
$$

其中：
- `tanh` 输出范围为 $(-1,1)$
- 建议默认 $c = 3$（可配置，通常 2~4 都合理）

再映射到 0~1：

$$
heat\_{rz01} = \frac{heat\_{rz\_clip} + 1}{2} \in (0,1)
$$

---

## 3. 混合热度：`heat_mix`

最终用于**综合评分 / 排序**的热度指标：

$$
heat\_{mix} = \alpha \cdot heat\_{pct} + (1-\alpha) \cdot heat\_{rz01}
$$

推荐默认参数：
- $\alpha = 0.7$
- $c = 3$

解释：
- `heat_pct` 提供强鲁棒的“相对位置”
- `heat_rz01` 提供幅度信息（区分头部强弱差距）
- 混合后既稳健又保留一定强度信息，适合做榜单排序与综合评分

---

## 1. 平均热度（avg_heat）

$$
\frac{1}{N} \sum heat_i
$$

用于衡量题材整体流量规模。

---

## 2. 中位热度（median_heat）

用于避免极端爆款干扰。

---

## 3. 热度增长率（heat_growth）

$$
\frac{heat_t - heat_{t-1}}{heat_{t-1}}
$$

用于判断流量趋势。

---

## 4. 单书效率（efficiency）

$$
\frac{total\_heat}{book\_count}
$$

### 含义：

- 单本书平均获得的流量

### 用途：

- 判断是否内卷
- 识别蓝海赛道

---

# 五、竞争指标（Competition Metrics）

竞争指标衡量题材内部流量分配结构。

它们回答的是：

> 这个赛道卷不卷？

---

## 1. 头部集中度（head_ratio）

$$
\frac{top3\_heat}{total\_heat}
$$

### 含义：

- 判断是否赢家通吃

### 用途：

- 识别垄断赛道
- 判断新人是否有机会

---

## 2. 入榜门槛（entry_threshold）

进入 Top N 所需最低热度。

### 含义：

- 衡量准入难度

### 用途：

- 判断赛道是否适合新作者

---

## 3. 单书效率（efficiency）

同上。

用于衡量流量是否被稀释。

---

# 六、生命周期指标（Lifecycle Metrics）

用于判断题材发展阶段。

---

## 1. 占比趋势（share_slope）

判断市场结构是否扩张。

---

## 2. 热度趋势（heat_slope）

判断流量是否上升。

---

## 3. 生命周期阶段

| 阶段 | 特征 |
|------|------|
| 萌芽 | 占比低 + 增长快 |
| 成长 | 占比上升 + 热度上升 |
| 成熟 | 占比高 + 增长趋稳 |
| 衰退 | 占比下降 + 热度下降 |

---

# 七、风险指标（Risk Metrics）

---

## 1. 热度波动率（heat_volatility）

$$
std(weekly\_heat)
$$

判断流量稳定性。

---

## 2. 消失率（disappear_rate）

$$
\frac{\text{曾出现但近期消失}}{\text{曾出现}}
$$

识别短期噱头型题材。

---

# 八、新书驱动指标

---

## 1. 新书比例（new_entry_ratio）

$$
\frac{新书数量}{该题材总书数}
$$

判断平台是否在扶持该题材。

---

# 九、共现结构指标

---

## 1. 共现概率

$$
P(tagA, tagB)
$$

用于分析爆款组合。

---

# 十、跨平台差异指标

---

## 1. 占比差异（share_diff）

$$
share_{qidian} - share_{fanqie}
$$

---

## 2. 热度差异（heat_diff）

$$
avg\_heat_{qidian} - avg\_heat_{fanqie}
$$

---

## 3. 排名差异（rank_diff）

$$
mean\_rank_{qidian} - mean\_rank_{fanqie}
$$

---

# 十一、综合评分体系

用于构建决策引擎。

---

## 1. 稳定指数（Safe Score）

$$
w_1 \cdot \frac{1}{mean\_rank} + w_2 \cdot top\_appearance - w_3 \cdot rank\_std
$$

---

## 2. 机会指数（Chance Score）

$$
w_1 \cdot (-rank\_slope) + w_2 \cdot heat\_growth + w_3 \cdot share\_growth
$$

---

## 3. 蓝海指数（Blue Ocean Score）

$$
w_1 \cdot efficiency - w_2 \cdot tag\_share
$$

---

## 4. 风险指数（Risk Score）

$$
w_1 \cdot heat\_volatility + w_2 \cdot disappear\_rate
$$

---
