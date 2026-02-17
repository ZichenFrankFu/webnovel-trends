# Tests Documentation
## 1. Spider Tests
### 1.1. qidian_test
#### 1.1.1 快速测试（抓取一个榜单的第一本小说，获取该小说的 metadata + 第一章，不写库，用于快速验证 HTML 结构是否发生变化）
```bash 
python qidian_test.py --test quick --rank_key "月票榜"
```
#### 1.1.2 完整测试（抓取一个榜单的前三本小说及其 metadata，抓取每本小说前5章正文，写入测试数据库）
```bash 
python qidian_test.py --test full --rank_key "月票榜"
```
#### 1.1.3 测试多个榜单（按多个榜单循环抓取，默认每榜单抓1本小说，并保存每本小说前3章正文）
```bash
python qidian_test.py --test multi_ranks --rank_keys "月票榜,畅销榜,推荐榜"
```
#### 1.1.4 智能补全测试（只测试抓取，不写入数据库）
```bash
python qidian_test.py --test smart_fetch --rank_key "月票榜" --pages 1 --chapter_n1 3 --chapter_n2 5
```
#### 1.1.5 qidian_test Args
##### 1.1.5.1 Test Modes (--test)
| test value | 说明 |
|-----------|------|
| `quick` | 快速 HTML 结构检测（不写库） |
| `full` | 单榜单完整流程测试（写库） |
| `multi_ranks` | 多榜单循环测试（写库） |

##### 1.1.5.2 Common Arguments
这些参数在不同测试模式下可选，未提供时会使用默认值
当 chapter_n > 0 时，将触发章节抓取与写库逻辑

| 参数 | 类型 | 说明 | 默认值 |
|----|----|----|----|
| `--rank_key` | string | 指定单个榜单 key | 从 `config.WEBSITES["fanqie"]["rank_urls"]` 自动选择一个常用榜单 |
| `--rank_keys` | string (CSV) | 多个榜单 key（仅 `multi_ranks` 使用） | 从 config 中选取 2～3 个常用榜单 |
| `--top_n` | int | 每个榜单抓取的小说数量 | `quick=1` / `full=3` / `multi_ranks=1` |
| `--chapter_n` | int | 每本小说抓取的章节数量 | `quick=1` / `full=5` / `multi_ranks=0` |
| `--pages` | int | 榜单翻页数量 | `1` |
| `--verbose` | flag | 输出详细日志与分段计时 | 关闭 |

### 1.2. fanqie_test
#### 1.2.1 测试反爬字体解密（仅番茄）
```bash
python fanqie_test.py --test decryption
```
#### 1.2.2 快速测试（抓取一个榜单的第一本小说，获取该小说的 metadata + 第一章，不写库，用于快速验证 HTML 结构是否发生变化）
```bash
python fanqie_test.py --test quick --rank_key "阅读榜科幻末世"
```
#### 1.2.3 完整测试（顺序执行所有测试类型, 覆盖：榜单、详情、章节、智能补全、去重、字体解密、多榜单, 写入测试数据库）
```bash
python fanqie_test.py --test full --rank_key "阅读榜科幻末世"
```
#### 1.2.4 测试多个榜单
```bash
python fanqie_test.py --test multi_ranks --rank_keys "阅读榜西方奇幻,阅读榜科幻末世,新书榜西方奇幻"
```
#### 1.2.5 智能补全测试（只测试抓取，不写入数据库）
```bash
python fanqie_test.py --test smart_fetch --rank_key "阅读榜西方奇幻" --pages 1 --chapter_n1 3 --chapter_n2 5
```
#### 1.2.6 小说改名测试
```bash
python fanqie_test.py --test fake_rename
```
#### 1.2.7 fanqie_test Args
##### 1.2.7.1 Test Modes (`--test`)
| test value | 说明 |
|-----------|------|
| `decryption` | 仅测试番茄小说字体 / 数字解密模块 |
| `quick` | 快速 HTML 结构检测（不写库） |
| `full` | 单榜单完整流程测试（写库） |
| `multi_ranks` | 多榜单循环测试（写库） |

##### 1.2.7.2 Common Arguments
这些参数在不同测试模式下可选，未提供时会使用默认值
当 chapter_n > 0 时，将触发章节抓取与写库逻辑。

| 参数 | 类型 | 说明                           | 默认值 |
|----|----|------------------------------|----|
| `--rank_key` | string | 指定单个榜单 key                   | 从 `config.WEBSITES["fanqie"]["rank_urls"]` 自动选择一个常用榜单 |
| `--rank_keys` | string (CSV) | 多个榜单 key（仅 `multi_ranks` 使用） | 从 config 中选取 2～3 个常用榜单 |
| `--top_n` | int | 每个榜单抓取的小说数量                  | `quick=1` / `full=3` / `multi_ranks=1` |
| `--chapter_n` | int | 每本小说抓取的章节数量                  | `quick=1` / `full=5` / `multi_ranks=0` |
| `--pages` | int | 榜单翻页数量(番茄小说榜单只有一页，固定为1）      | `1` |
| `--verbose` | flag | 输出详细日志与分段计时                  | 关闭 |

