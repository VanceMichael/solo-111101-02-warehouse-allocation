# 大促多渠道库存采集对账

离线采集工程使用 Python 标准库和 SQLite。adapter 读取本项目的真实来源格式，sources 提供可重复响应序列，capture 原子归档解析后的来源记录。--capture 只记录原始观察，不作业务投影；--run 在同一状态库上完成按渠道水位的快照/增量采集、幂等裁定与可解释对账投影（规则见下文）。

## 环境与准备

推荐 Python 3.12.12，支持 Python 3.11+；requirements.lock 为空表示没有第三方包，运行、测试和验收均不下载数据或依赖。Dockerfile 提供同一 Linux 环境。需要容器时先在正式运行之外准备：

```sh
docker build -t q002-warehouse-allocation:initial .
docker run --rm q002-warehouse-allocation:initial
```

## 基线测试与原始采集

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q collector_02 tests acceptance
scratch=$(mktemp -d)
python3 -m collector_02.cli --database "$scratch/raw.sqlite3" --capture fixtures --route /warehouse
python3 -m collector_02.cli --database "$scratch/raw.sqlite3" --capture fixtures --route /warehouse
```

第一次 decoded=2、inserted=2，重抓同一观测 inserted=0。临时库不提交。tests 是已实现采集基础能力的回归测试，不代表扩展功能完成。

## 来源协议

仓库 CSV 是 physical 与 reserved 的同一水位观测，available=两者之差，跨仓求和；渠道 CSV 的 quantity 是已展示额度 channel_claim，不再从 available 扣一次。aliases 是渠道 SKU 到内部 SKU 的显式映射，未知 SKU 隔离，不做模糊匹配。gap=available-channel_claim。不能把不同水位的分区拼成完整快照；较低水位、reserved 大于 physical 或负数量都不替代已确认行。

expected_partitions 指明全量预期仓库；只有相同最高完整水位覆盖全部分区才 published=true，缺失分区或 503 返回 partial 而不清零旧库存。kind=delta 的 CSV 则按 event_id 对 physical、reserved 应用有符号增量，base_watermark 必须指向已完整采集快照，event_watermark 单独推进；负向调整可以合法，调整后库存越界才隔离。ADJ-1 重投或进程重启只扣减一次，重抓同一基准快照也不能抹掉已应用调整。相同 event_id 不同内容或同键同水位不同快照数量作为冲突隔离。将原始行、水位选择依据与差额一起输出以供补采。

FixtureTransport 每次进程创建从 routes.json 首条响应开始，同一路由多次 request 依次推进，最后一条可重复；calls 保存请求证据。源数据相对 fixture_root 读取，不能越出该目录。所有响应是本地合成数据，不发出公网请求。单场景 JSON 的 fixture_root 相对当前工作目录解析；在仓库根目录执行即可。run_id 是采集批次标识，name 仅作展示标签。

## 扩展接口与可观察输出

`python3 -m collector_02.cli --run 场景文件 --database 状态库 --output 结果文件`

场景文件含 name、run_id、sources（有序根来源）、options、fixture_root；name 只作日志标签。expected 是公开最小预期，产品不应读取它。所有来源和选项应独立支持；四个场景不能替代全部边界测试。单场景 JSON（如 `fixtures/balanced.json`）可直接作为场景文件；`fixtures/scenarios.json` 是验收脚本批量使用的数组。场景字段：

- `run_id`：采集批次标识，同一 run_id 的多次进程执行共享状态库中的同一批事实，用于重启与重跑；不同 run_id 互相隔离。
- `sources`：有序路由数组，同一进程内按序请求；重跑时每路路由从 `routes.json` 首条响应重新开始，最后一条响应可重复。
- `options.aliases`：渠道 SKU → 内部 SKU 的显式映射表；未列入的渠道 SKU 以 `unknown_sku` 隔离，不做任何模糊匹配。多个渠道 SKU 可显式映射到同一内部 SKU，额度按内部 SKU 求和。
- `options.expected_partitions`：全量预期仓库（分区）数组，决定完整水位与 `published`。

业务结果投影：inventory（按 sku 排序，含 available、channel_claim、gap）、watermark（最近完整快照，无完整版本为 null）、delta_watermark（已应用增量事件的最高 event_watermark，无则 null）、coverage（`full`/`partial`）、published、ignored、quarantine。另附 `inputs`（每次抓取采用的来源、attempt、HTTP 状态、ETag、响应体 sha256、解析行数）、`coverage_detail`（预期分区、完整水位集合、选中水位）与 `notifications`。数组顺序不影响检查，公开预期数组是子集，但空数组要求确实为空。内部 SQLite 表、架构和调度实现由开发者选择。每个场景在同一状态库上用两个独立进程执行，以检查重启和重复输入；业务投影应保持一致，notifications 为数量时表示此次新建通知数，第二次应为0，为数组时表示持久化待处理通知清单。本实现通知持久化在 `notifications` 表（含 quarantine、coverage 两类，按业务键去重），输出中的 `notifications` 为本次新建数量。

### 水位与裁定规则

- 仓库 CSV 同一行的 physical 与 reserved 属于同一水位观测；`available=physical-reserved`，按内部 SKU 跨仓求和。渠道 CSV 的 quantity 是已展示额度 channel_claim，只参与 gap，不再从 available 扣一次；`gap=available-channel_claim`。
- 一个水位只有覆盖 `expected_partitions` 全部分区才计入完整水位集合，`watermark` 取其中最高者；不同水位的分区绝不拼接。较低水位的行进入 `ignored`（`stale_watermark`，附原始行与选中水位依据），不替代已确认行；不完整水位进入 `ignored`（`incomplete_partitions`，附已到/缺失分区）供补采。
- 分区缺失或路由返回 503 时 `coverage=partial`、`published=false`，不清零任何已发布库存：无完整快照则 inventory 为空、watermark 为 null；曾发布完整快照则继续沿用。503 以 `source_unavailable` 记录，附 HTTP 状态与 attempt。补采缺失分区后再次运行同一 run_id 即可自动完成对账，无需手工清库。
- 负数量、reserved 大于 physical 的快照行不替代已确认行，按 `negative_quantity`、`reserved_exceeds_physical` 隔离并保留原始行。
- `kind=delta` 的 CSV 按 `event_id` 幂等受理：相同 event_id 相同内容的重投（含进程重启）只应用一次；相同 event_id 不同内容按 `event_conflict` 隔离，保留已接受与被拒两份内容。同键同水位不同快照数量按 `snapshot_conflict` 隔离，渠道同键同水位不同额度按 `channel_conflict` 隔离。
- 增量按 event_watermark 顺序应用有符号的 physical/reserved 调整；`base_watermark` 必须指向已完整采集的快照，否则事件先以 `delta_base_missing` 隔离，等基准补采后下一次运行自动生效。负向调整合法，只有调整后 physical/reserved 为负或 reserved 大于 physical 才以 `delta_out_of_bounds` 隔离，且不写入库存。换基准的事件会重新锚定到新基准快照，避免跨基准叠加；重抓基准快照不会抹掉已应用调整。`event_watermark` 独立于快照水位推进。
- 渠道 SKU 无显式映射以 `unknown_sku` 隔离；已映射内部 SKU 在仓库快照中没有对应行时以 `inventory_missing` 隔离，绝不把未知差异当作零。
- `ignored`/`quarantine` 每条均含 reason、sku、source、watermark、origin（ingestion/projection）、原始行 raw 与 detail（水位选择依据、接受/拒绝值、差额等），可直接用于补采与复核。

### 持久化与幂等

状态库（`RunStore`）只追加事实、按业务键去重，业务投影每次运行从持久事实整体重建：

- `snapshot_rows`：唯一键 (run_id, warehouse, sku, watermark)，重复抓取同观测不新增、不同数量冲突隔离。
- `channel_rows`：唯一键 (run_id, channel, channel_sku, watermark)，重复投递不重复累计额度。
- `delta_events`：唯一键 (run_id, event_id)，状态 applied/quarantined/pending 每次对账重算。
- `findings`：ignored/quarantine 裁定，投影类每轮重建、采集类持久保留；`notifications` 按业务键去重；`fetches` 保存每次抓取证据。

因此任何输入重放都不会重复扣加库存，失败后直接重跑同一命令即可。

## 验收与正式展示

```sh
python3 acceptance/check.py --contracts-only
python3 acceptance/check.py
```

contracts-only 仅检查材料结构，不能当成功能验收。第二条执行四个场景的完整功能验收：每个场景独立临时目录，执行两次、每次进程超时5秒，八次总预算40秒，无固定 sleep。正式展示从干净状态执行基线测试和第二条命令，预算小于90秒；依赖和镜像预先准备，不能用旧日志代替运行。请另加参数变化及边界回归，不修改预期来迁就实现。仓库内 `tests/test_run_idempotency.py`、`tests/test_run_aliases.py`、`tests/test_run_watermark.py` 覆盖重复导入与进程重启、显式别名与多规格编码聚合、冲突隔离、越界增量、503 不清零以及补采分区后自动完成对账等边界。
