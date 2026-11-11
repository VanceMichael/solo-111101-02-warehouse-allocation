# 大促多渠道库存采集对账

离线采集工程使用 Python 标准库和 SQLite。adapter 读取本项目的真实来源格式，sources 提供可重复响应序列，capture 原子归档解析后的来源记录。已有 --capture 只记录原始观察，不作业务投影；--run 工作流是待扩展功能。

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

场景文件含 name、run_id、sources（有序根来源）、options、fixture_root；name 只作日志标签。expected 是公开最小预期，产品不应读取它。所有来源和选项应独立支持；四个场景不能替代全部边界测试。业务结果投影：inventory（按 sku 排序，含 available、channel_claim、gap）、watermark（最近完整快照，无完整版本为 null）、delta_watermark、coverage、published、ignored、quarantine。数组顺序不影响检查，公开预期数组是子集，但空数组要求确实为空。内部 SQLite 表、架构和调度实现由开发者选择。每个场景在同一状态库上用两个独立进程执行，以检查重启和重复输入；业务投影应保持一致，notifications 为数量时表示此次新建通知数，第二次应为0，为数组时表示持久化待处理通知清单。

## 验收与正式展示

```sh
python3 acceptance/check.py --contracts-only
python3 acceptance/check.py
```

contracts-only 仅检查材料结构，不能当成功能验收。第二条在初始版本会因尚无 --run 而失败，这是待开发功能的验收入口，不属于基线测试。完成开发后应四个场景均 PASS；每个场景独立临时目录，执行两次、每次进程超时5秒，八次总预算40秒，无固定 sleep。正式展示从干净状态执行基线测试和第二条命令，预算小于90秒；依赖和镜像预先准备，不能用旧日志代替运行。请另加参数变化及边界回归，不修改预期来迁就实现。
