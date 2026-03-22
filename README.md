# fustor-benchmark

`fustor-benchmark` is a benchmark toolkit for current `fs-meta HTTP v1` surfaces.

Dedicated baseline workload documentation:

- [BASELINE_TESTING.md](/root/repo/fustor-benchmark/BASELINE_TESTING.md)

## Scope

It compares:

- `os_baseline`: cross-NFS submission discovery with traditional `find`
- `os_integrity`: cross-NFS double-sweep + silence-window validation
- `tree_materialized`: `GET /api/fs-meta/v1/tree`
- `find_on_demand_success`: `GET /api/fs-meta/v1/on-demand-force-find` on the success path
- `find_on_demand_contention`: same endpoint under intentional same-group contention

## 业务场景与用户痛点

GSA 汇交数据库的一个典型痛点是：同一批次的数据可能被用户分散上传到多个 NFS 服务区。审核流程不是只关心“文件有没有出现”，而是需要尽快回答下面几类问题：

- 用户是否已经开始上传。
- 用户实际上传到了哪个 NFS 服务区、哪个挂载路径。
- 用户是“部分上传中”还是“已经完整上传完毕”。
- 在高频轮询、多人审核、多个服务区并行检查的情况下，查询链路还能否稳定工作。

很多现网流程的基线做法是对所有 NFS 挂载路径高频执行 `find`，靠遍历结果去发现实际落盘位置，再结合静默窗口判断“是否已经传完”。这种方式有几个直接问题：

- 要跨多个挂载点反复扫描，延迟和 CPU/IO 开销都很高。
- 一致性判断通常需要二次采样甚至等待静默窗口，审核链路会被物理等待时间拖慢。
- 当多个审核任务同时盯同一个用户或同一个服务区时，遍历放大更明显。

这个 benchmark 就是针对这个业务动作设计的，不是在抽象比较“文件系统查询快不快”，而是在比较“发现用户上传位置并判断上传阶段”这条链路的几种实现代价：

- `os_baseline`：等价于审核系统拿着 submission 标识，在所有配置的 NFS 服务区上执行传统 `find` 去发现实际落点。
- `os_integrity`：等价于审核系统跨所有 NFS 做两轮 `find` 发现与元数据快照，中间等待静默窗口后再确认上传是否稳定。
- `tree_materialized`：等价于先看 fs-meta 的物化索引，快速判断用户数据可能已经落在哪个服务区。
- `find_on_demand_success`：等价于在需要强制新鲜视图时，直接做 fs-meta 的 fresh 查询。
- `find_on_demand_contention`：专门测“多人同时盯同一组 NFS 服务区”时 fresh 查询的冲突和退让情况。

如果使用 `--root-layout=named-roots --root-ids=nfs1,nfs2,nfs3`，报告里的多个 group 就可以直接理解为多个 NFS 服务区，这和 GSA 审核场景是对齐的。

## HTML 报告格式与阅读方式

HTML 报告默认输出到 `capanix-benchmark-run/results/query-find.html`。它面向的不是开发者调试页面，而是给使用者快速判断“哪条查询链路更适合审核发现与上传完成识别”。

### 1. 页面结构

报告固定分为下面几个区域：

1. `Info Bar`
   显示测试时间、数据规模、请求数、目标深度、并发度、完整性校验静默窗口。
2. `Latency Summary`
   顶部卡片展示 `OS Baseline`、`OS Integrity`、`tree (materialized)` 的平均延迟，以及 `tree` 相对 `OS Integrity` 的延迟收益。
3. `Throughput Summary`
   顶部卡片展示同三条路径的 QPS，以及 `tree` 相对 `OS Integrity` 的吞吐收益。
4. `Charts`
   包含吞吐柱状图、平均延迟柱状图、以及 `Min/P50/P75/P90/P95/P99/Max` 延迟曲线图。
5. `Detailed Metrics Comparison (ms)`
   以表格方式同时列出四条主路径：`OS Baseline`、`OS Integrity`、`tree (materialized)`、`on-demand-force-find (success path)`。
6. `On-demand Contention`
   单独展示 fresh 查询在同组竞争下的成功数、`NOT_READY/conflict` 数、其他错误数。
7. `Test Methodology`
   用自然语言解释每条测试路径到底在模拟什么动作。

说明：

- 顶部两组 Summary 卡片当前重点强调 `tree` 相对 OS 路径的收益。
- `on-demand-force-find` 的性能数据主要出现在 `Detailed Metrics Comparison` 和 `On-demand Contention` 两个区域。

### 2. 主要字段含义

- `Timestamp`：本次测试生成报告的时间。
- `Data Scale`：当前查询范围内的总文件数和目录数，来自 `/stats` 聚合结果。
- `Requests`：每个 benchmark phase 的总请求数。
- `Target Depth`：从生成数据根目录向下抽样叶子目录时的目标深度。
- `Concurrency`：请求发起侧的并发度。
- `Integrity Intv`：`os_integrity` 的静默窗口，表示两次采样之间的物理等待时间。

### 3. 四类核心测试路径怎么理解

- `OS Baseline`
  在所有配置的 NFS 根上执行传统 `find`，先发现 submission 落点，再对命中的 submission 子树做一次递归元数据扫描。
- `OS Integrity`
  先跨所有 NFS 做一次 discovery + 元数据快照，再等待静默窗口，再做第二轮 discovery + 元数据快照；它更接近“审核要确认上传是否稳定完成”的传统做法。报告里它被当作强一致校验基线，但页面也明确标注这是 best-case。
- `tree (materialized)`
  走 fs-meta 物化索引路径，适合回答“用户可能已经传到哪个服务区、哪个路径上了”这类高频发现问题。
- `on-demand-force-find (success path)`
  走 fs-meta fresh 路径，适合回答“现在立刻强制看一眼最新状态”这类诊断问题。

### 4. 建议重点关注的指标

针对 GSA 审核场景，建议按下面顺序读报告：

1. 先看 `Data Scale`、`Requests`、`Concurrency`
   确认这次测试规模是否接近你关心的审核负载。
2. 再看 `tree (materialized)` 对 `OS Integrity` 的 `Avg Latency` 和 `QPS`
   这反映“常规审核轮询”从反复跑 `find` 迁移到物化查询后，是否能更快发现用户上传位置。
3. 再看 `on-demand-force-find (success path)` 的 `Avg/P95/P99`
   这反映“当物化结果还没追上时，临时强制查最新状态”的代价。
4. 最后看 `On-demand Contention`
   如果 `NOT_READY / conflict` 高，说明同一个 NFS 服务区上的 fresh 查询会互相排队或冲突，这通常意味着审核并发需要按 group 做限流或分流。

### 5. 报告中的业务解释口径

可以按下面口径解读结果：

- `tree` 更适合做高频、常规、全局发现。
- `on-demand-force-find` 更适合做少量、诊断式、强制新鲜确认。
- `OS Integrity` 代表传统“靠多次 `find` 加等待窗口确认上传完成”的基线成本。
- `find_on_demand_contention` 不是普通失败率，而是专门反映同组 fresh 查询的竞争行为。

换句话说，这份报告回答的核心问题是：

- 在多 NFS 服务区并存时，系统能多快定位用户的实际上传落点。
- 在需要判断“是否已经完整上传完毕”时，继续依赖高频 `find` 要付出多大代价。
- 在物化索引和 fresh 查询两条 fs-meta 路径之间，业务应该把哪条链路用于常规审核，哪条链路用于补充确认。

## Quick Start

```bash
uv run fustor-benchmark generate capanix-benchmark-run/data --num-dirs 200
uv run fustor-benchmark query capanix-benchmark-run/data \
  --base-url http://127.0.0.1:18102 \
  --username admin --password admin
```

The benchmark now follows the current fs-meta product API:

- management login uses `POST /api/fs-meta/v1/session/login`
- when `--username/--password` is provided, the benchmark creates a temporary query API key via `POST /api/fs-meta/v1/query-api-keys`
- query traffic uses that query API key on `GET /tree`, `GET /stats`, and `GET /on-demand-force-find`
- materialized readiness waits on `/stats` becoming queryable instead of polling legacy `/health`

If you already have a query credential, you can skip management login:

```bash
uv run fustor-benchmark query capanix-benchmark-run/data \
  --base-url http://127.0.0.1:18102 \
  --query-api-key "$FS_META_QUERY_API_KEY"
```

## Query Contract

- `/tree` and `/on-demand-force-find` are called with `group_order`, `group_page_size`, and `entry_page_size`
- the benchmark follows PIT pagination with `pit_id` and `entry_after` until the selected group page has been fully read
- default query shaping matches the current spec baseline: `group_order=group-key`, `group_page_size=1`, `entry_page_size=1000`
- `--root-layout=named-roots --root-ids=nfs1,nfs2,nfs3` makes the benchmark treat `TARGET_DIR/nfs1`, `TARGET_DIR/nfs2`, and `TARGET_DIR/nfs3` as separate fs-meta groups
- in `named-roots` mode, `--num-dirs` still means the total UUID directory count across the whole dataset, not per root; the generator distributes that total across the configured roots
- `find_on_demand_success` explicitly targets one `group` per request and runs at most one inflight request per group; cross-group concurrency is real wall-clock throughput up to `min(concurrency, group_count)`
- legacy query knobs such as `limit`, `best`, and `best_strategy` are no longer used

## Runtime Modes

- `external` (default): connect to an already running fs-meta service.
- `local`: execute caller-provided `--start-cmd` / `--stop-cmd` lifecycle hooks.

## Notes

- All benchmark artifacts are written to `./capanix-benchmark-run/`.
- The data generator refuses to run unless target path is inside `capanix-benchmark-run`.
- When the benchmark creates a temporary query API key itself, it revokes that key after the run finishes.

## Container Benchmark

To run both the benchmark process and the fs-meta fixture inside containers:

```bash
./scripts/run-container-query-benchmark.sh
```

This workflow:

- builds a benchmark image from this repository
- builds an fs-meta fixture image from `../fustor/target/debug/fs_meta_api_fixture`
- generates dataset inside the benchmark container
- runs the fs-meta fixture in a dedicated container on an isolated Docker network
- runs the benchmark phases from a second container against `http://fustor-benchmark-fsmeta:18102`
- batches PIT-backed `/tree` and `/on-demand-force-find` phases and restarts the fs-meta container between batches to avoid the upstream `PIT_MAX_SESSIONS=128` limit from contaminating later requests

Useful overrides:

```bash
CONCURRENCY=8 NUM_REQUESTS=80 INTEGRITY_INTERVAL=1 ./scripts/run-container-query-benchmark.sh
UPSTREAM_ROOT=/abs/path/to/fustor HOST_PORT=18184 ./scripts/run-container-query-benchmark.sh
ROOT_LAYOUT=single-root ROOT_IDS=bench-root ./scripts/run-container-query-benchmark.sh
PIT_PHASE_BATCH_REQUESTS=64 ./scripts/run-container-query-benchmark.sh
```

Data reuse rules:

- the container script writes a layout manifest under `capanix-benchmark-run/data/.layout-manifest.json`
- the container script also writes `capanix-benchmark-run/fixture-manifest.json` with the staged `fs_meta_api_fixture` sha256/mtime/size
- if you change `ROOT_LAYOUT`, `ROOT_IDS`, `NUM_DIRS`, `NUM_SUBDIRS`, or `FILES_PER_SUBDIR`, the script now refuses to reuse the old dataset
- set `RECREATE_DATA=1` to rebuild the dataset when those parameters change

Artifacts are still written to the host under `./capanix-benchmark-run/`, with fs-meta container logs in `./capanix-benchmark-run/container-logs/`.
Intermediate per-phase chunk JSON files are written under `./capanix-benchmark-run/results/phase-chunks/`.
