# Baseline 测试说明

本文档说明 `fustor-benchmark` 中“传统 NFS `find` 基线”当前是如何建模、如何执行、如何调参、输出哪些结果，以及它覆盖了什么、没有覆盖什么。

需要先区分两层含义：

- 业务场景层：GSA 审编面对的是真实多 NFS 服务区和真实挂载路径。
- benchmark 实现层：当前仓库主要通过多 root 和容器化 fixture 来近似建模这条查询链路；是否接入真实 NFS，取决于你提供给 benchmark 的底层目录本身是不是 NFS 挂载。

本文档对应的 baseline 负载是当前代码中的：

- `os_baseline`
- `os_integrity`

它们的目标不是替代 fs-meta 路径，而是给 GSA 汇交流程提供一个可以复现的、面向业务动作的“传统做法”参照系。

## 1. 业务背景

GSA 汇交数据库审核的一个现实问题是：

- 用户可能把同一批次数据分散上传到多个 NFS 服务区。
- 审编人员需要尽快知道该批数据是否已经开始上传。
- 审编人员需要定位数据究竟落到了哪个 NFS 服务区、哪个挂载路径。
- 审编人员还需要判断当前状态是“仍在上传”还是“已经完整上传完毕”。

传统做法通常不是靠中心化索引，而是对多个 NFS 挂载区高频执行 `find`：

1. 先在所有 NFS 上搜索目标 submission 的目录。
2. 找到命中的路径后，再递归扫描该 submission 子树。
3. 如果需要确认“是否已传完”，就再等一段静默窗口后重复一次，比较结果是否稳定。

本项目的 baseline 测试就是专门对这条传统审核链路建模。

## 2. Baseline 的设计目标

本 baseline 主要回答下面几个问题：

1. 如果继续使用传统 `find`，跨多个 NFS 找到一个 submission 落点需要多久。
2. 如果继续使用传统“静默窗口 + 二次确认”，判断某个 submission 是否稳定需要多久。
3. 在给定并发下，传统 `find` 路径大致会产生多少 discovery 扫描、metadata 扫描和元数据解析量。

本 baseline 不追求模拟所有 NFS 内核细节，而是优先模拟 GSA 审编动作本身。

## 3. 当前 baseline 的两条测试路径

### 3.1 `os_baseline`

`os_baseline` 模拟“只做一次传统发现”的路径：

1. 从采样到的目标目录反推出 submission 标识。
2. 在所有配置的 NFS 根下执行 discovery `find`，查找该 submission 所在目录。
3. 对命中的 submission 目录做一次递归 metadata 扫描。
4. 统计总耗时和 I/O 代理指标。

它对应的问题是：

- 仅靠传统 `find`，审编人员多久能发现 submission 已经落在哪些 NFS 上。

### 3.2 `os_integrity`

`os_integrity` 模拟“传统双轮确认”的路径：

1. 第一次执行跨 NFS discovery + metadata snapshot。
2. 等待固定静默窗口 `integrity_interval`。
3. 第二次执行跨 NFS discovery + metadata snapshot。
4. 比较两次 inventory 是否完全一致。
5. 给出稳定性统计与总耗时。

它对应的问题是：

- 在传统审核思路下，为了更稳地判断“是否已经完整上传完毕”，需要付出多大等待与扫描代价。

## 4. 当前代码中的 workload 模型

### 4.1 目标发现与 submission 标识提取

benchmark 先基于生成数据树和 `target_depth` 发现一批叶子目录目标。默认 `target_depth=5`。

当前生成数据的目录结构是：

```text
<root>/upload/submit/{c1}/{c2}/{submission_uuid}/sub_X/data_XXXX.dat
```

因此在默认结构下：

- `target_depth=5` 通常命中 `{submission_uuid}` 目录。
- baseline 会把该目录相对路径拆开，取 `upload/submit/{c1}/{c2}/{submission_uuid}` 中的第 5 段作为 `submission_id`。

如果采样路径不符合这个结构，baseline 会退化为使用目标目录 basename 作为 submission 标识。

### 4.2 多 NFS 根的组织方式

多 NFS 通过 `named-roots` 模式建模：

- 每个 root 表示一个独立 NFS 服务区。
- root id 通过 `--root-ids` 指定。

例如：

```bash
--root-layout named-roots --root-ids nfs1,nfs2,nfs3,nfs4,nfs5
```

表示当前 baseline 会把：

- `TARGET_DIR/nfs1`
- `TARGET_DIR/nfs2`
- `TARGET_DIR/nfs3`
- `TARGET_DIR/nfs4`
- `TARGET_DIR/nfs5`

当作 5 个逻辑 root 来执行传统 discovery。

这里需要明确：

- 在业务语义上，这些 root 可以理解为 5 个 NFS 服务区。
- 在当前仓库实现上，这些 root 只是 benchmark 看到的 5 个根路径。
- 只有当这些根路径本身就是宿主机或容器内的真实 NFS 挂载点时，这次测试才会包含真实 NFS 的挂载行为。
- 如果这些根路径只是本地目录，那么这次测试建模的是“跨多个服务区遍历”的查询动作，而不是 NFS 协议本身。

### 4.3 `os_baseline` 的命令级模型

对每个请求，当前实现会执行：

1. Discovery：

```bash
find <root>/upload/submit -type d -name <submission_id> -print
```

每个 NFS 根执行一次。

2. Metadata 扫描：

```bash
find <candidate_dir> -printf "%P|%y|%s|%T@|%C@\n"
```

每个命中的 submission 目录执行一次。

基于这两步，baseline 记录：

- 扫描了多少个 root
- 实际做了多少次 discovery `find`
- 命中了多少个 candidate submission 目录
- 实际做了多少次 metadata `find`
- 一共解析了多少行 metadata
- 其中有多少文件、多少目录

### 4.4 `os_integrity` 的命令级模型

`os_integrity` 不是旧版的“第一次 `find` + 第二次逐文件 `os.stat`”了。

当前实现是：

1. 第一次完整执行 `os_baseline` 的 discovery + snapshot。
2. 等待 `integrity_interval` 秒。
3. 第二次完整执行 discovery + snapshot。
4. 比较两次 inventory 是否完全一致。

这里的 inventory key 形如：

```text
<root_label>:<relative_path>
```

比较维度包括：

- 文件/目录类型
- 大小
- `mtime`
- `ctime`

如果两次 inventory 完全一致，则记为 `stable=True`；否则为 `stable=False`。

## 5. 部署形态与真实 NFS 说明

当前仓库支持两种常见运行方式，但都不等价于“默认就是多 Docker 集群 + 真实 NFS 挂载”。

### 5.1 直接运行 `query`

直接执行：

```bash
uv run fustor-benchmark query ...
```

时，benchmark 只会读取你给定的 `TARGET_DIR` 和 `--root-layout/--root-ids`。

这意味着：

- benchmark 不负责创建 NFS 挂载。
- benchmark 也不判断这些目录背后是不是 NFS。
- 如果 `TARGET_DIR/nfs1 ... nfs5` 本身是已经挂好的真实 NFS 路径，那么 baseline 就是在真实 NFS 上运行。
- 如果它们只是本地测试目录，那么 baseline 只是逻辑上模拟“跨多个 NFS 服务区遍历”。

### 5.2 容器化 benchmark 脚本

仓库里的容器脚本是：

```text
scripts/run-container-query-benchmark.sh
```

它当前做的是：

- 创建一个 Docker network。
- 启动 1 个 `fs-meta` fixture 容器。
- 启动 benchmark runner 容器分 phase 发请求。
- 把宿主机数据目录以 bind mount 方式挂进容器：

```text
-v "$data_dir:/bench-data:ro"
```

- 在 `named-roots` 模式下，把 `/bench-data/nfs1`、`/bench-data/nfs2` 等路径配置成多个 root。

因此当前容器方案的准确描述应该是：

- 它是一个 Docker 化 benchmark harness。
- 它不是多节点 Docker 集群。
- 它默认也不是“容器内主动去挂真实 NFS”。
- 它默认使用的是宿主机目录 bind mount 进容器后的路径。

如果要做“真实 NFS 挂载 + 容器化 benchmark”，需要你额外准备真实 NFS 挂载，并把这些挂载路径暴露给 benchmark/fixture；当前仓库默认脚本并不负责 NFS mount 生命周期管理。

## 6. 测试参数

### 6.1 Query 命令相关参数

当前 baseline 使用和整体 benchmark 相同的 `query` 子命令。

关键参数如下：

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `--concurrency` | `20` | 并发 worker 数 |
| `--num-requests` | `200` | 总请求数 |
| `--target-depth` | `5` | 从数据树中采样目标目录的相对深度 |
| `--integrity-interval` | `60.0` | `os_integrity` 的静默窗口，单位秒 |
| `--root-layout` | `single-root` | 数据目录如何解释成单 NFS 或多 NFS |
| `--root-ids` | `nfs1,nfs2,nfs3` | 多 NFS 根 id 列表 |

说明：

- 要模拟 5 个 NFS，必须显式指定 5 个 `root_ids`。
- `os_baseline` 和 `os_integrity` 都通过 `ProcessPoolExecutor(max_workers=concurrency)` 并发执行。

### 6.2 数据生成参数

关键参数如下：

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `--num-dirs` | `1000` | 整个数据集中的 submission UUID 目录总数 |
| `--num-subdirs` | `4` | 每个 submission 下的子目录数 |
| `--files-per-subdir` | `250` | 每个子目录下的文件数 |
| `--root-layout` | `single-root` | 单根或多根布局 |
| `--root-ids` | `nfs1,nfs2,nfs3` | 多根布局时的 root 列表 |

默认数据量可推导为：

```text
total_files = num_dirs * num_subdirs * files_per_subdir
```

例如默认值下：

```text
1000 * 4 * 250 = 1,000,000 files
```

## 7. 推荐的 5 root baseline 启动方式

下面命令演示的是“5 个 root”的 baseline 运行方式。

- 如果这 5 个 root 是真实 NFS 挂载点，那么它可以用于真实 NFS baseline。
- 如果这 5 个 root 是本地目录，那么它用于逻辑建模和功能/性能对比。

### 7.1 生成 5 个 root 的数据

```bash
uv run fustor-benchmark generate capanix-benchmark-run/data \
  --root-layout named-roots \
  --root-ids nfs1,nfs2,nfs3,nfs4,nfs5 \
  --num-dirs 5000 \
  --num-subdirs 4 \
  --files-per-subdir 250
```

### 7.2 运行 baseline 与 fs-meta 对比

```bash
uv run fustor-benchmark query capanix-benchmark-run/data \
  --base-url http://127.0.0.1:18102 \
  --root-layout named-roots \
  --root-ids nfs1,nfs2,nfs3,nfs4,nfs5 \
  --concurrency 20 \
  --num-requests 200 \
  --target-depth 5 \
  --integrity-interval 60 \
  --username admin \
  --password admin
```

## 8. 输出结果

### 8.1 通用统计字段

`os_baseline` 和 `os_integrity` 都会输出通用统计项：

- `qps`
- `avg`
- `min`
- `max`
- `stddev`
- `p50`
- `p75`
- `p90`
- `p95`
- `p99`
- `raw`

说明：

- 延迟统计单位是毫秒。
- `qps = count / total_wall_time`。

### 8.2 baseline 专用字段

当前 baseline 还会额外输出下面这些字段：

| 字段 | 含义 |
|---|---|
| `model` | 当前 baseline 模型名，固定为 `multi_nfs_submission_discovery` |
| `nfs_root_count` | 当前配置的 NFS 根数量 |
| `poll_rounds_per_request` | 每个请求做了几轮 discovery；`os_baseline=1`，`os_integrity=2` |
| `total_roots_scanned_per_request` | 每个请求平均会遍历多少个 root |
| `total_roots_with_search_path_per_request` | 每个请求平均实际参与搜索的 root 数 |
| `total_discovery_find_calls_per_request` | 每个请求平均执行多少次 discovery `find` |
| `total_metadata_find_calls_per_request` | 每个请求平均执行多少次 metadata `find` |
| `candidate_count_per_poll` | 每轮 discovery 平均命中多少个 candidate submission 目录 |
| `metadata_lines_parsed_per_request` | 每个请求平均解析多少行 metadata 输出 |
| `file_count_per_poll` | 每轮 snapshot 平均覆盖多少个文件 |
| `dir_count_per_poll` | 每轮 snapshot 平均覆盖多少个目录 |

`os_integrity` 还会额外输出：

| 字段 | 含义 |
|---|---|
| `poll_interval_seconds` | 两轮 snapshot 之间的等待时间 |
| `stable_snapshot_count` | 两轮结果完全一致的请求数 |
| `unstable_snapshot_count` | 两轮结果不一致的请求数 |
| `stable_rate` | 稳定率 |

### 8.3 HTML 与 JSON 的区别

当前：

- HTML 报告会展示 `os_baseline` / `os_integrity` 的延迟和吞吐主指标。
- baseline 专用字段主要写在 JSON 中。

也就是说，如果需要分析传统 `find` 的 discovery 放大倍数、metadata 解析量、稳定率，应优先查看：

```text
capanix-benchmark-run/results/query-find.json
```

## 9. 这个 baseline 已经覆盖的内容

当前 baseline 已经覆盖：

1. 多 root discovery
   按 root 顺序对所有配置的根路径做 submission 查找；当这些根路径实际对应不同 NFS 挂载时，也就等价于多 NFS discovery。
2. 命中后递归扫描
   对 discovery 命中的 submission 子树做 metadata `find`。
3. 传统静默窗口确认
   通过两轮 snapshot + 固定等待窗口做稳定性判断。
4. 扫描放大代理指标
   记录 discovery `find` 次数、metadata `find` 次数、解析行数、覆盖文件数和目录数。

## 10. 当前 baseline 没有覆盖的内容

下面这些内容当前没有被显式建模：

1. NFS 协议层更新延迟
   没有模拟 attribute cache、close-to-open、一致性失效、服务端元数据复制等协议细节。
2. 网络 I/O 指标
   没有采集 NFS RPC 数、网络字节数、服务端 RTT。
3. CPU 细分指标
   没有拆分 `find` 进程 CPU、Python 解析 CPU、用户态/内核态 CPU。
4. 用户持续上传过程
   默认生成器写的是静态文件，不会模拟真实长时间写入、断点续传、跨 NFS 分批上传。
5. 业务清单校验
   当前只比对 snapshot 是否稳定，不比对“是否达到应交清单”。
6. 真正的轮询调度器
   当前 `num_requests` 表示发起多少个样本请求，不表示“每隔多少秒轮询一次全局队列”。
7. 默认的真实 NFS 挂载编排
   当前仓库不会自动搭建 NFS 服务端、自动执行客户端 mount、也不会自动形成多节点 Docker 集群。

## 11. 如何理解 `integrity_interval`

`integrity_interval` 当前代表的是：

- 审编人员或审核系统为了避免误判“上传完成”，主动等待的一段静默窗口。

它不是：

- NFS 元数据传播延迟
- 文件系统事件通知延迟
- 服务端索引构建延迟

所以在解释 `os_integrity` 结果时，应把它看成“传统人工/脚本确认成本”，而不是协议层固有延迟。

## 12. 如何把这个 baseline 用在 GSA 指标设计中

建议用它作为“传统做法”的参照，而不是最终真值模型。

重点关注：

1. `os_baseline.avg/p95/p99`
   表示仅靠多 NFS `find` 发现落点的时间成本。
2. `os_integrity.avg/p95/p99`
   表示传统“等一段再确认”的完整判断成本。
3. `total_discovery_find_calls_per_request`
   表示每个审核请求会把多少个 NFS discovery 扫描打到系统上。
4. `total_metadata_find_calls_per_request`
   表示命中后还会继续放大多少次递归扫描。
5. `metadata_lines_parsed_per_request`
   表示每个请求大致要搬运和解析多少行元数据。
6. `stable_rate`
   表示在当前静默窗口下，有多少请求能被判定为“稳定”。

如果你的目标是设计合理的 GSA 基准测试指标，建议把这些 baseline 字段和 fs-meta 的 `tree_materialized` / `find_on_demand_*` 一起对照看。

## 13. 代码位置

当前 baseline 实现在以下文件：

- workload 细节：
  [src/capanix_benchmark/tasks.py](/root/repo/fustor-benchmark/src/capanix_benchmark/tasks.py)
- 请求编排与结果聚合：
  [src/capanix_benchmark/runner.py](/root/repo/fustor-benchmark/src/capanix_benchmark/runner.py)
- CLI 参数：
  [src/capanix_benchmark/cli.py](/root/repo/fustor-benchmark/src/capanix_benchmark/cli.py)
- 报告模板：
  [src/capanix_benchmark/query_template.html](/root/repo/fustor-benchmark/src/capanix_benchmark/query_template.html)

## 14. 当前文档适用范围

本文档描述的是“当前仓库实现”的 baseline 行为，不是抽象设计稿。

如果后续继续扩展：

- 上传过程模拟
- 真实轮询调度
- NFS 协议层缓存/延迟注入
- per-user / per-batch 清单比对

则需要同步更新本文档。
