# 规模拐点判定说明

本文档说明如何用 `fustor-benchmark` 的多轮结果，得出这样一类结论：

- 在 `5亿` 文件规模及以下，新方案性能不比传统 baseline 差。
- 超过 `5亿` 后，某个指标开始明显变差。

这里的“结论”必须是阈值化、可复现、可审计的，不应依赖人工肉眼看图。

## 1. 先明确结论类型

这个结论不是“系统整体感觉变慢了”，而是：

1. 在某个锚点规模，例如 `500,000,000` 文件，新方案相对 baseline 满足“不劣于”阈值。
2. 在锚点之后的更大规模上，至少一个核心指标连续跨过退化阈值。

因此，判定需要同时回答两个问题：

1. `500M` 规模是否“不劣于 baseline”。
2. `>500M` 之后是否出现“性能拐点”。

## 2. 推荐的对照关系

建议把业务动作拆成两条线，而不是混成一个总分：

- 上传落点发现：
  `tree_materialized` 对 `os_baseline`
- 上传完成确认：
  `find_on_demand_success` 对 `os_integrity`

原因很简单：

- `os_baseline` 代表传统跨 NFS `find` 定位 submission 落点。
- `os_integrity` 代表传统“双轮扫描 + 静默窗口”确认是否稳定。
- `tree_materialized` 更像常规审核的发现路径。
- `find_on_demand_success` 更像需要强制确认时的 fresh 路径。

## 3. 推荐的主判定指标

### 3.1 上传落点发现

- `discovery_p95_ratio = tree_materialized.p95 / os_baseline.p95`
- `discovery_qps_ratio = tree_materialized.qps / os_baseline.qps`

### 3.2 上传完成确认

- `integrity_p95_ratio = find_on_demand_success.p95 / os_integrity.p95`
- `integrity_qps_ratio = find_on_demand_success.qps / os_integrity.qps`

### 3.3 守门指标

- `ondemand_success_rate = find_on_demand_success.success_rate`
- `ondemand_contention_not_ready_rate = find_on_demand_contention.not_ready_count / attempted_count`
- `os_integrity_stable_rate = os_integrity.stable_rate`

说明：

- 前 4 个是核心性能指标。
- `ondemand_success_rate` 和 `ondemand_contention_not_ready_rate` 用来防止“靠大量失败/退让隐藏真实代价”。
- `os_integrity_stable_rate` 不是新方案性能指标，而是环境守门指标。它太低时，说明这轮测试本身并不适合拿来做“上传完成判定”的强结论。

## 4. 推荐阈值

下面是当前建议的默认阈值，也是 `scale-breakpoint` 命令的默认值。

### 4.1 “不劣于 baseline”阈值

- `discovery_p95_ratio <= 1.10`
- `discovery_qps_ratio >= 0.90`
- `integrity_p95_ratio <= 1.10`
- `integrity_qps_ratio >= 0.90`
- `ondemand_success_rate >= 0.99`
- `ondemand_contention_not_ready_rate <= 0.05`
- `os_integrity_stable_rate >= 0.95`

解释：

- p95 最多允许比 baseline 慢 `10%`
- QPS 最多允许比 baseline 低 `10%`
- fresh 路径成功率至少 `99%`
- 同组竞争下 `NOT_READY` 比例不超过 `5%`
- baseline 的稳定快照比例至少 `95%`

### 4.2 “规模拐点”阈值

- `p95 ratio > 1.25`
- `qps ratio < 0.80`
- `contention_not_ready_rate > 0.15`
- 至少连续 `2` 个规模点满足，才确认拐点

解释：

- 这不是轻微抖动，而是显著退化。
- 使用“连续两个规模点”是为了避免偶发噪声把单点异常误判成拐点。

## 5. 推荐的规模点

不要只测 `500M` 和 `1B` 两点。建议至少：

- `100M`
- `300M`
- `500M`
- `700M`
- `1B`

并且要求：

- 同一套 NFS 环境
- 同一挂载拓扑
- 同一 `target_depth`
- 同一 `concurrency`
- 同一 `num_requests`
- 同一 `integrity_interval`
- 同一批 benchmark 代码版本

如果条件变了，就不应该把这些结果放到同一条规模曲线上解释。

## 6. 如何使用工具

对每个规模点先跑一轮标准 `query` benchmark，得到对应的 `query-find.json`。

然后执行：

```bash
uv run fustor-benchmark scale-breakpoint \
  run-100m/results/query-find.json \
  run-300m/results/query-find.json \
  run-500m/results/query-find.json \
  run-700m/results/query-find.json \
  run-1b/results/query-find.json \
  --anchor-files 500000000 \
  --output-json capanix-benchmark-run/results/scale-breakpoint.json \
  --output-md capanix-benchmark-run/results/scale-breakpoint.md
```

工具会输出两类结论：

1. 锚点结论
   例如：`500M` 规模点是否满足“不劣于 baseline”
2. 拐点结论
   例如：`700M` 开始 `discovery_p95_ratio` 连续两点超过阈值，因此认为 `>500M` 后出现规模拐点

## 7. 如何读结果

`scale-breakpoint` 结果分三部分：

### 7.1 `Scale Table`

每个规模点都会展示：

- `Discovery P95 Ratio`
- `Discovery QPS Ratio`
- `Integrity P95 Ratio`
- `Integrity QPS Ratio`
- `Success Rate`
- `Contention NOT_READY`
- `Stable Rate`
- `Verdict`

其中 `Verdict=pass` 表示该规模点满足“可下结论”的全部阈值。

### 7.2 `Anchor Verdict`

重点看：

- `anchor_scale_not_worse_than_baseline`
- `anchor_scale_conclusion_ready`
- `all_scales_up_to_anchor_not_worse_than_baseline`

如果这些是 `true`，就可以说：

- 在 `500M` 规模点上，新方案不劣于 baseline
- 或更强一点：在 `<=500M` 的测试区间内，新方案都不劣于 baseline

### 7.3 `Breakpoint Verdict`

重点看：

- `breakpoint_detected`
- `overall_breakpoint.metric`
- `overall_breakpoint.first_violation_files`
- `overall_breakpoint.confirmed_at_files`

如果这里显示：

- `metric=discovery_p95_ratio`
- `first_violation_files=700,000,000`
- `confirmed_at_files=1,000,000,000`

则可以写：

- 超过 `500M` 后，上传落点发现路径的 `p95` 相对 baseline 出现显著退化
- 该退化从 `700M` 开始出现，并在 `1B` 规模继续存在，因此可视为规模拐点

## 8. 推荐的结论写法

推荐：

- 在 `500M` 规模点，`tree_materialized` 与 `find_on_demand_success` 分别相对 `os_baseline` 与 `os_integrity` 满足预设阈值，因此判定该规模下“不劣于 baseline”。
- 在 `700M` 与 `1B` 规模点，`discovery_p95_ratio` 连续超过 `1.25`，因此判定 `>500M` 后出现上传落点发现路径的性能拐点。

不推荐：

- 超过 `500M` 后系统变慢了。
- 大规模时整体性能不太行。

后两种写法不可审计，也无法和报告字段直接对应。

## 9. 能否得出这个结论

可以，但只能得出“经验性、阈值化”的结论，不能仅靠当前 benchmark 得出完整因果解释。

当前 benchmark 能支持：

- 哪个规模点开始越过阈值
- 是哪类指标先出问题
- 是延迟退化、吞吐退化，还是 `NOT_READY` 上升

当前 benchmark 还不能单独解释：

- 为什么会退化
- 是 NFS RPC、服务端 CPU、客户端 CPU、网络，还是元数据缓存导致

如果需要解释“为什么”，还要补采：

- NFS client/server CPU
- iowait
- 网络吞吐
- NFS RPC 指标
- 磁盘与 inode 压力

## 10. 当前代码支持情况

当前仓库已经提供：

- 多轮 `query-find.json` 结果输入
- `scale-breakpoint` 命令
- JSON 结构化分析输出
- Markdown 判定报告输出

命令入口在：

- [cli.py](/root/repo/fustor-benchmark/src/capanix_benchmark/cli.py)

分析逻辑在：

- [scale_breakpoint.py](/root/repo/fustor-benchmark/src/capanix_benchmark/scale_breakpoint.py)
