# NFS 相互挂载测试方案

本文档定义当前 5 台服务器之间的 NFS 相互挂载方案，用于支持 `fustor-benchmark` 的真实 NFS 测试环境，尤其是：

- 基于传统 `find` 的跨 NFS submission 发现
- 客户端写入过程中的完整性检测
- fs-meta 在真实 NFS 挂载环境下的查询与新鲜度验证

本文档是测试方案的一部分，和 [BASELINE_TESTING.md](/root/repo/fustor-benchmark/BASELINE_TESTING.md) 配套使用。

## 1. 设计目标

本方案的目标是：

- 让 5 台服务器都提供 NFS 导出目录
- 让 5 台服务器都能作为 NFS 客户端访问其他服务器上的测试数据
- 明确禁止“本机客户端挂载本机导出的 NFS”
- 保持客户端挂载为可写，以便测试“上传中”状态和完整性检测逻辑
- 避免挂载点与本地数据目录重叠，防止递归扫描和测试污染

## 2. 当前服务器与导出目录

当前服务端集合：

- `10.0.82.144`
- `10.0.82.145`
- `10.0.82.146`
- `10.0.82.147`
- `10.0.82.148`

当前统一导出目录：

- `/data/fustor-nfs`

当前统一导出范围：

- `10.0.82.0/24`

建议统一导出参数：

```text
/data/fustor-nfs 10.0.82.0/24(rw,sync,insecure,no_subtree_check,no_root_squash)
```

说明：

- `rw` 是必须的，因为测试场景需要客户端执行真实写入，再验证完整性检测逻辑。
- `sync` 更适合这类一致性和完整性相关测试。
- `no_subtree_check` 可以减少 subtree 检查带来的额外开销。
- `no_root_squash` 便于实验环境中统一处理权限问题。

## 3. 挂载原则

本方案采用“全互挂，但跳过本机”的原则。

含义如下：

- 每台机器都继续作为 NFS server，对外导出自己的 `/data/fustor-nfs`
- 每台机器也作为 NFS client，挂载另外 4 台机器的 `/data/fustor-nfs`
- 每台机器严格不挂载自己导出的 NFS

禁止的行为：

- 在 `10.0.82.144` 上把 `10.0.82.144:/data/fustor-nfs` 挂到本机
- 在任意机器上把远端 NFS 挂到本机的 `/data/fustor-nfs` 下面

原因：

- 本机挂本机没有意义，会让“远端 NFS 访问路径”和“本地磁盘路径”混淆
- 挂到 `/data/fustor-nfs` 下会造成数据树递归嵌套，污染 benchmark 数据和目录扫描结果

## 4. 客户端挂载根目录

统一客户端挂载根目录：

- `/mnt/fustor-peers`

推荐挂载点命名：

- `/mnt/fustor-peers/nfs144`
- `/mnt/fustor-peers/nfs145`
- `/mnt/fustor-peers/nfs146`
- `/mnt/fustor-peers/nfs147`
- `/mnt/fustor-peers/nfs148`

规则：

- 每台机器只挂其中 4 个远端目录
- 本机对应的挂载点可以不创建；如果创建，也不得执行挂载

## 5. 挂载矩阵

### 5.1 `10.0.82.144`

挂载：

- `10.0.82.145:/data/fustor-nfs -> /mnt/fustor-peers/nfs145`
- `10.0.82.146:/data/fustor-nfs -> /mnt/fustor-peers/nfs146`
- `10.0.82.147:/data/fustor-nfs -> /mnt/fustor-peers/nfs147`
- `10.0.82.148:/data/fustor-nfs -> /mnt/fustor-peers/nfs148`

不挂载：

- `10.0.82.144:/data/fustor-nfs`

### 5.2 `10.0.82.145`

挂载：

- `10.0.82.144:/data/fustor-nfs -> /mnt/fustor-peers/nfs144`
- `10.0.82.146:/data/fustor-nfs -> /mnt/fustor-peers/nfs146`
- `10.0.82.147:/data/fustor-nfs -> /mnt/fustor-peers/nfs147`
- `10.0.82.148:/data/fustor-nfs -> /mnt/fustor-peers/nfs148`

不挂载：

- `10.0.82.145:/data/fustor-nfs`

### 5.3 `10.0.82.146`

挂载：

- `10.0.82.144:/data/fustor-nfs -> /mnt/fustor-peers/nfs144`
- `10.0.82.145:/data/fustor-nfs -> /mnt/fustor-peers/nfs145`
- `10.0.82.147:/data/fustor-nfs -> /mnt/fustor-peers/nfs147`
- `10.0.82.148:/data/fustor-nfs -> /mnt/fustor-peers/nfs148`

不挂载：

- `10.0.82.146:/data/fustor-nfs`

### 5.4 `10.0.82.147`

挂载：

- `10.0.82.144:/data/fustor-nfs -> /mnt/fustor-peers/nfs144`
- `10.0.82.145:/data/fustor-nfs -> /mnt/fustor-peers/nfs145`
- `10.0.82.146:/data/fustor-nfs -> /mnt/fustor-peers/nfs146`
- `10.0.82.148:/data/fustor-nfs -> /mnt/fustor-peers/nfs148`

不挂载：

- `10.0.82.147:/data/fustor-nfs`

### 5.5 `10.0.82.148`

挂载：

- `10.0.82.144:/data/fustor-nfs -> /mnt/fustor-peers/nfs144`
- `10.0.82.145:/data/fustor-nfs -> /mnt/fustor-peers/nfs145`
- `10.0.82.146:/data/fustor-nfs -> /mnt/fustor-peers/nfs146`
- `10.0.82.147:/data/fustor-nfs -> /mnt/fustor-peers/nfs147`

不挂载：

- `10.0.82.148:/data/fustor-nfs`

## 6. 挂载参数建议

由于这里不是只读查询测试，而是需要覆盖客户端写入后的完整性检测路径，因此客户端挂载必须保持可写。

推荐挂载参数：

```text
rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount
```

说明：

- `rw`：允许客户端执行真实写入，用于测试上传中和完整性检测逻辑
- `vers=3`：和当前 CentOS 7 环境兼容性最好，排障也最直接
- `proto=tcp,mountproto=tcp`：统一走 TCP
- `hard`：远端暂时不可达时不直接给应用返回软错误
- `timeo=600,retrans=2`：适合作为 benchmark 环境的保守默认值
- `_netdev`：告诉系统这是网络文件系统
- `nofail`：远端暂时不可用时不阻塞整个系统启动
- `noauto,x-systemd.automount`：避免 5 台机器开机时因互相依赖产生长时间阻塞

## 7. `/etc/fstab` 示例

### 7.1 `10.0.82.144`

```fstab
10.0.82.145:/data/fustor-nfs /mnt/fustor-peers/nfs145 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.146:/data/fustor-nfs /mnt/fustor-peers/nfs146 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.147:/data/fustor-nfs /mnt/fustor-peers/nfs147 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.148:/data/fustor-nfs /mnt/fustor-peers/nfs148 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
```

### 7.2 `10.0.82.145`

```fstab
10.0.82.144:/data/fustor-nfs /mnt/fustor-peers/nfs144 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.146:/data/fustor-nfs /mnt/fustor-peers/nfs146 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.147:/data/fustor-nfs /mnt/fustor-peers/nfs147 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.148:/data/fustor-nfs /mnt/fustor-peers/nfs148 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
```

### 7.3 `10.0.82.146`

```fstab
10.0.82.144:/data/fustor-nfs /mnt/fustor-peers/nfs144 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.145:/data/fustor-nfs /mnt/fustor-peers/nfs145 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.147:/data/fustor-nfs /mnt/fustor-peers/nfs147 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.148:/data/fustor-nfs /mnt/fustor-peers/nfs148 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
```

### 7.4 `10.0.82.147`

```fstab
10.0.82.144:/data/fustor-nfs /mnt/fustor-peers/nfs144 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.145:/data/fustor-nfs /mnt/fustor-peers/nfs145 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.146:/data/fustor-nfs /mnt/fustor-peers/nfs146 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.148:/data/fustor-nfs /mnt/fustor-peers/nfs148 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
```

### 7.5 `10.0.82.148`

```fstab
10.0.82.144:/data/fustor-nfs /mnt/fustor-peers/nfs144 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.145:/data/fustor-nfs /mnt/fustor-peers/nfs145 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.146:/data/fustor-nfs /mnt/fustor-peers/nfs146 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
10.0.82.147:/data/fustor-nfs /mnt/fustor-peers/nfs147 nfs rw,vers=3,proto=tcp,mountproto=tcp,hard,timeo=600,retrans=2,_netdev,nofail,noauto,x-systemd.automount 0 0
```

## 8. 启动与验证步骤

### 8.1 服务端

每台服务器需要满足：

- `rpcbind` 已启动并设置开机自启
- `nfs-server` 已启动并设置开机自启
- `/etc/exports.d/fustor-benchmark.exports` 存在
- `showmount -e localhost` 能看到 `/data/fustor-nfs`

### 8.2 客户端

每台客户端需要执行：

```bash
mkdir -p /mnt/fustor-peers
mkdir -p /mnt/fustor-peers/nfs144 /mnt/fustor-peers/nfs145 /mnt/fustor-peers/nfs146 /mnt/fustor-peers/nfs147 /mnt/fustor-peers/nfs148
systemctl daemon-reload
mount /mnt/fustor-peers/nfsXXX
```

注意：

- 只挂载远端 4 个目录
- 本机对应的挂载点不要执行 `mount`

### 8.3 验证

建议至少验证以下几项：

1. `showmount -e <peer-ip>` 可看到远端 `/data/fustor-nfs`
2. `mount | grep /mnt/fustor-peers` 可看到 4 条远端挂载
3. 在某台客户端向某个远端挂载点创建测试文件，服务端本地目录可见
4. 删除测试文件后，服务端本地目录同步消失

## 9. 与 benchmark 的关系

### 9.1 传统 NFS baseline

这个互挂方案主要服务于：

- `os_baseline`
- `os_integrity`

因为这两条路径依赖客户端在多个真实 NFS 挂载点上执行传统 `find` 和二次确认。

### 9.2 `explicit-roots` 配置方式

在任意一台客户端上运行 benchmark 时，建议使用：

```bash
--root-layout explicit-roots
```

然后把本机能看到的 4 个远端挂载点写成 `--root-specs`。

例如在 `10.0.82.145` 上：

```bash
--root-specs \
nfs144=/mnt/fustor-peers/nfs144,\
nfs146=/mnt/fustor-peers/nfs146,\
nfs147=/mnt/fustor-peers/nfs147,\
nfs148=/mnt/fustor-peers/nfs148
```

### 9.3 一个重要限制

由于本机不能挂本机 NFS，所以在“5 台互挂、无额外客户端”的前提下：

- 任意单台机器最多只能看到 4 个远端 NFS
- 不能在同一台机器上以“纯远端客户端视角”同时访问 5 个 NFS

如果测试目标必须是“单客户端同时看到 5 个 NFS”，则需要额外提供：

- 第 6 台专用客户端
- 或一台独立测试机/容器节点

## 10. 为什么这里必须用可写挂载

本方案明确不使用只读挂载。

原因不是为了通用访问便利，而是为了覆盖以下测试链路：

- 用户上传过程中目录先出现、文件逐步增加
- 文件大小和时间戳在上传过程中持续变化
- 客户端停止写入后，系统如何识别“静默窗口”
- `os_integrity` 或 fs-meta fresh 查询如何判断“已稳定”与“未稳定”

如果客户端使用只读挂载：

- 无法在真实客户端路径上模拟上传行为
- 无法验证“边写边查”的完整性检测逻辑
- 会把测试退化成纯查询 benchmark，而不是 GSA 审编场景里的“上传中识别 + 完整性判断”

## 11. 推荐使用方式

对于当前 5 台机器，推荐这样分工：

- 服务端数据真实落盘仍在各自本机的 `/data/fustor-nfs`
- 需要模拟“客户端上传”的测试，在某台机器上向远端 4 个挂载点执行写入
- 需要跑 benchmark 时，在某台机器上以 `explicit-roots` 指向这 4 个远端挂载点
- 需要全 5 NFS 同时视图时，再增加第 6 台专用客户端

这样可以同时满足：

- 不挂载本机导出的 NFS
- 保持真实远端 NFS 语义
- 支持客户端写入与完整性检测
- 避免把 benchmark 数据树和挂载树混在一起
