# etcd_k8s_toolkit

Cargo 工作区包含：

- `etcd_proxy`：基于 tonic 的 etcd 代理，支持缓冲 JSONL 请求日志；
- `etcd_k8s_bench`：Kubernetes 形状的 etcd API 压测器。

## 启动 etcd_proxy

```sh
cargo run --release -p etcd_proxy -- \
  --etcd-endpoint 'http://[::1]:2379' \
  --listen-addr '[::1]:50051' \
  --max-message-size 67108864 \
  --log-file etcd_proxy.jsonl
```

完整日志和 transport 参数见 `cargo run -p etcd_proxy -- --help`。

## etcd_k8s_bench 的建模边界

benchmark 将 etcd wire 行为对齐到 Kubernetes v1.35.5：

- linearizable point/prefix `Range` 和固定 revision 分页；
- Create、Update、point Delete 的 ModRevision CAS `Txn`；
- 带 `prev_kv`、`progress_notify` 和 require-leader metadata 的资源根 `Watch`；
- Event/masterlease 的 `LeaseGrant → Txn Put.lease`；
- 通过 `compact_rev_key` 协调的 logical `Compact`；
- `Maintenance/Status`。

它不是 API Server、controller 模拟器或逐事件 trace 重放器。profile 定义精确拓扑、资源公式、更新放大比和阶段时长。工具先创建全部计划对象并等待全局创建屏障，再完成精确的成功逻辑更新数；deadline 到期仍有缺口则运行失败。

## 运行 benchmark

必须使用专用空 etcd。工具先执行 full-keyspace、count-only 预检，只要存在任意 key 就拒绝运行。

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s \
  --endpoint 'http://127.0.0.1:2379' \
  --nodes 2000 \
  --pods 40000 \
  --pod-size 4096 \
  --metrics-listen '127.0.0.1:9898'
```

默认 `k8s` profile 为每个 Node 生成一个 Coordination NodeLease。显式关闭 NodeLease：

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s-no-node-lease \
  --endpoint 'http://127.0.0.1:2379'
```

`--config <path>` 加载经过审查的自定义 profile，与显式 `--profile` 互斥。

`--time-scale` 同时缩放更新阶段时长和周期任务间隔，不改变对象数或成功更新目标。值越小，表示在越短时间内施加同一份精确更新配额。更新目标在硬 deadline 的前 95% 均匀发出，后 5% 只用于完成已入队工作，不是阶段延长。

## 默认精确压力形状

默认 `k8s` profile 定义：

- 2,000 个 Node，Pod key 总数精确为 40,000；
- Event 以 Pod 为基数、倍率 `1/1`，因此精确创建 40,000 个 Event key；
- Pod 更新放大比为 10，每个 Pod 必须成功更新恰好 10 次；
- 每 Node 一个 NodeLease；
- 一个覆盖 2,000 个 eligible Node 的 DaemonSet，其 Pod 已包含在 40,000 Pod 总量中；
- 100 个 Service，每个选择 100 个 backend；
- ideal packing 下 100 个 EndpointSlice，每片 100 endpoints；
- controller 对象数、更新放大、Range 混合和周期节奏采用 trace 默认输入。

Kubernetes 官方文档定义资源关系，但不规定 Service、Event 或 controller 对象的通用比例。因此这些值必须是显式 profile 输入。EndpointSlice ideal packing 是确定性压测模型；真实 controller 的变更历史可能留下更多未填满的 slice。

## 生成 DaemonSet-update 压力 profile

```sh
cargo run --release -p etcd_k8s_bench -- gen_profile \
  --nodes 10000 \
  --batch-size 500 \
  --txn-rps 2000 \
  --name rolling-update
```

生成结果使用每 Node 一个 Pod、每 Pod 四次成功更新、每 Node 三个 Event，并按配置 wave 为 DaemonSet 生成每波两次更新。`--txn-rps` 只用于推导硬更新阶段时长，不作为运行时第二套配额真相源。命令不会覆盖已有文件。该形状的 Pod 数由 Node 数推导，因此 `gen_profile` 拒绝 `--pods`。

运行生成的 profile：

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --config etcd_k8s_bench/workloads/rolling-update.json
```

## 指标与对账

Prometheus 指标位于 `/metrics`。主要层次：

- `etcd_k8s_bench_resource_objects{resource,state="planned|succeeded|deficit"}`；
- `etcd_k8s_bench_resource_updates{resource,state="target|succeeded|deficit"}`；
- `etcd_k8s_bench_operations_total`：逻辑操作结果；
- `etcd_k8s_bench_requests_total`：物理 RPC 结果；
- `etcd_k8s_bench_txn_total` 和 `etcd_k8s_bench_cas_retries_total`；
- `etcd_k8s_bench_range_requests_total{shape,outcome}`；
- Watch、lease、heartbeat、revision、compactor、inflight 和 dropped 指标。

CAS conflict、retry、result unknown 和 queue drop 不消耗成功更新配额。key、ID、endpoint 和错误文本不会成为指标标签。

详细设计见 [`etcd_k8s_bench/DESIGN_zh.md`](etcd_k8s_bench/DESIGN_zh.md)，英文使用说明见 [README.md](README.md)。
