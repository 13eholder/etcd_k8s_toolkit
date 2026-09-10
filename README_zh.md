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

benchmark 是 **Kubernetes 形状的合成压力生成器**，对齐 Kubernetes
v1.35.5 使用 etcd 时可观察的 wire 形状：

- linearizable point/prefix `Range` 和固定 revision 分页；
- Create、Update 以及保留的低层 point Delete 的 ModRevision CAS `Txn`；
- 带 `prev_kv`、`progress_notify` 和 require-leader metadata 的资源根
  `Watch`；
- Event/masterlease 通过 `LeaseGrant → Txn Put.lease` 使用 native lease；
- 通过 `compact_rev_key` 协调的 logical `Compact`；
- `Maintenance/Status`。

profile 定义精确拓扑、资源公式、synthetic value 大小、成功写放大、
Range 混合、Watch 订阅和实际时间 timeline。工具先创建全部计划对象并
等待全局创建屏障，再调度配置的成功逻辑更新数。这是施压目标而非
controller 实现：没有 kube-apiserver、watch-cache、controller 拓扑、
对象因果状态机或逐事件 trace 重放，`rolling-update` 也不执行真实
rollout 删除/替换。结果不能直接解释为经过校准的 Kubernetes 容量或
拓扑结论。

## 运行 benchmark

必须使用专用空 etcd。进入 scheduler 前，工具先执行 full-keyspace、
count-only 预检，只要存在任意 key 就拒绝运行。配置、exporter、连接、
初始 `Status` 和 preflight 错误都不属于 scheduler 结果协议，而是写入
stderr 并以退出码 1 结束。

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s \
  --endpoint 'http://127.0.0.1:2379' \
  --nodes 2000 \
  --pods 40000 \
  --pod-size 4096 \
  --metrics-listen '127.0.0.1:9898'
```

默认 `k8s` profile 为每个 Node 生成一个 Coordination NodeLease。显式
关闭 NodeLease：

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s-no-node-lease \
  --endpoint 'http://127.0.0.1:2379'
```

`--config <path>` 加载经过审查的自定义 profile，与显式 `--profile`
互斥。

profile 只使用一套实际时间：timeline 时长、Range RPS、周期间隔和
native lease TTL 均不做倍率缩放。需要调整压力时，直接修改显式时长、
速率或更新目标。更新目标在**应用更新阶段**硬 deadline 的前 95% 分布
派发，后 5% 只是成功更新的完成余量，不是整个运行的墙钟上限。timeline
结束后，已 accepted 的后台工作在自己的 `operation_timeout` 下 drain，
随后 Watch teardown 也在自己的 `operation_timeout` 下执行。活动派发/
更新监督期间的 Watch terminal 或 worker 错误会中止该阶段；正常 timeline
后 drain 中晚到的 Watch terminal 会在 teardown 时收集，最终仍使
`achieved=false`。队列背压可能延迟派发并产生追赶补发，因此不保证严格
均匀的派发。

工具不承诺吞吐或延迟性能保证；成功运行表示所选合成 profile 的对账
目标达成。

## 默认精确压力形状

内置 `k8s` 和 `k8s-no-node-lease` 共享应用对象、Range 混合、时间配置
和 Watch 根；区别只有 Coordination NodeLease 流量：

- 2,000 个 Node，Pod key 总数精确为 40,000；
- Event 以 Pod 为基数、倍率 `1/1`，因此精确创建 40,000 个 Event key；
- Pod 更新放大比为 10，每个 Pod 必须成功更新恰好 10 次；
- 一个覆盖 2,000 个 eligible Node 的 DaemonSet，其 Pod 已包含在 40,000
  Pod 总量中；
- 100 个 Service，每个选择 100 个 backend；
- ideal packing 下 100 个 EndpointSlice，每片 100 endpoints；
- controller 对象数、更新放大、Range 混合和周期节奏采用 trace 默认输入。

每个默认 profile 为以下九个资源根各建立一条 Watch RPC stream/订阅：
`pods`、`events`、`replicasets`、`deployments`、`leases`、`daemonsets`、
`services`、`endpointslices` 和 `minions`。虽然 profile 有
`masterleases` 资源和 Range，默认并不订阅 `masterleases` 根。每条配置
各自建立 stream/订阅，重复 prefix 不合并；这些 stream 复用 benchmark
共享的 client transport，不代表每条配置新建一个 TCP connection。这是
合成的订阅建立和事件扇出压力，不是由 trace 推导的连接数，也不是
apiserver/watch-cache 拓扑；订阅集合扩大后不能与旧的单 Pod Watch
profile 等同。

默认配置使用 `start_revision=0`、`prev_kv=true`、`progress_notify=true`。
对 current Watch，首次 Created header 单独保存为恢复基线；在尚未交付
事件或 progress 前断线，从该 Created revision 恢复。显式历史起点
`start_revision=r>0` 不会被 Created 推进；在尚未交付前断线，仍从 `r`
恢复。两种形式一旦交付事件或 progress，都从最后交付 revision+1 恢复。
benchmark 不在 open 时发送显式 RequestProgress，也不启用 fragments。

Kubernetes 官方文档定义资源关系，但不规定 Service、Event 或
controller 对象的通用比例。因此这些值必须是显式 profile 输入。
EndpointSlice ideal packing 是确定性压测模型；真实 controller 的变更
历史可能留下更多未填满的 slice。

## 资源行为与 Lease

JSON 的 `behavior` 只有四个可调度值：

| `behavior` | 压力语义 |
|---|---|
| `create_only` | 创建对象，不产生更新配额。 |
| `guaranteed_update` | ModRevision CAS 更新；失败 Txn 分支返回当前 KV，直接驱动下一次 CAS。 |
| `event` | 从配置的 pool 使用 native etcd lease 创建；若 `update_amplification > 0`，调度的更新也使用带 native lease 的 storage CAS（内置 profile 为 0）；不存在 Event/controller 外层重试循环。 |
| `master_lease` | 使用 native lease pool 的内部 masterlease 创建和周期更新；属于辅助流量，不计入 application target。 |

旧 `retry_policy` 字段和 `conditional_delete` 配置值都会被拒绝，没有
兼容 alias。低层 ConditionalDelete Txn 构造仍保留用于协议覆盖，但没有
活动 ResourceBehavior 会调度它。storage CAS 使用 failure KV、同一操作
deadline 和已有 attempt budget；Node heartbeat 的 status/NodeLease
重试上限仍是显式 profile 限制。

不要混淆以下 Lease 路径：

- API Lease 是 `/registry/leases/...` 资源，使用普通 ModRevision CAS，
  `Put.lease=0`；
- Coordination NodeLease 是 `/registry/leases/kube-node-lease/...`，
  作为 Kubernetes 对象创建和续期，同样不挂 native etcd lease；
- 内部 masterlease 是 `/registry/masterleases/...`，由 `master_lease`
  behavior 驱动；
- Event 和 masterlease TTL 是 native etcd lease，由 pool 复用或 Grant。
  native TTL 到期是 etcd 的真实行为，不是 controller 重试模拟。

CAS 和可变请求结果保持区分：`conflict`、`not_found`、`timeout`、
`result_unknown` 不能互换。`result_unknown` 可能已经到达 etcd，不能
盲目重放。

## 生成 DaemonSet-update 压力 profile

```sh
cargo run --release -p etcd_k8s_bench -- gen_profile \
  --nodes 10000 \
  --batch-size 500 \
  --txn-rps 2000 \
  --name rolling-update
```

生成结果使用每 Node 一个 Pod、每 Pod 四次成功更新、每 Node 三个 Event，
并按配置 wave 为 DaemonSet 生成每波两次更新。`--txn-rps` 只从该操作
预算推导硬更新阶段时长；写出的 timeline 才是运行时真相源。命令不会
覆盖已有文件。该形状的 Pod 数由 Node 数推导，因此 `gen_profile`
拒绝 `--pods`。

Watch 从原始数组过滤而非重建：保留现存 `ResourceSpec.key_pattern` 按
prefix 后接 `/` 的边界匹配条目，始终保留 `minions`，启用 NodeLease
时额外保留 `leases`。所有保留条目的字段和重复项都不变。用户显式
配置、且对应资源仍被保留的 `masterleases` Watch 可以留下，但生成器
不会自动添加该根。默认生成 profile 因而有 `pods`、`events`、
`daemonsets`、`minions`、`leases` 五个根；禁用 NodeLease 且没有应用
`leases` 资源时为四个根。其他资源数量、TTL、Range 比例和周期不变。
这只是压力形状生成器，不是真实 rolling-update 状态机。

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

排队等待和 RPC latency 是两套口径：
`etcd_k8s_bench_queue_wait_seconds{work_class}` 测量从 accepted 入队
到 worker 取出的时间；`etcd_k8s_bench_request_duration_seconds{rpc}`
只测量物理 RPC。已 accepted 但取消且没有执行结果的工作计为
`unfinished`，不会静默算成功，也不能据此断言 etcd 没有提交。
key、ID、endpoint 和错误文本不会成为指标标签。

scheduler 启动后恰好向 stdout 输出一行 JSON。顶层字段严格为
`achieved`、`interrupted`、`schedule_complete`、`scope`（字面值
`"scheduler"`）、`termination_error`、`targets`、`work_items` 和
`accounting_invariants`。`targets` 包含 `bootstrap`、
`application.creates`、`application.updates` 和 `range_requests`。
`work_items` 固定包含 `bootstrap`、`create`、`update`、`range`、
`master_lease`、`heartbeat`、`status`、`compact` 八类；每项包含
`work_class`、可空 `target`、`offered`、`accepted`、`completed`、
`succeeded`、`failed`、`result_unknown`、`dropped`、`dropped_full`、
`dropped_closed`、`unfinished`、`unissued`、可空 `target_deficit` 和
有界 `first_error`。有限目标的 `target_deficit` 为
`target - succeeded`；没有固定目标的辅助类，其 `target` 和
`target_deficit` 都为 `null`。

对账恒等式为：`offered = accepted + dropped + unissued`；
`completed = succeeded + failed`；`accepted = completed + unfinished`；
`dropped = dropped_full + dropped_closed`；`result_unknown` 是
`failed` 的子集。background 工作项失败会在继续施压时收集，不会被静默
忽略。只有 schedule 完整、未中断、无终止错误、恒等式全部成立、每个
有限 target 达成，且没有 failed/dropped/unfinished/unissued 时，
`achieved` 才为 true。进入 scheduler 后未达标即退出码 1；配置、连接、
Status、preflight 错误发生在这行 JSON 之前，也退出码 1。不能用 exit 0、
最后一次 metrics 抓取或物理 RPC 成功代替逻辑对账结果。

CAS conflict、retry、result unknown 和 queue drop 不消耗成功更新配额。

详细设计见 [`etcd_k8s_bench/DESIGN_zh.md`](etcd_k8s_bench/DESIGN_zh.md)，
英文使用说明见 [README.md](README.md)。

