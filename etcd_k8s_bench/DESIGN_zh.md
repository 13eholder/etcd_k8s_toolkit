# etcd_k8s_bench 设计说明

## 1. 目标

`etcd_k8s_bench` 是 Kubernetes 形状的 etcd API **合成压力生成器**。

它精确对齐 Kubernetes v1.35.5 使用 etcd 时可观察的接口行为：

- RPC 方法和请求字段；
- `/registry/...` key 与 prefix `range_end`；
- ModRevision/Version CAS Txn；
- List snapshot pagination；
- Watch create/recovery/require-leader；
- Event/masterlease native lease；
- logical compaction 和 Status。

负载不是逐事件 trace 重放，也不是 controller 因果仿真，而是由拓扑、
资源关系、对象基数、synthetic value、成功写放大、Range/Watch 形状和
阶段 deadline 生成。相同 profile 与 seed 产生相同对象计划和操作顺序；
这些是可计算的后端压力目标，不是对真实 Kubernetes 容量、连接数或
控制面拓扑的校准结论。

## 2. 非目标

- 不实现 kube-apiserver、controller-manager、scheduler、admission、
  watch-cache 或它们的拓扑；
- 不编码真实 Kubernetes protobuf 对象、managedFields 或 storage envelope；
- 不复刻某次 trace 的逐事件时间线、故障和环境噪声；
- 不模拟多 endpoint、mTLS/Auth、egress dialer 或 kubeadm Cluster 成员管理；
- 不实现真实 rolling-update 的删除/替换、因果状态机或 wave controller；
- 不把 Service、Event、Deployment 等经验比例表述成 Kubernetes 官方固定比例；
- 不把默认 Watch 数量解释成 trace 推导的连接数或真实 apiserver 扇出。

## 3. etcd API 契约

稳态接口为 5 个一元 RPC 加一个流式 RPC：

| RPC | K8s 形状 |
|---|---|
| `KV/Range` | point 或 prefix；linearizable；分页续页使用 `last_key+NUL` 和首页 revision |
| `KV/Txn` | Create/Update/Delete 的 ModRevision CAS；compact journal 使用 Version CAS |
| `KV/Compact` | `physical=false`，目标是 journal 保存的上一安全 revision |
| `Lease/LeaseGrant` | `ID=0`，返回 ID 写入后续 `Put.lease` |
| `Maintenance/Status` | 空请求，作为健康和 revision 观测 |
| `Watch/Watch` | 资源根 prefix、`prev_kv`、`progress_notify`、require-leader metadata |

不生成 LeaseKeepAlive、LeaseRevoke、LeaseTimeToLive 或 LeaseLeases。
每条 `watches` 配置建立一条独立 Watch RPC stream/订阅；相同 prefix
也不合并。这些 stream 复用 benchmark 的共享 client transport，不代表
每项配置新建 TCP connection。内置 `k8s` 与 `k8s-no-node-lease` 都订阅
九个根：`pods/events/replicasets/deployments/leases/daemonsets/services/endpointslices/minions`。
`masterleases` 资源虽然参与压力和 Range，但不是默认 Watch 根；这是合成
的订阅建立和事件扇出压力，不是 trace 或真实控制面拓扑。

### 3.1 Range

- point-get：`range_end` 为空；
- resource-root Watch：例如 `/registry/pods → /registry/podt`；
- namespaced List：例如 `/registry/pods/bench-bench/ → /registry/pods/bench-bench0`；
- bounded List 从 limit 500 开始；selector 丢弃时最多扩展到 10000；
- 续页 key 是上一页最后一个 key 加 `\x00`；range_end 和 snapshot revision 不变；
- snapshot 由首页请求的显式 revision（若有）或首页响应确定；历史续页响应 header 可以报告更新的当前 revision，不要求各页 header 相等；
- 不使用 arbitrary range、serializable、sort 或 min/max revision filter。

### 3.2 Txn

低层 Txn 形状保留 Create、GuaranteedUpdate、ConditionalDelete 和
compact journal CAS：

```text
Create:
  compare ModRevision(key)==0
  success Put
  failure empty

GuaranteedUpdate:
  compare ModRevision(key)==expected
  success Put
  failure point Range

ConditionalDelete:
  compare ModRevision(key)==expected
  success point DeleteRange (range_end empty)
  failure point Range
```

配置层 `behavior` 只允许 `create_only`、`guaranteed_update`、`event`、
`master_lease`。旧 `retry_policy` 字段和 `conditional_delete` 配置值被
拒绝；低层 ConditionalDelete 构造能力保留，但没有对应调度行为。
Event 不实现 controller 外层重试。storage CAS 使用 failure KV、同一
操作 deadline 和既有 attempt budget；Node heartbeat/NodeLease 的显式
重试上限仍保留。

CAS conflict 是正常协调结果，不是 transport failure。失败分支的 KV
直接驱动下一次 CAS；`conflict`、`not_found`、`timeout` 和
`result_unknown` 是不同结果。可变请求的 `result_unknown` 可能已经提交，
因此不能盲目重放。

### 3.3 Watch

内置 Watch 使用无尾斜杠 `resource_prefix`。Watch create 携带：

- prefix range_end；
- start revision；
- `prev_kv=true`；
- `progress_notify=true`；
- gRPC metadata `hasleader=true`。

Watch 在 bootstrap 和全部有效 create 完成后建立；所有 Watch 返回
Created 后，update window 才开始，因此它不覆盖前面的创建阶段。对
current `start_revision=0`，首次 Created header 不推进已交付的
`last_revision`，而是单独保存为恢复基线；在尚未交付事件或 progress
前断线，从该 Created revision 恢复。对显式历史起点 `r>0`，Created
不会推进历史起点；在尚未交付前断线，仍从 `r` 恢复。两种形式一旦
交付事件或 progress，都从最后交付 revision+1 恢复。
断线只对可恢复 transport disconnect 重连；Compacted、server cancel、
no-leader 和 context cancel 分开处理并回报 scheduler。

`progress_notify` 是 Watch create option。当前 profile 不在 open 时发送
显式 RequestProgress，也没有启用 fragments；如果未来启用独立条件事件，
必须仍建模为同一双向流的一部分。

### 3.4 Lease

API Lease 和 Coordination NodeLease 都是普通 Kubernetes Lease 对象：

```text
/registry/leases/<object>
Create/GuaranteedUpdate
Put.lease=0

/registry/leases/kube-node-lease/<node>
Create/GuaranteedUpdate
Put.lease=0
```

内部 masterlease 是 `/registry/masterleases/<id>`，由
`master_lease` behavior 驱动。Event 和 masterlease 才使用 native etcd
lease：

```text
GetLease(objectTTL)
  → reuse，或 LeaseGrant(objectTTL + effectiveReuse)
  → Txn Put.lease=leaseID
```

`effectiveReuse=min(configuredReuse,floor(5%×objectTTL))`。Delete 不取得
或 Revoke native lease；旧 lease 由 etcd 按真实 TTL 自然过期。Event 没有
controller 外层重试，API Lease/NodeLease 的 CAS 和 native TTL 不能混为
同一语义。

## 4. 精确 workload schema

### 4.1 权威拓扑

```json
"topology": {
  "node_count": 2000,
  "pod_count": 40000
}
```

`pod_count` 是最终全部 Pod key 数，包括 Deployment、ReplicaSet、DaemonSet 等拥有的 Pod；关系不会在该总量外追加 Pod。

### 4.2 普通资源数量

```json
"count": {"basis":"pods","numerator":1,"denominator":1}
"count": {"basis":"nodes","numerator":1,"denominator":1}
"count": {"basis":"fixed","count":20}
"count": {"basis":"derived","relation":"services"}
```

scaled count 使用 checked integer：

```text
base × numerator / denominator
```

零分母、溢出、非整除和零结果在配置阶段拒绝，不做浮点舍入。

### 4.3 特殊资源关系

```json
"relationships": {
  "daemon_set_groups": [
    {"count":1,"eligible_nodes_per_daemonset":2000}
  ],
  "service_groups": [
    {
      "count":100,
      "backend_pods_per_service":100,
      "endpoint_groups_per_service":1,
      "endpoints_per_group":100,
      "max_endpoints_per_slice":100,
      "extra_slices_per_group":0
    }
  ]
}
```

- Node/minions=`node_count`；启用 NodeLease 时 NodeLease=`node_count`；
- DaemonSet objects=`Σ group.count`；DaemonSet-owned Pods=`Σ count×eligible_nodes`，且包含在 `pod_count`；
- Service objects=`Σ group.count`；backend 数由 profile 显式提供；
- EndpointSlice 先按 Service/地址族/协议/端口分组；ideal packing 下每组为 `ceil(endpoints/capacity)`；`extra_slices_per_group` 表达历史 fragmentation；
- derived relation 与资源名强绑定，不能把 EndpointSlice 公式挂到其他资源。

官方关系来源：

- https://v1-35.docs.kubernetes.io/docs/concepts/architecture/nodes/
- https://v1-35.docs.kubernetes.io/docs/concepts/workloads/controllers/daemonset/
- https://v1-35.docs.kubernetes.io/docs/concepts/services-networking/service/
- https://v1-35.docs.kubernetes.io/docs/concepts/services-networking/endpoint-slices/

### 4.4 Resource behavior

`ResourceSpec.behavior` 的有效枚举只有：

| 值 | 调度含义 |
|---|---|
| `create_only` | 只创建对象；`update_amplification` 必须为 0。 |
| `guaranteed_update` | 按 ModRevision CAS 产生成功更新目标。 |
| `event` | 创建对象并通过 lease pool 挂 native etcd TTL；若 `update_amplification > 0`，调度的更新也使用带 native lease 的 storage CAS（内置 profile 为 0）。 |
| `master_lease` | 内部 masterlease 创建和周期更新；不计入 application target。 |

serde 的未知字段和未知枚举值会在配置阶段拒绝，因此旧
`retry_policy` 字段、`conditional_delete` 值都没有活动兼容路径。低层
delete Txn 仍可由协议代码构造，但不产生调度工作项。

## 5. 阶段执行

```text
空库预检
  → Node/可选 NodeLease bootstrap
  → 计算 WorkloadPlan
  → 创建全部 resource key
  → 全局创建屏障
  → 建立 Watch 并等待所有有效 Created
  → 限时成功更新阶段 + Range/周期任务
  → worker drain 与 Watch teardown
  → 成功或 deficit 失败
```

Watch 在有效 create 屏障之后建立，因此不覆盖 bootstrap/create；只有
全部 Watch readiness 成功后才打开 update window。

### 5.1 创建屏障

`CreateCursor` 惰性遍历所有 resource/id。只有每个资源都满足：

```text
successful_creates == planned_objects
```

才启动 Watch readiness 和 update task。Create collision、timeout、result
unknown、worker failure 或 queue close 都阻止屏障开放；已进入 scheduler
但未完成目标的运行最终必须报告 deficit/failed。

### 5.2 写放大

每个资源声明非负整数：

```json
"update_amplification": 10
```

目标为：

```text
target_successful_updates = object_count × update_amplification
```

`UpdateCursor` 按目标权重交织资源，并在每个资源内循环全部 key；每个
key 精确成功更新 `update_amplification` 次。CAS conflict、transport
error、result unknown 和失败 RPC 不消耗成功配额。

### 5.3 Deadline

profile timeline 的 duration 总和是 update phase 硬 deadline。更新目标在
前 95% 窗口内调度，后 5% 只是应用成功更新的完成余量，不是整个运行的
墙钟上限。timeline 结束后，已 accepted 的后台工作在独立的
`operation_timeout` 下正常 drain，随后 Watch teardown 也在独立的
`operation_timeout` 下执行；这些阶段不延长 update deadline。deadline
到期仍有 deficit 时 run 失败并 abort queued workers。

所有时间参数都表示同一套实际时间：phase duration、Range RPS、periodic
interval 和 native lease TTL 不使用第二套虚拟时钟。调整压力必须显式
修改阶段时长、速率或成功更新目标；生成器的 rate 参数只用于推导时间，
不改变运行时的时间语义。

## 6. 内置 profile

| CLI | workload | NodeLease | 默认 |
|---|---|---:|---:|
| `k8s` | `k8s` | 开启 | 是 |
| `k8s-no-node-lease` | `k8s_no_node_lease` | 关闭 | 否 |

共同默认：

- 2,000 Node；40,000 Pod；40,000 Event；
- Pod update amplification=10；
- 20 Deployment、20 ReplicaSet、6 leader-election Lease、1 DaemonSet、1 masterlease；
- 100 Service、100 ideal-packed EndpointSlice；
- 2,310 秒 update phase；
- trace-derived Range mix、controller-object amplification 和周期 cadence；
- 单 namespace 和 synthetic fixed-size value。这些是明确压力参数，不是完整对象/namespace 仿真。

两个内置 profile 的 Watch 数组相同，均为九条独立 Watch RPC
stream/订阅：
`pods/events/replicasets/deployments/leases/daemonsets/services/endpointslices/minions`。
无默认 `masterleases` stream；每条配置保留自己的字段，重复 prefix 不
合并；这些 stream 复用 benchmark 的共享 client transport，不代表每项
配置新建 TCP connection。相对旧单 Pod Watch，这改变了订阅建立与事件
扇出压力，但不代表真实拓扑或 trace 连接数。

## 7. DaemonSet-update 生成器

`gen_profile` 生成一个确定性压力形状，而不是 rollout 引擎：

- Pod 数等于 Node 数；
- 每个 Pod 四次成功更新；
- 每 Node 三个 Event；
- 每 `batch_size` 构成一波，每波两个 DaemonSet 更新；
- `txn_rps` 是 application operation budget rate（包含创建和更新），
  只用于由总预算推导 phase duration，不是保证的 RPC 吞吐；
- 不执行真实 controller rollout、Pod delete/recreate 或 wave barrier。

生成器从原始 `watches` 数组过滤并保留原条目：按保留资源的
`key_pattern` 做 prefix 后接 `/` 的边界匹配，始终保留 `minions`，
启用 NodeLease 时额外保留 `leases`；显式匹配且仍保留的 masterleases
订阅可以留下，但不会自动添加。默认结果为
`pods/events/daemonsets/minions/leases` 五个根；禁用 NodeLease 且没有
应用 `leases` 时为四个根。重复项和所有字段不变，其他资源数量、TTL、
Range 比例和周期 cadence 不变。

因此名称描述压力意图，不代表 controller 行为复刻。


## 8. 并发与错误

- 默认 32 workers，总 bounded queue 4096；
- 同 key 稳定 hash 到同 worker，保持 key-local CAS state 串行；
- create/update phase 使用可等待的 queue reserve，直到 deadline；background
  traffic 使用 try-send，队列满或关闭分别计数；
- `accepted` 在入队时记录时间，worker dequeue 时记录
  `queue_wait_seconds`；这不是 RPC latency；
- mutable RPC timeout 可能结果未知，不盲目重放；
- `conflict`、`not_found`、`timeout`、`result_unknown` 和 transport
  error 分开报告；CAS conflict 不等于 transport failure；
- background 逻辑失败不会立即 fail-fast；scheduler 继续收集并在最终
  `work_items`/`achieved` 中对账；致命 worker/drain 错误仍中止。
- worker error、panic、join failure 和 drain timeout 传播为 run failure；
- 活动 dispatch/update supervision 期间的 Watch terminal 会 abort
  update/workers；正常 timeline 后 drain 中的晚到 Watch terminal 在随后
  teardown 时收集，最终仍使 `achieved=false`。取消的 accepted work 只能
  计为 unfinished，不能断言后端没有提交。

## 9. 指标

分层口径：

```text
planned objects
successful creates / create deficit
target successful updates
successful updates / update deficit
logical operation outcomes
physical RPC outcomes
CAS conflicts/retries
queue wait / queue drops
```

关键指标：

- `etcd_k8s_bench_resource_objects{resource,state}`；
- `etcd_k8s_bench_resource_updates{resource,state}`；
- `etcd_k8s_bench_operations_total{operation,resource,outcome}`；
- `etcd_k8s_bench_requests_total{rpc,outcome}`；
- `etcd_k8s_bench_request_duration_seconds{rpc}`：单个物理 RPC 时长；
- `etcd_k8s_bench_queue_wait_seconds{work_class}`：入队到 dequeue 的等待；
- `etcd_k8s_bench_range_requests_total{shape,outcome}`；
- `etcd_k8s_bench_txn_total`、`etcd_k8s_bench_cas_retries_total`；
- Watch、lease、heartbeat、revision、compact、inflight 和 dropped 指标。

所有标签都是固定枚举或已校验 profile 名称；key、ID、endpoint 和错误
文本不进入标签。RPC 成功、最后一次 metrics 抓取或进程 exit 0 都不能
替代逻辑工作项对账。

### 9.1 scheduler 结果协议

进入 scheduler 后，程序向 stdout 输出恰好一行 JSON；配置、连接、初始
Status 和 empty-database preflight 错误发生在 scheduler 之外，不伪造
这份结果。顶层字段严格为：

```text
achieved, interrupted, schedule_complete, scope, termination_error,
targets, work_items, accounting_invariants
```

其中 `scope` 为 `"scheduler"`；`targets` 包含 `bootstrap`、
`application.creates`、`application.updates`、`range_requests`。
`work_items` 固定包含八类：`bootstrap`、`create`、`update`、`range`、
`master_lease`、`heartbeat`、`status`、`compact`。每项字段为
`work_class`、可空 `target`、`offered`、`accepted`、`completed`、
`succeeded`、`failed`、`result_unknown`、`dropped`、`dropped_full`、
`dropped_closed`、`unfinished`、`unissued`、可空 `target_deficit` 和
有界 `first_error`；有限目标的 `target_deficit` 为
`target - succeeded`，没有固定目标的辅助类其 `target` 和
`target_deficit` 为 `null`。

每类必须满足：

```text
offered = accepted + dropped + unissued
completed = succeeded + failed
accepted = completed + unfinished
dropped = dropped_full + dropped_closed
result_unknown ⊆ failed
```

`achieved` 只有在 schedule 完整、未中断、无 termination error、上述
恒等式成立、所有有限 target 达成，且没有 failed/dropped/unfinished/
unissued 时才为 true。进入 scheduler 但未达标即退出码 1；成功逻辑
结果必须同时满足目标与这些坏结果检查。

## 10. 验证命令

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

最终行为验证必须使用专用空 etcd，并同时检查请求日志、metrics 和最终 keyspace。仅有进程 exit 0 不足以证明对象基数和成功放大正确。
