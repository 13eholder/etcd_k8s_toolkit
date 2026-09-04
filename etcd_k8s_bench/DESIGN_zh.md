# etcd_k8s_bench 设计说明

## 1. 目标

`etcd_k8s_bench` 是 Kubernetes 形状的 etcd API 压测器。

它精确对齐 Kubernetes v1.35.5 使用 etcd 时可观察的接口行为：

- RPC 方法和请求字段；
- `/registry/...` key 与 prefix `range_end`；
- ModRevision/Version CAS Txn；
- List snapshot pagination；
- Watch create/recovery/require-leader；
- Event/masterlease native lease；
- logical compaction 和 Status。

负载不是逐事件 trace 重放，而是由拓扑、资源关系、对象基数、成功写放大和阶段 deadline 生成。相同 profile 与 seed 产生相同对象计划和操作顺序。

## 2. 非目标

- 不实现 kube-apiserver、controller-manager、scheduler、admission 或 watch cache；
- 不编码真实 Kubernetes protobuf 对象、managedFields 或 storage envelope；
- 不复刻某次 trace 的逐事件时间线、故障和环境噪声；
- 不模拟多 endpoint、mTLS/Auth、egress dialer 或 kubeadm Cluster 成员管理；
- 不把 Service、Event、Deployment 等经验比例表述成 Kubernetes 官方固定比例。

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

### 3.1 Range

- point-get：`range_end` 为空；
- resource-root Watch：例如 `/registry/pods → /registry/podt`；
- namespaced List：例如 `/registry/pods/bench-bench/ → /registry/pods/bench-bench0`；
- bounded List 从 limit 500 开始；selector 丢弃时最多扩展到 10000；
- 续页 key 是上一页最后一个 key 加 `\x00`；range_end 和 snapshot revision 不变；
- 不使用 arbitrary range、serializable、sort 或 min/max revision filter。

### 3.2 Txn

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

CAS conflict 是正常协调结果，不是 transport failure。失败分支的 KV 直接驱动下一次 CAS。

### 3.3 Watch

内置 Watch 使用无尾斜杠 `resource_prefix`。Watch create 携带：

- prefix range_end；
- start revision；
- `prev_kv=true`；
- `progress_notify=true`；
- gRPC metadata `hasleader=true`。

created response 的 header 不推进恢复 revision；事件和 progress response 才推进。断线后从最后已交付 revision+1 恢复。Compacted、server cancel、no-leader 和 context cancel 分开处理。

`progress_notify` 是 Watch create option。当前 profile 不在 open 时发送显式 RequestProgress；未来若启用，必须作为同一双向流中的独立条件事件建模。

### 3.4 Lease

Coordination NodeLease 是普通 Kubernetes Lease 对象：

```text
/registry/leases/kube-node-lease/<node>
Create/GuaranteedUpdate
Put.lease=0
```

Event 和 masterlease 使用 native etcd lease：

```text
GetLease(objectTTL)
  → reuse，或 LeaseGrant(objectTTL + effectiveReuse)
  → Txn Put.lease=leaseID
```

`effectiveReuse=min(configuredReuse,floor(5%×objectTTL))`。Delete 不取得或 Revoke lease。旧 lease 由 etcd 自然过期。

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

## 5. 阶段执行

```text
空库预检
  → Node/可选 NodeLease bootstrap
  → 计算 WorkloadPlan
  → 创建全部 resource key
  → 全局创建屏障
  → 限时成功更新阶段 + Range/Watch/周期任务
  → worker drain
  → 成功或 deficit 失败
```

### 5.1 创建屏障

`CreateCursor` 惰性遍历所有 resource/id。只有每个资源都满足：

```text
successful_creates == planned_objects
```

才启动 update task。Create collision、timeout、result unknown、worker failure 或 queue close 都阻止屏障开放。

### 5.2 写放大

每个资源声明非负整数：

```json
"update_amplification": 10
```

目标：

```text
target_successful_updates = object_count × update_amplification
```

`UpdateCursor` 按目标权重交织资源，并在每个资源内循环全部 key。每个 key 精确成功更新 `update_amplification` 次。CAS conflict、transport error、result unknown 和失败 RPC 不消耗成功配额。

### 5.3 Deadline

profile timeline 的 duration 总和是 update phase 硬 deadline。更新目标在前 95% 窗口内均匀调度，保留后 5% 给已入队请求完成和 worker drain；这段 reserve 属于同一个硬 deadline，不延长阶段。deadline 到期仍有 deficit 时 run 失败并 abort queued workers。

`--time-scale` 同时缩放 phase duration 和 periodic intervals；对象数和成功更新目标不变。

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
- 单 namespace 和 synthetic fixed-size value。这两项是明确压力参数，不是完整对象/namespace 仿真。

## 7. DaemonSet-update 生成器

`gen_profile` 生成一个确定性压力形状：

- Pod 数等于 Node 数；
- 每个 Pod 四次成功更新；
- 每 Node 三个 Event；
- 每 `batch_size` 构成一波，每波两个 DaemonSet 更新；
- `txn_rps` 只用于 `total_operations/txn_rps` 推导 phase duration；
- 不执行真实 controller rollout、Pod delete/recreate 或 wave barrier。

因此名称描述压力意图，不代表 controller 行为复刻。

## 8. 并发与错误

- 默认 32 workers，总 bounded queue 4096；
- 同 key 稳定 hash 到同 worker，保持 key-local CAS state 串行；
- 创建/update phase 使用 async send；background traffic 使用 try-send 并记录 backpressure/queue_closed；
- mutable RPC timeout 可能结果未知，不盲目重放；
- worker error、panic、join failure 和 drain timeout 传播为 run failure；
- Watch terminal error 立即 abort update/workers。

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
queue drops
```

关键指标：

- `etcd_k8s_bench_resource_objects{resource,state}`；
- `etcd_k8s_bench_resource_updates{resource,state}`；
- `etcd_k8s_bench_operations_total{operation,resource,outcome}`；
- `etcd_k8s_bench_requests_total{rpc,outcome}`；
- `etcd_k8s_bench_range_requests_total{shape,outcome}`；
- `etcd_k8s_bench_txn_total`、`cas_retries_total`；
- Watch、lease、heartbeat、revision、compact、inflight 和 dropped 指标。

所有标签都是固定枚举或已校验 profile 名称；key、ID、endpoint 和错误文本不进入标签。

## 10. 验证命令

```sh
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
```

最终行为验证必须使用专用空 etcd，并同时检查请求日志、metrics 和最终 keyspace。仅有进程 exit 0 不足以证明对象基数和成功放大正确。
