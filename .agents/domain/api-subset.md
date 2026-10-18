# API 子集与参数边界

## 范围与支持状态

本文集中回答“哪个调用者需要/使用哪个 etcd API、哪些参数及语义”，不把 protobuf 声明当作已实现能力。当前产品目标见 [设计目标](design-goals.md)。本项目不是存储后端实现，以下“支持”分别指研究需求、benchmark 请求生成或 proxy 转发，不能合并成一个兼容性认证结论。

| 层次 | 当前范围 | 权威依据 |
| --- | --- | --- |
| Kubernetes 存储需求 | kube-apiserver：Range、Txn、Compact、LeaseGrant、Status 五个一元 RPC及 Watch 双向流 | [Stage 004 最小接口 ADR](../stages/004-k8s-mode-fidelity-correction/docs/adr/001-k8s-etcd-minimal-interface.md)、版本化 ref 研究 |
| 当前 benchmark 活跃路径 | Range、Txn、Compact、Status 四个一元 RPC及 Watch；Put 在 Txn 内，不独立调用 Put RPC | [Stage 006 冻结契约](../stages/006-rule-driven-timed-pressure/docs/adr/003-approved-defaults-and-execution-contract.md)、[client.rs](../../etcd_k8s_bench/src/client.rs)、[scheduler.rs](../../etcd_k8s_bench/src/scheduler.rs) |
| benchmark 保留的底层形状 | ConditionalDelete、部分通用 Range 执行器形状；不等于当前负载可配置项 | [workload.rs](../../etcd_k8s_bench/src/workload.rs)、scheduler 的 RangePager |
| proxy 转发 | KV、Watch、Lease、Maintenance 四个 service 的已注册方法，参数面比 benchmark 宽 | [proxy main](../../etcd_proxy/src/main.rs)、[proxy.rs](../../etcd_proxy/src/proxy.rs) |
| kubeadm 生命周期 | MemberList/Add/Remove/Promote 与 Status；Sync 是客户端操作 | Stage 004 ADR；不属于当前 bench/proxy 注册面 |
| 仅协议声明 | Cluster、Auth；另有未进入当前 build 的 Lock/Election proto | [rpc.proto](../../proto/rpc.proto)、两个 crate 的 build.rs |

**5+1 与 4+1 不矛盾**：Stage 006 的六类资源模型不施加 native TTL/LeaseGrant 压力，普通 NodeLease 使用 `Put.lease=0`；这没有改变 Kubernetes 对 native Lease 的需求，也没有移除 proxy Lease 转发。

## 来源与阅读规则

- Kubernetes 参数列按仓库已接受的 Stage 004 ADR 及 `ref/` 对 **Kubernetes v1.35.5（研究标识 commit 6636cbce）、etcd client v3.6.5** 的分析整理；本次未重新审计该上游版本或联网复验全部官方结论。
- benchmark 列按当前 Stage 006 决策、[执行规格](../stages/006-rule-driven-timed-pressure/spec/timed-pressure.md)与实际构造函数核对；不从旧 profile、已删除流量数据推导参数。
- 本地字段定义以 [proto/rpc.proto](../../proto/rpc.proto)、[proto/kv.proto](../../proto/kv.proto) 为准；Rust 生成代码来自 [bench build.rs](../../etcd_k8s_bench/build.rs) 和 [proxy build.rs](../../etcd_proxy/build.rs)。本地 proto 不自动等于 etcd v3.6.5 的完整最新 schema。
- 表中 `0`、`false`、空 bytes/数组是 protobuf 默认值，不一定在 wire 中显式编码；`\x00` 是一个 NUL 字节，不是空字符串。
- “未使用”不表示后端应拒绝该合法参数；“业务不直接读取响应字段”不授权把字段清零。客户端流管理、错误转换、事件 helper 和端点发现可能间接依赖它。
- 本文是当前领域汇总入口，不自行改变已接受决定。发现目标与实现差异时同时保留两者，见末节。

## 通用 key、revision 与传输语义

- key/value 是不透明 bytes。Range/Watch 使用半开范围 `[key, range_end)`；空 `range_end` 表示点操作，单 NUL 上界表示从 key 到末尾，`key=range_end=\x00` 表示全 keyspace。
- `prefix_end` 从末尾递增首个非 `0xff` 字节并截断尾部；全 `0xff` 或空前缀返回 NUL。不能普遍用“追加一个字符”实现上界。列表 namespace 前缀通常含尾 `/`；资源根 Watch 按根本身构造，不能互换。
- 当前六类 key 域及基数见设计目标；另有根级 `compact_rev_key`，不能仅允许 `/registry/` 下的 key。
- KV 线性一致读、原子 CAS、全局 revision、固定 revision 快照与可恢复 Watch 是不同契约。Watch 不等于与 KV 线性一致或有界延迟；失败 CAS 回读不必推进 revision。
- benchmark 单 endpoint、共享 tonic Channel，默认消息编解码上限 64MiB；`request_timeout_ms` 同时用于 connect timeout/Endpoint timeout，TCP keepalive 为 30s。不是 Kubernetes Go client 的完整连接/重试模型；没有多 endpoint 发现、TLS/认证配置面的实现承诺。
- `hasleader=true` 是 Watch gRPC request metadata，不是 proto 字段。Kubernetes 的 TLS/CA、HTTP/2 keepalive 等部署需求不能由当前 benchmark 的 TCP keepalive 代替。

## KV Range

来源：Stage 004 ADR 的 Range 表；[client preflight](../../etcd_k8s_bench/src/client.rs)、[ResourcePlan 请求构造](../../etcd_k8s_bench/src/plan.rs)、[RangePager](../../etcd_k8s_bench/src/scheduler.rs)。以下覆盖本地 `RangeRequest` 全部 13 个字段。

| 字段 / 类型 | Kubernetes 已审阅调用面 | 当前 benchmark 活跃取值 |
| --- | --- | --- |
| `key: bytes` | 对象完整 key、资源/namespace prefix、续页 last key + NUL、health key、journal key | 主动点读对象 key；列表为实际占用 namespace/root prefix；preflight 为 NUL；Txn failure Range 为该 CAS key |
| `range_end: bytes` | 点读空；prefix/count 为 prefix_end；journal 点读空 | 点读/Txn failure 空；列表 prefix_end；preflight NUL |
| `limit: int64` | 点读 0/1；无界列表 0；K8s 分页常从 500 起，selector 场景可增长至 10000 | 点读/Txn failure/preflight 为 0；列表逻辑目标来自 `load.list_limit`（默认 100），首个物理页 `min(500, 目标)` |
| `revision: int64` | 0 当前读；指定 resourceVersion/续页固定历史快照 | 主动首请求 0；后续页固定第一页快照 revision；preflight/failure 0。不是独立 CLI 历史 Range 选项 |
| `sort_order: enum` | NONE=0 | NONE=0 |
| `sort_target: enum` | KEY=0 默认 | KEY=0 |
| `serializable: bool` | false，线性一致读 | false |
| `keys_only: bool` | 条件使用，如资源大小估算；不是默认高频读 | bench 活跃 RPC 为 false；外部 verify 的 etcdctl 最终 keyspace 采集使用 keys-only，不算 benchmark 测量负载 |
| `count_only: bool` | prefix Count 使用 true，仍有 range_end | 只读全库 preflight true；主动读/failure false |
| `min_mod_revision: int64` | 0，不使用过滤 | 0 |
| `max_mod_revision: int64` | 0，不使用过滤 | 0 |
| `min_create_revision: int64` | 0，不使用过滤 | 0 |
| `max_create_revision: int64` | 0，不使用过滤 | 0 |

### 场景与分页

- **空库 preflight**：`{key:[0], range_end:[0], count_only:true}`；必须成功且 count 为 0 才继续。不能拿单 prefix 为空代替全库为空。
- **point GET**：完整对象 key，其他字段默认；覆盖 Pod/Node/Deployment/ReplicaSet/DaemonSet 的 ID 域，NodeLease 不在主动读权重中。
- **有界 LIST**：上述五类资源按其实际占用的 namespace/root 轮转；逻辑目标默认 100，不是每次固定读取 500 个。后续请求 `key=最后扫描key+NUL`，保留原 `range_end` 与 snapshot revision。
- **保留执行器**：`limit=0` 的无界 prefix 保持一次请求形状；selector 丢弃时可增长 page limit，最高 10000，仍受剩余逻辑目标约束。当前默认 selector 不丢弃，不应把这段底层能力描述为现行全量遍历负载。
- 非法范围/乱序/无推进页应作为协议错误，历史被 compacted 不能假装完整成功。**当前执行器尚未实现全部这些校验**，详见已知差异。

### 响应字段

| 字段 | 语义与使用边界 |
| --- | --- |
| `header.revision` | Kubernetes LIST/resourceVersion/Watch 起点；bench 第一页 snapshot 与最新 revision 观察。不能只返回伪造固定值 |
| `kvs[].key` | 对象定位、返回范围与分页游标 |
| `kvs[].value` | Kubernetes transform/decode；bench 不解码 Kubernetes 对象，CAS/journal 路径按其协议使用 value |
| `kvs[].mod_revision` | Kubernetes resourceVersion、bench 对象 CAS 状态 |
| `kvs[].version` | compact journal VERSION CAS；与 mod_revision 不同 |
| `kvs[].create_revision`、`kvs[].lease` | 不因当前对象路径未直接使用就删掉语义；Lease/事件辅助逻辑和外部 keyspace 核对可能依赖 |
| `count` | 匹配计数、Count/preflight 与分页结果；不是无条件等于返回 kvs 长度 |
| `more` | 后续页判断，不能在还有结果时随意置 false |

## KV Txn 及嵌套操作

来源：Stage 004 ADR 的四类模板；[workload 构造与 CAS 状态机](../../etcd_k8s_bench/src/workload.rs)、[scheduler 的 execute_txn/compaction](../../etcd_k8s_bench/src/scheduler.rs)。这些都是一个 `KV/Txn` RPC 内的分支，不是独立 Put/Delete RPC。

### 四类模板与当前活跃性

| 场景 | compare | success | failure | 当前状态 |
| --- | --- | --- | --- | --- |
| Create | `MOD(key)==0` | 一个 Put | **空数组** | 准备六类对象；创建冲突不盲重试 |
| GuaranteedUpdate | `MOD(key)==已知mod_revision` | 一个 Put | 一个点 Range(key) | 应用更新、NodeReady、Node status、普通 NodeLease |
| ConditionalDelete | `MOD(key)==已知mod_revision` | 一个单 key DeleteRange | 一个点 Range(key) | Kubernetes 子集及底层 helper 保留，Stage 006 不主动调度 |
| compaction journal | `VERSION(compact_rev_key)==已知version` | 一个 Put，value 为上一事务 header revision 的十进制 ASCII | 一个点 Range(compact_rev_key) | 当前独立周期协调 |

`TxnRequest.compare/success/failure` 都是数组，但当前模板恰一个 compare、一个 success、零或一个 failure；不产生嵌套 Txn 或多 key 多 op 事务。failure 中声明 Range 不证明其被执行，更不证明发生了冲突。

### Compare 全字段边界

| 字段 / 类型 | 当前子集 |
| --- | --- |
| `result: CompareResult` | EQUAL=0；不生成 GREATER/LESS/NOT_EQUAL |
| `target: CompareTarget` | MOD=2 用于对象；VERSION=0 仅 journal；不生成 CREATE/VALUE/LEASE compare |
| `key: bytes` | 被比较的单个完整 key |
| `range_end: bytes` | 空，不做范围 compare |
| `target_union.mod_revision: int64` | MOD 时选中；Create 为 0，更新/删除为期望 revision |
| `target_union.version: int64` | VERSION 时选中；journal 的已知 key version，初始可为 0 |
| `target_union.create_revision/value/lease` | 未选中的 oneof，不应解释成同时存在的零值比较 |

### PutRequest 全字段边界

| 字段 / 类型 | Kubernetes 子集 | 当前 benchmark |
| --- | --- | --- |
| `key: bytes` | 对象或 journal key | 六类对象或 journal |
| `value: bytes` | 编码/加密后的不透明对象；journal 数字文本 | 指定大小的合成字节；journal 数字文本 |
| `lease: int64` | 无 TTL 为 0；native TTL 对象附 LeaseGrant 返回 ID | **恒 0**，包括普通 NodeLease |
| `prev_kv: bool` | false | false |
| `ignore_value: bool` | false | false |
| `ignore_lease: bool` | false | false |

### DeleteRangeRequest 全字段边界

`key: bytes` 为单个完整对象 key；`range_end: bytes` 为空；`prev_kv: bool=false`。这是 Kubernetes 条件删除/bench 保留 helper 的形状，不包括 prefix/range delete，也不包括 LeaseRevoke。当前六类定时压力不主动执行它。

### Txn 响应与失败

- `TxnResponse.succeeded` 决定 CAS 分支，false 不是 gRPC error；`header.revision` 用于更新本地状态。
- `responses[]` 必须对应实际执行分支。Update/Delete/journal 的失败分支使用 `response_range.kvs`；不能用缺失/错误 response op 冒充合法冲突。
- Update 冲突后以返回的当前 KV/revision 重建后续 attempt，属于同一个逻辑操作；Create collision 不自动重试。已发送的可变请求丢失响应为 result_unknown，不原样重放。
- journal 使用返回 KV 的 `version` 与文本 value 恢复协调状态；协调失败不是增加一次对象成功更新。一次物理 RPC 成功也不等于一次键空间变更。

## KV Compact

| 字段 / 类型 | Kubernetes 子集 | 当前 benchmark |
| --- | --- | --- |
| `revision: int64` | compactor journal 确定的上一安全 revision | `CompactorState` 由 journal Txn 响应导出有效目标；不是任取最新 revision |
| `physical: bool` | false，logical compaction | false |

`CompactionResponse.header` 在当前 bench 用于观察 revision；Kubernetes compactor 不以响应 payload 当对象业务结果。当前每个周期先做 journal Txn，首次可能只建立 journal；CAS 冲突接受其他协调者状态，本 tick 不抢占重试；有有效目标才发 Compact。RPC 错误/未知结果不触发原样可变重放，后续周期按状态继续。

Kubernetes journal 的 Get→point Watch 跟随与故障后约 1s 重建是另一条生产路径；当前 bench 的周期协调不复现完整跟随生命周期。

## Watch 双向流

来源：[watch.rs](../../etcd_k8s_bench/src/watch.rs)、[plan.watch_specs](../../etcd_k8s_bench/src/plan.rs)、[Stage 006 Watch ADR](../stages/006-rule-driven-timed-pressure/docs/adr/005-watch-lifecycle-and-outcome-corrections.md)、Stage 004/ref。一个 Watch RPC 包含 create/cancel/progress 子请求，RequestProgress 不是单独 RPC。

### WatchCreateRequest 全字段

| 字段 / 类型 | Kubernetes 已审阅调用面 | 当前 benchmark |
| --- | --- | --- |
| `key: bytes` | 对象资源 prefix；journal 为点 key `compact_rev_key` | 六类固定资源根；每根订阅数由 `watch.subscriptions_per_root` 指定，默认 1 |
| `range_end: bytes` | 对象 prefix_end；journal 空 | 资源根 prefix_end；不包含 journal point Watch |
| `start_revision: int64` | 初始 LIST revision+1 或显式 resourceVersion 对应起点 | `watch.start_revision`，默认 0，配置要求非负；恢复值由已交付进度推导 |
| `progress_notify: bool` | watch-cache 路径通常 true；Events/journal 等按路径，不能泛化 | `watch.progress_notify` 可配置，默认 true |
| `filters: repeated enum` | 空，不设置 NOPUT/NODELETE | 空，无配置项 |
| `prev_kv: bool` | 对象 Watch true，支撑 DELETE/selector；journal 不设置 | `watch.prev_kv` 可配置，默认 true；允许变更不表示完整复刻 K8s 对象消费 |
| `watch_id: int64` | create 不指定，服务端分配 | 0 默认，服务端分配 |
| `fragment: bool` | 已审阅请求路径 false | false，无配置项；不能宣称实现完整碎片聚合 |

### 流控制与恢复

- `WatchCancelRequest.watch_id: int64`：取消当前已建立订阅的 ID，**合法 ID=0 也要取消**；不能用非零判断 ID 是否有效。
- `WatchProgressRequest {}`：Kubernetes client 在支持该能力时条件发送；当前 bench 没有主动 progress_request 调度，只消费服务端 progress_notify 响应；proxy 可以转发该子请求。
- metadata `hasleader=true`：bench 打开每条 Watch 时设置。RequestUnion 内不携带该字段。
- 只有合法 Created 打开当前 ready；提前 progress/event 不能替代。初始 `start_revision=0` 的 Created header 作为恢复基线，不当作已交付事件。
- 有已交付 revision 时从 `last_revision+1` 恢复；尚无已交付事件时，初始 0 的订阅从保存的 Created header revision 本身恢复（不加 1），显式非零起点则沿用配置。不能简单在每次断线后用新 header 跳过历史。
- Created/连接变化、恢复和取消受准备/有效测量截止/teardown deadline 约束；不自动 relist 补造完整性。
- Watch 建流本地 timeout（`Cancelled` 且带 typed `tonic::TimeoutExpired` source）按可恢复 transport 处理，受同一阶段 deadline/退避约束；普通 Cancelled 及明确 semantic terminal 不放开重试，准备期终态直接使准备失败。

### WatchResponse 与事件

| 字段 | 所需语义与消费 |
| --- | --- |
| `watch_id`、`created` | 订阅标识与建立确认；不能省略或把任何首响应当 Created |
| `header.revision` | 无事件非 Created 的 progress revision；Created header 不等于已交付事件 revision |
| `canceled`、`compact_revision`、`cancel_reason` | server cancel、历史已压缩、no-leader 等终止分类；非普通空事件 |
| `fragment` | 协议流管理字段；当前 bench 不消费该字段，未实现完整碎片聚合校验 |
| `events[].type` | PUT/DELETE，Kubernetes 事件消费依赖类型；当前 bench 不按类型解码对象/执行 selector，不能视为完整事件语义验证 |
| `events[].kv`、`events[].prev_kv` | 对象、旧对象、修改 revision；bench 从事件 kv/prev_kv 的 mod_revision 推进并记录事件数；不按 create_revision/version/lease 做对象语义校验 |

测量期断线/semantic terminal 形成覆盖缺口，其他压力继续；重连不能清除已有缺口。Kubernetes 的完整 reflector/relist 行为不是 bench 的恢复保证。

最终 per-run Watch `payload_bytes` 由 [MeasurementRecorder::watch_response](../../etcd_k8s_bench/src/measurement.rs) 累加 `Event.encoded_len()`，不含外层 WatchResponse/header/transport。bench 不维护第二套局部 key/value 字节累加；proxy detail 日志仍有自己的 key/value 字节口径，不能混作同一指标。

## LeaseGrant 与 native TTL

**Kubernetes 需求存在，当前 benchmark 不施压。** Stage 004 曾将旧 bench Lease client 收敛为仅 Grant；Stage 006 又移出 native TTL 资源及 Grant 调度。生成 proto 仍含 Lease，不证明当前 ClientBundle 有 Lease client。

| 参数/响应 | Kubernetes 调用边界 |
| --- | --- |
| `LeaseGrantRequest.TTL: int64` | 秒，来自对象 TTL 与有效复用窗口；示例 events 默认 3600s 对象 TTL + 60s reuse = 3660s；masterlease/peerserverlease 示例 15s。不是协议只允许这些 TTL |
| `LeaseGrantRequest.ID: int64` | 0，由服务端分配 |
| `LeaseGrantResponse.ID` | 附入后续 Txn Put.lease |
| `TTL/header/error` | 业务未直接读取不等于可伪造；必须保留 client/协议错误语义 |

leaseManager 缓存命中不必 Grant；TTL 到期由后端删除关联 key，不产生客户端 DeleteRange。显式对象删除不必 Revoke。LeaseKeepAlive/KeepAliveOnce/Revoke/TimeToLive/Leases 不在已审阅 apiserver native lease 调用链中。NodeLease 是普通 `coordination.k8s.io/Lease` 对象：当前 bench 对其做 KV CAS，`Put.lease=0`，不模拟 native TTL。

## Maintenance Status

`StatusRequest {}` 是空消息。Kubernetes 使用 version 做能力判断、db size 做指标；kubeadm 还用 errors 做健康/quorum 判断。当前 bench 用于 preflight 和独立周期，读取 header revision 并计量 RPC，不等于复现 Kubernetes 的版本探测/多 endpoint 仲裁。

本地 `StatusResponse` 字段为 `header`、`version`、`dbSize`、`leader`、`raftIndex`、`raftTerm`、`raftAppliedIndex`、`errors`、`dbSizeInUse`、`isLearner`（Rust 对应 snake_case）。较新上游研究可能列出 `storage_version/db_size_quota/downgrade_info` 等，本地 proto 没有这些字段，不能写成本项目已生成/转发的强类型能力。

## kubeadm 生命周期面

只用于区分 Kubernetes 全工具链需求，不属于当前 benchmark，proxy 也未注册 Cluster service。

| 操作 | 请求参数 | 说明 |
| --- | --- | --- |
| `Cluster/MemberList` | 上游 client 有 linearizable 参数语义；**本地 MemberListRequest 是空消息** | 不能把上游参数写成本地已支持字段 |
| `Cluster/MemberAdd` | `peerURLs[]`、`isLearner=false` | 新成员；`MemberAddAsLearner` 是同一 RPC 设置 `isLearner=true`，不是另一个 wire 方法 |
| `Cluster/MemberRemove` | `ID: uint64` | 删除成员；已不存在的处理按 kubeadm 调用策略 |
| `Cluster/MemberPromote` | `ID: uint64` | learner 提升；追赶失败/已提升按客户端语义处理 |
| `Maintenance/Status` | 空消息，目标 endpoint 在连接层 | kubeadm 多 endpoint 仲裁，不是本 bench 单 endpoint 的容量保证 |
| `clientv3.Sync` | 客户端刷新 endpoint 列表 | 非独立 RPC |

成员响应涉及 `ID/name/peerURLs/clientURLs/isLearner` 及成员列表。`MemberUpdate` 虽在本地 proto 中定义（`ID, peerURLs[]`），不由已审阅 kubeadm 子集自动纳入当前需求。

## Proxy 转发参数面

proxy 在 `main.rs` 仅注册 KV/Watch/Lease/Maintenance。以下“转发”表示将当前生成 message 传给上游，没有施加 benchmark 的单 key/单 op/固定字段限制；语义由上游实现，未做全面兼容验证。

| service / 方法 | 请求字段或形状 | 与 benchmark 的区别 |
| --- | --- | --- |
| KV/Range | 全部 13 个 RangeRequest 字段，见上表；不重写 | 可转发非 benchmark 的排序/过滤等参数 |
| KV/Put | `key,value,lease,prev_kv,ignore_value,ignore_lease` | 支持独立 Put RPC 转发 |
| KV/DeleteRange | `key,range_end,prev_kv` | 不限于单 key 或条件 Txn |
| KV/Txn | `compare[],success[],failure[]`；完整 RequestOp oneof，包括 nested Txn | 不按 K8s 模板裁剪；不等于本地执行事务 |
| KV/Compact | `revision,physical` | 不强制 physical=false |
| Watch/Watch | create 全 8 字段、cancel.watch_id、空 progress 子请求 | 不按六根/固定配置裁剪 |
| Lease/LeaseGrant | `TTL,ID` | 可携带非零请求 ID；不是仅 K8s 自动分配形状 |
| Lease/LeaseRevoke | `ID` | bench 不调度 |
| Lease/LeaseKeepAlive | 双向流，每条 `ID` | bench 不调度；KeepAliveOnce 是客户端用法而非独立 proto RPC |
| Lease/LeaseTimeToLive | `ID,keys` | bench 不调度 |
| Lease/LeaseLeases | 空消息 | bench 不调度 |
| Maintenance/Status | 空消息 | 上游响应直接转发 |
| Maintenance/Alarm | `action`=GET/ACTIVATE/DEACTIVATE、`memberID`、`alarm`=NONE/NOSPACE/CORRUPT | bench 不调度 |
| Maintenance/Defragment | 空消息 | bench 不调度，勿与 logical Compact 混同 |
| Maintenance/Hash | 空消息 | bench 不调度 |
| Maintenance/HashKV | `revision` | bench 不调度 |
| Maintenance/Snapshot | 空请求，服务端响应流 | bench 不调度 |
| Maintenance/MoveLeader | `targetID` | bench 不调度 |

转发边界：

- 一元方法把原 `Request` 交给上游，保留其中请求 metadata；Watch/KeepAlive 拆流后使用原 metadata/extensions 重建请求，包含 `hasleader`。不应写成“仅保留 hasleader”。
- Watch 响应通过新 `Response` 包装，只转发消息流/Status；没有完整保留上游初始响应 metadata 的承诺。
- Watch 区分上游 Status 与 clean EOF，下游输入错误通过 oneshot 通知响应任务；KeepAlive 输入错误目前与 EOF 都结束请求流，不能宣称所有流错误完全透明。
- 默认上下游消息上限 64MiB，可由 `--max-message-size` 调整。没有独立 TLS/Auth 配置面或自动重试/重连保证。
- 日志可能包含完整请求/Watch value，不能当脱敏审计；有界日志背压会影响数据面。logger flush 不是 fsync 持久性保证。
- Cluster/Auth 未注册；[v3lock.proto](../../proto/v3lock.proto)、[v3election.proto](../../proto/v3election.proto) 未进入当前 build 输入。LeaseCheckpoint 虽有 message，当前 Lease service/代理不因此具备公开 Checkpoint RPC。

## 错误、重试与响应校验

| 场景 | 当前 benchmark 语义 |
| --- | --- |
| 本地结构化证据确认未发送 | 可区分 NotSent；不能把所有 tonic 错误推断为未发送 |
| 测量路径 Range/Status transport 或明确 transient 失败 | 按方法在操作 deadline 内重试；setup 的 Status/preflight 各执行一次，不套用该循环 |
| 已发送 Txn/Compact 超时或丢失响应 | result_unknown，不原样重放；timeout 与 unknown 可重叠 |
| `TxnResponse.succeeded=false` | 合法 CAS 控制流，按 Create/Update/Delete/journal 分别处理，不等同 gRPC ABORTED |
| Range Compacted/未来 revision/异常页 | 保留错误，不把缺失快照当空成功；不实现完整 apiserver HTTP 错误翻译 |
| Watch EOF、非 no-leader 的 Unavailable/Unknown/DeadlineExceeded | deadline 内有界恢复 |
| Watch no-leader、compacted、server canceled、其他非重试状态 | semantic terminal；准备期失败，测量期记 gap 并继续其他压力 |
| 本地 supervisor 取消 | 正常回收或用户中断，不能伪装服务端 terminal；不证明远端未提交 |

Kubernetes 上层会将部分错误映射为 404/409/410/504、重新 LIST 或业务 reconcile；这些是 apiserver/client 层行为，不是本工具承诺返回 HTTP 状态。详细研究见 [错误分析](../../ref/k8s_etcd_error_handling_analysis.md)，实现入口见 [ClientError](../../etcd_k8s_bench/src/client.rs) 与 [工程错误指南](../docs/ERROR_HANDLING.md)。

当前重试参数：Range 每页最多 8 次 attempt，所有页共享一次逻辑操作 deadline，重试不额外退避；周期 Status 无独立 attempt 上限，以操作 deadline 为界、每次最多睡眠 10ms；对象 CAS 冲突在同一操作期限内重建请求，不重放可变 transport 错误。只读 transient Code 为 Unavailable/DeadlineExceeded/ResourceExhausted/Aborted，timeout 通过 typed source/Code 识别，不解析正文猜测。活跃 RPC 错误路径均按 RequestDisposition::Unknown 分类，NotSent 仅在有结构化未发送证据时才成立。

Watch 使用独立恢复策略：退避 `50ms × min(reconnects,20)`，open/backoff 监听可更新的绝对 deadline；已经建立的流可保留到 drain 后的单独 teardown。该 deadline 不代表每条 Watch 都在 measurement cutoff 瞬间被关闭。

## 已知差异与证据边界

1. **Create 文案差异已按实现澄清**：当前 `build_create_txn` 的 failure 为空，Update/Delete/journal 才带 Range；旧活动说明曾一概写 failure Range。不能为匹配文案新增一次请求。
2. **Range 校验未全覆盖**：`RangePager::observe_response` 检查 snapshot revision 与 `more=true` 空页，但未见完整 key 范围、排序和跨页推进校验。前文所列为应有协议语义，不声称当前 bench 完整检测异常后端。
3. **Watch ready/阶段字段**：recorder 仅准备期清零 storage.ready；最终字段不能简单解释为当前连接数。断线/terminal 仍通过 gap 影响 observation_complete；跨阶段 disconnect/terminal 投影另有源码风险，未由本次文档整理验证。
4. **协议版本差异**：上游研究的 MemberList.linearizable、较新 Status 字段不在本地 proto；Kubernetes 需求与当前工具强类型参数面分别列示，不静默扩充 schema。
5. **验证范围**：这些表来自当前源码、proto 与已接受资料的字段核对。本次没有重跑 RPC/故障/高压实验；生成了请求不等于后端完整兼容，通过正常 smoke 不等于故障路径已证明。

## 研究资料索引与维护

- [特性与字段使用分析](../../ref/k8s_etcd_feature_usage_analysis.md)：调用与字段面。
- [存储后端需求综合](../../ref/k8s_storage_backend_requirements.md)：apiserver 与 kubeadm 范围、revision/一致性/TTL 边界。
- [Range QA](../../ref/k8s_etcd_range_qa.md)：prefix、namespace、分页。
- [Watch prefix 与删除 QA](../../ref/k8s_etcd_watch_prefix_and_delete_qa.md)：对象/journal Watch、点删除参数。
- [Lease 使用分析](../../ref/k8s_etcd_lease_usage_analysis.md)：Grant、reuse 与 NodeLease 区别。
- [错误处理分析](../../ref/k8s_etcd_error_handling_analysis.md)：分层转换与恢复。
- [官方文档使用模式汇总](../../ref/kubernetes-1.35-etcd-usage-patterns.md)：外部来源及其适用版本，不能从文档规模建议推导合成压力。

修改请求构造、proto、proxy 注册面或 Stage 范围时，变更作者同步更新本文的参数/活跃性/差异，并在 [设计目标](design-goals.md) 同步当前有效目标与边界。旧 ref/ADR 留作来源，不再把它们的旧 benchmark 状态当当前支持面；保留真实的 Kubernetes 研究边界。审批依据仍是已接受决定，文档不能把实现缺陷合法化。
