# 合成压力的 etcd 访问模式

本文区分官方协议事实与 benchmark 的合成选择。压力参数来自 [synthetic-k8s.json](../workloads/synthetic-k8s.json)，不来自历史 trace 或真实集群统计；这些模式不能换算 Kubernetes 容量。

## 六类 key 域

| 对象 | benchmark 固定根 | 范围 |
| --- | --- | --- |
| Pod | `/registry/pods` | namespace 内 |
| Node | `/registry/minions` | cluster |
| Coordination NodeLease | `/registry/leases/kube-node-lease` | 专用 namespace |
| Deployment | `/registry/deployments` | namespace 内 |
| ReplicaSet | `/registry/replicasets` | namespace 内 |
| DaemonSet | `/registry/daemonsets` | namespace 内 |

key 的具体名称由当前 plan 确定性生成；NodeLease 与对应 Node 名称相同。namespace 是命名空间分布参数，不另写 Namespace 资源。控制器存量按配置关系计算，不执行控制器循环或真实 rollout。value 是指定长度的合成数据，不是 Kubernetes 序列化对象。普通 NodeLease 的存储不附加 native etcd lease。

另有独立协调 key `compact_rev_key`，它不是第七类资源；根据实际 compaction tick 可以不存在或存在一个。准备完成后的 keyspace 只能包含计划六类资源和这个可选 journal。

## 写入：ModRevision CAS

官方 Kubernetes etcd storage 实现通过底层客户端执行 create、guaranteed update 及有条件删除，乐观并发由 revision 表达。benchmark 的 Create 使用 `ModRevision == 0`，成功分支 Put，失败分支点 Range；更新比较已知 ModRevision，冲突返回当前 KV 后重建下一次请求的 revision 和 value。Create 冲突不是可盲重试的 transport 错误。

单次逻辑操作可产生多个 Txn RPC attempt。已发送的可变操作遇到 timeout/连接丢失时，客户端可能不知道服务端是否已提交；benchmark 将此类结果标记 result_unknown，不原样重放。该标记与 timeout 可以重叠。客户端取消只证明本地等待结束，不证明远端未提交。

低层保留的 conditional-delete 请求形状不意味着当前定时工作负载执行真实删除/替换生命周期。

## 读取：范围与快照分页

etcd Range 使用半开字节范围 `[key, range_end)`；点读使用空 range_end。prefix end 按字节递增求上界，不能简单在字符串后追加字符充当通用前缀边界。benchmark 的主动读包括线性一致点读和 limit 有界列表；默认列表 limit 为 100，不隐式变成固定 500 条。

列表第一页获取响应 revision，后续页固定该快照，按最后一个已扫描 key 后的位置推进。revision 保留历史受 compaction 限制，压缩后的快照不能假装成功继续。`more=true` 却没有可推进 key、返回范围/顺序不合法等是协议错误，不能以成功的 transport RPC 掩盖。主动读逻辑次数、物理 Range 页数与返回条数是不同量。

官方线性一致 KV 保证不意味着 Watch 线性一致，也不意味着多页请求在不固定 revision 时自动共享快照。

## Watch：Created、恢复与观测缺口

benchmark 为六个固定根创建订阅，默认每根一个，`prev_kv=true`、`progress_notify=true`、`start_revision=0`。请求使用 etcd 的 `hasleader=true` require-leader metadata。

只有收到合法 Created 才打开该订阅的 ready 屏障；提前到来的 progress/event 不替代 Created。准备期必须全部订阅当前 ready，历史上曾 ready 不足以开始测量。断开后按已交付 revision 恢复，不能跳过历史；合法 watch_id=0 也必须作为当前 ID 取消。

官方 Watch 在可用历史内提供有序、原子、可靠、可恢复事件，但不保证与 KV 操作之间的线性一致性或有界交付延迟。benchmark 将可恢复 transport 断开与 compaction/server cancel/no-leader/semantic terminal 区分；无法恢复的缺口不冒充完整 relist 修复。

测量期间 semantic terminal 保留 coverage 缺口，不因此提前结束普通压力窗口。coverage 按窗口内订阅可用区间累计，多订阅根按订阅时间聚合；重连不能清除早先缺口。截止后的主动 teardown 不反向损坏测量窗口。观察完整与请求承接能力分别判断。

## NodeLease 与 native etcd lease

Kubernetes Node 心跳通过更新 `coordination.k8s.io` Lease 对象的 `spec.renewTime` 表达；这是 API 对象层的租约语义。etcd Lease API 则是 Grant、Put.lease、KeepAlive、Revoke 和 TTL 过期删除机制，两者不能因名称相同而混同。

当前 benchmark 用普通 KV CAS 更新合成 NodeLease，另有 Node status 周期更新；不执行 native LeaseGrant/KeepAlive 持续压力、不模拟 TTL 到期，也不宣称模拟节点故障判定或控制器选主。

## Status 与 compaction

Status 用于前置检查和独立周期观测。Compaction 通过 `compact_rev_key` Version CAS 协调，只有合适的已知历史 revision 才发逻辑 Compact；第一轮可能只建立 journal。协调冲突、journal 缺失/非法内容需按实际状态恢复，不能拿陈旧本地 revision 盲目 Compact。协调 Txn 与 Compact 必须分别计入实际物理请求。

## 研究资料的使用边界

- Kubernetes调用的子集不是通用etcd兼容性认证。业务层未直接读取的字段仍可能被客户端用于流管理、事件判定和端点发现，不能据此将响应字段清零；“未调用某能力”也不等于后端实现该能力会失真。
- Txn请求声明failure Range不证明该分支被执行，更不证明发生了CAS冲突。revision随实际键空间变更推进，不能用“成功RPC数”普遍代替写入或revision增量。
- RequestProgress是Watch流内的`progress_request`，不是独立一元RPC。重叠订阅必须独立交付；共享存储前缀不证明实际存在固定数量的cache订阅。
- 原生TTL示例与Kubernetes leaseManager的复用窗口/附加计数来自客户端配置和策略，不是etcd协议只允许某些TTL或固定key数的限制。
- Kubernetes compactor的journal跟随是Get后进入Watch，退出/失败后约1s重建；不是健康状态每秒必发一次Get。当前synthetic的周期协调不宣称复现完整apiserver跟随生命周期。
- 源码默认值、官方规模说明和本工具的合成参数分别解释，不能用已删除的历史采样补齐重试安全性、精确建流数量、跨资源映射或容量证明。

## 官方依据

- [Kubernetes v1.35.5 etcd3 store](https://github.com/kubernetes/kubernetes/blob/v1.35.5/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go)：key 处理、Create、GuaranteedUpdate、列表与 revision 边界。
- [etcd v3.6 API guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/)：KV 严格可串行化、Watch 恢复/交付边界、timeout 的提交不确定性、native Lease。
- [Kubernetes Leases](https://kubernetes.io/docs/concepts/architecture/leases/)：NodeLease 心跳对象与 renewTime。
- [Kubernetes v1.35.5 compactor](https://github.com/kubernetes/kubernetes/blob/v1.35.5/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go)：journal CAS与Get/Watch跟随的重建周期。

上述来源支持协议解释；六类存量、value 大小、速率、权重与周期是本工具明确选择的合成配置，不应归因于官方建议值。
