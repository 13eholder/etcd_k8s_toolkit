# 当前设计目标

## 定位与文档职责

`etcd_k8s_toolkit` 的当前产品目标是：**按 Kubernetes 访问规则生成固定存量，在独立真实时间窗口内按目标速率和周期施压，以可解释、可核算的结果粗略评估和比较存储后端承载能力。**

本文只记录当前目标、非目标、领域概念与验收要求，不记录设计演进、旧目标或实施状态。参数级协议边界见 [API 子集](api-subset.md)，工程编码/测试规则见 [工程知识索引](../docs/index.md)，操作命令见 [README](../../README_zh.md)。决策依据另见 [设计索引](../docs/design-docs/index.md)。

## 适用边界

本文描述应满足的产品契约，不是实现完成或验收通过声明。详细验收条件见 [执行规格](../stages/006-rule-driven-timed-pressure/spec/timed-pressure.md)，实现导航见 [设计说明](../../etcd_k8s_bench/DESIGN_zh.md)。

当前 benchmark 不施加 native Lease 压力；这不改变 Kubernetes 对 native Lease 的需求，也不限制 proxy 的通用 Lease 转发能力。三者的参数范围分别以 API 子集文档为准。

## 当前系统边界

| 组成 | 当前职责 | 不能由此推出 |
| --- | --- | --- |
| `etcd_k8s_bench` | 规则到计划、存量准备、开环混合负载、生命周期、每次运行计量 | 完整 Kubernetes 控制面、通用 etcd 兼容认证 |
| `etcd_proxy` | etcd gRPC 转发、局部请求与 Watch 观测 | 自身实现存储、持久性、透明覆盖所有 etcd 服务/metadata |
| `verify/run_synthetic.py` | 调用同一个 plan/run，保存配置、命令、原始输出、后端状态与采样 | 自动部署后端、清库、提高 quota |
| `verify/check_report.py` | 离线独立检查可观察的报告算术与可选外部证据 | 重算原始 HDR 分位数、证明后端容量或全面兼容 |

高压力比较默认直连后端，避免逐请求日志改变被测负载。proxy 可用于小规模协议证据；不是主路径必经层。后端与生成器同机时记录资源竞争。

## 压力模型

### 固定存量与六类资源

| 资源 | 默认数量 | key 域 | 测量职责 |
| --- | ---: | --- | --- |
| Pod | 40,000 | `/registry/pods/<namespace>/<name>` | 应用更新、主动读、Watch |
| Node | 2,000 | `/registry/minions/<name>` | NodeReady 准备、周期 status、主动读、Watch |
| NodeLease | 2,000 | `/registry/leases/kube-node-lease/<node>` | 周期普通 KV 心跳、Watch |
| Deployment | 200 | `/registry/deployments/<namespace>/<name>` | 应用更新、主动读、Watch |
| ReplicaSet | 200 | `/registry/replicasets/<namespace>/<name>` | 应用更新、主动读、Watch |
| DaemonSet | 10 | `/registry/daemonsets/<namespace>/<name>` | 应用更新、主动读、Watch |

默认共 44,410 个资源 key。namespace 默认 100，仅用于 key 分布，不另写 Namespace 资源。DaemonSet 对应 Pod 数为 `node_count × daemonset_count`；其余 Pod 按 `pods_per_deployment` 分组，Deployment 与 ReplicaSet 数量相同。关系只推导基数，不执行控制器。

`compact_rev_key` 是独立协调 journal，按实际周期可以不存在或有一个，不是第七类资源。NodeReady 更新不增加 key。value 是指定大小的不透明合成字节，不是可交给 apiserver 的 Kubernetes 对象。

“滚动更新式压力”指大量既有对象持续更新及其关联访问，**不要求真实 rollout、Pod 删除重建、GC、跨资源因果联动**。ConditionalDelete 请求 helper 的存在不意味着测量期有主动删除。

### 配置与默认条件

唯一默认参数源是 [synthetic-k8s.json](../../etcd_k8s_bench/workloads/synthetic-k8s.json)；本文数值是该配置的可读投影，不另立默认值。外部 JSON 完整替换，CLI 标量最后覆盖，再统一校验；未知字段拒绝。`plan` 与 `run` 共用同一个生效配置与编译计划，`plan` 不连接后端。

| 条件 | 当前默认 | 解释 |
| --- | --- | --- |
| 应用逻辑更新 | 10,000/s × 600s | 计划 6,000,000 次；不是保证的物理 Txn/s |
| 主动读/写比 | 1:20 | 计划主动读 300,000 次，独立于 Watch |
| 更新权重 | Pod/Deployment/ReplicaSet/DaemonSet = 95/2/2/1 | 资源逻辑流按各自有理数速率取整 |
| 主动读资源权重 | Pod/Node/Deployment/ReplicaSet/DaemonSet = 90/5/2/2/1 | NodeLease 不参与主动读权重 |
| 点读/有界列表权重 | 90/10；列表逻辑上限 100 | 一次逻辑列表可能有多个物理 Range |
| NodeLease / Node status | 10s、确定性 5% jitter / 300s | 独立周期，不计入应用更新目标 |
| Status / compaction 协调 | 10s / 60s | 一次协调不必然产生 Compact RPC |
| Watch | 六根各 1 个订阅 | 不乘 Node 数；事件速率由实际写入与订阅决定 |
| value | NodeLease 512B，其他 4096B | 不是 wire 字节数或真实对象平均大小 |
| workers / 总队列容量 | 128 / 4096 | 执行资源，不取代目标到达速率 |
| RPC / 逻辑操作 timeout | 5s / 10s | 操作 deadline 从 worker 开始执行计时，不含排队 |
| prepare / drain / Watch teardown | 180s / 10s / 5s | 独立生命周期预算 |
| 时间桶 / 观测预算 / seed | 1s / 128MiB / 1 | 有界观测估算不是进程绝不 OOM 的保证 |

历史流量只参考访问形状、读写比例及 key，不能用于隐式封顶目标速率或换算真实 Node/Pod 容量。目标压力应能探索远高于历史观测的区间，但不承诺未经实测的最大 Txn/s。

### 开环与单一真实时间

- 测量按 1ms tick 投放，允许单 tick 内小批量；不承诺微秒均匀到达。
- 每条流计划按有理数速率和时长向下取整，不能仅从聚合速率推算短窗口各流次数。
- 调度迟到跳过过期 slots，记 `missed_schedule`，不补发历史、不以成功配额延长窗口。
- 队列有界、满则 `dropped_full`；不通过无限排队掩盖过载。最终 admission 再检查时间边界。
- 逻辑更新、CAS attempt、物理 Txn、逻辑列表、Range page、Watch event 是不同计量单位。
- worker 并发只影响执行能力；固定并发尽力打满不是当前主要压力模型。

## 生命周期与结果含义

`校验/计划 → connect 与只读 preflight → prepare → measurement → drain → Watch teardown / 任务回收 → report`。

1. 使用显式指定的专用可丢弃空后端。Status 和全 keyspace count-only 检查成功后才写入；任何已有 key 均拒绝，不清库、不调 quota。
2. 创建六类对象、执行 NodeReady，再等待全部订阅当前 Created-ready。准备失败不开启测量；Watch 不要求覆盖准备创建过程。
3. measurement 是 `[start, effective_cutoff)`；恰好 cutoff 属于 drain。普通 RPC 失败、超时、过载和测量期 semantic Watch gap 不截断正常配置窗口。
4. 截止后停止新投放，已接受工作允许有界 drain，余项取消；随后单独回收 Watch。取消本地等待不证明远端未提交。
5. 中断或软件/生命周期故障可以提前截止；保留部分结果，所有自有任务回收后输出最终报告。

| 问题 | 判定载体 | 不能混同 |
| --- | --- | --- |
| 实验是否完成 | `experiment.status` 与退出码 | 有丢弃/错误不必然执行失败 |
| 目标是否投出、后端接住多少 | planned/offered/missed/dropped/accepted/completed/failed/canceled、吞吐/延迟 | 后端没收到的漏投不是后端拒绝 |
| 测量窗口与观察是否完整 | `measurement_complete`、Watch coverage、`observation_complete` | 完整观察不等于请求全部成功 |
| 工具证据是否自洽 | checker 与原始工件 | checker 成功不等于容量达标，也不代替故障反例验证 |

退出码：0 正常完成实验；1 执行/setup/生命周期或结果交付失败；2 参数/配置错误、无运行报告；130 中断。未配置 SLO 阈值时只报告事实，不自行判定承载达标。

最终报告恰有九个顶层字段：`schema_version`、`experiment`、`resolved_config`、`plan`、`timing`、`preparation`、`measurement`、`drain`、`accounting`。最终 per-run 数据是结果真相源；在线 Prometheus 与低频 CPU/RSS 是辅助观察，不从最后一次 scrape 拼报告。

## 结果可信性要求

- 计划、投放、接受 cohort、阶段事件、桶和 HDR 样本分别可对账；跨 cutoff 的 RPC 可在 measurement 开始、drain 完成，单阶段 starts/completions 不强行相等。
- logical 延迟从接受到终止，含排队；queue wait 从接受到开始；RPC 延迟只覆盖单次 attempt。取消不造完成样本，overflow/censored 不静默丢失。
- `timeout` 与 `result_unknown` 是可重叠的 failed 子集，不能相加。可变请求已发送但响应丢失时不盲重放。
- Watch 只累计实际可观察区间，重连不抹去先前缺口；截止后的回收不能改写已冻结测量窗口。完整 relist 不在当前范围。
- protobuf payload 不是 HTTP/2/TLS/网络字节；采样 RSS 峰值不是精确进程峰值。
- checker 根据原始报告独立检查可见算术，不只相信 `accounting=true`；不复制计划编译算法或声称重算缺少原始计数的 HDR 分位数。

## 明确非目标

历史 trace 重放、压力强度/时间曲线复刻、真实 Kubernetes 容量换算、控制面 SLO 模型、apiserver/watch-cache/controller/admission 仿真、真实对象编码、真实连接拓扑、完整 relist、native TTL 持续压力与主动 Pod 删除重建，均不是当前验收要求。也不自动引入分布式生成器、无限队列、自动 quota 调整或后端部署权限。

## 验收要求

- 默认 plan 的资源基数、逻辑预算和配置覆盖正确；非法配置在连接后端前拒绝。
- 正常运行准备与测量分离，完整执行配置窗口；最终 keyspace 符合六类资源和可选 journal 的要求。
- 后端慢、普通错误或队列满时持续计量；调度暂停后跳过过期 slots，不追赶补发。
- cutoff 前后事件、跨窗 RPC、drain 完成与取消正确归属，无重复计数或伪造样本。
- Watch readiness 失败不能开启测量；断线恢复与 semantic terminal 分别处理，覆盖缺口如实保留。
- 中断与软件故障有界回收，分别返回有效部分报告和正确退出码。
- 准备摘要、逻辑操作、物理 RPC、时间桶与延迟样本可独立核对；验证工具能拒绝相互矛盾的报告。
- 真实后端压力验证覆盖 1k/10k/50k/100k 逻辑更新/s 各 60s，以及默认 10k/s × 600s。记录完整配置、命令、后端状态与原始结果，不以测试 double 替代真实压力证据。
- 比较后端时同时报告实际 offered/missed/drop、完成吞吐与延迟，区分生成器限制和后端限制；不以配置速率、退出码或单次结果宣称后端容量。

## 维护约定

目标获批准变更后，直接更新本文，使其始终只表达当前有效目标，不追加演进日志、旧目标对照或实施评估。API 范围同步更新 `api-subset.md`；决策过程、变更原因和验收记录保留在独立的 ADR/Stage 文档中。实现缺陷或单次实验结果不能自动改变产品目标。
