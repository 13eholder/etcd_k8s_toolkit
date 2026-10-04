# 规则驱动的定时定速压力设计

## 目标与非目标

工具在专用空 etcd 上产生可解释、可核算的 Kubernetes 形状合成压力。配置决定存量、物理请求形状、逻辑速率与真实时间窗口；结果分别表达实验是否完成、目标投放是否兑现、后端承接能力和 Watch 观测是否完整。

不重放真实 trace，不实现 apiserver/watch-cache/controller，不模拟真实 rollout、native TTL 持续压力或完整 relist。合成字节不冒充 Kubernetes 对象；logical update/s 不可直接换算 Kubernetes 集群容量。

## 模块边界

| 模块 | 职责 |
| --- | --- |
| `config.rs` / `cli.rs` | 唯一完整 synthetic JSON、CLI 覆盖、边界验证、plan/run 入口 |
| `plan.rs` | 六类资源、确定性 key/value、速率有理数、周期事件、Watch 计划、容量预算 |
| `scheduler.rs` | 严格准备、1ms 开环投放、有界队列、worker、阶段控制和自有任务回收 |
| `pacing.rs` | 在投放调用线程上高分辨率等待；单次 sleep 请求至多 1ms 后协作式 yield，无额外定时线程、不忙等 |
| `workload.rs` | Txn/CAS 请求与响应状态转换 |
| `client.rs` | 共用连接、自适应 HTTP/2 接收窗口、实际 RPC、typed transport 错误、只读 preflight |
| `watch.rs` | Created、revision、当前就绪、恢复与 terminal 分类 |
| `measurement.rs` | 单调时钟事件归属、token/cohort、计数、桶、HDR、Watch coverage |
| `metrics.rs` | 有界在线观测、exporter 连接与后台任务回收 |
| `report.rs` | 最终九字段协议投影，不发明新的计量语义 |
| `verify/` | 调用唯一 plan/run、采集原始证据、独立离线算术核对 |

配置解析与预算编译只有一处。周期计划在 run 配置克隆时共享，不随 worker 深拷贝。工具不重复编译压力预算，也不修改后端 quota 或自动清库。

## 默认存量与计划

权威输入：[synthetic-k8s.json](workloads/synthetic-k8s.json)。默认 2,000 Node、40,000 Pod、100 namespace、10 DaemonSet、200 Deployment、200 ReplicaSet、2,000 普通 NodeLease，共 44,410 个资源 key。namespace 仅控制 key 分布，不另创建 Namespace 对象。DaemonSet 消耗 `node_count × daemonset_count` 个 Pod，其余 Pod 按 pods_per_deployment 分组，Deployment 与 ReplicaSet 数量相同。

默认应用更新 10,000/s × 600s，计划 6,000,000 次；主动读写比 1:20，计划 300,000 次。四类应用更新流、五类资源各两种主动读形状组成 14 条逻辑应用流；另有 NodeLease、Node status、Status、Compact 四条周期流。零权重流仍保留身份，计划按每流有理数向下取整，不能从总预算推测每个短窗口的取整结果。

普通 NodeLease 10s 周期、5% 确定性 jitter，Node status 300s，Status 10s，Compact 60s。周期流与应用逻辑更新预算分开。value 默认 4,096 字节，NodeLease 为 512 字节。具体 key、CAS、Range、Watch 与 lease 区别见 [访问模式](docs/access-patterns.md)。

## 生命周期

1. 解析完整配置、应用 CLI 覆盖、验证并编译计划。`plan` 到此输出，不接触后端。
2. `run` 建立 exporter 与连接，执行 Status 和全 keyspace count-only preflight；非空直接拒绝，不清库。
3. 在准备期限内创建六类资源并执行 NodeReady。所有准备成功且全部 Watch 当前 Created-ready 后，原子进入测量。
4. 按 1ms tick 开环投放。迟到只投当前 tick，旧 slots 计 missed_schedule，不追赶、不以成功配额延长窗口。队列满明确 dropped_full。
5. 冻结有效截止；停止新投放，已接受工作进入有界 drain。之后单独停止 Watch，回收全部自有任务、exporter 与连接，再输出报告。

准备、measurement、drain、Watch teardown 独立计时。中断和致命生命周期错误可以提前截止；普通 RPC 失败、后端过载和测量期 Watch semantic gap 不缩短完整配置窗口。过载正常完成仍 exit0，不能用 exit0 宣称压力达标。

1ms 投放窗口不能依赖 Tokio 向上取整的毫秒定时轮等待。投放在 `Runtime::block_on` 调用线程上直接等待，去掉独立定时线程加 Notify 的二次唤醒；单次 sleep 请求至多一个 tick 后协作式 yield，RPC timeout 等继续使用 Tokio。OS 实际睡眠可能超过请求时长，真正的调度停顿仍跳过过期 slots，不改变计划、计数或截止以伪装目标达标。CLI 默认使用 4 个 Tokio 执行器线程，保留 `TOKIO_WORKER_THREADS` 覆盖；不等同于配置的 128 个逻辑 worker。tick 目标使用固定有界栈空间，更新 key 复用分片计算结果，避免每 tick 分配和每更新重复构造。

## 事件归属与守恒

measurement 为半开窗口 `[start, effective_cutoff)`，正好截止归 drain。admission、logical start/terminal、RPC start/completion 分别按真实事件时间归属，不能用一次入口时间代表后续事件。跨窗 RPC 在 measurement 开始、drain 完成是合法情况；阶段 starts/completions 不强行相等。

计划按完整配置时长固定。每流 `planned = offered + missed_schedule + not_offered_after_stop`。offered 再拆分 accepted 与显式拒绝；accepted cohort 在窗口内完成、drain 完成或最终取消之间守恒。队列满不变成已接受；等待准备队列许可但尚未获准的任务不能冒称已入队。没有真实完成的 canceled/in-flight RPC 不产生完成样本。

timeout 与 result_unknown 是可重叠的 failed 子集。外层逻辑 deadline 与服务端 DeadlineExceeded 都保留实际错误来源及发送状态；可变操作结果未知时不原样重放。普通失败不会掩盖第一次错误，也不自动终止测量。

## 观测边界

- 最终 per-run 计数独立于进程全局 recorder，两个运行之间不能污染。
- 固定数量桶按有效窗口裁剪，部分末桶保留真实宽度；未观察的区间不造速率。
- HDR 有界，报告可观察的元数据、样本、分位数、max 和 overflow；不能将溢出或 censored 样本静默当成功观测。
- protobuf payload 字节是编码后的消息大小，不等于 HTTP/2、TLS 或网络流量。
- Watch coverage 累计窗口内各订阅真实可用区间；当前 ready 与曾经 ready 不同，重连不抹掉缺口，截止后 teardown 不反写窗口缺口。
- Prometheus 使用有限在线投影，不保留无限原始延迟样本。抓取和 CPU/RSS 采样有成本，建议约 1s；采样峰值不等于精确最大值。
- 高压默认直连，不经全量请求日志代理。后端与生成器共享机器时必须记录资源竞争，不能把局部协议 fixture 当真实后端吞吐。
- [本机 Prometheus + Grafana](monitoring/README.md)复用在线指标，额外导出无标签 `etcd_k8s_bench_target_update_rps` gauge 表示当前应用更新配置目标。看板只显示已有 sum/count 派生均值与最大值，不冒充 HDR 分位数；Prometheus 保留已抓取历史，与 exporter 生命周期分离。

## 报告与退出

最终报告恰有 `schema_version`、`experiment`、`resolved_config`、`plan`、`timing`、`preparation`、`measurement`、`drain`、`accounting` 九个顶层字段。setup/准备/运行失败尽可能保留已发生阶段和部分结果；未开始阶段保持契约规定的空状态，不制造观测。

| 退出码 | 语义 |
| --- | --- |
| 0 | 正常完成实验，不保证压力达标或 Watch observation_complete |
| 1 | 执行/setup/生命周期失败 |
| 2 | 参数/配置错误，无运行报告 |
| 130 | 中断，保留可获得的部分报告 |

## 可重复验证

[英文使用说明](../README.md)与[中文使用说明](../README_zh.md)给出构建、plan、小规模 run 和采集命令。`verify/run_synthetic.py` 使用新输出目录保存配置副本、原始 stdout/stderr、argv/退出码、二进制 SHA256、机器/后端状态、keyspace、metrics、CPU/RSS。它不管理后端，不修改 quota；watchdog 与测量 cutoff 分开，所有自有子进程/采集线程有界回收。

`verify/check_report.py` 完全离线，核对 schema、身份、守恒、截止、桶、速率、HDR 可观察元数据和退出一致性。不能只相信 accounting 布尔值，不能声称从聚合报告重算原始 HDR 分位数。未提供 plan/keyspace 明确未检查；准备未完成不能作为完整六类存量检查通过。

每次真实验证使用新空实例和目录。压力比较保持对象形状/value/周期不变，只改变更新速率；默认 10k/s×600s 与 1k/10k/50k/100k/s 各60s单独记录。后端过载可以是有效实验结果，但未跑高档、报告矛盾或生成器少投无解释不能作为验证通过。优化前先 profile 定位，再局部修改并重跑受影响档位。
