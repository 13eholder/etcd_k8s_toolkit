# etcd_k8s_bench 对抗性审查（2026-10-18）

## 一、目前能提供什么测试

**工具形态**：`plan`（离线编译，不触后端）+ `run`（写真实后端，仅显式指定的专用空实例）。

**一次 run 的生命周期**：Status + 全 keyspace count-only preflight（非空拒绝，不清库）→ 严格准备（创建存量 + NodeReady，且全部 Watch Created-ready 后才进测量）→ 1ms tick 开环投放 → 有界 drain → Watch teardown → 九字段 JSON 报告 + 退出码（0/1/2/130，过载仍 exit 0）。

**默认负载**：44,410 个 key（2,000 Node / 40,000 Pod / 2,000 NodeLease 等六类），应用更新 10,000/s×600s，读写比 1:20，共 14 条应用流（4 类更新 + 5 类资源 × 2 种读形状）+ 4 条周期流（NodeLease 续约 10s±5% jitter、Node status 300s、Status 10s、Compact 60s），value 4KB/512B。

**结果核算**：每流守恒（planned = offered + missed + not_offered；accepted 在完成/drain/取消间守恒）、分桶速率、HDR 延迟（overflow 时如实标 `quantiles_unavailable` 而非静默）、Watch coverage（重连不抹缺口）、Prometheus/Grafana 在线观测。

**独立核验**：`verify/run_synthetic.py` 采集证据（配置副本、stdout/stderr、SHA256、keyspace、CPU/RSS），`verify/check_report.py` 完全离线重算守恒/截止/桶/速率。

**明示非目标**：不重放真实 trace、不模拟 watch-cache/controller/rollout、value 非真 K8s 对象、update/s 不能换算集群容量。这些边界文档写得很清楚，不算缺陷。

## 二、确定的 bug

| # | 位置 | 问题 | 证据 |
|---|---|---|---|
| 1 | `verify/check_report.py:903-917` | **离线核验器把 timeout 重复计入样本期望**：`expected = succeeded + failed + timeout`，但 Rust 侧 timeout 是 failed 的子集且每个 terminal 只记一次直方图样本（`measurement.rs:2542-2575`） | 已复现：把 fixture 一个成功改成超时失败（守恒保持）→ checker 误拒 `sample_count: must equal terminal outcome samples`；反之加一个幻影样本的假报告反而通过。**任何真实发生超时的实验（过载的常态）都过不了离线核验**；fixture 全零 timeout 恰好掩盖了它 |
| 2 | `watch.rs:488-492` + `client.rs:159` | **Watch 建流的本地 endpoint 超时被误分类为服务端 semantic terminal**：tonic 把本地超时映射为 `Code::Cancelled`，`is_recoverable_disconnect` 只认 `Unavailable/Unknown/DeadlineExceeded`，client 专门实现的 `is_timeout()`（`client.rs:94-107`）在 watch 路径完全没用 | 后果：慢后端下建流排队超时 → 订阅永久终止且错误来源被误报为服务端语义问题，违背 DESIGN「保留实际错误来源及发送状态」 |
| 3 | 测试套件 | **时间敏感 flake**：完整 `cargo test` 并行下间歇失败，4 次全量运行复现了 3 个不同测试各失败一次（`test_worker_heartbeat/status_failure…`、`test_worker_outer_attempt_deadline…`）；隔离运行全部通过，HEAD 提交版也通过 | 测量窗口按真实时钟判定（如 1ms admission window），宿主机调度停顿被计为 missed/failed 归属错误。工作树里 `test_dispatch_rate` 改用 `start_paused` 冻结时钟正是同一问题的修补，但只修了一个测试 |

## 三、测试过程的瑕疵（核验器逃逸面）

`check_report.py` 声称「独立重算、不只信 accounting 布尔」，但以下关系**没有重算**，矛盾报告可通过（均经构造实证）：

- `started_before_cutoff > accepted` 不拒绝（`:1496-1506` 的 `max(0,…)` 把超出的 started 吸进 cutoff_running）；
- `terminal_after_cutoff > started_after_cutoff`（甚至 started_after=0 而 succeeded_after>0）不拒绝；
- `queue[]` 数组与 `logical_total/queue_total/rpc_total` 聚合直方图从不与流计数对账，measurement/drain 聚合无流求和核对；
- `accounting.no_arithmetic_overflow` 只与常量 `True` 比对（`:1819`）；
- evidence 标注被无关错误污染（`:1930`）：报告有 rpc 速率错误时，本身完好的 plan 工件被标 `invalid`。

## 四、实现层瑕疵（按严重度摘选）

- **drain 起点归属缝隙**：懒冻结路径把 `drain_start_ns` 记为触发冻结事件的本地时间而非 `start + effective_cutoff`，dispatcher 停顿时 `[cutoff, freeze)` 区间既不算测量也不算 drain（`measurement.rs:1898`）。
- **timeout 截断样本与真实完成样本混入同一 HDR**，报告无 per-outcome 拆分，censored 观测事后不可分辨（`measurement.rs:2712-2714`）。
- **在线指标与报告舍入方向相反**：Prometheus 用 `as_micros()` 截断、HDR 向上取整，派生均值系统性偏低，无文档说明。
- **准备期 Watch 一旦 terminal 无恢复路径**（不重建、不清 terminal_code），只能等 preparation 超时整跑失败——「严格准备」下偏保守，但一次瞬时断连代价是整个实验。
- 死代码/脆弱面：`dropped_closed` 恒 0 字段、`_cutoff_deadline` 死参数、drain 超时靠错误字符串前缀匹配（`scheduler.rs:2904`）、Status/Compact 周期流全压 worker 0（`scheduler.rs:2067-2070`）、`WatchState.event_bytes` 只写不读、no-leader 判定靠全等字符串（`watch.rs:495-500`）。

## 五、需要修改/补全（建议优先级）

**P0 — 核验链正确性**（工具的公信力所在）：

1. 修 checker timeout 双重计数，并**加一个 timeout>0 的真实形状回归用例**——`test_verify.py` 14 个测试全没覆盖这条路径；
2. 修 watch 建流超时的分类：把 `Cancelled` + `TimeoutExpired` source 归为可恢复或独立的 timeout terminal；
3. 消除测试 flake：把对真实时钟敏感的断言迁移到受控时钟（`start_paused` / 已有的 `MonotonicClock` 注入模式）。

**P1 — 核验覆盖补全**：checker 补 started≤accepted、after-cutoff 终态≤开始、queue/聚合直方图对账、`no_arithmetic_overflow` 独立核对；修 evidence 三态标注。

**P2 — 口径与清理**：文档明示 `cutoff_queued/running` 是「截止瞬间曾在队/在跑」语义、dropped_full 的 per-shard 粒度、两种微秒舍入差异；清理死代码；drain 超时改 typed 判定；`run_synthetic.py` 修 SIGINT 去重、plan 阶段 Ctrl-C 传播、SIGTERM 处理。

## 结论

核心引擎（计划算术、tick 投放、守恒、归属、生命周期）经 5 路独立审查未发现破约缺陷，设计文档与实现一致性罕见地好。**薄弱点集中在核验器**（一个已复现的假阴性 bug + 多处逃逸面）和**测试套件的时间敏感性**——对于一个以「可核算」为卖点、且把「后端过载是合法结果」写进设计的工具，过载报告反而过不了自家离线核验，是最该先修的一处。

---

*审查方法：5 路并行只读子审查（scheduler/pacing、measurement/report/metrics、plan/config/cli、client/workload/watch/heartbeat、verify/monitoring）+ 主线独立复核；关键结论（checker 假阴性、watch 误分类、测试 flake）均由审查者本人复现确认。*
