# 架构与模块依赖

## 权威设计与系统边界

**已确立规范**：bench 的模块职责、生命周期及观测边界由 [DESIGN_zh.md](../../etcd_k8s_bench/DESIGN_zh.md) 维护；请求形状和参数边界由 [API 子集](../domain/api-subset.md) 维护。本页只提供代码导航，不复制完整契约。设计与历史决策见 [设计索引](design-docs/index.md)。

**现有实践**：[Cargo.toml](../../Cargo.toml)定义两个独立二进制 crate，没有互相声明 Cargo 依赖。它们共享 proto 输入，不共享一个 Rust library crate。proxy 是可选代理而非 benchmark 运行必经层；高压默认直连的原因与边界见 DESIGN 的观测章节。

## bench 数据流

`main/cli → config → plan → scheduler → client/workload/watch → etcd`；事件经 `measurement` 归集，`report` 输出最终 JSON，`metrics` 提供在线辅助观测。

| 修改关注点 | 源码入口 | 边界 |
| --- | --- | --- |
| 解析、CLI 覆盖、退出 | [main](../../etcd_k8s_bench/src/main.rs)、[cli](../../etcd_k8s_bench/src/cli.rs)、[config](../../etcd_k8s_bench/src/config.rs) | plan/run 共用完整配置及校验 |
| 资源与投放计划 | [plan](../../etcd_k8s_bench/src/plan.rs)、[workloads](../../etcd_k8s_bench/workloads/synthetic-k8s.json) | 合成参数不是生产 trace 或官方容量建议 |
| 阶段与任务所有权 | [scheduler](../../etcd_k8s_bench/src/scheduler.rs)、[heartbeat](../../etcd_k8s_bench/src/heartbeat.rs) | 准备、测量、drain、Watch teardown 分离 |
| 高分辨率投放等待 | [pacing](../../etcd_k8s_bench/src/pacing.rs) | 在投放调用线程直接等待，每次 sleep 请求至多 1ms 后协作式 yield；无额外线程，不改变迟到跳过和有界队列语义 |
| RPC 与状态转换 | [client](../../etcd_k8s_bench/src/client.rs)、[workload](../../etcd_k8s_bench/src/workload.rs)、[watch](../../etcd_k8s_bench/src/watch.rs) | 传输结果、CAS 冲突、Watch 语义终止分别处理 |
| 计量与输出 | [measurement](../../etcd_k8s_bench/src/measurement.rs)、[report](../../etcd_k8s_bench/src/report.rs)、[metrics](../../etcd_k8s_bench/src/metrics.rs) | 最终 per-run 报告与在线全局 recorder 分离 |
| 外部证据 | [runner](../../etcd_k8s_bench/verify/run_synthetic.py)、[checker](../../etcd_k8s_bench/verify/check_report.py) | 采集与独立算术核对，不复制调度算法 |
| 本机性能看板 | [monitoring](../../etcd_k8s_bench/monitoring/README.md) | Prometheus 抓取现有在线指标，Grafana 自动加载六面板；最终 JSON 仍是计量真相源 |

## proxy 数据流

**现有实践**：[main.rs](../../etcd_proxy/src/main.rs)解析 Clap 参数、配置连接与 server；[proxy.rs](../../etcd_proxy/src/proxy.rs)实现 KV、Watch、Lease、Maintenance 转发及日志管线。下游 gRPC 请求经 tonic client 到上游，流式 RPC 有独立请求/响应转发路径。不是存储实现，不赋予新的持久性保证；具体错误与日志行为见 [错误处理](ERROR_HANDLING.md)。

## 声明依赖与生成边界

- [workspace manifest](../../Cargo.toml)：Tokio 异步运行时、tonic gRPC、prost 编解码、Clap 参数、serde JSON；声明版本范围与锁定解析版本分别看 manifest 和 [Cargo.lock](../../Cargo.lock)。
- [bench manifest](../../etcd_k8s_bench/Cargo.toml)：另有 thiserror、HDR、metrics exporter 与 Hyper HTTP 服务依赖；[proxy manifest](../../etcd_proxy/Cargo.toml)：另有 async-stream、rand。
- 两个 [bench build.rs](../../etcd_k8s_bench/build.rs)、[proxy build.rs](../../etcd_proxy/build.rs) 均用 tonic-prost-build 编译 `proto/rpc.proto`；[bench pb](../../etcd_k8s_bench/src/pb/mod.rs)、[proxy pb](../../etcd_proxy/src/pb/mod.rs) 引入 `OUT_DIR` 产物。`proto/` 中存在额外文件不证明 server 已注册对应服务。

依赖清单只证明当前声明依赖，**不证明允许或禁止的架构依赖**。目前未发现 crate 间禁止访问清单、自动分层检查或公共 Rust API 稳定性政策；变更依赖边界需要维护者确认，不把本页的数据流示意当作强制 DAG。

## 生命周期与并发核验边界

bench 自有任务必须按当前设计回收后才输出最终结果；共享计划、有限队列与有限观测预算有设计依据。具体实现与错误核验应沿 scheduler、watch、metrics 调用链检查。没有全项目锁顺序或 unsafe/FFI 政策证据，见 [编程规范缺口](CODING.md#待确认政策)。初始化没有执行并发、崩溃或真实后端验证，源码观察不等同于这些性质已被实验证明。
