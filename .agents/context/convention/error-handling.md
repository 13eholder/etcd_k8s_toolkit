# 错误处理与结果边界

## bench 已确立行为

权威维护位置为 [设计说明](../../../etcd_k8s_bench/DESIGN_zh.md)“生命周期”“事件归属与守恒”“报告与退出”和 [API 子集](../arch/design-docs/api-subset.md)的 CAS、分页、Watch 章节。这里只给变更导航：

- 参数/配置错误退出 2，不输出运行报告；执行/setup/生命周期失败退出 1；中断 130；正常完成 0 不代表压力达标或 Watch 观察完整。
- 普通请求失败、队列满与测量期 Watch semantic gap 不提前结束普通配置窗口。准备失败不能开启测量。
- 可变请求已发送后超时/丢失响应可能已提交，不能盲目原样重放；timeout 与 result_unknown 可重叠，取消不证明远端未提交。
- CAS 冲突、transport 失败、分页协议错误和 Watch semantic terminal 分开处理。读操作可恢复不意味着可变操作可无条件重试。

## 实现入口与分类

**现有实践**：[client.rs](../../../etcd_k8s_bench/src/client.rs)的 `ClientError` 保留 Transport source 或 tonic Status；`is_timeout` 根据 `DeadlineExceeded` 或 source chain 的 `tonic::TimeoutExpired` 判断，不从文字猜 timeout。`retry_decision(method, disposition)` 结合 Range/Status 与 Txn/Compact 的可变性，以及有结构化证据的 NotSent/Unknown，返回 Retry、NotSent、ResultUnknown 或 Stop。

该分类函数不等于调用方已实施重试。修改重试必须继续检查 [scheduler.rs](../../../etcd_k8s_bench/src/scheduler.rs)中实际循环、预算和取消边界；CAS 响应转换在 [workload.rs](../../../etcd_k8s_bench/src/workload.rs)，Watch 重连/终止在 [watch.rs](../../../etcd_k8s_bench/src/watch.rs)。错误向 CLI/报告的转换沿 [cli.rs](../../../etcd_k8s_bench/src/cli.rs)、[report.rs](../../../etcd_k8s_bench/src/report.rs)核验，不只检查错误 enum。

Watch open 同样复用 typed TimeoutExpired source 判断，进入有界恢复；不是把全部 Cancelled 都当作 timeout。dispatch/drain 用 ControlFailure 和结构化超时事实传递信号是否已消费、Interrupted/SignalFailure 及 worker 首因，不能匹配错误正文决定后续是否 poll signal future。runner 的 SIGINT/SIGTERM 转发、plan 启动门禁及未执行证据属内部 verify 工具（`etcd_k8s_bench/verify/`，仅本地）行为。

## proxy 现有行为及缺口

以下是源码观察，不是统一错误政策，来源 [main.rs](../../../etcd_proxy/src/main.rs)、[proxy.rs](../../../etcd_proxy/src/proxy.rs)：

- 一元方法记录请求后调用上游，返回 tonic Status；没有额外重试/统一翻译层。
- Watch 的下游输入错误通过 oneshot 通知响应转发任务，上游错误作为 Status 交付，EOF 才按正常流结束。转发 task 的 JoinHandle 未保存在服务对象中，不能把 bench 的全任务 join 承诺扩展到 proxy。
- LeaseKeepAlive 输入读取将 `Ok(None)` 与 `Err(_)` 都作为 break；这与 Watch 保留错误的路径不同。属于当前实现差异，不能写成“全部流透传错误”的保证，是否统一需另行决定。
- 启动连接、serve、日志写入与通道失败存在 `expect`/`unwrap`。build.rs 的 proto 编译失败也会 panic；没有证据支持“所有错误均以 Result 交付”的全项目保证。
- Logger 用有界 channel、缓冲和后台 writer；日志背压可能阻塞请求路径。`Logger::flush` 等待写入和文件 flush，不是 fsync/掉电持久性保证。Watch 日志只覆盖成功响应，不能仅凭日志缺失判定 EOF/Status/取消原因；普通请求 Debug 日志可能含 key/value，不视为脱敏审计日志。

## 日志责任与待确认项

bench 的最终报告、stderr 与在线 metrics 承担不同输出角色，不能把最后一次 scrape 当运行结果。proxy 的请求日志只是协议观察工具，不是完整调用结果账本。尚无跨模块“只记一次错误”的日志责任表、全局 panic 恢复政策或 proxy 日志安全/持久性承诺；是否统一由维护者决定。仓库未做过系统性故障注入验证。

新增错误测试时读取 [测试要求](testing.md#测试要求)：必须从真实被测边界触发并检查调用方可见结果，不能只构造错误值或断言 mock 回显。

Create failure 文案已按空分支澄清；分页范围/顺序校验要求仍有实现差异。参数级边界与已知差异以 [领域 API 子集](../arch/design-docs/api-subset.md#已知差异与证据边界)为当前汇总。引用契约不表示已经证明全部实现符合它。
