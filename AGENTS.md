# etcd_k8s_toolkit：智能体入口

本 Cargo 工作区包含 `etcd_proxy`（etcd gRPC 代理与请求日志）和 `etcd_k8s_bench`（Kubernetes 形状的合成压力工具）。不是 Kubernetes 控制面，也不是容量换算模型。

## 所有任务适用

- 先确定修改的 crate；共享 RPC 输入为 `proto/`，生成代码来自各 crate 的 `build.rs`，不手改生成输出。
- 保留用户已有改动与历史证据。真实 benchmark 会写后端，只能使用显式指定的专用、可丢弃空实例；不清库、不改 quota。
- 区分明确规范、现有实践、待确认和冲突；实现样本不自动成为强制政策，历史验证不代表当前验证。
- 修改接口、依赖、错误语义或测试方式时，同步维护对应权威文档；移动文档时更新引用。详细维护约定见索引。

## 按任务读取

仅读取当前任务涉及的专题，不要求预先通读知识库。

| 读取条件 | 目标 | 用途 |
| --- | --- | --- |
| 开始不熟悉的任务、寻找依据或缺口 | [.agents/docs/index.md](.agents/docs/index.md) | 七类知识覆盖、状态与维护责任 |
| 修改模块间调用、共享协议或任务生命周期 | [架构与依赖](.agents/docs/ARCHITECTURE.md) | 职责、数据流、生成边界 |
| 修改 Rust 实现、资源所有权或命名 | [编程规范](.agents/docs/CODING.md) | 明确配置、实践与未确认政策 |
| 增加错误分支、重试或修改退出行为 | [错误处理](.agents/docs/ERROR_HANDLING.md) | typed error、未知结果、流终止边界 |
| 新增、修改或审查测试 | [测试与验收](.agents/docs/TESTING.md) | 原有完整测试规则、命名、执行前提 |
| 核对支持的 RPC、参数或 K8s/bench/proxy 边界 | [API 子集](.agents/domain/api-subset.md) | 参数级当前领域说明，区分活跃调用、保留能力与研究需求 |
| 修改压力模型或解释当前产品目标 | [设计目标](.agents/domain/design-goals.md) | 当前目标、非目标、结果语义和验收要求 |
| 追溯设计批准与历史验收依据 | [设计索引](.agents/docs/design-docs/index.md) | 原始 Spec、ADR、Stage 和研究资料 |

## 测试要求

原有测试要求完整迁至 [测试与验收：测试要求](.agents/docs/TESTING.md#测试要求)，仍适用于本工作区的新增测试、测试修改和测试审查；该专题是唯一维护位置。保留此章节供旧锚点引用。