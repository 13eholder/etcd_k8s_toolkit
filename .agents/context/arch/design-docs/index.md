# 设计与行为契约索引

## 当前入口

当前领域知识集中于 [设计目标](design-goals.md) 和 [API 子集](api-subset.md)：前者回答现阶段做什么/不做什么，后者细化到参数及 K8s/bench/proxy 边界。本索引负责原始设计与批准来源导航，不搬移或重写旧 Spec/ADR。

注：Stage/ADR 归档位于 `.agents/stages/`，按 `.gitignore` 仅本地保留；下文指向 Stage 的链接在克隆仓库中不可达，其结论已内联本目录文档。

| 使用场景 | 权威来源 | 状态与范围 |
| --- | --- | --- |
| 核对当前目标、非目标和验收要求 | [领域设计目标](design-goals.md) | 当前目标契约；不记录演进或实现状态 |
| 核对支持的 API 与参数 | [领域 API 子集](api-subset.md) | K8s 研究需求、bench 活跃路径、proxy 转发分别列示 |
| 了解工具目标与使用安全边界 | [中文 README](../../../../README_zh.md)、[英文 README](../../../../README.md) | 当前用户说明；只使用专用可丢弃空后端 |
| 修改 bench 模块、生命周期和计量 | [DESIGN_zh](../../../../etcd_k8s_bench/DESIGN_zh.md) | 当前设计摘要；不模拟 Kubernetes 控制面或容量 |
| 修改 CAS、分页、Watch、compaction 请求 | [API 子集](api-subset.md) | 当前参数级行为契约及官方来源索引，区分协议事实与合成选择 |
| 修改输入或默认压力 | [synthetic-k8s.json](../../../../etcd_k8s_bench/workloads/synthetic-k8s.json)、[Stage 006 Spec](../../../stages/006-rule-driven-timed-pressure/spec/timed-pressure.md) | 默认值维护在 JSON；完整 schema/验收语义在规格 |
| 追踪决定及验收历史 | [Stage 总索引](../../../stages/INDEX.md)、[Stage 006](../../../stages/006-rule-driven-timed-pressure/STAGE.md)、[VERIFY](../../../stages/006-rule-driven-timed-pressure/VERIFY.md) | 历史记录；不代表当前重跑证明 |
| 核对对抗审查修复和验收 | [修复计划](../../../../tasks/plan.md)、[执行清单](../../../../tasks/todo.md) | 已完成修复记录；不代替历史 Stage 验收或真实后端性能证明 |

## 已接受决定

现有 ADR 保留在 Stage 原目录，以各文档状态和继承说明为准：

- [ADR-001：目标](../../../stages/006-rule-driven-timed-pressure/docs/adr/001-rule-driven-timed-pressure-goals.md)。
- [ADR-002：资源与候选参数](../../../stages/006-rule-driven-timed-pressure/docs/adr/002-default-resources-and-parameters.md)。
- [ADR-003：批准默认值与执行契约](../../../stages/006-rule-driven-timed-pressure/docs/adr/003-approved-defaults-and-execution-contract.md)：明确接续 ADR-002 的待确认项；不能把旧候选值当作仍未批准，也不能把设计批准当作实现证明。
- [ADR-004：报告与计量边界](../../../stages/006-rule-driven-timed-pressure/docs/adr/004-report-measurement-boundaries.md)。
- [ADR-005：Watch 生命周期与结果修正](../../../stages/006-rule-driven-timed-pressure/docs/adr/005-watch-lifecycle-and-outcome-corrections.md)。
- [ADR-006：合成验证工具](../../../stages/006-rule-driven-timed-pressure/docs/adr/006-synthetic-verification-tools.md)。

规则驱动定时压力替代历史 profile/成功配额模型；旧 Stage 001–005 留作背景，不能恢复已被当前 ADR/Spec 替代的配置及调度语义。proxy 的通用 Lease 转发不因 bench 不施加 native TTL 压力而被取消。

## 研究基线及证据限制

Kubernetes 侧结论的研究基线是 Kubernetes v1.35.5（研究标识 commit 6636cbce）与 etcd client v3.6.5 的上游分析；原始研究笔记按 `.gitignore` 仅保留在本地 `ref/`，不随仓库发布，其结论已内联到 [领域 API 子集](api-subset.md)。研究结论描述 Kubernetes 的调用需求，不是本工具已实现能力清单；引用时注明版本基线。

历史审查/Task Contract 对当时根 AGENTS 的角色分工引用仅作历史上下文；当前根规则没有这些分工条文，不能从已封版局部 Contract 重新提升为全局规则。历史记录中的旧行号不改写，现根入口保留测试规则的可达导航。

存在的状态差异：ADR-003 留有“已接受，待实施”，Stage 索引则标记 006 done；前者是决定时点的快照，后者是阶段交付状态。保留两者，不刷新历史以制造一致。

未发现独立 proxy 设计审批文档或专题 owner 清单；proxy 章节只记录源码实践。历史原始实验产物可能只在本机保留（见根 `.gitignore`），不能以链接或摘要替代原始证据。

proxy 的历史设计依据另见 [Stage 002](../../../stages/002-etcd-k8s-bench-protocol-fidelity/STAGE.md) 和其 [协议保真规格](../../../stages/002-etcd-k8s-bench-protocol-fidelity/spec/task.md)，包含 Watch 转发与观测改动；这是历史局部设计，不是独立的 proxy 全面生命周期规格。
