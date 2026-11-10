# etcd_k8s_toolkit：智能体入口

本 Cargo 工作区包含 `etcd_proxy`（etcd gRPC 代理与请求日志）和 `etcd_k8s_bench`（Kubernetes 形状的合成压力工具）。不是 Kubernetes 控制面，也不是容量换算模型。

## 所有任务适用

- 先确定修改的 crate；共享 RPC 输入为 `proto/`，生成代码来自各 crate 的 `build.rs`，不手改生成输出。
- 保留用户已有改动与历史证据。真实 benchmark 会写后端，只能使用显式指定的专用、可丢弃空实例；不清库、不改 quota。
- 区分明确规范、现有实践、待确认和冲突；实现样本不自动成为强制政策，历史验证不代表当前验证。
- 修改接口、依赖、错误语义、测试方式时，同步维护对应权威文档；移动文档时更新引用。

## 按任务读取

仅读取当前任务涉及的专题，不要求预先通读知识库。约定（可被一次提交违反的规则）在 `.agents/context/convention/`，系统事实与设计依据在 `.agents/context/arch/`。

| 读取条件 | 链接 |
| --- | --- |
| 修改 Rust 风格、并发或 ownership 用法 | [convention/code-style.md](.agents/context/convention/code-style.md) |
| 新增或命名文件、模块、类型、函数、测试 | [convention/naming.md](.agents/context/convention/naming.md) |
| 修改跨模块调用、共享协议或新增依赖 | [convention/interfaces.md](.agents/context/convention/interfaces.md) |
| 新增错误分支、重试或修改退出行为 | [convention/error-handling.md](.agents/context/convention/error-handling.md) |
| 新增、修改或审查测试 | [convention/testing.md](.agents/context/convention/testing.md) |
| 写提交信息 | [convention/git-conventions.md](.agents/context/convention/git-conventions.md) |
| 调整组件职责、数据流或系统边界 | [arch/architecture.md](.agents/context/arch/architecture.md) |
| 修改压力模型或解释当前产品目标 | [arch/design-docs/design-goals.md](.agents/context/arch/design-docs/design-goals.md) |
| 核对支持的 RPC、参数或 K8s/bench/proxy 边界 | [arch/design-docs/api-subset.md](.agents/context/arch/design-docs/api-subset.md) |
| 追溯设计批准与历史验收依据 | [arch/design-docs/index.md](.agents/context/arch/design-docs/index.md) |
| 了解已知缺口、债务与待维护者决策项 | [arch/tech-debt.md](.agents/context/arch/tech-debt.md) |

Stage 历史索引（仅本地保留，不入库）：[.agents/stages/INDEX.md](.agents/stages/INDEX.md)。用户使用入口：[中文 README](README_zh.md)、[英文 README](README.md)。

## 测试要求

测试规则唯一维护于 [测试与验收：测试要求](.agents/context/convention/testing.md#测试要求)，适用于本工作区的新增测试、测试修改和测试审查。

## 维护约定

- 每条规则一个权威位置：修改接口/协议、依赖、错误语义、测试方式或提交约定时，由变更作者同步更新对应专题文档及本表导航；不把同一规则复制到多个入口。
- 当前领域目标与参数契约维护于 [arch/design-docs/](.agents/context/arch/design-docs/)（design-goals、api-subset），默认输入维护于 synthetic JSON；原 ADR/Spec 保留批准与详细验收依据；DESIGN_zh 保留实现说明并与 design-docs 同步，不各自发明新承诺。
- 移动文档同步更新相对链接、章节锚点和本表导航。历史 ADR 不删除、不刷新日期伪装新核验；替代决定沿用既有 Stage ADR 机制。
- 新结论标记来源、状态、适用范围和实际验证方式；仓库级缺口汇总于 [tech-debt](.agents/context/arch/tech-debt.md)。
- 生成代码的修改指向 build.rs/proto，不手写派生内容；历史验证产物保留原始记录，不补造缺失证据。
- 重复整理知识库时复用这套入口和专题，只有证据/规则变化才更新；不重建平行索引、不仅为刷新日期改写文件。
