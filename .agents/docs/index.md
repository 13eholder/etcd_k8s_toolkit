# 工程知识索引

本知识库是 `etcd_k8s_toolkit` 的文档基线。只加载任务涉及的专题；已有设计、历史决策、参考分析保留原路径。初始化结构完成不表示所有工程政策或运行性质均已确认。

## 按任务导航

| 当前任务 | 读取路径与用途 |
| --- | --- |
| 修改跨模块调用、依赖或共享 RPC | [架构](ARCHITECTURE.md) → manifest / build.rs / 相关源码，定位职责与生成边界 |
| 修改 Rust 命名、资源所有权或格式 | [编程规范](CODING.md) → rustfmt / 代表性实现，区分配置要求和实践 |
| 增加错误分支、重试或修改退出 | [错误处理](ERROR_HANDLING.md) → client / scheduler / watch，再读 [测试要求](TESTING.md#测试要求) |
| 新增、修改、审查测试或准备验证 | [测试与验收](TESTING.md) → 完整原规则、命令前置条件与状态 |
| 核对 API 支持到具体参数 | [领域 API 子集](../domain/api-subset.md) → K8s 需求、bench 活跃请求、proxy 转发与字段边界 |
| 改压力模型、报告或产品目标 | [领域设计目标](../domain/design-goals.md) → 当前目标与验收要求；批准来源见 [设计索引](design-docs/index.md) |

已有索引：[Stage 索引](../stages/INDEX.md)。用户使用入口：[中文 README](../../README_zh.md)、[英文 README](../../README.md)。

对抗审查修复的 reviewer 批准、当前进度与实际验证见 [修复计划](../../tasks/plan.md)；该记录不改写本索引下方保留的初始化历史验证。

## 七类覆盖表

“已确立规范”须有明确规则或已接受决定；“现有实践”仅描述配置/源码样本；“待确认”列出需要补充的证据；“存在冲突”保留不同来源及范围，不自行改变承诺。

| 主题 | 文档位置 | 依据 | 状态与边界 |
| --- | --- | --- | --- |
| 设计文档 | [领域设计目标](../domain/design-goals.md)、[领域 API 子集](../domain/api-subset.md)、[设计索引](design-docs/index.md) | DESIGN、Stage 006 Spec/ADR、Stage 索引、README、版本化 ref | **已确立规范**：当前 synthetic 目标、生命周期与结果边界；domain 集中当前解释，原 ADR 保留批准依据；历史差异见下节 |
| 编程规范 | [CODING](CODING.md) | Cargo.toml、rustfmt.toml、build.rs、client/scheduler/proxy | **已确立规范**：显式配置与 bench 设计约束；**现有实践**：并发/错误模式；**待确认**：unsafe、锁顺序、通用 panic 政策 |
| 架构设计 | [ARCHITECTURE](ARCHITECTURE.md) | DESIGN 模块表、main/cli/scheduler、proxy service | **已确立规范**：bench 模块职责；**现有实践**：proxy 转发与日志；不把示意图视为稳定 API |
| 模块依赖 | [ARCHITECTURE](ARCHITECTURE.md#声明依赖与生成边界) | workspace / crate manifests、Cargo.lock、pb/mod.rs | **现有实践**：两个 binary 及共享 proto；**待确认**：允许/禁止的依赖边界与 API 稳定政策 |
| 错误处理 | [ERROR_HANDLING](ERROR_HANDLING.md) | DESIGN、访问模式、ClientError/retry_decision、proxy streams/logger | **已确立规范**：bench 未知结果与退出语义；**现有实践**：proxy 转发/panic；**存在冲突**：不同流的错误交付不一致，不能推广统一保证 |
| 测试原则 | [TESTING](TESTING.md#测试要求) | 原根 AGENTS 原文、inline Rust 测试、Python verify、Stage VERIFY | **已确立规范**：原规则完整迁移；**现有实践**：测试组织及历史命令；本次未运行代码测试 |
| 命名规范 | [CODING](CODING.md)、[测试命名](TESTING.md#测试命名) | 原 AGENTS 明文命名规则、跨模块 Rust 样本 | **已确立规范**：测试命名；**现有实践**：文件/模块/类型/函数；**待确认**：一般变量、缩写和公共 API 名称政策 |

## 缺口与冲突

1. **工程政策缺口**：未发现通用贡献指南、统一工具链 pin、clippy 配置、CI 或专门文档检查入口；需要维护者提供适用规范/CI 位置，不能把 Stage 历史 formatter 版本当永久 pin。
2. **并发与安全边界**：未找到全局锁顺序、unsafe/FFI 审批、panic 恢复政策。源码样本不覆盖传递依赖或生成产物；需明确设计/维护者确认。
3. **proxy 实现差异**：Watch 保留 Status，LeaseKeepAlive 输入错误与 EOF 都停止流；spawn 句柄未集中 join，日志 flush 不承诺 fsync。需另行决定是否统一错误/回收契约，初始化不修改行为。
4. **依赖与 ownership**：清单不证明依赖许可；未发现专题 owner 清单。历史 ADR 的决策者不自动成为当前维护 owner。各专题 owner 均待确认。
5. **历史状态差异**：ADR-003 的“已接受，待实施”与 Stage 006 的 done 属于不同记录时点；ADR-002 候选参数由 ADR-003 接续，不重写历史。旧 Contract 引用的智能体分工不能作为现行全局政策。
6. **证据可用性**：部分历史原始实验产物按 `.gitignore` 仅本地保留；历史通过数与研究资料不是当前运行证明。外部官方链接本次未重新联网核验。
7. **Create 文案已澄清**：原访问模式把 Create failure 写为点 Range，已依据 `build_create_txn` 和 Stage 004 模板修正为空；Update/Delete/journal 才带失败 Range。参数级结论维护于 [领域 API 子集](../domain/api-subset.md)，未修改实现或历史 ADR。
8. **分页校验证据不足**：访问模式“读取”要求范围/顺序不合法作为协议错误；[scheduler.rs](../../etcd_k8s_bench/src/scheduler.rs) `RangePager::observe_response`（639–705 行）检查 snapshot revision 和 `more=true` 空页，但未见范围/顺序校验。不能把完整要求写成已验证能力；需要补充调用链证据或单独修复。
9. **ready 字段解释待确认**：[measurement.rs](../../etcd_k8s_bench/src/measurement.rs) `watch_disconnect` 仅在 Preparation 清零 `storage.ready`；准备门禁要求当前 ready，不能由此推断最终报告 ready 字段始终代表当前连接数。需明确报告字段口径，不据此否认已存在的准备门禁。
10. **历史引用缺口**：早期 Stage 文档曾引用旧 `.agents/docs/plan.md`、`task.md`、`archive/` 等路径，当前知识库不重建这些旧布局。历史需求从 Stage 索引和现存 Stage 文件追踪；未逐一修复归档中的旧链接，本次只核验新增/修改文档的链接。


## 维护约定

- 修改接口/协议、依赖、错误语义、测试方式时，由该变更作者同步更新对应权威来源及必要导航；不要复制同一规则到多个入口。
- 测试政策唯一维护于 TESTING 的“测试要求”；默认输入维护于 synthetic JSON。当前领域目标及参数支持范围集中于 `.agents/domain/` 两份文档，原 ADR/Spec 保留批准与详细验收依据；DESIGN/访问模式保留实现与使用说明，并与 domain 同步，不各自发明新承诺。
- 移动文档同步更新相对链接、章节锚点和旧入口。历史 ADR 不删除、不刷新日期伪装新核验；替代决定沿用既有 Stage ADR 机制，不由本次初始化新设审批流程。
- 新结论标记来源、状态、适用范围和实际验证方式；缺口在本索引汇总。日期仅表示检查时间，不表示事实成立。
- 生成代码的修改指向 build.rs/proto，不手写派生内容；历史验证产物保留生成命令及原始记录，不补造缺失证据。
- 再次初始化复用这套入口和专题，只有证据/规则发生变化才更新；不重复迁移政策、不增加平行索引、不仅为刷新日期改写文件。

## 本次核验与任务走查

本次范围为文档基线，不改变源码、测试、运行协议或既有 Stage。代码/配置结论来自阅读与定向检索，不声称完整代码审计。构建、测试、formatter 与真实后端实验均未运行，命令来源、工作目录及前置条件见 [测试指南](TESTING.md#命令与验证状态)。

文档检查采用临时只读脚本：检查本次 Markdown 的本地文件链接与锚点、七类表项和原测试政策逐字保留；检查 `.agents/docs` 不再被通用 `docs/` 忽略规则遮蔽。未建立永久脚本或 CI。

两个任务的导航走查：

1. **修改共享 RPC 或模块间调用**：根 AGENTS → ARCHITECTURE → “声明依赖与生成边界” → 两个 build.rs、proto/rpc.proto、crate manifests。能定位两个二进制共享协议但无 crate 互依赖；依赖许可仍为明确缺口。无需阅读错误/测试细则来完成边界定位。
2. **增加可变 RPC 超时分支及回归测试**：根 AGENTS → ERROR_HANDLING → ClientError/retry_decision、scheduler 与访问模式 → TESTING 的测试边界、确定性及命名 → 命令表。能定位“未知结果不可盲重放”和“真实被测边界验证”要求；无需通读历史 Stage 或格式规范。

原 AGENTS 测试政策迁移保持全文与适用范围，根保留旧“测试要求”锚点及到唯一维护位置的链接；历史审查中的根文件引用仍能到达规则，历史行号不改写。已有 `.omp/plugins` 改动不在本次修改范围。

执行结果：7 份新增/修改 Markdown 的 112 个本地链接及锚点检查通过；原测试政策逐字比较通过；上述两条导航路径检查通过。`git check-ignore` 对知识库文件无匹配（退出 1），确认新增例外有效。入口仅一处索引链接；只读重复检查前后文件哈希不变。未声称重新执行整个初始化流程或修复所有历史链接。
