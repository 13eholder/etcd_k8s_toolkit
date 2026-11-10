# 测试与验收

## 测试要求

本节适用于本工作区的新增测试、测试修改和测试审查。测试的目的，是保护模块可观测的行为契约，而不是固定当前实现。

### 测试边界与价值

- 单元测试只测模块的可观测行为：给定输入或事件，检查输出、错误、对外协议请求、可见副作用、状态转换或生命周期结果。
- “可观测”以模块调用方或协议对端为视角，不以 Rust `pub`/private 可见性划线。纯计算模块的计算结果可以是契约；读取内部字段、断言某个私有 helper 被调用，不因此成为行为测试。
- 每个测试必须能回答：保护什么契约，什么合理的错误实现会使它失败。答不出来的测试不新增；审查时建议删除或改写。
- 不把生产算法复制到 `#[cfg(test)]` 或测试 helper 后只验证副本；测试必须经过实际生产路径。fixture可以构造输入，不能替被测模块实现其行为。
- 不测试 trivial getter/setter、原样赋值/转发、无业务意义的默认常量、仅由类型系统或第三方库保证的行为。不要为了覆盖率、测试数量或“一函数一测试”编写测试。
- 不断言私有容器布局、辅助函数拆分、内部调用顺序、无契约意义的精确计数，或源码文本。模块行为不变的重构不应迫使测试改写。
- 错误路径必须触发真实的被测边界，并验证调用方可见结果；只构造一个错误值、让 mock 返回预设值后断言同一值，不能证明错误传播正确。
- 协议字段、配置优先级、分页快照、取消语义、计量守恒等若属于明确契约，可以精确断言；不能把“断言简单”误判为“没有价值”。

### 测试组织与风格

- 一个测试聚焦一个场景及其可观测结果；同一契约的输入边界优先用表驱动，避免复制大量仅输入不同的测试。
- 使用最小但足以表达场景的 fixture。复用有实际重复的准备代码，不引入掩盖被测行为的大型测试框架或层层 helper。
- Mock/test double 只替代模块外部依赖，不替代正在验证的逻辑。不以私有方法调用次数或接线断言代替结果；只有请求次数本身是协议/重试契约时才断言次数。
- 异步与并发测试使用可控事件、屏障、通知或虚拟时间驱动，不依赖任意 sleep、碰运气的轮询或真实墙钟时长。超时仅作为防挂保护，不作为完成同步信号。
- 测试必须独立、确定性、可并行运行；不依赖测试执行顺序、共享全局 recorder/环境变量残留、固定端口或已有外部服务状态。
- 真实 CLI/RPC/后端组合行为使用适当的集成或 smoke 验证，不用内部单测冒充端到端证据。测试 double 的结果不得表述为真实后端性能。

### 测试命名

- 测试函数统一使用 `test_<场景>_<行为>` 的 snake_case 名称。场景可包含模块名和必要条件；末尾指出被验证的行为或结果。
- 名称简洁、直接、便于定位，例如 `test_client_cas_error`、`test_client_sent_error`、`test_watch_resume`、`test_queue_full_drop`。
- 不使用 Stage 编号、任务编号、日期、`regression_123` 等过程标签，不将长段需求或实现步骤拼成函数名。
- 避免 `test_works`、`test_basic`、`test_case_1` 等无法说明意图的名称；也不靠机械截短名称丢失关键场景。复杂背景写在必要的短注释或 fixture 中。
- 参数化场景使用简短、有意义的 case 标签。测试 helper 不强制使用 `test_` 前缀，避免与测试入口混淆。

### 审查与变更门禁

- 逐项审查时分别判断行为价值和命名风格；命名不合规不等于测试应删除。
- 审查结论使用“保留 / 改写 / 合并 / 删除”，并说明保护的契约、当前问题及建议动作。改写或删除前确认是否会失去独有的有效覆盖。
- 已有测试通过不等于质量合格；测试质量也不以通过数量、行覆盖率或 mock 数量衡量。
- 旧目标被新设计替代时迁移对应行为测试，不继续冻结旧实现；仍有效的协议、安全和生命周期契约必须保留。
- 此规则不是自动删除现有测试的授权。仅请求审查时先交付完整证据与逐项结论，未经实施请求不批量删改测试。

## 规则来源与适用范围

以上“测试要求”及四个子节是本工作区测试规则的唯一权威维护位置，适用于本工作区的新增测试、测试修改和测试审查。

## 现有测试组织

**现有实践**：bench 的 Rust 行为测试位于 `etcd_k8s_bench/tests/unit/*.rs`，由对应 `src` 模块通过 `#[path]` 引入；proxy 仍有内联测试。bench 的 [client 测试](../../../etcd_k8s_bench/tests/unit/client.rs)、[watch 测试](../../../etcd_k8s_bench/tests/unit/watch.rs)、[scheduler 测试](../../../etcd_k8s_bench/tests/unit/scheduler.rs)使用协议 fixture 验证客户端与生命周期。[Python 测试](../../../etcd_k8s_bench/verify/test_verify.py)覆盖离线报告检查及采集器真实进程/信号行为。fixture 不证明真实 etcd 性能。

真实实验的前置条件和解释口径以 [使用说明](../../../README_zh.md)、[设计说明](../../../etcd_k8s_bench/DESIGN_zh.md)为准；历史运行记录见 [Stage 006 VERIFY](../../stages/006-rule-driven-timed-pressure/VERIFY.md)。历史通过数不代表当前运行结果。

## 常用命令

以下命令工作目录均为仓库根目录。

| 命令 | 前置条件与来源 | 用途 |
| --- | --- | --- |
| `cargo build --release --workspace` | Rust 2024 工具链、Cargo 依赖可用、`protoc` 可用；[README](../../../README_zh.md)、两个 crate 的 `build.rs` | 构建两个二进制 |
| `cargo check --workspace` | 同上；Stage 006 VERIFY | 工作区编译检查 |
| `cargo test --workspace` | 同上；Stage 006 VERIFY；dev-dependency 启用 Tokio `test-util` | Rust 行为测试，不代替真实后端验证 |
| `cargo +nightly-2026-04-11 fmt --all -- --check` | 对应 rustup 工具链及 rustfmt；[格式配置](../../../rustfmt.toml) | 格式检查 |
| `python3 -m unittest discover -s etcd_k8s_bench/verify -p test_verify.py -v` | Python 3.10+、标准库；Stage 006 VERIFY、Python 测试入口 | checker 与 runner 测试 |
| `./target/release/etcd_k8s_bench plan --config etcd_k8s_bench/verify/synthetic-small.json` | 已构建 release binary；README | 无后端计划 smoke |

真实 `run`、`run_synthetic.py` 与 `check_report.py` 完整参数沿用 README，不在这里复制。必须显式指定专用可丢弃空后端、已有 etcdctl、全新输出目录；checker 传实际子进程退出码。工具不管理后端、不清库、不调整 quota。报告一致不等于实验成功。

executor 采样 Tokio 单调时钟，纯 CAS 状态机只接收同域转换的时间值；网络 I/O 与 paused-clock 的自动推进不能混作完成同步。历史执行与验收记录（各 Stage VERIFY 与修复计划）仅本地保留，历史通过数不代表当前运行结果。

## 文档验证

仓库无专用文档检查命令或 CI。文档变更必须保证本地文件链接与章节锚点有效、规则不出现第二个维护位置；可用一次性只读脚本核查，不引入永久脚手架。已知缺口见 [技术债务](../arch/tech-debt.md)。
