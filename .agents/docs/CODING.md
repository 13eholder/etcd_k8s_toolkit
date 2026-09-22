# 编程规范与命名

## 已确立依据

- [Cargo 工作区配置](../../Cargo.toml)声明 edition 2024、resolver 3，两个 crate 继承版本与 edition；不能据此推出未声明的最低 rustc 版本或组织级依赖许可。
- [rustfmt.toml](../../rustfmt.toml)是格式配置维护位置：4 空格缩进、模块粒度 imports、StdExternalCrate 分组、注释格式化等。部分配置需要 nightly；已用过的命令及未执行状态见 [测试指南](TESTING.md#命令与验证状态)。没有另立格式规则。
- 测试的行为边界、隔离性、确定性及名称以 [测试要求](TESTING.md#测试要求) 为唯一规范来源，不能用已有不合规样本覆盖它。
- 协议生成入口为 [bench build.rs](../../etcd_k8s_bench/build.rs) 和 [proxy build.rs](../../etcd_proxy/build.rs)，共享输入为 [rpc.proto](../../proto/rpc.proto) 及其导入。`pb/mod.rs` 从 `OUT_DIR` 引入派生 Rust；修改协议输入而非派生文件。

## 现有实践及适用范围

- 文件/模块和函数采用 snake_case；类型采用 CamelCase。例如 bench 的 `client.rs`、`watch.rs` 和 `scheduler.rs`，proxy 的 `proxy.rs`。这是跨模块样本，不是已存在的完整命名政策；不据此补造缩写、变量前后缀或公共 API 命名规则。
- bench 使用 `Result` 与 typed error，proxy RPC 使用 `tonic::Status`；不同边界不强行合成一种错误类型，见 [错误指南](ERROR_HANDLING.md)。
- bench 的计划共享和自有异步任务回收是明确设计要求，来源 [设计说明](../../etcd_k8s_bench/DESIGN_zh.md)“模块边界”“生命周期”。共享计划避免 worker 深拷贝，不代表所有数据都必须 `Arc` 化。
- Tokio/channel/Arc 的具体用法以各模块为范围；修改并发路径要追踪谁持有 sender、谁终止流、谁 join 任务，不能仅依赖 drop 推断所有后台工作已结束。对应实现入口见 [架构](ARCHITECTURE.md)。

## 待确认政策

未发现统一贡献指南、clippy 配置、工具链 pin 或通用命名政策。未找到项目级锁顺序、跨 await 持锁政策、unsafe/FFI 审批边界和 panic 总体政策；当前源码样本不能证明这些政策存在。需要维护者确认或提供明确规范。未声明不等于允许使用 unsafe，也不等于项目承诺完全没有 FFI；传递依赖没有做审计。

这些缺口不阻止复用已有实现，但新增约束应先写明依据和适用范围，不能以语言惯例冒充项目决策。主题 owner 待确认。
