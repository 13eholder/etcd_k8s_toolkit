# 代码风格与资源管理

## 已确立依据

- [Cargo 工作区配置](../../../Cargo.toml)声明 edition 2024、resolver 3，两个 crate 继承版本与 edition；不能据此推出未声明的最低 rustc 版本或组织级依赖许可。
- [rustfmt.toml](../../../rustfmt.toml)是格式配置维护位置：4 空格缩进、模块粒度 imports、StdExternalCrate 分组、注释格式化等。部分配置需要 nightly；格式检查命令见 [测试指南](testing.md#常用命令)。没有另立格式规则。
- 协议生成与依赖边界唯一维护于 [接口与依赖边界](interfaces.md)，本页不重复。

## 现有实践及适用范围

- bench 的计划共享和自有异步任务回收是明确设计要求，来源 [设计说明](../../../etcd_k8s_bench/DESIGN_zh.md)“模块边界”“生命周期”。共享计划避免 worker 深拷贝，不代表所有数据都必须 `Arc` 化。
- Tokio/channel/Arc 的具体用法以各模块为范围；修改并发路径要追踪谁持有 sender、谁终止流、谁 join 任务，不能仅依赖 drop 推断所有后台工作已结束。对应实现入口见 [架构](../arch/architecture.md)。

