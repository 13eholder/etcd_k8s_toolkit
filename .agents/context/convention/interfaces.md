# 接口与依赖边界

## 声明依赖与生成边界

- [workspace manifest](../../../Cargo.toml)：Tokio 异步运行时、tonic gRPC、prost 编解码、Clap 参数、serde JSON；声明版本范围与锁定解析版本分别看 manifest 和 [Cargo.lock](../../../Cargo.lock)。
- [bench manifest](../../../etcd_k8s_bench/Cargo.toml)另有 thiserror、HDR、metrics exporter 与 Hyper HTTP 服务依赖；[proxy manifest](../../../etcd_proxy/Cargo.toml)另有 async-stream、rand。
- [bench build.rs](../../../etcd_k8s_bench/build.rs)与[proxy build.rs](../../../etcd_proxy/build.rs)均用 tonic-prost-build 编译共享的 `proto/rpc.proto`；[bench pb](../../../etcd_k8s_bench/src/pb/mod.rs)、[proxy pb](../../../etcd_proxy/src/pb/mod.rs)引入 `OUT_DIR` 产物。`proto/` 中存在额外文件不证明 server 已注册对应服务。修改协议输入而非派生文件。

## 边界状态

依赖清单只证明当前声明依赖，**不证明允许或禁止的架构依赖**。目前无 crate 间禁止访问清单、自动分层检查或公共 Rust API 稳定性政策；变更依赖边界需要维护者确认，不把 [架构](../arch/architecture.md)的数据流示意当作强制 DAG。
