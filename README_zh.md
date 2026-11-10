# etcd_k8s_toolkit

本仓库提供两个用于评估 etcd 兼容存储后端的工具：

- `etcd_k8s_bench`：Kubernetes 形状的 etcd API 压测器。按给定参数施压：先准备固定存量，再在时间窗口内按目标逻辑速率投放，输出可对账的 JSON 报告。
- `etcd_proxy`：带缓冲 JSONL 请求日志的 etcd gRPC 代理，用于观测客户端与后端之间的实际请求。

压测工具只模拟负载存储特征，用于粗略比较存储后端对于Kubernetes负载的承载能力。如果需要测量具体的Kubernetes集群承载容量，请运行正式的Kubernetes集群进行测试。


## 仓库结构

| 路径                         | 内容                                                                                   |
| ---------------------------- | -------------------------------------------------------------------------------------- |
| `etcd_proxy/`                | etcd gRPC 代理：KV/Watch/Lease/Maintenance 转发与 JSONL 请求日志                       |
| `etcd_k8s_bench/`            | 压测器：config、plan、scheduler、client、workload、watch、measurement、report、metrics |
| `etcd_k8s_bench/workloads/`  | 内嵌默认负载 `synthetic-k8s.json`                                                      |
| `etcd_k8s_bench/monitoring/` | 本机 Prometheus + Grafana 看板指南                                                     |
| `proto/`                     | 共享 etcd RPC schema，由两个 crate 编译——改 proto，不改生成代码                        |
| `docs/`                      | 用户文档：配置参考、负载模型、结果解读                                                 |

## 依赖与编译

- 支持 edition 2024 的 Rust 工具链（rustup 安装）。
- `protoc` 在 PATH 中：两个 crate 的 `build.rs` 均需编译 `proto/rpc.proto`。

```sh
cargo build --release --workspace
```

## 快速运行

### etcd_proxy

```sh
cargo run --release -p etcd_proxy -- \
  --etcd-endpoint 'http://[::1]:2379' \
  --listen-addr '[::1]:50051' \
  --max-message-size 67108864 \
  --log-file etcd_proxy.jsonl
```

proxy 将已注册的 KV/Watch/Lease/Maintenance 方法转发至单一上游 endpoint 并记录日志。完整参数见 `cargo run -p etcd_proxy -- --help`；转发面与已知限制见 [API 子集](.agents/context/arch/design-docs/api-subset.md#proxy-转发参数面)。

### etcd_k8s_bench：先 plan

`plan` 不连接后端，仅输出当前配置产生的负载计划，用于开跑前确认负载形状：

```sh
./target/release/etcd_k8s_bench plan
./target/release/etcd_k8s_bench run --help
```

### etcd_k8s_bench：run

**压测会写入数据**，必须使用专用、可丢弃的 etcd 实例。preflight 要求后端为空，发现已有 key 即拒绝运行；工具不清库、不修改 quota。每次运行使用新的空实例。

```sh
./target/release/etcd_k8s_bench run \
  --endpoint http://127.0.0.1:12379 \
  --metrics-listen 127.0.0.1:19898
```

省略 `--config` 时使用内嵌默认负载：600s 窗口、10,000 逻辑更新/s。

## 核心参数与默认值

配置为一份完整 JSON，`plan` 与 `run` 共用；常用标量（如 `--update-rps`、`--duration`）可用命令行覆盖，覆盖先于校验生效。`--config` 为整体替换而非局部合并；未知字段拒绝。

默认负载（[synthetic-k8s.json](etcd_k8s_bench/workloads/synthetic-k8s.json)）：

| 默认项                                             | 数值                               |
| -------------------------------------------------- | ---------------------------------- |
| Node / Pod / namespace                             | 2,000 / 40,000 / 100               |
| DaemonSet / Deployment / ReplicaSet                | 10 / 200 / 200                     |
| 六类资源总 key 数                                  | 44,410                             |
| 应用逻辑更新                                       | 10,000/s × 600s，计划 6,000,000 次 |
| 主动读写比                                         | 1:20，计划主动读 300,000 次        |
| 更新权重 Pod / Deployment / ReplicaSet / DaemonSet | 95 / 2 / 2 / 1                     |
| value 大小                                         | 4,096 字节（NodeLease 512 字节）   |
| worker / 队列总容量                                | 128 / 4,096                        |
| 单次 RPC / 逻辑操作超时                            | 5s / 10s                           |

应用流量之外有四条周期流：NodeLease 每 10s（5% 确定性抖动）、Node status 每 300s、Status 每 10s、compaction 协调每 60s，不计入更新速率预算。
NodeLease 指 `/registry/leases/kube-node-lease` 下的普通 KV，不是 etcd native lease。
完整配置字段与校验规则见[配置参考](docs/配置参考.md)；各流量的请求形状见[负载模型](docs/负载模型.md)。

## 结果与可视化

`run` 输出单份 JSON 报告（九个顶层字段）；参数或配置错误不产生报告。

| 退出码 | 含义                                      |
| ------ | ----------------------------------------- |
| 0      | 实验完成；不代表压力达标或 Watch 观测完整 |
| 1      | 执行、准备或生命周期失败                  |
| 2      | 参数或配置错误                            |
| 130    | 中断                                      |

解读要点：

1. 逻辑速率与物理请求数不同：一次逻辑更新可能产生多个 Txn attempt，一次列表可能包含多个 Range page。planned、offered、missed_schedule、dropped_full、accepted、completed、canceled 需分别核对，不互相换算。
2. 计数之间是守恒关系而非相等关系：timeout 与 result_unknown 为可重叠的 failed 子集，不能相加。完整对账公式与常见误读见[结果解读](docs/结果解读.md)。
3. 在线指标不提供 p99，精确分位数以最终 JSON 为准；HDR 与在线指标的口径差异见[结果解读](docs/结果解读.md)。

本机 Prometheus + Grafana 看板的启动与清理见[监控指南](etcd_k8s_bench/monitoring/README.md)；六个面板覆盖投放、漏投/丢弃/错误、RPC 吞吐与延迟、Watch、后端空间/quota。

## 实现原理

一次 `run` 的生命周期：

1. 解析配置、生成计划：校验通过后负载在时间上确定，开跑前可知计划投放量。
2. preflight：确认后端可用且为空。
3. 准备存量：创建六类资源并执行 NodeReady，等待全部 Watch 收到 Created 且当前就绪；准备不计入测量窗口。
4. 开环投放：1ms 粒度。调度迟到跳过过期 slot、不补发；队列满显式丢弃（dropped_full），不以排队掩盖过载。
5. drain 与收尾：到点冻结窗口，已接受工作在有界时间内执行，随后单独回收 Watch，输出报告。普通请求失败、后端过载与 Watch 缺口不缩短测量窗口。

模块边界、调度实现与事件归属见[设计说明](etcd_k8s_bench/DESIGN_zh.md)。

## 文档

用户文档：

- [配置参考](docs/配置参考.md)：JSON 字段、命令行覆盖与校验规则；
- [负载模型](docs/负载模型.md)：存量、应用/周期流量与 Watch 的请求形状；
- [结果解读](docs/结果解读.md)：报告字段、计数守恒与常见误读；
- [监控指南](etcd_k8s_bench/monitoring/README.md)：本机 Prometheus + Grafana 看板。

设计与契约（维护者向）：

- [设计说明](etcd_k8s_bench/DESIGN_zh.md)：模块边界、生命周期、事件归属与守恒；
- [设计目标](.agents/context/arch/design-docs/design-goals.md)：当前目标、非目标、结果语义与验收要求；
- [API 子集](.agents/context/arch/design-docs/api-subset.md)：参数级 key、CAS、分页、Watch 语义与 proxy 转发面。

## 许可证

Apache-2.0，见 [LICENSE](LICENSE)。
