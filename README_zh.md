# etcd_k8s_toolkit

Cargo 工作区包含：

- `etcd_proxy`：基于 tonic 的 etcd 代理，支持缓冲 JSONL 请求日志；
- `etcd_k8s_bench`：Kubernetes 形状的 etcd API 压测器。

## 启动 etcd_proxy

```sh
cargo run --release -p etcd_proxy -- \
  --etcd-endpoint 'http://[::1]:2379' \
  --listen-addr '[::1]:50051' \
  --max-message-size 67108864 \
  --log-file etcd_proxy.jsonl
```

完整日志和 transport 参数见 `cargo run -p etcd_proxy -- --help`。

## 规则驱动的定时合成压力

`etcd_k8s_bench` 使用六类资源的合成存量与 Kubernetes 形状的 etcd 请求，按真实时间开环投放。它不是生产 trace 重放、Kubernetes 控制面或容量换算模型。value 是合成字节，不是可交给 apiserver 的 API 对象；不模拟真实 rollout、watch-cache、控制器因果关系或 native TTL 持续压力。

### 构建与查看计划

```sh
cargo build --release --workspace
./target/release/etcd_k8s_bench plan
./target/release/etcd_k8s_bench plan --config etcd_k8s_bench/verify/synthetic-small.json
./target/release/etcd_k8s_bench run --help
```

`plan` 不连接后端。`plan` 与 `run` 使用同一份完整 JSON，标量 CLI 覆盖先于验证。唯一默认输入为内嵌的 [synthetic-k8s.json](etcd_k8s_bench/workloads/synthetic-k8s.json)。`--config` 替换完整输入，不是局部合并；未知字段拒绝。时长须为正整数毫秒；`--seed 0` 合法。

| 默认项 | 数值 |
| --- | --- |
| Node / Pod / namespace | 2,000 / 40,000 / 100 |
| DaemonSet / Deployment / ReplicaSet | 10 / 200 / 200 |
| 普通 NodeLease key | 2,000 |
| 六类资源总 key 数 | 44,410 |
| 应用逻辑更新 | 10,000/s × 600s，计划 6,000,000 次 |
| 主动读写比 | 1:20，计划主动读 300,000 次 |
| Pod / Deployment / ReplicaSet / DaemonSet 更新权重 | 95 / 2 / 2 / 1 |
| 点读 / 有界列表权重；列表 limit | 90 / 10；100 |
| worker / 总有界队列容量 | 128 / 4,096 |
| 单 RPC / 逻辑操作 timeout | 5s / 10s |
| 准备 / drain / Watch teardown timeout | 180s / 10s / 5s |
| 报告桶 / 观测内存预算 | 1s / 128MiB |

NodeLease 每 10s 更新，带确定性 5% jitter；Node status 每 300s，Status 每 10s，compaction 协调每 60s。这四类周期流独立于应用更新和主动读。NodeLease 是 `/registry/leases/kube-node-lease` 下普通 KV，不是 etcd native lease。除 NodeLease 默认 512 字节外，其他资源 value 默认 4,096 字节。

### 在专用空后端运行

工具会写数据。必须显式提供专用、可丢弃的 etcd endpoint，不要使用生产集群。Status 和全 keyspace count-only preflight 必须成功，任意已有 key 均导致拒绝。benchmark 和验证工具均不清库、不调整 quota；每次运行使用新空实例及数据目录。

```sh
./target/release/etcd_k8s_bench run \
  --config etcd_k8s_bench/verify/synthetic-small.json \
  --endpoint http://127.0.0.1:12379 \
  --metrics-listen 127.0.0.1:19898
```

准备阶段建立六类资源、执行 NodeReady，并等待全部 Watch 收到 Created 且当前仍 ready。准备不计入测量窗口。投放粒度为 **1ms**；调度迟到会跳过过期 slots，不补发追赶、不等待成功配额。单 tick 内可以批量投放。队列满明确丢弃，不扩大队列掩盖过载。

投放直接在 `Runtime::block_on` 的调用线程上高分辨率等待，避免 Tokio 毫秒取整和额外定时线程的跨线程唤醒。单次 sleep 请求不超过 1ms，随后协作式 yield；不忙等。共享主机上的 OS/执行器迟到仍如实计入 `missed_schedule`；不承诺硬实时零漏投。默认 Tokio 执行器为 4 个线程，仍可用 `TOKIO_WORKER_THREADS` 覆盖；它与配置中的 128 个逻辑 worker 不同。gRPC 使用自适应 HTTP/2 接收窗口；RPC timeout 等继续使用 Tokio。

测量窗口为 `[start, effective_cutoff)`，正好截止的事件属于 drain。已接受任务可以在有界 drain 中执行，随后单独回收 Watch；所有自有任务 join 后输出最终报告。普通请求失败、过载和测量期间 semantic Watch gap 不提前结束正常配置窗口。

### 正确解读结果

`run` 输出单份 JSON，恰有九个顶层字段：`schema_version`、`experiment`、`resolved_config`、`plan`、`timing`、`preparation`、`measurement`、`drain`、`accounting`。参数或配置错误不输出报告。

| 退出码 | 意义 |
| --- | --- |
| 0 | 实验完成；不表示压力达标或 Watch 观测完整 |
| 1 | 执行、setup 或生命周期失败 |
| 2 | 参数或配置错误 |
| 130 | 中断 |

分别核对 planned、offered、missed_schedule、dropped_full、accepted、completed 和 canceled。一次逻辑更新可能产生多个 Txn attempt，一次列表可能包含多个 Range page，**逻辑 update/s 不等于物理 Txn/s**。RPC start 和 completion 按实际事件阶段计数，单阶段不要求相等。timeout 与 result_unknown 可以重叠，均为 failed 的子集，不能相加当总失败。取消不能伪造 RPC 完成或延迟样本。

Watch coverage、observation_complete 与工作量守恒分开判断，重连不抹掉历史缺口。HDR 的元数据、样本、overflow 和 censored 必须一起解读。protobuf payload 字节不等于 wire/TLS/网络字节。在线 Prometheus 和低频 CPU/RSS 是辅助观测，不能替代最终 per-run 报告；建议约 1s 抓取一次，高压不启用全量请求日志。采样 RSS 峰值不是精确进程最大值。

### 最小 Prometheus + Grafana 看板

[运行指南与配置](etcd_k8s_bench/monitoring/README.md)提供本机原生 Prometheus 和 Grafana 的启动、看板自动加载、指标口径及清理命令。六个面板覆盖更新目标/实际投放、漏投/丢弃/错误、RPC 吞吐与平均/最大延迟、Watch、etcd 空间/quota。在线指标不提供 p99；准确分位数读取最终 JSON。bench 退出后 exporter 关闭，历史抓取由 Prometheus 保留，直到 retention 或显式清理存储目录。

### 采集可重复证据

Python 3.10+ 标准库工具保存原始 stdout/stderr、命令、二进制 SHA256、机器信息、后端 status/keyspace、metrics 与进程采样。后端由操作者预先启动，输出目录必须不存在。

```sh
python3 etcd_k8s_bench/verify/run_synthetic.py \
  --binary ./target/release/etcd_k8s_bench \
  --etcdctl /usr/local/bin/etcdctl \
  --endpoint http://127.0.0.1:12379 \
  --config etcd_k8s_bench/verify/synthetic-small.json \
  --metrics-listen 127.0.0.1:19898 \
  --output-dir /tmp/synthetic-evidence-new \
  --timeout-seconds 240

python3 etcd_k8s_bench/verify/check_report.py \
  --report /tmp/synthetic-evidence-new/run.stdout.json \
  --exit-code 0 \
  --plan /tmp/synthetic-evidence-new/plan.stdout.json
```

必须传真实子进程退出码，不能假定为 0。可选 `--keyspace PATH` 检查采集的最终 etcdctl keys-only JSON。checker 返回 0 仅表示所提供证据一致，不代表实验成功；未提供 plan/keyspace 时明确列为未检查。runner 的 `--timeout-seconds` 是外部 watchdog，不是测量时长；`--interrupt-after-seconds` 从子进程启动计时，包含准备阶段。可选 `--backend-metrics-url` 采集后端指标。采集错误、watchdog 和强杀不能伪装正常完成。

协议详见 [访问模式](etcd_k8s_bench/docs/access-patterns.md)，实现边界见 [设计说明](etcd_k8s_bench/DESIGN_zh.md)。
