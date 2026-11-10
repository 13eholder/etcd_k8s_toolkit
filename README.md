# etcd_k8s_toolkit

This repository provides two tools for evaluating etcd-compatible storage backends:

- `etcd_k8s_bench`: a Kubernetes-shaped etcd API pressure generator. It applies pressure from given parameters: prepare a fixed keyspace, offer work at the target logical rate inside a time window, and emit an accountable JSON report.
- `etcd_proxy`: an etcd gRPC proxy with buffered JSONL request logging, for observing the actual requests between clients and the backend.

The benchmark simulates only the storage-access characteristics of the load, and is meant for roughly comparing how storage backends carry Kubernetes-shaped load. To measure the capacity of a concrete Kubernetes cluster, run a real Kubernetes cluster and test against it.

## Repository layout

| Path | Contents |
| --- | --- |
| `etcd_proxy/` | etcd gRPC proxy: KV/Watch/Lease/Maintenance forwarding plus JSONL request logging |
| `etcd_k8s_bench/` | pressure generator: config, plan, scheduler, client, workload, watch, measurement, report, metrics |
| `etcd_k8s_bench/workloads/` | embedded default workload `synthetic-k8s.json` |
| `etcd_k8s_bench/monitoring/` | local Prometheus + Grafana dashboard guide |
| `proto/` | shared etcd RPC schema, compiled by both crates — edit the proto, never the generated code |
| `docs/` | user guides: configuration reference, workload model, result interpretation |

## Prerequisites and build

- A Rust toolchain supporting edition 2024 (install via rustup).
- `protoc` on `PATH`: both crates' `build.rs` scripts compile `proto/rpc.proto`.

```sh
cargo build --release --workspace
```

## Quick start

### etcd_proxy

```sh
cargo run --release -p etcd_proxy -- \
  --etcd-endpoint 'http://[::1]:2379' \
  --listen-addr '[::1]:50051' \
  --max-message-size 67108864 \
  --log-file etcd_proxy.jsonl
```

The proxy forwards registered KV/Watch/Lease/Maintenance methods to a single upstream endpoint and logs them; it implements no storage itself. See `cargo run -p etcd_proxy -- --help` for all options; the exact forwarded surface and known limitations are in the [API subset](.agents/context/arch/design-docs/api-subset.md#proxy-转发参数面) (中文).

### etcd_k8s_bench: plan first

`plan` does not contact a backend; it prints the load plan produced by the current configuration, to confirm the load shape before running:

```sh
./target/release/etcd_k8s_bench plan
./target/release/etcd_k8s_bench run --help
```

### etcd_k8s_bench: run

**The benchmark writes data.** Use a dedicated, disposable etcd instance. Preflight requires the backend to be empty; any existing key rejects the run. The tool never clears the backend or changes quota. Use a fresh empty instance for every run.

```sh
./target/release/etcd_k8s_bench run \
  --endpoint http://127.0.0.1:12379 \
  --metrics-listen 127.0.0.1:19898
```

Without `--config`, the embedded default workload applies: a 600s window at 10,000 logical updates/s.

## Core parameters and defaults

Configuration is a single complete JSON shared by `plan` and `run`; common scalars (e.g. `--update-rps`, `--duration`) can be overridden on the command line, applied before validation. `--config` replaces the whole input rather than merging; unknown fields are rejected.

Default workload ([synthetic-k8s.json](etcd_k8s_bench/workloads/synthetic-k8s.json)):

| Default | Value |
| --- | --- |
| Nodes / Pods / namespaces | 2,000 / 40,000 / 100 |
| DaemonSets / Deployments / ReplicaSets | 10 / 200 / 200 |
| Total resource keys | 44,410 |
| Application logical updates | 10,000/s for 600s: 6,000,000 planned |
| Active read/write ratio | 1:20: 300,000 planned reads |
| Update weights Pod / Deployment / ReplicaSet / DaemonSet | 95 / 2 / 2 / 1 |
| Value size | 4,096 bytes (NodeLease 512 bytes) |
| Workers / total queue capacity | 128 / 4,096 |
| RPC / logical-operation timeout | 5s / 10s |

Beyond application traffic, four periodic streams run independently of the update-rate budget: NodeLease every 10s (deterministic 5% jitter), Node status every 300s, Status every 10s, compaction coordination every 60s. NodeLease is an ordinary KV under `/registry/leases/kube-node-lease`, not an etcd native lease. Full configuration fields and validation rules: [configuration reference](docs/configuration.md); request shapes of each stream: [workload model](docs/workload-model.md).

## Results and visualization

`run` emits one JSON report (nine top-level fields); argument or configuration errors produce no report.

| Exit | Meaning |
| --- | --- |
| 0 | Experiment completed; not proof of target pressure or complete Watch observation |
| 1 | Execution/preparation/lifecycle failure |
| 2 | Invalid arguments or configuration |
| 130 | Interrupted |

Interpretation rules:

1. Logical rates differ from physical request counts: one logical update may cause multiple Txn attempts; one logical list may span multiple Range pages. planned, offered, missed_schedule, dropped_full, accepted, completed, and canceled are checked separately, never converted into each other.
2. Counts are conserved, not equal: timeout and result_unknown are overlapping subsets of failed and must not be added together. Full accounting formulas and common misreadings: [result interpretation](docs/result-interpretation.md).
3. Online metrics provide no p99; exact quantiles come from the final JSON. HDR vs online-metric semantics: [result interpretation](docs/result-interpretation.md).

Local Prometheus + Grafana startup and cleanup: [monitoring guide](etcd_k8s_bench/monitoring/README.md); six panels cover offered updates, losses/errors, RPC throughput and latency, Watch, and backend size/quota.

## Implementation overview

The lifecycle of one `run`:

1. Parse configuration and compile the plan: after validation the load is fixed in time, and the planned totals are known before running.
2. Preflight: the backend must be reachable and empty.
3. Preparation: create the six resource domains, apply NodeReady, and wait until every Watch has observed Created and is currently ready; preparation is outside the measurement window.
4. Open-loop offering at 1ms granularity: a delayed scheduler skips expired slots without catching up; full queues drop work explicitly (dropped_full) rather than hiding overload in queues.
5. Drain and teardown: freeze the window on time, let accepted work finish within a bounded drain, tear down Watch separately, then emit the report. Ordinary request failures, backend overload, and Watch gaps do not shorten the measurement window.

Module boundaries, scheduling, and event attribution: [design document](etcd_k8s_bench/DESIGN_zh.md) (中文).

## Documentation

User guides:

- [Configuration reference](docs/configuration.md): JSON fields, CLI overrides, validation rules
- [Workload model](docs/workload-model.md): keyspace, application/periodic streams, Watch request shapes
- [Result interpretation](docs/result-interpretation.md): report fields, accounting invariants, common misreadings
- [Monitoring guide](etcd_k8s_bench/monitoring/README.md): local Prometheus + Grafana dashboard

Chinese versions of the three guides are alongside them in `docs/`（配置参考 / 负载模型 / 结果解读）.

Design and contracts (中文, maintainer-facing):

- [Design document](etcd_k8s_bench/DESIGN_zh.md): module boundaries, lifecycle, event attribution and accounting
- [Design goals](.agents/context/arch/design-docs/design-goals.md): current goals, non-goals, result semantics, acceptance criteria
- [API subset](.agents/context/arch/design-docs/api-subset.md): parameter-level keys, CAS, pagination, Watch semantics, proxy forwarding surface

## License

Apache-2.0. See [LICENSE](LICENSE).
