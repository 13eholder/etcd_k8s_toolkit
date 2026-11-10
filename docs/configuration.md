# Configuration Reference

Complete configuration input and command-line overrides for `etcd_k8s_bench`. The authoritative behavioral contracts are linked at the end; this page is the field-level usage reference.

## Input and precedence

1. The base input is one complete JSON document; without `--config`, the embedded [synthetic-k8s.json](../etcd_k8s_bench/workloads/synthetic-k8s.json) is used.
2. `--config` replaces the entire input; it is not a partial merge. Unknown JSON fields are rejected.
3. Command-line scalar overrides apply last, on top of the JSON, and before validation — a bad file value can be repaired from the command line.
4. `plan` and `run` share the same effective configuration and compiled plan; `plan` never contacts a backend.

## JSON fields

### Top level

| Field | Type | Meaning |
| --- | --- | --- |
| `name` | string | Workload name; must be a valid workload name |
| `seed` | u64 | Seed for synthetic keys and periodic jitter; `0` is a valid deterministic seed |

### topology: keyspace size

| Field | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `node_count` | 2000 | >0 | Node count; also determines the NodeLease count and the Pods consumed by DaemonSets |
| `pod_count` | 40000 | >0 | Total Pod keys; must exceed `node_count × daemonset_count`, with the remainder divisible by `pods_per_deployment` |
| `namespace_count` | 100 | >0, ≤ `pod_count` | Only distributes keys; no Namespace objects are created |

Derivation: DaemonSets consume `node_count × daemonset_count` Pods; remaining Pods are grouped by `pods_per_deployment`, with equal Deployment and ReplicaSet counts (200 each by default). Total resource keys are the sum of the six resource counts: 44,410 by default.

### controllers: controller topology

| Field | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `daemonset_count` | 10 | >0 | DaemonSet count; each consumes `node_count` Pods |
| `pods_per_deployment` | 100 | >0 | Pods per Deployment (and ReplicaSet) group |

### value_size_bytes: value sizes

| Field | Default | Meaning |
| --- | --- | --- |
| `pods` / `nodes` / `deployments` / `replicasets` / `daemonsets` | 4096 | Synthetic bytes; must be >0 and not exceed `--max-message-size` |
| `node_leases` | 512 | Same constraints |

Values are opaque synthetic bytes, not protobuf messages, and not wire byte counts.

### load: application traffic

| Field | Default | Meaning |
| --- | --- | --- |
| `duration_ms` | 600000 | Measurement window length, positive integer milliseconds |
| `update_rps` | 10000 | Target application logical update rate; not a guaranteed physical Txn/s |
| `read_ratio` | 1/20 | Active reads relative to updates, rounded per-stream at rational rates; `denominator` must be >0 |
| `update_weights` | pods 95 / deployments 2 / replicasets 2 / daemonsets 1 | Update resource weights; total must be >0 |
| `read_weights` | pods 90 / nodes 5 / deployments 2 / replicasets 2 / daemonsets 1 | Active-read resource weights; total must be >0; NodeLease does not participate |
| `read_shape_weights` | point 90 / bounded_list 10 | Point-read vs bounded-list weights; total must be >0 |
| `list_limit` | 100 | Target items per logical list; one logical list may paginate into multiple physical Ranges |

### periodic: periodic streams

| Field | Default | Meaning |
| --- | --- | --- |
| `node_lease_interval_ms` | 10000 | NodeLease ordinary-KV heartbeat interval |
| `node_lease_jitter_percent` | 5 | Deterministic jitter percent, 0–99 |
| `node_status_interval_ms` | 300000 | Node status update interval |
| `status_interval_ms` | 10000 | Maintenance Status interval |
| `compact_interval_ms` | 60000 | Compaction coordination interval; one coordination does not necessarily emit a Compact RPC |

Periodic streams are independent of the `update_rps` budget.

### watch: subscriptions

| Field | Default | Meaning |
| --- | --- | --- |
| `subscriptions_per_root` | 1 | Subscriptions per resource root, >0; does not scale with Node count |
| `start_revision` | 0 | Subscription start revision; must be non-negative |
| `prev_kv` | true | Request prev_kv in events |
| `progress_notify` | true | Request server progress notifications |

### execution: execution resources and timeouts

| Field | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `workers` | 128 | >0 | Logical workers; execution capacity only, does not replace the target arrival rate |
| `queue_capacity` | 4096 | ≥ `workers` | Total queue capacity, allocated across key shards; full queues drop work explicitly |
| `request_timeout_ms` | 5000 | ≤ `operation_timeout_ms` | Per-RPC timeout |
| `operation_timeout_ms` | 10000 | — | Logical-operation timeout, counted from worker start; excludes queueing |
| `preparation_timeout_ms` | 180000 | >0 | Total preparation budget |
| `drain_timeout_ms` | 10000 | >0 | Post-cutoff drain budget |
| `watch_teardown_timeout_ms` | 5000 | >0 | Watch teardown budget |

All duration fields above are positive integer milliseconds.

### report: observation

| Field | Default | Constraint | Meaning |
| --- | --- | --- | --- |
| `bucket_ms` | 1000 | ≤ `duration_ms` | Report time-bucket width |
| `memory_limit_mib` | 128 | >0 | Observation memory budget; a bounded-observation estimate, not a guarantee the process never OOMs |

## Command-line overrides

Shared by `plan` and `run`:

| Flag | Overrides | Notes |
| --- | --- | --- |
| `--config <path>` | — | Replaces the entire JSON input |
| `--nodes` | `topology.node_count` | |
| `--pods` | `topology.pod_count` | |
| `--namespaces` | `topology.namespace_count` | |
| `--pod-size` | `value_size_bytes.pods` | Changes only Pod values; other resources unchanged |
| `--seed` | `seed` | |
| `--update-rps` | `load.update_rps` | |
| `--duration` | `load.duration_ms` | Accepts forms like `600s`, `2500ms` |
| `--workers` | `execution.workers` | |
| `--queue-capacity` | `execution.queue_capacity` | |
| `--request-timeout` | `execution.request_timeout_ms` | e.g. `5s`, `250ms` |
| `--operation-timeout` | `execution.operation_timeout_ms` | e.g. `10s`, `3m` |
| `--max-message-size` | not in JSON | gRPC send/receive message cap, default 64MiB; no value size may exceed it |

`run` only:

| Flag | Default | Notes |
| --- | --- | --- |
| `--endpoint` | `http://127.0.0.1:2379` | etcd gRPC endpoint; must point at a dedicated empty instance |
| `--metrics-listen` | `127.0.0.1:9898` | Prometheus exporter listen address |

## Validation rejects before any backend contact

Configuration never reaches the backend until validation passes. Beyond the constraints above: all three weight totals must be positive; `namespace_count ≤ pod_count`; `queue_capacity ≥ workers`; `request_timeout ≤ operation_timeout`; `bucket_ms ≤ duration_ms`; remaining Pods must be positive and divisible by `pods_per_deployment`. Argument or configuration errors exit with code 2 and produce no report.

## Authoritative sources

- Goals and default-value semantics: [design goals](../.agents/context/arch/design-docs/design-goals.md) (中文)
- Protocol parameter boundaries: [API subset](../.agents/context/arch/design-docs/api-subset.md) (中文)
