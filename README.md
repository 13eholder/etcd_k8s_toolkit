# etcd_k8s_toolkit

Cargo workspace containing:

- `etcd_proxy`: a tonic etcd proxy with buffered JSONL request logging;
- `etcd_k8s_bench`: a Kubernetes-shaped etcd API pressure generator.

## Run etcd_proxy

```sh
cargo run --release -p etcd_proxy -- \
  --etcd-endpoint 'http://[::1]:2379' \
  --listen-addr '[::1]:50051' \
  --max-message-size 67108864 \
  --log-file etcd_proxy.jsonl
```

Use `cargo run -p etcd_proxy -- --help` for all logging and transport options.

## Synthetic benchmark

`etcd_k8s_bench` applies rule-driven, timed, open-loop pressure. It models six
resource domains and Kubernetes-shaped etcd requests, not a Kubernetes control
plane or a replay of observed production traffic. Values are synthetic bytes,
not API objects. No real rollout, watch cache, controller causality, or native
TTL lifecycle is simulated. Results are not a conversion to Kubernetes capacity.

### Build and inspect the plan

```sh
cargo build --release --workspace
./target/release/etcd_k8s_bench plan
./target/release/etcd_k8s_bench plan --config etcd_k8s_bench/verify/synthetic-small.json
./target/release/etcd_k8s_bench run --help
```

`plan` does not contact a backend. `plan` and `run` consume the same complete
JSON configuration and scalar overrides, applied before validation. The embedded
[default configuration](etcd_k8s_bench/workloads/synthetic-k8s.json) is the only
default workload. `--config` replaces that input; it is not a partial overlay.
Unknown configuration fields are rejected. Duration must be a positive integral
number of milliseconds; `--seed 0` is valid.

| Default | Value |
| --- | --- |
| Nodes / Pods / namespaces | 2,000 / 40,000 / 100 |
| DaemonSets / Deployments / ReplicaSets | 10 / 200 / 200 |
| Ordinary NodeLease keys | 2,000 |
| Total resource keys | 44,410 |
| Application logical updates | 10,000/s for 600s: 6,000,000 planned |
| Active reads | 1:20 relative to updates: 300,000 planned |
| Update weights: Pod / Deployment / ReplicaSet / DaemonSet | 95 / 2 / 2 / 1 |
| Point / bounded-list read weights; list limit | 90 / 10; 100 |
| Workers / total bounded queue capacity | 128 / 4,096 |
| RPC / logical-operation timeout | 5s / 10s |
| Preparation / drain / Watch teardown timeout | 180s / 10s / 5s |
| Report bucket / observation memory budget | 1s / 128MiB |

NodeLease renewals run every 10s with deterministic 5% jitter; Node status every
300s, Status every 10s, and compaction coordination every 60s. These periodic
streams are **additional** to application updates and active reads. NodeLease
means an ordinary KV object under `/registry/leases/kube-node-lease`, not an etcd
native lease. All resource values default to 4,096 bytes except NodeLease (512).

### Run against a dedicated empty backend

The benchmark writes data. Supply a dedicated disposable etcd endpoint; never
point these examples at a production cluster. Status and a full-keyspace
count-only preflight must succeed, and any existing key causes rejection.
Neither benchmark nor verification tooling clears the backend or changes quota.
Use a new empty instance/data directory for every run.

```sh
./target/release/etcd_k8s_bench run \
  --config etcd_k8s_bench/verify/synthetic-small.json \
  --endpoint http://127.0.0.1:12379 \
  --metrics-listen 127.0.0.1:19898
```

Preparation creates all six resource domains, applies NodeReady updates, and
waits for every Watch to be currently ready after Created. Preparation is outside
the measurement window. Application and periodic work is offered at **1ms tick
granularity**. A delayed scheduler skips expired slots: it does not catch up or
wait for a success quota. Within-tick batches are expected. Full queues drop
work explicitly; queues are bounded.

Pacing waits at high resolution on `Runtime::block_on`'s caller thread, avoiding
both Tokio's millisecond rounding and a separate timer thread's wakeup handoff.
Each sleep request is at most 1ms, followed by a cooperative yield; no busy-wait.
Actual OS/executor stalls still count as `missed_schedule`; zero loss is not a
hard-real-time guarantee. The Tokio executor defaults to 4 threads and honors
`TOKIO_WORKER_THREADS`; this is separate from the 128 logical workers. gRPC uses
adaptive HTTP/2 receive windows. RPC timeouts still use Tokio.

The measurement window is `[start, effective_cutoff)`. An event exactly at the
cutoff belongs to drain. Accepted work can execute during bounded drain; Watch
teardown follows. The final report is emitted after owned tasks are joined.
Ordinary request errors, overload, and measurement-time semantic Watch gaps do
not shorten an otherwise normal configured window.

### Interpret the result

`run` emits one JSON report with nine top-level fields: `schema_version`,
`experiment`, `resolved_config`, `plan`, `timing`, `preparation`, `measurement`,
`drain`, and `accounting`. Configuration/argument errors have no report.

| Exit | Meaning |
| --- | --- |
| 0 | Experiment completed; **not** proof of target pressure or complete Watch observation |
| 1 | Execution/setup/lifecycle failure |
| 2 | Invalid arguments or configuration |
| 130 | Interruption |

Compare planned, offered, missed-schedule, dropped-full, accepted, completed,
and canceled work separately. A logical update may cause multiple physical Txn
attempts; a list can issue multiple Range pages. **Logical update/s is not
physical Txn/s.** RPC starts and completions are classified by their own event
phase, so a single phase need not have equal starts and completions. Timeout and
result-unknown counts are overlapping subsets of failed work, not additive error
categories. Canceled RPCs do not fabricate completion or latency samples.

`dropped_full` is saturation of the target key shard, not proof that every worker
queue is full. `cutoff_queued/running` are residuals derived from the final
accepted cohort, including possible pre-cutoff cancellations; they are not live
queue snapshots. See the [accounting design](etcd_k8s_bench/DESIGN_zh.md#事件归属与守恒).

Watch observation completeness and per-root coverage are separate from workload
accounting; reconnecting does not erase an earlier gap. HDR `sample_count + overflow`
is the observed count; cancellations are censored separately and do not create
completion samples. Success and error terminals share HDR summaries, and a timeout
latency is not the backend's actual completion time. HDR rounds up to at least 1µs;
online microsecond sum/count/max truncate downward. Protobuf payload bytes are not
wire/TLS/network bytes. Online metrics and sampled CPU/RSS are supporting evidence,
not substitutes for the final report; sampled peak RSS is not an exact maximum.

### Minimal Prometheus + Grafana dashboard

The [local monitoring guide](etcd_k8s_bench/monitoring/README.md) includes native
Prometheus/Grafana startup, automatic datasource/dashboard provisioning, metric
semantics, and cleanup. Six panels cover target/offered/accepted updates, losses
and errors, RPC throughput and mean/maximum latency, Watch, and etcd size/quota.
Online metrics do not provide p99; use final JSON for exact quantiles. The exporter
stops with the benchmark; Prometheus retains scraped history until retention or
explicit storage-directory cleanup.

### Capture repeatable evidence

Python 3.10+ standard-library tools capture raw stdout/stderr, commands, binary
SHA256, environment, backend status/keyspace, metrics, and process observations.
The backend must already be running. The output directory must not exist.

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

Pass the actual child exit code to the checker, not an assumed zero. Optional
`--keyspace PATH` checks the captured final etcdctl keys-only JSON.
Checker exit 0 means supplied evidence is consistent, not that the experiment
succeeded; omitted plan/keyspace checks are reported as unchecked. The runner's
`--timeout-seconds` is an external watchdog, not measurement duration. Optional
`--interrupt-after-seconds` is measured from child startup, including preparation.
Optional `--backend-metrics-url` captures backend metrics. Collection failures and
forced termination remain distinct from benchmark results.

The runner latches SIGINT/SIGTERM and sends at most one graceful SIGINT per child,
retaining the original signal and actual child return code. A graceful benchmark
interruption remains 130. A failed plan or cancellation latched at the final gate
after output files open prevents `run`. Cancellation after launch begins is handled
as an executed child, not an atomic rollback of process creation. A skipped run
has `executed:false`, a null exit code and a reason in its invocation metadata;
it has no fabricated report, and `check.json` explicitly says not checked. Skipped
sampling/post-run collection also records its reason. The watchdog still has a
bounded kill deadline after an earlier SIGINT; tool failures return 1.

The checker verifies observable counts, type ranges and arithmetic rather than
trusting accounting flags. A final summary cannot independently prove all internal
arithmetic history. See the [repair plan and verification](tasks/plan.md).

See the [API subset](.agents/domain/api-subset.md) for keys, CAS,
pagination and Watch semantics, and [design (中文)](etcd_k8s_bench/DESIGN_zh.md)
for lifecycle and accounting invariants.
