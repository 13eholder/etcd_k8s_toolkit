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

## What etcd_k8s_bench models

The benchmark is a Kubernetes-shaped **synthetic pressure generator** for
etcd, aligned with the observable wire shapes used by Kubernetes v1.35.5:

- linearizable point and prefix `Range`, including snapshot pagination;
- ModRevision CAS `Txn` for create and update, plus the retained low-level
  point-delete transaction shape;
- resource-root `Watch` with `prev_kv`, `progress_notify`, and require-leader
  metadata;
- Event and internal masterlease native-lease attachment through
  `LeaseGrant` followed by `Txn Put.lease`;
- logical `Compact` coordinated through `compact_rev_key`;
- `Maintenance/Status`.

The profile supplies exact topology, resource formulas, synthetic value sizes,
write amplification, Range mix, Watch subscriptions, and a real-time
timeline. The benchmark creates every planned object, waits for the global
creation barrier, and then schedules the configured successful logical
updates. These are pressure targets, not a controller implementation: there is
no kube-apiserver, watch-cache, controller topology, object-causal state
machine, or event-by-event trace replay, and `rolling-update` does not perform
real rollout deletion/replacement. The resulting pressure must not be read as
a calibrated Kubernetes capacity or topology claim.

## Run the benchmark

Use a dedicated empty etcd. Before entering the scheduler, the benchmark
performs a full-keyspace, count-only preflight and refuses to run when any key
exists. Configuration, exporter, connection, initial `Status`, and preflight
errors are outside the scheduler result protocol and are reported on stderr
with exit status 1.

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s \
  --endpoint 'http://127.0.0.1:2379' \
  --nodes 2000 \
  --pods 40000 \
  --pod-size 4096 \
  --metrics-listen '127.0.0.1:9898'
```

`k8s` is the default and includes one Coordination NodeLease per Node. To
omit NodeLease traffic explicitly:

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s-no-node-lease \
  --endpoint 'http://127.0.0.1:2379'
```

`--config <path>` loads a reviewed custom profile and is mutually exclusive
with an explicit `--profile`.

The profile uses one real-time clock: timeline durations, Range RPS, periodic
intervals, and native lease TTLs are not rescaled. Change explicit profile
durations, rates, or update targets to adjust pressure. Updates are dispatched
over the first 95% of the hard **application update-phase** deadline; the
remaining 5% is only the successful-update completion margin, not a whole-run
wall-clock cap. After the timeline, accepted background work drains under its
own `operation_timeout`, then Watch teardown runs under its own
`operation_timeout`. A Watch terminal or worker error aborts active dispatch/
update supervision; a Watch terminal observed during normal post-timeline
drain is collected during teardown and still makes `achieved=false`. Queue
backpressure can delay dispatch and produce catch-up bursts, so dispatch is
not promised to be strictly uniform.

Each default profile has one Watch RPC stream/subscription for each of these
nine resource roots: `pods`, `events`, `replicasets`, `deployments`, `leases`,
`daemonsets`, `services`, `endpointslices`, and `minions`. The `masterleases`
resource is intentionally not a default Watch root. Every configured entry
creates its own stream/subscription; duplicate prefixes are not merged. These
streams reuse the benchmark's shared client transport and do not imply one new
TCP connection per entry. This is synthetic subscription-establishment and
event-fanout pressure, not a trace-derived connection count or an
apiserver/watch-cache topology. The wider subscription set is not equivalent
to the former single-Pod-Watch profile.

The defaults use `start_revision=0`, `prev_kv=true`, and
`progress_notify=true`. For this current Watch, the first Created header is
stored separately as the recovery baseline; if no event or progress has been
delivered before a disconnect, recovery starts from that Created revision.
For an explicitly configured historical `start_revision=r>0`, Created does
not advance the historical start: before any delivery, recovery starts again
at `r`. Once either Watch has delivered an event or progress response, both
forms recover from the last delivered revision plus one. The benchmark does
not send an explicit RequestProgress on open and does not enable fragments.

Kubernetes documentation defines the resource relationships, but not
universal Service, Event, or controller-object ratios. Those values are
explicit profile inputs. EndpointSlice ideal packing is a deterministic
pressure model; real controller history can leave additional non-full slices.

The benchmark makes no throughput or latency guarantee. A successful run is
an achieved accounting result for the selected synthetic profile.

## Exact default shape

The built-in `k8s` and `k8s-no-node-lease` profiles share the same application
objects, Range mix, timing, and Watch roots; only Coordination NodeLease
traffic differs:

- 2,000 Nodes and 40,000 total Pod keys;
- 40,000 Event keys (`basis=pods`, scale `1/1`);
- Pod update amplification 10: every Pod must be updated successfully exactly
  10 times;
- one DaemonSet covering 2,000 eligible Nodes; those Pods are included in the
  40,000 Pod total;
- 100 Services with 100 selected backends each;
- 100 ideal-packed EndpointSlices at 100 endpoints per slice;
- trace-derived fixed controller-object counts, update amplification, Range
  mix, and periodic cadence.

## Resource behavior and leases

The JSON `behavior` field has exactly four schedulable values:

| `behavior` | Pressure semantics |
|---|---|
| `create_only` | Create the object; no scheduled update quota. |
| `guaranteed_update` | ModRevision CAS update; the failed Txn branch returns the current KV for the next CAS attempt. |
| `event` | Create with a native etcd lease from its configured pool; if `update_amplification > 0`, scheduled updates also use native-lease storage CAS (the built-in profiles use 0); there is no Event/controller outer retry loop. |
| `master_lease` | Internal masterlease create and periodic update traffic using its native lease pool; it is auxiliary traffic, not the application target. |

The former `retry_policy` field and the `conditional_delete` configuration
value are rejected; there is no compatibility alias. The low-level
ConditionalDelete Txn builder remains for protocol coverage, but no active
resource behavior schedules it. Storage CAS retries use the failure-branch KV,
the same operation deadline, and the existing attempt budget. Node heartbeat
status and NodeLease retry limits remain explicit profile limits.

Do not conflate these lease paths:

- an API Lease is the `/registry/leases/...` resource and uses ordinary
  ModRevision CAS with `Put.lease=0`;
- a Coordination NodeLease is `/registry/leases/kube-node-lease/...`, created
  and renewed as a Kubernetes object, also without a native etcd lease;
- an internal masterlease is `/registry/masterleases/...` and is driven by
  the `master_lease` behavior;
- Event and masterlease TTLs are native etcd leases, reused or granted by the
  configured pool. Native TTL expiry is real etcd behavior, not a controller
  retry simulation.

CAS and mutable-request outcomes remain distinct: `conflict`, `not_found`,
`timeout`, and `result_unknown` are not interchangeable. A mutable
`result_unknown` may already have reached etcd and is never blindly replayed.

## Generate a DaemonSet-update pressure profile

```sh
cargo run --release -p etcd_k8s_bench -- gen_profile \
  --nodes 10000 \
  --batch-size 500 \
  --txn-rps 2000 \
  --name rolling-update
```

The generated profile uses one Pod per Node, four successful Pod updates per
key, three Event objects per Node, and two DaemonSet updates per configured
wave. `--txn-rps` derives the hard update-phase duration from that operation
budget; the written timeline remains the runtime source of truth. Existing
files are never overwritten. `gen_profile` rejects `--pods` because this
shape derives Pod count from Node count.

Watch entries are filtered from the original Watch array rather than
reconstructed: entries whose existing `ResourceSpec.key_pattern` matches a
retained resource prefix (with a path-boundary match) are kept, `minions` is
kept for Node status, and `leases` is additionally kept when NodeLease is
enabled. All retained fields and duplicate entries remain intact. An
explicit Watch for a retained `masterleases` resource may survive; the
generator never adds that root automatically. The default generated profile
therefore has `pods`, `events`, `daemonsets`, `minions`, and `leases` (five
roots); with NodeLease disabled and no application `leases` resource, it has
four. Other resource quantities, TTLs, Range proportions, and periodic
cadence are unchanged. This is a pressure-shape generator, not a real
rolling-update state machine.

Run it with:

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --config etcd_k8s_bench/workloads/rolling-update.json
```

## Metrics and result accounting

Prometheus metrics are exposed at `/metrics`. Key layers:

- `etcd_k8s_bench_resource_objects{resource,state="planned|succeeded|deficit"}`;
- `etcd_k8s_bench_resource_updates{resource,state="target|succeeded|deficit"}`;
- `etcd_k8s_bench_operations_total`: logical operation outcomes;
- `etcd_k8s_bench_requests_total`: physical RPC outcomes;
- `etcd_k8s_bench_txn_total` and `etcd_k8s_bench_cas_retries_total`;
- `etcd_k8s_bench_range_requests_total{shape,outcome}`;
- Watch, lease, heartbeat, revision, compactor, inflight, and drop metrics.

Queue waiting and RPC latency are separate measurements:
`etcd_k8s_bench_queue_wait_seconds{work_class}` measures accepted enqueue to
worker dequeue, while
`etcd_k8s_bench_request_duration_seconds{rpc}` measures the physical RPC.
Accepted work canceled before an execution result is `unfinished`; it is not
silently counted as success and does not prove that etcd did not commit.
Keys, IDs, endpoints, and error text are not metric labels.

Once the scheduler has started, it prints exactly one JSON line to stdout.
The top-level fields are `achieved`, `interrupted`, `schedule_complete`,
`scope` (the literal `"scheduler"`), `termination_error`, `targets`,
`work_items`, and `accounting_invariants`. `targets` contains
`bootstrap`, `application.creates`, `application.updates`, and
`range_requests`. `work_items` contains the fixed classes `bootstrap`,
`create`, `update`, `range`, `master_lease`, `heartbeat`, `status`, and
`compact`; each entry contains `work_class`, nullable `target`, `offered`,
`accepted`, `completed`, `succeeded`, `failed`, `result_unknown`, `dropped`,
`dropped_full`, `dropped_closed`, `unfinished`, `unissued`,
nullable `target_deficit`, and bounded `first_error`. For a finite target,
`target_deficit` is `target - succeeded`; auxiliary classes without a fixed
target report `null` for both `target` and `target_deficit`.

The report obeys these accounting equations:
`offered = accepted + dropped + unissued`;
`completed = succeeded + failed`;
`accepted = completed + unfinished`;
`dropped = dropped_full + dropped_closed`; and
`result_unknown` is a subset of `failed`. Background work-item failures are
collected while scheduling continues; they are not silently ignored. `achieved`
is true only when the schedule is complete, not interrupted, termination has
no error, all invariants hold, every finite target is met, and no work item is
failed, dropped, unfinished, or unissued. A scheduler run that does not
achieve this exits 1. Configuration/connect/Status/preflight failures occur
before this JSON protocol and also exit 1. Do not use exit 0, a final metrics
scrape, or successful physical RPCs as a substitute for this logical result.

CAS conflicts, retries, result-unknown outcomes, and queue drops do not
consume successful update quota.

See [`etcd_k8s_bench/DESIGN_zh.md`](etcd_k8s_bench/DESIGN_zh.md) for the
detailed design and [README_zh.md](README_zh.md) for Chinese usage.

