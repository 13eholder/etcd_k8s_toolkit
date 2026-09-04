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

The benchmark aligns its etcd wire behavior with Kubernetes v1.35.5:

- linearizable point and prefix `Range`, including snapshot pagination;
- ModRevision CAS `Txn` for create, update, and point delete;
- resource-root `Watch` with `prev_kv`, `progress_notify`, and require-leader metadata;
- Event/masterlease `LeaseGrant` followed by `Txn Put.lease`;
- logical `Compact` coordinated through `compact_rev_key`;
- `Maintenance/Status`.

It is not an API server, controller simulator, or event-by-event trace replayer. A profile defines exact topology, resource formulas, update amplification, and phase duration. The benchmark creates every planned object first, waits for a global creation barrier, and then performs the exact number of successful logical updates. A run fails when the update deadline expires with a deficit.

## Run the benchmark

Use a dedicated empty etcd. The benchmark performs a full-keyspace, count-only preflight and refuses to run when any key exists.

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s \
  --endpoint 'http://127.0.0.1:2379' \
  --nodes 2000 \
  --pods 40000 \
  --pod-size 4096 \
  --metrics-listen '127.0.0.1:9898'
```

`k8s` is the default and includes one Coordination NodeLease per Node. To omit NodeLease traffic explicitly:

```sh
cargo run --release -p etcd_k8s_bench -- run \
  --profile k8s-no-node-lease \
  --endpoint 'http://127.0.0.1:2379'
```

`--config <path>` loads a reviewed custom profile and is mutually exclusive with an explicit `--profile`.

`--time-scale` scales the update-phase duration and periodic intervals together. It never changes object counts or successful update targets. A smaller value applies the same exact update quota in less time. Update targets are dispatched uniformly in the first 95% of the hard phase deadline; the remaining 5% is reserved for completing already queued work, not an extension.

## Exact default shape

The default `k8s` profile declares:

- 2,000 Nodes and 40,000 total Pod keys;
- 40,000 Event keys (`basis=pods`, scale `1/1`);
- Pod update amplification 10: every Pod must be updated successfully exactly 10 times;
- one NodeLease per Node;
- one DaemonSet covering 2,000 eligible Nodes; those Pods are included in the 40,000 Pod total;
- 100 Services with 100 selected backends each;
- 100 ideal-packed EndpointSlices at 100 endpoints per slice;
- trace-derived fixed controller-object counts, update amplification, Range mix, and periodic cadence.

Kubernetes documentation defines the resource relationships, but not universal Service, Event, or controller-object ratios. Those values are explicit profile inputs. EndpointSlice ideal packing is a deterministic pressure model; real controller history can leave additional non-full slices.

## Generate a DaemonSet-update pressure profile

```sh
cargo run --release -p etcd_k8s_bench -- gen_profile \
  --nodes 10000 \
  --batch-size 500 \
  --txn-rps 2000 \
  --name rolling-update
```

The generated profile uses one Pod per Node, four successful Pod updates per key, three Event objects per Node, and two DaemonSet updates per configured wave. `--txn-rps` derives the hard update-phase duration; it is not stored as a second runtime source of truth. Existing files are never overwritten. `gen_profile` rejects `--pods` because this shape derives Pod count from Node count.

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

CAS conflicts, retries, result-unknown outcomes, and queue drops do not consume successful update quota. Keys, IDs, endpoints, and error text are not metric labels.

See [`etcd_k8s_bench/DESIGN_zh.md`](etcd_k8s_bench/DESIGN_zh.md) for the detailed design and [README_zh.md](README_zh.md) for Chinese usage.
