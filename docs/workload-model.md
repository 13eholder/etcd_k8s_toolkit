# Workload Model

The load applied by `etcd_k8s_bench` has four parts: a fixed keyspace, application traffic, periodic streams, and Watch subscriptions. This page describes the request shapes each part actually sends to etcd. Parameter-level protocol detail: [API subset](../.agents/context/arch/design-docs/api-subset.md) (中文); goal semantics: [design goals](../.agents/context/arch/design-docs/design-goals.md) (中文).

## Fixed keyspace

| Resource | Default count | Key domain | Traffic it participates in |
| --- | ---: | --- | --- |
| Pod | 40,000 | `/registry/pods/<namespace>/<name>` | application updates, active reads, Watch |
| Node | 2,000 | `/registry/minions/<name>` | NodeReady, periodic status, active reads, Watch |
| NodeLease | 2,000 | `/registry/leases/kube-node-lease/<node>` | periodic heartbeat, Watch |
| Deployment | 200 | `/registry/deployments/<namespace>/<name>` | application updates, active reads, Watch |
| ReplicaSet | 200 | `/registry/replicasets/<namespace>/<name>` | application updates, active reads, Watch |
| DaemonSet | 10 | `/registry/daemonsets/<namespace>/<name>` | application updates, active reads, Watch |

44,410 resource keys by default. Namespaces only distribute keys; no Namespace objects are written. DaemonSets consume `node_count × daemonset_count` Pods; remaining Pods are grouped by `pods_per_deployment`, with equal Deployment and ReplicaSet counts. These relations only derive cardinalities; no controller logic is executed.

Values are synthetic bytes of the configured size (4,096 by default, 512 for NodeLease), not Kubernetes objects acceptable to an apiserver. `compact_rev_key` is a standalone coordination journal for compaction, not a seventh resource class.

## Application traffic

### Updates: single-key CAS

Each logical update is one `KV/Txn`: compare asserts `MOD(key)` equals the held mod_revision, the success branch is a single Put, the failure branch is a single point Range. After a conflict, subsequent attempts are rebuilt from the returned current KV/revision within the same logical operation; Create conflicts are not retried automatically. Put `lease` is always 0, including NodeLease.

Default weights: Pod / Deployment / ReplicaSet / DaemonSet = 95 / 2 / 2 / 1. Node and NodeLease do not participate in update weights.

### Active reads

- Point read: a linearizable Range (`serializable=false`) on a complete object key, covering Pod / Node / Deployment / ReplicaSet / DaemonSet.
- Bounded list: rotates over each resource's actually occupied namespace/root prefix, with logical target `list_limit` (default 100); the first page's physical `limit` is `min(500, target)`, later pages use `key` = last key + NUL with unchanged `range_end`, pinned to the first page's snapshot revision.

Default read/write ratio 1:20; read weights Pod / Node / Deployment / ReplicaSet / DaemonSet = 90 / 5 / 2 / 2 / 1; point / list = 90 / 10.

## Periodic streams

| Stream | Default period | Request |
| --- | --- | --- |
| NodeLease heartbeat | 10s, deterministic 5% jitter | CAS update of an ordinary KV, `Put.lease=0`; not an etcd native lease |
| Node status | 300s | CAS update of the Node key |
| Status | 10s | empty Maintenance Status request; reads the header revision |
| compaction coordination | 60s | journal Txn first (`VERSION(compact_rev_key)` CAS); a Compact (logical, `physical=false`) is emitted only with a valid target. The first round may only create the journal; on CAS conflict the other coordinator's state is accepted without preemption retry in that tick |

Periodic streams are independent of the application update-rate budget.

## Watch

Each of the six resource roots gets `subscriptions_per_root` (default 1) subscriptions over the root prefix half-open range, with `start_revision=0`, `prev_kv=true`, `progress_notify=true`, and `hasleader=true` in the gRPC metadata. A subscription counts as ready only after a valid Created; after a disconnect it resumes from the last delivered revision + 1 (or from the saved Created header revision if nothing was delivered yet). No relist is performed, and reconnecting does not erase an existing coverage gap.

## Not stressed, and known limits

- native TTL / LeaseGrant: not stressed; NodeLease is an ordinary KV.
- Active deletion: the ConditionalDelete request builder is retained but not scheduled.
- Transaction shape is fixed at one compare, one success, zero or one failure; no nested Txns and no multi-key multi-op transactions.
- Range response validation is not exhaustive: the current checks cover snapshot revision and `more=true` empty pages; full key-range, ordering, and cross-page progression checks are not implemented — this is not a complete detector of a misbehaving backend.
- Watch events are not decoded by type or consumed with selector/fragment semantics; no watch cache, reflector, or controller causality is simulated.

## Distinct units of measure

Logical update, CAS attempt, physical Txn, logical list, Range page, and Watch event are different units: one logical update may produce multiple Txn attempts, and one logical list may produce multiple Range pages. Rates and counts in the report must be checked per unit; see [result interpretation](result-interpretation.md).
