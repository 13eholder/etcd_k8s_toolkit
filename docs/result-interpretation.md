# Result Interpretation

`run` emits a single JSON report. This page explains the report structure, the accounting invariants, and common misreadings; the authoritative implementation semantics are linked at the end.

## Report structure

The report has exactly nine top-level fields:

| Field | Contents |
| --- | --- |
| `schema_version` | Report schema version |
| `experiment` | Experiment status (whether it completed, and why) |
| `resolved_config` | Effective configuration (JSON after command-line overrides) |
| `plan` | Compiled offering plan |
| `timing` | Per-phase timing: preparation, measurement, drain, Watch teardown |
| `preparation` | Preparation-phase summary |
| `measurement` | In-window counts, time buckets, and latency distributions |
| `drain` | Post-cutoff teardown counts |
| `accounting` | Conservation-check results |

Argument or configuration errors (exit code 2) produce no report. Execution failures (exit code 1) preserve partial results for completed phases where possible; phases never started remain in their contractual empty state — no observations are fabricated.

## Exit codes

| Exit | Meaning |
| --- | --- |
| 0 | Experiment completed; not proof of target pressure or complete Watch observation |
| 1 | Execution, preparation, or lifecycle failure |
| 2 | Invalid arguments or configuration; no run report |
| 130 | Interrupted; partial report preserved where available |

With no SLO thresholds configured, the tool reports facts only and does not judge whether capacity is adequate.

## Three questions, answered separately

| Question | Evidence | Do not conflate |
| --- | --- | --- |
| Did the experiment complete? | `experiment.status` and the exit code | Drops/errors do not imply execution failure |
| Was the target offered, and how much did the backend take? | planned / offered / missed_schedule / dropped_full / accepted / completed / failed / canceled, throughput and latency | Work never sent by the generator is not backend rejection |
| Were the window and observation complete? | `measurement_complete`, Watch coverage, `observation_complete` | Complete observation does not imply all requests succeeded |

## Accounting invariants

Each stream's plan is fixed for the full configured duration:

- `planned = offered + missed_schedule + not_offered_after_stop`
- `offered = accepted + explicit rejections`

The accepted cohort is conserved across in-window completion, drain completion, and final cancellation. With B/U as started before/after cutoff, Tb/Ta as succeeded+failed before/after cutoff, and Cq/Cr as the two cancellation classes:

- `accepted = Tb + Ta + Cq + Cr`
- `B + U = Tb + Ta + Cr`
- `Tb ≤ B ≤ accepted`, `U ≤ accepted − B`; do not require `Ta ≤ U` — work in flight at cutoff may finish during drain.

`cutoff_queued = accepted − B` and `cutoff_running = B − Tb` are residuals derived from the final cohort; they may include cancellations that happened before the cutoff and are not live queue snapshots at the cutoff instant.

## Common misreadings

1. **Logical rate ≠ physical request count.** One logical update may produce multiple Txn attempts; one logical list may paginate into multiple Range pages. Rates and counts are checked per unit, never converted into each other.
2. **timeout and result_unknown are overlapping subsets of failed**; adding them is not the total failure count.
3. **dropped_full means the target key shard's queue was full**, not that every worker queue was full.
4. **Exit 0 does not prove target pressure**: an overloaded backend still yields exit 0 when the experiment completes normally.
5. **Cancellations produce no completion samples**: they are listed as censored; HDR `sample_count + overflow` is the observed count, and with overflow-only data the quantiles are null while max stays exact.
6. **Two latency roundings differ**: HDR rounds up (at least 1µs), online microsecond metrics truncate down; means and quantiles cannot be converted between them. HDR mixes success and error terminals with no per-outcome distribution; a timeout sample is the latency at which the client observed termination, not proof of when the backend finished.
7. **protobuf payload bytes ≠ HTTP/2/TLS/network bytes**.
8. **Watch coverage accumulates only actually observable intervals**; reconnecting does not erase an earlier gap; currently ready differs from once ready.
9. **Online metrics are auxiliary**: Prometheus has no p99 (exact quantiles live in the final JSON), and sampled CPU/RSS peaks are not exact process maxima.

## Principles for comparing backends

When comparing two backends, report actual offered / missed_schedule / dropped_full alongside completed throughput and latency, and distinguish generator limits from backend limits. Never claim backend capacity from the configured rate, the exit code, or a single run. For tiered comparisons, keep object shapes, value sizes, and periods fixed and vary only the update rate.

## Online dashboard

The [monitoring guide](../etcd_k8s_bench/monitoring/README.md) covers local Prometheus + Grafana startup, automatic dashboard provisioning, and cleanup; six panels cover offered updates, losses/errors, RPC throughput and latency, Watch, and backend size/quota. The exporter stops with the benchmark; Prometheus retains scraped history until retention or explicit cleanup.

## Authoritative sources

- Result semantics and credibility requirements: [design goals](../.agents/context/arch/design-docs/design-goals.md) (中文)
- Event attribution, conservation, and observation boundaries: [design document](../etcd_k8s_bench/DESIGN_zh.md) (中文)
