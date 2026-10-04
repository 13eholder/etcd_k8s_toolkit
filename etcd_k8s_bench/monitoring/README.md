# Local Prometheus + Grafana

A minimal local dashboard for `etcd_k8s_bench`. Requires native `prometheus`, `promtool`, and Grafana binaries; no Docker, exporter sidecar, or external Grafana plugin is required. Commands below run from the workspace root. Both web services bind only to loopback. Anonymous Grafana access is **Viewer**; do not expose these development settings to a public network.

## Start

Use separate terminals for the two foreground services. Adjust the Grafana installation home if it is not `/usr/share/grafana`.

```sh
# Terminal 1: Prometheus
PROM_DATA=$(mktemp -d /tmp/bench-prometheus.XXXXXX)
promtool check config etcd_k8s_bench/monitoring/prometheus.yml
prometheus \
  --config.file=etcd_k8s_bench/monitoring/prometheus.yml \
  --storage.tsdb.path="$PROM_DATA" \
  --storage.tsdb.retention.time=1d \
  --web.listen-address=127.0.0.1:19090
# After stopping Prometheus with Ctrl-C:
rm -rf "$PROM_DATA"
```

```sh
# Terminal 2: Grafana
export BENCH_MONITORING_DIR="$PWD/etcd_k8s_bench/monitoring"
GRAFANA_DATA=$(mktemp -d /tmp/bench-grafana.XXXXXX)
grafana server \
  --homepath=/usr/share/grafana \
  --config="$BENCH_MONITORING_DIR/grafana.ini" \
  cfg:default.paths.data="$GRAFANA_DATA/data" \
  cfg:default.paths.logs="$GRAFANA_DATA/logs" \
  cfg:default.paths.plugins="$GRAFANA_DATA/plugins"
# After stopping Grafana with Ctrl-C:
rm -rf "$GRAFANA_DATA"
```

`BENCH_MONITORING_DIR` must be an absolute path: Grafana resolves relative provisioning paths against its installation home, not the shell working directory. Datasource and dashboard are provisioned automatically.

Open:

- Prometheus: <http://127.0.0.1:19090>
- Grafana: <http://127.0.0.1:13000/d/etcd-k8s-bench>

Before running the benchmark, start a **dedicated disposable empty etcd**. Configure its optional metrics listener as `--listen-metrics-urls http://127.0.0.1:12381`. Then run:

```sh
./target/release/etcd_k8s_bench run \
  --endpoint http://127.0.0.1:12379 \
  --update-rps 2000 --duration 10s \
  --metrics-listen 127.0.0.1:19898
```

The sample keeps the default topology and other workload settings. It writes to etcd; never use a production backend. The benchmark does not launch, clear, or change quota on etcd. Stop the dedicated etcd and remove its temporary data directory after each test.

## Panels and interpretation

The dashboard answers: was the update target offered, was work lost or rejected, how did RPC/Watch perform, and how close was the backend to its quota?

| Panel | Meaning |
| --- | --- |
| Application updates | Configured target gauge vs. offered/accepted update rate; excludes periodic NodeLease writes and reads |
| Measurement losses / logical errors | Cumulative `missed_schedule`, full-queue drops, and logical errors; generator loss is not an etcd error |
| RPC completions / errors | Physical RPC attempt completion rate by method and outcome |
| RPC latency | Online mean (sum/count) and cumulative maximum in ms; **not p50/p99** |
| Watch | Actual event rate and cumulative disconnect count |
| Dedicated etcd database / quota | Optional backend size and quota; no listener means no data, not zero size |

Scrapes run every 1s; rates use 5s windows and may be smoothed at measurement boundaries. Scrapes are not atomic snapshots of all counters. The final per-run JSON is authoritative for exact counts, HDR p50/p99, cutoff accounting, and Watch coverage. No latency histogram or percentile is invented from the online sum/count metrics.

The exporter ends when the benchmark exits. Prometheus retains previously scraped samples until retention or explicit data-directory cleanup. `up=0` after exit is expected; use the Grafana time selector to inspect the completed run's interval. A short run needs several scrapes before a rate appears. Use new storage directories for isolated runs; a new benchmark process resets counters.

## Verification performed

Validated with Prometheus/promtool 3.5.0, Grafana 13.1.0, and dedicated etcd 3.6.5 (16GiB quota). A real 2000/s × 10s run populated all six panels. Every panel query was executed through Grafana's provisioned Prometheus proxy and returned finite historical samples after the benchmark exporter exited. Chromium rendered all six panels with real data and no client-side errors. Temporary etcd and monitoring processes/data are test-owned and cleaned after verification.

The earlier pacing implementation replaced Tokio's millisecond-rounded wait with a reusable OS condition-variable timer. The following are historical same-host results, not measurements of the later direct-wait implementation:

| Application update load | Planned | Offered | Missed | Full-queue drops |
| --- | ---: | ---: | ---: | ---: |
| Previous 2000/s run (historical raw report) | 20,000 | 16,273 | 3,727 | 0 |
| Fixed 2000/s run, monitoring enabled | 20,000 | 19,884 | 116 | 0 |
| Fixed 10000/s run, monitoring enabled | 100,000 | 99,311 | 689 | 32,898 |

These are offered logical updates, not committed Txn counts. The 2k run offered 99.42% of its budget, with Txn completion 2096.2/s (includes periodic writes), Txn p99 11.519ms, and Range p99 12.607ms. The 10k run offered 99.311% but accepted only 66,413 updates; bounded-queue drops expose execution/backpressure limits rather than silently lowering the generation target. Both completed the full 10s window, returned exit 0, and passed the independent checker including plan/keyspace evidence. Remaining real deadline misses are retained; no historical slots are replayed and no absolute zero-loss guarantee is claimed on a shared host. The old raw report was read, not rerun or deleted.

Current `cargo test --workspace`: 166 tests passed. Python verification suite: 14 tests passed. The controlled-clock dispatcher test covers the full update budget at exact 1ms wakeups; the native pacing test covers canceling a long wait without delaying a new tick or returning early. Existing delayed-wake, no-catch-up, cutoff, and cancellation tests remain valid. Native backend and browser smoke verification, not these tests, establish performance/dashboard results.

## Higher-load diagnosis and fixes

Default topology, opaque value sizes, read/Watch shapes, 128 logical workers, total queue capacity 4096, 16GiB etcd quota, and a 10s measurement window were retained. Each run used a new empty native etcd 3.6.5 instance. Backend processes/data and profiling files were removed after every run; raw reports and diagnostic summaries were exported separately.

The first 5k run offered 49,525/50,000 updates, missed 475, and dropped none. The first 10k run offered 99,224/100,000, missed 776, and dropped 33,048 at admission. Txn completions were 6286.9/s and queue-wait p99 was 688.127ms. These baseline runs included CPU profiling; profiles covered preparation plus measurement and are not pure measurement-window latency/capacity proofs.

Evidence and retained changes:

- `perf` identified mutex contention (6.58% of sampled CPU events attributed to user-space mutex contention in the 10k run), allocation/copy costs, and executor task stealing. Limit default executor threads to 4 rather than one per available host CPU; preserve Tokio's standard `TOKIO_WORKER_THREADS` override. Logical worker count remains 128.
- Enable tonic's adaptive HTTP/2 receive-window policy on the existing shared channel. A controlled comparison at 10k, executor=4, `GOMAXPROCS=8`, `GOGC=100` increased accepted updates from 78,343 to 87,155. Do not equate this comparison with a guarantee for other hosts.
- Replace the separate condition-variable timer plus Notify handoff with direct high-resolution sleeps on the dispatcher's caller thread. Request at most 1ms per sleep, then yield cooperatively. No busy-wait, expired-slot replay, extended measurement window, or counter patching.
- Remove the per-tick target-vector allocation and duplicate update-key construction.
- Go pprof showed `runtime.gcDrain` at 39.56% cumulative CPU in the first 10k profile. For **dedicated test instances only**, explicitly set `GOMAXPROCS=8 GOGC=200` in final validation. Higher GOGC trades memory for less frequent GC; this is not a benchmark-side setting and is not applied to production or existing etcd instances. Sampled backend peak RSS in final runs was 666.8MiB (5k) and 1016.7MiB (10k).

Rejected diagnostics were retained in exported results: 256 logical workers improved throughput little while increasing tail latency and sampled RSS, so the default remains 128. A separate Watch connection did not demonstrate a throughput improvement and was removed. Backend GOMAXPROCS/GOGC tuning is stated explicitly, not silently attributed to benchmark-only code fixes.

Final validation used the retained fixes, default logical execution settings, and the dedicated backend's `GOMAXPROCS=8 GOGC=200`:

| Application update target | Planned / offered | Missed | Accepted / eventually successful | Full-queue drops | Txn completions/s | Txn p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5000/s × 10s | 50,000 / 50,000 | 0 | 50,000 | 0 | 5106.9 | 19.455ms |
| 10000/s × 10s | 100,000 / 100,000 | 0 | 86,539 | 13,461 | 8346.2 | 49.151ms |

The generation targets were fully offered in these runs. **The 10k execution path did not accept every update.** Its queue-wait p99 was 544.767ms; 83,313 updates succeeded inside measurement and 3,226 during drain. This is visible execution/backend backpressure, not proof of 10k successful writes/s. Physical Txn rates include periodic writes and differ from application updates. Both runs had no logical failures, full 10s measurement, complete six-root Watch coverage, final keyspace checked against the plan, exit 0, and independent checker success. With untuned backend runtime defaults, direct pacing still missed 129 updates at 5k and 267 at 10k; do not present tuned results as a universal zero-loss guarantee.

Reproduce on a disposable backend by adding `GOMAXPROCS=8 GOGC=200` to that etcd process's environment (not the shell globally), keeping its dedicated data directory and 16GiB quota. Run the benchmark separately:

```sh
./target/release/etcd_k8s_bench run --endpoint http://127.0.0.1:12379 \
  --update-rps 5000 --duration 10s --metrics-listen 127.0.0.1:19898
# Stop etcd and remove its temporary data, then start a new empty instance:
./target/release/etcd_k8s_bench run --endpoint http://127.0.0.1:12379 \
  --update-rps 10000 --duration 10s --metrics-listen 127.0.0.1:19898
```

API/GC references: [tonic 0.14.6 adaptive windows](https://docs.rs/tonic/0.14.6/tonic/transport/struct.Endpoint.html#method.http2_adaptive_window), [Tokio executor threads](https://docs.rs/tokio/1.52.3/tokio/runtime/struct.Builder.html#method.worker_threads), [Go GC CPU/memory trade-off](https://go.dev/doc/gc-guide#GOGC).
