use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use super::*;

#[test]
fn test_metrics_exposition() {
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        let run_metrics = RunMetrics::new();
        run_metrics.set_revision(1_234_567);
        run_metrics.set_compact_revision(1_234_566);
        run_metrics.set_compactor_state("active");
        run_metrics.record_compactor_error();
        run_metrics.record_compactor_recovery();
        run_metrics.record_range_result(2, 7, 10, 17, 8, 2);
    });

    let rendered = recorder.handle().render();
    assert_metric(&rendered, REVISION, &[], 1_234_567.0);
    assert_metric(&rendered, COMPACT_REVISION, &[], 1_234_566.0);
    assert_metric(&rendered, COMPACTOR_STATE, &[], 0.0);
    assert_metric(&rendered, COMPACTOR_ERRORS_TOTAL, &[], 1.0);
    assert_metric(&rendered, COMPACTOR_RECOVERIES_TOTAL, &[], 1.0);
    assert_metric(&rendered, RANGE_PAGES, &[], 2.0);
    assert_metric(&rendered, RANGE_SNAPSHOT_REVISION, &[], 7.0);
    assert_metric(&rendered, RANGE_SERVER_COUNT, &[], 10.0);
    assert_metric(&rendered, RANGE_LAST_KEY_BYTES, &[], 17.0);
    assert_metric(&rendered, RANGE_SELECTED, &[], 8.0);
    assert_metric(&rendered, RANGE_DISCARDED, &[], 2.0);
}

#[test]
fn test_run_metrics_finite_latency_projection() {
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        let run_metrics = RunMetrics::new();
        run_metrics.record_logical_offered("measurement", 0, 3);
        run_metrics.record_logical_accepted("measurement", 0, 2);
        run_metrics.record_logical_dropped_full("measurement", 0, 1);
        run_metrics.record_logical_missed_schedule("measurement", 0, 4);
        run_metrics.record_logical_not_offered_after_stop("measurement", 0, 5);
        run_metrics.record_logical_canceled_queued("measurement", 0, 6);
        run_metrics.record_logical_canceled_running("measurement", 0, 7);
        run_metrics.record_logical_started("measurement", 0);
        run_metrics.record_logical_started("preparation", 0);
        run_metrics.record_logical_completed(
            "measurement",
            0,
            Outcome::Success,
            Duration::from_micros(3),
        );
        run_metrics.record_queue_wait("measurement", 0, Duration::from_micros(7));
        run_metrics.record_rpc_started("measurement", 1, 11);
        run_metrics.record_rpc_completed(
            "measurement",
            1,
            Outcome::Success,
            13,
            Duration::from_micros(5),
        );
    });
    let rendered = recorder.handle().render();
    assert_metric(
        &rendered,
        LOGICAL_STARTED_TOTAL,
        &[
            ("phase", "preparation"),
            ("work_class", "create"),
            ("resource", "pods"),
        ],
        1.0,
    );
    for (name, expected) in [
        (LOGICAL_OFFERED_TOTAL, 3.0),
        (LOGICAL_ACCEPTED_TOTAL, 2.0),
        (LOGICAL_DROPPED_FULL_TOTAL, 1.0),
        (LOGICAL_MISSED_SCHEDULE_TOTAL, 4.0),
        (LOGICAL_NOT_OFFERED_AFTER_STOP_TOTAL, 5.0),
        (LOGICAL_CANCELED_QUEUED_TOTAL, 6.0),
        (LOGICAL_CANCELED_RUNNING_TOTAL, 7.0),
    ] {
        assert_metric(
            &rendered,
            name,
            &[
                ("phase", "measurement"),
                ("work_class", "update"),
                ("resource", "pods"),
            ],
            expected,
        );
    }
    assert_metric(
        &rendered,
        LOGICAL_DURATION_SAMPLES_TOTAL,
        &[
            ("phase", "measurement"),
            ("work_class", "update"),
            ("resource", "pods"),
        ],
        1.0,
    );
    assert_metric(
        &rendered,
        LOGICAL_DURATION_MICROSECONDS_TOTAL,
        &[
            ("phase", "measurement"),
            ("work_class", "update"),
            ("resource", "pods"),
        ],
        3.0,
    );
    assert_metric(
        &rendered,
        LOGICAL_DURATION_MAX_MICROSECONDS,
        &[
            ("phase", "measurement"),
            ("work_class", "update"),
            ("resource", "pods"),
        ],
        3.0,
    );
    assert_metric(
        &rendered,
        QUEUE_WAIT_SAMPLES_PHASE_TOTAL,
        &[
            ("phase", "measurement"),
            ("work_class", "update"),
            ("resource", "pods"),
        ],
        1.0,
    );
    assert_metric(
        &rendered,
        QUEUE_WAIT_MICROSECONDS_PHASE_TOTAL,
        &[
            ("phase", "measurement"),
            ("work_class", "update"),
            ("resource", "pods"),
        ],
        7.0,
    );
    assert_metric(
        &rendered,
        QUEUE_WAIT_MAX_MICROSECONDS_PHASE,
        &[
            ("phase", "measurement"),
            ("work_class", "update"),
            ("resource", "pods"),
        ],
        7.0,
    );
    assert_metric(
        &rendered,
        RPC_DURATION_SAMPLES_PHASE_TOTAL,
        &[("phase", "measurement"), ("rpc", "txn")],
        1.0,
    );
    assert_metric(
        &rendered,
        RPC_DURATION_MICROSECONDS_PHASE_TOTAL,
        &[("phase", "measurement"), ("rpc", "txn")],
        5.0,
    );
    assert_metric(
        &rendered,
        RPC_DURATION_MAX_MICROSECONDS_PHASE,
        &[("phase", "measurement"), ("rpc", "txn")],
        5.0,
    );
}

#[tokio::test]
async fn test_exporter_scrape_and_shutdown_reclaims_listener() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let address = listener.local_addr().unwrap();
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        ::metrics::counter!("etcd_k8s_bench_exporter_test_total").increment(1);
    });

    let (shutdown, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(serve_metrics_listener(
        listener,
        recorder.handle(),
        shutdown_rx,
    ));

    let mut malformed = TcpStream::connect(address).await.unwrap();
    let mut malformed_response = Vec::new();
    malformed
        .write_all(b"not an HTTP request\r\n\r\n")
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        malformed.read_to_end(&mut malformed_response),
    )
    .await
    .unwrap()
    .unwrap();

    let mut scrape = TcpStream::connect(address).await.unwrap();
    scrape
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    scrape.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("etcd_k8s_bench_exporter_test_total"));

    let mut root = TcpStream::connect(address).await.unwrap();
    root.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut root_response = Vec::new();
    root.read_to_end(&mut root_response).await.unwrap();
    let root_response = String::from_utf8(root_response).unwrap();
    assert!(root_response.starts_with("HTTP/1.1 200 OK"));
    assert!(root_response.contains("etcd_k8s_bench_exporter_test_total"));

    // Drive an accepted HTTP/1.1 keep-alive connection before shutdown.
    let mut held = TcpStream::connect(address).await.unwrap();
    held.write_all(
        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n",
    )
    .await
    .unwrap();
    let held_response = read_http_response(&mut held).await;
    assert!(held_response.starts_with("HTTP/1.1 200 OK"));
    assert!(held_response.contains("etcd_k8s_bench_exporter_test_total"));

    // A second response on the same socket proves the first request was
    // accepted without closing the keep-alive connection.
    held.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        .await
        .unwrap();
    let held_second_response = read_http_response(&mut held).await;
    assert!(held_second_response.starts_with("HTTP/1.1 200 OK"));
    assert!(held_second_response.contains("etcd_k8s_bench_exporter_test_total"));

    shutdown.send(()).unwrap();
    assert!(task.await.unwrap().is_ok());

    let mut after_shutdown = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(1), held.read(&mut after_shutdown))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read, 0,
        "held exporter connection remained open after shutdown"
    );
    drop(held);
    // The listener itself and all accepted connections have been
    // reclaimed before shutdown returns.
    let rebound = TcpListener::bind(address).await.unwrap();
    drop(rebound);
}

#[tokio::test]
async fn test_exporter_cleanup_drains_completed_panic_and_held_task() {
    struct DropSentinel {
        drops: Arc<AtomicUsize>,
        completed: Option<oneshot::Sender<()>>,
    }

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.drops.fetch_add(1, AtomicOrdering::Release);
            if let Some(completed) = self.completed.take() {
                let _ = completed.send(());
            }
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let (panic_done_tx, panic_done_rx) = oneshot::channel();
    let (held_entered_tx, held_entered_rx) = oneshot::channel();
    let mut connections = JoinSet::new();
    let panic_drops = drops.clone();

    let panic_handle = connections.spawn(async move {
        let _sentinel = DropSentinel {
            drops: panic_drops,
            completed: Some(panic_done_tx),
        };
        panic!("injected exporter panic");
        #[allow(unreachable_code)]
        Ok::<(), hyper::Error>(())
    });
    let held_drops = drops.clone();
    let _held_handle = connections.spawn(async move {
        let _sentinel = DropSentinel {
            drops: held_drops,
            completed: None,
        };
        held_entered_tx.send(()).unwrap();
        std::future::pending::<Result<(), hyper::Error>>().await
    });

    // The panic is complete, but its JoinSet entry is intentionally still
    // unjoined when cleanup begins.
    panic_done_rx.await.unwrap();
    held_entered_rx.await.unwrap();
    assert!(panic_handle.is_finished());

    let result = abort_metric_connections(&mut connections).await;
    assert!(
        connections.is_empty(),
        "cleanup left connection tasks unjoined"
    );
    assert_eq!(drops.load(AtomicOrdering::Acquire), 2);

    let error = result.unwrap_err();
    assert!(error.contains("injected exporter panic"));
}

async fn read_http_response(stream: &mut TcpStream) -> String {
    let mut response = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 1024];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut chunk))
            .await
            .expect("timed out reading HTTP response")
            .expect("failed to read HTTP response");
        assert!(read > 0, "exporter closed before sending HTTP headers");
        response.extend_from_slice(&chunk[..read]);
        if let Some(position) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };

    let headers = std::str::from_utf8(&response[..header_end])
        .expect("exporter response headers were not UTF-8");
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length").then(|| {
                value
                    .trim()
                    .parse::<usize>()
                    .expect("invalid content length")
            })
        })
        .expect("exporter response omitted content length");
    let response_length = header_end + content_length;
    while response.len() < response_length {
        let mut chunk = [0_u8; 1024];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut chunk))
            .await
            .expect("timed out reading HTTP response body")
            .expect("failed to read HTTP response body");
        assert!(read > 0, "exporter closed before sending the HTTP body");
        response.extend_from_slice(&chunk[..read]);
    }
    response.truncate(response_length);
    String::from_utf8(response).expect("exporter response was not UTF-8")
}

fn assert_metric(rendered: &str, name: &str, labels: &[(&str, &str)], expected: f64) {
    let label_text = if labels.is_empty() {
        String::new()
    } else {
        let labels = labels
            .iter()
            .map(|(key, value)| format!("{key}=\"{value}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!("{{{labels}}}")
    };
    let sample_name = name.to_string();
    let prefix = format!("{sample_name}{label_text}");
    let value = rendered
        .lines()
        .find_map(|line| {
            line.strip_prefix(&prefix)
                .and_then(|rest| rest.strip_prefix(' '))
                .and_then(|value| value.parse::<f64>().ok())
        })
        .unwrap_or_else(|| panic!("metric {prefix} missing from:\n{rendered}"));
    assert_eq!(value, expected, "metric {prefix}");
}
