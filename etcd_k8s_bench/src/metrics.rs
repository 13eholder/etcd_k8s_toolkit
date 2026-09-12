//! Low-cardinality metrics for the benchmark.
//!
//! This module deliberately only emits fixed metric names.  Labels are either
//! represented by enums below or by validated names supplied by the workload
//! configuration; request keys, IDs, endpoints, and error text never become
//! labels.

use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder};
use thiserror::Error;

pub const SCHEDULED_TOTAL: &str = "etcd_k8s_bench_scheduled_total";
pub const OPERATIONS_TOTAL: &str = "etcd_k8s_bench_operations_total";
pub const REQUESTS_TOTAL: &str = "etcd_k8s_bench_requests_total";
pub const REQUEST_DURATION_SECONDS: &str = "etcd_k8s_bench_request_duration_seconds";
pub const QUEUE_WAIT_SECONDS: &str = "etcd_k8s_bench_queue_wait_seconds";
pub const REQUEST_BYTES: &str = "etcd_k8s_bench_request_bytes";
pub const TXN_TOTAL: &str = "etcd_k8s_bench_txn_total";
pub const CAS_RETRIES_TOTAL: &str = "etcd_k8s_bench_cas_retries_total";
pub const NODE_HEARTBEATS_TOTAL: &str = "etcd_k8s_bench_node_heartbeats_total";
pub const DROPPED_TOTAL: &str = "etcd_k8s_bench_dropped_total";
pub const INFO: &str = "etcd_k8s_bench_info";
pub const WATCH_EVENTS_TOTAL: &str = "etcd_k8s_bench_watch_events_total";
pub const WATCH_EVENT_BYTES: &str = "etcd_k8s_bench_watch_event_bytes";
pub const WATCH_CANCEL_TOTAL: &str = "etcd_k8s_bench_watch_cancel_total";
pub const WATCH_RECONNECT_TOTAL: &str = "etcd_k8s_bench_watch_reconnect_total";
pub const RANGE_REQUESTS_TOTAL: &str = "etcd_k8s_bench_range_requests_total";
pub const RANGE_PAGES: &str = "etcd_k8s_bench_range_pages";
pub const RANGE_SNAPSHOT_REVISION: &str = "etcd_k8s_bench_range_snapshot_revision";
pub const RANGE_SERVER_COUNT: &str = "etcd_k8s_bench_range_server_count";
pub const RANGE_LAST_KEY_BYTES: &str = "etcd_k8s_bench_range_last_key_bytes";
pub const RANGE_SELECTED: &str = "etcd_k8s_bench_range_selected";
pub const RANGE_DISCARDED: &str = "etcd_k8s_bench_range_discarded";
static MAX_REVISION: AtomicI64 = AtomicI64::new(0);
static MAX_COMPACT_REVISION: AtomicI64 = AtomicI64::new(0);
static WATCH_RECONNECTS: AtomicU64 = AtomicU64::new(0);
static REVISION_GAUGE_LOCK: Mutex<()> = Mutex::new(());
#[cfg(test)]
static WATCH_RECONNECT_TEST_LOCK: Mutex<()> = Mutex::new(());

pub const INFLIGHT: &str = "etcd_k8s_bench_inflight";
pub const REVISION: &str = "etcd_k8s_bench_revision";
pub const COMPACT_REVISION: &str = "etcd_k8s_bench_compact_revision";
pub const COMPACT_TOTAL: &str = "etcd_k8s_bench_compact_total";
pub const POD_SIZE_BYTES: &str = "etcd_k8s_bench_pod_size_bytes";
pub const NODES: &str = "etcd_k8s_bench_nodes";
pub const COMPACTOR_STATE: &str = "etcd_k8s_bench_compactor_state";
pub const COMPACTOR_ERRORS_TOTAL: &str = "etcd_k8s_bench_compactor_errors_total";
pub const COMPACTOR_RECOVERIES_TOTAL: &str = "etcd_k8s_bench_compactor_recoveries_total";

/// Labels which may safely be supplied by a workload configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceName(String);

/// A workload label used by the informational metric.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadName(String);

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LabelError {
    #[error("label name must not be empty")]
    Empty,
    #[error("label name is too long")]
    TooLong,
    #[error("label name contains an invalid character")]
    InvalidCharacter,
}

impl ResourceName {
    pub fn new(name: impl Into<String>) -> Result<Self, LabelError> {
        validate_name(name.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl WorkloadName {
    pub fn new(name: impl Into<String>) -> Result<Self, LabelError> {
        validate_name(name.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ResourceName {
    type Error = LabelError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

impl TryFrom<&str> for WorkloadName {
    type Error = LabelError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

fn validate_name(name: String) -> Result<String, LabelError> {
    if name.is_empty() {
        return Err(LabelError::Empty);
    }
    if name.len() > 64 {
        return Err(LabelError::TooLong);
    }
    if !name.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
    }) {
        return Err(LabelError::InvalidCharacter);
    }
    Ok(name)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Success,
    GrpcError,
    Conflict,
    NotFound,
    Timeout,
    /// The mutable request may have been processed, so the caller must
    /// reconcile instead of blindly replaying it.
    ResultUnknown,
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::GrpcError => "grpc_error",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::Timeout => "timeout",
            Self::ResultUnknown => "result_unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Create,
    Update,
    Delete,
    Read,
    Compact,
}

impl Operation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Read => "read",
            Self::Compact => "compact",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rpc {
    Txn,
    Range,
    Status,
    Compact,
}

impl Rpc {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Txn => "txn",
            Self::Range => "range",
            Self::Status => "status",
            Self::Compact => "compact",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeShape {
    Point,
    CountOnly,
    UnboundedPrefix,
    PagedPrefix,
}

impl RangeShape {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::CountOnly => "count_only",
            Self::UnboundedPrefix => "unbounded_prefix",
            Self::PagedPrefix => "paged_prefix",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxnShape {
    Create,
    Update,
    Delete,
    CompactRevision,
}

impl TxnShape {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::CompactRevision => "compact_revision",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxnResult {
    Committed,
    Conflict,
}

impl TxnResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Conflict => "conflict",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatKind {
    NodeStatus,
    NodeLease,
}

impl HeartbeatKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeStatus => "node_status",
            Self::NodeLease => "node_lease",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    Backpressure,
    QueueClosed,
}

impl DropReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Backpressure => "backpressure",
            Self::QueueClosed => "queue_closed",
        }
    }
}

const REQUEST_DURATION_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];
const REQUEST_BYTES_BUCKETS: &[f64] = &[
    128.0,
    512.0,
    1024.0,
    4096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
];

/// Installs the Prometheus recorder and scrape endpoint exactly once in the
/// process.  The exporter owns the HTTP listener; this module does not create
/// a registry or an HTTP server itself.
pub fn install_exporter(listen: SocketAddr) -> Result<(), BuildError> {
    PrometheusBuilder::new()
        .with_http_listener(listen)
        .set_buckets_for_metric(
            Matcher::Full(REQUEST_DURATION_SECONDS.to_owned()),
            REQUEST_DURATION_BUCKETS,
        )?
        .set_buckets_for_metric(
            Matcher::Full(REQUEST_BYTES.to_owned()),
            REQUEST_BYTES_BUCKETS,
        )?
        .install()
}

pub fn record_scheduled(operation: Operation, resource: &ResourceName) {
    metrics::counter!(SCHEDULED_TOTAL, "operation" => operation.as_str(), "resource" => resource.as_str().to_owned())
        .increment(1);
}

/// Records one logical resource operation, not an individual CAS/RPC attempt.
pub fn record_operation(operation: Operation, resource: &ResourceName, outcome: Outcome) {
    metrics::counter!(
        OPERATIONS_TOTAL,
        "operation" => operation.as_str(),
        "resource" => resource.as_str().to_owned(),
        "outcome" => outcome.as_str()
    )
    .increment(1);
}
/// Publishes the last completed Range result without exposing keys as labels.
/// The key itself remains available in the result path; metrics carry only its
/// byte length to keep cardinality bounded.
pub fn record_range_result(
    pages: u64,
    snapshot_revision: i64,
    server_count: i64,
    last_key: &[u8],
    selected: u64,
    discarded: u64,
) {
    metrics::gauge!(RANGE_PAGES).set(pages as f64);
    metrics::gauge!(RANGE_SNAPSHOT_REVISION).set(snapshot_revision as f64);
    metrics::gauge!(RANGE_SERVER_COUNT).set(server_count as f64);
    metrics::gauge!(RANGE_LAST_KEY_BYTES).set(last_key.len() as f64);
    metrics::gauge!(RANGE_SELECTED).set(selected as f64);
    metrics::gauge!(RANGE_DISCARDED).set(discarded as f64);
}

pub fn record_request(rpc: Rpc, outcome: Outcome) {
    metrics::counter!(REQUESTS_TOTAL, "rpc" => rpc.as_str(), "outcome" => outcome.as_str())
        .increment(1);
}

pub fn record_request_duration(rpc: Rpc, duration: Duration) {
    metrics::histogram!(REQUEST_DURATION_SECONDS, "rpc" => rpc.as_str())
        .record(duration.as_secs_f64());
}
/// Records time spent waiting in a scheduler queue. This histogram is
/// deliberately separate from RPC duration so transport latency is not
/// presented as end-to-end work-item latency.
pub fn record_queue_wait(work_class: &'static str, duration: Duration) {
    metrics::histogram!(
        QUEUE_WAIT_SECONDS,
        "work_class" => work_class
    )
    .record(duration.as_secs_f64());
}

pub fn record_range_request(shape: RangeShape, outcome: Outcome) {
    metrics::counter!(
        RANGE_REQUESTS_TOTAL,
        "shape" => shape.as_str(),
        "outcome" => outcome.as_str()
    )
    .increment(1);
}

pub fn record_request_bytes(resource: &ResourceName, bytes: usize) {
    metrics::histogram!(REQUEST_BYTES, "resource" => resource.as_str().to_owned())
        .record(bytes as f64);
}

/// Records one etcd Txn attempt; use `record_operation` for the enclosing
/// logical operation and this function for every attempt separately.
pub fn record_txn(resource: &ResourceName, shape: TxnShape, result: TxnResult) {
    metrics::counter!(
        TXN_TOTAL,
        "resource" => resource.as_str().to_owned(),
        "shape" => shape.as_str(),
        "result" => result.as_str()
    )
    .increment(1);
}
pub fn record_cas_retry(resource: &ResourceName, operation: Operation) {
    metrics::counter!(
        CAS_RETRIES_TOTAL,
        "resource" => resource.as_str().to_owned(),
        "operation" => operation.as_str()
    )
    .increment(1);
}

pub fn record_node_heartbeat(kind: HeartbeatKind, outcome: Outcome) {
    metrics::counter!(
        NODE_HEARTBEATS_TOTAL,
        "kind" => kind.as_str(),
        "outcome" => outcome.as_str()
    )
    .increment(1);
}
pub fn record_dropped(reason: DropReason) {
    metrics::counter!(DROPPED_TOTAL, "reason" => reason.as_str()).increment(1);
}

pub fn record_watch_response(
    watch: &WorkloadName,
    response: &crate::pb::etcdserverpb::WatchResponse,
) {
    let events = response.events.len() as u64;
    let bytes: u64 = response
        .events
        .iter()
        .map(|event| {
            event
                .kv
                .as_ref()
                .map_or(0, |kv| (kv.key.len() + kv.value.len()) as u64)
                + event
                    .prev_kv
                    .as_ref()
                    .map_or(0, |kv| (kv.key.len() + kv.value.len()) as u64)
        })
        .sum();
    metrics::counter!(WATCH_EVENTS_TOTAL, "watch" => watch.as_str().to_owned()).increment(events);
    metrics::counter!(WATCH_EVENT_BYTES, "watch" => watch.as_str().to_owned()).increment(bytes);
}

pub fn record_watch_cancel(watch: &WorkloadName, reason: &'static str) {
    metrics::counter!(WATCH_CANCEL_TOTAL, "watch" => watch.as_str().to_owned(), "reason" => reason)
        .increment(1);
}

pub fn record_watch_reconnect(watch: &WorkloadName) {
    WATCH_RECONNECTS.fetch_add(1, Ordering::Relaxed);
    metrics::counter!(WATCH_RECONNECT_TOTAL, "watch" => watch.as_str().to_owned()).increment(1);
}

#[cfg(test)]
pub fn reset_watch_reconnects() {
    WATCH_RECONNECTS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub fn lock_watch_reconnect_tests() -> std::sync::MutexGuard<'static, ()> {
    WATCH_RECONNECT_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
pub fn watch_reconnects_total() -> u64 {
    WATCH_RECONNECTS.load(Ordering::Relaxed)
}

pub fn add_inflight(rpc: Rpc) {
    metrics::gauge!(INFLIGHT, "rpc" => rpc.as_str()).increment(1);
}

pub fn remove_inflight(rpc: Rpc) {
    metrics::gauge!(INFLIGHT, "rpc" => rpc.as_str()).decrement(1);
}

pub fn set_revision(revision: i64) {
    let _guard = REVISION_GAUGE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let maximum = MAX_REVISION
        .fetch_max(revision, Ordering::Relaxed)
        .max(revision);
    metrics::gauge!(REVISION).set(maximum as f64);
}

/// Publish the latest compact revision observed locally or from a winning
/// remote compactor.  It is monotonic for the same reason as the etcd header
/// revision gauge, so a stale failure response cannot move it backwards.
pub fn set_compact_revision(revision: i64) {
    let maximum = MAX_COMPACT_REVISION
        .fetch_max(revision, Ordering::Relaxed)
        .max(revision);
    metrics::gauge!(COMPACT_REVISION).set(maximum as f64);
}

pub fn record_compact(outcome: Outcome) {
    metrics::counter!(COMPACT_TOTAL, "outcome" => outcome.as_str()).increment(1);
}
/// Publish the current compactor state as a numeric gauge:
/// `0` means active and `1` means resync-required.
pub fn set_compactor_state(state: &'static str) {
    let value = if state == "active" { 0.0 } else { 1.0 };
    metrics::gauge!(COMPACTOR_STATE).set(value);
    metrics::gauge!(COMPACTOR_STATE, "state" => state).set(1.0);
}

pub fn record_compactor_error(error: &'static str) {
    metrics::counter!(COMPACTOR_ERRORS_TOTAL, "error" => error).increment(1);
}

pub fn record_compactor_recovery(recovery: &'static str) {
    metrics::counter!(COMPACTOR_RECOVERIES_TOTAL, "recovery" => recovery).increment(1);
}

pub fn record_info(version: &'static str, workload: &WorkloadName) {
    metrics::gauge!(INFO, "version" => version, "workload" => workload.as_str().to_owned())
        .set(1.0);
}

pub fn set_pod_size(bytes: u64) {
    metrics::gauge!(POD_SIZE_BYTES).set(bytes as f64);
}

pub fn set_nodes(nodes: u64) {
    metrics::gauge!(NODES).set(nodes as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::etcdserverpb::WatchResponse;
    use crate::pb::mvccpb::KeyValue;

    #[test]
    fn test_metrics_exposition() {
        let _serial = lock_watch_reconnect_tests();
        reset_watch_reconnects();
        let recorder = PrometheusBuilder::new().build_recorder();
        let resource = ResourceName::new("pods").unwrap();
        let workload = WorkloadName::new("k8s").unwrap();
        let watch_response = WatchResponse {
            events: vec![crate::pb::mvccpb::Event {
                kv: Some(KeyValue {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ..Default::default()
                }),
                prev_kv: Some(KeyValue {
                    key: b"p".to_vec(),
                    value: b"q".to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        };

        metrics::with_local_recorder(&recorder, || {
            record_scheduled(Operation::Create, &resource);
            for outcome in [
                Outcome::Success,
                Outcome::GrpcError,
                Outcome::Conflict,
                Outcome::NotFound,
                Outcome::Timeout,
                Outcome::ResultUnknown,
            ] {
                record_operation(Operation::Create, &resource, outcome);
            }
            record_request(Rpc::Txn, Outcome::Success);
            record_request_duration(Rpc::Txn, Duration::from_millis(2));
            record_queue_wait("create", Duration::from_millis(1));
            record_request_bytes(&resource, 4096);
            record_txn(&resource, TxnShape::Create, TxnResult::Committed);
            record_range_request(RangeShape::PagedPrefix, Outcome::Success);
            record_cas_retry(&resource, Operation::Update);
            record_node_heartbeat(HeartbeatKind::NodeLease, Outcome::Success);
            record_dropped(DropReason::Backpressure);
            add_inflight(Rpc::Txn);
            remove_inflight(Rpc::Txn);
            set_revision(1_234_567);
            set_compact_revision(1_234_566);
            record_compact(Outcome::Success);
            set_compactor_state("active");
            record_compactor_error("failure_kv_missing");
            record_compactor_recovery("missing_key");
            record_range_result(2, 7, 10, b"/registry/pods/z", 8, 2);
            record_watch_response(&workload, &watch_response);
            record_watch_reconnect(&workload);
            record_watch_cancel(&workload, "eof");
            record_info("0.1.0", &workload);
            set_pod_size(4096);
            set_nodes(2000);
        });

        let rendered = recorder.handle().render();
        assert_metric(
            &rendered,
            SCHEDULED_TOTAL,
            &[("operation", "create"), ("resource", "pods")],
            1.0,
        );
        for (outcome, label) in [
            (Outcome::Success, "success"),
            (Outcome::GrpcError, "grpc_error"),
            (Outcome::Conflict, "conflict"),
            (Outcome::NotFound, "not_found"),
            (Outcome::Timeout, "timeout"),
            (Outcome::ResultUnknown, "result_unknown"),
        ] {
            assert_eq!(outcome.as_str(), label);
            assert_metric(
                &rendered,
                OPERATIONS_TOTAL,
                &[
                    ("operation", "create"),
                    ("resource", "pods"),
                    ("outcome", label),
                ],
                1.0,
            );
        }
        assert_metric(
            &rendered,
            REQUESTS_TOTAL,
            &[("rpc", "txn"), ("outcome", "success")],
            1.0,
        );
        assert_metric(
            &rendered,
            REQUEST_DURATION_SECONDS,
            &[("rpc", "txn")],
            0.002,
        );
        assert_metric(
            &rendered,
            QUEUE_WAIT_SECONDS,
            &[("work_class", "create")],
            0.001,
        );
        assert_metric(&rendered, REQUEST_BYTES, &[("resource", "pods")], 4096.0);
        assert_metric(
            &rendered,
            TXN_TOTAL,
            &[
                ("resource", "pods"),
                ("shape", "create"),
                ("result", "committed"),
            ],
            1.0,
        );
        assert_metric(
            &rendered,
            RANGE_REQUESTS_TOTAL,
            &[("shape", "paged_prefix"), ("outcome", "success")],
            1.0,
        );
        assert_metric(
            &rendered,
            CAS_RETRIES_TOTAL,
            &[("resource", "pods"), ("operation", "update")],
            1.0,
        );
        assert_metric(
            &rendered,
            NODE_HEARTBEATS_TOTAL,
            &[("kind", "node_lease"), ("outcome", "success")],
            1.0,
        );
        assert_metric(&rendered, DROPPED_TOTAL, &[("reason", "backpressure")], 1.0);
        assert_metric(&rendered, INFLIGHT, &[("rpc", "txn")], 0.0);
        assert_metric(&rendered, REVISION, &[], 1_234_567.0);
        assert_metric(&rendered, COMPACT_REVISION, &[], 1_234_566.0);
        assert_metric(&rendered, COMPACT_TOTAL, &[("outcome", "success")], 1.0);
        assert_metric(&rendered, COMPACTOR_STATE, &[], 0.0);
        assert_metric(&rendered, COMPACTOR_STATE, &[("state", "active")], 1.0);
        assert_metric(
            &rendered,
            COMPACTOR_ERRORS_TOTAL,
            &[("error", "failure_kv_missing")],
            1.0,
        );
        assert_metric(
            &rendered,
            COMPACTOR_RECOVERIES_TOTAL,
            &[("recovery", "missing_key")],
            1.0,
        );
        assert_metric(&rendered, RANGE_PAGES, &[], 2.0);
        assert_metric(&rendered, RANGE_SNAPSHOT_REVISION, &[], 7.0);
        assert_metric(&rendered, RANGE_SERVER_COUNT, &[], 10.0);
        assert_metric(
            &rendered,
            RANGE_LAST_KEY_BYTES,
            &[],
            b"/registry/pods/z".len() as f64,
        );
        assert_metric(&rendered, RANGE_SELECTED, &[], 8.0);
        assert_metric(&rendered, RANGE_DISCARDED, &[], 2.0);
        assert_metric(&rendered, WATCH_EVENTS_TOTAL, &[("watch", "k8s")], 1.0);
        assert_metric(&rendered, WATCH_EVENT_BYTES, &[("watch", "k8s")], 4.0);
        assert_metric(&rendered, WATCH_RECONNECT_TOTAL, &[("watch", "k8s")], 1.0);
        assert_metric(
            &rendered,
            WATCH_CANCEL_TOTAL,
            &[("watch", "k8s"), ("reason", "eof")],
            1.0,
        );
        assert_metric(
            &rendered,
            INFO,
            &[("version", "0.1.0"), ("workload", "k8s")],
            1.0,
        );
        assert_metric(&rendered, POD_SIZE_BYTES, &[], 4096.0);
        assert_metric(&rendered, NODES, &[], 2000.0);
        assert_eq!(watch_reconnects_total(), 1);
        reset_watch_reconnects();
    }

    #[test]
    fn test_reconnect_metric_concurrent() {
        let _serial = lock_watch_reconnect_tests();
        reset_watch_reconnects();
        let recorder = PrometheusBuilder::new().build_recorder();
        let watch = WorkloadName::new("pods").unwrap();

        std::thread::scope(|scope| {
            for _ in 0..2 {
                let recorder = &recorder;
                let watch = watch.clone();
                scope.spawn(move || {
                    metrics::with_local_recorder(recorder, || record_watch_reconnect(&watch));
                });
            }
        });

        assert_eq!(watch_reconnects_total(), 2);
        let rendered = recorder.handle().render();
        assert_metric(&rendered, WATCH_RECONNECT_TOTAL, &[("watch", "pods")], 2.0);
        reset_watch_reconnects();
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
        let sample_name = if matches!(
            name,
            REQUEST_DURATION_SECONDS | QUEUE_WAIT_SECONDS | REQUEST_BYTES
        ) {
            format!("{name}_sum")
        } else {
            name.to_string()
        };
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
}
