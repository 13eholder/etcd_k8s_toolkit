//! Low-cardinality metrics for the benchmark.
//!
//! This module deliberately only emits fixed metric names.  Labels are either
//! represented by enums below or by validated names supplied by the workload
//! configuration; request keys, IDs, endpoints, and error text never become
//! labels.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder};

pub const SCHEDULED_TOTAL: &str = "etcd_k8s_bench_scheduled_total";
pub const OPERATIONS_TOTAL: &str = "etcd_k8s_bench_operations_total";
pub const REQUESTS_TOTAL: &str = "etcd_k8s_bench_requests_total";
pub const REQUEST_DURATION_SECONDS: &str = "etcd_k8s_bench_request_duration_seconds";
pub const REQUEST_BYTES: &str = "etcd_k8s_bench_request_bytes";
pub const TXN_TOTAL: &str = "etcd_k8s_bench_txn_total";
pub const CAS_RETRIES_TOTAL: &str = "etcd_k8s_bench_cas_retries_total";
pub const LEASE_KEEPALIVE_TOTAL: &str = "etcd_k8s_bench_lease_keepalive_total";
pub const NODE_HEARTBEATS_TOTAL: &str = "etcd_k8s_bench_node_heartbeats_total";
pub const DROPPED_TOTAL: &str = "etcd_k8s_bench_dropped_total";
static MAX_REVISION: AtomicI64 = AtomicI64::new(0);
static REVISION_GAUGE_LOCK: Mutex<()> = Mutex::new(());

pub const INFLIGHT: &str = "etcd_k8s_bench_inflight";
pub const REVISION: &str = "etcd_k8s_bench_revision";
pub const COMPACT_TOTAL: &str = "etcd_k8s_bench_compact_total";
pub const INFO: &str = "etcd_k8s_bench_info";
pub const POD_SIZE_BYTES: &str = "etcd_k8s_bench_pod_size_bytes";
pub const NODES: &str = "etcd_k8s_bench_nodes";

/// Labels which may safely be supplied by a workload configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceName(String);

/// Labels which may safely be supplied by a lease-pool configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolName(String);

/// A workload label used by the informational metric.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadName(String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelError {
    Empty,
    TooLong,
    InvalidCharacter,
}

impl fmt::Display for LabelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Empty => "label name must not be empty",
            Self::TooLong => "label name is too long",
            Self::InvalidCharacter => "label name contains an invalid character",
        };
        f.write_str(message)
    }
}

impl std::error::Error for LabelError {}

impl ResourceName {
    pub fn new(name: impl Into<String>) -> Result<Self, LabelError> {
        validate_name(name.into()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl PoolName {
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

impl TryFrom<&str> for PoolName {
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
    Timeout,
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::GrpcError => "grpc_error",
            Self::Timeout => "timeout",
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
    LeaseGrant,
    LeaseKeepAlive,
    Status,
    Compact,
}

impl Rpc {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Txn => "txn",
            Self::Range => "range",
            Self::LeaseGrant => "lease_grant",
            Self::LeaseKeepAlive => "lease_keepalive",
            Self::Status => "status",
            Self::Compact => "compact",
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
pub enum KeepAliveMessage {
    Request,
    Response,
}

impl KeepAliveMessage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatKind {
    Node,
    NodeStatus,
    NodeLease,
}

impl HeartbeatKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
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

pub fn record_request(rpc: Rpc, outcome: Outcome) {
    metrics::counter!(REQUESTS_TOTAL, "rpc" => rpc.as_str(), "outcome" => outcome.as_str())
        .increment(1);
}

pub fn record_request_duration(rpc: Rpc, duration: Duration) {
    metrics::histogram!(REQUEST_DURATION_SECONDS, "rpc" => rpc.as_str())
        .record(duration.as_secs_f64());
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

pub fn record_lease_keepalive(pool: &PoolName, message: KeepAliveMessage, outcome: Outcome) {
    metrics::counter!(
        LEASE_KEEPALIVE_TOTAL,
        "pool" => pool.as_str().to_owned(),
        "message" => message.as_str(),
        "outcome" => outcome.as_str()
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

pub fn record_compact(outcome: Outcome) {
    metrics::counter!(COMPACT_TOTAL, "outcome" => outcome.as_str()).increment(1);
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

    #[test]
    fn metric_names_match_plan() {
        let names = [
            SCHEDULED_TOTAL,
            OPERATIONS_TOTAL,
            REQUESTS_TOTAL,
            REQUEST_DURATION_SECONDS,
            REQUEST_BYTES,
            TXN_TOTAL,
            CAS_RETRIES_TOTAL,
            LEASE_KEEPALIVE_TOTAL,
            NODE_HEARTBEATS_TOTAL,
            DROPPED_TOTAL,
            INFLIGHT,
            REVISION,
            COMPACT_TOTAL,
            INFO,
            POD_SIZE_BYTES,
            NODES,
        ];
        assert!(names.iter().all(|name| name.starts_with("etcd_k8s_bench_")));
        assert_eq!(names.len(), 16);
    }

    #[test]
    fn outcome_mapping_is_fixed() {
        assert_eq!(Outcome::Success.as_str(), "success");
        assert_eq!(Outcome::GrpcError.as_str(), "grpc_error");
        assert_eq!(Outcome::Timeout.as_str(), "timeout");
    }

    #[test]
    fn recorder_registers_all_recording_paths() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let resource = ResourceName::new("pods").unwrap();
        let pool = PoolName::new("events").unwrap();
        let workload = WorkloadName::new("density").unwrap();

        // Building a Prometheus recorder is the registration step.  Running
        // every recording path against it verifies all keys and labels are
        // accepted without requiring a process-global recorder.
        metrics::with_local_recorder(&recorder, || {
            record_scheduled(Operation::Create, &resource);
            record_operation(Operation::Create, &resource, Outcome::Success);
            record_request(Rpc::Txn, Outcome::Success);
            record_request_duration(Rpc::Txn, Duration::from_millis(2));
            record_request_bytes(&resource, 4096);
            record_txn(&resource, TxnShape::Create, TxnResult::Committed);
            record_cas_retry(&resource, Operation::Update);
            record_lease_keepalive(&pool, KeepAliveMessage::Request, Outcome::Success);
            record_node_heartbeat(HeartbeatKind::NodeLease, Outcome::Success);
            record_dropped(DropReason::Backpressure);
            add_inflight(Rpc::Txn);
            remove_inflight(Rpc::Txn);
            set_revision(7);
            record_compact(Outcome::Success);
            record_info("0.1.0", &workload);
            set_pod_size(4096);
            set_nodes(2003);
        });
    }
}
