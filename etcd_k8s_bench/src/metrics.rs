//! Low-cardinality metrics for the benchmark.
//!
//! This module deliberately only emits fixed metric names.  Labels are either
//! represented by enums below or by validated names supplied by the workload
//! configuration; request keys, IDs, endpoints, and error text never become
//! labels.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;

const RPC_LABELS: [&str; 4] = ["range", "txn", "status", "compact"];

pub const RANGE_PAGES: &str = "etcd_k8s_bench_range_pages";
pub const RANGE_SNAPSHOT_REVISION: &str = "etcd_k8s_bench_range_snapshot_revision";
pub const RANGE_SERVER_COUNT: &str = "etcd_k8s_bench_range_server_count";
pub const RANGE_LAST_KEY_BYTES: &str = "etcd_k8s_bench_range_last_key_bytes";
pub const RANGE_SELECTED: &str = "etcd_k8s_bench_range_selected";
pub const RANGE_DISCARDED: &str = "etcd_k8s_bench_range_discarded";
pub const REVISION: &str = "etcd_k8s_bench_revision";
pub const COMPACT_REVISION: &str = "etcd_k8s_bench_compact_revision";
pub const COMPACTOR_STATE: &str = "etcd_k8s_bench_compactor_state";
pub const COMPACTOR_ERRORS_TOTAL: &str = "etcd_k8s_bench_compactor_errors_total";
pub const COMPACTOR_RECOVERIES_TOTAL: &str = "etcd_k8s_bench_compactor_recoveries_total";

/// Workload names are validated at configuration boundaries even though they are not emitted as
/// metric labels.
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

impl WorkloadName {
    pub fn new(name: impl Into<String>) -> Result<Self, LabelError> {
        validate_name(name.into()).map(Self)
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
    /// A mutable request timed out after being handed to the transport.
    TimeoutUnknown,
    /// The storage retry-attempt budget was exhausted without a timeout.
    RetryExhausted,
}

impl Outcome {
    pub const COUNT: usize = 8;

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::GrpcError => "grpc_error",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::Timeout => "timeout",
            Self::ResultUnknown => "result_unknown",
            Self::TimeoutUnknown => "timeout_unknown",
            Self::RetryExhausted => "retry_exhausted",
        }
    }

    pub const fn is_success(self) -> bool {
        matches!(self, Self::Success)
    }

    pub const fn is_timeout(self) -> bool {
        matches!(self, Self::Timeout | Self::TimeoutUnknown)
    }

    pub const fn is_result_unknown(self) -> bool {
        matches!(self, Self::ResultUnknown | Self::TimeoutUnknown)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    Backpressure,
    QueueClosed,
}

/// Fixed labels used by the per-run online projection. The arrays are
/// intentionally finite so metric handles can be registered once after the
/// exporter recorder is installed and then reused on every hot-path event.
const PHASE_LABELS: [&str; 3] = ["preparation", "measurement", "drain"];
const LOGICAL_LABELS: [(&str, Option<&str>); 18] = [
    ("update", Some("pods")),
    ("update", Some("deployments")),
    ("update", Some("replicasets")),
    ("update", Some("daemonsets")),
    ("point_get", Some("pods")),
    ("bounded_list", Some("pods")),
    ("point_get", Some("nodes")),
    ("bounded_list", Some("nodes")),
    ("point_get", Some("deployments")),
    ("bounded_list", Some("deployments")),
    ("point_get", Some("replicasets")),
    ("bounded_list", Some("replicasets")),
    ("point_get", Some("daemonsets")),
    ("bounded_list", Some("daemonsets")),
    ("node_lease", Some("node_leases")),
    ("node_status", Some("nodes")),
    ("status", None),
    ("compact", None),
];

const PREPARATION_LABELS: [(&str, Option<&str>); 7] = [
    ("create", Some("pods")),
    ("create", Some("nodes")),
    ("create", Some("node_leases")),
    ("create", Some("deployments")),
    ("create", Some("replicasets")),
    ("create", Some("daemonsets")),
    ("node_ready", None),
];

fn logical_labels_for_phase(phase: usize, index: usize) -> (&'static str, Option<&'static str>) {
    if phase == 0 {
        PREPARATION_LABELS
            .get(index)
            .copied()
            .unwrap_or(("unused", None))
    } else {
        LOGICAL_LABELS[index]
    }
}

pub const LOGICAL_STARTED_TOTAL: &str = "etcd_k8s_bench_logical_started_total";
pub const TARGET_UPDATE_RPS: &str = "etcd_k8s_bench_target_update_rps";
pub const LOGICAL_COMPLETED_TOTAL: &str = "etcd_k8s_bench_logical_completed_total";
pub const LOGICAL_ERRORS_TOTAL: &str = "etcd_k8s_bench_logical_errors_total";
pub const LOGICAL_OFFERED_TOTAL: &str = "etcd_k8s_bench_logical_offered_total";
pub const LOGICAL_ACCEPTED_TOTAL: &str = "etcd_k8s_bench_logical_accepted_total";
pub const LOGICAL_DROPPED_FULL_TOTAL: &str = "etcd_k8s_bench_logical_dropped_full_total";
pub const LOGICAL_MISSED_SCHEDULE_TOTAL: &str = "etcd_k8s_bench_logical_missed_schedule_total";
pub const LOGICAL_NOT_OFFERED_AFTER_STOP_TOTAL: &str =
    "etcd_k8s_bench_logical_not_offered_after_stop_total";
pub const LOGICAL_CANCELED_QUEUED_TOTAL: &str = "etcd_k8s_bench_logical_canceled_queued_total";
pub const LOGICAL_CANCELED_RUNNING_TOTAL: &str = "etcd_k8s_bench_logical_canceled_running_total";
pub const LOGICAL_DURATION_SAMPLES_TOTAL: &str = "etcd_k8s_bench_logical_duration_samples_total";
pub const LOGICAL_DURATION_MICROSECONDS_TOTAL: &str =
    "etcd_k8s_bench_logical_duration_microseconds_total";
pub const LOGICAL_DURATION_MAX_MICROSECONDS: &str =
    "etcd_k8s_bench_logical_duration_max_microseconds";
pub const QUEUE_WAIT_SAMPLES_PHASE_TOTAL: &str = "etcd_k8s_bench_queue_wait_samples_phase_total";
pub const QUEUE_WAIT_MICROSECONDS_PHASE_TOTAL: &str =
    "etcd_k8s_bench_queue_wait_microseconds_phase_total";
pub const QUEUE_WAIT_MAX_MICROSECONDS_PHASE: &str =
    "etcd_k8s_bench_queue_wait_max_microseconds_phase";
pub const RPC_STARTED_TOTAL: &str = "etcd_k8s_bench_rpc_started_total";
pub const RPC_COMPLETED_TOTAL: &str = "etcd_k8s_bench_rpc_completed_total";
pub const RPC_REQUEST_BYTES_TOTAL: &str = "etcd_k8s_bench_rpc_request_bytes_total";
pub const RPC_RESPONSE_BYTES_TOTAL: &str = "etcd_k8s_bench_rpc_response_bytes_total";
pub const RPC_DURATION_SAMPLES_PHASE_TOTAL: &str =
    "etcd_k8s_bench_rpc_duration_samples_phase_total";
pub const RPC_DURATION_MICROSECONDS_PHASE_TOTAL: &str =
    "etcd_k8s_bench_rpc_duration_microseconds_phase_total";
pub const RPC_DURATION_MAX_MICROSECONDS_PHASE: &str =
    "etcd_k8s_bench_rpc_duration_max_microseconds_phase";
pub const WATCH_CREATED_TOTAL: &str = "etcd_k8s_bench_watch_created_total";
pub const WATCH_DISCONNECT_TOTAL: &str = "etcd_k8s_bench_watch_disconnect_total";
pub const WATCH_TERMINAL_TOTAL: &str = "etcd_k8s_bench_watch_terminal_total";
pub const WATCH_EVENTS_PHASE_TOTAL: &str = "etcd_k8s_bench_watch_events_phase_total";
pub const WATCH_EVENT_BYTES_PHASE_TOTAL: &str = "etcd_k8s_bench_watch_event_bytes_phase_total";

#[derive(Clone)]
pub struct RunMetrics {
    logical_started: [[::metrics::Counter; 18]; 3],
    logical_completed: [[::metrics::Counter; 18]; 3],
    logical_errors: [[::metrics::Counter; 18]; 3],
    logical_offered: [[::metrics::Counter; 18]; 3],
    logical_accepted: [[::metrics::Counter; 18]; 3],
    logical_dropped_full: [[::metrics::Counter; 18]; 3],
    logical_missed_schedule: [[::metrics::Counter; 18]; 3],
    logical_not_offered_after_stop: [[::metrics::Counter; 18]; 3],
    logical_canceled_queued: [[::metrics::Counter; 18]; 3],
    logical_canceled_running: [[::metrics::Counter; 18]; 3],
    logical_duration_samples: [[::metrics::Counter; 18]; 3],
    logical_duration_micros: [[::metrics::Counter; 18]; 3],
    logical_duration_max_micros: [[::metrics::Gauge; 18]; 3],
    logical_duration_max_values: Arc<[[AtomicU64; 18]; 3]>,
    max_gauge_lock: Arc<Mutex<()>>,
    queue_wait_samples: [[::metrics::Counter; 18]; 3],
    queue_wait_micros: [[::metrics::Counter; 18]; 3],
    queue_wait_max_micros: [[::metrics::Gauge; 18]; 3],
    queue_wait_max_values: Arc<[[AtomicU64; 18]; 3]>,
    rpc_started: [[::metrics::Counter; 4]; 3],
    rpc_completed: [[[::metrics::Counter; Outcome::COUNT]; 4]; 3],
    rpc_request_bytes: [[::metrics::Counter; 4]; 3],
    rpc_response_bytes: [[::metrics::Counter; 4]; 3],
    rpc_duration_samples: [[::metrics::Counter; 4]; 3],
    rpc_duration_micros: [[::metrics::Counter; 4]; 3],
    rpc_duration_max_micros: [[::metrics::Gauge; 4]; 3],
    rpc_duration_max_values: Arc<[[AtomicU64; 4]; 3]>,
    watch_created: [[::metrics::Counter; 1]; 3],
    watch_disconnect: [[::metrics::Counter; 1]; 3],
    watch_terminal: [[::metrics::Counter; 1]; 3],
    watch_events: [[::metrics::Counter; 1]; 3],
    watch_event_bytes: [[::metrics::Counter; 1]; 3],
    revision: ::metrics::Gauge,
    compact_revision: ::metrics::Gauge,
    compactor_state: ::metrics::Gauge,
    compactor_errors: ::metrics::Counter,
    compactor_recoveries: ::metrics::Counter,
    range_pages: ::metrics::Gauge,
    range_snapshot_revision: ::metrics::Gauge,
    range_server_count: ::metrics::Gauge,
    range_last_key_bytes: ::metrics::Gauge,
    range_selected: ::metrics::Gauge,
    range_discarded: ::metrics::Gauge,
    revision_max: Arc<AtomicI64>,
    compact_revision_max: Arc<AtomicI64>,
}

impl RunMetrics {
    /// Register all bounded phase/method/stream handles against the currently
    /// installed metrics recorder. This must be called after exporter setup;
    /// constructing handles before that would bind them to the no-op recorder.
    pub fn new() -> Self {
        let logical_counter_handles = |name: &'static str| {
            std::array::from_fn(|phase| {
                std::array::from_fn(|index| {
                    let (work_class, resource) = logical_labels_for_phase(phase, index);
                    ::metrics::counter!(
                        name,
                        "phase" => PHASE_LABELS[phase],
                        "work_class" => work_class,
                        "resource" => resource.unwrap_or("none")
                    )
                })
            })
        };
        let logical_gauge_handles = |name: &'static str| {
            std::array::from_fn(|phase| {
                std::array::from_fn(|index| {
                    let (work_class, resource) = logical_labels_for_phase(phase, index);
                    ::metrics::gauge!(
                        name,
                        "phase" => PHASE_LABELS[phase],
                        "work_class" => work_class,
                        "resource" => resource.unwrap_or("none")
                    )
                })
            })
        };
        let rpc_counter_handles = |name: &'static str| {
            std::array::from_fn(|phase| {
                std::array::from_fn(|method| {
                    ::metrics::counter!(
                        name,
                        "phase" => PHASE_LABELS[phase],
                        "rpc" => RPC_LABELS[method]
                    )
                })
            })
        };
        let rpc_gauge_handles = |name: &'static str| {
            std::array::from_fn(|phase| {
                std::array::from_fn(|method| {
                    ::metrics::gauge!(
                        name,
                        "phase" => PHASE_LABELS[phase],
                        "rpc" => RPC_LABELS[method]
                    )
                })
            })
        };
        let rpc_outcome_handles = || {
            std::array::from_fn(|phase| {
                std::array::from_fn(|method| {
                    std::array::from_fn(|outcome| {
                        ::metrics::counter!(
                            RPC_COMPLETED_TOTAL,
                            "phase" => PHASE_LABELS[phase],
                            "rpc" => RPC_LABELS[method],
                            "outcome" => outcome_label(outcome)
                        )
                    })
                })
            })
        };
        let phase_single = |name: &'static str| {
            std::array::from_fn(|phase| [::metrics::counter!(name, "phase" => PHASE_LABELS[phase])])
        };
        let logical_duration_max_values: Arc<[[AtomicU64; 18]; 3]> =
            Arc::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }));
        let queue_wait_max_values: Arc<[[AtomicU64; 18]; 3]> =
            Arc::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }));
        let rpc_duration_max_values: Arc<[[AtomicU64; 4]; 3]> =
            Arc::new(std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }));
        let max_gauge_lock = Arc::new(Mutex::new(()));
        Self {
            logical_started: logical_counter_handles(LOGICAL_STARTED_TOTAL),
            logical_completed: logical_counter_handles(LOGICAL_COMPLETED_TOTAL),
            logical_errors: logical_counter_handles(LOGICAL_ERRORS_TOTAL),
            logical_offered: logical_counter_handles(LOGICAL_OFFERED_TOTAL),
            logical_accepted: logical_counter_handles(LOGICAL_ACCEPTED_TOTAL),
            logical_dropped_full: logical_counter_handles(LOGICAL_DROPPED_FULL_TOTAL),
            logical_missed_schedule: logical_counter_handles(LOGICAL_MISSED_SCHEDULE_TOTAL),
            logical_not_offered_after_stop: logical_counter_handles(
                LOGICAL_NOT_OFFERED_AFTER_STOP_TOTAL,
            ),
            logical_canceled_queued: logical_counter_handles(LOGICAL_CANCELED_QUEUED_TOTAL),
            logical_canceled_running: logical_counter_handles(LOGICAL_CANCELED_RUNNING_TOTAL),
            logical_duration_samples: logical_counter_handles(LOGICAL_DURATION_SAMPLES_TOTAL),
            logical_duration_micros: logical_counter_handles(LOGICAL_DURATION_MICROSECONDS_TOTAL),
            logical_duration_max_micros: logical_gauge_handles(LOGICAL_DURATION_MAX_MICROSECONDS),
            logical_duration_max_values,
            max_gauge_lock,
            queue_wait_samples: logical_counter_handles(QUEUE_WAIT_SAMPLES_PHASE_TOTAL),
            queue_wait_micros: logical_counter_handles(QUEUE_WAIT_MICROSECONDS_PHASE_TOTAL),
            queue_wait_max_micros: logical_gauge_handles(QUEUE_WAIT_MAX_MICROSECONDS_PHASE),
            queue_wait_max_values,
            rpc_started: rpc_counter_handles(RPC_STARTED_TOTAL),
            rpc_completed: rpc_outcome_handles(),
            rpc_request_bytes: rpc_counter_handles(RPC_REQUEST_BYTES_TOTAL),
            rpc_response_bytes: rpc_counter_handles(RPC_RESPONSE_BYTES_TOTAL),
            rpc_duration_samples: rpc_counter_handles(RPC_DURATION_SAMPLES_PHASE_TOTAL),
            rpc_duration_micros: rpc_counter_handles(RPC_DURATION_MICROSECONDS_PHASE_TOTAL),
            rpc_duration_max_micros: rpc_gauge_handles(RPC_DURATION_MAX_MICROSECONDS_PHASE),
            rpc_duration_max_values,
            watch_created: phase_single(WATCH_CREATED_TOTAL),
            watch_disconnect: phase_single(WATCH_DISCONNECT_TOTAL),
            watch_terminal: phase_single(WATCH_TERMINAL_TOTAL),
            watch_events: phase_single(WATCH_EVENTS_PHASE_TOTAL),
            watch_event_bytes: phase_single(WATCH_EVENT_BYTES_PHASE_TOTAL),
            revision: ::metrics::gauge!(REVISION),
            compact_revision: ::metrics::gauge!(COMPACT_REVISION),
            compactor_state: ::metrics::gauge!(COMPACTOR_STATE),
            compactor_errors: ::metrics::counter!(COMPACTOR_ERRORS_TOTAL),
            compactor_recoveries: ::metrics::counter!(COMPACTOR_RECOVERIES_TOTAL),
            range_pages: ::metrics::gauge!(RANGE_PAGES),
            range_snapshot_revision: ::metrics::gauge!(RANGE_SNAPSHOT_REVISION),
            range_server_count: ::metrics::gauge!(RANGE_SERVER_COUNT),
            range_last_key_bytes: ::metrics::gauge!(RANGE_LAST_KEY_BYTES),
            range_selected: ::metrics::gauge!(RANGE_SELECTED),
            range_discarded: ::metrics::gauge!(RANGE_DISCARDED),
            revision_max: Arc::new(AtomicI64::new(0)),
            compact_revision_max: Arc::new(AtomicI64::new(0)),
        }
    }

    fn phase_index(phase: &str) -> Option<usize> {
        PHASE_LABELS.iter().position(|value| *value == phase)
    }

    fn record_logical_count(
        counters: &[[::metrics::Counter; 18]; 3],
        phase: &str,
        stream: usize,
        count: u64,
    ) {
        if let Some(phase) = Self::phase_index(phase) {
            if let Some(counter) = counters[phase].get(stream) {
                counter.increment(count);
            }
        }
    }

    pub fn record_logical_offered(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_offered, phase, stream, count);
    }

    pub fn record_logical_accepted(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_accepted, phase, stream, count);
    }

    pub fn record_logical_dropped_full(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_dropped_full, phase, stream, count);
    }

    pub fn record_logical_missed_schedule(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_missed_schedule, phase, stream, count);
    }

    pub fn record_logical_not_offered_after_stop(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_not_offered_after_stop, phase, stream, count);
    }

    pub fn record_logical_canceled_queued(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_canceled_queued, phase, stream, count);
    }

    pub fn record_logical_canceled_running(&self, phase: &str, stream: usize, count: u64) {
        Self::record_logical_count(&self.logical_canceled_running, phase, stream, count);
    }

    pub fn record_logical_started(&self, phase: &str, stream: usize) {
        if let Some(phase) = Self::phase_index(phase) {
            if let Some(counter) = self.logical_started[phase].get(stream) {
                counter.increment(1);
            }
        }
    }

    pub fn record_logical_completed(
        &self,
        phase: &str,
        stream: usize,
        outcome: Outcome,
        duration: Duration,
    ) {
        let Some(phase) = Self::phase_index(phase) else {
            return;
        };
        let Some(completed) = self.logical_completed[phase].get(stream) else {
            return;
        };
        completed.increment(1);
        if !outcome.is_success() {
            self.logical_errors[phase][stream].increment(1);
        }
        let micros = duration_micros(duration);
        self.logical_duration_samples[phase][stream].increment(1);
        self.logical_duration_micros[phase][stream].increment(micros);
        update_max(
            &self.logical_duration_max_values[phase][stream],
            &self.logical_duration_max_micros[phase][stream],
            micros,
            &self.max_gauge_lock,
        );
    }

    pub fn record_queue_wait(&self, phase: &str, stream: usize, duration: Duration) {
        let Some(phase) = Self::phase_index(phase) else {
            return;
        };
        if self.queue_wait_samples[phase].get(stream).is_none() {
            return;
        }
        let micros = duration_micros(duration);
        self.queue_wait_samples[phase][stream].increment(1);
        self.queue_wait_micros[phase][stream].increment(micros);
        update_max(
            &self.queue_wait_max_values[phase][stream],
            &self.queue_wait_max_micros[phase][stream],
            micros,
            &self.max_gauge_lock,
        );
    }

    pub fn record_rpc_started(&self, phase: &str, method: usize, request_bytes: u64) {
        let Some(phase) = Self::phase_index(phase) else {
            return;
        };
        let Some(started) = self.rpc_started[phase].get(method) else {
            return;
        };
        let Some(request_bytes_total) = self.rpc_request_bytes[phase].get(method) else {
            return;
        };
        started.increment(1);
        request_bytes_total.increment(request_bytes);
    }

    pub fn record_rpc_completed(
        &self,
        phase: &str,
        method: usize,
        outcome: Outcome,
        response_bytes: u64,
        duration: Duration,
    ) {
        let Some(phase) = Self::phase_index(phase) else {
            return;
        };
        let Some(completed) = self.rpc_completed[phase].get(method) else {
            return;
        };
        let Some(response_bytes_total) = self.rpc_response_bytes[phase].get(method) else {
            return;
        };
        let Some(duration_samples) = self.rpc_duration_samples[phase].get(method) else {
            return;
        };
        let Some(duration_micros_total) = self.rpc_duration_micros[phase].get(method) else {
            return;
        };
        let Some(duration_max_value) = self.rpc_duration_max_values[phase].get(method) else {
            return;
        };
        let Some(duration_max_gauge) = self.rpc_duration_max_micros[phase].get(method) else {
            return;
        };
        completed[outcome.index()].increment(1);
        response_bytes_total.increment(response_bytes);
        let micros = duration_micros(duration);
        duration_samples.increment(1);
        duration_micros_total.increment(micros);
        update_max(
            duration_max_value,
            duration_max_gauge,
            micros,
            &self.max_gauge_lock,
        );
    }

    pub fn record_watch_created(&self, phase: &str) {
        if let Some(phase) = Self::phase_index(phase) {
            self.watch_created[phase][0].increment(1);
        }
    }

    pub fn record_watch_disconnect(&self, phase: &str) {
        if let Some(phase) = Self::phase_index(phase) {
            self.watch_disconnect[phase][0].increment(1);
        }
    }

    pub fn record_watch_terminal(&self, phase: &str) {
        if let Some(phase) = Self::phase_index(phase) {
            self.watch_terminal[phase][0].increment(1);
        }
    }

    pub fn record_watch_events(&self, phase: &str, events: u64, bytes: u64) {
        if let Some(phase) = Self::phase_index(phase) {
            self.watch_events[phase][0].increment(events);
            self.watch_event_bytes[phase][0].increment(bytes);
        }
    }

    pub fn set_revision(&self, revision: i64) {
        let maximum = self
            .revision_max
            .fetch_max(revision, Ordering::Relaxed)
            .max(revision);
        self.revision.set(maximum as f64);
    }

    pub fn set_compact_revision(&self, revision: i64) {
        let maximum = self
            .compact_revision_max
            .fetch_max(revision, Ordering::Relaxed)
            .max(revision);
        self.compact_revision.set(maximum as f64);
    }

    pub fn set_compactor_state(&self, state: &'static str) {
        self.compactor_state
            .set(if state == "active" { 0.0 } else { 1.0 });
    }

    pub fn record_compactor_error(&self) {
        self.compactor_errors.increment(1);
    }

    pub fn record_compactor_recovery(&self) {
        self.compactor_recoveries.increment(1);
    }

    pub fn record_range_result(
        &self,
        pages: u64,
        snapshot_revision: i64,
        server_count: i64,
        last_key_bytes: u64,
        selected: u64,
        discarded: u64,
    ) {
        self.range_pages.set(pages as f64);
        self.range_snapshot_revision.set(snapshot_revision as f64);
        self.range_server_count.set(server_count as f64);
        self.range_last_key_bytes.set(last_key_bytes as f64);
        self.range_selected.set(selected as f64);
        self.range_discarded.set(discarded as f64);
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn update_max(max_value: &AtomicU64, gauge: &::metrics::Gauge, value: u64, gauge_lock: &Mutex<()>) {
    let mut current = max_value.load(Ordering::Relaxed);
    while value > current {
        match max_value.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                let _guard = gauge_lock.lock().expect("max gauge lock is not poisoned");
                gauge.set(max_value.load(Ordering::Relaxed) as f64);
                return;
            }
            Err(observed) => current = observed,
        }
    }
}

impl Default for RunMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Outcome {
    const fn index(self) -> usize {
        match self {
            Self::Success => 0,
            Self::GrpcError => 1,
            Self::Conflict => 2,
            Self::NotFound => 3,
            Self::Timeout => 4,
            Self::ResultUnknown => 5,
            Self::TimeoutUnknown => 6,
            Self::RetryExhausted => 7,
        }
    }
}

const fn outcome_label(index: usize) -> &'static str {
    match index {
        0 => "success",
        1 => "grpc_error",
        2 => "conflict",
        3 => "not_found",
        4 => "timeout",
        5 => "result_unknown",
        6 => "timeout_unknown",
        _ => "retry_exhausted",
    }
}

/// Own the exporter listener for the lifetime of a run.
///
/// The recorder is built without the exporter's high-level HTTP future.  The
/// listener below is deliberately owned by this guard so its connections are
/// joined before the run exits.
pub struct ExporterGuard {
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl ExporterGuard {
    pub async fn shutdown(mut self) -> Result<(), String> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(format!("metrics exporter task failed: {error}")),
        }
    }
}

impl Drop for ExporterGuard {
    fn drop(&mut self) {
        let _ = self.shutdown.take().map(|shutdown| shutdown.send(()));
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Build and install the recorder, then serve its rendered text through the
/// benchmark-owned HTTP/1 listener.
pub fn install_exporter(listen: SocketAddr) -> Result<ExporterGuard, String> {
    let listener = std::net::TcpListener::bind(listen)
        .map_err(|error| format!("metrics listener bind {listen}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("metrics listener setup {listen}: {error}"))?;
    let listener = TcpListener::from_std(listener)
        .map_err(|error| format!("metrics listener setup {listen}: {error}"))?;

    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    ::metrics::set_global_recorder(recorder).map_err(|error| error.to_string())?;

    let (shutdown, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(serve_metrics_listener(listener, handle, shutdown_rx));
    Ok(ExporterGuard {
        task: Some(task),
        shutdown: Some(shutdown),
    })
}

async fn serve_metrics_listener(
    listener: TcpListener,
    handle: PrometheusHandle,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<(), String> {
    let mut connections = JoinSet::new();
    let run_result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let connection_handle = handle.clone();
                        connections.spawn(async move {
                            serve_metrics_connection(stream, connection_handle).await
                        });
                    }
                    Err(error) => break Err(format!("metrics listener accept: {error}")),
                }
            }
            result = connections.join_next(), if !connections.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {}
                    // A client closing a scrape connection is ordinary and
                    // must not turn a run into a failed experiment.
                    // Hyper reports malformed requests and peer I/O
                    // disconnects as connection errors. They are scrape
                    // client failures, not pressure-experiment failures.
                    Some(Ok(Err(_))) => {}
                    Some(Err(error)) if error.is_cancelled() => {}
                    Some(Err(error)) => {
                        break Err(format!("metrics connection task failed: {error}"));
                    }
                    None => {}
                }
        }
        }
    };
    drop(listener);
    let cleanup_result = abort_metric_connections(&mut connections).await;
    match (run_result, cleanup_result) {
        (Err(primary), _) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn abort_metric_connections(
    connections: &mut JoinSet<Result<(), hyper::Error>>,
) -> Result<(), String> {
    connections.abort_all();
    let mut first_error = None;
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() && first_error.is_none() {
                first_error = Some(format!("metrics connection task failed: {error}"));
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn serve_metrics_connection(
    stream: TcpStream,
    handle: PrometheusHandle,
) -> Result<(), hyper::Error> {
    let io = TokioIo::new(stream);
    let service =
        service_fn(move |request: Request<Incoming>| metrics_response(request, handle.clone()));
    http1::Builder::new().serve_connection(io, service).await
}

async fn metrics_response(
    request: Request<Incoming>,
    handle: PrometheusHandle,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let (status, body) =
        if request.method() == Method::GET && matches!(request.uri().path(), "/" | "/metrics") {
            (StatusCode::OK, handle.render())
        } else {
            (
                StatusCode::NOT_FOUND,
                "not found; scrape /metrics\n".to_owned(),
            )
        };
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    Ok(response)
}

#[cfg(test)]
#[path = "../tests/unit/metrics.rs"]
mod tests;
