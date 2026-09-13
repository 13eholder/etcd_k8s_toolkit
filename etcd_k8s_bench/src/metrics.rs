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
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use tokio::sync::oneshot;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
}
