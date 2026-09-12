use serde::Serialize;

/// A scalar view of one bounded HDR histogram.  The recorder never exposes or
/// clones its backing histogram; this view is materialized only at report time.
#[derive(Clone, Debug, Default, Serialize)]
pub struct HistogramView {
    pub low_us: u64,
    pub high_us: u64,
    pub significant_digits: u8,
    pub auto_resize: bool,
    pub coordinated_omission: bool,
    pub sample_count: u64,
    pub p50_us: Option<u64>,
    pub p90_us: Option<u64>,
    pub p99_us: Option<u64>,
    pub max_us: Option<u64>,
    pub overflow: u64,
    pub exact_max_us: Option<u64>,
    /// True when overflow means HDR quantiles cannot be represented reliably.
    pub quantiles_unavailable: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LogicalStreamReport {
    pub work_class: String,
    pub resource: Option<String>,
    pub planned: u64,
    pub offered: u64,
    pub accepted: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub missed_schedule: u64,
    pub not_offered_after_stop: u64,
    pub started_before_cutoff: u64,
    pub started_after_cutoff: u64,
    pub succeeded_before_cutoff: u64,
    pub failed_before_cutoff: u64,
    pub succeeded_after_cutoff: u64,
    pub failed_after_cutoff: u64,
    pub timeout_before_cutoff: u64,
    pub timeout_after_cutoff: u64,
    pub result_unknown_before_cutoff: u64,
    pub result_unknown_after_cutoff: u64,
    pub terminal_after_cutoff: u64,
    pub canceled_queued: u64,
    pub canceled_running: u64,
    pub cutoff_queued: u64,
    pub cutoff_running: u64,
    pub first_error_code: u64,
    pub first_error: Option<String>,
    pub logical_latency: HistogramView,
    pub queue_latency: HistogramView,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct QueueStreamReport {
    pub work_class: String,
    pub resource: Option<String>,
    pub sample_count: u64,
    pub latency: HistogramView,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RpcReport {
    pub method: String,
    pub started: u64,
    pub completed: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub timeout: u64,
    pub result_unknown: u64,
    pub payload_bytes: u64,
    pub latency: HistogramView,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PhaseReport {
    pub phase: String,
    pub logical: Vec<LogicalStreamReport>,
    pub queue: Vec<QueueStreamReport>,
    pub rpc: Vec<RpcReport>,
    pub logical_total: HistogramView,
    pub queue_total: HistogramView,
    pub rpc_total: HistogramView,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct BucketReport {
    pub start_ns: u64,
    pub end_ns: u64,
    pub logical_offered: u64,
    pub logical_started: u64,
    pub logical_completed: u64,
    pub logical_errors: u64,
    pub rpc_started: u64,
    pub rpc_completed: u64,
    pub watch_events: u64,
    pub watch_payload_bytes: u64,
    pub logical_latency: HistogramView,
    pub queue_latency: HistogramView,
    pub rpc_latency: HistogramView,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CutoffReport {
    pub configured_ns: Option<u64>,
    pub effective_ns: Option<u64>,
    pub queued: u64,
    pub running: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct TimingReport {
    pub setup_ns: Option<u64>,
    pub preparation_ns: Option<u64>,
    pub configured_measurement_ns: Option<u64>,
    pub observed_measurement_ns: Option<u64>,
    pub drain_ns: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MeasurementAccounting {
    pub planned_conserved: bool,
    pub offered_conserved: bool,
    pub accepted_cohort_conserved: bool,
    pub result_unknown_subset_failed: bool,
    pub no_active_tokens: bool,
    pub no_arithmetic_overflow: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HistogramMetadata {
    pub low_us: u64,
    pub significant_digits: u8,
    pub auto_resize: bool,
    pub coordinated_omission: bool,
    pub overflow_is_not_truncated: bool,
    pub duration_rounding: String,
}

/// Bounded per-run measurement output.  It contains preparation, measurement
/// and drain views while retaining no per-request history or HDR clone.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MeasurementReport {
    pub preparation: PhaseReport,
    pub measurement: PhaseReport,
    pub drain: PhaseReport,
    pub buckets: Vec<BucketReport>,
    pub cutoff: CutoffReport,
    pub timing: TimingReport,
    pub accounting: MeasurementAccounting,
    pub metadata: HistogramMetadata,
}
