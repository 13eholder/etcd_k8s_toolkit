use std::io::Write;

use serde::Serialize;

use crate::config::{SyntheticConfig, SyntheticResourceCounts};
use crate::plan::{
    CompiledPlan, HistogramPlan, LogicalWorkClass, PeriodicStreamPlan, ResourceKind, ResourcePlan,
};

/// A scalar view of one bounded HDR histogram. The recorder never exposes or
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
    /// Sum of protobuf request bytes at start and response bytes at completion
    /// in this phase. It excludes gRPC framing, headers and status metadata.
    pub payload_bytes: u64,
    pub request_payload_bytes: u64,
    pub response_payload_bytes: u64,
    pub started_per_second: Option<f64>,
    pub completed_per_second: Option<f64>,
    pub latency: HistogramView,
    /// Range-specific aggregate fields. They remain zero for other methods.
    pub range_pages: u64,
    pub range_snapshot_revision: i64,
    pub range_server_count: i64,
    pub range_last_key_bytes: u64,
    pub range_selected: u64,
    pub range_discarded: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PreparationResourceReport {
    pub resource: String,
    pub created: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PhaseReport {
    pub phase: String,
    pub logical: Vec<LogicalStreamReport>,
    pub queue: Vec<QueueStreamReport>,
    pub rpc: Vec<RpcReport>,
    pub resources: Vec<PreparationResourceReport>,
    pub logical_total: HistogramView,
    pub queue_total: HistogramView,
    pub rpc_total: HistogramView,
    pub timed_out: bool,
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
    pub txn_started: u64,
    pub txn_completed: u64,
    pub txn_started_per_second: Option<f64>,
    pub txn_completed_per_second: Option<f64>,
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
    pub watch_teardown_ns: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MeasurementAccounting {
    pub planned_conserved: bool,
    pub offered_conserved: bool,
    pub accepted_cohort_conserved: bool,
    pub result_unknown_subset_failed: bool,
    pub no_active_tokens: bool,
    pub no_arithmetic_overflow: bool,
    pub measurement_complete: bool,
    pub observation_complete: bool,
    pub histogram_overflow: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct HistogramMetadata {
    pub low_us: u64,
    pub significant_digits: u8,
    pub auto_resize: bool,
    pub coordinated_omission: bool,
    pub overflow_is_not_truncated: bool,
    pub duration_rounding: String,
    pub payload_definition: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyReport {
    pub logical_total: HistogramView,
    pub queue_total: HistogramView,
    pub rpc_total: HistogramView,
    pub metadata: HistogramMetadata,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WatchStreamReport {
    pub root: String,
    pub stream: String,
    pub ready: bool,
    pub observable_duration_ms: f64,
    pub disconnects: u64,
    pub terminal: Option<String>,
    pub events: u64,
    pub payload_bytes: u64,
    pub observation_complete: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WatchRootReport {
    pub root: String,
    pub expected_streams: u64,
    pub ready_streams: u64,
    pub observable_stream_ms: f64,
    pub observed_measurement_ms: f64,
    pub coverage_ratio: Option<f64>,
    pub observation_complete: bool,
    pub streams: Vec<WatchStreamReport>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WatchReport {
    pub roots: Vec<WatchRootReport>,
    pub observation_complete: bool,
}

/// Bounded per-run measurement output. It is an internal snapshot consumed by
/// the final report view and retains no per-request history or HDR clone.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MeasurementReport {
    pub preparation: PhaseReport,
    pub measurement: PhaseReport,
    pub drain: PhaseReport,
    pub buckets: Vec<BucketReport>,
    pub cutoff: CutoffReport,
    pub timing: TimingReport,
    pub accounting: MeasurementAccounting,
    pub latency: LatencyReport,
    pub watch: WatchReport,
    pub drain_watch: WatchReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExperimentReport {
    pub status: RunStatus,
    pub failure_phase: Option<String>,
    pub first_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedConfigReport {
    #[serde(flatten)]
    pub config: SyntheticConfig,
    pub endpoint: String,
    pub max_message_size: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct PlanReport {
    pub resource_counts: SyntheticResourceCounts,
    pub resources: Vec<ResourcePlan>,
    pub logical_streams: Vec<crate::plan::LogicalStreamPlan>,
    pub periodic_plan: Vec<PeriodicStreamPlan>,
    pub pacing_tick_ms: u64,
    pub update_rounding_deficit: u64,
    pub read_rounding_deficit: u64,
    pub estimated_measurement_memory_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct FinalTimingReport {
    pub setup_ms: Option<f64>,
    pub prepare_ms: Option<f64>,
    pub configured_measurement_ms: u64,
    pub observed_measurement_ms: Option<f64>,
    pub drain_ms: Option<f64>,
    pub watch_teardown_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MeasurementSection {
    pub logical: Vec<LogicalStreamReport>,
    pub queue: Vec<QueueStreamReport>,
    pub rpc: Vec<RpcReport>,
    pub txn_started_per_second: Option<f64>,
    pub txn_completed_per_second: Option<f64>,
    pub logical_total: HistogramView,
    pub queue_total: HistogramView,
    pub rpc_total: HistogramView,
    pub latency: LatencyReport,
    pub buckets: Vec<BucketReport>,
    pub cutoff: CutoffReport,
    pub watch: WatchReport,
}

#[derive(Clone, Debug, Serialize)]
pub struct DrainSection {
    pub logical: Vec<LogicalStreamReport>,
    pub queue: Vec<QueueStreamReport>,
    pub rpc: Vec<RpcReport>,
    pub logical_total: HistogramView,
    pub queue_total: HistogramView,
    pub rpc_total: HistogramView,
    pub watch: WatchReport,
    pub timed_out: bool,
}

/// The only serialized run protocol. Its nine fields intentionally contain no
/// compatibility `status/error/measurement` wrapper or `achieved` alias.
#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    pub schema_version: u8,
    pub experiment: ExperimentReport,
    pub resolved_config: ResolvedConfigReport,
    pub plan: PlanReport,
    pub timing: FinalTimingReport,
    pub preparation: PhaseReport,
    pub measurement: MeasurementSection,
    pub drain: DrainSection,
    pub accounting: MeasurementAccounting,
}

impl RunReport {
    pub fn status(&self) -> RunStatus {
        self.experiment.status
    }

    /// Preserve an exporter lifecycle error in the final report without
    /// replacing a prior execution error or user-interruption status.
    pub fn setup_failure(
        config: &crate::config::RunConfig,
        status: RunStatus,
        phase: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        let setup_ns =
            u64::try_from(config.setup_started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Self::from_measurement(
            config,
            status,
            Some(phase.into()),
            Some(error.into()),
            empty_measurement(&config.plan, Some(setup_ns)),
        )
    }

    pub fn record_exporter_failure(&mut self, error: impl Into<String>) {
        let error = bound_error(format!("metrics exporter: {}", error.into()));
        if self.experiment.first_error.is_none() {
            self.experiment.first_error = Some(error);
        }
        if self.experiment.status == RunStatus::Completed {
            self.experiment.status = RunStatus::Failed;
            self.experiment.failure_phase = Some("exporter".into());
        }
    }

    pub fn from_measurement(
        config: &crate::config::RunConfig,
        status: RunStatus,
        failure_phase: Option<String>,
        first_error: Option<String>,
        measurement: MeasurementReport,
    ) -> Self {
        let configured_measurement_ms = config.plan.config.load.duration_ms;
        let measurement_phase = measurement.measurement.clone();
        let mut drain_phase = measurement.drain.clone();
        drain_phase.timed_out = drain_phase.timed_out || measurement.drain.timed_out;
        let timing = FinalTimingReport {
            setup_ms: measurement.timing.setup_ns.map(nanos_to_millis),
            prepare_ms: measurement
                .timing
                .preparation_ns
                .zip(measurement.timing.setup_ns)
                .map(|(end, setup)| nanos_to_millis(end.saturating_sub(setup))),
            configured_measurement_ms,
            observed_measurement_ms: measurement
                .timing
                .observed_measurement_ns
                .map(nanos_to_millis),
            drain_ms: measurement.timing.drain_ns.map(nanos_to_millis),
            watch_teardown_ms: measurement.timing.watch_teardown_ns.map(nanos_to_millis),
        };
        let txn_rates = measurement_phase
            .rpc
            .iter()
            .find(|rpc| rpc.method == "txn")
            .map(|rpc| (rpc.started_per_second, rpc.completed_per_second))
            .unwrap_or((None, None));
        Self {
            schema_version: 1,
            experiment: ExperimentReport {
                status,
                failure_phase,
                first_error: first_error.map(bound_error),
            },
            resolved_config: ResolvedConfigReport {
                config: config.plan.config.clone(),
                endpoint: config.endpoint.clone(),
                max_message_size: config.max_message_size,
            },
            plan: PlanReport {
                resource_counts: config.plan.resource_counts.clone(),
                resources: config.plan.resources.clone(),
                logical_streams: config.plan.logical_streams.clone(),
                periodic_plan: config.plan.periodic_plan.clone(),
                pacing_tick_ms: config.plan.pacing_tick_ms,
                update_rounding_deficit: config.plan.update_rounding_deficit,
                read_rounding_deficit: config.plan.read_rounding_deficit,
                estimated_measurement_memory_bytes: config
                    .plan
                    .measurement_layout
                    .estimated_measurement_memory_bytes,
            },
            timing,
            preparation: measurement.preparation,
            measurement: MeasurementSection {
                logical: measurement_phase.logical,
                queue: measurement_phase.queue,
                rpc: measurement_phase.rpc,
                txn_started_per_second: txn_rates.0,
                txn_completed_per_second: txn_rates.1,
                logical_total: measurement_phase.logical_total,
                queue_total: measurement_phase.queue_total,
                rpc_total: measurement_phase.rpc_total,
                latency: measurement.latency,
                buckets: measurement.buckets,
                cutoff: measurement.cutoff,
                watch: measurement.watch,
            },
            drain: DrainSection {
                logical: drain_phase.logical,
                queue: drain_phase.queue,
                rpc: drain_phase.rpc,
                logical_total: drain_phase.logical_total,
                queue_total: drain_phase.queue_total,
                rpc_total: drain_phase.rpc_total,
                watch: measurement.drain_watch,
                timed_out: drain_phase.timed_out,
            },
            accounting: measurement.accounting,
        }
    }

    /// Serialize directly to a writer so a large result is never assembled as
    /// one intermediate JSON string.
    pub fn write_json<W: Write>(&self, writer: W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, self)
    }
}

fn bound_error(error: String) -> String {
    error.chars().take(256).collect()
}

fn nanos_to_millis(value: u64) -> f64 {
    value as f64 / 1_000_000.0
}

fn logical_labels(index: usize) -> (&'static str, Option<&'static str>) {
    match index {
        0 => ("update", Some("pods")),
        1 => ("update", Some("deployments")),
        2 => ("update", Some("replicasets")),
        3 => ("update", Some("daemonsets")),
        4 => ("point_get", Some("pods")),
        5 => ("bounded_list", Some("pods")),
        6 => ("point_get", Some("nodes")),
        7 => ("bounded_list", Some("nodes")),
        8 => ("point_get", Some("deployments")),
        9 => ("bounded_list", Some("deployments")),
        10 => ("point_get", Some("replicasets")),
        11 => ("bounded_list", Some("replicasets")),
        12 => ("point_get", Some("daemonsets")),
        13 => ("bounded_list", Some("daemonsets")),
        14 => ("node_lease", Some("node_leases")),
        15 => ("node_status", Some("nodes")),
        16 => ("status", None),
        17 => ("compact", None),
        _ => ("unknown", None),
    }
}
fn empty_preparation_resources() -> Vec<PreparationResourceReport> {
    [
        "pods",
        "nodes",
        "node_leases",
        "deployments",
        "replicasets",
        "daemonsets",
    ]
    .into_iter()
    .map(|resource| PreparationResourceReport {
        resource: resource.into(),
        ..Default::default()
    })
    .collect()
}

fn measurement_stream_index(work_class: LogicalWorkClass, resource: ResourceKind) -> Option<usize> {
    match (work_class, resource) {
        (LogicalWorkClass::Update, ResourceKind::Pods) => Some(0),
        (LogicalWorkClass::Update, ResourceKind::Deployments) => Some(1),
        (LogicalWorkClass::Update, ResourceKind::ReplicaSets) => Some(2),
        (LogicalWorkClass::Update, ResourceKind::DaemonSets) => Some(3),
        (LogicalWorkClass::PointGet, ResourceKind::Pods) => Some(4),
        (LogicalWorkClass::PointGet, ResourceKind::Nodes) => Some(6),
        (LogicalWorkClass::PointGet, ResourceKind::Deployments) => Some(8),
        (LogicalWorkClass::PointGet, ResourceKind::ReplicaSets) => Some(10),
        (LogicalWorkClass::PointGet, ResourceKind::DaemonSets) => Some(12),
        (LogicalWorkClass::BoundedList, ResourceKind::Pods) => Some(5),
        (LogicalWorkClass::BoundedList, ResourceKind::Nodes) => Some(7),
        (LogicalWorkClass::BoundedList, ResourceKind::Deployments) => Some(9),
        (LogicalWorkClass::BoundedList, ResourceKind::ReplicaSets) => Some(11),
        (LogicalWorkClass::BoundedList, ResourceKind::DaemonSets) => Some(13),
        _ => None,
    }
}

fn empty_histogram(plan: HistogramPlan) -> HistogramView {
    HistogramView {
        low_us: plan.low_us,
        high_us: plan.high_us,
        significant_digits: 2,
        ..Default::default()
    }
}

fn empty_phase(
    phase: &str,
    logical_count: usize,
    rpc_count: usize,
    logical_histogram: HistogramPlan,
    queue_histogram: HistogramPlan,
    rpc_histogram: HistogramPlan,
) -> PhaseReport {
    let logical = (0..logical_count)
        .map(|index| {
            let (work_class, resource) = if phase == "preparation" {
                match index {
                    0 => ("create", Some("pods")),
                    1 => ("create", Some("nodes")),
                    2 => ("create", Some("node_leases")),
                    3 => ("create", Some("deployments")),
                    4 => ("create", Some("replicasets")),
                    5 => ("create", Some("daemonsets")),
                    _ => ("node_ready", None),
                }
            } else {
                logical_labels(index)
            };
            LogicalStreamReport {
                work_class: work_class.into(),
                resource: resource.map(str::to_owned),
                logical_latency: empty_histogram(logical_histogram),
                queue_latency: empty_histogram(queue_histogram),
                ..Default::default()
            }
        })
        .collect();
    let queue = (0..logical_count)
        .map(|index| {
            let (work_class, resource) = if phase == "preparation" {
                match index {
                    0 => ("create", Some("pods")),
                    1 => ("create", Some("nodes")),
                    2 => ("create", Some("node_leases")),
                    3 => ("create", Some("deployments")),
                    4 => ("create", Some("replicasets")),
                    5 => ("create", Some("daemonsets")),
                    _ => ("node_ready", None),
                }
            } else {
                logical_labels(index)
            };
            QueueStreamReport {
                work_class: work_class.into(),
                resource: resource.map(str::to_owned),
                latency: empty_histogram(queue_histogram),
                ..Default::default()
            }
        })
        .collect();
    let rpc = (0..rpc_count)
        .map(|index| RpcReport {
            method: match index {
                0 => "range",
                1 => "txn",
                2 => "status",
                _ => "compact",
            }
            .into(),
            latency: empty_histogram(rpc_histogram),
            ..Default::default()
        })
        .collect();
    PhaseReport {
        phase: phase.into(),
        logical,
        queue,
        rpc,
        resources: if phase == "preparation" {
            empty_preparation_resources()
        } else {
            Vec::new()
        },
        logical_total: empty_histogram(logical_histogram),
        queue_total: empty_histogram(queue_histogram),
        rpc_total: empty_histogram(rpc_histogram),
        timed_out: false,
    }
}

fn empty_watch(plan: &CompiledPlan) -> WatchReport {
    let mut roots = Vec::new();
    for spec in plan.watch_specs() {
        let root_index = if roots
            .last()
            .is_some_and(|root: &WatchRootReport| root.root == spec.resource_prefix)
        {
            roots.len() - 1
        } else {
            roots.push(WatchRootReport {
                root: spec.resource_prefix.clone(),
                ..Default::default()
            });
            roots.len() - 1
        };
        roots[root_index].expected_streams = roots[root_index].expected_streams.saturating_add(1);
        roots[root_index].streams.push(WatchStreamReport {
            root: spec.resource_prefix,
            stream: spec.name,
            ..Default::default()
        });
    }
    WatchReport {
        roots,
        observation_complete: false,
    }
}

fn empty_measurement(plan: &CompiledPlan, setup_ns: Option<u64>) -> MeasurementReport {
    let bucket_count = usize::try_from(plan.measurement_layout.bucket_count).unwrap_or(0);
    let prepare_logical_histogram = plan.measurement_layout.prepare_logical_histogram;
    let prepare_queue_histogram = plan.measurement_layout.prepare_queue_histogram;
    let prepare_rpc_histogram = plan.measurement_layout.prepare_rpc_histogram;
    let measurement_logical_histogram = plan.measurement_layout.measurement_logical_histogram;
    let measurement_queue_histogram = plan.measurement_layout.measurement_queue_histogram;
    let measurement_rpc_histogram = plan.measurement_layout.measurement_rpc_histogram;
    let buckets = (0..bucket_count)
        .map(|_| BucketReport {
            start_ns: 0,
            end_ns: 0,
            logical_latency: empty_histogram(measurement_logical_histogram),
            queue_latency: empty_histogram(measurement_queue_histogram),
            rpc_latency: empty_histogram(measurement_rpc_histogram),
            ..Default::default()
        })
        .collect();

    let mut preparation = empty_phase(
        "preparation",
        7,
        4,
        prepare_logical_histogram,
        prepare_queue_histogram,
        prepare_rpc_histogram,
    );
    let preparation_planned = [
        plan.resource_counts.pods,
        plan.resource_counts.nodes,
        plan.resource_counts.node_leases,
        plan.resource_counts.deployments,
        plan.resource_counts.replicasets,
        plan.resource_counts.daemonsets,
        plan.resource_counts.nodes,
    ];
    for (row, planned) in preparation.logical.iter_mut().zip(preparation_planned) {
        row.planned = planned;
        row.not_offered_after_stop = planned;
    }

    let mut measurement = empty_phase(
        "measurement",
        18,
        4,
        measurement_logical_histogram,
        measurement_queue_histogram,
        measurement_rpc_histogram,
    );
    for stream in &plan.logical_streams {
        if let Some(index) = measurement_stream_index(stream.work_class, stream.resource) {
            measurement.logical[index].planned = stream.planned;
            measurement.logical[index].not_offered_after_stop = stream.planned;
        }
    }
    for stream in &plan.periodic_plan {
        let index = 14 + stream.kind.index();
        measurement.logical[index].planned = stream.planned;
        measurement.logical[index].not_offered_after_stop = stream.planned;
    }
    let drain = empty_phase(
        "drain",
        18,
        4,
        measurement_logical_histogram,
        measurement_queue_histogram,
        measurement_rpc_histogram,
    );
    let planned_conserved = preparation
        .logical
        .iter()
        .chain(measurement.logical.iter())
        .all(|row| {
            row.planned
                == row
                    .offered
                    .saturating_add(row.missed_schedule)
                    .saturating_add(row.not_offered_after_stop)
        });
    let offered_conserved = preparation
        .logical
        .iter()
        .chain(measurement.logical.iter())
        .all(|row| {
            row.offered
                == row
                    .accepted
                    .saturating_add(row.dropped_full)
                    .saturating_add(row.dropped_closed)
        });
    let accepted_cohort_conserved = measurement.logical.iter().all(|row| {
        row.accepted
            == row
                .succeeded_before_cutoff
                .saturating_add(row.failed_before_cutoff)
                .saturating_add(row.terminal_after_cutoff)
                .saturating_add(row.canceled_queued)
                .saturating_add(row.canceled_running)
    });
    let result_unknown_subset_failed = preparation
        .logical
        .iter()
        .chain(measurement.logical.iter())
        .all(|row| {
            row.result_unknown_before_cutoff <= row.failed_before_cutoff
                && row.result_unknown_after_cutoff <= row.failed_after_cutoff
        });
    MeasurementReport {
        preparation,
        measurement,
        drain,
        buckets,
        cutoff: CutoffReport::default(),
        timing: TimingReport {
            setup_ns,
            ..Default::default()
        },
        accounting: MeasurementAccounting {
            planned_conserved,
            offered_conserved,
            accepted_cohort_conserved,
            result_unknown_subset_failed,
            no_active_tokens: true,
            no_arithmetic_overflow: true,
            measurement_complete: false,
            observation_complete: false,
            histogram_overflow: false,
        },
        latency: LatencyReport {
            logical_total: empty_histogram(measurement_logical_histogram),
            queue_total: empty_histogram(measurement_queue_histogram),
            rpc_total: empty_histogram(measurement_rpc_histogram),
            metadata: HistogramMetadata {
                low_us: 1,
                significant_digits: 2,
                auto_resize: false,
                coordinated_omission: false,
                overflow_is_not_truncated: true,
                duration_rounding: "max(1, ceil(duration_ns / 1000))".into(),
                payload_definition:
                    "protobuf message encoded_len; framing, headers and status metadata excluded"
                        .into(),
            },
        },
        watch: empty_watch(plan),
        drain_watch: empty_watch(plan),
    }
}

#[cfg(test)]
#[path = "../tests/unit/report.rs"]
mod tests;
