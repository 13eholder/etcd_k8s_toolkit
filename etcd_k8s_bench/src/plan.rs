//! Compiled synthetic Kubernetes workload plans.
//!
//! This module is the single configuration-to-plan boundary. CLI, tests and
//! the scheduler consume `CompiledPlan` instead of deriving budgets locally.

use std::mem::size_of;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::config::{ConfigError, SyntheticConfig, SyntheticResourceCounts, WatchSpec};
use crate::heartbeat::{HeartbeatItem, HeartbeatItemKind};
use crate::pb::etcdserverpb::RangeRequest;
use crate::watch::prefix_range_end;

pub const DEFAULT_SYNTHETIC_CONFIG: &str = include_str!("../workloads/synthetic-k8s.json");
pub const PACING_TICK: Duration = Duration::from_millis(1);
const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const NANOS_PER_SECOND: u128 = 1_000_000_000;
const HDR_LOW_US: u64 = 1;
const HDR_SIGNIFICANT_DIGITS: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Pods,
    Nodes,
    NodeLeases,
    Deployments,
    #[serde(rename = "replicasets")]
    ReplicaSets,
    #[serde(rename = "daemonsets")]
    DaemonSets,
}

impl ResourceKind {
    pub const ALL: [Self; 6] = [
        Self::Pods,
        Self::Nodes,
        Self::NodeLeases,
        Self::Deployments,
        Self::ReplicaSets,
        Self::DaemonSets,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pods => "pods",
            Self::Nodes => "nodes",
            Self::NodeLeases => "node_leases",
            Self::Deployments => "deployments",
            Self::ReplicaSets => "replicasets",
            Self::DaemonSets => "daemonsets",
        }
    }

    pub const fn index(self) -> usize {
        match self {
            Self::Pods => 0,
            Self::Nodes => 1,
            Self::NodeLeases => 2,
            Self::Deployments => 3,
            Self::ReplicaSets => 4,
            Self::DaemonSets => 5,
        }
    }

    pub const fn root_prefix(self) -> &'static str {
        match self {
            Self::Pods => "/registry/pods",
            Self::Nodes => "/registry/minions",
            Self::NodeLeases => "/registry/leases/kube-node-lease",
            Self::Deployments => "/registry/deployments",
            Self::ReplicaSets => "/registry/replicasets",
            Self::DaemonSets => "/registry/daemonsets",
        }
    }

    const fn is_namespaced(self) -> bool {
        matches!(
            self,
            Self::Pods | Self::Deployments | Self::ReplicaSets | Self::DaemonSets
        )
    }

    const fn name_stem(self) -> &'static str {
        match self {
            Self::Nodes | Self::NodeLeases => "node",
            Self::Pods => "pod",
            Self::Deployments => "deployment",
            Self::ReplicaSets => "replicaset",
            Self::DaemonSets => "daemonset",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResourcePlan {
    pub kind: ResourceKind,
    pub count: u64,
    pub key_prefix: String,
    pub value_size_bytes: u64,
    pub namespace_count: u64,
    pub list_limit: u64,
}

impl ResourcePlan {
    /// Return the namespace occupied by an object ID, or `None` for
    /// cluster-scoped resources. Namespaced resources only rotate through
    /// namespaces that can actually contain one of this resource's IDs.
    pub fn namespace(&self, id: u64) -> Option<String> {
        if !self.kind.is_namespaced() {
            return None;
        }
        let occupied = self.count.min(self.namespace_count);
        assert!(
            occupied > 0,
            "validated namespaced resources must occupy at least one namespace"
        );
        Some(format!("bench-ns-{:05}", id % occupied))
    }

    /// Map a logical ordinal to a stable resource ID. The seed changes the
    /// starting point while the modulo sequence still visits every ID in a
    /// complete resource-domain cycle.
    pub fn id_for_ordinal(&self, ordinal: u64, seed: u64) -> u64 {
        assert!(
            self.count > 0,
            "validated resources must have a positive count"
        );
        let offset = splitmix64(seed ^ self.kind.index() as u64) % self.count;
        let ordinal = ordinal % self.count;
        u64::try_from((u128::from(offset) + u128::from(ordinal)) % u128::from(self.count))
            .expect("resource ID modulo must fit u64")
    }

    pub fn key(&self, id: u64) -> Vec<u8> {
        let name = format!("{}-{id:05}", self.kind.name_stem());
        match self.namespace(id) {
            Some(namespace) => format!("{}{namespace}/{name}", self.key_prefix).into_bytes(),
            None => format!("{}{name}", self.key_prefix).into_bytes(),
        }
    }

    pub fn key_for_ordinal(&self, ordinal: u64, seed: u64) -> Vec<u8> {
        self.key(self.id_for_ordinal(ordinal, seed))
    }

    pub fn value(&self, id: u64, sequence: u64) -> Vec<u8> {
        let size = usize::try_from(self.value_size_bytes)
            .expect("validated resource values must fit in usize");
        patterned_value(size, id, sequence)
    }

    pub fn value_for_ordinal(&self, ordinal: u64, seed: u64, sequence: u64) -> Vec<u8> {
        self.value(self.id_for_ordinal(ordinal, seed), sequence)
    }

    pub fn point_get_request(&self, id: u64) -> RangeRequest {
        RangeRequest {
            key: self.key(id),
            ..Default::default()
        }
    }

    pub fn point_get_request_for_ordinal(&self, ordinal: u64, seed: u64) -> RangeRequest {
        self.point_get_request(self.id_for_ordinal(ordinal, seed))
    }

    /// Return a prefix containing only objects in one occupied namespace.
    pub fn list_prefix(&self, id: u64) -> Vec<u8> {
        match self.namespace(id) {
            Some(namespace) => format!("{}{namespace}/", self.key_prefix).into_bytes(),
            None => self.key_prefix.clone().into_bytes(),
        }
    }

    pub fn list_request(&self, id: u64) -> Result<RangeRequest, ConfigError> {
        let limit = i64::try_from(self.list_limit)
            .map_err(|_| ConfigError::Invalid("load.list_limit exceeds i64".into()))?;
        let key = self.list_prefix(id);
        Ok(RangeRequest {
            range_end: prefix_range_end(&key),
            key,
            limit,
            ..Default::default()
        })
    }

    pub fn list_request_for_ordinal(
        &self,
        ordinal: u64,
        seed: u64,
    ) -> Result<RangeRequest, ConfigError> {
        self.list_request(self.id_for_ordinal(ordinal, seed))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RationalRate {
    pub numerator: u128,
    pub denominator: u128,
}

impl RationalRate {
    fn new(numerator: u128, denominator: u128) -> Result<Self, ConfigError> {
        if denominator == 0 {
            return Err(ConfigError::Invalid(
                "rate denominator must be non-zero".into(),
            ));
        }
        Ok(Self {
            numerator,
            denominator,
        })
    }

    pub fn planned(self, elapsed: Duration) -> Result<u64, ConfigError> {
        if self.denominator == 0 {
            return Err(ConfigError::Invalid(
                "rate denominator must be non-zero".into(),
            ));
        }
        let scaled = self
            .numerator
            .checked_mul(elapsed.as_nanos())
            .ok_or_else(|| ConfigError::Invalid("rate plan multiplication overflows".into()))?;
        let denominator = self
            .denominator
            .checked_mul(NANOS_PER_SECOND)
            .ok_or_else(|| ConfigError::Invalid("rate plan denominator overflows".into()))?;
        u64::try_from(scaled / denominator)
            .map_err(|_| ConfigError::Invalid("planned operation count exceeds u64".into()))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalWorkClass {
    Update,
    PointGet,
    BoundedList,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LogicalStreamPlan {
    pub work_class: LogicalWorkClass,
    pub resource: ResourceKind,
    pub rate: RationalRate,
    pub planned: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeriodicKind {
    NodeLease,
    NodeStatus,
    Status,
    Compact,
}

impl PeriodicKind {
    pub const fn index(self) -> usize {
        match self {
            Self::NodeLease => 0,
            Self::NodeStatus => 1,
            Self::Status => 2,
            Self::Compact => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PeriodicEvent {
    pub at_ns: u64,
    pub kind: PeriodicKind,
    pub node: Option<u64>,
    pub ordinal: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PeriodicStreamPlan {
    pub kind: PeriodicKind,
    pub planned: u64,
}

/// Fixed-width counter storage used by every preallocated logical, queue and
/// RPC stream. `first_error_code` stores a bounded classification; error text
/// is not retained in the hot-path storage.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CounterStorage {
    pub planned: u64,
    pub offered: u64,
    pub accepted: u64,
    pub dropped_full: u64,
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
    pub first_error_code: u64,
    pub rpc_started: u64,
    pub rpc_completed: u64,
    pub rpc_succeeded: u64,
    pub rpc_failed: u64,
    pub rpc_timeout: u64,
    pub rpc_result_unknown: u64,
    pub rpc_payload_bytes: u64,
    pub request_payload_bytes: u64,
    pub response_payload_bytes: u64,
}

/// Fixed-width monotonic phase state.  The recorder stores this exact type so
/// the plan's metadata estimate and the run's timing storage cannot drift.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseClockStorage {
    pub start: Instant,
    pub last: Instant,
    pub measurement_start: Option<Instant>,
    pub configured_cutoff_ns: u64,
    pub effective_cutoff_ns: Option<u64>,
    pub setup_end_ns: Option<u64>,
    pub preparation_end_ns: Option<u64>,
    pub drain_start_ns: Option<u64>,
    pub drain_end_ns: Option<u64>,
    pub watch_teardown_start_ns: Option<u64>,
    pub watch_teardown_end_ns: Option<u64>,
    pub phase: u8,
}

/// Fixed-width bucket counter storage.  Histograms are kept beside this
/// counter in the recorder; this type is only the scalar bucket metadata.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BucketCounterStorage {
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
    pub watch_events: u64,
    pub watch_payload_bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WatchStreamStateStorage {
    pub created: u64,
    pub ready: u64,
    pub connected_since_ns: u64,
    pub observable_duration_ns: u64,
    pub disconnects: u64,
    pub terminal_code: u64,
    pub events: u64,
    pub payload_bytes: u64,
    pub observation_complete: u64,
}

/// Fixed-width metadata paired with each HDR histogram. Overflow and exact
/// maximum are tracked outside the HDR allocation itself.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistogramCounterStorage {
    pub overflow: u64,
    pub exact_max_us: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct HistogramPlan {
    pub low_us: u64,
    pub high_us: u64,
    pub backing_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MeasurementLayout {
    pub bucket_count: u64,
    pub bucket_width_ns: u64,
    pub logical_streams_per_phase: u64,
    pub queue_streams_per_phase: u64,
    pub rpc_methods_per_phase: u64,
    pub prepare_logical_streams: u64,
    pub watch_streams: u64,
    pub periodic_events: u64,
    pub periodic_capacity: u64,
    pub periodic_index_bytes: u64,
    pub prepare_logical_histogram: HistogramPlan,
    pub prepare_queue_histogram: HistogramPlan,
    pub prepare_rpc_histogram: HistogramPlan,
    pub measurement_logical_histogram: HistogramPlan,
    pub measurement_queue_histogram: HistogramPlan,
    pub measurement_rpc_histogram: HistogramPlan,
    pub phase_histogram_bytes: u64,
    pub bucket_histogram_bytes: u64,
    pub histogram_scratch_bytes: u64,
    pub histogram_counter_bytes: u64,
    pub stream_counter_bytes: u64,
    pub bucket_counter_bytes: u64,
    pub watch_state_bytes: u64,
    pub periodic_schedule_bytes: u64,
    /// Fixed recorder/phase/vector metadata, including monotonic timing.
    pub measurement_metadata_bytes: u64,
    /// Bounded first-error storage for every preallocated logical stream.
    pub error_storage_bytes: u64,
    /// In-flight token timing residency, not a per-operation history.
    pub token_timing_bytes: u64,
    pub estimated_measurement_memory_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct CompiledPlan {
    pub config: SyntheticConfig,
    pub resource_counts: SyntheticResourceCounts,
    pub resources: Vec<ResourcePlan>,
    pub logical_streams: Vec<LogicalStreamPlan>,
    pub periodic_events: Vec<PeriodicEvent>,
    pub periodic_plan: Vec<PeriodicStreamPlan>,
    /// Positions of each periodic class in the time-sorted event schedule.
    /// Each vector is exact-capacity and lets the dispatcher skip old events
    /// with four bounded prefix searches instead of visiting every event.
    pub periodic_indexes: [Vec<usize>; 4],
    pub pacing_tick_ms: u64,
    pub update_rounding_deficit: u64,
    pub read_rounding_deficit: u64,
    pub measurement_layout: MeasurementLayout,
}

#[derive(Serialize)]
pub struct LogicalPlanOutput<'a> {
    pub streams: &'a [LogicalStreamPlan],
    pub update_rounding_deficit: u64,
    pub read_rounding_deficit: u64,
}

#[derive(Serialize)]
pub struct PlanOutput<'a> {
    pub schema_version: u8,
    pub resolved_config: &'a SyntheticConfig,
    pub resource_counts: &'a SyntheticResourceCounts,
    pub logical_plan: LogicalPlanOutput<'a>,
    pub periodic_plan: &'a [PeriodicStreamPlan],
    pub pacing_tick_ms: u64,
    pub estimated_measurement_memory_bytes: u64,
}

impl CompiledPlan {
    pub fn output(&self) -> PlanOutput<'_> {
        PlanOutput {
            schema_version: 1,
            resolved_config: &self.config,
            resource_counts: &self.resource_counts,
            logical_plan: LogicalPlanOutput {
                streams: &self.logical_streams,
                update_rounding_deficit: self.update_rounding_deficit,
                read_rounding_deficit: self.read_rounding_deficit,
            },
            periodic_plan: &self.periodic_plan,
            pacing_tick_ms: self.pacing_tick_ms,
            estimated_measurement_memory_bytes: self
                .measurement_layout
                .estimated_measurement_memory_bytes,
        }
    }

    /// Serialize the read-only plan directly to a caller-provided writer.
    ///
    /// Keeping serialization on the writer path avoids constructing a second
    /// full-size JSON buffer for the plan output.
    pub fn write_json<W: std::io::Write>(&self, writer: W) -> Result<(), serde_json::Error> {
        serde_json::to_writer(writer, &self.output())
    }

    /// Return the compiled mapping for one of the six fixed resource kinds.
    pub fn resource(&self, kind: ResourceKind) -> &ResourcePlan {
        &self.resources[kind.index()]
    }

    /// Build the deterministic key for a resource operation.
    pub fn resource_key(&self, kind: ResourceKind, ordinal: u64, seed: u64) -> Vec<u8> {
        self.resource(kind).key_for_ordinal(ordinal, seed)
    }

    /// Build a deterministic synthetic value for a resource operation.
    pub fn resource_value(
        &self,
        kind: ResourceKind,
        ordinal: u64,
        seed: u64,
        sequence: u64,
    ) -> Vec<u8> {
        self.resource(kind)
            .value_for_ordinal(ordinal, seed, sequence)
    }

    /// Build an exact-key Range request for one logical point read.
    pub fn point_get_request(&self, kind: ResourceKind, ordinal: u64, seed: u64) -> RangeRequest {
        self.resource(kind)
            .point_get_request_for_ordinal(ordinal, seed)
    }

    /// Build a bounded prefix Range request for one logical list read.
    pub fn list_request(
        &self,
        kind: ResourceKind,
        ordinal: u64,
        seed: u64,
    ) -> Result<RangeRequest, ConfigError> {
        self.resource(kind).list_request_for_ordinal(ordinal, seed)
    }

    /// Generate the configured subscriptions for all six fixed resource
    /// roots. Subscription multiplicity is per root, never per Node.
    pub fn watch_specs(&self) -> Vec<WatchSpec> {
        let subscriptions = self.config.watch.subscriptions_per_root;
        let mut specs = Vec::new();
        for kind in ResourceKind::ALL {
            for subscription in 0..subscriptions {
                let name = if subscriptions == 1 {
                    kind.as_str().to_owned()
                } else {
                    format!("{}-{subscription}", kind.as_str())
                };
                specs.push(WatchSpec {
                    name,
                    resource_prefix: kind.root_prefix().to_owned(),
                    start_revision: self.config.watch.start_revision,
                    prev_kv: self.config.watch.prev_kv,
                    progress_notify: self.config.watch.progress_notify,
                });
            }
        }
        specs
    }

    /// Convert a scheduled node event into the protocol item consumed by the
    /// heartbeat worker. Non-node periodic streams return `None`.
    pub fn heartbeat_item(&self, event: &PeriodicEvent) -> Option<HeartbeatItem> {
        let node = usize::try_from(event.node?).ok()?;
        let node_id = u64::try_from(node).ok()?;
        let (kind, resource) = match event.kind {
            PeriodicKind::NodeLease => {
                (HeartbeatItemKind::NodeLeaseRenew, ResourceKind::NodeLeases)
            }
            PeriodicKind::NodeStatus => (HeartbeatItemKind::NodeStatus, ResourceKind::Nodes),
            PeriodicKind::Status | PeriodicKind::Compact => return None,
        };
        let key = String::from_utf8(self.resource(resource).key(node_id)).ok()?;
        Some(HeartbeatItem::from_plan(
            node,
            kind,
            key,
            Duration::from_nanos(event.at_ns),
        ))
    }
}

impl CompiledPlan {
    pub fn compile(config: SyntheticConfig, max_message_size: usize) -> Result<Self, ConfigError> {
        config.validate(max_message_size)?;
        i64::try_from(config.load.list_limit)
            .map_err(|_| ConfigError::Invalid("load.list_limit exceeds i64".into()))?;
        let duration = duration_ms(config.load.duration_ms, "load.duration_ms")?;
        let counts = config.resource_counts()?;
        let resources = resource_plans(&config, counts);
        let logical_streams = logical_streams(&config, duration)?;
        let update_nominal =
            RationalRate::new(u128::from(config.load.update_rps), 1)?.planned(duration)?;
        let update_planned = planned_for_class(&logical_streams, LogicalWorkClass::Update)?;
        let read_base_numerator = u128::from(config.load.update_rps)
            .checked_mul(u128::from(config.load.read_ratio.numerator))
            .ok_or_else(|| ConfigError::Invalid("read rate multiplication overflows".into()))?;
        let read_nominal = RationalRate::new(
            read_base_numerator,
            u128::from(config.load.read_ratio.denominator),
        )?
        .planned(duration)?;
        let read_planned = logical_streams
            .iter()
            .filter(|stream| stream.work_class != LogicalWorkClass::Update)
            .try_fold(0u64, |total, stream| {
                total.checked_add(stream.planned).ok_or_else(|| {
                    ConfigError::Invalid("read planned operation count overflows".into())
                })
            })?;
        let update_rounding_deficit =
            update_nominal.checked_sub(update_planned).ok_or_else(|| {
                ConfigError::Invalid("update plan exceeds nominal operation count".into())
            })?;
        let read_rounding_deficit = read_nominal.checked_sub(read_planned).ok_or_else(|| {
            ConfigError::Invalid("read plan exceeds nominal operation count".into())
        })?;

        // The capacity bound is computed without allocating the event Vec.
        // Run the complete memory estimate before reserving that capacity.
        let periodic_capacity_bound = periodic_capacity(&config, duration)?;
        measurement_layout(&config, periodic_capacity_bound, 0)?;
        let periodic_events = periodic_events(&config, duration, periodic_capacity_bound)?;
        let index_counts = periodic_index_counts(&periodic_events);
        let index_capacity_bound = periodic_index_capacity(index_counts)?;
        // Validate the exact per-kind index allocation before reserving any
        // index Vec. The initial conservative estimate above protects the
        // event schedule itself; this second estimate protects the indexes.
        let mut layout =
            measurement_layout(&config, periodic_capacity_bound, index_capacity_bound)?;
        let periodic_indexes = periodic_indexes(&periodic_events, index_counts)?;
        let actual_periodic_capacity = u64::try_from(periodic_events.capacity())
            .map_err(|_| ConfigError::Invalid("periodic capacity exceeds u64".into()))?;
        let actual_index_capacity = periodic_indexes.iter().try_fold(0u64, |total, index| {
            total
                .checked_add(
                    u64::try_from(index.capacity())
                        .map_err(|_| ConfigError::Invalid("periodic index exceeds u64".into()))?,
                )
                .ok_or_else(|| ConfigError::Invalid("periodic index capacity overflows".into()))
        })?;
        if actual_periodic_capacity != periodic_capacity_bound
            || actual_index_capacity != index_capacity_bound
        {
            layout = measurement_layout(&config, actual_periodic_capacity, actual_index_capacity)?;
        }
        layout.periodic_events = u64::try_from(periodic_events.len())
            .map_err(|_| ConfigError::Invalid("periodic event count exceeds u64".into()))?;
        layout.periodic_capacity = actual_periodic_capacity;
        let periodic_plan = periodic_plan(&periodic_events)?;
        Ok(Self {
            config,
            resource_counts: counts,
            resources,
            logical_streams,
            periodic_events,
            periodic_plan,
            periodic_indexes,
            pacing_tick_ms: 1,
            update_rounding_deficit,
            read_rounding_deficit,
            measurement_layout: layout,
        })
    }
}

fn duration_ms(value: u64, field: &str) -> Result<Duration, ConfigError> {
    let nanos = value
        .checked_mul(NANOS_PER_MILLISECOND)
        .ok_or_else(|| ConfigError::Invalid(format!("{field} exceeds u64 nanoseconds")))?;
    Ok(Duration::from_nanos(nanos))
}

fn splitmix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn patterned_value(size: usize, id: u64, sequence: u64) -> Vec<u8> {
    let mut value = vec![0; size];
    let marker = splitmix64(id ^ sequence);
    for (index, byte) in value.iter_mut().enumerate() {
        *byte = marker.rotate_left((index % 63) as u32) as u8;
    }
    value
}

fn resource_plans(config: &SyntheticConfig, counts: SyntheticResourceCounts) -> Vec<ResourcePlan> {
    ResourceKind::ALL
        .into_iter()
        .map(|kind| {
            let (count, value_size_bytes, key_prefix) = match kind {
                ResourceKind::Pods => {
                    (counts.pods, config.value_size_bytes.pods, "/registry/pods/")
                }
                ResourceKind::Nodes => (
                    counts.nodes,
                    config.value_size_bytes.nodes,
                    "/registry/minions/",
                ),
                ResourceKind::NodeLeases => (
                    counts.node_leases,
                    config.value_size_bytes.node_leases,
                    "/registry/leases/kube-node-lease/",
                ),
                ResourceKind::Deployments => (
                    counts.deployments,
                    config.value_size_bytes.deployments,
                    "/registry/deployments/",
                ),
                ResourceKind::ReplicaSets => (
                    counts.replicasets,
                    config.value_size_bytes.replicasets,
                    "/registry/replicasets/",
                ),
                ResourceKind::DaemonSets => (
                    counts.daemonsets,
                    config.value_size_bytes.daemonsets,
                    "/registry/daemonsets/",
                ),
            };
            ResourcePlan {
                kind,
                count,
                key_prefix: key_prefix.into(),
                value_size_bytes,
                namespace_count: config.topology.namespace_count,
                list_limit: config.load.list_limit,
            }
        })
        .collect()
}

fn logical_streams(
    config: &SyntheticConfig,
    duration: Duration,
) -> Result<Vec<LogicalStreamPlan>, ConfigError> {
    let mut streams = Vec::with_capacity(14);
    let update_total = u128::from(config.load.update_weights.checked_total()?);
    for (resource, weight) in [
        (ResourceKind::Pods, config.load.update_weights.pods),
        (
            ResourceKind::Deployments,
            config.load.update_weights.deployments,
        ),
        (
            ResourceKind::ReplicaSets,
            config.load.update_weights.replicasets,
        ),
        (
            ResourceKind::DaemonSets,
            config.load.update_weights.daemonsets,
        ),
    ] {
        let numerator = u128::from(config.load.update_rps)
            .checked_mul(u128::from(weight))
            .ok_or_else(|| ConfigError::Invalid("update rate multiplication overflows".into()))?;
        let rate = RationalRate::new(numerator, update_total)?;
        streams.push(LogicalStreamPlan {
            work_class: LogicalWorkClass::Update,
            resource,
            rate,
            planned: rate.planned(duration)?,
        });
    }

    let read_weights_total = u128::from(config.load.read_weights.checked_total()?);
    let read_shape_total = u128::from(config.load.read_shape_weights.checked_total()?);
    let read_base_numerator = u128::from(config.load.update_rps)
        .checked_mul(u128::from(config.load.read_ratio.numerator))
        .ok_or_else(|| ConfigError::Invalid("read rate multiplication overflows".into()))?;
    let read_base_denominator = u128::from(config.load.read_ratio.denominator)
        .checked_mul(read_weights_total)
        .and_then(|value| value.checked_mul(read_shape_total))
        .ok_or_else(|| ConfigError::Invalid("read rate denominator overflows".into()))?;
    for (resource, resource_weight) in [
        (ResourceKind::Pods, config.load.read_weights.pods),
        (ResourceKind::Nodes, config.load.read_weights.nodes),
        (
            ResourceKind::Deployments,
            config.load.read_weights.deployments,
        ),
        (
            ResourceKind::ReplicaSets,
            config.load.read_weights.replicasets,
        ),
        (
            ResourceKind::DaemonSets,
            config.load.read_weights.daemonsets,
        ),
    ] {
        for (work_class, shape_weight) in [
            (
                LogicalWorkClass::PointGet,
                config.load.read_shape_weights.point,
            ),
            (
                LogicalWorkClass::BoundedList,
                config.load.read_shape_weights.bounded_list,
            ),
        ] {
            let numerator = read_base_numerator
                .checked_mul(u128::from(resource_weight))
                .and_then(|value| value.checked_mul(u128::from(shape_weight)))
                .ok_or_else(|| ConfigError::Invalid("read rate multiplication overflows".into()))?;
            let rate = RationalRate::new(numerator, read_base_denominator)?;
            streams.push(LogicalStreamPlan {
                work_class,
                resource,
                rate,
                planned: rate.planned(duration)?,
            });
        }
    }
    Ok(streams)
}

fn planned_for_class(
    streams: &[LogicalStreamPlan],
    class: LogicalWorkClass,
) -> Result<u64, ConfigError> {
    streams
        .iter()
        .filter(|stream| stream.work_class == class)
        .try_fold(0u64, |total, stream| {
            total
                .checked_add(stream.planned)
                .ok_or_else(|| ConfigError::Invalid("planned operation count overflows".into()))
        })
}

fn periodic_capacity(config: &SyntheticConfig, duration: Duration) -> Result<u64, ConfigError> {
    let duration_ns = u128::from(checked_u64_ns(duration, "load.duration_ms")?);
    let end_ns = duration_ns
        .checked_sub(1)
        .ok_or_else(|| ConfigError::Invalid("load.duration_ms must be positive".into()))?;
    let lease_ns = u128::from(checked_u64_ns(
        duration_ms(
            config.periodic.node_lease_interval_ms,
            "periodic.node_lease_interval_ms",
        )?,
        "periodic.node_lease_interval_ms",
    )?);
    let jitter_ns = lease_ns
        .checked_mul(u128::from(config.periodic.node_lease_jitter_percent))
        .and_then(|value| value.checked_div(100))
        .ok_or_else(|| ConfigError::Invalid("NodeLease jitter overflows".into()))?;
    let shortest_lease = lease_ns
        .checked_sub(jitter_ns)
        .filter(|value| *value > 0)
        .ok_or_else(|| ConfigError::Invalid("NodeLease shortest interval is zero".into()))?;
    let status_ns = u128::from(checked_u64_ns(
        duration_ms(
            config.periodic.node_status_interval_ms,
            "periodic.node_status_interval_ms",
        )?,
        "periodic.node_status_interval_ms",
    )?);
    let lease_per_node = end_ns / shortest_lease;
    let status_per_node = duration_ns.div_ceil(status_ns);
    let per_node = lease_per_node
        .checked_add(status_per_node)
        .ok_or_else(|| ConfigError::Invalid("periodic capacity overflows".into()))?;
    let mut capacity = per_node
        .checked_mul(u128::from(config.topology.node_count))
        .ok_or_else(|| ConfigError::Invalid("periodic capacity overflows".into()))?;
    for interval_ms in [
        config.periodic.status_interval_ms,
        config.periodic.compact_interval_ms,
    ] {
        let interval_ns = u128::from(checked_u64_ns(
            duration_ms(interval_ms, "periodic interval")?,
            "periodic interval",
        )?);
        capacity = capacity
            .checked_add(end_ns / interval_ns)
            .ok_or_else(|| ConfigError::Invalid("periodic capacity overflows".into()))?;
    }
    u64::try_from(capacity)
        .map_err(|_| ConfigError::Invalid("periodic capacity exceeds u64".into()))
}

fn push_periodic_event(
    events: &mut Vec<PeriodicEvent>,
    event: PeriodicEvent,
    capacity: usize,
) -> Result<(), ConfigError> {
    if events.len() >= capacity {
        return Err(ConfigError::Invalid(
            "periodic capacity bound is smaller than generated schedule".into(),
        ));
    }
    events.push(event);
    Ok(())
}

fn periodic_events(
    config: &SyntheticConfig,
    duration: Duration,
    capacity_bound: u64,
) -> Result<Vec<PeriodicEvent>, ConfigError> {
    let capacity = usize::try_from(capacity_bound)
        .map_err(|_| ConfigError::Invalid("periodic capacity exceeds usize".into()))?;
    let mut events = Vec::new();
    events
        .try_reserve_exact(capacity)
        .map_err(|error| ConfigError::Invalid(format!("periodic allocation failed: {error}")))?;

    let nodes = config.topology.node_count;
    let lease_interval = duration_ms(
        config.periodic.node_lease_interval_ms,
        "periodic.node_lease_interval_ms",
    )?;
    let status_interval = duration_ms(
        config.periodic.node_status_interval_ms,
        "periodic.node_status_interval_ms",
    )?;
    for node in 0..nodes {
        let mut at = jittered_interval(
            config.seed,
            node,
            0,
            lease_interval,
            config.periodic.node_lease_jitter_percent,
        )?;
        let mut ordinal = 0u64;
        while at < duration {
            push_periodic_event(
                &mut events,
                PeriodicEvent {
                    at_ns: checked_u64_ns(at, "NodeLease schedule")?,
                    kind: PeriodicKind::NodeLease,
                    node: Some(node),
                    ordinal,
                },
                capacity,
            )?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| ConfigError::Invalid("NodeLease ordinal overflows".into()))?;
            let interval = jittered_interval(
                config.seed,
                node,
                ordinal,
                lease_interval,
                config.periodic.node_lease_jitter_percent,
            )?;
            at = at
                .checked_add(interval)
                .ok_or_else(|| ConfigError::Invalid("NodeLease schedule overflows".into()))?;
        }

        let node_index = node
            .checked_add(1)
            .ok_or_else(|| ConfigError::Invalid("Node status node index overflows".into()))?;
        let phase_ns = status_interval
            .as_nanos()
            .checked_mul(u128::from(node_index))
            .ok_or_else(|| ConfigError::Invalid("Node status phase overflows".into()))?
            / u128::from(nodes);
        let phase_ns = u64::try_from(phase_ns).map_err(|_| {
            ConfigError::Invalid("Node status phase exceeds u64 nanoseconds".into())
        })?;
        let mut at = Duration::from_nanos(phase_ns);
        let mut ordinal = 0u64;
        while at < duration {
            push_periodic_event(
                &mut events,
                PeriodicEvent {
                    at_ns: checked_u64_ns(at, "Node status schedule")?,
                    kind: PeriodicKind::NodeStatus,
                    node: Some(node),
                    ordinal,
                },
                capacity,
            )?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| ConfigError::Invalid("Node status ordinal overflows".into()))?;
            at = at
                .checked_add(status_interval)
                .ok_or_else(|| ConfigError::Invalid("Node status schedule overflows".into()))?;
        }
    }
    for (kind, interval_ms, field) in [
        (
            PeriodicKind::Status,
            config.periodic.status_interval_ms,
            "periodic.status_interval_ms",
        ),
        (
            PeriodicKind::Compact,
            config.periodic.compact_interval_ms,
            "periodic.compact_interval_ms",
        ),
    ] {
        let interval = duration_ms(interval_ms, field)?;
        let mut at = interval;
        let mut ordinal = 0u64;
        while at < duration {
            push_periodic_event(
                &mut events,
                PeriodicEvent {
                    at_ns: checked_u64_ns(at, "periodic schedule")?,
                    kind,
                    node: None,
                    ordinal,
                },
                capacity,
            )?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| ConfigError::Invalid("periodic ordinal overflows".into()))?;
            at = at
                .checked_add(interval)
                .ok_or_else(|| ConfigError::Invalid("periodic schedule overflows".into()))?;
        }
    }

    events.sort_unstable_by_key(|event| (event.at_ns, event.kind, event.node, event.ordinal));
    Ok(events)
}

fn jittered_interval(
    seed: u64,
    node: u64,
    ordinal: u64,
    base: Duration,
    jitter_percent: u8,
) -> Result<Duration, ConfigError> {
    let base_ns = base.as_nanos();
    let jitter = base_ns
        .checked_mul(u128::from(jitter_percent))
        .and_then(|value| value.checked_div(100))
        .ok_or_else(|| ConfigError::Invalid("NodeLease jitter overflows".into()))?;
    if jitter == 0 {
        return Ok(base);
    }
    let mut z = seed
        .wrapping_add(node.wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .wrapping_add(ordinal.rotate_left(17));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    let span = jitter
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| ConfigError::Invalid("NodeLease jitter span overflows".into()))?;
    let sample = u128::from(z) % span;
    let nanos = if sample <= jitter {
        base_ns
            .checked_sub(jitter - sample)
            .ok_or_else(|| ConfigError::Invalid("NodeLease interval underflows".into()))?
    } else {
        base_ns
            .checked_add(sample - jitter)
            .ok_or_else(|| ConfigError::Invalid("NodeLease interval overflows".into()))?
    };
    let nanos = u64::try_from(nanos)
        .map_err(|_| ConfigError::Invalid("NodeLease interval overflows u64".into()))?;
    if nanos == 0 {
        return Err(ConfigError::Invalid(
            "NodeLease interval must remain positive".into(),
        ));
    }
    Ok(Duration::from_nanos(nanos))
}

fn checked_u64_ns(duration: Duration, field: &str) -> Result<u64, ConfigError> {
    u64::try_from(duration.as_nanos())
        .map_err(|_| ConfigError::Invalid(format!("{field} exceeds u64 nanoseconds")))
}

fn periodic_plan(events: &[PeriodicEvent]) -> Result<Vec<PeriodicStreamPlan>, ConfigError> {
    [
        PeriodicKind::NodeLease,
        PeriodicKind::NodeStatus,
        PeriodicKind::Status,
        PeriodicKind::Compact,
    ]
    .into_iter()
    .map(|kind| {
        let planned = u64::try_from(events.iter().filter(|event| event.kind == kind).count())
            .map_err(|_| ConfigError::Invalid("periodic event count exceeds u64".into()))?;
        Ok(PeriodicStreamPlan { kind, planned })
    })
    .collect()
}

fn periodic_index_counts(events: &[PeriodicEvent]) -> [usize; 4] {
    let mut counts = [0usize; 4];
    for event in events {
        counts[event.kind.index()] = counts[event.kind.index()].saturating_add(1);
    }
    counts
}

fn periodic_index_capacity(counts: [usize; 4]) -> Result<u64, ConfigError> {
    counts.into_iter().try_fold(0u64, |total, count| {
        total
            .checked_add(
                u64::try_from(count)
                    .map_err(|_| ConfigError::Invalid("periodic index exceeds u64".into()))?,
            )
            .ok_or_else(|| ConfigError::Invalid("periodic index capacity overflows".into()))
    })
}

fn periodic_indexes(
    events: &[PeriodicEvent],
    counts: [usize; 4],
) -> Result<[Vec<usize>; 4], ConfigError> {
    let mut indexes = std::array::from_fn(|_| Vec::new());
    for (index, count) in counts.into_iter().enumerate() {
        indexes[index].try_reserve_exact(count).map_err(|error| {
            ConfigError::Invalid(format!("periodic index allocation failed: {error}"))
        })?;
    }
    for (position, event) in events.iter().enumerate() {
        indexes[event.kind.index()].push(position);
    }
    Ok(indexes)
}

fn size_bytes<T>(field: &str) -> Result<u64, ConfigError> {
    u64::try_from(size_of::<T>()).map_err(|_| ConfigError::Invalid(format!("{field} exceeds u64")))
}

fn histogram_plan(low_us: u64, high_us: u64) -> Result<HistogramPlan, ConfigError> {
    let mut histogram =
        hdrhistogram::Histogram::<u64>::new_with_bounds(low_us, high_us, HDR_SIGNIFICANT_DIGITS)
            .map_err(|error| ConfigError::Invalid(format!("HDR bounds are invalid: {error}")))?;
    histogram.auto(false);
    let counts_bytes = u64::try_from(histogram.distinct_values())
        .map_err(|_| ConfigError::Invalid("HDR estimate exceeds u64".into()))?
        .checked_mul(size_bytes::<u64>("HDR counter size")?)
        .ok_or_else(|| ConfigError::Invalid("HDR estimate overflows".into()))?;
    let backing_bytes = size_bytes::<hdrhistogram::Histogram<u64>>("HDR estimate")?
        .checked_add(counts_bytes)
        .ok_or_else(|| ConfigError::Invalid("HDR estimate overflows".into()))?;
    Ok(HistogramPlan {
        low_us,
        high_us,
        backing_bytes,
    })
}

fn checked_add(total: &mut u64, value: u64, field: &str) -> Result<(), ConfigError> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| ConfigError::Invalid(format!("{field} estimate overflows")))?;
    Ok(())
}

fn phase_histogram_bytes(
    logical: HistogramPlan,
    queue: HistogramPlan,
    rpc: HistogramPlan,
    logical_streams: u64,
    queue_streams: u64,
    rpc_methods: u64,
) -> Result<u64, ConfigError> {
    let logical_instances = logical_streams
        .checked_add(1)
        .ok_or_else(|| ConfigError::Invalid("phase histogram instance count overflows".into()))?;
    let queue_instances = queue_streams
        .checked_add(1)
        .ok_or_else(|| ConfigError::Invalid("phase histogram instance count overflows".into()))?;
    let rpc_instances = rpc_methods
        .checked_add(1)
        .ok_or_else(|| ConfigError::Invalid("phase histogram instance count overflows".into()))?;
    let mut total = 0u64;
    checked_add(
        &mut total,
        logical
            .backing_bytes
            .checked_mul(logical_instances)
            .ok_or_else(|| ConfigError::Invalid("phase HDR estimate overflows".into()))?,
        "phase HDR",
    )?;
    checked_add(
        &mut total,
        queue
            .backing_bytes
            .checked_mul(queue_instances)
            .ok_or_else(|| ConfigError::Invalid("phase HDR estimate overflows".into()))?,
        "phase HDR",
    )?;
    checked_add(
        &mut total,
        rpc.backing_bytes
            .checked_mul(rpc_instances)
            .ok_or_else(|| ConfigError::Invalid("phase HDR estimate overflows".into()))?,
        "phase HDR",
    )?;
    Ok(total)
}

fn measurement_layout(
    config: &SyntheticConfig,
    periodic_capacity: u64,
    periodic_index_capacity: u64,
) -> Result<MeasurementLayout, ConfigError> {
    let bucket_count = config.load.duration_ms.div_ceil(config.report.bucket_ms);
    let bucket_width_ns = config
        .report
        .bucket_ms
        .checked_mul(NANOS_PER_MILLISECOND)
        .ok_or_else(|| ConfigError::Invalid("bucket width overflows".into()))?;
    let prepare_high_us = config
        .execution
        .preparation_timeout_ms
        .checked_mul(1_000)
        .ok_or_else(|| ConfigError::Invalid("preparation HDR bound overflows".into()))?;
    let measurement_high_us = config
        .load
        .duration_ms
        .checked_add(config.execution.drain_timeout_ms)
        .and_then(|value| value.checked_mul(1_000))
        .ok_or_else(|| ConfigError::Invalid("measurement HDR bound overflows".into()))?;
    let rpc_high_us = config
        .execution
        .request_timeout_ms
        .checked_mul(1_000)
        .ok_or_else(|| ConfigError::Invalid("RPC HDR bound overflows".into()))?;

    let prepare_logical_histogram = histogram_plan(HDR_LOW_US, prepare_high_us)?;
    let prepare_queue_histogram = histogram_plan(HDR_LOW_US, prepare_high_us)?;
    let prepare_rpc_histogram = histogram_plan(HDR_LOW_US, rpc_high_us)?;
    let measurement_logical_histogram = histogram_plan(HDR_LOW_US, measurement_high_us)?;
    let measurement_queue_histogram = histogram_plan(HDR_LOW_US, measurement_high_us)?;
    let measurement_rpc_histogram = histogram_plan(HDR_LOW_US, rpc_high_us)?;

    let prepare_phase = phase_histogram_bytes(
        prepare_logical_histogram,
        prepare_queue_histogram,
        prepare_rpc_histogram,
        7,
        7,
        4,
    )?;
    let measurement_phase = phase_histogram_bytes(
        measurement_logical_histogram,
        measurement_queue_histogram,
        measurement_rpc_histogram,
        18,
        18,
        4,
    )?;
    let drain_phase = measurement_phase;
    let mut phase_histogram_total = 0u64;
    checked_add(&mut phase_histogram_total, prepare_phase, "phase HDR")?;
    checked_add(&mut phase_histogram_total, measurement_phase, "phase HDR")?;
    checked_add(&mut phase_histogram_total, drain_phase, "phase HDR")?;

    let bucket_histogram_per_bucket = measurement_logical_histogram
        .backing_bytes
        .checked_add(measurement_queue_histogram.backing_bytes)
        .and_then(|value| value.checked_add(measurement_rpc_histogram.backing_bytes))
        .ok_or_else(|| ConfigError::Invalid("bucket HDR estimate overflows".into()))?;
    let bucket_histogram_bytes = bucket_histogram_per_bucket
        .checked_mul(bucket_count)
        .ok_or_else(|| ConfigError::Invalid("bucket HDR estimate overflows".into()))?;
    let histogram_scratch_bytes = [
        prepare_logical_histogram.backing_bytes,
        prepare_queue_histogram.backing_bytes,
        prepare_rpc_histogram.backing_bytes,
        measurement_logical_histogram.backing_bytes,
        measurement_queue_histogram.backing_bytes,
        measurement_rpc_histogram.backing_bytes,
    ]
    .into_iter()
    .max()
    .unwrap_or(0);
    let bucket_histogram_instances = bucket_count
        .checked_mul(3)
        .ok_or_else(|| ConfigError::Invalid("bucket HDR instance count overflows".into()))?;

    let prepare_streams = 7u64
        .checked_add(7)
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| ConfigError::Invalid("counter instance count overflows".into()))?;
    let measurement_streams = 18u64
        .checked_add(18)
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| ConfigError::Invalid("counter instance count overflows".into()))?;
    let stream_counter_instances = prepare_streams
        .checked_add(measurement_streams)
        .and_then(|value| value.checked_add(measurement_streams))
        .ok_or_else(|| ConfigError::Invalid("counter instance count overflows".into()))?;
    let measurement_histogram_instances = measurement_streams
        .checked_add(3)
        .ok_or_else(|| ConfigError::Invalid("measurement HDR instance count overflows".into()))?;
    let phase_histogram_instances = prepare_streams
        .checked_add(3)
        .and_then(|value| value.checked_add(measurement_histogram_instances))
        .and_then(|value| value.checked_add(measurement_histogram_instances))
        .ok_or_else(|| ConfigError::Invalid("phase HDR instance count overflows".into()))?;
    let histogram_counter_instances = phase_histogram_instances
        .checked_add(bucket_histogram_instances)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| ConfigError::Invalid("HDR counter instance count overflows".into()))?;
    let histogram_size = size_bytes::<hdrhistogram::Histogram<u64>>("HDR estimate")?;
    let histogram_slot_size = crate::measurement::histogram_slot_size_bytes()
        .ok_or_else(|| ConfigError::Invalid("HDR slot estimate exceeds u64".into()))?;
    let histogram_counter_size = histogram_slot_size
        .checked_sub(histogram_size)
        .ok_or_else(|| ConfigError::Invalid("HDR slot estimate underflows".into()))?;
    let histogram_counter_bytes = histogram_counter_instances
        .checked_mul(histogram_counter_size)
        .ok_or_else(|| ConfigError::Invalid("HDR counter estimate overflows".into()))?;
    let counter_size = size_bytes::<CounterStorage>("counter estimate")?;
    let stream_counter_bytes = stream_counter_instances
        .checked_mul(counter_size)
        .ok_or_else(|| ConfigError::Invalid("stream counter estimate overflows".into()))?;
    let bucket_counter_size = size_bytes::<BucketCounterStorage>("bucket counter estimate")?;
    let bucket_counter_bytes = bucket_count
        .checked_mul(bucket_counter_size)
        .ok_or_else(|| ConfigError::Invalid("bucket counter estimate overflows".into()))?;

    let watch_roots = u64::try_from(ResourceKind::ALL.len())
        .map_err(|_| ConfigError::Invalid("Watch root count exceeds u64".into()))?;
    let watch_streams = watch_roots
        .checked_mul(config.watch.subscriptions_per_root)
        .ok_or_else(|| ConfigError::Invalid("Watch stream count overflows".into()))?;
    let watch_state_size = crate::measurement::watch_state_size_bytes()
        .ok_or_else(|| ConfigError::Invalid("Watch state estimate overflows".into()))?;
    let watch_state_bytes = watch_streams
        .checked_mul(watch_state_size)
        .ok_or_else(|| ConfigError::Invalid("Watch estimate overflows".into()))?;

    let periodic_event_size = size_bytes::<PeriodicEvent>("periodic estimate")?;
    let periodic_schedule_bytes = periodic_capacity
        .checked_mul(periodic_event_size)
        .ok_or_else(|| ConfigError::Invalid("periodic estimate overflows".into()))?;

    let measurement_metadata_bytes = crate::measurement::recorder_metadata_bytes()
        .ok_or_else(|| ConfigError::Invalid("measurement metadata estimate overflows".into()))?;
    let periodic_index_bytes = periodic_index_capacity
        .checked_mul(size_bytes::<usize>("periodic index estimate")?)
        .ok_or_else(|| ConfigError::Invalid("periodic index estimate overflows".into()))?;
    let logical_error_instances = 7u64
        .checked_add(18)
        .and_then(|value| value.checked_add(18))
        .ok_or_else(|| ConfigError::Invalid("error storage instance count overflows".into()))?;
    let error_storage_bytes = crate::measurement::error_storage_size_bytes()
        .checked_mul(logical_error_instances)
        .ok_or_else(|| ConfigError::Invalid("error storage estimate overflows".into()))?;
    let token_slots = config
        .execution
        .queue_capacity
        .checked_add(config.execution.workers)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| ConfigError::Invalid("token timing instance count overflows".into()))?;
    let token_timing_bytes = crate::measurement::token_storage_size_bytes()
        .checked_mul(token_slots)
        .ok_or_else(|| ConfigError::Invalid("token timing estimate overflows".into()))?;

    let mut estimated = 0u64;
    checked_add(&mut estimated, phase_histogram_total, "phase HDR")?;
    checked_add(&mut estimated, bucket_histogram_bytes, "bucket HDR")?;
    checked_add(&mut estimated, histogram_scratch_bytes, "HDR scratch")?;
    checked_add(&mut estimated, histogram_counter_bytes, "HDR counters")?;
    checked_add(&mut estimated, stream_counter_bytes, "stream counters")?;
    checked_add(&mut estimated, bucket_counter_bytes, "bucket counters")?;
    checked_add(&mut estimated, watch_state_bytes, "Watch state")?;
    checked_add(&mut estimated, periodic_schedule_bytes, "periodic schedule")?;
    checked_add(&mut estimated, periodic_index_bytes, "periodic index")?;
    checked_add(
        &mut estimated,
        measurement_metadata_bytes,
        "measurement metadata",
    )?;
    checked_add(&mut estimated, error_storage_bytes, "error storage")?;
    checked_add(&mut estimated, token_timing_bytes, "token timing")?;

    let memory_limit = config
        .report
        .memory_limit_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| ConfigError::Invalid("report memory limit overflows".into()))?;
    if estimated > memory_limit {
        return Err(ConfigError::Invalid(format!(
            "estimated measurement memory ({estimated} bytes) exceeds report.memory_limit_mib ({})",
            config.report.memory_limit_mib
        )));
    }
    Ok(MeasurementLayout {
        bucket_count,
        bucket_width_ns,
        logical_streams_per_phase: 18,
        queue_streams_per_phase: 18,
        rpc_methods_per_phase: 4,
        prepare_logical_streams: 7,
        watch_streams,
        periodic_events: 0,
        periodic_capacity,
        periodic_index_bytes,
        prepare_logical_histogram,
        prepare_queue_histogram,
        prepare_rpc_histogram,
        measurement_logical_histogram,
        measurement_queue_histogram,
        measurement_rpc_histogram,
        phase_histogram_bytes: phase_histogram_total,
        bucket_histogram_bytes,
        histogram_scratch_bytes,
        histogram_counter_bytes,
        stream_counter_bytes,
        bucket_counter_bytes,
        watch_state_bytes,
        periodic_schedule_bytes,
        measurement_metadata_bytes,
        error_storage_bytes,
        token_timing_bytes,
        estimated_measurement_memory_bytes: estimated,
    })
}
#[cfg(test)]
#[path = "../tests/unit/plan.rs"]
mod tests;
