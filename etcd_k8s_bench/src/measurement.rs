use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use thiserror::Error;

pub use crate::metrics::{DropReason, Outcome};
use crate::plan::{
    BucketCounterStorage, CompiledPlan, CounterStorage, HistogramCounterStorage, HistogramPlan,
    LogicalWorkClass, MeasurementLayout, PeriodicKind, PhaseClockStorage, ResourceKind,
};
use crate::report::{
    BucketReport, CutoffReport, HistogramMetadata, HistogramView, LogicalStreamReport,
    MeasurementAccounting, MeasurementReport, PhaseReport, QueueStreamReport, RpcReport,
    TimingReport,
};

const PHASE_PREPARATION: u8 = 0;
const PHASE_MEASUREMENT: u8 = 1;
const PHASE_DRAIN: u8 = 2;
const PHASE_FINISHED: u8 = 3;
const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const NANOS_PER_MICROSECOND: u64 = 1_000;
const HDR_SIGNIFICANT_DIGITS: u8 = 2;
const MAX_ERROR_CONTEXT: usize = 256;
pub const PREPARATION_STREAM_COUNT: usize = 7;
pub const MEASUREMENT_STREAM_COUNT: usize = 18;
pub const RPC_METHOD_COUNT: usize = 4;
const PREPARATION_LOGICAL_STREAMS: usize = PREPARATION_STREAM_COUNT;
const MEASUREMENT_LOGICAL_STREAMS: usize = MEASUREMENT_STREAM_COUNT;
const RPC_METHODS: usize = RPC_METHOD_COUNT;
/// A monotonic source used by the recorder. Production uses the same monotonic
/// clock as Tokio; tests can provide a deterministic event clock without
/// reaching into counters or mutating private recorder state.
pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioMonotonicClock;

impl MonotonicClock for TokioMonotonicClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Preparation,
    Measurement,
    Drain,
    Finished,
}

impl Phase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparation => "preparation",
            Self::Measurement => "measurement",
            Self::Drain => "drain",
            Self::Finished => "finished",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Preparation => PHASE_PREPARATION,
            Self::Measurement => PHASE_MEASUREMENT,
            Self::Drain => PHASE_DRAIN,
            Self::Finished => PHASE_FINISHED,
        }
    }

    const fn from_code(code: u8) -> Self {
        match code {
            PHASE_PREPARATION => Self::Preparation,
            PHASE_MEASUREMENT => Self::Measurement,
            PHASE_DRAIN => Self::Drain,
            PHASE_FINISHED => Self::Finished,
            _ => Self::Finished,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PreparationStream {
    Create(ResourceKind),
    NodeReady,
}

impl PreparationStream {
    pub const fn index(self) -> usize {
        match self {
            Self::Create(kind) => kind.index(),
            Self::NodeReady => 6,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create(kind) => match kind {
                ResourceKind::Pods => "create_pods",
                ResourceKind::Nodes => "create_nodes",
                ResourceKind::NodeLeases => "create_node_leases",
                ResourceKind::Deployments => "create_deployments",
                ResourceKind::ReplicaSets => "create_replicasets",
                ResourceKind::DaemonSets => "create_daemonsets",
            },
            Self::NodeReady => "node_ready",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LogicalStreamIdentity {
    Application {
        work_class: LogicalWorkClass,
        resource: ResourceKind,
    },
    Periodic {
        kind: PeriodicKind,
    },
}

impl LogicalStreamIdentity {
    pub const fn measurement_index(self) -> Option<usize> {
        match self {
            Self::Application {
                work_class: LogicalWorkClass::Update,
                resource,
            } => match resource {
                ResourceKind::Pods => Some(0),
                ResourceKind::Deployments => Some(1),
                ResourceKind::ReplicaSets => Some(2),
                ResourceKind::DaemonSets => Some(3),
                ResourceKind::Nodes | ResourceKind::NodeLeases => None,
            },
            Self::Application {
                work_class,
                resource,
            } => {
                let resource_index = match resource {
                    ResourceKind::Pods => 0,
                    ResourceKind::Nodes => 1,
                    ResourceKind::Deployments => 2,
                    ResourceKind::ReplicaSets => 3,
                    ResourceKind::DaemonSets => 4,
                    ResourceKind::NodeLeases => return None,
                };
                let shape_index = match work_class {
                    LogicalWorkClass::PointGet => 0,
                    LogicalWorkClass::BoundedList => 1,
                    LogicalWorkClass::Update => return None,
                };
                Some(4 + resource_index * 2 + shape_index)
            }
            Self::Periodic { kind } => Some(14 + kind.index()),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Application { work_class, .. } => match work_class {
                LogicalWorkClass::Update => "update",
                LogicalWorkClass::PointGet => "point_get",
                LogicalWorkClass::BoundedList => "bounded_list",
            },
            Self::Periodic { kind } => kind.as_str(),
        }
    }
}

impl PeriodicKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NodeLease => "node_lease",
            Self::NodeStatus => "node_status",
            Self::Status => "status",
            Self::Compact => "compact",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StreamId {
    Preparation(PreparationStream),
    Measurement(LogicalStreamIdentity),
}

impl StreamId {
    pub const fn preparation(stream: PreparationStream) -> Self {
        Self::Preparation(stream)
    }

    pub const fn measurement(stream: LogicalStreamIdentity) -> Self {
        Self::Measurement(stream)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RpcMethod {
    Range,
    Txn,
    Status,
    Compact,
}

impl RpcMethod {
    pub const ALL: [Self; RPC_METHODS] = [Self::Range, Self::Txn, Self::Status, Self::Compact];

    pub const fn index(self) -> usize {
        match self {
            Self::Range => 0,
            Self::Txn => 1,
            Self::Status => 2,
            Self::Compact => 3,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Txn => "txn",
            Self::Status => "status",
            Self::Compact => "compact",
        }
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum MeasurementError {
    #[error("measurement layout is invalid: {0}")]
    InvalidLayout(String),
    #[error("measurement phase transition is invalid: {0}")]
    InvalidPhase(String),
    #[error("measurement clock duration overflows u64 nanoseconds")]
    ClockOverflow,
    #[error("measurement counter overflows")]
    CounterOverflow,
    #[error("measurement histogram bounds are invalid: {0}")]
    HistogramBounds(String),
    #[error("measurement histogram allocation exceeds the compiled layout")]
    HistogramAllocationExceedsLayout,
    #[error("measurement has {0} active logical tokens")]
    ActiveTokens(u64),
    #[error("measurement has {0} active RPC attempts")]
    ActiveRpcAttempts(u64),
}

/// The recorder-owned absolute window boundary. Scheduler deadlines are
/// derived from this value so admission, cutoff and bucket timestamps share
/// one clock sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MeasurementWindow {
    pub start: Instant,
    pub cutoff: Instant,
}

#[derive(Debug)]
pub enum OfferResult {
    Token(MeasurementToken),
    /// The configured window expired normally; this slot is already counted
    /// as missed and must not be attributed to early stop by the caller.
    MissedSchedule,
    NotOfferedAfterStop,
    WrongPhase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TokenAction {
    Applied,
    RejectedAfterTick,
    RejectedAfterCutoff,
    RejectedAfterStop,
    AlreadyFinalized,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenLifecycle {
    Offered,
    Accepted {
        accepted_run_ns: u64,
    },
    Started {
        accepted_run_ns: u64,
        started_run_ns: u64,
    },
    Terminal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TokenStream {
    phase: Phase,
    index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClockSample {
    run_ns: u64,
    measurement_ns: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
struct ErrorSlot {
    len: u16,
    code: u64,
    bytes: [u8; MAX_ERROR_CONTEXT],
}

impl Default for ErrorSlot {
    fn default() -> Self {
        Self {
            len: 0,
            code: 0,
            bytes: [0; MAX_ERROR_CONTEXT],
        }
    }
}

impl ErrorSlot {
    fn set(&mut self, code: u64, message: &str) {
        if self.len != 0 {
            return;
        }
        let mut length = 0usize;
        for character in message.chars() {
            let mut encoded = [0u8; 4];
            let bytes = character.encode_utf8(&mut encoded).as_bytes();
            if length + bytes.len() > MAX_ERROR_CONTEXT {
                break;
            }
            self.bytes[length..length + bytes.len()].copy_from_slice(bytes);
            length += bytes.len();
        }
        self.code = code;
        self.len = u16::try_from(length).unwrap_or(u16::MAX);
    }

    fn view(&self) -> Option<String> {
        if self.len == 0 {
            return None;
        }
        Some(String::from_utf8_lossy(&self.bytes[..usize::from(self.len)]).into_owned())
    }
}

struct HistogramSlot {
    histogram: Histogram<u64>,
    metadata: HistogramCounterStorage,
}

impl HistogramSlot {
    fn new(plan: HistogramPlan) -> Result<Self, MeasurementError> {
        let mut histogram =
            Histogram::<u64>::new_with_bounds(plan.low_us, plan.high_us, HDR_SIGNIFICANT_DIGITS)
                .map_err(|error| MeasurementError::HistogramBounds(error.to_string()))?;
        histogram.auto(false);
        Ok(Self {
            histogram,
            metadata: HistogramCounterStorage::default(),
        })
    }

    fn record_duration(&mut self, duration_ns: u64) -> Result<(), MeasurementError> {
        let micros = u64::try_from(
            (u128::from(duration_ns) + u128::from(NANOS_PER_MICROSECOND - 1))
                / u128::from(NANOS_PER_MICROSECOND),
        )
        .map_err(|_| MeasurementError::ClockOverflow)?
        .max(1);
        self.record_us(micros)
    }

    fn record_us(&mut self, micros: u64) -> Result<(), MeasurementError> {
        if micros > self.histogram.high() {
            self.metadata.overflow = self
                .metadata
                .overflow
                .checked_add(1)
                .ok_or(MeasurementError::CounterOverflow)?;
            self.metadata.exact_max_us = self.metadata.exact_max_us.max(micros);
            return Ok(());
        }
        if self.histogram.record(micros).is_err() {
            self.metadata.overflow = self
                .metadata
                .overflow
                .checked_add(1)
                .ok_or(MeasurementError::CounterOverflow)?;
            self.metadata.exact_max_us = self.metadata.exact_max_us.max(micros);
            return Ok(());
        }
        self.metadata.exact_max_us = self.metadata.exact_max_us.max(micros);
        Ok(())
    }

    fn view(&self) -> HistogramView {
        let sample_count = self.histogram.len();
        let has_samples = sample_count > 0 || self.metadata.overflow > 0;
        let unavailable = self.metadata.overflow > 0;
        let quantile = |quantile: f64| {
            if sample_count == 0 || unavailable {
                None
            } else {
                Some(self.histogram.value_at_quantile(quantile))
            }
        };
        HistogramView {
            low_us: self.histogram.low(),
            high_us: self.histogram.high(),
            significant_digits: HDR_SIGNIFICANT_DIGITS,
            auto_resize: false,
            coordinated_omission: false,
            sample_count,
            p50_us: quantile(0.50),
            p90_us: quantile(0.90),
            p99_us: quantile(0.99),
            max_us: if has_samples {
                Some(self.metadata.exact_max_us)
            } else {
                None
            },
            overflow: self.metadata.overflow,
            exact_max_us: if has_samples {
                Some(self.metadata.exact_max_us)
            } else {
                None
            },
            quantiles_unavailable: unavailable,
        }
    }

    fn histogram_backing_bytes(&self) -> Result<u64, MeasurementError> {
        let count_size =
            u64::try_from(size_of::<u64>()).map_err(|_| MeasurementError::ClockOverflow)?;
        u64::try_from(self.histogram.distinct_values())
            .map_err(|_| MeasurementError::ClockOverflow)?
            .checked_mul(count_size)
            .ok_or(MeasurementError::CounterOverflow)
    }

    fn allocated_bytes(&self) -> Result<u64, MeasurementError> {
        let element_size = u64::try_from(size_of::<HistogramSlot>())
            .map_err(|_| MeasurementError::ClockOverflow)?;
        element_size
            .checked_add(self.histogram_backing_bytes()?)
            .ok_or(MeasurementError::CounterOverflow)
    }
}

struct PhaseData {
    logical: Vec<CounterStorage>,
    queue: Vec<CounterStorage>,
    rpc: Vec<CounterStorage>,
    logical_histograms: Vec<HistogramSlot>,
    queue_histograms: Vec<HistogramSlot>,
    rpc_histograms: Vec<HistogramSlot>,
    logical_total: HistogramSlot,
    queue_total: HistogramSlot,
    rpc_total: HistogramSlot,
    errors: Vec<ErrorSlot>,
}

impl PhaseData {
    fn histogram_vector(
        count: usize,
        plan: HistogramPlan,
    ) -> Result<Vec<HistogramSlot>, MeasurementError> {
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|error| {
            MeasurementError::InvalidLayout(format!("histogram vector allocation failed: {error}"))
        })?;
        for _ in 0..count {
            values.push(HistogramSlot::new(plan)?);
        }
        if values.capacity() != count {
            return Err(MeasurementError::InvalidLayout(
                "histogram vector capacity exceeds compiled count".into(),
            ));
        }
        Ok(values)
    }

    fn new(
        logical_count: usize,
        queue_count: usize,
        rpc_count: usize,
        logical_plan: HistogramPlan,
        queue_plan: HistogramPlan,
        rpc_plan: HistogramPlan,
    ) -> Result<Self, MeasurementError> {
        let logical_histograms = Self::histogram_vector(logical_count, logical_plan)?;
        let queue_histograms = Self::histogram_vector(queue_count, queue_plan)?;
        let rpc_histograms = Self::histogram_vector(rpc_count, rpc_plan)?;
        Ok(Self {
            logical: vec![CounterStorage::default(); logical_count],
            queue: vec![CounterStorage::default(); queue_count],
            rpc: vec![CounterStorage::default(); rpc_count],
            logical_histograms,
            queue_histograms,
            rpc_histograms,
            logical_total: HistogramSlot::new(logical_plan)?,
            queue_total: HistogramSlot::new(queue_plan)?,
            rpc_total: HistogramSlot::new(rpc_plan)?,
            errors: vec![ErrorSlot::default(); logical_count],
        })
    }

    fn allocated_bytes(&self) -> Result<u64, MeasurementError> {
        let counter_size = u64::try_from(size_of::<CounterStorage>())
            .map_err(|_| MeasurementError::ClockOverflow)?;
        let error_size =
            u64::try_from(size_of::<ErrorSlot>()).map_err(|_| MeasurementError::ClockOverflow)?;
        let histogram_slot_size = u64::try_from(size_of::<HistogramSlot>())
            .map_err(|_| MeasurementError::ClockOverflow)?;
        let mut bytes = 0u64;
        for vector in [&self.logical, &self.queue, &self.rpc] {
            bytes = bytes
                .checked_add(
                    u64::try_from(vector.capacity())
                        .map_err(|_| MeasurementError::ClockOverflow)?
                        .checked_mul(counter_size)
                        .ok_or(MeasurementError::CounterOverflow)?,
                )
                .ok_or(MeasurementError::CounterOverflow)?;
        }
        bytes = bytes
            .checked_add(
                u64::try_from(self.errors.capacity())
                    .map_err(|_| MeasurementError::ClockOverflow)?
                    .checked_mul(error_size)
                    .ok_or(MeasurementError::CounterOverflow)?,
            )
            .ok_or(MeasurementError::CounterOverflow)?;
        for vector in [
            &self.logical_histograms,
            &self.queue_histograms,
            &self.rpc_histograms,
        ] {
            bytes = bytes
                .checked_add(
                    u64::try_from(vector.capacity())
                        .map_err(|_| MeasurementError::ClockOverflow)?
                        .checked_mul(histogram_slot_size)
                        .ok_or(MeasurementError::CounterOverflow)?,
                )
                .ok_or(MeasurementError::CounterOverflow)?;
            for histogram in vector {
                bytes = bytes
                    .checked_add(histogram.histogram_backing_bytes()?)
                    .ok_or(MeasurementError::CounterOverflow)?;
            }
        }
        for histogram in [&self.logical_total, &self.queue_total, &self.rpc_total] {
            bytes = bytes
                .checked_add(histogram.allocated_bytes()?)
                .ok_or(MeasurementError::CounterOverflow)?;
        }
        Ok(bytes)
    }
}

struct BucketData {
    counters: BucketCounterStorage,
    logical: HistogramSlot,
    queue: HistogramSlot,
    rpc: HistogramSlot,
}

impl BucketData {
    fn new(layout: &MeasurementLayout) -> Result<Self, MeasurementError> {
        Ok(Self {
            counters: BucketCounterStorage::default(),
            logical: HistogramSlot::new(layout.measurement_logical_histogram)?,
            queue: HistogramSlot::new(layout.measurement_queue_histogram)?,
            rpc: HistogramSlot::new(layout.measurement_rpc_histogram)?,
        })
    }

    fn allocated_bytes(&self) -> Result<u64, MeasurementError> {
        let counter_size = u64::try_from(size_of::<BucketCounterStorage>())
            .map_err(|_| MeasurementError::ClockOverflow)?;
        counter_size
            .checked_add(self.logical.allocated_bytes()?)
            .and_then(|value| value.checked_add(self.queue.allocated_bytes().ok()?))
            .and_then(|value| value.checked_add(self.rpc.allocated_bytes().ok()?))
            .ok_or(MeasurementError::CounterOverflow)
    }
}

struct MeasurementState {
    clock: PhaseClockStorage,
    preparation: PhaseData,
    measurement: PhaseData,
    drain: PhaseData,
    buckets: Vec<BucketData>,
    planned_preparation: [u64; PREPARATION_LOGICAL_STREAMS],
    planned_measurement: [u64; MEASUREMENT_LOGICAL_STREAMS],
    active_tokens: u64,
    active_rpc_attempts: u64,
    arithmetic_overflow: bool,
}

impl MeasurementState {
    fn add(value: &mut u64, amount: u64) -> Result<(), MeasurementError> {
        *value = value
            .checked_add(amount)
            .ok_or(MeasurementError::CounterOverflow)?;
        Ok(())
    }

    fn phase(&self) -> Phase {
        Phase::from_code(self.clock.phase)
    }

    fn phase_data(&self, phase: Phase) -> &PhaseData {
        match phase {
            Phase::Preparation => &self.preparation,
            Phase::Measurement => &self.measurement,
            Phase::Drain => &self.drain,
            Phase::Finished => &self.drain,
        }
    }

    fn phase_data_mut(&mut self, phase: Phase) -> &mut PhaseData {
        match phase {
            Phase::Preparation => &mut self.preparation,
            Phase::Measurement => &mut self.measurement,
            Phase::Drain | Phase::Finished => &mut self.drain,
        }
    }
}

struct RecorderInner {
    layout: MeasurementLayout,
    clock: Arc<dyn MonotonicClock>,
    state: Mutex<MeasurementState>,
}

fn lock_recover<'a, T>(mutex: &'a Mutex<T>) -> MutexGuard<'a, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn recorder_metadata_bytes() -> Option<u64> {
    u64::try_from(size_of::<RecorderInner>()).ok()
}

pub(crate) fn error_storage_size_bytes() -> u64 {
    u64::try_from(size_of::<ErrorSlot>()).unwrap_or(u64::MAX)
}

pub(crate) fn token_storage_size_bytes() -> u64 {
    u64::try_from(size_of::<MeasurementToken>()).unwrap_or(u64::MAX)
}

pub(crate) fn histogram_slot_size_bytes() -> Option<u64> {
    u64::try_from(size_of::<HistogramSlot>()).ok()
}

#[derive(Clone)]
pub struct MeasurementRecorder {
    inner: Arc<RecorderInner>,
}

impl fmt::Debug for MeasurementRecorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MeasurementRecorder")
            .field("phase", &self.phase())
            .field("layout", &self.inner.layout)
            .finish_non_exhaustive()
    }
}

impl MeasurementRecorder {
    fn add(value: &mut u64, amount: u64) -> Result<(), MeasurementError> {
        MeasurementState::add(value, amount)
    }

    fn set_error(data: &mut PhaseData, index: usize, code: u64, message: &str) {
        let first = data.errors[index].len == 0;
        data.errors[index].set(code, message);
        if first {
            data.logical[index].first_error_code = code;
        }
    }

    pub fn new(plan: &CompiledPlan) -> Result<Self, MeasurementError> {
        Self::with_clock(plan, Arc::new(TokioMonotonicClock))
    }

    pub fn new_with_clock(
        plan: &CompiledPlan,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self, MeasurementError> {
        Self::with_clock(plan, clock)
    }

    pub fn with_clock(
        plan: &CompiledPlan,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self, MeasurementError> {
        let layout = plan.measurement_layout.clone();
        if layout.logical_streams_per_phase != u64::try_from(MEASUREMENT_LOGICAL_STREAMS).unwrap()
            || layout.queue_streams_per_phase != u64::try_from(MEASUREMENT_LOGICAL_STREAMS).unwrap()
            || layout.rpc_methods_per_phase != u64::try_from(RPC_METHODS).unwrap()
            || layout.prepare_logical_streams != u64::try_from(PREPARATION_LOGICAL_STREAMS).unwrap()
        {
            return Err(MeasurementError::InvalidLayout(
                "compiled stream counts do not match fixed recorder identities".into(),
            ));
        }
        if plan.logical_streams.len() != 14 || plan.periodic_plan.len() != 4 {
            return Err(MeasurementError::InvalidLayout(
                "compiled plan does not contain the fixed logical stream shape".into(),
            ));
        }
        let start = clock.now();
        let configured_duration_ns = plan
            .config
            .load
            .duration_ms
            .checked_mul(NANOS_PER_MILLISECOND)
            .ok_or(MeasurementError::ClockOverflow)?;
        let clock_state = PhaseClockStorage {
            start,
            last: start,
            measurement_start: None,
            configured_cutoff_ns: configured_duration_ns,
            effective_cutoff_ns: None,
            preparation_end_ns: None,
            drain_start_ns: None,
            drain_end_ns: None,
            phase: PHASE_PREPARATION,
        };
        let mut planned_measurement = [0u64; MEASUREMENT_LOGICAL_STREAMS];
        let mut seen = [false; MEASUREMENT_LOGICAL_STREAMS];
        for stream in &plan.logical_streams {
            let identity = LogicalStreamIdentity::Application {
                work_class: stream.work_class,
                resource: stream.resource,
            };
            let index = identity.measurement_index().ok_or_else(|| {
                MeasurementError::InvalidLayout("application stream has no fixed identity".into())
            })?;
            if seen[index] {
                return Err(MeasurementError::InvalidLayout(
                    "compiled plan contains duplicate logical stream identities".into(),
                ));
            }
            seen[index] = true;
            planned_measurement[index] = stream.planned;
        }
        for stream in &plan.periodic_plan {
            let identity = LogicalStreamIdentity::Periodic { kind: stream.kind };
            let index = identity
                .measurement_index()
                .expect("periodic stream identities always have an index");
            if seen[index] {
                return Err(MeasurementError::InvalidLayout(
                    "compiled plan contains duplicate periodic stream identities".into(),
                ));
            }
            seen[index] = true;
            planned_measurement[index] = stream.planned;
        }
        if seen.iter().any(|seen| !seen) {
            return Err(MeasurementError::InvalidLayout(
                "compiled plan is missing a fixed logical stream identity".into(),
            ));
        }
        let planned_preparation = [
            plan.resource_counts.pods,
            plan.resource_counts.nodes,
            plan.resource_counts.node_leases,
            plan.resource_counts.deployments,
            plan.resource_counts.replicasets,
            plan.resource_counts.daemonsets,
            plan.resource_counts.nodes,
        ];
        let preparation = PhaseData::new(
            PREPARATION_LOGICAL_STREAMS,
            PREPARATION_LOGICAL_STREAMS,
            RPC_METHODS,
            layout.prepare_logical_histogram,
            layout.prepare_queue_histogram,
            layout.prepare_rpc_histogram,
        )?;
        let measurement = PhaseData::new(
            MEASUREMENT_LOGICAL_STREAMS,
            MEASUREMENT_LOGICAL_STREAMS,
            RPC_METHODS,
            layout.measurement_logical_histogram,
            layout.measurement_queue_histogram,
            layout.measurement_rpc_histogram,
        )?;
        let drain = PhaseData::new(
            MEASUREMENT_LOGICAL_STREAMS,
            MEASUREMENT_LOGICAL_STREAMS,
            RPC_METHODS,
            layout.measurement_logical_histogram,
            layout.measurement_queue_histogram,
            layout.measurement_rpc_histogram,
        )?;
        let bucket_count = usize::try_from(layout.bucket_count)
            .map_err(|_| MeasurementError::InvalidLayout("bucket count exceeds usize".into()))?;
        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(BucketData::new(&layout)?);
        }
        if buckets.capacity() > bucket_count {
            return Err(MeasurementError::InvalidLayout(
                "bucket capacity exceeds compiled bucket count".into(),
            ));
        }
        let state = MeasurementState {
            clock: clock_state,
            preparation,
            measurement,
            drain,
            buckets,
            planned_preparation,
            planned_measurement,
            active_tokens: 0,
            active_rpc_attempts: 0,
            arithmetic_overflow: false,
        };
        let recorder = Self {
            inner: Arc::new(RecorderInner {
                layout,
                clock,
                state: Mutex::new(state),
            }),
        };
        if recorder.allocated_bytes()? > recorder.inner.layout.estimated_measurement_memory_bytes {
            return Err(MeasurementError::InvalidLayout(
                "actual preallocated storage exceeds compiled measurement budget".into(),
            ));
        }
        Ok(recorder)
    }

    pub fn layout(&self) -> &MeasurementLayout {
        &self.inner.layout
    }

    pub fn phase(&self) -> Phase {
        lock_recover(&self.inner.state).phase()
    }

    pub fn allocated_bytes(&self) -> Result<u64, MeasurementError> {
        let state = lock_recover(&self.inner.state);
        let mut bytes = recorder_metadata_bytes().ok_or(MeasurementError::ClockOverflow)?;
        for phase in [&state.preparation, &state.measurement, &state.drain] {
            bytes = bytes
                .checked_add(phase.allocated_bytes()?)
                .ok_or(MeasurementError::CounterOverflow)?;
        }
        for bucket in &state.buckets {
            bytes = bytes
                .checked_add(bucket.allocated_bytes()?)
                .ok_or(MeasurementError::CounterOverflow)?;
        }
        Ok(bytes)
    }

    pub fn start_measurement(&self) -> Result<MeasurementWindow, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.phase() != Phase::Preparation {
            return Err(MeasurementError::InvalidPhase(
                "measurement must start from preparation".into(),
            ));
        }
        let bucket_ns = self.inner.layout.bucket_width_ns;
        if bucket_ns == 0 {
            return Err(MeasurementError::InvalidLayout(
                "measurement bucket width must be non-zero".into(),
            ));
        }
        let configured_cutoff_ns = state.clock.configured_cutoff_ns;
        for (index, bucket) in state.buckets.iter_mut().enumerate() {
            let start_ns = u64::try_from(index)
                .map_err(|_| MeasurementError::ClockOverflow)?
                .checked_mul(bucket_ns)
                .ok_or(MeasurementError::ClockOverflow)?;
            let end_ns = start_ns
                .checked_add(bucket_ns)
                .ok_or(MeasurementError::ClockOverflow)?
                .min(configured_cutoff_ns);
            bucket.counters.start_ns = start_ns;
            bucket.counters.end_ns = end_ns;
        }
        // Bucket bounds are fixed metadata, not pressure time. Take the
        // authoritative epoch sample only after all bucket initialization.
        let sample = self.sample_now(&mut state)?;
        state.clock.preparation_end_ns = Some(sample.run_ns);
        let measurement_start = state.clock.last;
        let cutoff = measurement_start
            .checked_add(Duration::from_nanos(configured_cutoff_ns))
            .ok_or(MeasurementError::ClockOverflow)?;
        state.clock.measurement_start = Some(measurement_start);
        state.clock.phase = PHASE_MEASUREMENT;
        Ok(MeasurementWindow {
            start: measurement_start,
            cutoff,
        })
    }

    pub fn stop_measurement(&self) -> Result<Cutoff, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.phase() == Phase::Drain || state.phase() == Phase::Finished {
            return Ok(Cutoff {
                configured_ns: state.clock.configured_cutoff_ns,
                effective_ns: state.clock.effective_cutoff_ns.unwrap_or(0),
            });
        }
        if state.phase() != Phase::Measurement {
            return Err(MeasurementError::InvalidPhase(
                "measurement must start before it can stop".into(),
            ));
        }
        let sample = self.sample_now(&mut state)?;
        let effective_ns = sample
            .measurement_ns
            .unwrap_or(0)
            .min(state.clock.configured_cutoff_ns);
        self.freeze_cutoff(&mut state, sample.run_ns, effective_ns)?;
        Ok(Cutoff {
            configured_ns: state.clock.configured_cutoff_ns,
            effective_ns,
        })
    }

    pub fn finish(&self) -> Result<MeasurementReport, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.clock.measurement_start.is_none() {
            return Err(MeasurementError::InvalidPhase(
                "measurement has not started".into(),
            ));
        }
        if state.phase() == Phase::Measurement {
            let sample = self.sample_now(&mut state)?;
            let effective_ns = sample
                .measurement_ns
                .unwrap_or(0)
                .min(state.clock.configured_cutoff_ns);
            self.freeze_cutoff(&mut state, sample.run_ns, effective_ns)?;
        }
        if state.active_tokens != 0 {
            return Err(MeasurementError::ActiveTokens(state.active_tokens));
        }
        if state.active_rpc_attempts != 0 {
            return Err(MeasurementError::ActiveRpcAttempts(
                state.active_rpc_attempts,
            ));
        }
        if state.phase() == Phase::Drain {
            let sample = self.sample_now(&mut state)?;
            state.clock.drain_end_ns = Some(sample.run_ns);
            state.clock.phase = PHASE_FINISHED;
        }
        Ok(self.build_report(&state))
    }

    pub fn report(&self) -> Result<MeasurementReport, MeasurementError> {
        let state = lock_recover(&self.inner.state);
        if state.phase() != Phase::Finished {
            return Err(MeasurementError::InvalidPhase(
                "call finish after drain and token join before reporting".into(),
            ));
        }
        if state.active_tokens != 0 {
            return Err(MeasurementError::ActiveTokens(state.active_tokens));
        }
        if state.active_rpc_attempts != 0 {
            return Err(MeasurementError::ActiveRpcAttempts(
                state.active_rpc_attempts,
            ));
        }
        Ok(self.build_report(&state))
    }

    pub fn into_report(self) -> Result<MeasurementReport, MeasurementError> {
        self.report()
    }

    pub fn offer(&self, stream: LogicalStreamIdentity) -> Result<OfferResult, MeasurementError> {
        self.offer_in(Phase::Measurement, StreamId::Measurement(stream))
    }

    pub fn offer_preparation(
        &self,
        stream: PreparationStream,
    ) -> Result<OfferResult, MeasurementError> {
        self.offer_in(Phase::Preparation, StreamId::Preparation(stream))
    }

    pub fn offer_in(
        &self,
        phase: Phase,
        stream: StreamId,
    ) -> Result<OfferResult, MeasurementError> {
        let index = match stream {
            StreamId::Preparation(value) => {
                if phase != Phase::Preparation {
                    return Ok(OfferResult::WrongPhase);
                }
                value.index()
            }
            StreamId::Measurement(value) => {
                if phase != Phase::Measurement {
                    return Ok(OfferResult::WrongPhase);
                }
                value.measurement_index().ok_or_else(|| {
                    MeasurementError::InvalidLayout("logical stream has no fixed index".into())
                })?
            }
        };
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        if state.phase() != phase {
            if phase == Phase::Measurement && state.clock.measurement_start.is_some() {
                let normal_cutoff =
                    state.clock.effective_cutoff_ns == Some(state.clock.configured_cutoff_ns);
                let counter = if normal_cutoff {
                    &mut state.measurement.logical[index].missed_schedule
                } else {
                    &mut state.measurement.logical[index].not_offered_after_stop
                };
                Self::add(counter, 1)?;
                return Ok(if normal_cutoff {
                    OfferResult::MissedSchedule
                } else {
                    OfferResult::NotOfferedAfterStop
                });
            }
            return Ok(OfferResult::WrongPhase);
        }
        if phase == Phase::Measurement {
            let measurement_ns = sample.measurement_ns.unwrap_or(0);
            let cutoff = state.clock.configured_cutoff_ns;
            if measurement_ns >= cutoff {
                self.freeze_cutoff(&mut state, sample.run_ns, cutoff)?;
                Self::add(&mut state.measurement.logical[index].missed_schedule, 1)?;
                return Ok(OfferResult::MissedSchedule);
            }
        }
        Self::add(&mut state.active_tokens, 1)?;
        Ok(OfferResult::Token(MeasurementToken {
            recorder: self.clone(),
            stream: TokenStream { phase, index },
            lifecycle: Mutex::new(TokenLifecycle::Offered),
        }))
    }

    pub fn record_missed_schedule(
        &self,
        stream: LogicalStreamIdentity,
        count: u64,
    ) -> Result<(), MeasurementError> {
        let index = stream
            .measurement_index()
            .ok_or_else(|| MeasurementError::InvalidLayout("logical stream has no index".into()))?;
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        if state.phase() == Phase::Preparation {
            return Err(MeasurementError::InvalidPhase(
                "missed schedule requires an active measurement window".into(),
            ));
        }
        if state.phase() != Phase::Measurement {
            let normal_cutoff =
                state.clock.effective_cutoff_ns == Some(state.clock.configured_cutoff_ns);
            let counter = if normal_cutoff {
                &mut state.measurement.logical[index].missed_schedule
            } else {
                &mut state.measurement.logical[index].not_offered_after_stop
            };
            Self::add(counter, count)?;
            return Ok(());
        }
        let measurement_ns = sample.measurement_ns.unwrap_or(0);
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        if measurement_ns >= cutoff {
            self.freeze_cutoff(&mut state, sample.run_ns, cutoff)?;
            let normal_cutoff =
                state.clock.effective_cutoff_ns == Some(state.clock.configured_cutoff_ns);
            let counter = if normal_cutoff {
                &mut state.measurement.logical[index].missed_schedule
            } else {
                &mut state.measurement.logical[index].not_offered_after_stop
            };
            Self::add(counter, count)?;
        } else {
            Self::add(&mut state.measurement.logical[index].missed_schedule, count)?;
        }
        Ok(())
    }

    /// Record slots known by the dispatcher to have scheduled before the
    /// effective cutoff. This remains a missed count even when discovery of a
    /// skipped batch occurs after the normal cutoff.
    pub fn record_missed_schedule_before_cutoff(
        &self,
        stream: LogicalStreamIdentity,
        count: u64,
    ) -> Result<(), MeasurementError> {
        let index = stream
            .measurement_index()
            .ok_or_else(|| MeasurementError::InvalidLayout("logical stream has no index".into()))?;
        let mut state = lock_recover(&self.inner.state);
        if state.phase() == Phase::Preparation {
            return Err(MeasurementError::InvalidPhase(
                "missed schedule requires an active measurement window".into(),
            ));
        }
        let _ = self.sample_now(&mut state)?;
        Self::add(&mut state.measurement.logical[index].missed_schedule, count)?;
        Ok(())
    }

    pub fn record_not_offered_after_stop(
        &self,
        stream: LogicalStreamIdentity,
        count: u64,
    ) -> Result<(), MeasurementError> {
        let index = stream
            .measurement_index()
            .ok_or_else(|| MeasurementError::InvalidLayout("logical stream has no index".into()))?;
        let mut state = lock_recover(&self.inner.state);
        if state.phase() == Phase::Measurement {
            let sample = self.sample_now(&mut state)?;
            let cutoff = sample
                .measurement_ns
                .unwrap_or(0)
                .min(state.clock.configured_cutoff_ns);
            self.freeze_cutoff(&mut state, sample.run_ns, cutoff)?;
        }
        if state.phase() != Phase::Drain && state.phase() != Phase::Finished {
            return Err(MeasurementError::InvalidPhase(
                "not-offered-after-stop requires a stopped measurement".into(),
            ));
        }
        Self::add(
            &mut state.measurement.logical[index].not_offered_after_stop,
            count,
        )?;
        Ok(())
    }

    pub fn record_error(
        &self,
        stream: LogicalStreamIdentity,
        code: u64,
        message: &str,
    ) -> Result<(), MeasurementError> {
        let index = stream
            .measurement_index()
            .ok_or_else(|| MeasurementError::InvalidLayout("logical stream has no index".into()))?;
        let mut state = lock_recover(&self.inner.state);
        Self::set_error(&mut state.measurement, index, code, message);
        Ok(())
    }

    pub fn record_preparation_error(
        &self,
        stream: PreparationStream,
        code: u64,
        message: &str,
    ) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let index = stream.index();
        Self::set_error(&mut state.preparation, index, code, message);
        Ok(())
    }

    pub fn start_rpc(&self, method: RpcMethod) -> Result<RpcAttempt, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let (phase, index) = match state.phase() {
            Phase::Preparation => (Phase::Preparation, method.index()),
            Phase::Measurement => {
                let measurement_ns = sample.measurement_ns.unwrap_or(0);
                let cutoff = state
                    .clock
                    .effective_cutoff_ns
                    .unwrap_or(state.clock.configured_cutoff_ns);
                if measurement_ns >= cutoff {
                    self.freeze_cutoff(&mut state, sample.run_ns, cutoff)?;
                    (Phase::Drain, method.index())
                } else {
                    (Phase::Measurement, method.index())
                }
            }
            Phase::Drain => (Phase::Drain, method.index()),
            Phase::Finished => {
                return Err(MeasurementError::InvalidPhase(
                    "cannot start RPC after reporting".into(),
                ));
            }
        };
        let counter = &mut state.phase_data_mut(phase).rpc[index];
        Self::add(&mut counter.rpc_started, 1)?;
        if phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.rpc_started, 1)?;
            }
        }
        Self::add(&mut state.active_rpc_attempts, 1)?;
        Ok(RpcAttempt {
            recorder: self.clone(),
            method,
            start_phase: phase,
            start_run_ns: sample.run_ns,
            lifecycle: Mutex::new(RpcLifecycle::Open),
        })
    }

    fn sample_now(&self, state: &mut MeasurementState) -> Result<ClockSample, MeasurementError> {
        let mut now = self.inner.clock.now();
        if now < state.clock.last {
            now = state.clock.last;
        }
        state.clock.last = now;
        let run_ns = u64::try_from(now.duration_since(state.clock.start).as_nanos())
            .map_err(|_| MeasurementError::ClockOverflow)?;
        let measurement_ns = state
            .clock
            .measurement_start
            .map(|start| {
                u64::try_from(now.duration_since(start).as_nanos())
                    .map_err(|_| MeasurementError::ClockOverflow)
            })
            .transpose()?;
        Ok(ClockSample {
            run_ns,
            measurement_ns,
        })
    }

    fn bucket_width_ns(&self) -> Result<u64, MeasurementError> {
        let width = self.inner.layout.bucket_width_ns;
        if width == 0 || self.inner.layout.bucket_count == 0 {
            return Err(MeasurementError::InvalidLayout(
                "measurement must have at least one non-empty bucket".into(),
            ));
        }
        Ok(width)
    }

    fn freeze_cutoff(
        &self,
        state: &mut MeasurementState,
        run_ns: u64,
        effective_ns: u64,
    ) -> Result<(), MeasurementError> {
        if state.clock.effective_cutoff_ns.is_none() {
            state.clock.effective_cutoff_ns = Some(effective_ns);
            state.clock.drain_start_ns = Some(run_ns);
            state.clock.phase = PHASE_DRAIN;
            for (index, bucket) in state.buckets.iter_mut().enumerate() {
                let nominal_start = u64::try_from(index)
                    .map_err(|_| MeasurementError::ClockOverflow)?
                    .checked_mul(self.bucket_width_ns()?)
                    .ok_or(MeasurementError::ClockOverflow)?;
                let nominal_end = nominal_start
                    .checked_add(self.bucket_width_ns()?)
                    .ok_or(MeasurementError::ClockOverflow)?;
                bucket.counters.start_ns = nominal_start.min(effective_ns);
                bucket.counters.end_ns =
                    nominal_end.min(effective_ns).max(bucket.counters.start_ns);
            }
        }
        Ok(())
    }

    fn bucket_mut<'a>(
        &self,
        state: &'a mut MeasurementState,
        measurement_ns: u64,
    ) -> Option<&'a mut BucketData> {
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        if measurement_ns >= cutoff {
            return None;
        }
        let width = self.bucket_width_ns().ok()?;
        let index = usize::try_from(measurement_ns / width).ok()?;
        state.buckets.get_mut(index)
    }

    fn build_report(&self, state: &MeasurementState) -> MeasurementReport {
        let preparation = phase_report(
            Phase::Preparation,
            &state.preparation,
            &state.planned_preparation,
            None,
        );
        let measurement = phase_report(
            Phase::Measurement,
            &state.measurement,
            &state.planned_measurement,
            Some(&state.drain),
        );
        let drain_planned = [0u64; MEASUREMENT_LOGICAL_STREAMS];
        let drain = phase_report(Phase::Drain, &state.drain, &drain_planned, None);
        let effective_ns = state.clock.effective_cutoff_ns;
        let cutoff_queued = state
            .measurement
            .logical
            .iter()
            .map(|counter| {
                counter
                    .accepted
                    .saturating_sub(counter.started_before_cutoff)
            })
            .fold(0u64, u64::saturating_add);
        let cutoff_running = state
            .measurement
            .logical
            .iter()
            .map(|counter| {
                counter.started_before_cutoff.saturating_sub(
                    counter
                        .succeeded_before_cutoff
                        .saturating_add(counter.failed_before_cutoff),
                )
            })
            .fold(0u64, u64::saturating_add);
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
        let timing = TimingReport {
            setup_ns: None,
            preparation_ns: state.clock.preparation_end_ns,
            configured_measurement_ns: state
                .clock
                .measurement_start
                .map(|_| state.clock.configured_cutoff_ns),
            observed_measurement_ns: effective_ns,
            drain_ns: state
                .clock
                .drain_start_ns
                .zip(state.clock.drain_end_ns)
                .map(|(start, end)| end.saturating_sub(start)),
        };
        let cutoff = CutoffReport {
            configured_ns: state
                .clock
                .measurement_start
                .map(|_| state.clock.configured_cutoff_ns),
            effective_ns,
            queued: cutoff_queued,
            running: cutoff_running,
        };
        let buckets = state
            .buckets
            .iter()
            .map(|bucket| BucketReport {
                start_ns: bucket.counters.start_ns,
                end_ns: bucket.counters.end_ns,
                logical_offered: bucket.counters.logical_offered,
                logical_started: bucket.counters.logical_started,
                logical_completed: bucket.counters.logical_completed,
                logical_errors: bucket.counters.logical_errors,
                rpc_started: bucket.counters.rpc_started,
                rpc_completed: bucket.counters.rpc_completed,
                watch_events: bucket.counters.watch_events,
                watch_payload_bytes: bucket.counters.watch_payload_bytes,
                logical_latency: bucket.logical.view(),
                queue_latency: bucket.queue.view(),
                rpc_latency: bucket.rpc.view(),
            })
            .collect();
        MeasurementReport {
            preparation,
            measurement,
            drain,
            buckets,
            cutoff,
            timing,
            accounting: MeasurementAccounting {
                planned_conserved,
                offered_conserved,
                accepted_cohort_conserved,
                result_unknown_subset_failed,
                no_active_tokens: state.active_tokens == 0 && state.active_rpc_attempts == 0,
                no_arithmetic_overflow: !state.arithmetic_overflow,
            },
            metadata: HistogramMetadata {
                low_us: 1,
                significant_digits: HDR_SIGNIFICANT_DIGITS,
                auto_resize: false,
                coordinated_omission: false,
                overflow_is_not_truncated: true,
                duration_rounding: "max(1, ceil(duration_ns / 1000))".into(),
            },
        }
    }

    fn accept_token(
        &self,
        token: &MeasurementToken,
        tick_end_ns: Option<u64>,
    ) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&token.lifecycle);
        if !matches!(*lifecycle, TokenLifecycle::Offered) {
            return Ok(TokenAction::AlreadyFinalized);
        }
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        if token.stream.phase == Phase::Measurement {
            let measurement_ns = sample.measurement_ns.unwrap_or(0);
            let cutoff = state
                .clock
                .effective_cutoff_ns
                .unwrap_or(state.clock.configured_cutoff_ns);
            let after_cutoff = measurement_ns >= cutoff
                || state.phase() == Phase::Drain
                || state.phase() == Phase::Finished;
            let after_tick = tick_end_ns.is_some_and(|end| measurement_ns >= end);
            if after_cutoff || after_tick {
                let normal_cutoff = state.clock.effective_cutoff_ns.map_or(true, |effective| {
                    effective == state.clock.configured_cutoff_ns
                });
                let counter = if normal_cutoff {
                    &mut state.measurement.logical[token.stream.index].missed_schedule
                } else {
                    &mut state.measurement.logical[token.stream.index].not_offered_after_stop
                };
                Self::add(counter, 1)?;
                state.active_tokens = state
                    .active_tokens
                    .checked_sub(1)
                    .ok_or(MeasurementError::CounterOverflow)?;
                *lifecycle = TokenLifecycle::Terminal;
                return Ok(if after_cutoff {
                    if normal_cutoff {
                        TokenAction::RejectedAfterCutoff
                    } else {
                        TokenAction::RejectedAfterStop
                    }
                } else {
                    TokenAction::RejectedAfterTick
                });
            }
        } else if state.phase() != Phase::Preparation {
            return Err(MeasurementError::InvalidPhase(
                "preparation token outlived the preparation phase".into(),
            ));
        }
        let counter = &mut state.phase_data_mut(token.stream.phase).logical[token.stream.index];
        Self::add(&mut counter.offered, 1)?;
        Self::add(&mut counter.accepted, 1)?;
        if token.stream.phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.logical_offered, 1)?;
            }
        }
        *lifecycle = TokenLifecycle::Accepted {
            accepted_run_ns: sample.run_ns,
        };
        Ok(TokenAction::Applied)
    }

    fn drop_token(
        &self,
        token: &MeasurementToken,
        reason: DropReason,
    ) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&token.lifecycle);
        if !matches!(*lifecycle, TokenLifecycle::Offered) {
            return Ok(TokenAction::AlreadyFinalized);
        }
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        if token.stream.phase == Phase::Measurement {
            let cutoff = state
                .clock
                .effective_cutoff_ns
                .unwrap_or(state.clock.configured_cutoff_ns);
            if sample.measurement_ns.unwrap_or(0) >= cutoff
                || state.phase() == Phase::Drain
                || state.phase() == Phase::Finished
            {
                let normal_cutoff = state.clock.effective_cutoff_ns.map_or(true, |effective| {
                    effective == state.clock.configured_cutoff_ns
                });
                let counter = if normal_cutoff {
                    &mut state.measurement.logical[token.stream.index].missed_schedule
                } else {
                    &mut state.measurement.logical[token.stream.index].not_offered_after_stop
                };
                Self::add(counter, 1)?;
                state.active_tokens = state
                    .active_tokens
                    .checked_sub(1)
                    .ok_or(MeasurementError::CounterOverflow)?;
                *lifecycle = TokenLifecycle::Terminal;
                return Ok(if normal_cutoff {
                    TokenAction::RejectedAfterCutoff
                } else {
                    TokenAction::RejectedAfterStop
                });
            }
        }
        if reason == DropReason::QueueClosed {
            let counter = &mut state.phase_data_mut(token.stream.phase).logical[token.stream.index];
            Self::add(&mut counter.not_offered_after_stop, 1)?;
            state.active_tokens = state
                .active_tokens
                .checked_sub(1)
                .ok_or(MeasurementError::CounterOverflow)?;
            *lifecycle = TokenLifecycle::Terminal;
            return Ok(TokenAction::Applied);
        }
        let counter = &mut state.phase_data_mut(token.stream.phase).logical[token.stream.index];
        Self::add(&mut counter.offered, 1)?;
        match reason {
            DropReason::Backpressure => Self::add(&mut counter.dropped_full, 1)?,
            DropReason::QueueClosed => Self::add(&mut counter.dropped_closed, 1)?,
        }
        if token.stream.phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.logical_offered, 1)?;
            }
        }
        state.active_tokens = state
            .active_tokens
            .checked_sub(1)
            .ok_or(MeasurementError::CounterOverflow)?;
        *lifecycle = TokenLifecycle::Terminal;
        Ok(TokenAction::Applied)
    }

    fn start_token(&self, token: &MeasurementToken) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&token.lifecycle);
        let accepted_run_ns = match *lifecycle {
            TokenLifecycle::Accepted { accepted_run_ns } => accepted_run_ns,
            TokenLifecycle::Offered | TokenLifecycle::Started { .. } | TokenLifecycle::Terminal => {
                return Ok(TokenAction::AlreadyFinalized);
            }
        };
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let (target_phase, before_cutoff) = if token.stream.phase == Phase::Preparation {
            (Phase::Preparation, true)
        } else {
            let measurement_ns = sample.measurement_ns.unwrap_or(0);
            let cutoff = state
                .clock
                .effective_cutoff_ns
                .unwrap_or(state.clock.configured_cutoff_ns);
            (
                if measurement_ns < cutoff {
                    Phase::Measurement
                } else {
                    Phase::Drain
                },
                measurement_ns < cutoff,
            )
        };
        let counter = &mut state.phase_data_mut(target_phase).logical[token.stream.index];
        if before_cutoff {
            Self::add(&mut counter.started_before_cutoff, 1)?;
        } else {
            Self::add(&mut counter.started_after_cutoff, 1)?;
        }
        let queue_wait = sample
            .run_ns
            .checked_sub(accepted_run_ns)
            .ok_or(MeasurementError::ClockOverflow)?;
        let phase_data = state.phase_data_mut(target_phase);
        phase_data.queue_histograms[token.stream.index].record_duration(queue_wait)?;
        phase_data.queue_total.record_duration(queue_wait)?;
        if target_phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.logical_started, 1)?;
                bucket.queue.record_duration(queue_wait)?;
            }
        }
        *lifecycle = TokenLifecycle::Started {
            accepted_run_ns,
            started_run_ns: sample.run_ns,
        };
        Ok(TokenAction::Applied)
    }

    fn terminal_token(
        &self,
        token: &MeasurementToken,
        outcome: Outcome,
        error: Option<(u64, &str)>,
    ) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&token.lifecycle);
        let (accepted_run_ns, started_run_ns) = match *lifecycle {
            TokenLifecycle::Started {
                accepted_run_ns,
                started_run_ns,
            } => (accepted_run_ns, started_run_ns),
            TokenLifecycle::Offered
            | TokenLifecycle::Accepted { .. }
            | TokenLifecycle::Terminal => return Ok(TokenAction::AlreadyFinalized),
        };
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let (target_phase, before_cutoff) = if token.stream.phase == Phase::Preparation {
            (Phase::Preparation, true)
        } else {
            let measurement_ns = sample.measurement_ns.unwrap_or(0);
            let cutoff = state
                .clock
                .effective_cutoff_ns
                .unwrap_or(state.clock.configured_cutoff_ns);
            let before = measurement_ns < cutoff;
            (
                if before {
                    Phase::Measurement
                } else {
                    Phase::Drain
                },
                before,
            )
        };
        let logical_latency = sample
            .run_ns
            .checked_sub(accepted_run_ns)
            .ok_or(MeasurementError::ClockOverflow)?;
        let counter = &mut state.phase_data_mut(target_phase).logical[token.stream.index];
        if before_cutoff {
            if outcome == Outcome::Success {
                Self::add(&mut counter.succeeded_before_cutoff, 1)?;
            } else {
                Self::add(&mut counter.failed_before_cutoff, 1)?;
                if outcome == Outcome::Timeout {
                    Self::add(&mut counter.timeout_before_cutoff, 1)?;
                }
                if outcome == Outcome::ResultUnknown {
                    Self::add(&mut counter.result_unknown_before_cutoff, 1)?;
                }
            }
        } else {
            Self::add(&mut counter.terminal_after_cutoff, 1)?;
            if outcome == Outcome::Success {
                Self::add(&mut counter.succeeded_after_cutoff, 1)?;
            } else {
                Self::add(&mut counter.failed_after_cutoff, 1)?;
                if outcome == Outcome::Timeout {
                    Self::add(&mut counter.timeout_after_cutoff, 1)?;
                }
                if outcome == Outcome::ResultUnknown {
                    Self::add(&mut counter.result_unknown_after_cutoff, 1)?;
                }
            }
        }
        if let Some((code, message)) = error {
            Self::set_error(
                &mut state.phase_data_mut(token.stream.phase),
                token.stream.index,
                code,
                message,
            );
        }
        let phase_data = state.phase_data_mut(target_phase);
        phase_data.logical_histograms[token.stream.index].record_duration(logical_latency)?;
        phase_data.logical_total.record_duration(logical_latency)?;
        if before_cutoff && target_phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.logical_completed, 1)?;
                if outcome != Outcome::Success {
                    Self::add(&mut bucket.counters.logical_errors, 1)?;
                }
                bucket.logical.record_duration(logical_latency)?;
            }
        }
        state.active_tokens = state
            .active_tokens
            .checked_sub(1)
            .ok_or(MeasurementError::CounterOverflow)?;
        *lifecycle = TokenLifecycle::Terminal;
        let _ = started_run_ns;
        Ok(TokenAction::Applied)
    }

    fn miss_token(&self, token: &MeasurementToken) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&token.lifecycle);
        if !matches!(*lifecycle, TokenLifecycle::Offered) {
            return Ok(TokenAction::AlreadyFinalized);
        }
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let index = token.stream.index;
        let before_cutoff = token.stream.phase == Phase::Measurement
            && state.phase() == Phase::Measurement
            && sample.measurement_ns.unwrap_or(0)
                < state
                    .clock
                    .effective_cutoff_ns
                    .unwrap_or(state.clock.configured_cutoff_ns);
        let counter = &mut state.measurement.logical[index];
        if before_cutoff {
            Self::add(&mut counter.missed_schedule, 1)?;
        } else {
            Self::add(&mut counter.not_offered_after_stop, 1)?;
        }
        state.active_tokens = state
            .active_tokens
            .checked_sub(1)
            .ok_or(MeasurementError::CounterOverflow)?;
        *lifecycle = TokenLifecycle::Terminal;
        Ok(TokenAction::Applied)
    }

    fn cancel_token(&self, token: &MeasurementToken) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&token.lifecycle);
        let mut state = lock_recover(&self.inner.state);
        match *lifecycle {
            TokenLifecycle::Offered => {
                drop(lifecycle);
                drop(state);
                self.drop_token(token, DropReason::QueueClosed)
            }
            TokenLifecycle::Accepted { .. } => {
                Self::add(
                    &mut state.phase_data_mut(token.stream.phase).logical[token.stream.index]
                        .canceled_queued,
                    1,
                )?;
                state.active_tokens = state
                    .active_tokens
                    .checked_sub(1)
                    .ok_or(MeasurementError::CounterOverflow)?;
                *lifecycle = TokenLifecycle::Terminal;
                Ok(TokenAction::Applied)
            }
            TokenLifecycle::Started { .. } => {
                Self::add(
                    &mut state.phase_data_mut(token.stream.phase).logical[token.stream.index]
                        .canceled_running,
                    1,
                )?;
                state.active_tokens = state
                    .active_tokens
                    .checked_sub(1)
                    .ok_or(MeasurementError::CounterOverflow)?;
                *lifecycle = TokenLifecycle::Terminal;
                Ok(TokenAction::Applied)
            }
            TokenLifecycle::Terminal => Ok(TokenAction::AlreadyFinalized),
        }
    }

    fn finish_rpc(
        &self,
        attempt: &RpcAttempt,
        outcome: Outcome,
        payload_bytes: u64,
    ) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&attempt.lifecycle);
        if !matches!(*lifecycle, RpcLifecycle::Open) {
            return Ok(TokenAction::AlreadyFinalized);
        }
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let (target_phase, before_cutoff) = match state.phase() {
            Phase::Preparation => (Phase::Preparation, true),
            Phase::Measurement => {
                let measurement_ns = sample.measurement_ns.unwrap_or(0);
                let cutoff = state
                    .clock
                    .effective_cutoff_ns
                    .unwrap_or(state.clock.configured_cutoff_ns);
                if measurement_ns < cutoff {
                    (Phase::Measurement, true)
                } else {
                    self.freeze_cutoff(&mut state, sample.run_ns, cutoff)?;
                    (Phase::Drain, false)
                }
            }
            Phase::Drain | Phase::Finished => (Phase::Drain, false),
        };
        let counter = &mut state.phase_data_mut(target_phase).rpc[attempt.method.index()];
        Self::add(&mut counter.rpc_completed, 1)?;
        if outcome == Outcome::Success {
            Self::add(&mut counter.rpc_succeeded, 1)?;
        } else {
            Self::add(&mut counter.rpc_failed, 1)?;
            if outcome == Outcome::Timeout {
                Self::add(&mut counter.rpc_timeout, 1)?;
            }
            if outcome == Outcome::ResultUnknown {
                Self::add(&mut counter.rpc_result_unknown, 1)?;
            }
        }
        Self::add(&mut counter.rpc_payload_bytes, payload_bytes)?;
        let latency = sample
            .run_ns
            .checked_sub(attempt.start_run_ns)
            .ok_or(MeasurementError::ClockOverflow)?;
        let phase_data = state.phase_data_mut(target_phase);
        phase_data.rpc_histograms[attempt.method.index()].record_duration(latency)?;
        phase_data.rpc_total.record_duration(latency)?;
        if before_cutoff && target_phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.rpc_completed, 1)?;
                bucket.rpc.record_duration(latency)?;
            }
        }
        state.active_rpc_attempts = state
            .active_rpc_attempts
            .checked_sub(1)
            .ok_or(MeasurementError::CounterOverflow)?;
        *lifecycle = RpcLifecycle::Finished;
        Ok(TokenAction::Applied)
    }

    fn abandon_rpc(&self, attempt: &RpcAttempt) -> Result<TokenAction, MeasurementError> {
        let mut lifecycle = lock_recover(&attempt.lifecycle);
        if !matches!(*lifecycle, RpcLifecycle::Open) {
            return Ok(TokenAction::AlreadyFinalized);
        }
        let mut state = lock_recover(&self.inner.state);
        state.active_rpc_attempts = state
            .active_rpc_attempts
            .checked_sub(1)
            .ok_or(MeasurementError::CounterOverflow)?;
        *lifecycle = RpcLifecycle::Finished;
        Ok(TokenAction::Applied)
    }
}

pub struct MeasurementToken {
    recorder: MeasurementRecorder,
    stream: TokenStream,
    lifecycle: Mutex<TokenLifecycle>,
}

impl fmt::Debug for MeasurementToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MeasurementToken")
            .field("phase", &self.stream.phase)
            .field("index", &self.stream.index)
            .field("lifecycle", &lock_recover(&self.lifecycle))
            .finish()
    }
}

impl MeasurementToken {
    pub fn miss_before_cutoff(&self) -> Result<TokenAction, MeasurementError> {
        self.recorder.miss_token(self)
    }

    pub fn accept(&self) -> Result<TokenAction, MeasurementError> {
        self.recorder.accept_token(self, None)
    }

    /// Apply the final queue admission check against the recorder clock.
    /// `tick_end_ns` is exclusive; a token that crosses it is finalized as a
    /// missed slot and must not be sent to a worker.
    pub fn accept_for_tick(&self, tick_end_ns: u64) -> Result<TokenAction, MeasurementError> {
        self.recorder.accept_token(self, Some(tick_end_ns))
    }

    pub fn drop_for(&self, reason: DropReason) -> Result<TokenAction, MeasurementError> {
        self.recorder.drop_token(self, reason)
    }

    pub fn started(&self) -> Result<TokenAction, MeasurementError> {
        self.recorder.start_token(self)
    }

    pub fn terminal(&self, outcome: Outcome) -> Result<TokenAction, MeasurementError> {
        self.recorder.terminal_token(self, outcome, None)
    }

    pub fn terminal_with_error(
        &self,
        outcome: Outcome,
        code: u64,
        message: &str,
    ) -> Result<TokenAction, MeasurementError> {
        self.recorder
            .terminal_token(self, outcome, Some((code, message)))
    }

    pub fn cancel(&self) -> Result<TokenAction, MeasurementError> {
        self.recorder.cancel_token(self)
    }
}

impl Drop for MeasurementToken {
    fn drop(&mut self) {
        let recorder = self.recorder.clone();
        let _ = recorder.cancel_token(self);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RpcLifecycle {
    Open,
    Finished,
}

pub struct RpcAttempt {
    recorder: MeasurementRecorder,
    method: RpcMethod,
    start_phase: Phase,
    start_run_ns: u64,
    lifecycle: Mutex<RpcLifecycle>,
}

impl fmt::Debug for RpcAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcAttempt")
            .field("method", &self.method)
            .field("start_phase", &self.start_phase)
            .field("start_run_ns", &self.start_run_ns)
            .field("lifecycle", &lock_recover(&self.lifecycle))
            .finish()
    }
}

impl RpcAttempt {
    pub fn finish(
        &self,
        outcome: Outcome,
        payload_bytes: u64,
    ) -> Result<TokenAction, MeasurementError> {
        self.recorder.finish_rpc(self, outcome, payload_bytes)
    }
}

impl Drop for RpcAttempt {
    fn drop(&mut self) {
        let recorder = self.recorder.clone();
        let _ = recorder.abandon_rpc(self);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cutoff {
    pub configured_ns: u64,
    pub effective_ns: u64,
}

fn measurement_identity(index: usize) -> LogicalStreamIdentity {
    match index {
        0 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::Pods,
        },
        1 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::Deployments,
        },
        2 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::ReplicaSets,
        },
        3 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::DaemonSets,
        },
        4 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::PointGet,
            resource: ResourceKind::Pods,
        },
        5 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::BoundedList,
            resource: ResourceKind::Pods,
        },
        6 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::PointGet,
            resource: ResourceKind::Nodes,
        },
        7 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::BoundedList,
            resource: ResourceKind::Nodes,
        },
        8 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::PointGet,
            resource: ResourceKind::Deployments,
        },
        9 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::BoundedList,
            resource: ResourceKind::Deployments,
        },
        10 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::PointGet,
            resource: ResourceKind::ReplicaSets,
        },
        11 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::BoundedList,
            resource: ResourceKind::ReplicaSets,
        },
        12 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::PointGet,
            resource: ResourceKind::DaemonSets,
        },
        13 => LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::BoundedList,
            resource: ResourceKind::DaemonSets,
        },
        14 => LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::NodeLease,
        },
        15 => LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::NodeStatus,
        },
        16 => LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::Status,
        },
        17 => LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::Compact,
        },
        _ => panic!("fixed measurement stream index out of range"),
    }
}
/// Return the fixed measurement identity at `index`.  The mapping is stable
/// across runs and retains zero-weight streams so report shape never changes.
pub fn measurement_stream(index: usize) -> Option<LogicalStreamIdentity> {
    (index < MEASUREMENT_LOGICAL_STREAMS).then(|| measurement_identity(index))
}

/// Return the fixed preparation identity at `index`.
pub fn preparation_stream(index: usize) -> Option<PreparationStream> {
    (index < PREPARATION_LOGICAL_STREAMS).then(|| preparation_identity(index))
}

fn preparation_identity(index: usize) -> PreparationStream {
    match index {
        0 => PreparationStream::Create(ResourceKind::Pods),
        1 => PreparationStream::Create(ResourceKind::Nodes),
        2 => PreparationStream::Create(ResourceKind::NodeLeases),
        3 => PreparationStream::Create(ResourceKind::Deployments),
        4 => PreparationStream::Create(ResourceKind::ReplicaSets),
        5 => PreparationStream::Create(ResourceKind::DaemonSets),
        6 => PreparationStream::NodeReady,
        _ => panic!("fixed preparation stream index out of range"),
    }
}

fn identity_parts(stream: LogicalStreamIdentity) -> (String, Option<String>) {
    match stream {
        LogicalStreamIdentity::Application {
            work_class,
            resource,
        } => (
            match work_class {
                LogicalWorkClass::Update => "update",
                LogicalWorkClass::PointGet => "point_get",
                LogicalWorkClass::BoundedList => "bounded_list",
            }
            .into(),
            Some(resource.as_str().into()),
        ),
        LogicalStreamIdentity::Periodic { kind } => (
            kind.as_str().into(),
            match kind {
                PeriodicKind::NodeLease => Some(ResourceKind::NodeLeases.as_str().into()),
                PeriodicKind::NodeStatus => Some(ResourceKind::Nodes.as_str().into()),
                PeriodicKind::Status | PeriodicKind::Compact => None,
            },
        ),
    }
}

fn preparation_parts(stream: PreparationStream) -> (String, Option<String>) {
    match stream {
        PreparationStream::Create(kind) => ("create".into(), Some(kind.as_str().into())),
        PreparationStream::NodeReady => ("node_ready".into(), None),
    }
}

fn logical_report(
    counter: &CounterStorage,
    error: &ErrorSlot,
    logical_histogram: &HistogramSlot,
    queue_histogram: &HistogramSlot,
    planned: u64,
    identity: Option<LogicalStreamIdentity>,
    preparation: Option<PreparationStream>,
    overlay: Option<&CounterStorage>,
) -> LogicalStreamReport {
    let (work_class, resource) = match (identity, preparation) {
        (Some(identity), None) => identity_parts(identity),
        (None, Some(preparation)) => preparation_parts(preparation),
        _ => ("unknown".into(), None),
    };
    let terminal_after = overlay.map_or(counter.terminal_after_cutoff, |value| {
        counter
            .terminal_after_cutoff
            .saturating_add(value.terminal_after_cutoff)
    });
    let started_after = overlay.map_or(counter.started_after_cutoff, |value| {
        counter
            .started_after_cutoff
            .saturating_add(value.started_after_cutoff)
    });
    let succeeded_after = overlay.map_or(counter.succeeded_after_cutoff, |value| {
        counter
            .succeeded_after_cutoff
            .saturating_add(value.succeeded_after_cutoff)
    });
    let failed_after = overlay.map_or(counter.failed_after_cutoff, |value| {
        counter
            .failed_after_cutoff
            .saturating_add(value.failed_after_cutoff)
    });
    let timeout_after = overlay.map_or(counter.timeout_after_cutoff, |value| {
        counter
            .timeout_after_cutoff
            .saturating_add(value.timeout_after_cutoff)
    });
    let unknown_after = overlay.map_or(counter.result_unknown_after_cutoff, |value| {
        counter
            .result_unknown_after_cutoff
            .saturating_add(value.result_unknown_after_cutoff)
    });
    let terminal_before = counter
        .succeeded_before_cutoff
        .saturating_add(counter.failed_before_cutoff);
    let cutoff_queued = counter
        .accepted
        .saturating_sub(counter.started_before_cutoff);
    let cutoff_running = counter
        .started_before_cutoff
        .saturating_sub(terminal_before);
    LogicalStreamReport {
        work_class,
        resource,
        planned,
        offered: counter.offered,
        accepted: counter.accepted,
        dropped_full: counter.dropped_full,
        dropped_closed: counter.dropped_closed,
        missed_schedule: counter.missed_schedule,
        not_offered_after_stop: counter.not_offered_after_stop,
        started_before_cutoff: counter.started_before_cutoff,
        started_after_cutoff: started_after,
        succeeded_before_cutoff: counter.succeeded_before_cutoff,
        failed_before_cutoff: counter.failed_before_cutoff,
        succeeded_after_cutoff: succeeded_after,
        failed_after_cutoff: failed_after,
        timeout_before_cutoff: counter.timeout_before_cutoff,
        timeout_after_cutoff: timeout_after,
        result_unknown_before_cutoff: counter.result_unknown_before_cutoff,
        result_unknown_after_cutoff: unknown_after,
        terminal_after_cutoff: terminal_after,
        canceled_queued: counter.canceled_queued,
        canceled_running: counter.canceled_running,
        cutoff_queued,
        cutoff_running,
        first_error_code: counter.first_error_code.max(error.code),
        first_error: error.view(),
        logical_latency: logical_histogram.view(),
        queue_latency: queue_histogram.view(),
    }
}

fn phase_report(
    phase: Phase,
    data: &PhaseData,
    planned: &[u64],
    overlay: Option<&PhaseData>,
) -> PhaseReport {
    let logical = (0..data.logical.len())
        .map(|index| {
            let (identity, preparation) = match phase {
                Phase::Preparation => (None, Some(preparation_identity(index))),
                Phase::Measurement | Phase::Drain | Phase::Finished => {
                    (Some(measurement_identity(index)), None)
                }
            };
            let overlay_counter = overlay.map(|value| &value.logical[index]);
            logical_report(
                &data.logical[index],
                &data.errors[index],
                &data.logical_histograms[index],
                &data.queue_histograms[index],
                planned[index],
                identity,
                preparation,
                overlay_counter,
            )
        })
        .collect();
    let queue = (0..data.queue_histograms.len())
        .map(|index| {
            let (work_class, resource) = match phase {
                Phase::Preparation => preparation_parts(preparation_identity(index)),
                Phase::Measurement | Phase::Drain | Phase::Finished => {
                    identity_parts(measurement_identity(index))
                }
            };
            QueueStreamReport {
                work_class,
                resource,
                sample_count: data.queue_histograms[index].histogram.len(),
                latency: data.queue_histograms[index].view(),
            }
        })
        .collect();
    let rpc = RpcMethod::ALL
        .into_iter()
        .map(|method| {
            let counter = &data.rpc[method.index()];
            RpcReport {
                method: method.as_str().into(),
                started: counter.rpc_started,
                completed: counter.rpc_completed,
                succeeded: counter.rpc_succeeded,
                failed: counter.rpc_failed,
                timeout: counter.rpc_timeout,
                result_unknown: counter.rpc_result_unknown,
                payload_bytes: counter.rpc_payload_bytes,
                latency: data.rpc_histograms[method.index()].view(),
            }
        })
        .collect();
    PhaseReport {
        phase: phase.as_str().into(),
        logical,
        queue,
        rpc,
        logical_total: data.logical_total.view(),
        queue_total: data.queue_total.view(),
        rpc_total: data.rpc_total.view(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::*;

    struct TestClock {
        start: Instant,
        nanos: AtomicU64,
    }

    impl TestClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                start: Instant::now(),
                nanos: AtomicU64::new(0),
            })
        }

        fn set(&self, nanos: u64) {
            self.nanos.store(nanos, Ordering::Release);
        }
    }

    impl MonotonicClock for TestClock {
        fn now(&self) -> Instant {
            self.start + Duration::from_nanos(self.nanos.load(Ordering::Acquire))
        }
    }

    fn small_plan_with(update_rps: u64, duration_ms: u64, bucket_ms: u64) -> CompiledPlan {
        let mut config = crate::config::SyntheticConfig::from_json(
            crate::plan::DEFAULT_SYNTHETIC_CONFIG,
            64 * 1024 * 1024,
        )
        .unwrap();
        config.topology.node_count = 1;
        config.topology.pod_count = 2;
        config.topology.namespace_count = 1;
        config.controllers.daemonset_count = 1;
        config.controllers.pods_per_deployment = 1;
        config.load.update_rps = update_rps;
        config.load.update_weights.pods = 1;
        config.load.update_weights.deployments = 0;
        config.load.update_weights.replicasets = 0;
        config.load.update_weights.daemonsets = 0;
        config.load.read_ratio.numerator = 0;
        config.load.duration_ms = duration_ms;
        config.report.bucket_ms = bucket_ms;
        CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap()
    }

    fn small_plan() -> CompiledPlan {
        small_plan_with(70, 100, 10)
    }
    fn record_preparation(recorder: &MeasurementRecorder, plan: &CompiledPlan) {
        let planned = [
            plan.resource_counts.pods,
            plan.resource_counts.nodes,
            plan.resource_counts.node_leases,
            plan.resource_counts.deployments,
            plan.resource_counts.replicasets,
            plan.resource_counts.daemonsets,
            1,
        ];
        for (index, count) in planned.into_iter().enumerate() {
            for _ in 0..count {
                let token = match recorder
                    .offer_preparation(preparation_identity(index))
                    .unwrap()
                {
                    OfferResult::Token(token) => token,
                    other => panic!("unexpected preparation offer result: {other:?}"),
                };
                token.accept().unwrap();
                token.started().unwrap();
                token.terminal(Outcome::Success).unwrap();
            }
        }
    }

    fn make_recorder(clock: &Arc<TestClock>) -> MeasurementRecorder {
        let plan = small_plan();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        record_preparation(&recorder, &plan);
        recorder
    }

    #[test]
    fn test_report_accounting() {
        let clock = TestClock::new();
        let recorder = make_recorder(&clock);
        recorder.start_measurement().unwrap();
        let stream = LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::Pods,
        };
        let accepted = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        accepted.accept().unwrap();
        accepted.started().unwrap();
        accepted.terminal(Outcome::Success).unwrap();
        let failed = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        failed.accept().unwrap();
        failed.started().unwrap();
        failed.terminal(Outcome::Timeout).unwrap();
        let unknown = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        unknown.accept().unwrap();
        unknown.started().unwrap();
        unknown.terminal(Outcome::ResultUnknown).unwrap();
        let dropped = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        dropped.drop_for(DropReason::Backpressure).unwrap();
        let canceled_queued = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        canceled_queued.accept().unwrap();
        drop(canceled_queued);
        let canceled_running = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        canceled_running.accept().unwrap();
        canceled_running.started().unwrap();
        drop(canceled_running);
        recorder.record_missed_schedule(stream, 1).unwrap();
        clock.set(100_000_000);
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = report
            .measurement
            .logical
            .iter()
            .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
            .unwrap();
        assert_eq!(row.accepted, 5);
        assert_eq!(row.succeeded_before_cutoff, 1);
        assert_eq!(row.failed_before_cutoff, 2);
        assert_eq!(row.timeout_before_cutoff, 1);
        assert_eq!(row.result_unknown_before_cutoff, 1);
        assert_eq!(row.dropped_full, 1);
        assert_eq!(row.canceled_queued, 1);
        assert_eq!(row.canceled_running, 1);
        assert_eq!(row.missed_schedule, 1);
        assert_eq!(row.not_offered_after_stop, 0);
        assert!(report.accounting.planned_conserved);
        assert!(report.accounting.offered_conserved);
        assert!(report.accounting.accepted_cohort_conserved);
        let second_clock = TestClock::new();
        let second = make_recorder(&second_clock);
        second.start_measurement().unwrap();
        second.record_missed_schedule(stream, 7).unwrap();
        second_clock.set(100_000_000);
        second.stop_measurement().unwrap();
        let second_report = second.finish().unwrap();
        let second_row = second_report
            .measurement
            .logical
            .iter()
            .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
            .unwrap();
        assert_eq!(second_row.offered, 0);
        assert_eq!(second_row.accepted, 0);
        assert_eq!(second_row.missed_schedule, 7);
        assert!(second_report.accounting.planned_conserved);
    }

    #[test]
    fn test_measurement_window_excludes_preparation_time() {
        let clock = TestClock::new();
        let recorder = make_recorder(&clock);
        clock.set(123_000_000);
        let window = recorder.start_measurement().unwrap();
        assert_eq!(
            window.start.duration_since(clock.start),
            Duration::from_millis(123)
        );
        assert_eq!(
            window.cutoff.duration_since(window.start),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn test_cutoff_classify() {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        record_preparation(&recorder, &plan);
        recorder.start_measurement().unwrap();
        let stream = LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::Pods,
        };
        let token = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        token.accept().unwrap();
        clock.set(10_000_000);
        recorder.stop_measurement().unwrap();
        clock.set(10_000_001);
        token.started().unwrap();
        token.terminal(Outcome::Success).unwrap();
        let report = recorder.finish().unwrap();
        let row = report
            .measurement
            .logical
            .iter()
            .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
            .unwrap();
        assert_eq!(report.cutoff.effective_ns, Some(10_000_000));
        assert_eq!(row.started_before_cutoff, 0);
        assert_eq!(row.started_after_cutoff, 1);
        assert_eq!(row.terminal_after_cutoff, 1);
        assert_eq!(row.cutoff_queued, 1);
        assert_eq!(row.cutoff_running, 0);
        assert!(report.accounting.planned_conserved);
        assert!(report.accounting.accepted_cohort_conserved);
    }

    #[test]
    fn test_normal_cutoff_rejection_keeps_remaining_slots_missed() {
        let clock = TestClock::new();
        let mut config = crate::config::SyntheticConfig::from_json(
            crate::plan::DEFAULT_SYNTHETIC_CONFIG,
            64 * 1024 * 1024,
        )
        .unwrap();
        config.topology.node_count = 1;
        config.topology.pod_count = 2;
        config.topology.namespace_count = 1;
        config.controllers.daemonset_count = 1;
        config.controllers.pods_per_deployment = 1;
        config.load.duration_ms = 10;
        config.load.update_rps = 100;
        config.load.update_weights.pods = 1;
        config.load.update_weights.deployments = 0;
        config.load.update_weights.replicasets = 0;
        config.load.update_weights.daemonsets = 0;
        config.load.read_ratio.numerator = 0;
        config.periodic.node_lease_interval_ms = 1;
        config.periodic.node_lease_jitter_percent = 0;
        config.periodic.node_status_interval_ms = 10;
        config.periodic.status_interval_ms = 10;
        config.periodic.compact_interval_ms = 10;
        config.report.bucket_ms = 1;
        let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        record_preparation(&recorder, &plan);
        recorder.start_measurement().unwrap();
        let application = LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::Pods,
        };
        let periodic = LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::NodeLease,
        };
        let application_token = match recorder.offer(application).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected application offer: {other:?}"),
        };
        let periodic_token = match recorder.offer(periodic).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected periodic offer: {other:?}"),
        };
        clock.set(10_000_000);
        assert_eq!(
            application_token.accept().unwrap(),
            TokenAction::RejectedAfterCutoff
        );
        assert_eq!(
            periodic_token.accept().unwrap(),
            TokenAction::RejectedAfterCutoff
        );
        let application_planned = plan
            .logical_streams
            .iter()
            .find(|stream| {
                stream.work_class == LogicalWorkClass::Update
                    && stream.resource == ResourceKind::Pods
            })
            .unwrap()
            .planned;
        let periodic_planned = plan
            .periodic_plan
            .iter()
            .find(|stream| stream.kind == PeriodicKind::NodeLease)
            .unwrap()
            .planned;
        recorder
            .record_missed_schedule_before_cutoff(
                application,
                application_planned.saturating_sub(1),
            )
            .unwrap();
        recorder
            .record_missed_schedule_before_cutoff(periodic, periodic_planned.saturating_sub(1))
            .unwrap();
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        for identity in [application, periodic] {
            let row = &report.measurement.logical[identity.measurement_index().unwrap()];
            assert_eq!(row.not_offered_after_stop, 0);
            assert_eq!(row.planned, row.missed_schedule);
            assert_eq!(row.offered, 0);
        }
        assert!(report.accounting.planned_conserved);
    }

    #[test]
    fn test_histogram_overflow() {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        record_preparation(&recorder, &plan);
        recorder.start_measurement().unwrap();
        let stream = LogicalStreamIdentity::Application {
            work_class: LogicalWorkClass::Update,
            resource: ResourceKind::Pods,
        };
        let token = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        token.accept().unwrap();
        clock.set(100_000_000);
        recorder.stop_measurement().unwrap();
        clock.set(20_000_000_000);
        token.started().unwrap();
        token.terminal(Outcome::Success).unwrap();
        let report = recorder.finish().unwrap();
        let row = report
            .drain
            .logical
            .iter()
            .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
            .unwrap();
        assert!(row.logical_latency.overflow > 0);
        assert_eq!(row.logical_latency.exact_max_us, Some(20_000_000));
        assert!(row.logical_latency.p99_us.is_none());
        assert!(row.logical_latency.quantiles_unavailable);
        assert!(report.accounting.planned_conserved);
        assert!(report.accounting.accepted_cohort_conserved);
    }
}
