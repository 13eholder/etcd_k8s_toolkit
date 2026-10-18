use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use prost::Message;
use thiserror::Error;
use tokio::sync::watch as readiness_channel;

use crate::config::WatchSpec;
use crate::metrics::RunMetrics;
pub use crate::metrics::{DropReason, Outcome};
use crate::plan::{
    BucketCounterStorage, CompiledPlan, CounterStorage, HistogramCounterStorage, HistogramPlan,
    LogicalWorkClass, MeasurementLayout, PeriodicKind, PhaseClockStorage, ResourceKind,
    WatchStreamStateStorage,
};
use crate::report::{
    BucketReport, CutoffReport, HistogramMetadata, HistogramView, LatencyReport,
    LogicalStreamReport, MeasurementAccounting, MeasurementReport, PhaseReport,
    PreparationResourceReport, QueueStreamReport, RpcReport, TimingReport, WatchReport,
    WatchRootReport, WatchStreamReport,
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
pub const WATCH_TERMINAL_COMPACTED: u64 = 1;
pub const WATCH_TERMINAL_CANCELED: u64 = 2;
pub const WATCH_TERMINAL_NO_LEADER: u64 = 3;
pub const WATCH_TERMINAL_SEMANTIC: u64 = 4;
pub const WATCH_TERMINAL_PHASE_DEADLINE: u64 = 5;
pub const WATCH_TERMINAL_CONTEXT_CANCELED: u64 = 6;
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

#[derive(Clone, Copy, Debug, Default)]
struct RangeStats {
    pages: u64,
    snapshot_revision: i64,
    server_count: i64,
    last_key_bytes: u64,
    selected: u64,
    discarded: u64,
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
    range: RangeStats,
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
            range: RangeStats::default(),
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

#[repr(C)]
struct WatchData {
    storage: WatchStreamStateStorage,
    measurement_connected_since_ns: u64,
    drain_connected_since_ns: u64,
    measurement_gap: u64,
    measurement_terminal: u64,
    measurement_events: u64,
    measurement_payload_bytes: u64,
    drain_events: u64,
    drain_payload_bytes: u64,
}

impl WatchData {
    const INACTIVE: u64 = u64::MAX;

    fn new() -> Self {
        Self {
            storage: WatchStreamStateStorage {
                connected_since_ns: Self::INACTIVE,
                ..Default::default()
            },
            measurement_connected_since_ns: Self::INACTIVE,
            drain_connected_since_ns: Self::INACTIVE,
            measurement_gap: 0,
            measurement_terminal: 0,
            measurement_events: 0,
            measurement_payload_bytes: 0,
            drain_events: 0,
            drain_payload_bytes: 0,
        }
    }
}

pub(crate) fn watch_state_size_bytes() -> Option<u64> {
    u64::try_from(size_of::<WatchData>()).ok()
}

struct MeasurementState {
    clock: PhaseClockStorage,
    preparation: PhaseData,
    measurement: PhaseData,
    drain: PhaseData,
    buckets: Vec<BucketData>,
    watch: Vec<WatchData>,
    planned_preparation: [u64; PREPARATION_LOGICAL_STREAMS],
    planned_measurement: [u64; MEASUREMENT_LOGICAL_STREAMS],
    active_tokens: u64,
    active_rpc_attempts: u64,
    arithmetic_overflow: bool,
    drain_timed_out: bool,
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
    metrics: Option<RunMetrics>,
    watch_specs: Vec<WatchSpec>,
    state: Mutex<MeasurementState>,
    fatal_error: Mutex<ErrorSlot>,
    watch_readiness: readiness_channel::Sender<()>,
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

    fn record_internal_error(&self, error: &MeasurementError) {
        let mut fatal = lock_recover(&self.inner.fatal_error);
        fatal.set(0xFFFF_FFFF_FFFF_FFFEu64, &error.to_string());
    }

    fn fatal_error(&self) -> Option<String> {
        lock_recover(&self.inner.fatal_error).view()
    }

    fn fatal_error_result(&self) -> Option<MeasurementError> {
        self.fatal_error().map(|error| {
            MeasurementError::InvalidPhase(format!("recorder invariant failure: {error}"))
        })
    }

    #[cfg(test)]
    pub fn new(plan: &CompiledPlan) -> Result<Self, MeasurementError> {
        Self::with_clock(plan, Arc::new(TokioMonotonicClock))
    }

    pub fn new_with_metrics_at(
        plan: &CompiledPlan,
        metrics: RunMetrics,
        start: Instant,
    ) -> Result<Self, MeasurementError> {
        Self::with_clock_and_metrics_start(
            plan,
            Arc::new(TokioMonotonicClock),
            Some(metrics),
            start,
        )
    }

    #[cfg(test)]
    pub fn with_clock(
        plan: &CompiledPlan,
        clock: Arc<dyn MonotonicClock>,
    ) -> Result<Self, MeasurementError> {
        Self::with_clock_and_metrics(plan, clock, None)
    }

    #[cfg(test)]
    pub fn with_clock_and_metrics(
        plan: &CompiledPlan,
        clock: Arc<dyn MonotonicClock>,
        metrics: Option<RunMetrics>,
    ) -> Result<Self, MeasurementError> {
        let start = clock.now();
        Self::with_clock_and_metrics_start(plan, clock, metrics, start)
    }

    fn with_clock_and_metrics_start(
        plan: &CompiledPlan,
        clock: Arc<dyn MonotonicClock>,
        metrics: Option<RunMetrics>,
        start: Instant,
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
            setup_end_ns: None,
            preparation_end_ns: None,
            drain_start_ns: None,
            drain_end_ns: None,
            watch_teardown_start_ns: None,
            watch_teardown_end_ns: None,
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
        let watch_count = usize::try_from(layout.watch_streams).map_err(|_| {
            MeasurementError::InvalidLayout("watch stream count exceeds usize".into())
        })?;
        let mut watch = Vec::with_capacity(watch_count);
        for _ in 0..watch_count {
            watch.push(WatchData::new());
        }
        if watch.capacity() != watch_count {
            return Err(MeasurementError::InvalidLayout(
                "watch state capacity exceeds compiled stream count".into(),
            ));
        }
        let state = MeasurementState {
            clock: clock_state,
            preparation,
            measurement,
            drain,
            buckets,
            watch,
            planned_preparation,
            planned_measurement,
            active_tokens: 0,
            active_rpc_attempts: 0,
            arithmetic_overflow: false,
            drain_timed_out: false,
        };
        let (watch_readiness, _readiness_receiver) = readiness_channel::channel(());
        let recorder = Self {
            inner: Arc::new(RecorderInner {
                layout,
                clock,
                metrics,
                watch_specs: plan.watch_specs(),
                state: Mutex::new(state),
                fatal_error: Mutex::new(ErrorSlot::default()),
                watch_readiness,
            }),
        };
        if recorder.allocated_bytes()? > recorder.inner.layout.estimated_measurement_memory_bytes {
            return Err(MeasurementError::InvalidLayout(
                "actual preallocated storage exceeds compiled measurement budget".into(),
            ));
        }
        Ok(recorder)
    }

    pub fn phase(&self) -> Phase {
        lock_recover(&self.inner.state).phase()
    }

    pub(crate) fn watch_readiness(&self) -> readiness_channel::Receiver<()> {
        self.inner.watch_readiness.subscribe()
    }

    fn notify_watch_readiness(&self) {
        self.inner.watch_readiness.send_replace(());
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
        let watch_size =
            u64::try_from(size_of::<WatchData>()).map_err(|_| MeasurementError::ClockOverflow)?;
        bytes = bytes
            .checked_add(
                u64::try_from(state.watch.capacity())
                    .map_err(|_| MeasurementError::ClockOverflow)?
                    .checked_mul(watch_size)
                    .ok_or(MeasurementError::CounterOverflow)?,
            )
            .ok_or(MeasurementError::CounterOverflow)?;
        Ok(bytes)
    }

    pub fn mark_setup_complete(&self) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.phase() != Phase::Preparation {
            return Err(MeasurementError::InvalidPhase(
                "setup can only finish during preparation".into(),
            ));
        }
        let sample = self.sample_now(&mut state)?;
        state.clock.setup_end_ns = Some(sample.run_ns);
        Ok(())
    }

    /// Capture the end of preparation when the run exits before measurement
    /// starts. Setup failures must not call this method: they leave
    /// `preparation_ns` null because preparation was never entered.
    pub(crate) fn mark_preparation_complete(&self) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.clock.preparation_end_ns.is_some() {
            return Ok(());
        }
        if state.phase() != Phase::Preparation || state.clock.setup_end_ns.is_none() {
            return Err(MeasurementError::InvalidPhase(
                "preparation can only finish after setup and before measurement".into(),
            ));
        }
        let sample = self.sample_now(&mut state)?;
        state.clock.preparation_end_ns = Some(sample.run_ns);
        Ok(())
    }

    pub fn begin_watch_teardown(&self) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        if state.clock.watch_teardown_start_ns.is_none() {
            state.clock.watch_teardown_start_ns = Some(sample.run_ns);
        }
        if state.phase() == Phase::Drain
            && state.clock.measurement_start.is_some()
            && state.clock.drain_end_ns.is_none()
        {
            state.clock.drain_end_ns = Some(sample.run_ns);
        }
        Ok(())
    }

    pub fn end_watch_teardown(&self) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        if state.clock.watch_teardown_end_ns.is_none() {
            state.clock.watch_teardown_end_ns = Some(sample.run_ns);
        }
        self.close_active_watch_connections(&mut state, sample)?;
        Ok(())
    }

    pub fn mark_drain_timeout(&self) {
        let mut state = lock_recover(&self.inner.state);
        state.drain_timed_out = true;
    }

    fn close_active_watch_connections(
        &self,
        state: &mut MeasurementState,
        sample: ClockSample,
    ) -> Result<(), MeasurementError> {
        let Some(measurement_ns) = sample.measurement_ns else {
            return Ok(());
        };
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        let measurement_end = measurement_ns.min(cutoff);
        for watch in &mut state.watch {
            if watch.measurement_connected_since_ns != WatchData::INACTIVE {
                watch.storage.observable_duration_ns = watch
                    .storage
                    .observable_duration_ns
                    .checked_add(
                        measurement_end.saturating_sub(watch.measurement_connected_since_ns),
                    )
                    .ok_or(MeasurementError::CounterOverflow)?;
                watch.measurement_connected_since_ns = WatchData::INACTIVE;
                watch.storage.connected_since_ns = WatchData::INACTIVE;
            }
            if watch.drain_connected_since_ns != WatchData::INACTIVE {
                watch.drain_connected_since_ns = WatchData::INACTIVE;
                watch.storage.connected_since_ns = WatchData::INACTIVE;
            }
        }
        Ok(())
    }

    pub fn watch_created(&self, index: usize) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let phase = state.phase();
        let configured_cutoff = state.clock.configured_cutoff_ns;
        let watch = state.watch.get_mut(index).ok_or_else(|| {
            MeasurementError::InvalidLayout("watch stream index out of range".into())
        })?;
        Self::add(&mut watch.storage.created, 1)?;
        watch.storage.ready = 1;
        match phase {
            Phase::Measurement => {
                let measurement_ns = sample.measurement_ns.unwrap_or(0);
                if watch.measurement_connected_since_ns == WatchData::INACTIVE {
                    watch.measurement_connected_since_ns = measurement_ns;
                }
            }
            Phase::Drain => {
                if watch.drain_connected_since_ns == WatchData::INACTIVE {
                    watch.drain_connected_since_ns =
                        sample.measurement_ns.unwrap_or(configured_cutoff);
                }
            }
            Phase::Preparation | Phase::Finished => {}
        }
        watch.storage.connected_since_ns = match phase {
            Phase::Measurement => watch.measurement_connected_since_ns,
            Phase::Drain => watch.drain_connected_since_ns,
            Phase::Preparation | Phase::Finished => WatchData::INACTIVE,
        };
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_watch_created(phase.as_str());
        }
        self.notify_watch_readiness();
        Ok(())
    }

    pub fn watch_disconnect(&self, index: usize) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let phase = state.phase();
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        let measurement_ns = sample.measurement_ns.unwrap_or(0);
        let watch = state.watch.get_mut(index).ok_or_else(|| {
            MeasurementError::InvalidLayout("watch stream index out of range".into())
        })?;
        if phase == Phase::Preparation {
            watch.storage.ready = 0;
        }
        Self::add(&mut watch.storage.disconnects, 1)?;
        if phase == Phase::Measurement {
            if watch.measurement_connected_since_ns != WatchData::INACTIVE {
                let end = measurement_ns.min(cutoff);
                watch.storage.observable_duration_ns = watch
                    .storage
                    .observable_duration_ns
                    .checked_add(end.saturating_sub(watch.measurement_connected_since_ns))
                    .ok_or(MeasurementError::CounterOverflow)?;
                watch.measurement_connected_since_ns = WatchData::INACTIVE;
                watch.storage.connected_since_ns = WatchData::INACTIVE;
            }
            if measurement_ns < cutoff {
                watch.measurement_gap = 1;
            }
        } else if phase == Phase::Drain {
            watch.drain_connected_since_ns = WatchData::INACTIVE;
            watch.storage.connected_since_ns = WatchData::INACTIVE;
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_watch_disconnect(phase.as_str());
        }
        self.notify_watch_readiness();
        Ok(())
    }

    pub fn watch_terminal(&self, index: usize, code: u64) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let phase = state.phase();
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        let measurement_ns = sample.measurement_ns.unwrap_or(0);
        let watch = state.watch.get_mut(index).ok_or_else(|| {
            MeasurementError::InvalidLayout("watch stream index out of range".into())
        })?;
        if phase == Phase::Preparation {
            watch.storage.ready = 0;
        }
        watch.storage.terminal_code = code;
        if phase == Phase::Measurement {
            if watch.measurement_connected_since_ns != WatchData::INACTIVE {
                let end = measurement_ns.min(cutoff);
                watch.storage.observable_duration_ns = watch
                    .storage
                    .observable_duration_ns
                    .checked_add(end.saturating_sub(watch.measurement_connected_since_ns))
                    .ok_or(MeasurementError::CounterOverflow)?;
                watch.measurement_connected_since_ns = WatchData::INACTIVE;
                watch.storage.connected_since_ns = WatchData::INACTIVE;
            }
            if measurement_ns < cutoff {
                watch.measurement_gap = 1;
                watch.measurement_terminal = 1;
            }
        } else {
            watch.drain_connected_since_ns = WatchData::INACTIVE;
            watch.storage.connected_since_ns = WatchData::INACTIVE;
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_watch_terminal(phase.as_str());
        }
        self.notify_watch_readiness();
        Ok(())
    }

    pub fn watch_context_cancel(&self, index: usize) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let phase = state.phase();
        let measurement_ns = sample.measurement_ns.unwrap_or(0);
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        let watch = state.watch.get_mut(index).ok_or_else(|| {
            MeasurementError::InvalidLayout("watch stream index out of range".into())
        })?;
        if phase == Phase::Preparation {
            watch.storage.ready = 0;
        }
        if watch.measurement_connected_since_ns != WatchData::INACTIVE {
            watch.storage.observable_duration_ns =
                watch.storage.observable_duration_ns.saturating_add(
                    measurement_ns
                        .min(cutoff)
                        .saturating_sub(watch.measurement_connected_since_ns),
                );
            watch.measurement_connected_since_ns = WatchData::INACTIVE;
        }
        watch.drain_connected_since_ns = WatchData::INACTIVE;
        watch.storage.connected_since_ns = WatchData::INACTIVE;
        watch.storage.terminal_code = WATCH_TERMINAL_CONTEXT_CANCELED;
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_watch_terminal(phase.as_str());
        }
        self.notify_watch_readiness();
        Ok(())
    }

    pub fn watch_response(
        &self,
        index: usize,
        response: &crate::pb::etcdserverpb::WatchResponse,
    ) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let phase = if state.phase() == Phase::Measurement
            && sample.measurement_ns.unwrap_or(0)
                < state
                    .clock
                    .effective_cutoff_ns
                    .unwrap_or(state.clock.configured_cutoff_ns)
        {
            Phase::Measurement
        } else if state.clock.measurement_start.is_some() {
            Phase::Drain
        } else {
            Phase::Preparation
        };
        let events =
            u64::try_from(response.events.len()).map_err(|_| MeasurementError::CounterOverflow)?;
        let payload_bytes = response.events.iter().try_fold(0u64, |total, event| {
            total
                .checked_add(event.encoded_len() as u64)
                .ok_or(MeasurementError::CounterOverflow)
        })?;
        {
            let watch = state.watch.get_mut(index).ok_or_else(|| {
                MeasurementError::InvalidLayout("watch stream index out of range".into())
            })?;
            if phase == Phase::Measurement {
                Self::add(&mut watch.measurement_events, events)?;
                Self::add(&mut watch.measurement_payload_bytes, payload_bytes)?;
                Self::add(&mut watch.storage.events, events)?;
                Self::add(&mut watch.storage.payload_bytes, payload_bytes)?;
            } else if phase == Phase::Drain {
                Self::add(&mut watch.drain_events, events)?;
                Self::add(&mut watch.drain_payload_bytes, payload_bytes)?;
                Self::add(&mut watch.storage.events, events)?;
                Self::add(&mut watch.storage.payload_bytes, payload_bytes)?;
            }
        }
        if phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.watch_events, events)?;
                Self::add(&mut bucket.counters.watch_payload_bytes, payload_bytes)?;
            }
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_watch_events(phase.as_str(), events, payload_bytes);
        }
        Ok(())
    }

    pub fn record_range_result(
        &self,
        pages: u64,
        snapshot_revision: i64,
        server_count: i64,
        last_key_bytes: u64,
        selected: u64,
        discarded: u64,
    ) -> Result<(), MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        let sample = self.sample_now(&mut state)?;
        let phase = match state.phase() {
            Phase::Preparation => Phase::Preparation,
            Phase::Measurement
                if sample.measurement_ns.unwrap_or(0)
                    < state
                        .clock
                        .effective_cutoff_ns
                        .unwrap_or(state.clock.configured_cutoff_ns) =>
            {
                Phase::Measurement
            }
            Phase::Measurement => {
                let cutoff = state
                    .clock
                    .effective_cutoff_ns
                    .unwrap_or(state.clock.configured_cutoff_ns);
                self.freeze_cutoff(&mut state, cutoff)?;
                Phase::Drain
            }
            Phase::Drain | Phase::Finished => Phase::Drain,
        };
        let range = &mut state.phase_data_mut(phase).range;
        Self::add(&mut range.pages, pages)?;
        range.snapshot_revision = range.snapshot_revision.max(snapshot_revision);
        range.server_count = server_count;
        range.last_key_bytes = last_key_bytes;
        Self::add(&mut range.selected, selected)?;
        Self::add(&mut range.discarded, discarded)?;
        Ok(())
    }

    pub fn set_revision_metric(&self, revision: i64) {
        if let Some(metrics) = &self.inner.metrics {
            metrics.set_revision(revision);
        }
    }

    pub fn set_compact_revision_metric(&self, revision: i64) {
        if let Some(metrics) = &self.inner.metrics {
            metrics.set_compact_revision(revision);
        }
    }

    pub fn set_compactor_state_metric(&self, state: &'static str) {
        if let Some(metrics) = &self.inner.metrics {
            metrics.set_compactor_state(state);
        }
    }

    pub fn record_compactor_error_metric(&self) {
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_compactor_error();
        }
    }

    pub fn record_compactor_recovery_metric(&self) {
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_compactor_recovery();
        }
    }

    pub fn record_range_result_metric(
        &self,
        pages: u64,
        snapshot_revision: i64,
        server_count: i64,
        last_key_bytes: u64,
        selected: u64,
        discarded: u64,
    ) {
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_range_result(
                pages,
                snapshot_revision,
                server_count,
                last_key_bytes,
                selected,
                discarded,
            );
        }
    }

    pub fn finish_without_measurement(&self) -> Result<MeasurementReport, MeasurementError> {
        if let Some(error) = self.fatal_error_result() {
            return Err(error);
        }
        let mut state = lock_recover(&self.inner.state);
        if state.clock.measurement_start.is_some() {
            return Err(MeasurementError::InvalidPhase(
                "measurement already started".into(),
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
        let planned_preparation = state.planned_preparation;
        let planned_measurement = state.planned_measurement;
        for (index, (counter, planned)) in state
            .preparation
            .logical
            .iter_mut()
            .zip(planned_preparation)
            .enumerate()
        {
            let accounted = counter
                .offered
                .checked_add(counter.missed_schedule)
                .and_then(|value| value.checked_add(counter.not_offered_after_stop))
                .ok_or(MeasurementError::CounterOverflow)?;
            let remaining = planned.saturating_sub(accounted);
            Self::add(&mut counter.not_offered_after_stop, remaining)?;
            if let Some(metrics) = &self.inner.metrics {
                metrics.record_logical_not_offered_after_stop("preparation", index, remaining);
            }
        }
        for (index, (counter, planned)) in state
            .measurement
            .logical
            .iter_mut()
            .zip(planned_measurement)
            .enumerate()
        {
            let accounted = counter
                .offered
                .checked_add(counter.missed_schedule)
                .and_then(|value| value.checked_add(counter.not_offered_after_stop))
                .ok_or(MeasurementError::CounterOverflow)?;
            let remaining = planned.saturating_sub(accounted);
            Self::add(&mut counter.not_offered_after_stop, remaining)?;
            if let Some(metrics) = &self.inner.metrics {
                metrics.record_logical_not_offered_after_stop("measurement", index, remaining);
            }
        }
        self.sample_now(&mut state)?;
        state.clock.phase = PHASE_FINISHED;
        Ok(self.build_report(&state))
    }

    pub(crate) fn try_start_measurement(
        &self,
    ) -> Result<Option<MeasurementWindow>, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.phase() != Phase::Preparation {
            return Err(MeasurementError::InvalidPhase(
                "measurement must start from preparation".into(),
            ));
        }
        if state
            .watch
            .iter()
            .any(|watch| watch.storage.ready == 0 || watch.storage.terminal_code != 0)
        {
            return Ok(None);
        }
        self.start_measurement_locked(&mut state).map(Some)
    }

    #[cfg(test)]
    pub fn start_measurement(&self) -> Result<MeasurementWindow, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        self.start_measurement_locked(&mut state)
    }

    fn start_measurement_locked(
        &self,
        state: &mut MeasurementState,
    ) -> Result<MeasurementWindow, MeasurementError> {
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
        let sample = self.sample_now(state)?;
        if state.clock.setup_end_ns.is_none() {
            state.clock.setup_end_ns = Some(sample.run_ns);
        }
        state.clock.preparation_end_ns = Some(sample.run_ns);
        let measurement_start = state.clock.last;
        let cutoff = measurement_start
            .checked_add(Duration::from_nanos(configured_cutoff_ns))
            .ok_or(MeasurementError::ClockOverflow)?;
        state.clock.measurement_start = Some(measurement_start);
        state.clock.phase = PHASE_MEASUREMENT;
        for watch in &mut state.watch {
            if watch.storage.ready != 0 && watch.storage.terminal_code == 0 {
                watch.measurement_connected_since_ns = 0;
                watch.storage.connected_since_ns = 0;
            }
        }
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
        self.freeze_cutoff(&mut state, effective_ns)?;
        Ok(Cutoff {
            configured_ns: state.clock.configured_cutoff_ns,
            effective_ns,
        })
    }

    pub fn finish(&self) -> Result<MeasurementReport, MeasurementError> {
        if let Some(error) = self.fatal_error_result() {
            return Err(error);
        }
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
            self.freeze_cutoff(&mut state, effective_ns)?;
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
            if state.clock.drain_end_ns.is_none() {
                let sample = self.sample_now(&mut state)?;
                state.clock.drain_end_ns = Some(sample.run_ns);
            }
            state.clock.phase = PHASE_FINISHED;
        }
        Ok(self.build_report(&state))
    }

    pub fn finish_partial(&self) -> Result<MeasurementReport, MeasurementError> {
        let mut state = lock_recover(&self.inner.state);
        if state.clock.measurement_start.is_none() {
            return Err(MeasurementError::InvalidPhase(
                "measurement has not started".into(),
            ));
        }
        if state.phase() == Phase::Measurement {
            match self.sample_now(&mut state) {
                Ok(sample) => {
                    let effective_ns = sample
                        .measurement_ns
                        .unwrap_or(0)
                        .min(state.clock.configured_cutoff_ns);
                    if self.freeze_cutoff(&mut state, effective_ns).is_err() {
                        state.arithmetic_overflow = true;
                    }
                }
                Err(_) => state.arithmetic_overflow = true,
            }
        }
        if state.phase() == Phase::Drain && state.clock.drain_end_ns.is_none() {
            if let Ok(sample) = self.sample_now(&mut state) {
                state.clock.drain_end_ns = Some(sample.run_ns);
            } else {
                state.arithmetic_overflow = true;
            }
        }
        state.clock.phase = PHASE_FINISHED;
        Ok(self.build_report(&state))
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
                if let Some(metrics) = &self.inner.metrics {
                    if normal_cutoff {
                        metrics.record_logical_missed_schedule(phase.as_str(), index, 1);
                    } else {
                        metrics.record_logical_not_offered_after_stop(phase.as_str(), index, 1);
                    }
                }
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
                self.freeze_cutoff(&mut state, cutoff)?;
                Self::add(&mut state.measurement.logical[index].missed_schedule, 1)?;
                if let Some(metrics) = &self.inner.metrics {
                    metrics.record_logical_missed_schedule(phase.as_str(), index, 1);
                }
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

    #[cfg(test)]
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
            if let Some(metrics) = &self.inner.metrics {
                if normal_cutoff {
                    metrics.record_logical_missed_schedule("measurement", index, count);
                } else {
                    metrics.record_logical_not_offered_after_stop("measurement", index, count);
                }
            }
            return Ok(());
        }
        let measurement_ns = sample.measurement_ns.unwrap_or(0);
        let cutoff = state
            .clock
            .effective_cutoff_ns
            .unwrap_or(state.clock.configured_cutoff_ns);
        if measurement_ns >= cutoff {
            self.freeze_cutoff(&mut state, cutoff)?;
            let normal_cutoff =
                state.clock.effective_cutoff_ns == Some(state.clock.configured_cutoff_ns);
            let counter = if normal_cutoff {
                &mut state.measurement.logical[index].missed_schedule
            } else {
                &mut state.measurement.logical[index].not_offered_after_stop
            };
            Self::add(counter, count)?;
            if let Some(metrics) = &self.inner.metrics {
                if normal_cutoff {
                    metrics.record_logical_missed_schedule("measurement", index, count);
                } else {
                    metrics.record_logical_not_offered_after_stop("measurement", index, count);
                }
            }
        } else {
            Self::add(&mut state.measurement.logical[index].missed_schedule, count)?;
            if let Some(metrics) = &self.inner.metrics {
                metrics.record_logical_missed_schedule("measurement", index, count);
            }
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
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_logical_missed_schedule("measurement", index, count);
        }
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
            self.freeze_cutoff(&mut state, cutoff)?;
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
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_logical_not_offered_after_stop("measurement", index, count);
        }
        Ok(())
    }

    pub fn start_rpc_with_payload(
        &self,
        method: RpcMethod,
        request_payload_bytes: u64,
    ) -> Result<RpcAttempt, MeasurementError> {
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
                    self.freeze_cutoff(&mut state, cutoff)?;
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
        Self::add(&mut counter.request_payload_bytes, request_payload_bytes)?;
        Self::add(&mut counter.rpc_payload_bytes, request_payload_bytes)?;
        if phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.rpc_started, 1)?;
                if method == RpcMethod::Txn {
                    Self::add(&mut bucket.counters.txn_started, 1)?;
                }
            }
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_rpc_started(phase.as_str(), method.index(), request_payload_bytes);
        }
        Self::add(&mut state.active_rpc_attempts, 1)?;
        Ok(RpcAttempt {
            recorder: self.clone(),
            method,
            start_phase: phase,
            start_run_ns: sample.run_ns,
            request_payload_bytes,
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
        effective_ns: u64,
    ) -> Result<(), MeasurementError> {
        if state.clock.effective_cutoff_ns.is_none() {
            let start = state.clock.measurement_start.ok_or_else(|| {
                MeasurementError::InvalidPhase("cutoff requires a measurement start".into())
            })?;
            let start_run_ns = u64::try_from(start.duration_since(state.clock.start).as_nanos())
                .map_err(|_| MeasurementError::ClockOverflow)?;
            let drain_start_ns = start_run_ns
                .checked_add(effective_ns)
                .ok_or(MeasurementError::ClockOverflow)?;
            state.clock.effective_cutoff_ns = Some(effective_ns);
            state.clock.drain_start_ns = Some(drain_start_ns);
            for watch in &mut state.watch {
                if watch.measurement_connected_since_ns != WatchData::INACTIVE {
                    watch.storage.observable_duration_ns =
                        watch.storage.observable_duration_ns.saturating_add(
                            effective_ns.saturating_sub(watch.measurement_connected_since_ns),
                        );
                    watch.measurement_connected_since_ns = WatchData::INACTIVE;
                    watch.storage.connected_since_ns = WatchData::INACTIVE;
                }
            }
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

    fn watch_report_for_state(&self, state: &MeasurementState, drain: bool) -> WatchReport {
        let measurement_started = state.clock.measurement_start.is_some();
        let observed_ns = if measurement_started {
            state
                .clock
                .effective_cutoff_ns
                .unwrap_or(state.clock.configured_cutoff_ns)
        } else {
            0
        };
        let mut roots = Vec::<WatchRootReport>::new();
        let mut root_observable_ns = Vec::<u128>::new();
        for (index, spec) in self.inner.watch_specs.iter().enumerate() {
            let Some(watch) = state.watch.get(index) else {
                continue;
            };
            let root_index = if roots
                .last()
                .is_some_and(|root| root.root == spec.resource_prefix)
            {
                roots.len() - 1
            } else {
                roots.push(WatchRootReport {
                    root: spec.resource_prefix.clone(),
                    expected_streams: 0,
                    ready_streams: 0,
                    observable_stream_ms: 0.0,
                    observed_measurement_ms: 0.0,
                    coverage_ratio: None,
                    observation_complete: false,
                    streams: Vec::new(),
                });
                root_observable_ns.push(0);
                roots.len() - 1
            };
            let observable_ns = watch.storage.observable_duration_ns;
            let complete = measurement_started
                && observed_ns > 0
                && watch.storage.ready != 0
                && watch.measurement_gap == 0
                && watch.measurement_terminal == 0
                && observable_ns >= observed_ns;
            let events = if drain {
                watch.drain_events
            } else {
                watch.measurement_events
            };
            let payload_bytes = if drain {
                watch.drain_payload_bytes
            } else {
                watch.measurement_payload_bytes
            };
            roots[root_index].expected_streams =
                roots[root_index].expected_streams.saturating_add(1);
            if watch.storage.ready != 0 {
                roots[root_index].ready_streams = roots[root_index].ready_streams.saturating_add(1);
            }
            root_observable_ns[root_index] =
                root_observable_ns[root_index].saturating_add(u128::from(observable_ns));
            roots[root_index].streams.push(WatchStreamReport {
                root: spec.resource_prefix.clone(),
                stream: spec.name.clone(),
                ready: watch.storage.ready != 0,
                observable_duration_ms: observable_ns as f64 / NANOS_PER_MILLISECOND as f64,
                disconnects: watch.storage.disconnects,
                terminal: watch_terminal_label(watch.storage.terminal_code),
                events,
                payload_bytes,
                observation_complete: complete,
            });
        }
        let mut observation_complete = measurement_started && observed_ns > 0;
        for (root_index, root) in roots.iter_mut().enumerate() {
            let denominator = u128::from(root.expected_streams) * u128::from(observed_ns);
            let numerator = root_observable_ns[root_index];
            root.observable_stream_ms = numerator as f64 / NANOS_PER_MILLISECOND as f64;
            root.observed_measurement_ms = observed_ns as f64 / NANOS_PER_MILLISECOND as f64;
            root.coverage_ratio = (denominator > 0).then(|| numerator as f64 / denominator as f64);
            root.observation_complete = !root.streams.is_empty()
                && root
                    .streams
                    .iter()
                    .all(|stream| stream.observation_complete);
            observation_complete &= root.observation_complete;
        }
        WatchReport {
            roots,
            observation_complete,
        }
    }

    fn build_report(&self, state: &MeasurementState) -> MeasurementReport {
        let preparation = phase_report(
            Phase::Preparation,
            &state.preparation,
            &state.planned_preparation,
            None,
        );
        let mut measurement_phase = phase_report(
            Phase::Measurement,
            &state.measurement,
            &state.planned_measurement,
            Some(&state.drain),
        );
        let effective_ns = state.clock.effective_cutoff_ns;
        if let Some(duration_ns) = effective_ns.filter(|duration| *duration > 0) {
            for rpc in &mut measurement_phase.rpc {
                let denominator = duration_ns as f64;
                rpc.started_per_second = Some(rpc.started as f64 * 1_000_000_000.0 / denominator);
                rpc.completed_per_second =
                    Some(rpc.completed as f64 * 1_000_000_000.0 / denominator);
            }
        }
        let drain_planned = [0u64; MEASUREMENT_LOGICAL_STREAMS];
        let mut drain = phase_report(Phase::Drain, &state.drain, &drain_planned, None);
        for (drain_row, measurement_row) in drain
            .logical
            .iter_mut()
            .zip(measurement_phase.logical.iter())
        {
            drain_row.canceled_queued = measurement_row.canceled_queued;
            drain_row.canceled_running = measurement_row.canceled_running;
        }
        drain.timed_out = state.drain_timed_out;
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
            .chain(measurement_phase.logical.iter())
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
            .chain(measurement_phase.logical.iter())
            .all(|row| {
                row.offered
                    == row
                        .accepted
                        .saturating_add(row.dropped_full)
                        .saturating_add(row.dropped_closed)
            });
        let accepted_cohort_conserved = measurement_phase.logical.iter().all(|row| {
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
            .chain(measurement_phase.logical.iter())
            .all(|row| {
                row.result_unknown_before_cutoff <= row.failed_before_cutoff
                    && row.result_unknown_after_cutoff <= row.failed_after_cutoff
            });
        let watch = self.watch_report_for_state(state, false);
        let drain_watch = self.watch_report_for_state(state, true);
        let timing = TimingReport {
            setup_ns: state.clock.setup_end_ns,
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
            watch_teardown_ns: state
                .clock
                .watch_teardown_start_ns
                .zip(state.clock.watch_teardown_end_ns)
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
            .map(|bucket| {
                let width_ns = bucket
                    .counters
                    .end_ns
                    .saturating_sub(bucket.counters.start_ns);
                let txn_started_per_second = (width_ns > 0).then(|| {
                    bucket.counters.txn_started as f64 * 1_000_000_000.0 / width_ns as f64
                });
                let txn_completed_per_second = (width_ns > 0).then(|| {
                    bucket.counters.txn_completed as f64 * 1_000_000_000.0 / width_ns as f64
                });
                BucketReport {
                    start_ns: bucket.counters.start_ns,
                    end_ns: bucket.counters.end_ns,
                    logical_offered: bucket.counters.logical_offered,
                    logical_started: bucket.counters.logical_started,
                    logical_completed: bucket.counters.logical_completed,
                    logical_errors: bucket.counters.logical_errors,
                    rpc_started: bucket.counters.rpc_started,
                    rpc_completed: bucket.counters.rpc_completed,
                    txn_started: bucket.counters.txn_started,
                    txn_completed: bucket.counters.txn_completed,
                    txn_started_per_second,
                    txn_completed_per_second,
                    watch_events: bucket.counters.watch_events,
                    watch_payload_bytes: bucket.counters.watch_payload_bytes,
                    logical_latency: bucket.logical.view(),
                    queue_latency: bucket.queue.view(),
                    rpc_latency: bucket.rpc.view(),
                }
            })
            .collect();
        let metadata = HistogramMetadata {
            low_us: 1,
            significant_digits: HDR_SIGNIFICANT_DIGITS,
            auto_resize: false,
            coordinated_omission: false,
            overflow_is_not_truncated: true,
            duration_rounding: "max(1, ceil(duration_ns / 1000))".into(),
            payload_definition:
                "protobuf message encoded_len; framing, headers and status metadata excluded".into(),
        };
        let histogram_overflow = phase_has_histogram_overflow(&state.preparation)
            || phase_has_histogram_overflow(&state.measurement)
            || phase_has_histogram_overflow(&state.drain)
            || state.buckets.iter().any(|bucket| {
                bucket.logical.metadata.overflow != 0
                    || bucket.queue.metadata.overflow != 0
                    || bucket.rpc.metadata.overflow != 0
            });
        let observation_complete = watch.observation_complete;
        let measurement_complete = state.clock.measurement_start.is_some()
            && effective_ns == Some(state.clock.configured_cutoff_ns)
            && planned_conserved
            && offered_conserved
            && accepted_cohort_conserved
            && result_unknown_subset_failed
            && state.active_tokens == 0
            && state.active_rpc_attempts == 0
            && !state.arithmetic_overflow;
        MeasurementReport {
            preparation,
            measurement: measurement_phase.clone(),
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
                measurement_complete,
                observation_complete,
                histogram_overflow,
            },
            latency: LatencyReport {
                logical_total: measurement_phase.logical_total.clone(),
                queue_total: measurement_phase.queue_total.clone(),
                rpc_total: measurement_phase.rpc_total.clone(),
                metadata,
            },
            watch,
            drain_watch,
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
                if let Some(metrics) = &self.inner.metrics {
                    if normal_cutoff {
                        metrics.record_logical_missed_schedule(
                            token.stream.phase.as_str(),
                            token.stream.index,
                            1,
                        );
                    } else {
                        metrics.record_logical_not_offered_after_stop(
                            token.stream.phase.as_str(),
                            token.stream.index,
                            1,
                        );
                    }
                }
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
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_logical_offered(token.stream.phase.as_str(), token.stream.index, 1);
            metrics.record_logical_accepted(token.stream.phase.as_str(), token.stream.index, 1);
        }
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
                if let Some(metrics) = &self.inner.metrics {
                    if normal_cutoff {
                        metrics.record_logical_missed_schedule(
                            token.stream.phase.as_str(),
                            token.stream.index,
                            1,
                        );
                    } else {
                        metrics.record_logical_not_offered_after_stop(
                            token.stream.phase.as_str(),
                            token.stream.index,
                            1,
                        );
                    }
                }
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
            if let Some(metrics) = &self.inner.metrics {
                metrics.record_logical_not_offered_after_stop(
                    token.stream.phase.as_str(),
                    token.stream.index,
                    1,
                );
            }
            state.active_tokens = state
                .active_tokens
                .checked_sub(1)
                .ok_or(MeasurementError::CounterOverflow)?;
            *lifecycle = TokenLifecycle::Terminal;
            return Ok(TokenAction::Applied);
        }
        let counter = &mut state.phase_data_mut(token.stream.phase).logical[token.stream.index];
        Self::add(&mut counter.offered, 1)?;
        Self::add(&mut counter.dropped_full, 1)?;
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_logical_offered(token.stream.phase.as_str(), token.stream.index, 1);
            metrics.record_logical_dropped_full(token.stream.phase.as_str(), token.stream.index, 1);
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
        let queue_wait = sample
            .run_ns
            .checked_sub(accepted_run_ns)
            .ok_or(MeasurementError::ClockOverflow)?;
        let counter = &mut state.phase_data_mut(target_phase).logical[token.stream.index];
        if before_cutoff {
            Self::add(&mut counter.started_before_cutoff, 1)?;
        } else {
            Self::add(&mut counter.started_after_cutoff, 1)?;
        }
        let phase_data = state.phase_data_mut(target_phase);
        phase_data.queue_histograms[token.stream.index].record_duration(queue_wait)?;
        phase_data.queue_total.record_duration(queue_wait)?;
        if target_phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.logical_started, 1)?;
                bucket.queue.record_duration(queue_wait)?;
            }
        }
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_logical_started(target_phase.as_str(), token.stream.index);
            metrics.record_queue_wait(
                target_phase.as_str(),
                token.stream.index,
                Duration::from_nanos(queue_wait),
            );
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
            if outcome.is_success() {
                Self::add(&mut counter.succeeded_before_cutoff, 1)?;
            } else {
                Self::add(&mut counter.failed_before_cutoff, 1)?;
                if outcome.is_timeout() {
                    Self::add(&mut counter.timeout_before_cutoff, 1)?;
                }
                if outcome.is_result_unknown() {
                    Self::add(&mut counter.result_unknown_before_cutoff, 1)?;
                }
            }
        } else {
            Self::add(&mut counter.terminal_after_cutoff, 1)?;
            if outcome.is_success() {
                Self::add(&mut counter.succeeded_after_cutoff, 1)?;
            } else {
                Self::add(&mut counter.failed_after_cutoff, 1)?;
                if outcome.is_timeout() {
                    Self::add(&mut counter.timeout_after_cutoff, 1)?;
                }
                if outcome.is_result_unknown() {
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
                if !outcome.is_success() {
                    Self::add(&mut bucket.counters.logical_errors, 1)?;
                }
                bucket.logical.record_duration(logical_latency)?;
            }
        }
        state.active_tokens = state
            .active_tokens
            .checked_sub(1)
            .ok_or(MeasurementError::CounterOverflow)?;
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_logical_completed(
                target_phase.as_str(),
                token.stream.index,
                outcome,
                Duration::from_nanos(logical_latency),
            );
        }
        *lifecycle = TokenLifecycle::Terminal;
        let _ = started_run_ns;
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
                if let Some(metrics) = &self.inner.metrics {
                    metrics.record_logical_canceled_queued(
                        token.stream.phase.as_str(),
                        token.stream.index,
                        1,
                    );
                }
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
                if let Some(metrics) = &self.inner.metrics {
                    metrics.record_logical_canceled_running(
                        token.stream.phase.as_str(),
                        token.stream.index,
                        1,
                    );
                }
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
                    self.freeze_cutoff(&mut state, cutoff)?;
                    (Phase::Drain, false)
                }
            }
            Phase::Drain | Phase::Finished => (Phase::Drain, false),
        };
        let counter = &mut state.phase_data_mut(target_phase).rpc[attempt.method.index()];
        Self::add(&mut counter.rpc_completed, 1)?;
        if outcome.is_success() {
            Self::add(&mut counter.rpc_succeeded, 1)?;
        } else {
            Self::add(&mut counter.rpc_failed, 1)?;
            if outcome.is_timeout() {
                Self::add(&mut counter.rpc_timeout, 1)?;
            }
            if outcome.is_result_unknown() {
                Self::add(&mut counter.rpc_result_unknown, 1)?;
            }
        }
        Self::add(&mut counter.response_payload_bytes, payload_bytes)?;
        Self::add(&mut counter.rpc_payload_bytes, payload_bytes)?;
        let latency = sample
            .run_ns
            .checked_sub(attempt.start_run_ns)
            .ok_or(MeasurementError::ClockOverflow)?;
        if let Some(metrics) = &self.inner.metrics {
            metrics.record_rpc_completed(
                target_phase.as_str(),
                attempt.method.index(),
                outcome,
                payload_bytes,
                Duration::from_nanos(latency),
            );
        }
        let phase_data = state.phase_data_mut(target_phase);
        phase_data.rpc_histograms[attempt.method.index()].record_duration(latency)?;
        phase_data.rpc_total.record_duration(latency)?;
        if before_cutoff && target_phase == Phase::Measurement {
            if let Some(bucket) = self.bucket_mut(&mut state, sample.measurement_ns.unwrap_or(0)) {
                Self::add(&mut bucket.counters.rpc_completed, 1)?;
                if attempt.method == RpcMethod::Txn {
                    Self::add(&mut bucket.counters.txn_completed, 1)?;
                }
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

    pub(crate) fn recorder(&self) -> MeasurementRecorder {
        self.recorder.clone()
    }
}

impl Drop for MeasurementToken {
    fn drop(&mut self) {
        let recorder = self.recorder.clone();
        if let Err(error) = recorder.cancel_token(self) {
            recorder.record_internal_error(&error);
        }
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
    request_payload_bytes: u64,
    lifecycle: Mutex<RpcLifecycle>,
}

impl fmt::Debug for RpcAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcAttempt")
            .field("method", &self.method)
            .field("start_phase", &self.start_phase)
            .field("request_payload_bytes", &self.request_payload_bytes)
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
        if let Err(error) = recorder.abandon_rpc(self) {
            recorder.record_internal_error(&error);
        }
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
        dropped_closed: 0,
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

fn preparation_resources(logical: &[CounterStorage]) -> Vec<PreparationResourceReport> {
    [
        ("pods", 0usize),
        ("nodes", 1),
        ("node_leases", 2),
        ("deployments", 3),
        ("replicasets", 4),
        ("daemonsets", 5),
    ]
    .into_iter()
    .map(|(resource, index)| {
        let counter = logical.get(index).copied().unwrap_or_default();
        PreparationResourceReport {
            resource: resource.into(),
            created: counter.succeeded_before_cutoff,
            failed: counter.failed_before_cutoff,
        }
    })
    .collect()
}

fn watch_terminal_label(code: u64) -> Option<String> {
    match code {
        0 => None,
        WATCH_TERMINAL_COMPACTED => Some("compacted".into()),
        WATCH_TERMINAL_CANCELED => Some("canceled".into()),
        WATCH_TERMINAL_NO_LEADER => Some("no_leader".into()),
        WATCH_TERMINAL_SEMANTIC => Some("semantic".into()),
        WATCH_TERMINAL_PHASE_DEADLINE => Some("phase_deadline".into()),
        WATCH_TERMINAL_CONTEXT_CANCELED => Some("context_canceled".into()),
        _ => Some("unknown".into()),
    }
}

fn phase_has_histogram_overflow(data: &PhaseData) -> bool {
    data.logical_histograms
        .iter()
        .chain(data.queue_histograms.iter())
        .chain(data.rpc_histograms.iter())
        .chain([&data.logical_total, &data.queue_total, &data.rpc_total].into_iter())
        .any(|histogram| histogram.metadata.overflow != 0)
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
            let range = if method == RpcMethod::Range {
                data.range
            } else {
                RangeStats::default()
            };
            RpcReport {
                method: method.as_str().into(),
                started: counter.rpc_started,
                completed: counter.rpc_completed,
                succeeded: counter.rpc_succeeded,
                failed: counter.rpc_failed,
                timeout: counter.rpc_timeout,
                result_unknown: counter.rpc_result_unknown,
                payload_bytes: counter.rpc_payload_bytes,
                request_payload_bytes: counter.request_payload_bytes,
                response_payload_bytes: counter.response_payload_bytes,
                latency: data.rpc_histograms[method.index()].view(),
                range_pages: range.pages,
                range_snapshot_revision: range.snapshot_revision,
                range_server_count: range.server_count,
                range_last_key_bytes: range.last_key_bytes,
                range_selected: range.selected,
                range_discarded: range.discarded,
                ..Default::default()
            }
        })
        .collect();
    PhaseReport {
        phase: phase.as_str().into(),
        logical,
        queue,
        rpc,
        resources: if phase == Phase::Preparation {
            preparation_resources(&data.logical)
        } else {
            Vec::new()
        },
        logical_total: data.logical_total.view(),
        queue_total: data.queue_total.view(),
        rpc_total: data.rpc_total.view(),
        timed_out: false,
    }
}

#[cfg(test)]
#[path = "../tests/unit/measurement.rs"]
mod tests;
