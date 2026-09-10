use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{Mutex, Notify, mpsc, oneshot, watch as shutdown_watch};
use tokio::task::JoinSet;
use tokio::time::{self, Instant as TokioInstant};

use crate::client::{ClientBundle, RequestDisposition, RetryDecision, RpcMethod};
#[cfg(test)]
use crate::config::TopologySpec;
use crate::config::{
    EtcdLeasePoolSpec, ResourceBehavior, ResourceCountSpec, ResourceSpec, RuntimeConfig,
    WorkloadSpec,
};
use crate::heartbeat::{HeartbeatItem, HeartbeatItemKind, NodeHeartbeatPlanner};
use crate::lease::{LeaseEvent, LeasePool, LeasePoolConfig};
use crate::metrics::{
    self, DropReason, HeartbeatKind, Operation, Outcome, PoolName, RangeShape, ResourceName, Rpc,
    TxnResult, TxnShape,
};
use crate::pb::etcdserverpb::{CompactionRequest, LeaseGrantRequest, RangeRequest, TxnResponse};
use crate::report::{
    AccountingInvariants, ApplicationTargets, RunReport, RunTargets, WorkItemReport,
};
use crate::watch::prefix_range_end;
use crate::workload::{
    CasDecision, CasLoop, CasRetryPolicy, ExhaustedReason, KeyState, ResourceOperation, TxnAttempt,
    build_compact_revision_txn, failure_value, failure_version,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkClass {
    Bootstrap,
    Create,
    Update,
    Range,
    MasterLease,
    Heartbeat,
    Status,
    Compact,
}

impl WorkClass {
    const ALL: [Self; 8] = [
        Self::Bootstrap,
        Self::Create,
        Self::Update,
        Self::Range,
        Self::MasterLease,
        Self::Heartbeat,
        Self::Status,
        Self::Compact,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Create => "create",
            Self::Update => "update",
            Self::Range => "range",
            Self::MasterLease => "master_lease",
            Self::Heartbeat => "heartbeat",
            Self::Status => "status",
            Self::Compact => "compact",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Bootstrap => 0,
            Self::Create => 1,
            Self::Update => 2,
            Self::Range => 3,
            Self::MasterLease => 4,
            Self::Heartbeat => 5,
            Self::Status => 6,
            Self::Compact => 7,
        }
    }
}

const MAX_ERROR_CONTEXT: usize = 256;

#[derive(Debug)]
struct ClassAccounting {
    offered: AtomicU64,
    accepted: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    result_unknown: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
    unfinished: AtomicU64,
    unissued: AtomicU64,
    first_error: std::sync::Mutex<Option<String>>,
}

impl ClassAccounting {
    fn new() -> Self {
        Self {
            offered: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            result_unknown: AtomicU64::new(0),
            dropped_full: AtomicU64::new(0),
            dropped_closed: AtomicU64::new(0),
            unfinished: AtomicU64::new(0),
            unissued: AtomicU64::new(0),
            first_error: std::sync::Mutex::new(None),
        }
    }
}

#[derive(Debug)]
struct RunAccounting {
    classes: [ClassAccounting; 8],
}

impl RunAccounting {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            classes: std::array::from_fn(|_| ClassAccounting::new()),
        })
    }

    fn offer(self: &Arc<Self>, class: WorkClass) -> AccountingToken {
        self.class(class)
            .offered
            .fetch_add(1, AtomicOrdering::Relaxed);
        AccountingToken {
            accounting: Arc::clone(self),
            class,
            state: TokenState::Offered,
            accepted_at: None,
            phase: None,
        }
    }

    fn class(&self, class: WorkClass) -> &ClassAccounting {
        &self.classes[class.index()]
    }

    fn note_error(&self, class: WorkClass, message: impl Into<String>) {
        let mut slot = self
            .class(class)
            .first_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_some() {
            return;
        }
        let message = message.into();
        *slot = Some(message.chars().take(MAX_ERROR_CONTEXT).collect());
    }

    fn report(
        &self,
        targets: RunTargets,
        interrupted: bool,
        schedule_complete: bool,
        termination_error: Option<String>,
    ) -> RunReport {
        let mut work_items = Vec::with_capacity(WorkClass::ALL.len());
        let mut invariants = AccountingInvariants {
            offered_equals_accepted_plus_dropped_plus_unissued: true,
            accepted_equals_completed_plus_unfinished: true,
            accepted_equals_succeeded_plus_failed_plus_unfinished: true,
            result_unknown_subset_failed: true,
        };
        for class in WorkClass::ALL {
            let counters = self.class(class);
            let offered = counters.offered.load(AtomicOrdering::Acquire);
            let accepted = counters.accepted.load(AtomicOrdering::Acquire);
            let succeeded = counters.succeeded.load(AtomicOrdering::Acquire);
            let failed = counters.failed.load(AtomicOrdering::Acquire);
            let completed = succeeded.saturating_add(failed);
            let result_unknown = counters.result_unknown.load(AtomicOrdering::Acquire);
            let dropped_full = counters.dropped_full.load(AtomicOrdering::Acquire);
            let dropped_closed = counters.dropped_closed.load(AtomicOrdering::Acquire);
            let dropped = dropped_full.saturating_add(dropped_closed);
            let unfinished = counters.unfinished.load(AtomicOrdering::Acquire);
            let unissued = counters.unissued.load(AtomicOrdering::Acquire);
            let offered_valid =
                offered == accepted.saturating_add(dropped).saturating_add(unissued);
            let accepted_valid = accepted == completed.saturating_add(unfinished);
            let accepted_expanded_valid =
                accepted == succeeded.saturating_add(failed).saturating_add(unfinished);
            let unknown_valid = result_unknown <= failed;
            invariants.offered_equals_accepted_plus_dropped_plus_unissued &= offered_valid;
            invariants.accepted_equals_completed_plus_unfinished &= accepted_valid;
            invariants.accepted_equals_succeeded_plus_failed_plus_unfinished &=
                accepted_expanded_valid;
            invariants.result_unknown_subset_failed &= unknown_valid;
            let target = match class {
                WorkClass::Bootstrap => Some(targets.bootstrap),
                WorkClass::Create => Some(targets.application.creates),
                WorkClass::Update => Some(targets.application.updates),
                WorkClass::Range => Some(targets.range_requests),
                WorkClass::MasterLease
                | WorkClass::Heartbeat
                | WorkClass::Status
                | WorkClass::Compact => None,
            };
            let target_deficit = target.map(|target| target.saturating_sub(succeeded));
            let first_error = counters
                .first_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            work_items.push(WorkItemReport {
                work_class: class.as_str().to_string(),
                target,
                offered,
                accepted,
                completed,
                succeeded,
                failed,
                result_unknown,
                dropped,
                dropped_full,
                dropped_closed,
                unfinished,
                unissued,
                target_deficit,
                first_error,
            });
        }
        let invariants_valid = invariants.offered_equals_accepted_plus_dropped_plus_unissued
            && invariants.accepted_equals_completed_plus_unfinished
            && invariants.accepted_equals_succeeded_plus_failed_plus_unfinished
            && invariants.result_unknown_subset_failed;
        let targets_met = work_items.iter().all(|item| {
            item.target
                .is_none_or(|target| item.succeeded >= target && item.target_deficit == Some(0))
        });
        let no_bad_results = work_items.iter().all(|item| {
            item.failed == 0 && item.dropped == 0 && item.unfinished == 0 && item.unissued == 0
        });
        RunReport {
            achieved: schedule_complete
                && !interrupted
                && termination_error.is_none()
                && invariants_valid
                && targets_met
                && no_bad_results,
            interrupted,
            schedule_complete,
            scope: "scheduler",
            termination_error: termination_error
                .map(|error| error.chars().take(MAX_ERROR_CONTEXT).collect::<String>()),
            targets,
            work_items,
            accounting_invariants: invariants,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TokenState {
    Offered,
    Accepted,
    Terminal,
}

#[derive(Debug)]
struct AccountingToken {
    accounting: Arc<RunAccounting>,
    class: WorkClass,
    state: TokenState,
    accepted_at: Option<Instant>,
    phase: Option<(Arc<PhaseTracker>, usize)>,
}

impl AccountingToken {
    fn mark_accepted(&mut self) {
        if !matches!(self.state, TokenState::Offered) {
            return;
        }
        self.state = TokenState::Accepted;
        self.accepted_at = Some(Instant::now());
        self.accounting
            .class(self.class)
            .accepted
            .fetch_add(1, AtomicOrdering::Release);
    }

    fn record_dequeue(&mut self) {
        if !matches!(self.state, TokenState::Accepted) {
            return;
        }
        if let Some(accepted_at) = self.accepted_at.take() {
            metrics::record_queue_wait(self.class.as_str(), accepted_at.elapsed());
        }
    }

    fn mark_complete(&mut self, outcome: Outcome) {
        if !matches!(self.state, TokenState::Accepted) {
            return;
        }
        let counters = self.accounting.class(self.class);
        if outcome == Outcome::Success {
            counters.succeeded.fetch_add(1, AtomicOrdering::Release);
        } else {
            counters.failed.fetch_add(1, AtomicOrdering::Release);
            if outcome == Outcome::ResultUnknown {
                counters
                    .result_unknown
                    .fetch_add(1, AtomicOrdering::Release);
            }
        }
        if let Some((tracker, resource)) = &self.phase {
            tracker.record(*resource, outcome);
        }
        self.state = TokenState::Terminal;
    }

    fn mark_dropped(&mut self, reason: DropReason) {
        if !matches!(self.state, TokenState::Offered) {
            return;
        }
        match reason {
            DropReason::Backpressure => {
                self.accounting
                    .class(self.class)
                    .dropped_full
                    .fetch_add(1, AtomicOrdering::Release);
            }
            DropReason::QueueClosed => {
                self.accounting
                    .class(self.class)
                    .dropped_closed
                    .fetch_add(1, AtomicOrdering::Release);
            }
        }
        self.state = TokenState::Terminal;
    }

    fn mark_unissued(&mut self) {
        if !matches!(self.state, TokenState::Offered) {
            return;
        }
        self.accounting
            .class(self.class)
            .unissued
            .fetch_add(1, AtomicOrdering::Release);
        self.state = TokenState::Terminal;
    }

    fn mark_unfinished(&mut self) {
        if !matches!(self.state, TokenState::Accepted) {
            return;
        }
        self.accounting
            .class(self.class)
            .unfinished
            .fetch_add(1, AtomicOrdering::Release);
        if let Some((tracker, resource)) = &self.phase {
            tracker.record_unfinished(*resource);
        }
        self.state = TokenState::Terminal;
    }
}

impl Drop for AccountingToken {
    fn drop(&mut self) {
        match self.state {
            TokenState::Offered => self.mark_unissued(),
            TokenState::Accepted => self.mark_unfinished(),
            TokenState::Terminal => {}
        }
    }
}

fn tracked(accounting: &Arc<RunAccounting>, class: WorkClass) -> AccountingToken {
    accounting.offer(class)
}

fn tracked_phase(
    accounting: &Arc<RunAccounting>,
    class: WorkClass,
    completion: Arc<PhaseTracker>,
    resource: usize,
) -> AccountingToken {
    let mut token = accounting.offer(class);
    token.phase = Some((completion, resource));
    token
}

#[derive(Debug)]
enum WorkItem {
    Txn {
        resource: usize,
        key: Vec<u8>,
        operation: ResourceOperation,
        value: Vec<u8>,
        pool: Option<String>,
        accounting: AccountingToken,
    },
    Range {
        request: RangeRequest,
        selector_keep_every: u64,
        target: u64,
        accounting: AccountingToken,
    },
    Heartbeat {
        item: HeartbeatItem,
        accounting: AccountingToken,
    },
    Bootstrap {
        item: HeartbeatItem,
        done: oneshot::Sender<Result<(), String>>,
        accounting: AccountingToken,
    },
    Status {
        accounting: AccountingToken,
    },
    CompactTick {
        accounting: AccountingToken,
    },
}

impl WorkItem {
    fn mark_accepted(&mut self) {
        match self {
            Self::Txn { accounting, .. }
            | Self::Range { accounting, .. }
            | Self::Heartbeat { accounting, .. }
            | Self::Bootstrap { accounting, .. }
            | Self::Status { accounting }
            | Self::CompactTick { accounting } => accounting.mark_accepted(),
        }
    }

    fn record_dequeue(&mut self) {
        match self {
            Self::Txn { accounting, .. }
            | Self::Range { accounting, .. }
            | Self::Heartbeat { accounting, .. }
            | Self::Bootstrap { accounting, .. }
            | Self::Status { accounting }
            | Self::CompactTick { accounting } => accounting.record_dequeue(),
        }
    }

    fn mark_dropped(&mut self, reason: DropReason) {
        match self {
            Self::Txn { accounting, .. }
            | Self::Range { accounting, .. }
            | Self::Heartbeat { accounting, .. }
            | Self::Bootstrap { accounting, .. }
            | Self::Status { accounting }
            | Self::CompactTick { accounting } => accounting.mark_dropped(reason),
        }
    }

    fn mark_unissued(&mut self) {
        match self {
            Self::Txn { accounting, .. }
            | Self::Range { accounting, .. }
            | Self::Heartbeat { accounting, .. }
            | Self::Bootstrap { accounting, .. }
            | Self::Status { accounting }
            | Self::CompactTick { accounting } => accounting.mark_unissued(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactorStatus {
    Active,
    ResyncRequired,
}

impl Default for CompactorStatus {
    fn default() -> Self {
        Self::Active
    }
}

impl CompactorStatus {
    fn as_metric(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::ResyncRequired => "resync_required",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactorRecovery {
    MissingKey,
    InvalidValue,
}

impl CompactorRecovery {
    fn as_metric(self) -> &'static str {
        match self {
            Self::MissingKey => "missing_key",
            Self::InvalidValue => "invalid_value",
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
enum CompactorObservationError {
    #[error("compact journal failure response is missing a key-value")]
    FailureKvMissing,
    #[error("compact journal failure value is not valid UTF-8")]
    FailureValueNotUtf8,
    #[error("compact journal failure value is not a decimal revision")]
    FailureValueNotDecimal,
    #[error("compact journal failure response is missing key version")]
    FailureVersionMissing,
    #[error("compact journal failure key version is invalid")]
    FailureVersionInvalid,
}

impl CompactorObservationError {
    fn as_metric(self) -> &'static str {
        match self {
            Self::FailureKvMissing => "failure_kv_missing",
            Self::FailureValueNotUtf8 => "failure_value_not_utf8",
            Self::FailureValueNotDecimal => "failure_value_not_decimal",
            Self::FailureVersionMissing => "failure_version_missing",
            Self::FailureVersionInvalid => "failure_version_invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompactorState {
    compact_version: i64,
    previous_txn_header_revision: i64,
    status: CompactorStatus,
    recovery: Option<CompactorRecovery>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompactTxnObservation {
    header_revision: i64,
    compact_target: Option<i64>,
    remote_compact_revision: Option<i64>,
    succeeded: bool,
    recovered: bool,
    recovery: Option<CompactorRecovery>,
}

impl CompactorState {
    fn txn_inputs(&self) -> (i64, i64) {
        (self.compact_version, self.previous_txn_header_revision)
    }

    fn is_active(&self) -> bool {
        self.status == CompactorStatus::Active
    }

    fn mark_resync_required(&mut self, header_revision: i64, recovery: CompactorRecovery) {
        self.compact_version = 0;
        self.previous_txn_header_revision = header_revision;
        self.status = CompactorStatus::ResyncRequired;
        self.recovery = Some(recovery);
    }

    fn mark_recovered(&mut self, header_revision: i64, version: i64) {
        self.compact_version = version;
        self.previous_txn_header_revision = header_revision;
        self.status = CompactorStatus::Active;
        self.recovery = None;
    }

    fn observe(
        &mut self,
        response: &TxnResponse,
    ) -> Result<CompactTxnObservation, CompactorObservationError> {
        let header_revision = response.header.as_ref().map_or(0, |header| header.revision);
        let old_previous = self.previous_txn_header_revision;
        let was_resync = !self.is_active();
        if response.succeeded {
            let recovery = self.recovery;
            if was_resync {
                // A Version==0 compare succeeding proves the journal key was
                // absent.  Rebuilding it is safe and this tick must not
                // compact anything based on the stale local header.
                self.mark_recovered(header_revision, 1);
            } else {
                self.previous_txn_header_revision = header_revision;
                self.compact_version = self.compact_version.saturating_add(1);
            }
            return Ok(CompactTxnObservation {
                header_revision,
                compact_target: (!was_resync && old_previous > 0).then_some(old_previous),
                remote_compact_revision: None,
                succeeded: true,
                recovered: was_resync,
                recovery,
            });
        }

        let Some(value) = failure_value(response) else {
            self.mark_resync_required(header_revision, CompactorRecovery::MissingKey);
            return Err(CompactorObservationError::FailureKvMissing);
        };
        let value = match std::str::from_utf8(value) {
            Ok(value) => value,
            Err(_) => {
                self.mark_resync_required(header_revision, CompactorRecovery::InvalidValue);
                return Err(CompactorObservationError::FailureValueNotUtf8);
            }
        };
        let remote_compact_revision = match value.parse::<i64>() {
            Ok(value) if value >= 0 => value,
            Ok(_) | Err(_) => {
                self.mark_resync_required(header_revision, CompactorRecovery::InvalidValue);
                return Err(CompactorObservationError::FailureValueNotDecimal);
            }
        };
        let Some(version) = failure_version(response) else {
            self.mark_resync_required(header_revision, CompactorRecovery::InvalidValue);
            return Err(CompactorObservationError::FailureVersionMissing);
        };
        if version <= 0 {
            self.mark_resync_required(header_revision, CompactorRecovery::InvalidValue);
            return Err(CompactorObservationError::FailureVersionInvalid);
        }
        let recovery = self.recovery;
        if was_resync {
            // A Version==0 probe can only fail while the key exists.  A
            // legal value/version pair therefore supplies a complete,
            // auditable resync snapshot; no Compact is issued in this tick.
            self.mark_recovered(header_revision, version);
        } else {
            self.previous_txn_header_revision = header_revision;
            self.compact_version = version;
        }
        Ok(CompactTxnObservation {
            header_revision,
            compact_target: None,
            remote_compact_revision: Some(remote_compact_revision),
            succeeded: false,
            recovered: was_resync,
            recovery,
        })
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScheduleStats {
    pub scheduled: u64,
    pub dropped: u64,
}

pub struct Scheduler {
    workload: WorkloadSpec,
    nodes: usize,
    seed: u64,
}
impl Scheduler {
    pub fn new(config: &RuntimeConfig) -> Self {
        Self {
            workload: config.workload.clone(),
            nodes: config.nodes as usize,
            seed: config.seed,
        }
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(
            self.workload
                .timeline
                .iter()
                .map(|s| s.duration_s)
                .sum::<f64>(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Range,
    MasterLease {
        resource: usize,
        id: u64,
        ordinal: u64,
    },
    NodeLease(usize),
    NodeStatus {
        node: usize,
        tick: u64,
    },
    Status(u64),
    Compact(u64),
    PhaseEnd,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Scheduled {
    at: Duration,
    rank: u8,
    sequence: u64,
    source: Source,
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.rank.cmp(&self.rank))
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct RateSegment {
    offset: Duration,
    duration: Duration,
    count: u64,
    emitted: u64,
}
struct RateCursor {
    segments: Vec<RateSegment>,
    index: usize,
    ordinal: u64,
}
impl RateCursor {
    fn new(workload: &WorkloadSpec, seed: u64, tag: u64) -> Self {
        let mut offset = Duration::ZERO;
        let mut segments = Vec::with_capacity(workload.timeline.len());
        for (index, segment) in workload.timeline.iter().enumerate() {
            let duration = Duration::from_secs_f64(segment.duration_s);
            let rate = segment.range_rps;
            let expected = rate.max(0.0) * duration.as_secs_f64();
            let base = expected.floor() as u64;
            let count = base
                + u64::from(expected.fract() + deterministic_unit(seed ^ tag, index as u64) >= 1.0);
            segments.push(RateSegment {
                offset,
                duration,
                count,
                emitted: 0,
            });
            offset = offset.saturating_add(duration);
        }
        Self {
            segments,
            index: 0,
            ordinal: 0,
        }
    }

    fn next(&mut self) -> Option<(Duration, u64)> {
        loop {
            let segment = self.segments.get_mut(self.index)?;
            if segment.emitted < segment.count {
                segment.emitted += 1;
                self.ordinal = self.ordinal.wrapping_add(1);
                let at = segment.offset.saturating_add(
                    segment
                        .duration
                        .mul_f64(segment.emitted as f64 / segment.count as f64),
                );
                return Some((at, self.ordinal - 1));
            }
            self.index += 1;
        }
    }

    fn total(&self) -> u64 {
        self.segments.iter().map(|s| s.count).sum()
    }
}

pub(crate) fn application_operation_total(workload: &WorkloadSpec) -> u64 {
    workload
        .resources
        .iter()
        .filter(|resource| resource.behavior != ResourceBehavior::MasterLease)
        .try_fold(0u64, |total, resource| {
            let count = workload
                .resource_count(resource)
                .expect("workload resource counts were validated");
            let updates = count
                .checked_mul(resource.update_amplification)
                .expect("workload update target was validated");
            total
                .checked_add(count)
                .and_then(|value| value.checked_add(updates))
        })
        .expect("workload application target was validated")
}

/// Total number of independent Range requests the scheduler will emit for
/// this workload's actual timeline, using the same per-segment deterministic
/// rounding as the runtime cursor.
pub(crate) fn range_request_total(workload: &WorkloadSpec, seed: u64) -> u64 {
    RateCursor::new(workload, seed, 0xB2).total()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PlannedOperation {
    resource: usize,
    id: u64,
}

#[derive(Clone, Debug)]
struct PlannedResource {
    resource: usize,
    count: u64,
    update_amplification: u64,
}

#[derive(Clone, Debug)]
struct WorkloadPlan {
    resources: Vec<PlannedResource>,
    total_creates: u64,
    total_updates: u64,
}

impl WorkloadPlan {
    fn build(workload: &WorkloadSpec) -> Result<Self, String> {
        let mut resources = Vec::with_capacity(workload.resources.len());
        let mut total_creates = 0u64;
        let mut total_updates = 0u64;
        for (resource, spec) in workload.resources.iter().enumerate() {
            let count = workload
                .resource_count(spec)
                .map_err(|error| error.to_string())?;
            let updates = count
                .checked_mul(spec.update_amplification)
                .ok_or_else(|| format!("resource {:?} update target overflows u64", spec.name))?;
            total_creates = total_creates
                .checked_add(count)
                .ok_or_else(|| "total create target overflows u64".to_string())?;
            total_updates = total_updates
                .checked_add(updates)
                .ok_or_else(|| "total update target overflows u64".to_string())?;
            resources.push(PlannedResource {
                resource,
                count,
                update_amplification: spec.update_amplification,
            });
        }
        Ok(Self {
            resources,
            total_creates,
            total_updates,
        })
    }

    fn total_creates(&self) -> u64 {
        self.total_creates
    }

    fn total_updates(&self) -> u64 {
        self.total_updates
    }

    fn create_targets(&self, resource_count: usize) -> Vec<u64> {
        let mut targets = vec![0; resource_count];
        for resource in &self.resources {
            targets[resource.resource] = resource.count;
        }
        targets
    }

    fn update_targets(&self, resource_count: usize) -> Vec<u64> {
        let mut targets = vec![0; resource_count];
        for resource in &self.resources {
            targets[resource.resource] = resource
                .count
                .checked_mul(resource.update_amplification)
                .expect("workload update target was validated");
        }
        targets
    }

    fn creates(&self) -> CreateCursor<'_> {
        CreateCursor {
            resources: &self.resources,
            resource_index: 0,
            id: 0,
        }
    }
}

struct CreateCursor<'a> {
    resources: &'a [PlannedResource],
    resource_index: usize,
    id: u64,
}

impl Iterator for CreateCursor<'_> {
    type Item = PlannedOperation;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let resource = self.resources.get(self.resource_index)?;
            if self.id < resource.count {
                let operation = PlannedOperation {
                    resource: resource.resource,
                    id: self.id,
                };
                self.id += 1;
                return Some(operation);
            }
            self.resource_index += 1;
            self.id = 0;
        }
    }
}

#[derive(Clone, Debug)]
struct UpdateResource {
    resource: usize,
    count: u64,
    remaining: u64,
    weight: u64,
    emitted: u64,
    deficit: i128,
    start_id: u64,
}

struct UpdateCursor {
    resources: Vec<UpdateResource>,
    total: u64,
    remaining: u64,
}

impl UpdateCursor {
    fn new(plan: &WorkloadPlan, seed: u64) -> Self {
        let resources: Vec<_> = plan
            .resources
            .iter()
            .filter(|resource| resource.update_amplification > 0)
            .map(|resource| {
                let target = resource
                    .count
                    .checked_mul(resource.update_amplification)
                    .expect("workload update target was validated");
                UpdateResource {
                    resource: resource.resource,
                    count: resource.count,
                    weight: target,
                    remaining: target,
                    emitted: 0,
                    deficit: 0,
                    start_id: splitmix(seed ^ resource.resource as u64) % resource.count,
                }
            })
            .collect();
        Self {
            resources,
            total: plan.total_updates,
            remaining: plan.total_updates,
        }
    }
}

impl Iterator for UpdateCursor {
    type Item = PlannedOperation;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let mut best = None;
        for (index, resource) in self.resources.iter_mut().enumerate() {
            if resource.remaining == 0 {
                continue;
            }
            resource.deficit += resource.weight as i128;
            if best.is_none_or(|(_, deficit)| resource.deficit > deficit) {
                best = Some((index, resource.deficit));
            }
        }
        let (index, _) = best?;
        let resource = &mut self.resources[index];
        resource.deficit -= self.total as i128;
        let id = ((resource.start_id as u128 + resource.emitted as u128) % resource.count as u128)
            as u64;
        resource.emitted += 1;
        resource.remaining -= 1;
        self.remaining -= 1;
        Some(PlannedOperation {
            resource: resource.resource,
            id,
        })
    }
}

#[derive(Clone, Copy)]
enum PhaseKind {
    Create,
    Update,
}

#[derive(Debug)]
struct PhaseTracker {
    target: u64,
    targets: Vec<u64>,
    completed: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    result_unknown: AtomicU64,
    unfinished: AtomicU64,
    succeeded_by_resource: Vec<AtomicU64>,
    failed_by_resource: Vec<AtomicU64>,
    notify: Notify,
}

impl PhaseTracker {
    fn new(targets: Vec<u64>) -> Self {
        let target = targets
            .iter()
            .try_fold(0u64, |total, target| total.checked_add(*target))
            .expect("phase targets were validated");
        let succeeded_by_resource = targets.iter().map(|_| AtomicU64::new(0)).collect();
        let failed_by_resource = targets.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            target,
            targets,
            completed: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            result_unknown: AtomicU64::new(0),
            unfinished: AtomicU64::new(0),
            succeeded_by_resource,
            failed_by_resource,
            notify: Notify::new(),
        }
    }

    fn record(&self, resource: usize, outcome: Outcome) {
        if outcome == Outcome::Success {
            self.succeeded.fetch_add(1, AtomicOrdering::Relaxed);
            if let Some(succeeded) = self.succeeded_by_resource.get(resource) {
                succeeded.fetch_add(1, AtomicOrdering::Relaxed);
            }
        } else {
            self.failed.fetch_add(1, AtomicOrdering::Relaxed);
            if outcome == Outcome::ResultUnknown {
                self.result_unknown.fetch_add(1, AtomicOrdering::Relaxed);
            }
            if let Some(failed) = self.failed_by_resource.get(resource) {
                failed.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
        self.record_terminal();
    }

    fn record_unfinished(&self, resource: usize) {
        self.unfinished.fetch_add(1, AtomicOrdering::Relaxed);
        self.record_terminal();
        if let Some(failed) = self.failed_by_resource.get(resource) {
            failed.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    fn record_terminal(&self) {
        let completed = self.completed.fetch_add(1, AtomicOrdering::Release) + 1;
        if completed >= self.target {
            self.notify.notify_waiters();
        }
    }

    fn result(&self, phase: &str) -> Result<(), String> {
        let completed = self.completed.load(AtomicOrdering::Acquire);
        let succeeded = self.succeeded.load(AtomicOrdering::Acquire);
        let deficit = self.target.saturating_sub(succeeded);
        if completed == self.target && deficit == 0 {
            Ok(())
        } else {
            Err(self.status(phase, completed, succeeded, deficit))
        }
    }

    fn deadline_error(&self, phase: &str) -> String {
        let completed = self.completed.load(AtomicOrdering::Acquire);
        let succeeded = self.succeeded.load(AtomicOrdering::Acquire);
        let deficit = self.target.saturating_sub(succeeded);
        format!(
            "{phase} phase deadline exceeded: {}",
            self.status(phase, completed, succeeded, deficit)
        )
    }

    fn status(&self, phase: &str, completed: u64, succeeded: u64, deficit: u64) -> String {
        format!(
            "{phase} phase incomplete: target={}, completed={completed}, succeeded={succeeded}, deficit={deficit}",
            self.target
        )
    }

    fn publish(&self, workload: &WorkloadSpec, kind: PhaseKind) {
        for (resource, target) in self.targets.iter().copied().enumerate() {
            let Some(spec) = workload.resources.get(resource) else {
                continue;
            };
            let label = ResourceName::try_from(spec.name.as_str())
                .expect("workload resource labels were validated");
            let succeeded = self.succeeded_by_resource[resource].load(AtomicOrdering::Acquire);
            let failed = self.failed_by_resource[resource].load(AtomicOrdering::Acquire);
            let deficit = target.saturating_sub(succeeded);
            match kind {
                PhaseKind::Create => {
                    metrics::set_resource_objects(&label, "planned", target);
                    metrics::set_resource_objects(&label, "succeeded", succeeded);
                    metrics::set_resource_objects(&label, "failed", failed);
                    metrics::set_resource_objects(&label, "deficit", deficit);
                }
                PhaseKind::Update => {
                    metrics::set_resource_updates(&label, "target", target);
                    metrics::set_resource_updates(&label, "succeeded", succeeded);
                    metrics::set_resource_updates(&label, "failed", failed);
                    metrics::set_resource_updates(&label, "deficit", deficit);
                }
            }
        }
    }

    async fn wait_until(&self, phase: &str, deadline: TokioInstant) -> Result<(), String> {
        loop {
            let notified = self.notify.notified();
            if self.completed.load(AtomicOrdering::Acquire) >= self.target {
                return self.result(phase);
            }
            if time::timeout_at(deadline, notified).await.is_err() {
                return Err(self.deadline_error(phase));
            }
        }
    }
}

#[derive(Clone)]
struct RangeProducer {
    deficits: Vec<f64>,
    total: f64,
}
impl RangeProducer {
    fn new(weights: impl Iterator<Item = f64>) -> Self {
        let deficits: Vec<f64> = weights.collect();
        Self {
            total: deficits.iter().sum(),
            deficits,
        }
    }

    fn next(&mut self, weights: &[f64]) -> Option<usize> {
        if self.deficits.is_empty() || self.total <= 0.0 {
            return None;
        }
        for (d, w) in self.deficits.iter_mut().zip(weights) {
            *d += *w;
        }
        let index = self
            .deficits
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .map(|(i, _)| i)?;
        self.deficits[index] -= self.total;
        Some(index)
    }
}

struct RangeShapePlan {
    prefix_total: u64,
    seen: u64,
    count_only: u64,
}

impl RangeShapePlan {
    fn new(prefix_total: u64, count_only: u64) -> Self {
        Self {
            prefix_total,
            seen: 0,
            count_only,
        }
    }

    fn takes(slot: u64, target: u64, total: u64) -> bool {
        if total == 0 {
            return false;
        }
        let before = slot as u128 * target as u128 / total as u128;
        let after = (slot + 1) as u128 * target as u128 / total as u128;
        after > before
    }

    fn apply(&mut self, request: &mut RangeRequest) {
        let slot = self.seen;
        self.seen = self.seen.saturating_add(1);
        if !Self::takes(slot, self.count_only, self.prefix_total) {
            return;
        }
        request.count_only = true;
        request.limit = 0;
    }
}
/// Explicit selector policy used by the Range pager. One means all candidates
/// match; larger values deterministically discard every Nth candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RangeSelector {
    All { target: Option<u64> },
    DropEvery { every: usize, target: Option<u64> },
}

impl RangeSelector {
    fn selected_in_page(self, key_count: usize, page_offset: usize) -> usize {
        match self {
            Self::All { .. } => key_count,
            Self::DropEvery { every, .. } if every <= 1 => key_count,
            Self::DropEvery { every, .. } => (0..key_count)
                .filter(|index| (page_offset + *index + 1) % every != 0)
                .count(),
        }
    }

    fn target(self) -> Option<u64> {
        match self {
            Self::All { target } | Self::DropEvery { target, .. } => target,
        }
    }
}

fn configured_range_selector(selector_keep_every: u64, target: u64) -> RangeSelector {
    let target = (target > 0).then_some(target);
    if selector_keep_every <= 1 {
        RangeSelector::All { target }
    } else {
        RangeSelector::DropEvery {
            every: usize::try_from(selector_keep_every).unwrap_or(usize::MAX),
            target,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RangeExecutionResult {
    pages: u64,
    snapshot_revision: i64,
    server_count: i64,
    last_key: Vec<u8>,
    selected: u64,
    discarded: u64,
}
#[derive(Debug, Error)]
#[error("{message}")]
struct RangeExecutionError {
    outcome: Outcome,
    message: String,
}

impl RangeExecutionError {
    fn new(outcome: Outcome, message: impl Into<String>) -> Self {
        Self {
            outcome,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RangePageOutcome {
    Complete,
    Continue(RangeRequest),
}

#[derive(Clone, Debug)]
struct RangePager {
    request: RangeRequest,
    paged: bool,
    revision: Option<i64>,
    page_limit: i64,
    pages: u64,
    server_count: i64,
    last_key: Vec<u8>,
    selected: u64,
    discarded: u64,
    target: Option<u64>,
}

impl RangePager {
    const INITIAL_LIMIT: i64 = 500;
    const MAX_LIMIT: i64 = 10_000;

    fn new(mut request: RangeRequest, selector: RangeSelector) -> Self {
        // Only bounded prefix LISTs use the internal 500-page pager.  An
        // unbounded prefix (`limit=0`) preserves its one-shot wire shape.
        let paged = !request.count_only && !request.range_end.is_empty() && request.limit > 0;
        let page_limit = if paged {
            Self::INITIAL_LIMIT
        } else {
            request.limit
        };
        if paged {
            request.limit = page_limit;
        }
        Self {
            request,
            paged,
            revision: None,
            page_limit,
            pages: 0,
            server_count: 0,
            last_key: Vec::new(),
            selected: 0,
            discarded: 0,
            target: selector.target(),
        }
    }

    fn request(&self) -> RangeRequest {
        self.request.clone()
    }

    fn result(&self) -> RangeExecutionResult {
        RangeExecutionResult {
            pages: self.pages,
            snapshot_revision: self.revision.unwrap_or_default(),
            server_count: self.server_count,
            last_key: self.last_key.clone(),
            selected: self.selected,
            discarded: self.discarded,
        }
    }

    fn observe_response(
        &mut self,
        response: &crate::pb::etcdserverpb::RangeResponse,
        selected_in_page: usize,
    ) -> Result<RangePageOutcome, String> {
        let response_revision = response.header.as_ref().map_or(0, |header| header.revision);
        if self.revision.is_none() {
            // Historical Range responses report the server's current revision,
            // not necessarily the snapshot selected by the request.
            // https://github.com/etcd-io/etcd/blob/v3.6.5/server/storage/mvcc/kvstore_txn.go
            let snapshot = if self.request.revision > 0 {
                self.request.revision
            } else {
                response_revision
            };
            if self.paged && snapshot <= 0 {
                return Err("Range page has no valid snapshot revision".to_string());
            }
            self.revision = Some(snapshot);
        }
        self.pages = self.pages.saturating_add(1);
        self.server_count = response.count;
        if let Some(last) = response.kvs.last() {
            self.last_key = last.key.clone();
        }
        let selected_in_page = selected_in_page.min(response.kvs.len());
        self.selected = self.selected.saturating_add(selected_in_page as u64);
        self.discarded = self
            .discarded
            .saturating_add((response.kvs.len() - selected_in_page) as u64);
        if !self.paged {
            return Ok(RangePageOutcome::Complete);
        }
        if response.more && response.kvs.is_empty() {
            return Err("Range response has more=true but no KVs".to_string());
        }
        if self.target.is_some_and(|target| self.selected >= target) {
            return Ok(RangePageOutcome::Complete);
        }
        if !response.more {
            return Ok(RangePageOutcome::Complete);
        }
        let last_key = response
            .kvs
            .last()
            .map(|kv| kv.key.clone())
            .ok_or_else(|| "Range response has more=true but no KVs".to_string())?;
        if selected_in_page < response.kvs.len() {
            self.page_limit = self.page_limit.saturating_mul(2).min(Self::MAX_LIMIT);
        }
        self.request.key = {
            let mut key = last_key;
            key.push(0);
            key
        };
        self.request.limit = self.page_limit;
        self.request.revision = self.revision.unwrap_or_default();
        Ok(RangePageOutcome::Continue(self.request()))
    }
}
fn range_shape(request: &RangeRequest) -> RangeShape {
    if request.range_end.is_empty() {
        RangeShape::Point
    } else if request.count_only {
        RangeShape::CountOnly
    } else if request.limit == 0 {
        RangeShape::UnboundedPrefix
    } else {
        RangeShape::PagedPrefix
    }
}

async fn execute_range(
    client: &ClientBundle,
    request: RangeRequest,
    selector: RangeSelector,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Result<RangeExecutionResult, RangeExecutionError> {
    const MAX_PAGE_ATTEMPTS: u32 = 8;
    if timeout.is_zero() {
        return Err(RangeExecutionError::new(
            Outcome::Timeout,
            "Range operation deadline is zero",
        ));
    }
    let deadline = Instant::now() + timeout;
    let mut pager = RangePager::new(request, selector);
    let mut page_offset = 0usize;
    loop {
        let page_request = pager.request();
        let shape = range_shape(&page_request);
        let mut attempts = 0;
        let response = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                metrics::record_request(Rpc::Range, Outcome::Timeout);
                metrics::record_range_request(shape, Outcome::Timeout);
                break Err(RangeExecutionError::new(
                    Outcome::Timeout,
                    "Range operation deadline exceeded",
                ));
            }
            attempts += 1;
            let start = Instant::now();
            metrics::add_inflight(Rpc::Range);
            let result = time::timeout(remaining, client.range(page_request.clone())).await;
            metrics::remove_inflight(Rpc::Range);
            metrics::record_request_duration(Rpc::Range, start.elapsed());
            metrics::record_request_bytes(
                &ResourceName::new("range").unwrap(),
                page_request.key.len() + page_request.range_end.len(),
            );
            match result {
                Ok(Ok(response)) => {
                    metrics::record_request(Rpc::Range, Outcome::Success);
                    metrics::record_range_request(shape, Outcome::Success);
                    break Ok(response);
                }
                Ok(Err(error)) => {
                    let decision =
                        error.retry_decision(RpcMethod::Range, RequestDisposition::Unknown);
                    let outcome = match decision {
                        RetryDecision::ResultUnknown => Outcome::ResultUnknown,
                        RetryDecision::NotSent | RetryDecision::Retry | RetryDecision::Stop => {
                            Outcome::GrpcError
                        }
                    };
                    metrics::record_request(Rpc::Range, outcome);
                    metrics::record_range_request(shape, outcome);
                    if decision == RetryDecision::Retry && attempts < MAX_PAGE_ATTEMPTS {
                        continue;
                    }
                    let message = if decision == RetryDecision::Retry {
                        format!("Range retry limit reached: {error}")
                    } else {
                        error.to_string()
                    };
                    break Err(RangeExecutionError::new(outcome, message));
                }
                Err(_) => {
                    metrics::record_request(Rpc::Range, Outcome::Timeout);
                    metrics::record_range_request(shape, Outcome::Timeout);
                    break Err(RangeExecutionError::new(
                        Outcome::Timeout,
                        "Range page request deadline exceeded",
                    ));
                }
            }
        }?;
        if let Some(header) = response.header.as_ref() {
            update_revision(&latest_revision, header.revision);
        }
        let selected = selector.selected_in_page(response.kvs.len(), page_offset);
        page_offset = page_offset.saturating_add(response.kvs.len());
        match pager
            .observe_response(&response, selected)
            .map_err(|error| RangeExecutionError::new(Outcome::GrpcError, error))?
        {
            RangePageOutcome::Complete => return Ok(pager.result()),
            RangePageOutcome::Continue(_) => {}
        }
    }
}

fn exact_prefix_total(
    mut producer: RangeProducer,
    weights: &[f64],
    ranges: &[crate::config::RangeSpec],
    scheduled_ranges: u64,
) -> u64 {
    (0..scheduled_ranges)
        .filter_map(|_| producer.next(weights))
        .filter(|index| ranges[*index].kind == "prefix")
        .count() as u64
}

/// Exact number of prefix-kind requests among `range_total` scheduled Range
/// requests, using the same deterministic producer sequence as the runtime.
/// Monotone in `range_total` (longer schedules extend the same sequence).
pub(crate) fn exact_prefix_capacity(workload: &WorkloadSpec, range_total: u64) -> u64 {
    let weights: Vec<f64> = workload.ranges.iter().map(|range| range.weight).collect();
    let producer = RangeProducer::new(weights.iter().copied());
    exact_prefix_total(producer, &weights, &workload.ranges, range_total)
}

/// Masterlease tick time for `ordinal`, bounded by workload duration so
/// periodic traffic never outlives the run. Computes the prospective time
/// without overflowing even for huge intervals or ordinals.
fn masterlease_tick_at(interval: Duration, ordinal: u64, duration: Duration) -> Option<Duration> {
    let secs = interval.as_secs_f64() * (ordinal + 1) as f64;
    if !secs.is_finite() || secs > duration.as_secs_f64() {
        return None;
    }
    Some(Duration::from_secs_f64(secs))
}

fn range_shape_plan(workload: &WorkloadSpec, prefix_total: u64) -> Result<RangeShapePlan, String> {
    let count_only = workload.range_count_only_count;
    if count_only > prefix_total {
        return Err(format!(
            "Range shape count ({count_only}) exceeds scheduled prefix Range capacity ({prefix_total})",
        ));
    }
    Ok(RangeShapePlan::new(prefix_total, count_only))
}
fn splitmix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn deterministic_unit(seed: u64, index: u64) -> f64 {
    const MANTISSA_BITS: u32 = 53;
    (splitmix(seed ^ index) >> (64 - MANTISSA_BITS)) as f64 / (1u64 << MANTISSA_BITS) as f64
}

fn stable_shard(key: &[u8], workers: usize) -> usize {
    if workers == 0 {
        0
    } else {
        key.iter()
            .fold(0u64, |hash, byte| splitmix(hash ^ *byte as u64)) as usize
            % workers
    }
}
fn patterned_key(pattern: &str, id: u64) -> Vec<u8> {
    pattern
        .replace("{ns}", "bench")
        .replace("{id}", &format!("{id:05}"))
        .into_bytes()
}
fn resource_key(resource: &ResourceSpec, id: u64) -> Vec<u8> {
    patterned_key(&resource.key_pattern, id)
}
fn patterned_value(size: usize, id: u64, n: u64) -> Vec<u8> {
    let mut value = vec![0; size];
    let marker = splitmix(id ^ n);
    for (i, byte) in value.iter_mut().enumerate() {
        *byte = marker.rotate_left((i % 63) as u32) as u8;
    }
    value
}
fn resource_value(resource: &ResourceSpec, id: u64, n: u64) -> Vec<u8> {
    patterned_value(resource.value_size_bytes, id, n)
}
fn range_request(spec: &crate::config::RangeSpec, n: u64) -> RangeRequest {
    let key = spec
        .key_pattern
        .replace("{ns}", "bench")
        .replace("{id}", &format!("{:05}", n % 1000))
        .into_bytes();
    RangeRequest {
        key: key.clone(),
        range_end: if spec.kind == "prefix" {
            prefix_range_end(&key)
        } else {
            Vec::new()
        },
        limit: spec.limit as i64,
        count_only: spec.count_only,
        serializable: false,
        keys_only: false,
        ..Default::default()
    }
}
fn node_status_phase(node: usize, nodes: usize, interval: Duration) -> Duration {
    interval.mul_f64((node + 1) as f64 / nodes as f64)
}

fn heartbeat_interval(
    seed: u64,
    node: usize,
    tick: u64,
    base: Duration,
    jitter_ratio: f64,
) -> Duration {
    let nanos = base.as_nanos() as u128;
    let jitter = (nanos as f64 * jitter_ratio).round() as u128;
    if jitter == 0 {
        return base;
    }
    let mut z = seed
        .wrapping_add((node as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .wrapping_add(tick.rotate_left(17));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    let span = jitter.saturating_mul(2).saturating_add(1);
    let offset = (z as u128 % span) as i128 - jitter as i128;
    Duration::from_nanos((nanos as i128 + offset).max(1) as u64)
}
struct SharedPool {
    name: PoolName,
    pool: Mutex<LeasePool>,
}

type WatchTaskOutput = (
    String,
    Result<crate::watch::WatchOutcome, crate::watch::WatchError>,
);

fn spawn_watches(
    client: &ClientBundle,
    specs: &[crate::config::WatchSpec],
) -> (
    shutdown_watch::Sender<bool>,
    JoinSet<WatchTaskOutput>,
    Vec<oneshot::Receiver<()>>,
) {
    let (shutdown, receiver) = shutdown_watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut ready_receivers = Vec::with_capacity(specs.len());
    for spec in specs.iter().cloned() {
        let name = spec.name.clone();
        let watch_receiver = receiver.clone();
        let watch_client = client.clone();
        let (ready_sender, ready_receiver) = oneshot::channel();
        ready_receivers.push(ready_receiver);
        tasks.spawn(async move {
            let result = crate::watch::run(watch_client, spec, watch_receiver, ready_sender).await;
            (name, result)
        });
    }
    (shutdown, tasks, ready_receivers)
}

fn watch_failure(
    name: &str,
    result: Result<crate::watch::WatchOutcome, crate::watch::WatchError>,
) -> Option<String> {
    match result {
        Ok(outcome) if outcome.terminal == crate::watch::WatchTerminal::ContextCanceled => None,
        Ok(outcome) => Some(format!("watch {name} terminated: {:?}", outcome.terminal)),
        Err(error) => Some(format!("watch {name} failed: {error}")),
    }
}

async fn wait_for_watch_ready(
    tasks: &mut JoinSet<WatchTaskOutput>,
    ready_receivers: Vec<oneshot::Receiver<()>>,
    timeout: Duration,
    ctrl_c: &mut CtrlCFuture,
) -> Result<(), String> {
    if ready_receivers.is_empty() {
        return Ok(());
    }
    let deadline = TokioInstant::now() + timeout;
    for mut ready in ready_receivers {
        let mut readiness_closed = false;
        loop {
            tokio::select! {
                biased;
                _ = ctrl_c.as_mut() => {
                    return Err("interrupted while waiting for watch readiness".into());
                }
                watch_result = tasks.join_next(), if !tasks.is_empty() => {
                    match watch_result {
                        Some(Ok((name, result))) => {
                            if let Some(error) = watch_failure(&name, result) {
                                return Err(error);
                            }
                            return Err(format!("watch {name} terminated before Created readiness"));
                        }
                        Some(Err(error)) => return Err(format!("watch task failed: {error}")),
                        None => return Err("all watch tasks terminated before readiness".into()),
                    }
                }
                ready_result = &mut ready, if !readiness_closed => {
                    match ready_result {
                        Ok(()) => break,
                        Err(_) if tasks.is_empty() => {
                            return Err("watch terminated before Created readiness".into());
                        }
                        Err(_) => readiness_closed = true,
                    }
                }
                _ = time::sleep_until(deadline) => {
                    return Err("watch readiness timed out".into());
                }
            }
        }
    }
    Ok(())
}

fn push_bounded_error(errors: &mut Vec<String>, error: String) {
    if errors.len() < 8 {
        errors.push(error.chars().take(MAX_ERROR_CONTEXT).collect());
    }
}

type CtrlCFuture = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

enum SupervisedScheduleControl {
    Deadline,
    Interrupted,
    UpdateFinished(Result<u64, String>),
}

async fn wait_for_supervised_schedule_event(
    watch_tasks: &mut JoinSet<WatchTaskOutput>,
    worker_tasks: &mut JoinSet<Result<(), String>>,
    update_task: &mut Option<tokio::task::JoinHandle<Result<u64, String>>>,
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> Result<SupervisedScheduleControl, String> {
    tokio::select! {
        biased;
        _ = ctrl_c.as_mut() => Ok(SupervisedScheduleControl::Interrupted),
        watch_result = watch_tasks.join_next(), if !watch_tasks.is_empty() => {
            match watch_result {
                Some(Ok((name, result))) => {
                    match watch_failure(&name, result) {
                        Some(error) => Err(error),
                        None => Err(format!("watch {name} terminated unexpectedly")),
                    }
                }
                Some(Err(error)) => Err(format!("watch task failed: {error}")),
                None => Err("all watch tasks terminated unexpectedly".to_string()),
            }
        }
        worker_result = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
            match worker_result {
                Some(Ok(Ok(()))) => Err("worker exited unexpectedly".to_string()),
                Some(Ok(Err(error))) => Err(format!("worker failed: {error}")),
                Some(Err(error)) => Err(format!("worker task failed: {error}")),
                None => Err("all worker tasks terminated unexpectedly".to_string()),
            }
        }
        update_result = async { update_task.as_mut().expect("guarded update task").await }, if update_task.is_some() => {
            let result = match update_result {
                Ok(result) => result,
                Err(error) => Err(format!("update phase task failed: {error}")),
            };
            Ok(SupervisedScheduleControl::UpdateFinished(result))
        }
        _ = time::sleep_until(deadline) => Ok(SupervisedScheduleControl::Deadline),
    }
}
enum EnqueueResult {
    Accepted,
    Closed,
    Unissued,
}

async fn enqueue_until(
    sender: &mpsc::Sender<WorkItem>,
    mut item: WorkItem,
    deadline: TokioInstant,
) -> EnqueueResult {
    match time::timeout_at(deadline, sender.reserve()).await {
        Ok(Ok(permit)) => {
            item.mark_accepted();
            permit.send(item);
            EnqueueResult::Accepted
        }
        Ok(Err(_)) => {
            item.mark_dropped(DropReason::QueueClosed);
            metrics::record_dropped(DropReason::QueueClosed);
            EnqueueResult::Closed
        }
        Err(_) => {
            item.mark_unissued();
            EnqueueResult::Unissued
        }
    }
}

async fn run_create_phase(
    workload: &WorkloadSpec,
    plan: &WorkloadPlan,
    senders: &[mpsc::Sender<WorkItem>],
    workers: usize,
    timeout: Duration,
    accounting: &Arc<RunAccounting>,
) -> Result<u64, String> {
    let tracker = Arc::new(PhaseTracker::new(
        plan.create_targets(workload.resources.len()),
    ));
    debug_assert_eq!(tracker.target, plan.total_creates());
    let deadline = TokioInstant::now() + timeout;
    let result: Result<u64, String> = async {
        let mut scheduled = 0u64;
        for operation in plan.creates() {
            let spec = &workload.resources[operation.resource];
            let key = resource_key(spec, operation.id);
            let label = ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string())?;
            let item = WorkItem::Txn {
                resource: operation.resource,
                key: key.clone(),
                operation: ResourceOperation::Create,
                value: resource_value(spec, operation.id, 0),
                pool: spec.lease_pool.clone(),
                accounting: tracked_phase(
                    accounting,
                    if spec.behavior == ResourceBehavior::MasterLease {
                        WorkClass::MasterLease
                    } else {
                        WorkClass::Create
                    },
                    Arc::clone(&tracker),
                    operation.resource,
                ),
            };
            metrics::record_scheduled(Operation::Create, &label);
            match enqueue_until(&senders[stable_shard(&key, workers)], item, deadline).await {
                EnqueueResult::Accepted => scheduled += 1,
                EnqueueResult::Closed => return Err("create phase worker queue closed".to_string()),
                EnqueueResult::Unissued => return Err(tracker.deadline_error("create")),
            }
        }
        tracker.wait_until("create", deadline).await?;
        Ok(scheduled)
    }
    .await;
    tracker.publish(workload, PhaseKind::Create);
    result
}

const UPDATE_DISPATCH_FRACTION: f64 = 0.95;

fn update_dispatch_at(duration: Duration, ordinal: u64, total: u64) -> Duration {
    debug_assert!(total > 0);
    debug_assert!(ordinal < total);
    duration.mul_f64(UPDATE_DISPATCH_FRACTION * ordinal as f64 / total as f64)
}

async fn run_update_phase(
    workload: WorkloadSpec,
    plan: WorkloadPlan,
    senders: Vec<mpsc::Sender<WorkItem>>,
    workers: usize,
    seed: u64,
    started: TokioInstant,
    duration: Duration,
    accounting: Arc<RunAccounting>,
) -> Result<u64, String> {
    let total = plan.total_updates();
    let tracker = Arc::new(PhaseTracker::new(
        plan.update_targets(workload.resources.len()),
    ));
    let deadline = started + duration;
    let result: Result<u64, String> = async {
        let mut scheduled = 0u64;
        for (ordinal, operation) in UpdateCursor::new(&plan, seed).enumerate() {
            let at = started + update_dispatch_at(duration, ordinal as u64, total);
            time::sleep_until(at).await;
            let spec = &workload.resources[operation.resource];
            let key = resource_key(spec, operation.id);
            let label = ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string())?;
            let item = WorkItem::Txn {
                resource: operation.resource,
                key: key.clone(),
                operation: ResourceOperation::GuaranteedUpdate,
                value: resource_value(spec, operation.id, ordinal as u64 + 1),
                pool: spec.lease_pool.clone(),
                accounting: tracked_phase(
                    &accounting,
                    if spec.behavior == ResourceBehavior::MasterLease {
                        WorkClass::MasterLease
                    } else {
                        WorkClass::Update
                    },
                    Arc::clone(&tracker),
                    operation.resource,
                ),
            };
            metrics::record_scheduled(Operation::Update, &label);
            match enqueue_until(&senders[stable_shard(&key, workers)], item, deadline).await {
                EnqueueResult::Accepted => scheduled += 1,
                EnqueueResult::Closed => return Err("update phase worker queue closed".to_string()),
                EnqueueResult::Unissued => return Err(tracker.deadline_error("update")),
            }
        }
        tracker.wait_until("update", deadline).await?;
        Ok(scheduled)
    }
    .await;
    tracker.publish(&workload, PhaseKind::Update);
    result
}

fn try_enqueue(
    sender: &mpsc::Sender<WorkItem>,
    mut item: WorkItem,
    stats: &mut ScheduleStats,
) -> bool {
    match sender.try_reserve() {
        Ok(permit) => {
            item.mark_accepted();
            permit.send(item);
            true
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            item.mark_dropped(DropReason::Backpressure);
            stats.dropped += 1;
            metrics::record_dropped(DropReason::Backpressure);
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            item.mark_dropped(DropReason::QueueClosed);
            stats.dropped += 1;
            metrics::record_dropped(DropReason::QueueClosed);
            false
        }
    }
}

fn application_targets(workload: &WorkloadSpec, plan: &WorkloadPlan) -> ApplicationTargets {
    let mut targets = ApplicationTargets::default();
    for resource in &plan.resources {
        if workload.resources[resource.resource].behavior == ResourceBehavior::MasterLease {
            continue;
        }
        targets.creates = targets.creates.saturating_add(resource.count);
        targets.updates = targets
            .updates
            .saturating_add(resource.count.saturating_mul(resource.update_amplification));
    }
    targets
}

fn combine_errors(first: Option<String>, second: Option<String>) -> Option<String> {
    match (first, second) {
        (Some(first), Some(second)) => Some(format!("{first}; {second}")),
        (first, None) | (None, first) => first,
    }
}

pub async fn run(config: RuntimeConfig, client: ClientBundle) -> RunReport {
    run_with_ctrl_c(config, client, Box::pin(tokio::signal::ctrl_c())).await
}

async fn run_with_ctrl_c(
    config: RuntimeConfig,
    client: ClientBundle,
    mut ctrl_c: CtrlCFuture,
) -> RunReport {
    let accounting = RunAccounting::new();
    if config.queue_capacity < config.workers {
        return accounting.report(
            RunTargets::default(),
            false,
            false,
            Some(format!(
                "queue_capacity ({}) must be at least workers ({})",
                config.queue_capacity, config.workers
            )),
        );
    }
    let scheduler = Scheduler::new(&config);
    let plan = match WorkloadPlan::build(&scheduler.workload) {
        Ok(plan) => plan,
        Err(error) => return accounting.report(RunTargets::default(), false, false, Some(error)),
    };
    let targets = RunTargets {
        bootstrap: scheduler.nodes as u64
            * if scheduler.workload.nodes.node_lease_enabled {
                3
            } else {
                2
            },
        application: application_targets(&scheduler.workload, &plan),
        range_requests: range_request_total(&scheduler.workload, scheduler.seed),
    };
    let queue_capacity = config.queue_capacity.max(1);
    let workers_count = config.workers.max(1);
    let mut senders = Vec::with_capacity(workers_count);
    let mut worker_tasks = JoinSet::new();
    let latest_revision = Arc::new(AtomicI64::new(0));
    let cas_conflicts = Arc::new(AtomicU64::new(config.workload.cas_conflicts));
    let lease_epoch = TokioInstant::now();
    let mut stats = ScheduleStats::default();
    let pools: Arc<HashMap<String, Arc<SharedPool>>> = Arc::new(
        config
            .workload
            .etcd_lease_pools
            .iter()
            .map(|spec| {
                let pool = lease_pool(spec);
                let name = PoolName::new(spec.name.clone()).expect("validated lease pool label");
                (
                    spec.name.clone(),
                    Arc::new(SharedPool {
                        name,
                        pool: Mutex::new(pool),
                    }),
                )
            })
            .collect(),
    );
    let base_capacity = queue_capacity / workers_count;
    let extra_capacity = queue_capacity % workers_count;
    for worker_index in 0..workers_count {
        let capacity = base_capacity + if worker_index < extra_capacity { 1 } else { 0 };
        let (tx, rx) = mpsc::channel(capacity.max(1));
        senders.push(tx);
        worker_tasks.spawn(worker_loop(
            rx,
            client.clone(),
            config.workload.clone(),
            config.operation_timeout,
            pools.clone(),
            latest_revision.clone(),
            lease_epoch,
            cas_conflicts.clone(),
        ));
    }

    let planner = NodeHeartbeatPlanner::new(crate::heartbeat::HeartbeatConfig {
        nodes: scheduler.nodes,
        lease_interval: Duration::from_secs_f64(scheduler.workload.nodes.lease_interval_s),
        lease_jitter: scheduler.workload.nodes.lease_jitter,
        status_interval: Duration::from_secs_f64(scheduler.workload.nodes.status_interval_s),
        seed: scheduler.seed,
        node_lease_enabled: scheduler.workload.nodes.node_lease_enabled,
    });
    let mut bootstrap_interrupted = false;
    let mut bootstrap_acks = Vec::with_capacity(scheduler.nodes.saturating_mul(3));
    'bootstrap_send: for heartbeat in planner.bootstrap() {
        let shard = stable_shard(heartbeat.key.as_bytes(), workers_count);
        let (done_tx, done_rx) = oneshot::channel();
        let resource = if matches!(heartbeat.kind, HeartbeatItemKind::NodeLeaseCreate) {
            ResourceName::new("node_leases")
        } else {
            ResourceName::new("nodes")
        }
        .expect("validated heartbeat metric label");
        let operation = if matches!(
            heartbeat.kind,
            HeartbeatItemKind::NodeCreate | HeartbeatItemKind::NodeLeaseCreate
        ) {
            Operation::Create
        } else {
            Operation::Update
        };
        let mut item = WorkItem::Bootstrap {
            item: heartbeat,
            done: done_tx,
            accounting: tracked(&accounting, WorkClass::Bootstrap),
        };
        metrics::record_scheduled(operation, &resource);
        stats.scheduled = stats.scheduled.saturating_add(1);
        let send_result = {
            let result = tokio::select! {
                _ = &mut ctrl_c => {
                    item.mark_unissued();
                    bootstrap_interrupted = true;
                    None
                }
                result = senders[shard].reserve() => Some(result),
            };
            match result {
                Some(Ok(permit)) => {
                    item.mark_accepted();
                    permit.send(item);
                    bootstrap_acks.push(done_rx);
                    Ok(())
                }
                Some(Err(_)) => {
                    item.mark_dropped(DropReason::QueueClosed);
                    metrics::record_dropped(DropReason::QueueClosed);
                    Err(())
                }
                None => break 'bootstrap_send,
            }
        };
        if send_result.is_err() {
            abort_worker_tasks(senders, worker_tasks, pools).await;
            return accounting.report(
                targets,
                false,
                false,
                Some("bootstrap worker queue closed".to_string()),
            );
        }
    }
    if !bootstrap_interrupted {
        'bootstrap_ack: for done_rx in bootstrap_acks {
            let result = tokio::select! {
                _ = &mut ctrl_c => {
                    bootstrap_interrupted = true;
                    break 'bootstrap_ack;
                }
                result = done_rx => result,
            };
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    abort_worker_tasks(senders, worker_tasks, pools).await;
                    return accounting.report(
                        targets,
                        false,
                        false,
                        Some(format!("bootstrap: {error}")),
                    );
                }
                Err(_) => {
                    abort_worker_tasks(senders, worker_tasks, pools).await;
                    return accounting.report(
                        targets,
                        false,
                        false,
                        Some("bootstrap worker closed".to_string()),
                    );
                }
            }
        }
    }
    if !bootstrap_allows_main_load(bootstrap_interrupted) {
        abort_worker_tasks(senders, worker_tasks, pools).await;
        return accounting.report(targets, true, false, None);
    }

    let create_result = tokio::select! {
        _ = &mut ctrl_c => None,
        result = run_create_phase(
            &scheduler.workload,
            &plan,
            &senders,
            workers_count,
            config.operation_timeout,
            &accounting,
        ) => Some(result),
    };
    match create_result {
        Some(Ok(scheduled)) => stats.scheduled = stats.scheduled.saturating_add(scheduled),
        Some(Err(error)) => {
            abort_worker_tasks(senders, worker_tasks, pools).await;
            return accounting.report(targets, false, false, Some(error));
        }
        None => {
            abort_worker_tasks(senders, worker_tasks, pools).await;
            return accounting.report(targets, true, false, None);
        }
    }

    let (watch_shutdown, mut watch_tasks, ready_receivers) =
        spawn_watches(&client, &scheduler.workload.watches);
    if let Err(error) = wait_for_watch_ready(
        &mut watch_tasks,
        ready_receivers,
        config.operation_timeout,
        &mut ctrl_c,
    )
    .await
    {
        let interrupted = error.starts_with("interrupted while waiting");
        abort_worker_tasks(senders, worker_tasks, pools).await;
        let (stop_result, interrupted) = stop_watches_with_ctrl(
            watch_shutdown,
            watch_tasks,
            config.operation_timeout,
            &mut ctrl_c,
            interrupted,
        )
        .await;
        return accounting.report(
            targets,
            interrupted,
            false,
            combine_errors(Some(error), stop_result.err()),
        );
    }

    let duration = scheduler.duration();
    let started = TokioInstant::now();
    let update_task = tokio::spawn(run_update_phase(
        scheduler.workload.clone(),
        plan,
        senders.clone(),
        workers_count,
        scheduler.seed,
        started,
        duration,
        Arc::clone(&accounting),
    ));
    let mut ranges = RateCursor::new(&scheduler.workload, scheduler.seed, 0xB2);
    let mut range_producer = RangeProducer::new(scheduler.workload.ranges.iter().map(|r| r.weight));
    let weights: Vec<f64> = scheduler.workload.ranges.iter().map(|r| r.weight).collect();
    let prefix_total = exact_prefix_total(
        range_producer.clone(),
        &weights,
        &scheduler.workload.ranges,
        ranges.total(),
    );
    let mut range_shapes = match range_shape_plan(&scheduler.workload, prefix_total) {
        Ok(plan) => plan,
        Err(error) => {
            update_task.abort();
            let _ = update_task.await;
            abort_worker_tasks(senders, worker_tasks, pools).await;
            let (stop_result, interrupted) = stop_watches_with_ctrl(
                watch_shutdown,
                watch_tasks,
                config.operation_timeout,
                &mut ctrl_c,
                false,
            )
            .await;
            return accounting.report(
                targets,
                interrupted,
                false,
                combine_errors(Some(error), stop_result.err()),
            );
        }
    };
    let mut update_task = Some(update_task);
    let mut heap = BinaryHeap::new();
    let mut sequence = 0u64;
    let push = |at: Duration,
                rank: u8,
                source: Source,
                heap: &mut BinaryHeap<Scheduled>,
                sequence: &mut u64| {
        heap.push(Scheduled {
            at,
            rank,
            sequence: *sequence,
            source,
        });
        *sequence = sequence.wrapping_add(1);
    };
    if let Some((at, _)) = ranges.next() {
        push(at, 2, Source::Range, &mut heap, &mut sequence);
    }
    let status_interval = Duration::from_secs_f64(scheduler.workload.nodes.status_interval_s);
    if scheduler.workload.nodes.node_lease_enabled {
        for node in 0..scheduler.nodes {
            let lease_at = heartbeat_interval(
                scheduler.seed,
                node,
                0,
                Duration::from_secs_f64(scheduler.workload.nodes.lease_interval_s),
                scheduler.workload.nodes.lease_jitter,
            );
            if lease_at <= duration {
                push(
                    lease_at,
                    0,
                    Source::NodeLease(node),
                    &mut heap,
                    &mut sequence,
                );
            }
        }
    }
    for node in 0..scheduler.nodes {
        if !status_interval.is_zero() {
            let status_at = node_status_phase(node, scheduler.nodes, status_interval);
            if status_at <= duration {
                push(
                    status_at,
                    0,
                    Source::NodeStatus { node, tick: 0 },
                    &mut heap,
                    &mut sequence,
                );
            }
        }
    }
    let master_lease_interval = Duration::from_secs_f64(scheduler.workload.master_lease_interval_s);
    for (resource, spec) in scheduler.workload.resources.iter().enumerate() {
        if spec.behavior == ResourceBehavior::MasterLease {
            let count = scheduler
                .workload
                .resource_count(spec)
                .expect("validated masterlease count");
            if let Some(at) = masterlease_tick_at(master_lease_interval, 0, duration) {
                for id in 0..count {
                    push(
                        at,
                        0,
                        Source::MasterLease {
                            resource,
                            id,
                            ordinal: 0,
                        },
                        &mut heap,
                        &mut sequence,
                    );
                }
            }
        }
    }
    let status_total = scheduler.workload.status_count.saturating_sub(1);
    if status_total > 0 {
        push(
            duration.mul_f64(1.0 / status_total as f64),
            3,
            Source::Status(1),
            &mut heap,
            &mut sequence,
        );
    }
    let compact_interval = Duration::from_secs_f64(scheduler.workload.compact_interval_s);
    if compact_interval <= duration {
        push(
            compact_interval,
            4,
            Source::Compact(0),
            &mut heap,
            &mut sequence,
        );
    }
    push(duration, 5, Source::PhaseEnd, &mut heap, &mut sequence);

    let mut interrupted = false;
    let mut schedule_error = None;
    while let Some(event) = heap.peek().copied() {
        let deadline = started + event.at;
        let control = match wait_for_supervised_schedule_event(
            &mut watch_tasks,
            &mut worker_tasks,
            &mut update_task,
            deadline,
            &mut ctrl_c,
        )
        .await
        {
            Ok(control) => control,
            Err(error) => {
                schedule_error = Some(error);
                break;
            }
        };
        match control {
            SupervisedScheduleControl::Deadline => {
                heap.pop();
            }
            SupervisedScheduleControl::Interrupted => {
                interrupted = true;
                break;
            }
            SupervisedScheduleControl::UpdateFinished(result) => {
                update_task.take();
                if let Err(error) = result {
                    schedule_error = Some(error);
                    break;
                }
                // A completed producer must not consume the pending timed event.
                continue;
            }
        }
        match event.source {
            Source::PhaseEnd => {}
            Source::Range => {
                if let Some((at, _)) = ranges.next() {
                    push(at, 2, Source::Range, &mut heap, &mut sequence);
                }
                if let Some(index) = range_producer.next(&weights) {
                    let mut request =
                        range_request(&scheduler.workload.ranges[index], event.sequence);
                    if scheduler.workload.ranges[index].kind == "prefix" {
                        range_shapes.apply(&mut request);
                    }
                    let label = ResourceName::new("range").expect("static metric label");
                    metrics::record_scheduled(Operation::Read, &label);
                    stats.scheduled = stats.scheduled.saturating_add(1);
                    try_enqueue(
                        &senders[stable_shard(&request.key, workers_count)],
                        WorkItem::Range {
                            request,
                            selector_keep_every: scheduler.workload.ranges[index]
                                .selector_keep_every,
                            target: scheduler.workload.ranges[index].limit,
                            accounting: tracked(&accounting, WorkClass::Range),
                        },
                        &mut stats,
                    );
                }
            }
            Source::MasterLease {
                resource,
                id,
                ordinal,
            } => {
                let spec = &scheduler.workload.resources[resource];
                let operation = ResourceOperation::GuaranteedUpdate;
                let key = resource_key(spec, id);
                let label = ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string());
                let Ok(label) = label else {
                    schedule_error = Some("masterlease metric label is invalid".to_string());
                    break;
                };
                metrics::record_scheduled(operation_metric(operation), &label);
                stats.scheduled = stats.scheduled.saturating_add(1);
                try_enqueue(
                    &senders[stable_shard(&key, workers_count)],
                    WorkItem::Txn {
                        resource,
                        key: key.clone(),
                        operation,
                        value: resource_value(spec, id, ordinal),
                        pool: spec.lease_pool.clone(),
                        accounting: tracked(&accounting, WorkClass::MasterLease),
                    },
                    &mut stats,
                );
                if let Some(at) = masterlease_tick_at(master_lease_interval, ordinal + 1, duration)
                {
                    push(
                        at,
                        0,
                        Source::MasterLease {
                            resource,
                            id,
                            ordinal: ordinal + 1,
                        },
                        &mut heap,
                        &mut sequence,
                    );
                }
            }
            Source::NodeLease(node) => {
                let item = HeartbeatItem {
                    node,
                    kind: HeartbeatItemKind::NodeLeaseRenew,
                    key: format!("/registry/leases/kube-node-lease/node-{node:04}"),
                    at: event.at,
                };
                let key = item.key.clone();
                let label = ResourceName::new("node_leases").expect("static metric label");
                metrics::record_scheduled(Operation::Update, &label);
                stats.scheduled = stats.scheduled.saturating_add(1);
                try_enqueue(
                    &senders[stable_shard(key.as_bytes(), workers_count)],
                    WorkItem::Heartbeat {
                        item,
                        accounting: tracked(&accounting, WorkClass::Heartbeat),
                    },
                    &mut stats,
                );
                let next = event.at.saturating_add(heartbeat_interval(
                    scheduler.seed,
                    node,
                    event.sequence,
                    Duration::from_secs_f64(scheduler.workload.nodes.lease_interval_s),
                    scheduler.workload.nodes.lease_jitter,
                ));
                if next <= duration {
                    push(next, 0, Source::NodeLease(node), &mut heap, &mut sequence);
                }
            }
            Source::NodeStatus { node, tick } => {
                let item = HeartbeatItem {
                    node,
                    kind: HeartbeatItemKind::NodeStatus,
                    key: format!("/registry/minions/node-{node:04}"),
                    at: event.at,
                };
                let key = item.key.clone();
                let label = ResourceName::new("nodes").expect("static metric label");
                metrics::record_scheduled(Operation::Update, &label);
                stats.scheduled = stats.scheduled.saturating_add(1);
                try_enqueue(
                    &senders[stable_shard(key.as_bytes(), workers_count)],
                    WorkItem::Heartbeat {
                        item,
                        accounting: tracked(&accounting, WorkClass::Heartbeat),
                    },
                    &mut stats,
                );
                let next_at = event.at.saturating_add(status_interval);
                if next_at <= duration {
                    push(
                        next_at,
                        0,
                        Source::NodeStatus {
                            node,
                            tick: tick + 1,
                        },
                        &mut heap,
                        &mut sequence,
                    );
                }
            }
            Source::Status(index) => {
                if index <= status_total {
                    let label = ResourceName::new("status").expect("static metric label");
                    metrics::record_scheduled(Operation::Read, &label);
                    stats.scheduled = stats.scheduled.saturating_add(1);
                    let shard = index as usize % workers_count;
                    try_enqueue(
                        &senders[shard],
                        WorkItem::Status {
                            accounting: tracked(&accounting, WorkClass::Status),
                        },
                        &mut stats,
                    );
                    if index < status_total {
                        push(
                            duration.mul_f64((index + 1) as f64 / status_total as f64),
                            3,
                            Source::Status(index + 1),
                            &mut heap,
                            &mut sequence,
                        );
                    }
                }
            }
            Source::Compact(tick) => {
                let compact_key =
                    ResourceName::new("compact_rev_key").expect("static metric label");
                metrics::record_scheduled(
                    if tick == 0 {
                        Operation::Create
                    } else {
                        Operation::Update
                    },
                    &compact_key,
                );
                stats.scheduled = stats.scheduled.saturating_add(1);
                try_enqueue(
                    &senders[stable_shard(b"compact_rev_key", workers_count)],
                    WorkItem::CompactTick {
                        accounting: tracked(&accounting, WorkClass::Compact),
                    },
                    &mut stats,
                );
                let next = event.at.saturating_add(compact_interval);
                if next <= duration {
                    push(next, 4, Source::Compact(tick + 1), &mut heap, &mut sequence);
                }
            }
        }
    }

    let schedule_complete = !interrupted && schedule_error.is_none() && heap.is_empty();
    let join_deadline = TokioInstant::now() + config.operation_timeout;
    while !interrupted && schedule_error.is_none() && update_task.is_some() {
        match wait_for_supervised_schedule_event(
            &mut watch_tasks,
            &mut worker_tasks,
            &mut update_task,
            join_deadline,
            &mut ctrl_c,
        )
        .await
        {
            Ok(SupervisedScheduleControl::UpdateFinished(result)) => {
                update_task.take();
                schedule_error = result.err();
            }
            Ok(SupervisedScheduleControl::Interrupted) => interrupted = true,
            Ok(SupervisedScheduleControl::Deadline) => {
                schedule_error = Some("update phase join timed out".to_string());
            }
            Err(error) => schedule_error = Some(error),
        }
    }
    if interrupted || schedule_error.is_some() {
        if let Some(task) = update_task.take() {
            task.abort();
            let _ = task.await;
        }
        abort_worker_tasks(senders, worker_tasks, pools).await;
        let (stop_result, interrupted) = stop_watches_with_ctrl(
            watch_shutdown,
            watch_tasks,
            config.operation_timeout,
            &mut ctrl_c,
            interrupted,
        )
        .await;
        return accounting.report(
            targets,
            interrupted,
            schedule_complete,
            combine_errors(schedule_error, stop_result.err()),
        );
    }

    // Keep watches live while accepted work drains; both teardown stages
    // continue observing the same cancellation future used during dispatch.
    let (drain_result, interrupted) = drain_worker_tasks(
        senders,
        worker_tasks,
        pools,
        config.operation_timeout,
        &mut ctrl_c,
    )
    .await;
    let (stop_result, interrupted) = stop_watches_with_ctrl(
        watch_shutdown,
        watch_tasks,
        config.operation_timeout,
        &mut ctrl_c,
        interrupted,
    )
    .await;
    accounting.report(
        targets,
        interrupted,
        schedule_complete,
        combine_errors(drain_result.err(), stop_result.err()),
    )
}
fn bootstrap_allows_main_load(interrupted: bool) -> bool {
    !interrupted
}

fn update_revision(latest_revision: &AtomicI64, revision: i64) {
    let previous = latest_revision.fetch_max(revision, AtomicOrdering::Relaxed);
    metrics::set_revision(previous.max(revision));
}

async fn abort_worker_tasks(
    senders: Vec<mpsc::Sender<WorkItem>>,
    mut worker_tasks: JoinSet<Result<(), String>>,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
) {
    drop(senders);
    worker_tasks.abort_all();
    while worker_tasks.join_next().await.is_some() {}
    drop(pools);
}

async fn drain_worker_tasks(
    senders: Vec<mpsc::Sender<WorkItem>>,
    mut worker_tasks: JoinSet<Result<(), String>>,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
    timeout: Duration,
    ctrl_c: &mut CtrlCFuture,
) -> (Result<(), String>, bool) {
    drop(senders);
    let deadline = TokioInstant::now() + timeout;
    let mut errors = Vec::new();
    let mut interrupted = false;
    while !worker_tasks.is_empty() {
        if interrupted {
            worker_tasks.abort_all();
            while worker_tasks.join_next().await.is_some() {}
            break;
        }
        tokio::select! {
            biased;
            _ = ctrl_c.as_mut() => {
                interrupted = true;
            }
            result = worker_tasks.join_next() => {
                match result {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => {
                        push_bounded_error(&mut errors, format!("worker failed: {error}"));
                    }
                    Some(Err(error)) => {
                        push_bounded_error(&mut errors, format!("worker task failed: {error}"));
                    }
                    None => break,
                }
            }
            _ = time::sleep_until(deadline) => {
                worker_tasks.abort_all();
                while worker_tasks.join_next().await.is_some() {}
                push_bounded_error(&mut errors, "worker drain timed out".to_string());
                break;
            }
        }
    }
    drop(pools);
    let result = if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    };
    (result, interrupted)
}

async fn stop_watches_with_ctrl(
    shutdown: shutdown_watch::Sender<bool>,
    mut tasks: JoinSet<WatchTaskOutput>,
    timeout: Duration,
    ctrl_c: &mut CtrlCFuture,
    mut interrupted: bool,
) -> (Result<(), String>, bool) {
    let _ = shutdown.send(true);
    let deadline = TokioInstant::now() + timeout;
    let mut errors = Vec::new();
    while !tasks.is_empty() {
        tokio::select! {
            biased;
            _ = ctrl_c.as_mut(), if !interrupted => interrupted = true,
            result = tasks.join_next() => {
                match result {
                    Some(Ok((name, result))) => {
                        if let Some(error) = watch_failure(&name, result) {
                            push_bounded_error(&mut errors, error);
                        }
                    }
                    Some(Err(error)) => push_bounded_error(
                        &mut errors,
                        format!("watch task failed during shutdown: {error}"),
                    ),
                    None => break,
                }
            }
            _ = time::sleep_until(deadline) => {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                push_bounded_error(&mut errors, "watch shutdown timed out".to_string());
                break;
            }
        }
    }
    let result = if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    };
    (result, interrupted)
}

fn lease_pool(spec: &EtcdLeasePoolSpec) -> LeasePool {
    LeasePool::new(LeasePoolConfig {
        // WorkloadSpec::validate rejects non-integral/overflowing values.
        object_ttl: Duration::from_secs(spec.object_ttl_s),
        reuse_window: Duration::from_secs(spec.reuse_s),
        max_objects: spec.max_objects,
    })
}
async fn worker_loop(
    mut receiver: mpsc::Receiver<WorkItem>,
    client: ClientBundle,
    workload: WorkloadSpec,
    timeout: Duration,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
    latest_revision: Arc<AtomicI64>,
    lease_epoch: TokioInstant,
    cas_conflicts: Arc<AtomicU64>,
) -> Result<(), String> {
    let mut states = HashMap::new();
    let mut compactor = CompactorState::default();
    while let Some(mut item) = receiver.recv().await {
        item.record_dequeue();
        match item {
            WorkItem::Txn {
                resource,
                key,
                operation,
                value,
                pool,
                mut accounting,
            } => {
                let spec = workload.resources.get(resource).ok_or_else(|| {
                    let error = format!("unknown resource index {resource}");
                    accounting
                        .accounting
                        .note_error(accounting.class, error.clone());
                    error
                })?;
                let label = ResourceName::try_from(spec.name.as_str()).map_err(|error| {
                    accounting
                        .accounting
                        .note_error(accounting.class, error.to_string());
                    error.to_string()
                })?;
                let operation_started = Instant::now();
                let lease_pool = if operation.uses_native_lease() {
                    pool.as_deref()
                        .and_then(|name| pools.get(name))
                        .map(|pool| pool.as_ref())
                } else {
                    None
                };
                let remaining_timeout = timeout.saturating_sub(operation_started.elapsed());
                let inject_conflict = operation == ResourceOperation::GuaranteedUpdate
                    && cas_conflicts
                        .fetch_update(
                            AtomicOrdering::Relaxed,
                            AtomicOrdering::Relaxed,
                            |remaining| remaining.checked_sub(1),
                        )
                        .is_ok();
                let outcome = execute_txn(
                    &client,
                    &label,
                    &mut states,
                    key,
                    value,
                    operation,
                    inject_conflict,
                    CasRetryPolicy::until_deadline(),
                    spec.behavior == ResourceBehavior::MasterLease,
                    remaining_timeout,
                    lease_pool,
                    lease_epoch,
                    latest_revision.clone(),
                )
                .await;
                if outcome != Outcome::Success {
                    accounting.accounting.note_error(
                        accounting.class,
                        format!("transaction outcome: {}", outcome.as_str()),
                    );
                }
                accounting.mark_complete(outcome);
            }
            WorkItem::Range {
                request,
                selector_keep_every,
                target,
                mut accounting,
            } => {
                let selector = configured_range_selector(selector_keep_every, target);
                let label = ResourceName::new("range").unwrap();
                let outcome = match execute_range(
                    &client,
                    request,
                    selector,
                    timeout,
                    latest_revision.clone(),
                )
                .await
                {
                    Ok(result) => {
                        metrics::record_range_result(
                            result.pages,
                            result.snapshot_revision,
                            result.server_count,
                            &result.last_key,
                            result.selected,
                            result.discarded,
                        );
                        Outcome::Success
                    }
                    Err(error) => {
                        accounting
                            .accounting
                            .note_error(WorkClass::Range, error.message.clone());
                        error.outcome
                    }
                };
                metrics::record_operation(Operation::Read, &label, outcome);
                accounting.mark_complete(outcome);
            }
            WorkItem::Bootstrap {
                item,
                done,
                mut accounting,
            } => {
                let outcome = execute_heartbeat(
                    &client,
                    &mut states,
                    item,
                    timeout,
                    workload.nodes.status_retry_limit,
                    workload.nodes.lease_retry_limit,
                    latest_revision.clone(),
                )
                .await;
                if outcome != Outcome::Success {
                    accounting.accounting.note_error(
                        WorkClass::Bootstrap,
                        format!("heartbeat outcome: {}", outcome.as_str()),
                    );
                }
                accounting.mark_complete(outcome);
                let result = if outcome == Outcome::Success {
                    Ok(())
                } else {
                    Err(format!("heartbeat failed: {}", outcome.as_str()))
                };
                let _ = done.send(result);
            }
            WorkItem::Heartbeat {
                item,
                mut accounting,
            } => {
                let outcome = execute_heartbeat(
                    &client,
                    &mut states,
                    item,
                    timeout,
                    workload.nodes.status_retry_limit,
                    workload.nodes.lease_retry_limit,
                    latest_revision.clone(),
                )
                .await;
                if outcome != Outcome::Success {
                    accounting.accounting.note_error(
                        WorkClass::Heartbeat,
                        format!("heartbeat outcome: {}", outcome.as_str()),
                    );
                }
                accounting.mark_complete(outcome);
            }
            WorkItem::Status { mut accounting } => {
                let label = ResourceName::new("status").unwrap();
                let outcome = execute_status(&client, timeout, latest_revision.clone()).await;
                if outcome != Outcome::Success {
                    accounting.accounting.note_error(
                        WorkClass::Status,
                        format!("status outcome: {}", outcome.as_str()),
                    );
                }
                metrics::record_operation(Operation::Read, &label, outcome);
                accounting.mark_complete(outcome);
            }
            WorkItem::CompactTick { mut accounting } => {
                let outcome =
                    execute_compact_tick(&client, &mut compactor, timeout, latest_revision.clone())
                        .await;
                if outcome != Outcome::Success {
                    accounting.accounting.note_error(
                        WorkClass::Compact,
                        format!("compact tick outcome: {}", outcome.as_str()),
                    );
                }
                accounting.mark_complete(outcome);
            }
        }
    }
    Ok(())
}
async fn execute_status(
    client: &ClientBundle,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            metrics::record_request(Rpc::Status, Outcome::Timeout);
            return Outcome::Timeout;
        }
        let start = Instant::now();
        metrics::add_inflight(Rpc::Status);
        let result = time::timeout(remaining, client.status()).await;
        metrics::remove_inflight(Rpc::Status);
        metrics::record_request_duration(Rpc::Status, start.elapsed());
        metrics::record_request_bytes(&ResourceName::new("status").unwrap(), 0);
        match result {
            Ok(Ok(response)) => {
                if let Some(header) = response.header.as_ref() {
                    update_revision(&latest_revision, header.revision);
                }
                metrics::record_request(Rpc::Status, Outcome::Success);
                return Outcome::Success;
            }
            Ok(Err(error)) => {
                match error.retry_decision(RpcMethod::Status, RequestDisposition::Unknown) {
                    RetryDecision::Retry => {
                        metrics::record_request(Rpc::Status, Outcome::GrpcError);
                        let delay = Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(Instant::now()));
                        if delay.is_zero() {
                            return Outcome::Timeout;
                        }
                        time::sleep(delay).await;
                    }
                    RetryDecision::NotSent => {
                        metrics::record_request(Rpc::Status, Outcome::GrpcError);
                        return Outcome::GrpcError;
                    }
                    RetryDecision::ResultUnknown | RetryDecision::Stop => {
                        metrics::record_request(Rpc::Status, Outcome::GrpcError);
                        return Outcome::GrpcError;
                    }
                }
            }
            Err(_) => {
                metrics::record_request(Rpc::Status, Outcome::Timeout);
                return Outcome::Timeout;
            }
        }
    }
}
async fn execute_compact_tick(
    client: &ClientBundle,
    compactor: &mut CompactorState,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let (coordination_version, previous_txn_header_revision) = compactor.txn_inputs();
    let value = previous_txn_header_revision.to_string();
    let label = ResourceName::new("compact_rev_key").unwrap();
    let operation = if coordination_version == 0 {
        Operation::Create
    } else {
        Operation::Update
    };
    metrics::set_compactor_state(compactor.status.as_metric());

    let deadline = Instant::now() + timeout;
    if timeout.is_zero() {
        metrics::record_operation(operation, &label, Outcome::Timeout);
        return Outcome::Timeout;
    }

    let request = build_compact_revision_txn(value.as_bytes(), coordination_version);
    let start = Instant::now();
    metrics::add_inflight(Rpc::Txn);
    let result = time::timeout(timeout, client.txn(request)).await;
    metrics::remove_inflight(Rpc::Txn);
    metrics::record_request_duration(Rpc::Txn, start.elapsed());
    metrics::record_request_bytes(&label, b"compact_rev_key".len() + value.len());
    let response = match result {
        Ok(Ok(response)) => {
            metrics::record_request(Rpc::Txn, Outcome::Success);
            response
        }
        Ok(Err(error)) => {
            let decision = error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown);
            let outcome = match decision {
                RetryDecision::ResultUnknown => Outcome::ResultUnknown,
                RetryDecision::NotSent | RetryDecision::Retry | RetryDecision::Stop => {
                    Outcome::GrpcError
                }
            };
            metrics::record_request(Rpc::Txn, outcome);
            metrics::record_operation(operation, &label, outcome);
            return outcome;
        }
        Err(_) => {
            metrics::record_request(Rpc::Txn, Outcome::ResultUnknown);
            metrics::record_operation(operation, &label, Outcome::ResultUnknown);
            return Outcome::ResultUnknown;
        }
    };

    if let Some(header) = response.header.as_ref() {
        update_revision(&latest_revision, header.revision);
    }
    let observation = match compactor.observe(&response) {
        Ok(observation) => observation,
        Err(error) => {
            metrics::record_txn(&label, TxnShape::CompactRevision, TxnResult::Conflict);
            metrics::record_compactor_error(error.as_metric());
            metrics::set_compactor_state(compactor.status.as_metric());
            metrics::record_operation(operation, &label, Outcome::GrpcError);
            return Outcome::GrpcError;
        }
    };
    metrics::set_compactor_state(compactor.status.as_metric());
    if observation.recovered {
        if let Some(recovery) = observation.recovery {
            metrics::record_compactor_recovery(recovery.as_metric());
        } else {
            metrics::record_compactor_recovery("resync");
        }
    }
    metrics::record_txn(
        &label,
        TxnShape::CompactRevision,
        if observation.succeeded {
            TxnResult::Committed
        } else {
            TxnResult::Conflict
        },
    );
    if !observation.succeeded {
        // A CAS conflict is coordination, not a business-object retry. The
        // embedded Range supplies both the next logical version and the
        // revision another compactor already published.
        if let Some(revision) = observation.remote_compact_revision {
            metrics::set_compact_revision(revision);
        }
        metrics::record_operation(operation, &label, Outcome::Success);
        return Outcome::Success;
    }
    metrics::record_operation(operation, &label, Outcome::Success);

    let Some(revision) = observation.compact_target else {
        return Outcome::Success;
    };
    let compact = ResourceName::new("compact").unwrap();
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        metrics::record_operation(Operation::Compact, &compact, Outcome::Timeout);
        return Outcome::Timeout;
    }
    let start = Instant::now();
    metrics::add_inflight(Rpc::Compact);
    let result = time::timeout(
        remaining,
        client.compact(CompactionRequest {
            revision,
            physical: false,
        }),
    )
    .await;
    metrics::remove_inflight(Rpc::Compact);
    metrics::record_request_duration(Rpc::Compact, start.elapsed());
    metrics::record_request_bytes(&compact, 8);
    let outcome = match result {
        Ok(Ok(response)) => {
            if let Some(header) = response.header.as_ref() {
                update_revision(&latest_revision, header.revision);
            }
            metrics::set_compact_revision(revision);
            Outcome::Success
        }
        Ok(Err(error)) => {
            let decision = error.retry_decision(RpcMethod::Compact, RequestDisposition::Unknown);
            match decision {
                RetryDecision::ResultUnknown => Outcome::ResultUnknown,
                RetryDecision::NotSent | RetryDecision::Retry | RetryDecision::Stop => {
                    Outcome::GrpcError
                }
            }
        }
        Err(_) => Outcome::ResultUnknown,
    };
    metrics::record_request(Rpc::Compact, outcome);
    metrics::record_compact(outcome);
    metrics::record_operation(Operation::Compact, &compact, outcome);
    outcome
}
async fn acquire_lease(
    shared: &SharedPool,
    client: &ClientBundle,
    started: TokioInstant,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Result<i64, Outcome> {
    let now = started.elapsed();
    let deadline = Instant::now() + timeout;
    // The upstream lease manager serializes cache decisions and Grant while
    // holding its mutex; retain that single-flight lifecycle per pool.
    let mut pool = shared.pool.lock().await;
    match pool.acquire_at(now) {
        LeaseEvent::Reuse { lease_id } if lease_id != 0 => {
            metrics::record_lease_get(&shared.name, "reuse");
            metrics::set_lease_attached_objects(&shared.name, pool.attached_objects());
            Ok(lease_id)
        }
        LeaseEvent::Reuse { .. } => {
            metrics::record_lease_get(&shared.name, "error");
            Err(Outcome::GrpcError)
        }
        LeaseEvent::Grant { ttl } => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                metrics::record_lease_get(&shared.name, "timeout");
                metrics::record_request(Rpc::LeaseGrant, Outcome::Timeout);
                return Err(Outcome::Timeout);
            }
            let ttl_seconds = match i64::try_from(ttl.as_secs()) {
                Ok(ttl_seconds) => ttl_seconds,
                Err(_) => {
                    metrics::record_lease_get(&shared.name, "error");
                    metrics::record_request(Rpc::LeaseGrant, Outcome::GrpcError);
                    return Err(Outcome::GrpcError);
                }
            };
            let start = Instant::now();
            metrics::add_inflight(Rpc::LeaseGrant);
            let response = time::timeout(
                remaining,
                client.lease_grant(LeaseGrantRequest {
                    ttl: ttl_seconds,
                    id: 0,
                }),
            )
            .await;
            metrics::remove_inflight(Rpc::LeaseGrant);
            metrics::record_request_duration(Rpc::LeaseGrant, start.elapsed());
            metrics::record_request_bytes(&ResourceName::new("lease").unwrap(), 16);
            match response {
                Ok(Ok(response)) if response.id != 0 => {
                    pool.record_grant(response.id, now);
                    metrics::record_lease_get(&shared.name, "grant");
                    metrics::set_lease_attached_objects(&shared.name, 1);
                    metrics::record_request(Rpc::LeaseGrant, Outcome::Success);
                    if let Some(header) = response.header {
                        update_revision(&latest_revision, header.revision);
                    }
                    Ok(response.id)
                }
                Ok(Ok(_)) => {
                    metrics::record_lease_get(&shared.name, "error");
                    metrics::record_request(Rpc::LeaseGrant, Outcome::GrpcError);
                    Err(Outcome::GrpcError)
                }
                Ok(Err(error)) => {
                    let decision =
                        error.retry_decision(RpcMethod::LeaseGrant, RequestDisposition::Unknown);
                    let outcome = match decision {
                        RetryDecision::ResultUnknown => Outcome::ResultUnknown,
                        RetryDecision::NotSent | RetryDecision::Retry | RetryDecision::Stop => {
                            Outcome::GrpcError
                        }
                    };
                    metrics::record_lease_get(
                        &shared.name,
                        if outcome == Outcome::ResultUnknown {
                            "unknown"
                        } else {
                            "error"
                        },
                    );
                    metrics::record_request(Rpc::LeaseGrant, outcome);
                    Err(outcome)
                }
                // A local deadline can race with a server-side grant. The
                // lease id is therefore unknown; never submit another grant.
                Err(_) => {
                    metrics::record_lease_get(&shared.name, "unknown");
                    metrics::record_request(Rpc::LeaseGrant, Outcome::ResultUnknown);
                    Err(Outcome::ResultUnknown)
                }
            }
        }
    }
}
fn operation_metric(op: ResourceOperation) -> Operation {
    match op {
        ResourceOperation::Create => Operation::Create,
        ResourceOperation::GuaranteedUpdate => Operation::Update,
        ResourceOperation::ConditionalDelete => Operation::Delete,
    }
}
fn shape(op: ResourceOperation) -> TxnShape {
    match op {
        ResourceOperation::Create => TxnShape::Create,
        ResourceOperation::GuaranteedUpdate => TxnShape::Update,
        ResourceOperation::ConditionalDelete => TxnShape::Delete,
    }
}

fn storage_operation_for_state(
    operation: ResourceOperation,
    state: &KeyState,
    recreate_missing: bool,
) -> Option<ResourceOperation> {
    if matches!(operation, ResourceOperation::Create) || state.mod_revision().is_some() {
        Some(operation)
    } else if recreate_missing && operation == ResourceOperation::GuaranteedUpdate {
        Some(ResourceOperation::Create)
    } else {
        None
    }
}
fn take_storage_attempt(used: &mut u32, limit: u32) -> bool {
    if *used >= limit {
        return false;
    }
    *used = used.saturating_add(1);
    true
}

async fn execute_txn(
    client: &ClientBundle,
    label: &ResourceName,
    states: &mut HashMap<Vec<u8>, KeyState>,
    key: Vec<u8>,
    value: Vec<u8>,
    operation: ResourceOperation,
    mut inject_conflict: bool,
    cas_policy: CasRetryPolicy,
    recreate_missing: bool,
    timeout: Duration,
    lease_pool: Option<&SharedPool>,
    lease_epoch: TokioInstant,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let lease_pool = lease_pool.filter(|_| operation.uses_native_lease());
    let deadline = Instant::now() + timeout;
    let mut current_value = value;
    let state = states.get(&key).cloned().unwrap_or_default();
    let mut storage_attempts = 0u32;
    let Some(mut storage_operation) =
        storage_operation_for_state(operation, &state, recreate_missing)
    else {
        metrics::record_txn(label, shape(operation), TxnResult::Conflict);
        metrics::record_operation(operation_metric(operation), label, Outcome::NotFound);
        return Outcome::NotFound;
    };
    loop {
        if Instant::now() >= deadline {
            metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
            return Outcome::Timeout;
        }
        let mut attempt = match storage_operation {
            ResourceOperation::Create => TxnAttempt::create(key.clone(), current_value.clone()),
            ResourceOperation::GuaranteedUpdate => {
                let revision = state.mod_revision().unwrap_or(0);
                let expected_revision = if inject_conflict {
                    inject_conflict = false;
                    revision.saturating_sub(1)
                } else {
                    revision
                };
                TxnAttempt::update(key.clone(), current_value.clone(), expected_revision)
            }
            ResourceOperation::ConditionalDelete => {
                let revision = state.mod_revision().unwrap_or(0);
                TxnAttempt::delete(key.clone(), revision)
            }
        };
        let mut cas = CasLoop::new(attempt.clone(), cas_policy, deadline);
        loop {
            if !take_storage_attempt(&mut storage_attempts, cas_policy.max_attempts) {
                metrics::record_operation(operation_metric(operation), label, Outcome::Conflict);
                return Outcome::Conflict;
            }
            // Kubernetes etcd3 gets a lease inside each Put attempt; a CAS
            // retry may therefore reuse or obtain a different lease.
            let lease_id = match lease_pool {
                Some(shared) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        metrics::record_operation(
                            operation_metric(operation),
                            label,
                            Outcome::Timeout,
                        );
                        return Outcome::Timeout;
                    }
                    match acquire_lease(
                        shared,
                        client,
                        lease_epoch,
                        remaining,
                        latest_revision.clone(),
                    )
                    .await
                    {
                        Ok(lease_id) => lease_id,
                        Err(outcome) => {
                            metrics::record_operation(operation_metric(operation), label, outcome);
                            return outcome;
                        }
                    }
                }
                None => 0,
            };
            let request = attempt.request_with_lease(lease_id);
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                return Outcome::Timeout;
            }
            let start = Instant::now();
            metrics::add_inflight(Rpc::Txn);
            let response = time::timeout(remaining, client.txn(request)).await;
            metrics::remove_inflight(Rpc::Txn);
            metrics::record_request_duration(Rpc::Txn, start.elapsed());
            metrics::record_request_bytes(label, current_value.len() + key.len());
            let response: TxnResponse = match response {
                Ok(Ok(response)) => {
                    if let Some(header) = response.header.as_ref() {
                        update_revision(&latest_revision, header.revision);
                    }
                    metrics::record_request(Rpc::Txn, Outcome::Success);
                    response
                }
                Err(_) => {
                    // A local deadline can race with server-side execution.
                    // Treat the mutable result as unknown and never replay.
                    metrics::record_request(Rpc::Txn, Outcome::ResultUnknown);
                    metrics::record_operation(
                        operation_metric(operation),
                        label,
                        Outcome::ResultUnknown,
                    );
                    return Outcome::ResultUnknown;
                }
                Ok(Err(error)) => {
                    let decision =
                        error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown);
                    let outcome = match decision {
                        RetryDecision::ResultUnknown => Outcome::ResultUnknown,
                        RetryDecision::NotSent | RetryDecision::Retry | RetryDecision::Stop => {
                            Outcome::GrpcError
                        }
                    };
                    metrics::record_request(Rpc::Txn, outcome);
                    metrics::record_operation(operation_metric(operation), label, outcome);
                    return outcome;
                }
            };
            metrics::record_txn(
                label,
                shape(storage_operation),
                if response.succeeded {
                    TxnResult::Committed
                } else {
                    TxnResult::Conflict
                },
            );
            match cas.observe(&response, Instant::now()) {
                CasDecision::Succeeded { state: next, .. } => {
                    if operation == ResourceOperation::ConditionalDelete {
                        states.remove(&key);
                    } else {
                        states.insert(key.clone(), next);
                    }
                    metrics::record_operation(operation_metric(operation), label, Outcome::Success);
                    return Outcome::Success;
                }
                CasDecision::CreateCollision { latest, .. } => {
                    states.insert(key.clone(), latest);
                    metrics::record_operation(
                        operation_metric(operation),
                        label,
                        Outcome::Conflict,
                    );
                    return Outcome::Conflict;
                }
                CasDecision::Retry { latest, .. } => {
                    metrics::record_cas_retry(label, operation_metric(operation));
                    current_value = if storage_operation == ResourceOperation::ConditionalDelete {
                        Vec::new()
                    } else {
                        resource_value_for_retry(&current_value, attempt.number)
                    };
                    let Some(next) = cas.retry_with_value(current_value.clone(), &latest) else {
                        metrics::record_operation(
                            operation_metric(operation),
                            label,
                            Outcome::NotFound,
                        );
                        return Outcome::NotFound;
                    };
                    attempt = next.clone();
                }
                CasDecision::Exhausted {
                    reason: ExhaustedReason::MissingFailureKey,
                    ..
                } if recreate_missing
                    && storage_operation == ResourceOperation::GuaranteedUpdate =>
                {
                    metrics::record_cas_retry(label, operation_metric(operation));
                    storage_operation = ResourceOperation::Create;
                    break;
                }
                CasDecision::Exhausted { reason, .. } => {
                    let outcome = match reason {
                        ExhaustedReason::CancelledOrDeadline => Outcome::Timeout,
                        ExhaustedReason::MissingFailureKey => Outcome::NotFound,
                        ExhaustedReason::RetryPolicy => Outcome::Conflict,
                    };
                    metrics::record_operation(operation_metric(operation), label, outcome);
                    return outcome;
                }
            }
        }
    }
}
fn resource_value_for_retry(value: &[u8], attempt: u32) -> Vec<u8> {
    let mut out = value.to_vec();
    if let Some(first) = out.first_mut() {
        *first = first.wrapping_add(attempt as u8);
    }
    out
}
async fn execute_heartbeat(
    client: &ClientBundle,
    states: &mut HashMap<Vec<u8>, KeyState>,
    item: HeartbeatItem,
    timeout: Duration,
    status_retry_limit: u32,
    lease_retry_limit: u32,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let (resource, kind) = match item.kind {
        HeartbeatItemKind::NodeCreate
        | HeartbeatItemKind::NodeReady
        | HeartbeatItemKind::NodeStatus => (
            "nodes",
            if matches!(item.kind, HeartbeatItemKind::NodeStatus) {
                HeartbeatKind::NodeStatus
            } else {
                HeartbeatKind::Node
            },
        ),
        HeartbeatItemKind::NodeLeaseCreate | HeartbeatItemKind::NodeLeaseRenew => {
            ("node_leases", HeartbeatKind::NodeLease)
        }
    };
    let operation = if matches!(
        item.kind,
        HeartbeatItemKind::NodeCreate | HeartbeatItemKind::NodeLeaseCreate
    ) {
        ResourceOperation::Create
    } else {
        ResourceOperation::GuaranteedUpdate
    };
    let spec = ResourceSpec {
        name: resource.into(),
        key_pattern: item.key.clone(),
        count: ResourceCountSpec::Fixed { count: 1 },
        update_amplification: 1,
        value_size_bytes: if resource == "nodes" { 2048 } else { 256 },
        behavior: ResourceBehavior::GuaranteedUpdate,
        lease_pool: None,
    };
    let label = ResourceName::try_from(resource).expect("static heartbeat metric label");
    let policy = CasRetryPolicy {
        max_attempts: match item.kind {
            HeartbeatItemKind::NodeStatus => status_retry_limit.max(1),
            HeartbeatItemKind::NodeLeaseRenew => lease_retry_limit.max(1),
            _ => 1,
        },
    };
    let outcome = execute_txn(
        client,
        &label,
        states,
        item.key.into_bytes(),
        resource_value(&spec, item.node as u64, item.at.as_nanos() as u64),
        operation,
        false,
        policy,
        false,
        timeout,
        None,
        TokioInstant::now(),
        latest_revision,
    )
    .await;
    metrics::record_node_heartbeat(kind, outcome);
    outcome
}
#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::pb::etcdserverpb::ResponseOp;
    use crate::pb::etcdserverpb::compare::TargetUnion;
    use crate::pb::mvccpb;

    #[derive(Clone)]
    struct LeaseTxnProbe {
        grant_ttls: std::sync::Arc<std::sync::Mutex<Vec<i64>>>,
        txn_leases: std::sync::Arc<std::sync::Mutex<Vec<i64>>>,
        txn_calls: std::sync::Arc<AtomicUsize>,
    }

    impl LeaseTxnProbe {
        fn new() -> Self {
            Self {
                grant_ttls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                txn_leases: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                txn_calls: std::sync::Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[tonic::async_trait]
    impl crate::pb::etcdserverpb::kv_server::Kv for LeaseTxnProbe {
        async fn range(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::RangeRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::RangeResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "range is not part of this probe",
            ))
        }

        async fn put(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::PutRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::PutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "put is not part of this probe",
            ))
        }

        async fn delete_range(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::DeleteRangeRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::DeleteRangeResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "delete_range is not part of this probe",
            ))
        }

        async fn txn(
            &self,
            request: tonic::Request<crate::pb::etcdserverpb::TxnRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::TxnResponse>, tonic::Status> {
            let call = self
                .txn_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let request = request.into_inner();
            let lease = request
                .success
                .first()
                .and_then(|operation| match operation.request.as_ref() {
                    Some(crate::pb::etcdserverpb::request_op::Request::RequestPut(put)) => {
                        Some(put.lease)
                    }
                    _ => None,
                })
                .unwrap_or(-1);
            self.txn_leases.lock().unwrap().push(lease);

            match call {
                0 => Ok(tonic::Response::new(crate::pb::etcdserverpb::TxnResponse {
                    header: Some(crate::pb::etcdserverpb::ResponseHeader {
                        revision: 2,
                        ..Default::default()
                    }),
                    succeeded: false,
                    responses: vec![ResponseOp {
                        response: Some(
                            crate::pb::etcdserverpb::response_op::Response::ResponseRange(
                                crate::pb::etcdserverpb::RangeResponse {
                                    kvs: vec![mvccpb::KeyValue {
                                        key: b"k".to_vec(),
                                        value: b"remote".to_vec(),
                                        mod_revision: 1,
                                        version: 2,
                                        ..Default::default()
                                    }],
                                    ..Default::default()
                                },
                            ),
                        ),
                    }],
                    ..Default::default()
                })),
                1 => Ok(tonic::Response::new(crate::pb::etcdserverpb::TxnResponse {
                    header: Some(crate::pb::etcdserverpb::ResponseHeader {
                        revision: 3,
                        ..Default::default()
                    }),
                    succeeded: true,
                    ..Default::default()
                })),
                _ => Err(tonic::Status::internal("unexpected Txn call")),
            }
        }

        async fn compact(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::CompactionRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::CompactionResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "compact is not part of this probe",
            ))
        }
    }

    #[tonic::async_trait]
    impl crate::pb::etcdserverpb::lease_server::Lease for LeaseTxnProbe {
        type LeaseKeepAliveStream = tokio_stream::Empty<
            Result<crate::pb::etcdserverpb::LeaseKeepAliveResponse, tonic::Status>,
        >;

        async fn lease_grant(
            &self,
            request: tonic::Request<crate::pb::etcdserverpb::LeaseGrantRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::LeaseGrantResponse>, tonic::Status>
        {
            let ttl = request.into_inner().ttl;
            let mut grants = self.grant_ttls.lock().unwrap();
            let id = 100 + grants.len() as i64;
            grants.push(ttl);
            Ok(tonic::Response::new(
                crate::pb::etcdserverpb::LeaseGrantResponse {
                    id,
                    ttl,
                    ..Default::default()
                },
            ))
        }

        async fn lease_revoke(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::LeaseRevokeRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::LeaseRevokeResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "lease_revoke is not part of this probe",
            ))
        }

        async fn lease_keep_alive(
            &self,
            _request: tonic::Request<
                tonic::Streaming<crate::pb::etcdserverpb::LeaseKeepAliveRequest>,
            >,
        ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "lease_keep_alive is not part of this probe",
            ))
        }

        async fn lease_time_to_live(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::LeaseTimeToLiveRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::LeaseTimeToLiveResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "lease_time_to_live is not part of this probe",
            ))
        }

        async fn lease_leases(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::LeaseLeasesRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::LeaseLeasesResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "lease_leases is not part of this probe",
            ))
        }
    }

    #[derive(Clone, Default)]
    struct CreatePhaseProbe {
        requests: Arc<std::sync::Mutex<Vec<crate::pb::etcdserverpb::TxnRequest>>>,
        txn_replies:
            Arc<std::sync::Mutex<VecDeque<oneshot::Receiver<Result<TxnResponse, tonic::Status>>>>>,
        txn_entered: Arc<Notify>,
        range_requests: Arc<std::sync::Mutex<Vec<RangeRequest>>>,
        range_reply: Arc<std::sync::Mutex<Option<crate::pb::etcdserverpb::RangeResponse>>>,
        compact_requests: Arc<std::sync::Mutex<Vec<CompactionRequest>>>,
    }

    impl CreatePhaseProbe {
        fn next_txn_reply(&self) -> oneshot::Sender<Result<TxnResponse, tonic::Status>> {
            let (sender, receiver) = oneshot::channel();
            self.txn_replies.lock().unwrap().push_back(receiver);
            sender
        }

        async fn wait_for_txns(&self, count: usize) {
            time::timeout(Duration::from_secs(3), async {
                loop {
                    let notified = self.txn_entered.notified();
                    if self.requests.lock().unwrap().len() >= count {
                        return;
                    }
                    notified.await;
                }
            })
            .await
            .expect("probe did not receive the expected Txn");
        }
    }

    #[tonic::async_trait]
    impl crate::pb::etcdserverpb::kv_server::Kv for CreatePhaseProbe {
        async fn range(
            &self,
            request: tonic::Request<crate::pb::etcdserverpb::RangeRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::RangeResponse>, tonic::Status>
        {
            self.range_requests
                .lock()
                .unwrap()
                .push(request.into_inner());
            self.range_reply
                .lock()
                .unwrap()
                .clone()
                .map(tonic::Response::new)
                .ok_or_else(|| tonic::Status::unimplemented("unscripted Range"))
        }

        async fn put(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::PutRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::PutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented(
                "put is not part of this probe",
            ))
        }

        async fn delete_range(
            &self,
            _request: tonic::Request<crate::pb::etcdserverpb::DeleteRangeRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::DeleteRangeResponse>, tonic::Status>
        {
            Err(tonic::Status::unimplemented(
                "delete_range is not part of this probe",
            ))
        }

        async fn txn(
            &self,
            request: tonic::Request<crate::pb::etcdserverpb::TxnRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::TxnResponse>, tonic::Status> {
            let revision = {
                let mut requests = self.requests.lock().unwrap();
                requests.push(request.into_inner());
                requests.len() as i64
            };
            let reply = self.txn_replies.lock().unwrap().pop_front();
            self.txn_entered.notify_one();
            match reply {
                Some(reply) => reply
                    .await
                    .expect("test dropped a scripted Txn response")
                    .map(tonic::Response::new),
                None => Ok(tonic::Response::new(compact_response(
                    revision, true, 0, b"",
                ))),
            }
        }

        async fn compact(
            &self,
            request: tonic::Request<crate::pb::etcdserverpb::CompactionRequest>,
        ) -> Result<tonic::Response<crate::pb::etcdserverpb::CompactionResponse>, tonic::Status>
        {
            self.compact_requests
                .lock()
                .unwrap()
                .push(request.into_inner());
            Err(tonic::Status::permission_denied(
                "compaction denied after journal commit",
            ))
        }
    }

    async fn start_create_probe(
        probe: CreatePhaseProbe,
    ) -> (ClientBundle, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(crate::pb::etcdserverpb::kv_server::KvServer::new(probe))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = ClientBundle::connect(
            &format!("http://{address}"),
            Duration::from_secs(3),
            1024 * 1024,
        )
        .await
        .unwrap();
        (client, server)
    }

    fn spawn_probe_worker(
        client: ClientBundle,
        workload: WorkloadSpec,
    ) -> (
        mpsc::Sender<WorkItem>,
        tokio::task::JoinHandle<Result<(), String>>,
    ) {
        let (sender, receiver) = mpsc::channel(4);
        let worker = tokio::spawn(worker_loop(
            receiver,
            client,
            workload,
            Duration::from_secs(3),
            Arc::new(HashMap::new()),
            Arc::new(AtomicI64::new(0)),
            TokioInstant::now(),
            Arc::new(AtomicU64::new(0)),
        ));
        (sender, worker)
    }

    fn spawn_owned_probe_worker(
        client: ClientBundle,
        workload: WorkloadSpec,
    ) -> (
        mpsc::Sender<WorkItem>,
        JoinSet<Result<(), String>>,
        std::sync::Weak<()>,
    ) {
        let (sender, receiver) = mpsc::channel(4);
        let lifetime = Arc::new(());
        let weak = Arc::downgrade(&lifetime);
        let mut workers = JoinSet::new();
        workers.spawn(async move {
            let _lifetime = lifetime;
            worker_loop(
                receiver,
                client,
                workload,
                Duration::from_secs(3),
                Arc::new(HashMap::new()),
                Arc::new(AtomicI64::new(0)),
                TokioInstant::now(),
                Arc::new(AtomicU64::new(0)),
            )
            .await
        });
        (sender, workers, weak)
    }

    fn pending_ctrl_c() -> CtrlCFuture {
        Box::pin(std::future::pending::<Result<(), std::io::Error>>())
    }

    fn controlled_ctrl_c() -> (oneshot::Sender<()>, CtrlCFuture) {
        let (sender, receiver) = oneshot::channel();
        let future: CtrlCFuture = Box::pin(async move {
            receiver
                .await
                .expect("test canceled signal sender prematurely");
            Ok(())
        });
        (sender, future)
    }

    fn report_class(report: &RunReport, class: WorkClass) -> &WorkItemReport {
        report
            .work_items
            .iter()
            .find(|item| item.work_class == class.as_str())
            .unwrap()
    }

    fn assert_report_equations(report: &RunReport) {
        for item in &report.work_items {
            assert_eq!(
                item.offered,
                item.accepted + item.dropped + item.unissued,
                "{}",
                item.work_class
            );
            assert_eq!(
                item.accepted,
                item.completed + item.unfinished,
                "{}",
                item.work_class
            );
            assert_eq!(
                item.completed,
                item.succeeded + item.failed,
                "{}",
                item.work_class
            );
            assert_eq!(
                item.dropped,
                item.dropped_full + item.dropped_closed,
                "{}",
                item.work_class
            );
            assert!(item.result_unknown <= item.failed, "{}", item.work_class);
        }
        assert!(
            report
                .accounting_invariants
                .offered_equals_accepted_plus_dropped_plus_unissued
        );
        assert!(
            report
                .accounting_invariants
                .accepted_equals_completed_plus_unfinished
        );
        assert!(
            report
                .accounting_invariants
                .accepted_equals_succeeded_plus_failed_plus_unfinished
        );
        assert!(report.accounting_invariants.result_unknown_subset_failed);
    }

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(
                future.as_mut().poll(cx).is_pending(),
                "operation unexpectedly completed"
            );
            std::task::Poll::Ready(())
        })
        .await;
    }

    fn probe_heartbeat(kind: HeartbeatItemKind) -> HeartbeatItem {
        HeartbeatItem {
            node: 0,
            kind,
            key: "/registry/minions/node-0000".into(),
            at: Duration::ZERO,
        }
    }

    fn probe_workload() -> WorkloadSpec {
        let mut workload = exact_phase_workload();
        workload.topology.node_count = 1;
        workload.topology.pod_count = 1;
        workload
            .resources
            .retain(|resource| resource.name == "pods");
        workload.resources[0].update_amplification = 1;
        workload.etcd_lease_pools.clear();
        workload.watches.clear();
        workload.ranges.clear();
        workload.range_count_only_count = 0;
        workload.cas_conflicts = 0;
        workload.status_count = 0;
        workload.compact_interval_s = 300.0;
        workload.timeline = vec![crate::config::TimelineSegment {
            duration_s: 1.0,
            range_rps: 0.0,
        }];
        workload
    }

    fn probe_config(workload: WorkloadSpec) -> RuntimeConfig {
        RuntimeConfig {
            endpoint: "http://127.0.0.1:1".into(),
            nodes: workload.topology.node_count,
            pod_size: workload.pod_size_bytes,
            workload,
            workers: 2,
            queue_capacity: 8,
            request_timeout: Duration::from_secs(3),
            operation_timeout: Duration::from_secs(3),
            max_message_size: 1024 * 1024,
            metrics_listen: "127.0.0.1:0".parse().unwrap(),
            seed: 7,
        }
    }

    #[tokio::test]
    async fn guaranteed_update_reacquires_native_lease_for_each_attempt() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let probe = LeaseTxnProbe::new();
        let server_probe = probe.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            ready_tx.send(()).unwrap();
            Server::builder()
                .add_service(crate::pb::etcdserverpb::kv_server::KvServer::new(
                    server_probe.clone(),
                ))
                .add_service(crate::pb::etcdserverpb::lease_server::LeaseServer::new(
                    server_probe,
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        ready_rx.await.unwrap();

        let client = ClientBundle::connect(
            &format!("http://{address}"),
            Duration::from_secs(2),
            1024 * 1024,
        )
        .await
        .unwrap();
        let key = b"k".to_vec();
        let mut states = HashMap::new();
        states.insert(key.clone(), KeyState::Present { mod_revision: 1 });
        let shared = SharedPool {
            name: PoolName::new("masterleases").unwrap(),
            pool: Mutex::new(LeasePool::new(LeasePoolConfig {
                object_ttl: Duration::from_secs(15),
                reuse_window: Duration::from_secs(60),
                max_objects: 1,
            })),
        };
        let result = execute_txn(
            &client,
            &ResourceName::new("events").unwrap(),
            &mut states,
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            false,
            CasRetryPolicy { max_attempts: 5 },
            false,
            Duration::from_secs(2),
            Some(&shared),
            TokioInstant::now(),
            Arc::new(AtomicI64::new(0)),
        )
        .await;

        assert_eq!(result, Outcome::Success);
        assert_eq!(*probe.grant_ttls.lock().unwrap(), vec![15, 15]);
        assert_eq!(*probe.txn_leases.lock().unwrap(), vec![100, 101]);

        server.abort();
        let _ = server.await;
    }

    fn resource_cas_conflict(mod_revision: Option<i64>) -> TxnResponse {
        TxnResponse {
            header: Some(crate::pb::etcdserverpb::ResponseHeader {
                revision: 10,
                ..Default::default()
            }),
            succeeded: false,
            responses: vec![crate::pb::etcdserverpb::ResponseOp {
                response: Some(
                    crate::pb::etcdserverpb::response_op::Response::ResponseRange(
                        crate::pb::etcdserverpb::RangeResponse {
                            kvs: mod_revision
                                .map(|mod_revision| mvccpb::KeyValue {
                                    key: b"k".to_vec(),
                                    value: b"latest".to_vec(),
                                    mod_revision,
                                    ..Default::default()
                                })
                                .into_iter()
                                .collect(),
                            ..Default::default()
                        },
                    ),
                ),
            }],
        }
    }

    async fn run_resource_cas_probe(
        operation: ResourceOperation,
        recreate_missing: bool,
        max_attempts: u32,
        timeout: Duration,
        replies: Vec<Result<TxnResponse, tonic::Status>>,
    ) -> (
        Outcome,
        Vec<crate::pb::etcdserverpb::TxnRequest>,
        Option<i64>,
    ) {
        let probe = CreatePhaseProbe::default();
        let expected_requests = replies.len();
        for reply in replies {
            probe.next_txn_reply().send(reply).unwrap();
        }
        let (client, server) = start_create_probe(probe.clone()).await;
        let key = b"k".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 1 })]);
        let outcome = execute_txn(
            &client,
            &ResourceName::new("pods").unwrap(),
            &mut states,
            key.clone(),
            b"value".to_vec(),
            operation,
            false,
            CasRetryPolicy { max_attempts },
            recreate_missing,
            timeout,
            None,
            TokioInstant::now(),
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        server.abort();
        let _ = server.await;
        let requests = std::mem::take(&mut *probe.requests.lock().unwrap());
        assert_eq!(
            requests.len(),
            expected_requests,
            "resource CAS must issue exactly the scripted attempts"
        );
        (
            outcome,
            requests,
            states.get(&key).and_then(KeyState::mod_revision),
        )
    }

    #[tokio::test]
    async fn resource_cas_distinguishes_conflict_and_not_found() {
        for (name, operation, recreate, budget, replies, expected, revisions) in [
            (
                "create collision",
                ResourceOperation::Create,
                false,
                u32::MAX,
                vec![TxnResponse {
                    header: Some(crate::pb::etcdserverpb::ResponseHeader {
                        revision: 10,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                Outcome::Conflict,
                vec![0],
            ),
            (
                "ordinary missing key",
                ResourceOperation::GuaranteedUpdate,
                false,
                5,
                vec![resource_cas_conflict(None)],
                Outcome::NotFound,
                vec![1],
            ),
            (
                "finite attempt exhaustion",
                ResourceOperation::GuaranteedUpdate,
                false,
                2,
                vec![
                    resource_cas_conflict(Some(2)),
                    resource_cas_conflict(Some(3)),
                ],
                Outcome::Conflict,
                vec![1, 2],
            ),
            (
                "masterlease recreation shares the attempt budget",
                ResourceOperation::GuaranteedUpdate,
                true,
                1,
                vec![resource_cas_conflict(None)],
                Outcome::Conflict,
                vec![1],
            ),
        ] {
            let (outcome, requests, _) = run_resource_cas_probe(
                operation,
                recreate,
                budget,
                Duration::from_secs(3),
                replies.into_iter().map(Ok).collect(),
            )
            .await;
            assert_eq!(outcome, expected, "{name}");
            for (request, revision) in requests.iter().zip(revisions) {
                assert_eq!(
                    request.compare[0].target_union,
                    Some(TargetUnion::ModRevision(revision)),
                    "{name}"
                );
            }
        }
    }

    #[tokio::test]
    async fn resource_cas_rebuilds_from_failure_kv_and_publishes_commit_revision() {
        let (outcome, requests, revision) = run_resource_cas_probe(
            ResourceOperation::GuaranteedUpdate,
            false,
            u32::MAX,
            Duration::from_secs(3),
            vec![
                Ok(resource_cas_conflict(Some(2))),
                Ok(TxnResponse {
                    succeeded: true,
                    header: Some(crate::pb::etcdserverpb::ResponseHeader {
                        revision: 10,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            ],
        )
        .await;
        assert_eq!(outcome, Outcome::Success);
        assert_eq!(revision, Some(10));
        assert_eq!(
            requests[0].compare[0].target_union,
            Some(TargetUnion::ModRevision(1))
        );
        assert_eq!(
            requests[1].compare[0].target_union,
            Some(TargetUnion::ModRevision(2))
        );
        assert_ne!(requests[0].success, requests[1].success);
    }

    #[tokio::test]
    async fn resource_cas_recreates_missing_masterlease_without_resetting_budget() {
        let (outcome, requests, revision) = run_resource_cas_probe(
            ResourceOperation::GuaranteedUpdate,
            true,
            2,
            Duration::from_secs(3),
            vec![
                Ok(resource_cas_conflict(None)),
                Ok(TxnResponse {
                    succeeded: true,
                    header: Some(crate::pb::etcdserverpb::ResponseHeader {
                        revision: 10,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            ],
        )
        .await;
        assert_eq!(outcome, Outcome::Success);
        assert_eq!(revision, Some(10));
        assert_eq!(
            requests[0].compare[0].target_union,
            Some(TargetUnion::ModRevision(1))
        );
        assert_eq!(
            requests[1].compare[0].target_union,
            Some(TargetUnion::ModRevision(0))
        );
        assert!(requests[1].failure.is_empty());
    }

    #[tokio::test]
    async fn resource_cas_expired_before_send_is_timeout() {
        let (outcome, requests, _) = run_resource_cas_probe(
            ResourceOperation::GuaranteedUpdate,
            false,
            5,
            Duration::ZERO,
            Vec::new(),
        )
        .await;
        assert_eq!(outcome, Outcome::Timeout);
        assert!(requests.is_empty());
    }

    #[tokio::test]
    async fn resource_cas_unknown_mutable_result_is_not_replayed() {
        for status in [
            tonic::Status::unavailable("response lost after possible commit"),
            tonic::Status::deadline_exceeded("commit result is unknown"),
        ] {
            let (outcome, requests, _) = run_resource_cas_probe(
                ResourceOperation::GuaranteedUpdate,
                false,
                5,
                Duration::from_secs(3),
                vec![Err(status)],
            )
            .await;
            assert_eq!(outcome, Outcome::ResultUnknown);
            assert_eq!(requests.len(), 1);
        }
    }

    #[tokio::test]
    async fn create_barrier_precedes_exact_successful_update_phase() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let probe = CreatePhaseProbe::default();
        let requests = Arc::clone(&probe.requests);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(crate::pb::etcdserverpb::kv_server::KvServer::new(probe))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = ClientBundle::connect(
            &format!("http://{address}"),
            Duration::from_secs(1),
            1024 * 1024,
        )
        .await
        .unwrap();
        let mut workload = exact_phase_workload();
        workload
            .resources
            .retain(|resource| resource.name == "pods");
        workload.etcd_lease_pools.clear();
        let plan = WorkloadPlan::build(&workload).unwrap();
        let accounting = RunAccounting::new();
        let pools = Arc::new(HashMap::new());
        let (sender, receiver) = mpsc::channel(2);
        let worker = tokio::spawn(worker_loop(
            receiver,
            client,
            workload.clone(),
            Duration::from_secs(1),
            Arc::clone(&pools),
            Arc::new(AtomicI64::new(0)),
            TokioInstant::now(),
            Arc::new(AtomicU64::new(0)),
        ));

        let scheduled = run_create_phase(
            &workload,
            &plan,
            std::slice::from_ref(&sender),
            1,
            Duration::from_secs(2),
            &accounting,
        )
        .await
        .unwrap();
        assert_eq!(scheduled, 4);
        assert_eq!(requests.lock().unwrap().len(), 4);

        let updates = run_update_phase(
            workload.clone(),
            plan,
            vec![sender.clone()],
            1,
            7,
            TokioInstant::now(),
            Duration::from_millis(100),
            Arc::clone(&accounting),
        )
        .await
        .unwrap();
        assert_eq!(updates, 8);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 12);
        for request in &requests[..4] {
            assert!(matches!(
                request.compare[0].target_union,
                Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                    0
                ))
            ));
        }
        for request in &requests[4..] {
            assert!(matches!(
                request.compare[0].target_union,
                Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                    revision
                )) if revision > 0
            ));
        }
        drop(requests);

        drop(sender);
        worker.await.unwrap().unwrap();
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn phase_tracker_deadline_reports_remaining_success_deficit() {
        let tracker = PhaseTracker::new(vec![2]);
        tracker.record(0, Outcome::Success);
        let error = tracker
            .wait_until("update", TokioInstant::now() + Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(error.contains("target=2"));
        assert!(error.contains("succeeded=1"));
        assert!(error.contains("deficit=1"));
    }

    #[test]
    fn update_dispatch_reserves_time_for_completion() {
        let duration = Duration::from_secs(100);
        assert_eq!(update_dispatch_at(duration, 0, 10), Duration::ZERO);
        assert_eq!(
            update_dispatch_at(duration, 9, 10),
            Duration::from_millis(85_500)
        );
        assert_eq!(
            update_dispatch_at(duration, 9_999, 10_000),
            Duration::from_nanos(94_990_500_000)
        );
    }

    #[test]
    fn lazy_cursor_has_bounded_state() {
        let workload = WorkloadSpec {
            name: "k8s".into(),
            pod_size_bytes: 4096,
            topology: TopologySpec {
                node_count: 1,
                pod_count: 10_000,
            },
            relationships: crate::config::ResourceRelationshipsSpec::default(),
            nodes: crate::config::NodeSpec {
                status_interval_s: 300.0,
                lease_interval_s: 10.0,
                lease_jitter: 0.0,
                status_retry_limit: 5,
                lease_retry_limit: 5,
                node_lease_enabled: false,
            },
            timeline: vec![crate::config::TimelineSegment {
                duration_s: 10.0,
                range_rps: 1000.0,
            }],
            resources: vec![ResourceSpec {
                name: "pods".into(),
                key_pattern: "/p/{id}".into(),
                count: ResourceCountSpec::Pods {
                    numerator: 1,
                    denominator: 1,
                },
                update_amplification: 1,
                value_size_bytes: 4096,
                behavior: ResourceBehavior::GuaranteedUpdate,
                lease_pool: None,
            }],
            ranges: vec![],
            watches: vec![],
            etcd_lease_pools: vec![],
            status_count: 0,
            compact_interval_s: 300.0,
            master_lease_interval_s: 10.0,
            cas_conflicts: 0,
            range_count_only_count: 0,
        };
        let cursor = RateCursor::new(&workload, 1, 1);
        assert_eq!(cursor.total(), 10000);
    }
    #[test]
    fn stable_routing_is_repeatable() {
        assert_eq!(stable_shard(b"/k", 7), stable_shard(b"/k", 7));
    }
    #[test]
    fn prefix_end_handles_ff() {
        assert_eq!(prefix_range_end(&[0xff]), vec![0]);
        assert_eq!(prefix_range_end(&[b'a', 0xff]), vec![b'b']);
    }

    #[test]
    fn range_shape_classifies_each_kubernetes_request_form() {
        assert_eq!(range_shape(&RangeRequest::default()), RangeShape::Point);
        assert_eq!(
            range_shape(&RangeRequest {
                range_end: b"z".to_vec(),
                count_only: true,
                ..Default::default()
            }),
            RangeShape::CountOnly
        );
        assert_eq!(
            range_shape(&RangeRequest {
                range_end: b"z".to_vec(),
                ..Default::default()
            }),
            RangeShape::UnboundedPrefix
        );
        assert_eq!(
            range_shape(&RangeRequest {
                range_end: b"z".to_vec(),
                limit: 500,
                ..Default::default()
            }),
            RangeShape::PagedPrefix
        );
    }
    #[test]
    fn embedded_ranges_scan_resource_prefixes() {
        let workload =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        let pods = workload
            .ranges
            .iter()
            .find(|range| range.key_pattern == "/registry/pods/bench-{ns}/")
            .unwrap();
        let request = range_request(pods, 42);
        assert_eq!(request.key, b"/registry/pods/bench-bench/");
        assert_eq!(request.range_end, prefix_range_end(&request.key));

        let minions = workload
            .ranges
            .iter()
            .find(|range| range.key_pattern == "/registry/minions/")
            .unwrap();
        let request = range_request(minions, 42);
        assert_eq!(request.key, b"/registry/minions/");
        assert_eq!(request.range_end, prefix_range_end(&request.key));
    }

    #[test]
    fn application_targets_are_exact_and_exclude_masterlease_updates() {
        let workload =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        assert_eq!(application_operation_total(&workload), 492_618);
        let plan = WorkloadPlan::build(&workload).unwrap();
        assert_eq!(plan.total_creates(), 80_248);
        assert_eq!(plan.total_updates(), 412_371);
    }

    #[test]
    fn node_status_phases_are_uniform_and_repeatable() {
        let interval = Duration::from_secs(300);
        let phases: Vec<_> = (0..4)
            .map(|node| node_status_phase(node, 4, interval))
            .collect();
        assert_eq!(
            phases,
            vec![
                Duration::from_secs(75),
                Duration::from_secs(150),
                Duration::from_secs(225),
                Duration::from_secs(300),
            ]
        );
    }

    #[test]
    fn node_status_ticks_follow_each_nodes_phase_until_deadline() {
        let nodes = 2000;
        let interval = Duration::from_secs(300);
        let duration = Duration::from_secs(2310);
        let mut total = 0;
        for node in 0..nodes {
            let mut at = node_status_phase(node, nodes, interval);
            while at <= duration {
                total += 1;
                at = at.saturating_add(interval);
            }
        }
        assert_eq!(total, 15_400);
    }

    #[test]
    fn rolling_update_workload_schedules_exact_budget() {
        let base =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        let base_range = range_request_total(&base, 1);
        let base_app = application_operation_total(&base);
        let (nodes, batch) = (100u64, 10u64);
        let waves = nodes.div_ceil(batch);
        let workload = base
            .rolling_update(nodes, batch, 500.0, "rolling-test", base_range, base_app)
            .unwrap();
        let expected = 8 * nodes + 1 + 2 * waves;
        assert_eq!(application_operation_total(&workload), expected);
        let ranges = RateCursor::new(&workload, 1, 0xB2);
        let weights: Vec<f64> = workload.ranges.iter().map(|range| range.weight).collect();
        let producer = RangeProducer::new(weights.iter().copied());
        let prefix_total = exact_prefix_total(producer, &weights, &workload.ranges, ranges.total());
        range_shape_plan(&workload, prefix_total).unwrap();
    }

    #[test]
    fn masterlease_ticks_stop_at_workload_duration() {
        let interval = Duration::from_secs(10);
        let duration = Duration::from_secs(15);
        // Derived short run: only the ordinal-0 tick at t=10s fits.
        assert_eq!(masterlease_tick_at(interval, 0, duration), Some(interval));
        assert_eq!(masterlease_tick_at(interval, 1, duration), None);
        // An interval longer than the run suppresses even the initial tick.
        assert_eq!(
            masterlease_tick_at(Duration::from_secs(300), 0, duration),
            None
        );
        // Trace mode: exactly 231 ticks, the last one lands on t == duration.
        let trace_duration = Duration::from_secs(2310);
        let ticks = (0..231)
            .filter(|&ordinal| masterlease_tick_at(interval, ordinal, trace_duration).is_some())
            .count();
        assert_eq!(ticks, 231);
        assert_eq!(masterlease_tick_at(interval, 231, trace_duration), None);
        // Huge intervals cannot overflow even while ordinals remain.
        assert_eq!(
            masterlease_tick_at(Duration::from_secs_f64(1e19), 1, trace_duration),
            None
        );
    }

    #[test]
    fn range_shape_plan_emits_exact_trace_counts() {
        let workload =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        let ranges = RateCursor::new(&workload, 1, 0xB2);
        assert_eq!(ranges.total(), 22_080);
        let weights: Vec<f64> = workload.ranges.iter().map(|range| range.weight).collect();
        let producer = RangeProducer::new(weights.iter().copied());
        let prefix_total = exact_prefix_total(producer, &weights, &workload.ranges, ranges.total());
        let mut plan = range_shape_plan(&workload, prefix_total).unwrap();
        assert_eq!(plan.prefix_total, 16_875);
        assert_eq!(plan.count_only, 220);
        let mut count_only = 0;
        for _ in 0..plan.prefix_total {
            let mut request = RangeRequest {
                limit: 500,
                ..Default::default()
            };
            plan.apply(&mut request);
            count_only += u64::from(request.count_only);
        }
        assert_eq!(count_only, 220);
    }

    #[test]
    fn range_shapes_follow_actual_prefix_slots() {
        let mut workload =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        workload.ranges = vec![
            crate::config::RangeSpec {
                key_pattern: "/registry/prefix/".into(),
                kind: "prefix".into(),
                limit: 500,
                count_only: false,
                weight: 1.0,
                selector_keep_every: 1,
            },
            crate::config::RangeSpec {
                key_pattern: "/registry/point".into(),
                kind: "point".into(),
                limit: 0,
                count_only: false,
                weight: 1.0,
                selector_keep_every: 1,
            },
        ];
        workload.range_count_only_count = 0;
        let weights = vec![1.0, 1.0];
        let producer = RangeProducer::new(weights.iter().copied());
        let prefix_total = exact_prefix_total(producer.clone(), &weights, &workload.ranges, 3);
        assert_eq!(prefix_total, 1);
        let mut plan = range_shape_plan(&workload, prefix_total).unwrap();
        let mut producer = producer;
        let mut shaped = 0;
        for _ in 0..3 {
            let index = producer.next(&weights).unwrap();
            if workload.ranges[index].kind == "prefix" {
                let mut request = RangeRequest {
                    limit: 500,
                    ..Default::default()
                };
                plan.apply(&mut request);
                shaped += u64::from(request.count_only);
            }
        }
        assert_eq!(shaped, 0);
    }

    fn range_page(
        revision: i64,
        keys: &[&[u8]],
        more: bool,
        count: i64,
    ) -> crate::pb::etcdserverpb::RangeResponse {
        crate::pb::etcdserverpb::RangeResponse {
            header: Some(crate::pb::etcdserverpb::ResponseHeader {
                revision,
                ..Default::default()
            }),
            kvs: keys
                .iter()
                .map(|key| crate::pb::mvccpb::KeyValue {
                    key: key.to_vec(),
                    ..Default::default()
                })
                .collect(),
            more,
            count,
        }
    }

    #[test]
    fn range_pager_pages_without_duplicates_and_fixes_revision() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 10_000,
            ..Default::default()
        };
        let mut pager = RangePager::new(request, RangeSelector::All { target: None });
        assert_eq!(pager.request().limit, 500);
        let first = range_page(42, &[b"a", b"b"], true, 4);
        assert!(matches!(
            pager.observe_response(&first, 2).unwrap(),
            RangePageOutcome::Continue(_)
        ));
        let next = pager.request();
        assert_eq!(next.key, b"b\0");
        assert_eq!(next.range_end, b"/registry/pods0");
        assert_eq!(next.revision, 42);
        let second = range_page(45, &[b"c", b"d"], false, 4);
        assert_eq!(
            pager.observe_response(&second, 2).unwrap(),
            RangePageOutcome::Complete
        );
        assert_eq!(pager.result().last_key, b"d");
        assert_eq!(pager.result().selected, 4);
        assert_eq!(pager.result().discarded, 0);
        assert_eq!(pager.result().snapshot_revision, 42);
    }

    #[test]
    fn range_pager_rejects_missing_revision_and_empty_more_page() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 500,
            ..Default::default()
        };
        let mut pager = RangePager::new(request.clone(), RangeSelector::All { target: None });
        let missing_revision = range_page(0, &[b"a"], true, 2);
        assert!(pager.observe_response(&missing_revision, 1).is_err());

        let mut empty = RangePager::new(request, RangeSelector::All { target: None });
        let response = range_page(7, &[], true, 1);
        let error = empty.observe_response(&response, 0).unwrap_err();
        assert!(error.contains("more=true"));
    }

    #[test]
    fn range_pager_preserves_explicit_snapshot_with_advancing_headers() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            revision: 7,
            limit: 500,
            ..Default::default()
        };
        let mut pager = RangePager::new(request, RangeSelector::All { target: None });
        assert!(matches!(
            pager
                .observe_response(&range_page(9, &[b"a"], true, 2), 1)
                .unwrap(),
            RangePageOutcome::Continue(_)
        ));
        assert_eq!(pager.request().revision, 7);
        assert_eq!(pager.request().key, b"a\0");
        assert_eq!(
            pager
                .observe_response(&range_page(12, &[b"b"], false, 1), 1)
                .unwrap(),
            RangePageOutcome::Complete
        );
        assert_eq!(pager.result().snapshot_revision, 7);
        assert_eq!(pager.result().selected, 2);
    }

    #[test]
    fn range_pager_selector_doubles_limit_to_ten_thousand() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 10_000,
            ..Default::default()
        };
        let spec = crate::config::RangeSpec {
            key_pattern: "/registry/pods/".into(),
            kind: "prefix".into(),
            limit: 0,
            count_only: false,
            weight: 1.0,
            selector_keep_every: 2,
        };
        let selector = configured_range_selector(spec.selector_keep_every, spec.limit);
        let mut pager = RangePager::new(request, selector);
        for expected_limit in [500, 1_000, 2_000, 4_000, 8_000, 10_000] {
            assert_eq!(pager.request().limit, expected_limit);
            let key = [expected_limit as u8];
            let response = range_page(9, &[&key, b"keep"], expected_limit != 10_000, 12);
            let selected = selector.selected_in_page(response.kvs.len(), 0);
            let outcome = pager.observe_response(&response, selected).unwrap();
            if expected_limit != 10_000 {
                assert!(matches!(outcome, RangePageOutcome::Continue(_)));
            } else {
                assert_eq!(outcome, RangePageOutcome::Complete);
            }
        }
        assert_eq!(pager.result().discarded, 6);
    }

    #[test]
    fn range_pager_point_count_only_and_unbounded_prefix_are_single_request_shapes() {
        for request in [
            RangeRequest {
                key: b"/registry/pods/item".to_vec(),
                limit: 1,
                ..Default::default()
            },
            RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                count_only: true,
                limit: 0,
                ..Default::default()
            },
            RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                limit: 0,
                ..Default::default()
            },
        ] {
            let original = request.clone();
            let mut pager = RangePager::new(
                request,
                RangeSelector::DropEvery {
                    every: 2,
                    target: Some(100),
                },
            );
            assert_eq!(pager.request(), original);
            let response = range_page(3, &[b"a"], true, 1);
            assert_eq!(
                pager.observe_response(&response, 0).unwrap(),
                RangePageOutcome::Complete
            );
            assert_eq!(pager.result().pages, 1);
        }
    }

    #[test]
    fn masterlease_update_recreates_a_missing_key() {
        assert_eq!(
            storage_operation_for_state(
                ResourceOperation::GuaranteedUpdate,
                &KeyState::Absent,
                true,
            ),
            Some(ResourceOperation::Create)
        );
        assert_eq!(
            storage_operation_for_state(
                ResourceOperation::GuaranteedUpdate,
                &KeyState::Absent,
                false,
            ),
            None
        );
    }

    #[test]
    fn storage_attempt_budget_does_not_reset_between_retry_kinds() {
        let mut used = 0;
        for _ in 0..5 {
            assert!(take_storage_attempt(&mut used, 5));
        }
        assert!(!take_storage_attempt(&mut used, 5));
    }

    #[test]
    fn embedded_read_only_resources_are_created_before_ranges() {
        let workload =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        for name in ["services", "endpointslices"] {
            let resource = workload
                .resources
                .iter()
                .find(|resource| resource.name == name)
                .unwrap();
            assert_eq!(workload.resource_count(resource).unwrap(), 100);
            assert!(
                patterned_key(&resource.key_pattern, 0)
                    .starts_with(format!("/registry/{name}/bench-bench/").as_bytes())
            );
        }
    }

    #[test]
    fn try_enqueue_counts_closed_queue_as_drop() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let mut stats = ScheduleStats::default();
        let accounting = RunAccounting::new();
        assert!(!try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status),
            },
            &mut stats,
        ));
        assert_eq!(stats.dropped, 1);
        let report = accounting.report(RunTargets::default(), false, true, None);
        let status = report_class(&report, WorkClass::Status);
        assert_eq!(status.offered, 1);
        assert_eq!(status.accepted, 0);
        assert_eq!(status.dropped_closed, 1);
        assert_eq!(status.dropped_full, 0);
        assert_eq!(status.unissued, 0);
        assert!(!report.achieved);
        assert_report_equations(&report);
    }

    #[tokio::test]
    async fn drain_worker_tasks_propagates_worker_error() {
        let mut workers = JoinSet::new();
        workers.spawn(async { Err("worker failed".to_string()) });
        let (result, interrupted) = drain_worker_tasks(
            Vec::new(),
            workers,
            Arc::new(HashMap::new()),
            Duration::from_secs(1),
            &mut pending_ctrl_c(),
        )
        .await;
        assert!(result.unwrap_err().contains("worker failed"));
        assert!(!interrupted);
    }

    #[tokio::test]
    async fn scheduler_watch_supervision_reports_terminal_before_deadline() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            (
                "pods".to_string(),
                Ok(crate::watch::WatchOutcome {
                    terminal: crate::watch::WatchTerminal::Canceled {
                        reason: "server canceled".into(),
                    },
                    state: crate::watch::WatchState::default(),
                }),
            )
        });
        let mut ctrl_c: CtrlCFuture =
            Box::pin(std::future::pending::<Result<(), std::io::Error>>());

        let result = time::timeout(
            Duration::from_secs(1),
            wait_for_supervised_schedule_event(
                &mut tasks,
                &mut JoinSet::new(),
                &mut None,
                TokioInstant::now() + Duration::from_secs(60),
                &mut ctrl_c,
            ),
        )
        .await
        .unwrap();

        let Err(error) = result else {
            panic!("watch terminal should stop scheduling");
        };
        assert_eq!(
            error,
            "watch pods terminated: Canceled { reason: \"server canceled\" }"
        );
    }

    #[test]
    fn scheduler_watch_supervision_accepts_context_shutdown() {
        let outcome = crate::watch::WatchOutcome {
            terminal: crate::watch::WatchTerminal::ContextCanceled,
            state: crate::watch::WatchState::default(),
        };
        assert_eq!(watch_failure("pods", Ok(outcome)), None);
    }

    #[tokio::test]
    async fn watch_failure_aborts_actual_inflight_and_queued_workers_before_returning() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, mut workers, worker_lifetime) =
            spawn_owned_probe_worker(client, probe_workload());
        let (done, ack) = oneshot::channel();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut ScheduleStats::default()
        ));
        probe.wait_for_txns(1).await;
        assert!(try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status),
            },
            &mut ScheduleStats::default()
        ));

        let mut watches = JoinSet::new();
        watches.spawn(async {
            (
                "pods".to_string(),
                Err(crate::watch::WatchError::Rpc(tonic::Status::internal(
                    "server failed",
                ))),
            )
        });
        let mut ctrl_c = pending_ctrl_c();
        let result = time::timeout(
            Duration::from_secs(1),
            wait_for_supervised_schedule_event(
                &mut watches,
                &mut workers,
                &mut None,
                TokioInstant::now() + Duration::from_secs(60),
                &mut ctrl_c,
            ),
        )
        .await
        .unwrap();
        let Err(error) = result else {
            panic!("watch failure should stop scheduling");
        };
        assert!(error.contains("server failed"));
        time::timeout(
            Duration::from_secs(1),
            abort_worker_tasks(vec![sender], workers, Arc::new(HashMap::new())),
        )
        .await
        .unwrap();
        let (shutdown, _) = shutdown_watch::channel(false);
        let (stopped, interrupted) = stop_watches_with_ctrl(
            shutdown,
            watches,
            Duration::from_secs(1),
            &mut ctrl_c,
            false,
        )
        .await;
        stopped.unwrap();
        assert!(!interrupted);
        assert!(worker_lifetime.upgrade().is_none());
        assert!(ack.await.is_err());
        let _ = held_reply.send(Ok(compact_response(11, true, 0, b"")));
        let report = accounting.report(RunTargets::default(), false, false, Some(error));
        assert_eq!(report_class(&report, WorkClass::Bootstrap).unfinished, 1);
        assert_eq!(report_class(&report, WorkClass::Status).unfinished, 1);
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert!(!report.achieved);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    fn compact_response(
        header_revision: i64,
        succeeded: bool,
        remote_version: i64,
        remote_value: &[u8],
    ) -> TxnResponse {
        TxnResponse {
            header: Some(crate::pb::etcdserverpb::ResponseHeader {
                revision: header_revision,
                ..Default::default()
            }),
            succeeded,
            responses: if succeeded {
                Vec::new()
            } else {
                vec![ResponseOp {
                    response: Some(
                        crate::pb::etcdserverpb::response_op::Response::ResponseRange(
                            crate::pb::etcdserverpb::RangeResponse {
                                kvs: vec![mvccpb::KeyValue {
                                    version: remote_version,
                                    value: remote_value.to_vec(),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            },
                        ),
                    ),
                }]
            },
            ..Default::default()
        }
    }
    fn compact_response_without_kv(header_revision: i64) -> TxnResponse {
        TxnResponse {
            header: Some(crate::pb::etcdserverpb::ResponseHeader {
                revision: header_revision,
                ..Default::default()
            }),
            succeeded: false,
            responses: vec![ResponseOp {
                response: Some(
                    crate::pb::etcdserverpb::response_op::Response::ResponseRange(
                        crate::pb::etcdserverpb::RangeResponse::default(),
                    ),
                ),
            }],
            ..Default::default()
        }
    }

    #[test]
    fn compactor_first_tick_puts_zero_without_compacting() {
        let mut state = CompactorState::default();
        assert_eq!(state.txn_inputs(), (0, 0));
        let observation = state.observe(&compact_response(11, true, 0, b"")).unwrap();
        assert_eq!(observation.compact_target, None);
        assert_eq!(
            state,
            CompactorState {
                compact_version: 1,
                previous_txn_header_revision: 11,
                status: CompactorStatus::Active,
                recovery: None,
            }
        );
    }

    #[test]
    fn compactor_success_targets_previous_txn_header() {
        let mut state = CompactorState::default();
        state.observe(&compact_response(11, true, 0, b"")).unwrap();
        assert_eq!(state.txn_inputs(), (1, 11));
        let observation = state.observe(&compact_response(20, true, 0, b"")).unwrap();
        assert_eq!(observation.compact_target, Some(11));
        assert_eq!(state.txn_inputs(), (2, 20));
    }

    #[test]
    fn compactor_cas_failure_syncs_remote_state_without_retry() {
        let mut state = CompactorState {
            compact_version: 2,
            previous_txn_header_revision: 20,
            status: CompactorStatus::Active,
            recovery: None,
        };
        let observation = state
            .observe(&compact_response(30, false, 7, b"19"))
            .unwrap();
        assert_eq!(observation.compact_target, None);
        assert_eq!(observation.remote_compact_revision, Some(19));
        assert_eq!(state.txn_inputs(), (7, 30));
    }

    #[test]
    fn compactor_next_tick_after_failure_uses_own_header() {
        let mut state = CompactorState {
            compact_version: 2,
            previous_txn_header_revision: 20,
            status: CompactorStatus::Active,
            recovery: None,
        };
        state
            .observe(&compact_response(30, false, 7, b"19"))
            .unwrap();
        assert_eq!(state.txn_inputs(), (7, 30));
        let observation = state.observe(&compact_response(40, true, 0, b"")).unwrap();
        assert_eq!(observation.compact_target, Some(30));
        assert_eq!(state.txn_inputs(), (8, 40));
    }

    #[test]
    fn compactor_missing_failure_kv_requires_resync() {
        let mut state = CompactorState {
            compact_version: 7,
            previous_txn_header_revision: 20,
            status: CompactorStatus::Active,
            recovery: None,
        };
        assert_eq!(
            state.observe(&compact_response_without_kv(30)),
            Err(CompactorObservationError::FailureKvMissing)
        );
        assert_eq!(state.status, CompactorStatus::ResyncRequired);
        assert_eq!(state.recovery, Some(CompactorRecovery::MissingKey));
        assert_eq!(state.txn_inputs(), (0, 30));

        // The next tick is a single Version==0 CAS probe.  Its success
        // rebuilds the journal and cannot trigger a stale Compact.
        let recovery = state.observe(&compact_response(40, true, 0, b"")).unwrap();
        assert!(recovery.recovered);
        assert_eq!(recovery.compact_target, None);
        assert_eq!(state.status, CompactorStatus::Active);
        assert_eq!(state.recovery, None);
        assert_eq!(state.txn_inputs(), (1, 40));

        let next = state.observe(&compact_response(50, true, 0, b"")).unwrap();
        assert_eq!(next.compact_target, Some(40));
        assert_eq!(state.txn_inputs(), (2, 50));
    }

    #[test]
    fn compactor_malformed_value_stays_explicit_until_legal_remote_state() {
        let mut state = CompactorState {
            compact_version: 7,
            previous_txn_header_revision: 20,
            status: CompactorStatus::Active,
            recovery: None,
        };
        assert_eq!(
            state.observe(&compact_response(30, false, 7, b"not-a-revision")),
            Err(CompactorObservationError::FailureValueNotDecimal)
        );
        assert_eq!(state.status, CompactorStatus::ResyncRequired);
        assert_eq!(state.recovery, Some(CompactorRecovery::InvalidValue));
        // Every malformed tick coordinates again and remains explicit; it
        // never carries forward the old version/header into Compact.
        assert_eq!(
            state.observe(&compact_response(40, false, 8, b"\xff")),
            Err(CompactorObservationError::FailureValueNotUtf8)
        );
        assert_eq!(state.status, CompactorStatus::ResyncRequired);
        assert_eq!(state.txn_inputs(), (0, 40));

        // A legal remote value completes resync in the same failed Txn
        // response, still without a Compact on this tick.
        let recovery = state
            .observe(&compact_response(50, false, 9, b"19"))
            .unwrap();
        assert!(recovery.recovered);
        assert_eq!(recovery.compact_target, None);
        assert_eq!(recovery.remote_compact_revision, Some(19));
        assert_eq!(state.status, CompactorStatus::Active);
        assert_eq!(state.recovery, None);
        assert_eq!(state.txn_inputs(), (9, 50));

        let next = state.observe(&compact_response(60, true, 0, b"")).unwrap();
        assert_eq!(next.compact_target, Some(50));
        assert_eq!(state.txn_inputs(), (10, 60));
    }
    fn exact_phase_workload() -> WorkloadSpec {
        let mut workload =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        workload.topology.node_count = 2;
        workload.topology.pod_count = 4;
        workload
            .resources
            .retain(|resource| resource.name == "pods" || resource.name == "events");
        workload.relationships = crate::config::ResourceRelationshipsSpec::default();
        let pods = workload
            .resources
            .iter_mut()
            .find(|resource| resource.name == "pods")
            .unwrap();
        pods.count = ResourceCountSpec::Pods {
            numerator: 1,
            denominator: 1,
        };
        pods.update_amplification = 2;
        let events = workload
            .resources
            .iter_mut()
            .find(|resource| resource.name == "events")
            .unwrap();
        events.count = ResourceCountSpec::Pods {
            numerator: 1,
            denominator: 1,
        };
        events.update_amplification = 1;
        workload
    }

    #[test]
    fn workload_plan_has_exact_create_and_update_totals() {
        let workload = exact_phase_workload();
        let plan = WorkloadPlan::build(&workload).unwrap();
        assert_eq!(plan.total_creates(), 8);
        assert_eq!(plan.total_updates(), 12);
        let creates: Vec<_> = plan.creates().collect();
        assert_eq!(creates.len(), 8);
        assert_eq!(
            creates
                .iter()
                .filter(|operation| operation.resource == 0)
                .count(),
            4
        );
        assert_eq!(
            creates
                .iter()
                .filter(|operation| operation.resource == 1)
                .count(),
            4
        );
    }

    #[test]
    fn workload_plan_scales_to_two_thousand_nodes_and_forty_thousand_pods() {
        let mut workload = exact_phase_workload();
        workload.topology.node_count = 2_000;
        workload.topology.pod_count = 40_000;
        workload
            .resources
            .iter_mut()
            .find(|resource| resource.name == "pods")
            .unwrap()
            .update_amplification = 10;
        let plan = WorkloadPlan::build(&workload).unwrap();
        assert_eq!(plan.resources[0].count, 40_000);
        assert_eq!(plan.resources[1].count, 40_000);
        assert_eq!(plan.total_creates(), 80_000);
        assert_eq!(plan.total_updates(), 440_000);
        assert_eq!(plan.creates().count(), 80_000);
    }

    #[test]
    fn update_cursor_hits_every_key_exact_amplification_times() {
        let workload = exact_phase_workload();
        let plan = WorkloadPlan::build(&workload).unwrap();
        let mut counts = HashMap::new();
        for operation in UpdateCursor::new(&plan, 7) {
            *counts
                .entry((operation.resource, operation.id))
                .or_insert(0u64) += 1;
        }
        for id in 0..4 {
            assert_eq!(counts.get(&(0, id)), Some(&2));
            assert_eq!(counts.get(&(1, id)), Some(&1));
        }
        assert_eq!(counts.values().sum::<u64>(), 12);
    }

    #[test]
    fn phase_tracker_publishes_per_resource_targets_successes_and_deficits() {
        let workload = exact_phase_workload();
        let plan = WorkloadPlan::build(&workload).unwrap();
        let tracker = PhaseTracker::new(plan.create_targets(workload.resources.len()));
        for _ in 0..4 {
            tracker.record(0, Outcome::Success);
        }
        for _ in 0..3 {
            tracker.record(1, Outcome::Success);
        }
        tracker.record(1, Outcome::ResultUnknown);

        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        ::metrics::with_local_recorder(&recorder, || {
            tracker.publish(&workload, PhaseKind::Create);
        });
        let rendered = recorder.handle().render();
        let value = |resource: &str, state: &str| {
            rendered
                .lines()
                .find(|line| {
                    line.starts_with(metrics::RESOURCE_OBJECTS)
                        && line.contains(&format!("resource=\"{resource}\""))
                        && line.contains(&format!("state=\"{state}\""))
                })
                .and_then(|line| line.rsplit_once(' '))
                .and_then(|(_, value)| value.parse::<u64>().ok())
                .unwrap()
        };
        assert_eq!(value("pods", "planned"), 4);
        assert_eq!(value("pods", "succeeded"), 4);
        assert_eq!(value("events", "planned"), 4);
        assert_eq!(value("events", "succeeded"), 3);
        assert_eq!(value("events", "deficit"), 1);
    }

    #[test]
    fn phase_result_rejects_any_success_deficit() {
        let tracker = PhaseTracker::new(vec![3]);
        tracker.record(0, Outcome::Success);
        tracker.record(0, Outcome::ResultUnknown);
        tracker.record(0, Outcome::Success);
        let error = tracker.result("create").unwrap_err();
        assert!(error.contains("target=3"));
        assert!(error.contains("succeeded=2"));
        assert!(error.contains("deficit=1"));
    }
    #[test]
    fn run_accounting_reports_all_terminal_buckets_and_equations() {
        let accounting = RunAccounting::new();
        let mut success = tracked(&accounting, WorkClass::Create);
        success.mark_accepted();
        success.mark_complete(Outcome::Success);

        let mut unknown = tracked(&accounting, WorkClass::Create);
        unknown.mark_accepted();
        unknown.mark_complete(Outcome::ResultUnknown);

        let mut dropped = tracked(&accounting, WorkClass::Create);
        dropped.mark_dropped(DropReason::Backpressure);

        let mut unissued = tracked(&accounting, WorkClass::Create);
        unissued.mark_unissued();

        {
            let mut unfinished = tracked(&accounting, WorkClass::Create);
            unfinished.mark_accepted();
        }

        let report = accounting.report(RunTargets::default(), false, true, None);
        let create = report
            .work_items
            .iter()
            .find(|item| item.work_class == "create")
            .unwrap();
        assert_eq!(create.offered, 5);
        assert_eq!(create.accepted, 3);
        assert_eq!(create.completed, 2);
        assert_eq!(create.succeeded, 1);
        assert_eq!(create.failed, 1);
        assert_eq!(create.result_unknown, 1);
        assert_eq!(create.dropped, 1);
        assert_eq!(create.dropped_full, 1);
        assert_eq!(create.unfinished, 1);
        assert_eq!(create.unissued, 1);
        assert!(
            report
                .accounting_invariants
                .offered_equals_accepted_plus_dropped_plus_unissued
        );
        assert!(
            report
                .accounting_invariants
                .accepted_equals_completed_plus_unfinished
        );
        assert!(
            report
                .accounting_invariants
                .accepted_equals_succeeded_plus_failed_plus_unfinished
        );
        assert!(report.accounting_invariants.result_unknown_subset_failed);
        assert!(!report.achieved);
    }

    #[tokio::test]
    async fn blocking_enqueue_timeout_marks_item_unissued() {
        let accounting = RunAccounting::new();
        let (sender, receiver) = mpsc::channel(1);
        let mut stats = ScheduleStats::default();
        assert!(try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status)
            },
            &mut stats,
        ));

        let second = WorkItem::Status {
            accounting: tracked(&accounting, WorkClass::Status),
        };
        let result = enqueue_until(
            &sender,
            second,
            TokioInstant::now() + Duration::from_millis(1),
        )
        .await;
        assert!(matches!(result, EnqueueResult::Unissued));
        drop(receiver);

        let report = accounting.report(RunTargets::default(), true, false, None);
        let status = report
            .work_items
            .iter()
            .find(|item| item.work_class == "status")
            .unwrap();
        assert_eq!(status.offered, 2);
        assert_eq!(status.accepted, 1);
        assert_eq!(status.unfinished, 1);
        assert_eq!(status.unissued, 1);
        assert_report_equations(&report);
    }

    #[tokio::test]
    async fn bootstrap_worker_preserves_unknown_rpc_result_and_error_ack() {
        let probe = CreatePhaseProbe::default();
        probe
            .next_txn_reply()
            .send(Err(tonic::Status::deadline_exceeded("response lost")))
            .unwrap();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, worker) = spawn_probe_worker(client, probe_workload());
        let (done, ack) = oneshot::channel();
        let mut stats = ScheduleStats::default();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut stats,
        ));
        drop(sender);
        let error = time::timeout(Duration::from_secs(3), ack)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.contains("result_unknown"));
        time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let report = accounting.report(RunTargets::default(), false, true, None);
        let bootstrap = report_class(&report, WorkClass::Bootstrap);
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert_eq!(bootstrap.offered, 1);
        assert_eq!(bootstrap.accepted, 1);
        assert_eq!(bootstrap.completed, 1);
        assert_eq!(bootstrap.succeeded, 0);
        assert_eq!(bootstrap.failed, 1);
        assert_eq!(bootstrap.result_unknown, 1);
        assert_eq!(bootstrap.unfinished, 0);
        assert!(!report.achieved);
        assert_report_equations(&report);
        let untouched_run = RunAccounting::new().report(RunTargets::default(), false, true, None);
        assert!(
            untouched_run
                .work_items
                .iter()
                .all(|item| item.offered == 0)
        );
        assert_report_equations(&untouched_run);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn heartbeat_worker_preserves_unknown_rpc_result_after_bootstrap() {
        let probe = CreatePhaseProbe::default();
        probe
            .next_txn_reply()
            .send(Ok(compact_response(11, true, 0, b"")))
            .unwrap();
        probe
            .next_txn_reply()
            .send(Err(tonic::Status::unavailable("committed response lost")))
            .unwrap();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, worker) = spawn_probe_worker(client, probe_workload());
        let (done, ack) = oneshot::channel();
        let mut stats = ScheduleStats::default();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut stats,
        ));
        time::timeout(Duration::from_secs(3), ack)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(try_enqueue(
            &sender,
            WorkItem::Heartbeat {
                item: probe_heartbeat(HeartbeatItemKind::NodeStatus),
                accounting: tracked(&accounting, WorkClass::Heartbeat),
            },
            &mut stats,
        ));
        drop(sender);
        time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let report = accounting.report(RunTargets::default(), false, true, None);
        let heartbeat = report_class(&report, WorkClass::Heartbeat);
        let requests = probe.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            2,
            "periodic heartbeat must reach the RPC, without replay"
        );
        assert!(matches!(
            requests[1].compare[0].target_union,
            Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                11
            ))
        ));
        drop(requests);
        assert_eq!(report_class(&report, WorkClass::Bootstrap).succeeded, 1);
        assert_eq!(heartbeat.offered, 1);
        assert_eq!(heartbeat.accepted, 1);
        assert_eq!(heartbeat.completed, 1);
        assert_eq!(heartbeat.succeeded, 0);
        assert_eq!(heartbeat.failed, 1);
        assert_eq!(heartbeat.result_unknown, 1);
        assert_eq!(heartbeat.unfinished, 0);
        assert!(!report.achieved);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    async fn run_compact_probe(responses: Vec<TxnResponse>) -> (RunReport, CreatePhaseProbe) {
        let probe = CreatePhaseProbe::default();
        let ticks = responses.len();
        for response in responses {
            probe.next_txn_reply().send(Ok(response)).unwrap();
        }
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, worker) = spawn_probe_worker(client, probe_workload());
        let mut stats = ScheduleStats::default();
        for _ in 0..ticks {
            assert!(try_enqueue(
                &sender,
                WorkItem::CompactTick {
                    accounting: tracked(&accounting, WorkClass::Compact)
                },
                &mut stats,
            ));
        }
        drop(sender);
        time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let report = accounting.report(RunTargets::default(), false, true, None);
        assert_report_equations(&report);
        let compact = report_class(&report, WorkClass::Compact);
        assert_eq!(compact.offered, ticks as u64);
        assert_eq!(compact.accepted, ticks as u64);
        assert_eq!(compact.completed, ticks as u64);
        assert_eq!(compact.unfinished, 0);
        server.abort();
        let _ = server.await;
        (report, probe)
    }

    #[tokio::test]
    async fn compact_worker_first_journal_is_one_successful_noop_tick() {
        let (report, probe) = run_compact_probe(vec![compact_response(11, true, 0, b"")]).await;
        assert_eq!(
            *probe.requests.lock().unwrap(),
            vec![build_compact_revision_txn(b"0", 0)]
        );
        assert!(probe.compact_requests.lock().unwrap().is_empty());
        let compact = report_class(&report, WorkClass::Compact);
        assert_eq!(compact.succeeded, 1);
        assert_eq!(compact.failed, 0);
        assert_eq!(compact.result_unknown, 0);
        assert!(report.achieved);
    }

    #[tokio::test]
    async fn compact_worker_legal_cas_resync_succeeds_without_compact_or_retry() {
        let (report, probe) = run_compact_probe(vec![
            compact_response(11, true, 0, b""),
            compact_response(22, false, 9, b"7"),
        ])
        .await;
        assert_eq!(
            *probe.requests.lock().unwrap(),
            vec![
                build_compact_revision_txn(b"0", 0),
                build_compact_revision_txn(b"11", 1),
            ]
        );
        assert!(probe.compact_requests.lock().unwrap().is_empty());
        let compact = report_class(&report, WorkClass::Compact);
        assert_eq!(compact.succeeded, 2);
        assert_eq!(compact.failed, 0);
        assert_eq!(compact.result_unknown, 0);
        assert!(report.achieved);
    }

    #[tokio::test]
    async fn compact_worker_rpc_failure_is_not_hidden_by_successful_journal() {
        let (report, probe) = run_compact_probe(vec![
            compact_response(11, true, 0, b""),
            compact_response(22, true, 0, b""),
        ])
        .await;
        assert_eq!(
            *probe.requests.lock().unwrap(),
            vec![
                build_compact_revision_txn(b"0", 0),
                build_compact_revision_txn(b"11", 1),
            ]
        );
        assert_eq!(
            *probe.compact_requests.lock().unwrap(),
            vec![CompactionRequest {
                revision: 11,
                physical: false,
            }]
        );
        let compact = report_class(&report, WorkClass::Compact);
        assert_eq!(compact.succeeded, 1);
        assert_eq!(compact.failed, 1);
        assert_eq!(compact.result_unknown, 0);
        assert!(compact.first_error.is_some());
        assert!(!report.achieved);
    }

    #[tokio::test]
    async fn range_worker_rejects_invalid_page_despite_successful_rpc() {
        let probe = CreatePhaseProbe::default();
        *probe.range_reply.lock().unwrap() = Some(range_page(7, &[], true, 1));
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, worker) = spawn_probe_worker(client, probe_workload());
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 500,
            ..Default::default()
        };
        assert!(try_enqueue(
            &sender,
            WorkItem::Range {
                request: request.clone(),
                selector_keep_every: 1,
                target: 0,
                accounting: tracked(&accounting, WorkClass::Range),
            },
            &mut ScheduleStats::default(),
        ));
        drop(sender);
        time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let report = accounting.report(RunTargets::default(), false, true, None);
        assert_eq!(*probe.range_requests.lock().unwrap(), vec![request]);
        let range = report_class(&report, WorkClass::Range);
        assert_eq!(range.accepted, 1);
        assert_eq!(range.completed, 1);
        assert_eq!(range.succeeded, 0);
        assert_eq!(range.failed, 1);
        assert_eq!(range.result_unknown, 0);
        assert_eq!(range.unfinished, 0);
        assert!(range.first_error.as_ref().unwrap().contains("more=true"));
        assert!(!report.achieved);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn try_enqueue_full_keeps_rejected_item_out_of_accepted_bucket() {
        let accounting = RunAccounting::new();
        let (sender, receiver) = mpsc::channel(1);
        let mut stats = ScheduleStats::default();
        assert!(try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status)
            },
            &mut stats,
        ));
        assert!(!try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status)
            },
            &mut stats,
        ));
        drop(receiver);
        let report = accounting.report(RunTargets::default(), false, true, None);
        let status = report_class(&report, WorkClass::Status);
        assert_eq!(stats.dropped, 1);
        assert_eq!(status.offered, 2);
        assert_eq!(status.accepted, 1);
        assert_eq!(status.dropped_full, 1);
        assert_eq!(status.dropped_closed, 0);
        assert_eq!(status.unfinished, 1);
        assert_eq!(status.unissued, 0);
        assert!(!report.achieved);
        assert_report_equations(&report);
    }

    #[tokio::test]
    async fn canceled_pending_reserve_is_unissued_not_accepted_or_dropped() {
        let accounting = RunAccounting::new();
        let (sender, receiver) = mpsc::channel(1);
        assert!(try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status)
            },
            &mut ScheduleStats::default(),
        ));
        let mut enqueue = Box::pin(enqueue_until(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status),
            },
            TokioInstant::now() + Duration::from_secs(60),
        ));
        assert_pending(enqueue.as_mut()).await;
        drop(enqueue);
        drop(receiver);

        let report = accounting.report(RunTargets::default(), true, false, None);
        let status = report_class(&report, WorkClass::Status);
        assert_eq!(status.offered, 2);
        assert_eq!(status.accepted, 1);
        assert_eq!(status.completed, 0);
        assert_eq!(status.dropped, 0);
        assert_eq!(status.unfinished, 1);
        assert_eq!(status.unissued, 1);
        assert_report_equations(&report);
    }

    #[tokio::test]
    async fn aborted_worker_preserves_inflight_and_queued_items_as_unfinished() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, workers, worker_lifetime) = spawn_owned_probe_worker(client, probe_workload());
        let (first_done, first_ack) = oneshot::channel();
        let (second_done, second_ack) = oneshot::channel();
        let mut stats = ScheduleStats::default();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done: first_done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut stats,
        ));
        probe.wait_for_txns(1).await;
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeReady),
                done: second_done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut stats,
        ));
        time::timeout(
            Duration::from_secs(3),
            abort_worker_tasks(vec![sender], workers, Arc::new(HashMap::new())),
        )
        .await
        .unwrap();
        assert!(worker_lifetime.upgrade().is_none());
        assert!(first_ack.await.is_err());
        assert!(second_ack.await.is_err());
        let _ = held_reply.send(Ok(compact_response(11, true, 0, b"")));

        let report = accounting.report(RunTargets::default(), true, false, None);
        let bootstrap = report_class(&report, WorkClass::Bootstrap);
        assert_eq!(
            probe.requests.lock().unwrap().len(),
            1,
            "queued work must not reach RPC"
        );
        assert_eq!(bootstrap.offered, 2);
        assert_eq!(bootstrap.accepted, 2);
        assert_eq!(bootstrap.completed, 0);
        assert_eq!(bootstrap.succeeded, 0);
        assert_eq!(bootstrap.failed, 0);
        assert_eq!(bootstrap.result_unknown, 0);
        assert_eq!(bootstrap.unfinished, 2);
        assert_eq!(bootstrap.unissued, 0);
        assert!(!report.achieved);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn readiness_barrier_requires_every_created_before_update_rpc() {
        let probe = CreatePhaseProbe::default();
        let (client, server) = start_create_probe(probe.clone()).await;
        let workload = probe_workload();
        let plan = WorkloadPlan::build(&workload).unwrap();
        let accounting = RunAccounting::new();
        let (sender, workers, worker_lifetime) = spawn_owned_probe_worker(client, workload.clone());
        run_create_phase(
            &workload,
            &plan,
            std::slice::from_ref(&sender),
            1,
            Duration::from_secs(3),
            &accounting,
        )
        .await
        .unwrap();
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        let (first_created, first_ready) = oneshot::channel();
        let (last_created, last_ready) = oneshot::channel();
        let mut watches = JoinSet::new();
        for _ in 0..2 {
            watches.spawn(std::future::pending::<WatchTaskOutput>());
        }
        let mut ctrl_c = pending_ctrl_c();
        let update_started = std::cell::Cell::new(false);
        let mut gated_update = Box::pin(async {
            wait_for_watch_ready(
                &mut watches,
                vec![first_ready, last_ready],
                Duration::from_secs(3),
                &mut ctrl_c,
            )
            .await?;
            update_started.set(true);
            run_update_phase(
                workload,
                plan,
                vec![sender.clone()],
                1,
                7,
                TokioInstant::now(),
                Duration::from_secs(3),
                Arc::clone(&accounting),
            )
            .await
        });
        assert_pending(gated_update.as_mut()).await;
        assert!(!update_started.get());
        first_created.send(()).unwrap();
        assert_pending(gated_update.as_mut()).await;
        assert!(
            !update_started.get(),
            "one Created must not release a two-Watch barrier"
        );
        assert_eq!(
            probe.requests.lock().unwrap().len(),
            1,
            "no update RPC before final Created"
        );
        last_created.send(()).unwrap();
        assert_eq!(
            time::timeout(Duration::from_secs(3), gated_update)
                .await
                .unwrap()
                .unwrap(),
            1
        );
        assert!(update_started.get());
        assert_eq!(probe.requests.lock().unwrap().len(), 2);
        assert!(matches!(
            probe.requests.lock().unwrap()[1].compare[0].target_union,
            Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                1
            ))
        ));
        let (drained, interrupted) = drain_worker_tasks(
            vec![sender],
            workers,
            Arc::new(HashMap::new()),
            Duration::from_secs(1),
            &mut ctrl_c,
        )
        .await;
        drained.unwrap();
        assert!(!interrupted);
        assert!(worker_lifetime.upgrade().is_none());
        watches.abort_all();
        while watches.join_next().await.is_some() {}
        let report = accounting.report(RunTargets::default(), false, true, None);
        assert_eq!(report_class(&report, WorkClass::Update).succeeded, 1);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn readiness_closed_channel_preserves_delayed_watch_terminal_cause() {
        let (created, ready) = oneshot::channel();
        let (terminal, terminate) = oneshot::channel();
        let mut watches = JoinSet::new();
        watches.spawn(async move {
            terminate.await.unwrap();
            (
                "pods".to_string(),
                Ok(crate::watch::WatchOutcome {
                    terminal: crate::watch::WatchTerminal::Canceled {
                        reason: "authorization revoked".into(),
                    },
                    state: crate::watch::WatchState::default(),
                }),
            )
        });
        drop(created);
        let mut ctrl_c = pending_ctrl_c();
        let mut readiness = Box::pin(wait_for_watch_ready(
            &mut watches,
            vec![ready],
            Duration::from_secs(3),
            &mut ctrl_c,
        ));
        assert_pending(readiness.as_mut()).await;
        terminal.send(()).unwrap();
        let error = time::timeout(Duration::from_secs(1), readiness)
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.contains("watch pods terminated"));
        assert!(error.contains("authorization revoked"));
        assert!(watches.is_empty());
        let report = RunAccounting::new().report(RunTargets::default(), false, false, Some(error));
        assert!(!report.achieved);
        assert!(
            report
                .termination_error
                .unwrap()
                .contains("authorization revoked")
        );
    }

    #[tokio::test]
    async fn readiness_timeout_does_not_release_update_phase() {
        let (_created, ready) = oneshot::channel();
        let mut watches = JoinSet::new();
        watches.spawn(std::future::pending::<WatchTaskOutput>());
        let mut ctrl_c = pending_ctrl_c();
        let error = time::timeout(
            Duration::from_secs(1),
            wait_for_watch_ready(
                &mut watches,
                vec![ready],
                Duration::from_millis(1),
                &mut ctrl_c,
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error, "watch readiness timed out");
        watches.abort_all();
        while watches.join_next().await.is_some() {}
        let report = RunAccounting::new().report(RunTargets::default(), false, false, Some(error));
        assert!(!report.achieved);
        assert_eq!(
            report.termination_error.as_deref(),
            Some("watch readiness timed out")
        );
    }

    #[tokio::test]
    async fn bootstrap_blocked_rpc_prevents_application_offers_until_cancellation() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let (cancel, ctrl_c) = controlled_ctrl_c();
        let run = tokio::spawn(run_with_ctrl_c(
            probe_config(probe_workload()),
            client,
            ctrl_c,
        ));
        probe.wait_for_txns(1).await;
        cancel.send(()).unwrap();
        let report = time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap();
        let _ = held_reply.send(Ok(compact_response(11, true, 0, b"")));
        assert!(report.interrupted);
        assert!(!report.achieved);
        assert!(!report.schedule_complete);
        assert_eq!(report_class(&report, WorkClass::Create).offered, 0);
        assert_eq!(report_class(&report, WorkClass::Update).offered, 0);
        let bootstrap = report_class(&report, WorkClass::Bootstrap);
        assert_eq!(bootstrap.accepted, 2);
        assert_eq!(bootstrap.unfinished, 2);
        assert_eq!(bootstrap.completed, 0);
        assert_eq!(bootstrap.result_unknown, 0);
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn bootstrap_error_ack_stops_before_application_create_or_update() {
        let probe = CreatePhaseProbe::default();
        probe
            .next_txn_reply()
            .send(Err(tonic::Status::permission_denied("bootstrap rejected")))
            .unwrap();
        let (client, server) = start_create_probe(probe.clone()).await;
        let report = time::timeout(
            Duration::from_secs(1),
            run_with_ctrl_c(probe_config(probe_workload()), client, pending_ctrl_c()),
        )
        .await
        .unwrap();
        assert!(!report.achieved);
        assert!(!report.interrupted);
        assert!(!report.schedule_complete);
        assert!(
            report
                .termination_error
                .as_ref()
                .unwrap()
                .starts_with("bootstrap:")
        );
        assert_eq!(report_class(&report, WorkClass::Create).offered, 0);
        assert_eq!(report_class(&report, WorkClass::Update).offered, 0);
        assert!(report_class(&report, WorkClass::Bootstrap).failed >= 1);
        assert_eq!(
            report_class(&report, WorkClass::Bootstrap).result_unknown,
            0
        );
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn nonempty_drain_cancellation_joins_owned_tasks_before_partial_report() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, workers, worker_lifetime) = spawn_owned_probe_worker(client, probe_workload());
        let (done, ack) = oneshot::channel();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut ScheduleStats::default()
        ));
        probe.wait_for_txns(1).await;
        assert!(try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status),
            },
            &mut ScheduleStats::default()
        ));
        let (shutdown, mut watch_shutdown) = shutdown_watch::channel(false);
        let watch_guard = Arc::new(());
        let watch_lifetime = Arc::downgrade(&watch_guard);
        let mut watches = JoinSet::new();
        watches.spawn(async move {
            let _guard = watch_guard;
            watch_shutdown.changed().await.unwrap();
            (
                "pods".into(),
                Ok(crate::watch::WatchOutcome {
                    terminal: crate::watch::WatchTerminal::ContextCanceled,
                    state: crate::watch::WatchState::default(),
                }),
            )
        });
        let (cancel, mut ctrl_c) = controlled_ctrl_c();
        let mut draining = Box::pin(drain_worker_tasks(
            vec![sender],
            workers,
            Arc::new(HashMap::new()),
            Duration::from_secs(60),
            &mut ctrl_c,
        ));
        assert_pending(draining.as_mut()).await;
        assert!(worker_lifetime.upgrade().is_some());
        assert!(watch_lifetime.upgrade().is_some());
        cancel.send(()).unwrap();
        let (drain_result, interrupted) = time::timeout(Duration::from_secs(1), draining)
            .await
            .unwrap();
        drain_result.unwrap();
        assert!(interrupted);
        assert!(worker_lifetime.upgrade().is_none());
        assert!(ack.await.is_err());
        let (stop_result, interrupted) = time::timeout(
            Duration::from_secs(1),
            stop_watches_with_ctrl(
                shutdown,
                watches,
                Duration::from_secs(1),
                &mut ctrl_c,
                interrupted,
            ),
        )
        .await
        .unwrap();
        stop_result.unwrap();
        assert!(
            interrupted,
            "completed cancellation future must remain latched through Watch stop"
        );
        assert!(watch_lifetime.upgrade().is_none());
        let _ = held_reply.send(Ok(compact_response(11, true, 0, b"")));
        let report = accounting.report(RunTargets::default(), interrupted, true, None);
        assert!(report.interrupted);
        assert!(report.schedule_complete);
        assert!(!report.achieved);
        for class in [WorkClass::Bootstrap, WorkClass::Status] {
            let item = report_class(&report, class);
            assert_eq!(item.accepted, 1);
            assert_eq!(item.completed, 0);
            assert_eq!(item.failed, 0);
            assert_eq!(item.result_unknown, 0);
            assert_eq!(item.unfinished, 1);
        }
        assert_report_equations(&report);
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn late_watch_terminal_during_normal_drain_remains_a_run_failure() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, workers, worker_lifetime) = spawn_owned_probe_worker(client, probe_workload());
        let (done, ack) = oneshot::channel();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut ScheduleStats::default()
        ));
        probe.wait_for_txns(1).await;
        let (shutdown, shutdown_observer) = shutdown_watch::channel(false);
        let (terminal, terminate) = oneshot::channel();
        let (observed, observation) = oneshot::channel();
        let guard = Arc::new(());
        let watch_lifetime = Arc::downgrade(&guard);
        let mut watches = JoinSet::new();
        watches.spawn(async move {
            let _guard = guard;
            terminate.await.unwrap();
            observed.send(()).unwrap();
            (
                "pods".into(),
                Ok(crate::watch::WatchOutcome {
                    terminal: crate::watch::WatchTerminal::Canceled {
                        reason: "late server cancellation".into(),
                    },
                    state: crate::watch::WatchState::default(),
                }),
            )
        });
        let mut ctrl_c = pending_ctrl_c();
        let mut draining = Box::pin(drain_worker_tasks(
            vec![sender],
            workers,
            Arc::new(HashMap::new()),
            Duration::from_secs(3),
            &mut ctrl_c,
        ));
        assert_pending(draining.as_mut()).await;
        assert!(
            !*shutdown_observer.borrow(),
            "Watch must remain live while worker RPC is held"
        );
        terminal.send(()).unwrap();
        time::timeout(Duration::from_secs(1), observation)
            .await
            .unwrap()
            .unwrap();
        assert_pending(draining.as_mut()).await;
        held_reply
            .send(Ok(compact_response(11, true, 0, b"")))
            .unwrap();
        let (drained, interrupted) = time::timeout(Duration::from_secs(1), draining)
            .await
            .unwrap();
        drained.unwrap();
        assert!(!interrupted);
        ack.await.unwrap().unwrap();
        assert!(worker_lifetime.upgrade().is_none());
        let (stopped, interrupted) = stop_watches_with_ctrl(
            shutdown,
            watches,
            Duration::from_secs(1),
            &mut ctrl_c,
            false,
        )
        .await;
        let error = stopped.unwrap_err();
        assert!(error.contains("late server cancellation"));
        assert!(!interrupted);
        assert!(watch_lifetime.upgrade().is_none());
        let report = accounting.report(RunTargets::default(), false, true, Some(error));
        assert_eq!(report_class(&report, WorkClass::Bootstrap).succeeded, 1);
        assert!(!report.achieved);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn watch_stop_timeout_aborts_and_joins_before_failure_report() {
        let (shutdown, observer) = shutdown_watch::channel(false);
        let guard = Arc::new(());
        let lifetime = Arc::downgrade(&guard);
        let mut watches = JoinSet::new();
        watches.spawn(async move {
            let _guard = guard;
            std::future::pending::<WatchTaskOutput>().await
        });
        let (stopped, interrupted) = time::timeout(
            Duration::from_secs(1),
            stop_watches_with_ctrl(
                shutdown,
                watches,
                Duration::from_millis(1),
                &mut pending_ctrl_c(),
                false,
            ),
        )
        .await
        .unwrap();
        assert!(*observer.borrow());
        assert!(
            lifetime.upgrade().is_none(),
            "Watch task must be dropped before reporting timeout"
        );
        assert!(!interrupted);
        let error = stopped.unwrap_err();
        assert!(error.contains("watch shutdown timed out"));
        let report = RunAccounting::new().report(RunTargets::default(), false, true, Some(error));
        assert!(!report.achieved);
        assert!(
            report
                .termination_error
                .unwrap()
                .contains("watch shutdown timed out")
        );
    }

    #[tokio::test]
    async fn successful_update_completion_does_not_discard_pending_periodic_event() {
        let probe = CreatePhaseProbe::default();
        let (client, server) = start_create_probe(probe.clone()).await;
        let mut workload = probe_workload();
        workload.compact_interval_s = 1.0;
        let report = time::timeout(
            Duration::from_secs(3),
            run_with_ctrl_c(probe_config(workload), client, pending_ctrl_c()),
        )
        .await
        .unwrap();
        assert_eq!(report_class(&report, WorkClass::Update).succeeded, 1);
        assert_eq!(report_class(&report, WorkClass::Compact).accepted, 1);
        assert_eq!(report_class(&report, WorkClass::Compact).succeeded, 1);
        assert_eq!(probe.requests.lock().unwrap().len(), 5);
        assert!(probe.compact_requests.lock().unwrap().is_empty());
        assert!(report.schedule_complete);
        assert!(report.achieved, "{:?}", report.termination_error);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn early_worker_panic_is_supervised_before_long_timeline_and_aborts_owned_work() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let (sender, mut workers, worker_lifetime) =
            spawn_owned_probe_worker(client, probe_workload());
        let (done, ack) = oneshot::channel();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut ScheduleStats::default()
        ));
        probe.wait_for_txns(1).await;
        assert!(try_enqueue(
            &sender,
            WorkItem::Status {
                accounting: tracked(&accounting, WorkClass::Status),
            },
            &mut ScheduleStats::default()
        ));
        workers.spawn(async {
            panic!("injected worker panic");
        });
        let producer_guard = Arc::new(());
        let producer_lifetime = Arc::downgrade(&producer_guard);
        let mut update = Some(tokio::spawn(async move {
            let _guard = producer_guard;
            std::future::pending::<Result<u64, String>>().await
        }));
        let mut watches = JoinSet::new();
        let result = time::timeout(
            Duration::from_secs(1),
            wait_for_supervised_schedule_event(
                &mut watches,
                &mut workers,
                &mut update,
                TokioInstant::now() + Duration::from_secs(60),
                &mut pending_ctrl_c(),
            ),
        )
        .await
        .unwrap();
        let Err(error) = result else {
            panic!("fatal worker panic must preempt timeline");
        };
        assert!(error.contains("worker task failed"));
        assert!(error.contains("injected worker panic"));
        let producer = update.take().unwrap();
        producer.abort();
        assert!(producer.await.unwrap_err().is_cancelled());
        time::timeout(
            Duration::from_secs(1),
            abort_worker_tasks(vec![sender], workers, Arc::new(HashMap::new())),
        )
        .await
        .unwrap();
        assert!(producer_lifetime.upgrade().is_none());
        assert!(worker_lifetime.upgrade().is_none());
        assert!(ack.await.is_err());
        let _ = held_reply.send(Ok(compact_response(11, true, 0, b"")));
        let report = accounting.report(RunTargets::default(), false, false, Some(error));
        assert_eq!(report_class(&report, WorkClass::Bootstrap).unfinished, 1);
        assert_eq!(report_class(&report, WorkClass::Status).unfinished, 1);
        assert_eq!(report_class(&report, WorkClass::Status).completed, 0);
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert!(!report.achieved);
        assert!(!report.schedule_complete);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn early_update_producer_queue_failure_is_supervised_before_long_timeline() {
        let probe = CreatePhaseProbe::default();
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let accounting = RunAccounting::new();
        let workload = probe_workload();
        let plan = WorkloadPlan::build(&workload).unwrap();
        let (sender, mut workers, worker_lifetime) =
            spawn_owned_probe_worker(client, workload.clone());
        let (done, ack) = oneshot::channel();
        assert!(try_enqueue(
            &sender,
            WorkItem::Bootstrap {
                item: probe_heartbeat(HeartbeatItemKind::NodeCreate),
                done,
                accounting: tracked(&accounting, WorkClass::Bootstrap),
            },
            &mut ScheduleStats::default()
        ));
        probe.wait_for_txns(1).await;
        let (closed_sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let producer_guard = Arc::new(());
        let producer_lifetime = Arc::downgrade(&producer_guard);
        let producer_accounting = Arc::clone(&accounting);
        let mut update = Some(tokio::spawn(async move {
            let _guard = producer_guard;
            run_update_phase(
                workload,
                plan,
                vec![closed_sender],
                1,
                7,
                TokioInstant::now(),
                Duration::from_secs(60),
                producer_accounting,
            )
            .await
        }));
        let mut watches = JoinSet::new();
        let result = time::timeout(
            Duration::from_secs(1),
            wait_for_supervised_schedule_event(
                &mut watches,
                &mut workers,
                &mut update,
                TokioInstant::now() + Duration::from_secs(60),
                &mut pending_ctrl_c(),
            ),
        )
        .await
        .unwrap();
        let Ok(SupervisedScheduleControl::UpdateFinished(Err(error))) = result else {
            panic!("actual update producer failure must preempt the long timeline");
        };
        update.take();
        assert!(error.contains("update phase worker queue closed"));
        time::timeout(
            Duration::from_secs(1),
            abort_worker_tasks(vec![sender], workers, Arc::new(HashMap::new())),
        )
        .await
        .unwrap();
        assert!(producer_lifetime.upgrade().is_none());
        assert!(worker_lifetime.upgrade().is_none());
        assert!(ack.await.is_err());
        let _ = held_reply.send(Ok(compact_response(11, true, 0, b"")));
        let report = accounting.report(RunTargets::default(), false, false, Some(error));
        let update = report_class(&report, WorkClass::Update);
        assert_eq!(update.offered, 1);
        assert_eq!(update.accepted, 0);
        assert_eq!(update.dropped_closed, 1);
        assert_eq!(update.unissued, 0);
        assert_eq!(report_class(&report, WorkClass::Bootstrap).unfinished, 1);
        assert_eq!(probe.requests.lock().unwrap().len(), 1);
        assert!(!report.achieved);
        assert!(!report.schedule_complete);
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn run_cancellation_during_final_compact_drain_returns_interrupted_partial_report() {
        let probe = CreatePhaseProbe::default();
        for revision in 1..=3 {
            probe
                .next_txn_reply()
                .send(Ok(compact_response(revision, true, 0, b"")))
                .unwrap();
        }
        let held_reply = probe.next_txn_reply();
        let (client, server) = start_create_probe(probe.clone()).await;
        let mut workload = probe_workload();
        workload.resources[0].update_amplification = 0;
        workload.timeline[0].duration_s = 0.05;
        workload.compact_interval_s = 0.05;
        let (cancel, ctrl_c) = controlled_ctrl_c();
        let run = tokio::spawn(run_with_ctrl_c(probe_config(workload), client, ctrl_c));
        probe.wait_for_txns(4).await;
        cancel.send(()).unwrap();
        let report = time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap();
        let _ = held_reply.send(Ok(compact_response(4, true, 0, b"")));
        assert!(
            report.schedule_complete,
            "cancellation must exercise post-schedule drain"
        );
        assert!(report.interrupted);
        assert!(!report.achieved);
        assert!(report.termination_error.is_none());
        assert_eq!(report_class(&report, WorkClass::Bootstrap).succeeded, 2);
        assert_eq!(report_class(&report, WorkClass::Create).succeeded, 1);
        let compact = report_class(&report, WorkClass::Compact);
        assert_eq!(compact.accepted, 1);
        assert_eq!(compact.completed, 0);
        assert_eq!(compact.unfinished, 1);
        assert_eq!(compact.failed, 0);
        assert_eq!(compact.result_unknown, 0);
        assert!(probe.compact_requests.lock().unwrap().is_empty());
        assert_report_equations(&report);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn watch_stop_cancellation_is_latched_until_owned_watch_joins() {
        let (shutdown, observer) = shutdown_watch::channel(false);
        let (finish, finished) = oneshot::channel();
        let guard = Arc::new(());
        let lifetime = Arc::downgrade(&guard);
        let mut watches = JoinSet::new();
        watches.spawn(async move {
            let _guard = guard;
            finished.await.unwrap();
            (
                "pods".into(),
                Ok(crate::watch::WatchOutcome {
                    terminal: crate::watch::WatchTerminal::ContextCanceled,
                    state: crate::watch::WatchState::default(),
                }),
            )
        });
        let (cancel, mut ctrl_c) = controlled_ctrl_c();
        let mut stopping = Box::pin(stop_watches_with_ctrl(
            shutdown,
            watches,
            Duration::from_secs(3),
            &mut ctrl_c,
            false,
        ));
        assert_pending(stopping.as_mut()).await;
        assert!(*observer.borrow());
        cancel.send(()).unwrap();
        assert_pending(stopping.as_mut()).await;
        assert!(
            lifetime.upgrade().is_some(),
            "report must wait for the owned Watch task"
        );
        finish.send(()).unwrap();
        let (stopped, interrupted) = time::timeout(Duration::from_secs(1), stopping)
            .await
            .unwrap();
        stopped.unwrap();
        assert!(interrupted);
        assert!(lifetime.upgrade().is_none());
        let report = RunAccounting::new().report(RunTargets::default(), interrupted, true, None);
        assert!(report.interrupted);
        assert!(!report.achieved);
    }
}
