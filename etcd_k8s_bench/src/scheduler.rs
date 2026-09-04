use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot, watch as shutdown_watch};
use tokio::task::JoinSet;
use tokio::time::{self, Instant as TokioInstant};

use crate::client::{ClientBundle, RequestDisposition, RetryDecision, RpcMethod};
use crate::config::{
    EtcdLeasePoolSpec, ResourceSpec, RetryPolicy as ConfigRetryPolicy, RuntimeConfig, WorkloadSpec,
};
use crate::heartbeat::{HeartbeatItem, HeartbeatItemKind, NodeHeartbeatPlanner};
use crate::lease::{LeaseEvent, LeasePool, LeasePoolConfig};
use crate::metrics::{
    self, DropReason, HeartbeatKind, Operation, Outcome, ResourceName, Rpc, TxnResult, TxnShape,
};
use crate::pb::etcdserverpb::{CompactionRequest, LeaseGrantRequest, RangeRequest, TxnResponse};
use crate::workload::{
    CasDecision, CasLoop, ExhaustedReason, KeyState, ResourceOperation,
    RetryPolicy as CasRetryPolicy, TxnAttempt, build_compact_revision_txn, failure_value,
    failure_version,
};

#[derive(Debug)]
pub enum WorkItem {
    Txn {
        resource: usize,
        key: Vec<u8>,
        operation: ResourceOperation,
        value: Vec<u8>,
        pool: Option<String>,
    },
    Range {
        request: RangeRequest,
        selector_keep_every: u64,
        target: u64,
    },
    Heartbeat(HeartbeatItem),
    Bootstrap {
        item: HeartbeatItem,
        done: oneshot::Sender<Result<(), String>>,
    },
    BootstrapFixture {
        name: String,
        key: Vec<u8>,
        value: Vec<u8>,
        done: oneshot::Sender<Result<(), String>>,
    },
    Status,
    CompactTick,
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
    rate_scale: f64,
    time_scale: f64,
    seed: u64,
}
impl Scheduler {
    pub fn new(config: &RuntimeConfig) -> Self {
        Self {
            workload: config.workload.clone(),
            nodes: config.nodes as usize,
            rate_scale: config.rate_scale,
            time_scale: config.time_scale,
            seed: config.seed,
        }
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(
            self.workload
                .timeline
                .iter()
                .map(|s| s.duration_s)
                .sum::<f64>()
                * self.time_scale,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    App,
    Range,
    MasterLease { resource: usize, ordinal: u64 },
    NodeLease(usize),
    NodeStatus { node: usize, tick: u64 },
    Status(u64),
    Compact(u64),
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
    fn new(
        workload: &WorkloadSpec,
        rate_scale: f64,
        app: bool,
        time_scale: f64,
        seed: u64,
        tag: u64,
    ) -> Self {
        let mut offset = Duration::ZERO;
        let mut segments = Vec::with_capacity(workload.timeline.len());
        for (index, segment) in workload.timeline.iter().enumerate() {
            let duration = Duration::from_secs_f64(segment.duration_s * time_scale);
            let rate = if app {
                segment.application_txn_rps * rate_scale
            } else {
                segment.range_rps
            };
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

    fn retarget_total(&mut self, total: u64, seed: u64) {
        let current = self.total();
        if current == total || current == 0 {
            return;
        }
        let exact: Vec<f64> = self
            .segments
            .iter()
            .map(|segment| total as f64 * segment.count as f64 / current as f64)
            .collect();
        let mut assigned = 0u64;
        for (segment, value) in self.segments.iter_mut().zip(&exact) {
            segment.count = value.floor() as u64;
            segment.emitted = 0;
            assigned += segment.count;
        }
        let mut order: Vec<usize> = (0..self.segments.len()).collect();
        order.sort_by(|a, b| {
            exact[*b]
                .fract()
                .partial_cmp(&exact[*a].fract())
                .unwrap_or(Ordering::Equal)
                .then_with(|| splitmix(seed ^ *a as u64).cmp(&splitmix(seed ^ *b as u64)))
        });
        for index in order
            .into_iter()
            .take(total.saturating_sub(assigned) as usize)
        {
            self.segments[index].count += 1;
        }
        self.index = 0;
        self.ordinal = 0;
    }
}

pub(crate) fn application_operation_total(
    workload: &WorkloadSpec,
    rate_scale: f64,
    time_scale: f64,
) -> u64 {
    let weight: f64 = workload
        .resources
        .iter()
        .filter(|resource| resource.retry_policy != ConfigRetryPolicy::MasterLease)
        .map(|resource| resource.create_weight + resource.update_weight + resource.delete_weight)
        .sum();
    (weight * rate_scale * time_scale).round() as u64
}

/// Total number of independent Range requests the scheduler will emit for
/// this workload at base scale, using the same per-segment deterministic
/// rounding as the runtime cursor.
pub(crate) fn range_request_total(workload: &WorkloadSpec, seed: u64) -> u64 {
    RateCursor::new(workload, 1.0, false, 1.0, seed, 0xB2).total()
}

struct ResourceQuota {
    spec_index: usize,
    remaining: [u64; 3],
    deficits: [f64; 3],
    total_weight: f64,
    next_id: u64,
    present: Vec<bool>,
    present_cursor: usize,
}
struct LifecycleProducer {
    resources: Vec<ResourceQuota>,
    resource_deficits: Vec<f64>,
    total_weight: f64,
}
impl LifecycleProducer {
    fn new(workload: &WorkloadSpec, total: u64, seed: u64) -> Self {
        let total_weight: f64 = workload
            .resources
            .iter()
            .filter(|resource| resource.retry_policy != ConfigRetryPolicy::MasterLease)
            .map(|r| r.create_weight + r.update_weight + r.delete_weight)
            .sum();
        let mut exact = Vec::with_capacity(workload.resources.len());
        let mut floors = 0u64;
        for spec in &workload.resources {
            if spec.retry_policy == ConfigRetryPolicy::MasterLease {
                exact.push(0.0);
                continue;
            }
            let weight = spec.create_weight + spec.update_weight + spec.delete_weight;
            let value = if total_weight > 0.0 {
                total as f64 * weight / total_weight
            } else {
                0.0
            };
            floors += value.floor() as u64;
            exact.push(value);
        }
        let mut quotas: Vec<u64> = exact.iter().map(|value| value.floor() as u64).collect();
        let mut left = total.saturating_sub(floors);
        let mut order: Vec<usize> = (0..exact.len()).collect();
        order.sort_by(|a, b| {
            exact[*b]
                .fract()
                .partial_cmp(&exact[*a].fract())
                .unwrap_or(Ordering::Equal)
                .then_with(|| splitmix(seed ^ *a as u64).cmp(&splitmix(seed ^ *b as u64)))
        });
        for index in order {
            if left == 0 {
                break;
            }
            quotas[index] += 1;
            left -= 1;
        }
        let mut resources = Vec::with_capacity(workload.resources.len());
        for (index, spec) in workload.resources.iter().enumerate() {
            if spec.retry_policy == ConfigRetryPolicy::MasterLease {
                continue;
            }
            let weight = spec.create_weight + spec.update_weight + spec.delete_weight;
            let quota = quotas[index];
            let op_weights = [spec.create_weight, spec.update_weight, spec.delete_weight];
            let mut remaining = [0; 3];
            let mut assigned = 0;
            for op in 0..3 {
                let value = if weight > 0.0 {
                    quota as f64 * op_weights[op] / weight
                } else {
                    0.0
                };
                remaining[op] = value.floor() as u64;
                assigned += remaining[op];
            }
            let mut op_left = quota.saturating_sub(assigned);
            for op in 0..3 {
                if op_left > 0 && op_weights[op] > 0.0 {
                    remaining[op] += 1;
                    op_left -= 1;
                }
            }
            let create_overflow = remaining[0].saturating_sub(spec.unique_keys);
            remaining[0] -= create_overflow;
            if op_weights[1] > 0.0 {
                remaining[1] += create_overflow;
            } else {
                remaining[2] += create_overflow;
            }
            resources.push(ResourceQuota {
                spec_index: index,
                remaining,
                deficits: [0.0; 3],
                total_weight: weight,
                next_id: 0,
                present: vec![false; spec.unique_keys as usize],
                present_cursor: splitmix(seed ^ index as u64) as usize,
            });
        }
        Self {
            resource_deficits: vec![0.0; resources.len()],
            resources,
            total_weight,
        }
    }

    fn next(&mut self, specs: &[ResourceSpec]) -> Option<(usize, ResourceOperation, u64)> {
        let mut best: Option<(usize, f64)> = None;
        for (index, resource) in self.resources.iter_mut().enumerate() {
            let eligible = (resource.remaining[0] > 0
                && resource.next_id < resource.present.len() as u64)
                || (resource.remaining[1] > 0 && resource.present.iter().any(|p| *p))
                || (resource.remaining[2] > 0 && resource.present.iter().any(|p| *p));
            if !eligible {
                continue;
            }
            self.resource_deficits[index] += resource.total_weight;
            if best.map_or(true, |(_, deficit)| self.resource_deficits[index] > deficit) {
                best = Some((index, self.resource_deficits[index]));
            }
        }
        let (index, _) = best?;
        self.resource_deficits[index] -= self.total_weight;
        let resource = &mut self.resources[index];
        let spec = &specs[resource.spec_index];
        let weights = [spec.create_weight, spec.update_weight, spec.delete_weight];
        for op in 0..3 {
            resource.deficits[op] += weights[op];
        }
        let op = (0..3)
            .filter(|op| {
                resource.remaining[*op] > 0
                    && match *op {
                        0 => resource.next_id < resource.present.len() as u64,
                        _ => resource.present.iter().any(|p| *p),
                    }
            })
            .max_by(|a, b| {
                resource.deficits[*a]
                    .partial_cmp(&resource.deficits[*b])
                    .unwrap_or(Ordering::Equal)
            })?;
        resource.deficits[op] -= resource.total_weight;
        resource.remaining[op] -= 1;
        let id = match op {
            0 => {
                let id = resource.next_id;
                resource.next_id += 1;
                resource.present[id as usize] = true;
                id
            }
            1 => next_present_id(resource)?,
            _ => {
                let id = next_present_id(resource)?;
                resource.present[id as usize] = false;
                id
            }
        };
        Some((
            resource.spec_index,
            match op {
                0 => ResourceOperation::Create,
                1 => ResourceOperation::GuaranteedUpdate,
                _ => ResourceOperation::ConditionalDelete,
            },
            id,
        ))
    }
}
fn next_present_id(resource: &mut ResourceQuota) -> Option<u64> {
    let len = resource.present.len();
    if len == 0 {
        return None;
    }
    let start = resource.present_cursor % len;
    for offset in 0..len {
        let id = (start + offset) % len;
        if resource.present[id] {
            resource.present_cursor = (id + 1) % len;
            return Some(id as u64);
        }
    }
    None
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
        match self.revision {
            None => self.revision = Some(response_revision),
            Some(revision) if revision != response_revision => {
                return Err(format!(
                    "Range page revision changed from {revision} to {response_revision}"
                ));
            }
            Some(_) => {}
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
        let mut attempts = 0;
        let response = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                metrics::record_request(Rpc::Range, Outcome::Timeout);
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

/// Masterlease tick time for `ordinal`, bounded by both the configured
/// ordinal count and the workload duration so periodic traffic never
/// outlives the run.  Computes the prospective time without overflowing
/// even for huge intervals or ordinals.
fn masterlease_tick_at(
    interval: Duration,
    count: u64,
    ordinal: u64,
    duration: Duration,
) -> Option<Duration> {
    if ordinal >= count {
        return None;
    }
    let secs = interval.as_secs_f64() * (ordinal + 1) as f64;
    if !secs.is_finite() || secs > duration.as_secs_f64() {
        return None;
    }
    Some(Duration::from_secs_f64(secs))
}

fn range_shape_plan(
    workload: &WorkloadSpec,
    prefix_total: u64,
    time_scale: f64,
) -> Result<RangeShapePlan, String> {
    let count_only = (workload.range_count_only_count as f64 * time_scale).round() as u64;
    if count_only > prefix_total {
        return Err(format!(
            "scaled Range shape count ({count_only}) exceeds scheduled prefix Range capacity ({prefix_total})",
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
fn prefix_end(mut key: Vec<u8>) -> Vec<u8> {
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            key.truncate(i + 1);
            return key;
        }
    }
    vec![0]
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
            prefix_end(key)
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
    pool: Mutex<LeasePool>,
}

type WatchTaskOutput = (
    String,
    Result<crate::watch::WatchOutcome, crate::watch::WatchError>,
);

fn spawn_watches(
    client: &ClientBundle,
    specs: &[crate::config::WatchSpec],
) -> (shutdown_watch::Sender<bool>, JoinSet<WatchTaskOutput>) {
    let (shutdown, receiver) = shutdown_watch::channel(false);
    let mut tasks = JoinSet::new();
    for spec in specs.iter().cloned() {
        let name = spec.name.clone();
        let watch_receiver = receiver.clone();
        let watch_client = client.clone();
        tasks.spawn(async move {
            let result = crate::watch::run(watch_client, spec, watch_receiver).await;
            (name, result)
        });
    }
    (shutdown, tasks)
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

async fn stop_watches(
    shutdown: shutdown_watch::Sender<bool>,
    mut tasks: JoinSet<WatchTaskOutput>,
    timeout: Duration,
) {
    let _ = shutdown.send(true);
    let remaining = timeout;
    match time::timeout(remaining, async {
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((name, result)) => {
                    if let Some(error) = watch_failure(&name, result) {
                        eprintln!("watch shutdown error: {error}");
                    }
                }
                Err(error) => eprintln!("watch task failed during shutdown: {error}"),
            }
        }
    })
    .await
    {
        Ok(()) => {}
        Err(_) => {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            eprintln!("watch shutdown timed out");
        }
    }
}

type CtrlCFuture = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

enum ScheduleControl {
    Deadline,
    Interrupted,
    WatchCompleted,
}

async fn wait_for_schedule_event(
    tasks: &mut JoinSet<WatchTaskOutput>,
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> Result<ScheduleControl, String> {
    tokio::select! {
        biased;
        watch_result = tasks.join_next(), if !tasks.is_empty() => {
            match watch_result {
                Some(Ok((name, result))) => {
                    if let Some(error) = watch_failure(&name, result) {
                        Err(error)
                    } else {
                        Ok(ScheduleControl::WatchCompleted)
                    }
                }
                Some(Err(error)) => Err(format!("watch task failed: {error}")),
                None => Ok(ScheduleControl::WatchCompleted),
            }
        }
        _ = time::sleep_until(deadline) => Ok(ScheduleControl::Deadline),
        _ = ctrl_c.as_mut() => Ok(ScheduleControl::Interrupted),
    }
}

pub async fn run(config: RuntimeConfig, client: ClientBundle) -> Result<ScheduleStats, String> {
    if config.queue_capacity < config.workers {
        return Err(format!(
            "queue_capacity ({}) must be at least workers ({})",
            config.queue_capacity, config.workers
        ));
    }
    let scheduler = Scheduler::new(&config);
    let queue_capacity = config.queue_capacity.max(1);
    let workers_count = config.workers.max(1);
    let mut senders = Vec::with_capacity(workers_count);
    let mut workers = Vec::with_capacity(workers_count);
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
                (
                    spec.name.clone(),
                    Arc::new(SharedPool {
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
        workers.push(tokio::spawn(worker_loop(
            rx,
            client.clone(),
            config.workload.clone(),
            config.operation_timeout,
            pools.clone(),
            latest_revision.clone(),
            lease_epoch,
            cas_conflicts.clone(),
        )));
    }
    let planner = NodeHeartbeatPlanner::new(crate::heartbeat::HeartbeatConfig {
        nodes: scheduler.nodes,
        lease_interval: Duration::from_secs_f64(
            scheduler.workload.nodes.lease_interval_s * scheduler.time_scale,
        ),
        lease_jitter: scheduler.workload.nodes.lease_jitter,
        status_interval: Duration::from_secs_f64(
            scheduler.workload.nodes.status_interval_s * scheduler.time_scale,
        ),
        seed: scheduler.seed,
        node_lease_enabled: scheduler.workload.nodes.node_lease_enabled,
    });
    let started = TokioInstant::now();
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
    let mut bootstrap_interrupted = false;
    let mut bootstrap_acks = Vec::with_capacity(scheduler.nodes.saturating_mul(3));
    'bootstrap_send: for item in planner.bootstrap() {
        let shard = stable_shard(item.key.as_bytes(), workers_count);
        let (done_tx, done_rx) = oneshot::channel();
        let resource = if matches!(item.kind, HeartbeatItemKind::NodeLeaseCreate) {
            ResourceName::new("node_leases")
        } else {
            ResourceName::new("nodes")
        };
        let operation = if matches!(
            item.kind,
            HeartbeatItemKind::NodeCreate | HeartbeatItemKind::NodeLeaseCreate
        ) {
            Operation::Create
        } else {
            Operation::Update
        };
        let send_result = tokio::select! {
            _ = &mut ctrl_c => {
                bootstrap_interrupted = true;
                break 'bootstrap_send;
            }
            result = senders[shard].send(WorkItem::Bootstrap { item, done: done_tx }) => result,
        };
        send_result.map_err(|_| "bootstrap worker closed".to_string())?;
        metrics::record_scheduled(operation, &resource.unwrap());
        stats.scheduled += 1;
        bootstrap_acks.push(done_rx);
    }
    if !bootstrap_interrupted {
        'fixture_send: for fixture in &scheduler.workload.range_fixtures {
            let label = ResourceName::new(fixture.name.clone()).expect("validated fixture label");
            for id in 0..fixture.count {
                let key = patterned_key(&fixture.key_pattern, id);
                let shard = stable_shard(&key, workers_count);
                let (done_tx, done_rx) = oneshot::channel();
                let item = WorkItem::BootstrapFixture {
                    name: fixture.name.clone(),
                    value: patterned_value(fixture.value_size_bytes, id, 0),
                    key,
                    done: done_tx,
                };
                let send_result = tokio::select! {
                    _ = &mut ctrl_c => {
                        bootstrap_interrupted = true;
                        break 'fixture_send;
                    }
                    result = senders[shard].send(item) => result,
                };
                send_result.map_err(|_| "bootstrap worker closed".to_string())?;
                metrics::record_scheduled(Operation::Create, &label);
                stats.scheduled += 1;
                bootstrap_acks.push(done_rx);
            }
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
                Ok(Err(error)) => return Err(format!("bootstrap: {error}")),
                Err(_) => return Err("bootstrap worker closed".to_string()),
                Ok(Ok(())) => {}
            }
        }
    }
    if !bootstrap_allows_main_load(bootstrap_interrupted) {
        drain_workers(senders, workers, pools, config.operation_timeout).await;
        return Ok(stats);
    }
    let (watch_shutdown, mut watch_tasks) = spawn_watches(&client, &scheduler.workload.watches);
    let duration = scheduler.duration();
    let mut app = RateCursor::new(
        &scheduler.workload,
        scheduler.rate_scale,
        true,
        scheduler.time_scale,
        scheduler.seed,
        0xA1,
    );
    app.retarget_total(
        application_operation_total(
            &scheduler.workload,
            scheduler.rate_scale,
            scheduler.time_scale,
        ),
        scheduler.seed ^ 0xA1,
    );
    let mut ranges = RateCursor::new(
        &scheduler.workload,
        1.0,
        false,
        scheduler.time_scale,
        scheduler.seed,
        0xB2,
    );
    let mut lifecycle = LifecycleProducer::new(&scheduler.workload, app.total(), scheduler.seed);
    let mut range_producer = RangeProducer::new(scheduler.workload.ranges.iter().map(|r| r.weight));
    let weights: Vec<f64> = scheduler.workload.ranges.iter().map(|r| r.weight).collect();
    let prefix_total = exact_prefix_total(
        range_producer.clone(),
        &weights,
        &scheduler.workload.ranges,
        ranges.total(),
    );
    let mut range_shapes =
        match range_shape_plan(&scheduler.workload, prefix_total, scheduler.time_scale) {
            Ok(plan) => plan,
            Err(error) => {
                stop_watches(watch_shutdown, watch_tasks, config.operation_timeout).await;
                drain_workers(senders, workers, pools, config.operation_timeout).await;
                return Err(error);
            }
        };
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
    if let Some((at, _)) = app.next() {
        push(at, 1, Source::App, &mut heap, &mut sequence);
    }
    if let Some((at, _)) = ranges.next() {
        push(at, 2, Source::Range, &mut heap, &mut sequence);
    }
    let status_interval =
        Duration::from_secs_f64(scheduler.workload.nodes.status_interval_s * scheduler.time_scale);
    if scheduler.workload.nodes.node_lease_enabled {
        for node in 0..scheduler.nodes {
            let lease_at = heartbeat_interval(
                scheduler.seed,
                node,
                0,
                Duration::from_secs_f64(
                    scheduler.workload.nodes.lease_interval_s * scheduler.time_scale,
                ),
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
    let master_lease_interval =
        Duration::from_secs_f64(scheduler.workload.master_lease_interval_s * scheduler.time_scale);
    for (resource, spec) in scheduler.workload.resources.iter().enumerate() {
        if spec.retry_policy == ConfigRetryPolicy::MasterLease {
            let count =
                (spec.create_weight + spec.update_weight + spec.delete_weight).round() as u64;
            if let Some(at) = masterlease_tick_at(master_lease_interval, count, 0, duration) {
                push(
                    at,
                    0,
                    Source::MasterLease {
                        resource,
                        ordinal: 0,
                    },
                    &mut heap,
                    &mut sequence,
                );
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
    let compact_interval =
        Duration::from_secs_f64(scheduler.workload.compact_interval_s * scheduler.time_scale);
    if compact_interval <= duration {
        push(
            compact_interval,
            4,
            Source::Compact(0),
            &mut heap,
            &mut sequence,
        );
    }
    let mut ctrl_c: CtrlCFuture = Box::pin(tokio::signal::ctrl_c());
    while let Some(event) = heap.pop() {
        let deadline = started + event.at;
        let control = match wait_for_schedule_event(&mut watch_tasks, deadline, &mut ctrl_c).await {
            Ok(control) => control,
            Err(error) => {
                cleanup_watch_failure(
                    watch_shutdown,
                    watch_tasks,
                    senders,
                    workers,
                    pools,
                    config.operation_timeout,
                )
                .await;
                return Err(error);
            }
        };
        match control {
            ScheduleControl::Deadline => {}
            ScheduleControl::Interrupted => break,
            ScheduleControl::WatchCompleted => continue,
        }
        match event.source {
            Source::App => {
                if let Some((at, _)) = app.next() {
                    push(at, 1, Source::App, &mut heap, &mut sequence);
                }
                if let Some((resource, operation, id)) =
                    lifecycle.next(&scheduler.workload.resources)
                {
                    let spec = &scheduler.workload.resources[resource];
                    let key = resource_key(spec, id);
                    let label =
                        ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string())?;
                    metrics::record_scheduled(operation_metric(operation), &label);
                    stats.scheduled += 1;
                    let item = WorkItem::Txn {
                        resource,
                        key: key.clone(),
                        operation,
                        value: resource_value(spec, id, event.sequence),
                        pool: spec.lease_pool.clone(),
                    };
                    match senders[stable_shard(&key, workers_count)].try_send(item) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::Backpressure);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::QueueClosed);
                        }
                    }
                } else {
                    // Producer ran out of schedulable operations before the
                    // rate cursor did: count it instead of skipping silently.
                    stats.dropped += 1;
                    metrics::record_dropped(DropReason::Exhausted);
                }
            }
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
                    metrics::record_scheduled(
                        Operation::Read,
                        &ResourceName::new("range").unwrap(),
                    );
                    stats.scheduled += 1;
                    match senders[stable_shard(&request.key, workers_count)].try_send(
                        WorkItem::Range {
                            request,
                            selector_keep_every: scheduler.workload.ranges[index]
                                .selector_keep_every,
                            target: scheduler.workload.ranges[index].limit,
                        },
                    ) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::Backpressure);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }
            Source::MasterLease { resource, ordinal } => {
                let spec = &scheduler.workload.resources[resource];
                let operation = if ordinal == 0 {
                    ResourceOperation::Create
                } else {
                    ResourceOperation::GuaranteedUpdate
                };
                let key = resource_key(spec, 0);
                let label =
                    ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string())?;
                metrics::record_scheduled(operation_metric(operation), &label);
                stats.scheduled += 1;
                let item = WorkItem::Txn {
                    resource,
                    key: key.clone(),
                    operation,
                    value: resource_value(spec, 0, ordinal),
                    pool: spec.lease_pool.clone(),
                };
                match senders[stable_shard(&key, workers_count)].try_send(item) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::QueueClosed);
                    }
                }
                let count =
                    (spec.create_weight + spec.update_weight + spec.delete_weight).round() as u64;
                if let Some(at) =
                    masterlease_tick_at(master_lease_interval, count, ordinal + 1, duration)
                {
                    push(
                        at,
                        0,
                        Source::MasterLease {
                            resource,
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
                metrics::record_scheduled(
                    Operation::Update,
                    &ResourceName::new("node_leases").unwrap(),
                );
                stats.scheduled += 1;
                match senders[stable_shard(key.as_bytes(), workers_count)]
                    .try_send(WorkItem::Heartbeat(item))
                {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(_) => {}
                }
                let next = event.at.saturating_add(heartbeat_interval(
                    scheduler.seed,
                    node,
                    event.sequence,
                    Duration::from_secs_f64(
                        scheduler.workload.nodes.lease_interval_s * scheduler.time_scale,
                    ),
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
                metrics::record_scheduled(Operation::Update, &ResourceName::new("nodes").unwrap());
                stats.scheduled += 1;
                match senders[stable_shard(key.as_bytes(), workers_count)]
                    .try_send(WorkItem::Heartbeat(item))
                {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(_) => {}
                }
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
                    metrics::record_scheduled(
                        Operation::Read,
                        &ResourceName::new("status").unwrap(),
                    );
                    stats.scheduled += 1;
                    let shard = index as usize % workers_count;
                    match senders[shard].try_send(WorkItem::Status) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::Backpressure);
                        }
                        Err(_) => {}
                    }
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
                let compact_key = ResourceName::new("compact_rev_key").unwrap();
                metrics::record_scheduled(
                    if tick == 0 {
                        Operation::Create
                    } else {
                        Operation::Update
                    },
                    &compact_key,
                );
                stats.scheduled += 1;
                match senders[stable_shard(b"compact_rev_key", workers_count)]
                    .try_send(WorkItem::CompactTick)
                {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(_) => {}
                }
                let next = event.at.saturating_add(compact_interval);
                if next <= duration {
                    push(next, 4, Source::Compact(tick + 1), &mut heap, &mut sequence);
                }
            }
        }
    }
    stop_watches(watch_shutdown, watch_tasks, config.operation_timeout).await;
    drain_workers(senders, workers, pools, config.operation_timeout).await;
    Ok(stats)
}
fn bootstrap_allows_main_load(interrupted: bool) -> bool {
    !interrupted
}

fn update_revision(latest_revision: &AtomicI64, revision: i64) {
    let previous = latest_revision.fetch_max(revision, AtomicOrdering::Relaxed);
    metrics::set_revision(previous.max(revision));
}
async fn cleanup_watch_failure(
    watch_shutdown: shutdown_watch::Sender<bool>,
    watch_tasks: JoinSet<WatchTaskOutput>,
    senders: Vec<mpsc::Sender<WorkItem>>,
    workers: Vec<tokio::task::JoinHandle<Result<(), String>>>,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
    timeout: Duration,
) {
    abort_workers(senders, workers, pools).await;
    stop_watches(watch_shutdown, watch_tasks, timeout).await;
}

async fn drain_workers(
    senders: Vec<mpsc::Sender<WorkItem>>,
    workers: Vec<tokio::task::JoinHandle<Result<(), String>>>,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
    timeout: Duration,
) {
    drop(senders);
    let drain_deadline = TokioInstant::now() + timeout;
    for worker in workers {
        let remaining = drain_deadline.saturating_duration_since(TokioInstant::now());
        let mut worker = worker;
        tokio::select! {
            result = &mut worker => {
                let _ = result;
            }
            _ = time::sleep(remaining) => {
                worker.abort();
                let _ = worker.await;
            }
        }
    }
    close_pools(pools);
}

async fn abort_workers(
    senders: Vec<mpsc::Sender<WorkItem>>,
    workers: Vec<tokio::task::JoinHandle<Result<(), String>>>,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
) {
    drop(senders);
    for worker in &workers {
        worker.abort();
    }
    for worker in workers {
        let _ = worker.await;
    }
    close_pools(pools);
}

fn close_pools(pools: Arc<HashMap<String, Arc<SharedPool>>>) {
    drop(pools);
}

fn lease_pool(spec: &EtcdLeasePoolSpec) -> LeasePool {
    LeasePool::new(LeasePoolConfig {
        object_ttl: Duration::from_secs_f64(spec.object_ttl_s),
        reuse_window: Duration::from_secs_f64(spec.reuse_s),
        max_objects: spec.max_objects.min(u32::MAX as u64) as u32,
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
    while let Some(item) = receiver.recv().await {
        match item {
            WorkItem::Txn {
                resource,
                key,
                operation,
                value,
                pool,
            } => {
                if let Some(spec) = workload.resources.get(resource) {
                    if let Ok(label) = ResourceName::try_from(spec.name.as_str()) {
                        let operation_started = Instant::now();
                        let lease_id = match pool.as_deref().and_then(|name| pools.get(name)) {
                            Some(shared) => match acquire_lease(
                                shared,
                                &client,
                                lease_epoch,
                                timeout,
                                latest_revision.clone(),
                            )
                            .await
                            {
                                Ok(id) => id,
                                Err(outcome) => {
                                    metrics::record_operation(
                                        operation_metric(operation),
                                        &label,
                                        outcome,
                                    );
                                    continue;
                                }
                            },
                            None => 0,
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
                        let _ = execute_txn(
                            &client,
                            &label,
                            &mut states,
                            key,
                            value,
                            operation,
                            inject_conflict,
                            retry_policy(spec),
                            CasRetryPolicy::generic_controller(),
                            spec.retry_policy == ConfigRetryPolicy::MasterLease,
                            remaining_timeout,
                            lease_id,
                            latest_revision.clone(),
                        )
                        .await;
                    }
                }
            }
            WorkItem::Range {
                request,
                selector_keep_every,
                target,
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
                        let _ = error.message;
                        error.outcome
                    }
                };
                metrics::record_operation(Operation::Read, &label, outcome);
            }
            WorkItem::Bootstrap { item, done } => {
                let result = execute_heartbeat(
                    &client,
                    &mut states,
                    item,
                    timeout,
                    workload.nodes.status_retry_limit,
                    workload.nodes.lease_retry_limit,
                    latest_revision.clone(),
                )
                .await;
                let _ = done.send(result);
            }
            WorkItem::BootstrapFixture {
                name,
                key,
                value,
                done,
            } => {
                let result = match ResourceName::new(name) {
                    Ok(label) => {
                        let policy = CasRetryPolicy {
                            max_attempts: 1,
                            delay: Duration::ZERO,
                            jitter_ratio: 0.0,
                        };
                        match execute_txn(
                            &client,
                            &label,
                            &mut states,
                            key,
                            value,
                            ResourceOperation::Create,
                            false,
                            policy,
                            policy,
                            false,
                            timeout,
                            0,
                            latest_revision.clone(),
                        )
                        .await
                        {
                            Outcome::Success => Ok(()),
                            outcome => Err(format!("range fixture create failed: {outcome:?}")),
                        }
                    }
                    Err(error) => Err(error.to_string()),
                };
                let _ = done.send(result);
            }
            WorkItem::Heartbeat(item) => {
                let _ = execute_heartbeat(
                    &client,
                    &mut states,
                    item,
                    timeout,
                    workload.nodes.status_retry_limit,
                    workload.nodes.lease_retry_limit,
                    latest_revision.clone(),
                )
                .await;
            }
            WorkItem::Status => {
                let label = ResourceName::new("status").unwrap();
                let outcome = execute_status(&client, timeout, latest_revision.clone()).await;
                metrics::record_operation(Operation::Read, &label, outcome);
            }
            WorkItem::CompactTick => {
                execute_compact_tick(&client, &mut compactor, timeout, latest_revision.clone())
                    .await;
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
) {
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
        return;
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
            return;
        }
        Err(_) => {
            metrics::record_request(Rpc::Txn, Outcome::ResultUnknown);
            metrics::record_operation(operation, &label, Outcome::ResultUnknown);
            return;
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
            return;
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
        // A CAS conflict is coordination, not a business-object retry.  The
        // embedded Range supplies both the next logical version and the
        // revision another compactor already published.
        if let Some(revision) = observation.remote_compact_revision {
            metrics::set_compact_revision(revision);
        }
        metrics::record_operation(operation, &label, Outcome::Success);
        return;
    }
    metrics::record_operation(operation, &label, Outcome::Success);

    let Some(revision) = observation.compact_target else {
        return;
    };
    let compact = ResourceName::new("compact").unwrap();
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        metrics::record_operation(Operation::Compact, &compact, Outcome::Timeout);
        return;
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
    let mut pool = shared.pool.lock().await;
    let mut id = None;
    let mut failure = None;
    for event in pool.acquire_at(now) {
        match event {
            LeaseEvent::Expire { .. } => {}
            LeaseEvent::Reuse { lease_id } => {
                if lease_id == 0 {
                    pool.invalidate(lease_id);
                    failure = Some(Outcome::Timeout);
                } else {
                    id = Some(lease_id);
                }
            }
            LeaseEvent::Grant { ttl } => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    metrics::record_request(Rpc::LeaseGrant, Outcome::Timeout);
                    failure = Some(Outcome::Timeout);
                    continue;
                }
                let start = Instant::now();
                metrics::add_inflight(Rpc::LeaseGrant);
                let response = time::timeout(
                    remaining,
                    client.lease_grant(LeaseGrantRequest {
                        ttl: ttl.as_secs() as i64,
                        id: 0,
                    }),
                )
                .await;
                metrics::remove_inflight(Rpc::LeaseGrant);
                metrics::record_request_duration(Rpc::LeaseGrant, start.elapsed());
                metrics::record_request_bytes(&ResourceName::new("lease").unwrap(), 16);
                match response {
                    Ok(Ok(response)) if response.id != 0 => {
                        id = Some(response.id);
                        pool.record_grant(response.id, now);
                        metrics::record_request(Rpc::LeaseGrant, Outcome::Success);
                        if let Some(header) = response.header {
                            update_revision(&latest_revision, header.revision);
                        }
                    }
                    Ok(Ok(_)) => {
                        metrics::record_request(Rpc::LeaseGrant, Outcome::GrpcError);
                        failure = Some(Outcome::GrpcError);
                    }
                    Ok(Err(error)) => {
                        let decision = error
                            .retry_decision(RpcMethod::LeaseGrant, RequestDisposition::Unknown);
                        let outcome = match decision {
                            RetryDecision::ResultUnknown => Outcome::ResultUnknown,
                            RetryDecision::NotSent | RetryDecision::Retry => Outcome::GrpcError,
                            RetryDecision::Stop => Outcome::GrpcError,
                        };
                        metrics::record_request(Rpc::LeaseGrant, outcome);
                        failure = Some(outcome);
                    }
                    // A local deadline can race with a server-side grant.  The
                    // lease id is therefore unknown; never submit another grant.
                    Err(_) => {
                        metrics::record_request(Rpc::LeaseGrant, Outcome::ResultUnknown);
                        failure = Some(Outcome::ResultUnknown);
                    }
                }
            }
        }
    }
    id.ok_or_else(|| failure.unwrap_or(Outcome::GrpcError))
}
fn retry_policy(spec: &ResourceSpec) -> CasRetryPolicy {
    match spec.retry_policy {
        ConfigRetryPolicy::CreateOnly => CasRetryPolicy {
            max_attempts: 1,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        },
        ConfigRetryPolicy::GuaranteedUpdate
        | ConfigRetryPolicy::ConditionalDelete
        | ConfigRetryPolicy::MasterLease => CasRetryPolicy::generic_controller(),
        ConfigRetryPolicy::Event => CasRetryPolicy::event(),
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
    _transport_policy: CasRetryPolicy,
    cas_policy: CasRetryPolicy,
    recreate_missing: bool,
    timeout: Duration,
    lease_id: i64,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let deadline = Instant::now() + timeout;
    let mut current_value = value;
    let mut state = states.get(&key).cloned().unwrap_or_default();
    let mut storage_attempts = 0u32;
    let Some(mut storage_operation) =
        storage_operation_for_state(operation, &state, recreate_missing)
    else {
        metrics::record_txn(label, shape(operation), TxnResult::Conflict);
        metrics::record_operation(operation_metric(operation), label, Outcome::GrpcError);
        return Outcome::GrpcError;
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
                metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                return Outcome::Timeout;
            }
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
                        Outcome::GrpcError,
                    );
                    return Outcome::GrpcError;
                }
                CasDecision::Retry { latest, .. } => {
                    metrics::record_cas_retry(label, operation_metric(operation));
                    state = latest.clone();
                    current_value = if storage_operation == ResourceOperation::ConditionalDelete {
                        Vec::new()
                    } else {
                        resource_value_for_retry(&current_value, attempt.number)
                    };
                    let Some(next) = cas.retry_with_value(current_value.clone(), &latest) else {
                        metrics::record_operation(
                            operation_metric(operation),
                            label,
                            Outcome::GrpcError,
                        );
                        return Outcome::GrpcError;
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
                    state = KeyState::Absent;
                    break;
                }
                CasDecision::Exhausted { .. } => {
                    metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                    return Outcome::Timeout;
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
) -> Result<(), String> {
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
        unique_keys: 1,
        create_weight: 1.0,
        update_weight: 1.0,
        delete_weight: 0.0,
        value_size_bytes: if resource == "nodes" { 2048 } else { 256 },
        retry_policy: ConfigRetryPolicy::GuaranteedUpdate,
        lease_pool: None,
    };
    let label = ResourceName::try_from(resource).map_err(|e| e.to_string())?;
    let mut policy = if matches!(item.kind, HeartbeatItemKind::NodeStatus) {
        CasRetryPolicy::node_status()
    } else if matches!(item.kind, HeartbeatItemKind::NodeLeaseRenew) {
        CasRetryPolicy::node_lease()
    } else {
        CasRetryPolicy {
            max_attempts: 1,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    };
    if matches!(item.kind, HeartbeatItemKind::NodeStatus) {
        policy.max_attempts = status_retry_limit.max(1);
    } else if matches!(item.kind, HeartbeatItemKind::NodeLeaseRenew) {
        policy.max_attempts = lease_retry_limit.max(1);
    }
    let outcome = execute_txn(
        client,
        &label,
        states,
        item.key.into_bytes(),
        resource_value(&spec, item.node as u64, item.at.as_nanos() as u64),
        operation,
        false,
        policy,
        policy,
        false,
        timeout,
        0,
        latest_revision,
    )
    .await;
    metrics::record_node_heartbeat(kind, outcome);
    if outcome != Outcome::Success {
        return Err(format!("heartbeat {kind:?} failed: {outcome:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::etcdserverpb::ResponseOp;
    use crate::pb::mvccpb;
    #[test]
    fn lazy_cursor_has_bounded_state() {
        let workload = WorkloadSpec {
            name: "mock_k8".into(),
            pod_size_bytes: 4096,
            nodes: crate::config::NodeSpec {
                count: 1,
                status_interval_s: 300.0,
                lease_interval_s: 10.0,
                lease_jitter: 0.0,
                status_retry_limit: 5,
                lease_retry_limit: 5,
                node_lease_enabled: false,
            },
            timeline: vec![crate::config::TimelineSegment {
                duration_s: 10.0,
                application_txn_rps: 1000.0,
                range_rps: 0.0,
            }],
            resources: vec![ResourceSpec {
                name: "pods".into(),
                key_pattern: "/p/{id}".into(),
                unique_keys: 10000,
                create_weight: 1.0,
                update_weight: 1.0,
                delete_weight: 0.0,
                value_size_bytes: 4096,
                retry_policy: ConfigRetryPolicy::GuaranteedUpdate,
                lease_pool: None,
            }],
            ranges: vec![],
            range_fixtures: vec![],
            watches: vec![],
            etcd_lease_pools: vec![],
            status_count: 0,
            compact_interval_s: 300.0,
            master_lease_interval_s: 10.0,
            cas_conflicts: 0,
            range_count_only_count: 0,
        };
        let cursor = RateCursor::new(&workload, 1.0, true, 1.0, 1, 1);
        assert_eq!(cursor.total(), 10000);
    }
    #[test]
    fn stable_routing_is_repeatable() {
        assert_eq!(stable_shard(b"/k", 7), stable_shard(b"/k", 7));
    }
    #[test]
    fn prefix_end_handles_ff() {
        assert_eq!(prefix_end(vec![0xff]), vec![0]);
        assert_eq!(prefix_end(vec![b'a', 0xff]), vec![b'b']);
    }
    #[test]
    fn present_keys_rotate_deterministically() {
        let mut resource = ResourceQuota {
            spec_index: 0,
            remaining: [0, 0, 0],
            deficits: [0.0; 3],
            total_weight: 0.0,
            next_id: 3,
            present: vec![true, true, true],
            present_cursor: 0,
        };
        assert_eq!(next_present_id(&mut resource), Some(0));
        assert_eq!(next_present_id(&mut resource), Some(1));
        assert_eq!(next_present_id(&mut resource), Some(2));
        assert_eq!(next_present_id(&mut resource), Some(0));
    }
    #[test]
    fn embedded_ranges_scan_resource_prefixes() {
        let workload = WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
        let pods = workload
            .ranges
            .iter()
            .find(|range| range.key_pattern == "/registry/pods/bench-{ns}/")
            .unwrap();
        let request = range_request(pods, 42);
        assert_eq!(request.key, b"/registry/pods/bench-bench/");
        assert_eq!(request.range_end, prefix_end(request.key.clone()));

        let minions = workload
            .ranges
            .iter()
            .find(|range| range.key_pattern == "/registry/minions/")
            .unwrap();
        let request = range_request(minions, 42);
        assert_eq!(request.key, b"/registry/minions/");
        assert_eq!(request.range_end, prefix_end(request.key.clone()));
    }

    #[test]
    fn application_quotas_are_exact_and_exclude_masterlease() {
        let workload = WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
        let total = application_operation_total(&workload, 1.0, 1.0);
        assert_eq!(total, 514_085);
        assert_eq!(application_operation_total(&workload, 1.0, 0.001), 514);
        let producer = LifecycleProducer::new(&workload, total, 1);
        assert!(producer.resources.iter().all(|quota| {
            workload.resources[quota.spec_index].retry_policy != ConfigRetryPolicy::MasterLease
        }));
        assert_eq!(
            producer
                .resources
                .iter()
                .flat_map(|quota| quota.remaining)
                .sum::<u64>(),
            total
        );
        assert!(producer.resources.iter().all(|quota| {
            quota.remaining[0] <= workload.resources[quota.spec_index].unique_keys
        }));
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
        let nodes = 2003;
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
        assert_eq!(total, 15_423);
    }

    #[test]
    fn rolling_update_workload_schedules_exact_budget() {
        let base = WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
        let base_range = range_request_total(&base, 1);
        let base_app = application_operation_total(&base, 1.0, 1.0);
        let (nodes, batch) = (100u64, 10u64);
        let waves = nodes.div_ceil(batch);
        let workload = base
            .rolling_update(nodes, batch, 500.0, "rolling-test", base_range, base_app)
            .unwrap();
        let expected = 8 * nodes + 1 + 2 * waves;
        assert_eq!(application_operation_total(&workload, 1.0, 1.0), expected);
        let cursor = RateCursor::new(&workload, 1.0, true, 1.0, 1, 0xA1);
        assert_eq!(cursor.total(), expected);
        let ranges = RateCursor::new(&workload, 1.0, false, 1.0, 1, 0xB2);
        let weights: Vec<f64> = workload.ranges.iter().map(|range| range.weight).collect();
        let producer = RangeProducer::new(weights.iter().copied());
        let prefix_total = exact_prefix_total(producer, &weights, &workload.ranges, ranges.total());
        range_shape_plan(&workload, prefix_total, 1.0).unwrap();
    }

    #[test]
    fn masterlease_ticks_stop_at_workload_duration() {
        let interval = Duration::from_secs(10);
        let duration = Duration::from_secs(15);
        // Derived short run: only the ordinal-0 tick at t=10s fits.
        assert_eq!(
            masterlease_tick_at(interval, 231, 0, duration),
            Some(interval)
        );
        assert_eq!(masterlease_tick_at(interval, 231, 1, duration), None);
        // An interval longer than the run suppresses even the initial tick.
        assert_eq!(
            masterlease_tick_at(Duration::from_secs(300), 231, 0, duration),
            None
        );
        // Trace mode: exactly 231 ticks, the last one lands on t == duration.
        let trace_duration = Duration::from_secs(2310);
        let ticks = (0..231)
            .filter(|&ordinal| {
                masterlease_tick_at(interval, 231, ordinal, trace_duration).is_some()
            })
            .count();
        assert_eq!(ticks, 231);
        assert_eq!(
            masterlease_tick_at(interval, 231, 231, trace_duration),
            None
        );
        // Huge intervals cannot overflow even while ordinals remain.
        assert_eq!(
            masterlease_tick_at(Duration::from_secs_f64(1e19), 2, 1, trace_duration),
            None
        );
    }

    #[test]
    fn range_shape_plan_emits_exact_trace_counts() {
        let workload = WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
        let ranges = RateCursor::new(&workload, 1.0, false, 1.0, 1, 0xB2);
        assert_eq!(ranges.total(), 22_080);
        let weights: Vec<f64> = workload.ranges.iter().map(|range| range.weight).collect();
        let producer = RangeProducer::new(weights.iter().copied());
        let prefix_total = exact_prefix_total(producer, &weights, &workload.ranges, ranges.total());
        let mut plan = range_shape_plan(&workload, prefix_total, 1.0).unwrap();
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
            WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
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
        let mut plan = range_shape_plan(&workload, prefix_total, 1.0).unwrap();
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
        let second = range_page(42, &[b"c", b"d"], false, 4);
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
    fn range_pager_rejects_revision_change_and_empty_more_page() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 500,
            ..Default::default()
        };
        let mut pager = RangePager::new(request.clone(), RangeSelector::All { target: None });
        let first = range_page(7, &[b"a"], true, 2);
        assert!(pager.observe_response(&first, 1).is_ok());
        let changed = range_page(8, &[b"b"], false, 2);
        assert!(pager.observe_response(&changed, 1).is_err());

        let mut empty = RangePager::new(request, RangeSelector::All { target: None });
        let response = range_page(7, &[], true, 1);
        let error = empty.observe_response(&response, 0).unwrap_err();
        assert!(error.contains("more=true"));
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
    fn embedded_read_only_prefixes_have_bootstrap_fixtures() {
        let workload = WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
        for prefix in ["services", "endpointslices"] {
            let fixture = workload
                .range_fixtures
                .iter()
                .find(|fixture| fixture.name.starts_with(prefix))
                .unwrap();
            assert_eq!(fixture.count, 100);
            assert!(
                patterned_key(&fixture.key_pattern, 0)
                    .starts_with(format!("/registry/{prefix}/bench-bench/").as_bytes())
            );
        }
    }

    #[test]
    fn interrupted_bootstrap_blocks_main_load() {
        assert!(!bootstrap_allows_main_load(true));
        assert!(bootstrap_allows_main_load(false));
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

        let result = wait_for_schedule_event(
            &mut tasks,
            TokioInstant::now() + Duration::from_secs(60),
            &mut ctrl_c,
        )
        .await;

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
    async fn watch_failure_aborts_queued_workers_before_returning() {
        let mut watch_tasks = JoinSet::new();
        watch_tasks.spawn(async {
            (
                "pods".to_string(),
                Err(crate::watch::WatchError::Terminal(
                    crate::watch::WatchOutcome {
                        terminal: crate::watch::WatchTerminal::Canceled {
                            reason: "server failed".into(),
                        },
                        state: crate::watch::WatchState::default(),
                    },
                )),
            )
        });
        let mut ctrl_c: CtrlCFuture =
            Box::pin(std::future::pending::<Result<(), std::io::Error>>());
        let result = wait_for_schedule_event(
            &mut watch_tasks,
            TokioInstant::now() + Duration::from_secs(60),
            &mut ctrl_c,
        )
        .await;
        let Err(error) = result else {
            panic!("watch failure should stop scheduling");
        };
        assert!(error.starts_with("watch pods failed: watch terminated: Canceled"));
        assert!(error.contains("server failed"));

        let (sender, mut receiver) = mpsc::channel(2);
        let processed = Arc::new(AtomicU64::new(0));
        let processed_by_worker = processed.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered_by_worker = entered.clone();
        let worker = tokio::spawn(async move {
            while receiver.recv().await.is_some() {
                processed_by_worker.fetch_add(1, AtomicOrdering::Relaxed);
                entered_by_worker.notify_waiters();
                std::future::pending::<()>().await;
            }
            Ok(())
        });
        let entered_wait = entered.notified();
        sender.send(WorkItem::Status).await.unwrap();
        entered_wait.await;
        sender.send(WorkItem::Status).await.unwrap();

        let pool = Arc::new(SharedPool {
            pool: Mutex::new(LeasePool::new(LeasePoolConfig::default())),
        });
        let pool_weak = Arc::downgrade(&pool);
        let mut pool_map = HashMap::new();
        pool_map.insert("test".to_string(), pool.clone());
        drop(pool);

        let (watch_shutdown, _) = shutdown_watch::channel(false);
        cleanup_watch_failure(
            watch_shutdown,
            watch_tasks,
            vec![sender],
            vec![worker],
            Arc::new(pool_map),
            Duration::from_secs(1),
        )
        .await;

        assert_eq!(processed.load(AtomicOrdering::Relaxed), 1);
        assert!(pool_weak.upgrade().is_none());
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
}
