use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Notify, mpsc, oneshot, watch as shutdown_watch};
use tokio::task::JoinSet;
use tokio::time::{self, Instant as TokioInstant};

use crate::client::{ClientBundle, RequestDisposition, RetryDecision, RpcMethod};
use crate::config::RunConfig;
use crate::heartbeat::{HeartbeatItem, HeartbeatItemKind};
use crate::measurement::{
    LogicalStreamIdentity, MeasurementRecorder, MeasurementToken, OfferResult, PreparationStream,
    TokenAction,
};
use crate::metrics::{
    self, DropReason, HeartbeatKind, Operation, Outcome, RangeShape, ResourceName, Rpc, TxnResult,
    TxnShape,
};
use crate::pb::etcdserverpb::{CompactionRequest, RangeRequest, TxnResponse};
use crate::plan::{CompiledPlan, LogicalWorkClass, PeriodicEvent, PeriodicKind, ResourceKind};
use crate::report::MeasurementReport;
use crate::workload::{
    CasDecision, CasLoop, CasRetryPolicy, ExhaustedReason, KeyState, ResourceOperation, TxnAttempt,
    build_compact_revision_txn, failure_value, failure_version,
};

const MAX_ERROR_CONTEXT: usize = 256;

/// Result of one scheduler invocation. A missing measurement is intentional
/// for setup/preparation failures: no synthetic report claims a window that
/// never started.
#[derive(Debug, Serialize)]
pub struct RunResult {
    pub status: RunStatus,
    pub error: Option<String>,
    pub measurement: Option<MeasurementReport>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Completed,
    Interrupted,
    Failed,
}

impl RunResult {
    fn failed(error: impl Into<String>) -> Self {
        Self {
            status: RunStatus::Failed,
            error: Some(bound_error(error.into())),
            measurement: None,
        }
    }

    fn interrupted(error: Option<String>, measurement: Option<MeasurementReport>) -> Self {
        Self {
            status: RunStatus::Interrupted,
            error: error.map(bound_error),
            measurement,
        }
    }

    fn completed(measurement: MeasurementReport) -> Self {
        Self {
            status: RunStatus::Completed,
            error: None,
            measurement: Some(measurement),
        }
    }
}

fn bound_error(error: String) -> String {
    error.chars().take(MAX_ERROR_CONTEXT).collect()
}

#[derive(Debug)]
struct PreparationTracker {
    remaining: AtomicU64,
    first_error: StdMutex<Option<String>>,
    notify: Notify,
}

impl PreparationTracker {
    fn new(total: u64) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicU64::new(total),
            first_error: StdMutex::new(None),
            notify: Notify::new(),
        })
    }

    fn complete(&self, outcome: Outcome) {
        if outcome != Outcome::Success {
            let mut error = self.first_error.lock().unwrap();
            if error.is_none() {
                *error = Some(format!("preparation outcome: {}", outcome.as_str()));
            }
        }
        if self.remaining.fetch_sub(1, AtomicOrdering::AcqRel) == 1 {
            self.notify.notify_waiters();
        }
    }

    async fn wait(&self, deadline: TokioInstant) -> Result<(), String> {
        loop {
            // Register before checking the predicate. `notify_waiters` does
            // not retain a permit for a future created after the wakeup.
            let notified = self.notify.notified();
            if TokioInstant::now() >= deadline {
                return Err("preparation deadline exceeded".into());
            }
            if self.remaining.load(AtomicOrdering::Acquire) == 0 {
                return self.first_error.lock().unwrap().clone().map_or(Ok(()), Err);
            }
            if time::timeout_at(deadline, notified).await.is_err() {
                return Err("preparation deadline exceeded".into());
            }
        }
    }
}

#[derive(Debug)]
enum WorkItem {
    Txn {
        resource: ResourceKind,
        key: Vec<u8>,
        operation: ResourceOperation,
        value: Vec<u8>,
        token: MeasurementToken,
        preparation: Option<Arc<PreparationTracker>>,
    },
    Range {
        request: RangeRequest,
        selector: RangeSelector,
        token: MeasurementToken,
    },
    Heartbeat {
        item: HeartbeatItem,
        resource: ResourceKind,
        token: MeasurementToken,
    },
    Status {
        token: MeasurementToken,
    },
    CompactTick {
        token: MeasurementToken,
    },
}

impl WorkItem {
    fn token(&self) -> &MeasurementToken {
        match self {
            Self::Txn { token, .. }
            | Self::Range { token, .. }
            | Self::Heartbeat { token, .. }
            | Self::Status { token }
            | Self::CompactTick { token } => token,
        }
    }

    fn accept(&self) -> Result<TokenAction, String> {
        self.token().accept().map_err(|error| error.to_string())
    }

    fn accept_for_tick(&self, tick_end_ns: u64) -> Result<TokenAction, String> {
        self.token()
            .accept_for_tick(tick_end_ns)
            .map_err(|error| error.to_string())
    }

    fn started(&self) -> Result<(), String> {
        self.token()
            .started()
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn terminal(&self, outcome: Outcome) -> Result<(), String> {
        self.token()
            .terminal(outcome)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn terminal_with_error(
        &self,
        outcome: Outcome,
        code: u64,
        message: &str,
    ) -> Result<(), String> {
        self.token()
            .terminal_with_error(outcome, code, message)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn drop_for(&self, reason: DropReason) -> Result<(), String> {
        self.token()
            .drop_for(reason)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn preparation(&self) -> Option<&Arc<PreparationTracker>> {
        match self {
            Self::Txn { preparation, .. } => preparation.as_ref(),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DispatchOutcome {
    Accepted,
    Dropped,
    Missed,
    MissedToken,
    Stopped,
}

enum OfferedMeasurement {
    Token(MeasurementToken),
    MissedSchedule,
    NotOfferedAfterStop,
}

#[derive(Debug)]
struct DispatchState {
    next_ordinals: Vec<u64>,
    periodic_cursor: usize,
    next_tick: u64,
}

impl DispatchState {
    fn new(plan: &CompiledPlan) -> Self {
        Self {
            next_ordinals: vec![0; plan.logical_streams.len()],
            periodic_cursor: 0,
            next_tick: 0,
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
        // Only bounded prefix LISTs use the internal pager. An unbounded
        // prefix (`limit=0`) preserves its one-shot wire shape.
        let paged = !request.count_only && !request.range_end.is_empty() && request.limit > 0;
        let target = selector.target().or_else(|| {
            if request.limit > 0 {
                u64::try_from(request.limit).ok()
            } else {
                None
            }
        });
        let page_limit = if paged {
            target
                .and_then(|limit| i64::try_from(limit).ok())
                .map_or(Self::INITIAL_LIMIT, |limit| Self::INITIAL_LIMIT.min(limit))
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
            target,
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
        let mut next_limit = self.page_limit;
        if selected_in_page < response.kvs.len() {
            next_limit = next_limit.saturating_mul(2).min(Self::MAX_LIMIT);
        }
        if let Some(target) = self.target {
            let remaining = target.saturating_sub(self.selected);
            let remaining =
                i64::try_from(remaining).map_or(Self::MAX_LIMIT, |remaining| remaining.max(1));
            next_limit = next_limit.min(remaining);
        }
        self.page_limit = next_limit;
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
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> Result<(), String> {
    if ready_receivers.is_empty() {
        return Ok(());
    }
    for mut ready in ready_receivers {
        let mut readiness_closed = false;
        loop {
            if TokioInstant::now() >= deadline {
                return Err("watch readiness timed out".into());
            }
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
                        Ok(()) if TokioInstant::now() < deadline => break,
                        Ok(()) => return Err("watch readiness timed out".into()),
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
    if TokioInstant::now() >= deadline {
        return Err("watch readiness timed out".into());
    }
    Ok(())
}

type CtrlCFuture = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

fn update_revision(latest_revision: &AtomicI64, revision: i64) {
    let previous = latest_revision.fetch_max(revision, AtomicOrdering::Relaxed);
    metrics::set_revision(previous.max(revision));
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

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn stable_shard(key: &[u8], workers: usize) -> usize {
    if workers == 0 {
        return 0;
    }
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    (splitmix64(hash) as usize) % workers
}

fn stream_identity(stream: &crate::plan::LogicalStreamPlan) -> LogicalStreamIdentity {
    LogicalStreamIdentity::Application {
        work_class: stream.work_class,
        resource: stream.resource,
    }
}

fn periodic_identity(kind: PeriodicKind) -> LogicalStreamIdentity {
    LogicalStreamIdentity::Periodic { kind }
}

fn resource_name(kind: ResourceKind) -> ResourceName {
    ResourceName::new(kind.as_str()).expect("fixed resource names are valid metric labels")
}

fn run_duration(plan: &CompiledPlan) -> Duration {
    Duration::from_millis(plan.config.load.duration_ms)
}

fn operation_timeout(plan: &CompiledPlan) -> Duration {
    Duration::from_millis(plan.config.execution.operation_timeout_ms)
}

fn request_timeout(plan: &CompiledPlan) -> Duration {
    Duration::from_millis(plan.config.execution.request_timeout_ms)
}

fn preparation_timeout(plan: &CompiledPlan) -> Duration {
    Duration::from_millis(plan.config.execution.preparation_timeout_ms)
}

fn drain_timeout(plan: &CompiledPlan) -> Duration {
    Duration::from_millis(plan.config.execution.drain_timeout_ms)
}

fn watch_teardown_timeout(plan: &CompiledPlan) -> Duration {
    Duration::from_millis(plan.config.execution.watch_teardown_timeout_ms)
}

fn nanos(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_nanos()).map_err(|_| "duration exceeds u64 nanoseconds".into())
}

fn planned_at(stream: &crate::plan::LogicalStreamPlan, elapsed_ns: u64) -> Result<u64, String> {
    stream
        .rate
        .planned(Duration::from_nanos(elapsed_ns))
        .map_err(|error| error.to_string())
}

fn record_missed(
    recorder: &MeasurementRecorder,
    identity: LogicalStreamIdentity,
    count: u64,
) -> Result<(), String> {
    if count == 0 {
        return Ok(());
    }
    recorder
        .record_missed_schedule_before_cutoff(identity, count)
        .map_err(|error| error.to_string())
}

fn record_not_offered(
    recorder: &MeasurementRecorder,
    identity: LogicalStreamIdentity,
    count: u64,
) -> Result<(), String> {
    if count == 0 {
        return Ok(());
    }
    recorder
        .record_not_offered_after_stop(identity, count)
        .map_err(|error| error.to_string())
}
fn resource_count(plan: &CompiledPlan, kind: ResourceKind) -> u64 {
    match kind {
        ResourceKind::Pods => plan.resource_counts.pods,
        ResourceKind::Nodes => plan.resource_counts.nodes,
        ResourceKind::NodeLeases => plan.resource_counts.node_leases,
        ResourceKind::Deployments => plan.resource_counts.deployments,
        ResourceKind::ReplicaSets => plan.resource_counts.replicasets,
        ResourceKind::DaemonSets => plan.resource_counts.daemonsets,
    }
}

fn preparation_total(plan: &CompiledPlan) -> u64 {
    plan.resource_counts
        .total()
        .saturating_add(plan.resource_counts.nodes)
}

async fn enqueue_preparation_item(
    sender: &mpsc::Sender<WorkItem>,
    item: WorkItem,
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> Result<(), String> {
    let permit = tokio::select! {
        biased;
        _ = ctrl_c.as_mut() => return Err("interrupted during preparation".into()),
        result = time::timeout_at(deadline, sender.reserve()) => {
            match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return Err("preparation worker queue closed".into()),
                Err(_) => return Err("preparation enqueue deadline exceeded".into()),
            }
        }
    };
    if item.accept()? != TokenAction::Applied {
        return Err("preparation admission was not applied".into());
    }
    permit.send(item);
    Ok(())
}

async fn run_preparation(
    plan: Arc<CompiledPlan>,
    recorder: &MeasurementRecorder,
    senders: &[mpsc::Sender<WorkItem>],
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> Result<(), String> {
    let tracker = PreparationTracker::new(preparation_total(&plan));
    for kind in ResourceKind::ALL {
        let stream = PreparationStream::Create(kind);
        let count = resource_count(&plan, kind);
        for ordinal in 0..count {
            let token = match recorder
                .offer_preparation(stream)
                .map_err(|error| error.to_string())?
            {
                OfferResult::Token(token) => token,
                OfferResult::MissedSchedule
                | OfferResult::NotOfferedAfterStop
                | OfferResult::WrongPhase => {
                    return Err("preparation recorder rejected create token".into());
                }
            };
            let key = plan.resource(kind).key(ordinal);
            let value = plan.resource(kind).value(ordinal, 0);
            let item = WorkItem::Txn {
                resource: kind,
                key: key.clone(),
                operation: ResourceOperation::Create,
                value,
                token,
                preparation: Some(tracker.clone()),
            };
            let shard = stable_shard(&key, senders.len());
            metrics::record_scheduled(Operation::Create, &resource_name(kind));
            if let Err(error) =
                enqueue_preparation_item(&senders[shard], item, deadline, ctrl_c).await
            {
                return Err(error);
            }
        }
    }

    // NodeReady is an ordinary update against each Node key. It is deliberately
    // queued only after every create has been offered, preserving a deterministic
    // same-shard create-before-ready order without a native lease barrier.
    for ordinal in 0..plan.resource_counts.nodes {
        let token = match recorder
            .offer_preparation(PreparationStream::NodeReady)
            .map_err(|error| error.to_string())?
        {
            OfferResult::Token(token) => token,
            OfferResult::MissedSchedule
            | OfferResult::NotOfferedAfterStop
            | OfferResult::WrongPhase => {
                return Err("preparation recorder rejected NodeReady token".into());
            }
        };
        let key = plan.resource(ResourceKind::Nodes).key(ordinal);
        let value = plan.resource(ResourceKind::Nodes).value(ordinal, 1);
        let item = WorkItem::Txn {
            resource: ResourceKind::Nodes,
            key: key.clone(),
            operation: ResourceOperation::GuaranteedUpdate,
            value,
            token,
            preparation: Some(tracker.clone()),
        };
        let shard = stable_shard(&key, senders.len());
        metrics::record_scheduled(Operation::Update, &resource_name(ResourceKind::Nodes));
        if let Err(error) = enqueue_preparation_item(&senders[shard], item, deadline, ctrl_c).await
        {
            return Err(error);
        }
    }

    tracker.wait(deadline).await
}
fn spawn_workers(
    plan: Arc<CompiledPlan>,
    client: &ClientBundle,
) -> (Vec<mpsc::Sender<WorkItem>>, JoinSet<Result<(), String>>) {
    let workers = usize::try_from(plan.config.execution.workers)
        .expect("validated worker count must fit usize");
    let capacity = usize::try_from(plan.config.execution.queue_capacity)
        .expect("validated queue capacity must fit usize");
    let base = capacity / workers;
    let remainder = capacity % workers;
    let mut senders = Vec::with_capacity(workers);
    let mut tasks = JoinSet::new();
    let operation_timeout = operation_timeout(&plan);
    let latest_revision = Arc::new(AtomicI64::new(0));
    for index in 0..workers {
        let queue_capacity = base + usize::from(index < remainder);
        let (sender, receiver) = mpsc::channel(queue_capacity.max(1));
        senders.push(sender);
        let worker_plan = plan.clone();
        let worker_client = client.clone();
        let worker_revision = latest_revision.clone();
        tasks.spawn(async move {
            worker_loop(
                receiver,
                worker_client,
                worker_plan,
                operation_timeout,
                worker_revision,
            )
            .await
        });
    }
    (senders, tasks)
}

async fn abort_workers(
    senders: Vec<mpsc::Sender<WorkItem>>,
    mut tasks: JoinSet<Result<(), String>>,
) {
    drop(senders);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

async fn drain_workers(
    senders: Vec<mpsc::Sender<WorkItem>>,
    mut tasks: JoinSet<Result<(), String>>,
    timeout: Duration,
) -> Result<(), String> {
    drop(senders);
    let deadline = TokioInstant::now() + timeout;
    let mut first_error = None;
    loop {
        let joined = match time::timeout_at(deadline, tasks.join_next()).await {
            Ok(joined) => joined,
            Err(_) => {
                tasks.abort_all();
                while let Some(joined) = tasks.join_next().await {
                    if let Err(error) = joined {
                        if !error.is_cancelled() && first_error.is_none() {
                            first_error = Some(format!("worker task failed: {error}"));
                        }
                    }
                }
                return Err(first_error.map_or_else(
                    || "worker drain deadline exceeded".to_string(),
                    |error| format!("worker drain deadline exceeded: {error}"),
                ));
            }
        };
        let Some(joined) = joined else {
            break;
        };
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
            Ok(Err(_)) => {}
            Err(error) if !error.is_cancelled() && first_error.is_none() => {
                first_error = Some(format!("worker task failed: {error}"));
            }
            Err(_) => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn stop_watches(
    shutdown: shutdown_watch::Sender<bool>,
    mut tasks: JoinSet<WatchTaskOutput>,
    timeout: Duration,
) -> Result<(), String> {
    let _ = shutdown.send(true);
    let deadline = TokioInstant::now() + timeout;
    let mut first_error = None;
    loop {
        let joined = match time::timeout_at(deadline, tasks.join_next()).await {
            Ok(joined) => joined,
            Err(_) => {
                tasks.abort_all();
                while let Some(joined) = tasks.join_next().await {
                    if let Err(error) = joined {
                        if !error.is_cancelled() && first_error.is_none() {
                            first_error = Some(format!("watch task failed: {error}"));
                        }
                    }
                }
                return Err(first_error.map_or_else(
                    || "watch teardown deadline exceeded".to_string(),
                    |error| format!("watch teardown deadline exceeded: {error}"),
                ));
            }
        };
        let Some(joined) = joined else {
            break;
        };
        if let Err(error) = joined {
            if !error.is_cancelled() && first_error.is_none() {
                first_error = Some(format!("watch task failed: {error}"));
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}
fn offer_measurement(
    recorder: &MeasurementRecorder,
    identity: LogicalStreamIdentity,
) -> Result<OfferedMeasurement, String> {
    match recorder
        .offer(identity)
        .map_err(|error| error.to_string())?
    {
        OfferResult::Token(token) => Ok(OfferedMeasurement::Token(token)),
        OfferResult::MissedSchedule => Ok(OfferedMeasurement::MissedSchedule),
        OfferResult::NotOfferedAfterStop => Ok(OfferedMeasurement::NotOfferedAfterStop),
        OfferResult::WrongPhase => Err(format!(
            "measurement offer for {} occurred outside measurement phase",
            identity.as_str()
        )),
    }
}

fn dispatch_application_item(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    stream: &crate::plan::LogicalStreamPlan,
    ordinal: u64,
    sender: &mpsc::Sender<WorkItem>,
    tick_deadline: TokioInstant,
    _cutoff_deadline: TokioInstant,
    tick_end_ns: u64,
) -> Result<DispatchOutcome, String> {
    let identity = stream_identity(stream);
    if TokioInstant::now() >= tick_deadline {
        return Ok(DispatchOutcome::Missed);
    }
    let token = match offer_measurement(recorder, identity)? {
        OfferedMeasurement::Token(token) => token,
        OfferedMeasurement::MissedSchedule => return Ok(DispatchOutcome::MissedToken),
        OfferedMeasurement::NotOfferedAfterStop => return Ok(DispatchOutcome::Stopped),
    };
    let item = match stream.work_class {
        LogicalWorkClass::Update => {
            let key = plan.resource_key(stream.resource, ordinal, plan.config.seed);
            let value = plan.resource_value(stream.resource, ordinal, plan.config.seed, ordinal);
            WorkItem::Txn {
                resource: stream.resource,
                key,
                operation: ResourceOperation::GuaranteedUpdate,
                value,
                token,
                preparation: None,
            }
        }
        LogicalWorkClass::PointGet => WorkItem::Range {
            request: plan.point_get_request(stream.resource, ordinal, plan.config.seed),
            selector: RangeSelector::All { target: None },
            token,
        },
        LogicalWorkClass::BoundedList => WorkItem::Range {
            request: plan
                .list_request(stream.resource, ordinal, plan.config.seed)
                .map_err(|error| error.to_string())?,
            selector: RangeSelector::All {
                target: Some(plan.config.load.list_limit),
            },
            token,
        },
    };
    let operation = match stream.work_class {
        LogicalWorkClass::Update => Operation::Update,
        LogicalWorkClass::PointGet | LogicalWorkClass::BoundedList => Operation::Read,
    };
    metrics::record_scheduled(operation, &resource_name(stream.resource));
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(())) => {
            item.drop_for(DropReason::Backpressure)?;
            return Ok(DispatchOutcome::Dropped);
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            item.drop_for(DropReason::QueueClosed)?;
            return Err("measurement worker queue closed".into());
        }
    };
    match item.accept_for_tick(tick_end_ns)? {
        TokenAction::Applied => {
            permit.send(item);
            Ok(DispatchOutcome::Accepted)
        }
        TokenAction::RejectedAfterTick => Ok(DispatchOutcome::MissedToken),
        TokenAction::RejectedAfterCutoff => Ok(DispatchOutcome::MissedToken),
        TokenAction::RejectedAfterStop => Ok(DispatchOutcome::Stopped),
        TokenAction::AlreadyFinalized => Err("measurement token finalized before admission".into()),
    }
}

fn dispatch_periodic_item(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    event: &PeriodicEvent,
    sender: &mpsc::Sender<WorkItem>,
    tick_deadline: TokioInstant,
    _cutoff_deadline: TokioInstant,
    tick_end_ns: u64,
) -> Result<DispatchOutcome, String> {
    let identity = periodic_identity(event.kind);
    if TokioInstant::now() >= tick_deadline {
        return Ok(DispatchOutcome::Missed);
    }
    let token = match offer_measurement(recorder, identity)? {
        OfferedMeasurement::Token(token) => token,
        OfferedMeasurement::MissedSchedule => return Ok(DispatchOutcome::MissedToken),
        OfferedMeasurement::NotOfferedAfterStop => return Ok(DispatchOutcome::Stopped),
    };
    let item = match event.kind {
        PeriodicKind::NodeLease | PeriodicKind::NodeStatus => {
            let heartbeat = plan
                .heartbeat_item(event)
                .ok_or_else(|| "node periodic event has no heartbeat mapping".to_string())?;
            let resource = match event.kind {
                PeriodicKind::NodeLease => ResourceKind::NodeLeases,
                PeriodicKind::NodeStatus => ResourceKind::Nodes,
                PeriodicKind::Status | PeriodicKind::Compact => unreachable!(),
            };
            WorkItem::Heartbeat {
                item: heartbeat,
                resource,
                token,
            }
        }
        PeriodicKind::Status => WorkItem::Status { token },
        PeriodicKind::Compact => WorkItem::CompactTick { token },
    };
    let (operation, resource) = match event.kind {
        PeriodicKind::NodeLease => (Operation::Update, ResourceKind::NodeLeases),
        PeriodicKind::NodeStatus => (Operation::Update, ResourceKind::Nodes),
        PeriodicKind::Status => (Operation::Read, ResourceKind::Nodes),
        PeriodicKind::Compact => (Operation::Compact, ResourceKind::Nodes),
    };
    metrics::record_scheduled(operation, &resource_name(resource));
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(())) => {
            item.drop_for(DropReason::Backpressure)?;
            return Ok(DispatchOutcome::Dropped);
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            item.drop_for(DropReason::QueueClosed)?;
            return Err("measurement worker queue closed".into());
        }
    };
    match item.accept_for_tick(tick_end_ns)? {
        TokenAction::Applied => {
            permit.send(item);
            Ok(DispatchOutcome::Accepted)
        }
        TokenAction::RejectedAfterTick => Ok(DispatchOutcome::MissedToken),
        TokenAction::RejectedAfterCutoff => Ok(DispatchOutcome::MissedToken),
        TokenAction::RejectedAfterStop => Ok(DispatchOutcome::Stopped),
        TokenAction::AlreadyFinalized => Err("measurement token finalized before admission".into()),
    }
}

fn skip_application_before(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    state: &mut DispatchState,
    start_ns: u64,
) -> Result<(), String> {
    for (index, stream) in plan.logical_streams.iter().enumerate() {
        let target = planned_at(stream, start_ns)?;
        if target > state.next_ordinals[index] {
            record_missed(
                recorder,
                stream_identity(stream),
                target - state.next_ordinals[index],
            )?;
            state.next_ordinals[index] = target;
        }
    }
    Ok(())
}

fn periodic_event_lower_bound(plan: &CompiledPlan, at_ns: u64) -> usize {
    let mut low = 0;
    let mut high = plan.periodic_events.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if plan.periodic_events[middle].at_ns < at_ns {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

fn periodic_index_lower_bound(index: &[usize], position: usize) -> usize {
    let mut low = 0;
    let mut high = index.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if index[middle] < position {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

fn periodic_counts_between(
    plan: &CompiledPlan,
    start: usize,
    end: usize,
) -> Result<[u64; 4], String> {
    let mut counts = [0; 4];
    for kind in [
        PeriodicKind::NodeLease,
        PeriodicKind::NodeStatus,
        PeriodicKind::Status,
        PeriodicKind::Compact,
    ] {
        let index = &plan.periodic_indexes[kind.index()];
        let first = periodic_index_lower_bound(index, start);
        let last = periodic_index_lower_bound(index, end);
        counts[kind.index()] =
            u64::try_from(last.saturating_sub(first)).map_err(|_| "periodic count exceeds u64")?;
    }
    Ok(counts)
}

fn mark_periodic_range(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    state: &mut DispatchState,
    end: usize,
    missed: bool,
) -> Result<(), String> {
    let start = state.periodic_cursor.min(end);
    if start == end {
        state.periodic_cursor = end;
        return Ok(());
    }
    let counts = periodic_counts_between(plan, start, end)?;
    for kind in [
        PeriodicKind::NodeLease,
        PeriodicKind::NodeStatus,
        PeriodicKind::Status,
        PeriodicKind::Compact,
    ] {
        let count = counts[kind.index()];
        if count == 0 {
            continue;
        }
        if missed {
            record_missed(recorder, periodic_identity(kind), count)?;
        } else {
            record_not_offered(recorder, periodic_identity(kind), count)?;
        }
    }
    state.periodic_cursor = end;
    Ok(())
}

fn mark_application_range(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    state: &mut DispatchState,
    targets: &[u64],
    missed: bool,
) -> Result<(), String> {
    for (index, stream) in plan.logical_streams.iter().enumerate() {
        let remaining = targets[index].saturating_sub(state.next_ordinals[index]);
        if remaining == 0 {
            continue;
        }
        if missed {
            record_missed(recorder, stream_identity(stream), remaining)?;
        } else {
            record_not_offered(recorder, stream_identity(stream), remaining)?;
        }
        state.next_ordinals[index] = targets[index];
    }
    Ok(())
}

fn dispatch_tick(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    senders: &[mpsc::Sender<WorkItem>],
    state: &mut DispatchState,
    tick_start_ns: u64,
    tick_end_ns: u64,
    tick_deadline: TokioInstant,
    _cutoff_deadline: TokioInstant,
) -> Result<(), String> {
    let mut targets = Vec::with_capacity(plan.logical_streams.len());
    for (index, stream) in plan.logical_streams.iter().enumerate() {
        let start = planned_at(stream, tick_start_ns)?;
        let end = planned_at(stream, tick_end_ns)?;
        if state.next_ordinals[index] < start {
            record_missed(
                recorder,
                stream_identity(stream),
                start - state.next_ordinals[index],
            )?;
            state.next_ordinals[index] = start;
        }
        targets.push(end);
    }

    let periodic_start = periodic_event_lower_bound(plan, tick_start_ns);
    if state.periodic_cursor < periodic_start {
        mark_periodic_range(plan, recorder, state, periodic_start, true)?;
    }
    let periodic_end = periodic_event_lower_bound(plan, tick_end_ns);

    loop {
        if TokioInstant::now() >= tick_deadline {
            mark_application_range(plan, recorder, state, &targets, true)?;
            mark_periodic_range(plan, recorder, state, periodic_end, true)?;
            break;
        }

        let mut progressed = false;
        let mut stop_dispatch = false;
        for (index, stream) in plan.logical_streams.iter().enumerate() {
            if state.next_ordinals[index] >= targets[index] {
                continue;
            }
            progressed = true;
            let ordinal = state.next_ordinals[index];
            state.next_ordinals[index] = state.next_ordinals[index].saturating_add(1);
            let key = plan.resource_key(stream.resource, ordinal, plan.config.seed);
            let shard = stable_shard(&key, senders.len());
            match dispatch_application_item(
                plan,
                recorder,
                stream,
                ordinal,
                &senders[shard],
                tick_deadline,
                _cutoff_deadline,
                tick_end_ns,
            )? {
                DispatchOutcome::Accepted | DispatchOutcome::Dropped => {}
                DispatchOutcome::Missed => {
                    record_missed(recorder, stream_identity(stream), 1)?;
                    mark_application_range(plan, recorder, state, &targets, true)?;
                    mark_periodic_range(plan, recorder, state, periodic_end, true)?;
                    stop_dispatch = true;
                    break;
                }
                DispatchOutcome::MissedToken => {
                    mark_application_range(plan, recorder, state, &targets, true)?;
                    mark_periodic_range(plan, recorder, state, periodic_end, true)?;
                    stop_dispatch = true;
                    break;
                }
                DispatchOutcome::Stopped => {
                    mark_application_range(plan, recorder, state, &targets, false)?;
                    mark_periodic_range(plan, recorder, state, periodic_end, false)?;
                    stop_dispatch = true;
                    break;
                }
            }
        }
        if stop_dispatch {
            break;
        }

        if state.periodic_cursor < periodic_end {
            progressed = true;
            let event = &plan.periodic_events[state.periodic_cursor];
            let shard = plan
                .heartbeat_item(event)
                .map(|item| stable_shard(item.key.as_bytes(), senders.len()))
                .unwrap_or(0);
            match dispatch_periodic_item(
                plan,
                recorder,
                event,
                &senders[shard],
                tick_deadline,
                _cutoff_deadline,
                tick_end_ns,
            )? {
                DispatchOutcome::Accepted | DispatchOutcome::Dropped => {
                    state.periodic_cursor += 1;
                }
                DispatchOutcome::Missed => {
                    record_missed(recorder, periodic_identity(event.kind), 1)?;
                    state.periodic_cursor += 1;
                    mark_application_range(plan, recorder, state, &targets, true)?;
                    mark_periodic_range(plan, recorder, state, periodic_end, true)?;
                    break;
                }
                DispatchOutcome::MissedToken => {
                    state.periodic_cursor += 1;
                    mark_application_range(plan, recorder, state, &targets, true)?;
                    mark_periodic_range(plan, recorder, state, periodic_end, true)?;
                    break;
                }
                DispatchOutcome::Stopped => {
                    state.periodic_cursor += 1;
                    mark_application_range(plan, recorder, state, &targets, false)?;
                    mark_periodic_range(plan, recorder, state, periodic_end, false)?;
                    break;
                }
            }
        }

        if !progressed {
            break;
        }
    }
    Ok(())
}
#[derive(Debug)]
enum ScheduleControl {
    Tick,
    Supervisor,
    Interrupted,
}

async fn wait_for_schedule_event(
    worker_tasks: &mut JoinSet<Result<(), String>>,
    watch_tasks: &mut JoinSet<WatchTaskOutput>,
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> Result<ScheduleControl, String> {
    tokio::select! {
        biased;
        _ = ctrl_c.as_mut() => Ok(ScheduleControl::Interrupted),
        worker_result = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
            match worker_result {
                Some(Ok(Ok(()))) => Err("worker exited before the run ended".into()),
                Some(Ok(Err(error))) => Err(error),
                Some(Err(error)) => Err(format!("worker task failed: {error}")),
                None => Err("all workers exited before the run ended".into()),
            }
        }
        watch_result = watch_tasks.join_next(), if !watch_tasks.is_empty() => {
            match watch_result {
                Some(Ok((_name, _result))) => Ok(ScheduleControl::Supervisor),
                Some(Err(error)) => Err(format!("watch task failed: {error}")),
                None => Err("all watch tasks exited before the run ended".into()),
            }
        }
        _ = time::sleep_until(deadline) => Ok(ScheduleControl::Tick),
    }
}
fn mark_remaining_missed(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    state: &mut DispatchState,
) -> Result<(), String> {
    for (index, stream) in plan.logical_streams.iter().enumerate() {
        let remaining = stream.planned.saturating_sub(state.next_ordinals[index]);
        record_missed(recorder, stream_identity(stream), remaining)?;
        state.next_ordinals[index] = stream.planned;
    }
    mark_periodic_range(plan, recorder, state, plan.periodic_events.len(), true)
}

fn mark_remaining_not_offered(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    state: &mut DispatchState,
) -> Result<(), String> {
    for (index, stream) in plan.logical_streams.iter().enumerate() {
        let remaining = stream.planned.saturating_sub(state.next_ordinals[index]);
        record_not_offered(recorder, stream_identity(stream), remaining)?;
        state.next_ordinals[index] = stream.planned;
    }
    mark_periodic_range(plan, recorder, state, plan.periodic_events.len(), false)
}

enum DispatchExit {
    Completed(DispatchState),
    Interrupted(DispatchState),
    Failed(String, DispatchState),
}

async fn dispatch_until(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    senders: &[mpsc::Sender<WorkItem>],
    worker_tasks: &mut JoinSet<Result<(), String>>,
    watch_tasks: &mut JoinSet<WatchTaskOutput>,
    started: TokioInstant,
    cutoff: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
) -> DispatchExit {
    let duration_ns = match nanos(run_duration(plan)) {
        Ok(value) => value,
        Err(error) => return DispatchExit::Failed(error, DispatchState::new(plan)),
    };
    let tick_ns = nanos(crate::plan::PACING_TICK).expect("1ms fits u64 nanoseconds");
    let mut state = DispatchState::new(plan);

    while state.next_tick.saturating_mul(tick_ns) < duration_ns {
        let mut tick_start_ns = state.next_tick.saturating_mul(tick_ns);
        let mut tick_end_ns = tick_start_ns.saturating_add(tick_ns).min(duration_ns);
        let mut tick_deadline = started + Duration::from_nanos(tick_end_ns);
        let wait_deadline = started + Duration::from_nanos(tick_start_ns);
        loop {
            match wait_for_schedule_event(
                worker_tasks,
                watch_tasks,
                wait_deadline.min(cutoff),
                ctrl_c,
            )
            .await
            {
                Ok(ScheduleControl::Interrupted) => {
                    return DispatchExit::Interrupted(state);
                }
                Ok(ScheduleControl::Supervisor) if TokioInstant::now() < wait_deadline => {
                    continue;
                }
                Ok(ScheduleControl::Supervisor | ScheduleControl::Tick) => break,
                Err(error) => return DispatchExit::Failed(error, state),
            }
        }

        let elapsed_ns =
            nanos(TokioInstant::now().saturating_duration_since(started)).unwrap_or(u64::MAX);
        if elapsed_ns >= duration_ns {
            return match mark_remaining_missed(plan, recorder, &mut state) {
                Ok(()) => DispatchExit::Completed(state),
                Err(error) => DispatchExit::Failed(error, state),
            };
        }
        let current_tick = elapsed_ns / tick_ns;
        if current_tick > state.next_tick {
            let skipped_start_ns = current_tick.saturating_mul(tick_ns).min(duration_ns);
            if let Err(error) =
                skip_application_before(plan, recorder, &mut state, skipped_start_ns)
            {
                return DispatchExit::Failed(error, state);
            }
            state.next_tick = current_tick;
            tick_start_ns = skipped_start_ns;
            tick_end_ns = tick_start_ns.saturating_add(tick_ns).min(duration_ns);
            tick_deadline = started + Duration::from_nanos(tick_end_ns);
        }
        if let Err(error) = dispatch_tick(
            plan,
            recorder,
            senders,
            &mut state,
            tick_start_ns,
            tick_end_ns,
            tick_deadline,
            cutoff,
        ) {
            return DispatchExit::Failed(error, state);
        }
        state.next_tick = state.next_tick.saturating_add(1);
    }
    loop {
        if TokioInstant::now() >= cutoff {
            break;
        }
        match wait_for_schedule_event(worker_tasks, watch_tasks, cutoff, ctrl_c).await {
            Ok(ScheduleControl::Interrupted) => return DispatchExit::Interrupted(state),
            Ok(ScheduleControl::Supervisor | ScheduleControl::Tick) => {}
            Err(error) => return DispatchExit::Failed(error, state),
        }
    }
    match mark_remaining_missed(plan, recorder, &mut state) {
        Ok(()) => DispatchExit::Completed(state),
        Err(error) => DispatchExit::Failed(error, state),
    }
}
async fn worker_loop(
    mut receiver: mpsc::Receiver<WorkItem>,
    client: ClientBundle,
    plan: Arc<CompiledPlan>,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Result<(), String> {
    let mut states = HashMap::<Vec<u8>, KeyState>::new();
    let mut compactor = CompactorState::default();
    while let Some(item) = receiver.recv().await {
        let preparation = item.preparation().cloned();
        item.started()?;
        let (outcome, error_message) = match &item {
            WorkItem::Txn {
                resource,
                key,
                operation,
                value,
                ..
            } => {
                let label = resource_name(*resource);
                let outcome = execute_txn(
                    &client,
                    &label,
                    &mut states,
                    key.clone(),
                    value.clone(),
                    *operation,
                    CasRetryPolicy::until_deadline(),
                    true,
                    timeout,
                    latest_revision.clone(),
                )
                .await;
                (outcome, None)
            }
            WorkItem::Range {
                request, selector, ..
            } => match execute_range(
                &client,
                request.clone(),
                *selector,
                timeout,
                latest_revision.clone(),
            )
            .await
            {
                Ok(_) => (Outcome::Success, None),
                Err(error) => (error.outcome, Some(error.message)),
            },
            WorkItem::Heartbeat { item, resource, .. } => {
                let outcome = execute_heartbeat(
                    &client,
                    &plan,
                    &mut states,
                    item.clone(),
                    *resource,
                    timeout,
                    latest_revision.clone(),
                )
                .await;
                (outcome, None)
            }
            WorkItem::Status { .. } => (
                execute_status(&client, timeout, latest_revision.clone()).await,
                None,
            ),
            WorkItem::CompactTick { .. } => (
                execute_compact_tick(&client, &mut compactor, timeout, latest_revision.clone())
                    .await,
                None,
            ),
        };
        if let Some(message) = error_message {
            item.terminal_with_error(outcome, 0, &message)?;
        } else {
            item.terminal(outcome)?;
        }
        if let Some(preparation) = preparation {
            preparation.complete(outcome);
        }
    }
    Ok(())
}
fn resource_value_for_retry(value: &[u8], attempt: u32) -> Vec<u8> {
    let mut retry = value.to_vec();
    if let Some(first) = retry.first_mut() {
        *first = first.wrapping_add((attempt & 0xff) as u8);
    }
    retry
}

async fn execute_txn(
    client: &ClientBundle,
    label: &ResourceName,
    states: &mut HashMap<Vec<u8>, KeyState>,
    key: Vec<u8>,
    value: Vec<u8>,
    operation: ResourceOperation,
    cas_policy: CasRetryPolicy,
    recreate_missing: bool,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let deadline = Instant::now() + timeout;
    let mut effective_operation = match states.get(&key) {
        Some(state) => storage_operation_for_state(operation, state, recreate_missing),
        None if operation == ResourceOperation::Create => Some(ResourceOperation::Create),
        None if recreate_missing => Some(ResourceOperation::Create),
        None => None,
    };
    let Some(mut effective_operation_value) = effective_operation.take() else {
        metrics::record_operation(operation_metric(operation), label, Outcome::NotFound);
        return Outcome::NotFound;
    };
    let mut storage_attempts = 0;

    loop {
        let expected = states
            .get(&key)
            .and_then(KeyState::mod_revision)
            .unwrap_or_default();
        let attempt = match effective_operation_value {
            ResourceOperation::Create => TxnAttempt::create(key.clone(), value.clone()),
            ResourceOperation::GuaranteedUpdate => {
                TxnAttempt::update(key.clone(), value.clone(), expected)
            }
            ResourceOperation::ConditionalDelete => TxnAttempt::delete(key.clone(), expected),
        };
        let mut cas = CasLoop::new(attempt, cas_policy, deadline);
        let next_operation = loop {
            if !take_storage_attempt(&mut storage_attempts, cas_policy.max_attempts) {
                metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                return Outcome::Timeout;
            }
            let request = cas.request();
            let start = Instant::now();
            let remaining = deadline.saturating_duration_since(start);
            if remaining.is_zero() {
                metrics::record_txn(label, shape(effective_operation_value), TxnResult::Conflict);
                metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                return Outcome::Timeout;
            }
            metrics::add_inflight(Rpc::Txn);
            let result = time::timeout(remaining, client.txn(request)).await;
            metrics::remove_inflight(Rpc::Txn);
            metrics::record_request_duration(Rpc::Txn, start.elapsed());
            metrics::record_request_bytes(label, key.len() + value.len());
            let response = match result {
                Ok(Ok(response)) => {
                    metrics::record_request(Rpc::Txn, Outcome::Success);
                    response
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
                    metrics::record_txn(
                        label,
                        shape(effective_operation_value),
                        TxnResult::Conflict,
                    );
                    metrics::record_operation(operation_metric(operation), label, outcome);
                    return outcome;
                }
                Err(_) => {
                    metrics::record_request(Rpc::Txn, Outcome::ResultUnknown);
                    metrics::record_txn(
                        label,
                        shape(effective_operation_value),
                        TxnResult::Conflict,
                    );
                    metrics::record_operation(
                        operation_metric(operation),
                        label,
                        Outcome::ResultUnknown,
                    );
                    return Outcome::ResultUnknown;
                }
            };
            if let Some(header) = response.header.as_ref() {
                update_revision(&latest_revision, header.revision);
            }
            match cas.observe(&response, Instant::now()) {
                CasDecision::Succeeded {
                    header_revision: _,
                    state,
                } => {
                    *states.entry(key.clone()).or_default() = state;
                    metrics::record_txn(
                        label,
                        shape(effective_operation_value),
                        TxnResult::Committed,
                    );
                    metrics::record_operation(operation_metric(operation), label, Outcome::Success);
                    return Outcome::Success;
                }
                CasDecision::CreateCollision {
                    header_revision: _,
                    latest,
                } => {
                    *states.entry(key.clone()).or_default() = latest.clone();
                    metrics::record_txn(
                        label,
                        shape(effective_operation_value),
                        TxnResult::Conflict,
                    );
                    if operation == ResourceOperation::GuaranteedUpdate
                        && latest.mod_revision().is_some()
                    {
                        break Some(ResourceOperation::GuaranteedUpdate);
                    }
                    // A failed canonical Create is a conflict even when the
                    // server omitted its failure KV.
                    metrics::record_operation(
                        operation_metric(operation),
                        label,
                        Outcome::Conflict,
                    );
                    return Outcome::Conflict;
                }
                CasDecision::Retry {
                    header_revision: _,
                    latest,
                    next_attempt,
                } => {
                    *states.entry(key.clone()).or_default() = latest.clone();
                    metrics::record_txn(
                        label,
                        shape(effective_operation_value),
                        TxnResult::Conflict,
                    );
                    let retry_value = resource_value_for_retry(&value, next_attempt);
                    if cas.retry_with_value(retry_value, &latest).is_none() {
                        metrics::record_operation(
                            operation_metric(operation),
                            label,
                            Outcome::NotFound,
                        );
                        return Outcome::NotFound;
                    }
                }
                CasDecision::Exhausted {
                    header_revision: _,
                    latest,
                    reason,
                } => {
                    *states.entry(key.clone()).or_default() = latest;
                    metrics::record_txn(
                        label,
                        shape(effective_operation_value),
                        TxnResult::Conflict,
                    );
                    if reason == ExhaustedReason::MissingFailureKey
                        && recreate_missing
                        && operation == ResourceOperation::GuaranteedUpdate
                    {
                        break Some(ResourceOperation::Create);
                    }
                    let outcome = match reason {
                        ExhaustedReason::CancelledOrDeadline => Outcome::Timeout,
                        ExhaustedReason::MissingFailureKey => Outcome::NotFound,
                        ExhaustedReason::RetryPolicy => Outcome::Conflict,
                    };
                    metrics::record_operation(operation_metric(operation), label, outcome);
                    return outcome;
                }
            }
        };
        if let Some(next_operation) = next_operation {
            effective_operation_value = next_operation;
        } else {
            return Outcome::Conflict;
        }
    }
}
async fn execute_heartbeat(
    client: &ClientBundle,
    plan: &CompiledPlan,
    states: &mut HashMap<Vec<u8>, KeyState>,
    item: HeartbeatItem,
    resource: ResourceKind,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let key = item.key.into_bytes();
    let sequence = u64::try_from(item.at.as_nanos()).unwrap_or(u64::MAX);
    let value = plan.resource(resource).value(item.node as u64, sequence);
    let label = resource_name(resource);
    let outcome = execute_txn(
        client,
        &label,
        states,
        key,
        value,
        ResourceOperation::GuaranteedUpdate,
        CasRetryPolicy::until_deadline(),
        true,
        timeout,
        latest_revision,
    )
    .await;
    let heartbeat_kind = match item.kind {
        HeartbeatItemKind::NodeLeaseRenew => HeartbeatKind::NodeLease,
        HeartbeatItemKind::NodeStatus => HeartbeatKind::NodeStatus,
    };
    metrics::record_node_heartbeat(heartbeat_kind, outcome);
    outcome
}

pub async fn run(config: RunConfig, client: ClientBundle) -> RunResult {
    run_with_ctrl_c(config, client, Box::pin(tokio::signal::ctrl_c())).await
}

async fn run_with_ctrl_c(
    config: RunConfig,
    client: ClientBundle,
    mut ctrl_c: CtrlCFuture,
) -> RunResult {
    let plan = Arc::new(config.plan);
    let request_timeout = request_timeout(&plan);
    let status = match time::timeout(request_timeout, client.status()).await {
        Ok(Ok(response)) => {
            if let Some(header) = response.header.as_ref() {
                metrics::set_revision(header.revision);
            }
            Ok(())
        }
        Ok(Err(error)) => Err(format!("status preflight failed: {error}")),
        Err(_) => Err("status preflight timed out".into()),
    };
    if let Err(error) = status {
        return RunResult::failed(error);
    }
    let preflight = match time::timeout(request_timeout, client.preflight_count()).await {
        Ok(Ok(response)) if response.count == 0 => Ok(()),
        Ok(Ok(response)) => Err(format!(
            "preflight requires an empty keyspace (found {} keys)",
            response.count
        )),
        Ok(Err(error)) => Err(format!("preflight failed: {error}")),
        Err(_) => Err("preflight timed out".into()),
    };
    if let Err(error) = preflight {
        return RunResult::failed(error);
    }

    let recorder = match MeasurementRecorder::new(&plan) {
        Ok(recorder) => recorder,
        Err(error) => return RunResult::failed(format!("measurement setup failed: {error}")),
    };
    let (senders, mut worker_tasks) = spawn_workers(plan.clone(), &client);
    let preparation_deadline = TokioInstant::now() + preparation_timeout(&plan);
    if let Err(error) = run_preparation(
        plan.clone(),
        &recorder,
        &senders,
        preparation_deadline,
        &mut ctrl_c,
    )
    .await
    {
        abort_workers(senders, worker_tasks).await;
        return RunResult::failed(error);
    }

    let watch_specs = plan.watch_specs();
    let (watch_shutdown, mut watch_tasks, ready_receivers) = spawn_watches(&client, &watch_specs);
    if let Err(error) = wait_for_watch_ready(
        &mut watch_tasks,
        ready_receivers,
        preparation_deadline,
        &mut ctrl_c,
    )
    .await
    {
        let _ = stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await;
        abort_workers(senders, worker_tasks).await;
        return RunResult::failed(error);
    }
    if TokioInstant::now() >= preparation_deadline {
        let _ = stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await;
        abort_workers(senders, worker_tasks).await;
        return RunResult::failed("preparation deadline exceeded");
    }

    let measurement_window = match recorder.start_measurement() {
        Ok(window) => window,
        Err(error) => {
            let _ = stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await;
            abort_workers(senders, worker_tasks).await;
            return RunResult::failed(format!("measurement start failed: {error}"));
        }
    };
    let dispatch_started = TokioInstant::from_std(measurement_window.start);
    let dispatch_cutoff = TokioInstant::from_std(measurement_window.cutoff);

    let (dispatch_error, interrupted, mut state) = match dispatch_until(
        &plan,
        &recorder,
        &senders,
        &mut worker_tasks,
        &mut watch_tasks,
        dispatch_started,
        dispatch_cutoff,
        &mut ctrl_c,
    )
    .await
    {
        DispatchExit::Completed(state) => (None, false, state),
        DispatchExit::Interrupted(state) => (None, true, state),
        DispatchExit::Failed(error, state) => (Some(error), false, state),
    };

    let _cutoff = match recorder.stop_measurement() {
        Ok(cutoff) => cutoff,
        Err(error) => {
            let _ = stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await;
            abort_workers(senders, worker_tasks).await;
            return RunResult::failed(format!("measurement stop failed: {error}"));
        }
    };
    // Slots not yet enqueued at an early stop are classified only after the
    // cutoff freezes. A normal window has already classified every slot as
    // accepted, dropped, or missed.
    if dispatch_error.is_some() || interrupted {
        if let Err(error) = mark_remaining_not_offered(&plan, &recorder, &mut state) {
            let _ = stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await;
            abort_workers(senders, worker_tasks).await;
            return RunResult::failed(error);
        }
    }

    let drain_error = drain_workers(senders, worker_tasks, drain_timeout(&plan)).await;
    let watch_error =
        stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await;
    let first_error = dispatch_error
        .or_else(|| drain_error.err())
        .or_else(|| watch_error.err());
    let report = match recorder.finish() {
        Ok(report) => report,
        Err(error) => return RunResult::failed(format!("measurement finish failed: {error}")),
    };
    if interrupted {
        RunResult::interrupted(first_error, Some(report))
    } else if let Some(error) = first_error {
        RunResult {
            status: RunStatus::Failed,
            error: Some(bound_error(error)),
            measurement: Some(report),
        }
    } else {
        RunResult::completed(report)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use tokio::net::TcpListener;
    use tokio::sync::{Notify, oneshot};
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::{Endpoint, Server};
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::config::SyntheticConfig;
    use crate::measurement::MonotonicClock;
    use crate::pb::etcdserverpb::kv_server::{Kv, KvServer};
    use crate::pb::etcdserverpb::maintenance_server::{Maintenance, MaintenanceServer};
    use crate::pb::etcdserverpb::watch_server::{Watch, WatchServer};
    use crate::pb::etcdserverpb::{
        AlarmRequest, AlarmResponse, CompactionRequest, CompactionResponse, DefragmentRequest,
        DefragmentResponse, DeleteRangeRequest, DeleteRangeResponse, HashKvRequest, HashKvResponse,
        HashRequest, HashResponse, MoveLeaderRequest, MoveLeaderResponse, PutRequest, PutResponse,
        RangeResponse, ResponseHeader, ResponseOp, SnapshotRequest, SnapshotResponse,
        StatusRequest, StatusResponse, TxnRequest, TxnResponse, WatchRequest, WatchResponse,
    };
    use crate::pb::mvccpb::KeyValue;
    use crate::plan::{CompiledPlan, DEFAULT_SYNTHETIC_CONFIG};

    fn small_plan() -> CompiledPlan {
        let mut config = SyntheticConfig::parse_json(DEFAULT_SYNTHETIC_CONFIG).unwrap();
        config.topology.node_count = 1;
        config.topology.pod_count = 2;
        config.topology.namespace_count = 1;
        config.controllers.daemonset_count = 1;
        config.controllers.pods_per_deployment = 1;
        config.load.duration_ms = 20;
        config.load.update_rps = 1_000;
        config.load.update_weights.pods = 1;
        config.load.update_weights.deployments = 0;
        config.load.update_weights.replicasets = 0;
        config.load.update_weights.daemonsets = 0;
        config.watch.subscriptions_per_root = 1;
        config.periodic.node_lease_interval_ms = 60_000;
        config.periodic.node_status_interval_ms = 60_000;
        config.periodic.status_interval_ms = 60_000;
        config.periodic.compact_interval_ms = 60_000;
        config.execution.workers = 1;
        config.execution.queue_capacity = 64;
        config.report.bucket_ms = 10;
        CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap()
    }

    fn controlled_run_plan() -> CompiledPlan {
        let mut config = small_plan().config;
        config.load.duration_ms = 200;
        CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap()
    }

    fn cancellation_run_plan() -> CompiledPlan {
        let mut config = controlled_run_plan().config;
        config.execution.queue_capacity = 1;
        CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap()
    }

    fn pending_ctrl_c() -> CtrlCFuture {
        Box::pin(std::future::pending::<Result<(), std::io::Error>>())
    }

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    struct FinalAcceptCutoffClock {
        origin: Instant,
        calls: AtomicUsize,
        armed: AtomicBool,
    }

    impl FinalAcceptCutoffClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                origin: Instant::now(),
                calls: AtomicUsize::new(0),
                armed: AtomicBool::new(false),
            })
        }

        fn arm_for_cutoff(&self) {
            self.calls.store(0, AtomicOrdering::Release);
            self.armed.store(true, AtomicOrdering::Release);
        }
    }

    impl MonotonicClock for FinalAcceptCutoffClock {
        fn now(&self) -> Instant {
            let call = self.calls.fetch_add(1, AtomicOrdering::AcqRel);
            let armed = self.armed.load(AtomicOrdering::Acquire);
            self.origin
                + if armed && call >= 2 {
                    Duration::from_millis(20)
                } else {
                    Duration::ZERO
                }
        }
    }

    fn range_page(revision: i64, keys: &[&[u8]], more: bool, count: i64) -> RangeResponse {
        RangeResponse {
            header: Some(ResponseHeader {
                revision,
                ..Default::default()
            }),
            kvs: keys
                .iter()
                .map(|key| KeyValue {
                    key: key.to_vec(),
                    ..Default::default()
                })
                .collect(),
            more,
            count,
            ..Default::default()
        }
    }

    fn compact_response(revision: i64, succeeded: bool, version: i64, value: &[u8]) -> TxnResponse {
        TxnResponse {
            header: Some(ResponseHeader {
                revision,
                ..Default::default()
            }),
            succeeded,
            responses: if succeeded {
                Vec::new()
            } else {
                vec![ResponseOp {
                    response: Some(
                        crate::pb::etcdserverpb::response_op::Response::ResponseRange(
                            RangeResponse {
                                kvs: vec![KeyValue {
                                    mod_revision: version,
                                    version,
                                    value: value.to_vec(),
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
    struct TxnGate {
        hold_first: AtomicBool,
        observed: Notify,
        release: Notify,
        inflight: Arc<AtomicUsize>,
    }

    impl TxnGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                hold_first: AtomicBool::new(true),
                observed: Notify::new(),
                release: Notify::new(),
                inflight: Arc::new(AtomicUsize::new(0)),
            })
        }
    }

    struct TxnInflightGuard {
        inflight: Arc<AtomicUsize>,
    }

    impl Drop for TxnInflightGuard {
        fn drop(&mut self) {
            self.inflight.fetch_sub(1, AtomicOrdering::AcqRel);
        }
    }

    #[derive(Clone)]
    struct WatchFixture {
        created: Arc<Notify>,
    }

    #[tonic::async_trait]
    impl Watch for WatchFixture {
        type WatchStream = ReceiverStream<Result<WatchResponse, Status>>;

        async fn watch(
            &self,
            request: Request<tonic::Streaming<WatchRequest>>,
        ) -> Result<Response<Self::WatchStream>, Status> {
            let mut requests = request.into_inner();
            let created = self.created.clone();
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                if requests.next().await.is_none() {
                    return;
                }
                created.notify_one();
                if sender
                    .send(Ok(WatchResponse {
                        created: true,
                        watch_id: 1,
                        header: Some(ResponseHeader {
                            revision: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                while requests.next().await.is_some() {}
            });
            Ok(Response::new(ReceiverStream::new(receiver)))
        }
    }

    #[derive(Clone)]
    struct TxnFixture {
        responses: Arc<StdMutex<VecDeque<Result<TxnResponse, Status>>>>,
        requests: Arc<StdMutex<Vec<TxnRequest>>>,
        compact_responses: Option<Arc<StdMutex<VecDeque<Result<CompactionResponse, Status>>>>>,
        compact_requests: Option<Arc<StdMutex<Vec<CompactionRequest>>>>,
        range_requests: Option<Arc<AtomicUsize>>,
        range_more: bool,
        prep_request_count: Option<usize>,
        load_observed: Option<Arc<Notify>>,
        gate: Option<Arc<TxnGate>>,
    }

    #[tonic::async_trait]
    impl Kv for TxnFixture {
        async fn range(
            &self,
            _request: Request<crate::pb::etcdserverpb::RangeRequest>,
        ) -> Result<Response<RangeResponse>, Status> {
            if let Some(requests) = &self.range_requests {
                requests.fetch_add(1, AtomicOrdering::AcqRel);
            }
            Ok(Response::new(RangeResponse {
                header: Some(ResponseHeader {
                    revision: 7,
                    ..Default::default()
                }),
                more: self.range_more,
                count: 0,
                ..Default::default()
            }))
        }

        async fn put(
            &self,
            _request: Request<PutRequest>,
        ) -> Result<Response<PutResponse>, Status> {
            Ok(Response::new(PutResponse::default()))
        }

        async fn delete_range(
            &self,
            _request: Request<DeleteRangeRequest>,
        ) -> Result<Response<DeleteRangeResponse>, Status> {
            Ok(Response::new(DeleteRangeResponse::default()))
        }

        async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
            let request = request.into_inner();
            let request_count = {
                let mut requests = self.requests.lock().unwrap();
                requests.push(request);
                requests.len()
            };
            if self
                .prep_request_count
                .is_some_and(|prep_count| request_count > prep_count)
            {
                if let Some(load_observed) = &self.load_observed {
                    load_observed.notify_one();
                }
            }
            if let Some(gate) = &self.gate {
                if gate.hold_first.swap(false, AtomicOrdering::AcqRel) {
                    gate.inflight.fetch_add(1, AtomicOrdering::AcqRel);
                    let _guard = TxnInflightGuard {
                        inflight: gate.inflight.clone(),
                    };
                    gate.observed.notify_one();
                    gate.release.notified().await;
                }
            }
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(TxnResponse {
                        succeeded: true,
                        ..Default::default()
                    })
                })?;
            Ok(Response::new(response))
        }

        async fn compact(
            &self,
            request: Request<CompactionRequest>,
        ) -> Result<Response<CompactionResponse>, Status> {
            if let Some(requests) = &self.compact_requests {
                requests.lock().unwrap().push(request.into_inner());
            }
            let response = self
                .compact_responses
                .as_ref()
                .and_then(|responses| responses.lock().unwrap().pop_front())
                .unwrap_or_else(|| Ok(CompactionResponse::default()))?;
            Ok(Response::new(response))
        }
    }

    async fn serve_txn_fixture(
        responses: Vec<Result<TxnResponse, Status>>,
    ) -> (
        ClientBundle,
        Arc<StdMutex<Vec<TxnRequest>>>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let fixture = TxnFixture {
            responses: Arc::new(StdMutex::new(responses.into_iter().collect())),
            requests: requests.clone(),
            compact_responses: None,
            compact_requests: None,
            range_requests: None,
            range_more: true,
            prep_request_count: None,
            load_observed: None,
            gate: None,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::new(fixture))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (
            ClientBundle::from_channel(channel, 64 * 1024 * 1024),
            requests,
            shutdown_tx,
            server,
        )
    }

    async fn serve_compact_fixture(
        txn_responses: Vec<Result<TxnResponse, Status>>,
        compact_responses: Vec<Result<CompactionResponse, Status>>,
    ) -> (
        ClientBundle,
        Arc<StdMutex<Vec<TxnRequest>>>,
        Arc<StdMutex<Vec<CompactionRequest>>>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let compact_requests = Arc::new(StdMutex::new(Vec::new()));
        let fixture = TxnFixture {
            responses: Arc::new(StdMutex::new(txn_responses.into_iter().collect())),
            requests: requests.clone(),
            compact_responses: Some(Arc::new(StdMutex::new(
                compact_responses.into_iter().collect(),
            ))),
            compact_requests: Some(compact_requests.clone()),
            range_requests: None,
            range_more: true,
            prep_request_count: None,
            load_observed: None,
            gate: None,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::new(fixture))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (
            ClientBundle::from_channel(channel, 64 * 1024 * 1024),
            requests,
            compact_requests,
            shutdown_tx,
            server,
        )
    }

    #[derive(Clone)]
    struct MaintenanceFixture {
        status_error: Option<String>,
    }

    #[tonic::async_trait]
    impl Maintenance for MaintenanceFixture {
        type SnapshotStream = tokio_stream::Empty<Result<SnapshotResponse, Status>>;

        async fn alarm(
            &self,
            _request: Request<AlarmRequest>,
        ) -> Result<Response<AlarmResponse>, Status> {
            Ok(Response::new(AlarmResponse::default()))
        }

        async fn status(
            &self,
            _request: Request<StatusRequest>,
        ) -> Result<Response<StatusResponse>, Status> {
            if let Some(error) = &self.status_error {
                return Err(Status::unavailable(error.clone()));
            }
            Ok(Response::new(StatusResponse {
                header: Some(ResponseHeader {
                    revision: 1,
                    ..Default::default()
                }),
                ..Default::default()
            }))
        }

        async fn defragment(
            &self,
            _request: Request<DefragmentRequest>,
        ) -> Result<Response<DefragmentResponse>, Status> {
            Ok(Response::new(DefragmentResponse::default()))
        }

        async fn hash(
            &self,
            _request: Request<HashRequest>,
        ) -> Result<Response<HashResponse>, Status> {
            Ok(Response::new(HashResponse::default()))
        }

        async fn hash_kv(
            &self,
            _request: Request<HashKvRequest>,
        ) -> Result<Response<HashKvResponse>, Status> {
            Ok(Response::new(HashKvResponse::default()))
        }

        async fn snapshot(
            &self,
            _request: Request<SnapshotRequest>,
        ) -> Result<Response<Self::SnapshotStream>, Status> {
            Ok(Response::new(tokio_stream::empty()))
        }

        async fn move_leader(
            &self,
            _request: Request<MoveLeaderRequest>,
        ) -> Result<Response<MoveLeaderResponse>, Status> {
            Ok(Response::new(MoveLeaderResponse::default()))
        }
    }

    async fn serve_run_fixture(
        txn_responses: Vec<Result<TxnResponse, Status>>,
        status_error: Option<String>,
    ) -> (
        ClientBundle,
        Arc<StdMutex<Vec<TxnRequest>>>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let fixture = TxnFixture {
            responses: Arc::new(StdMutex::new(txn_responses.into_iter().collect())),
            requests: requests.clone(),
            compact_responses: None,
            compact_requests: None,
            range_requests: None,
            range_more: true,
            prep_request_count: None,
            load_observed: None,
            gate: None,
        };
        let maintenance = MaintenanceFixture { status_error };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::new(fixture))
                .add_service(MaintenanceServer::new(maintenance))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (
            ClientBundle::from_channel(channel, 64 * 1024 * 1024),
            requests,
            shutdown_tx,
            server,
        )
    }

    async fn serve_controlled_run_fixture(
        prep_request_count: usize,
    ) -> (
        ClientBundle,
        Arc<StdMutex<Vec<TxnRequest>>>,
        Arc<AtomicUsize>,
        Arc<TxnGate>,
        Arc<Notify>,
        Arc<Notify>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let range_requests = Arc::new(AtomicUsize::new(0));
        let gate = TxnGate::new();
        let created = Arc::new(Notify::new());
        let load_observed = Arc::new(Notify::new());
        let fixture = TxnFixture {
            responses: Arc::new(StdMutex::new(VecDeque::new())),
            requests: requests.clone(),
            compact_responses: None,
            compact_requests: None,
            range_requests: Some(range_requests.clone()),
            range_more: false,
            prep_request_count: Some(prep_request_count),
            load_observed: Some(load_observed.clone()),
            gate: Some(gate.clone()),
        };
        let maintenance = MaintenanceFixture { status_error: None };
        let watch = WatchFixture {
            created: created.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(KvServer::new(fixture))
                .add_service(MaintenanceServer::new(maintenance))
                .add_service(WatchServer::new(watch))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (
            ClientBundle::from_channel(channel, 64 * 1024 * 1024),
            requests,
            range_requests,
            gate,
            created,
            load_observed,
            shutdown_tx,
            server,
        )
    }

    fn run_config(plan: CompiledPlan) -> crate::config::RunConfig {
        crate::config::RunConfig {
            plan,
            endpoint: "http://fixture".into(),
            metrics_listen: "127.0.0.1:0".parse().unwrap(),
            max_message_size: 64 * 1024 * 1024,
        }
    }

    fn txn_put_key(request: &TxnRequest) -> Option<Vec<u8>> {
        request
            .success
            .iter()
            .find_map(|operation| match operation.request.as_ref()? {
                crate::pb::etcdserverpb::request_op::Request::RequestPut(put) => {
                    Some(put.key.clone())
                }
                _ => None,
            })
    }

    #[tokio::test]
    async fn test_run_with_ctrl_c_create_failure_withholds_measurement() {
        let plan = small_plan();
        let first_create_failure = TxnResponse {
            header: Some(ResponseHeader {
                revision: 2,
                ..Default::default()
            }),
            succeeded: false,
            ..Default::default()
        };
        let (client, requests, shutdown, server) =
            serve_run_fixture(vec![Ok(first_create_failure)], None).await;
        let result = run_with_ctrl_c(run_config(plan.clone()), client, pending_ctrl_c()).await;
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.measurement.is_none());
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("preparation outcome: conflict"))
        );
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            usize::try_from(preparation_total(&plan)).unwrap()
        );
        assert_eq!(
            requests[0].compare[0].target_union,
            Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                0
            ))
        );
        drop(requests);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_run_with_ctrl_c_bootstrap_unknown_no_replay() {
        let plan = small_plan();
        let prep_count = usize::try_from(preparation_total(&plan)).unwrap();
        let first_pod_key = plan.resource(ResourceKind::Pods).key(0);
        let (client, requests, shutdown, server) = serve_run_fixture(
            vec![Err(Status::unavailable("bootstrap response lost"))],
            None,
        )
        .await;
        let result = run_with_ctrl_c(run_config(plan.clone()), client, pending_ctrl_c()).await;
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.measurement.is_none());
        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("preparation outcome: result_unknown"))
        );
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| txn_put_key(request).as_deref() == Some(first_pod_key.as_slice()))
                .count(),
            1
        );
        assert_eq!(requests.len(), prep_count);
        drop(requests);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_run_with_ctrl_c_preparation_cancel_withholds_measurement() {
        let plan = small_plan();
        let (client, requests, shutdown, server) = serve_run_fixture(Vec::new(), None).await;
        let result = run_with_ctrl_c(
            run_config(plan),
            client,
            Box::pin(async { Ok::<(), std::io::Error>(()) }),
        )
        .await;
        assert_eq!(result.status, RunStatus::Failed);
        assert!(result.measurement.is_none());
        assert!(requests.lock().unwrap().is_empty());
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_run_fixture_holds_preparation_before_watch_and_load() {
        let plan = controlled_run_plan();
        let prep_count = usize::try_from(preparation_total(&plan)).unwrap();
        let (client, requests, range_requests, gate, created, load_observed, shutdown, server) =
            serve_controlled_run_fixture(prep_count).await;
        let observed = gate.observed.notified();
        let created_ready = created.notified();
        let load_ready = load_observed.notified();
        let (cancel, receiver) = oneshot::channel();
        let ctrl_c: CtrlCFuture = Box::pin(async move {
            receiver
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "cancel sender dropped")
                })
                .map(|_| ())
        });
        let run = tokio::spawn(run_with_ctrl_c(run_config(plan.clone()), client, ctrl_c));

        observed.await;
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 1);
        assert_eq!(range_requests.load(AtomicOrdering::Acquire), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);

        gate.release.notify_one();
        created_ready.await;
        let requests_before_load = requests.lock().unwrap();
        assert_eq!(requests_before_load.len(), prep_count);
        for kind in ResourceKind::ALL {
            if resource_count(&plan, kind) > 0 {
                let key = plan.resource(kind).key(0);
                assert!(
                    requests_before_load
                        .iter()
                        .any(|request| txn_put_key(request).as_deref() == Some(key.as_slice()))
                );
            }
        }
        drop(requests_before_load);

        load_ready.await;
        let requests_after_load = requests.lock().unwrap();
        assert!(requests_after_load.len() > prep_count);
        assert!(
            requests_after_load
                .iter()
                .skip(prep_count)
                .any(|request| txn_put_key(request).is_some())
        );
        drop(requests_after_load);
        cancel.send(()).unwrap();
        let result = run.await.unwrap();
        assert_eq!(result.status, RunStatus::Interrupted);
        assert!(result.measurement.is_some());
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 0);

        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_run_fixture_cancellation_aborts_inflight_preparation() {
        let plan = cancellation_run_plan();
        let prep_count = usize::try_from(preparation_total(&plan)).unwrap();
        let (client, requests, range_requests, gate, _created, _load_observed, shutdown, server) =
            serve_controlled_run_fixture(prep_count).await;
        let observed = gate.observed.notified();
        let (cancel, receiver) = oneshot::channel();
        let ctrl_c: CtrlCFuture = Box::pin(async move {
            receiver
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "cancel sender dropped")
                })
                .map(|_| ())
        });
        let run = tokio::spawn(run_with_ctrl_c(run_config(plan), client, ctrl_c));

        observed.await;
        assert_eq!(range_requests.load(AtomicOrdering::Acquire), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);
        cancel.send(()).unwrap();
        let result = run.await.unwrap();
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(
            result.error.as_deref(),
            Some("interrupted during preparation")
        );
        assert!(result.measurement.is_none());
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.inflight.load(AtomicOrdering::Acquire) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);

        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    struct DropSentinel(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for DropSentinel {
        fn drop(&mut self) {
            self.0.store(true, AtomicOrdering::Release);
        }
    }

    #[test]
    fn test_cas_retry_refreshes_revision() {
        let mut cas = CasLoop::new(
            TxnAttempt::update(b"k", b"v", 1),
            CasRetryPolicy::until_deadline(),
            Instant::now() + Duration::from_secs(1),
        );
        let decision = cas.observe(&compact_response(10, false, 2, b"latest"), Instant::now());
        let CasDecision::Retry { latest, .. } = decision else {
            panic!("expected CAS retry");
        };
        let attempt = cas.retry_with_value(b"recomputed", &latest).unwrap();
        assert_eq!(attempt.expected_mod_revision, 2);
        assert_eq!(
            attempt.request().compare[0].target_union,
            Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                2
            ))
        );
    }

    #[test]
    fn test_cas_collision_and_expired_not_sent() {
        let mut create = CasLoop::new(
            TxnAttempt::create(b"k", b"v"),
            CasRetryPolicy::until_deadline(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(matches!(
            create.observe(&compact_response(3, false, 1, b"existing"), Instant::now()),
            CasDecision::CreateCollision { .. }
        ));

        let mut expired = CasLoop::new(
            TxnAttempt::update(b"k", b"v", 1),
            CasRetryPolicy::until_deadline(),
            Instant::now() - Duration::from_secs(1),
        );
        assert!(matches!(
            expired.observe(&compact_response(4, false, 2, b"latest"), Instant::now()),
            CasDecision::Exhausted {
                reason: ExhaustedReason::CancelledOrDeadline,
                ..
            }
        ));
    }

    #[test]
    fn test_cas_missing_failure_key_is_terminal() {
        let mut cas = CasLoop::new(
            TxnAttempt::update(b"k", b"v", 1),
            CasRetryPolicy::until_deadline(),
            Instant::now() + Duration::from_secs(1),
        );
        let response = TxnResponse {
            header: Some(ResponseHeader {
                revision: 5,
                ..Default::default()
            }),
            succeeded: false,
            ..Default::default()
        };
        assert!(matches!(
            cas.observe(&response, Instant::now()),
            CasDecision::Exhausted {
                reason: ExhaustedReason::MissingFailureKey,
                ..
            }
        ));
    }

    #[test]
    fn test_update_missing_recreates_as_create() {
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

    #[tokio::test]
    async fn test_execute_txn_missing_failure_recreates_via_real_rpc() {
        let missing = TxnResponse {
            header: Some(ResponseHeader {
                revision: 11,
                ..Default::default()
            }),
            succeeded: false,
            ..Default::default()
        };
        let committed = TxnResponse {
            header: Some(ResponseHeader {
                revision: 12,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Ok(missing), Ok(committed)]).await;
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 10 })]);
        let outcome = execute_txn(
            &client,
            &ResourceName::new("pods").unwrap(),
            &mut states,
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        assert_eq!(outcome, Outcome::Success);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].failure.len(), 1);
        assert!(requests[1].failure.is_empty());
        drop(requests);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_txn_retry_sends_refreshed_revision_and_value() {
        let conflict = compact_response(10, false, 2, b"latest");
        let committed = TxnResponse {
            header: Some(ResponseHeader {
                revision: 12,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Ok(conflict), Ok(committed)]).await;
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 1 })]);
        let outcome = execute_txn(
            &client,
            &ResourceName::new("pods").unwrap(),
            &mut states,
            key.clone(),
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        assert_eq!(outcome, Outcome::Success);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].compare[0].target_union,
            Some(crate::pb::etcdserverpb::compare::TargetUnion::ModRevision(
                2
            ))
        );
        let Some(crate::pb::etcdserverpb::request_op::Request::RequestPut(put)) =
            requests[1].success[0].request.as_ref()
        else {
            panic!("retry request did not contain a Put");
        };
        assert_eq!(put.value, b"xalue");
        assert_eq!(states.get(&key).and_then(KeyState::mod_revision), Some(12));
        drop(requests);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_txn_create_collision_without_failure_key_is_conflict() {
        let collision = TxnResponse {
            header: Some(ResponseHeader {
                revision: 11,
                ..Default::default()
            }),
            succeeded: false,
            ..Default::default()
        };
        let (client, requests, shutdown, server) = serve_txn_fixture(vec![Ok(collision)]).await;
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let outcome = execute_txn(
            &client,
            &ResourceName::new("pods").unwrap(),
            &mut HashMap::new(),
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        assert_eq!(outcome, Outcome::Conflict);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_txn_unknown_response_is_not_replayed() {
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Err(Status::unavailable("response lost"))]).await;
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 10 })]);
        let outcome = execute_txn(
            &client,
            &ResourceName::new("pods").unwrap(),
            &mut states,
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        assert_eq!(outcome, Outcome::ResultUnknown);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_txn_expired_deadline_sends_no_rpc() {
        let (client, requests, shutdown, server) = serve_txn_fixture(Vec::new()).await;
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let outcome = execute_txn(
            &client,
            &ResourceName::new("pods").unwrap(),
            &mut HashMap::new(),
            key,
            b"value".to_vec(),
            ResourceOperation::Create,
            CasRetryPolicy::until_deadline(),
            false,
            Duration::ZERO,
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        assert_eq!(outcome, Outcome::Timeout);
        assert!(requests.lock().unwrap().is_empty());
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_heartbeat_unknown_response_is_not_replayed() {
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Err(Status::unavailable("heartbeat response lost"))]).await;
        let mut states = HashMap::new();
        let outcome = execute_heartbeat(
            &client,
            &small_plan(),
            &mut states,
            HeartbeatItem {
                node: 0,
                kind: HeartbeatItemKind::NodeLeaseRenew,
                key: "/registry/leases/node-00000".into(),
                at: Duration::ZERO,
            },
            ResourceKind::NodeLeases,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await;
        assert_eq!(outcome, Outcome::ResultUnknown);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_range_invalid_page_via_real_rpc() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(Vec::new()).await;
        let error = execute_range(
            &client,
            RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                limit: 500,
                ..Default::default()
            },
            RangeSelector::All { target: None },
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await
        .unwrap_err();
        assert_eq!(error.outcome, Outcome::GrpcError);
        assert!(error.message.contains("more=true"));
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_compact_tick_runs_coordination_and_compact_via_real_rpc() {
        let first = TxnResponse {
            header: Some(ResponseHeader {
                revision: 1,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let second = TxnResponse {
            header: Some(ResponseHeader {
                revision: 2,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Ok(first), Ok(second)]).await;
        let mut compactor = CompactorState::default();
        let revision = Arc::new(AtomicI64::new(0));
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                revision.clone()
            )
            .await,
            Outcome::Success
        );
        assert_eq!(
            execute_compact_tick(&client, &mut compactor, Duration::from_secs(1), revision).await,
            Outcome::Success
        );
        assert_eq!(requests.lock().unwrap().len(), 2);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_compact_first_tick_noop_sends_no_compact_rpc() {
        let response = TxnResponse {
            header: Some(ResponseHeader {
                revision: 1,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let (client, txn_requests, compact_requests, shutdown, server) =
            serve_compact_fixture(vec![Ok(response)], Vec::new()).await;
        let mut compactor = CompactorState::default();
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                Arc::new(AtomicI64::new(0)),
            )
            .await,
            Outcome::Success
        );
        assert_eq!(txn_requests.lock().unwrap().len(), 1);
        assert!(compact_requests.lock().unwrap().is_empty());
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_compact_conflict_sends_no_compact_rpc_or_retry() {
        let (client, txn_requests, compact_requests, shutdown, server) =
            serve_compact_fixture(vec![Ok(compact_response(6, false, 2, b"7"))], Vec::new()).await;
        let mut compactor = CompactorState {
            compact_version: 1,
            previous_txn_header_revision: 4,
            status: CompactorStatus::Active,
            recovery: None,
        };
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                Arc::new(AtomicI64::new(0)),
            )
            .await,
            Outcome::Success
        );
        assert_eq!(txn_requests.lock().unwrap().len(), 1);
        assert!(compact_requests.lock().unwrap().is_empty());
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_compact_rpc_unknown_is_not_replayed() {
        let coordination = TxnResponse {
            header: Some(ResponseHeader {
                revision: 6,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let (client, txn_requests, compact_requests, shutdown, server) = serve_compact_fixture(
            vec![Ok(coordination)],
            vec![Err(Status::unavailable("compact response lost"))],
        )
        .await;
        let mut compactor = CompactorState {
            compact_version: 1,
            previous_txn_header_revision: 5,
            status: CompactorStatus::Active,
            recovery: None,
        };
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                Arc::new(AtomicI64::new(0)),
            )
            .await,
            Outcome::ResultUnknown
        );
        assert_eq!(txn_requests.lock().unwrap().len(), 1);
        assert_eq!(compact_requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_worker_loop_executes_real_txn_and_finishes_token() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(Vec::new()).await;
        let plan = Arc::new(small_plan());
        let clock = FinalAcceptCutoffClock::new();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let token = match recorder.offer(identity).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        token.accept().unwrap();
        let item = WorkItem::Txn {
            resource: stream.resource,
            key: plan.resource_key(stream.resource, 0, plan.config.seed),
            operation: ResourceOperation::GuaranteedUpdate,
            value: plan.resource_value(stream.resource, 0, plan.config.seed, 0),
            token,
            preparation: None,
        };
        let (sender, receiver) = mpsc::channel(1);
        sender.send(item).await.unwrap();
        drop(sender);
        worker_loop(
            receiver,
            client.clone(),
            plan,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await
        .unwrap();
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 1);
        assert_eq!(row.succeeded_before_cutoff, 1);
    }

    #[tokio::test]
    async fn test_worker_loop_invalid_range_records_error_and_first_error() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(Vec::new()).await;
        let plan = Arc::new(small_plan());
        let clock = FinalAcceptCutoffClock::new();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| {
                matches!(
                    stream.work_class,
                    LogicalWorkClass::PointGet | LogicalWorkClass::BoundedList
                )
            })
            .unwrap();
        let identity = stream_identity(stream);
        let token = match recorder.offer(identity).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        token.accept().unwrap();
        let item = WorkItem::Range {
            request: RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                limit: 500,
                ..Default::default()
            },
            selector: RangeSelector::All { target: None },
            token,
        };
        let (sender, receiver) = mpsc::channel(1);
        sender.send(item).await.unwrap();
        drop(sender);
        worker_loop(
            receiver,
            client.clone(),
            plan,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        )
        .await
        .unwrap();
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.failed_before_cutoff, 1);
        assert!(row.first_error.as_deref().unwrap().contains("more=true"));
    }

    #[tokio::test]
    async fn test_prepare_blocks_until_all_resources_complete() {
        let tracker = PreparationTracker::new(2);
        let mut waiting = Box::pin(tracker.wait(TokioInstant::now() + Duration::from_secs(1)));
        assert_pending(waiting.as_mut()).await;
        tracker.complete(Outcome::Success);
        assert_pending(waiting.as_mut()).await;
        tracker.complete(Outcome::Success);
        waiting.await.unwrap();
    }

    #[tokio::test]
    async fn test_prepare_deadline_deficit() {
        let tracker = PreparationTracker::new(2);
        tracker.complete(Outcome::Success);
        let error = tracker
            .wait(TokioInstant::now() + Duration::from_millis(1))
            .await
            .unwrap_err();
        assert_eq!(error, "preparation deadline exceeded");
    }

    #[tokio::test(start_paused = true)]
    async fn test_preparation_tracker_rejects_completion_at_deadline() {
        let tracker = PreparationTracker::new(0);
        let deadline = TokioInstant::now();
        let error = tracker.wait(deadline).await.unwrap_err();
        assert_eq!(error, "preparation deadline exceeded");
    }

    #[tokio::test(start_paused = true)]
    async fn test_watch_ready_rejects_queued_success_at_deadline() {
        let (ready_tx, ready_rx) = oneshot::channel();
        ready_tx.send(()).unwrap();
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<WatchTaskOutput>());
        let mut ctrl_c = pending_ctrl_c();
        let error =
            wait_for_watch_ready(&mut tasks, vec![ready_rx], TokioInstant::now(), &mut ctrl_c)
                .await
                .unwrap_err();
        assert_eq!(error, "watch readiness timed out");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn test_prepare_error_ack_blocks_measurement() {
        let tracker = PreparationTracker::new(1);
        tracker.complete(Outcome::GrpcError);
        let error = tracker
            .wait(TokioInstant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error, "preparation outcome: grpc_error");
    }

    #[tokio::test(start_paused = true)]
    async fn test_preparation_consumes_budget_before_watch_readiness() {
        let deadline = TokioInstant::now() + Duration::from_secs(1);
        let tracker = PreparationTracker::new(1);
        let mut preparation = Box::pin(tracker.wait(deadline));
        assert_pending(preparation.as_mut()).await;
        time::advance(Duration::from_millis(900)).await;
        tracker.complete(Outcome::Success);
        preparation.await.unwrap();

        let (ready_tx, ready_rx) = oneshot::channel();
        tokio::spawn(async move {
            time::sleep_until(deadline).await;
            let _ = ready_tx.send(());
        });
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<WatchTaskOutput>());
        let mut ctrl_c = pending_ctrl_c();
        let mut waiting = Box::pin(wait_for_watch_ready(
            &mut tasks,
            vec![ready_rx],
            deadline,
            &mut ctrl_c,
        ));
        assert_pending(waiting.as_mut()).await;
        time::advance(Duration::from_millis(100)).await;
        assert_eq!(waiting.await.unwrap_err(), "watch readiness timed out");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn test_prepare_cancel_leaves_measurement_in_preparation() {
        let plan = Arc::new(small_plan());
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        let (cancel, receiver) = oneshot::channel();
        let mut ctrl_c: CtrlCFuture = Box::pin(async move {
            receiver
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::Other, "cancel sender dropped")
                })
                .map(|_| ())
        });
        cancel.send(()).unwrap();
        let error = run_preparation(
            plan,
            &recorder,
            &[sender],
            TokioInstant::now() + Duration::from_secs(1),
            &mut ctrl_c,
        )
        .await
        .unwrap_err();
        assert_eq!(error, "interrupted during preparation");
        assert_eq!(recorder.phase(), crate::measurement::Phase::Preparation);
    }

    #[tokio::test]
    async fn test_prepare_barrier_and_node_ready_order() {
        let plan = Arc::new(small_plan());
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(64);
        let acknowledged = Arc::new(AtomicU64::new(0));
        let worker_acknowledged = acknowledged.clone();
        let observed = Arc::new(StdMutex::new(Vec::new()));
        let worker_observed = observed.clone();
        let worker = tokio::spawn(async move {
            while let Some(item) = receiver.recv().await {
                let preparation = item.preparation().cloned();
                match &item {
                    WorkItem::Txn {
                        resource,
                        key,
                        operation,
                        ..
                    } => worker_observed
                        .lock()
                        .unwrap()
                        .push((*resource, *operation, key.clone())),
                    other => panic!("unexpected preparation item: {other:?}"),
                }
                item.started().unwrap();
                item.terminal(Outcome::Success).unwrap();
                if let Some(preparation) = preparation {
                    preparation.complete(Outcome::Success);
                    worker_acknowledged.fetch_add(1, AtomicOrdering::Relaxed);
                }
            }
        });
        let mut ctrl_c = pending_ctrl_c();
        run_preparation(
            plan.clone(),
            &recorder,
            &[sender],
            TokioInstant::now() + Duration::from_secs(1),
            &mut ctrl_c,
        )
        .await
        .unwrap();
        assert_eq!(
            acknowledged.load(AtomicOrdering::Relaxed),
            preparation_total(&plan)
        );
        worker.await.unwrap();
        let observed = observed.lock().unwrap();
        assert_eq!(
            observed.len(),
            usize::try_from(preparation_total(&plan)).unwrap()
        );
        let node_key = plan.resource(ResourceKind::Nodes).key(0);
        let node_ready = observed
            .iter()
            .position(|(resource, operation, key)| {
                *resource == ResourceKind::Nodes
                    && *operation == ResourceOperation::GuaranteedUpdate
                    && key == &node_key
            })
            .expect("NodeReady request missing");
        assert!(
            observed[..node_ready]
                .iter()
                .all(|(_, operation, _)| *operation == ResourceOperation::Create)
        );
        assert_eq!(recorder.phase(), crate::measurement::Phase::Preparation);
    }

    #[tokio::test]
    async fn test_interrupted_dispatch_preserves_partial_report() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let mut workers = JoinSet::new();
        let mut watches = JoinSet::new();
        let mut ctrl_c = {
            let (_cancel, receiver) = oneshot::channel::<()>();
            Box::pin(async move {
                receiver
                    .await
                    .map(|_| ())
                    .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "interrupt"))
            }) as CtrlCFuture
        };
        let started = TokioInstant::now();
        let cutoff = started + run_duration(&plan);
        let exit = dispatch_until(
            &plan,
            &recorder,
            &[],
            &mut workers,
            &mut watches,
            started,
            cutoff,
            &mut ctrl_c,
        )
        .await;
        let mut state = match exit {
            DispatchExit::Interrupted(state) => state,
            _ => panic!("expected interrupted dispatch"),
        };
        recorder.stop_measurement().unwrap();
        mark_remaining_not_offered(&plan, &recorder, &mut state).unwrap();
        let report = recorder.finish().unwrap();
        assert!(
            report
                .measurement
                .logical
                .iter()
                .any(|stream| stream.not_offered_after_stop > 0)
        );
    }

    #[test]
    fn test_cas_unknown_no_replay() {
        let error = crate::client::ClientError::Rpc(tonic::Status::unavailable("response lost"));
        assert_eq!(
            error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown),
            RetryDecision::ResultUnknown
        );
        assert_eq!(
            error.retry_decision(RpcMethod::Range, RequestDisposition::Unknown),
            RetryDecision::Retry
        );
    }

    #[test]
    fn test_compact_rpc_unknown_policy_no_replay() {
        let error = crate::client::ClientError::Rpc(tonic::Status::unavailable("response lost"));
        assert_eq!(
            error.retry_decision(RpcMethod::Compact, RequestDisposition::Unknown),
            RetryDecision::ResultUnknown
        );
    }

    #[test]
    fn test_range_shape_classification() {
        assert_eq!(range_shape(&RangeRequest::default()), RangeShape::Point);
        assert_eq!(
            range_shape(&RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                count_only: true,
                ..Default::default()
            }),
            RangeShape::CountOnly
        );
        assert_eq!(
            range_shape(&RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                ..Default::default()
            }),
            RangeShape::UnboundedPrefix
        );
        assert_eq!(
            range_shape(&RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                limit: 10,
                ..Default::default()
            }),
            RangeShape::PagedPrefix
        );
    }

    #[test]
    fn test_list_pages_pin_snapshot() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 10_000,
            ..Default::default()
        };
        let mut pager = RangePager::new(request, RangeSelector::All { target: None });
        assert_eq!(pager.request().limit, 500);
        assert!(matches!(
            pager
                .observe_response(&range_page(42, &[b"a", b"b"], true, 4), 2)
                .unwrap(),
            RangePageOutcome::Continue(_)
        ));
        assert_eq!(pager.request().revision, 42);
        assert_eq!(pager.request().key, b"b\0");
        assert_eq!(
            pager
                .observe_response(&range_page(45, &[b"c", b"d"], false, 4), 2)
                .unwrap(),
            RangePageOutcome::Complete
        );
        assert_eq!(pager.result().snapshot_revision, 42);
        assert_eq!(pager.result().selected, 4);
    }

    #[test]
    fn test_list_explicit_snapshot() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            revision: 77,
            limit: 500,
            ..Default::default()
        };
        let mut pager = RangePager::new(request, RangeSelector::All { target: Some(1) });
        assert_eq!(
            pager
                .observe_response(&range_page(42, &[b"a"], false, 1), 1)
                .unwrap(),
            RangePageOutcome::Complete
        );
        assert_eq!(pager.result().snapshot_revision, 77);
    }

    #[test]
    fn test_list_invalid_page_error() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 500,
            ..Default::default()
        };
        let mut missing_revision =
            RangePager::new(request.clone(), RangeSelector::All { target: None });
        assert!(
            missing_revision
                .observe_response(&range_page(0, &[b"a"], true, 1), 1)
                .is_err()
        );
        let mut empty = RangePager::new(request, RangeSelector::All { target: None });
        let error = empty
            .observe_response(&range_page(7, &[], true, 0), 0)
            .unwrap_err();
        assert!(error.contains("more=true"));
    }

    #[test]
    fn test_list_limit_bounds_physical_pages() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 600,
            ..Default::default()
        };
        let mut pager = RangePager::new(request, RangeSelector::All { target: Some(600) });
        assert_eq!(pager.request().limit, 500);
        assert!(matches!(
            pager
                .observe_response(
                    &range_page(
                        9,
                        &[b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i", b"j"],
                        true,
                        10_000,
                    ),
                    1,
                )
                .unwrap(),
            RangePageOutcome::Continue(_)
        ));
        assert_eq!(pager.request().limit, 599);
    }

    #[test]
    fn test_list_selector_limit_growth() {
        let request = RangeRequest {
            key: b"/registry/pods/".to_vec(),
            range_end: b"/registry/pods0".to_vec(),
            limit: 10_000,
            ..Default::default()
        };
        let selector = RangeSelector::DropEvery {
            every: 2,
            target: None,
        };
        let mut pager = RangePager::new(request, selector);
        for expected_limit in [500, 1_000, 2_000, 4_000, 8_000, 9_995] {
            assert_eq!(pager.request().limit, expected_limit);
            let key = [expected_limit as u8];
            let response = range_page(9, &[&key, b"keep"], expected_limit != 9_995, 12);
            let selected = selector.selected_in_page(response.kvs.len(), 0);
            let outcome = pager.observe_response(&response, selected).unwrap();
            if expected_limit != 9_995 {
                assert!(matches!(outcome, RangePageOutcome::Continue(_)));
            } else {
                assert_eq!(outcome, RangePageOutcome::Complete);
            }
        }
        assert_eq!(pager.result().discarded, 6);
    }

    #[test]
    fn test_range_single_request_shapes() {
        for request in [
            RangeRequest {
                key: b"/registry/pods/item".to_vec(),
                ..Default::default()
            },
            RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                count_only: true,
                ..Default::default()
            },
            RangeRequest {
                key: b"/registry/pods/".to_vec(),
                range_end: b"/registry/pods0".to_vec(),
                ..Default::default()
            },
        ] {
            assert!(!matches!(range_shape(&request), RangeShape::PagedPrefix));
            let limit = request.limit;
            let pager = RangePager::new(request, RangeSelector::All { target: None });
            assert_eq!(pager.request().limit, limit);
        }
    }

    #[test]
    fn test_dispatch_rate() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream_index = plan
            .logical_streams
            .iter()
            .position(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let stream = &plan.logical_streams[stream_index];
        let expected = planned_at(stream, 1_000_000).unwrap();
        assert!(expected > 0);
        let (sender, mut receiver) = mpsc::channel(64);
        let now = TokioInstant::now();
        let deadline = now + Duration::from_secs(1);
        let mut state = DispatchState::new(&plan);
        dispatch_tick(
            &plan,
            &recorder,
            &[sender.clone()],
            &mut state,
            0,
            1_000_000,
            deadline,
            deadline,
        )
        .unwrap();
        assert_eq!(state.next_ordinals[stream_index], expected);
        let mut accepted = 0;
        while let Ok(item) = receiver.try_recv() {
            accepted += 1;
            drop(item);
        }
        assert_eq!(accepted, expected);
        drop(sender);
        drop(receiver);
        recorder.stop_measurement().unwrap();
        recorder.finish().unwrap();
    }

    #[test]
    fn test_dispatch_no_catchup() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream_index = plan
            .logical_streams
            .iter()
            .position(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let stream = &plan.logical_streams[stream_index];
        let (sender, mut receiver) = mpsc::channel(64);
        let mut state = DispatchState::new(&plan);
        let now = TokioInstant::now();
        let expired = now - Duration::from_millis(1);
        dispatch_tick(
            &plan,
            &recorder,
            &[sender.clone()],
            &mut state,
            0,
            1_000_000,
            expired,
            now + Duration::from_secs(1),
        )
        .unwrap();
        let at_three_ms = planned_at(stream, 3_000_000).unwrap();
        skip_application_before(&plan, &recorder, &mut state, 3_000_000).unwrap();
        assert_eq!(state.next_ordinals[stream_index], at_three_ms);
        let deadline = TokioInstant::now() + Duration::from_secs(1);
        dispatch_tick(
            &plan,
            &recorder,
            &[sender.clone()],
            &mut state,
            3_000_000,
            4_000_000,
            deadline,
            deadline,
        )
        .unwrap();
        let at_four_ms = planned_at(stream, 4_000_000).unwrap();
        assert_eq!(state.next_ordinals[stream_index], at_four_ms);
        let mut accepted = 0;
        while let Ok(item) = receiver.try_recv() {
            accepted += 1;
            drop(item);
        }
        assert_eq!(accepted, at_four_ms.saturating_sub(at_three_ms));
        let cutoff = TokioInstant::now() - Duration::from_millis(1);
        dispatch_tick(
            &plan,
            &recorder,
            &[sender.clone()],
            &mut state,
            4_000_000,
            5_000_000,
            deadline,
            cutoff,
        )
        .unwrap();
        assert_eq!(
            state.next_ordinals[stream_index],
            planned_at(stream, 5_000_000).unwrap()
        );
        while let Ok(item) = receiver.try_recv() {
            drop(item);
        }
        drop(sender);
        drop(receiver);
        recorder.stop_measurement().unwrap();
        recorder.finish().unwrap();
    }

    #[test]
    fn test_dispatch_interleaves_periodic_and_skips_elapsed_events() {
        let mut config = small_plan().config;
        config.load.update_rps = 2_000;
        config.periodic.node_lease_interval_ms = 1;
        config.periodic.node_lease_jitter_percent = 0;
        config.periodic.node_status_interval_ms = 1;
        config.periodic.status_interval_ms = 1;
        config.periodic.compact_interval_ms = 1;
        let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(128);
        let mut state = DispatchState::new(&plan);
        let deadline = TokioInstant::now() + Duration::from_secs(1);
        dispatch_tick(
            &plan,
            &recorder,
            &[sender.clone()],
            &mut state,
            10_000_000,
            11_000_000,
            deadline,
            deadline,
        )
        .unwrap();

        let mut first_periodic = None;
        let mut last_application = None;
        let mut total = 0;
        while let Ok(item) = receiver.try_recv() {
            let is_periodic = matches!(
                &item,
                WorkItem::Heartbeat { .. } | WorkItem::Status { .. } | WorkItem::CompactTick { .. }
            );
            if is_periodic {
                if first_periodic.is_none() {
                    first_periodic = Some(total);
                }
            } else {
                last_application = Some(total);
            }
            total += 1;
            drop(item);
        }
        let first_periodic = first_periodic.expect("current periodic event was not dispatched");
        let last_application = last_application.expect("application event was not dispatched");
        assert!(first_periodic > 0);
        assert!(first_periodic < last_application);

        drop(sender);
        drop(receiver);
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let periodic_kinds = [
            PeriodicKind::NodeLease,
            PeriodicKind::NodeStatus,
            PeriodicKind::Status,
            PeriodicKind::Compact,
        ];
        let mut expected_missed = [0u64; 4];
        let mut expected_current = [0u64; 4];
        for event in &plan.periodic_events {
            if event.at_ns < 10_000_000 {
                expected_missed[event.kind.index()] += 1;
            } else if event.at_ns < 11_000_000 {
                expected_current[event.kind.index()] += 1;
            }
        }
        for kind in periodic_kinds {
            let identity = periodic_identity(kind);
            let row = &report.measurement.logical[identity.measurement_index().unwrap()];
            assert_eq!(row.missed_schedule, expected_missed[kind.index()]);
            assert_eq!(row.accepted, expected_current[kind.index()]);
        }
    }

    #[test]
    fn test_final_cutoff_rejection_marks_remaining_slots_missed() {
        let mut config = small_plan().config;
        config.periodic.node_lease_interval_ms = 1;
        config.periodic.node_lease_jitter_percent = 0;
        config.periodic.node_status_interval_ms = 1;
        config.periodic.status_interval_ms = 1;
        config.periodic.compact_interval_ms = 1;
        let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
        let clock = FinalAcceptCutoffClock::new();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        for kind in ResourceKind::ALL {
            for _ in 0..resource_count(&plan, kind) {
                let token = match recorder
                    .offer_preparation(PreparationStream::Create(kind))
                    .unwrap()
                {
                    OfferResult::Token(token) => token,
                    other => panic!("unexpected create preparation offer: {other:?}"),
                };
                token.accept().unwrap();
                token.started().unwrap();
                token.terminal(Outcome::Success).unwrap();
            }
        }
        for _ in 0..plan.resource_counts.nodes {
            let token = match recorder
                .offer_preparation(PreparationStream::NodeReady)
                .unwrap()
            {
                OfferResult::Token(token) => token,
                other => panic!("unexpected NodeReady preparation offer: {other:?}"),
            };
            token.accept().unwrap();
            token.started().unwrap();
            token.terminal(Outcome::Success).unwrap();
        }
        clock.arm_for_cutoff();
        recorder.start_measurement().unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(128);
        let mut state = DispatchState::new(&plan);
        let deadline = TokioInstant::now() + Duration::from_secs(1);
        dispatch_tick(
            &plan,
            &recorder,
            &[sender.clone()],
            &mut state,
            0,
            10_000_000,
            deadline,
            deadline,
        )
        .unwrap();
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(sender);
        drop(receiver);
        mark_remaining_missed(&plan, &recorder, &mut state).unwrap();
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        assert!(report.accounting.planned_conserved);
        for row in &report.measurement.logical {
            assert_eq!(row.not_offered_after_stop, 0);
            assert_eq!(row.planned, row.missed_schedule);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_dispatch_until_dispatches_current_tick_after_late_wake() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let skipped = planned_at(stream, 3_000_000).unwrap();
        let current = planned_at(stream, 4_000_000)
            .unwrap()
            .saturating_sub(skipped);
        assert!(current > 0);
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(64);
        let senders = vec![sender.clone()];
        let mut worker_tasks = JoinSet::new();
        let mut watch_tasks = JoinSet::new();
        let mut first_ctrl_poll = true;
        let mut ctrl_c: CtrlCFuture = Box::pin(std::future::poll_fn(move |_cx| {
            if std::mem::replace(&mut first_ctrl_poll, false) {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(Ok(()))
            }
        }));
        let started = TokioInstant::now() - Duration::from_millis(3);
        let cutoff = started + run_duration(&plan);
        let exit = dispatch_until(
            &plan,
            &recorder,
            &senders,
            &mut worker_tasks,
            &mut watch_tasks,
            started,
            cutoff,
            &mut ctrl_c,
        )
        .await;
        let mut state = match exit {
            DispatchExit::Interrupted(state) => state,
            DispatchExit::Completed(_) => panic!("late wake should be interrupted"),
            DispatchExit::Failed(error, _) => panic!("late wake failed: {error}"),
        };
        let mut items = Vec::new();
        while let Ok(item) = receiver.try_recv() {
            items.push(item);
        }
        assert_eq!(items.len(), usize::try_from(current).unwrap());
        for item in &items {
            match item {
                WorkItem::Txn { resource, key, .. } => {
                    assert_eq!(*resource, stream.resource);
                    assert_eq!(
                        key,
                        &plan.resource_key(stream.resource, skipped, plan.config.seed)
                    );
                }
                other => panic!("late wake emitted non-update work item: {other:?}"),
            }
        }
        drop(items);
        drop(senders);
        drop(sender);
        drop(receiver);
        recorder.stop_measurement().unwrap();
        mark_remaining_not_offered(&plan, &recorder, &mut state).unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.missed_schedule, skipped);
        assert_eq!(row.offered, current);
        assert_eq!(row.accepted, current);
    }

    #[tokio::test(start_paused = true)]
    async fn test_watch_completion_does_not_dispatch_future_tick_early() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(128);
        let senders = vec![sender];
        let mut worker_tasks = JoinSet::new();
        let mut watch_tasks = JoinSet::new();
        let (watch_release_tx, watch_release_rx) = oneshot::channel();
        let (watch_done_tx, watch_done_rx) = oneshot::channel();
        watch_tasks.spawn(async move {
            watch_release_rx.await.unwrap();
            let _ = watch_done_tx.send(());
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
        let mut ctrl_c = pending_ctrl_c();
        let started = TokioInstant::now();
        let cutoff = started + run_duration(&plan);
        let mut dispatch = Box::pin(dispatch_until(
            &plan,
            &recorder,
            &senders,
            &mut worker_tasks,
            &mut watch_tasks,
            started,
            cutoff,
            &mut ctrl_c,
        ));
        assert_pending(dispatch.as_mut()).await;
        let initial = receiver.len();
        assert!(initial > 0);
        time::advance(Duration::from_micros(100)).await;
        watch_release_tx.send(()).unwrap();
        watch_done_rx.await.unwrap();
        assert!(TokioInstant::now() < started + Duration::from_millis(1));
        assert_pending(dispatch.as_mut()).await;
        assert_eq!(receiver.len(), initial);
        time::advance(run_duration(&plan) + Duration::from_millis(1)).await;
        let mut state = match dispatch.await {
            DispatchExit::Completed(state) => state,
            DispatchExit::Interrupted(_) => panic!("watch terminal interrupted the run"),
            DispatchExit::Failed(error, _) => panic!("watch terminal failed the run: {error}"),
        };
        while let Ok(item) = receiver.try_recv() {
            drop(item);
        }
        drop(receiver);
        drop(senders);
        recorder.stop_measurement().unwrap();
        mark_remaining_missed(&plan, &recorder, &mut state).unwrap();
        let report = recorder.finish().unwrap();
        assert_eq!(
            report.timing.observed_measurement_ns,
            Some(u64::try_from(run_duration(&plan).as_nanos()).unwrap())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_dispatch_until_waits_for_cutoff_before_completion() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let (sender, receiver) = mpsc::channel::<WorkItem>(64);
        let senders = vec![sender.clone()];
        let mut worker_tasks = JoinSet::new();
        let mut watch_tasks = JoinSet::new();
        let mut ctrl_c = pending_ctrl_c();
        let started = TokioInstant::now();
        let cutoff = started + run_duration(&plan);
        let exit = dispatch_until(
            &plan,
            &recorder,
            &senders,
            &mut worker_tasks,
            &mut watch_tasks,
            started,
            cutoff,
            &mut ctrl_c,
        )
        .await;
        let mut state = match exit {
            DispatchExit::Completed(state) => state,
            DispatchExit::Interrupted(_) => panic!("normal dispatch was interrupted"),
            DispatchExit::Failed(error, _) => panic!("normal dispatch failed: {error}"),
        };
        assert!(TokioInstant::now() >= cutoff);
        drop(senders);
        drop(sender);
        drop(receiver);
        recorder.stop_measurement().unwrap();
        mark_remaining_not_offered(&plan, &recorder, &mut state).unwrap();
        recorder.finish().unwrap();
    }

    #[test]
    fn test_compactor_journal_revision_and_recovery() {
        let mut state = CompactorState::default();
        assert_eq!(state.txn_inputs(), (0, 0));
        assert_eq!(
            state
                .observe(&compact_response(11, true, 0, b""))
                .unwrap()
                .compact_target,
            None
        );
        let conflict = state
            .observe(&compact_response(30, false, 7, b"19"))
            .unwrap();
        assert_eq!(conflict.compact_target, None);
        assert_eq!(conflict.remote_compact_revision, Some(19));
        assert_eq!(state.txn_inputs(), (7, 30));
        let next = state.observe(&compact_response(40, true, 0, b"")).unwrap();
        assert_eq!(next.compact_target, Some(30));
        assert_eq!(state.txn_inputs(), (8, 40));

        let mut invalid = CompactorState {
            compact_version: 7,
            previous_txn_header_revision: 20,
            ..Default::default()
        };
        assert_eq!(
            invalid.observe(&compact_response(30, false, 7, b"bad")),
            Err(CompactorObservationError::FailureValueNotDecimal)
        );
        assert_eq!(invalid.status, CompactorStatus::ResyncRequired);
        let recovered = invalid
            .observe(&compact_response(50, false, 9, b"19"))
            .unwrap();
        assert!(recovered.recovered);
        assert_eq!(invalid.txn_inputs(), (9, 50));
    }

    #[test]
    fn test_offered_token_crossing_tick_is_missed_once() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let token = match recorder.offer(identity).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        assert_eq!(
            token.accept_for_tick(0).unwrap(),
            TokenAction::RejectedAfterTick
        );
        assert_eq!(
            token.accept_for_tick(0).unwrap(),
            TokenAction::AlreadyFinalized
        );
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.offered, 0);
        assert_eq!(row.missed_schedule, 1);
        assert_eq!(row.not_offered_after_stop, 0);
    }

    #[test]
    fn test_rejected_admission_never_reaches_worker_receiver() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let (sender, mut receiver) = mpsc::channel(1);
        let result = dispatch_application_item(
            &plan,
            &recorder,
            stream,
            0,
            &sender,
            TokioInstant::now() + Duration::from_secs(1),
            TokioInstant::now() + Duration::from_secs(1),
            0,
        )
        .unwrap();
        assert_eq!(result, DispatchOutcome::MissedToken);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(sender);
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 0);
        assert_eq!(row.missed_schedule, 1);
    }

    #[test]
    fn test_queue_full_drop() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = LogicalStreamIdentity::Application {
            work_class: stream.work_class,
            resource: stream.resource,
        };
        let filler = match recorder
            .offer(LogicalStreamIdentity::Periodic {
                kind: PeriodicKind::Compact,
            })
            .unwrap()
        {
            OfferResult::Token(token) => WorkItem::CompactTick { token },
            other => panic!("unexpected filler offer: {other:?}"),
        };
        filler.accept().unwrap();
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(filler).unwrap();
        let result = dispatch_application_item(
            &plan,
            &recorder,
            stream,
            0,
            &sender,
            TokioInstant::now() + Duration::from_secs(1),
            TokioInstant::now() + Duration::from_secs(1),
            1_000_000,
        )
        .unwrap();
        assert_eq!(result, DispatchOutcome::Dropped);
        drop(sender);
        drop(receiver);
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.dropped_full, 1);
    }

    #[test]
    fn test_queue_closed_drop() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let error = dispatch_application_item(
            &plan,
            &recorder,
            stream,
            0,
            &sender,
            TokioInstant::now() + Duration::from_secs(1),
            TokioInstant::now() + Duration::from_secs(1),
            1_000_000,
        )
        .unwrap_err();
        assert_eq!(error, "measurement worker queue closed");
        drop(sender);
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.dropped_closed, 0);
        assert_eq!(row.not_offered_after_stop, 1);
    }

    #[tokio::test]
    async fn test_worker_abort_unfinished() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let token = match recorder.offer(identity).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected offer result: {other:?}"),
        };
        let item = WorkItem::Txn {
            resource: stream.resource,
            key: plan.resource_key(stream.resource, 0, plan.config.seed),
            operation: ResourceOperation::GuaranteedUpdate,
            value: plan.resource_value(stream.resource, 0, plan.config.seed, 0),
            token,
            preparation: None,
        };
        item.accept().unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(1);
        let (started, started_rx) = oneshot::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let Some(item) = receiver.recv().await else {
                return Ok(());
            };
            item.started().unwrap();
            started.send(()).unwrap();
            std::future::pending::<()>().await;
            Ok::<(), String>(())
        });
        sender.send(item).await.unwrap();
        started_rx.await.unwrap();
        abort_workers(vec![sender], tasks).await;
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.canceled_running, 1);
        assert_eq!(row.accepted, 1);
    }

    #[tokio::test]
    async fn test_watch_ready_waits_all() {
        let (first_tx, first_rx) = oneshot::channel();
        let (last_tx, last_rx) = oneshot::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<WatchTaskOutput>());
        tasks.spawn(std::future::pending::<WatchTaskOutput>());
        let mut ctrl_c = pending_ctrl_c();
        let mut waiting = Box::pin(wait_for_watch_ready(
            &mut tasks,
            vec![first_rx, last_rx],
            TokioInstant::now() + Duration::from_secs(1),
            &mut ctrl_c,
        ));
        assert_pending(waiting.as_mut()).await;
        first_tx.send(()).unwrap();
        assert_pending(waiting.as_mut()).await;
        last_tx.send(()).unwrap();
        waiting.await.unwrap();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn test_watch_ready_preserves_terminal_cause() {
        let (ready_tx, ready_rx) = oneshot::channel();
        drop(ready_tx);
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            time::sleep(Duration::from_millis(1)).await;
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
        let mut ctrl_c = pending_ctrl_c();
        let error = wait_for_watch_ready(
            &mut tasks,
            vec![ready_rx],
            TokioInstant::now() + Duration::from_secs(1),
            &mut ctrl_c,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            "watch pods terminated: Canceled { reason: \"server canceled\" }"
        );
    }

    #[test]
    fn test_watch_shutdown_not_failure() {
        let outcome = crate::watch::WatchOutcome {
            terminal: crate::watch::WatchTerminal::ContextCanceled,
            state: crate::watch::WatchState::default(),
        };
        assert_eq!(watch_failure("pods", Ok(outcome)), None);
    }

    #[tokio::test]
    async fn test_watch_ready_timeout() {
        let (_ready_tx, ready_rx) = oneshot::channel();
        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<WatchTaskOutput>());
        let mut ctrl_c = pending_ctrl_c();
        let error = wait_for_watch_ready(
            &mut tasks,
            vec![ready_rx],
            TokioInstant::now() + Duration::from_millis(1),
            &mut ctrl_c,
        )
        .await
        .unwrap_err();
        assert_eq!(error, "watch readiness timed out");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    #[tokio::test]
    async fn test_watch_teardown_timeout_joins_and_drops_task() {
        let (shutdown, _state) = shutdown_watch::channel(false);
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sentinel = DropSentinel(dropped.clone());
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let _sentinel = sentinel;
            std::future::pending::<WatchTaskOutput>().await
        });
        let error = stop_watches(shutdown, tasks, Duration::from_millis(1))
            .await
            .unwrap_err();
        assert_eq!(error, "watch teardown deadline exceeded");
        assert!(dropped.load(AtomicOrdering::Acquire));
    }

    #[tokio::test]
    async fn test_worker_drain_timeout_joins_and_drops_task() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sentinel = DropSentinel(dropped.clone());
        let mut workers = JoinSet::new();
        workers.spawn(async move {
            let _sentinel = sentinel;
            std::future::pending::<Result<(), String>>().await
        });
        let error = drain_workers(Vec::new(), workers, Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(error.contains("worker drain deadline exceeded"));
        assert!(dropped.load(AtomicOrdering::Acquire));
    }

    #[tokio::test]
    async fn test_worker_panic_preempts_schedule() {
        let mut workers = JoinSet::new();
        workers.spawn(async {
            panic!("injected worker panic");
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        });
        let mut watches = JoinSet::new();
        let error = wait_for_schedule_event(
            &mut workers,
            &mut watches,
            TokioInstant::now() + Duration::from_secs(60),
            &mut pending_ctrl_c(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("worker task failed"));
        assert!(error.contains("injected worker panic"));
    }

    #[tokio::test]
    async fn test_worker_drain_error() {
        let mut workers = JoinSet::new();
        workers.spawn(async { Err::<(), _>("worker failed".to_string()) });
        let error = drain_workers(Vec::new(), workers, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.contains("worker failed"));
    }
}
