use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use prost::Message;
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{Notify, mpsc, oneshot, watch as shutdown_watch};
use tokio::task::JoinSet;
use tokio::time::{self, Instant as TokioInstant};

use crate::client::{ClientBundle, RequestDisposition, RetryDecision, RpcMethod};
use crate::config::RunConfig;
use crate::heartbeat::HeartbeatItem;
use crate::measurement::{
    LogicalStreamIdentity, MeasurementRecorder, MeasurementToken, MeasurementWindow, OfferResult,
    PreparationStream, TokenAction,
};
use crate::metrics::{self, DropReason, Outcome};
use crate::pb::etcdserverpb::{CompactionRequest, RangeRequest, TxnResponse};
use crate::plan::{CompiledPlan, LogicalWorkClass, PeriodicEvent, PeriodicKind, ResourceKind};
use crate::report::RunReport;
use crate::workload::{
    CasDecision, CasLoop, CasRetryPolicy, ExhaustedReason, KeyState, ResourceOperation, TxnAttempt,
    build_compact_revision_txn, failure_value, failure_version,
};

const MAX_ERROR_CONTEXT: usize = 256;

/// The canonical final report returned by the scheduler and serialized by
/// `main`; all phase sections remain present for setup and partial runs.
#[derive(Debug)]
pub struct RunResult {
    pub report: RunReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Completed,
    Interrupted,
    Failed,
}
#[derive(Debug)]
enum ControlFailure {
    Interrupted(String),
    SignalFailure(String),
    ExecutionFailure(String),
}

impl ControlFailure {
    fn interrupted(message: impl Into<String>) -> Self {
        Self::Interrupted(message.into())
    }

    fn signal_failure(message: impl Into<String>) -> Self {
        Self::SignalFailure(message.into())
    }

    fn execution_failure(message: impl Into<String>) -> Self {
        Self::ExecutionFailure(message.into())
    }

    fn is_interrupted(&self) -> bool {
        matches!(self, Self::Interrupted(_))
    }
}

impl std::fmt::Display for ControlFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Interrupted(message)
            | Self::SignalFailure(message)
            | Self::ExecutionFailure(message) => formatter.write_str(message),
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

    #[cfg(test)]
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

    async fn wait_with_ctrl_c(
        &self,
        deadline: TokioInstant,
        ctrl_c: &mut CtrlCFuture,
    ) -> Result<(), ControlFailure> {
        loop {
            let notified = self.notify.notified();
            if TokioInstant::now() >= deadline {
                return Err(ControlFailure::execution_failure(
                    "preparation deadline exceeded",
                ));
            }
            if self.remaining.load(AtomicOrdering::Acquire) == 0 {
                return self
                    .first_error
                    .lock()
                    .unwrap()
                    .clone()
                    .map_or(Ok(()), |error| {
                        Err(ControlFailure::execution_failure(error))
                    });
            }
            tokio::select! {
                biased;
                signal = ctrl_c.as_mut() => {
                    return match signal {
                        Ok(()) => Err(ControlFailure::interrupted(
                            "interrupted during preparation",
                        )),
                        Err(error) => Err(ControlFailure::signal_failure(
                            format!("ctrl-c signal failed: {error}"),
                        )),
                    };
                }
                result = time::timeout_at(deadline, notified) => {
                    if result.is_err() {
                        return Err(ControlFailure::execution_failure(
                            "preparation deadline exceeded",
                        ));
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum WorkItem {
    Txn {
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
    All {
        target: Option<u64>,
    },
    #[cfg(test)]
    DropEvery {
        every: usize,
        target: Option<u64>,
    },
}

impl RangeSelector {
    fn selected_in_page(self, key_count: usize, _page_offset: usize) -> usize {
        match self {
            Self::All { .. } => key_count,
            #[cfg(test)]
            Self::DropEvery { every, .. } if every <= 1 => key_count,
            #[cfg(test)]
            Self::DropEvery { every, .. } => (0..key_count)
                .filter(|index| (_page_offset + *index + 1) % every != 0)
                .count(),
        }
    }

    fn target(self) -> Option<u64> {
        match self {
            Self::All { target } => target,
            #[cfg(test)]
            Self::DropEvery { target, .. } => target,
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
    software_error: bool,
}

impl RangeExecutionError {
    fn new(outcome: Outcome, message: impl Into<String>) -> Self {
        Self {
            outcome,
            message: bound_error(message.into()),
            software_error: false,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            outcome: Outcome::GrpcError,
            message: bound_error(message.into()),
            software_error: true,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct ExecutorResult {
    outcome: Outcome,
    error: Option<String>,
}

impl ExecutorResult {
    fn new(outcome: Outcome) -> Self {
        Self {
            outcome,
            error: None,
        }
    }

    fn with_error(outcome: Outcome, error: impl Into<String>) -> Self {
        Self {
            outcome,
            error: Some(bound_error(error.into())),
        }
    }
}

fn client_error_outcome(
    error: &crate::client::ClientError,
    method: RpcMethod,
    decision: RetryDecision,
) -> Outcome {
    if error.is_timeout() {
        return if method.is_mutable() {
            Outcome::TimeoutUnknown
        } else {
            Outcome::Timeout
        };
    }
    if decision == RetryDecision::ResultUnknown {
        Outcome::ResultUnknown
    } else {
        Outcome::GrpcError
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

async fn execute_range(
    client: &ClientBundle,
    request: RangeRequest,
    selector: RangeSelector,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
    recorder: &MeasurementRecorder,
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
                break Err(RangeExecutionError::new(
                    Outcome::Timeout,
                    "Range operation deadline exceeded",
                ));
            }
            attempts += 1;
            let rpc_attempt = recorder
                .start_rpc_with_payload(
                    crate::measurement::RpcMethod::Range,
                    page_request.encoded_len() as u64,
                )
                .map_err(|error| RangeExecutionError::internal(error.to_string()))?;
            let result = time::timeout(remaining, client.range(page_request.clone())).await;
            match result {
                Ok(Ok(response)) => {
                    rpc_attempt
                        .finish(Outcome::Success, response.encoded_len() as u64)
                        .map_err(|error| RangeExecutionError::internal(error.to_string()))?;
                    break Ok(response);
                }
                Ok(Err(error)) => {
                    let decision =
                        error.retry_decision(RpcMethod::Range, RequestDisposition::Unknown);
                    let attempt_outcome = client_error_outcome(&error, RpcMethod::Range, decision);
                    rpc_attempt
                        .finish(attempt_outcome, 0)
                        .map_err(|finish_error| {
                            RangeExecutionError::internal(finish_error.to_string())
                        })?;
                    if decision == RetryDecision::Retry && attempts < MAX_PAGE_ATTEMPTS {
                        continue;
                    }
                    let outcome = if decision == RetryDecision::Retry {
                        Outcome::RetryExhausted
                    } else {
                        attempt_outcome
                    };
                    let message = if decision == RetryDecision::Retry {
                        format!("Range retry limit reached: {error}")
                    } else {
                        error.to_string()
                    };
                    break Err(RangeExecutionError::new(outcome, message));
                }
                Err(_) => {
                    rpc_attempt
                        .finish(Outcome::Timeout, 0)
                        .map_err(|error| RangeExecutionError::internal(error.to_string()))?;
                    break Err(RangeExecutionError::new(
                        Outcome::Timeout,
                        "Range page request deadline exceeded",
                    ));
                }
            }
        }?;
        if let Some(header) = response.header.as_ref() {
            update_revision(&latest_revision, header.revision, recorder);
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
    recorder: MeasurementRecorder,
    deadline: crate::watch::WatchDeadline,
) -> (
    shutdown_watch::Sender<bool>,
    JoinSet<WatchTaskOutput>,
    Vec<oneshot::Receiver<()>>,
) {
    let (shutdown, receiver) = shutdown_watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut ready_receivers = Vec::with_capacity(specs.len());
    for (index, spec) in specs.iter().cloned().enumerate() {
        let name = spec.name.clone();
        let watch_receiver = receiver.clone();
        let watch_client = client.clone();
        let watch_recorder = recorder.clone();
        let watch_deadline = deadline.clone();
        let (ready_sender, ready_receiver) = oneshot::channel();
        ready_receivers.push(ready_receiver);
        tasks.spawn(async move {
            let result = crate::watch::run_with_recorder(
                watch_client,
                spec,
                watch_receiver,
                ready_sender,
                watch_recorder,
                index,
                watch_deadline,
            )
            .await;
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
) -> Result<(), ControlFailure> {
    if ready_receivers.is_empty() {
        return Ok(());
    }
    for mut ready in ready_receivers {
        let mut readiness_closed = false;
        loop {
            if TokioInstant::now() >= deadline {
                return Err(ControlFailure::execution_failure(
                    "watch readiness timed out",
                ));
            }
            tokio::select! {
                biased;
                signal = ctrl_c.as_mut() => {
                    return match signal {
                        Ok(()) => Err(ControlFailure::interrupted(
                            "interrupted while waiting for watch readiness",
                        )),
                        Err(error) => Err(ControlFailure::signal_failure(
                            format!("ctrl-c signal failed: {error}"),
                        )),
                    };
                }
                watch_result = tasks.join_next(), if !tasks.is_empty() => {
                    match watch_result {
                        Some(Ok((name, result))) => {
                            if let Some(error) = watch_failure(&name, result) {
                                return Err(ControlFailure::execution_failure(error));
                            }
                            return Err(ControlFailure::execution_failure(format!(
                                "watch {name} terminated before Created readiness"
                            )));
                        }
                        Some(Err(error)) => {
                            return Err(ControlFailure::execution_failure(format!(
                                "watch task failed: {error}"
                            )));
                        }
                        None => {
                            return Err(ControlFailure::execution_failure(
                                "all watch tasks terminated before readiness",
                            ));
                        }
                    }
                }
                ready_result = &mut ready, if !readiness_closed => {
                    match ready_result {
                        Ok(()) if TokioInstant::now() < deadline => break,
                        Ok(()) => {
                            return Err(ControlFailure::execution_failure(
                                "watch readiness timed out",
                            ));
                        }
                        Err(_) if tasks.is_empty() => {
                            return Err(ControlFailure::execution_failure(
                                "watch terminated before Created readiness",
                            ));
                        }
                        Err(_) => readiness_closed = true,
                    }
                }
                _ = time::sleep_until(deadline) => {
                    return Err(ControlFailure::execution_failure(
                        "watch readiness timed out",
                    ));
                }
            }
        }
    }
    if TokioInstant::now() >= deadline {
        return Err(ControlFailure::execution_failure(
            "watch readiness timed out",
        ));
    }
    Ok(())
}

async fn wait_for_measurement_start(
    tasks: &mut JoinSet<WatchTaskOutput>,
    ready_receivers: Vec<oneshot::Receiver<()>>,
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
    recorder: &MeasurementRecorder,
) -> Result<MeasurementWindow, ControlFailure> {
    wait_for_watch_ready(tasks, ready_receivers, deadline, ctrl_c).await?;
    let mut readiness = recorder.watch_readiness();
    loop {
        // Mark the notification observed before checking recorder state. If a
        // watch changes between this check and the await below, `changed`
        // still observes the newer version instead of losing the wakeup.
        readiness.borrow_and_update();
        if TokioInstant::now() >= deadline {
            return Err(ControlFailure::execution_failure(
                "watch readiness timed out",
            ));
        }
        match recorder.try_start_measurement() {
            Ok(Some(window)) => return Ok(window),
            Ok(None) => {}
            Err(error) => {
                return Err(ControlFailure::execution_failure(format!(
                    "measurement start failed: {error}"
                )));
            }
        }
        tokio::select! {
            biased;
            signal = ctrl_c.as_mut() => {
                return match signal {
                    Ok(()) => Err(ControlFailure::interrupted(
                        "interrupted while waiting for watch readiness",
                    )),
                    Err(error) => Err(ControlFailure::signal_failure(
                        format!("ctrl-c signal failed: {error}"),
                    )),
                };
            }
            watch_result = tasks.join_next(), if !tasks.is_empty() => {
                match watch_result {
                    Some(Ok((name, result))) => {
                        if let Some(error) = watch_failure(&name, result) {
                            return Err(ControlFailure::execution_failure(error));
                        }
                        return Err(ControlFailure::execution_failure(format!(
                            "watch {name} terminated before measurement start"
                        )));
                    }
                    Some(Err(error)) => {
                        return Err(ControlFailure::execution_failure(format!(
                            "watch task failed: {error}"
                        )));
                    }
                    None => {
                        return Err(ControlFailure::execution_failure(
                            "all watch tasks terminated before measurement start",
                        ));
                    }
                }
            }
            result = readiness.changed() => {
                if result.is_err() {
                    return Err(ControlFailure::execution_failure(
                        "watch readiness channel closed",
                    ));
                }
            }
            _ = time::sleep_until(deadline) => {
                return Err(ControlFailure::execution_failure(
                    "watch readiness timed out",
                ));
            }
        }
    }
}

pub(crate) type CtrlCFuture = Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>;

fn update_revision(latest_revision: &AtomicI64, revision: i64, recorder: &MeasurementRecorder) {
    let previous = latest_revision.fetch_max(revision, AtomicOrdering::Relaxed);
    recorder.set_revision_metric(previous.max(revision));
}

async fn execute_status(
    client: &ClientBundle,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
    recorder: &MeasurementRecorder,
) -> Result<ExecutorResult, String> {
    let deadline = Instant::now() + timeout;
    let mut first_error = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(first_error.map_or_else(
                || ExecutorResult::new(Outcome::Timeout),
                |error| ExecutorResult::with_error(Outcome::Timeout, error),
            ));
        }
        let rpc_attempt = recorder
            .start_rpc_with_payload(
                crate::measurement::RpcMethod::Status,
                crate::pb::etcdserverpb::StatusRequest::default().encoded_len() as u64,
            )
            .map_err(|error| format!("status recorder start failed: {error}"))?;
        let result = time::timeout(remaining, client.status()).await;
        match result {
            Ok(Ok(response)) => {
                rpc_attempt
                    .finish(Outcome::Success, response.encoded_len() as u64)
                    .map_err(|error| format!("status recorder finish failed: {error}"))?;
                if let Some(header) = response.header.as_ref() {
                    update_revision(&latest_revision, header.revision, recorder);
                }
                return Ok(ExecutorResult::new(Outcome::Success));
            }
            Ok(Err(error)) => {
                let decision = error.retry_decision(RpcMethod::Status, RequestDisposition::Unknown);
                let attempt_outcome = client_error_outcome(&error, RpcMethod::Status, decision);
                rpc_attempt
                    .finish(attempt_outcome, 0)
                    .map_err(|finish_error| {
                        format!("status recorder finish failed: {finish_error}")
                    })?;
                match decision {
                    RetryDecision::Retry => {
                        if !error.is_timeout() && first_error.is_none() {
                            first_error = Some(bound_error(error.to_string()));
                        }
                        let delay = Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(Instant::now()));
                        if delay.is_zero() {
                            return Ok(first_error.map_or_else(
                                || ExecutorResult::new(Outcome::Timeout),
                                |error| ExecutorResult::with_error(Outcome::Timeout, error),
                            ));
                        }
                        time::sleep(delay).await;
                    }
                    RetryDecision::NotSent | RetryDecision::ResultUnknown | RetryDecision::Stop => {
                        return Ok(ExecutorResult::with_error(
                            attempt_outcome,
                            error.to_string(),
                        ));
                    }
                }
            }
            Err(_) => {
                rpc_attempt
                    .finish(Outcome::Timeout, 0)
                    .map_err(|error| format!("status recorder finish failed: {error}"))?;
                return Ok(first_error.map_or_else(
                    || ExecutorResult::new(Outcome::Timeout),
                    |error| ExecutorResult::with_error(Outcome::Timeout, error),
                ));
            }
        }
    }
}

async fn execute_compact_tick(
    client: &ClientBundle,
    compactor: &mut CompactorState,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
    recorder: &MeasurementRecorder,
) -> Result<ExecutorResult, String> {
    let (coordination_version, previous_txn_header_revision) = compactor.txn_inputs();
    let value = previous_txn_header_revision.to_string();
    recorder.set_compactor_state_metric(compactor.status.as_metric());

    let deadline = Instant::now() + timeout;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Ok(ExecutorResult::new(Outcome::Timeout));
    }

    let request = build_compact_revision_txn(value.as_bytes(), coordination_version);
    let request_payload = request.encoded_len() as u64;
    let rpc_attempt = recorder
        .start_rpc_with_payload(crate::measurement::RpcMethod::Txn, request_payload)
        .map_err(|error| format!("compact coordination recorder start failed: {error}"))?;
    let result = time::timeout(remaining, client.txn(request)).await;
    let response = match result {
        Ok(Ok(response)) => {
            rpc_attempt
                .finish(Outcome::Success, response.encoded_len() as u64)
                .map_err(|error| format!("compact coordination recorder finish failed: {error}"))?;
            response
        }
        Ok(Err(error)) => {
            let decision = error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown);
            let outcome = client_error_outcome(&error, RpcMethod::Txn, decision);
            rpc_attempt.finish(outcome, 0).map_err(|finish_error| {
                format!("compact coordination recorder finish failed: {finish_error}")
            })?;
            return Ok(ExecutorResult::with_error(outcome, error.to_string()));
        }
        Err(_) => {
            rpc_attempt
                .finish(Outcome::TimeoutUnknown, 0)
                .map_err(|error| format!("compact coordination recorder finish failed: {error}"))?;
            return Ok(ExecutorResult::new(Outcome::TimeoutUnknown));
        }
    };

    if let Some(header) = response.header.as_ref() {
        update_revision(&latest_revision, header.revision, recorder);
    }
    let observation = match compactor.observe(&response) {
        Ok(observation) => observation,
        Err(error) => {
            recorder.record_compactor_error_metric();
            recorder.set_compactor_state_metric(compactor.status.as_metric());
            return Ok(ExecutorResult::with_error(
                Outcome::GrpcError,
                error.to_string(),
            ));
        }
    };
    recorder.set_compactor_state_metric(compactor.status.as_metric());
    if observation.recovered {
        recorder.record_compactor_recovery_metric();
    }
    if !observation.succeeded {
        // A CAS conflict is coordination, not a business-object retry. The
        // embedded Range supplies both the next logical version and the
        // revision another compactor already published.
        if let Some(revision) = observation.remote_compact_revision {
            recorder.set_compact_revision_metric(revision);
        }
        return Ok(ExecutorResult::new(Outcome::Success));
    }

    let Some(revision) = observation.compact_target else {
        return Ok(ExecutorResult::new(Outcome::Success));
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Ok(ExecutorResult::new(Outcome::Timeout));
    }
    let compact_request = CompactionRequest {
        revision,
        physical: false,
    };
    let rpc_attempt = recorder
        .start_rpc_with_payload(
            crate::measurement::RpcMethod::Compact,
            compact_request.encoded_len() as u64,
        )
        .map_err(|error| format!("compact recorder start failed: {error}"))?;
    let result = time::timeout(remaining, client.compact(compact_request)).await;
    let outcome = match result {
        Ok(Ok(response)) => {
            rpc_attempt
                .finish(Outcome::Success, response.encoded_len() as u64)
                .map_err(|error| format!("compact recorder finish failed: {error}"))?;
            if let Some(header) = response.header.as_ref() {
                update_revision(&latest_revision, header.revision, recorder);
            }
            recorder.set_compact_revision_metric(revision);
            ExecutorResult::new(Outcome::Success)
        }
        Ok(Err(error)) => {
            let decision = error.retry_decision(RpcMethod::Compact, RequestDisposition::Unknown);
            let outcome = client_error_outcome(&error, RpcMethod::Compact, decision);
            rpc_attempt.finish(outcome, 0).map_err(|finish_error| {
                format!("compact recorder finish failed: {finish_error}")
            })?;
            ExecutorResult::with_error(outcome, error.to_string())
        }
        Err(_) => {
            rpc_attempt
                .finish(Outcome::TimeoutUnknown, 0)
                .map_err(|error| format!("compact recorder finish failed: {error}"))?;
            ExecutorResult::new(Outcome::TimeoutUnknown)
        }
    };
    Ok(outcome)
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
) -> Result<(), ControlFailure> {
    let permit = tokio::select! {
        biased;
        signal = ctrl_c.as_mut() => {
            return Err(match signal {
                Ok(()) => ControlFailure::interrupted("interrupted during preparation"),
                Err(error) => ControlFailure::signal_failure(
                    format!("ctrl-c signal failed: {error}"),
                ),
            });
        }
        result = time::timeout_at(deadline, sender.reserve()) => {
            match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    return Err(ControlFailure::execution_failure(
                        "preparation worker queue closed",
                    ));
                }
                Err(_) => {
                    return Err(ControlFailure::execution_failure(
                        "preparation enqueue deadline exceeded",
                    ));
                }
            }
        }
    };
    if item
        .accept()
        .map_err(|error| ControlFailure::execution_failure(error.to_string()))?
        != TokenAction::Applied
    {
        return Err(ControlFailure::execution_failure(
            "preparation admission was not applied",
        ));
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
) -> Result<(), ControlFailure> {
    let tracker = PreparationTracker::new(preparation_total(&plan));
    for kind in ResourceKind::ALL {
        let stream = PreparationStream::Create(kind);
        let count = resource_count(&plan, kind);
        for ordinal in 0..count {
            let token = match recorder
                .offer_preparation(stream)
                .map_err(|error| ControlFailure::execution_failure(error.to_string()))?
            {
                OfferResult::Token(token) => token,
                OfferResult::MissedSchedule
                | OfferResult::NotOfferedAfterStop
                | OfferResult::WrongPhase => {
                    return Err(ControlFailure::execution_failure(
                        "preparation recorder rejected create token",
                    ));
                }
            };
            let key = plan.resource(kind).key(ordinal);
            let value = plan.resource(kind).value(ordinal, 0);
            let item = WorkItem::Txn {
                key: key.clone(),
                operation: ResourceOperation::Create,
                value,
                token,
                preparation: Some(tracker.clone()),
            };
            let shard = stable_shard(&key, senders.len());
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
            .map_err(|error| ControlFailure::execution_failure(error.to_string()))?
        {
            OfferResult::Token(token) => token,
            OfferResult::MissedSchedule
            | OfferResult::NotOfferedAfterStop
            | OfferResult::WrongPhase => {
                return Err(ControlFailure::execution_failure(
                    "preparation recorder rejected NodeReady token",
                ));
            }
        };
        let key = plan.resource(ResourceKind::Nodes).key(ordinal);
        let value = plan.resource(ResourceKind::Nodes).value(ordinal, 1);
        let item = WorkItem::Txn {
            key: key.clone(),
            operation: ResourceOperation::GuaranteedUpdate,
            value,
            token,
            preparation: Some(tracker.clone()),
        };
        let shard = stable_shard(&key, senders.len());
        if let Err(error) = enqueue_preparation_item(&senders[shard], item, deadline, ctrl_c).await
        {
            return Err(error);
        }
    }

    tracker.wait_with_ctrl_c(deadline, ctrl_c).await
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
) -> Result<(), String> {
    drop(senders);
    tasks.abort_all();
    let mut first_error = None;
    while let Some(joined) = tasks.join_next().await {
        capture_worker_join_error(joined, &mut first_error);
    }
    first_error.map_or(Ok(()), Err)
}

fn append_cleanup_error(primary: String, cleanup: Result<(), String>) -> String {
    match cleanup {
        Ok(()) => primary,
        Err(error) => format!("{primary}; cleanup failed: {error}"),
    }
}

fn capture_worker_join_error(
    joined: Result<Result<(), String>, tokio::task::JoinError>,
    first_error: &mut Option<String>,
) {
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) if first_error.is_none() => *first_error = Some(error),
        Ok(Err(_)) => {}
        Err(error) if !error.is_cancelled() && first_error.is_none() => {
            *first_error = Some(format!("worker task failed: {error}"));
        }
        Err(_) => {}
    }
}

async fn drain_workers_with_ctrl_c(
    senders: Vec<mpsc::Sender<WorkItem>>,
    mut tasks: JoinSet<Result<(), String>>,
    timeout: Duration,
    ctrl_c: &mut CtrlCFuture,
) -> (Result<(), String>, bool) {
    drop(senders);
    let deadline = TokioInstant::now() + timeout;
    let mut first_error = None;
    loop {
        let joined = tokio::select! {
            biased;
            signal = ctrl_c.as_mut() => {
                tasks.abort_all();
                while let Some(joined) = tasks.join_next().await {
                    capture_worker_join_error(joined, &mut first_error);
                }
                let interrupted = signal.is_ok();
                let signal_error = match signal {
                    Ok(()) => first_error.unwrap_or_else(|| "interrupted during drain".into()),
                    Err(error) => first_error.map_or_else(
                        || format!("ctrl-c signal failed: {error}"),
                        |first| format!("ctrl-c signal failed: {error}; {first}"),
                    ),
                };
                return (Err(signal_error), interrupted);
            }
            joined = time::timeout_at(deadline, tasks.join_next()) => joined,
        };
        let joined = match joined {
            Ok(joined) => joined,
            Err(_) => {
                tasks.abort_all();
                while let Some(joined) = tasks.join_next().await {
                    capture_worker_join_error(joined, &mut first_error);
                }
                return (
                    Err(first_error.map_or_else(
                        || "worker drain deadline exceeded".to_string(),
                        |error| format!("worker drain deadline exceeded: {error}"),
                    )),
                    false,
                );
            }
        };
        let Some(joined) = joined else {
            break;
        };
        capture_worker_join_error(joined, &mut first_error);
    }
    (first_error.map_or(Ok(()), Err), false)
}

fn capture_watch_join_error(
    joined: Result<WatchTaskOutput, tokio::task::JoinError>,
    first_error: &mut Option<String>,
) {
    match joined {
        Ok((_name, Ok(_))) => {}
        Ok((name, Err(error))) => {
            if first_error.is_none() {
                *first_error = Some(format!("watch {name} failed: {error}"));
            }
        }
        Err(error) if !error.is_cancelled() && first_error.is_none() => {
            *first_error = Some(format!("watch task failed: {error}"));
        }
        Err(_) => {}
    }
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
                    capture_watch_join_error(joined, &mut first_error);
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
        capture_watch_join_error(joined, &mut first_error);
    }
    first_error.map_or(Ok(()), Err)
}

async fn teardown_watches(
    recorder: &MeasurementRecorder,
    shutdown: shutdown_watch::Sender<bool>,
    tasks: JoinSet<WatchTaskOutput>,
    timeout: Duration,
) -> Result<(), String> {
    let mut first_error = recorder
        .begin_watch_teardown()
        .err()
        .map(|error| format!("watch teardown timing failed: {error}"));
    if let Err(error) = stop_watches(shutdown, tasks, timeout).await {
        first_error = Some(match first_error {
            Some(first) => format!("{first}; cleanup failed: {error}"),
            None => error,
        });
    }
    if let Err(error) = recorder.end_watch_teardown() {
        first_error = Some(match first_error {
            Some(first) => format!("{first}; watch teardown timing failed: {error}"),
            None => format!("watch teardown timing failed: {error}"),
        });
    }
    first_error.map_or(Ok(()), Err)
}

async fn stop_watches_with_ctrl_c(
    shutdown: shutdown_watch::Sender<bool>,
    mut tasks: JoinSet<WatchTaskOutput>,
    timeout: Duration,
    ctrl_c: &mut CtrlCFuture,
) -> (Result<(), String>, bool) {
    let _ = shutdown.send(true);
    let deadline = TokioInstant::now() + timeout;
    let mut first_error = None;
    loop {
        let joined = tokio::select! {
            biased;
            signal = ctrl_c.as_mut() => {
                tasks.abort_all();
                while let Some(joined) = tasks.join_next().await {
                    capture_watch_join_error(joined, &mut first_error);
                }
                let interrupted = signal.is_ok();
                let signal_error = match signal {
                    Ok(()) => {
                        first_error.unwrap_or_else(|| "interrupted during watch teardown".into())
                    }
                    Err(error) => first_error.map_or_else(
                        || format!("ctrl-c signal failed: {error}"),
                        |first| format!("ctrl-c signal failed: {error}; {first}"),
                    ),
                };
                return (Err(signal_error), interrupted);
            }
            joined = time::timeout_at(deadline, tasks.join_next()) => joined,
        };
        let joined = match joined {
            Ok(joined) => joined,
            Err(_) => {
                tasks.abort_all();
                while let Some(joined) = tasks.join_next().await {
                    capture_watch_join_error(joined, &mut first_error);
                }
                return (
                    Err(first_error.map_or_else(
                        || "watch teardown deadline exceeded".to_string(),
                        |error| format!("watch teardown deadline exceeded: {error}"),
                    )),
                    false,
                );
            }
        };
        let Some(joined) = joined else {
            break;
        };
        capture_watch_join_error(joined, &mut first_error);
    }
    (first_error.map_or(Ok(()), Err), false)
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
    key: Vec<u8>,
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
    let permit = match sender.try_reserve() {
        Ok(permit) => permit,
        Err(mpsc::error::TrySendError::Full(())) => {
            token
                .drop_for(DropReason::Backpressure)
                .map_err(|error| error.to_string())?;
            return Ok(DispatchOutcome::Dropped);
        }
        Err(mpsc::error::TrySendError::Closed(())) => {
            token
                .drop_for(DropReason::QueueClosed)
                .map_err(|error| error.to_string())?;
            return Err("measurement worker queue closed".into());
        }
    };
    let item = match stream.work_class {
        LogicalWorkClass::Update => {
            let value = plan.resource_value(stream.resource, ordinal, plan.config.seed, ordinal);
            WorkItem::Txn {
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
    let mut targets = [0; crate::measurement::MEASUREMENT_STREAM_COUNT];
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
        targets[index] = end;
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
                key,
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

async fn wait_for_schedule_event<F: Future<Output = ()>>(
    worker_tasks: &mut JoinSet<Result<(), String>>,
    watch_tasks: &mut JoinSet<WatchTaskOutput>,
    deadline: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
    sleep_until: &impl Fn(TokioInstant) -> F,
) -> Result<ScheduleControl, String> {
    tokio::select! {
        biased;
        signal = ctrl_c.as_mut() => match signal {
            Ok(()) => Ok(ScheduleControl::Interrupted),
            Err(error) => Err(format!("ctrl-c signal failed: {error}")),
        },
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
                Some(Ok((_name, Ok(_outcome)))) => Ok(ScheduleControl::Supervisor),
                Some(Ok((name, Err(error)))) => Err(format!("watch {name} failed: {error}")),
                Some(Err(error)) => Err(format!("watch task failed: {error}")),
                None => Err("all watch tasks exited before the run ended".into()),
            }
        }
        _ = sleep_until(deadline) => Ok(ScheduleControl::Tick),
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

async fn dispatch_until<F: Future<Output = ()>>(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    senders: &[mpsc::Sender<WorkItem>],
    worker_tasks: &mut JoinSet<Result<(), String>>,
    watch_tasks: &mut JoinSet<WatchTaskOutput>,
    started: TokioInstant,
    cutoff: TokioInstant,
    ctrl_c: &mut CtrlCFuture,
    sleep_until: impl Fn(TokioInstant) -> F,
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
                &sleep_until,
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
        match wait_for_schedule_event(worker_tasks, watch_tasks, cutoff, ctrl_c, &sleep_until).await
        {
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
        let recorder = item.token().recorder();
        let execution = match &item {
            WorkItem::Txn {
                key,
                operation,
                value,
                ..
            } => {
                execute_txn(
                    &client,
                    &mut states,
                    key.clone(),
                    value.clone(),
                    *operation,
                    CasRetryPolicy::until_deadline(),
                    true,
                    timeout,
                    latest_revision.clone(),
                    &recorder,
                )
                .await?
            }
            WorkItem::Range {
                request, selector, ..
            } => match execute_range(
                &client,
                request.clone(),
                *selector,
                timeout,
                latest_revision.clone(),
                &recorder,
            )
            .await
            {
                Ok(result) => {
                    recorder
                        .record_range_result(
                            result.pages,
                            result.snapshot_revision,
                            result.server_count,
                            result.last_key.len() as u64,
                            result.selected,
                            result.discarded,
                        )
                        .map_err(|error| error.to_string())?;
                    recorder.record_range_result_metric(
                        result.pages,
                        result.snapshot_revision,
                        result.server_count,
                        result.last_key.len() as u64,
                        result.selected,
                        result.discarded,
                    );
                    ExecutorResult::new(Outcome::Success)
                }
                Err(error) if error.software_error => {
                    return Err(format!("range executor failed: {}", error.message));
                }
                Err(error) => ExecutorResult::with_error(error.outcome, error.message),
            },
            WorkItem::Heartbeat { item, resource, .. } => {
                execute_heartbeat(
                    &client,
                    &plan,
                    &mut states,
                    item.clone(),
                    *resource,
                    timeout,
                    latest_revision.clone(),
                    &recorder,
                )
                .await?
            }
            WorkItem::Status { .. } => {
                execute_status(&client, timeout, latest_revision.clone(), &recorder).await?
            }
            WorkItem::CompactTick { .. } => {
                execute_compact_tick(
                    &client,
                    &mut compactor,
                    timeout,
                    latest_revision.clone(),
                    &recorder,
                )
                .await?
            }
        };
        let outcome = execution.outcome;
        if let Some(message) = execution.error.as_deref() {
            item.terminal_with_error(outcome, 0, message)?;
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
    states: &mut HashMap<Vec<u8>, KeyState>,
    key: Vec<u8>,
    value: Vec<u8>,
    operation: ResourceOperation,
    cas_policy: CasRetryPolicy,
    recreate_missing: bool,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
    recorder: &MeasurementRecorder,
) -> Result<ExecutorResult, String> {
    let deadline = Instant::now() + timeout;
    let mut effective_operation = match states.get(&key) {
        Some(state) => storage_operation_for_state(operation, state, recreate_missing),
        None if operation == ResourceOperation::Create => Some(ResourceOperation::Create),
        None if recreate_missing => Some(ResourceOperation::Create),
        None => None,
    };
    let Some(mut effective_operation_value) = effective_operation.take() else {
        return Ok(ExecutorResult::new(Outcome::NotFound));
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
            let request = cas.request();
            let start = Instant::now();
            let remaining = deadline.saturating_duration_since(start);
            if remaining.is_zero() {
                return Ok(ExecutorResult::new(Outcome::Timeout));
            }
            if !take_storage_attempt(&mut storage_attempts, cas_policy.max_attempts) {
                return Ok(ExecutorResult::with_error(
                    Outcome::RetryExhausted,
                    format!("txn retry policy exhausted after {storage_attempts} attempts"),
                ));
            }
            let rpc_attempt = recorder
                .start_rpc_with_payload(
                    crate::measurement::RpcMethod::Txn,
                    request.encoded_len() as u64,
                )
                .map_err(|error| format!("txn recorder start failed: {error}"))?;
            let result = time::timeout(remaining, client.txn(request)).await;
            let response = match result {
                Ok(Ok(response)) => {
                    rpc_attempt
                        .finish(Outcome::Success, response.encoded_len() as u64)
                        .map_err(|error| format!("txn recorder finish failed: {error}"))?;
                    response
                }
                Ok(Err(error)) => {
                    let decision =
                        error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown);
                    let outcome = client_error_outcome(&error, RpcMethod::Txn, decision);
                    rpc_attempt.finish(outcome, 0).map_err(|finish_error| {
                        format!("txn recorder finish failed: {finish_error}")
                    })?;
                    return Ok(ExecutorResult::with_error(outcome, error.to_string()));
                }
                Err(_) => {
                    rpc_attempt
                        .finish(Outcome::TimeoutUnknown, 0)
                        .map_err(|error| format!("txn recorder finish failed: {error}"))?;
                    return Ok(ExecutorResult::new(Outcome::TimeoutUnknown));
                }
            };
            if let Some(header) = response.header.as_ref() {
                update_revision(&latest_revision, header.revision, recorder);
            }
            match cas.observe(&response, Instant::now()) {
                CasDecision::Succeeded {
                    header_revision: _,
                    state,
                } => {
                    *states.entry(key.clone()).or_default() = state;
                    break None;
                }
                CasDecision::CreateCollision {
                    header_revision: _,
                    latest,
                } => {
                    *states.entry(key.clone()).or_default() = latest.clone();
                    if operation == ResourceOperation::GuaranteedUpdate
                        && latest.mod_revision().is_some()
                    {
                        break Some(ResourceOperation::GuaranteedUpdate);
                    }
                    // A failed canonical Create is a conflict even when the
                    // server omitted its failure KV.
                    return Ok(ExecutorResult::new(Outcome::Conflict));
                }
                CasDecision::Retry {
                    header_revision: _,
                    latest,
                    next_attempt,
                } => {
                    *states.entry(key.clone()).or_default() = latest.clone();
                    let retry_value = resource_value_for_retry(&value, next_attempt);
                    if cas.retry_with_value(retry_value, &latest).is_none() {
                        return Ok(ExecutorResult::new(Outcome::NotFound));
                    }
                }
                CasDecision::Exhausted {
                    header_revision: _,
                    latest,
                    reason,
                } => {
                    *states.entry(key.clone()).or_default() = latest;
                    if reason == ExhaustedReason::MissingFailureKey
                        && recreate_missing
                        && operation == ResourceOperation::GuaranteedUpdate
                    {
                        break Some(ResourceOperation::Create);
                    }
                    let outcome = match reason {
                        ExhaustedReason::CancelledOrDeadline => Outcome::Timeout,
                        ExhaustedReason::MissingFailureKey => Outcome::NotFound,
                        ExhaustedReason::RetryPolicy => Outcome::RetryExhausted,
                    };
                    let result = if reason == ExhaustedReason::RetryPolicy {
                        ExecutorResult::with_error(
                            outcome,
                            format!("txn retry policy exhausted after {storage_attempts} attempts"),
                        )
                    } else {
                        ExecutorResult::new(outcome)
                    };
                    return Ok(result);
                }
            }
        };
        if let Some(next_operation) = next_operation {
            effective_operation_value = next_operation;
        } else {
            return Ok(ExecutorResult::new(Outcome::Success));
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
    recorder: &MeasurementRecorder,
) -> Result<ExecutorResult, String> {
    let key = item.key.into_bytes();
    let sequence = u64::try_from(item.at.as_nanos()).unwrap_or(u64::MAX);
    let value = plan.resource(resource).value(item.node as u64, sequence);
    execute_txn(
        client,
        states,
        key,
        value,
        ResourceOperation::GuaranteedUpdate,
        CasRetryPolicy::until_deadline(),
        true,
        timeout,
        latest_revision,
        recorder,
    )
    .await
}

pub(crate) async fn run_with_existing_ctrl_c(
    config: RunConfig,
    client: ClientBundle,
    ctrl_c: CtrlCFuture,
) -> RunResult {
    run_with_ctrl_c(config, client, ctrl_c).await
}

fn unstarted_result(
    config: &RunConfig,
    status: RunStatus,
    phase: &str,
    error: impl Into<String>,
    recorder: &MeasurementRecorder,
) -> RunResult {
    let error = bound_error(error.into());
    let error = if phase != "setup" {
        match recorder.mark_preparation_complete() {
            Ok(()) => error,
            Err(timing_error) => bound_error(format!(
                "{error}; preparation timing failed: {timing_error}"
            )),
        }
    } else {
        error
    };
    let report_status = match status {
        RunStatus::Completed => crate::report::RunStatus::Completed,
        RunStatus::Interrupted => crate::report::RunStatus::Interrupted,
        RunStatus::Failed => crate::report::RunStatus::Failed,
    };
    let measurement = match recorder.finish_without_measurement() {
        Ok(measurement) => measurement,
        Err(finish_error) => {
            let error = bound_error(format!(
                "{error}; measurement finalization failed: {finish_error}"
            ));
            return RunResult {
                report: crate::report::RunReport::setup_failure(
                    config,
                    report_status,
                    phase,
                    error,
                ),
            };
        }
    };
    let report = crate::report::RunReport::from_measurement(
        config,
        report_status,
        Some(phase.to_owned()),
        Some(error),
        measurement,
    );
    RunResult { report }
}
fn failed_result_with_measurement(
    config: &RunConfig,
    phase: &str,
    error: impl Into<String>,
    recorder: &MeasurementRecorder,
) -> RunResult {
    let mut error = bound_error(error.into());
    let measurement = match recorder.finish() {
        Ok(measurement) => measurement,
        Err(finish_error) => {
            error = bound_error(format!(
                "{error}; measurement finalization failed: {finish_error}"
            ));
            match recorder.finish_partial() {
                Ok(measurement) => measurement,
                Err(partial_error) => {
                    error = bound_error(format!(
                        "{error}; partial measurement finalization failed: {partial_error}"
                    ));
                    return RunResult {
                        report: crate::report::RunReport::setup_failure(
                            config,
                            crate::report::RunStatus::Failed,
                            phase,
                            error,
                        ),
                    };
                }
            }
        }
    };
    let report = crate::report::RunReport::from_measurement(
        config,
        crate::report::RunStatus::Failed,
        Some(phase.to_owned()),
        Some(error),
        measurement,
    );
    RunResult { report }
}

pub(crate) async fn run_with_ctrl_c(
    config: RunConfig,
    client: ClientBundle,
    mut ctrl_c: CtrlCFuture,
) -> RunResult {
    let run_config = config.clone();
    let plan = config.plan.clone();
    ::metrics::gauge!(metrics::TARGET_UPDATE_RPS).set(plan.config.load.update_rps as f64);
    let recorder = match MeasurementRecorder::new_with_metrics_at(
        &plan,
        metrics::RunMetrics::new(),
        config.setup_started_at,
    ) {
        Ok(recorder) => recorder,
        Err(error) => {
            let error = bound_error(format!("measurement setup failed: {error}"));
            let report = crate::report::RunReport::setup_failure(
                &run_config,
                crate::report::RunStatus::Failed,
                "setup",
                error,
            );
            return RunResult { report };
        }
    };
    let request_timeout = request_timeout(&plan);
    let setup = tokio::select! {
        result = async {
            match time::timeout(request_timeout, client.status()).await {
                Ok(Ok(response)) => {
                    if let Some(header) = response.header.as_ref() {
                        recorder.set_revision_metric(header.revision);
                    }
                    match time::timeout(request_timeout, client.preflight_count()).await {
                        Ok(Ok(response)) if response.count == 0 => Ok(()),
                        Ok(Ok(response)) => Err(format!(
                            "preflight requires an empty keyspace (found {} keys)",
                            response.count
                        )),
                        Ok(Err(error)) => Err(format!("preflight failed: {error}")),
                        Err(_) => Err("preflight timed out".into()),
                    }
                }
                Ok(Err(error)) => Err(format!("status preflight failed: {error}")),
                Err(_) => Err("status preflight timed out".into()),
            }
        } => result.map_err(|error| (RunStatus::Failed, error)),
        signal = ctrl_c.as_mut() => match signal {
            Ok(()) => Err((RunStatus::Interrupted, "interrupted during setup".into())),
            Err(error) => Err((
                RunStatus::Failed,
                format!("ctrl-c signal failed: {error}"),
            )),
        },
    };
    if let Err((status, error)) = setup {
        let error = match recorder.mark_setup_complete() {
            Ok(()) => error,
            Err(timing_error) => {
                format!("{error}; setup timing failed: {timing_error}")
            }
        };
        return unstarted_result(&run_config, status, "setup", error, &recorder);
    }
    if let Err(error) = recorder.mark_setup_complete() {
        return unstarted_result(
            &run_config,
            RunStatus::Failed,
            "setup",
            format!("setup timing failed: {error}"),
            &recorder,
        );
    }
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
        let status = if error.is_interrupted() {
            RunStatus::Interrupted
        } else {
            RunStatus::Failed
        };
        let error = match recorder.mark_preparation_complete() {
            Ok(()) => error.to_string(),
            Err(timing_error) => {
                format!("{error}; preparation timing failed: {timing_error}")
            }
        };
        let error = append_cleanup_error(error, abort_workers(senders, worker_tasks).await);
        return unstarted_result(&run_config, status, "preparation", error, &recorder);
    }

    let watch_specs = plan.watch_specs();
    let watch_deadline = crate::watch::WatchDeadline::new();
    watch_deadline.set(preparation_deadline);
    let (watch_shutdown, mut watch_tasks, ready_receivers) = spawn_watches(
        &client,
        &watch_specs,
        recorder.clone(),
        watch_deadline.clone(),
    );
    let measurement_window = match wait_for_measurement_start(
        &mut watch_tasks,
        ready_receivers,
        preparation_deadline,
        &mut ctrl_c,
        &recorder,
    )
    .await
    {
        Ok(window) => window,
        Err(error) => {
            let status = if error.is_interrupted() {
                RunStatus::Interrupted
            } else {
                RunStatus::Failed
            };
            let error = match recorder.mark_preparation_complete() {
                Ok(()) => error.to_string(),
                Err(timing_error) => {
                    format!("{error}; preparation timing failed: {timing_error}")
                }
            };
            let error = append_cleanup_error(
                error,
                teardown_watches(
                    &recorder,
                    watch_shutdown,
                    watch_tasks,
                    watch_teardown_timeout(&plan),
                )
                .await,
            );
            let error = append_cleanup_error(error, abort_workers(senders, worker_tasks).await);
            return unstarted_result(&run_config, status, "preparation", error, &recorder);
        }
    };
    let dispatch_started = TokioInstant::from_std(measurement_window.start);
    let dispatch_cutoff = TokioInstant::from_std(measurement_window.cutoff);
    watch_deadline.set(dispatch_cutoff);

    let (dispatch_error, mut interrupted, mut state) = match dispatch_until(
        &plan,
        &recorder,
        &senders,
        &mut worker_tasks,
        &mut watch_tasks,
        dispatch_started,
        dispatch_cutoff,
        &mut ctrl_c,
        crate::pacing::sleep_until,
    )
    .await
    {
        DispatchExit::Completed(state) => (None, false, state),
        DispatchExit::Interrupted(state) => (None, true, state),
        DispatchExit::Failed(error, state) => (Some(error), false, state),
    };

    let cutoff = match recorder.stop_measurement() {
        Ok(cutoff) => cutoff,
        Err(error) => {
            let error = append_cleanup_error(
                format!("measurement stop failed: {error}"),
                teardown_watches(
                    &recorder,
                    watch_shutdown,
                    watch_tasks,
                    watch_teardown_timeout(&plan),
                )
                .await,
            );
            let error = append_cleanup_error(error, abort_workers(senders, worker_tasks).await);
            return failed_result_with_measurement(&run_config, "measurement", error, &recorder);
        }
    };
    // Publish the effective cutoff before any drain or Watch teardown await.
    // Watches still opening/backing off must observe this latest boundary.
    watch_deadline.set(dispatch_started + Duration::from_nanos(cutoff.effective_ns));
    // Slots not yet enqueued at an early stop are classified only after the
    // cutoff freezes. A normal window has already classified every slot as
    // accepted, dropped, or missed.
    if dispatch_error.is_some() || interrupted {
        if let Err(error) = mark_remaining_not_offered(&plan, &recorder, &mut state) {
            let error = append_cleanup_error(
                error,
                teardown_watches(
                    &recorder,
                    watch_shutdown,
                    watch_tasks,
                    watch_teardown_timeout(&plan),
                )
                .await,
            );
            let error = append_cleanup_error(error, abort_workers(senders, worker_tasks).await);
            return failed_result_with_measurement(&run_config, "measurement", error, &recorder);
        }
    }

    let ctrl_c_completed = interrupted
        || dispatch_error
            .as_ref()
            .is_some_and(|error| error.starts_with("ctrl-c signal failed:"));
    let (drain_error, drain_interrupted) = if ctrl_c_completed {
        let mut pending_ctrl_c: CtrlCFuture =
            Box::pin(std::future::pending::<Result<(), std::io::Error>>());
        drain_workers_with_ctrl_c(
            senders,
            worker_tasks,
            drain_timeout(&plan),
            &mut pending_ctrl_c,
        )
        .await
    } else {
        drain_workers_with_ctrl_c(senders, worker_tasks, drain_timeout(&plan), &mut ctrl_c).await
    };
    if drain_interrupted {
        interrupted = true;
    }
    let drain_signal_completed = drain_interrupted
        || drain_error
            .as_ref()
            .err()
            .is_some_and(|error| error.starts_with("ctrl-c signal failed:"));
    let drain_timed_out = drain_error.as_ref().err().is_some_and(|error| {
        error == "worker drain deadline exceeded"
            || error.starts_with("worker drain deadline exceeded:")
    });
    if drain_timed_out {
        recorder.mark_drain_timeout();
    }
    let watch_begin_error = recorder
        .begin_watch_teardown()
        .err()
        .map(|error| format!("watch teardown setup failed: {error}"));
    let (watch_error, watch_interrupted) = if ctrl_c_completed || drain_signal_completed {
        (
            stop_watches(watch_shutdown, watch_tasks, watch_teardown_timeout(&plan)).await,
            false,
        )
    } else {
        stop_watches_with_ctrl_c(
            watch_shutdown,
            watch_tasks,
            watch_teardown_timeout(&plan),
            &mut ctrl_c,
        )
        .await
    };
    if watch_interrupted {
        interrupted = true;
    }
    let watch_end_error = recorder
        .end_watch_teardown()
        .err()
        .map(|error| format!("watch teardown finish failed: {error}"));
    let first_error = dispatch_error
        .or_else(|| {
            drain_error
                .err()
                .filter(|error| error != "worker drain deadline exceeded")
        })
        .or(watch_begin_error)
        .or_else(|| watch_error.err())
        .or(watch_end_error);
    let report = match recorder.finish() {
        Ok(report) => report,
        Err(error) => {
            let error = first_error.as_ref().map_or_else(
                || format!("measurement finish failed: {error}"),
                |first| format!("{first}; measurement finish failed: {error}"),
            );
            return failed_result_with_measurement(&run_config, "execution", error, &recorder);
        }
    };
    let status = if interrupted {
        RunStatus::Interrupted
    } else if first_error.is_some() {
        RunStatus::Failed
    } else {
        RunStatus::Completed
    };
    let report_status = match status {
        RunStatus::Completed => crate::report::RunStatus::Completed,
        RunStatus::Interrupted => crate::report::RunStatus::Interrupted,
        RunStatus::Failed => crate::report::RunStatus::Failed,
    };
    let failure_phase = first_error.as_ref().map(|_| "execution".to_owned());
    let final_report = crate::report::RunReport::from_measurement(
        &run_config,
        report_status,
        failure_phase,
        first_error,
        report,
    );
    RunResult {
        report: final_report,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

    use tokio::net::TcpListener;
    use tokio::sync::{Notify, oneshot};
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::{Endpoint, Server};
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::config::SyntheticConfig;
    use crate::heartbeat::HeartbeatItemKind;
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

    fn executor_recorder() -> MeasurementRecorder {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        recorder
    }

    fn record_test_preparation(recorder: &MeasurementRecorder, plan: &CompiledPlan) {
        for kind in ResourceKind::ALL {
            let stream = PreparationStream::Create(kind);
            for _ in 0..resource_count(plan, kind) {
                let token = match recorder.offer_preparation(stream).unwrap() {
                    OfferResult::Token(token) => token,
                    other => panic!("unexpected preparation offer result: {other:?}"),
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
                other => panic!("unexpected NodeReady offer result: {other:?}"),
            };
            token.accept().unwrap();
            token.started().unwrap();
            token.terminal(Outcome::Success).unwrap();
        }
    }

    fn finish_executor_recorder(recorder: &MeasurementRecorder) {
        recorder.stop_measurement().unwrap();
        recorder.finish().unwrap();
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
    struct ExecutorPhaseClock {
        origin: Instant,
        nanos: AtomicU64,
    }

    impl ExecutorPhaseClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                origin: Instant::now(),
                nanos: AtomicU64::new(0),
            })
        }

        fn set_millis(&self, millis: u64) {
            self.nanos
                .store(millis.saturating_mul(1_000_000), AtomicOrdering::Release);
        }
    }

    impl MonotonicClock for ExecutorPhaseClock {
        fn now(&self) -> Instant {
            self.origin + Duration::from_nanos(self.nanos.load(AtomicOrdering::Acquire))
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
        finished: Arc<Notify>,
        remote_tasks: StdMutex<Vec<tokio::task::JoinHandle<()>>>,
    }

    impl TxnGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                hold_first: AtomicBool::new(true),
                observed: Notify::new(),
                release: Notify::new(),
                inflight: Arc::new(AtomicUsize::new(0)),
                finished: Arc::new(Notify::new()),
                remote_tasks: StdMutex::new(Vec::new()),
            })
        }
    }

    struct TxnInflightGuard {
        inflight: Arc<AtomicUsize>,
        finished: Arc<Notify>,
    }

    impl Drop for TxnInflightGuard {
        fn drop(&mut self) {
            self.inflight.fetch_sub(1, AtomicOrdering::AcqRel);
            self.finished.notify_one();
        }
    }
    async fn join_remote_tasks(gate: &Arc<TxnGate>) {
        let tasks = std::mem::take(&mut *gate.remote_tasks.lock().unwrap());
        for task in tasks {
            time::timeout(Duration::from_secs(1), task)
                .await
                .expect("controlled fixture task did not finish")
                .expect("controlled fixture task panicked");
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
    struct InterruptedWatchFixture;

    #[tonic::async_trait]
    impl Watch for InterruptedWatchFixture {
        type WatchStream = ReceiverStream<Result<WatchResponse, Status>>;

        async fn watch(
            &self,
            request: Request<tonic::Streaming<WatchRequest>>,
        ) -> Result<Response<Self::WatchStream>, Status> {
            let mut requests = request.into_inner();
            let (sender, receiver) = tokio::sync::mpsc::channel(1);
            tokio::spawn(async move {
                if requests.next().await.is_none() {
                    return;
                }
                let _ = sender
                    .send(Ok(WatchResponse {
                        watch_id: 1,
                        canceled: true,
                        cancel_reason: "interrupted by server".into(),
                        ..Default::default()
                    }))
                    .await;
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
                    let (response_sender, response_receiver) =
                        oneshot::channel::<Result<TxnResponse, Status>>();
                    let task_gate = gate.clone();
                    let responses = self.responses.clone();
                    let remote_task = tokio::spawn(async move {
                        task_gate.inflight.fetch_add(1, AtomicOrdering::AcqRel);
                        let _guard = TxnInflightGuard {
                            inflight: task_gate.inflight.clone(),
                            finished: task_gate.finished.clone(),
                        };
                        task_gate.observed.notify_one();
                        task_gate.release.notified().await;
                        let response = responses.lock().unwrap().pop_front().unwrap_or_else(|| {
                            Ok(TxnResponse {
                                succeeded: true,
                                ..Default::default()
                            })
                        });
                        let _ = response_sender.send(response);
                    });
                    gate.remote_tasks.lock().unwrap().push(remote_task);
                    let response = response_receiver
                        .await
                        .map_err(|_| Status::cancelled("controlled fixture task canceled"))?;
                    return Ok(Response::new(response?));
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
        status_responses: Option<Arc<StdMutex<VecDeque<Result<StatusResponse, Status>>>>>,
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
            if let Some(responses) = &self.status_responses {
                let response = responses.lock().unwrap().pop_front().unwrap_or_else(|| {
                    Ok(StatusResponse {
                        header: Some(ResponseHeader {
                            revision: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })
                })?;
                return Ok(Response::new(response));
            }
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
        let maintenance = MaintenanceFixture {
            status_error,
            status_responses: None,
        };
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
    async fn serve_status_fixture(
        status_responses: Vec<Result<StatusResponse, Status>>,
    ) -> (
        ClientBundle,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let fixture = TxnFixture {
            responses: Arc::new(StdMutex::new(VecDeque::new())),
            requests: Arc::new(StdMutex::new(Vec::new())),
            compact_responses: None,
            compact_requests: None,
            range_requests: None,
            range_more: false,
            prep_request_count: None,
            load_observed: None,
            gate: None,
        };
        let maintenance = MaintenanceFixture {
            status_error: None,
            status_responses: Some(Arc::new(StdMutex::new(
                status_responses.into_iter().collect(),
            ))),
        };
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
            shutdown_tx,
            server,
        )
    }
    async fn serve_interrupted_watch_fixture() -> (
        ClientBundle,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let fixture = TxnFixture {
            responses: Arc::new(StdMutex::new(VecDeque::new())),
            requests,
            compact_responses: None,
            compact_requests: None,
            range_requests: None,
            range_more: false,
            prep_request_count: None,
            load_observed: None,
            gate: None,
        };
        let maintenance = MaintenanceFixture {
            status_error: None,
            status_responses: None,
        };
        let watch = InterruptedWatchFixture;
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
        let maintenance = MaintenanceFixture {
            status_error: None,
            status_responses: None,
        };
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
    fn accepted_worker_token(
        recorder: &MeasurementRecorder,
        identity: LogicalStreamIdentity,
    ) -> MeasurementToken {
        let token = match recorder.offer(identity).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("unexpected worker offer result: {other:?}"),
        };
        token.accept().unwrap();
        token
    }
    async fn run_worker_items(
        client: ClientBundle,
        plan: Arc<CompiledPlan>,
        items: Vec<WorkItem>,
        timeout: Duration,
    ) -> Result<(), String> {
        let (sender, receiver) = mpsc::channel(items.len().max(1));
        for item in items {
            sender
                .send(item)
                .await
                .map_err(|_| "worker fixture queue closed".to_string())?;
        }
        drop(sender);
        worker_loop(receiver, client, plan, timeout, Arc::new(AtomicI64::new(0))).await
    }

    fn run_config(plan: Arc<CompiledPlan>) -> crate::config::RunConfig {
        crate::config::RunConfig {
            plan,
            endpoint: "http://fixture".into(),
            metrics_listen: "127.0.0.1:0".parse().unwrap(),
            setup_started_at: std::time::Instant::now(),
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
    async fn test_run_watch_server_interrupted_text_is_failed_and_teardown_timed() {
        let plan = small_plan();
        let (client, shutdown, server) = serve_interrupted_watch_fixture().await;

        let result = run_with_ctrl_c(run_config(Arc::new(plan)), client, pending_ctrl_c()).await;

        assert_eq!(result.report.status(), crate::report::RunStatus::Failed);
        assert_eq!(
            result.report.experiment.failure_phase.as_deref(),
            Some("preparation")
        );
        assert!(
            result
                .report
                .experiment
                .first_error
                .as_deref()
                .is_some_and(|error| error.contains("interrupted by server"))
        );
        assert!(result.report.timing.observed_measurement_ms.is_none());
        assert!(result.report.timing.watch_teardown_ms.is_some());

        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_run_with_ctrl_c_create_failure_withholds_measurement() {
        let plan = Arc::new(small_plan());
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
        assert_eq!(result.report.status(), crate::report::RunStatus::Failed);
        assert!(result.report.timing.observed_measurement_ms.is_none());
        assert!(result.report.timing.prepare_ms.is_some());
        assert!(!result.report.accounting.measurement_complete);
        assert!(
            result
                .report
                .experiment
                .first_error
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
        let plan = Arc::new(small_plan());
        let prep_count = usize::try_from(preparation_total(&plan)).unwrap();
        let first_pod_key = plan.resource(ResourceKind::Pods).key(0);
        let (client, requests, shutdown, server) = serve_run_fixture(
            vec![Err(Status::unavailable("bootstrap response lost"))],
            None,
        )
        .await;
        let result = run_with_ctrl_c(run_config(plan.clone()), client, pending_ctrl_c()).await;
        assert_eq!(result.report.status(), crate::report::RunStatus::Failed);
        assert!(result.report.timing.prepare_ms.is_some());
        assert!(result.report.timing.observed_measurement_ms.is_none());
        assert!(!result.report.accounting.measurement_complete);
        assert!(
            result
                .report
                .experiment
                .first_error
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
        let plan = controlled_run_plan();
        let prep_count = usize::try_from(preparation_total(&plan)).unwrap();
        let (client, requests, _range_requests, gate, _created, _load_observed, shutdown, server) =
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
        let run = tokio::spawn(run_with_ctrl_c(run_config(Arc::new(plan)), client, ctrl_c));

        time::timeout(Duration::from_secs(1), observed)
            .await
            .expect("controlled fixture did not observe preparation request");
        assert_eq!(requests.lock().unwrap().len(), 1);
        cancel.send(()).unwrap();
        let result = run.await.unwrap();
        assert_eq!(
            result.report.status(),
            crate::report::RunStatus::Interrupted
        );
        assert_eq!(
            result.report.experiment.failure_phase.as_deref(),
            Some("preparation")
        );
        assert_eq!(
            result.report.experiment.first_error.as_deref(),
            Some("interrupted during preparation")
        );
        assert!(result.report.timing.prepare_ms.is_some());
        assert!(result.report.timing.observed_measurement_ms.is_none());
        assert!(!result.report.accounting.measurement_complete);
        assert!(result.report.accounting.no_active_tokens);
        assert!(
            result
                .report
                .preparation
                .logical
                .iter()
                .any(|row| row.canceled_queued.saturating_add(row.canceled_running) > 0)
        );
        gate.release.notify_one();
        join_remote_tasks(&gate).await;
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_run_fixture_holds_preparation_before_watch_and_load() {
        let plan = Arc::new(controlled_run_plan());
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

        time::timeout(Duration::from_secs(1), observed)
            .await
            .expect("controlled fixture did not observe preparation request");
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 1);
        assert_eq!(range_requests.load(AtomicOrdering::Acquire), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);

        gate.release.notify_one();
        join_remote_tasks(&gate).await;
        time::timeout(Duration::from_secs(1), created_ready)
            .await
            .expect("controlled fixture did not report watch readiness");
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

        time::timeout(Duration::from_secs(1), load_ready)
            .await
            .expect("controlled fixture did not observe load request");
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
        assert_eq!(
            result.report.status(),
            crate::report::RunStatus::Interrupted
        );
        assert!(result.report.timing.observed_measurement_ms.is_some());
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 0);

        join_remote_tasks(&gate).await;
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
        let run = tokio::spawn(run_with_ctrl_c(run_config(Arc::new(plan)), client, ctrl_c));

        time::timeout(Duration::from_secs(1), observed)
            .await
            .expect("controlled fixture did not observe preparation request");
        assert_eq!(range_requests.load(AtomicOrdering::Acquire), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);
        cancel.send(()).unwrap();
        let result = run.await.unwrap();
        assert_eq!(
            result.report.status(),
            crate::report::RunStatus::Interrupted
        );
        assert_eq!(
            result.report.experiment.first_error.as_deref(),
            Some("interrupted during preparation")
        );
        assert!(result.report.timing.observed_measurement_ms.is_none());
        assert!(!result.report.accounting.measurement_complete);
        assert!(result.report.accounting.no_active_tokens);
        assert!(
            result
                .report
                .preparation
                .logical
                .iter()
                .any(|row| row.canceled_queued.saturating_add(row.canceled_running) > 0)
        );
        gate.release.notify_one();
        join_remote_tasks(&gate).await;
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 0);
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
        let recorder = executor_recorder();
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 10 })]);
        let outcome = execute_txn(
            &client,
            &mut states,
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::Success);
        finish_executor_recorder(&recorder);
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
        let recorder = executor_recorder();
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 1 })]);
        let outcome = execute_txn(
            &client,
            &mut states,
            key.clone(),
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::Success);
        finish_executor_recorder(&recorder);
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
        let recorder = executor_recorder();
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let outcome = execute_txn(
            &client,
            &mut HashMap::new(),
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::Conflict);
        finish_executor_recorder(&recorder);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_txn_unknown_response_is_not_replayed() {
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Err(Status::unavailable("response lost"))]).await;
        let recorder = executor_recorder();
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let mut states = HashMap::from([(key.clone(), KeyState::Present { mod_revision: 10 })]);
        let outcome = execute_txn(
            &client,
            &mut states,
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy::until_deadline(),
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::ResultUnknown);
        finish_executor_recorder(&recorder);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_txn_expired_deadline_sends_no_rpc() {
        let (client, requests, shutdown, server) = serve_txn_fixture(Vec::new()).await;
        let recorder = executor_recorder();
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let outcome = execute_txn(
            &client,
            &mut HashMap::new(),
            key,
            b"value".to_vec(),
            ResourceOperation::Create,
            CasRetryPolicy::until_deadline(),
            false,
            Duration::ZERO,
            Arc::new(AtomicI64::new(0)),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::Timeout);
        assert!(!outcome.outcome.is_result_unknown());
        finish_executor_recorder(&recorder);
        assert!(requests.lock().unwrap().is_empty());
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn test_execute_txn_retry_budget_exhaustion_is_distinct_from_conflict_and_timeout() {
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Ok(compact_response(10, false, 2, b"latest"))]).await;
        let recorder = executor_recorder();
        let key = b"/registry/pods/bench-ns-00000/pod-00000".to_vec();
        let outcome = execute_txn(
            &client,
            &mut HashMap::from([(key.clone(), KeyState::Present { mod_revision: 1 })]),
            key,
            b"value".to_vec(),
            ResourceOperation::GuaranteedUpdate,
            CasRetryPolicy { max_attempts: 1 },
            true,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::RetryExhausted);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("retry policy exhausted"))
        );
        finish_executor_recorder(&recorder);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_heartbeat_unknown_response_is_not_replayed() {
        let (client, requests, shutdown, server) =
            serve_txn_fixture(vec![Err(Status::unavailable("heartbeat response lost"))]).await;
        let plan = small_plan();
        let recorder = executor_recorder();
        let mut states = HashMap::new();
        let outcome = execute_heartbeat(
            &client,
            &plan,
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
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(outcome.outcome, Outcome::ResultUnknown);
        finish_executor_recorder(&recorder);
        assert_eq!(requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_execute_range_invalid_page_via_real_rpc() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(Vec::new()).await;
        let recorder = executor_recorder();
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
            &recorder,
        )
        .await
        .unwrap_err();
        assert_eq!(error.outcome, Outcome::GrpcError);
        assert!(error.message.contains("more=true"));
        finish_executor_recorder(&recorder);
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
        let recorder = executor_recorder();
        let mut compactor = CompactorState::default();
        let revision = Arc::new(AtomicI64::new(0));
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                revision.clone(),
                &recorder,
            )
            .await
            .unwrap()
            .outcome,
            Outcome::Success,
        );
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                revision,
                &recorder,
            )
            .await
            .unwrap()
            .outcome,
            Outcome::Success,
        );
        finish_executor_recorder(&recorder);
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
        let recorder = executor_recorder();
        let mut compactor = CompactorState::default();
        assert_eq!(
            execute_compact_tick(
                &client,
                &mut compactor,
                Duration::from_secs(1),
                Arc::new(AtomicI64::new(0)),
                &recorder,
            )
            .await
            .unwrap()
            .outcome,
            Outcome::Success,
        );
        finish_executor_recorder(&recorder);
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
        let recorder = executor_recorder();
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
                &recorder,
            )
            .await
            .unwrap()
            .outcome,
            Outcome::Success,
        );
        finish_executor_recorder(&recorder);
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
        let recorder = executor_recorder();
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
                &recorder,
            )
            .await
            .unwrap()
            .outcome,
            Outcome::ResultUnknown,
        );
        finish_executor_recorder(&recorder);
        assert_eq!(txn_requests.lock().unwrap().len(), 1);
        assert_eq!(compact_requests.lock().unwrap().len(), 1);
        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_compact_missing_failure_key_recovers_next_tick() {
        let missing = TxnResponse {
            header: Some(ResponseHeader {
                revision: 10,
                ..Default::default()
            }),
            succeeded: false,
            ..Default::default()
        };
        let recovered = TxnResponse {
            header: Some(ResponseHeader {
                revision: 11,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let active = TxnResponse {
            header: Some(ResponseHeader {
                revision: 12,
                ..Default::default()
            }),
            succeeded: true,
            ..Default::default()
        };
        let (client, txn_requests, compact_requests, shutdown, server) = serve_compact_fixture(
            vec![Ok(missing), Ok(recovered), Ok(active)],
            vec![Ok(CompactionResponse::default())],
        )
        .await;
        let recorder = executor_recorder();
        let mut compactor = CompactorState::default();
        let latest_revision = Arc::new(AtomicI64::new(0));

        let first = execute_compact_tick(
            &client,
            &mut compactor,
            Duration::from_secs(1),
            latest_revision.clone(),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(first.outcome, Outcome::GrpcError);
        assert!(compact_requests.lock().unwrap().is_empty());

        let second = execute_compact_tick(
            &client,
            &mut compactor,
            Duration::from_secs(1),
            latest_revision.clone(),
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(second.outcome, Outcome::Success);
        assert!(compact_requests.lock().unwrap().is_empty());

        let third = execute_compact_tick(
            &client,
            &mut compactor,
            Duration::from_secs(1),
            latest_revision,
            &recorder,
        )
        .await
        .unwrap();
        assert_eq!(third.outcome, Outcome::Success);

        finish_executor_recorder(&recorder);
        let requests = txn_requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let expected_versions = [0, 0, 1];
        let expected_values: [&[u8]; 3] = [b"0", b"10", b"11"];
        for index in 0..3 {
            assert_eq!(
                requests[index].compare[0].target_union,
                Some(crate::pb::etcdserverpb::compare::TargetUnion::Version(
                    expected_versions[index],
                ))
            );
            let Some(crate::pb::etcdserverpb::request_op::Request::RequestPut(put)) =
                requests[index].success[0].request.as_ref()
            else {
                panic!("compact coordination request {index} did not contain a Put");
            };
            assert_eq!(put.value, expected_values[index]);
        }
        drop(requests);
        let compact_requests = compact_requests.lock().unwrap();
        assert_eq!(compact_requests.len(), 1);
        assert_eq!(compact_requests[0].revision, 11);
        drop(compact_requests);
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
        assert!(row.first_error.is_none());
    }
    #[tokio::test]
    async fn test_worker_server_deadline_exceeded_records_failed_timeout_unknown_once() {
        let (client, _requests, shutdown, server) =
            serve_txn_fixture(vec![Err(Status::deadline_exceeded("server deadline"))]).await;
        let plan = Arc::new(small_plan());
        let recorder = executor_recorder();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let item = WorkItem::Txn {
            key: plan.resource_key(stream.resource, 0, plan.config.seed),
            operation: ResourceOperation::GuaranteedUpdate,
            value: plan.resource_value(stream.resource, 0, plan.config.seed, 0),
            token: accepted_worker_token(&recorder, identity),
            preparation: None,
        };

        run_worker_items(client, plan, vec![item], Duration::from_secs(1))
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();

        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 1);
        assert_eq!(row.failed_before_cutoff, 1);
        assert_eq!(row.timeout_before_cutoff, 1);
        assert_eq!(row.result_unknown_before_cutoff, 1);
        assert_eq!(row.logical_latency.sample_count, 1);
        let rpc = &report.measurement.rpc[crate::measurement::RpcMethod::Txn.index()];
        assert_eq!(rpc.started, 1);
        assert_eq!(rpc.completed, 1);
        assert_eq!(rpc.failed, 1);
        assert_eq!(rpc.timeout, 1);
        assert_eq!(rpc.result_unknown, 1);
        assert_eq!(rpc.latency.sample_count, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn test_worker_outer_attempt_deadline_records_timeout_unknown_without_replay() {
        let (client, requests, _range_requests, gate, _created, _load_observed, shutdown, server) =
            serve_controlled_run_fixture(0).await;
        let plan = Arc::new(small_plan());
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let item = WorkItem::Txn {
            key: plan.resource_key(stream.resource, 0, plan.config.seed),
            operation: ResourceOperation::GuaranteedUpdate,
            value: plan.resource_value(stream.resource, 0, plan.config.seed, 0),
            token: accepted_worker_token(&recorder, identity),
            preparation: None,
        };
        let (sender, receiver) = mpsc::channel(1);
        sender.send(item).await.unwrap();
        drop(sender);
        let worker = tokio::spawn(worker_loop(
            receiver,
            client,
            plan,
            Duration::from_millis(1),
            Arc::new(AtomicI64::new(0)),
        ));

        time::timeout(Duration::from_secs(1), gate.observed.notified())
            .await
            .expect("controlled fixture did not observe request");
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 1);
        time::advance(Duration::from_millis(1)).await;
        worker.await.unwrap().unwrap();
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 1);
        assert_eq!(requests.lock().unwrap().len(), 1);

        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.failed_before_cutoff, 1);
        assert_eq!(row.timeout_before_cutoff, 1);
        assert_eq!(row.result_unknown_before_cutoff, 1);
        assert_eq!(row.logical_latency.sample_count, 1);
        let rpc = &report.measurement.rpc[crate::measurement::RpcMethod::Txn.index()];
        assert_eq!(rpc.failed, 1);
        assert_eq!(rpc.timeout, 1);
        assert_eq!(rpc.result_unknown, 1);
        assert_eq!(rpc.latency.sample_count, 1);

        let finished = gate.finished.notified();
        gate.release.notify_one();
        time::timeout(Duration::from_secs(1), finished)
            .await
            .expect("controlled fixture did not finish remote work");
        join_remote_tasks(&gate).await;
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 0);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn test_worker_txn_failure_preserves_first_error_while_continuing() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(vec![
            Err(Status::permission_denied("txn denied first")),
            Err(Status::permission_denied("txn denied later")),
            Ok(TxnResponse {
                succeeded: true,
                ..Default::default()
            }),
        ])
        .await;
        let plan = Arc::new(small_plan());
        let recorder = executor_recorder();
        let stream = plan
            .logical_streams
            .iter()
            .find(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let identity = stream_identity(stream);
        let resource = stream.resource;
        let key = plan.resource_key(resource, 0, plan.config.seed);
        let value = plan.resource_value(resource, 0, plan.config.seed, 0);
        let make_item = |token| WorkItem::Txn {
            key: key.clone(),
            operation: ResourceOperation::GuaranteedUpdate,
            value: value.clone(),
            token,
            preparation: None,
        };
        let items = vec![
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
        ];

        run_worker_items(client, plan.clone(), items, Duration::from_secs(1))
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();

        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 3);
        assert_eq!(row.failed_before_cutoff, 2);
        assert_eq!(row.succeeded_before_cutoff, 1);
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| error.contains("txn denied first"))
        );
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| !error.contains("txn denied later"))
        );
    }

    #[tokio::test]
    async fn test_worker_heartbeat_failure_preserves_first_error_while_continuing() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(vec![
            Err(Status::permission_denied("heartbeat denied first")),
            Err(Status::permission_denied("heartbeat denied later")),
            Ok(TxnResponse {
                succeeded: true,
                ..Default::default()
            }),
        ])
        .await;
        let plan = Arc::new(small_plan());
        let recorder = executor_recorder();
        let identity = LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::NodeLease,
        };
        let make_item = |token| WorkItem::Heartbeat {
            item: HeartbeatItem {
                node: 0,
                kind: HeartbeatItemKind::NodeLeaseRenew,
                key: "/registry/leases/node-00000".into(),
                at: Duration::ZERO,
            },
            resource: ResourceKind::NodeLeases,
            token,
        };
        let items = vec![
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
        ];

        run_worker_items(client, plan, items, Duration::from_secs(1))
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();

        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 3);
        assert_eq!(row.failed_before_cutoff, 2);
        assert_eq!(row.succeeded_before_cutoff, 1);
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| error.contains("heartbeat denied first"))
        );
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| !error.contains("heartbeat denied later"))
        );
    }

    #[tokio::test]
    async fn test_worker_status_failure_preserves_first_error_while_continuing() {
        let (client, shutdown, server) = serve_status_fixture(vec![
            Err(Status::permission_denied("status denied first")),
            Err(Status::permission_denied("status denied later")),
            Ok(StatusResponse {
                header: Some(ResponseHeader {
                    revision: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ])
        .await;
        let plan = Arc::new(small_plan());
        let recorder = executor_recorder();
        let identity = LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::Status,
        };
        let make_item = |token| WorkItem::Status { token };
        let items = vec![
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
        ];

        run_worker_items(client, plan, items, Duration::from_secs(1))
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();

        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 3);
        assert_eq!(row.failed_before_cutoff, 2);
        assert_eq!(row.succeeded_before_cutoff, 1);
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| error.contains("status denied first"))
        );
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| !error.contains("status denied later"))
        );
    }

    #[tokio::test]
    async fn test_worker_compact_failure_preserves_first_error_while_continuing() {
        let (client, _requests, shutdown, server) = serve_txn_fixture(vec![
            Err(Status::permission_denied("compact denied first")),
            Err(Status::permission_denied("compact denied later")),
            Ok(TxnResponse {
                header: Some(ResponseHeader {
                    revision: 2,
                    ..Default::default()
                }),
                succeeded: true,
                ..Default::default()
            }),
        ])
        .await;
        let plan = Arc::new(small_plan());
        let recorder = executor_recorder();
        let identity = LogicalStreamIdentity::Periodic {
            kind: PeriodicKind::Compact,
        };
        let make_item = |token| WorkItem::CompactTick { token };
        let items = vec![
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
            make_item(accepted_worker_token(&recorder, identity)),
        ];

        run_worker_items(client, plan, items, Duration::from_secs(1))
            .await
            .unwrap();
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();

        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.accepted, 3);
        assert_eq!(row.failed_before_cutoff, 2);
        assert_eq!(row.succeeded_before_cutoff, 1);
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| error.contains("compact denied first"))
        );
        assert!(
            row.first_error
                .as_deref()
                .is_some_and(|error| !error.contains("compact denied later"))
        );
    }
    #[tokio::test]
    async fn test_txn_executor_uses_actual_payload_and_completion_phase() {
        let plan = Arc::new(small_plan());
        let clock = ExecutorPhaseClock::new();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        recorder.mark_setup_complete().unwrap();
        recorder.start_measurement().unwrap();

        let (client, requests, _range_requests, gate, _created, _load_observed, shutdown, server) =
            serve_controlled_run_fixture(0).await;
        let key = b"/registry/pods/direct".to_vec();
        let value = b"payload".to_vec();
        let mut states = HashMap::new();
        let mut execution = Box::pin(execute_txn(
            &client,
            &mut states,
            key,
            value,
            ResourceOperation::Create,
            CasRetryPolicy::until_deadline(),
            false,
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
            &recorder,
        ));
        let observed = gate.observed.notified();
        tokio::select! {
            result = &mut execution => panic!("executor completed before the tonic response: {result:?}"),
            _ = observed => {}
        }
        clock.set_millis(25);
        gate.release.notify_one();
        assert_eq!(execution.await.unwrap().outcome, Outcome::Success);
        join_remote_tasks(&gate).await;

        let request = requests.lock().unwrap().first().cloned().unwrap();
        let expected_request_payload = request.encoded_len() as u64;
        let expected_response_payload = TxnResponse {
            succeeded: true,
            ..Default::default()
        }
        .encoded_len() as u64;
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let measurement_rpc = &report.measurement.rpc[crate::measurement::RpcMethod::Txn.index()];
        let drain_rpc = &report.drain.rpc[crate::measurement::RpcMethod::Txn.index()];
        assert_eq!(measurement_rpc.started, 1);
        assert_eq!(measurement_rpc.completed, 0);
        assert_eq!(
            measurement_rpc.request_payload_bytes,
            expected_request_payload
        );
        assert_eq!(measurement_rpc.response_payload_bytes, 0);
        assert_eq!(drain_rpc.started, 0);
        assert_eq!(drain_rpc.completed, 1);
        assert_eq!(drain_rpc.request_payload_bytes, 0);
        assert_eq!(drain_rpc.response_payload_bytes, expected_response_payload);

        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
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
    async fn test_prepare_pending_reserve_deadline_and_cancel() {
        for cancel_case in [false, true] {
            let plan = Arc::new(small_plan());
            let recorder = MeasurementRecorder::new(&plan).unwrap();
            let (sender, _receiver) = mpsc::channel::<WorkItem>(1);
            let held_permit = sender.reserve().await.unwrap();
            let make_item = || {
                let token = match recorder
                    .offer_preparation(PreparationStream::Create(ResourceKind::Pods))
                    .unwrap()
                {
                    OfferResult::Token(token) => token,
                    other => panic!("unexpected preparation offer result: {other:?}"),
                };
                WorkItem::Txn {
                    key: plan.resource(ResourceKind::Pods).key(0),
                    operation: ResourceOperation::Create,
                    value: plan.resource(ResourceKind::Pods).value(0, 0),
                    token,
                    preparation: None,
                }
            };
            let item = make_item();
            let deadline = TokioInstant::now() + Duration::from_millis(1);
            let error = if cancel_case {
                let (cancel, receiver) = oneshot::channel();
                let mut ctrl_c: CtrlCFuture = Box::pin(async move {
                    receiver
                        .await
                        .map_err(|_| {
                            std::io::Error::new(std::io::ErrorKind::Other, "cancel sender dropped")
                        })
                        .map(|_| ())
                });
                let mut enqueue = Box::pin(enqueue_preparation_item(
                    &sender,
                    item,
                    deadline,
                    &mut ctrl_c,
                ));
                assert_pending(enqueue.as_mut()).await;
                cancel.send(()).unwrap();
                enqueue.await.unwrap_err()
            } else {
                let mut ctrl_c = pending_ctrl_c();
                let mut enqueue = Box::pin(enqueue_preparation_item(
                    &sender,
                    item,
                    deadline,
                    &mut ctrl_c,
                ));
                assert_pending(enqueue.as_mut()).await;
                time::advance(Duration::from_millis(1)).await;
                enqueue.await.unwrap_err()
            };
            assert!(if cancel_case {
                matches!(error, ControlFailure::Interrupted(_))
            } else {
                matches!(error, ControlFailure::ExecutionFailure(_))
            });
            drop(held_permit);
            drop(sender);
            let report = recorder.finish_without_measurement().unwrap();
            let row =
                &report.preparation.logical[PreparationStream::Create(ResourceKind::Pods).index()];
            assert_eq!(row.offered, 0);
            assert_eq!(row.accepted, 0);
            assert_eq!(row.not_offered_after_stop, plan.resource_counts.pods);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_prepare_success_deficit_blocks_run() {
        let plan = Arc::new(small_plan());
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(128);
        let senders = vec![sender.clone()];
        let first_success = Arc::new(Notify::new());
        let worker_success = first_success.clone();
        let worker = tokio::spawn(async move {
            let mut skipped_completion = false;
            while let Some(item) = receiver.recv().await {
                let preparation = item
                    .preparation()
                    .cloned()
                    .expect("preparation item tracker");
                item.started().unwrap();
                item.terminal(Outcome::Success).unwrap();
                if skipped_completion {
                    preparation.complete(Outcome::Success);
                } else {
                    skipped_completion = true;
                    worker_success.notify_one();
                }
            }
        });
        let deadline = TokioInstant::now() + Duration::from_millis(1);
        let mut ctrl_c = pending_ctrl_c();
        let mut preparation = Box::pin(run_preparation(
            plan.clone(),
            &recorder,
            &senders,
            deadline,
            &mut ctrl_c,
        ));
        tokio::select! {
            result = &mut preparation => {
                panic!("preparation completed before success deficit was observed: {result:?}");
            }
            _ = first_success.notified() => {}
        }
        let error = preparation.await.unwrap_err();
        assert!(matches!(error, ControlFailure::ExecutionFailure(_)));
        assert!(TokioInstant::now() >= deadline);
        drop(senders);
        drop(sender);
        worker.await.unwrap();
        let report = recorder.finish_without_measurement().unwrap();
        assert!(report.timing.preparation_ns.is_none());
        assert!(!report.accounting.measurement_complete);
        assert!(
            report
                .preparation
                .logical
                .iter()
                .any(|row| row.succeeded_before_cutoff > 0)
        );
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
        assert_eq!(error.to_string(), "watch readiness timed out");
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
        assert_eq!(
            waiting.await.unwrap_err().to_string(),
            "watch readiness timed out"
        );
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
        assert_eq!(error.to_string(), "interrupted during preparation");
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
                    WorkItem::Txn { key, operation, .. } => worker_observed
                        .lock()
                        .unwrap()
                        .push((*operation, key.clone())),
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
            .position(|(operation, key)| {
                *operation == ResourceOperation::GuaranteedUpdate && key == &node_key
            })
            .expect("NodeReady request missing");
        assert!(
            observed[..node_ready]
                .iter()
                .all(|(operation, _)| *operation == ResourceOperation::Create)
        );
        assert_eq!(recorder.phase(), crate::measurement::Phase::Preparation);
    }

    #[tokio::test]
    async fn test_interrupted_dispatch_preserves_partial_report() {
        let mut config = small_plan().config;
        config.load.duration_ms = 1_000;
        let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        recorder.start_measurement().unwrap();
        let mut workers = JoinSet::new();
        let mut watches = JoinSet::new();
        let (sender, mut work_rx) = mpsc::channel(1);
        let (cancel, receiver) = oneshot::channel::<()>();
        workers.spawn(async move {
            let _item = work_rx.recv().await.expect("dispatch item");
            cancel.send(()).expect("dispatch cancellation receiver");
            std::future::pending::<()>().await;
            Ok::<(), String>(())
        });
        let mut ctrl_c: CtrlCFuture = Box::pin(async move {
            receiver
                .await
                .map(|_| ())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "interrupt"))
        });
        let started = TokioInstant::now();
        let cutoff = started + run_duration(&plan);
        let exit = dispatch_until(
            &plan,
            &recorder,
            &[sender],
            &mut workers,
            &mut watches,
            started,
            cutoff,
            &mut ctrl_c,
            time::sleep_until,
        )
        .await;
        let mut state = match exit {
            DispatchExit::Interrupted(state) => state,
            _ => panic!("expected interrupted dispatch"),
        };
        workers.abort_all();
        while workers.join_next().await.is_some() {}
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
            time::sleep_until,
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
                WorkItem::Txn { key, .. } => {
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
            time::sleep_until,
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
            time::sleep_until,
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
            plan.resource_key(stream.resource, 0, plan.config.seed),
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
            plan.resource_key(stream.resource, 0, plan.config.seed),
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
        let result = dispatch_application_item(
            &plan,
            &recorder,
            stream,
            0,
            plan.resource_key(stream.resource, 0, plan.config.seed),
            &sender,
            TokioInstant::now() + Duration::from_secs(1),
            TokioInstant::now() + Duration::from_secs(1),
            1_000_000,
        );
        assert!(result.is_err());
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
        abort_workers(vec![sender], tasks).await.unwrap();
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.canceled_running, 1);
        assert_eq!(row.accepted, 1);
        assert_eq!(
            report.drain.logical[identity.measurement_index().unwrap()].canceled_running,
            1
        );
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
    async fn test_measurement_start_waits_for_current_watch_ready() {
        let plan = small_plan();
        let recorder = MeasurementRecorder::new(&plan).unwrap();
        let mut ready_receivers = Vec::new();
        recorder.watch_created(0).unwrap();
        let (first_ready_tx, first_ready_rx) = oneshot::channel();
        first_ready_tx.send(()).unwrap();
        ready_receivers.push(first_ready_rx);
        recorder.watch_disconnect(0).unwrap();
        for index in 1..plan.watch_specs().len() {
            recorder.watch_created(index).unwrap();
            let (ready_tx, ready_rx) = oneshot::channel();
            ready_tx.send(()).unwrap();
            ready_receivers.push(ready_rx);
        }

        let mut tasks = JoinSet::new();
        tasks.spawn(std::future::pending::<WatchTaskOutput>());
        let deadline = TokioInstant::now() + Duration::from_secs(1);
        let mut ctrl_c = pending_ctrl_c();
        let mut waiting = Box::pin(wait_for_measurement_start(
            &mut tasks,
            ready_receivers,
            deadline,
            &mut ctrl_c,
            &recorder,
        ));
        assert_pending(waiting.as_mut()).await;
        assert_eq!(recorder.phase(), crate::measurement::Phase::Preparation);

        recorder.watch_created(0).unwrap();
        let _window = waiting.await.unwrap();
        assert_eq!(recorder.phase(), crate::measurement::Phase::Measurement);

        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        recorder.stop_measurement().unwrap();
        recorder.finish().unwrap();
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
            error.to_string(),
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
        assert_eq!(error.to_string(), "watch readiness timed out");
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
    async fn test_watch_teardown_ctrl_c_aborts_and_joins_tasks() {
        let (shutdown, _state) = shutdown_watch::channel(false);
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sentinel = DropSentinel(dropped.clone());
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let _sentinel = sentinel;
            std::future::pending::<WatchTaskOutput>().await
        });
        let mut ctrl_c: CtrlCFuture = Box::pin(async { Ok::<(), std::io::Error>(()) });
        let (result, interrupted) =
            stop_watches_with_ctrl_c(shutdown, tasks, Duration::from_secs(1), &mut ctrl_c).await;
        assert!(interrupted);
        assert_eq!(result.unwrap_err(), "interrupted during watch teardown");
        assert!(dropped.load(AtomicOrdering::Acquire));
    }

    #[tokio::test]
    async fn test_worker_drain_ctrl_c_aborts_and_joins_tasks() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sentinel = DropSentinel(dropped.clone());
        let mut workers = JoinSet::new();
        workers.spawn(async move {
            let _sentinel = sentinel;
            std::future::pending::<Result<(), String>>().await
        });
        let mut ctrl_c: CtrlCFuture = Box::pin(async { Ok::<(), std::io::Error>(()) });
        let (result, interrupted) =
            drain_workers_with_ctrl_c(Vec::new(), workers, Duration::from_secs(1), &mut ctrl_c)
                .await;
        assert!(interrupted);
        assert_eq!(result.unwrap_err(), "interrupted during drain");
        assert!(dropped.load(AtomicOrdering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn test_worker_drain_timeout_joins_and_drops_task() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sentinel = DropSentinel(dropped.clone());
        let mut workers = JoinSet::new();
        workers.spawn(async move {
            let _sentinel = sentinel;
            std::future::pending::<Result<(), String>>().await
        });
        let mut ctrl_c = pending_ctrl_c();
        let mut drain = Box::pin(drain_workers_with_ctrl_c(
            Vec::new(),
            workers,
            Duration::from_millis(1),
            &mut ctrl_c,
        ));
        assert_pending(drain.as_mut()).await;
        time::advance(Duration::from_millis(1)).await;
        let (result, interrupted) = drain.await;
        assert!(!interrupted);
        let error = result.unwrap_err();
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
            &time::sleep_until,
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
        let mut ctrl_c = pending_ctrl_c();
        let (result, interrupted) =
            drain_workers_with_ctrl_c(Vec::new(), workers, Duration::from_secs(1), &mut ctrl_c)
                .await;
        assert!(!interrupted);
        assert!(result.unwrap_err().contains("worker failed"));
    }
    #[tokio::test]
    async fn test_worker_late_dequeue_runs_once_in_drain() {
        let (client, requests, _range_requests, gate, _created, _load_observed, shutdown, server) =
            serve_controlled_run_fixture(0).await;
        let plan = Arc::new(small_plan());
        let clock = ExecutorPhaseClock::new();
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
        let make_item = |ordinal| WorkItem::Txn {
            key: plan.resource_key(stream.resource, ordinal, plan.config.seed),
            operation: ResourceOperation::GuaranteedUpdate,
            value: plan.resource_value(stream.resource, ordinal, plan.config.seed, ordinal),
            token: accepted_worker_token(&recorder, identity),
            preparation: None,
        };
        let first = make_item(0);
        let second = make_item(1);
        let (sender, receiver) = mpsc::channel(1);
        sender.send(first).await.unwrap();
        let worker = tokio::spawn(worker_loop(
            receiver,
            client.clone(),
            plan.clone(),
            Duration::from_secs(1),
            Arc::new(AtomicI64::new(0)),
        ));
        sender.send(second).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), gate.observed.notified())
            .await
            .expect("first RPC was not observed");
        drop(sender);

        clock.set_millis(20);
        recorder.stop_measurement().unwrap();
        clock.set_millis(25);
        gate.release.notify_one();
        worker.await.unwrap().unwrap();
        join_remote_tasks(&gate).await;
        assert_eq!(gate.inflight.load(AtomicOrdering::Acquire), 0);

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let keys = requests.iter().filter_map(txn_put_key).collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                plan.resource_key(stream.resource, 0, plan.config.seed),
                plan.resource_key(stream.resource, 1, plan.config.seed),
            ]
        );
        drop(requests);

        let report = recorder.finish().unwrap();
        let measurement = &report.measurement.logical[identity.measurement_index().unwrap()];
        let drain = &report.drain.logical[identity.measurement_index().unwrap()];
        assert_eq!(measurement.accepted, 2);
        assert_eq!(measurement.started_before_cutoff, 1);
        assert_eq!(measurement.started_after_cutoff, 1);
        assert_eq!(measurement.terminal_after_cutoff, 2);
        assert_eq!(measurement.cutoff_queued, 1);
        assert_eq!(measurement.cutoff_running, 1);
        assert_eq!(drain.started_after_cutoff, 1);
        assert_eq!(drain.terminal_after_cutoff, 2);
        let measurement_rpc = &report.measurement.rpc[crate::measurement::RpcMethod::Txn.index()];
        let drain_rpc = &report.drain.rpc[crate::measurement::RpcMethod::Txn.index()];
        assert_eq!(measurement_rpc.started, 1);
        assert_eq!(measurement_rpc.completed, 0);
        assert_eq!(drain_rpc.started, 1);
        assert_eq!(drain_rpc.completed, 2);
        assert!(report.accounting.accepted_cohort_conserved);

        drop(client);
        shutdown.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_dispatch_resume_skips_old_ticks() {
        let mut config = small_plan().config;
        config.load.duration_ms = 20;
        config.load.update_rps = 1_000;
        config.load.read_ratio.numerator = 0;
        let plan = Arc::new(CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap());
        let clock = ExecutorPhaseClock::new();
        let recorder =
            MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
                .unwrap();
        record_test_preparation(&recorder, &plan);
        recorder.start_measurement().unwrap();
        let stream_index = plan
            .logical_streams
            .iter()
            .position(|stream| stream.work_class == LogicalWorkClass::Update)
            .unwrap();
        let stream = &plan.logical_streams[stream_index];
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(64);
        let senders = vec![sender.clone()];
        let mut worker_tasks = JoinSet::new();
        let mut watch_tasks = JoinSet::new();
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
            time::sleep_until,
        ));
        assert_pending(dispatch.as_mut()).await;
        let collect_keys = |receiver: &mut mpsc::Receiver<WorkItem>| {
            let mut keys = Vec::new();
            while let Ok(item) = receiver.try_recv() {
                match &item {
                    WorkItem::Txn { key, .. } => keys.push(key.clone()),
                    other => panic!("paused dispatcher emitted unexpected item: {other:?}"),
                }
                drop(item);
            }
            keys
        };
        assert_eq!(
            collect_keys(&mut receiver),
            vec![plan.resource_key(stream.resource, 0, plan.config.seed)]
        );

        time::advance(Duration::from_millis(5)).await;
        assert_pending(dispatch.as_mut()).await;
        assert_eq!(
            collect_keys(&mut receiver),
            vec![plan.resource_key(stream.resource, 5, plan.config.seed)]
        );

        time::advance(Duration::from_millis(20)).await;
        let state = match dispatch.await {
            DispatchExit::Completed(state) => state,
            DispatchExit::Interrupted(_) => panic!("dispatcher was interrupted"),
            DispatchExit::Failed(error, _) => panic!("dispatcher failed: {error}"),
        };
        assert!(collect_keys(&mut receiver).is_empty());
        assert_eq!(state.next_ordinals[stream_index], stream.planned);
        drop(senders);
        drop(sender);
        drop(receiver);

        clock.set_millis(20);
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[stream_identity(stream).measurement_index().unwrap()];
        assert_eq!(row.offered, 2);
        assert_eq!(row.accepted, 2);
        assert_eq!(row.missed_schedule, stream.planned - 2);
        assert_eq!(row.canceled_queued, 2);
        assert!(report.accounting.planned_conserved);
    }

    #[tokio::test(start_paused = true)]
    async fn test_precise_ticks_offer_entire_update_budget() {
        let mut config = small_plan().config;
        config.load.update_rps = 2_000;
        config.load.read_ratio.numerator = 0;
        let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
        let clock = ExecutorPhaseClock::new();
        let recorder =
            MeasurementRecorder::with_clock(&plan, clock.clone() as Arc<dyn MonotonicClock>)
                .unwrap();
        record_test_preparation(&recorder, &plan);
        recorder.start_measurement().unwrap();
        let (sender, mut receiver) = mpsc::channel::<WorkItem>(64);
        let senders = vec![sender];
        let mut workers = JoinSet::new();
        let mut watches = JoinSet::new();
        let mut ctrl_c = pending_ctrl_c();
        let started = TokioInstant::now();
        let cutoff = started + run_duration(&plan);
        // Drive exact timer wakeups through the production dispatcher.
        let mut dispatch = Box::pin(dispatch_until(
            &plan,
            &recorder,
            &senders,
            &mut workers,
            &mut watches,
            started,
            cutoff,
            &mut ctrl_c,
            |deadline| {
                std::future::poll_fn(move |_| {
                    if TokioInstant::now() >= deadline {
                        std::task::Poll::Ready(())
                    } else {
                        std::task::Poll::Pending
                    }
                })
            },
        ));
        let mut offered = 0;
        for tick in 0..plan.config.load.duration_ms {
            clock.set_millis(tick);
            assert_pending(dispatch.as_mut()).await;
            while let Ok(item) = receiver.try_recv() {
                offered += 1;
                drop(item);
            }
            time::advance(crate::plan::PACING_TICK).await;
        }
        clock.set_millis(plan.config.load.duration_ms);
        assert!(matches!(dispatch.await, DispatchExit::Completed(_)));
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let updates = report
            .measurement
            .logical
            .iter()
            .find(|row| row.work_class == "update")
            .unwrap();
        assert_eq!(offered, 40);
        assert_eq!(updates.offered, updates.planned);
        assert_eq!(updates.missed_schedule, 0);
        assert_eq!(updates.dropped_full, 0);
        assert!(report.accounting.planned_conserved);
    }
}
