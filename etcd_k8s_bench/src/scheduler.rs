use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

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

    fn signal_consumed(&self) -> bool {
        matches!(self, Self::Interrupted(_) | Self::SignalFailure(_))
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

    fn send_for_tick(
        self,
        permit: mpsc::Permit<'_, WorkItem>,
        tick_end_ns: u64,
    ) -> Result<DispatchOutcome, String> {
        match self.accept_for_tick(tick_end_ns)? {
            TokenAction::Applied => {
                permit.send(self);
                Ok(DispatchOutcome::Accepted)
            }
            TokenAction::RejectedAfterTick | TokenAction::RejectedAfterCutoff => {
                Ok(DispatchOutcome::MissedToken)
            }
            TokenAction::RejectedAfterStop => Ok(DispatchOutcome::Stopped),
            TokenAction::AlreadyFinalized => {
                Err("measurement token finalized before admission".into())
            }
        }
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
    let deadline = TokioInstant::now() + timeout;
    let mut pager = RangePager::new(request, selector);
    let mut page_offset = 0usize;
    loop {
        let page_request = pager.request();
        let mut attempts = 0;
        let response = loop {
            let remaining = deadline.saturating_duration_since(TokioInstant::now());
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
    let deadline = TokioInstant::now() + timeout;
    let mut first_error = None;
    loop {
        let remaining = deadline.saturating_duration_since(TokioInstant::now());
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
                            .min(deadline.saturating_duration_since(TokioInstant::now()));
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

    let deadline = TokioInstant::now() + timeout;
    let remaining = deadline.saturating_duration_since(TokioInstant::now());
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
    let remaining = deadline.saturating_duration_since(TokioInstant::now());
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

struct DrainResult {
    failure: Option<ControlFailure>,
    timed_out: bool,
}

async fn drain_workers_with_ctrl_c(
    senders: Vec<mpsc::Sender<WorkItem>>,
    mut tasks: JoinSet<Result<(), String>>,
    timeout: Duration,
    ctrl_c: &mut CtrlCFuture,
) -> DrainResult {
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
                let failure = match signal {
                    Ok(()) => ControlFailure::interrupted(
                        first_error.unwrap_or_else(|| "interrupted during drain".into())
                    ),
                    Err(error) => ControlFailure::signal_failure(first_error.map_or_else(
                        || format!("ctrl-c signal failed: {error}"),
                        |first| format!("ctrl-c signal failed: {error}; {first}"),
                    )),
                };
                return DrainResult { failure: Some(failure), timed_out: false };
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
                return DrainResult {
                    failure: first_error.map(ControlFailure::execution_failure),
                    timed_out: true,
                };
            }
        };
        let Some(joined) = joined else {
            break;
        };
        capture_worker_join_error(joined, &mut first_error);
    }
    DrainResult {
        failure: first_error.map(ControlFailure::execution_failure),
        timed_out: false,
    }
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
    item.send_for_tick(permit, tick_end_ns)
}

fn dispatch_periodic_item(
    plan: &CompiledPlan,
    recorder: &MeasurementRecorder,
    event: &PeriodicEvent,
    sender: &mpsc::Sender<WorkItem>,
    tick_deadline: TokioInstant,
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
    item.send_for_tick(permit, tick_end_ns)
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
    plan.periodic_events
        .partition_point(|event| event.at_ns < at_ns)
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
        let first = index.partition_point(|&position| position < start);
        let last = index.partition_point(|&position| position < end);
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
) -> Result<ScheduleControl, ControlFailure> {
    tokio::select! {
        biased;
        signal = ctrl_c.as_mut() => match signal {
            Ok(()) => Ok(ScheduleControl::Interrupted),
            Err(error) => Err(ControlFailure::signal_failure(format!("ctrl-c signal failed: {error}"))),
        },
        worker_result = worker_tasks.join_next(), if !worker_tasks.is_empty() => {
            match worker_result {
                Some(Ok(Ok(()))) => Err(ControlFailure::execution_failure("worker exited before the run ended")),
                Some(Ok(Err(error))) => Err(ControlFailure::execution_failure(error)),
                Some(Err(error)) => Err(ControlFailure::execution_failure(format!("worker task failed: {error}"))),
                None => Err(ControlFailure::execution_failure("all workers exited before the run ended")),
            }
        }
        watch_result = watch_tasks.join_next(), if !watch_tasks.is_empty() => {
            match watch_result {
                Some(Ok((_name, Ok(_outcome)))) => Ok(ScheduleControl::Supervisor),
                Some(Ok((name, Err(error)))) => Err(ControlFailure::execution_failure(format!("watch {name} failed: {error}"))),
                Some(Err(error)) => Err(ControlFailure::execution_failure(format!("watch task failed: {error}"))),
                None => Err(ControlFailure::execution_failure("all watch tasks exited before the run ended")),
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
    Failed(ControlFailure, DispatchState),
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
        Err(error) => {
            return DispatchExit::Failed(
                ControlFailure::execution_failure(error),
                DispatchState::new(plan),
            );
        }
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
                Err(error) => DispatchExit::Failed(ControlFailure::execution_failure(error), state),
            };
        }
        let current_tick = elapsed_ns / tick_ns;
        if current_tick > state.next_tick {
            let skipped_start_ns = current_tick.saturating_mul(tick_ns).min(duration_ns);
            if let Err(error) =
                skip_application_before(plan, recorder, &mut state, skipped_start_ns)
            {
                return DispatchExit::Failed(ControlFailure::execution_failure(error), state);
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
        ) {
            return DispatchExit::Failed(ControlFailure::execution_failure(error), state);
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
        Err(error) => DispatchExit::Failed(ControlFailure::execution_failure(error), state),
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
    let deadline = TokioInstant::now() + timeout;
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
        let mut cas = CasLoop::new(attempt, cas_policy, deadline.into_std());
        let next_operation = loop {
            let request = cas.request();
            let start = TokioInstant::now();
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
            match cas.observe(&response, TokioInstant::now().into_std()) {
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
            .is_some_and(ControlFailure::signal_consumed);
    let drain_result = if ctrl_c_completed {
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
    if drain_result
        .failure
        .as_ref()
        .is_some_and(ControlFailure::is_interrupted)
    {
        interrupted = true;
    }
    let drain_signal_completed = drain_result
        .failure
        .as_ref()
        .is_some_and(ControlFailure::signal_consumed);
    if drain_result.timed_out {
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
        .map(|error| error.to_string())
        .or_else(|| drain_result.failure.map(|error| error.to_string()))
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
#[path = "../tests/unit/scheduler.rs"]
mod tests;
