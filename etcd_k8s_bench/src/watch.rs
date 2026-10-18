//! Watch stream construction, lifecycle state, and resumable event consumption.
use std::pin::Pin;

use thiserror::Error;
use tokio::sync::{oneshot, watch as deadline_channel, watch as shutdown_channel};
use tokio::time::{Duration, Instant, sleep_until};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Status};

use crate::client::ClientBundle;
use crate::config::WatchSpec;
use crate::measurement::{
    MeasurementRecorder, WATCH_TERMINAL_CANCELED, WATCH_TERMINAL_COMPACTED,
    WATCH_TERMINAL_NO_LEADER, WATCH_TERMINAL_PHASE_DEADLINE, WATCH_TERMINAL_SEMANTIC,
};
use crate::pb::etcdserverpb::{
    WatchCancelRequest, WatchCreateRequest, WatchRequest, WatchResponse, watch_request,
};
use crate::pb::mvccpb;

type WatchResponseStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;
/// A shared absolute deadline used only while opening or reconnecting a watch.
/// Established streams remain open until the scheduler explicitly shuts them
/// down so drain-time responses are still observed.
#[derive(Clone)]
pub struct WatchDeadline {
    deadline: deadline_channel::Sender<Option<Instant>>,
}

impl Default for WatchDeadline {
    fn default() -> Self {
        let (deadline, _receiver) = deadline_channel::channel(None);
        Self { deadline }
    }
}

impl WatchDeadline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, deadline: Instant) {
        self.deadline.send_replace(Some(deadline));
    }

    fn subscribe(&self) -> deadline_channel::Receiver<Option<Instant>> {
        self.deadline.subscribe()
    }
}
#[tonic::async_trait]
pub(crate) trait WatchConnector: Clone + Send + Sync + 'static {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static;
}

#[tonic::async_trait]
impl WatchConnector for ClientBundle {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        self.watch_stream(requests)
            .await
            .map(|stream| Box::pin(stream) as WatchResponseStream)
    }
}

/// Return the etcd range end for a byte prefix.
pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    if prefix.is_empty() {
        return vec![0];
    }
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

pub fn create_request(spec: &WatchSpec, start_revision: i64) -> WatchRequest {
    WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: spec.resource_prefix.as_bytes().to_vec(),
                range_end: prefix_range_end(spec.resource_prefix.as_bytes()),
                start_revision,
                prev_kv: spec.prev_kv,
                progress_notify: spec.progress_notify,
                ..Default::default()
            },
        )),
    }
}

pub fn cancel_request(watch_id: i64) -> WatchRequest {
    WatchRequest {
        request_union: Some(watch_request::RequestUnion::CancelRequest(
            WatchCancelRequest { watch_id },
        )),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchState {
    pub last_revision: i64,
    pub watch_id: i64,
    pub event_count: u64,
    pub event_bytes: u64,
    pub reconnects: u64,
    /// Whether `watch_id` belongs to the currently established substream.
    watch_id_valid: bool,
    /// Recovery baseline from an initial current (start_revision=0) Created.
    ///
    /// This is deliberately separate from `last_revision`: a Created header
    /// acknowledges the current store revision but does not deliver an event.
    recovery_revision: i64,
}

impl WatchState {
    pub fn next_start_revision(&self, configured: i64) -> i64 {
        if self.last_revision > 0 {
            self.last_revision.saturating_add(1)
        } else if self.recovery_revision > 0 {
            self.recovery_revision
        } else {
            configured
        }
    }

    pub fn observe(&mut self, response: &WatchResponse) -> Option<WatchTerminal> {
        if response.watch_id != -1 {
            self.watch_id = response.watch_id;
            self.watch_id_valid = true;
        }
        if response.compact_revision > 0 {
            return Some(WatchTerminal::Compacted {
                revision: response.compact_revision,
            });
        }
        if response.canceled {
            return Some(WatchTerminal::Canceled {
                reason: response.cancel_reason.clone(),
            });
        }

        // A created response only acknowledges stream construction. Its
        // header is the server's current revision, not a delivered revision.
        // For event responses, advance only to the highest delivered event;
        // DELETE events may carry that revision only in prev_kv. An empty,
        // non-created response is the progress notification and may use its
        // header revision as the delivered point.
        if response.events.is_empty() {
            if !response.created {
                if let Some(header) = response.header.as_ref() {
                    self.last_revision = self.last_revision.max(header.revision);
                }
            }
        } else {
            let delivered = response
                .events
                .iter()
                .filter_map(|event| {
                    event
                        .kv
                        .as_ref()
                        .map(|kv| kv.mod_revision)
                        .filter(|revision| *revision > 0)
                        .or_else(|| event.prev_kv.as_ref().map(|kv| kv.mod_revision))
                })
                .max()
                .unwrap_or(0);
            self.last_revision = self.last_revision.max(delivered);
        }
        for event in &response.events {
            self.event_count = self.event_count.saturating_add(1);
            self.event_bytes = self.event_bytes.saturating_add(event_bytes(event));
        }
        None
    }

    fn observe_recovery_baseline(&mut self, response: &WatchResponse, configured: i64) {
        if configured != 0
            || !response.created
            || !response.events.is_empty()
            || self.last_revision > 0
            || self.recovery_revision > 0
        {
            return;
        }
        if let Some(header) = response.header.as_ref() {
            if header.revision > 0 {
                self.recovery_revision = header.revision;
            }
        }
    }
}

fn event_bytes(event: &mvccpb::Event) -> u64 {
    event
        .kv
        .as_ref()
        .map_or(0, |kv| (kv.key.len() + kv.value.len()) as u64)
        + event
            .prev_kv
            .as_ref()
            .map_or(0, |kv| (kv.key.len() + kv.value.len()) as u64)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchTerminal {
    Canceled { reason: String },
    Compacted { revision: i64 },
    NoLeader,
    Semantic,
    PhaseDeadline,
    ContextCanceled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchOutcome {
    pub terminal: WatchTerminal,
    pub state: WatchState,
}

#[derive(Debug, Error)]
pub enum WatchError {
    #[error("watch RPC: {0}")]
    Rpc(#[source] Status),
    #[error("watch measurement: {0}")]
    Internal(String),
}

/// Run one watch through the canonical recorder-backed lifecycle.
pub(crate) async fn run_with_recorder<C>(
    client: C,
    spec: WatchSpec,
    mut shutdown: shutdown_channel::Receiver<bool>,
    ready: oneshot::Sender<()>,
    recorder: MeasurementRecorder,
    index: usize,
    deadline: WatchDeadline,
) -> Result<WatchOutcome, WatchError>
where
    C: WatchConnector,
{
    let mut state = WatchState::default();
    let mut ready = Some(ready);
    let mut deadline_updates = deadline.subscribe();
    let mut deadline_updates_closed = false;
    loop {
        if *shutdown.borrow() {
            return context_canceled(state, &recorder, index);
        }
        // A peer can close a stream immediately after it is opened. Keep the
        // reconnect state machine, but never spin while a peer is unavailable.
        if state.reconnects > 0 {
            let delay = Duration::from_millis(50 * state.reconnects.min(20));
            let delay_deadline = Instant::now() + delay;
            loop {
                let current_deadline = *deadline_updates.borrow_and_update();
                let now = Instant::now();
                if current_deadline.is_some_and(|deadline| now >= deadline) {
                    return phase_deadline(state, &recorder, index);
                }
                let wake_at = current_deadline
                    .map_or(delay_deadline, |deadline| deadline.min(delay_deadline));
                tokio::select! {
                    _ = sleep_until(wake_at) => {
                        let now = Instant::now();
                        if current_deadline.is_some_and(|deadline| now >= deadline) {
                            return phase_deadline(state, &recorder, index);
                        }
                        if now >= delay_deadline {
                            break;
                        }
                    }
                    _ = shutdown.changed() => {
                        return context_canceled(state, &recorder, index);
                    }
                    result = deadline_updates.changed(), if !deadline_updates_closed => {
                        if result.is_err() {
                            deadline_updates_closed = true;
                        }
                    }
                }
            }
        }
        if *shutdown.borrow() {
            return context_canceled(state, &recorder, index);
        }
        let current_deadline = *deadline_updates.borrow_and_update();
        if current_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return phase_deadline(state, &recorder, index);
        }
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        state.watch_id_valid = false;
        let start = state.next_start_revision(spec.start_revision);
        tx.send(create_request(&spec, start))
            .await
            .map_err(|_| WatchError::Rpc(Status::cancelled("watch request channel closed")))?;
        let open_future = client.open_watch(ReceiverStream::new(rx));
        tokio::pin!(open_future);
        let open_result = loop {
            let current_deadline = *deadline_updates.borrow_and_update();
            if current_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return phase_deadline(state, &recorder, index);
            }
            tokio::select! {
                result = &mut open_future => break result,
                _ = shutdown.changed() => {
                    return context_canceled(state, &recorder, index);
                }
                result = deadline_updates.changed(), if !deadline_updates_closed => {
                    if result.is_err() {
                        deadline_updates_closed = true;
                    }
                }
                _ = async {
                    if let Some(deadline) = current_deadline {
                        sleep_until(deadline).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    return phase_deadline(state, &recorder, index);
                }
            }
        };
        let mut stream = match open_result {
            Ok(stream) => stream,
            Err(status) if is_no_leader_status(&status) => {
                return terminal_outcome(
                    state,
                    &recorder,
                    index,
                    WATCH_TERMINAL_NO_LEADER,
                    WatchTerminal::NoLeader,
                );
            }
            Err(status) if is_recoverable_disconnect(&status) => {
                record_reconnect(&mut state);
                continue;
            }
            Err(_status) => {
                return terminal_outcome(
                    state,
                    &recorder,
                    index,
                    WATCH_TERMINAL_SEMANTIC,
                    WatchTerminal::Semantic,
                );
            }
        };
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if state.watch_id_valid {
                        let _ = tx.send(cancel_request(state.watch_id)).await;
                    }
                    return context_canceled(state, &recorder, index);
                }
                response = stream.next() => match response {
                    Some(Ok(response)) => {
                        if let Some(header) = response.header.as_ref() {
                            recorder.set_revision_metric(header.revision);
                        }
                        recorder
                            .watch_response(index, &response)
                            .map_err(|error| WatchError::Internal(error.to_string()))?;
                        if let Some(terminal) = state.observe(&response) {
                            let code = if response.compact_revision > 0 {
                                WATCH_TERMINAL_COMPACTED
                            } else {
                                WATCH_TERMINAL_CANCELED
                            };
                            recorder
                                .watch_terminal(index, code)
                                .map_err(|error| WatchError::Internal(error.to_string()))?;
                            return Ok(WatchOutcome { terminal, state });
                        }
                        if response.created && response.watch_id == -1 {
                            recorder
                                .watch_terminal(index, WATCH_TERMINAL_SEMANTIC)
                                .map_err(|error| WatchError::Internal(error.to_string()))?;
                            return Ok(WatchOutcome {
                                terminal: WatchTerminal::Semantic,
                                state,
                            });
                        }
                        if response.created {
                            recorder
                                .watch_created(index)
                                .map_err(|error| WatchError::Internal(error.to_string()))?;
                        }
                        state.observe_recovery_baseline(&response, spec.start_revision);
                        if response.created {
                            if let Some(sender) = ready.take() {
                                let _ = sender.send(());
                            }
                        }
                    }
                    None => {
                        // A clean EOF is a transport transition for a
                        // long-lived watch, not a terminal scheduler result.
                        recorder
                            .watch_disconnect(index)
                            .map_err(|error| WatchError::Internal(error.to_string()))?;
                        record_reconnect(&mut state);
                        break;
                    }
                    Some(Err(status)) if is_no_leader_status(&status) => {
                        return terminal_outcome(
                            state,
                            &recorder,
                            index,
                            WATCH_TERMINAL_NO_LEADER,
                            WatchTerminal::NoLeader,
                        );
                    }
                    Some(Err(status)) if is_recoverable_disconnect(&status) => {
                        recorder
                            .watch_disconnect(index)
                            .map_err(|error| WatchError::Internal(error.to_string()))?;
                        record_reconnect(&mut state);
                        break;
                    }
                    Some(Err(_status)) => {
                        return terminal_outcome(
                            state,
                            &recorder,
                            index,
                            WATCH_TERMINAL_SEMANTIC,
                            WatchTerminal::Semantic,
                        );
                    }
                }
            }
        }
    }
}

fn terminal_outcome(
    state: WatchState,
    recorder: &MeasurementRecorder,
    index: usize,
    code: u64,
    terminal: WatchTerminal,
) -> Result<WatchOutcome, WatchError> {
    recorder
        .watch_terminal(index, code)
        .map_err(|error| WatchError::Internal(error.to_string()))?;
    Ok(WatchOutcome { terminal, state })
}

fn phase_deadline(
    state: WatchState,
    recorder: &MeasurementRecorder,
    index: usize,
) -> Result<WatchOutcome, WatchError> {
    terminal_outcome(
        state,
        recorder,
        index,
        WATCH_TERMINAL_PHASE_DEADLINE,
        WatchTerminal::PhaseDeadline,
    )
}

fn context_canceled(
    state: WatchState,
    recorder: &MeasurementRecorder,
    index: usize,
) -> Result<WatchOutcome, WatchError> {
    recorder
        .watch_context_cancel(index)
        .map_err(|error| WatchError::Internal(error.to_string()))?;
    Ok(WatchOutcome {
        terminal: WatchTerminal::ContextCanceled,
        state,
    })
}

fn is_recoverable_disconnect(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::DeadlineExceeded
    )
}

const ETCD_NO_LEADER_MESSAGE: &str = "etcdserver: no leader";

fn is_no_leader_status(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::Unknown)
        && status.message().trim() == ETCD_NO_LEADER_MESSAGE
}

fn record_reconnect(state: &mut WatchState) {
    state.reconnects = state.reconnects.saturating_add(1);
}
#[cfg(test)]
#[path = "../tests/unit/watch.rs"]
mod tests;
