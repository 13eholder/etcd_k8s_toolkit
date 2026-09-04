//! Watch stream construction, lifecycle state, and resumable event consumption.
use std::pin::Pin;

use thiserror::Error;
use tokio::sync::watch as shutdown_channel;
use tokio::time::{Duration, sleep};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Status};

use crate::client::ClientBundle;
use crate::config::WatchSpec;
use crate::metrics::{self, WorkloadName};
use crate::pb::etcdserverpb::{
    WatchCancelRequest, WatchCreateRequest, WatchRequest, WatchResponse, watch_request,
};
use crate::pb::mvccpb;

type WatchResponseStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

#[tonic::async_trait]
trait WatchConnector: Clone + Send + Sync + 'static {
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
}

impl WatchState {
    pub fn next_start_revision(&self, configured: i64) -> i64 {
        if self.last_revision > 0 {
            self.last_revision.saturating_add(1)
        } else {
            configured
        }
    }

    pub fn observe(&mut self, response: &WatchResponse) -> Option<WatchTerminal> {
        if response.watch_id != 0 && response.watch_id != -1 {
            self.watch_id = response.watch_id;
        }
        if let Some(header) = response.header.as_ref() {
            metrics::set_revision(header.revision);
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
}

/// Run one long-lived watch. Only transport disconnects are resumed; semantic
/// cancellation and compaction are returned to the scheduler as terminal outcomes.
pub async fn run(
    client: ClientBundle,
    spec: WatchSpec,
    shutdown: shutdown_channel::Receiver<bool>,
) -> Result<WatchOutcome, WatchError> {
    run_with_connector(client, spec, shutdown).await
}

async fn run_with_connector<C>(
    client: C,
    spec: WatchSpec,
    mut shutdown: shutdown_channel::Receiver<bool>,
) -> Result<WatchOutcome, WatchError>
where
    C: WatchConnector,
{
    let watch_name =
        WorkloadName::new(spec.name.clone()).expect("workload watch label was validated");
    let mut state = WatchState::default();
    loop {
        // A peer can close a stream immediately after it is opened.  Keep the
        // reconnect state machine, but yield between attempts so a persistent
        // close cannot become a busy loop.
        if state.reconnects > 0 {
            let delay = Duration::from_millis(50 * state.reconnects.min(20));
            tokio::select! {
                _ = sleep(delay) => {}
                _ = shutdown.changed() => {
                    let outcome = WatchOutcome { terminal: WatchTerminal::ContextCanceled, state };
                    metrics::record_watch_cancel(&watch_name, "context");
                    return Ok(outcome);
                }
            }
        }
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let start = state.next_start_revision(spec.start_revision);
        tx.send(create_request(&spec, start))
            .await
            .map_err(|_| WatchError::Rpc(Status::cancelled("watch request channel closed")))?;
        let mut stream = match client.open_watch(ReceiverStream::new(rx)).await {
            Ok(stream) => stream,
            Err(status) if is_no_leader_status(&status) => return Err(WatchError::Rpc(status)),
            Err(status) if is_recoverable_disconnect(&status) => {
                record_reconnect(&mut state, &watch_name);
                continue;
            }
            Err(status) => return Err(WatchError::Rpc(status)),
        };
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if state.watch_id != 0 {
                        let _ = tx.send(cancel_request(state.watch_id)).await;
                    }
                    let outcome = WatchOutcome { terminal: WatchTerminal::ContextCanceled, state };
                    metrics::record_watch_cancel(&watch_name, "context");
                    return Ok(outcome);
                }
                response = stream.next() => match response {
                    Some(Ok(response)) => {
                        metrics::record_watch_response(&watch_name, &response);
                        if let Some(terminal) = state.observe(&response) {
                            metrics::record_watch_cancel(
                                &watch_name,
                                if response.compact_revision > 0 { "compacted" } else { "server" }
                            );
                            return Ok(WatchOutcome { terminal, state });
                        }
                    }
                    None => {
                        // A clean EOF is a transport transition for a
                        // long-lived watch, not a terminal scheduler result.
                        record_reconnect(&mut state, &watch_name);
                        break;
                    }
                    Some(Err(status)) if is_no_leader_status(&status) => {
                        return Err(WatchError::Rpc(status));
                    }
                    Some(Err(status)) if is_recoverable_disconnect(&status) => {
                        record_reconnect(&mut state, &watch_name);
                        break;
                    }
                    Some(Err(status)) => return Err(WatchError::Rpc(status)),
                }
            }
        }
    }
}

fn is_recoverable_disconnect(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::Unknown)
}

const ETCD_NO_LEADER_MESSAGE: &str = "etcdserver: no leader";

fn is_no_leader_status(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::Unknown)
        && status.message().trim() == ETCD_NO_LEADER_MESSAGE
}

fn record_reconnect(state: &mut WatchState, watch: &WorkloadName) {
    state.reconnects = state.reconnects.saturating_add(1);
    metrics::record_watch_reconnect(watch);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{Mutex, oneshot};

    use super::*;
    use crate::pb::etcdserverpb::ResponseHeader;

    #[derive(Clone)]
    struct FakeWatchClient {
        starts: Arc<Mutex<Vec<i64>>>,
        calls: Arc<Mutex<usize>>,
        second_open: Arc<Mutex<Option<oneshot::Sender<()>>>>,
        first_events: bool,
    }

    #[tonic::async_trait]
    impl WatchConnector for FakeWatchClient {
        async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
        where
            S: Stream<Item = WatchRequest> + Send + 'static,
        {
            tokio::pin!(requests);
            let request = requests.next().await.expect("create request");
            let Some(watch_request::RequestUnion::CreateRequest(create)) = request.request_union
            else {
                panic!("first request must create watch");
            };
            self.starts.lock().await.push(create.start_revision);
            assert!(
                tokio::time::timeout(Duration::from_millis(1), requests.next())
                    .await
                    .is_err(),
                "watch open must not send an unconditional progress request"
            );
            let call = {
                let mut calls = self.calls.lock().await;
                *calls += 1;
                *calls
            };
            if call == 1 {
                let response = if self.first_events {
                    WatchResponse {
                        header: Some(ResponseHeader {
                            revision: 12,
                            ..Default::default()
                        }),
                        watch_id: 1,
                        events: vec![mvccpb::Event {
                            kv: Some(mvccpb::KeyValue {
                                mod_revision: 8,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                } else {
                    WatchResponse {
                        header: Some(ResponseHeader {
                            revision: 12,
                            ..Default::default()
                        }),
                        watch_id: 1,
                        created: true,
                        ..Default::default()
                    }
                };
                Ok(Box::pin(tokio_stream::once(Ok(response))))
            } else {
                if let Some(notify) = self.second_open.lock().await.take() {
                    let _ = notify.send(());
                }
                Ok(Box::pin(
                    tokio_stream::once(Ok(WatchResponse {
                        header: Some(ResponseHeader {
                            revision: 13,
                            ..Default::default()
                        }),
                        watch_id: 2,
                        ..Default::default()
                    }))
                    .chain(tokio_stream::pending::<Result<WatchResponse, Status>>()),
                ))
            }
        }
    }
    fn spec() -> WatchSpec {
        WatchSpec {
            name: "pods".into(),
            resource_prefix: "/registry/pods".into(),
            start_revision: 7,
            prev_kv: true,
            progress_notify: true,
        }
    }
    #[derive(Clone)]
    struct NoLeaderOpenClient {
        calls: Arc<Mutex<usize>>,
    }

    #[tonic::async_trait]
    impl WatchConnector for NoLeaderOpenClient {
        async fn open_watch<S>(&self, _requests: S) -> Result<WatchResponseStream, Status>
        where
            S: Stream<Item = WatchRequest> + Send + 'static,
        {
            *self.calls.lock().await += 1;
            Err(Status::new(Code::Unavailable, ETCD_NO_LEADER_MESSAGE))
        }
    }

    #[derive(Clone)]
    struct NoLeaderStreamClient {
        calls: Arc<Mutex<usize>>,
    }

    #[tonic::async_trait]
    impl WatchConnector for NoLeaderStreamClient {
        async fn open_watch<S>(&self, _requests: S) -> Result<WatchResponseStream, Status>
        where
            S: Stream<Item = WatchRequest> + Send + 'static,
        {
            *self.calls.lock().await += 1;
            Ok(Box::pin(tokio_stream::once(Err(Status::new(
                Code::Unavailable,
                ETCD_NO_LEADER_MESSAGE,
            )))))
        }
    }

    #[tokio::test]
    async fn no_leader_on_open_is_forwarded_without_reconnect() {
        let calls = Arc::new(Mutex::new(0));
        let client = NoLeaderOpenClient {
            calls: Arc::clone(&calls),
        };
        let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
        let result = run_with_connector(client, spec(), shutdown_rx).await;
        let Err(WatchError::Rpc(status)) = result else {
            panic!("expected no-leader RPC");
        };
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), ETCD_NO_LEADER_MESSAGE);
        assert_eq!(*calls.lock().await, 1);
    }

    #[tokio::test]
    async fn no_leader_on_stream_is_forwarded_without_reconnect() {
        let calls = Arc::new(Mutex::new(0));
        let client = NoLeaderStreamClient {
            calls: Arc::clone(&calls),
        };
        let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
        let result = run_with_connector(client, spec(), shutdown_rx).await;
        let Err(WatchError::Rpc(status)) = result else {
            panic!("expected no-leader RPC");
        };
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), ETCD_NO_LEADER_MESSAGE);
        assert_eq!(*calls.lock().await, 1);
    }

    #[tokio::test]
    async fn run_path_reconnects_after_created_eof_without_skipping_history() {
        let _serial = metrics::lock_watch_reconnect_tests();
        metrics::reset_watch_reconnects();

        let starts = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(Mutex::new(0usize));
        let (second_open_tx, second_open_rx) = oneshot::channel();
        let client = FakeWatchClient {
            starts: Arc::clone(&starts),
            calls,
            second_open: Arc::new(Mutex::new(Some(second_open_tx))),
            first_events: false,
        };
        let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
        let task = tokio::spawn(run_with_connector(client, spec(), shutdown_rx));

        second_open_rx.await.expect("second watch stream opened");
        assert_eq!(&*starts.lock().await, &[7, 7]);
        shutdown.send(true).expect("watch shutdown receiver exists");
        let outcome = task.await.expect("watch task join").expect("watch outcome");
        assert_eq!(outcome.state.reconnects, 1);
        assert_eq!(outcome.state.last_revision, 13);
        metrics::reset_watch_reconnects();
    }

    #[tokio::test]
    async fn run_path_resumes_after_delivered_event_revision() {
        let _serial = metrics::lock_watch_reconnect_tests();
        metrics::reset_watch_reconnects();

        let starts = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(Mutex::new(0usize));
        let (second_open_tx, second_open_rx) = oneshot::channel();
        let client = FakeWatchClient {
            starts: Arc::clone(&starts),
            calls,
            second_open: Arc::new(Mutex::new(Some(second_open_tx))),
            first_events: true,
        };
        let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
        let task = tokio::spawn(run_with_connector(client, spec(), shutdown_rx));

        second_open_rx.await.expect("second watch stream opened");
        assert_eq!(&*starts.lock().await, &[7, 9]);
        shutdown.send(true).expect("watch shutdown receiver exists");
        let outcome = task.await.expect("watch task join").expect("watch outcome");
        assert_eq!(outcome.state.reconnects, 1);
        assert_eq!(outcome.state.last_revision, 13);
        metrics::reset_watch_reconnects();
    }

    #[test]
    fn prefix_end_and_require_leader_contract_are_fixed() {
        assert_eq!(crate::client::REQUIRE_LEADER_METADATA, "hasleader");
        assert_eq!(prefix_range_end(b"/pods"), b"/podt".to_vec());
        assert_eq!(prefix_range_end(&[0xff]), vec![0]);
    }

    #[test]
    fn messages_and_metadata_are_wire_complete() {
        let create = create_request(&spec(), 9);
        let watch_request::RequestUnion::CreateRequest(request) = create.request_union.unwrap()
        else {
            panic!()
        };
        assert_eq!(request.start_revision, 9);
        assert_eq!(request.range_end, b"/registry/podt".to_vec());
        assert!(request.prev_kv && request.progress_notify);
        assert!(matches!(
            cancel_request(3).request_union,
            Some(watch_request::RequestUnion::CancelRequest(_))
        ));
    }

    #[test]
    fn state_distinguishes_created_events_progress_and_canceled() {
        let mut state = WatchState::default();
        state.observe(&WatchResponse {
            header: Some(ResponseHeader {
                revision: 12,
                ..Default::default()
            }),
            watch_id: 3,
            created: true,
            ..Default::default()
        });
        assert_eq!(state.last_revision, 0);
        assert_eq!(state.next_start_revision(7), 7);

        state.observe(&WatchResponse {
            header: Some(ResponseHeader {
                revision: 99,
                ..Default::default()
            }),
            events: vec![mvccpb::Event {
                kv: Some(mvccpb::KeyValue {
                    mod_revision: 8,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(state.last_revision, 8);
        assert_eq!(state.next_start_revision(7), 9);

        state.observe(&WatchResponse {
            header: Some(ResponseHeader {
                revision: 12,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(state.last_revision, 12);

        let terminal = state.observe(&WatchResponse {
            header: Some(ResponseHeader {
                revision: 100,
                ..Default::default()
            }),
            canceled: true,
            cancel_reason: "no leader".into(),
            ..Default::default()
        });

        assert_eq!(
            terminal,
            Some(WatchTerminal::Canceled {
                reason: "no leader".into()
            })
        );
        assert_eq!(state.last_revision, 12);

        let mut delete_state = WatchState::default();
        delete_state.observe(&WatchResponse {
            events: vec![mvccpb::Event {
                prev_kv: Some(mvccpb::KeyValue {
                    mod_revision: 15,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(delete_state.last_revision, 15);
    }

    #[test]
    fn two_independent_watch_reconnects_share_process_total() {
        let _serial = metrics::lock_watch_reconnect_tests();
        metrics::reset_watch_reconnects();
        let mut first = WatchState::default();
        let mut second = WatchState::default();
        let watch = WorkloadName::new("pods").unwrap();
        record_reconnect(&mut first, &watch);
        record_reconnect(&mut second, &watch);
        assert_eq!(metrics::watch_reconnects_total(), 2);
        assert_eq!(first.reconnects, 1);
        assert_eq!(second.reconnects, 1);
        metrics::reset_watch_reconnects();
    }
    #[test]
    fn thiserror_preserves_watch_error_source() {
        let error = WatchError::Rpc(Status::unavailable("temporary"));
        assert!(error.to_string().starts_with("watch RPC: "));
        assert!(std::error::Error::source(&error).is_some());
    }
}
