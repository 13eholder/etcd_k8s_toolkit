use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use tokio::sync::{Mutex, Notify, mpsc, oneshot};

use super::*;
use crate::measurement::MeasurementRecorder;
use crate::pb::etcdserverpb::ResponseHeader;
use crate::plan::CompiledPlan;

#[derive(Clone)]
struct FakeWatchClient {
    starts: Arc<Mutex<Vec<i64>>>,
    calls: Arc<Mutex<usize>>,
    second_open: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    first_events: bool,
}
struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
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
        {
            let next_request = requests.next();
            tokio::pin!(next_request);
            let waker = Waker::from(Arc::new(NoopWake));
            let mut context = Context::from_waker(&waker);
            assert!(
                matches!(next_request.as_mut().poll(&mut context), Poll::Pending),
                "watch open must not send an unconditional progress request"
            );
        }
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
fn recorder_plan() -> CompiledPlan {
    let mut config = crate::config::SyntheticConfig::from_json(
        crate::plan::DEFAULT_SYNTHETIC_CONFIG,
        64 * 1024 * 1024,
    )
    .unwrap();
    config.topology.node_count = 4;
    config.topology.pod_count = 80;
    config.topology.namespace_count = 1;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 4;
    config.load.update_rps = 10;
    config.load.update_weights.pods = 1;
    config.load.update_weights.deployments = 0;
    config.load.update_weights.replicasets = 0;
    config.load.update_weights.daemonsets = 0;
    config.load.read_ratio.numerator = 0;
    config.load.duration_ms = 100;
    config.report.bucket_ms = 10;
    CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap()
}

fn test_recorder() -> MeasurementRecorder {
    let plan = recorder_plan();
    let recorder = MeasurementRecorder::new(&plan).unwrap();
    recorder.mark_setup_complete().unwrap();
    recorder.start_measurement().unwrap();
    recorder
}

fn finish_test_recorder(recorder: &MeasurementRecorder) {
    recorder.stop_measurement().unwrap();
    recorder.finish().unwrap();
}

#[derive(Clone)]
struct DelayedCreatedClient {
    responses: Arc<Mutex<Option<mpsc::Receiver<Result<WatchResponse, Status>>>>>,
    opened: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    processed: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[tonic::async_trait]
impl WatchConnector for DelayedCreatedClient {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        tokio::pin!(requests);
        let _ = requests.next().await.expect("create request");
        if let Some(notify) = self.opened.lock().await.take() {
            let _ = notify.send(());
        }
        let response_rx = self
            .responses
            .lock()
            .await
            .take()
            .expect("response stream requested once");
        Ok(Box::pin(AcknowledgeFirstResponseStream {
            inner: ReceiverStream::new(response_rx),
            first_delivered: false,
            processed: self.processed.lock().await.take(),
        }))
    }
}

struct AcknowledgeFirstResponseStream {
    inner: ReceiverStream<Result<WatchResponse, Status>>,
    first_delivered: bool,
    processed: Option<oneshot::Sender<()>>,
}

impl Stream for AcknowledgeFirstResponseStream {
    type Item = Result<WatchResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.first_delivered = true;
                Poll::Ready(Some(item))
            }
            Poll::Pending if this.first_delivered => {
                if let Some(processed) = this.processed.take() {
                    let _ = processed.send(());
                }
                Poll::Pending
            }
            Poll::Ready(None) if this.first_delivered => {
                if let Some(processed) = this.processed.take() {
                    let _ = processed.send(());
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

#[derive(Clone)]
struct BlockingOpenClient {
    opened: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

#[tonic::async_trait]
impl WatchConnector for BlockingOpenClient {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        tokio::pin!(requests);
        let _ = requests.next().await.expect("create request");
        if let Some(notify) = self.opened.lock().await.take() {
            let _ = notify.send(());
        }
        std::future::pending::<Result<WatchResponseStream, Status>>().await
    }
}

#[derive(Clone)]
struct TerminalClient {
    response: Arc<Mutex<Option<WatchResponse>>>,
}

#[tonic::async_trait]
impl WatchConnector for TerminalClient {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        tokio::pin!(requests);
        let _ = requests.next().await.expect("create request");

        let response = self
            .response
            .lock()
            .await
            .take()
            .expect("terminal response requested once");
        Ok(Box::pin(tokio_stream::once(Ok(response))))
    }
}
struct NotifyAfterCreatedStream {
    response: Option<Result<WatchResponse, Status>>,
    processed: Option<oneshot::Sender<()>>,
}

impl Stream for NotifyAfterCreatedStream {
    type Item = Result<WatchResponse, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(response) = this.response.take() {
            std::task::Poll::Ready(Some(response))
        } else {
            if let Some(processed) = this.processed.take() {
                let _ = processed.send(());
            }
            std::task::Poll::Pending
        }
    }
}

#[derive(Clone)]
struct ReconnectIdClient {
    calls: Arc<Mutex<usize>>,
    second_created: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    cancel_id: Arc<Mutex<Option<oneshot::Sender<i64>>>>,
}

#[tonic::async_trait]
impl WatchConnector for ReconnectIdClient {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        let mut requests = Box::pin(requests);
        let _ = requests.next().await.expect("create request");
        let call = {
            let mut calls = self.calls.lock().await;
            *calls += 1;
            *calls
        };
        if call == 1 {
            return Ok(Box::pin(tokio_stream::once(Ok(WatchResponse {
                watch_id: 7,
                created: true,
                ..Default::default()
            }))));
        }

        let cancel_id = self.cancel_id.lock().await.take();
        tokio::spawn(async move {
            while let Some(request) = requests.next().await {
                if let Some(watch_request::RequestUnion::CancelRequest(cancel)) =
                    request.request_union
                {
                    if let Some(sender) = cancel_id {
                        let _ = sender.send(cancel.watch_id);
                    }
                    break;
                }
            }
        });
        Ok(Box::pin(NotifyAfterCreatedStream {
            response: Some(Ok(WatchResponse {
                watch_id: 0,
                created: true,
                ..Default::default()
            })),
            processed: self.second_created.lock().await.take(),
        }))
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

#[derive(Clone, Copy, Debug)]
enum StatusPath {
    Open,
    Stream,
}

#[derive(Clone)]
struct StatusClient {
    path: StatusPath,
    code: Code,
    observed: Arc<Notify>,
}

#[tonic::async_trait]
impl WatchConnector for StatusClient {
    async fn open_watch<S>(&self, requests: S) -> Result<WatchResponseStream, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        tokio::pin!(requests);
        let _ = requests.next().await.expect("create request");
        self.observed.notify_one();
        let status = Status::new(self.code, "watch status");
        match self.path {
            StatusPath::Open => Err(status),
            StatusPath::Stream => Ok(Box::pin(tokio_stream::once(Err(status)))),
        }
    }
}

#[tokio::test]
async fn test_watch_status_entrypoints() {
    let statuses = [
        (Code::Unavailable, true),
        (Code::Unknown, true),
        (Code::DeadlineExceeded, true),
        (Code::Cancelled, false),
        (Code::InvalidArgument, false),
        (Code::NotFound, false),
        (Code::AlreadyExists, false),
        (Code::PermissionDenied, false),
        (Code::ResourceExhausted, false),
        (Code::FailedPrecondition, false),
        (Code::Aborted, false),
        (Code::OutOfRange, false),
        (Code::Unimplemented, false),
        (Code::Internal, false),
        (Code::DataLoss, false),
        (Code::Unauthenticated, false),
    ];

    for path in [StatusPath::Open, StatusPath::Stream] {
        for (code, recoverable) in statuses {
            let observed = Arc::new(Notify::new());
            let client = StatusClient {
                path,
                code,
                observed: Arc::clone(&observed),
            };
            let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
            let (ready_tx, ready_rx) = oneshot::channel();
            let recorder = test_recorder();
            let task = tokio::spawn(run_with_recorder(
                client,
                spec(),
                shutdown_rx,
                ready_tx,
                recorder.clone(),
                0,
                WatchDeadline::new(),
            ));

            observed.notified().await;
            if recoverable {
                shutdown
                    .send(true)
                    .expect("recoverable watch shutdown receiver exists");
            }
            let outcome = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("watch status classification completes")
                .expect("watch status task joins")
                .expect("watch status outcome");
            if recoverable {
                assert_eq!(outcome.terminal, WatchTerminal::ContextCanceled);
                assert_eq!(outcome.state.reconnects, 1);
            } else {
                assert_eq!(outcome.terminal, WatchTerminal::Semantic);
                assert_eq!(outcome.state.reconnects, 0);
            }
            assert!(ready_rx.await.is_err());
            finish_test_recorder(&recorder);
        }
    }
}

async fn assert_no_leader<C>(client: C, calls: Arc<Mutex<usize>>)
where
    C: WatchConnector,
{
    let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, _ready_rx) = oneshot::channel();
    let recorder = test_recorder();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        run_with_recorder(
            client,
            spec(),
            shutdown_rx,
            ready_tx,
            recorder.clone(),
            0,
            WatchDeadline::new(),
        ),
    )
    .await
    .expect("no-leader watch must not reconnect indefinitely")
    .expect("no-leader terminal outcome");
    assert_eq!(result.terminal, WatchTerminal::NoLeader);
    finish_test_recorder(&recorder);
    assert_eq!(*calls.lock().await, 1);
}

#[tokio::test]
async fn test_watch_no_leader_error() {
    let open_calls = Arc::new(Mutex::new(0));
    assert_no_leader(
        NoLeaderOpenClient {
            calls: Arc::clone(&open_calls),
        },
        open_calls,
    )
    .await;

    let stream_calls = Arc::new(Mutex::new(0));
    assert_no_leader(
        NoLeaderStreamClient {
            calls: Arc::clone(&stream_calls),
        },
        stream_calls,
    )
    .await;
}

#[tokio::test]
async fn test_watch_compacted_terminal() {
    let client = TerminalClient {
        response: Arc::new(Mutex::new(Some(WatchResponse {
            compact_revision: 11,
            watch_id: 1,
            created: true,
            ..Default::default()
        }))),
    };
    let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let recorder = test_recorder();
    let result = run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    )
    .await
    .expect("watch outcome");
    assert_eq!(result.terminal, WatchTerminal::Compacted { revision: 11 });
    assert!(ready_rx.await.is_err());
    finish_test_recorder(&recorder);
}

#[tokio::test]
async fn test_watch_server_cancel() {
    let client = TerminalClient {
        response: Arc::new(Mutex::new(Some(WatchResponse {
            canceled: true,
            cancel_reason: "server canceled".into(),
            watch_id: 1,
            created: true,
            ..Default::default()
        }))),
    };
    let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let recorder = test_recorder();
    let result = run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    )
    .await
    .expect("watch outcome");
    assert_eq!(
        result.terminal,
        WatchTerminal::Canceled {
            reason: "server canceled".into()
        }
    );
    assert!(ready_rx.await.is_err());
    finish_test_recorder(&recorder);
}

#[tokio::test]
async fn test_instrumented_watch_semantic_terminal_marks_gap() {
    let plan = recorder_plan();
    let recorder = MeasurementRecorder::new(&plan).unwrap();
    recorder.mark_setup_complete().unwrap();
    recorder.start_measurement().unwrap();
    let client = TerminalClient {
        response: Arc::new(Mutex::new(Some(WatchResponse {
            created: true,
            watch_id: -1,
            ..Default::default()
        }))),
    };
    let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let outcome = run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    )
    .await
    .unwrap();
    assert_eq!(outcome.terminal, WatchTerminal::Semantic);
    assert!(ready_rx.await.is_err());
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();
    let stream = report
        .watch
        .roots
        .iter()
        .flat_map(|root| root.streams.iter())
        .find(|stream| stream.stream == "pods")
        .expect("pods watch report");
    assert!(!stream.ready);
    assert_eq!(stream.terminal.as_deref(), Some("semantic"));
    assert!(!stream.observation_complete);
}

#[tokio::test]
async fn test_watch_resume() {
    for (start_revision, first_events, expected_starts) in [
        (7, false, vec![7, 7]),
        (0, false, vec![0, 12]),
        (7, true, vec![7, 9]),
    ] {
        let starts = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(Mutex::new(0usize));
        let (second_open_tx, second_open_rx) = oneshot::channel();
        let client = FakeWatchClient {
            starts: Arc::clone(&starts),
            calls,
            second_open: Arc::new(Mutex::new(Some(second_open_tx))),
            first_events,
        };
        let recorder = test_recorder();
        let mut case_spec = spec();
        case_spec.start_revision = start_revision;
        let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
        let (ready_tx, _ready_rx) = oneshot::channel();
        let task = tokio::spawn(run_with_recorder(
            client,
            case_spec,
            shutdown_rx,
            ready_tx,
            recorder.clone(),
            0,
            WatchDeadline::new(),
        ));

        tokio::time::timeout(Duration::from_secs(1), second_open_rx)
            .await
            .expect("second watch stream opens")
            .expect("second watch observer exists");
        assert_eq!(&*starts.lock().await, &expected_starts);
        shutdown.send(true).expect("watch shutdown receiver exists");
        let outcome = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("watch task shuts down")
            .expect("watch task join")
            .expect("watch outcome");
        assert_eq!(outcome.state.reconnects, 1);
        assert_eq!(outcome.state.last_revision, 13);
        finish_test_recorder(&recorder);
    }
}
#[tokio::test]
async fn test_instrumented_watch_reconnect_updates_coverage() {
    let plan = recorder_plan();
    let recorder = MeasurementRecorder::new(&plan).unwrap();
    recorder.mark_setup_complete().unwrap();
    recorder.start_measurement().unwrap();

    let starts = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(0usize));
    let (second_open_tx, second_open_rx) = oneshot::channel();
    let client = FakeWatchClient {
        starts,
        calls,
        second_open: Arc::new(Mutex::new(Some(second_open_tx))),
        first_events: false,
    };
    let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    ));

    ready_rx.await.expect("instrumented watch created");
    second_open_rx.await.expect("instrumented watch reconnects");
    shutdown.send(true).expect("watch shutdown receiver exists");
    let outcome = task.await.unwrap().unwrap();
    assert_eq!(outcome.state.reconnects, 1);
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();
    let stream = report
        .watch
        .roots
        .iter()
        .flat_map(|root| root.streams.iter())
        .find(|stream| stream.stream == "pods")
        .expect("pods watch report");
    assert!(stream.ready);
    assert_eq!(stream.disconnects, 1);
    assert!(!stream.observation_complete);
    assert!(!report.watch.observation_complete);
}

#[tokio::test]
async fn test_watch_zero_id_cancel() {
    let (second_created_tx, second_created_rx) = oneshot::channel();
    let (cancel_id_tx, cancel_id_rx) = oneshot::channel();
    let client = ReconnectIdClient {
        calls: Arc::new(Mutex::new(0)),
        second_created: Arc::new(Mutex::new(Some(second_created_tx))),
        cancel_id: Arc::new(Mutex::new(Some(cancel_id_tx))),
    };
    let recorder = test_recorder();
    let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    ));

    ready_rx.await.expect("first watch created");
    second_created_rx
        .await
        .expect("reconnected zero-ID watch created");
    shutdown.send(true).expect("watch shutdown receiver exists");
    let cancel_id = tokio::time::timeout(Duration::from_secs(1), cancel_id_rx)
        .await
        .expect("zero-ID cancel sent")
        .expect("cancel request observer exists");
    assert_eq!(cancel_id, 0);
    let outcome = task.await.expect("watch task join").expect("watch outcome");
    assert_eq!(outcome.terminal, WatchTerminal::ContextCanceled);
    finish_test_recorder(&recorder);
}
#[tokio::test]
async fn test_watch_created_ready() {
    let (response_tx, response_rx) = mpsc::channel(1);
    let (opened_tx, opened_rx) = oneshot::channel();
    let client = DelayedCreatedClient {
        responses: Arc::new(Mutex::new(Some(response_rx))),
        opened: Arc::new(Mutex::new(Some(opened_tx))),
        processed: Arc::new(Mutex::new(None)),
    };
    let recorder = test_recorder();
    let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    ));

    opened_rx.await.expect("watch RPC established");
    assert!(ready_rx.try_recv().is_err());
    response_tx
        .send(Ok(WatchResponse {
            header: Some(ResponseHeader {
                revision: 12,
                ..Default::default()
            }),
            watch_id: 1,
            created: true,
            ..Default::default()
        }))
        .await
        .expect("created response receiver exists");
    ready_rx.await.expect("created response releases readiness");

    shutdown.send(true).expect("watch shutdown receiver exists");
    let outcome = task.await.expect("watch task join").expect("watch outcome");
    assert_eq!(outcome.terminal, WatchTerminal::ContextCanceled);
    finish_test_recorder(&recorder);
}

#[tokio::test]
async fn test_watch_created_ordering() {
    for (label, first, expected_events) in [
        (
            "progress",
            WatchResponse {
                header: Some(ResponseHeader {
                    revision: 9,
                    ..Default::default()
                }),
                watch_id: 1,
                ..Default::default()
            },
            0,
        ),
        (
            "event",
            WatchResponse {
                header: Some(ResponseHeader {
                    revision: 9,
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
            },
            1,
        ),
    ] {
        let (response_tx, response_rx) = mpsc::channel(2);
        let (opened_tx, opened_rx) = oneshot::channel();
        let (processed_tx, processed_rx) = oneshot::channel();
        let client = DelayedCreatedClient {
            responses: Arc::new(Mutex::new(Some(response_rx))),
            opened: Arc::new(Mutex::new(Some(opened_tx))),
            processed: Arc::new(Mutex::new(Some(processed_tx))),
        };
        let recorder = test_recorder();
        let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
        let (ready_tx, mut ready_rx) = oneshot::channel();
        let task = tokio::spawn(run_with_recorder(
            client,
            spec(),
            shutdown_rx,
            ready_tx,
            recorder.clone(),
            0,
            WatchDeadline::new(),
        ));

        opened_rx.await.expect("watch RPC established");
        assert!(
            ready_rx.try_recv().is_err(),
            "{label} opened watch became ready"
        );
        response_tx
            .send(Ok(first))
            .await
            .expect("first response receiver exists");
        processed_rx
            .await
            .expect("consumer processed first response");
        assert!(
            ready_rx.try_recv().is_err(),
            "{label} became ready before Created response"
        );
        response_tx
            .send(Ok(WatchResponse {
                header: Some(ResponseHeader {
                    revision: 10,
                    ..Default::default()
                }),
                watch_id: 1,
                created: true,
                ..Default::default()
            }))
            .await
            .expect("created response receiver exists");
        ready_rx.await.expect("created response releases readiness");

        shutdown.send(true).expect("watch shutdown receiver exists");
        let outcome = task.await.expect("watch task join").expect("watch outcome");
        assert_eq!(outcome.terminal, WatchTerminal::ContextCanceled);
        assert_eq!(outcome.state.event_count, expected_events);
        finish_test_recorder(&recorder);
    }
}

#[tokio::test]
async fn test_watch_open_cancel() {
    let (opened_tx, opened_rx) = oneshot::channel();
    let client = BlockingOpenClient {
        opened: Arc::new(Mutex::new(Some(opened_tx))),
    };
    let recorder = test_recorder();
    let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        WatchDeadline::new(),
    ));

    opened_rx.await.expect("watch open started");
    shutdown.send(true).expect("watch shutdown receiver exists");
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("open cancellation completes")
        .expect("watch task join");
    let outcome = result.expect("watch outcome");
    assert_eq!(outcome.terminal, WatchTerminal::ContextCanceled);
    assert!(ready_rx.await.is_err());
    finish_test_recorder(&recorder);
}

#[tokio::test(start_paused = true)]
async fn test_watch_deadline_update_cancels_open() {
    let (opened_tx, opened_rx) = oneshot::channel();
    let client = BlockingOpenClient {
        opened: Arc::new(Mutex::new(Some(opened_tx))),
    };
    let recorder = test_recorder();
    let deadline = WatchDeadline::new();
    let initial_deadline = Instant::now() + Duration::from_secs(1);
    deadline.set(initial_deadline);
    let (_shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        deadline.clone(),
    ));

    opened_rx.await.expect("watch open started");
    deadline.set(Instant::now() + Duration::from_millis(10));
    tokio::time::advance(Duration::from_millis(11)).await;
    tokio::task::yield_now().await;
    let finished_early = task.is_finished();
    if !finished_early {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    let outcome = task
        .await
        .expect("watch task join")
        .expect("watch deadline outcome");
    assert!(
        finished_early,
        "shortening an open watch deadline must wake the open wait"
    );
    assert_eq!(outcome.terminal, WatchTerminal::PhaseDeadline);
    assert!(ready_rx.await.is_err());
    finish_test_recorder(&recorder);
}

#[tokio::test(start_paused = true)]
async fn test_watch_deadline_update_cancels_backoff_without_reconnect() {
    let starts = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(Mutex::new(0usize));
    let (second_open_tx, _second_open_rx) = oneshot::channel();
    let client = FakeWatchClient {
        starts,
        calls: calls.clone(),
        second_open: Arc::new(Mutex::new(Some(second_open_tx))),
        first_events: false,
    };
    let recorder = test_recorder();
    let deadline = WatchDeadline::new();
    deadline.set(Instant::now() + Duration::from_secs(1));
    let (shutdown, shutdown_rx) = shutdown_channel::channel(false);
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(run_with_recorder(
        client,
        spec(),
        shutdown_rx,
        ready_tx,
        recorder.clone(),
        0,
        deadline.clone(),
    ));

    ready_rx.await.expect("initial watch created");
    assert_eq!(*calls.lock().await, 1);
    deadline.set(Instant::now() + Duration::from_millis(10));
    tokio::time::advance(Duration::from_millis(11)).await;
    let finished_early = task.is_finished();
    if !finished_early {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        if !task.is_finished() {
            shutdown.send(true).expect("watch shutdown receiver exists");
        }
    }
    let outcome = task
        .await
        .expect("watch task join")
        .expect("watch deadline outcome");
    assert!(
        finished_early,
        "shortening a reconnect deadline must cancel backoff"
    );
    assert_eq!(*calls.lock().await, 1);
    assert_eq!(outcome.terminal, WatchTerminal::PhaseDeadline);
    finish_test_recorder(&recorder);
}

#[test]
fn test_watch_prefix_end() {
    assert_eq!(prefix_range_end(b"/pods"), b"/podt".to_vec());
    assert_eq!(prefix_range_end(&[0xff]), vec![0]);
    assert_eq!(prefix_range_end(&[]), vec![0]);
}

#[test]
fn test_watch_requests_match_spec() {
    let create = create_request(&spec(), 9);
    let watch_request::RequestUnion::CreateRequest(request) = create.request_union.unwrap()
    else {
        panic!("expected WatchCreateRequest")
    };
    assert_eq!(request.key, b"/registry/pods");
    assert_eq!(request.range_end, b"/registry/podt".to_vec());
    assert_eq!(request.start_revision, 9);
    assert!(request.prev_kv);
    assert!(request.progress_notify);

    let cancel = cancel_request(3);
    let watch_request::RequestUnion::CancelRequest(request) = cancel.request_union.unwrap()
    else {
        panic!("expected WatchCancelRequest")
    };
    assert_eq!(request.watch_id, 3);
}

#[test]
fn test_watch_revision_advance() {
    let created = WatchResponse {
        header: Some(ResponseHeader {
            revision: 12,
            ..Default::default()
        }),
        watch_id: 3,
        created: true,
        ..Default::default()
    };
    let mut state = WatchState::default();
    state.observe(&created);
    assert_eq!(state.last_revision, 0);
    assert_eq!(state.next_start_revision(7), 7);
    state.observe_recovery_baseline(&created, 0);
    assert_eq!(state.last_revision, 0);
    assert_eq!(state.next_start_revision(0), 12);

    let mut historical_state = WatchState::default();
    historical_state.observe(&created);
    historical_state.observe_recovery_baseline(&created, 7);
    assert_eq!(historical_state.last_revision, 0);
    assert_eq!(historical_state.next_start_revision(7), 7);

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

