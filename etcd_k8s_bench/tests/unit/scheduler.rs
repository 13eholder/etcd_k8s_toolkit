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

#[tokio::test(start_paused = true)]
async fn test_dispatch_rate() {
    let plan = small_plan();
    // Freeze both scheduler and recorder clocks so host scheduling cannot
    // consume the 1ms admission window while constructing the request.
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
