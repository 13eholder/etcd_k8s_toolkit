use std::fs::OpenOptions;
use std::mem;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use crate::pb::etcdserverpb::kv_client::KvClient;
use crate::pb::etcdserverpb::kv_server::Kv;
use crate::pb::etcdserverpb::lease_client::LeaseClient;
use crate::pb::etcdserverpb::lease_server::Lease;
use crate::pb::etcdserverpb::maintenance_client::MaintenanceClient;
use crate::pb::etcdserverpb::maintenance_server::Maintenance;
use crate::pb::etcdserverpb::watch_client::WatchClient;
use crate::pb::etcdserverpb::watch_server::Watch;
use crate::pb::etcdserverpb::{
    AlarmRequest, AlarmResponse, CompactionRequest, CompactionResponse, DefragmentRequest,
    DefragmentResponse, DeleteRangeRequest, DeleteRangeResponse, HashKvRequest, HashKvResponse,
    HashRequest, HashResponse, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
    LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest,
    LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, MoveLeaderRequest,
    MoveLeaderResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, SnapshotRequest,
    SnapshotResponse, StatusRequest, StatusResponse, TxnRequest, TxnResponse, WatchRequest,
    WatchResponse,
};
static NEXT_WATCH_STREAM_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct ProxyService {
    kv_client: KvClient<Channel>,
    watch_client: WatchClient<Channel>,
    lease_client: LeaseClient<Channel>,
    maintenance_client: MaintenanceClient<Channel>,
    logger: Arc<Logger>,
}

impl ProxyService {
    pub fn new(
        kv_client: KvClient<Channel>,
        watch_client: WatchClient<Channel>,
        lease_client: LeaseClient<Channel>,
        maintenance_client: MaintenanceClient<Channel>,
        logger: Arc<Logger>,
    ) -> Self {
        ProxyService {
            kv_client,
            watch_client,
            lease_client,
            maintenance_client,
            logger,
        }
    }
}
#[tonic::async_trait]
impl Kv for ProxyService {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        self.log_request("/etcdserverpb.KV/Range", request.get_ref())
            .await;
        let mut kv_client = self.kv_client.clone();
        kv_client.range(request).await
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        self.log_request("/etcdserverpb.KV/Put", request.get_ref())
            .await;
        let mut kv_client = self.kv_client.clone();
        kv_client.put(request).await
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        self.log_request("/etcdserverpb.KV/DeleteRange", request.get_ref())
            .await;
        let mut kv_client = self.kv_client.clone();
        kv_client.delete_range(request).await
    }

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        self.log_request("/etcdserverpb.KV/Txn", request.get_ref())
            .await;
        let mut kv_client = self.kv_client.clone();
        kv_client.txn(request).await
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        self.log_request("/etcdserverpb.KV/Compact", request.get_ref())
            .await;
        let mut kv_client = self.kv_client.clone();
        kv_client.compact(request).await
    }
}

#[tonic::async_trait]
impl Maintenance for ProxyService {
    type SnapshotStream = tonic::codec::Streaming<SnapshotResponse>;

    async fn alarm(
        &self,
        request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        self.log_request("/etcdserverpb.Maintenance/Alarm", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.alarm(request).await
    }

    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.log_request("/etcdserverpb.Maintenance/Status", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.status(request).await
    }

    async fn defragment(
        &self,
        request: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        self.log_request("/etcdserverpb.Maintenance/Defragment", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.defragment(request).await
    }

    async fn hash(&self, request: Request<HashRequest>) -> Result<Response<HashResponse>, Status> {
        self.log_request("/etcdserverpb.Maintenance/Hash", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.hash(request).await
    }

    async fn hash_kv(
        &self,
        request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        self.log_request("/etcdserverpb.Maintenance/HashKV", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.hash_kv(request).await
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        self.log_request("/etcdserverpb.Maintenance/Snapshot", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.snapshot(request).await
    }

    async fn move_leader(
        &self,
        request: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        self.log_request("/etcdserverpb.Maintenance/MoveLeader", request.get_ref())
            .await;
        let mut maintenance_client = self.maintenance_client.clone();
        maintenance_client.move_leader(request).await
    }
}

#[tonic::async_trait]
impl Lease for ProxyService {
    type LeaseKeepAliveStream = tonic::codec::Streaming<LeaseKeepAliveResponse>;

    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        self.log_request("/etcdserverpb.Lease/LeaseGrant", request.get_ref())
            .await;
        let mut lease_client = self.lease_client.clone();
        lease_client.lease_grant(request).await
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        self.log_request("/etcdserverpb.Lease/LeaseRevoke", request.get_ref())
            .await;
        let mut lease_client = self.lease_client.clone();
        lease_client.lease_revoke(request).await
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let mut lease_client = self.lease_client.clone();
        let logger = Arc::clone(&self.logger);
        let (metadata, extensions, mut inbound) = request.into_parts();
        let outbound = async_stream::stream! {
            loop {
                match inbound.message().await {
                    Ok(Some(message)) => {
                        logger.log(LogEntry {
                            timestamp_ms: now_ms(),
                            method: "/etcdserverpb.Lease/LeaseKeepAlive",
                            request: format!("{message:?}"),
                            stream_id: None,
                            direction: None,
                            sequence: None,
                            details: None,
                        }).await;
                        yield message;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        };
        let request = Request::from_parts(metadata, extensions, outbound);
        lease_client.lease_keep_alive(request).await
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        self.log_request("/etcdserverpb.Lease/LeaseTimeToLive", request.get_ref())
            .await;
        let mut lease_client = self.lease_client.clone();
        lease_client.lease_time_to_live(request).await
    }

    async fn lease_leases(
        &self,
        request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        self.log_request("/etcdserverpb.Lease/LeaseLeases", request.get_ref())
            .await;
        let mut lease_client = self.lease_client.clone();
        lease_client.lease_leases(request).await
    }
}

#[tonic::async_trait]
impl Watch for ProxyService {
    type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let stream_id = NEXT_WATCH_STREAM_ID.fetch_add(1, Ordering::Relaxed);
        let mut watch_client = self.watch_client.clone();
        let logger = Arc::clone(&self.logger);
        let (metadata, extensions, mut inbound) = request.into_parts();
        let require_leader = require_leader_enabled(&metadata);
        let (inbound_error_tx, inbound_error_rx) = oneshot::channel::<Status>();
        let (request_tx, request_rx) = mpsc::channel(8);
        let inbound_logger = Arc::clone(&logger);
        tokio::spawn(async move {
            let mut sequence = 0u64;
            loop {
                match inbound.message().await {
                    Ok(Some(message)) => {
                        let details = format!(
                            "{} require_leader={require_leader}",
                            watch_request_details(&message)
                        );
                        inbound_logger
                            .log(LogEntry {
                                timestamp_ms: now_ms(),
                                method: "/etcdserverpb.Watch/Watch",
                                request: format!("{message:?}"),
                                stream_id: Some(stream_id),
                                direction: Some("inbound"),
                                sequence: Some(sequence),
                                details: Some(details),
                            })
                            .await;
                        sequence = sequence.saturating_add(1);
                        if request_tx.send(message).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        let _ = inbound_error_tx.send(status);
                        break;
                    }
                }
            }
        });
        let request = Request::from_parts(metadata, extensions, ReceiverStream::new(request_rx));
        let response = watch_client.watch(request).await?;
        let upstream = response.into_inner();
        let (response_tx, response_rx) = mpsc::channel::<Result<WatchResponse, Status>>(8);
        tokio::spawn(dispatch_watch_responses(
            upstream,
            inbound_error_rx,
            response_tx,
            logger,
            stream_id,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(response_rx))))
    }
}

async fn dispatch_watch_responses<S>(
    upstream: S,
    mut inbound_error_rx: oneshot::Receiver<Status>,
    response_tx: mpsc::Sender<Result<WatchResponse, Status>>,
    logger: Arc<Logger>,
    stream_id: u64,
) where
    S: Stream<Item = Result<WatchResponse, Status>> + Send + 'static,
{
    let mut sequence = 0u64;
    let mut upstream = Box::pin(upstream);
    let mut inbound_error_open = true;
    loop {
        tokio::select! {
            biased;
            inbound_result = &mut inbound_error_rx, if inbound_error_open => {
                inbound_error_open = false;
                if let Ok(status) = inbound_result {
                    // Dropping the task immediately drops the
                    // upstream stream and cancels the response side.
                    let _ = response_tx.send(Err(status)).await;
                    break;
                }
            }
            response = upstream.next() => match response {
                Some(Ok(message)) => {
                    let details = watch_response_details(&message);
                    logger.log(LogEntry {
                        timestamp_ms: now_ms(),
                        method: "/etcdserverpb.Watch/Watch",
                        request: format!("{message:?}"),
                        stream_id: Some(stream_id),
                        direction: Some("outbound"),
                        sequence: Some(sequence),
                        details: Some(details),
                    }).await;
                    sequence = sequence.saturating_add(1);
                    if response_tx.send(Ok(message)).await.is_err() {
                        break;
                    }
                }
                Some(Err(status)) => {
                    let _ = response_tx.send(Err(status)).await;
                    break;
                }
                None => break,
            }
        }
    }
}

fn require_leader_enabled(metadata: &tonic::metadata::MetadataMap) -> bool {
    metadata
        .get("hasleader")
        .and_then(|value| value.to_str().ok())
        == Some("true")
}

impl ProxyService {
    async fn log_request<T: std::fmt::Debug>(&self, method: &'static str, request: &T) {
        self.logger
            .log(LogEntry {
                timestamp_ms: now_ms(),
                method,
                request: format!("{request:?}"),
                stream_id: None,
                direction: None,
                sequence: None,
                details: None,
            })
            .await;
    }
}

#[derive(Debug, Clone, Serialize)]
struct LogEntry {
    timestamp_ms: u128,
    method: &'static str,
    request: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    direction: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<String>,
}

fn watch_request_details(request: &WatchRequest) -> String {
    match request.request_union.as_ref() {
        Some(crate::pb::etcdserverpb::watch_request::RequestUnion::CreateRequest(create)) => {
            format!(
                "create key={} range_end={} start_revision={} prev_kv={} progress_notify={}",
                create.key.len(),
                create.range_end.len(),
                create.start_revision,
                create.prev_kv,
                create.progress_notify
            )
        }
        Some(crate::pb::etcdserverpb::watch_request::RequestUnion::ProgressRequest(_)) => {
            "progress_request watch_id=-1".into()
        }
        Some(crate::pb::etcdserverpb::watch_request::RequestUnion::CancelRequest(cancel)) => {
            format!("cancel watch_id={}", cancel.watch_id)
        }
        None => "empty".into(),
    }
}

fn watch_response_details(response: &WatchResponse) -> String {
    let event_bytes: usize = response
        .events
        .iter()
        .map(|event| {
            event
                .kv
                .as_ref()
                .map_or(0, |kv| kv.key.len() + kv.value.len())
                + event
                    .prev_kv
                    .as_ref()
                    .map_or(0, |kv| kv.key.len() + kv.value.len())
        })
        .sum();
    format!(
        "watch_id={} created={} canceled={} compact_revision={} cancel_reason={:?} events={} event_bytes={} revision={}",
        response.watch_id,
        response.created,
        response.canceled,
        response.compact_revision,
        response.cancel_reason,
        response.events.len(),
        event_bytes,
        response.header.as_ref().map_or(0, |header| header.revision)
    )
}

#[derive(Debug, Clone, Copy)]
pub struct LoggerOptions {
    pub buffer_capacity_bytes: usize,
    pub channel_capacity: usize,
    pub flush_interval: Duration,
}

enum LogCommand {
    Entry(LogEntry, Option<oneshot::Sender<()>>),
    Flush(oneshot::Sender<()>),
}

struct WriteBatch {
    bytes: Vec<u8>,
    completion: Option<oneshot::Sender<()>>,
}

pub struct Logger {
    sender: mpsc::Sender<LogCommand>,
}

impl Logger {
    pub fn new(file_path: impl AsRef<Path>, options: LoggerOptions) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .expect("failed to open log file");
        let file = tokio::fs::File::from_std(file);
        let (sender, receiver) = mpsc::channel(options.channel_capacity);
        tokio::spawn(run_logger(file, receiver, options));

        Logger { sender }
    }

    async fn log(&self, entry: LogEntry) {
        self.sender
            .send(LogCommand::Entry(entry, None))
            .await
            .expect("log writer stopped");
    }

    pub async fn flush(&self) {
        let (completion, finished) = oneshot::channel();
        self.sender
            .send(LogCommand::Flush(completion))
            .await
            .expect("log writer stopped");
        finished.await.expect("log writer stopped before flushing");
    }
}

async fn run_logger(
    file: tokio::fs::File,
    receiver: mpsc::Receiver<LogCommand>,
    options: LoggerOptions,
) {
    let (batch_sender, batch_receiver) = mpsc::channel(1);
    let (empty_buffer_sender, empty_buffer_receiver) = mpsc::channel(1);
    let writer = tokio::spawn(write_batches(
        file,
        batch_receiver,
        empty_buffer_sender,
        options.buffer_capacity_bytes,
    ));

    collect_entries(
        receiver,
        batch_sender,
        empty_buffer_receiver,
        options.buffer_capacity_bytes,
        options.flush_interval,
    )
    .await;
    writer.await.expect("log writer task failed");
}

async fn collect_entries(
    mut receiver: mpsc::Receiver<LogCommand>,
    batch_sender: mpsc::Sender<WriteBatch>,
    mut empty_buffer_receiver: mpsc::Receiver<Vec<u8>>,
    buffer_capacity_bytes: usize,
    flush_interval: Duration,
) {
    let mut active = Vec::with_capacity(buffer_capacity_bytes);
    let mut standby = Some(Vec::with_capacity(buffer_capacity_bytes));
    let mut flush_interval = tokio::time::interval(flush_interval);
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    flush_interval.tick().await;

    loop {
        tokio::select! {
            command = receiver.recv() => {
                match command {
                    Some(LogCommand::Entry(entry, received)) => {
                        serde_json::to_writer(&mut active, &entry)
                            .expect("failed to serialize log entry");
                        active.push(b'\n');
                        if let Some(received) = received {
                            let _ = received.send(());
                        }
                        if active.len() >= buffer_capacity_bytes {
                            submit_buffer(
                                &mut active,
                                &mut standby,
                                &batch_sender,
                                &mut empty_buffer_receiver,
                                None,
                            )
                            .await;
                        }
                    }
                    Some(LogCommand::Flush(completion)) => {
                        submit_buffer(
                            &mut active,
                            &mut standby,
                            &batch_sender,
                            &mut empty_buffer_receiver,
                            Some(completion),
                        )
                        .await;
                    }
                    None => {
                        if !active.is_empty() {
                            submit_buffer(
                                &mut active,
                                &mut standby,
                                &batch_sender,
                                &mut empty_buffer_receiver,
                                None,
                            )
                            .await;
                        }
                        break;
                    }
                }
            }
            _ = flush_interval.tick(), if !active.is_empty() => {
                submit_buffer(
                    &mut active,
                    &mut standby,
                    &batch_sender,
                    &mut empty_buffer_receiver,
                    None,
                )
                .await;
            }
        }
    }
}

async fn submit_buffer(
    active: &mut Vec<u8>,
    standby: &mut Option<Vec<u8>>,
    batch_sender: &mpsc::Sender<WriteBatch>,
    empty_buffer_receiver: &mut mpsc::Receiver<Vec<u8>>,
    completion: Option<oneshot::Sender<()>>,
) {
    let replacement = match standby.take() {
        Some(buffer) => buffer,
        None => empty_buffer_receiver
            .recv()
            .await
            .expect("log writer stopped before returning a buffer"),
    };
    let bytes = mem::replace(active, replacement);
    batch_sender
        .send(WriteBatch { bytes, completion })
        .await
        .unwrap_or_else(|_| panic!("log writer stopped before writing a buffer"));
}

async fn write_batches(
    mut file: tokio::fs::File,
    mut batch_receiver: mpsc::Receiver<WriteBatch>,
    empty_buffer_sender: mpsc::Sender<Vec<u8>>,
    buffer_capacity_bytes: usize,
) {
    while let Some(batch) = batch_receiver.recv().await {
        if !batch.bytes.is_empty() {
            file.write_all(&batch.bytes)
                .await
                .expect("failed to write log entries");
        }
        file.flush().await.expect("failed to flush log entries");

        if let Some(completion) = batch.completion {
            let _ = completion.send(());
        }

        let mut bytes = batch.bytes;
        bytes.clear();
        bytes.shrink_to(buffer_capacity_bytes);
        let _ = empty_buffer_sender.send(bytes).await;
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_leader_metadata_parse() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            "hasleader",
            tonic::metadata::MetadataValue::from_static("true"),
        );
        assert!(require_leader_enabled(&metadata));

        let mut legacy = tonic::metadata::MetadataMap::new();
        legacy.insert(
            "x-etcd-require-leader",
            tonic::metadata::MetadataValue::from_static("true"),
        );
        assert!(!require_leader_enabled(&legacy));
    }

    fn test_log_path(test_name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "etcd_proxy_rs_{test_name}_{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }
    fn logger_options(buffer_capacity_bytes: usize, flush_interval: Duration) -> LoggerOptions {
        LoggerOptions {
            buffer_capacity_bytes,
            channel_capacity: 16,
            flush_interval,
        }
    }

    #[tokio::test]
    async fn test_watch_status_forward() {
        let path = test_log_path("dispatch_upstream_status");
        let logger = Arc::new(Logger::new(
            &path,
            logger_options(1024, Duration::from_secs(1)),
        ));
        let (_inbound_error_tx, inbound_error_rx) = oneshot::channel();
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let mut expected = Status::with_details(
            tonic::Code::ResourceExhausted,
            "upstream status",
            tonic::codegen::Bytes::from_static(b"details"),
        );
        expected.metadata_mut().insert(
            "x-upstream-error",
            tonic::metadata::MetadataValue::from_static("yes"),
        );

        dispatch_watch_responses(
            tokio_stream::iter([Err(expected)]),
            inbound_error_rx,
            response_tx,
            Arc::clone(&logger),
            1,
        )
        .await;

        match tokio::time::timeout(Duration::from_secs(1), response_rx.recv()).await {
            Ok(Some(Err(status))) => {
                assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                assert_eq!(status.message(), "upstream status");
                assert_eq!(status.details(), b"details");
                assert_eq!(status.metadata().get("x-upstream-error").unwrap(), "yes");
            }
            other => panic!("expected upstream status, got {other:?}"),
        }
        assert!(response_rx.recv().await.is_none());
        logger.flush().await;
    }

    #[tokio::test]
    async fn test_watch_eof_close() {
        let path = test_log_path("dispatch_upstream_eof");
        let logger = Arc::new(Logger::new(
            &path,
            logger_options(1024, Duration::from_secs(1)),
        ));
        let (_inbound_error_tx, inbound_error_rx) = oneshot::channel();
        let (response_tx, mut response_rx) = mpsc::channel(1);

        dispatch_watch_responses(
            tokio_stream::empty::<Result<WatchResponse, Status>>(),
            inbound_error_rx,
            response_tx,
            Arc::clone(&logger),
            1,
        )
        .await;

        assert!(response_rx.recv().await.is_none());
        logger.flush().await;
    }

    #[derive(Clone, Default)]
    struct UpstreamWatchProbe {
        requests: Arc<std::sync::Mutex<Vec<WatchRequest>>>,
        notified: Arc<tokio::sync::Notify>,
    }

    #[tonic::async_trait]
    impl Watch for UpstreamWatchProbe {
        type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

        async fn watch(
            &self,
            request: Request<Streaming<WatchRequest>>,
        ) -> Result<Response<Self::WatchStream>, Status> {
            let mut inbound = request.into_inner();
            let probe = self.clone();
            tokio::spawn(async move {
                while let Ok(Some(request)) = inbound.message().await {
                    probe.requests.lock().unwrap().push(request);
                    probe.notified.notify_one();
                }
            });
            Ok(Response::new(Box::pin(tokio_stream::pending())))
        }
    }

    async fn wait_for_upstream_watches(probe: &UpstreamWatchProbe, count: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notified = probe.notified.notified();
                if probe.requests.lock().unwrap().len() >= count {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("upstream Watch did not receive the proxied request");
    }

    #[tokio::test]
    async fn test_watch_new_stream_logs() {
        let path = test_log_path("watch_new_streams");
        let logger = Arc::new(Logger::new(
            &path,
            logger_options(4096, Duration::from_secs(60)),
        ));
        let probe = UpstreamWatchProbe::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_probe = probe.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(crate::pb::etcdserverpb::watch_server::WatchServer::new(
                    server_probe,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let service = ProxyService::new(
            KvClient::new(channel.clone()),
            WatchClient::new(channel.clone()),
            LeaseClient::new(channel.clone()),
            MaintenanceClient::new(channel),
            Arc::clone(&logger),
        );

        let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_address = proxy_listener.local_addr().unwrap();
        let proxy_server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(crate::pb::etcdserverpb::watch_server::WatchServer::new(
                    service,
                ))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    proxy_listener,
                ))
                .await
                .unwrap();
        });
        let proxy_channel =
            tonic::transport::Endpoint::from_shared(format!("http://{proxy_address}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
        let mut proxy_client = WatchClient::new(proxy_channel);

        let mut response_streams = Vec::new();
        for (key, revision) in [
            (b"/registry/pods/a".as_slice(), 1),
            (b"/registry/pods/b", 2),
        ] {
            let (request_tx, request_rx) = mpsc::channel(1);
            let response = proxy_client
                .watch(Request::new(ReceiverStream::new(request_rx)))
                .await
                .unwrap();
            response_streams.push(response.into_inner());
            request_tx
                .send(WatchRequest {
                    request_union: Some(
                        crate::pb::etcdserverpb::watch_request::RequestUnion::CreateRequest(
                            crate::pb::etcdserverpb::WatchCreateRequest {
                                key: key.to_vec(),
                                range_end: b"\0".to_vec(),
                                start_revision: revision,
                                prev_kv: true,
                                progress_notify: true,
                                ..Default::default()
                            },
                        ),
                    ),
                })
                .await
                .unwrap();
            wait_for_upstream_watches(&probe, response_streams.len()).await;
        }
        logger.flush().await;

        let content = std::fs::read_to_string(&*path).unwrap();
        let entries: Vec<serde_json::Value> = content
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .filter(|entry| entry["method"] == "/etcdserverpb.Watch/Watch")
            .collect();
        assert_eq!(entries.len(), 2);
        let first_stream = entries[0]["stream_id"].as_u64().unwrap();
        let second_stream = entries[1]["stream_id"].as_u64().unwrap();
        assert_ne!(first_stream, second_stream);
        for (entry, revision) in entries.iter().zip([1, 2]) {
            assert_eq!(entry["direction"], "inbound");
            assert_eq!(entry["sequence"], 0);
            let details = entry["details"].as_str().unwrap();
            assert!(details.contains("create key="));
            assert!(details.contains(&format!("start_revision={revision}")));
            assert!(!details.contains("reconnect"));
        }
        drop(response_streams);
        proxy_server.abort();
        let _ = proxy_server.await;
        server.abort();
        let _ = server.await;
    }
    fn test_log_entry(request: String) -> LogEntry {
        LogEntry {
            timestamp_ms: 1,
            method: "/etcdserverpb.KV/Range",
            request,
            stream_id: None,
            direction: None,
            sequence: None,
            details: None,
        }
    }

    fn encoded_entry_len(entry: &LogEntry) -> usize {
        let mut bytes = Vec::new();
        serde_json::to_writer(&mut bytes, entry).unwrap();
        bytes.push(b'\n');
        bytes.len()
    }

    #[tokio::test]
    async fn test_logger_json_append() {
        let path = test_log_path("appends");
        std::fs::write(
            &*path,
            "{\"timestamp_ms\":0,\"method\":\"sentinel\",\"request\":\"existing \\\"quoted\\\" entry\"}\n",
        )
        .unwrap();
        let logger = Logger::new(&path, logger_options(1024, Duration::from_secs(60)));
        logger
            .log(test_log_entry("first \"quoted\"\nline".into()))
            .await;
        logger.flush().await;
        drop(logger);

        let content = std::fs::read_to_string(&*path).expect("failed to read log file");
        let lines: Vec<serde_json::Value> = content
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["method"], "sentinel");
        assert_eq!(lines[0]["request"], "existing \"quoted\" entry");
        assert_eq!(lines[1]["method"], "/etcdserverpb.KV/Range");
        assert_eq!(lines[1]["request"], "first \"quoted\"\nline");
    }

    async fn wait_entry_received(mut received: oneshot::Receiver<()>) {
        for _ in 0..100 {
            if received.try_recv().is_ok() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("collector did not receive the entry");
    }

    #[tokio::test(start_paused = true)]
    async fn test_logger_interval_flush() {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (batch_tx, mut batch_rx) = mpsc::channel(1);
        let (_empty_buffer_tx, empty_buffer_rx) = mpsc::channel(1);
        let collector = tokio::spawn(collect_entries(
            command_rx,
            batch_tx,
            empty_buffer_rx,
            1024,
            Duration::from_millis(20),
        ));
        tokio::task::yield_now().await;
        let (received_tx, received_rx) = oneshot::channel();
        command_tx
            .send(LogCommand::Entry(
                test_log_entry("interval".into()),
                Some(received_tx),
            ))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        wait_entry_received(received_rx).await;
        assert!(
            batch_rx.try_recv().is_err(),
            "entry must not flush before the interval boundary"
        );

        tokio::time::advance(Duration::from_millis(20)).await;
        let mut batch = None;
        for _ in 0..3 {
            tokio::task::yield_now().await;
            if let Ok(received) = batch_rx.try_recv() {
                batch = Some(received);
                break;
            }
        }
        let batch = batch.expect("entry must flush at the interval boundary");
        assert!(
            std::str::from_utf8(&batch.bytes)
                .unwrap()
                .contains("\"request\":\"interval\"")
        );
        drop(command_tx);
        collector.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_logger_capacity_flush() {
        for (case, request, adjust_capacity) in [
            ("below", "small".to_string(), 1024usize),
            ("equal", "x".repeat(128), 0),
            ("above", "x".repeat(128), usize::MAX),
        ] {
            let entry = test_log_entry(request);
            let encoded_len = encoded_entry_len(&entry);
            let capacity = if case == "below" {
                adjust_capacity
            } else if case == "equal" {
                encoded_len
            } else {
                encoded_len - 1
            };
            let (command_tx, command_rx) = mpsc::channel(1);
            let (batch_tx, mut batch_rx) = mpsc::channel(1);
            let (_empty_buffer_tx, empty_buffer_rx) = mpsc::channel(1);
            let collector = tokio::spawn(collect_entries(
                command_rx,
                batch_tx,
                empty_buffer_rx,
                capacity,
                Duration::from_secs(3600),
            ));
            tokio::task::yield_now().await;
            let (received_tx, received_rx) = oneshot::channel();
            command_tx
                .send(LogCommand::Entry(entry, Some(received_tx)))
                .await
                .unwrap();
            tokio::task::yield_now().await;
            wait_entry_received(received_rx).await;
            let batch = batch_rx.try_recv();
            if case == "below" {
                assert!(batch.is_err(), "{case} entry must stay buffered");
            } else {
                let batch = batch.unwrap_or_else(|_| panic!("{case} entry must flush"));
                assert_eq!(batch.bytes.len(), encoded_len);
            }
            drop(command_tx);
            collector.await.unwrap();
        }
    }
}
