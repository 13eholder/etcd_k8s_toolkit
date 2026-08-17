use std::fs::OpenOptions;
use std::mem;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
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

    type SnapshotStream = tonic::codec::Streaming<SnapshotResponse>;

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
    type WatchStream = tonic::codec::Streaming<WatchResponse>;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut watch_client = self.watch_client.clone();
        let (metadata, extensions, mut inbound) = request.into_parts();
        let outbound = async_stream::stream! {
            loop {
                match inbound.message().await {
                    Ok(Some(message)) => yield message,
                    Ok(None) | Err(_) => break,
                }
            }
        };
        let request = Request::from_parts(metadata, extensions, outbound);
        watch_client.watch(request).await
    }
}

impl ProxyService {
    async fn log_request<T: std::fmt::Debug>(&self, method: &'static str, request: &T) {
        self.logger
            .log(LogEntry {
                timestamp_ms: now_ms(),
                method,
                request: format!("{request:?}"),
            })
            .await;
    }
}

#[derive(Debug, Clone, Serialize)]
struct LogEntry {
    timestamp_ms: u128,
    method: &'static str,
    request: String,
}

#[derive(Debug, Clone, Copy)]
pub struct LoggerOptions {
    pub buffer_capacity_bytes: usize,
    pub channel_capacity: usize,
    pub flush_interval: Duration,
}

enum LogCommand {
    Entry(LogEntry),
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
            .send(LogCommand::Entry(entry))
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
                    Some(LogCommand::Entry(entry)) => {
                        serde_json::to_writer(&mut active, &entry)
                            .expect("failed to serialize log entry");
                        active.push(b'\n');
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

    async fn wait_for_file_size(path: &std::path::Path, minimum_bytes: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if tokio::fs::metadata(path)
                    .await
                    .is_ok_and(|metadata| metadata.len() >= minimum_bytes)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for log data");
    }

    #[tokio::test]
    async fn logger_appends_json_lines() {
        let path = test_log_path("appends");
        let logger = Logger::new(&path, logger_options(1024, Duration::from_secs(1)));

        logger
            .log(LogEntry {
                timestamp_ms: 1,
                method: "/etcdserverpb.KV/Range",
                request: "first".to_string(),
            })
            .await;
        logger
            .log(LogEntry {
                timestamp_ms: 2,
                method: "/etcdserverpb.Lease/LeaseGrant",
                request: "second".to_string(),
            })
            .await;
        logger.flush().await;

        let content = std::fs::read_to_string(&path).expect("failed to read log file");
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"method\":\"/etcdserverpb.KV/Range\""));
        assert!(lines[1].contains("\"request\":\"second\""));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn logger_flushes_on_interval() {
        let path = test_log_path("interval");
        let logger = Logger::new(&path, logger_options(1024, Duration::from_millis(20)));

        logger
            .log(LogEntry {
                timestamp_ms: 1,
                method: "/etcdserverpb.KV/Range",
                request: "interval".to_string(),
            })
            .await;

        wait_for_file_size(&path, 1).await;
        let content = std::fs::read_to_string(&path).expect("failed to read log file");
        assert!(content.contains("\"request\":\"interval\""));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn logger_flushes_at_buffer_capacity() {
        let path = test_log_path("capacity");
        let buffer_capacity_bytes = 128;
        let logger = Logger::new(
            &path,
            logger_options(buffer_capacity_bytes, Duration::from_secs(1)),
        );
        logger
            .log(LogEntry {
                timestamp_ms: 1,
                method: "/etcdserverpb.KV/Put",
                request: "x".repeat(buffer_capacity_bytes),
            })
            .await;

        wait_for_file_size(&path, buffer_capacity_bytes as u64).await;
        let content = std::fs::read_to_string(&path).expect("failed to read log file");
        assert!(content.contains("\"method\":\"/etcdserverpb.KV/Put\""));
        let _ = std::fs::remove_file(path);
    }
}
