use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

use crate::pb::etcdserverpb::kv_client::KvClient;
use crate::pb::etcdserverpb::kv_server::Kv;
use crate::pb::etcdserverpb::lease_client::LeaseClient;
use crate::pb::etcdserverpb::lease_server::Lease;
use crate::pb::etcdserverpb::watch_client::WatchClient;
use crate::pb::etcdserverpb::watch_server::Watch;
use crate::pb::etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse,
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
    LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, PutRequest, PutResponse, RangeRequest,
    RangeResponse, TxnRequest, TxnResponse, WatchRequest, WatchResponse,
};

#[derive(Clone)]
pub struct ProxyService {
    kv_client: KvClient<Channel>,
    watch_client: WatchClient<Channel>,
    lease_client: LeaseClient<Channel>,
    logger: Arc<Logger>,
}

impl ProxyService {
    pub fn new(
        kv_client: KvClient<Channel>,
        watch_client: WatchClient<Channel>,
        lease_client: LeaseClient<Channel>,
        logger: Arc<Logger>,
    ) -> Self {
        ProxyService {
            kv_client,
            watch_client,
            lease_client,
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
        let (metadata, extensions, mut inbound) = request.into_parts();
        let outbound = async_stream::stream! {
            loop{
                match inbound.message().await{
                    Ok(Some(message)) => yield message,
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

pub struct Logger {
    file: Mutex<File>,
}

impl Logger {
    pub fn new(file_path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .expect("failed to open log file");
        Logger {
            file: Mutex::new(file),
        }
    }

    async fn log(&self, entry: LogEntry) {
        let json_entry = serde_json::to_string(&entry).expect("failed to serialize log entry");
        let mut file = self.file.lock().await;
        writeln!(&mut *file, "{json_entry}").expect("failed to write log entry");
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

    #[tokio::test]
    async fn logger_appends_json_lines() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_log.jsonl");
        let logger = Logger::new(&path);

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

        let content = std::fs::read_to_string(&path).expect("failed to read log file");
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"method\":\"/etcdserverpb.KV/Range\""));
        assert!(lines[1].contains("\"request\":\"second\""));
        // let _ = std::fs::remove_file(path);
    }
}
