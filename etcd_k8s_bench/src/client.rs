use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;
use tokio_stream::Stream;
use tonic::codec::Streaming;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::pb::etcdserverpb::kv_client::KvClient;
use crate::pb::etcdserverpb::maintenance_client::MaintenanceClient;
use crate::pb::etcdserverpb::watch_client::WatchClient as GeneratedWatchClient;
use crate::pb::etcdserverpb::{
    CompactionRequest, CompactionResponse, RangeRequest, RangeResponse, StatusRequest,
    StatusResponse, TxnRequest, TxnResponse, WatchRequest, WatchResponse,
};

pub const REQUIRE_LEADER_METADATA: &str = "hasleader";
/// Construct the tonic Watch request with etcd's canonical require-leader
/// metadata. Keeping construction separate makes the metadata observable
/// before the request is handed to the generated client.
pub fn watch_request<S>(requests: S) -> Request<S> {
    let mut request = Request::new(requests);
    request
        .metadata_mut()
        .insert(REQUIRE_LEADER_METADATA, MetadataValue::from_static("true"));
    request
}

#[derive(Clone)]
pub struct ClientBundle {
    kv: KvClient<Channel>,
    maintenance: MaintenanceClient<Channel>,
    watch: GeneratedWatchClient<Channel>,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("etcd transport error: {0}")]
    Transport(#[source] tonic::transport::Error),
    #[error("etcd RPC error: {0}")]
    Rpc(#[from] Status),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcMutability {
    ReadOnly,
    Mutable,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcMethod {
    Range,
    Status,
    Txn,
    Compact,
}

impl RpcMethod {
    pub const fn mutability(self) -> RpcMutability {
        match self {
            Self::Range | Self::Status => RpcMutability::ReadOnly,
            Self::Txn | Self::Compact => RpcMutability::Mutable,
        }
    }

    pub const fn is_mutable(self) -> bool {
        matches!(self.mutability(), RpcMutability::Mutable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestDisposition {
    /// A local, structured failure proved that no request bytes were sent.
    NotSent,
    /// The request was handed to tonic, but no response was received.
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    Retry,
    NotSent,
    ResultUnknown,
    Stop,
}

impl ClientError {
    /// Return whether the transport reported a real timeout.
    ///
    /// Tonic maps its local endpoint timeout to a cancelled status while
    /// preserving `tonic::TimeoutExpired` in the source chain. A server-side
    /// deadline is reported directly as `DEADLINE_EXCEEDED`; neither signal
    /// should be inferred from status text.
    pub fn is_timeout(&self) -> bool {
        match self {
            Self::Transport(error) => has_timeout_source(error),
            Self::Rpc(status) => {
                status.code() == Code::DeadlineExceeded || has_timeout_source(status)
            }
        }
    }

    /// Classify one error in the context of the concrete RPC method.
    ///
    /// The old method-free classifier was deliberately removed: replaying a
    /// mutable request after a lost response can duplicate a lease or write.
    /// `NotSent` is only supplied by callers with structured local evidence;
    /// tonic RPC failures use `Unknown`.
    pub fn retry_decision(
        &self,
        method: RpcMethod,
        disposition: RequestDisposition,
    ) -> RetryDecision {
        if disposition == RequestDisposition::NotSent {
            return RetryDecision::NotSent;
        }
        let recoverable = self.is_timeout()
            || match self {
                Self::Transport(_) => true,
                Self::Rpc(status) => matches!(
                    status.code(),
                    Code::Unavailable
                        | Code::DeadlineExceeded
                        | Code::ResourceExhausted
                        | Code::Aborted
                ),
            };
        if !recoverable {
            return RetryDecision::Stop;
        }
        if method.is_mutable() {
            RetryDecision::ResultUnknown
        } else {
            RetryDecision::Retry
        }
    }
}

fn has_timeout_source(error: &(dyn StdError + 'static)) -> bool {
    let mut source = Some(error);
    while let Some(error) = source {
        if error.downcast_ref::<tonic::TimeoutExpired>().is_some() {
            return true;
        }
        source = error.source();
    }
    false
}

impl ClientBundle {
    pub async fn connect(
        endpoint: &str,
        request_timeout: Duration,
        max_message_size: usize,
    ) -> Result<Self, ClientError> {
        let endpoint = Endpoint::from_shared(endpoint.to_string())
            .map_err(|error| ClientError::Transport(error.into()))?
            .connect_timeout(request_timeout)
            .timeout(request_timeout)
            // Large prev_kv Watch streams need BDP-sized receive windows.
            // https://docs.rs/tonic/0.14.6/tonic/transport/struct.Endpoint.html#method.http2_adaptive_window
            .http2_adaptive_window(true)
            .tcp_keepalive(Some(Duration::from_secs(30)));
        let channel = endpoint.connect().await.map_err(ClientError::Transport)?;
        Ok(Self::from_channel(channel, max_message_size))
    }

    pub(crate) fn from_channel(channel: Channel, max_message_size: usize) -> Self {
        let kv = KvClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        let maintenance = MaintenanceClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        let watch = GeneratedWatchClient::new(channel)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        Self {
            kv,
            maintenance,
            watch,
        }
    }

    /// Open the single bidirectional Watch RPC with Kubernetes' require-leader
    /// metadata. The request stream remains owned by the caller for backpressure.
    pub async fn watch_stream<S>(&self, requests: S) -> Result<Streaming<WatchResponse>, Status>
    where
        S: Stream<Item = WatchRequest> + Send + 'static,
    {
        let mut client = self.watch.clone();
        Ok(client.watch(watch_request(requests)).await?.into_inner())
    }

    pub async fn status(&self) -> Result<StatusResponse, ClientError> {
        let mut client = self.maintenance.clone();
        Ok(client
            .status(Request::new(StatusRequest {}))
            .await?
            .into_inner())
    }

    /// Build the canonical etcd full-keyspace, count-only preflight request.
    fn preflight_request() -> RangeRequest {
        RangeRequest {
            key: vec![0],
            range_end: vec![0],
            count_only: true,
            serializable: false,
            keys_only: false,
            ..Default::default()
        }
    }

    /// The mandatory empty-database preflight shape: a full-keyspace,
    /// count-only Range, with no writes or lease operations preceding it.
    pub async fn preflight_count(&self) -> Result<RangeResponse, ClientError> {
        let mut client = self.kv.clone();
        Ok(client
            .range(Request::new(Self::preflight_request()))
            .await?
            .into_inner())
    }

    pub async fn txn(&self, request: TxnRequest) -> Result<TxnResponse, ClientError> {
        let mut client = self.kv.clone();
        Ok(client.txn(Request::new(request)).await?.into_inner())
    }

    pub async fn range(&self, request: RangeRequest) -> Result<RangeResponse, ClientError> {
        let mut client = self.kv.clone();
        Ok(client.range(Request::new(request)).await?.into_inner())
    }

    pub async fn compact(
        &self,
        request: CompactionRequest,
    ) -> Result<CompactionResponse, ClientError> {
        let mut client = self.kv.clone();
        Ok(client.compact(Request::new(request)).await?.into_inner())
    }
}

#[cfg(test)]
#[path = "../tests/unit/client.rs"]
mod tests;
