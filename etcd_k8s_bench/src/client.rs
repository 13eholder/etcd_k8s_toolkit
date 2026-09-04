use std::time::Duration;

use thiserror::Error;
use tokio_stream::Stream;
use tonic::codec::Streaming;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::pb::etcdserverpb::kv_client::KvClient;
use crate::pb::etcdserverpb::lease_client::LeaseClient;
use crate::pb::etcdserverpb::maintenance_client::MaintenanceClient;
use crate::pb::etcdserverpb::watch_client::WatchClient as GeneratedWatchClient;
use crate::pb::etcdserverpb::{
    CompactionRequest, CompactionResponse, LeaseGrantRequest, LeaseGrantResponse, RangeRequest,
    RangeResponse, StatusRequest, StatusResponse, TxnRequest, TxnResponse, WatchRequest,
    WatchResponse,
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
    lease: LeaseClient<Channel>,
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
    LeaseGrant,
    Compact,
}

impl RpcMethod {
    pub const fn mutability(self) -> RpcMutability {
        match self {
            Self::Range | Self::Status => RpcMutability::ReadOnly,
            Self::Txn | Self::LeaseGrant | Self::Compact => RpcMutability::Mutable,
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
    /// Classify one error in the context of the concrete RPC method.
    ///
    /// The old method-free classifier was deliberately removed: replaying
    /// a mutable request after a lost response can duplicate a lease or write.
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
        match self {
            Self::Transport(_) if !method.is_mutable() => RetryDecision::Retry,
            Self::Transport(_) => RetryDecision::ResultUnknown,
            Self::Rpc(status) if !method.is_mutable() => {
                if matches!(
                    status.code(),
                    Code::Unavailable
                        | Code::DeadlineExceeded
                        | Code::ResourceExhausted
                        | Code::Aborted
                ) {
                    RetryDecision::Retry
                } else {
                    RetryDecision::Stop
                }
            }
            Self::Rpc(status) => {
                if matches!(
                    status.code(),
                    Code::Unavailable
                        | Code::DeadlineExceeded
                        | Code::ResourceExhausted
                        | Code::Aborted
                ) {
                    RetryDecision::ResultUnknown
                } else {
                    RetryDecision::Stop
                }
            }
        }
    }
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
            .tcp_keepalive(Some(Duration::from_secs(30)));
        let channel = endpoint.connect().await.map_err(ClientError::Transport)?;
        let kv = KvClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        let lease = LeaseClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        let maintenance = MaintenanceClient::new(channel.clone())
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        let watch = GeneratedWatchClient::new(channel)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        Ok(Self {
            kv,
            lease,
            maintenance,
            watch,
        })
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

    pub async fn lease_grant(
        &self,
        request: LeaseGrantRequest,
    ) -> Result<LeaseGrantResponse, ClientError> {
        let mut client = self.lease.clone();
        Ok(client
            .lease_grant(Request::new(request))
            .await?
            .into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_request_contains_canonical_require_leader_metadata() {
        let request = watch_request(());
        assert_eq!(
            request
                .metadata()
                .get(REQUIRE_LEADER_METADATA)
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
    }

    #[test]
    fn preflight_request_is_canonical_full_keyspace_count_only() {
        assert_eq!(
            ClientBundle::preflight_request(),
            RangeRequest {
                key: vec![0],
                range_end: vec![0],
                count_only: true,
                serializable: false,
                keys_only: false,
                ..Default::default()
            }
        );
    }
    #[test]
    fn retry_classification_is_method_aware() {
        let codes = [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::ResourceExhausted,
            Code::Aborted,
        ];
        for code in codes {
            let error = ClientError::Rpc(Status::new(code, "transient"));
            assert_eq!(
                error.retry_decision(RpcMethod::Range, RequestDisposition::Unknown),
                RetryDecision::Retry
            );
            assert_eq!(
                error.retry_decision(RpcMethod::Status, RequestDisposition::Unknown),
                RetryDecision::Retry
            );
            assert_eq!(
                error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown),
                RetryDecision::ResultUnknown
            );
            assert_eq!(
                error.retry_decision(RpcMethod::LeaseGrant, RequestDisposition::Unknown),
                RetryDecision::ResultUnknown
            );
        }
    }

    #[test]
    fn context_and_cas_errors_are_not_transport_retries() {
        let cancelled = ClientError::Rpc(Status::cancelled("caller cancelled"));
        assert_eq!(
            cancelled.retry_decision(RpcMethod::Range, RequestDisposition::Unknown),
            RetryDecision::Stop
        );
        assert_eq!(
            cancelled.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown),
            RetryDecision::Stop
        );
        let conflict = ClientError::Rpc(Status::aborted("cas conflict"));
        assert_eq!(
            conflict.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown),
            RetryDecision::ResultUnknown
        );
    }

    #[test]
    fn explicit_not_sent_error_is_separately_classified() {
        let error = ClientError::Rpc(Status::unavailable("before send"));
        assert_eq!(
            error.retry_decision(RpcMethod::LeaseGrant, RequestDisposition::NotSent),
            RetryDecision::NotSent
        );
    }

    #[test]
    fn method_mutability_is_explicit() {
        assert_eq!(RpcMethod::Range.mutability(), RpcMutability::ReadOnly);
        assert_eq!(RpcMethod::Status.mutability(), RpcMutability::ReadOnly);
        assert_eq!(RpcMethod::Txn.mutability(), RpcMutability::Mutable);
        assert_eq!(RpcMethod::LeaseGrant.mutability(), RpcMutability::Mutable);
    }
    #[test]
    fn thiserror_preserves_client_display_and_source() {
        let error = ClientError::from(Status::unavailable("temporary"));
        assert!(error.to_string().starts_with("etcd RPC error: "));
        assert!(error.to_string().contains("temporary"));
        assert!(std::error::Error::source(&error).is_some());
    }
}
