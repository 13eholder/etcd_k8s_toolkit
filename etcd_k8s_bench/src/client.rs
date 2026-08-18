use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::pb::etcdserverpb::{
    CompactionRequest, CompactionResponse, LeaseGrantRequest, LeaseGrantResponse, RangeRequest,
    RangeResponse, StatusRequest, StatusResponse, TxnRequest, TxnResponse, kv_client::KvClient,
    lease_client::LeaseClient, maintenance_client::MaintenanceClient,
};

/// Shared tonic transport and independently clonable etcd service clients.
/// Cloning a bundle clones the HTTP/2 channel rather than opening another
/// connection, which lets each worker own a client without a mutex hot path.
#[derive(Clone)]
pub struct ClientBundle {
    kv: KvClient<Channel>,
    lease: LeaseClient<Channel>,
    maintenance: MaintenanceClient<Channel>,
}

#[derive(Debug)]
pub enum ClientError {
    Transport(tonic::transport::Error),
    Rpc(Status),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "etcd transport error: {error}"),
            Self::Rpc(error) => write!(f, "etcd RPC error: {error}"),
        }
    }
}
impl std::error::Error for ClientError {}
impl From<Status> for ClientError {
    fn from(value: Status) -> Self {
        Self::Rpc(value)
    }
}
impl ClientError {
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Rpc(status) => matches!(
                status.code(),
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Aborted
            ),
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
        let maintenance = MaintenanceClient::new(channel)
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);
        Ok(Self {
            kv,
            lease,
            maintenance,
        })
    }

    pub fn lease_client(&self) -> LeaseClient<Channel> {
        self.lease.clone()
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
}
