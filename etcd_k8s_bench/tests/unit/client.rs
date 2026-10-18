use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::time;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response};

use super::*;
use crate::pb::etcdserverpb::kv_server::{Kv, KvServer};
use crate::pb::etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, TxnRequest, TxnResponse,
};

#[derive(Clone)]
struct DelayedTxnFixture {
    observed: Arc<Notify>,
    release: Arc<Notify>,
    completed: Arc<AtomicBool>,
    complete: Arc<Notify>,
    remote_tasks: Arc<StdMutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[tonic::async_trait]
impl Kv for DelayedTxnFixture {
    async fn range(
        &self,
        _request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        Ok(Response::new(RangeResponse::default()))
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

    async fn txn(
        &self,
        _request: Request<TxnRequest>,
    ) -> Result<Response<TxnResponse>, Status> {
        let (response_sender, response_receiver) = oneshot::channel();
        let release = self.release.clone();
        let completed = self.completed.clone();
        let complete = self.complete.clone();
        let remote_task = tokio::spawn(async move {
            release.notified().await;
            completed.store(true, Ordering::Release);
            complete.notify_one();
            let _ = response_sender.send(Ok(Response::new(TxnResponse {
                succeeded: true,
                ..Default::default()
            })));
        });
        self.remote_tasks.lock().unwrap().push(remote_task);
        self.observed.notify_one();
        match response_receiver.await {
            Ok(response) => response,
            Err(_) => Err(Status::cancelled("delayed fixture handler canceled")),
        }
    }

    async fn compact(
        &self,
        _request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        Ok(Response::new(CompactionResponse::default()))
    }
}

fn has_tonic_timeout_source(error: &ClientError) -> bool {
    let mut source = error.source();
    while let Some(error) = source {
        if error.downcast_ref::<tonic::TimeoutExpired>().is_some() {
            return true;
        }
        source = error.source();
    }
    false
}

async fn join_remote_tasks(remote_tasks: Arc<StdMutex<Vec<tokio::task::JoinHandle<()>>>>) {
    let tasks = std::mem::take(&mut *remote_tasks.lock().unwrap());
    for task in tasks {
        time::timeout(Duration::from_secs(1), task)
            .await
            .expect("delayed fixture task did not finish")
            .expect("delayed fixture task panicked");
    }
}

#[tokio::test(start_paused = true)]
async fn test_client_endpoint_timeout_preserves_typed_source_and_mutability() {
    let observed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let complete = Arc::new(Notify::new());
    let remote_tasks = Arc::new(StdMutex::new(Vec::new()));
    let fixture = DelayedTxnFixture {
        observed: observed.clone(),
        release: release.clone(),
        completed: completed.clone(),
        complete: complete.clone(),
        remote_tasks: remote_tasks.clone(),
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

    let client = ClientBundle::connect(
        &format!("http://{address}"),
        Duration::from_millis(10),
        64 * 1024 * 1024,
    )
    .await
    .unwrap();
    let call = tokio::spawn(async move { client.txn(TxnRequest::default()).await });
    time::timeout(Duration::from_secs(1), observed.notified())
        .await
        .expect("delayed fixture did not observe request");
    assert!(!completed.load(Ordering::Acquire));
    time::advance(Duration::from_millis(10)).await;

    let error = call.await.unwrap().unwrap_err();
    let ClientError::Rpc(status) = &error else {
        panic!("endpoint timeout did not return an RPC status");
    };
    assert_eq!(status.code(), Code::Cancelled);
    assert!(error.is_timeout());
    assert!(has_tonic_timeout_source(&error));
    assert_eq!(
        error.retry_decision(RpcMethod::Txn, RequestDisposition::Unknown),
        RetryDecision::ResultUnknown
    );
    assert_eq!(
        error.retry_decision(RpcMethod::Range, RequestDisposition::Unknown),
        RetryDecision::Retry
    );

    release.notify_one();
    time::timeout(Duration::from_secs(1), complete.notified())
        .await
        .expect("delayed fixture did not finish remote work");
    assert!(completed.load(Ordering::Acquire));
    join_remote_tasks(remote_tasks).await;
    shutdown_tx.send(()).unwrap();
    time::timeout(Duration::from_secs(1), server)
        .await
        .expect("delayed fixture server did not shut down")
        .expect("delayed fixture server task panicked")
        .unwrap();
}
#[test]
fn test_client_watch_metadata() {
    let request = watch_request(());
    assert_eq!(
        request
            .metadata()
            .get("hasleader")
            .unwrap()
            .to_str()
            .unwrap(),
        "true"
    );
}

#[test]
fn test_client_preflight_count() {
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
fn test_client_retry_classification() {
    let codes = [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Aborted,
    ];
    for code in codes {
        let error = ClientError::Rpc(Status::new(code, "transient"));
        for method in [RpcMethod::Range, RpcMethod::Status] {
            assert_eq!(
                error.retry_decision(method, RequestDisposition::Unknown),
                RetryDecision::Retry,
                "{method:?} must remain retryable for {code:?}"
            );
        }
        for method in [RpcMethod::Txn, RpcMethod::Compact] {
            assert_eq!(
                error.retry_decision(method, RequestDisposition::Unknown),
                RetryDecision::ResultUnknown,
                "{method:?} must not be replayed after {code:?}"
            );
        }
    }
}

#[test]
fn test_client_cancel_error() {
    let cancelled = ClientError::Rpc(Status::cancelled("caller cancelled"));
    assert!(!cancelled.is_timeout());
    for method in [
        RpcMethod::Range,
        RpcMethod::Status,
        RpcMethod::Txn,
        RpcMethod::Compact,
    ] {
        assert_eq!(
            cancelled.retry_decision(method, RequestDisposition::Unknown),
            RetryDecision::Stop,
            "{method:?} cancellation must stop retrying"
        );
    }
}

#[test]
fn test_client_not_sent_classification() {
    let error = ClientError::Rpc(Status::unavailable("before send"));
    assert_eq!(
        error.retry_decision(RpcMethod::Txn, RequestDisposition::NotSent),
        RetryDecision::NotSent
    );
}
