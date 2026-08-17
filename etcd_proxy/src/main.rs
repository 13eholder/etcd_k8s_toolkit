#![allow(dead_code)]

mod pb;
mod proxy;

use std::sync::Arc;

use tonic::transport::Server;

use crate::pb::etcdserverpb::kv_client::KvClient;
use crate::pb::etcdserverpb::lease_client::LeaseClient;
use crate::pb::etcdserverpb::maintenance_client::MaintenanceClient;
use crate::pb::etcdserverpb::watch_client::WatchClient;

#[tokio::main]
async fn main() {
    const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

    let etcd_addr = "http://[::1]:2379";
    let kv_client = KvClient::connect(etcd_addr)
        .await
        .expect("kv_client failed to connect to etcd")
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);
    let watch_client = WatchClient::connect(etcd_addr)
        .await
        .expect("watch_client failed to connect to etcd")
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);
    let lease_client = LeaseClient::connect(etcd_addr)
        .await
        .expect("lease_client failed to connect to etcd")
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);
    let maintenance_client = MaintenanceClient::connect(etcd_addr)
        .await
        .expect("maintenance_client failed to connect to etcd")
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let logger = Arc::new(proxy::Logger::new("etcd_proxy.jsonl"));
    let proxy_service = proxy::ProxyService::new(
        kv_client,
        watch_client,
        lease_client,
        maintenance_client,
        Arc::clone(&logger),
    );
    let proxy_addr = "[::1]:50051"
        .parse()
        .expect("failed to parse proxy address");

    let serve_result = Server::builder()
        .add_service(
            pb::etcdserverpb::kv_server::KvServer::new(proxy_service.clone())
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .add_service(
            pb::etcdserverpb::watch_server::WatchServer::new(proxy_service.clone())
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .add_service(
            pb::etcdserverpb::lease_server::LeaseServer::new(proxy_service.clone())
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .add_service(
            pb::etcdserverpb::maintenance_server::MaintenanceServer::new(proxy_service.clone())
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        )
        .serve(proxy_addr)
        .await;

    logger.flush().await;
    serve_result.unwrap();
}
