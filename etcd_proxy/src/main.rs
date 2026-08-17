#![allow(dead_code)]

mod pb;
mod proxy;

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tonic::transport::Server;

use crate::pb::etcdserverpb::kv_client::KvClient;
use crate::pb::etcdserverpb::lease_client::LeaseClient;
use crate::pb::etcdserverpb::maintenance_client::MaintenanceClient;
use crate::pb::etcdserverpb::watch_client::WatchClient;

const DEFAULT_MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const DEFAULT_LOG_BUFFER_SIZE: usize = 4 * 1024 * 1024;
const DEFAULT_LOG_CHANNEL_CAPACITY: usize = 4096;
const DEFAULT_LOG_FLUSH_INTERVAL_MS: u64 = 100;

#[derive(Debug, Parser)]
#[command(version, about = "Proxy etcd gRPC traffic and log requests")]
struct Args {
    /// Upstream etcd endpoint.
    #[arg(long, default_value = "http://[::1]:2379")]
    etcd_endpoint: String,

    /// Address on which the proxy listens.
    #[arg(long, default_value = "[::1]:50051")]
    listen_addr: SocketAddr,

    /// Maximum gRPC encoding and decoding message size in bytes.
    #[arg(
        long,
        default_value_t = NonZeroUsize::new(DEFAULT_MAX_MESSAGE_SIZE).unwrap()
    )]
    max_message_size: NonZeroUsize,

    /// JSON Lines request log path.
    #[arg(long, default_value = "etcd_proxy.jsonl")]
    log_file: PathBuf,

    /// Log buffer size in bytes.
    #[arg(
        long,
        default_value_t = NonZeroUsize::new(DEFAULT_LOG_BUFFER_SIZE).unwrap()
    )]
    log_buffer_size: NonZeroUsize,

    /// Maximum number of queued log entries.
    #[arg(
        long,
        default_value_t = NonZeroUsize::new(DEFAULT_LOG_CHANNEL_CAPACITY).unwrap()
    )]
    log_channel_capacity: NonZeroUsize,

    /// Maximum delay before buffered logs are flushed, in milliseconds.
    #[arg(
        long,
        default_value_t = NonZeroU64::new(DEFAULT_LOG_FLUSH_INTERVAL_MS).unwrap()
    )]
    log_flush_interval_ms: NonZeroU64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let etcd_addr = args.etcd_endpoint;
    let max_message_size = args.max_message_size.get();

    let kv_client = KvClient::connect(etcd_addr.clone())
        .await
        .expect("kv_client failed to connect to etcd")
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);
    let watch_client = WatchClient::connect(etcd_addr.clone())
        .await
        .expect("watch_client failed to connect to etcd")
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);
    let lease_client = LeaseClient::connect(etcd_addr.clone())
        .await
        .expect("lease_client failed to connect to etcd")
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);
    let maintenance_client = MaintenanceClient::connect(etcd_addr)
        .await
        .expect("maintenance_client failed to connect to etcd")
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

    let logger = Arc::new(proxy::Logger::new(
        &args.log_file,
        proxy::LoggerOptions {
            buffer_capacity_bytes: args.log_buffer_size.get(),
            channel_capacity: args.log_channel_capacity.get(),
            flush_interval: Duration::from_millis(args.log_flush_interval_ms.get()),
        },
    ));
    let proxy_service = proxy::ProxyService::new(
        kv_client,
        watch_client,
        lease_client,
        maintenance_client,
        Arc::clone(&logger),
    );

    let serve_result = Server::builder()
        .add_service(
            pb::etcdserverpb::kv_server::KvServer::new(proxy_service.clone())
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
        )
        .add_service(
            pb::etcdserverpb::watch_server::WatchServer::new(proxy_service.clone())
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
        )
        .add_service(
            pb::etcdserverpb::lease_server::LeaseServer::new(proxy_service.clone())
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
        )
        .add_service(
            pb::etcdserverpb::maintenance_server::MaintenanceServer::new(proxy_service.clone())
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size),
        )
        .serve(args.listen_addr)
        .await;

    logger.flush().await;
    serve_result.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_use_documented_defaults() {
        let args = Args::try_parse_from(["etcd_proxy"]).expect("defaults should parse");

        assert_eq!(args.etcd_endpoint, "http://[::1]:2379");
        assert_eq!(args.listen_addr, "[::1]:50051".parse().unwrap());
        assert_eq!(args.max_message_size.get(), DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(args.log_file, PathBuf::from("etcd_proxy.jsonl"));
        assert_eq!(args.log_buffer_size.get(), DEFAULT_LOG_BUFFER_SIZE);
        assert_eq!(
            args.log_channel_capacity.get(),
            DEFAULT_LOG_CHANNEL_CAPACITY
        );
        assert_eq!(
            args.log_flush_interval_ms.get(),
            DEFAULT_LOG_FLUSH_INTERVAL_MS
        );
    }

    #[test]
    fn args_parse_runtime_overrides() {
        let args = Args::try_parse_from([
            "etcd_proxy",
            "--etcd-endpoint",
            "http://127.0.0.1:12379",
            "--listen-addr",
            "127.0.0.1:15051",
            "--max-message-size",
            "1024",
            "--log-file",
            "/tmp/proxy.jsonl",
            "--log-buffer-size",
            "2048",
            "--log-channel-capacity",
            "32",
            "--log-flush-interval-ms",
            "25",
        ])
        .expect("runtime overrides should parse");

        assert_eq!(args.etcd_endpoint, "http://127.0.0.1:12379");
        assert_eq!(args.listen_addr, "127.0.0.1:15051".parse().unwrap());
        assert_eq!(args.max_message_size.get(), 1024);
        assert_eq!(args.log_file, PathBuf::from("/tmp/proxy.jsonl"));
        assert_eq!(args.log_buffer_size.get(), 2048);
        assert_eq!(args.log_channel_capacity.get(), 32);
        assert_eq!(args.log_flush_interval_ms.get(), 25);
    }

    #[test]
    fn args_reject_zero_sizes_and_interval() {
        for argument in [
            "--max-message-size",
            "--log-buffer-size",
            "--log-channel-capacity",
            "--log-flush-interval-ms",
        ] {
            assert!(Args::try_parse_from(["etcd_proxy", argument, "0"]).is_err());
        }
    }
}
