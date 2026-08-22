mod cli;
mod client;
mod config;
mod heartbeat;
mod lease;
mod metrics;
mod pb;
mod scheduler;
mod watch;
mod workload;

use clap::Parser;
use cli::{Cli, Command};
use client::ClientBundle;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("etcd_k8s_bench: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let args = match cli.command {
        Command::GenProfile(args) => {
            let path = args
                .generate()
                .map_err(|error| format!("generate profile: {error}"))?;
            eprintln!("derived profile written to {}", path.display());
            eprintln!(
                "run it with: etcd_k8s_bench run --config {}",
                path.display()
            );
            return Ok(());
        }
        Command::Run(args) => args,
    };
    let config = args
        .load()
        .map_err(|error| format!("configuration: {error}"))?;
    metrics::install_exporter(config.metrics_listen)
        .map_err(|error| format!("metrics exporter: {error}"))?;
    let workload = metrics::WorkloadName::try_from(config.workload.name.as_str())
        .map_err(|error| error.to_string())?;
    metrics::record_info(env!("CARGO_PKG_VERSION"), &workload);
    metrics::set_pod_size(config.pod_size as u64);
    metrics::set_nodes(config.nodes);

    let client = ClientBundle::connect(
        &config.endpoint,
        config.request_timeout,
        config.max_message_size,
    )
    .await
    .map_err(|error| format!("connect to {}: {error}", config.endpoint))?;
    let status_resource = metrics::ResourceName::new("status").unwrap();
    metrics::record_scheduled(metrics::Operation::Read, &status_resource);
    let status = match client.status().await {
        Ok(status) => {
            metrics::record_request(metrics::Rpc::Status, metrics::Outcome::Success);
            metrics::record_operation(
                metrics::Operation::Read,
                &status_resource,
                metrics::Outcome::Success,
            );
            status
        }
        Err(error) => {
            metrics::record_request(metrics::Rpc::Status, metrics::Outcome::GrpcError);
            metrics::record_operation(
                metrics::Operation::Read,
                &status_resource,
                metrics::Outcome::GrpcError,
            );
            return Err(format!("initial Status: {error}"));
        }
    };
    if let Some(header) = status.header.as_ref() {
        metrics::set_revision(header.revision);
    }

    // This is deliberately unconditional. It must happen before every Grant
    // or Txn and there is no CLI bypass for a non-empty endpoint.
    let preflight = client
        .preflight_count()
        .await
        .map_err(|error| format!("empty-database preflight Range: {error}"))?;
    metrics::record_request(metrics::Rpc::Range, metrics::Outcome::Success);
    if preflight.count != 0 {
        return Err(format!(
            "empty-database preflight rejected endpoint: {} keys already exist",
            preflight.count
        ));
    }

    let stats = scheduler::run(config, client).await?;
    eprintln!(
        "scheduled {} work items ({} dropped due to backpressure)",
        stats.scheduled, stats.dropped
    );
    Ok(())
}
