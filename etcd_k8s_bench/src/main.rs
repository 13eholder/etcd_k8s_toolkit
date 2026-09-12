mod cli;
mod client;
mod config;
mod heartbeat;
mod measurement;
mod metrics;
mod pb;
mod plan;
mod report;
mod scheduler;
mod watch;
mod workload;

use std::fmt;
use std::io::Write;

use clap::Parser;
use cli::{Cli, Command, PlanArgs, RunArgs};
use client::ClientBundle;

#[derive(Debug)]
enum AppError {
    Config(String),
    Execution(String),
    Interrupted(String),
}
impl AppError {
    fn config(error: impl ToString) -> Self {
        Self::Config(error.to_string())
    }

    fn execution(error: impl ToString) -> Self {
        Self::Execution(error.to_string())
    }

    fn interrupted(error: impl ToString) -> Self {
        Self::Interrupted(error.to_string())
    }

    fn exit_code(&self) -> i32 {
        match self {
            Self::Config(_) => 2,
            Self::Execution(_) => 1,
            Self::Interrupted(_) => 130,
        }
    }
}
impl fmt::Display for AppError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => write!(formatter, "configuration: {error}"),
            Self::Execution(error) | Self::Interrupted(error) => formatter.write_str(error),
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("etcd_k8s_bench: {error}");
        std::process::exit(error.exit_code());
    }
}

async fn run() -> Result<(), AppError> {
    let cli = Cli::parse();
    match cli.command {
        Command::Plan(args) => run_plan(args),
        Command::Run(args) => run_new(args).await,
    }
}

fn run_plan(args: PlanArgs) -> Result<(), AppError> {
    let plan = args.load().map_err(AppError::config)?;
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    plan.write_json(&mut writer)
        .map_err(|error| AppError::execution(format!("encode plan: {error}")))?;
    writer
        .write_all(b"\n")
        .map_err(|error| AppError::execution(format!("write plan: {error}")))?;
    writer
        .flush()
        .map_err(|error| AppError::execution(format!("flush plan: {error}")))?;
    Ok(())
}

async fn run_new(args: RunArgs) -> Result<(), AppError> {
    let config = args.resolve().map_err(AppError::config)?;
    metrics::install_exporter(config.metrics_listen)
        .map_err(|error| AppError::execution(format!("metrics exporter: {error}")))?;
    let workload = metrics::WorkloadName::try_from(config.plan.config.name.as_str())
        .map_err(AppError::execution)?;
    metrics::record_info(env!("CARGO_PKG_VERSION"), &workload);
    metrics::set_pod_size(config.plan.config.value_size_bytes.pods);
    metrics::set_nodes(config.plan.config.topology.node_count);

    let request_timeout =
        std::time::Duration::from_millis(config.plan.config.execution.request_timeout_ms);
    let client = ClientBundle::connect(&config.endpoint, request_timeout, config.max_message_size)
        .await
        .map_err(|error| AppError::execution(format!("connect to {}: {error}", config.endpoint)))?;

    let result = scheduler::run(config, client).await;
    let failed = result.status == scheduler::RunStatus::Failed;
    let interrupted = result.status == scheduler::RunStatus::Interrupted;
    let json = serde_json::to_string(&result)
        .map_err(|error| AppError::execution(format!("encode run result: {error}")))?;
    println!("{json}");
    if failed {
        return Err(AppError::execution(
            result.error.as_deref().unwrap_or("synthetic run failed"),
        ));
    }
    if interrupted {
        return Err(AppError::interrupted("synthetic run interrupted"));
    }
    Ok(())
}
