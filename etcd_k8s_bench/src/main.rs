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
    let report_config = config.clone();
    let mut ctrl_c: scheduler::CtrlCFuture = Box::pin(tokio::signal::ctrl_c());
    let exporter = match metrics::install_exporter(config.metrics_listen) {
        Ok(exporter) => exporter,
        Err(error) => {
            let report = report::RunReport::setup_failure(
                &report_config,
                report::RunStatus::Failed,
                "setup",
                format!("metrics exporter: {error}"),
            );
            write_report(&report)?;
            return Err(AppError::execution(format!("metrics exporter: {error}")));
        }
    };

    let request_timeout =
        std::time::Duration::from_millis(config.plan.config.execution.request_timeout_ms);
    let connect: Result<ClientBundle, String> = tokio::select! {
        result = ClientBundle::connect(&config.endpoint, request_timeout, config.max_message_size) => {
            result.map_err(|error| error.to_string())
        }
        signal = ctrl_c.as_mut() => {
            match signal {
                Ok(()) => Err("interrupted while connecting".into()),
                Err(error) => Err(format!("ctrl-c signal failed: {error}")),
            }
        }
    };
    let client = match connect {
        Ok(client) => client,
        Err(error) => {
            let interrupted = error == "interrupted while connecting";
            let status = if interrupted {
                report::RunStatus::Interrupted
            } else {
                report::RunStatus::Failed
            };
            let mut failure = format!("connect to {}: {error}", report_config.endpoint);
            if let Err(shutdown_error) = exporter.shutdown().await {
                failure.push_str(&format!("; metrics exporter shutdown: {shutdown_error}"));
            }
            let report =
                report::RunReport::setup_failure(&report_config, status, "setup", failure.clone());
            write_report(&report)?;
            return if interrupted {
                Err(AppError::interrupted("synthetic run interrupted"))
            } else {
                Err(AppError::execution(failure))
            };
        }
    };

    let mut report = scheduler::run_with_existing_ctrl_c(config, client, ctrl_c)
        .await
        .report;
    if let Err(error) = exporter.shutdown().await {
        report.record_exporter_failure(error);
    }
    let status = report.status();
    let report_error = report.experiment.first_error.clone();
    write_report(&report)?;
    if status == report::RunStatus::Failed {
        return Err(AppError::execution(
            report_error.as_deref().unwrap_or("synthetic run failed"),
        ));
    }
    if status == report::RunStatus::Interrupted {
        return Err(AppError::interrupted("synthetic run interrupted"));
    }
    Ok(())
}

fn write_report(report: &report::RunReport) -> Result<(), AppError> {
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    report
        .write_json(&mut writer)
        .map_err(|error| AppError::execution(format!("encode run report: {error}")))?;
    writer
        .write_all(b"\n")
        .map_err(|error| AppError::execution(format!("write run report: {error}")))?;
    writer
        .flush()
        .map_err(|error| AppError::execution(format!("flush run report: {error}")))?;
    Ok(())
}
