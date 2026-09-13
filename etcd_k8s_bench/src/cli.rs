use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;

use crate::config::{ConfigError, RunConfig, SyntheticConfig, SyntheticConfigOverrides};
use crate::plan::{CompiledPlan, DEFAULT_SYNTHETIC_CONFIG};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:2379";

#[derive(Clone, Debug, Parser)]
#[command(
    name = "etcd_k8s_bench",
    about = "Apply synthetic Kubernetes-type etcd API pressure"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
    /// Compile the synthetic workload plan without contacting etcd.
    Plan(PlanArgs),
    /// Run the benchmark against a dedicated empty etcd endpoint.
    Run(RunArgs),
}

/// Synthetic configuration source and overrides shared by `plan` and `run`.
#[derive(Clone, Debug, clap::Args)]
pub struct BaseArgs {
    /// Complete synthetic workload JSON.  When omitted, use the embedded
    /// synthetic-k8s configuration.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Number of Nodes.
    #[arg(long, value_parser = parse_positive_u64)]
    pub nodes: Option<u64>,

    /// Total number of Pod keys.
    #[arg(long, value_parser = parse_positive_u64)]
    pub pods: Option<u64>,

    /// Number of namespaces.
    #[arg(long, value_parser = parse_positive_u64)]
    pub namespaces: Option<u64>,

    /// Pod value size in bytes.  Other resource value sizes are unchanged.
    #[arg(long, value_parser = parse_size)]
    pub pod_size: Option<usize>,

    /// Synthetic key and periodic jitter seed. Zero is a valid deterministic seed.
    #[arg(long, value_parser = parse_u64)]
    pub seed: Option<u64>,

    /// Logical application update rate.
    #[arg(long, value_parser = parse_positive_u64)]
    pub update_rps: Option<u64>,

    /// Measurement duration (for example, 60s or 2500ms).
    #[arg(long, value_parser = parse_duration)]
    pub duration: Option<Duration>,

    /// Number of gRPC workers.
    #[arg(long, value_parser = parse_positive_u64)]
    pub workers: Option<u64>,

    /// Total bounded work queue capacity.
    #[arg(long, value_parser = parse_positive_u64)]
    pub queue_capacity: Option<u64>,

    /// Maximum gRPC send and receive message size (for example, 64MiB).
    #[arg(long, default_value = "64MiB", value_parser = parse_size)]
    pub max_message_size: usize,

    /// Per-RPC timeout (for example, 5s or 250ms).
    #[arg(long, value_parser = parse_duration)]
    pub request_timeout: Option<Duration>,

    /// Logical operation timeout (for example, 10s or 3m).
    #[arg(long, value_parser = parse_duration)]
    pub operation_timeout: Option<Duration>,
}

#[derive(Clone, Debug, clap::Args)]
pub struct PlanArgs {
    #[command(flatten)]
    pub base: BaseArgs,
}

#[derive(Clone, Debug, clap::Args)]
pub struct RunArgs {
    #[command(flatten)]
    pub base: BaseArgs,

    /// etcd gRPC endpoint.
    #[arg(long, default_value = DEFAULT_ENDPOINT)]
    pub endpoint: String,

    /// Prometheus exporter listen address.
    #[arg(long, default_value = "127.0.0.1:9898")]
    pub metrics_listen: SocketAddr,
}

impl BaseArgs {
    fn synthetic_overrides(&self) -> Result<SyntheticConfigOverrides, ConfigError> {
        Ok(SyntheticConfigOverrides {
            nodes: self.nodes,
            pods: self.pods,
            namespaces: self.namespaces,
            pod_size_bytes: self
                .pod_size
                .map(|value| {
                    u64::try_from(value)
                        .map_err(|_| ConfigError::Invalid("pod_size does not fit in u64".into()))
                })
                .transpose()?,
            seed: self.seed,
            update_rps: self.update_rps,
            duration_ms: self
                .duration
                .map(|value| duration_to_millis(value, "--duration"))
                .transpose()?,
            workers: self.workers,
            queue_capacity: self.queue_capacity,
            request_timeout_ms: self
                .request_timeout
                .map(|value| duration_to_millis(value, "--request-timeout"))
                .transpose()?,
            operation_timeout_ms: self
                .operation_timeout
                .map(|value| duration_to_millis(value, "--operation-timeout"))
                .transpose()?,
        })
    }

    fn resolve_synthetic(&self) -> Result<SyntheticConfig, ConfigError> {
        let overrides = self.synthetic_overrides()?;
        SyntheticConfig::parse_with_overrides(
            self.config.as_deref(),
            DEFAULT_SYNTHETIC_CONFIG,
            &overrides,
        )
    }

    fn resolve_plan(&self) -> Result<CompiledPlan, ConfigError> {
        let config = self.resolve_synthetic()?;
        CompiledPlan::compile(config, self.max_message_size)
    }
}

impl PlanArgs {
    pub fn load(&self) -> Result<CompiledPlan, ConfigError> {
        self.base.resolve_plan()
    }

    #[cfg(test)]
    pub fn load_from<I, T>(args: I) -> Result<CompiledPlan, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli =
            Cli::try_parse_from(args).map_err(|error| ConfigError::Invalid(error.to_string()))?;
        match cli.command {
            Command::Plan(plan) => plan.load(),
            Command::Run(_) => Err(ConfigError::Invalid(
                "load_from expects the plan subcommand".into(),
            )),
        }
    }
}

impl RunArgs {
    /// Resolve the new synthetic run contract for the scheduler migration.
    pub fn resolve(&self) -> Result<RunConfig, ConfigError> {
        if self.endpoint.trim().is_empty() {
            return Err(ConfigError::Invalid("endpoint must not be empty".into()));
        }
        let plan = Arc::new(self.base.resolve_plan()?);
        Ok(RunConfig {
            plan,
            endpoint: self.endpoint.clone(),
            metrics_listen: self.metrics_listen,
            max_message_size: self.base.max_message_size,
            setup_started_at: Instant::now(),
        })
    }
}

fn duration_to_millis(value: Duration, field: &str) -> Result<u64, ConfigError> {
    if value.as_nanos() % 1_000_000 != 0 {
        return Err(ConfigError::Invalid(format!(
            "{field} must resolve to an integral number of milliseconds"
        )));
    }
    let millis = u64::try_from(value.as_millis())
        .map_err(|_| ConfigError::Invalid(format!("{field} is too large")))?;
    if millis == 0 {
        return Err(ConfigError::Invalid(format!(
            "{field} must be at least 1ms"
        )));
    }
    Ok(millis)
}

fn parse_u64(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{value:?} is not a valid unsigned integer"))
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = parse_u64(value)?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| "value must be greater than zero".to_string())
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(value) = value.strip_suffix("ms") {
        (value, 0.001)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1.0)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60.0)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 3600.0)
    } else {
        return Err("duration must use ms, s, m, or h suffix".into());
    };
    let seconds = number
        .parse::<f64>()
        .map_err(|_| format!("{value:?} is not a valid duration"))?
        * multiplier;
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err("duration must be finite and greater than zero".into());
    }
    Duration::try_from_secs_f64(seconds).map_err(|_| "duration is too large".to_string())
}

fn parse_size(value: &str) -> Result<usize, String> {
    let value = value.trim();
    let suffixes = [
        ("gib", 1024usize.pow(3)),
        ("gb", 1000usize.pow(3)),
        ("mib", 1024usize.pow(2)),
        ("mb", 1000usize.pow(2)),
        ("kib", 1024usize),
        ("kb", 1000usize),
        ("b", 1usize),
    ];
    let lower = value.to_ascii_lowercase();
    let (number, multiplier) = suffixes
        .iter()
        .find_map(|(suffix, multiplier)| {
            lower
                .strip_suffix(suffix)
                .map(|number| (number, *multiplier))
        })
        .ok_or_else(|| "size must use B, KiB, MiB, GiB, KB, MB, or GB suffix".to_string())?;
    let number = number
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("{value:?} is not a valid size"))?;
    number
        .checked_mul(multiplier)
        .filter(|size| *size > 0)
        .ok_or_else(|| "size must be greater than zero and fit in usize".to_string())
}
#[cfg(test)]
mod tests {
    use super::*;

    fn plan_config(args: &[&str]) -> Result<CompiledPlan, ConfigError> {
        let mut full = vec!["bench", "plan"];
        full.extend_from_slice(args);
        PlanArgs::load_from(full)
    }

    fn run_config(args: &[&str]) -> Result<RunConfig, ConfigError> {
        let mut full = vec!["bench", "run"];
        full.extend_from_slice(args);
        let cli =
            Cli::try_parse_from(full).map_err(|error| ConfigError::Invalid(error.to_string()))?;
        match cli.command {
            Command::Run(run) => run.resolve(),
            Command::Plan(_) => unreachable!(),
        }
    }

    fn temporary_config(tag: &str) -> (PathBuf, impl Drop) {
        struct Cleanup(PathBuf);

        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }

        let path = std::env::temp_dir().join(format!(
            "etcd-k8s-bench-cli-{}-{tag}.json",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        (path.clone(), Cleanup(path))
    }

    #[test]
    fn test_plan_defaults_load() {
        let plan = plan_config(&[]).unwrap();
        assert_eq!(plan.config.name, "synthetic-k8s");
        assert_eq!(plan.resource_counts.total(), 44_410);
        assert_eq!(plan.config.load.duration_ms, 600_000);
        assert_eq!(plan.config.load.update_rps, 10_000);
        assert_eq!(plan.config.execution.workers, 128);
        assert_eq!(plan.update_rounding_deficit, 0);
        assert_eq!(plan.read_rounding_deficit, 0);
    }

    #[test]
    fn test_cli_accepts_seed_zero() {
        let plan = plan_config(&["--seed", "0"]).unwrap();
        assert_eq!(plan.config.seed, 0);
    }

    #[test]
    fn test_cli_override() {
        let plan = plan_config(&[
            "--nodes",
            "20",
            "--pods",
            "1200",
            "--namespaces",
            "12",
            "--pod-size",
            "8192B",
            "--seed",
            "9",
            "--update-rps",
            "11",
            "--duration",
            "2s",
            "--workers",
            "4",
            "--queue-capacity",
            "8",
            "--request-timeout",
            "2s",
            "--operation-timeout",
            "3s",
        ])
        .unwrap();
        assert_eq!(plan.config.topology.node_count, 20);
        assert_eq!(plan.config.topology.pod_count, 1200);
        assert_eq!(plan.config.topology.namespace_count, 12);
        assert_eq!(plan.config.value_size_bytes.pods, 8192);
        assert_eq!(plan.config.value_size_bytes.nodes, 4096);
        assert_eq!(plan.config.seed, 9);
        assert_eq!(plan.config.load.update_rps, 11);
        assert_eq!(plan.config.load.duration_ms, 2_000);
        assert_eq!(plan.config.execution.workers, 4);
        assert_eq!(plan.config.execution.queue_capacity, 8);
        assert_eq!(plan.config.execution.request_timeout_ms, 2_000);
        assert_eq!(plan.config.execution.operation_timeout_ms, 3_000);
        assert_eq!(plan.resource_counts.pods, 1_200);
        assert_eq!(plan.resource_counts.deployments, 10);
    }

    #[test]
    fn test_cli_override_precedes_validation() {
        let mut config = SyntheticConfig::parse_json(DEFAULT_SYNTHETIC_CONFIG).unwrap();
        config.topology.node_count = 0;
        config.load.duration_ms = 0;
        let json = serde_json::to_string(&config).unwrap();
        let (path, _cleanup) = temporary_config("override-order");
        std::fs::write(&path, json).unwrap();

        let path_string = path.to_str().unwrap();
        let plan =
            plan_config(&["--config", path_string, "--nodes", "20", "--duration", "1s"]).unwrap();
        assert_eq!(plan.config.topology.node_count, 20);
        assert_eq!(plan.config.load.duration_ms, 1_000);
    }

    #[test]
    fn test_duration_requires_integral_milliseconds() {
        let error = plan_config(&["--duration", "1.5ms"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("integral number of milliseconds"));
    }

    #[test]
    fn test_run_and_plan_share_resolved_config() {
        let args = [
            "--nodes",
            "20",
            "--pods",
            "1200",
            "--namespaces",
            "12",
            "--pod-size",
            "8192B",
            "--seed",
            "9",
            "--update-rps",
            "11",
            "--duration",
            "2s",
            "--workers",
            "4",
            "--queue-capacity",
            "8",
            "--request-timeout",
            "2s",
            "--operation-timeout",
            "3s",
        ];
        let plan = plan_config(&args).unwrap();
        let mut run_args = vec![
            "--endpoint",
            "http://127.0.0.1:32379",
            "--metrics-listen",
            "127.0.0.1:39898",
        ];
        run_args.extend_from_slice(&args);
        let run = run_config(&run_args).unwrap();
        assert_eq!(run.plan.config, plan.config);
        assert_eq!(run.endpoint, "http://127.0.0.1:32379");
        assert_eq!(run.metrics_listen, "127.0.0.1:39898".parse().unwrap());
        assert_eq!(run.max_message_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_plan_writer_output_contract() {
        let plan = plan_config(&["--nodes", "20", "--pods", "1200"]).unwrap();
        let mut output = Vec::new();
        plan.write_json(&mut output).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(object.len(), 7);
        for field in [
            "schema_version",
            "resolved_config",
            "resource_counts",
            "logical_plan",
            "periodic_plan",
            "pacing_tick_ms",
            "estimated_measurement_memory_bytes",
        ] {
            assert!(object.contains_key(field), "missing plan field {field}");
        }
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["resolved_config"]["name"], "synthetic-k8s");
        assert_eq!(
            value["logical_plan"]["streams"].as_array().unwrap().len(),
            14
        );
        assert_eq!(value["logical_plan"]["update_rounding_deficit"], 0);
        assert_eq!(value["logical_plan"]["read_rounding_deficit"], 0);
        assert_eq!(value["pacing_tick_ms"], 1);
        assert!(
            value["estimated_measurement_memory_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    #[test]
    fn test_plan_rejects_run_only_flags() {
        assert!(Cli::try_parse_from(["bench", "plan", "--endpoint", DEFAULT_ENDPOINT]).is_err());
        assert!(
            Cli::try_parse_from(["bench", "plan", "--metrics-listen", "127.0.0.1:9898"]).is_err()
        );
    }
}
