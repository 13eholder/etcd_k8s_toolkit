use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;

use crate::config::{ConfigError, RuntimeConfig, WorkloadSpec};
#[cfg(test)]
use crate::config::{DEFAULT_NODE_COUNT, DEFAULT_POD_SIZE_BYTES};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:2379";

#[derive(Clone, Debug, Parser)]
#[command(
    name = "etcd_k8s_bench",
    about = "Replay the Kubernetes density workload against etcd"
)]
pub struct Cli {
    /// etcd gRPC endpoint.
    #[arg(long, default_value = DEFAULT_ENDPOINT)]
    pub endpoint: String,

    /// Workload JSON; defaults to the embedded density workload.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Pod value size in bytes.
    #[arg(long, value_parser = parse_positive_usize)]
    pub pod_size: Option<usize>,

    /// Number of Nodes and NodeLeases.
    #[arg(long, value_parser = parse_positive_u64)]
    pub nodes: Option<u64>,

    /// Scale application transaction rate; heartbeat cadence is unchanged.
    #[arg(long, default_value_t = 1.0, value_parser = parse_positive_f64)]
    pub rate_scale: f64,

    /// Scale workload and periodic task time.
    #[arg(long, default_value_t = 1.0, value_parser = parse_positive_f64)]
    pub time_scale: f64,

    /// Number of gRPC workers.
    #[arg(long, default_value_t = 32, value_parser = parse_positive_usize)]
    pub workers: usize,

    /// Total bounded work queue capacity.
    #[arg(long, default_value_t = 4096, value_parser = parse_positive_usize)]
    pub queue_capacity: usize,

    /// Single RPC timeout (for example, 10s or 250ms).
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    pub request_timeout: Duration,

    /// Resource operation deadline (for example, 3m).
    #[arg(long, default_value = "3m", value_parser = parse_duration)]
    pub operation_timeout: Duration,

    /// Maximum gRPC send and receive message size (for example, 64MiB).
    #[arg(long, default_value = "64MiB", value_parser = parse_size)]
    pub max_message_size: usize,

    /// Prometheus exporter listen address.
    #[arg(long, default_value = "127.0.0.1:9898")]
    pub metrics_listen: SocketAddr,

    /// Deterministic key and heartbeat jitter seed.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
}

impl Cli {
    pub fn load(&self) -> Result<RuntimeConfig, ConfigError> {
        let mut workload = match &self.config {
            Some(path) => WorkloadSpec::from_path(path)?,
            None => WorkloadSpec::from_json(include_str!("../workloads/density.json"))?,
        };

        if let Some(pod_size) = self.pod_size {
            workload.pod_size_bytes = pod_size;
            if let Some(pods) = workload
                .resources
                .iter_mut()
                .find(|resource| resource.name == "pods")
            {
                pods.value_size_bytes = pod_size;
            }
        }
        if let Some(nodes) = self.nodes {
            workload.nodes.count = nodes;
        }
        if self.queue_capacity < self.workers {
            return Err(ConfigError::Invalid(format!(
                "queue_capacity ({}) must be at least workers ({})",
                self.queue_capacity, self.workers
            )));
        }
        workload.validate(self.max_message_size)?;

        if self.endpoint.trim().is_empty() {
            return Err(ConfigError::Invalid("endpoint must not be empty".into()));
        }
        if !self.rate_scale.is_finite() || self.rate_scale <= 0.0 {
            return Err(ConfigError::Invalid(
                "rate_scale must be greater than zero".into(),
            ));
        }
        if !self.time_scale.is_finite() || self.time_scale <= 0.0 {
            return Err(ConfigError::Invalid(
                "time_scale must be greater than zero".into(),
            ));
        }
        validate_scaled_durations(&workload, self.time_scale)?;
        if self.max_message_size == 0 {
            return Err(ConfigError::Invalid(
                "max_message_size must be greater than zero".into(),
            ));
        }

        Ok(RuntimeConfig {
            endpoint: self.endpoint.clone(),
            pod_size: workload.pod_size_bytes,
            nodes: workload.nodes.count,
            workload,
            rate_scale: self.rate_scale,
            time_scale: self.time_scale,
            workers: self.workers,
            queue_capacity: self.queue_capacity,
            request_timeout: self.request_timeout,
            operation_timeout: self.operation_timeout,
            max_message_size: self.max_message_size,
            metrics_listen: self.metrics_listen,
            seed: self.seed,
        })
    }

    #[cfg(test)]
    pub fn load_from<I, T>(args: I) -> Result<RuntimeConfig, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli =
            Self::try_parse_from(args).map_err(|error| ConfigError::Invalid(error.to_string()))?;
        cli.load()
    }
}
fn validate_scaled_duration(
    seconds: f64,
    scale: f64,
    field: &str,
) -> Result<Duration, ConfigError> {
    let scaled = seconds * scale;
    let max_seconds = Duration::MAX.as_secs() as f64;
    if !scaled.is_finite() || scaled < 0.0 || scaled >= max_seconds {
        return Err(ConfigError::Invalid(format!(
            "{field} scaled duration is not representable"
        )));
    }
    Ok(Duration::from_secs_f64(scaled))
}

fn validate_scaled_durations(workload: &WorkloadSpec, scale: f64) -> Result<(), ConfigError> {
    let mut timeline_total = Duration::ZERO;
    for (index, segment) in workload.timeline.iter().enumerate() {
        let duration = validate_scaled_duration(
            segment.duration_s,
            scale,
            &format!("timeline[{index}].duration_s"),
        )?;
        timeline_total = timeline_total.checked_add(duration).ok_or_else(|| {
            ConfigError::Invalid("scaled timeline duration is not representable".into())
        })?;
    }
    let _ = timeline_total;
    validate_scaled_duration(
        workload.nodes.status_interval_s,
        scale,
        "nodes.status_interval_s",
    )?;
    validate_scaled_duration(
        workload.nodes.lease_interval_s,
        scale,
        "nodes.lease_interval_s",
    )?;
    validate_scaled_duration(workload.compact_interval_s, scale, "compact_interval_s")?;
    Ok(())
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{value:?} is not a valid positive integer"))?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| "value must be greater than zero".to_string())
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{value:?} is not a valid positive integer"))?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| "value must be greater than zero".to_string())
}

fn parse_positive_f64(value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("{value:?} is not a valid number"))?;
    (parsed.is_finite() && parsed > 0.0)
        .then_some(parsed)
        .ok_or_else(|| "value must be finite and greater than zero".to_string())
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

    #[test]
    fn defaults_load_embedded_density() {
        let config = Cli::load_from(["bench"]).unwrap();
        assert_eq!(config.pod_size, DEFAULT_POD_SIZE_BYTES);
        assert_eq!(config.nodes, DEFAULT_NODE_COUNT);
        assert_eq!(config.rate_scale, 1.0);
        assert_eq!(config.request_timeout, Duration::from_secs(10));
    }

    #[test]
    fn explicit_pod_and_nodes_override_workload() {
        let config = Cli::load_from(["bench", "--pod-size", "8192", "--nodes", "7"]).unwrap();
        assert_eq!(config.pod_size, 8192);
        assert_eq!(config.nodes, 7);
        assert_eq!(config.workload.pod_size_bytes, 8192);
        assert_eq!(config.workload.nodes.count, 7);
        assert_eq!(
            config
                .workload
                .resources
                .iter()
                .find(|r| r.name == "pods")
                .unwrap()
                .value_size_bytes,
            8192
        );
    }

    #[test]
    fn invalid_cli_values_name_the_field() {
        let error = Cli::load_from(["bench", "--nodes", "0"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("nodes"));
    }
    #[test]
    fn queue_capacity_must_cover_workers() {
        let error = Cli::load_from(["bench", "--workers", "4", "--queue-capacity", "3"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("queue_capacity"));
        assert!(error.contains("workers"));
    }
}
