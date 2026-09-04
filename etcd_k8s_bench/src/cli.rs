use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;

use crate::config::{ConfigError, Profile, ROLLING_UPDATE_NODE_COUNT, RuntimeConfig, WorkloadSpec};
#[cfg(test)]
use crate::config::{DEFAULT_NODE_COUNT, DEFAULT_POD_COUNT, DEFAULT_POD_SIZE_BYTES};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:2379";

#[derive(Clone, Debug, Parser)]
#[command(
    name = "etcd_k8s_bench",
    about = "Apply Kubernetes etcd API pressure from an exact workload model"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
    /// Run the benchmark against a dedicated empty etcd endpoint.
    Run(RunArgs),
    /// Generate a deterministic DaemonSet-update pressure profile, write it
    /// to workloads/<name>.json, and exit without touching etcd.
    #[command(name = "gen_profile")]
    GenProfile(GenProfileArgs),
}

/// Base workload selection and overrides shared by both subcommands.
#[derive(Clone, Debug, clap::Args)]
pub struct BaseArgs {
    /// Workload JSON; mutually exclusive with an explicitly selected built-in profile.
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Built-in workload profile. Defaults to k8s when omitted.
    #[arg(long, value_parser = parse_profile)]
    pub profile: Option<Profile>,

    /// Pod value size in bytes.
    #[arg(long, value_parser = parse_positive_usize)]
    pub pod_size: Option<usize>,

    /// Number of Nodes (and NodeLeases when the selected workload enables them).
    #[arg(long, value_parser = parse_positive_u64)]
    pub nodes: Option<u64>,

    /// Maximum gRPC send and receive message size (for example, 64MiB).
    #[arg(long, default_value = "64MiB", value_parser = parse_size)]
    pub max_message_size: usize,

    /// Deterministic key and heartbeat jitter seed.
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
}

#[derive(Clone, Debug, clap::Args)]
pub struct RunArgs {
    #[command(flatten)]
    pub base: BaseArgs,

    /// etcd gRPC endpoint.
    #[arg(long, default_value = DEFAULT_ENDPOINT)]
    pub endpoint: String,

    /// Exact total number of Pod keys.
    #[arg(long, value_parser = parse_positive_u64)]
    pub pods: Option<u64>,

    /// Scale update-phase duration and periodic task intervals; exact object and update counts are unchanged.
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

    /// Prometheus exporter listen address.
    #[arg(long, default_value = "127.0.0.1:9898")]
    pub metrics_listen: SocketAddr,
}

#[derive(Clone, Debug, clap::Args)]
pub struct GenProfileArgs {
    #[command(flatten)]
    pub base: BaseArgs,

    /// Pods recreated per rolling-update wave (sets the DaemonSet update
    /// count, 2 per wave).
    #[arg(long, default_value_t = 500, value_parser = parse_positive_u64)]
    pub batch_size: u64,

    /// Target application transactions per second; the run duration is
    /// derived as total operation budget divided by this rate.
    #[arg(long, default_value_t = 2000.0, value_parser = parse_positive_f64)]
    pub txn_rps: f64,

    /// Profile name; the workload is written to workloads/<name>.json.
    #[arg(
        long,
        default_value = "rolling-update",
        value_parser = parse_profile_name
    )]
    pub name: String,
}

impl BaseArgs {
    /// Load the base workload selected by --config/--profile (default k8s).
    fn base_workload(&self) -> Result<WorkloadSpec, ConfigError> {
        match (&self.config, self.profile) {
            (Some(_), Some(_)) => Err(ConfigError::Invalid(
                "--config and --profile are mutually exclusive".into(),
            )),
            (Some(path), None) => WorkloadSpec::from_path(path),
            (None, Some(Profile::K8sNoNodeLease)) => {
                WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json"))
            }
            (None, Some(Profile::K8s)) | (None, None) => {
                WorkloadSpec::from_json(include_str!("../workloads/k8s.json"))
            }
        }
    }

    /// Bake --pod-size/--nodes overrides into the workload.
    fn apply_overrides(&self, workload: &mut WorkloadSpec) {
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
            let previous = workload.topology.node_count;
            workload.topology.node_count = nodes;
            for group in &mut workload.relationships.daemon_set_groups {
                if group.eligible_nodes_per_daemonset == previous {
                    group.eligible_nodes_per_daemonset = nodes;
                }
            }
        }
    }
}

impl GenProfileArgs {
    /// Generate the derived workload profile and write it to
    /// `<CARGO_MANIFEST_DIR>/workloads/<name>.json`, returning the path.
    /// Does not connect to etcd or install the metrics exporter.
    pub fn generate(&self) -> Result<PathBuf, ConfigError> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("workloads");
        self.generate_to(&dir)
    }

    fn generate_to(&self, dir: &Path) -> Result<PathBuf, ConfigError> {
        let mut workload = self.base.base_workload()?;
        let base_range_total = crate::scheduler::range_request_total(&workload, self.base.seed);
        let base_app_total = crate::scheduler::application_operation_total(&workload);
        let nodes = self.base.nodes.unwrap_or(ROLLING_UPDATE_NODE_COUNT);
        workload = workload.rolling_update(
            nodes,
            self.batch_size,
            self.txn_rps,
            &self.name,
            base_range_total,
            base_app_total,
        )?;
        self.base.apply_overrides(&mut workload);
        workload.validate(self.base.max_message_size)?;
        write_profile(&workload, &self.name, dir)
    }
}

impl RunArgs {
    pub fn load(&self) -> Result<RuntimeConfig, ConfigError> {
        let mut workload = self.base.base_workload()?;
        self.base.apply_overrides(&mut workload);
        if let Some(pods) = self.pods {
            workload.topology.pod_count = pods;
        }
        if self.queue_capacity < self.workers {
            return Err(ConfigError::Invalid(format!(
                "queue_capacity ({}) must be at least workers ({})",
                self.queue_capacity, self.workers
            )));
        }
        workload.validate(self.base.max_message_size)?;

        if self.endpoint.trim().is_empty() {
            return Err(ConfigError::Invalid("endpoint must not be empty".into()));
        }
        if !self.time_scale.is_finite() || self.time_scale <= 0.0 {
            return Err(ConfigError::Invalid(
                "time_scale must be greater than zero".into(),
            ));
        }
        validate_scaled_durations(&workload, self.time_scale)?;
        if self.base.max_message_size == 0 {
            return Err(ConfigError::Invalid(
                "max_message_size must be greater than zero".into(),
            ));
        }

        Ok(RuntimeConfig {
            endpoint: self.endpoint.clone(),
            pod_size: workload.pod_size_bytes,
            nodes: workload.topology.node_count,
            workload,
            time_scale: self.time_scale,
            workers: self.workers,
            queue_capacity: self.queue_capacity,
            request_timeout: self.request_timeout,
            operation_timeout: self.operation_timeout,
            max_message_size: self.base.max_message_size,
            metrics_listen: self.metrics_listen,
            seed: self.base.seed,
        })
    }

    #[cfg(test)]
    pub fn load_from<I, T>(args: I) -> Result<RuntimeConfig, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli =
            Cli::try_parse_from(args).map_err(|error| ConfigError::Invalid(error.to_string()))?;
        match cli.command {
            Command::Run(run) => run.load(),
            Command::GenProfile(_) => Err(ConfigError::Invalid(
                "load_from expects the run subcommand".into(),
            )),
        }
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

fn parse_profile_name(value: &str) -> Result<String, String> {
    // Lowercase alphanumerics, '-' and '_' only: valid both as a metrics
    // label and as a file name (no path separators).
    if !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        Ok(value.to_string())
    } else {
        Err(format!(
            "profile name {value:?} must be non-empty lowercase [a-z0-9_-]"
        ))
    }
}

/// Write the derived workload as pretty JSON, refusing to overwrite an
/// existing profile (built-ins and earlier artifacts are protected).
/// `create_new` makes the no-overwrite guarantee atomic: concurrent
/// generations of the same name and pre-existing symlinks are both rejected.
fn write_profile(spec: &WorkloadSpec, name: &str, dir: &Path) -> Result<PathBuf, ConfigError> {
    std::fs::create_dir_all(dir).map_err(|source| ConfigError::Io {
        path: dir.display().to_string(),
        source,
    })?;
    let path = dir.join(format!("{name}.json"));
    let json = serde_json::to_string_pretty(spec).map_err(ConfigError::Json)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                ConfigError::Invalid(format!(
                    "profile {} already exists; choose another name or remove the file",
                    path.display()
                ))
            } else {
                ConfigError::Io {
                    path: path.display().to_string(),
                    source: error,
                }
            }
        })?;
    // On failure, remove the partial file so a retry with the same name works.
    if let Err(error) = std::io::Write::write_all(&mut file, format!("{json}\n").as_bytes()) {
        let _ = std::fs::remove_file(&path);
        return Err(ConfigError::Io {
            path: path.display().to_string(),
            source: error,
        });
    }
    Ok(path)
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{value:?} is not a valid positive integer"))?;
    (parsed > 0)
        .then_some(parsed)
        .ok_or_else(|| "value must be greater than zero".to_string())
}
fn parse_profile(value: &str) -> Result<Profile, String> {
    match value {
        "k8s" => Ok(Profile::K8s),
        "k8s-no-node-lease" => Ok(Profile::K8sNoNodeLease),
        _ => Err(format!(
            "unknown profile {value:?}; expected k8s or k8s-no-node-lease"
        )),
    }
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

    /// Parse args as the `run` subcommand and build its RuntimeConfig.
    fn run_config(args: &[&str]) -> Result<RuntimeConfig, ConfigError> {
        let mut full = vec!["bench", "run"];
        full.extend_from_slice(args);
        RunArgs::load_from(full)
    }

    /// Parse args as the `gen_profile` subcommand.
    fn gen_args(args: &[&str]) -> GenProfileArgs {
        let mut full = vec!["bench", "gen_profile"];
        full.extend_from_slice(args);
        match Cli::try_parse_from(&full)
            .expect("gen_profile arguments must parse")
            .command
        {
            Command::GenProfile(args) => args,
            Command::Run(_) => unreachable!(),
        }
    }

    #[test]
    fn defaults_load_k8s_with_exact_topology_and_node_leases() {
        let config = run_config(&[]).unwrap();
        assert_eq!(config.workload.name, "k8s");
        assert!(config.workload.nodes.node_lease_enabled);
        assert_eq!(config.pod_size, DEFAULT_POD_SIZE_BYTES);
        assert_eq!(config.nodes, DEFAULT_NODE_COUNT);
        assert_eq!(config.workload.topology.pod_count, DEFAULT_POD_COUNT);
        assert_eq!(config.request_timeout, Duration::from_secs(10));
    }
    #[test]
    fn explicit_no_node_lease_profile_disables_node_leases() {
        let config = run_config(&["--profile", "k8s-no-node-lease"]).unwrap();
        assert_eq!(config.workload.name, "k8s_no_node_lease");
        assert!(!config.workload.nodes.node_lease_enabled);
    }
    #[test]
    fn explicit_profile_and_custom_config_are_mutually_exclusive() {
        let error = run_config(&["--profile", "k8s", "--config", "custom.json"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn explicit_pod_size_pods_and_nodes_override_workload() {
        let config = run_config(&["--pod-size", "8192", "--pods", "800", "--nodes", "7"]).unwrap();
        assert_eq!(config.pod_size, 8192);
        assert_eq!(config.nodes, 7);
        assert_eq!(config.workload.pod_size_bytes, 8192);
        assert_eq!(config.workload.topology.node_count, 7);
        assert_eq!(config.workload.topology.pod_count, 800);
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
        let error = run_config(&["--nodes", "0"]).unwrap_err().to_string();
        assert!(error.contains("nodes"));
    }

    #[test]
    fn nodes_accepts_long_option_and_rejects_positional_compatibility() {
        let cli = Cli::try_parse_from(["bench", "run", "--nodes", "7"]).unwrap();
        match cli.command {
            Command::Run(run) => assert_eq!(run.base.nodes, Some(7)),
            Command::GenProfile(_) => unreachable!(),
        }
        assert!(Cli::try_parse_from(["bench", "run", "7"]).is_err());
    }
    #[test]
    fn queue_capacity_must_cover_workers() {
        let error = run_config(&["--workers", "4", "--queue-capacity", "3"])
            .unwrap_err()
            .to_string();
        assert!(error.contains("queue_capacity"));
        assert!(error.contains("workers"));
    }

    #[test]
    fn run_rejects_generation_flags() {
        // Derivation parameters belong to gen_profile; run has no such flags.
        assert!(Cli::try_parse_from(["bench", "run", "--duration", "300s"]).is_err());
        assert!(Cli::try_parse_from(["bench", "run", "--max-qps", "500"]).is_err());
        assert!(Cli::try_parse_from(["bench", "run", "--name", "x"]).is_err());
    }

    #[test]
    fn removed_rate_scale_and_legacy_profiles_are_rejected() {
        assert!(Cli::try_parse_from(["bench", "run", "--rate-scale", "2"]).is_err());
        assert!(Cli::try_parse_from(["bench", "run", "--profile", "mock-k8"]).is_err());
        assert!(Cli::try_parse_from(["bench", "run", "--profile", "real-k8"]).is_err());
    }

    #[test]
    fn time_scale_changes_duration_without_changing_exact_targets() {
        let base = run_config(&[]).unwrap();
        let scaled = run_config(&["--time-scale", "0.5"]).unwrap();
        assert_eq!(
            crate::scheduler::application_operation_total(&base.workload),
            crate::scheduler::application_operation_total(&scaled.workload)
        );
        assert_eq!(
            crate::scheduler::Scheduler::new(&scaled).duration(),
            crate::scheduler::Scheduler::new(&base).duration() / 2
        );
    }

    #[test]
    fn gen_profile_defaults_match_rolling_update() {
        let args = gen_args(&[]);
        assert_eq!(args.batch_size, 500);
        assert_eq!(args.txn_rps, 2000.0);
        assert_eq!(args.name, "rolling-update");
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "etcd-k8s-bench-gen-test-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn generate_profile_writes_loadable_workload() {
        let dir = temp_dir("write");
        let args = gen_args(&[
            "--nodes",
            "100",
            "--batch-size",
            "10",
            "--txn-rps",
            "500",
            "--name",
            "load-test",
        ]);
        let path = args.generate_to(&dir).unwrap();
        assert_eq!(path, dir.join("load-test.json"));
        let spec = WorkloadSpec::from_path(&path).unwrap();
        assert_eq!(spec.name, "load-test");
        assert_eq!(spec.topology.node_count, 100);
        assert_eq!(spec.timeline.len(), 1);
        // Budget 821 at 500 rps -> 1.642s.
        assert_eq!(spec.timeline[0].duration_s, 1.642);
        let pods = spec
            .resources
            .iter()
            .find(|resource| resource.name == "pods")
            .unwrap();
        assert_eq!(spec.resource_count(pods).unwrap(), 100);
        assert_eq!(pods.update_amplification, 4);
        let total = crate::scheduler::application_operation_total(&spec);
        assert_eq!(total, 8 * 100 + 1 + 2 * 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generate_profile_bakes_explicit_overrides() {
        let dir = temp_dir("overrides");
        let args = gen_args(&["--nodes", "7", "--pod-size", "8192", "--name", "small"]);
        let path = args.generate_to(&dir).unwrap();
        let spec = WorkloadSpec::from_path(&path).unwrap();
        assert_eq!(spec.topology.node_count, 7);
        assert_eq!(spec.pod_size_bytes, 8192);
        assert_eq!(
            spec.resources
                .iter()
                .find(|resource| resource.name == "pods")
                .unwrap()
                .value_size_bytes,
            8192
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gen_profile_rejects_explicit_pod_count() {
        assert!(
            Cli::try_parse_from(["bench", "gen_profile", "--pods", "100", "--name", "invalid"])
                .is_err()
        );
    }

    #[test]
    fn generate_profile_refuses_overwrite() {
        let dir = temp_dir("overwrite");
        let args = gen_args(&["--nodes", "10", "--name", "dup"]);
        args.generate_to(&dir).unwrap();
        let error = args.generate_to(&dir).unwrap_err().to_string();
        assert!(error.contains("already exists"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn gen_profile_rejects_run_only_flags() {
        for flag in [
            "--endpoint",
            "--workers",
            "--queue-capacity",
            "--request-timeout",
            "--operation-timeout",
            "--metrics-listen",
            "--rate-scale",
            "--time-scale",
        ] {
            let full = ["bench", "gen_profile", flag, "x"];
            assert!(
                Cli::try_parse_from(full).is_err(),
                "gen_profile must reject {flag}"
            );
        }
    }

    #[test]
    fn gen_profile_defaults_write_loadable_workload() {
        let dir = temp_dir("defaults");
        let args = gen_args(&[]);
        let path = args.generate_to(&dir).unwrap();
        let spec = WorkloadSpec::from_path(&path).unwrap();
        assert_eq!(spec.name, "rolling-update");
        assert_eq!(spec.topology.node_count, ROLLING_UPDATE_NODE_COUNT);
        assert_eq!(spec.timeline.len(), 1);
        // Budget 80041 at the default 2000 rps -> 40.0205s.
        assert_eq!(spec.timeline[0].duration_s, 40.0205);
        let app_total = crate::scheduler::application_operation_total(&spec);
        assert_eq!(app_total, 8 * 10_000 + 1 + 2 * 20);
        assert!(crate::scheduler::range_request_total(&spec, 1) > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generated_profile_loads_via_run_config() {
        let dir = temp_dir("roundtrip");
        let args = gen_args(&["--nodes", "10", "--batch-size", "5", "--name", "roundtrip"]);
        let path = args.generate_to(&dir).unwrap();
        let config = run_config(&["--config", path.to_str().unwrap()]).unwrap();
        assert_eq!(config.workload.name, "roundtrip");
        assert_eq!(config.nodes, 10);
        assert_eq!(
            crate::scheduler::application_operation_total(&config.workload),
            8 * 10 + 1 + 2 * 2
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generate_profile_rejects_unsafe_names() {
        for bad in ["../evil", "Foo", "a/b", ""] {
            let mut full = vec!["bench", "gen_profile", "--name"];
            full.push(bad);
            assert!(
                Cli::try_parse_from(&full).is_err(),
                "name {bad:?} must be rejected"
            );
        }
    }
}
