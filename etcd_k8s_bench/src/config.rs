use std::fs;
use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::metrics::WorkloadName;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid workload JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyntheticConfigOverrides {
    pub nodes: Option<u64>,
    pub pods: Option<u64>,
    pub namespaces: Option<u64>,
    pub pod_size_bytes: Option<u64>,
    pub seed: Option<u64>,
    pub update_rps: Option<u64>,
    pub duration_ms: Option<u64>,
    pub workers: Option<u64>,
    pub queue_capacity: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub operation_timeout_ms: Option<u64>,
}

/// Resolved synthetic run settings passed from the CLI to the scheduler.
///
/// The compiled plan owns every synthetic workload and timing value. The
/// transport endpoint, exporter address and message limit remain run-only
/// settings so `plan` stays read-only and backend independent.
#[derive(Clone, Debug)]
pub struct RunConfig {
    pub plan: crate::plan::CompiledPlan,
    pub endpoint: String,
    pub metrics_listen: SocketAddr,
    pub max_message_size: usize,
}

/// One long-lived Kubernetes-style prefix Watch stream.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WatchSpec {
    pub name: String,
    pub resource_prefix: String,
    #[serde(default)]
    pub start_revision: i64,
    #[serde(default)]
    pub prev_kv: bool,
    #[serde(default)]
    pub progress_notify: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticConfig {
    pub name: String,
    pub seed: u64,
    pub topology: SyntheticTopology,
    pub controllers: SyntheticControllers,
    pub value_size_bytes: SyntheticValueSizes,
    pub load: SyntheticLoad,
    pub periodic: SyntheticPeriodic,
    pub watch: SyntheticWatch,
    pub execution: SyntheticExecution,
    pub report: SyntheticReport,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticTopology {
    pub node_count: u64,
    pub pod_count: u64,
    pub namespace_count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticControllers {
    pub daemonset_count: u64,
    pub pods_per_deployment: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticValueSizes {
    pub pods: u64,
    pub nodes: u64,
    pub node_leases: u64,
    pub deployments: u64,
    pub replicasets: u64,
    pub daemonsets: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticLoad {
    pub duration_ms: u64,
    pub update_rps: u64,
    pub read_ratio: SyntheticRatio,
    pub update_weights: SyntheticUpdateWeights,
    pub read_weights: SyntheticReadWeights,
    pub read_shape_weights: SyntheticReadShapeWeights,
    pub list_limit: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticRatio {
    pub numerator: u64,
    pub denominator: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticUpdateWeights {
    pub pods: u64,
    pub deployments: u64,
    pub replicasets: u64,
    pub daemonsets: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticReadWeights {
    pub pods: u64,
    pub nodes: u64,
    pub deployments: u64,
    pub replicasets: u64,
    pub daemonsets: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticReadShapeWeights {
    pub point: u64,
    pub bounded_list: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticPeriodic {
    pub node_lease_interval_ms: u64,
    pub node_lease_jitter_percent: u8,
    pub node_status_interval_ms: u64,
    pub status_interval_ms: u64,
    pub compact_interval_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticWatch {
    pub subscriptions_per_root: u64,
    pub start_revision: i64,
    pub prev_kv: bool,
    pub progress_notify: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticExecution {
    pub workers: u64,
    pub queue_capacity: u64,
    pub request_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub preparation_timeout_ms: u64,
    pub drain_timeout_ms: u64,
    pub watch_teardown_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SyntheticReport {
    pub bucket_ms: u64,
    pub memory_limit_mib: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct SyntheticResourceCounts {
    pub pods: u64,
    pub nodes: u64,
    pub node_leases: u64,
    pub deployments: u64,
    pub replicasets: u64,
    pub daemonsets: u64,
}

impl SyntheticResourceCounts {
    pub fn checked_total(self) -> Result<u64, ConfigError> {
        self.pods
            .checked_add(self.nodes)
            .and_then(|total| total.checked_add(self.node_leases))
            .and_then(|total| total.checked_add(self.deployments))
            .and_then(|total| total.checked_add(self.replicasets))
            .and_then(|total| total.checked_add(self.daemonsets))
            .ok_or_else(|| ConfigError::Invalid("total synthetic resource count overflows".into()))
    }

    pub fn total(self) -> u64 {
        self.checked_total()
            .expect("SyntheticResourceCounts must be validated before total")
    }
}

fn checked_millis(value: u64, field: &str) -> Result<(), ConfigError> {
    value
        .checked_mul(1_000_000)
        .ok_or_else(|| ConfigError::Invalid(format!("{field} duration is not representable")))?;
    Ok(())
}

fn checked_memory_limit_bytes(memory_limit_mib: u64) -> Result<u64, ConfigError> {
    memory_limit_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| ConfigError::Invalid("report.memory_limit_mib overflows".into()))
}

impl SyntheticConfig {
    /// Parse a complete synthetic configuration without validating it.
    ///
    /// CLI overrides must be applied before validation so a bad value in a
    /// file can be repaired explicitly by the command line.
    pub fn parse_json(input: &str) -> Result<Self, ConfigError> {
        serde_json::from_str(input).map_err(ConfigError::Json)
    }

    /// Read and parse a complete synthetic configuration without validating
    /// it.  Callers apply all explicit overrides before `validate`.
    pub fn parse_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let input = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse_json(&input)
    }

    /// Parse the selected source and apply explicit CLI overrides.
    ///
    /// Validation is intentionally left to the final plan compiler so both
    /// `plan` and `run` validate the same effective configuration exactly
    /// once, after every override has been applied.
    pub fn parse_with_overrides(
        config_path: Option<&Path>,
        default_json: &str,
        overrides: &SyntheticConfigOverrides,
    ) -> Result<Self, ConfigError> {
        let mut config = match config_path {
            Some(path) => Self::parse_path(path)?,
            None => Self::parse_json(default_json)?,
        };
        config.apply_overrides(overrides);
        Ok(config)
    }

    pub fn apply_overrides(&mut self, overrides: &SyntheticConfigOverrides) {
        if let Some(nodes) = overrides.nodes {
            self.topology.node_count = nodes;
        }
        if let Some(pods) = overrides.pods {
            self.topology.pod_count = pods;
        }
        if let Some(namespaces) = overrides.namespaces {
            self.topology.namespace_count = namespaces;
        }
        if let Some(pod_size_bytes) = overrides.pod_size_bytes {
            self.value_size_bytes.pods = pod_size_bytes;
        }
        if let Some(seed) = overrides.seed {
            self.seed = seed;
        }
        if let Some(update_rps) = overrides.update_rps {
            self.load.update_rps = update_rps;
        }
        if let Some(duration_ms) = overrides.duration_ms {
            self.load.duration_ms = duration_ms;
        }
        if let Some(workers) = overrides.workers {
            self.execution.workers = workers;
        }
        if let Some(queue_capacity) = overrides.queue_capacity {
            self.execution.queue_capacity = queue_capacity;
        }
        if let Some(request_timeout_ms) = overrides.request_timeout_ms {
            self.execution.request_timeout_ms = request_timeout_ms;
        }
        if let Some(operation_timeout_ms) = overrides.operation_timeout_ms {
            self.execution.operation_timeout_ms = operation_timeout_ms;
        }
    }

    pub fn from_json(input: &str, max_message_size: usize) -> Result<Self, ConfigError> {
        let config = Self::parse_json(input)?;
        config.validate(max_message_size)?;
        Ok(config)
    }

    pub fn from_path(path: impl AsRef<Path>, max_message_size: usize) -> Result<Self, ConfigError> {
        let config = Self::parse_path(path)?;
        config.validate(max_message_size)?;
        Ok(config)
    }

    pub fn validate(&self, max_message_size: usize) -> Result<(), ConfigError> {
        WorkloadName::new(self.name.clone()).map_err(|error| {
            ConfigError::Invalid(format!("name is not a valid metric label: {error}"))
        })?;
        positive(self.topology.node_count, "topology.node_count")?;
        positive(self.topology.pod_count, "topology.pod_count")?;
        positive(self.topology.namespace_count, "topology.namespace_count")?;
        if self.topology.namespace_count > self.topology.pod_count {
            return Err(ConfigError::Invalid(
                "topology.namespace_count must not exceed pod_count".into(),
            ));
        }
        positive(
            self.controllers.daemonset_count,
            "controllers.daemonset_count",
        )?;
        positive(
            self.controllers.pods_per_deployment,
            "controllers.pods_per_deployment",
        )?;
        let counts = self.resource_counts()?;
        counts.checked_total()?;
        let max_message_size = u64::try_from(max_message_size)
            .map_err(|_| ConfigError::Invalid("max_message_size exceeds u64".into()))?;

        for (field, value) in [
            ("value_size_bytes.pods", self.value_size_bytes.pods),
            ("value_size_bytes.nodes", self.value_size_bytes.nodes),
            (
                "value_size_bytes.node_leases",
                self.value_size_bytes.node_leases,
            ),
            (
                "value_size_bytes.deployments",
                self.value_size_bytes.deployments,
            ),
            (
                "value_size_bytes.replicasets",
                self.value_size_bytes.replicasets,
            ),
            (
                "value_size_bytes.daemonsets",
                self.value_size_bytes.daemonsets,
            ),
        ] {
            positive(value, field)?;
            if value > max_message_size {
                return Err(ConfigError::Invalid(format!(
                    "{field} exceeds max_message_size ({max_message_size})"
                )));
            }
        }
        positive(self.load.duration_ms, "load.duration_ms")?;
        checked_millis(self.load.duration_ms, "load.duration_ms")?;
        positive(self.load.update_rps, "load.update_rps")?;
        positive(
            self.load.read_ratio.denominator,
            "load.read_ratio.denominator",
        )?;
        positive(self.load.list_limit, "load.list_limit")?;
        let update_weight_total = self.load.update_weights.checked_total()?;
        if update_weight_total == 0 {
            return Err(ConfigError::Invalid(
                "load.update_weights must have a positive total".into(),
            ));
        }
        let read_weight_total = self.load.read_weights.checked_total()?;
        if read_weight_total == 0 {
            return Err(ConfigError::Invalid(
                "load.read_weights must have a positive total".into(),
            ));
        }
        let read_shape_weight_total = self.load.read_shape_weights.checked_total()?;
        if read_shape_weight_total == 0 {
            return Err(ConfigError::Invalid(
                "load.read_shape_weights must have a positive total".into(),
            ));
        }

        positive(
            self.periodic.node_lease_interval_ms,
            "periodic.node_lease_interval_ms",
        )?;
        checked_millis(
            self.periodic.node_lease_interval_ms,
            "periodic.node_lease_interval_ms",
        )?;
        if self.periodic.node_lease_jitter_percent >= 100 {
            return Err(ConfigError::Invalid(
                "periodic.node_lease_jitter_percent must be in 0..99".into(),
            ));
        }
        positive(
            self.periodic.node_status_interval_ms,
            "periodic.node_status_interval_ms",
        )?;
        checked_millis(
            self.periodic.node_status_interval_ms,
            "periodic.node_status_interval_ms",
        )?;
        positive(
            self.periodic.status_interval_ms,
            "periodic.status_interval_ms",
        )?;
        checked_millis(
            self.periodic.status_interval_ms,
            "periodic.status_interval_ms",
        )?;
        positive(
            self.periodic.compact_interval_ms,
            "periodic.compact_interval_ms",
        )?;
        checked_millis(
            self.periodic.compact_interval_ms,
            "periodic.compact_interval_ms",
        )?;
        positive(
            self.watch.subscriptions_per_root,
            "watch.subscriptions_per_root",
        )?;
        if self.watch.start_revision < 0 {
            return Err(ConfigError::Invalid(
                "watch.start_revision must be non-negative".into(),
            ));
        }

        positive(self.execution.workers, "execution.workers")?;
        positive(self.execution.queue_capacity, "execution.queue_capacity")?;
        if self.execution.queue_capacity < self.execution.workers {
            return Err(ConfigError::Invalid(format!(
                "execution.queue_capacity ({}) must be at least workers ({})",
                self.execution.queue_capacity, self.execution.workers
            )));
        }
        positive(
            self.execution.request_timeout_ms,
            "execution.request_timeout_ms",
        )?;
        checked_millis(
            self.execution.request_timeout_ms,
            "execution.request_timeout_ms",
        )?;
        positive(
            self.execution.operation_timeout_ms,
            "execution.operation_timeout_ms",
        )?;
        checked_millis(
            self.execution.operation_timeout_ms,
            "execution.operation_timeout_ms",
        )?;
        if self.execution.request_timeout_ms > self.execution.operation_timeout_ms {
            return Err(ConfigError::Invalid(
                "execution.request_timeout_ms must not exceed operation_timeout_ms".into(),
            ));
        }
        positive(
            self.execution.preparation_timeout_ms,
            "execution.preparation_timeout_ms",
        )?;
        checked_millis(
            self.execution.preparation_timeout_ms,
            "execution.preparation_timeout_ms",
        )?;
        positive(
            self.execution.drain_timeout_ms,
            "execution.drain_timeout_ms",
        )?;
        checked_millis(
            self.execution.drain_timeout_ms,
            "execution.drain_timeout_ms",
        )?;
        positive(
            self.execution.watch_teardown_timeout_ms,
            "execution.watch_teardown_timeout_ms",
        )?;
        checked_millis(
            self.execution.watch_teardown_timeout_ms,
            "execution.watch_teardown_timeout_ms",
        )?;

        positive(self.report.bucket_ms, "report.bucket_ms")?;
        checked_millis(self.report.bucket_ms, "report.bucket_ms")?;
        if self.report.bucket_ms > self.load.duration_ms {
            return Err(ConfigError::Invalid(
                "report.bucket_ms must not exceed load.duration_ms".into(),
            ));
        }
        positive(self.report.memory_limit_mib, "report.memory_limit_mib")?;
        checked_memory_limit_bytes(self.report.memory_limit_mib)?;
        Ok(())
    }

    pub fn resource_counts(&self) -> Result<SyntheticResourceCounts, ConfigError> {
        positive(self.topology.node_count, "topology.node_count")?;
        positive(self.topology.pod_count, "topology.pod_count")?;
        positive(
            self.controllers.daemonset_count,
            "controllers.daemonset_count",
        )?;
        positive(
            self.controllers.pods_per_deployment,
            "controllers.pods_per_deployment",
        )?;
        let nodes = self.topology.node_count;
        let daemonsets = self.controllers.daemonset_count;
        let daemonset_pods = nodes
            .checked_mul(daemonsets)
            .ok_or_else(|| ConfigError::Invalid("DaemonSet Pod count overflows u64".into()))?;
        let remaining_pods = self
            .topology
            .pod_count
            .checked_sub(daemonset_pods)
            .ok_or_else(|| {
                ConfigError::Invalid(
                    "topology.pod_count must cover node_count × daemonset_count".into(),
                )
            })?;
        if remaining_pods == 0 {
            return Err(ConfigError::Invalid(
                "Deployment-managed Pod count must be positive".into(),
            ));
        }
        if remaining_pods % self.controllers.pods_per_deployment != 0 {
            return Err(ConfigError::Invalid(
                "remaining Pods must divide evenly by pods_per_deployment".into(),
            ));
        }
        let deployments = remaining_pods / self.controllers.pods_per_deployment;
        let counts = SyntheticResourceCounts {
            pods: self.topology.pod_count,
            nodes,
            node_leases: nodes,
            deployments,
            replicasets: deployments,
            daemonsets,
        };
        counts.checked_total()?;
        Ok(counts)
    }
}

impl SyntheticUpdateWeights {
    pub fn checked_total(&self) -> Result<u64, ConfigError> {
        checked_sum(
            [
                self.pods,
                self.deployments,
                self.replicasets,
                self.daemonsets,
            ],
            "load.update_weights",
        )
    }

    pub fn total(&self) -> u64 {
        self.checked_total()
            .expect("SyntheticUpdateWeights must be validated before total")
    }
}

impl SyntheticReadWeights {
    pub fn checked_total(&self) -> Result<u64, ConfigError> {
        checked_sum(
            [
                self.pods,
                self.nodes,
                self.deployments,
                self.replicasets,
                self.daemonsets,
            ],
            "load.read_weights",
        )
    }

    pub fn total(&self) -> u64 {
        self.checked_total()
            .expect("SyntheticReadWeights must be validated before total")
    }
}

impl SyntheticReadShapeWeights {
    pub fn checked_total(&self) -> Result<u64, ConfigError> {
        checked_sum([self.point, self.bounded_list], "load.read_shape_weights")
    }

    pub fn total(&self) -> u64 {
        self.checked_total()
            .expect("SyntheticReadShapeWeights must be validated before total")
    }
}

fn checked_sum(values: impl IntoIterator<Item = u64>, field: &str) -> Result<u64, ConfigError> {
    values.into_iter().try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| ConfigError::Invalid(format!("{field} total overflows")))
    })
}

fn positive(value: u64, field: &str) -> Result<(), ConfigError> {
    if value == 0 {
        return Err(ConfigError::Invalid(format!(
            "{field} must be greater than zero"
        )));
    }
    Ok(())
}
