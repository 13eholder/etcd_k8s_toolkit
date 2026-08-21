use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use std::{fmt, fs};

use serde::{Deserialize, Serialize};

use crate::metrics::{PoolName, ResourceName, WorkloadName};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    MockK8,
    RealK8,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub endpoint: String,
    pub profile: Option<Profile>,
    pub workload: WorkloadSpec,
    pub pod_size: usize,
    pub nodes: u64,
    pub rate_scale: f64,
    pub time_scale: f64,
    pub workers: usize,
    pub queue_capacity: usize,
    pub request_timeout: Duration,
    pub operation_timeout: Duration,
    pub max_message_size: usize,
    pub metrics_listen: SocketAddr,
    pub seed: u64,
}

#[cfg(test)]
pub const DEFAULT_POD_SIZE_BYTES: usize = 4096;
#[cfg(test)]
pub const DEFAULT_NODE_COUNT: u64 = 2003;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WorkloadSpec {
    pub name: String,
    pub pod_size_bytes: usize,
    pub nodes: NodeSpec,
    pub timeline: Vec<TimelineSegment>,
    pub resources: Vec<ResourceSpec>,
    pub ranges: Vec<RangeSpec>,
    #[serde(default)]
    pub range_fixtures: Vec<RangeFixtureSpec>,
    #[serde(default)]
    pub etcd_lease_pools: Vec<EtcdLeasePoolSpec>,
    #[serde(default)]
    pub status_count: u64,
    #[serde(default)]
    pub compact_interval_s: f64,
    pub master_lease_interval_s: f64,
    #[serde(default)]
    pub cas_conflicts: u64,
    #[serde(default)]
    pub range_count_only_count: u64,
    #[serde(default)]
    pub watches: Vec<WatchSpec>,
}

/// One long-lived Kubernetes-style prefix Watch stream.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct WatchSpec {
    pub name: String,
    pub key_prefix: String,
    #[serde(default)]
    pub start_revision: i64,
    #[serde(default)]
    pub prev_kv: bool,
    #[serde(default)]
    pub progress_notify: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct NodeSpec {
    pub count: u64,
    pub status_interval_s: f64,
    pub lease_interval_s: f64,
    pub lease_jitter: f64,
    pub status_retry_limit: u32,
    pub lease_retry_limit: u32,
    /// Whether node heartbeats also create and renew Coordination NodeLeases.
    #[serde(default = "default_node_lease_enabled")]
    pub node_lease_enabled: bool,
}

fn default_node_lease_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TimelineSegment {
    pub duration_s: f64,
    pub application_txn_rps: f64,
    pub range_rps: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResourceSpec {
    pub name: String,
    pub key_pattern: String,
    pub unique_keys: u64,
    pub create_weight: f64,
    pub update_weight: f64,
    pub delete_weight: f64,
    pub value_size_bytes: usize,
    pub retry_policy: RetryPolicy,
    #[serde(default)]
    pub lease_pool: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RangeSpec {
    pub key_pattern: String,
    pub kind: String,
    pub limit: u64,
    pub count_only: bool,
    pub weight: f64,
    /// Number of candidate KVs retained by the selector at a time. One means
    /// an explicit all-match policy; larger values deterministically discard
    /// every Nth candidate and trigger internal page growth.
    #[serde(default = "default_selector_keep_every")]
    pub selector_keep_every: u64,
}

fn default_selector_keep_every() -> u64 {
    1
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RangeFixtureSpec {
    pub name: String,
    pub key_pattern: String,
    pub count: u64,
    pub value_size_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EtcdLeasePoolSpec {
    pub name: String,
    pub object_ttl_s: f64,
    pub reuse_s: f64,
    pub max_objects: u64,
    #[serde(default)]
    pub keep_alive: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RetryPolicy {
    CreateOnly,
    GuaranteedUpdate,
    ConditionalDelete,
    Event,
    MasterLease,
}

#[derive(Debug)]
pub enum ConfigError {
    Io {
        path: String,
        source: std::io::Error,
    },
    Json(serde_json::Error),
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "cannot read config {path}: {source}"),
            Self::Json(source) => write!(f, "invalid workload JSON: {source}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Json(source) => Some(source),
            Self::Invalid(_) => None,
        }
    }
}

impl WorkloadSpec {
    pub fn from_json(input: &str) -> Result<Self, ConfigError> {
        let spec: Self = serde_json::from_str(input).map_err(ConfigError::Json)?;
        spec.validate(usize::MAX)?;
        Ok(spec)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let input = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_json(&input)
    }

    pub fn validate(&self, max_message_size: usize) -> Result<(), ConfigError> {
        WorkloadName::new(self.name.clone()).map_err(|error| {
            ConfigError::Invalid(format!("name is not a valid metric label: {error}"))
        })?;
        if self.pod_size_bytes == 0 {
            return Err(ConfigError::Invalid(
                "pod_size_bytes must be greater than zero".into(),
            ));
        }
        if self.pod_size_bytes > max_message_size {
            return Err(ConfigError::Invalid(format!(
                "pod_size_bytes ({}) exceeds max_message_size ({max_message_size})",
                self.pod_size_bytes
            )));
        }
        self.nodes.validate()?;
        if self.timeline.is_empty() {
            return Err(ConfigError::Invalid("timeline must not be empty".into()));
        }
        for (index, segment) in self.timeline.iter().enumerate() {
            if !segment.duration_s.is_finite() || segment.duration_s <= 0.0 {
                return Err(ConfigError::Invalid(format!(
                    "timeline[{index}].duration_s must be greater than zero"
                )));
            }
            for (field, value) in [
                ("application_txn_rps", segment.application_txn_rps),
                ("range_rps", segment.range_rps),
            ] {
                if !value.is_finite() || value < 0.0 {
                    return Err(ConfigError::Invalid(format!(
                        "timeline[{index}].{field} must be non-negative"
                    )));
                }
            }
        }

        for (index, resource) in self.resources.iter().enumerate() {
            ResourceName::new(resource.name.clone()).map_err(|error| {
                ConfigError::Invalid(format!(
                    "resources[{index}].name is not a valid metric label: {error}"
                ))
            })?;
            resource.validate(index, max_message_size)?;
            if let Some(pool) = &resource.lease_pool {
                if !self
                    .etcd_lease_pools
                    .iter()
                    .any(|candidate| candidate.name == *pool)
                {
                    return Err(ConfigError::Invalid(format!(
                        "resources[{index}].lease_pool references unknown pool {pool:?}"
                    )));
                }
            }
        }
        for (index, range) in self.ranges.iter().enumerate() {
            range.validate(index)?;
        }
        for (index, watch) in self.watches.iter().enumerate() {
            WorkloadName::new(watch.name.clone()).map_err(|error| {
                ConfigError::Invalid(format!(
                    "watches[{index}].name is not a valid metric label: {error}"
                ))
            })?;
            if watch.key_prefix.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "watches[{index}].key_prefix must not be empty"
                )));
            }
            if watch.start_revision < 0 {
                return Err(ConfigError::Invalid(format!(
                    "watches[{index}].start_revision must be non-negative"
                )));
            }
        }
        for (index, fixture) in self.range_fixtures.iter().enumerate() {
            fixture.validate(index, max_message_size)?;
        }
        for (index, pool) in self.etcd_lease_pools.iter().enumerate() {
            PoolName::new(pool.name.clone()).map_err(|error| {
                ConfigError::Invalid(format!(
                    "etcd_lease_pools[{index}].name is not a valid metric label: {error}"
                ))
            })?;
            pool.validate(index)?;
        }
        if self.compact_interval_s <= 0.0 || !self.compact_interval_s.is_finite() {
            return Err(ConfigError::Invalid(
                "compact_interval_s must be greater than zero".into(),
            ));
        }
        if !self.master_lease_interval_s.is_finite() || self.master_lease_interval_s <= 0.0 {
            return Err(ConfigError::Invalid(
                "master_lease_interval_s must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

impl NodeSpec {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.count == 0 {
            return Err(ConfigError::Invalid(
                "nodes.count must be greater than zero".into(),
            ));
        }
        for (field, value) in [
            ("status_interval_s", self.status_interval_s),
            ("lease_interval_s", self.lease_interval_s),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(ConfigError::Invalid(format!(
                    "nodes.{field} must be greater than zero"
                )));
            }
        }
        if !self.lease_jitter.is_finite() || !(0.0..1.0).contains(&self.lease_jitter) {
            return Err(ConfigError::Invalid(
                "nodes.lease_jitter must be in [0, 1)".into(),
            ));
        }
        if self.status_retry_limit == 0 {
            return Err(ConfigError::Invalid(
                "nodes.status_retry_limit must be greater than zero".into(),
            ));
        }
        if self.lease_retry_limit == 0 {
            return Err(ConfigError::Invalid(
                "nodes.lease_retry_limit must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

impl ResourceSpec {
    fn validate(&self, index: usize, max_message_size: usize) -> Result<(), ConfigError> {
        if self.name.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].name must not be empty"
            )));
        }
        if self.key_pattern.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].key_pattern must not be empty"
            )));
        }
        if self.unique_keys == 0 {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].unique_keys must be greater than zero"
            )));
        }
        for (field, value) in [
            ("create_weight", self.create_weight),
            ("update_weight", self.update_weight),
            ("delete_weight", self.delete_weight),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(ConfigError::Invalid(format!(
                    "resources[{index}].{field} must be non-negative"
                )));
            }
        }
        if self.create_weight + self.update_weight + self.delete_weight <= 0.0 {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}] operation weights must sum to a positive value"
            )));
        }
        if self.value_size_bytes == 0 {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].value_size_bytes must be greater than zero"
            )));
        }
        if self.value_size_bytes > max_message_size {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].value_size_bytes ({}) exceeds max_message_size {max_message_size}",
                self.value_size_bytes
            )));
        }
        Ok(())
    }
}

impl RangeSpec {
    fn validate(&self, index: usize) -> Result<(), ConfigError> {
        if self.key_pattern.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "ranges[{index}].key_pattern must not be empty"
            )));
        }
        if self.kind != "point" && self.kind != "prefix" {
            return Err(ConfigError::Invalid(format!(
                "ranges[{index}].kind must be point or prefix"
            )));
        }
        if !self.weight.is_finite() || self.weight <= 0.0 {
            return Err(ConfigError::Invalid(format!(
                "ranges[{index}].weight must be greater than zero"
            )));
        }
        if self.selector_keep_every == 0 {
            return Err(ConfigError::Invalid(format!(
                "ranges[{index}].selector_keep_every must be greater than zero"
            )));
        }
        Ok(())
    }
}

impl RangeFixtureSpec {
    fn validate(&self, index: usize, max_message_size: usize) -> Result<(), ConfigError> {
        ResourceName::new(self.name.clone()).map_err(|error| {
            ConfigError::Invalid(format!(
                "range_fixtures[{index}].name is not a valid metric label: {error}"
            ))
        })?;
        if self.key_pattern.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "range_fixtures[{index}].key_pattern must not be empty"
            )));
        }
        if self.count == 0 {
            return Err(ConfigError::Invalid(format!(
                "range_fixtures[{index}].count must be greater than zero"
            )));
        }
        if self.value_size_bytes == 0 || self.value_size_bytes > max_message_size {
            return Err(ConfigError::Invalid(format!(
                "range_fixtures[{index}].value_size_bytes must be in 1..={max_message_size}"
            )));
        }
        Ok(())
    }
}

impl EtcdLeasePoolSpec {
    fn validate(&self, index: usize) -> Result<(), ConfigError> {
        if self.name.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].name must not be empty"
            )));
        }
        if !self.object_ttl_s.is_finite() || self.object_ttl_s <= 0.0 {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].object_ttl_s must be greater than zero"
            )));
        }
        if !self.reuse_s.is_finite() || self.reuse_s < 0.0 {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].reuse_s must be non-negative"
            )));
        }
        if self.max_objects == 0 {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].max_objects must be greater than zero"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn embedded() -> WorkloadSpec {
        WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap()
    }
    #[test]
    fn embedded_mock_k8_preserves_resource_and_lease_shapes() {
        let spec = embedded();
        assert_eq!(
            spec.timeline
                .iter()
                .map(|segment| segment.duration_s)
                .sum::<f64>(),
            2_310.0
        );
        assert_eq!(spec.master_lease_interval_s, 10.0);
        assert_eq!(spec.cas_conflicts, 0);
        assert_eq!(spec.range_count_only_count, 220);
        assert_eq!(spec.range_fixtures.len(), 2);
        assert_eq!(spec.pod_size_bytes, 4096);
        assert_eq!(spec.nodes.count, 2003);

        let event = spec
            .resources
            .iter()
            .find(|resource| resource.name == "events")
            .unwrap();
        assert_eq!(event.unique_keys, 45_432);
        assert_eq!(event.create_weight, 45_432.0);
        assert_eq!(event.update_weight, 266.0);
        assert_eq!(event.delete_weight, 1.0);

        let pool = spec
            .etcd_lease_pools
            .iter()
            .find(|pool| pool.name == "events")
            .unwrap();
        assert_eq!(pool.object_ttl_s + pool.reuse_s, 3_660.0);
        assert_eq!(pool.object_ttl_s, 3_600.0);
        assert_eq!(pool.reuse_s, 60.0);
    }
    #[test]
    fn built_in_profiles_have_explicit_node_lease_modes() {
        let mock = WorkloadSpec::from_json(include_str!("../workloads/mock_k8.json")).unwrap();
        let real = WorkloadSpec::from_json(include_str!("../workloads/real_k8.json")).unwrap();
        assert!(!mock.nodes.node_lease_enabled);
        assert!(real.nodes.node_lease_enabled);
    }
    #[test]
    fn custom_node_spec_can_explicitly_disable_leases() {
        let node: NodeSpec = serde_json::from_str(
            r#"{
                "count": 1,
                "status_interval_s": 300,
                "lease_interval_s": 10,
                "lease_jitter": 0.04,
                "status_retry_limit": 5,
                "lease_retry_limit": 5,
                "node_lease_enabled": false
            }"#,
        )
        .unwrap();
        assert!(!node.node_lease_enabled);
    }

    #[test]
    fn rejects_zero_nodes() {
        let mut spec = embedded();
        spec.nodes.count = 0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("nodes.count"));
    }

    #[test]
    fn rejects_zero_pod_size() {
        let mut spec = embedded();
        spec.pod_size_bytes = 0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("pod_size_bytes"));
    }

    #[test]
    fn rejects_jitter_outside_half_open_interval() {
        let mut spec = embedded();
        spec.nodes.lease_jitter = 1.0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("lease_jitter"));
    }

    #[test]
    fn rejects_empty_timeline() {
        let mut spec = embedded();
        spec.timeline.clear();
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("timeline"));
    }
    #[test]
    fn rejects_invalid_metric_labels() {
        let mut spec = embedded();
        spec.name = "invalid label".into();
        assert!(spec.validate(64 * 1024 * 1024).is_err());
        spec.name = "mock_k8".into();
        spec.resources[0].name = "invalid label".into();
        assert!(spec.validate(64 * 1024 * 1024).is_err());
        spec.resources[0].name = "pods".into();
        spec.etcd_lease_pools[0].name = "invalid label".into();
        assert!(spec.validate(64 * 1024 * 1024).is_err());
        spec.etcd_lease_pools[0].name = "events".into();
        spec.range_fixtures[0].name = "invalid label".into();
        assert!(spec.validate(64 * 1024 * 1024).is_err());
    }

    #[test]
    fn range_selector_defaults_to_explicit_all_match() {
        let spec = embedded();
        assert!(
            spec.ranges
                .iter()
                .all(|range| range.selector_keep_every == 1)
        );
    }

    #[test]
    fn range_selector_rejects_zero() {
        let mut spec = embedded();
        spec.ranges[0].selector_keep_every = 0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("selector_keep_every"));
    }
}
