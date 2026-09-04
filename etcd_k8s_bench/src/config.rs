use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

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

/// Node count used by `gen_profile` rolling-update workloads when `--nodes`
/// is not given explicitly.
pub const ROLLING_UPDATE_NODE_COUNT: u64 = 10_000;

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
#[serde(deny_unknown_fields)]
pub struct EtcdLeasePoolSpec {
    pub name: String,
    pub object_ttl_s: f64,
    pub reuse_s: f64,
    pub max_objects: u64,
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

    /// Build a Kubernetes DaemonSet rolling-update workload from this base
    /// spec at a caller-chosen application transaction rate. The total
    /// budget is fixed by the shape — initial deploy (N Pods plus events
    /// and the DaemonSet) plus one full rolling pass (recreate every Pod
    /// under a fresh key with two status patches each, plus events and
    /// per-wave DaemonSet updates) — and the timeline is a single segment
    /// whose duration is `budget / txn_rps`. Quotas are run-wide; the
    /// deficit producer interleaves operations freely, so early operation
    /// is create-dominated (updates/deletes need present keys) and later
    /// operation is mixed churn. The Pod pool is 2N, so creates never
    /// exceed `unique_keys` and no slot recycling is needed. The Range rate
    /// follows the trace read/write ratio: `base_range_rps` scaled by
    /// `txn_rps / base_app_rps`. Periodic traffic (heartbeats, masterlease,
    /// compact, watches) keeps this base spec's cadence unchanged.
    ///
    /// `base_range_total`/`base_app_total` must come from the scheduler's
    /// total helpers so rates match the runtime cursor's rounding.
    pub fn rolling_update(
        &self,
        nodes: u64,
        batch_size: u64,
        txn_rps: f64,
        name: &str,
        base_range_total: u64,
        base_app_total: u64,
    ) -> Result<Self, ConfigError> {
        if nodes == 0 {
            return Err(ConfigError::Invalid(
                "nodes must be greater than zero".into(),
            ));
        }
        if batch_size == 0 {
            return Err(ConfigError::Invalid(
                "batch_size must be greater than zero".into(),
            ));
        }
        if nodes > u64::MAX / 8 {
            return Err(ConfigError::Invalid("nodes is unreasonably large".into()));
        }
        if !txn_rps.is_finite() || txn_rps <= 0.0 {
            return Err(ConfigError::Invalid(
                "txn_rps must be greater than zero".into(),
            ));
        }

        let find = |name: &str| self.resources.iter().find(|r| r.name == name);
        let pods = find("pods")
            .ok_or_else(|| ConfigError::Invalid("base workload has no pods resource".into()))?;
        let events = find("events");
        let daemonsets = find("daemonsets");
        // Fresh keys per rolling generation rely on {id} expansion; a static
        // pattern would collapse the 2N pool onto one key.
        for resource in [pods].into_iter().chain(events) {
            if !resource.key_pattern.contains("{id}") {
                return Err(ConfigError::Invalid(format!(
                    "base resource {:?} key_pattern must contain {{id}} for rolling-update key generation",
                    resource.name
                )));
            }
        }

        let waves = nodes.div_ceil(batch_size);
        // Budget: deploy N pods (+ N events) (+ 1 daemonset), then one
        // rolling pass of N creates + N deletes + 2N status patches
        // (+ 2N events) (+ 2 daemonset updates per wave).
        let has_events = u64::from(events.is_some());
        let has_daemonsets = u64::from(daemonsets.is_some());
        let budget = 5 * nodes + 3 * nodes * has_events + (1 + 2 * waves) * has_daemonsets;
        let seconds = budget as f64 / txn_rps;
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(ConfigError::Invalid(
                "derived duration is not finite; lower --txn-rps".into(),
            ));
        }
        let base_seconds: f64 = self.timeline.iter().map(|s| s.duration_s).sum();
        let (base_app_rps, base_range_rps) = if base_seconds > 0.0 {
            (
                base_app_total as f64 / base_seconds,
                base_range_total as f64 / base_seconds,
            )
        } else {
            (0.0, 0.0)
        };
        // Preserve the trace read/write ratio at the requested write rate.
        let range_rps = if base_app_rps > 0.0 {
            base_range_rps * txn_rps / base_app_rps
        } else {
            0.0
        };

        let mut spec = self.clone();
        spec.name = name.to_string();
        spec.nodes.count = nodes;
        spec.timeline = vec![TimelineSegment {
            duration_s: seconds,
            application_txn_rps: txn_rps,
            range_rps,
        }];

        let mut resources = vec![ResourceSpec {
            unique_keys: 2 * nodes,
            create_weight: (2 * nodes) as f64,
            update_weight: (2 * nodes) as f64,
            delete_weight: nodes as f64,
            ..pods.clone()
        }];
        if let Some(events) = events {
            resources.push(ResourceSpec {
                unique_keys: 3 * nodes,
                create_weight: (3 * nodes) as f64,
                update_weight: 0.0,
                delete_weight: 0.0,
                ..events.clone()
            });
        }
        if let Some(daemonsets) = daemonsets {
            resources.push(ResourceSpec {
                unique_keys: 1,
                create_weight: 1.0,
                update_weight: (2 * waves) as f64,
                delete_weight: 0.0,
                ..daemonsets.clone()
            });
        }
        // Masterlease traffic is driven by master_lease_interval_s and does
        // not count against the application budget; keep it unchanged.
        resources.extend(
            self.resources
                .iter()
                .filter(|r| r.retry_policy == RetryPolicy::MasterLease)
                .cloned(),
        );
        spec.resources = resources;

        // Scale the count-only Range share by the duration ratio and clamp it
        // against the exact deterministic prefix capacity at the floored
        // segment total; the runtime cursor never schedules fewer ranges
        // than that floor, so range_shape_plan can never fail at scale 1.
        // The margin (one per range shape, two per segment) covers rounding
        // drift when the run applies --rate-scale/--time-scale other than 1,
        // which rescales both sides of the check.
        let floor_range_total = (range_rps * seconds).floor() as u64;
        let scaled_count_only = if base_seconds > 0.0 {
            (self.range_count_only_count as f64 * seconds / base_seconds).round() as u64
        } else {
            self.range_count_only_count
        };
        let capacity = crate::scheduler::exact_prefix_capacity(&spec, floor_range_total);
        let drift_margin = spec.ranges.len() as u64 + 2;
        spec.range_count_only_count = scaled_count_only.min(capacity.saturating_sub(drift_margin));
        Ok(spec)
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
    fn removed_keep_alive_field_is_rejected() {
        let mut workload: serde_json::Value =
            serde_json::from_str(include_str!("../workloads/mock_k8.json")).unwrap();
        workload["etcd_lease_pools"][0]["keep_alive"] = serde_json::Value::Bool(true);

        let error = WorkloadSpec::from_json(&workload.to_string()).unwrap_err();
        assert!(error.to_string().contains("keep_alive"));
    }

    #[test]
    fn built_in_profiles_do_not_expose_keep_alive() {
        for json in [
            include_str!("../workloads/mock_k8.json"),
            include_str!("../workloads/real_k8.json"),
        ] {
            let workload: serde_json::Value = serde_json::from_str(json).unwrap();
            assert!(
                workload["etcd_lease_pools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|pool| pool.get("keep_alive").is_none())
            );
        }
    }
    #[test]
    fn config_io_error_preserves_path_and_source() {
        let error = ConfigError::Io {
            path: "workload.json".into(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
        };
        assert_eq!(
            error.to_string(),
            "cannot read config workload.json: missing"
        );
        assert!(std::error::Error::source(&error).is_some());
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

    fn rolling(base: &WorkloadSpec, nodes: u64, batch: u64) -> Result<WorkloadSpec, ConfigError> {
        let range = crate::scheduler::range_request_total(base, 1);
        let app = crate::scheduler::application_operation_total(base, 1.0, 1.0);
        base.rolling_update(nodes, batch, 500.0, "rolling-test", range, app)
    }

    #[test]
    fn rolling_update_shape_and_totals() {
        let base = embedded();
        let spec = rolling(&base, 100, 10).unwrap();
        assert_eq!(spec.name, "rolling-test");
        assert_eq!(spec.nodes.count, 100);
        // Single segment at the requested rate; duration = budget / txn_rps.
        // Budget 8N+1+2w = 800+1+20 = 821 at 500 rps -> 1.642s.
        assert_eq!(spec.timeline.len(), 1);
        assert_eq!(spec.timeline[0].duration_s, 1.642);
        assert_eq!(spec.timeline[0].application_txn_rps, 500.0);
        // Range rate preserves the trace read/write ratio at 500 rps.
        let base_seconds: f64 = base.timeline.iter().map(|s| s.duration_s).sum();
        let base_range_rps = crate::scheduler::range_request_total(&base, 1) as f64 / base_seconds;
        let base_app_rps =
            crate::scheduler::application_operation_total(&base, 1.0, 1.0) as f64 / base_seconds;
        assert_eq!(
            spec.timeline[0].range_rps,
            base_range_rps * 500.0 / base_app_rps
        );
        let pods = spec.resources.iter().find(|r| r.name == "pods").unwrap();
        assert_eq!(pods.unique_keys, 200);
        assert_eq!(pods.create_weight, 200.0);
        assert_eq!(pods.update_weight, 200.0);
        assert_eq!(pods.delete_weight, 100.0);
        let events = spec.resources.iter().find(|r| r.name == "events").unwrap();
        assert_eq!(events.unique_keys, 300);
        assert_eq!(events.create_weight, 300.0);
        assert_eq!(events.delete_weight, 0.0);
        let daemonsets = spec
            .resources
            .iter()
            .find(|r| r.name == "daemonsets")
            .unwrap();
        assert_eq!(daemonsets.unique_keys, 1);
        assert_eq!(daemonsets.update_weight, 20.0);
        // Total conservation: 8N + 1 + 2w application operations.
        let total: u64 = spec
            .resources
            .iter()
            .filter(|r| r.retry_policy != RetryPolicy::MasterLease)
            .map(|r| (r.create_weight + r.update_weight + r.delete_weight) as u64)
            .sum();
        assert_eq!(total, 8 * 100 + 1 + 2 * 10);
        // Masterlease resources are preserved unchanged.
        assert!(
            spec.resources
                .iter()
                .any(|r| r.retry_policy == RetryPolicy::MasterLease)
        );
        spec.validate(64 * 1024 * 1024).unwrap();
    }

    #[test]
    fn rolling_update_keeps_base_cadence() {
        let base = embedded();
        let spec = rolling(&base, 50, 7).unwrap();
        assert_eq!(spec.master_lease_interval_s, base.master_lease_interval_s);
        assert_eq!(spec.compact_interval_s, base.compact_interval_s);
        assert_eq!(spec.nodes.status_interval_s, base.nodes.status_interval_s);
        assert_eq!(spec.nodes.node_lease_enabled, base.nodes.node_lease_enabled);
        assert_eq!(spec.ranges, base.ranges);
        assert_eq!(spec.status_count, base.status_count);
        assert_eq!(spec.watches, base.watches);
        // Budget 8*50+1+2*ceil(50/7) = 417 at 500 rps -> 0.834s.
        assert_eq!(spec.timeline[0].duration_s, 0.834);
    }

    #[test]
    fn rolling_update_rejects_invalid_params() {
        let base = embedded();
        assert!(rolling(&base, 0, 10).is_err());
        assert!(rolling(&base, 100, 0).is_err());
        let range = crate::scheduler::range_request_total(&base, 1);
        let app = crate::scheduler::application_operation_total(&base, 1.0, 1.0);
        assert!(base.rolling_update(100, 10, 0.0, "x", range, app).is_err());
        assert!(
            base.rolling_update(100, 10, f64::NAN, "x", range, app)
                .is_err()
        );
        assert!(
            base.rolling_update(100, 10, 1e-310, "x", range, app)
                .is_err()
        );
    }

    #[test]
    fn rolling_update_scales_budget_to_present_resources() {
        // A base without events/daemonsets must not budget their operations.
        let mut base = embedded();
        base.resources
            .retain(|r| r.name == "pods" || r.retry_policy == RetryPolicy::MasterLease);
        let spec = rolling(&base, 100, 10).unwrap();
        assert!(
            spec.resources
                .iter()
                .all(|r| r.name != "events" && r.name != "daemonsets")
        );
        // Budget 5N (deploy N + rolling 4N) = 500 at 500 rps -> 1s.
        let total: u64 = spec
            .resources
            .iter()
            .filter(|r| r.retry_policy != RetryPolicy::MasterLease)
            .map(|r| (r.create_weight + r.update_weight + r.delete_weight) as u64)
            .sum();
        assert_eq!(total, 5 * 100);
        assert_eq!(spec.timeline[0].duration_s, 1.0);
        spec.validate(64 * 1024 * 1024).unwrap();
    }

    #[test]
    fn rolling_update_requires_id_placeholder_in_key_pattern() {
        let mut base = embedded();
        base.resources
            .iter_mut()
            .find(|r| r.name == "pods")
            .unwrap()
            .key_pattern = "/registry/pods/static".to_string();
        assert!(rolling(&base, 100, 10).is_err());
    }

    #[test]
    fn rolling_update_count_only_clamp_covers_scaling_drift() {
        // Tight count-only budget at the floored capacity: without the drift
        // margin a run with --time-scale != 1 could still fail
        // range_shape_plan. The clamp must land at zero here.
        let mut base = embedded();
        base.timeline = vec![TimelineSegment {
            duration_s: 30.0,
            application_txn_rps: 1.0,
            range_rps: 0.1,
        }];
        base.range_count_only_count = 3;
        let range_total = crate::scheduler::range_request_total(&base, 1);
        let app_total = crate::scheduler::application_operation_total(&base, 1.0, 1.0);
        let spec = base
            .rolling_update(1, 1, 100.0, "tight", range_total, app_total)
            .unwrap();
        assert_eq!(spec.range_count_only_count, 0);
    }
}
