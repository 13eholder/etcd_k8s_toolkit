use std::collections::HashSet;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::metrics::{PoolName, ResourceName, WorkloadName};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    K8sNoNodeLease,
    K8s,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub endpoint: String,
    pub workload: WorkloadSpec,
    pub pod_size: usize,
    pub nodes: u64,
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
pub const DEFAULT_NODE_COUNT: u64 = 2_000;
#[cfg(test)]
pub const DEFAULT_POD_COUNT: u64 = 40_000;

/// Node count used by `gen_profile` rolling-update workloads when `--nodes`
/// is not given explicitly.
pub const ROLLING_UPDATE_NODE_COUNT: u64 = 10_000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    pub name: String,
    pub pod_size_bytes: usize,
    pub topology: TopologySpec,
    pub relationships: ResourceRelationshipsSpec,
    pub nodes: NodeSpec,
    pub timeline: Vec<TimelineSegment>,
    pub resources: Vec<ResourceSpec>,
    pub ranges: Vec<RangeSpec>,
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
pub struct TopologySpec {
    pub node_count: u64,
    pub pod_count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "basis", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceCountSpec {
    Fixed { count: u64 },
    Pods { numerator: u64, denominator: u64 },
    Nodes { numerator: u64, denominator: u64 },
    Derived { relation: DerivedResourceKind },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DerivedResourceKind {
    DaemonSets,
    Services,
    EndpointSlices,
}
impl DerivedResourceKind {
    const fn resource_name(self) -> &'static str {
        match self {
            Self::DaemonSets => "daemonsets",
            Self::Services => "services",
            Self::EndpointSlices => "endpointslices",
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::DaemonSets => "daemon_sets",
            Self::Services => "services",
            Self::EndpointSlices => "endpoint_slices",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResourceRelationshipsSpec {
    pub daemon_set_groups: Vec<DaemonSetGroupSpec>,
    pub service_groups: Vec<ServiceGroupSpec>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DaemonSetGroupSpec {
    pub count: u64,
    pub eligible_nodes_per_daemonset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServiceGroupSpec {
    pub count: u64,
    pub backend_pods_per_service: u64,
    pub endpoint_groups_per_service: u64,
    pub endpoints_per_group: u64,
    pub max_endpoints_per_slice: u64,
    pub extra_slices_per_group: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct NodeSpec {
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
    pub range_rps: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResourceSpec {
    pub name: String,
    pub key_pattern: String,
    pub count: ResourceCountSpec,
    pub update_amplification: u64,
    pub value_size_bytes: usize,
    pub behavior: ResourceBehavior,
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
#[serde(deny_unknown_fields)]
pub struct EtcdLeasePoolSpec {
    pub name: String,
    pub object_ttl_s: u64,
    pub reuse_s: u64,
    pub max_objects: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceBehavior {
    CreateOnly,
    GuaranteedUpdate,
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
const MAX_ETCD_LEASE_SECONDS: u64 = i64::MAX as u64;
fn checked_duration(seconds: f64, field: &str) -> Result<Duration, ConfigError> {
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(ConfigError::Invalid(format!(
            "{field} must be greater than zero"
        )));
    }
    let duration = Duration::try_from_secs_f64(seconds)
        .map_err(|_| ConfigError::Invalid(format!("{field} duration is not representable")))?;
    if duration.is_zero() {
        return Err(ConfigError::Invalid(format!(
            "{field} duration is below nanosecond precision"
        )));
    }
    Ok(duration)
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
        self.topology.validate()?;
        self.nodes.validate()?;
        checked_duration(self.nodes.status_interval_s, "nodes.status_interval_s")?;
        checked_duration(self.nodes.lease_interval_s, "nodes.lease_interval_s")?;
        if self.timeline.is_empty() {
            return Err(ConfigError::Invalid("timeline must not be empty".into()));
        }
        let mut timeline_total = Duration::ZERO;
        for (index, segment) in self.timeline.iter().enumerate() {
            let duration =
                checked_duration(segment.duration_s, &format!("timeline[{index}].duration_s"))?;
            timeline_total = timeline_total.checked_add(duration).ok_or_else(|| {
                ConfigError::Invalid("timeline duration is not representable".into())
            })?;
            if !segment.range_rps.is_finite() || segment.range_rps < 0.0 {
                return Err(ConfigError::Invalid(format!(
                    "timeline[{index}].range_rps must be non-negative"
                )));
            }
        }

        let mut resource_names = HashSet::with_capacity(self.resources.len());
        for (index, resource) in self.resources.iter().enumerate() {
            ResourceName::new(resource.name.clone()).map_err(|error| {
                ConfigError::Invalid(format!(
                    "resources[{index}].name is not a valid metric label: {error}"
                ))
            })?;
            if !resource_names.insert(resource.name.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "resources[{index}] duplicate resource name {:?}",
                    resource.name
                )));
            }
            resource.validate(index, &self.topology, &self.relationships, max_message_size)?;
            let requires_native_lease = matches!(
                &resource.behavior,
                ResourceBehavior::Event | ResourceBehavior::MasterLease
            );
            match (&resource.lease_pool, requires_native_lease) {
                (None, true) => {
                    return Err(ConfigError::Invalid(format!(
                        "resources[{index}].behavior {:?} requires lease_pool",
                        resource.behavior
                    )));
                }
                (Some(_), false) => {
                    return Err(ConfigError::Invalid(format!(
                        "resources[{index}].lease_pool is only valid for Event or MasterLease"
                    )));
                }
                (Some(pool), true) => {
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
                (None, false) => {}
            }
        }
        let pods = self
            .resources
            .iter()
            .find(|resource| resource.name == "pods")
            .ok_or_else(|| ConfigError::Invalid("workload has no pods resource".into()))?;
        let pod_resource_count = self.resource_count(pods)?;
        if pod_resource_count != self.topology.pod_count {
            return Err(ConfigError::Invalid(format!(
                "pods resource count ({pod_resource_count}) must equal topology.pod_count ({})",
                self.topology.pod_count
            )));
        }
        self.relationships.validate(self)?;
        self.resources
            .iter()
            .try_fold((0u64, 0u64), |(objects, updates), resource| {
                let count = self.resource_count(resource)?;
                let update_target = count
                    .checked_mul(resource.update_amplification)
                    .ok_or_else(|| {
                        ConfigError::Invalid("total update target overflows u64".into())
                    })?;
                let objects = objects.checked_add(count).ok_or_else(|| {
                    ConfigError::Invalid("total object target overflows u64".into())
                })?;
                let updates = updates.checked_add(update_target).ok_or_else(|| {
                    ConfigError::Invalid("total update target overflows u64".into())
                })?;
                Ok::<_, ConfigError>((objects, updates))
            })?;
        for (index, range) in self.ranges.iter().enumerate() {
            range.validate(index)?;
        }
        for (index, watch) in self.watches.iter().enumerate() {
            WorkloadName::new(watch.name.clone()).map_err(|error| {
                ConfigError::Invalid(format!(
                    "watches[{index}].name is not a valid metric label: {error}"
                ))
            })?;
            if watch.resource_prefix.is_empty()
                || !watch.resource_prefix.starts_with("/registry/")
                || watch.resource_prefix.ends_with('/')
            {
                return Err(ConfigError::Invalid(format!(
                    "watches[{index}].resource_prefix must be a /registry resource root without a trailing slash"
                )));
            }
            if watch.start_revision < 0 {
                return Err(ConfigError::Invalid(format!(
                    "watches[{index}].start_revision must be non-negative"
                )));
            }
        }
        let mut pool_names = HashSet::with_capacity(self.etcd_lease_pools.len());
        for (index, pool) in self.etcd_lease_pools.iter().enumerate() {
            PoolName::new(pool.name.clone()).map_err(|error| {
                ConfigError::Invalid(format!(
                    "etcd_lease_pools[{index}].name is not a valid metric label: {error}"
                ))
            })?;
            if !pool_names.insert(pool.name.as_str()) {
                return Err(ConfigError::Invalid(format!(
                    "etcd_lease_pools[{index}] duplicate lease pool name {:?}",
                    pool.name
                )));
            }
            pool.validate(index)?;
        }
        checked_duration(self.compact_interval_s, "compact_interval_s")?;
        checked_duration(self.master_lease_interval_s, "master_lease_interval_s")?;
        Ok(())
    }

    pub fn resource_count(&self, resource: &ResourceSpec) -> Result<u64, ConfigError> {
        resource
            .count
            .resolve(&self.topology, &self.relationships, &resource.name)
    }

    /// Build a deterministic DaemonSet-update pressure profile from this
    /// base spec. The generated workload preserves the prior operation
    /// budget without replaying controller create/delete ordering: N Pod
    /// objects receive four successful updates each, 3N Event objects are
    /// created, and one DaemonSet receives two updates per wave. The timeline
    /// is a single update phase whose duration is `budget / txn_rps`.
    ///
    /// `base_range_total`/`base_app_total` must come from the scheduler's
    /// total helpers so the generated background read rate keeps the selected
    /// base profile's read/write ratio.
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
        // Deterministic keys rely on {id} expansion; a static pattern would
        // collapse the planned objects onto one key.
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
        spec.topology.node_count = nodes;
        spec.topology.pod_count = nodes;
        spec.relationships.service_groups.clear();
        spec.relationships.daemon_set_groups = if daemonsets.is_some() {
            vec![DaemonSetGroupSpec {
                count: 1,
                eligible_nodes_per_daemonset: nodes,
            }]
        } else {
            Vec::new()
        };
        spec.timeline = vec![TimelineSegment {
            duration_s: seconds,
            range_rps,
        }];

        let mut resources = vec![ResourceSpec {
            count: ResourceCountSpec::Pods {
                numerator: 1,
                denominator: 1,
            },
            update_amplification: 4,
            ..pods.clone()
        }];
        if let Some(events) = events {
            resources.push(ResourceSpec {
                count: ResourceCountSpec::Fixed { count: 3 * nodes },
                update_amplification: 0,
                ..events.clone()
            });
        }
        if let Some(daemonsets) = daemonsets {
            resources.push(ResourceSpec {
                count: ResourceCountSpec::Derived {
                    relation: DerivedResourceKind::DaemonSets,
                },
                update_amplification: 2 * waves,
                ..daemonsets.clone()
            });
        }
        // Masterlease traffic is driven by master_lease_interval_s and does
        // not count against the application budget; keep it unchanged.
        resources.extend(
            self.resources
                .iter()
                .filter(|r| r.behavior == ResourceBehavior::MasterLease)
                .cloned(),
        );
        spec.resources = resources;
        spec.watches.retain(|watch| {
            watch.resource_prefix == "/registry/minions"
                || (spec.nodes.node_lease_enabled && watch.resource_prefix == "/registry/leases")
                || spec.resources.iter().any(|resource| {
                    resource
                        .key_pattern
                        .strip_prefix(watch.resource_prefix.as_str())
                        .is_some_and(|suffix| suffix.starts_with('/'))
                })
        });

        // Adjust the count-only Range share for the generated profile's
        // timeline duration and clamp it against the exact deterministic
        // prefix capacity at the floored segment total. The runtime cursor
        // never schedules fewer ranges than that floor, so range_shape_plan
        // can never fail for a valid generated profile. The margin (one per
        // range shape, two per segment) covers rounding drift.
        let floor_range_total = (range_rps * seconds).floor() as u64;
        let duration_adjusted_count_only = if base_seconds > 0.0 {
            (self.range_count_only_count as f64 * seconds / base_seconds).round() as u64
        } else {
            self.range_count_only_count
        };
        let capacity = crate::scheduler::exact_prefix_capacity(&spec, floor_range_total);
        let drift_margin = spec.ranges.len() as u64 + 2;
        spec.range_count_only_count =
            duration_adjusted_count_only.min(capacity.saturating_sub(drift_margin));
        Ok(spec)
    }
}

impl TopologySpec {
    fn validate(&self) -> Result<(), ConfigError> {
        for (field, count) in [
            ("node_count", self.node_count),
            ("pod_count", self.pod_count),
        ] {
            if count == 0 {
                return Err(ConfigError::Invalid(format!(
                    "topology.{field} must be greater than zero"
                )));
            }
            if usize::try_from(count).is_err() {
                return Err(ConfigError::Invalid(format!(
                    "topology.{field} exceeds platform capacity"
                )));
            }
        }
        Ok(())
    }
}

impl ResourceRelationshipsSpec {
    pub fn daemon_set_objects(&self, topology: &TopologySpec) -> Result<u64, ConfigError> {
        self.daemon_set_groups
            .iter()
            .try_fold(0u64, |total, group| {
                validate_daemon_set_group(group, topology)?;
                total.checked_add(group.count).ok_or_else(|| {
                    ConfigError::Invalid("daemon set object count overflows u64".into())
                })
            })
    }

    pub fn daemon_set_pods(&self, topology: &TopologySpec) -> Result<u64, ConfigError> {
        self.daemon_set_groups
            .iter()
            .try_fold(0u64, |total, group| {
                validate_daemon_set_group(group, topology)?;
                let pods = group
                    .count
                    .checked_mul(group.eligible_nodes_per_daemonset)
                    .ok_or_else(|| {
                        ConfigError::Invalid("daemon set Pod count overflows u64".into())
                    })?;
                total.checked_add(pods).ok_or_else(|| {
                    ConfigError::Invalid("daemon set Pod count overflows u64".into())
                })
            })
    }

    pub fn service_objects(&self, topology: &TopologySpec) -> Result<u64, ConfigError> {
        self.service_groups.iter().try_fold(0u64, |total, group| {
            validate_service_group(group, topology)?;
            total
                .checked_add(group.count)
                .ok_or_else(|| ConfigError::Invalid("Service object count overflows u64".into()))
        })
    }

    pub fn endpoint_slice_objects(&self, topology: &TopologySpec) -> Result<u64, ConfigError> {
        self.service_groups.iter().try_fold(0u64, |total, group| {
            validate_service_group(group, topology)?;
            let ideal = group
                .endpoints_per_group
                .div_ceil(group.max_endpoints_per_slice);
            let slices_per_group =
                ideal
                    .checked_add(group.extra_slices_per_group)
                    .ok_or_else(|| {
                        ConfigError::Invalid("EndpointSlice group count overflows u64".into())
                    })?;
            let slices = group
                .count
                .checked_mul(group.endpoint_groups_per_service)
                .and_then(|value| value.checked_mul(slices_per_group))
                .ok_or_else(|| {
                    ConfigError::Invalid("EndpointSlice object count overflows u64".into())
                })?;
            total.checked_add(slices).ok_or_else(|| {
                ConfigError::Invalid("EndpointSlice object count overflows u64".into())
            })
        })
    }

    fn validate(&self, workload: &WorkloadSpec) -> Result<(), ConfigError> {
        let topology = &workload.topology;
        let daemon_set_pods = self.daemon_set_pods(topology)?;
        if daemon_set_pods > topology.pod_count {
            return Err(ConfigError::Invalid(format!(
                "daemon set Pods ({daemon_set_pods}) exceed topology.pod_count ({})",
                topology.pod_count
            )));
        }
        validate_relationship_resource(workload, "daemonsets", self.daemon_set_objects(topology)?)?;
        validate_relationship_resource(workload, "services", self.service_objects(topology)?)?;
        validate_relationship_resource(
            workload,
            "endpointslices",
            self.endpoint_slice_objects(topology)?,
        )
    }
}

fn validate_daemon_set_group(
    group: &DaemonSetGroupSpec,
    topology: &TopologySpec,
) -> Result<(), ConfigError> {
    if group.count == 0 {
        return Err(ConfigError::Invalid(
            "daemon_set_groups.count must be greater than zero".into(),
        ));
    }
    if group.eligible_nodes_per_daemonset == 0
        || group.eligible_nodes_per_daemonset > topology.node_count
    {
        return Err(ConfigError::Invalid(
            "daemon_set_groups.eligible_nodes_per_daemonset must be in 1..=topology.node_count"
                .into(),
        ));
    }
    Ok(())
}

fn validate_service_group(
    group: &ServiceGroupSpec,
    topology: &TopologySpec,
) -> Result<(), ConfigError> {
    if group.count == 0
        || group.backend_pods_per_service == 0
        || group.endpoint_groups_per_service == 0
        || group.endpoints_per_group == 0
    {
        return Err(ConfigError::Invalid(
            "service group counts and endpoint inputs must be greater than zero".into(),
        ));
    }
    if group.backend_pods_per_service > topology.pod_count {
        return Err(ConfigError::Invalid(
            "service_groups.backend_pods_per_service exceeds topology.pod_count".into(),
        ));
    }
    if !(1..=1_000).contains(&group.max_endpoints_per_slice) {
        return Err(ConfigError::Invalid(
            "service_groups.max_endpoints_per_slice must be in 1..=1000".into(),
        ));
    }
    Ok(())
}

fn validate_relationship_resource(
    workload: &WorkloadSpec,
    name: &str,
    expected: u64,
) -> Result<(), ConfigError> {
    let actual = workload
        .resources
        .iter()
        .find(|resource| resource.name == name)
        .map(|resource| workload.resource_count(resource))
        .transpose()?
        .unwrap_or(0);
    if actual != expected {
        return Err(ConfigError::Invalid(format!(
            "{name} resource count ({actual}) must equal relationship-derived count ({expected})"
        )));
    }
    Ok(())
}

impl ResourceCountSpec {
    pub fn resolve(
        &self,
        topology: &TopologySpec,
        relationships: &ResourceRelationshipsSpec,
        resource: &str,
    ) -> Result<u64, ConfigError> {
        match *self {
            Self::Fixed { count } => {
                if count == 0 {
                    return Err(ConfigError::Invalid(format!(
                        "resource {resource:?} count must be greater than zero"
                    )));
                }
                Ok(count)
            }
            Self::Pods {
                numerator,
                denominator,
            } => {
                scaled_resource_count(topology.pod_count, numerator, denominator, resource, "pods")
            }
            Self::Nodes {
                numerator,
                denominator,
            } => scaled_resource_count(
                topology.node_count,
                numerator,
                denominator,
                resource,
                "nodes",
            ),
            Self::Derived { relation } => match relation {
                DerivedResourceKind::DaemonSets => relationships.daemon_set_objects(topology),
                DerivedResourceKind::Services => relationships.service_objects(topology),
                DerivedResourceKind::EndpointSlices => {
                    relationships.endpoint_slice_objects(topology)
                }
            },
        }
    }
}

fn scaled_resource_count(
    base: u64,
    numerator: u64,
    denominator: u64,
    resource: &str,
    basis: &str,
) -> Result<u64, ConfigError> {
    if numerator == 0 {
        return Err(ConfigError::Invalid(format!(
            "resource {resource:?} {basis} numerator must be greater than zero"
        )));
    }
    if denominator == 0 {
        return Err(ConfigError::Invalid(format!(
            "resource {resource:?} {basis} denominator must be greater than zero"
        )));
    }
    let scaled = base.checked_mul(numerator).ok_or_else(|| {
        ConfigError::Invalid(format!("resource {resource:?} {basis} count overflows u64"))
    })?;
    if scaled % denominator != 0 {
        return Err(ConfigError::Invalid(format!(
            "resource {resource:?} {basis} count must resolve to an integer"
        )));
    }
    let count = scaled / denominator;
    if count == 0 {
        return Err(ConfigError::Invalid(format!(
            "resource {resource:?} count must be greater than zero"
        )));
    }
    Ok(count)
}

impl NodeSpec {
    fn validate(&self) -> Result<(), ConfigError> {
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
    fn validate(
        &self,
        index: usize,
        topology: &TopologySpec,
        relationships: &ResourceRelationshipsSpec,
        max_message_size: usize,
    ) -> Result<(), ConfigError> {
        if self.name.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].name must not be empty"
            )));
        }
        if let ResourceCountSpec::Derived { relation } = self.count
            && self.name != relation.resource_name()
        {
            return Err(ConfigError::Invalid(format!(
                "derived relation {} requires resource name {:?}",
                relation.as_str(),
                relation.resource_name()
            )));
        }
        if self.key_pattern.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}].key_pattern must not be empty"
            )));
        }
        let count = self.count.resolve(topology, relationships, &self.name)?;
        if usize::try_from(count).is_err() {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}] count exceeds platform capacity"
            )));
        }
        count
            .checked_mul(self.update_amplification)
            .ok_or_else(|| {
                ConfigError::Invalid(format!("resources[{index}] update target overflows u64"))
            })?;
        if self.update_amplification > 0
            && matches!(
                self.behavior,
                ResourceBehavior::CreateOnly | ResourceBehavior::MasterLease
            )
        {
            return Err(ConfigError::Invalid(format!(
                "resources[{index}] behavior {:?} cannot have update_amplification",
                self.behavior
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

impl EtcdLeasePoolSpec {
    fn validate(&self, index: usize) -> Result<(), ConfigError> {
        if self.name.trim().is_empty() {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].name must not be empty"
            )));
        }
        if self.object_ttl_s == 0 || self.object_ttl_s > MAX_ETCD_LEASE_SECONDS {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].object_ttl_s must be representable as an etcd TTL"
            )));
        }
        if self.reuse_s > MAX_ETCD_LEASE_SECONDS {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}].reuse_s must be representable as an etcd TTL"
            )));
        }
        let effective_reuse = self.reuse_s.min(self.object_ttl_s / 20);
        let grant_ttl = self
            .object_ttl_s
            .checked_add(effective_reuse)
            .filter(|ttl| *ttl <= MAX_ETCD_LEASE_SECONDS);
        if grant_ttl.is_none() {
            return Err(ConfigError::Invalid(format!(
                "etcd_lease_pools[{index}] grant TTL must be representable as an etcd TTL"
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
        WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap()
    }

    #[test]
    fn test_profiles_round_trip() {
        for input in [
            include_str!("../workloads/k8s.json"),
            include_str!("../workloads/k8s-no-node-lease.json"),
            include_str!("../workloads/rolling-update.json"),
        ] {
            let spec = WorkloadSpec::from_json(input).unwrap();
            let serialized = serde_json::to_string(&spec).unwrap();
            assert_eq!(WorkloadSpec::from_json(&serialized).unwrap(), spec);
        }
    }

    #[test]
    fn test_retry_policy_field_error() {
        let mut json = serde_json::to_value(embedded()).unwrap();
        let resource = json["resources"][0].as_object_mut().unwrap();
        let behavior = resource.remove("behavior").unwrap();
        resource.insert("retry_policy".into(), behavior);
        let error = WorkloadSpec::from_json(&json.to_string()).unwrap_err();
        assert!(error.to_string().contains("unknown field `retry_policy`"));
    }

    #[test]
    fn test_conditional_delete_error() {
        let mut json = serde_json::to_value(embedded()).unwrap();
        json["resources"][0]["behavior"] = serde_json::json!("conditional_delete");
        let error = WorkloadSpec::from_json(&json.to_string()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unknown variant `conditional_delete`")
        );
    }

    #[test]
    fn test_resource_counts_resolve() {
        let topology = TopologySpec {
            node_count: 2_000,
            pod_count: 40_000,
        };
        assert_eq!(
            ResourceCountSpec::Pods {
                numerator: 1,
                denominator: 1,
            }
            .resolve(&topology, &ResourceRelationshipsSpec::default(), "events",)
            .unwrap(),
            40_000
        );
        assert_eq!(
            ResourceCountSpec::Nodes {
                numerator: 1,
                denominator: 1,
            }
            .resolve(
                &topology,
                &ResourceRelationshipsSpec::default(),
                "node_leases",
            )
            .unwrap(),
            2_000
        );
        assert_eq!(
            ResourceCountSpec::Fixed { count: 20 }
                .resolve(
                    &topology,
                    &ResourceRelationshipsSpec::default(),
                    "deployments",
                )
                .unwrap(),
            20
        );
    }

    #[test]
    fn test_resource_count_bounds_error() {
        let topology = TopologySpec {
            node_count: 2_000,
            pod_count: 40_000,
        };
        let nonintegral = ResourceCountSpec::Pods {
            numerator: 1,
            denominator: 3,
        }
        .resolve(&topology, &ResourceRelationshipsSpec::default(), "events")
        .unwrap_err()
        .to_string();
        assert!(nonintegral.contains("must resolve to an integer"));

        let overflow = ResourceCountSpec::Pods {
            numerator: u64::MAX,
            denominator: 1,
        }
        .resolve(&topology, &ResourceRelationshipsSpec::default(), "events")
        .unwrap_err()
        .to_string();
        assert!(overflow.contains("overflows"));

        for count in [
            ResourceCountSpec::Fixed { count: 0 },
            ResourceCountSpec::Nodes {
                numerator: 0,
                denominator: 1,
            },
            ResourceCountSpec::Nodes {
                numerator: 1,
                denominator: 0,
            },
        ] {
            assert!(
                count
                    .resolve(&topology, &ResourceRelationshipsSpec::default(), "invalid",)
                    .is_err()
            );
        }
    }

    #[test]
    fn test_relationship_counts_resolve() {
        let topology = TopologySpec {
            node_count: 2_000,
            pod_count: 40_000,
        };
        let relationships = ResourceRelationshipsSpec {
            daemon_set_groups: vec![DaemonSetGroupSpec {
                count: 2,
                eligible_nodes_per_daemonset: 1_500,
            }],
            service_groups: vec![ServiceGroupSpec {
                count: 10,
                backend_pods_per_service: 200,
                endpoint_groups_per_service: 2,
                endpoints_per_group: 200,
                max_endpoints_per_slice: 100,
                extra_slices_per_group: 1,
            }],
        };
        assert_eq!(relationships.daemon_set_objects(&topology).unwrap(), 2);
        assert_eq!(relationships.daemon_set_pods(&topology).unwrap(), 3_000);
        assert_eq!(relationships.service_objects(&topology).unwrap(), 10);
        assert_eq!(relationships.endpoint_slice_objects(&topology).unwrap(), 60);
    }

    #[test]
    fn test_relationship_bounds_error() {
        let mut spec = embedded();
        spec.relationships.daemon_set_groups[0].eligible_nodes_per_daemonset =
            spec.topology.node_count + 1;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("eligible_nodes_per_daemonset"));

        let mut spec = embedded();
        spec.relationships.service_groups[0].max_endpoints_per_slice = 1_001;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("max_endpoints_per_slice"));
    }

    #[test]
    fn test_derived_resource_mismatch_error() {
        let mut spec = embedded();
        spec.resources
            .iter_mut()
            .find(|resource| resource.name == "services")
            .unwrap()
            .count = ResourceCountSpec::Derived {
            relation: DerivedResourceKind::EndpointSlices,
        };
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("derived relation endpoint_slices requires resource name"));
    }

    #[test]
    fn test_update_target_overflow_error() {
        let mut spec = embedded();
        spec.resources
            .iter_mut()
            .find(|resource| resource.name == "pods")
            .unwrap()
            .update_amplification = u64::MAX;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("update target overflows"));
    }

    #[test]
    fn test_keep_alive_field_error() {
        let mut workload: serde_json::Value =
            serde_json::from_str(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        workload["etcd_lease_pools"][0]["keep_alive"] = serde_json::Value::Bool(true);

        let error = WorkloadSpec::from_json(&workload.to_string()).unwrap_err();
        assert!(error.to_string().contains("keep_alive"));
    }

    #[test]
    fn test_config_read_error() {
        let path = std::env::temp_dir().join(format!(
            "etcd-k8s-bench-missing-config-{}",
            std::process::id()
        ));
        let expected_path = path.display().to_string();
        let error = WorkloadSpec::from_path(&path).unwrap_err();
        let ConfigError::Io { path, source } = error else {
            panic!("expected config I/O error");
        };
        assert_eq!(path, expected_path);
        assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
    }
    #[test]
    fn test_profile_workload_shape() {
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
        assert_eq!(spec.pod_size_bytes, 4096);
        assert_eq!(spec.topology.node_count, 2000);
        assert_eq!(spec.topology.pod_count, 40_000);
        let pods = spec
            .resources
            .iter()
            .find(|resource| resource.name == "pods")
            .unwrap();
        assert_eq!(spec.resource_count(pods).unwrap(), 40_000);

        let event = spec
            .resources
            .iter()
            .find(|resource| resource.name == "events")
            .unwrap();
        assert_eq!(spec.resource_count(event).unwrap(), 40_000);
        assert_eq!(event.update_amplification, 0);

        let pool = spec
            .etcd_lease_pools
            .iter()
            .find(|pool| pool.name == "events")
            .unwrap();
        assert_eq!(pool.object_ttl_s + pool.reuse_s, 3_660);
        assert_eq!(pool.object_ttl_s, 3_600);
        assert_eq!(pool.reuse_s, 60);
        let master = spec
            .etcd_lease_pools
            .iter()
            .find(|pool| pool.name == "masterleases")
            .unwrap();
        assert_eq!(master.object_ttl_s, 15);
        assert_eq!(master.reuse_s, 60);
    }
    #[test]
    fn test_lease_ttl_fraction_error() {
        let mut input: serde_json::Value =
            serde_json::from_str(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        input["etcd_lease_pools"][0]["object_ttl_s"] = serde_json::Value::from(0.5);
        assert!(WorkloadSpec::from_json(&input.to_string()).is_err());

        input["etcd_lease_pools"][0]["object_ttl_s"] = serde_json::Value::from(15.5);
        assert!(WorkloadSpec::from_json(&input.to_string()).is_err());
    }

    #[test]
    fn test_lease_ttl_upper_error() {
        let mut spec = embedded();
        spec.etcd_lease_pools[0].object_ttl_s = u64::MAX;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("representable"));
    }

    #[test]
    fn test_resource_lease_policy_error() {
        let mut spec = embedded();
        spec.resources
            .iter_mut()
            .find(|resource| resource.name == "events")
            .unwrap()
            .lease_pool = None;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("requires lease_pool"));

        let mut spec = embedded();
        spec.resources[0].lease_pool = Some("events".into());
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("lease_pool is only valid"));
    }
    #[test]
    fn test_lease_pool_duplicate_error() {
        let mut spec = embedded();
        let duplicate = spec.etcd_lease_pools[0].clone();
        spec.etcd_lease_pools.push(duplicate);
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("duplicate lease pool name"));
    }
    #[test]
    fn test_profiles_node_lease_modes() {
        let mock =
            WorkloadSpec::from_json(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        let real = WorkloadSpec::from_json(include_str!("../workloads/k8s.json")).unwrap();
        assert!(!mock.nodes.node_lease_enabled);
        assert!(real.nodes.node_lease_enabled);
    }
    #[test]
    fn test_watch_config_error() {
        let mut input: serde_json::Value =
            serde_json::from_str(include_str!("../workloads/k8s-no-node-lease.json")).unwrap();
        input["watches"][0]["key_prefix"] = input["watches"][0]["resource_prefix"].clone();
        let error = WorkloadSpec::from_json(&input.to_string()).unwrap_err();
        assert!(matches!(error, ConfigError::Json(_)));
        assert!(error.to_string().contains("unknown field `key_prefix`"));

        for prefix in ["", "pods", "/registry/pods/"] {
            let mut spec = embedded();
            spec.watches[0].resource_prefix = prefix.into();
            let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
            assert!(
                error.contains("watches[0].resource_prefix"),
                "unexpected error for {prefix:?}: {error}"
            );
        }
    }

    #[test]
    fn test_zero_nodes_error() {
        let mut spec = embedded();
        spec.topology.node_count = 0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("topology.node_count"));
    }

    #[test]
    fn test_zero_pod_size_error() {
        let mut spec = embedded();
        spec.pod_size_bytes = 0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("pod_size_bytes"));
    }

    #[test]
    fn test_node_jitter_upper_error() {
        let mut spec = embedded();
        spec.nodes.lease_jitter = 1.0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("lease_jitter"));
    }

    #[test]
    fn test_empty_timeline_error() {
        let mut spec = embedded();
        spec.timeline.clear();
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("timeline"));
    }
    #[test]
    fn test_config_metric_label_error() {
        let mut spec = embedded();
        spec.name = "invalid label".into();
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("name is not a valid metric label"));

        let mut spec = embedded();
        let index = spec
            .resources
            .iter()
            .position(|resource| resource.name == "events")
            .unwrap();
        spec.resources[index].name = "invalid label".into();
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains(&format!(
            "resources[{index}].name is not a valid metric label"
        )));

        let mut spec = embedded();
        spec.watches[0].name = "invalid label".into();
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("watches[0].name is not a valid metric label"));

        let mut spec = embedded();
        let mut pool = spec.etcd_lease_pools[0].clone();
        pool.name = "invalid label".into();
        let index = spec.etcd_lease_pools.len();
        spec.etcd_lease_pools.push(pool);
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains(&format!(
            "etcd_lease_pools[{index}].name is not a valid metric label"
        )));
    }

    #[test]
    fn test_range_fixtures_field_error() {
        let mut input: serde_json::Value =
            serde_json::from_str(include_str!("../workloads/k8s.json")).unwrap();
        input["range_fixtures"] = serde_json::json!([]);
        assert!(WorkloadSpec::from_json(&input.to_string()).is_err());
    }

    #[test]
    fn test_range_selector_all_match() {
        let spec = embedded();
        assert!(
            spec.ranges
                .iter()
                .all(|range| range.selector_keep_every == 1)
        );
    }

    #[test]
    fn test_range_selector_zero_error() {
        let mut spec = embedded();
        spec.ranges[0].selector_keep_every = 0;
        let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("selector_keep_every"));
    }

    #[test]
    fn test_duration_bounds() {
        let mut exact = embedded();
        exact.timeline[0].duration_s = 1e-9;
        exact.validate(64 * 1024 * 1024).unwrap();

        let mut subnano = embedded();
        subnano.timeline[0].duration_s = 0.1e-9;
        let error = subnano.validate(64 * 1024 * 1024).unwrap_err().to_string();
        assert!(error.contains("timeline[0].duration_s"));
        assert!(error.contains("nanosecond"));

        for field in [
            "nodes.status_interval_s",
            "nodes.lease_interval_s",
            "compact_interval_s",
            "master_lease_interval_s",
        ] {
            let mut spec = embedded();
            match field {
                "nodes.status_interval_s" => spec.nodes.status_interval_s = 0.1e-9,
                "nodes.lease_interval_s" => spec.nodes.lease_interval_s = 0.1e-9,
                "compact_interval_s" => spec.compact_interval_s = 0.1e-9,
                "master_lease_interval_s" => spec.master_lease_interval_s = 0.1e-9,
                _ => unreachable!(),
            }
            let error = spec.validate(64 * 1024 * 1024).unwrap_err().to_string();
            assert!(error.contains(field), "{error}");
            assert!(error.contains("nanosecond"), "{error}");
        }

        let mut too_large = embedded();
        too_large.timeline[0].duration_s = f64::MAX;
        let error = too_large
            .validate(64 * 1024 * 1024)
            .unwrap_err()
            .to_string();
        assert!(error.contains("timeline[0].duration_s"));
        assert!(error.contains("not representable"));

        let mut overflowing = embedded();
        overflowing.timeline = vec![
            TimelineSegment {
                duration_s: 1e19,
                range_rps: 0.0,
            },
            TimelineSegment {
                duration_s: 1e19,
                range_rps: 0.0,
            },
        ];
        let error = overflowing
            .validate(64 * 1024 * 1024)
            .unwrap_err()
            .to_string();
        assert!(error.contains("timeline duration is not representable"));
    }

    fn rolling(base: &WorkloadSpec, nodes: u64, batch: u64) -> Result<WorkloadSpec, ConfigError> {
        let range = crate::scheduler::range_request_total(base, 1);
        let app = crate::scheduler::application_operation_total(base);
        base.rolling_update(nodes, batch, 500.0, "rolling-test", range, app)
    }

    #[test]
    fn test_profiles_watch_roots() {
        let expected: HashSet<_> = [
            "/registry/pods",
            "/registry/events",
            "/registry/replicasets",
            "/registry/deployments",
            "/registry/leases",
            "/registry/daemonsets",
            "/registry/services",
            "/registry/endpointslices",
            "/registry/minions",
        ]
        .into_iter()
        .collect();
        for input in [
            include_str!("../workloads/k8s.json"),
            include_str!("../workloads/k8s-no-node-lease.json"),
        ] {
            let spec = WorkloadSpec::from_json(input).unwrap();
            let roots: HashSet<_> = spec
                .watches
                .iter()
                .map(|watch| watch.resource_prefix.as_str())
                .collect();
            assert_eq!(roots, expected);
            assert_eq!(spec.watches.len(), expected.len());
            assert!(spec.watches.iter().all(|watch| {
                watch.start_revision == 0 && watch.prev_kv && watch.progress_notify
            }));
        }
    }

    #[test]
    fn test_rolling_watch_options() {
        let mut base = embedded();
        let duplicate = WatchSpec {
            name: "pods-secondary".into(),
            start_revision: 17,
            prev_kv: false,
            progress_notify: false,
            ..base.watches[0].clone()
        };
        base.watches.insert(1, duplicate.clone());
        base.watches.push(WatchSpec {
            name: "prefix-collision".into(),
            resource_prefix: "/registry/pod".into(),
            ..duplicate.clone()
        });
        base.watches.push(WatchSpec {
            name: "explicit-masterleases".into(),
            resource_prefix: "/registry/masterleases".into(),
            ..duplicate
        });
        let expected: Vec<_> = base
            .watches
            .iter()
            .filter(|watch| {
                matches!(
                    watch.resource_prefix.as_str(),
                    "/registry/pods"
                        | "/registry/events"
                        | "/registry/daemonsets"
                        | "/registry/minions"
                        | "/registry/masterleases"
                )
            })
            .cloned()
            .collect();
        let spec = rolling(&base, 50, 7).unwrap();
        assert_eq!(spec.watches, expected);
        assert_eq!(spec.watches.len(), 6);
        spec.validate(64 * 1024 * 1024).unwrap();
    }

    #[test]
    fn test_rolling_custom_watch_root() {
        let mut base = embedded();
        base.resources
            .iter_mut()
            .find(|resource| resource.name == "pods")
            .unwrap()
            .key_pattern = "/registry/custom-pods/bench-{ns}/{id}".into();
        let custom = WatchSpec {
            name: "custom-pods".into(),
            resource_prefix: "/registry/custom-pods".into(),
            ..base.watches[0].clone()
        };
        base.watches.push(custom.clone());
        let spec = rolling(&base, 50, 7).unwrap();
        assert!(spec.watches.contains(&custom));
        assert!(
            !spec
                .watches
                .iter()
                .any(|watch| watch.resource_prefix == "/registry/pods")
        );
        spec.validate(64 * 1024 * 1024).unwrap();
    }

    #[test]
    fn test_rolling_node_lease_watch() {
        let mut base = embedded();
        for enabled in [false, true] {
            base.nodes.node_lease_enabled = enabled;
            let spec = rolling(&base, 50, 7).unwrap();
            assert_eq!(spec.watches.len(), if enabled { 5 } else { 4 });
            assert_eq!(
                spec.watches
                    .iter()
                    .any(|watch| watch.resource_prefix == "/registry/leases"),
                enabled
            );
            assert!(
                spec.watches
                    .iter()
                    .any(|watch| watch.resource_prefix == "/registry/minions")
            );
        }
    }

    #[test]
    fn test_rolling_empty_watches() {
        let mut base = embedded();
        base.watches.clear();
        assert!(rolling(&base, 50, 7).unwrap().watches.is_empty());
    }

    #[test]
    fn test_published_rolling_watches() {
        let base = WorkloadSpec::from_json(include_str!("../workloads/k8s.json")).unwrap();
        let generated = rolling(&base, 10_000, 500).unwrap();
        let published =
            WorkloadSpec::from_json(include_str!("../workloads/rolling-update.json")).unwrap();
        assert_eq!(published.watches, generated.watches);
    }

    #[test]
    fn test_rolling_budget() {
        let base = embedded();
        let spec = rolling(&base, 100, 10).unwrap();
        assert_eq!(spec.name, "rolling-test");
        assert_eq!(spec.topology.node_count, 100);
        // Single segment at the requested rate; duration = budget / txn_rps.
        // Budget 8N+1+2w = 800+1+20 = 821 at 500 rps -> 1.642s.
        assert_eq!(spec.timeline.len(), 1);
        assert_eq!(spec.timeline[0].duration_s, 1.642);
        // Range rate preserves the trace read/write ratio at 500 rps.
        let base_seconds: f64 = base.timeline.iter().map(|s| s.duration_s).sum();
        let base_range_rps = crate::scheduler::range_request_total(&base, 1) as f64 / base_seconds;
        let base_app_rps =
            crate::scheduler::application_operation_total(&base) as f64 / base_seconds;
        assert_eq!(
            spec.timeline[0].range_rps,
            base_range_rps * 500.0 / base_app_rps
        );
        let pods = spec.resources.iter().find(|r| r.name == "pods").unwrap();
        assert_eq!(pods.update_amplification, 4);
        assert_eq!(spec.resource_count(pods).unwrap(), 100);
        let events = spec.resources.iter().find(|r| r.name == "events").unwrap();
        assert_eq!(events.update_amplification, 0);
        assert_eq!(spec.resource_count(events).unwrap(), 300);
        let daemonsets = spec
            .resources
            .iter()
            .find(|r| r.name == "daemonsets")
            .unwrap();
        assert_eq!(daemonsets.update_amplification, 20);
        assert_eq!(spec.resource_count(daemonsets).unwrap(), 1);
        // Total conservation: 8N + 1 + 2w application operations.
        let total = crate::scheduler::application_operation_total(&spec);
        assert_eq!(total, 8 * 100 + 1 + 2 * 10);
        // Masterlease resources are preserved unchanged.
        assert!(
            spec.resources
                .iter()
                .any(|r| r.behavior == ResourceBehavior::MasterLease)
        );
        spec.validate(64 * 1024 * 1024).unwrap();
    }

    #[test]
    fn test_rolling_base_cadence() {
        let base = embedded();
        let spec = rolling(&base, 50, 7).unwrap();
        assert_eq!(spec.master_lease_interval_s, base.master_lease_interval_s);
        assert_eq!(spec.compact_interval_s, base.compact_interval_s);
        assert_eq!(spec.nodes.status_interval_s, base.nodes.status_interval_s);
        assert_eq!(spec.nodes.node_lease_enabled, base.nodes.node_lease_enabled);
        assert_eq!(spec.ranges, base.ranges);
        assert_eq!(spec.status_count, base.status_count);
        // Budget 8*50+1+2*ceil(50/7) = 417 at 500 rps -> 0.834s.
        assert_eq!(spec.timeline[0].duration_s, 0.834);
    }

    #[test]
    fn test_rolling_params_error() {
        let base = embedded();
        assert!(rolling(&base, 0, 10).is_err());
        assert!(rolling(&base, 100, 0).is_err());
        let range = crate::scheduler::range_request_total(&base, 1);
        let app = crate::scheduler::application_operation_total(&base);
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
    fn test_rolling_optional_budget() {
        // A base without events/daemonsets must not budget their operations.
        let mut base = embedded();
        base.resources
            .retain(|r| r.name == "pods" || r.behavior == ResourceBehavior::MasterLease);
        let spec = rolling(&base, 100, 10).unwrap();
        assert!(
            spec.resources
                .iter()
                .all(|r| r.name != "events" && r.name != "daemonsets")
        );
        // Budget 5N (deploy N + rolling 4N) = 500 at 500 rps -> 1s.
        let total = crate::scheduler::application_operation_total(&spec);
        assert_eq!(total, 5 * 100);
        assert_eq!(spec.timeline[0].duration_s, 1.0);
        spec.validate(64 * 1024 * 1024).unwrap();
    }

    #[test]
    fn test_rolling_static_key_error() {
        let mut base = embedded();
        base.resources
            .iter_mut()
            .find(|r| r.name == "pods")
            .unwrap()
            .key_pattern = "/registry/pods/static".to_string();
        assert!(rolling(&base, 100, 10).is_err());
    }

    #[test]
    fn test_rolling_count_only_capacity() {
        // The scaled count-only budget must remain positive while being
        // clamped into the exact scheduled prefix capacity.
        let mut base = embedded();
        base.timeline = vec![TimelineSegment {
            duration_s: 30.0,
            range_rps: 10.0,
        }];
        base.range_count_only_count = 1_000_000;
        let spec = base
            .rolling_update(100, 10, 100.0, "tight", 300, 30)
            .unwrap();
        let floor_range_total =
            (spec.timeline[0].range_rps * spec.timeline[0].duration_s).floor() as u64;
        let prefix_capacity = crate::scheduler::exact_prefix_capacity(&spec, floor_range_total);
        assert!(prefix_capacity > 0);
        assert!(spec.range_count_only_count > 0);
        assert!(spec.range_count_only_count <= prefix_capacity);
        crate::scheduler::range_shape_plan(&spec, prefix_capacity).unwrap();
    }
}
