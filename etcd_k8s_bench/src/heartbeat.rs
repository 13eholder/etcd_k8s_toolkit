//! Deterministic Kubernetes Node and Coordination NodeLease heartbeat planning.
//!
//! A [`HeartbeatItem`] is deliberately an abstract KV item.  The integration
//! layer turns it into a CAS transaction; this module never invokes etcd's
//! LeaseKeepAlive RPC (Coordination NodeLeases are ordinary KV objects).

use std::time::Duration;

/// The kind of KV item emitted by the heartbeat planner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatItemKind {
    /// Create `/registry/minions/<node>`.
    NodeCreate,
    /// Create `/registry/leases/kube-node-lease/<node>`.
    NodeLeaseCreate,
    /// The initial Ready status update for a Node.
    NodeReady,
    /// A regular Coordination NodeLease renewal (a KV CAS update).
    NodeLeaseRenew,
    /// A periodic Node status update.
    NodeStatus,
}

/// One abstract heartbeat KV item.  `at` is relative to the beginning of the
/// workload, making plans reproducible and straightforward to test.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatItem {
    pub node: usize,
    pub kind: HeartbeatItemKind,
    pub key: String,
    pub at: Duration,
}

impl HeartbeatItem {
    fn new(node: usize, kind: HeartbeatItemKind, at: Duration) -> Self {
        let key = match kind {
            HeartbeatItemKind::NodeCreate
            | HeartbeatItemKind::NodeReady
            | HeartbeatItemKind::NodeStatus => format!("/registry/minions/node-{node:04}"),
            HeartbeatItemKind::NodeLeaseCreate | HeartbeatItemKind::NodeLeaseRenew => {
                format!("/registry/leases/kube-node-lease/node-{node:04}")
            }
        };
        Self {
            node,
            kind,
            key,
            at,
        }
    }
}

/// Planner settings.  The defaults are the Kubernetes heartbeat defaults used
/// by the built-in profiles.
#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatConfig {
    pub nodes: usize,
    pub lease_interval: Duration,
    pub lease_jitter: f64,
    pub status_interval: Duration,
    pub seed: u64,
    pub node_lease_enabled: bool,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            nodes: 2_003,
            lease_interval: Duration::from_secs(10),
            lease_jitter: 0.04,
            status_interval: Duration::from_secs(300),
            seed: 1,
            node_lease_enabled: true,
        }
    }
}

impl HeartbeatConfig {
    fn validate(&self) {
        assert!(self.nodes > 0, "heartbeat nodes must be greater than zero");
        assert!(
            !self.lease_interval.is_zero(),
            "lease interval must be non-zero"
        );
        assert!(
            !self.status_interval.is_zero(),
            "status interval must be non-zero"
        );
        assert!(
            self.lease_jitter.is_finite() && (0.0..1.0).contains(&self.lease_jitter),
            "lease jitter must be in [0, 1)"
        );
    }
}

/// Deterministic Node heartbeat planner.
#[derive(Clone, Debug)]
pub struct NodeHeartbeatPlanner {
    config: HeartbeatConfig,
}

impl NodeHeartbeatPlanner {
    pub fn new(config: HeartbeatConfig) -> Self {
        config.validate();
        Self { config }
    }

    /// Emit bootstrap items in node order. Each node is created and receives
    /// its initial Ready update; Coordination NodeLease creation is included
    /// only when explicitly enabled.
    pub fn bootstrap(&self) -> Vec<HeartbeatItem> {
        let per_node = if self.config.node_lease_enabled { 3 } else { 2 };
        let mut result = Vec::with_capacity(self.config.nodes * per_node);
        for node in 0..self.config.nodes {
            result.push(HeartbeatItem::new(
                node,
                HeartbeatItemKind::NodeCreate,
                Duration::ZERO,
            ));
            if self.config.node_lease_enabled {
                result.push(HeartbeatItem::new(
                    node,
                    HeartbeatItemKind::NodeLeaseCreate,
                    Duration::ZERO,
                ));
            }
            result.push(HeartbeatItem::new(
                node,
                HeartbeatItemKind::NodeReady,
                Duration::ZERO,
            ));
        }
        result
    }

    /// Return the first Node status time for `node` when it falls inside the
    /// scheduling window.
    pub fn initial_status_at(&self, node: usize, window: Duration) -> Option<Duration> {
        let at = self
            .config
            .status_interval
            .mul_f64((node + 1) as f64 / self.config.nodes as f64);
        (at <= window).then_some(at)
    }

    /// Return the next Node status time after `current` when it falls inside
    /// the scheduling window.
    pub fn next_status_at(&self, current: Duration, window: Duration) -> Option<Duration> {
        let at = current.saturating_add(self.config.status_interval);
        (at <= window).then_some(at)
    }

    /// Return the next NodeLease renewal after `current` using the production
    /// deterministic jitter. Disabled NodeLeases never produce renewals.
    pub fn next_lease_at(
        &self,
        current: Duration,
        node: usize,
        sequence: u64,
        window: Duration,
    ) -> Option<Duration> {
        if !self.config.node_lease_enabled {
            return None;
        }
        let at = current.saturating_add(self.jittered_interval(node as u64, sequence));
        (at <= window).then_some(at)
    }

    fn jittered_interval(&self, node: u64, sequence: u64) -> Duration {
        let base = self.config.lease_interval.as_nanos() as u128;
        let jitter = (base as f64 * self.config.lease_jitter).round() as u128;
        if jitter == 0 {
            return self.config.lease_interval;
        }
        // SplitMix64 gives a small, stable PRNG without global state or a
        // dependency on a particular rand implementation/version.
        let mut z = self
            .config
            .seed
            .wrapping_add(node.wrapping_mul(0x9e37_79b9_7f4a_7c15))
            .wrapping_add(sequence.rotate_left(17));
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        let span = jitter.saturating_mul(2).saturating_add(1);
        let offset = (z as u128 % span) as i128 - jitter as i128;
        let nanos = (base as i128 + offset).max(1) as u64;
        Duration::from_nanos(nanos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_bootstrap_order() {
        let planner = NodeHeartbeatPlanner::new(HeartbeatConfig {
            nodes: 2,
            ..Default::default()
        });
        let items = planner.bootstrap();
        let expected = [
            (
                0,
                HeartbeatItemKind::NodeCreate,
                "/registry/minions/node-0000",
            ),
            (
                0,
                HeartbeatItemKind::NodeLeaseCreate,
                "/registry/leases/kube-node-lease/node-0000",
            ),
            (
                0,
                HeartbeatItemKind::NodeReady,
                "/registry/minions/node-0000",
            ),
            (
                1,
                HeartbeatItemKind::NodeCreate,
                "/registry/minions/node-0001",
            ),
            (
                1,
                HeartbeatItemKind::NodeLeaseCreate,
                "/registry/leases/kube-node-lease/node-0001",
            ),
            (
                1,
                HeartbeatItemKind::NodeReady,
                "/registry/minions/node-0001",
            ),
        ];
        assert_eq!(items.len(), expected.len());
        for (item, &(node, kind, key)) in items.iter().zip(&expected) {
            assert_eq!(item.node, node);
            assert_eq!(item.kind, kind);
            assert_eq!(item.key, key);
            assert_eq!(item.at, Duration::ZERO);
        }
    }

    #[test]
    fn test_heartbeat_lease_disabled() {
        let planner = NodeHeartbeatPlanner::new(HeartbeatConfig {
            nodes: 2,
            node_lease_enabled: false,
            ..Default::default()
        });
        let items = planner.bootstrap();
        assert_eq!(items.len(), 4);
        assert!(items.iter().all(|item| {
            matches!(
                item.kind,
                HeartbeatItemKind::NodeCreate | HeartbeatItemKind::NodeReady
            )
        }));
        assert_eq!(
            planner.next_lease_at(Duration::ZERO, 0, 0, Duration::from_secs(60)),
            None
        );
    }

    #[test]
    fn test_heartbeat_jitter_bounds() {
        let planner = NodeHeartbeatPlanner::new(HeartbeatConfig {
            nodes: 1,
            ..Default::default()
        });
        let window = Duration::from_secs(31);
        let collect = || {
            let mut times = Vec::new();
            let mut current = Duration::ZERO;
            let mut sequence = 0;
            while let Some(next) = planner.next_lease_at(current, 0, sequence, window) {
                times.push(next);
                current = next;
                sequence += 1;
            }
            times
        };
        let first = collect();
        assert_eq!(first, collect());
        assert!(!first.is_empty());

        let cadence = Duration::from_millis(9_600)..=Duration::from_millis(10_400);
        let mut previous = Duration::ZERO;
        for (index, &at) in first.iter().enumerate() {
            let interval = at
                .checked_sub(previous)
                .expect("renew timestamps must be nondecreasing");
            assert!(
                cadence.contains(&interval),
                "renew {index} interval {interval:?} is outside the four-percent cadence"
            );
            previous = at;
        }
    }
}
