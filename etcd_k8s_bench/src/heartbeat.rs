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
/// by the density workload.
#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatConfig {
    pub nodes: usize,
    pub lease_interval: Duration,
    pub lease_jitter: f64,
    pub status_interval: Duration,
    pub seed: u64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            nodes: 2_003,
            lease_interval: Duration::from_secs(10),
            lease_jitter: 0.04,
            status_interval: Duration::from_secs(300),
            seed: 1,
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

    /// Emit bootstrap items in node order.  Each node is created, its
    /// Coordination NodeLease is created, and then its initial Ready update is
    /// applied before any runtime item is emitted.
    pub fn bootstrap(&self) -> Vec<HeartbeatItem> {
        let mut result = Vec::with_capacity(self.config.nodes * 3);
        for node in 0..self.config.nodes {
            result.push(HeartbeatItem::new(
                node,
                HeartbeatItemKind::NodeCreate,
                Duration::ZERO,
            ));
            result.push(HeartbeatItem::new(
                node,
                HeartbeatItemKind::NodeLeaseCreate,
                Duration::ZERO,
            ));
            result.push(HeartbeatItem::new(
                node,
                HeartbeatItemKind::NodeReady,
                Duration::ZERO,
            ));
        }
        result
    }

    /// Emit bootstrap and all runtime items whose timestamp is no later than
    /// `window`.  A status update is never scheduled more than the configured
    /// status interval apart; NodeLease renewal uses deterministic jitter.
    #[cfg(test)]
    pub fn plan_until(&self, window: Duration) -> Vec<HeartbeatItem> {
        let mut result = self.bootstrap();
        if window.is_zero() {
            return result;
        }

        for node in 0..self.config.nodes {
            let mut lease_at = self.jittered_interval(node as u64, 0);
            while lease_at <= window {
                result.push(HeartbeatItem::new(
                    node,
                    HeartbeatItemKind::NodeLeaseRenew,
                    lease_at,
                ));
                let next = lease_at.saturating_add(
                    self.jittered_interval(node as u64, lease_at.as_nanos() as u64),
                );
                if next <= lease_at {
                    break;
                }
                lease_at = next;
            }

            let mut status_at = self.config.status_interval;
            while status_at <= window {
                result.push(HeartbeatItem::new(
                    node,
                    HeartbeatItemKind::NodeStatus,
                    status_at,
                ));
                status_at = status_at.saturating_add(self.config.status_interval);
            }
        }

        // Runtime items are emitted in timestamp order, with NodeLease before
        // Node status at a tie and node index as the final stable tie-breaker.
        result[3 * self.config.nodes..].sort_by_key(|item| {
            (
                item.at,
                match item.kind {
                    HeartbeatItemKind::NodeLeaseRenew => 0u8,
                    HeartbeatItemKind::NodeStatus => 1,
                    _ => 2,
                },
                item.node,
            )
        });
        result
    }

    #[cfg(test)]
    fn jittered_interval(&self, node: u64, tick: u64) -> Duration {
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
            .wrapping_add(tick.rotate_left(17));
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
    fn bootstrap_is_three_ordered_items_per_node() {
        let planner = NodeHeartbeatPlanner::new(HeartbeatConfig {
            nodes: 2,
            ..Default::default()
        });
        let items = planner.bootstrap();
        assert_eq!(items.len(), 6);
        assert_eq!(items[0].kind, HeartbeatItemKind::NodeCreate);
        assert_eq!(items[1].kind, HeartbeatItemKind::NodeLeaseCreate);
        assert_eq!(items[2].kind, HeartbeatItemKind::NodeReady);
        assert_eq!(items[3].node, 1);
    }

    #[test]
    fn jitter_is_repeatable_and_within_four_percent() {
        let planner = NodeHeartbeatPlanner::new(HeartbeatConfig {
            nodes: 1,
            ..Default::default()
        });
        let a = planner.plan_until(Duration::from_secs(31));
        let b = planner.plan_until(Duration::from_secs(31));
        assert_eq!(a, b);
        let renew_times: Vec<_> = a
            .iter()
            .filter(|x| x.kind == HeartbeatItemKind::NodeLeaseRenew)
            .map(|item| item.at)
            .collect();
        assert!(!renew_times.is_empty());

        let cadence = Duration::from_millis(9_600)..=Duration::from_millis(10_400);
        let mut previous = Duration::ZERO;
        for (index, &at) in renew_times.iter().enumerate() {
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
