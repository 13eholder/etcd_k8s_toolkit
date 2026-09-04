//! etcd TTL lease pooling backed exclusively by LeaseGrant.
//!
//! Coordination NodeLeases are intentionally not represented here: those are
//! ordinary KV transactions planned by `heartbeat`.  This module is for TTL
//! leases (notably Event objects) returned by `LeaseGrant`.

use std::time::Duration;

/// Pool defaults from Kubernetes' Event TTL lease manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeasePoolConfig {
    pub object_ttl: Duration,
    pub reuse_window: Duration,
    pub max_objects: u32,
}

impl Default for LeasePoolConfig {
    fn default() -> Self {
        Self {
            object_ttl: Duration::from_secs(3_600),
            reuse_window: Duration::from_secs(60),
            max_objects: 1_000,
        }
    }
}

impl LeasePoolConfig {
    fn validate(&self) {
        assert!(
            self.object_ttl > Duration::ZERO,
            "object TTL must be non-zero"
        );
        assert!(
            self.reuse_window <= self.object_ttl,
            "reuse window cannot exceed TTL"
        );
        assert!(
            self.max_objects > 0,
            "max_objects must be greater than zero"
        );
    }
}

/// An observable pool transition.  A `Grant` is a request to issue
/// `LeaseGrant(ttl)`; after the RPC succeeds call [`LeasePool::record_grant`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseEvent {
    Grant { ttl: Duration },
    Reuse { lease_id: i64 },
    Expire { lease_id: i64 },
}

/// A lease pool manager with the exact Grant/reuse/expire boundary semantics.
#[derive(Clone, Debug)]
pub struct LeasePool {
    config: LeasePoolConfig,
    current: Option<LeaseSlot>,
}

#[derive(Clone, Debug)]
struct LeaseSlot {
    id: i64,
    granted_at: Duration,
    objects: u32,
}

impl LeasePool {
    pub fn new(config: LeasePoolConfig) -> Self {
        config.validate();
        Self {
            config,
            current: None,
        }
    }

    /// The LeaseGrant TTL includes the object lifetime and the reuse grace
    /// window.  For the Event defaults this is 3660 seconds, matching the
    /// Kubernetes apiserver lease manager.
    pub fn grant_ttl(&self) -> Duration {
        self.config
            .object_ttl
            .saturating_add(self.config.reuse_window)
    }

    /// Plan allocation of one object.  Reuse is strictly before the reuse
    /// boundary and strictly below the object cap.  Crossing either boundary
    /// emits Expire followed by Grant.
    pub fn acquire_at(&mut self, now: Duration) -> Vec<LeaseEvent> {
        if let Some(slot) = &mut self.current {
            let within_window = now.saturating_sub(slot.granted_at) < self.config.reuse_window;
            if within_window && slot.objects < self.config.max_objects {
                slot.objects += 1;
                return vec![LeaseEvent::Reuse { lease_id: slot.id }];
            }
        }

        let mut events = Vec::with_capacity(2);
        if let Some(slot) = self.current.take() {
            events.push(LeaseEvent::Expire { lease_id: slot.id });
        }
        events.push(LeaseEvent::Grant {
            ttl: self.grant_ttl(),
        });
        events
    }

    /// Record the server-assigned ID after a Grant RPC.  The first object is
    /// counted at grant time, so exactly `max_objects` objects use one lease.
    pub fn record_grant(&mut self, lease_id: i64, now: Duration) {
        assert_ne!(lease_id, 0, "etcd must return a non-zero lease ID");
        self.current = Some(LeaseSlot {
            id: lease_id,
            granted_at: now,
            objects: 1,
        });
    }

    pub fn invalidate(&mut self, lease_id: i64) {
        if matches!(self.current.as_ref(), Some(slot) if slot.id == lease_id) {
            self.current = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_pool_reuses_until_both_boundaries() {
        let mut pool = LeasePool::new(LeasePoolConfig::default());
        assert_eq!(
            pool.acquire_at(Duration::ZERO),
            vec![LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }]
        );
        pool.record_grant(7, Duration::ZERO);
        for _ in 0..999 {
            assert_eq!(
                pool.acquire_at(Duration::from_secs(59)),
                vec![LeaseEvent::Reuse { lease_id: 7 }]
            );
        }
        assert_eq!(
            pool.acquire_at(Duration::from_secs(59)),
            vec![
                LeaseEvent::Expire { lease_id: 7 },
                LeaseEvent::Grant {
                    ttl: Duration::from_secs(3_660)
                }
            ]
        );

        pool.record_grant(8, Duration::from_secs(60));
        assert_eq!(
            pool.acquire_at(Duration::from_secs(120)),
            vec![
                LeaseEvent::Expire { lease_id: 8 },
                LeaseEvent::Grant {
                    ttl: Duration::from_secs(3_660)
                }
            ]
        );
    }

    #[test]
    fn invalidated_lease_is_not_reused() {
        let mut pool = LeasePool::new(LeasePoolConfig::default());
        pool.record_grant(7, Duration::ZERO);
        pool.invalidate(7);
        assert_eq!(
            pool.acquire_at(Duration::from_secs(1)),
            vec![LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }]
        );
    }
}
