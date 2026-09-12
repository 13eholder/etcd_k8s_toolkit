//! etcd TTL lease pooling backed exclusively by LeaseGrant.
//!
//! Coordination NodeLeases are intentionally not represented here: those are
//! ordinary KV transactions planned by `heartbeat`. This module is for TTL
//! leases (notably Event objects) returned by `LeaseGrant`.

use std::time::Duration;

const MAX_ETCD_LEASE_SECONDS: u64 = i64::MAX as u64;

/// Pool defaults from Kubernetes' Event TTL lease manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeasePoolConfig {
    pub object_ttl: Duration,
    pub reuse_window: Duration,
    pub max_objects: u64,
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
            self.object_ttl.as_secs() <= MAX_ETCD_LEASE_SECONDS,
            "object TTL exceeds etcd TTL range"
        );
        assert!(
            self.max_objects > 0,
            "max_objects must be greater than zero"
        );
        let effective_reuse = self
            .reuse_window
            .min(Duration::from_secs(self.object_ttl.as_secs() / 20));
        let grant_ttl = self
            .object_ttl
            .checked_add(effective_reuse)
            .expect("grant TTL must be representable");
        assert!(
            grant_ttl.as_secs() <= MAX_ETCD_LEASE_SECONDS,
            "grant TTL exceeds etcd TTL range"
        );
    }
}

/// An observable pool transition. A `Grant` requests a new native etcd lease;
/// replacing the cached slot never sends a revoke.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseEvent {
    Grant { ttl: Duration },
    Reuse { lease_id: i64 },
}

/// A lease pool manager matching Kubernetes' lease-manager reuse boundaries.
#[derive(Clone, Debug)]
pub struct LeasePool {
    config: LeasePoolConfig,
    current: Option<LeaseSlot>,
}

#[derive(Clone, Debug)]
struct LeaseSlot {
    id: i64,
    expires_at: Duration,
    objects: u64,
}

impl LeasePool {
    pub fn new(config: LeasePoolConfig) -> Self {
        config.validate();
        Self {
            config,
            current: None,
        }
    }

    /// Kubernetes caps the configured reuse duration at five percent of the
    /// requested TTL (`int64(0.05 * ttl)` in lease_manager.go).
    fn effective_reuse_window(&self) -> Duration {
        let five_percent = Duration::from_secs(self.config.object_ttl.as_secs() / 20);
        self.config.reuse_window.min(five_percent)
    }

    /// The LeaseGrant TTL includes the effective reuse grace window.
    pub fn grant_ttl(&self) -> Duration {
        self.config
            .object_ttl
            .checked_add(self.effective_reuse_window())
            .expect("grant TTL must be representable")
    }

    /// Plan one Kubernetes lease-manager `GetLease` call.
    ///
    /// Reuse requires both Kubernetes predicates:
    /// `now + object_ttl < expiration` and `now + grant_ttl > expiration`,
    /// plus the object cap. The resulting reuse interval is the strict open
    /// interval immediately after Grant and before the grace boundary.
    pub fn acquire_at(&mut self, now: Duration) -> LeaseEvent {
        let object_ttl = self.config.object_ttl;
        let grant_ttl = self.grant_ttl();
        let max_objects = self.config.max_objects;
        if let Some(slot) = &mut self.current {
            // Kubernetes counts every GetLease call before testing reuse,
            // including calls whose subsequent Put may fail.
            let Some(next_objects) = slot.objects.checked_add(1) else {
                return LeaseEvent::Grant { ttl: grant_ttl };
            };
            slot.objects = next_objects;
            let valid = now.saturating_add(object_ttl) < slot.expires_at;
            let sufficient = now.saturating_add(grant_ttl) > slot.expires_at;
            if valid && sufficient && next_objects <= max_objects {
                return LeaseEvent::Reuse { lease_id: slot.id };
            }
        }

        // The old native lease remains on the server and expires naturally.
        // Only a successful Grant replaces the local cache.
        LeaseEvent::Grant { ttl: grant_ttl }
    }

    /// Record the server-assigned ID after a successful Grant RPC.
    pub fn record_grant(&mut self, lease_id: i64, now: Duration) {
        assert_ne!(lease_id, 0, "etcd must return a non-zero lease ID");
        self.current = Some(LeaseSlot {
            id: lease_id,
            expires_at: now.saturating_add(self.grant_ttl()),
            objects: 1,
        });
    }

    pub fn attached_objects(&self) -> u64 {
        self.current.as_ref().map_or(0, |slot| slot.objects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_object_cap() {
        let mut pool = LeasePool::new(LeasePoolConfig::default());
        assert_eq!(
            pool.acquire_at(Duration::ZERO),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }
        );
        pool.record_grant(7, Duration::ZERO);

        for _ in 0..999 {
            assert_eq!(
                pool.acquire_at(Duration::from_secs(1)),
                LeaseEvent::Reuse { lease_id: 7 }
            );
        }
        assert_eq!(
            pool.acquire_at(Duration::from_secs(1)),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }
        );
    }

    #[test]
    fn test_lease_reuse_window() {
        let mut pool = LeasePool::new(LeasePoolConfig::default());
        pool.record_grant(7, Duration::ZERO);

        assert_eq!(
            pool.acquire_at(Duration::ZERO),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }
        );

        let mut pool = LeasePool::new(LeasePoolConfig::default());
        pool.record_grant(7, Duration::ZERO);
        assert_eq!(
            pool.acquire_at(Duration::from_secs(1)),
            LeaseEvent::Reuse { lease_id: 7 }
        );
        assert_eq!(
            pool.acquire_at(Duration::from_secs(60)),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }
        );
    }

    #[test]
    fn test_lease_short_ttl_no_reuse() {
        let mut pool = LeasePool::new(LeasePoolConfig {
            object_ttl: Duration::from_secs(15),
            reuse_window: Duration::from_secs(60),
            max_objects: 1000,
        });
        assert_eq!(
            pool.acquire_at(Duration::ZERO),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(15)
            }
        );
        pool.record_grant(7, Duration::ZERO);
        assert_eq!(
            pool.acquire_at(Duration::from_secs(1)),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(15)
            }
        );
    }

    #[test]
    fn test_lease_count_overflow() {
        let mut pool = LeasePool::new(LeasePoolConfig {
            object_ttl: Duration::from_secs(3600),
            reuse_window: Duration::from_secs(60),
            max_objects: u64::MAX,
        });
        pool.record_grant(7, Duration::ZERO);
        pool.current.as_mut().unwrap().objects = u64::MAX;
        assert_eq!(
            pool.acquire_at(Duration::from_secs(1)),
            LeaseEvent::Grant {
                ttl: Duration::from_secs(3_660)
            }
        );
    }
}
