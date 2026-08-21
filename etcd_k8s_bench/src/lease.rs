//! etcd TTL lease pooling and real LeaseKeepAlive stream management.
//!
//! Coordination NodeLeases are intentionally not represented here: those are
//! ordinary KV transactions planned by `heartbeat`.  This module is for TTL
//! leases (notably Event objects) returned by `LeaseGrant`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::metrics::{self, Outcome, PoolName, ResourceName, Rpc};
use crate::pb::etcdserverpb::LeaseKeepAliveRequest;
use crate::pb::etcdserverpb::lease_client::LeaseClient;

/// Pool defaults from Kubernetes' Event TTL lease manager.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeasePoolConfig {
    pub object_ttl: Duration,
    pub reuse_window: Duration,
    pub max_objects: u32,
    pub keep_alive: bool,
}

impl Default for LeasePoolConfig {
    fn default() -> Self {
        Self {
            object_ttl: Duration::from_secs(3_600),
            reuse_window: Duration::from_secs(60),
            max_objects: 1_000,
            keep_alive: false,
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

    pub fn start_keep_alive(
        &self,
        client: LeaseClient<Channel>,
        pool: PoolName,
    ) -> Option<KeepAliveHandle> {
        self.config
            .keep_alive
            .then(|| KeepAliveHandle::start(client, self.grant_ttl(), pool))
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

/// Deterministic keep-alive cadence helper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeepAlivePlanner {
    ttl: Duration,
}

impl KeepAlivePlanner {
    pub fn new(ttl: Duration) -> Self {
        assert!(!ttl.is_zero(), "keep-alive TTL must be non-zero");
        Self { ttl }
    }

    /// etcd clients send one request every TTL/3, rounded down but never zero.
    pub fn interval(&self) -> Duration {
        let nanos = (self.ttl.as_nanos() / 3).max(1);
        Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
    }
}

#[derive(Clone, Debug)]
struct ActiveLease {
    expires_at: Instant,
}

/// Handle for the background bidirectional tonic keep-alive stream.
///
/// The task owns a `LeaseClient<Channel>`, sends all currently live lease IDs
/// every TTL/3, consumes responses, and reconnects after stream errors.  A
/// lease is only retried while its local expiry deadline has not passed.
pub struct KeepAliveHandle {
    leases: Arc<Mutex<BTreeMap<i64, ActiveLease>>>,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl KeepAliveHandle {
    pub fn start(mut client: LeaseClient<Channel>, ttl: Duration, pool: PoolName) -> Self {
        let planner = KeepAlivePlanner::new(ttl);
        let leases = Arc::new(Mutex::new(BTreeMap::new()));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let worker_leases = Arc::clone(&leases);
        let task = tokio::spawn(async move {
            run_keep_alive(&mut client, planner, worker_leases, shutdown_rx, pool).await;
        });
        Self {
            leases,
            shutdown,
            task: Some(task),
        }
    }

    pub async fn register(&self, lease_id: i64, ttl: Duration) {
        assert_ne!(lease_id, 0, "lease ID must be non-zero");
        self.leases.lock().await.insert(
            lease_id,
            ActiveLease {
                expires_at: Instant::now() + ttl,
            },
        );
    }

    pub async fn is_active(&self, lease_id: i64) -> bool {
        self.leases.lock().await.contains_key(&lease_id)
    }

    pub async fn unregister(&self, lease_id: i64) {
        self.leases.lock().await.remove(&lease_id);
    }

    /// Stop both the sender and response-consumer task.  Dropping a handle
    /// also signals shutdown; callers that need deterministic completion should
    /// await this method.
    pub async fn close(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for KeepAliveHandle {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_keep_alive(
    client: &mut LeaseClient<Channel>,
    planner: KeepAlivePlanner,
    leases: Arc<Mutex<BTreeMap<i64, ActiveLease>>>,
    mut shutdown: watch::Receiver<bool>,
    pool: PoolName,
) {
    let interval = planner.interval();
    loop {
        let first_id = loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                _ = time::sleep(interval) => {
                    if let Some(id) = live_ids(&leases).await.first().copied() {
                        break id;
                    }
                }
            }
        };
        let (request_tx, request_rx) = mpsc::channel(64);
        if request_tx
            .send(LeaseKeepAliveRequest { id: first_id })
            .await
            .is_err()
        {
            metrics::record_lease_keepalive(
                &pool,
                metrics::KeepAliveMessage::Request,
                Outcome::GrpcError,
            );
            continue;
        }
        metrics::record_request_bytes(&ResourceName::new("lease").unwrap(), 8);
        metrics::record_lease_keepalive(
            &pool,
            metrics::KeepAliveMessage::Request,
            Outcome::Success,
        );
        let start = std::time::Instant::now();
        metrics::add_inflight(Rpc::LeaseKeepAlive);
        let response = client
            .lease_keep_alive(ReceiverStream::new(request_rx))
            .await;
        metrics::remove_inflight(Rpc::LeaseKeepAlive);
        metrics::record_request_duration(Rpc::LeaseKeepAlive, start.elapsed());
        let response = match response {
            Ok(response) => {
                metrics::record_request(Rpc::LeaseKeepAlive, Outcome::Success);
                response
            }
            Err(_) => {
                metrics::record_request(Rpc::LeaseKeepAlive, Outcome::GrpcError);
                if time::timeout(Duration::from_millis(100), shutdown.changed())
                    .await
                    .is_ok()
                    && *shutdown.borrow()
                {
                    return;
                }
                continue;
            }
        };
        let mut responses = response.into_inner();
        let mut ticker = time::interval_at(Instant::now() + interval, interval);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_ok() && *shutdown.borrow() {
                        return;
                    }
                }
                _ = ticker.tick() => {
                    let ids = live_ids(&leases).await;
                    for id in ids {
                        let result = request_tx.send(LeaseKeepAliveRequest { id }).await;
                        metrics::record_request_bytes(&ResourceName::new("lease").unwrap(), 8);
                        match result {
                            Ok(()) => {
                                metrics::record_lease_keepalive(
                                    &pool,
                                    metrics::KeepAliveMessage::Request,
                                    Outcome::Success,
                                );
                            }
                            Err(_) => {
                                metrics::record_lease_keepalive(
                                    &pool,
                                    metrics::KeepAliveMessage::Request,
                                    Outcome::GrpcError,
                                );
                                break;
                            }
                        }
                    }
                }
                response = responses.next() => {
                    let outcome = match response {
                        Some(Ok(response)) => {
                            if response.id != 0 {
                                apply_keepalive_response(&leases, response.id, response.ttl).await
                            } else {
                                Outcome::GrpcError
                            }
                        }
                        Some(Err(_)) | None => Outcome::GrpcError,
                    };
                    metrics::record_lease_keepalive(
                        &pool,
                        metrics::KeepAliveMessage::Response,
                        outcome,
                    );
                    if outcome != Outcome::Success {
                        break;
                    }
                }
            }
        }
        if time::timeout(Duration::from_millis(100), shutdown.changed())
            .await
            .is_ok()
            && *shutdown.borrow()
        {
            return;
        }
    }
}
fn apply_keepalive_ttl(
    leases: &mut BTreeMap<i64, ActiveLease>,
    lease_id: i64,
    ttl: i64,
    now: Instant,
) -> Outcome {
    if ttl <= 0 {
        leases.remove(&lease_id);
        return Outcome::Timeout;
    }
    if let Some(lease) = leases.get_mut(&lease_id) {
        lease.expires_at = now + Duration::from_secs(ttl as u64);
    }
    Outcome::Success
}

async fn apply_keepalive_response(
    leases: &Arc<Mutex<BTreeMap<i64, ActiveLease>>>,
    lease_id: i64,
    ttl: i64,
) -> Outcome {
    let mut guard = leases.lock().await;
    apply_keepalive_ttl(&mut guard, lease_id, ttl, Instant::now())
}
async fn live_ids(leases: &Arc<Mutex<BTreeMap<i64, ActiveLease>>>) -> Vec<i64> {
    let now = Instant::now();
    let mut leases = leases.lock().await;
    leases.retain(|_, lease| lease.expires_at > now);
    leases.keys().copied().collect()
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
    fn keep_alive_cadence_is_ttl_third() {
        assert_eq!(
            KeepAlivePlanner::new(Duration::from_secs(3_600)).interval(),
            Duration::from_secs(1_200)
        );
        assert_eq!(
            KeepAlivePlanner::new(Duration::from_secs(2)).interval(),
            Duration::from_nanos(666_666_666)
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
    #[test]
    fn zero_ttl_removes_active_lease() {
        let now = Instant::now();
        let mut leases = BTreeMap::from([(
            7,
            ActiveLease {
                expires_at: now + Duration::from_secs(30),
            },
        )]);
        assert_eq!(
            apply_keepalive_ttl(&mut leases, 7, 0, now),
            Outcome::Timeout
        );
        assert!(!leases.contains_key(&7));
    }

    #[test]
    fn positive_ttl_for_retired_lease_is_still_a_successful_response() {
        let mut leases = BTreeMap::new();
        assert_eq!(
            apply_keepalive_ttl(&mut leases, 7, 30, Instant::now()),
            Outcome::Success
        );
    }
}
