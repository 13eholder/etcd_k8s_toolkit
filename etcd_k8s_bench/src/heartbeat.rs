//! Deterministic Kubernetes Node and Coordination NodeLease heartbeat planning.
//!
//! A [`HeartbeatItem`] is deliberately an abstract KV item.  The integration
//! layer turns it into a CAS transaction; this module never invokes etcd's
//! LeaseKeepAlive RPC (Coordination NodeLeases are ordinary KV objects).

use std::time::Duration;

/// The kind of periodic heartbeat item emitted by the compiled plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatItemKind {
    /// A regular Coordination NodeLease renewal (a KV CAS update).
    NodeLeaseRenew,
    /// A periodic Node status update.
    NodeStatus,
}

/// One abstract heartbeat KV item. `at` is relative to the beginning of the
/// workload, making plans reproducible and straightforward to test.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatItem {
    pub node: usize,
    pub kind: HeartbeatItemKind,
    pub key: String,
    pub at: Duration,
}

impl HeartbeatItem {
    pub(crate) fn from_plan(
        node: usize,
        kind: HeartbeatItemKind,
        key: String,
        at: Duration,
    ) -> Self {
        Self {
            node,
            kind,
            key,
            at,
        }
    }
}
