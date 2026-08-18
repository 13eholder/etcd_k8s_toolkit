//! etcd transaction shapes and the storage-side CAS policy.
//!
//! This module deliberately does not know about a gRPC client.  A caller owns the
//! logical operation counter and submits each [`TxnAttempt`] to its client.  A
//! failed transaction contains the point Range requested below, so the caller can
//! feed that response straight back into [`CasLoop`] without issuing another
//! Range RPC.

use std::time::{Duration, Instant};

use crate::pb::{etcdserverpb, mvccpb};
use etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RangeRequest, RequestOp, TxnRequest, TxnResponse,
    compare::{CompareResult, CompareTarget, TargetUnion},
    request_op, response_op,
};

/// The storage operation represented by one logical resource operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceOperation {
    Create,
    GuaranteedUpdate,
    ConditionalDelete,
}

/// The locally known revision state for one key.
///
/// Values returned by a failed CAS remain local to that attempt. Keeping them
/// here would retain the complete benchmark dataset in every worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum KeyState {
    #[default]
    Absent,
    Present {
        mod_revision: i64,
        version: i64,
    },
}

impl KeyState {
    pub fn mod_revision(&self) -> Option<i64> {
        match self {
            Self::Absent => None,
            Self::Present { mod_revision, .. } => Some(*mod_revision),
        }
    }

    pub fn version(&self) -> Option<i64> {
        match self {
            Self::Absent => None,
            Self::Present { version, .. } => Some(*version),
        }
    }

    fn from_kv(kv: &mvccpb::KeyValue) -> Self {
        Self::Present {
            mod_revision: kv.mod_revision,
            version: kv.version,
        }
    }
}

fn point_range(key: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(request_op::Request::RequestRange(RangeRequest {
            key: key.to_vec(),
            ..Default::default()
        })),
    }
}

fn mod_compare(key: &[u8], revision: i64) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: CompareTarget::Mod as i32,
        key: key.to_vec(),
        target_union: Some(TargetUnion::ModRevision(revision)),
        ..Default::default()
    }
}
fn version_compare(key: &[u8], version: i64) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: CompareTarget::Version as i32,
        key: key.to_vec(),
        target_union: Some(TargetUnion::Version(version)),
        ..Default::default()
    }
}

fn put_op(key: &[u8], value: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(request_op::Request::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })),
    }
}

fn delete_op(key: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(request_op::Request::RequestDeleteRange(
            DeleteRangeRequest {
                key: key.to_vec(),
                ..Default::default()
            },
        )),
    }
}

/// Build a create CAS.  Create compares `ModRevision == 0`; a collision is
/// intentionally not a storage-layer retry and therefore has no failure op.
pub fn build_create_txn(key: &[u8], value: &[u8]) -> TxnRequest {
    TxnRequest {
        compare: vec![mod_compare(key, 0)],
        success: vec![put_op(key, value)],
        failure: Vec::new(),
    }
}

/// Build a GuaranteedUpdate CAS.  The failure branch is one point Range and is
/// consumed directly from the Txn response.
pub fn build_update_txn(key: &[u8], value: &[u8], mod_revision: i64) -> TxnRequest {
    TxnRequest {
        compare: vec![mod_compare(key, mod_revision)],
        success: vec![put_op(key, value)],
        failure: vec![point_range(key)],
    }
}

/// Build a ConditionalDelete CAS.  A failed compare returns the current KV via
/// its point Range so the caller can re-check its deletion predicate.
pub fn build_delete_txn(key: &[u8], mod_revision: i64) -> TxnRequest {
    TxnRequest {
        compare: vec![mod_compare(key, mod_revision)],
        success: vec![delete_op(key)],
        failure: vec![point_range(key)],
    }
}
/// Build the compactor coordination CAS. The key version is the logical clock;
/// a failed compare returns the current key without issuing another Range RPC.
pub fn build_compact_revision_txn(value: &[u8], version: i64) -> TxnRequest {
    const KEY: &[u8] = b"compact_rev_key";
    TxnRequest {
        compare: vec![version_compare(KEY, version)],
        success: vec![put_op(KEY, value)],
        failure: vec![point_range(KEY)],
    }
}

/// One actual Txn RPC attempt.  Attempt numbers start at one and are separate
/// from a caller's scheduled logical-operation count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxnAttempt {
    pub operation: ResourceOperation,
    pub key: Vec<u8>,
    /// The value submitted for this attempt.  Delete attempts have an empty
    /// value.  A retry must receive a newly generated value from its caller.
    pub value: Vec<u8>,
    pub expected_mod_revision: i64,
    pub number: u32,
}

impl TxnAttempt {
    pub fn create(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            operation: ResourceOperation::Create,
            key: key.into(),
            value: value.into(),
            expected_mod_revision: 0,
            number: 1,
        }
    }

    pub fn update(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>, mod_revision: i64) -> Self {
        Self {
            operation: ResourceOperation::GuaranteedUpdate,
            key: key.into(),
            value: value.into(),
            expected_mod_revision: mod_revision,
            number: 1,
        }
    }

    pub fn delete(key: impl Into<Vec<u8>>, mod_revision: i64) -> Self {
        Self {
            operation: ResourceOperation::ConditionalDelete,
            key: key.into(),
            value: Vec::new(),
            expected_mod_revision: mod_revision,
            number: 1,
        }
    }

    pub fn request(&self) -> TxnRequest {
        match self.operation {
            ResourceOperation::Create => build_create_txn(&self.key, &self.value),
            ResourceOperation::GuaranteedUpdate => {
                build_update_txn(&self.key, &self.value, self.expected_mod_revision)
            }
            ResourceOperation::ConditionalDelete => {
                build_delete_txn(&self.key, self.expected_mod_revision)
            }
        }
    }
    /// Build the transaction and attach a TTL lease to its successful Put.
    /// Lease attachment is intentionally limited to Put operations; CAS
    /// compare/failure Range shape remains unchanged.
    pub fn request_with_lease(&self, lease: i64) -> TxnRequest {
        let mut request = self.request();
        if lease != 0 {
            if let Some(request_op::Request::RequestPut(put)) = request
                .success
                .first_mut()
                .and_then(|op| op.request.as_mut())
            {
                put.lease = lease;
            }
        }
        request
    }

    /// Reconstruct the next request after a conflict.  This intentionally takes
    /// a value from the resource producer: blindly reusing the prior value is
    /// incorrect for status/renew-time updates.
    pub fn rebuild_with_value(&self, value: impl Into<Vec<u8>>, latest: &KeyState) -> Option<Self> {
        let revision = latest.mod_revision()?;
        Some(Self {
            operation: self.operation,
            key: self.key.clone(),
            value: value.into(),
            expected_mod_revision: revision,
            number: self.number.saturating_add(1),
        })
    }

    pub fn apply_response(&self, response: &TxnResponse, state: &mut KeyState) -> AppliedTxn {
        apply_txn_response(self, response, state)
    }
}

/// Result of applying a Txn response to the local key state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppliedTxn {
    Succeeded {
        header_revision: i64,
    },
    Conflict {
        header_revision: i64,
        latest: KeyState,
    },
    Failed {
        header_revision: i64,
        latest: KeyState,
    },
}

fn response_header_revision(response: &TxnResponse) -> i64 {
    response.header.as_ref().map_or(0, |header| header.revision)
}

fn failure_kv(response: &TxnResponse) -> Option<&mvccpb::KeyValue> {
    response
        .responses
        .iter()
        .find_map(|operation| match operation.response.as_ref() {
            Some(response_op::Response::ResponseRange(range)) => range.kvs.first(),
            _ => None,
        })
}

pub fn failure_version(response: &TxnResponse) -> Option<i64> {
    failure_kv(response).map(|kv| kv.version)
}

/// Apply a response and return the header revision observed by this attempt.
/// Failure KV data comes from the Txn failure Range; this function never asks
/// the caller to issue an independent Range.
pub fn apply_txn_response(
    attempt: &TxnAttempt,
    response: &TxnResponse,
    state: &mut KeyState,
) -> AppliedTxn {
    let header_revision = response_header_revision(response);
    if response.succeeded {
        match attempt.operation {
            ResourceOperation::ConditionalDelete => *state = KeyState::Absent,
            ResourceOperation::Create | ResourceOperation::GuaranteedUpdate => {
                let version = state.version().unwrap_or(0).saturating_add(1);
                *state = KeyState::Present {
                    mod_revision: header_revision,
                    version,
                };
            }
        }
        AppliedTxn::Succeeded { header_revision }
    } else {
        let latest = failure_kv(response)
            .map(KeyState::from_kv)
            .unwrap_or(KeyState::Absent);
        *state = latest.clone();
        if matches!(attempt.operation, ResourceOperation::Create) {
            AppliedTxn::Failed {
                header_revision,
                latest,
            }
        } else {
            AppliedTxn::Conflict {
                header_revision,
                latest,
            }
        }
    }
}

/// A pure, client-independent CAS state machine.  It only decides whether the
/// caller may submit another attempt; it does not sleep, perform I/O, or own a
/// timer.  Dropping/cancelling the future that drives it is therefore enough to
/// cancel an in-flight attempt.
pub struct CasLoop {
    current: TxnAttempt,
    policy: RetryPolicy,
    deadline: Instant,
}

impl CasLoop {
    pub fn new(current: TxnAttempt, policy: RetryPolicy, deadline: Instant) -> Self {
        Self {
            current,
            policy,
            deadline,
        }
    }

    /// Apply one response.  A retry decision carries the conflict KV and the
    /// next attempt number, but no request: the caller must regenerate value.
    pub fn observe(&mut self, response: &TxnResponse, now: Instant) -> CasDecision {
        let mut state = KeyState::Absent;
        let applied = self.current.apply_response(response, &mut state);
        match applied {
            AppliedTxn::Succeeded { header_revision } => CasDecision::Succeeded {
                header_revision,
                state,
            },
            AppliedTxn::Failed {
                header_revision,
                latest,
            } if self.current.operation == ResourceOperation::Create => {
                CasDecision::CreateCollision {
                    header_revision,
                    latest,
                }
            }
            AppliedTxn::Conflict {
                header_revision,
                latest,
            } => {
                if now >= self.deadline {
                    return CasDecision::Exhausted {
                        header_revision,
                        latest,
                        reason: ExhaustedReason::CancelledOrDeadline,
                    };
                }
                if latest.mod_revision().is_none() {
                    return CasDecision::Exhausted {
                        header_revision,
                        latest,
                        reason: ExhaustedReason::MissingFailureKey,
                    };
                }
                if !self
                    .policy
                    .allows_retry(self.current.number, now, self.deadline)
                {
                    return CasDecision::Exhausted {
                        header_revision,
                        latest,
                        reason: ExhaustedReason::RetryPolicy,
                    };
                }
                CasDecision::Retry {
                    header_revision,
                    latest,
                    next_attempt: self.current.number.saturating_add(1),
                }
            }
            AppliedTxn::Failed { .. } => unreachable!("only create returns Failed"),
        }
    }

    /// Complete the retry decision with a freshly generated resource value.
    pub fn retry_with_value(
        &mut self,
        value: impl Into<Vec<u8>>,
        latest: &KeyState,
    ) -> Option<&TxnAttempt> {
        self.current = self.current.rebuild_with_value(value, latest)?;
        Some(&self.current)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CasDecision {
    Succeeded {
        header_revision: i64,
        state: KeyState,
    },
    CreateCollision {
        header_revision: i64,
        latest: KeyState,
    },
    Retry {
        header_revision: i64,
        latest: KeyState,
        next_attempt: u32,
    },
    Exhausted {
        header_revision: i64,
        latest: KeyState,
        reason: ExhaustedReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExhaustedReason {
    CancelledOrDeadline,
    MissingFailureKey,
    RetryPolicy,
}

/// Resource-level retry policy.  Timing is data only; the scheduler decides
/// when to run the next logical operation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RetryPolicy {
    /// Maximum storage attempts, including the first attempt. `u32::MAX`
    /// represents controller-style rate-limited requeue.
    pub max_attempts: u32,
    pub delay: Duration,
    /// Symmetric jitter ratio. Event policy is 0.25 (±25%).
    pub jitter_ratio: f64,
}

impl RetryPolicy {
    pub const fn generic_controller() -> Self {
        Self {
            max_attempts: u32::MAX,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    }

    pub const fn node_status() -> Self {
        Self {
            max_attempts: 5,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    }

    pub const fn node_lease() -> Self {
        Self {
            max_attempts: 5,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    }

    pub const fn event() -> Self {
        Self {
            max_attempts: 12,
            delay: Duration::from_secs(10),
            jitter_ratio: 0.25,
        }
    }

    fn allows_retry(&self, attempt: u32, now: Instant, deadline: Instant) -> bool {
        attempt < self.max_attempts && now < deadline
    }

    /// Deterministically derive the event retry delay from a seed.  This is a
    /// pure calculation; no timer is started here.
    pub fn delay_for(&self, seed: u64, attempt: u32) -> Duration {
        if self.jitter_ratio == 0.0 || self.delay.is_zero() {
            return self.delay;
        }
        // SplitMix64 gives stable, inexpensive pseudo-randomness without a
        // global RNG or a dynamic label.  `unit` is in [0, 1).
        let mut x = seed.wrapping_add((attempt as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^= x >> 31;
        let unit = (x as f64) / (u64::MAX as f64);
        let factor = 1.0 - self.jitter_ratio + 2.0 * self.jitter_ratio * unit;
        self.delay.mul_f64(factor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etcdserverpb::ResponseOp;
    fn header(revision: i64) -> etcdserverpb::ResponseHeader {
        etcdserverpb::ResponseHeader {
            revision,
            ..Default::default()
        }
    }

    fn conflict(revision: i64, value: &[u8], version: i64) -> TxnResponse {
        TxnResponse {
            header: Some(header(revision + 1)),
            succeeded: false,
            responses: vec![ResponseOp {
                response: Some(response_op::Response::ResponseRange(
                    etcdserverpb::RangeResponse {
                        kvs: vec![mvccpb::KeyValue {
                            key: b"k".to_vec(),
                            value: value.to_vec(),
                            mod_revision: revision,
                            version,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )),
            }],
        }
    }

    #[test]
    fn txn_shapes_use_mod_compare_and_point_failure_range() {
        let create = build_create_txn(b"k", b"v");
        assert_eq!(create.compare[0].target, CompareTarget::Mod as i32);
        assert_eq!(
            create.compare[0].target_union,
            Some(TargetUnion::ModRevision(0))
        );
        assert!(create.failure.is_empty());

        let update = build_update_txn(b"k", b"v", 7);
        assert_eq!(
            update.compare[0].target_union,
            Some(TargetUnion::ModRevision(7))
        );
        assert_eq!(update.failure.len(), 1);
        match update.failure[0].request.as_ref() {
            Some(request_op::Request::RequestRange(range)) => {
                assert_eq!(range.key, b"k");
                assert!(range.range_end.is_empty());
            }
            other => panic!("unexpected failure op: {other:?}"),
        }
        let delete = build_delete_txn(b"k", 7);
        assert_eq!(delete.success.len(), 1);
        assert_eq!(delete.failure.len(), 1);
        let compact = build_compact_revision_txn(b"123", 7);
        assert_eq!(compact.compare[0].target, CompareTarget::Version as i32);
        assert_eq!(
            compact.compare[0].target_union,
            Some(TargetUnion::Version(7))
        );
        assert_eq!(compact.failure.len(), 1);
    }

    #[test]
    fn compact_conflict_exposes_server_version_for_resync() {
        assert_eq!(failure_version(&conflict(9, b"123", 4)), Some(4));
        assert_eq!(failure_version(&TxnResponse::default()), None);
    }

    #[test]
    fn conflict_kv_drives_next_attempt_revision_and_new_value() {
        let first = TxnAttempt::update(b"k", b"old", 3);
        let mut state = KeyState::Present {
            mod_revision: 3,
            version: 1,
        };
        let applied = first.apply_response(&conflict(9, b"latest", 4), &mut state);
        assert_eq!(
            applied,
            AppliedTxn::Conflict {
                header_revision: 10,
                latest: KeyState::Present {
                    mod_revision: 9,
                    version: 4,
                }
            }
        );
        let mut loop_state = CasLoop::new(
            first,
            RetryPolicy::node_status(),
            Instant::now() + Duration::from_secs(1),
        );
        let decision = loop_state.observe(&conflict(9, b"latest", 4), Instant::now());
        assert!(matches!(
            &decision,
            CasDecision::Retry {
                next_attempt: 2,
                ..
            }
        ));
        let latest = match decision {
            CasDecision::Retry { latest, .. } => latest,
            _ => unreachable!(),
        };
        let next = loop_state.retry_with_value(b"recomputed", &latest).unwrap();
        assert_eq!(next.number, 2);
        assert_eq!(next.expected_mod_revision, 9);
        assert_eq!(
            next.request().compare[0].target_union,
            Some(TargetUnion::ModRevision(9))
        );
        match next.request().success[0].request.as_ref() {
            Some(request_op::Request::RequestPut(put)) => assert_eq!(put.value, b"recomputed"),
            _ => panic!("expected put"),
        }
    }

    #[test]
    fn create_collision_does_not_retry() {
        let attempt = TxnAttempt::create(b"k", b"v");
        let mut loop_state = CasLoop::new(
            attempt,
            RetryPolicy::generic_controller(),
            Instant::now() + Duration::from_secs(1),
        );
        let response = TxnResponse {
            header: Some(header(4)),
            succeeded: false,
            responses: Vec::new(),
        };
        assert!(matches!(
            loop_state.observe(&response, Instant::now()),
            CasDecision::CreateCollision { .. }
        ));
    }

    #[test]
    fn event_policy_is_twelve_attempts_and_symmetric_jitter() {
        let policy = RetryPolicy::event();
        assert_eq!(policy.max_attempts, 12);
        for attempt in 0..32 {
            let delay = policy.delay_for(attempt as u64, attempt);
            assert!(delay >= Duration::from_millis(7_500));
            assert!(delay <= Duration::from_millis(12_500));
        }
    }
}
