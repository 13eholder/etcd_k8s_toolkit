//! etcd transaction shapes and the storage-side CAS policy.
//!
//! This module deliberately does not know about a gRPC client.  A caller owns the
//! logical operation counter and submits each [`TxnAttempt`] to its client.  A
//! failed transaction contains the point Range requested below, so the caller can
//! feed that response straight back into [`CasLoop`] without issuing another
//! Range RPC.

#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
use etcdserverpb::{
    Compare, DeleteRangeRequest, PutRequest, RangeRequest, RequestOp, TxnRequest, TxnResponse,
    request_op, response_op,
};

use crate::pb::{etcdserverpb, mvccpb};

/// The storage operation represented by one logical resource operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceOperation {
    Create,
    GuaranteedUpdate,
    ConditionalDelete,
}

impl ResourceOperation {
    /// Native etcd leases are attached to successful Put operations only.
    pub const fn uses_native_lease(self) -> bool {
        matches!(self, Self::Create | Self::GuaranteedUpdate)
    }
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
    },
}

impl KeyState {
    pub fn mod_revision(&self) -> Option<i64> {
        match self {
            Self::Absent => None,
            Self::Present { mod_revision, .. } => Some(*mod_revision),
        }
    }

    fn from_kv(kv: &mvccpb::KeyValue) -> Self {
        Self::Present {
            mod_revision: kv.mod_revision,
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

/// Return the coordination value from a failed compact CAS's embedded Range.
/// The caller owns parsing because malformed remote values are observable input.
pub fn failure_value(response: &TxnResponse) -> Option<&[u8]> {
    failure_kv(response).map(|kv| kv.value.as_slice())
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
                *state = KeyState::Present {
                    mod_revision: header_revision,
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
    policy: CasRetryPolicy,
    deadline: Instant,
}

impl CasLoop {
    pub fn new(current: TxnAttempt, policy: CasRetryPolicy, deadline: Instant) -> Self {
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

/// Storage-side CAS attempt budget; the driving operation owns its deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CasRetryPolicy {
    /// Maximum storage attempts, including the first. `u32::MAX` lets the
    /// operation deadline bound normal storage CAS contention.
    pub max_attempts: u32,
}

impl CasRetryPolicy {
    pub const fn until_deadline() -> Self {
        Self {
            max_attempts: u32::MAX,
        }
    }

    fn allows_retry(&self, attempt: u32, now: Instant, deadline: Instant) -> bool {
        attempt < self.max_attempts && now < deadline
    }
}

#[cfg(test)]
mod tests {
    use etcdserverpb::ResponseOp;

    use super::*;
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
    fn test_txn_protocol_shape() {
        fn assert_compare(
            compare: &Compare,
            key: &[u8],
            target: CompareTarget,
            target_union: TargetUnion,
        ) {
            assert_eq!(compare.result, CompareResult::Equal as i32);
            assert_eq!(compare.target, target as i32);
            assert_eq!(compare.key, key);
            assert_eq!(compare.target_union, Some(target_union));
        }
        fn assert_put(op: &RequestOp, key: &[u8], value: &[u8]) {
            match op.request.as_ref() {
                Some(request_op::Request::RequestPut(put)) => {
                    assert_eq!(put.key, key);
                    assert_eq!(put.value, value);
                }
                other => panic!("expected Put, got {other:?}"),
            }
        }
        fn assert_point_range(op: &RequestOp, key: &[u8]) {
            match op.request.as_ref() {
                Some(request_op::Request::RequestRange(range)) => {
                    assert_eq!(range.key, key);
                    assert!(range.range_end.is_empty());
                }
                other => panic!("expected point Range, got {other:?}"),
            }
        }

        let create = build_create_txn(b"k", b"v");
        assert_eq!(create.compare.len(), 1);
        assert_compare(
            &create.compare[0],
            b"k",
            CompareTarget::Mod,
            TargetUnion::ModRevision(0),
        );
        assert_eq!(create.success.len(), 1);
        assert_put(&create.success[0], b"k", b"v");
        assert!(create.failure.is_empty());

        let update = build_update_txn(b"k", b"v", 7);
        assert_compare(
            &update.compare[0],
            b"k",
            CompareTarget::Mod,
            TargetUnion::ModRevision(7),
        );
        assert_put(&update.success[0], b"k", b"v");
        assert_eq!(update.failure.len(), 1);
        assert_point_range(&update.failure[0], b"k");

        let delete = build_delete_txn(b"k", 7);
        assert_compare(
            &delete.compare[0],
            b"k",
            CompareTarget::Mod,
            TargetUnion::ModRevision(7),
        );
        assert_eq!(delete.success.len(), 1);
        match delete.success[0].request.as_ref() {
            Some(request_op::Request::RequestDeleteRange(delete)) => {
                assert_eq!(delete.key, b"k");
                assert!(delete.range_end.is_empty());
            }
            other => panic!("expected point DeleteRange, got {other:?}"),
        }
        assert_eq!(delete.failure.len(), 1);
        assert_point_range(&delete.failure[0], b"k");

        let compact = build_compact_revision_txn(b"123", 7);
        assert_compare(
            &compact.compare[0],
            b"compact_rev_key",
            CompareTarget::Version,
            TargetUnion::Version(7),
        );
        assert_put(&compact.success[0], b"compact_rev_key", b"123");
        assert_eq!(compact.failure.len(), 1);
        assert_point_range(&compact.failure[0], b"compact_rev_key");

        let recovery_probe = build_compact_revision_txn(b"30", 0);
        assert_compare(
            &recovery_probe.compare[0],
            b"compact_rev_key",
            CompareTarget::Version,
            TargetUnion::Version(0),
        );
        assert_put(&recovery_probe.success[0], b"compact_rev_key", b"30");
        assert_point_range(&recovery_probe.failure[0], b"compact_rev_key");
    }

    #[test]
    fn test_compact_conflict_fields() {
        let response = conflict(9, b"123", 4);
        assert_eq!(failure_version(&response), Some(4));
        assert_eq!(failure_value(&response), Some(&b"123"[..]));
        assert_eq!(failure_version(&TxnResponse::default()), None);
        assert_eq!(failure_value(&TxnResponse::default()), None);
    }

    #[test]
    fn test_cas_conflict_retry() {
        let first = TxnAttempt::update(b"k", b"old", 3);
        let mut state = KeyState::Present { mod_revision: 3 };
        let applied = first.apply_response(&conflict(9, b"latest", 4), &mut state);
        assert_eq!(
            applied,
            AppliedTxn::Conflict {
                header_revision: 10,
                latest: KeyState::Present { mod_revision: 9 }
            }
        );
        let started = Instant::now();
        let mut loop_state = CasLoop::new(
            first,
            CasRetryPolicy { max_attempts: 5 },
            started + Duration::from_secs(1),
        );
        let decision = loop_state.observe(&conflict(9, b"latest", 4), started);
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
    fn test_create_collision_no_retry() {
        let attempt = TxnAttempt::create(b"k", b"v");
        let mut loop_state = CasLoop::new(
            attempt,
            CasRetryPolicy::until_deadline(),
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
    fn test_operation_lease_policy() {
        assert!(ResourceOperation::Create.uses_native_lease());
        assert!(ResourceOperation::GuaranteedUpdate.uses_native_lease());
        assert!(!ResourceOperation::ConditionalDelete.uses_native_lease());
    }
    #[test]
    fn test_txn_lease_attachment() {
        for operation in [
            TxnAttempt::create(b"k", b"v"),
            TxnAttempt::update(b"k", b"v", 7),
        ] {
            let request = operation.request_with_lease(42);
            match request.success[0].request.as_ref() {
                Some(request_op::Request::RequestPut(put)) => assert_eq!(put.lease, 42),
                other => panic!("expected Put, got {other:?}"),
            }
            let without_lease = operation.request_with_lease(0);
            match without_lease.success[0].request.as_ref() {
                Some(request_op::Request::RequestPut(put)) => assert_eq!(put.lease, 0),
                other => panic!("expected Put, got {other:?}"),
            }
        }

        let delete = TxnAttempt::delete(b"k", 7).request_with_lease(42);
        assert_eq!(
            delete.compare[0].target_union,
            Some(TargetUnion::ModRevision(7))
        );
        match delete.success[0].request.as_ref() {
            Some(request_op::Request::RequestDeleteRange(delete)) => {
                assert!(delete.range_end.is_empty());
                assert!(!delete.prev_kv);
            }
            other => panic!("expected DeleteRange, got {other:?}"),
        }
        match delete.failure[0].request.as_ref() {
            Some(request_op::Request::RequestRange(range)) => {
                assert_eq!(range.key, b"k");
                assert!(range.range_end.is_empty());
            }
            other => panic!("expected point Range, got {other:?}"),
        }
    }
}
