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
