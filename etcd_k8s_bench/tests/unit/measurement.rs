use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::*;
use crate::report::{RunReport, RunStatus};

struct TestClock {
    start: Instant,
    nanos: AtomicU64,
}

impl TestClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            nanos: AtomicU64::new(0),
        })
    }

    fn set(&self, nanos: u64) {
        self.nanos.store(nanos, Ordering::Release);
    }
}

impl MonotonicClock for TestClock {
    fn now(&self) -> Instant {
        self.start + Duration::from_nanos(self.nanos.load(Ordering::Acquire))
    }
}

fn small_plan_with(update_rps: u64, duration_ms: u64, bucket_ms: u64) -> CompiledPlan {
    let mut config = crate::config::SyntheticConfig::from_json(
        crate::plan::DEFAULT_SYNTHETIC_CONFIG,
        64 * 1024 * 1024,
    )
    .unwrap();
    config.topology.node_count = 1;
    config.topology.pod_count = 2;
    config.topology.namespace_count = 1;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 1;
    config.load.update_rps = update_rps;
    config.load.update_weights.pods = 1;
    config.load.update_weights.deployments = 0;
    config.load.update_weights.replicasets = 0;
    config.load.update_weights.daemonsets = 0;
    config.load.read_ratio.numerator = 0;
    config.load.duration_ms = duration_ms;
    config.report.bucket_ms = bucket_ms;
    CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap()
}

fn small_plan() -> CompiledPlan {
    small_plan_with(70, 100, 10)
}
#[test]
fn test_fractional_watch_coverage_and_zero_window_report() {
    let plan = std::sync::Arc::new(small_plan());
    let build_report = |clock: Arc<TestClock>, cutoff_ns: u64| {
        let recorder = MeasurementRecorder::with_clock(
            &plan,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        )
        .unwrap();
        record_preparation(&recorder, &plan);
        recorder.start_measurement().unwrap();
        for index in 0..plan.watch_specs().len() {
            recorder.watch_created(index).unwrap();
        }
        clock.set(cutoff_ns);
        recorder.stop_measurement().unwrap();
        let measurement = recorder.finish().unwrap();
        let config = crate::config::RunConfig {
            plan: plan.clone(),
            endpoint: "http://fixture".into(),
            metrics_listen: "127.0.0.1:0".parse().unwrap(),
            max_message_size: 64 * 1024 * 1024,
            setup_started_at: Instant::now(),
        };
        RunReport::from_measurement(&config, RunStatus::Completed, None, None, measurement)
    };

    let fractional = build_report(TestClock::new(), 500_000);
    assert_eq!(fractional.timing.observed_measurement_ms, Some(0.5));
    assert!(
        fractional
            .measurement
            .watch
            .roots
            .iter()
            .all(|root| root.coverage_ratio == Some(1.0))
    );
    assert!(
        fractional
            .measurement
            .watch
            .roots
            .iter()
            .all(|root| root.observable_stream_ms > 0.0)
    );
    assert!(
        fractional
            .measurement
            .watch
            .roots
            .iter()
            .flat_map(|root| root.streams.iter())
            .all(|stream| stream.observable_duration_ms > 0.0)
    );

    let zero = build_report(TestClock::new(), 0);
    assert_eq!(zero.timing.observed_measurement_ms, Some(0.0));
    assert!(
        zero.measurement
            .watch
            .roots
            .iter()
            .all(|root| root.coverage_ratio.is_none())
    );
}
fn record_preparation(recorder: &MeasurementRecorder, plan: &CompiledPlan) {
    let planned = [
        plan.resource_counts.pods,
        plan.resource_counts.nodes,
        plan.resource_counts.node_leases,
        plan.resource_counts.deployments,
        plan.resource_counts.replicasets,
        plan.resource_counts.daemonsets,
        1,
    ];
    for (index, count) in planned.into_iter().enumerate() {
        for _ in 0..count {
            let token = match recorder
                .offer_preparation(preparation_identity(index))
                .unwrap()
            {
                OfferResult::Token(token) => token,
                other => panic!("unexpected preparation offer result: {other:?}"),
            };
            token.accept().unwrap();
            token.started().unwrap();
            token.terminal(Outcome::Success).unwrap();
        }
    }
}

fn make_recorder(clock: &Arc<TestClock>) -> MeasurementRecorder {
    let plan = small_plan();
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder
}

#[test]
fn test_report_accounting() {
    let clock = TestClock::new();
    let recorder = make_recorder(&clock);
    recorder.start_measurement().unwrap();
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };
    let accepted = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    accepted.accept().unwrap();
    accepted.started().unwrap();
    accepted.terminal(Outcome::Success).unwrap();
    let failed = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    failed.accept().unwrap();
    failed.started().unwrap();
    failed.terminal(Outcome::Timeout).unwrap();
    let unknown = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    unknown.accept().unwrap();
    unknown.started().unwrap();
    unknown.terminal(Outcome::ResultUnknown).unwrap();
    let dropped = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    dropped.drop_for(DropReason::Backpressure).unwrap();
    let canceled_queued = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    canceled_queued.accept().unwrap();
    drop(canceled_queued);
    let canceled_running = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    canceled_running.accept().unwrap();
    canceled_running.started().unwrap();
    drop(canceled_running);
    recorder.record_missed_schedule(stream, 1).unwrap();
    clock.set(100_000_000);
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();
    let row = report
        .measurement
        .logical
        .iter()
        .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
        .unwrap();
    assert_eq!(row.accepted, 5);
    assert_eq!(row.succeeded_before_cutoff, 1);
    assert_eq!(row.failed_before_cutoff, 2);
    assert_eq!(row.timeout_before_cutoff, 1);
    assert_eq!(row.result_unknown_before_cutoff, 1);
    assert_eq!(row.dropped_full, 1);
    assert_eq!(row.canceled_queued, 1);
    assert_eq!(row.canceled_running, 1);
    assert_eq!(row.missed_schedule, 1);
    assert_eq!(row.not_offered_after_stop, 0);
    assert!(report.accounting.planned_conserved);
    assert!(report.accounting.offered_conserved);
    assert!(report.accounting.accepted_cohort_conserved);
    let second_clock = TestClock::new();
    let second = make_recorder(&second_clock);
    second.start_measurement().unwrap();
    second.record_missed_schedule(stream, 7).unwrap();
    second_clock.set(100_000_000);
    second.stop_measurement().unwrap();
    let second_report = second.finish().unwrap();
    let second_row = second_report
        .measurement
        .logical
        .iter()
        .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
        .unwrap();
    assert_eq!(second_row.offered, 0);
    assert_eq!(second_row.accepted, 0);
    assert_eq!(second_row.missed_schedule, 7);
    assert!(second_report.accounting.planned_conserved);
}

#[test]
fn test_measurement_window_excludes_preparation_time() {
    let clock = TestClock::new();
    let recorder = make_recorder(&clock);
    clock.set(123_000_000);
    let window = recorder.start_measurement().unwrap();
    assert_eq!(
        window.start.duration_since(clock.start),
        Duration::from_millis(123)
    );
    assert_eq!(
        window.cutoff.duration_since(window.start),
        Duration::from_millis(100)
    );
}

#[test]
fn test_preparation_counters_do_not_pollute_measurement() {
    let clock = TestClock::new();
    let plan = small_plan();
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder.start_measurement().unwrap();
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();

    assert!(
        report
            .preparation
            .logical
            .iter()
            .any(|row| row.succeeded_before_cutoff > 0)
    );
    assert!(
        report
            .measurement
            .logical
            .iter()
            .all(|row| row.started_before_cutoff == 0 && row.succeeded_before_cutoff == 0)
    );
    assert!(
        report
            .measurement
            .rpc
            .iter()
            .all(|row| row.started == 0 && row.completed == 0)
    );
}
#[test]
fn test_preparation_disconnect_clears_historical_ready() {
    let clock = TestClock::new();
    let plan = small_plan();
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    for index in 0..plan.watch_specs().len() {
        recorder.watch_created(index).unwrap();
    }

    // The first stream disconnects during preparation and does not
    // reconnect before the measurement window starts.
    clock.set(10_000_000);
    recorder.watch_disconnect(0).unwrap();
    clock.set(20_000_000);
    recorder.start_measurement().unwrap();
    clock.set(120_000_000);
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();

    let stream = report
        .watch
        .roots
        .iter()
        .flat_map(|root| root.streams.iter())
        .find(|stream| stream.stream == "pods")
        .expect("pods watch report");
    assert!(!stream.ready);
    assert_eq!(stream.observable_duration_ms, 0.0);
    assert!(!stream.observation_complete);
    assert!(!report.watch.observation_complete);
}

#[test]
fn test_drain_timing_excludes_watch_teardown() {
    let clock = TestClock::new();
    let recorder = make_recorder(&clock);
    recorder.start_measurement().unwrap();

    clock.set(100_000_000);
    recorder.stop_measurement().unwrap();
    clock.set(150_000_000);
    recorder.begin_watch_teardown().unwrap();
    clock.set(350_000_000);
    recorder.end_watch_teardown().unwrap();
    clock.set(900_000_000);
    let report = recorder.finish().unwrap();

    assert_eq!(report.timing.drain_ns, Some(50_000_000));
    assert_eq!(report.timing.watch_teardown_ns, Some(200_000_000));
}

#[test]
fn test_cutoff_classify() {
    let clock = TestClock::new();
    let plan = small_plan_with(10, 100, 10);
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder.start_measurement().unwrap();
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };
    let token = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    token.accept().unwrap();
    clock.set(10_000_000);
    recorder.stop_measurement().unwrap();
    clock.set(10_000_001);
    token.started().unwrap();
    token.terminal(Outcome::Success).unwrap();
    let report = recorder.finish().unwrap();
    let row = report
        .measurement
        .logical
        .iter()
        .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
        .unwrap();
    assert_eq!(report.cutoff.effective_ns, Some(10_000_000));
    assert_eq!(row.started_before_cutoff, 0);
    assert_eq!(row.started_after_cutoff, 1);
    assert_eq!(row.terminal_after_cutoff, 1);
    assert_eq!(row.cutoff_queued, 1);
    assert_eq!(row.cutoff_running, 0);
    assert!(report.accounting.planned_conserved);
    assert!(report.accounting.accepted_cohort_conserved);
}
#[test]
fn test_rpc_payload_and_completion_phase_cross_cutoff() {
    let clock = TestClock::new();
    let plan = small_plan_with(10, 100, 10);
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder.start_measurement().unwrap();

    clock.set(99_000_000);
    let attempt = recorder.start_rpc_with_payload(RpcMethod::Txn, 11).unwrap();
    clock.set(100_000_000);
    assert_eq!(
        recorder.stop_measurement().unwrap().effective_ns,
        100_000_000
    );
    clock.set(101_000_000);
    attempt.finish(Outcome::Success, 13).unwrap();

    let report = recorder.finish().unwrap();
    let measurement_rpc = &report.measurement.rpc[RpcMethod::Txn.index()];
    let drain_rpc = &report.drain.rpc[RpcMethod::Txn.index()];
    assert_eq!(measurement_rpc.started, 1);
    assert_eq!(measurement_rpc.completed, 0);
    assert_eq!(measurement_rpc.request_payload_bytes, 11);
    assert_eq!(measurement_rpc.response_payload_bytes, 0);
    assert_eq!(drain_rpc.started, 0);
    assert_eq!(drain_rpc.completed, 1);
    assert_eq!(drain_rpc.request_payload_bytes, 0);
    assert_eq!(drain_rpc.response_payload_bytes, 13);
    assert_eq!(drain_rpc.payload_bytes, 13);
    assert_eq!(drain_rpc.latency.sample_count, 1);
}

#[test]
fn test_normal_cutoff_rejection_keeps_remaining_slots_missed() {
    let clock = TestClock::new();
    let mut config = crate::config::SyntheticConfig::from_json(
        crate::plan::DEFAULT_SYNTHETIC_CONFIG,
        64 * 1024 * 1024,
    )
    .unwrap();
    config.topology.node_count = 1;
    config.topology.pod_count = 2;
    config.topology.namespace_count = 1;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 1;
    config.load.duration_ms = 10;
    config.load.update_rps = 100;
    config.load.update_weights.pods = 1;
    config.load.update_weights.deployments = 0;
    config.load.update_weights.replicasets = 0;
    config.load.update_weights.daemonsets = 0;
    config.load.read_ratio.numerator = 0;
    config.periodic.node_lease_interval_ms = 1;
    config.periodic.node_lease_jitter_percent = 0;
    config.periodic.node_status_interval_ms = 10;
    config.periodic.status_interval_ms = 10;
    config.periodic.compact_interval_ms = 10;
    config.report.bucket_ms = 1;
    let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder.start_measurement().unwrap();
    let application = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };
    let periodic = LogicalStreamIdentity::Periodic {
        kind: PeriodicKind::NodeLease,
    };
    let application_token = match recorder.offer(application).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected application offer: {other:?}"),
    };
    let periodic_token = match recorder.offer(periodic).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected periodic offer: {other:?}"),
    };
    clock.set(10_000_000);
    assert_eq!(
        application_token.accept().unwrap(),
        TokenAction::RejectedAfterCutoff
    );
    assert_eq!(
        periodic_token.accept().unwrap(),
        TokenAction::RejectedAfterCutoff
    );
    let application_planned = plan
        .logical_streams
        .iter()
        .find(|stream| {
            stream.work_class == LogicalWorkClass::Update
                && stream.resource == ResourceKind::Pods
        })
        .unwrap()
        .planned;
    let periodic_planned = plan
        .periodic_plan
        .iter()
        .find(|stream| stream.kind == PeriodicKind::NodeLease)
        .unwrap()
        .planned;
    recorder
        .record_missed_schedule_before_cutoff(
            application,
            application_planned.saturating_sub(1),
        )
        .unwrap();
    recorder
        .record_missed_schedule_before_cutoff(periodic, periodic_planned.saturating_sub(1))
        .unwrap();
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();
    for identity in [application, periodic] {
        let row = &report.measurement.logical[identity.measurement_index().unwrap()];
        assert_eq!(row.not_offered_after_stop, 0);
        assert_eq!(row.planned, row.missed_schedule);
        assert_eq!(row.offered, 0);
    }
    assert!(report.accounting.planned_conserved);
}

#[test]
fn test_histogram_overflow() {
    let clock = TestClock::new();
    let plan = small_plan_with(10, 100, 10);
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder.start_measurement().unwrap();
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };
    let token = match recorder.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("unexpected offer result: {other:?}"),
    };
    token.accept().unwrap();
    clock.set(100_000_000);
    recorder.stop_measurement().unwrap();
    clock.set(20_000_000_000);
    token.started().unwrap();
    token.terminal(Outcome::Success).unwrap();
    let report = recorder.finish().unwrap();
    let row = report
        .drain
        .logical
        .iter()
        .find(|row| row.work_class == "update" && row.resource.as_deref() == Some("pods"))
        .unwrap();
    assert!(row.logical_latency.overflow > 0);
    assert_eq!(row.logical_latency.exact_max_us, Some(20_000_000));
    assert!(row.logical_latency.p99_us.is_none());
    assert!(row.logical_latency.quantiles_unavailable);
    assert!(report.accounting.planned_conserved);
    assert!(report.accounting.accepted_cohort_conserved);
}
#[test]
fn test_cutoff_boundaries() {
    let cutoff_ns = 100_000_000;
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };

    for (label, at_ns, admitted) in [
        ("before", cutoff_ns - 1, true),
        ("exact", cutoff_ns, false),
        ("after", cutoff_ns + 1, false),
    ] {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder = MeasurementRecorder::with_clock(
            &plan,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        )
        .unwrap();
        recorder.start_measurement().unwrap();

        clock.set(at_ns);
        let offered = recorder.offer(stream).unwrap();
        if admitted {
            let token = match offered {
                OfferResult::Token(token) => token,
                other => panic!("{label} offer was not admitted: {other:?}"),
            };
            token.accept().unwrap();
            token.started().unwrap();
            token.terminal(Outcome::Success).unwrap();
        } else {
            assert!(matches!(offered, OfferResult::MissedSchedule));
        }

        if at_ns < cutoff_ns {
            clock.set(cutoff_ns);
        }
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[stream.measurement_index().unwrap()];
        assert_eq!(report.cutoff.effective_ns, Some(cutoff_ns), "{label}");
        if admitted {
            assert_eq!(row.offered, 1, "{label}");
            assert_eq!(row.accepted, 1, "{label}");
            assert_eq!(row.started_before_cutoff, 1, "{label}");
            assert_eq!(row.succeeded_before_cutoff, 1, "{label}");
            assert_eq!(row.missed_schedule, 0, "{label}");
        } else {
            assert_eq!(row.offered, 0, "{label}");
            assert_eq!(row.accepted, 0, "{label}");
            assert_eq!(row.started_before_cutoff, 0, "{label}");
            assert_eq!(row.missed_schedule, 1, "{label}");
        }
    }

    for (label, at_ns, before_cutoff) in [
        ("before", cutoff_ns - 1, true),
        ("exact", cutoff_ns, false),
        ("after", cutoff_ns + 1, false),
    ] {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder = MeasurementRecorder::with_clock(
            &plan,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        )
        .unwrap();
        recorder.start_measurement().unwrap();
        let token = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("{label} lifecycle offer failed: {other:?}"),
        };
        token.accept().unwrap();
        clock.set(at_ns);
        token.started().unwrap();
        token.terminal(Outcome::Success).unwrap();
        if at_ns < cutoff_ns {
            clock.set(cutoff_ns);
        }
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let measurement = &report.measurement.logical[stream.measurement_index().unwrap()];
        let drain = &report.drain.logical[stream.measurement_index().unwrap()];
        if before_cutoff {
            assert_eq!(measurement.started_before_cutoff, 1, "{label}");
            assert_eq!(measurement.succeeded_before_cutoff, 1, "{label}");
            assert_eq!(measurement.terminal_after_cutoff, 0, "{label}");
            assert_eq!(drain.started_after_cutoff, 0, "{label}");
            assert_eq!(drain.terminal_after_cutoff, 0, "{label}");
        } else {
            assert_eq!(measurement.started_before_cutoff, 0, "{label}");
            assert_eq!(measurement.succeeded_before_cutoff, 0, "{label}");
            assert_eq!(measurement.terminal_after_cutoff, 1, "{label}");
            assert_eq!(drain.started_after_cutoff, 1, "{label}");
            assert_eq!(drain.terminal_after_cutoff, 1, "{label}");
        }
    }

    for (label, at_ns, completed_before_cutoff) in [
        ("before", cutoff_ns - 1, true),
        ("exact", cutoff_ns, false),
        ("after", cutoff_ns + 1, false),
    ] {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder = MeasurementRecorder::with_clock(
            &plan,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        )
        .unwrap();
        recorder.start_measurement().unwrap();
        let attempt = recorder.start_rpc_with_payload(RpcMethod::Txn, 7).unwrap();
        clock.set(at_ns);
        attempt.finish(Outcome::Success, 9).unwrap();
        if at_ns < cutoff_ns {
            clock.set(cutoff_ns);
        }
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let measurement = &report.measurement.rpc[RpcMethod::Txn.index()];
        let drain = &report.drain.rpc[RpcMethod::Txn.index()];
        assert_eq!(measurement.started, 1, "{label}");
        if completed_before_cutoff {
            assert_eq!(measurement.completed, 1, "{label}");
            assert_eq!(drain.completed, 0, "{label}");
        } else {
            assert_eq!(measurement.completed, 0, "{label}");
            assert_eq!(drain.started, 0, "{label}");
            assert_eq!(drain.completed, 1, "{label}");
            assert_eq!(drain.response_payload_bytes, 9, "{label}");
        }
    }
    for (label, at_ns, admitted) in [
        ("before", cutoff_ns - 1, true),
        ("exact", cutoff_ns, false),
        ("after", cutoff_ns + 1, false),
    ] {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder = MeasurementRecorder::with_clock(
            &plan,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        )
        .unwrap();
        recorder.start_measurement().unwrap();
        let token = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("{label} final-admission offer failed: {other:?}"),
        };
        clock.set(at_ns);
        let action = token.accept_for_tick(cutoff_ns).unwrap();
        if admitted {
            assert_eq!(action, TokenAction::Applied, "{label}");
            token.started().unwrap();
            token.terminal(Outcome::Success).unwrap();
        } else {
            assert_eq!(action, TokenAction::RejectedAfterCutoff, "{label}");
        }

        if at_ns < cutoff_ns {
            clock.set(cutoff_ns);
        }
        recorder.stop_measurement().unwrap();
        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[stream.measurement_index().unwrap()];
        assert_eq!(report.cutoff.effective_ns, Some(cutoff_ns), "{label}");
        if admitted {
            assert_eq!(row.offered, 1, "{label}");
            assert_eq!(row.accepted, 1, "{label}");
            assert_eq!(row.succeeded_before_cutoff, 1, "{label}");
            assert_eq!(row.missed_schedule, 0, "{label}");
        } else {
            assert_eq!(row.offered, 0, "{label}");
            assert_eq!(row.accepted, 0, "{label}");
            assert_eq!(row.missed_schedule, 1, "{label}");
        }
    }
}

#[test]
fn test_cutoff_queued_running() {
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };
    for running in [false, true] {
        let clock = TestClock::new();
        let plan = small_plan_with(10, 100, 10);
        let recorder = MeasurementRecorder::with_clock(
            &plan,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        )
        .unwrap();
        recorder.start_measurement().unwrap();
        let token = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("cutoff token was not offered: {other:?}"),
        };
        token.accept().unwrap();
        if running {
            token.started().unwrap();
        }
        clock.set(100_000_000);
        recorder.stop_measurement().unwrap();
        drop(token);

        let report = recorder.finish().unwrap();
        let row = &report.measurement.logical[stream.measurement_index().unwrap()];
        assert_eq!(row.accepted, 1);
        assert_eq!(row.canceled_queued, u64::from(!running));
        assert_eq!(row.canceled_running, u64::from(running));
        assert_eq!(row.cutoff_queued, u64::from(!running));
        assert_eq!(row.cutoff_running, u64::from(running));
        assert!(report.accounting.accepted_cohort_conserved);
    }
}

#[test]
fn test_bucket_boundaries_partial() {
    let clock = TestClock::new();
    let plan = small_plan_with(120, 25, 10);
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    record_preparation(&recorder, &plan);
    recorder.start_measurement().unwrap();
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };
    for at_ns in [9_999_999, 10_000_000, 24_999_999] {
        clock.set(at_ns);
        let token = match recorder.offer(stream).unwrap() {
            OfferResult::Token(token) => token,
            other => panic!("bucket offer failed at {at_ns}: {other:?}"),
        };
        token.accept().unwrap();
        token.started().unwrap();
        token.terminal(Outcome::Success).unwrap();
    }
    clock.set(25_000_000);
    recorder.stop_measurement().unwrap();
    let report = recorder.finish().unwrap();

    assert_eq!(report.buckets.len(), 3);
    assert_eq!(
        report
            .buckets
            .iter()
            .map(|bucket| (bucket.start_ns, bucket.end_ns))
            .collect::<Vec<_>>(),
        vec![
            (0, 10_000_000),
            (10_000_000, 20_000_000),
            (20_000_000, 25_000_000),
        ]
    );
    for bucket in &report.buckets {
        assert_eq!(bucket.logical_offered, 1);
        assert_eq!(bucket.logical_started, 1);
        assert_eq!(bucket.logical_completed, 1);
        assert_eq!(bucket.logical_errors, 0);
    }
    assert!(report.accounting.measurement_complete);
}

#[test]
fn test_run_isolation_keeps_observations() {
    let mut config = small_plan_with(100, 10, 10).config;
    config.execution.drain_timeout_ms = 1;
    let plan = Arc::new(CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap());
    let stream = LogicalStreamIdentity::Application {
        work_class: LogicalWorkClass::Update,
        resource: ResourceKind::Pods,
    };

    let clock_a = TestClock::new();
    let recorder_a =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock_a) as Arc<dyn MonotonicClock>)
            .unwrap();
    recorder_a.start_measurement().unwrap();
    recorder_a.watch_created(0).unwrap();
    let token_a = match recorder_a.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("run A offer failed: {other:?}"),
    };
    token_a.accept().unwrap();
    let rpc_a = recorder_a
        .start_rpc_with_payload(RpcMethod::Txn, 11)
        .unwrap();
    clock_a.set(1_000_000);
    recorder_a
        .watch_response(
            0,
            &crate::pb::etcdserverpb::WatchResponse {
                events: vec![crate::pb::mvccpb::Event {
                    kv: Some(crate::pb::mvccpb::KeyValue {
                        key: b"run-a".to_vec(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .unwrap();
    clock_a.set(10_000_000);
    recorder_a.stop_measurement().unwrap();
    clock_a.set(1_000_000_000);
    token_a.started().unwrap();
    token_a
        .terminal_with_error(Outcome::GrpcError, 41, "run-a failure")
        .unwrap();
    rpc_a.finish(Outcome::ResultUnknown, 13).unwrap();
    let report_a = recorder_a.finish().unwrap();

    let clock_b = TestClock::new();
    let recorder_b =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock_b) as Arc<dyn MonotonicClock>)
            .unwrap();
    recorder_b.start_measurement().unwrap();
    let token_b = match recorder_b.offer(stream).unwrap() {
        OfferResult::Token(token) => token,
        other => panic!("run B offer failed: {other:?}"),
    };
    token_b.accept().unwrap();
    token_b.started().unwrap();
    token_b.terminal(Outcome::Success).unwrap();
    let rpc_b = recorder_b
        .start_rpc_with_payload(RpcMethod::Status, 3)
        .unwrap();
    rpc_b.finish(Outcome::Success, 4).unwrap();
    clock_b.set(10_000_000);
    recorder_b.stop_measurement().unwrap();
    let report_b = recorder_b.finish().unwrap();

    let row_a = &report_a.measurement.logical[stream.measurement_index().unwrap()];
    let row_b = &report_b.measurement.logical[stream.measurement_index().unwrap()];
    assert_eq!(row_a.accepted, 1);
    assert_eq!(row_a.failed_after_cutoff, 1);
    assert_eq!(row_a.first_error.as_deref(), Some("run-a failure"));
    assert_eq!(row_b.accepted, 1);
    assert_eq!(row_b.succeeded_before_cutoff, 1);
    assert!(row_b.first_error.is_none());

    let txn_a = &report_a.measurement.rpc[RpcMethod::Txn.index()];
    let txn_a_drain = &report_a.drain.rpc[RpcMethod::Txn.index()];
    assert_eq!(txn_a.request_payload_bytes, 11);
    assert_eq!(txn_a.completed, 0);
    assert_eq!(txn_a_drain.response_payload_bytes, 13);
    assert_eq!(txn_a_drain.result_unknown, 1);
    let status_b = &report_b.measurement.rpc[RpcMethod::Status.index()];
    assert_eq!(status_b.started, 1);
    assert_eq!(status_b.completed, 1);
    assert_eq!(status_b.succeeded, 1);
    assert_eq!(status_b.failed, 0);
    assert_eq!(status_b.timeout, 0);
    assert_eq!(status_b.result_unknown, 0);
    assert_eq!(status_b.payload_bytes, 7);
    assert_eq!(status_b.request_payload_bytes, 3);
    assert_eq!(status_b.response_payload_bytes, 4);
    assert_eq!(status_b.latency.sample_count, 1);
    let txn_b = &report_b.measurement.rpc[RpcMethod::Txn.index()];
    let txn_b_drain = &report_b.drain.rpc[RpcMethod::Txn.index()];
    for txn in [txn_b, txn_b_drain] {
        assert_eq!(txn.started, 0);
        assert_eq!(txn.completed, 0);
        assert_eq!(txn.succeeded, 0);
        assert_eq!(txn.failed, 0);
        assert_eq!(txn.timeout, 0);
        assert_eq!(txn.result_unknown, 0);
        assert_eq!(txn.payload_bytes, 0);
        assert_eq!(txn.request_payload_bytes, 0);
        assert_eq!(txn.response_payload_bytes, 0);
        assert_eq!(txn.latency.sample_count, 0);
    }
    assert_eq!(report_b.measurement.rpc_total.sample_count, 1);
    assert_eq!(report_b.drain.rpc_total.sample_count, 0);
    assert_eq!(
        report_b
            .buckets
            .iter()
            .map(|bucket| bucket.rpc_started)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        report_b
            .buckets
            .iter()
            .map(|bucket| bucket.rpc_completed)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        report_b
            .buckets
            .iter()
            .map(|bucket| bucket.txn_started)
            .sum::<u64>(),
        0
    );
    assert_eq!(
        report_b
            .buckets
            .iter()
            .map(|bucket| bucket.txn_completed)
            .sum::<u64>(),
        0
    );

    assert!(report_a.accounting.histogram_overflow);
    assert!(!report_b.accounting.histogram_overflow);
    assert_eq!(report_a.buckets[0].watch_events, 1);
    assert_eq!(report_b.buckets[0].watch_events, 0);
    assert_eq!(
        report_b
            .buckets
            .iter()
            .map(|bucket| bucket.watch_events)
            .sum::<u64>(),
        0
    );
    assert_eq!(
        report_b
            .buckets
            .iter()
            .map(|bucket| bucket.watch_payload_bytes)
            .sum::<u64>(),
        0
    );
    let watch_a = report_a
        .watch
        .roots
        .iter()
        .flat_map(|root| root.streams.iter())
        .find(|stream| stream.stream == "pods")
        .unwrap();
    let watch_b = report_b
        .watch
        .roots
        .iter()
        .flat_map(|root| root.streams.iter())
        .find(|stream| stream.stream == "pods")
        .unwrap();
    assert_eq!(watch_a.events, 1);
    assert_eq!(watch_b.events, 0);
}
#[test]
fn test_watch_subscriptions_preserve_gap_coverage() {
    let mut config = small_plan_with(10, 100, 10).config;
    config.watch.subscriptions_per_root = 2;
    let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    let clock = TestClock::new();
    let recorder =
        MeasurementRecorder::with_clock(&plan, Arc::clone(&clock) as Arc<dyn MonotonicClock>)
            .unwrap();
    recorder.start_measurement().unwrap();
    for index in 0..plan.watch_specs().len() {
        recorder.watch_created(index).unwrap();
    }

    let event = |revision| crate::pb::etcdserverpb::WatchResponse {
        header: Some(crate::pb::etcdserverpb::ResponseHeader {
            revision,
            ..Default::default()
        }),
        events: vec![crate::pb::mvccpb::Event {
            kv: Some(crate::pb::mvccpb::KeyValue {
                mod_revision: revision,
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    };
    clock.set(10_000_000);
    recorder.watch_response(0, &event(11)).unwrap();
    clock.set(20_000_000);
    recorder.watch_disconnect(0).unwrap();
    clock.set(30_000_000);
    recorder.watch_created(0).unwrap();
    clock.set(40_000_000);
    recorder.watch_terminal(1, WATCH_TERMINAL_SEMANTIC).unwrap();
    clock.set(50_000_000);
    recorder.watch_response(0, &event(12)).unwrap();
    clock.set(100_000_000);
    recorder.stop_measurement().unwrap();
    clock.set(150_000_000);
    recorder.watch_disconnect(0).unwrap();
    let report = recorder.finish().unwrap();

    let pods = report
        .watch
        .roots
        .iter()
        .find(|root| root.root == "/registry/pods")
        .unwrap();
    assert_eq!(pods.expected_streams, 2);
    assert_eq!(pods.ready_streams, 2);
    assert_eq!(pods.observable_stream_ms, 130.0);
    assert_eq!(pods.observed_measurement_ms, 100.0);
    assert!(
        (pods.coverage_ratio.unwrap() - 0.65).abs() < 1e-9,
        "unexpected pod coverage: {:?}",
        pods.coverage_ratio
    );
    assert!(!pods.observation_complete);
    let pods_zero = pods
        .streams
        .iter()
        .find(|stream| stream.stream == "pods-0")
        .unwrap();
    assert_eq!(pods_zero.disconnects, 2);
    assert_eq!(pods_zero.events, 2);
    assert!(!pods_zero.observation_complete);
    let pods_one = pods
        .streams
        .iter()
        .find(|stream| stream.stream == "pods-1")
        .unwrap();
    assert_eq!(pods_one.terminal.as_deref(), Some("semantic"));
    assert_eq!(pods_one.events, 0);
    assert!(!pods_one.observation_complete);

    let minions = report
        .watch
        .roots
        .iter()
        .find(|root| root.root == "/registry/minions")
        .unwrap();
    assert_eq!(minions.expected_streams, 2);
    assert_eq!(minions.coverage_ratio, Some(1.0));
    assert!(minions.observation_complete);
    assert!(!report.watch.observation_complete);
}
