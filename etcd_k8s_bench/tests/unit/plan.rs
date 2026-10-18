use std::collections::HashSet;

use super::*;

fn small_config() -> SyntheticConfig {
    let mut config = default_config();
    config.topology.node_count = 4;
    config.topology.pod_count = 8;
    config.topology.namespace_count = 3;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 4;
    config.load.duration_ms = 10;
    config.load.list_limit = 7;
    config.report.bucket_ms = 1;
    config
}

#[test]
fn test_keys_unique() {
    let plan = CompiledPlan::compile(small_config(), 64 * 1024 * 1024).unwrap();
    let mut all_keys = HashSet::new();
    for kind in ResourceKind::ALL {
        let resource = plan.resource(kind);
        let keys: HashSet<_> = (0..resource.count).map(|id| resource.key(id)).collect();
        assert_eq!(keys.len(), usize::try_from(resource.count).unwrap());
        all_keys.extend(keys);
        let first = resource.value(0, 1);
        assert_eq!(
            first.len(),
            usize::try_from(resource.value_size_bytes).unwrap()
        );
        assert_eq!(first, resource.value(0, 1));
        assert_ne!(first, resource.value(0, 2));
    }
    assert_eq!(
        all_keys.len(),
        usize::try_from(plan.resource_counts.total()).unwrap()
    );
    assert_eq!(
        plan.resource(ResourceKind::Nodes).key(0),
        b"/registry/minions/node-00000"
    );
    assert_eq!(
        plan.resource(ResourceKind::NodeLeases).key(0),
        b"/registry/leases/kube-node-lease/node-00000"
    );

    let pod_namespaces: HashSet<_> = (0..plan.resource(ResourceKind::Pods).count)
        .filter_map(|id| plan.resource(ResourceKind::Pods).namespace(id))
        .collect();
    assert_eq!(
        pod_namespaces,
        [
            "bench-ns-00000".to_string(),
            "bench-ns-00001".to_string(),
            "bench-ns-00002".to_string()
        ]
        .into_iter()
        .collect()
    );
    let deployment_namespaces: HashSet<_> = (0..plan.resource(ResourceKind::Deployments).count)
        .filter_map(|id| plan.resource(ResourceKind::Deployments).namespace(id))
        .collect();
    assert_eq!(
        deployment_namespaces,
        ["bench-ns-00000".to_string()].into_iter().collect()
    );
}

#[test]
fn test_get_full_range() {
    let mut config = small_config();
    // Exercise the five-digit key boundary rather than only the first
    // few IDs. The controller relation remains valid: (1204 - 4) / 4.
    config.topology.pod_count = 1204;
    let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    for kind in ResourceKind::ALL {
        let resource = plan.resource(kind);
        let expected: HashSet<_> = (0..resource.count).map(|id| resource.key(id)).collect();
        let actual: HashSet<_> = (0..resource.count)
            .map(|ordinal| plan.point_get_request(kind, ordinal, 91).key)
            .collect();
        assert_eq!(actual, expected);
        assert!((0..resource.count).all(|ordinal| {
            let request = plan.point_get_request(kind, ordinal, 91);
            request.range_end.is_empty() && request.limit == 0
        }));
    }
    assert_ne!(
        plan.point_get_request(ResourceKind::Pods, 1_000, 91).key,
        plan.point_get_request(ResourceKind::Pods, 0, 91).key
    );
}

#[test]
fn test_list_limit() {
    let plan = CompiledPlan::compile(small_config(), 64 * 1024 * 1024).unwrap();
    let mut pod_namespaces = HashSet::new();
    for ordinal in 0..12 {
        let request = plan.list_request(ResourceKind::Pods, ordinal, 17).unwrap();
        assert_eq!(request.limit, 7);
        assert_eq!(request.range_end, prefix_range_end(&request.key));
        pod_namespaces.insert(String::from_utf8(request.key).unwrap());
    }
    assert_eq!(pod_namespaces.len(), 3);
    assert!(pod_namespaces.iter().all(|prefix| {
        ["bench-ns-00000", "bench-ns-00001", "bench-ns-00002"]
            .iter()
            .copied()
            .any(|namespace| prefix.contains(namespace))
    }));

    let mut deployment_prefixes = HashSet::new();
    for ordinal in 0..12 {
        let request = plan
            .list_request(ResourceKind::Deployments, ordinal, 17)
            .unwrap();
        deployment_prefixes.insert(request.key);
    }
    assert_eq!(
        deployment_prefixes,
        [b"/registry/deployments/bench-ns-00000/".to_vec()]
            .into_iter()
            .collect()
    );

    let nodes = plan.list_request(ResourceKind::Nodes, 0, 17).unwrap();
    assert_eq!(nodes.key, b"/registry/minions/");
    assert_eq!(nodes.range_end, prefix_range_end(b"/registry/minions/"));
}

#[test]
fn test_watch_specs() {
    let plan = CompiledPlan::compile(small_config(), 64 * 1024 * 1024).unwrap();
    let specs = plan.watch_specs();
    assert_eq!(specs.len(), ResourceKind::ALL.len());
    assert_eq!(
        specs
            .iter()
            .map(|spec| spec.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "pods",
            "nodes",
            "node_leases",
            "deployments",
            "replicasets",
            "daemonsets"
        ]
    );
    assert_eq!(
        specs
            .iter()
            .map(|spec| spec.resource_prefix.as_str())
            .collect::<Vec<_>>(),
        vec![
            "/registry/pods",
            "/registry/minions",
            "/registry/leases/kube-node-lease",
            "/registry/deployments",
            "/registry/replicasets",
            "/registry/daemonsets"
        ]
    );
    assert!(
        specs
            .iter()
            .all(|spec| { spec.start_revision == 0 && spec.prev_kv && spec.progress_notify })
    );
    let mut multi = small_config();
    multi.watch.subscriptions_per_root = 2;
    let multi_plan = CompiledPlan::compile(multi, 64 * 1024 * 1024).unwrap();
    let multi_specs = multi_plan.watch_specs();
    assert_eq!(multi_specs.len(), 2 * ResourceKind::ALL.len());
    assert_eq!(
        multi_specs
            .iter()
            .map(|spec| spec.name.as_str())
            .collect::<HashSet<_>>()
            .len(),
        multi_specs.len()
    );
}

#[test]
fn test_heartbeat_item_mapping() {
    let plan = CompiledPlan::compile(small_config(), 64 * 1024 * 1024).unwrap();
    let lease = PeriodicEvent {
        at_ns: 12_345,
        kind: PeriodicKind::NodeLease,
        node: Some(2),
        ordinal: 4,
    };
    let item = plan.heartbeat_item(&lease).unwrap();
    assert_eq!(item.kind, HeartbeatItemKind::NodeLeaseRenew);
    assert_eq!(item.node, 2);
    assert_eq!(
        item.key,
        String::from_utf8(plan.resource(ResourceKind::NodeLeases).key(2)).unwrap()
    );
    assert_eq!(item.at, Duration::from_nanos(12_345));

    let status = PeriodicEvent {
        kind: PeriodicKind::Status,
        ..lease
    };
    assert!(plan.heartbeat_item(&status).is_none());
}
#[test]
fn test_compiled_node_lease_jitter_bounds() {
    let mut config = default_config();
    config.topology.node_count = 1;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 1;
    config.topology.pod_count = 2;
    config.topology.namespace_count = 1;
    config.load.duration_ms = 31_000;
    config.periodic.node_lease_interval_ms = 10_000;
    config.periodic.node_lease_jitter_percent = 5;
    config.periodic.node_status_interval_ms = 300_000;
    config.periodic.status_interval_ms = 300_000;
    config.periodic.compact_interval_ms = 300_000;
    let first = CompiledPlan::compile(config.clone(), 64 * 1024 * 1024).unwrap();
    let second = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    assert_eq!(first.periodic_events, second.periodic_events);
    let leases: Vec<_> = first
        .periodic_events
        .iter()
        .filter(|event| event.kind == PeriodicKind::NodeLease && event.node == Some(0))
        .collect();
    assert_eq!(leases.len(), 3);
    let mut previous = 0;
    for (ordinal, event) in leases.iter().enumerate() {
        let interval = event.at_ns - previous;
        assert!(
            (9_500_000_000..=10_500_000_000).contains(&interval),
            "lease {ordinal} interval {interval}ns is outside the five-percent cadence"
        );
        assert_eq!(event.ordinal, ordinal as u64);
        previous = event.at_ns;
    }
}

#[test]
fn test_compiled_node_status_phase_spacing() {
    let mut config = default_config();
    config.topology.node_count = 4;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 1;
    config.topology.pod_count = 5;
    config.topology.namespace_count = 1;
    config.load.duration_ms = 1_000;
    config.periodic.node_lease_interval_ms = 300_000;
    config.periodic.node_status_interval_ms = 400;
    config.periodic.status_interval_ms = 300_000;
    config.periodic.compact_interval_ms = 300_000;
    let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    for node in 0..4 {
        let events: Vec<_> = plan
            .periodic_events
            .iter()
            .filter(|event| event.kind == PeriodicKind::NodeStatus && event.node == Some(node))
            .collect();
        assert!(!events.is_empty());
        assert_eq!(events[0].at_ns, (node + 1) * 100_000_000);
        for (ordinal, window) in events.windows(2).enumerate() {
            assert_eq!(window[0].ordinal, ordinal as u64);
            assert_eq!(window[1].ordinal, (ordinal + 1) as u64);
            assert_eq!(window[1].at_ns - window[0].at_ns, 400_000_000);
        }
    }
}

fn default_config() -> SyntheticConfig {
    SyntheticConfig::from_json(DEFAULT_SYNTHETIC_CONFIG, 64 * 1024 * 1024).unwrap()
}

#[test]
fn test_plan_counts() {
    let plan = CompiledPlan::compile(default_config(), 64 * 1024 * 1024).unwrap();
    assert!(
        plan.logical_streams
            .iter()
            .all(|stream| stream.resource != ResourceKind::NodeLeases)
    );
    assert_eq!(plan.resource_counts.total(), 44_410);
    assert_eq!(
        plan.resource_counts,
        SyntheticResourceCounts {
            pods: 40_000,
            nodes: 2_000,
            node_leases: 2_000,
            deployments: 200,
            replicasets: 200,
            daemonsets: 10,
        }
    );
    assert_eq!(plan.update_rounding_deficit, 0);
    assert_eq!(plan.read_rounding_deficit, 0);
    assert_eq!(
        plan.logical_streams
            .iter()
            .filter(|stream| stream.work_class == LogicalWorkClass::Update)
            .map(|stream| stream.planned)
            .try_fold(0u64, |total, value| total.checked_add(value))
            .unwrap(),
        6_000_000
    );
    assert_eq!(
        plan.logical_streams
            .iter()
            .filter(|stream| stream.work_class != LogicalWorkClass::Update)
            .map(|stream| stream.planned)
            .try_fold(0u64, |total, value| total.checked_add(value))
            .unwrap(),
        300_000
    );
    assert_eq!(
        u64::try_from(plan.periodic_events.len()).unwrap(),
        plan.measurement_layout.periodic_events
    );
    assert!(
        plan.measurement_layout.periodic_capacity >= plan.measurement_layout.periodic_events
    );
    assert!(plan.periodic_events.windows(2).all(|pair| {
        (pair[0].at_ns, pair[0].kind, pair[0].node, pair[0].ordinal)
            <= (pair[1].at_ns, pair[1].kind, pair[1].node, pair[1].ordinal)
    }));
}

#[test]
fn test_plan_rejects_zero_nodes() {
    let mut config = default_config();
    config.topology.node_count = 0;
    let error = CompiledPlan::compile(config, 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("topology.node_count"));
}

#[test]
fn test_plan_rejects_zero_pod_value_size() {
    let mut config = default_config();
    config.value_size_bytes.pods = 0;
    let error = CompiledPlan::compile(config, 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("value_size_bytes.pods"));
}

#[test]
fn test_node_lease_jitter_boundaries() {
    for (jitter, lower, upper) in [
        (0u8, 10_000_000_000, 10_000_000_000),
        (99u8, 100_000_000, 19_900_000_000),
    ] {
        let mut config = default_config();
        config.topology.node_count = 1;
        config.controllers.daemonset_count = 1;
        config.controllers.pods_per_deployment = 1;
        config.topology.pod_count = 2;
        config.topology.namespace_count = 1;
        config.load.duration_ms = 31_000;
        config.periodic.node_lease_interval_ms = 10_000;
        config.periodic.node_lease_jitter_percent = jitter;
        config.periodic.node_status_interval_ms = 300_000;
        config.periodic.status_interval_ms = 300_000;
        config.periodic.compact_interval_ms = 300_000;
        let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
        let leases: Vec<_> = plan
            .periodic_events
            .iter()
            .filter(|event| event.kind == PeriodicKind::NodeLease && event.node == Some(0))
            .collect();
        assert!(
            !leases.is_empty(),
            "jitter {jitter} produced no lease events"
        );
        let mut previous = 0;
        for (ordinal, event) in leases.iter().enumerate() {
            let interval = event.at_ns - previous;
            assert!(
                (lower..=upper).contains(&interval),
                "jitter {jitter} lease {ordinal} interval {interval}ns outside bounds"
            );
            previous = event.at_ns;
        }
    }

    let mut invalid = default_config();
    invalid.periodic.node_lease_jitter_percent = 100;
    let error = CompiledPlan::compile(invalid, 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("0..99"));
}

#[test]
fn test_plan_deterministic() {
    let first = CompiledPlan::compile(default_config(), 64 * 1024 * 1024).unwrap();
    let second = CompiledPlan::compile(default_config(), 64 * 1024 * 1024).unwrap();
    assert_eq!(first.resource_counts, second.resource_counts);
    assert_eq!(first.resources, second.resources);
    assert_eq!(first.logical_streams, second.logical_streams);
    assert_eq!(first.periodic_events, second.periodic_events);
    assert_eq!(first.periodic_plan, second.periodic_plan);
    assert_eq!(first.periodic_indexes, second.periodic_indexes);
    assert_eq!(first.measurement_layout, second.measurement_layout);
}
#[test]
fn test_run_config_clone_shares_periodic_storage() {
    let mut synthetic = small_config();
    synthetic.load.duration_ms = 500;
    synthetic.periodic.node_lease_interval_ms = 100;
    synthetic.periodic.node_lease_jitter_percent = 0;
    synthetic.periodic.node_status_interval_ms = 100;
    synthetic.periodic.status_interval_ms = 100;
    synthetic.periodic.compact_interval_ms = 100;
    let plan = CompiledPlan::compile(synthetic, 64 * 1024 * 1024).unwrap();
    assert!(!plan.periodic_events.is_empty());
    assert!(plan.periodic_indexes.iter().all(|index| !index.is_empty()));

    let config = crate::config::RunConfig {
        plan: std::sync::Arc::new(plan),
        endpoint: "http://fixture".into(),
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        max_message_size: 64 * 1024 * 1024,
        setup_started_at: std::time::Instant::now(),
    };
    let clone = config.clone();

    assert!(std::ptr::eq(
        config.plan.periodic_events.as_slice(),
        clone.plan.periodic_events.as_slice()
    ));
    for (original, copied) in config
        .plan
        .periodic_indexes
        .iter()
        .zip(clone.plan.periodic_indexes.iter())
    {
        assert!(std::ptr::eq(original.as_slice(), copied.as_slice()));
    }
}

#[test]
fn test_plan_zero_read_keeps_stream_shape() {
    let mut config = default_config();
    config.load.read_ratio.numerator = 0;
    let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    assert_eq!(plan.logical_streams.len(), 14);
    assert!(
        plan.logical_streams
            .iter()
            .filter(|stream| { stream.work_class != LogicalWorkClass::Update })
            .all(|stream| stream.planned == 0)
    );
    assert_eq!(
        plan.logical_streams
            .iter()
            .filter(|stream| stream.work_class == LogicalWorkClass::Update)
            .count(),
        4
    );
}

#[test]
fn test_periodic_capacity_is_reserved_and_budgeted() {
    let mut config = default_config();
    config.topology.node_count = 1;
    config.topology.pod_count = 10;
    config.topology.namespace_count = 1;
    config.controllers.daemonset_count = 1;
    config.controllers.pods_per_deployment = 9;
    config.load.duration_ms = 10;
    config.periodic.node_lease_interval_ms = 1;
    config.periodic.node_lease_jitter_percent = 0;
    config.periodic.node_status_interval_ms = 10;
    config.periodic.status_interval_ms = 10;
    config.periodic.compact_interval_ms = 10;
    config.report.bucket_ms = 1;
    let plan = CompiledPlan::compile(config, 64 * 1024 * 1024).unwrap();
    let index_size = u64::try_from(size_of::<usize>()).unwrap();
    assert_eq!(
        plan.measurement_layout.periodic_index_bytes,
        plan.periodic_indexes
            .iter()
            .map(|index| u64::try_from(index.capacity()).unwrap())
            .sum::<u64>()
            * index_size
    );
    assert_eq!(plan.measurement_layout.periodic_events, 9);
    let index_counts = periodic_index_counts(&plan.periodic_events);
    for (index, count) in plan.periodic_indexes.iter().zip(index_counts) {
        assert_eq!(index.len(), count);
        assert_eq!(index.capacity(), count);
    }
    assert_eq!(plan.measurement_layout.periodic_capacity, 10);
    let event_size = u64::try_from(size_of::<PeriodicEvent>()).unwrap();
    assert_eq!(
        plan.measurement_layout.periodic_schedule_bytes,
        plan.measurement_layout.periodic_capacity * event_size
    );
    assert!(
        plan.measurement_layout.periodic_schedule_bytes
            > plan.measurement_layout.periodic_events * event_size
    );
}

#[test]
fn test_config_invalid_error() {
    let valid: serde_json::Value = serde_json::from_str(DEFAULT_SYNTHETIC_CONFIG).unwrap();

    let mut unknown = valid.clone();
    unknown["legacy_profile"] = serde_json::Value::Bool(true);
    assert!(SyntheticConfig::from_json(&unknown.to_string(), 64 * 1024 * 1024).is_err());

    let mut zero_denominator = valid.clone();
    zero_denominator["load"]["read_ratio"]["denominator"] = serde_json::Value::from(0);
    let error = SyntheticConfig::from_json(&zero_denominator.to_string(), 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("read_ratio.denominator"));

    let mut invalid_relation = valid.clone();
    invalid_relation["topology"]["pod_count"] = serde_json::Value::from(20_001);
    assert!(
        SyntheticConfig::from_json(&invalid_relation.to_string(), 64 * 1024 * 1024).is_err()
    );

    let mut queue = valid.clone();
    queue["execution"]["workers"] = serde_json::Value::from(2);
    queue["execution"]["queue_capacity"] = serde_json::Value::from(1);
    let error = SyntheticConfig::from_json(&queue.to_string(), 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("queue_capacity"));
    assert!(error.contains("workers"));

    let mut weight_overflow = valid.clone();
    weight_overflow["load"]["update_weights"]["pods"] = serde_json::Value::from(u64::MAX);
    weight_overflow["load"]["update_weights"]["deployments"] = serde_json::Value::from(1);
    let error = SyntheticConfig::from_json(&weight_overflow.to_string(), 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("update_weights"));
    assert!(error.contains("overflows"));

    let mut count_overflow = valid.clone();
    count_overflow["topology"]["node_count"] = serde_json::Value::from(u64::MAX / 2);
    count_overflow["topology"]["pod_count"] = serde_json::Value::from(u64::MAX);
    count_overflow["controllers"]["daemonset_count"] = serde_json::Value::from(1);
    count_overflow["controllers"]["pods_per_deployment"] = serde_json::Value::from(1);
    let error = SyntheticConfig::from_json(&count_overflow.to_string(), 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("total synthetic resource count"));

    let mut duration_overflow = valid.clone();
    duration_overflow["load"]["duration_ms"] = serde_json::Value::from(u64::MAX);
    let error = SyntheticConfig::from_json(&duration_overflow.to_string(), 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("duration"));
    assert!(error.contains("representable"));

    let mut memory_overflow = valid.clone();
    memory_overflow["report"]["memory_limit_mib"] = serde_json::Value::from(u64::MAX);
    let error = SyntheticConfig::from_json(&memory_overflow.to_string(), 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("memory_limit_mib"));
    assert!(error.contains("overflows"));

    let mut memory = valid.clone();
    memory["report"]["memory_limit_mib"] = serde_json::Value::from(1);
    let error = CompiledPlan::compile(
        SyntheticConfig::from_json(&memory.to_string(), 64 * 1024 * 1024).unwrap(),
        64 * 1024 * 1024,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("estimated measurement memory"));

    let mut watch_overflow = valid.clone();
    watch_overflow["watch"]["subscriptions_per_root"] = serde_json::Value::from(u64::MAX);
    let config =
        SyntheticConfig::from_json(&watch_overflow.to_string(), 64 * 1024 * 1024).unwrap();
    let error = CompiledPlan::compile(config, 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(error.contains("Watch stream count"));

    let mut read_rate_overflow = valid.clone();
    read_rate_overflow["load"]["update_rps"] = serde_json::Value::from(u64::MAX);
    read_rate_overflow["load"]["read_ratio"]["numerator"] = serde_json::Value::from(u64::MAX);
    let config =
        SyntheticConfig::from_json(&read_rate_overflow.to_string(), 64 * 1024 * 1024).unwrap();
    let error = CompiledPlan::compile(config, 64 * 1024 * 1024)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("exceeds u64") || error.contains("overflow"),
        "rate overflow error lacks detail: {error}"
    );
}

#[test]
fn test_plan_rate_independent() {
    let base = default_config();
    let mut faster = base.clone();
    faster.load.update_rps = 20_000;
    let base_plan = CompiledPlan::compile(base.clone(), 64 * 1024 * 1024).unwrap();
    let faster_plan = CompiledPlan::compile(faster, 64 * 1024 * 1024).unwrap();
    assert_eq!(base_plan.resource_counts, faster_plan.resource_counts);
    assert_eq!(base_plan.resources, faster_plan.resources);
    assert_eq!(base_plan.periodic_events, faster_plan.periodic_events);

    let mut longer = base;
    longer.load.duration_ms = 1_200_000;
    longer.report.memory_limit_mib = 256;
    let longer_plan = CompiledPlan::compile(longer, 64 * 1024 * 1024).unwrap();
    assert_eq!(base_plan.resources, longer_plan.resources);
    assert_eq!(base_plan.config.periodic, longer_plan.config.periodic);
    assert!(longer_plan.periodic_events.len() > base_plan.periodic_events.len());
}
