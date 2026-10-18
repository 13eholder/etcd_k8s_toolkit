use super::*;
use crate::config::SyntheticConfig;
use crate::plan::{CompiledPlan, DEFAULT_SYNTHETIC_CONFIG};

#[test]
fn test_run_report_serializes_exact_nine_top_level_fields() {
    let synthetic =
        SyntheticConfig::from_json(DEFAULT_SYNTHETIC_CONFIG, 64 * 1024 * 1024).unwrap();
    let plan = CompiledPlan::compile(synthetic, 64 * 1024 * 1024).unwrap();
    let preparation_logical_high = plan.measurement_layout.prepare_logical_histogram.high_us;
    let measurement_logical_high = plan
        .measurement_layout
        .measurement_logical_histogram
        .high_us;
    let config = crate::config::RunConfig {
        plan: std::sync::Arc::new(plan),
        endpoint: "http://fixture".into(),
        metrics_listen: "127.0.0.1:0".parse().unwrap(),
        setup_started_at: std::time::Instant::now(),
        max_message_size: 64 * 1024 * 1024,
    };
    let report =
        RunReport::setup_failure(&config, RunStatus::Failed, "setup", "fixture failure");
    let mut encoded = Vec::new();
    report.write_json(&mut encoded).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    let object = value.as_object().unwrap();
    assert_eq!(object.len(), 9);
    for field in [
        "schema_version",
        "experiment",
        "resolved_config",
        "plan",
        "timing",
        "preparation",
        "measurement",
        "drain",
        "accounting",
    ] {
        assert!(
            object.contains_key(field),
            "missing top-level field {field}"
        );
    }
    assert!(
        value["measurement"]["watch"]["roots"]
            .as_array()
            .is_some_and(|roots| !roots.is_empty())
    );
    assert!(!object.contains_key("status"));
    assert!(!object.contains_key("error"));
    assert_eq!(
        object["resolved_config"]["endpoint"],
        serde_json::Value::String("http://fixture".into())
    );
    assert_eq!(
        object["resolved_config"]["max_message_size"],
        serde_json::Value::from(64_u64 * 1024 * 1024)
    );
    assert!(value["timing"]["setup_ms"].is_number());
    assert!(value["timing"]["prepare_ms"].is_null());
    let assert_empty_histogram = |view: &serde_json::Value, high_us: u64| {
        assert_eq!(view["low_us"], serde_json::Value::from(1_u64));
        assert_eq!(view["high_us"], serde_json::Value::from(high_us));
        assert_eq!(view["significant_digits"], serde_json::Value::from(2_u64));
        assert_eq!(view["sample_count"], serde_json::Value::from(0_u64));
    };
    assert_empty_histogram(
        &value["preparation"]["logical"][0]["logical_latency"],
        preparation_logical_high,
    );
    assert_empty_histogram(
        &value["measurement"]["logical"][0]["logical_latency"],
        measurement_logical_high,
    );
    let buckets = value["measurement"]["buckets"]
        .as_array()
        .expect("setup failure keeps fixed empty bucket slots");
    assert!(buckets.iter().all(
        |bucket| bucket["start_ns"] == serde_json::Value::from(0_u64)
            && bucket["end_ns"] == serde_json::Value::from(0_u64)
            && bucket["txn_started_per_second"].is_null()
            && bucket["txn_completed_per_second"].is_null()
    ));
    assert_eq!(
        value["preparation"]["logical"][0]["planned"],
        value["plan"]["resource_counts"]["pods"]
    );
    assert_eq!(
        value["preparation"]["logical"][0]["not_offered_after_stop"],
        value["preparation"]["logical"][0]["planned"]
    );
    assert_eq!(
        value["preparation"]["resources"].as_array().map(Vec::len),
        Some(6)
    );
    assert_eq!(value["accounting"]["planned_conserved"], true);
    assert_eq!(value["accounting"]["offered_conserved"], true);
    assert_eq!(value["accounting"]["measurement_complete"], false);
}
