use super::*;

fn plan_config(args: &[&str]) -> Result<CompiledPlan, ConfigError> {
    let mut full = vec!["bench", "plan"];
    full.extend_from_slice(args);
    PlanArgs::load_from(full)
}

fn run_config(args: &[&str]) -> Result<RunConfig, ConfigError> {
    let mut full = vec!["bench", "run"];
    full.extend_from_slice(args);
    let cli = Cli::try_parse_from(full).map_err(|error| ConfigError::Invalid(error.to_string()))?;
    match cli.command {
        Command::Run(run) => run.resolve(),
        Command::Plan(_) => unreachable!(),
    }
}

fn temporary_config(tag: &str) -> (PathBuf, impl Drop) {
    struct Cleanup(PathBuf);

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    let path = std::env::temp_dir().join(format!(
        "etcd-k8s-bench-cli-{}-{tag}.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    (path.clone(), Cleanup(path))
}

#[test]
fn test_plan_defaults_load() {
    let plan = plan_config(&[]).unwrap();
    assert_eq!(plan.config.name, "synthetic-k8s");
    assert_eq!(plan.resource_counts.total(), 44_410);
    assert_eq!(plan.config.load.duration_ms, 600_000);
    assert_eq!(plan.config.load.update_rps, 10_000);
    assert_eq!(plan.config.execution.workers, 128);
    assert_eq!(plan.update_rounding_deficit, 0);
    assert_eq!(plan.read_rounding_deficit, 0);
}

#[test]
fn test_cli_accepts_seed_zero() {
    let plan = plan_config(&["--seed", "0"]).unwrap();
    assert_eq!(plan.config.seed, 0);
}

#[test]
fn test_cli_override() {
    let plan = plan_config(&[
        "--nodes",
        "20",
        "--pods",
        "1200",
        "--namespaces",
        "12",
        "--pod-size",
        "8192B",
        "--seed",
        "9",
        "--update-rps",
        "11",
        "--duration",
        "2s",
        "--workers",
        "4",
        "--queue-capacity",
        "8",
        "--request-timeout",
        "2s",
        "--operation-timeout",
        "3s",
    ])
    .unwrap();
    assert_eq!(plan.config.topology.node_count, 20);
    assert_eq!(plan.config.topology.pod_count, 1200);
    assert_eq!(plan.config.topology.namespace_count, 12);
    assert_eq!(plan.config.value_size_bytes.pods, 8192);
    assert_eq!(plan.config.value_size_bytes.nodes, 4096);
    assert_eq!(plan.config.seed, 9);
    assert_eq!(plan.config.load.update_rps, 11);
    assert_eq!(plan.config.load.duration_ms, 2_000);
    assert_eq!(plan.config.execution.workers, 4);
    assert_eq!(plan.config.execution.queue_capacity, 8);
    assert_eq!(plan.config.execution.request_timeout_ms, 2_000);
    assert_eq!(plan.config.execution.operation_timeout_ms, 3_000);
    assert_eq!(plan.resource_counts.pods, 1_200);
    assert_eq!(plan.resource_counts.deployments, 10);
}

#[test]
fn test_cli_override_precedes_validation() {
    let mut config = SyntheticConfig::parse_json(DEFAULT_SYNTHETIC_CONFIG).unwrap();
    config.topology.node_count = 0;
    config.load.duration_ms = 0;
    let json = serde_json::to_string(&config).unwrap();
    let (path, _cleanup) = temporary_config("override-order");
    std::fs::write(&path, json).unwrap();

    let path_string = path.to_str().unwrap();
    let plan =
        plan_config(&["--config", path_string, "--nodes", "20", "--duration", "1s"]).unwrap();
    assert_eq!(plan.config.topology.node_count, 20);
    assert_eq!(plan.config.load.duration_ms, 1_000);
}

#[test]
fn test_duration_requires_integral_milliseconds() {
    let error = plan_config(&["--duration", "1.5ms"])
        .unwrap_err()
        .to_string();
    assert!(error.contains("integral number of milliseconds"));
}

#[test]
fn test_cli_rejects_zero_values() {
    for (flag, value) in [
        ("--nodes", "0"),
        ("--pods", "0"),
        ("--namespaces", "0"),
        ("--pod-size", "0B"),
        ("--update-rps", "0"),
        ("--duration", "0ms"),
        ("--workers", "0"),
        ("--queue-capacity", "0"),
        ("--max-message-size", "0B"),
        ("--request-timeout", "0ms"),
        ("--operation-timeout", "0ms"),
    ] {
        let error = Cli::try_parse_from(["bench", "plan", flag, value])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(flag.trim_start_matches("--")),
            "{flag} zero error omitted field: {error}"
        );
    }
}

#[test]
fn test_cli_nodes_option_only() {
    let cli = Cli::try_parse_from(["bench", "run", "--nodes", "7"]).unwrap();
    let Command::Run(run) = cli.command else {
        panic!("expected run command");
    };
    assert_eq!(run.base.nodes, Some(7));
    assert!(Cli::try_parse_from(["bench", "run", "7"]).is_err());
}

#[test]
fn test_cli_rejects_deleted_parameters() {
    for args in [
        ["bench", "plan", "--profile", "k8s"],
        ["bench", "run", "--profile", "k8s"],
        ["bench", "run", "--rate-scale", "2"],
        ["bench", "run", "--time-scale", "2"],
        ["bench", "run", "--max-qps", "100"],
        ["bench", "run", "--name", "legacy"],
    ] {
        assert!(
            Cli::try_parse_from(args).is_err(),
            "deleted CLI parameter was accepted: {args:?}"
        );
    }
}

#[test]
fn test_cli_missing_config_path_error() {
    let (path, _cleanup) = temporary_config("missing");
    let path_string = path.to_str().unwrap();
    let error = plan_config(&["--config", path_string]).unwrap_err();
    match error {
        ConfigError::Io {
            path: error_path,
            source,
        } => {
            assert_eq!(error_path, path_string);
            assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
        }
        other => panic!("expected ConfigError::Io, got {other:?}"),
    }
}

#[test]
fn test_run_and_plan_share_resolved_config() {
    let args = [
        "--nodes",
        "20",
        "--pods",
        "1200",
        "--namespaces",
        "12",
        "--pod-size",
        "8192B",
        "--seed",
        "9",
        "--update-rps",
        "11",
        "--duration",
        "2s",
        "--workers",
        "4",
        "--queue-capacity",
        "8",
        "--request-timeout",
        "2s",
        "--operation-timeout",
        "3s",
    ];
    let plan = plan_config(&args).unwrap();
    let mut run_args = vec![
        "--endpoint",
        "http://127.0.0.1:32379",
        "--metrics-listen",
        "127.0.0.1:39898",
    ];
    run_args.extend_from_slice(&args);
    let run = run_config(&run_args).unwrap();
    assert_eq!(run.plan.config, plan.config);
    assert_eq!(run.endpoint, "http://127.0.0.1:32379");
    assert_eq!(run.metrics_listen, "127.0.0.1:39898".parse().unwrap());
    assert_eq!(run.max_message_size, 64 * 1024 * 1024);
}

#[test]
fn test_plan_writer_output_contract() {
    let plan = plan_config(&["--nodes", "20", "--pods", "1200"]).unwrap();
    let mut output = Vec::new();
    plan.write_json(&mut output).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&output).unwrap();
    let object = value.as_object().unwrap();
    assert_eq!(object.len(), 7);
    for field in [
        "schema_version",
        "resolved_config",
        "resource_counts",
        "logical_plan",
        "periodic_plan",
        "pacing_tick_ms",
        "estimated_measurement_memory_bytes",
    ] {
        assert!(object.contains_key(field), "missing plan field {field}");
    }
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["resolved_config"]["name"], "synthetic-k8s");
    assert_eq!(
        value["logical_plan"]["streams"].as_array().unwrap().len(),
        14
    );
    assert_eq!(value["logical_plan"]["update_rounding_deficit"], 0);
    assert_eq!(value["logical_plan"]["read_rounding_deficit"], 0);
    assert_eq!(value["pacing_tick_ms"], 1);
    assert!(
        value["estimated_measurement_memory_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[test]
fn test_plan_rejects_run_only_flags() {
    assert!(Cli::try_parse_from(["bench", "plan", "--endpoint", DEFAULT_ENDPOINT]).is_err());
    assert!(Cli::try_parse_from(["bench", "plan", "--metrics-listen", "127.0.0.1:9898"]).is_err());
}
