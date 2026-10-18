#!/usr/bin/env python3
"""Independent, offline validation of the synthetic benchmark report.

The benchmark remains the sole plan/configuration compiler.  This module only
checks the serialized evidence and performs integer accounting from the report
itself; it never contacts etcd and never imports the Rust implementation.
"""
from __future__ import annotations

import argparse
import base64
import json
import math
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

U64_MAX = (1 << 64) - 1
U128_MAX = (1 << 128) - 1


class InputError(Exception):
    """The checker could not read or parse one of its input artifacts."""


class Validator:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.checks: dict[str, bool] = {}
        self.arithmetic_valid = True

    def arithmetic_error(self, path: str, message: str) -> None:
        self.arithmetic_valid = False
        self.error(path, message)

    def error(self, path: str, message: str) -> None:
        self.errors.append(f"{path}: {message}")

    def check(self, name: str, value: bool) -> None:
        self.checks[name] = bool(value)


REPORT_KEYS = {
    "schema_version",
    "experiment",
    "resolved_config",
    "plan",
    "timing",
    "preparation",
    "measurement",
    "drain",
    "accounting",
}
EXPERIMENT_KEYS = {"status", "failure_phase", "first_error"}
BASE_CONFIG_KEYS = {
    "name",
    "seed",
    "topology",
    "controllers",
    "value_size_bytes",
    "load",
    "periodic",
    "watch",
    "execution",
    "report",
}
RUNTIME_CONFIG_KEYS = BASE_CONFIG_KEYS | {"endpoint", "max_message_size"}
TOPOLOGY_KEYS = {"node_count", "pod_count", "namespace_count"}
CONTROLLER_KEYS = {"daemonset_count", "pods_per_deployment"}
VALUE_KEYS = {
    "pods",
    "nodes",
    "node_leases",
    "deployments",
    "replicasets",
    "daemonsets",
}
LOAD_KEYS = {
    "duration_ms",
    "update_rps",
    "read_ratio",
    "update_weights",
    "read_weights",
    "read_shape_weights",
    "list_limit",
}
RATIO_KEYS = {"numerator", "denominator"}
UPDATE_WEIGHT_KEYS = {"pods", "deployments", "replicasets", "daemonsets"}
READ_WEIGHT_KEYS = {"pods", "nodes", "deployments", "replicasets", "daemonsets"}
SHAPE_WEIGHT_KEYS = {"point", "bounded_list"}
PERIODIC_KEYS = {
    "node_lease_interval_ms",
    "node_lease_jitter_percent",
    "node_status_interval_ms",
    "status_interval_ms",
    "compact_interval_ms",
}
WATCH_CONFIG_KEYS = {
    "subscriptions_per_root",
    "start_revision",
    "prev_kv",
    "progress_notify",
}
EXECUTION_KEYS = {
    "workers",
    "queue_capacity",
    "request_timeout_ms",
    "operation_timeout_ms",
    "preparation_timeout_ms",
    "drain_timeout_ms",
    "watch_teardown_timeout_ms",
}
REPORT_CONFIG_KEYS = {"bucket_ms", "memory_limit_mib"}
PLAN_KEYS = {
    "resource_counts",
    "resources",
    "logical_streams",
    "periodic_plan",
    "pacing_tick_ms",
    "update_rounding_deficit",
    "read_rounding_deficit",
    "estimated_measurement_memory_bytes",
}
PLAN_OUTPUT_KEYS = {
    "schema_version",
    "resolved_config",
    "resource_counts",
    "logical_plan",
    "periodic_plan",
    "pacing_tick_ms",
    "estimated_measurement_memory_bytes",
}
LOGICAL_KEYS = {
    "work_class",
    "resource",
    "planned",
    "offered",
    "accepted",
    "dropped_full",
    "dropped_closed",
    "missed_schedule",
    "not_offered_after_stop",
    "started_before_cutoff",
    "started_after_cutoff",
    "succeeded_before_cutoff",
    "failed_before_cutoff",
    "succeeded_after_cutoff",
    "failed_after_cutoff",
    "timeout_before_cutoff",
    "timeout_after_cutoff",
    "result_unknown_before_cutoff",
    "result_unknown_after_cutoff",
    "terminal_after_cutoff",
    "canceled_queued",
    "canceled_running",
    "cutoff_queued",
    "cutoff_running",
    "first_error_code",
    "first_error",
    "logical_latency",
    "queue_latency",
}
QUEUE_KEYS = {"work_class", "resource", "sample_count", "latency"}
RPC_KEYS = {
    "method",
    "started",
    "completed",
    "succeeded",
    "failed",
    "timeout",
    "result_unknown",
    "payload_bytes",
    "request_payload_bytes",
    "response_payload_bytes",
    "started_per_second",
    "completed_per_second",
    "latency",
    "range_pages",
    "range_snapshot_revision",
    "range_server_count",
    "range_last_key_bytes",
    "range_selected",
    "range_discarded",
}
HISTOGRAM_KEYS = {
    "low_us",
    "high_us",
    "significant_digits",
    "auto_resize",
    "coordinated_omission",
    "sample_count",
    "p50_us",
    "p90_us",
    "p99_us",
    "max_us",
    "overflow",
    "exact_max_us",
    "quantiles_unavailable",
}
PREPARATION_PHASE_KEYS = {
    "phase",
    "logical",
    "queue",
    "rpc",
    "resources",
    "logical_total",
    "queue_total",
    "rpc_total",
    "timed_out",
}
MEASUREMENT_KEYS = {
    "logical",
    "queue",
    "rpc",
    "txn_started_per_second",
    "txn_completed_per_second",
    "logical_total",
    "queue_total",
    "rpc_total",
    "latency",
    "buckets",
    "cutoff",
    "watch",
}
DRAIN_KEYS = {
    "logical",
    "queue",
    "rpc",
    "logical_total",
    "queue_total",
    "rpc_total",
    "watch",
    "timed_out",
}
LATENCY_KEYS = {"logical_total", "queue_total", "rpc_total", "metadata"}
METADATA_KEYS = {
    "low_us",
    "significant_digits",
    "auto_resize",
    "coordinated_omission",
    "overflow_is_not_truncated",
    "duration_rounding",
    "payload_definition",
}
BUCKET_KEYS = {
    "start_ns",
    "end_ns",
    "logical_offered",
    "logical_started",
    "logical_completed",
    "logical_errors",
    "rpc_started",
    "rpc_completed",
    "txn_started",
    "txn_completed",
    "txn_started_per_second",
    "txn_completed_per_second",
    "watch_events",
    "watch_payload_bytes",
    "logical_latency",
    "queue_latency",
    "rpc_latency",
}
CUTOFF_KEYS = {"configured_ns", "effective_ns", "queued", "running"}
TIMING_KEYS = {
    "setup_ms",
    "prepare_ms",
    "configured_measurement_ms",
    "observed_measurement_ms",
    "drain_ms",
    "watch_teardown_ms",
}
ACCOUNTING_KEYS = {
    "planned_conserved",
    "offered_conserved",
    "accepted_cohort_conserved",
    "result_unknown_subset_failed",
    "no_active_tokens",
    "no_arithmetic_overflow",
    "measurement_complete",
    "observation_complete",
    "histogram_overflow",
}
WATCH_KEYS = {"roots", "observation_complete"}
WATCH_ROOT_KEYS = {
    "root",
    "expected_streams",
    "ready_streams",
    "observable_stream_ms",
    "observed_measurement_ms",
    "coverage_ratio",
    "observation_complete",
    "streams",
}
WATCH_STREAM_KEYS = {
    "root",
    "stream",
    "ready",
    "observable_duration_ms",
    "disconnects",
    "terminal",
    "events",
    "payload_bytes",
    "observation_complete",
}
PREPARATION_RESOURCES = ["pods", "nodes", "node_leases", "deployments", "replicasets", "daemonsets"]
RESOURCE_PREFIXES = {
    "pods": "/registry/pods/",
    "nodes": "/registry/minions/",
    "node_leases": "/registry/leases/kube-node-lease/",
    "deployments": "/registry/deployments/",
    "replicasets": "/registry/replicasets/",
    "daemonsets": "/registry/daemonsets/",
}
RESOURCE_STEMS = {
    "pods": "pod",
    "nodes": "node",
    "node_leases": "node",
    "deployments": "deployment",
    "replicasets": "replicaset",
    "daemonsets": "daemonset",
}
RESOURCE_ROOTS = {
    "pods": "/registry/pods",
    "nodes": "/registry/minions",
    "node_leases": "/registry/leases/kube-node-lease",
    "deployments": "/registry/deployments",
    "replicasets": "/registry/replicasets",
    "daemonsets": "/registry/daemonsets",
}
MEASUREMENT_IDENTITIES = [
    ("update", "pods"),
    ("update", "deployments"),
    ("update", "replicasets"),
    ("update", "daemonsets"),
    ("point_get", "pods"),
    ("bounded_list", "pods"),
    ("point_get", "nodes"),
    ("bounded_list", "nodes"),
    ("point_get", "deployments"),
    ("bounded_list", "deployments"),
    ("point_get", "replicasets"),
    ("bounded_list", "replicasets"),
    ("point_get", "daemonsets"),
    ("bounded_list", "daemonsets"),
    ("node_lease", "node_leases"),
    ("node_status", "nodes"),
    ("status", None),
    ("compact", None),
]
PREPARATION_IDENTITIES = [
    ("create", "pods"),
    ("create", "nodes"),
    ("create", "node_leases"),
    ("create", "deployments"),
    ("create", "replicasets"),
    ("create", "daemonsets"),
    ("node_ready", None),
]
RPC_METHODS = ["range", "txn", "status", "compact"]


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def load_json(path: str | Path) -> Any:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise InputError(f"cannot read {path}: {exc}") from exc
    try:
        return json.loads(
            text,
            object_pairs_hook=_object_pairs,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise InputError(f"invalid JSON in {path}: {exc}") from exc


def _object(value: Any, path: str, keys: set[str], v: Validator) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        v.error(path, "must be an object")
        return None
    missing = sorted(keys - set(value))
    extra = sorted(set(value) - keys)
    for key in missing:
        v.error(f"{path}.{key}", "missing field")
    for key in extra:
        v.error(f"{path}.{key}", "unknown field")
    return value


def _array(value: Any, path: str, v: Validator) -> list[Any] | None:
    if not isinstance(value, list):
        v.error(path, "must be an array")
        return None
    return value


def _uint(value: Any, path: str, v: Validator, *, positive: bool = False, maximum: int = U64_MAX) -> int | None:
    if type(value) is not int:
        v.error(path, "must be an integer (booleans are not integers)")
        return None
    if positive and value == 0:
        v.error(path, "must be positive")
        return None
    if not 0 <= value <= maximum:
        v.arithmetic_error(path, f"outside unsigned {maximum.bit_length()}-bit range")
        return None
    return value


def _integer(value: Any, path: str, v: Validator) -> int | None:
    if type(value) is not int:
        v.error(path, "must be an integer (booleans are not integers)")
        return None
    if not -(1 << 63) <= value < (1 << 63):
        v.arithmetic_error(path, "exceeds signed 64-bit range")
        return None
    return value


def _number(value: Any, path: str, v: Validator, *, allow_none: bool = False) -> float | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        v.error(path, "must be a finite number or null")
        return None
    result = float(value)
    if not math.isfinite(result):
        v.error(path, "must be finite")
        return None
    if result < 0:
        v.error(path, "must be non-negative")
        return None
    return result


def _string(value: Any, path: str, v: Validator, *, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str):
        v.error(path, "must be a string or null")
        return None
    return value


def _boolean(value: Any, path: str, v: Validator) -> bool | None:
    if type(value) is not bool:
        v.error(path, "must be a boolean")
        return None
    return value


def _eq(v: Validator, path: str, actual: Any, expected: Any) -> bool:
    if actual != expected:
        v.error(path, f"expected {expected!r}, got {actual!r}")
        return False
    return True


def _checked_sum(values: Iterable[int], v: Validator, path: str) -> int:
    total = sum(values)
    if not 0 <= total <= U64_MAX:
        v.arithmetic_error(path, "sum exceeds unsigned 64-bit range")
    return total


def _validate_config(value: Any, v: Validator, path: str, *, runtime: bool) -> dict[str, Any] | None:
    cfg = _object(value, path, RUNTIME_CONFIG_KEYS if runtime else BASE_CONFIG_KEYS, v)
    if cfg is None:
        return None
    _string(cfg.get("name"), f"{path}.name", v)
    _uint(cfg.get("seed"), f"{path}.seed", v)
    topology = _object(cfg.get("topology"), f"{path}.topology", TOPOLOGY_KEYS, v)
    controllers = _object(cfg.get("controllers"), f"{path}.controllers", CONTROLLER_KEYS, v)
    sizes = _object(cfg.get("value_size_bytes"), f"{path}.value_size_bytes", VALUE_KEYS, v)
    load = _object(cfg.get("load"), f"{path}.load", LOAD_KEYS, v)
    periodic = _object(cfg.get("periodic"), f"{path}.periodic", PERIODIC_KEYS, v)
    watch = _object(cfg.get("watch"), f"{path}.watch", WATCH_CONFIG_KEYS, v)
    execution = _object(cfg.get("execution"), f"{path}.execution", EXECUTION_KEYS, v)
    report = _object(cfg.get("report"), f"{path}.report", REPORT_CONFIG_KEYS, v)

    if runtime:
        _string(cfg.get("endpoint"), f"{path}.endpoint", v)
        _uint(cfg.get("max_message_size"), f"{path}.max_message_size", v, positive=True)

    counts: dict[str, int] | None = None
    if topology is not None and controllers is not None:
        nodes = _uint(topology.get("node_count"), f"{path}.topology.node_count", v, positive=True)
        pods = _uint(topology.get("pod_count"), f"{path}.topology.pod_count", v, positive=True)
        namespaces = _uint(
            topology.get("namespace_count"), f"{path}.topology.namespace_count", v, positive=True
        )
        daemonsets = _uint(
            controllers.get("daemonset_count"),
            f"{path}.controllers.daemonset_count",
            v,
            positive=True,
        )
        pods_per_deployment = _uint(
            controllers.get("pods_per_deployment"),
            f"{path}.controllers.pods_per_deployment",
            v,
            positive=True,
        )
        if None not in (nodes, pods, namespaces, daemonsets, pods_per_deployment):
            assert nodes is not None and pods is not None and namespaces is not None
            assert daemonsets is not None and pods_per_deployment is not None
            if namespaces > pods:
                v.error(f"{path}.topology.namespace_count", "must not exceed pod_count")
            daemon_pods = nodes * daemonsets
            remaining = pods - daemon_pods
            if remaining <= 0:
                v.error(path, "daemonset pods must leave a positive deployment remainder")
            elif remaining % pods_per_deployment:
                v.error(path, "deployment remainder must divide by pods_per_deployment")
            deployments = remaining // pods_per_deployment if remaining > 0 else 0
            counts = {
                "pods": pods,
                "nodes": nodes,
                "node_leases": nodes,
                "deployments": deployments,
                "replicasets": deployments,
                "daemonsets": daemonsets,
            }

    if sizes is not None:
        for key in VALUE_KEYS:
            _uint(sizes.get(key), f"{path}.value_size_bytes.{key}", v, positive=True)
    if load is not None:
        duration = _uint(load.get("duration_ms"), f"{path}.load.duration_ms", v, positive=True)
        update_rps = _uint(load.get("update_rps"), f"{path}.load.update_rps", v, positive=True)
        ratio = _object(load.get("read_ratio"), f"{path}.load.read_ratio", RATIO_KEYS, v)
        update_weights = _object(
            load.get("update_weights"), f"{path}.load.update_weights", UPDATE_WEIGHT_KEYS, v
        )
        read_weights = _object(
            load.get("read_weights"), f"{path}.load.read_weights", READ_WEIGHT_KEYS, v
        )
        shape_weights = _object(
            load.get("read_shape_weights"), f"{path}.load.read_shape_weights", SHAPE_WEIGHT_KEYS, v
        )
        _uint(load.get("list_limit"), f"{path}.load.list_limit", v, positive=True)
        if ratio is not None:
            _uint(ratio.get("numerator"), f"{path}.load.read_ratio.numerator", v)
            _uint(ratio.get("denominator"), f"{path}.load.read_ratio.denominator", v, positive=True)
        for key in UPDATE_WEIGHT_KEYS:
            if update_weights is not None:
                _uint(update_weights.get(key), f"{path}.load.update_weights.{key}", v)
        for key in READ_WEIGHT_KEYS:
            if read_weights is not None:
                _uint(read_weights.get(key), f"{path}.load.read_weights.{key}", v)
        for key in SHAPE_WEIGHT_KEYS:
            if shape_weights is not None:
                _uint(shape_weights.get(key), f"{path}.load.read_shape_weights.{key}", v)
        if update_weights is not None:
            total = sum(update_weights.get(key, 0) for key in UPDATE_WEIGHT_KEYS)
            if total == 0:
                v.error(f"{path}.load.update_weights", "total must be positive")
        if read_weights is not None:
            total = sum(read_weights.get(key, 0) for key in READ_WEIGHT_KEYS)
            if total == 0:
                v.error(f"{path}.load.read_weights", "total must be positive")
        if shape_weights is not None:
            total = sum(shape_weights.get(key, 0) for key in SHAPE_WEIGHT_KEYS)
            if total == 0:
                v.error(f"{path}.load.read_shape_weights", "total must be positive")
        _ = duration, update_rps
    if periodic is not None:
        for key in PERIODIC_KEYS:
            _uint(periodic.get(key), f"{path}.periodic.{key}", v, positive=key != "node_lease_jitter_percent")
        jitter = _uint(periodic.get("node_lease_jitter_percent"), f"{path}.periodic.node_lease_jitter_percent", v)
        if jitter is not None and jitter >= 100:
            v.error(f"{path}.periodic.node_lease_jitter_percent", "must be in 0..99")
    if watch is not None:
        _uint(watch.get("subscriptions_per_root"), f"{path}.watch.subscriptions_per_root", v, positive=True)
        start_revision = _integer(watch.get("start_revision"), f"{path}.watch.start_revision", v)
        if start_revision is not None and start_revision < 0:
            v.error(f"{path}.watch.start_revision", "must be non-negative")
        _boolean(watch.get("prev_kv"), f"{path}.watch.prev_kv", v)
        _boolean(watch.get("progress_notify"), f"{path}.watch.progress_notify", v)
    if execution is not None:
        for key in EXECUTION_KEYS:
            _uint(execution.get(key), f"{path}.execution.{key}", v, positive=True)
        workers = execution.get("workers")
        queue_capacity = execution.get("queue_capacity")
        request_timeout = execution.get("request_timeout_ms")
        operation_timeout = execution.get("operation_timeout_ms")
        if type(queue_capacity) is int and type(workers) is int and queue_capacity < workers:
            v.error(f"{path}.execution.queue_capacity", "must be at least workers")
        if type(request_timeout) is int and type(operation_timeout) is int and request_timeout > operation_timeout:
            v.error(f"{path}.execution.request_timeout_ms", "must not exceed operation_timeout_ms")
    if report is not None:
        bucket = _uint(report.get("bucket_ms"), f"{path}.report.bucket_ms", v, positive=True)
        memory = _uint(report.get("memory_limit_mib"), f"{path}.report.memory_limit_mib", v, positive=True)
        duration = load.get("duration_ms") if load is not None else None
        if type(bucket) is int and type(duration) is int and bucket > duration:
            v.error(f"{path}.report.bucket_ms", "must not exceed load.duration_ms")
        _ = memory

    if counts is None:
        return None
    return {
        "counts": counts,
        "config": cfg,
    }


def _expected_logical_streams(config: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    load = config.get("load")
    if not isinstance(load, dict):
        return None
    try:
        update_rps = int(load["update_rps"])
        update_weights = load["update_weights"]
        read_ratio = load["read_ratio"]
        read_weights = load["read_weights"]
        shape_weights = load["read_shape_weights"]
        duration_ms = int(load["duration_ms"])
        if not all(isinstance(value, dict) for value in [update_weights, read_ratio, read_weights, shape_weights]):
            return None
        update_total = sum(int(update_weights[key]) for key in UPDATE_WEIGHT_KEYS)
        read_total = sum(int(read_weights[key]) for key in READ_WEIGHT_KEYS)
        shape_total = sum(int(shape_weights[key]) for key in SHAPE_WEIGHT_KEYS)
        ratio_numerator = int(read_ratio["numerator"])
        ratio_denominator = int(read_ratio["denominator"])
    except (KeyError, TypeError, ValueError):
        return None
    if not update_total or not read_total or not shape_total or not ratio_denominator:
        return None
    result: list[dict[str, Any]] = []
    for resource in ["pods", "deployments", "replicasets", "daemonsets"]:
        numerator = update_rps * int(update_weights[resource])
        denominator = update_total
        result.append(
            {
                "work_class": "update",
                "resource": resource,
                "rate": {"numerator": numerator, "denominator": denominator},
                "planned": numerator * duration_ms // (denominator * 1000),
            }
        )
    read_base_numerator = update_rps * ratio_numerator
    read_base_denominator = ratio_denominator * read_total * shape_total
    for resource in ["pods", "nodes", "deployments", "replicasets", "daemonsets"]:
        for work_class, shape in [("point_get", "point"), ("bounded_list", "bounded_list")]:
            numerator = read_base_numerator * int(read_weights[resource]) * int(shape_weights[shape])
            denominator = read_base_denominator
            result.append(
                {
                    "work_class": work_class,
                    "resource": resource,
                    "rate": {"numerator": numerator, "denominator": denominator},
                    "planned": numerator * duration_ms // (denominator * 1000),
                }
            )
    return result


def _validate_plan_fields(
    value: Any,
    config_info: dict[str, Any] | None,
    v: Validator,
    path: str,
    *,
    report_plan: bool,
) -> dict[str, Any] | None:
    keys = PLAN_KEYS if report_plan else PLAN_OUTPUT_KEYS
    plan = _object(value, path, keys, v)
    if plan is None:
        return None
    if not report_plan:
        _eq(v, f"{path}.schema_version", plan.get("schema_version"), 1)
        _uint(plan.get("schema_version"), f"{path}.schema_version", v, positive=True)
    counts = plan.get("resource_counts")
    counts_obj = _object(counts, f"{path}.resource_counts", set(PREPARATION_RESOURCES), v)
    if counts_obj is not None and config_info is not None:
        for resource in PREPARATION_RESOURCES:
            actual = _uint(counts_obj.get(resource), f"{path}.resource_counts.{resource}", v)
            expected = config_info["counts"].get(resource)
            if actual is not None and expected is not None:
                _eq(v, f"{path}.resource_counts.{resource}", actual, expected)

    if report_plan:
        resources = _array(plan.get("resources"), f"{path}.resources", v)
        if resources is not None:
            if len(resources) != len(PREPARATION_RESOURCES):
                v.error(f"{path}.resources", "must contain exactly six resource plans")
            for index, item in enumerate(resources):
                obj = _object(
                    item,
                    f"{path}.resources[{index}]",
                    {"kind", "count", "key_prefix", "value_size_bytes", "namespace_count", "list_limit"},
                    v,
                )
                if obj is None:
                    continue
                kind = _string(obj.get("kind"), f"{path}.resources[{index}].kind", v)
                if kind is not None and index < len(PREPARATION_RESOURCES):
                    expected_kind = PREPARATION_RESOURCES[index]
                    _eq(v, f"{path}.resources[{index}].kind", kind, expected_kind)
                    expected_count = config_info["counts"].get(expected_kind) if config_info else None
                    expected_prefix = RESOURCE_PREFIXES[expected_kind]
                    expected_size = config_info["config"]["value_size_bytes"].get(expected_kind) if config_info else None
                    expected_namespaces = config_info["config"]["topology"].get("namespace_count") if config_info else None
                    expected_limit = config_info["config"]["load"].get("list_limit") if config_info else None
                    if expected_count is not None:
                        _eq(v, f"{path}.resources[{index}].count", obj.get("count"), expected_count)
                    _eq(v, f"{path}.resources[{index}].key_prefix", obj.get("key_prefix"), expected_prefix)
                    if expected_size is not None:
                        _eq(v, f"{path}.resources[{index}].value_size_bytes", obj.get("value_size_bytes"), expected_size)
                    if expected_namespaces is not None:
                        _eq(v, f"{path}.resources[{index}].namespace_count", obj.get("namespace_count"), expected_namespaces)
                    if expected_limit is not None:
                        _eq(v, f"{path}.resources[{index}].list_limit", obj.get("list_limit"), expected_limit)
                _uint(obj.get("count"), f"{path}.resources[{index}].count", v)
                _string(obj.get("key_prefix"), f"{path}.resources[{index}].key_prefix", v)
                _uint(obj.get("value_size_bytes"), f"{path}.resources[{index}].value_size_bytes", v)
                _uint(obj.get("namespace_count"), f"{path}.resources[{index}].namespace_count", v)
                _uint(obj.get("list_limit"), f"{path}.resources[{index}].list_limit", v)

    if report_plan:
        streams_value = plan.get("logical_streams")
        update_deficit = plan.get("update_rounding_deficit")
        read_deficit = plan.get("read_rounding_deficit")
    else:
        logical = _object(
            plan.get("logical_plan"),
            f"{path}.logical_plan",
            {"streams", "update_rounding_deficit", "read_rounding_deficit"},
            v,
        )
        streams_value = logical.get("streams") if logical is not None else None
        update_deficit = logical.get("update_rounding_deficit") if logical is not None else None
        read_deficit = logical.get("read_rounding_deficit") if logical is not None else None

    expected_streams = _expected_logical_streams(config_info["config"]) if config_info else None
    streams = _array(streams_value, f"{path}.logical_streams" if report_plan else f"{path}.logical_plan.streams", v)
    if streams is not None:
        if expected_streams is not None and len(streams) != len(expected_streams):
            v.error(f"{path}.logical_streams", "must contain exactly fourteen streams")
        for index, item in enumerate(streams):
            stream_path = f"{path}.logical_streams[{index}]" if report_plan else f"{path}.logical_plan.streams[{index}]"
            obj = _object(item, stream_path, {"work_class", "resource", "rate", "planned"}, v)
            if obj is None:
                continue
            work_class = _string(obj.get("work_class"), f"{stream_path}.work_class", v)
            resource = _string(obj.get("resource"), f"{stream_path}.resource", v)
            rate = _object(obj.get("rate"), f"{stream_path}.rate", {"numerator", "denominator"}, v)
            planned = _uint(obj.get("planned"), f"{stream_path}.planned", v)
            if rate is not None:
                numerator = _uint(rate.get("numerator"), f"{stream_path}.rate.numerator", v, maximum=U128_MAX)
                denominator = _uint(rate.get("denominator"), f"{stream_path}.rate.denominator", v, positive=True, maximum=U128_MAX)
            else:
                numerator = denominator = None
            if expected_streams is not None and index < len(expected_streams):
                expected = expected_streams[index]
                _eq(v, f"{stream_path}.work_class", work_class, expected["work_class"])
                _eq(v, f"{stream_path}.resource", resource, expected["resource"])
                if rate is not None:
                    _eq(v, f"{stream_path}.rate.numerator", numerator, expected["rate"]["numerator"])
                    _eq(v, f"{stream_path}.rate.denominator", denominator, expected["rate"]["denominator"])
                if planned is not None:
                    _eq(v, f"{stream_path}.planned", planned, expected["planned"])
    if config_info is not None and expected_streams is not None:
        update_nominal = int(config_info["config"]["load"]["update_rps"]) * int(config_info["config"]["load"]["duration_ms"]) // 1000
        ratio = config_info["config"]["load"]["read_ratio"]
        read_nominal = (
            int(config_info["config"]["load"]["update_rps"])
            * int(ratio["numerator"])
            * int(config_info["config"]["load"]["duration_ms"])
            // (int(ratio["denominator"]) * 1000)
        )
        update_planned = sum(x["planned"] for x in expected_streams if x["work_class"] == "update")
        read_planned = sum(x["planned"] for x in expected_streams if x["work_class"] != "update")
        _eq(v, f"{path}.update_rounding_deficit" if report_plan else f"{path}.logical_plan.update_rounding_deficit", update_deficit, update_nominal - update_planned)
        _eq(v, f"{path}.read_rounding_deficit" if report_plan else f"{path}.logical_plan.read_rounding_deficit", read_deficit, read_nominal - read_planned)
    _uint(update_deficit, f"{path}.update_rounding_deficit" if report_plan else f"{path}.logical_plan.update_rounding_deficit", v)
    _uint(read_deficit, f"{path}.read_rounding_deficit" if report_plan else f"{path}.logical_plan.read_rounding_deficit", v)

    periodic = _array(plan.get("periodic_plan"), f"{path}.periodic_plan", v)
    if periodic is not None:
        if len(periodic) != 4:
            v.error(f"{path}.periodic_plan", "must contain four periodic streams")
        for index, item in enumerate(periodic):
            item_path = f"{path}.periodic_plan[{index}]"
            obj = _object(item, item_path, {"kind", "planned"}, v)
            if obj is None:
                continue
            kind = _string(obj.get("kind"), f"{item_path}.kind", v)
            planned = _uint(obj.get("planned"), f"{item_path}.planned", v)
            if index < 4:
                _eq(v, f"{item_path}.kind", kind, ["node_lease", "node_status", "status", "compact"][index])
            _ = planned

    _eq(v, f"{path}.pacing_tick_ms", plan.get("pacing_tick_ms"), 1)
    _uint(plan.get("pacing_tick_ms"), f"{path}.pacing_tick_ms", v, positive=True)
    _uint(plan.get("estimated_measurement_memory_bytes"), f"{path}.estimated_measurement_memory_bytes", v, positive=True)
    return plan


def _hdr_highest_equivalent(value: int, low: int, significant_digits: int) -> int:
    """Return HDR Histogram's inclusive upper bound for one recorded value."""
    if value < 0 or value > (1 << 64) - 1:
        return value
    unit_magnitude = low.bit_length() - 1
    largest_single_unit = 2 * (10**significant_digits)
    sub_bucket_count_magnitude = (largest_single_unit - 1).bit_length()
    sub_bucket_mask = ((1 << sub_bucket_count_magnitude) - 1) << unit_magnitude
    leading_zero_count_base = 64 - unit_magnitude - sub_bucket_count_magnitude
    leading_zeros = 64 - (value | sub_bucket_mask).bit_length()
    bucket_index = leading_zero_count_base - leading_zeros
    shift = unit_magnitude + bucket_index
    lowest_equivalent = (value >> shift) << shift
    return min((1 << 64) - 1, lowest_equivalent + (1 << shift) - 1)
def _validate_histogram(value: Any, v: Validator, path: str, expected_high: int | None) -> int:
    histogram = _object(value, path, HISTOGRAM_KEYS, v)
    if histogram is None:
        return 0
    low = _uint(histogram.get("low_us"), f"{path}.low_us", v, positive=True)
    high = _uint(histogram.get("high_us"), f"{path}.high_us", v, positive=True)
    digits = _uint(histogram.get("significant_digits"), f"{path}.significant_digits", v, positive=True)
    _boolean(histogram.get("auto_resize"), f"{path}.auto_resize", v)
    _boolean(histogram.get("coordinated_omission"), f"{path}.coordinated_omission", v)
    samples = _uint(histogram.get("sample_count"), f"{path}.sample_count", v) or 0
    overflow = _uint(histogram.get("overflow"), f"{path}.overflow", v) or 0
    unavailable = _boolean(histogram.get("quantiles_unavailable"), f"{path}.quantiles_unavailable", v)
    if low is not None:
        _eq(v, f"{path}.low_us", low, 1)
    if high is not None and expected_high is not None:
        _eq(v, f"{path}.high_us", high, expected_high)
    if digits is not None:
        _eq(v, f"{path}.significant_digits", digits, 2)
    _eq(v, f"{path}.auto_resize", histogram.get("auto_resize"), False)
    _eq(v, f"{path}.coordinated_omission", histogram.get("coordinated_omission"), False)
    if unavailable is not None:
        _eq(v, f"{path}.quantiles_unavailable", unavailable, overflow > 0)
    quantile_fields = ["p50_us", "p90_us", "p99_us"]
    for field in quantile_fields + ["max_us", "exact_max_us"]:
        item = histogram.get(field)
        if item is not None:
            _uint(item, f"{path}.{field}", v)
    if overflow > 0:
        for field in quantile_fields:
            if histogram.get(field) is not None:
                v.error(f"{path}.{field}", "must be null when overflow is present")
    elif samples > 0:
        for field in quantile_fields:
            if histogram.get(field) is None:
                v.error(f"{path}.{field}", "missing for a non-empty representable histogram")
    if samples == 0 and overflow == 0:
        for field in quantile_fields + ["max_us", "exact_max_us"]:
            if histogram.get(field) is not None:
                v.error(f"{path}.{field}", "must be null when sample_count is zero")
    ordered = [histogram.get(field) for field in quantile_fields]
    ordered = [x for x in ordered if x is not None]
    if any(left > right for left, right in zip(ordered, ordered[1:])):
        v.error(path, "quantiles must be monotonic")
    exact_max = histogram.get("exact_max_us")
    if (samples > 0 or overflow > 0) and exact_max is None:
        v.error(f"{path}.exact_max_us", "is required for a non-empty histogram")
    if (samples > 0 or overflow > 0) and histogram.get("max_us") != exact_max:
        v.error(path, "max_us must equal exact_max_us")
    if samples and not overflow and type(exact_max) is int and type(low) is int and type(digits) is int:
        highest_equivalent = _hdr_highest_equivalent(exact_max, low, digits)
        for field in quantile_fields:
            quantile = histogram.get(field)
            if type(quantile) is int and quantile > highest_equivalent:
                v.error(
                    f"{path}.{field}",
                    f"exceeds HDR highest_equivalent(exact_max_us)={highest_equivalent}",
                )
    if overflow and histogram.get("exact_max_us") is None:
        v.error(f"{path}.exact_max_us", "is required when overflow is present")
    return _checked_sum((samples, overflow), v, path)


def _validate_logical(
    value: Any,
    v: Validator,
    path: str,
    expected_high: int | None,
    *,
    phase: str | None = None,
) -> dict[str, Any] | None:
    obj = _object(value, path, LOGICAL_KEYS, v)
    if obj is None:
        return None
    for key in LOGICAL_KEYS - {"work_class", "resource", "first_error", "logical_latency", "queue_latency"}:
        _uint(obj.get(key), f"{path}.{key}", v)
    _string(obj.get("work_class"), f"{path}.work_class", v)
    _string(obj.get("resource"), f"{path}.resource", v, allow_none=True)
    _string(obj.get("first_error"), f"{path}.first_error", v, allow_none=True)
    _eq(v, f"{path}.dropped_closed", obj.get("dropped_closed"), 0)
    logical_samples = _validate_histogram(obj.get("logical_latency"), v, f"{path}.logical_latency", expected_high)
    queue_samples = _validate_histogram(obj.get("queue_latency"), v, f"{path}.queue_latency", expected_high)
    terminal_before = (obj.get("succeeded_before_cutoff", 0) if type(obj.get("succeeded_before_cutoff")) is int else 0) + (obj.get("failed_before_cutoff", 0) if type(obj.get("failed_before_cutoff")) is int else 0)
    terminal_after = (obj.get("succeeded_after_cutoff", 0) if type(obj.get("succeeded_after_cutoff")) is int else 0) + (obj.get("failed_after_cutoff", 0) if type(obj.get("failed_after_cutoff")) is int else 0)
    if type(obj.get("terminal_after_cutoff")) is int and obj["terminal_after_cutoff"] != terminal_after:
        v.error(f"{path}.terminal_after_cutoff", "must equal succeeded_after_cutoff + failed_after_cutoff")
    for before, failed, label in [
        (obj.get("timeout_before_cutoff"), obj.get("failed_before_cutoff"), "before"),
        (obj.get("timeout_after_cutoff"), obj.get("failed_after_cutoff"), "after"),
        (obj.get("result_unknown_before_cutoff"), obj.get("failed_before_cutoff"), "before"),
        (obj.get("result_unknown_after_cutoff"), obj.get("failed_after_cutoff"), "after"),
    ]:
        if type(before) is int and type(failed) is int and before > failed:
            v.error(path, f"{label}-cutoff timeout/unknown must be a failed subset")
    if phase == "measurement":
        expected_logical = (
            (obj.get("succeeded_before_cutoff", 0) if type(obj.get("succeeded_before_cutoff")) is int else 0)
            + (obj.get("failed_before_cutoff", 0) if type(obj.get("failed_before_cutoff")) is int else 0)
        )
        expected_queue = obj.get("started_before_cutoff", 0) if type(obj.get("started_before_cutoff")) is int else 0
    elif phase in {"preparation", "drain"}:
        expected_logical = (
            (obj.get("succeeded_before_cutoff", 0) if type(obj.get("succeeded_before_cutoff")) is int else 0)
            + (obj.get("failed_before_cutoff", 0) if type(obj.get("failed_before_cutoff")) is int else 0)
            + (obj.get("succeeded_after_cutoff", 0) if type(obj.get("succeeded_after_cutoff")) is int else 0)
            + (obj.get("failed_after_cutoff", 0) if type(obj.get("failed_after_cutoff")) is int else 0)
        )
        expected_queue = (
            (obj.get("started_before_cutoff", 0) if type(obj.get("started_before_cutoff")) is int else 0)
            + (obj.get("started_after_cutoff", 0) if type(obj.get("started_after_cutoff")) is int else 0)
        )
    else:
        expected_logical = terminal_before + terminal_after
        expected_queue = (
            (obj.get("started_before_cutoff", 0) if type(obj.get("started_before_cutoff")) is int else 0)
            + (obj.get("started_after_cutoff", 0) if type(obj.get("started_after_cutoff")) is int else 0)
        )
    if type(logical_samples) is int and logical_samples != expected_logical:
        v.error(f"{path}.logical_latency", "sample_count + overflow must equal terminal outcomes")
    if type(queue_samples) is int and queue_samples != expected_queue:
        v.error(f"{path}.queue_latency", "sample_count + overflow must equal started outcomes")
    return obj


def _validate_queue(value: Any, v: Validator, path: str, expected_high: int | None) -> int:
    obj = _object(value, path, QUEUE_KEYS, v)
    if obj is None:
        return 0
    _string(obj.get("work_class"), f"{path}.work_class", v)
    _string(obj.get("resource"), f"{path}.resource", v, allow_none=True)
    declared_samples = _uint(obj.get("sample_count"), f"{path}.sample_count", v)
    observations = _validate_histogram(obj.get("latency"), v, f"{path}.latency", expected_high)
    latency = obj.get("latency")
    if declared_samples is not None and isinstance(latency, dict):
        _eq(v, f"{path}.sample_count", declared_samples, latency.get("sample_count"))
    return observations


def _validate_rpc(value: Any, v: Validator, path: str, expected_high: int | None) -> dict[str, Any] | None:
    obj = _object(value, path, RPC_KEYS, v)
    if obj is None:
        return None
    method = _string(obj.get("method"), f"{path}.method", v)
    for key in ["started", "completed", "succeeded", "failed", "timeout", "result_unknown", "payload_bytes", "request_payload_bytes", "response_payload_bytes", "range_pages", "range_last_key_bytes", "range_selected", "range_discarded"]:
        _uint(obj.get(key), f"{path}.{key}", v)
    for key in ["range_snapshot_revision", "range_server_count"]:
        value_int = _integer(obj.get(key), f"{path}.{key}", v)
        if value_int is not None and value_int < 0:
            v.error(f"{path}.{key}", "must be non-negative")
    _number(obj.get("started_per_second"), f"{path}.started_per_second", v, allow_none=True)
    _number(obj.get("completed_per_second"), f"{path}.completed_per_second", v, allow_none=True)
    completed = obj.get("completed") if type(obj.get("completed")) is int else 0
    succeeded = obj.get("succeeded") if type(obj.get("succeeded")) is int else 0
    failed = obj.get("failed") if type(obj.get("failed")) is int else 0
    if completed != _checked_sum((succeeded, failed), v, path):
        v.error(path, "completed must equal succeeded + failed")
    if type(obj.get("timeout")) is int and obj["timeout"] > failed:
        v.error(f"{path}.timeout", "must be a failed subset")
    if type(obj.get("result_unknown")) is int and obj["result_unknown"] > failed:
        v.error(f"{path}.result_unknown", "must be a failed subset")
    if type(obj.get("payload_bytes")) is int and type(obj.get("request_payload_bytes")) is int and type(obj.get("response_payload_bytes")) is int:
        if obj["payload_bytes"] != _checked_sum((obj["request_payload_bytes"], obj["response_payload_bytes"]), v, f"{path}.payload_bytes"):
            v.error(f"{path}.payload_bytes", "must equal request_payload_bytes + response_payload_bytes")
    observations = _validate_histogram(obj.get("latency"), v, f"{path}.latency", expected_high)
    if type(obj.get("completed")) is int and observations != obj["completed"]:
        v.error(f"{path}.latency", "sample_count + overflow must equal completed RPCs")
    _ = method
    return obj


def _rate_matches(actual: Any, expected: float | None) -> bool:
    if expected is None:
        return actual is None
    if isinstance(actual, bool) or not isinstance(actual, (int, float)):
        return False
    actual_value = float(actual)
    return math.isfinite(actual_value) and abs(actual_value - expected) <= 1e-9 * max(1.0, abs(expected))


def _validate_rpc_rates(
    rows: Sequence[Any],
    *,
    phase: str,
    observed_ms: float | None,
    v: Validator,
    path: str,
) -> None:
    expected_duration_ms = observed_ms if phase == "measurement" and observed_ms is not None and observed_ms > 0 else None
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        for field, count_field in [("started_per_second", "started"), ("completed_per_second", "completed")]:
            expected = None
            count = row.get(count_field)
            if expected_duration_ms is not None and type(count) is int:
                expected = float(count) / (expected_duration_ms / 1000.0)
            actual = row.get(field)
            if not _rate_matches(actual, expected):
                expectation = "null" if expected is None else "count / observed measurement duration"
                v.error(f"{path}[{index}].{field}", f"must equal {expectation}")


def _validate_plan_measurement_drain(
    plan: Any,
    measurement_rows: Sequence[Any],
    drain_rows: Sequence[Any],
    v: Validator,
) -> bool:
    if not isinstance(plan, dict):
        return False
    ok = True
    periodic_kinds = {"node_lease", "node_status", "status", "compact"}
    planned_by_identity: dict[tuple[str, str], int] = {}
    logical_streams = plan.get("logical_streams")
    if isinstance(logical_streams, list):
        for index, item in enumerate(logical_streams):
            if not isinstance(item, dict):
                continue
            work_class = item.get("work_class")
            resource = item.get("resource")
            planned = item.get("planned")
            if isinstance(work_class, str) and isinstance(resource, str) and type(planned) is int:
                key = (work_class, resource)
                if key in planned_by_identity:
                    v.error(f"report.plan.logical_streams[{index}]", "duplicate logical stream identity")
                    ok = False
                planned_by_identity[key] = planned
    for index, row in enumerate(measurement_rows):
        if not isinstance(row, dict):
            continue
        key = (row.get("work_class"), row.get("resource"))
        if not (isinstance(key[0], str) and isinstance(key[1], str)):
            continue
        if key[0] in periodic_kinds:
            continue
        expected = planned_by_identity.get((key[0], key[1]))
        if expected is None:
            v.error(f"report.measurement.logical[{index}].planned", "has no matching embedded plan stream")
            ok = False
        elif row.get("planned") != expected:
            v.error(f"report.measurement.logical[{index}].planned", "does not match embedded plan")
            ok = False

    periodic_by_kind: dict[str, int] = {}
    periodic_plan = plan.get("periodic_plan")
    if isinstance(periodic_plan, list):
        for index, item in enumerate(periodic_plan):
            if not isinstance(item, dict):
                continue
            kind = item.get("kind")
            planned = item.get("planned")
            if isinstance(kind, str) and type(planned) is int:
                if kind in periodic_by_kind:
                    v.error(f"report.plan.periodic_plan[{index}]", "duplicate periodic stream identity")
                    ok = False
                periodic_by_kind[kind] = planned
    periodic_kinds = {"node_lease", "node_status", "status", "compact"}
    for index, row in enumerate(measurement_rows):
        if not isinstance(row, dict) or row.get("work_class") not in periodic_kinds:
            continue
        kind = row.get("work_class")
        expected = periodic_by_kind.get(kind)
        if expected is None:
            v.error(f"report.measurement.logical[{index}].planned", "has no matching embedded periodic plan")
            ok = False
        elif row.get("planned") != expected:
            v.error(f"report.measurement.logical[{index}].planned", "does not match embedded periodic plan")
            ok = False

    measurement_by_identity: dict[tuple[Any, Any], dict[str, Any]] = {}
    drain_by_identity: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in measurement_rows:
        if isinstance(row, dict):
            measurement_by_identity[(row.get("work_class"), row.get("resource"))] = row
    for row in drain_rows:
        if isinstance(row, dict):
            drain_by_identity[(row.get("work_class"), row.get("resource"))] = row
    after_fields = [
        "started_after_cutoff",
        "succeeded_after_cutoff",
        "failed_after_cutoff",
        "timeout_after_cutoff",
        "result_unknown_after_cutoff",
        "terminal_after_cutoff",
        "canceled_queued",
        "canceled_running",
    ]
    for identity, measurement_row in measurement_by_identity.items():
        drain_row = drain_by_identity.get(identity)
        if drain_row is None:
            v.error("report.drain.logical", f"missing identity {identity!r}")
            ok = False
            continue
        for field in after_fields:
            if drain_row.get(field) != measurement_row.get(field):
                v.error(f"report.drain.logical[{identity!r}].{field}", "must equal measurement")
                ok = False
    for identity in drain_by_identity:
        if identity not in measurement_by_identity:
            v.error("report.drain.logical", f"has extra identity {identity!r}")
            ok = False
    return ok

def _validate_histogram_total(total: Any, members: Iterable[Any], v: Validator, path: str) -> None:
    if not isinstance(total, dict):
        return
    histograms = [item for item in members if isinstance(item, dict)]
    for field in ["sample_count", "overflow"]:
        counts = [item.get(field) for item in histograms]
        if all(type(count) is int for count in counts):
            expected = _checked_sum(counts, v, f"{path}.{field}")
            _eq(v, f"{path}.{field}", total.get(field), expected)
    maxima = [item["exact_max_us"] for item in histograms if type(item.get("exact_max_us")) is int]
    expected_max = max(maxima, default=None)
    for field in ["max_us", "exact_max_us"]:
        _eq(v, f"{path}.{field}", total.get(field), expected_max)


def _validate_phase_histograms(phase: Mapping[str, Any], v: Validator, path: str) -> None:
    logical = phase.get("logical")
    queue = phase.get("queue")
    rpc = phase.get("rpc")
    if isinstance(logical, list) and isinstance(queue, list):
        for index, (row, queued) in enumerate(zip(logical, queue)):
            if isinstance(row, dict) and isinstance(queued, dict):
                _eq(v, f"{path}.queue[{index}].latency", queued.get("latency"), row.get("queue_latency"))
    for rows, field, total in [
        (logical, "logical_latency", "logical_total"),
        (queue, "latency", "queue_total"),
        (rpc, "latency", "rpc_total"),
    ]:
        if isinstance(rows, list):
            _validate_histogram_total(
                phase.get(total),
                (row.get(field) for row in rows if isinstance(row, dict)),
                v,
                f"{path}.{total}",
            )


def _validate_phase_arrays(
    value: Any,
    v: Validator,
    path: str,
    *,
    phase: str,
    logical_count: int,
    expected_identities: Sequence[tuple[str, str | None]],
    logical_high: int,
    rpc_high: int,
    include_resources: bool,
) -> dict[str, Any] | None:
    keys = PREPARATION_PHASE_KEYS
    phase_obj = _object(value, path, keys, v)
    if phase_obj is None:
        return None
    _eq(v, f"{path}.phase", phase_obj.get("phase"), phase)
    logical = _array(phase_obj.get("logical"), f"{path}.logical", v)
    queue = _array(phase_obj.get("queue"), f"{path}.queue", v)
    rpc = _array(phase_obj.get("rpc"), f"{path}.rpc", v)
    if logical is not None:
        if len(logical) != logical_count:
            v.error(f"{path}.logical", f"must contain {logical_count} streams")
        for index, item in enumerate(logical):
            row = _validate_logical(item, v, f"{path}.logical[{index}]", logical_high, phase=phase)
            if row is not None and index < len(expected_identities):
                _eq(v, f"{path}.logical[{index}].work_class", row.get("work_class"), expected_identities[index][0])
                _eq(v, f"{path}.logical[{index}].resource", row.get("resource"), expected_identities[index][1])
    if queue is not None:
        if len(queue) != logical_count:
            v.error(f"{path}.queue", f"must contain {logical_count} streams")
        for index, item in enumerate(queue):
            row_path = f"{path}.queue[{index}]"
            _validate_queue(item, v, row_path, logical_high)
            if isinstance(item, dict) and index < len(expected_identities):
                _eq(v, f"{row_path}.work_class", item.get("work_class"), expected_identities[index][0])
                _eq(v, f"{row_path}.resource", item.get("resource"), expected_identities[index][1])
    if rpc is not None:
        if len(rpc) != 4:
            v.error(f"{path}.rpc", "must contain four methods")
        for index, item in enumerate(rpc):
            row = _validate_rpc(item, v, f"{path}.rpc[{index}]", rpc_high)
            if row is not None and index < len(RPC_METHODS):
                _eq(v, f"{path}.rpc[{index}].method", row.get("method"), RPC_METHODS[index])
    resources = _array(phase_obj.get("resources"), f"{path}.resources", v)
    if resources is not None:
        if include_resources:
            if len(resources) != 6:
                v.error(f"{path}.resources", "must contain six resources")
            for index, item in enumerate(resources):
                obj = _object(item, f"{path}.resources[{index}]", {"resource", "created", "failed"}, v)
                if obj is not None:
                    _string(obj.get("resource"), f"{path}.resources[{index}].resource", v)
                    _uint(obj.get("created"), f"{path}.resources[{index}].created", v)
                    _uint(obj.get("failed"), f"{path}.resources[{index}].failed", v)
                    if index < 6:
                        _eq(v, f"{path}.resources[{index}].resource", obj.get("resource"), PREPARATION_RESOURCES[index])
        elif resources:
            v.error(f"{path}.resources", "must be empty outside preparation")
    _validate_histogram(phase_obj.get("logical_total"), v, f"{path}.logical_total", logical_high)
    _validate_histogram(phase_obj.get("queue_total"), v, f"{path}.queue_total", logical_high)
    _validate_histogram(phase_obj.get("rpc_total"), v, f"{path}.rpc_total", rpc_high)
    timed_out = _boolean(phase_obj.get("timed_out"), f"{path}.timed_out", v)
    _validate_phase_histograms(phase_obj, v, path)
    _ = timed_out
    return phase_obj


def _validate_latency(value: Any, v: Validator, path: str, logical_high: int, rpc_high: int) -> None:
    obj = _object(value, path, LATENCY_KEYS, v)
    if obj is None:
        return
    _validate_histogram(obj.get("logical_total"), v, f"{path}.logical_total", logical_high)
    _validate_histogram(obj.get("queue_total"), v, f"{path}.queue_total", logical_high)
    _validate_histogram(obj.get("rpc_total"), v, f"{path}.rpc_total", rpc_high)
    metadata = _object(obj.get("metadata"), f"{path}.metadata", METADATA_KEYS, v)
    if metadata is not None:
        _uint(metadata.get("low_us"), f"{path}.metadata.low_us", v, positive=True)
        _uint(metadata.get("significant_digits"), f"{path}.metadata.significant_digits", v, positive=True)
        _boolean(metadata.get("auto_resize"), f"{path}.metadata.auto_resize", v)
        _boolean(metadata.get("coordinated_omission"), f"{path}.metadata.coordinated_omission", v)
        _boolean(metadata.get("overflow_is_not_truncated"), f"{path}.metadata.overflow_is_not_truncated", v)
        _string(metadata.get("duration_rounding"), f"{path}.metadata.duration_rounding", v)
        _string(metadata.get("payload_definition"), f"{path}.metadata.payload_definition", v)
        _eq(v, f"{path}.metadata.low_us", metadata.get("low_us"), 1)
        _eq(v, f"{path}.metadata.significant_digits", metadata.get("significant_digits"), 2)
        _eq(v, f"{path}.metadata.auto_resize", metadata.get("auto_resize"), False)
        _eq(v, f"{path}.metadata.coordinated_omission", metadata.get("coordinated_omission"), False)
        _eq(v, f"{path}.metadata.overflow_is_not_truncated", metadata.get("overflow_is_not_truncated"), True)


def _validate_watch(
    value: Any,
    config: Mapping[str, Any],
    observed_ms: float,
    v: Validator,
    path: str,
) -> tuple[int, int, bool]:
    obj = _object(value, path, WATCH_KEYS, v)
    if obj is None:
        return 0, 0, False
    roots = _array(obj.get("roots"), f"{path}.roots", v)
    report_complete = _boolean(obj.get("observation_complete"), f"{path}.observation_complete", v)
    watch_config = config.get("watch")
    subscriptions = (
        watch_config.get("subscriptions_per_root", 0)
        if isinstance(watch_config, dict)
        else 0
    )
    expected_roots = list(RESOURCE_ROOTS.values())
    event_total = 0
    payload_total = 0
    root_complete_values: list[bool] = []
    if roots is not None:
        if len(roots) != len(expected_roots):
            v.error(f"{path}.roots", "must contain six fixed roots")
        for index, root_value in enumerate(roots):
            root_path = f"{path}.roots[{index}]"
            root = _object(root_value, root_path, WATCH_ROOT_KEYS, v)
            if root is None:
                root_complete_values.append(False)
                continue
            if index < len(expected_roots):
                _eq(v, f"{root_path}.root", root.get("root"), expected_roots[index])
            expected_streams = _uint(
                root.get("expected_streams"), f"{root_path}.expected_streams", v, positive=True
            )
            ready_streams = _uint(root.get("ready_streams"), f"{root_path}.ready_streams", v)
            observable = _number(
                root.get("observable_stream_ms"), f"{root_path}.observable_stream_ms", v
            ) or 0.0
            observed = _number(
                root.get("observed_measurement_ms"), f"{root_path}.observed_measurement_ms", v
            ) or 0.0
            coverage = _number(
                root.get("coverage_ratio"), f"{root_path}.coverage_ratio", v, allow_none=True
            )
            complete = _boolean(
                root.get("observation_complete"), f"{root_path}.observation_complete", v
            )
            streams = _array(root.get("streams"), f"{root_path}.streams", v)
            stream_complete_values: list[bool] = []
            if expected_streams is not None and streams is not None:
                _eq(v, f"{root_path}.expected_streams", expected_streams, len(streams))
            if streams is not None:
                if len(streams) != subscriptions:
                    v.error(f"{root_path}.streams", "stream count must match subscriptions_per_root")
                expected_resource = PREPARATION_RESOURCES[index] if index < len(PREPARATION_RESOURCES) else None
                for subscription_index, stream_value in enumerate(streams):
                    stream_path = f"{root_path}.streams[{subscription_index}]"
                    stream = _object(stream_value, stream_path, WATCH_STREAM_KEYS, v)
                    if stream is None:
                        stream_complete_values.append(False)
                        continue
                    if expected_resource is not None and index < len(expected_roots):
                        expected_name = (
                            expected_resource
                            if subscriptions == 1
                            else f"{expected_resource}-{subscription_index}"
                        )
                        _eq(v, f"{stream_path}.root", stream.get("root"), expected_roots[index])
                        _eq(v, f"{stream_path}.stream", stream.get("stream"), expected_name)
                    ready = _boolean(stream.get("ready"), f"{stream_path}.ready", v)
                    duration = _number(
                        stream.get("observable_duration_ms"),
                        f"{stream_path}.observable_duration_ms",
                        v,
                    ) or 0.0
                    disconnects = _uint(
                        stream.get("disconnects"), f"{stream_path}.disconnects", v
                    ) or 0
                    terminal = _string(
                        stream.get("terminal"), f"{stream_path}.terminal", v, allow_none=True
                    )
                    events = _uint(stream.get("events"), f"{stream_path}.events", v) or 0
                    payload = _uint(
                        stream.get("payload_bytes"), f"{stream_path}.payload_bytes", v
                    ) or 0
                    stream_complete = _boolean(
                        stream.get("observation_complete"),
                        f"{stream_path}.observation_complete",
                        v,
                    )
                    if ready is False and duration:
                        v.error(
                            f"{stream_path}.observable_duration_ms",
                            "must be zero when stream is not ready",
                        )
                    if duration > observed + 1e-6:
                        v.error(
                            f"{stream_path}.observable_duration_ms",
                            "exceeds observed measurement window",
                        )
                    if stream_complete:
                        if ready is not True:
                            v.error(
                                f"{stream_path}.observation_complete",
                                "requires ready=true",
                            )
                        if disconnects != 0:
                            v.error(
                                f"{stream_path}.observation_complete",
                                "requires disconnects=0",
                            )
                        if duration + 1e-6 < observed:
                            v.error(
                                f"{stream_path}.observation_complete",
                                "requires observable duration to cover the measurement window",
                            )
                        if terminal not in (None, "context_canceled"):
                            v.error(
                                f"{stream_path}.observation_complete",
                                "terminal must be null or context_canceled",
                            )
                    if stream_complete is not None:
                        stream_complete_values.append(stream_complete)
                    event_total += events
                    payload_total += payload
                actual_ready = sum(
                    1
                    for stream in streams
                    if isinstance(stream, dict) and stream.get("ready") is True
                )
                _eq(v, f"{root_path}.ready_streams", ready_streams, actual_ready)
                actual_duration = sum(
                    float(stream.get("observable_duration_ms", 0.0))
                    for stream in streams
                    if isinstance(stream, dict)
                    and isinstance(stream.get("observable_duration_ms"), (int, float))
                    and not isinstance(stream.get("observable_duration_ms"), bool)
                )
                if abs(observable - actual_duration) > 1e-6:
                    v.error(f"{root_path}.observable_stream_ms", "must equal stream duration sum")
            if abs(observed - observed_ms) > 1e-5:
                v.error(
                    f"{root_path}.observed_measurement_ms",
                    "must equal report observed measurement duration",
                )
            denominator = float(expected_streams or 0) * observed
            if denominator > 0:
                expected_coverage = observable / denominator
                if coverage is None or abs(coverage - expected_coverage) > 1e-6:
                    v.error(
                        f"{root_path}.coverage_ratio",
                        "does not match observable duration / expected duration",
                    )
            elif coverage is not None:
                v.error(
                    f"{root_path}.coverage_ratio",
                    "must be null when observed duration is zero",
                )
            computed_root = (
                streams is not None
                and bool(streams)
                and len(stream_complete_values) == len(streams)
                and all(stream_complete_values)
            )
            root_complete_values.append(computed_root)
            if complete is not None:
                _eq(v, f"{root_path}.observation_complete", complete, computed_root)
        computed_report = bool(roots) and len(root_complete_values) == len(roots) and all(root_complete_values)
    else:
        computed_report = False
    if report_complete is not None:
        _eq(v, f"{path}.observation_complete", report_complete, computed_report)
    return event_total, payload_total, computed_report


def _validate_bucket(value: Any, v: Validator, path: str, logical_high: int, rpc_high: int) -> dict[str, Any] | None:
    bucket = _object(value, path, BUCKET_KEYS, v)
    if bucket is None:
        return None
    counter_fields = [
        "logical_offered",
        "logical_started",
        "logical_completed",
        "logical_errors",
        "rpc_started",
        "rpc_completed",
        "txn_started",
        "txn_completed",
        "watch_events",
        "watch_payload_bytes",
    ]
    for key in ["start_ns", "end_ns", *counter_fields]:
        _uint(bucket.get(key), f"{path}.{key}", v)
    start = _uint(bucket.get("start_ns"), f"{path}.start_ns", v) or 0
    end = _uint(bucket.get("end_ns"), f"{path}.end_ns", v) or 0
    if end < start:
        v.error(path, "end_ns must not precede start_ns")
    duration = end - start if end >= start else 0
    for key in ["txn_started_per_second", "txn_completed_per_second"]:
        rate = _number(bucket.get(key), f"{path}.{key}", v, allow_none=True)
        count_key = "txn_started" if key.startswith("txn_started") else "txn_completed"
        if duration:
            expected = float(bucket.get(count_key, 0)) * 1_000_000_000.0 / duration
            if rate is None or not _rate_matches(rate, expected):
                v.error(f"{path}.{key}", "does not match count / bucket duration")
        elif rate is not None:
            v.error(f"{path}.{key}", "must be null for a zero-width bucket")
    histogram_values = [
        ("logical_latency", logical_high),
        ("queue_latency", logical_high),
        ("rpc_latency", rpc_high),
    ]
    events = {
        "logical_latency": "logical_completed",
        "queue_latency": "logical_started",
        "rpc_latency": "rpc_completed",
    }
    for field, expected_high in histogram_values:
        observations = _validate_histogram(bucket.get(field), v, f"{path}.{field}", expected_high)
        _eq(v, f"{path}.{field}.observations", observations, bucket.get(events[field]))
    if end <= start:
        for field in counter_fields:
            if type(bucket.get(field)) is int and bucket[field] != 0:
                v.error(f"{path}.{field}", "must be zero for a zero-width bucket")
        for field, _expected_high in histogram_values:
            histogram = bucket.get(field)
            if not isinstance(histogram, dict):
                continue
            for histogram_field in ["sample_count", "overflow"]:
                if histogram.get(histogram_field) != 0:
                    v.error(
                        f"{path}.{field}.{histogram_field}",
                        "must be zero for a zero-width bucket",
                    )
            for histogram_field in ["p50_us", "p90_us", "p99_us", "max_us", "exact_max_us"]:
                if histogram.get(histogram_field) is not None:
                    v.error(
                        f"{path}.{field}.{histogram_field}",
                        "must be null for a zero-width bucket",
                    )
    return bucket


def _phase_conserved(rows: Sequence[Any], v: Validator, path: str) -> bool:
    ok = True
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        planned = row.get("planned")
        offered = row.get("offered")
        missed = row.get("missed_schedule")
        not_offered = row.get("not_offered_after_stop")
        accepted = row.get("accepted")
        full = row.get("dropped_full")
        closed = row.get("dropped_closed")
        if all(type(item) is int for item in [planned, offered, missed, not_offered]):
            if planned != _checked_sum((offered, missed, not_offered), v, f"{path}[{index}].planned"):
                v.error(f"{path}[{index}]", "planned != offered + missed_schedule + not_offered_after_stop")
                ok = False
        if all(type(item) is int for item in [offered, accepted, full, closed]):
            if offered != _checked_sum((accepted, full, closed), v, f"{path}[{index}].offered"):
                v.error(f"{path}[{index}]", "offered != accepted + dropped_full + dropped_closed")
                ok = False
    return ok


def _cohort_conserved(rows: Sequence[Any], v: Validator, path: str) -> bool:
    ok = True
    keys = [
        "accepted", "started_before_cutoff", "started_after_cutoff",
        "succeeded_before_cutoff", "failed_before_cutoff", "terminal_after_cutoff",
        "canceled_queued", "canceled_running", "cutoff_queued", "cutoff_running",
    ]
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not all(type(row.get(key)) is int for key in keys):
            continue
        at = f"{path}[{index}]"
        accepted = row["accepted"]
        started = row["started_before_cutoff"]
        after_started = row["started_after_cutoff"]
        terminal = _checked_sum((row["succeeded_before_cutoff"], row["failed_before_cutoff"]), v, at)
        after_terminal = row["terminal_after_cutoff"]
        canceled_queued = row["canceled_queued"]
        canceled_running = row["canceled_running"]
        if not 0 <= terminal <= started <= accepted:
            v.error(at, "requires terminal_before_cutoff <= started_before_cutoff <= accepted")
            ok = False
        if not 0 <= after_started <= accepted - started:
            v.error(at, "started_after_cutoff exceeds the accepted unstarted cohort")
            ok = False
        outcomes = _checked_sum((terminal, after_terminal, canceled_queued, canceled_running), v, at)
        ok = _eq(v, f"{at}.accepted", accepted, outcomes) and ok
        starts = _checked_sum((started, after_started), v, at)
        running_outcomes = _checked_sum((terminal, after_terminal, canceled_running), v, at)
        ok = _eq(v, f"{at}.started_cohort", starts, running_outcomes) and ok
        ok = _eq(v, f"{at}.cutoff_queued", row["cutoff_queued"], accepted - started) and ok
        ok = _eq(v, f"{at}.cutoff_running", row["cutoff_running"], started - terminal) and ok
    return ok


def _validate_report(report: Any, exit_code: int, plan_doc: Any | None, keyspace_doc: Any | None) -> dict[str, Any]:
    v = Validator()
    status: str | None = None
    config_info: dict[str, Any] | None = None
    measurement_started = False
    if not isinstance(report, dict):
        v.error("report", "must be an object")
        return _diagnostics(v, status, False, False, False)
    root = _object(report, "report", REPORT_KEYS, v)
    if root is None:
        return _diagnostics(v, status, False, False, False)
    _eq(v, "report.schema_version", root.get("schema_version"), 1)
    _uint(root.get("schema_version"), "report.schema_version", v, positive=True)
    experiment = _object(root.get("experiment"), "report.experiment", EXPERIMENT_KEYS, v)
    if experiment is not None:
        status = _string(experiment.get("status"), "report.experiment.status", v)
        if status not in {"completed", "failed", "interrupted"}:
            v.error("report.experiment.status", "must be completed, failed, or interrupted")
        _string(experiment.get("failure_phase"), "report.experiment.failure_phase", v, allow_none=True)
        _string(experiment.get("first_error"), "report.experiment.first_error", v, allow_none=True)
    config_info = _validate_config(root.get("resolved_config"), v, "report.resolved_config", runtime=True)
    timing = _object(root.get("timing"), "report.timing", TIMING_KEYS, v)
    configured_measurement_ms: int | None = None
    if timing is not None:
        for key in ["setup_ms", "prepare_ms", "observed_measurement_ms", "drain_ms", "watch_teardown_ms"]:
            _number(timing.get(key), f"report.timing.{key}", v, allow_none=True)
        configured_value = timing.get("configured_measurement_ms")
        if type(configured_value) is not int or configured_value <= 0:
            v.error(
                "report.timing.configured_measurement_ms",
                "must be a positive integer (booleans are not integers)",
            )
        else:
            configured_measurement_ms = configured_value
            if config_info is not None:
                try:
                    expected_duration = config_info["config"]["load"]["duration_ms"]
                except (KeyError, TypeError):
                    expected_duration = None
                if type(expected_duration) is int:
                    _eq(
                        v,
                        "report.timing.configured_measurement_ms",
                        configured_value,
                        expected_duration,
                    )
    _validate_plan_fields(root.get("plan"), config_info, v, "report.plan", report_plan=True)
    logical_high = 1
    rpc_high = 1
    preparation_high: int | None = None
    if config_info is not None:
        try:
            execution = config_info["config"]["execution"]
            load = config_info["config"]["load"]
            logical_high = (int(load["duration_ms"]) + int(execution["drain_timeout_ms"])) * 1000
            rpc_high = int(execution["request_timeout_ms"]) * 1000
            preparation_high = int(execution["preparation_timeout_ms"]) * 1000
        except (KeyError, TypeError, ValueError):
            pass
    preparation = _validate_phase_arrays(
        root.get("preparation"),
        v,
        "report.preparation",
        phase="preparation",
        logical_count=7,
        expected_identities=PREPARATION_IDENTITIES,
        logical_high=preparation_high or 1,
        rpc_high=rpc_high,
        include_resources=True,
    )
    preparation_rpc = preparation.get("rpc", []) if isinstance(preparation, dict) else []
    _validate_rpc_rates(
        preparation_rpc if isinstance(preparation_rpc, list) else [],
        phase="preparation",
        observed_ms=None,
        v=v,
        path="report.preparation.rpc",
    )
    measurement = _object(root.get("measurement"), "report.measurement", MEASUREMENT_KEYS, v)
    drain = _object(root.get("drain"), "report.drain", DRAIN_KEYS, v)
    measurement_rows: list[Any] = []
    drain_rows: list[Any] = []
    measurement_rpc: list[Any] = []
    drain_rpc: list[Any] = []
    observed_ms = 0.0
    observed_rate_ms: float | None = None
    if timing is not None:
        observed_value = timing.get("observed_measurement_ms")
        if isinstance(observed_value, (int, float)) and not isinstance(observed_value, bool):
            observed_ms = float(observed_value)
            observed_rate_ms = observed_ms
    measurement_watch_complete = False
    if measurement is not None:
        measurement_rows = _array(measurement.get("logical"), "report.measurement.logical", v) or []
        measurement_queue = _array(measurement.get("queue"), "report.measurement.queue", v) or []
        measurement_rpc = _array(measurement.get("rpc"), "report.measurement.rpc", v) or []
        if len(measurement_rows) != 18:
            v.error("report.measurement.logical", "must contain eighteen streams")
        if len(measurement_queue) != 18:
            v.error("report.measurement.queue", "must contain eighteen streams")
        if len(measurement_rpc) != 4:
            v.error("report.measurement.rpc", "must contain four methods")
        for index, row in enumerate(measurement_rows):
            obj = _validate_logical(row, v, f"report.measurement.logical[{index}]", logical_high, phase="measurement")
            if obj is not None and index < len(MEASUREMENT_IDENTITIES):
                _eq(v, f"report.measurement.logical[{index}].work_class", obj.get("work_class"), MEASUREMENT_IDENTITIES[index][0])
                _eq(v, f"report.measurement.logical[{index}].resource", obj.get("resource"), MEASUREMENT_IDENTITIES[index][1])
        for index, row in enumerate(measurement_queue):
            _validate_queue(row, v, f"report.measurement.queue[{index}]", logical_high)
            if isinstance(row, dict) and index < len(MEASUREMENT_IDENTITIES):
                _eq(v, f"report.measurement.queue[{index}].work_class", row.get("work_class"), MEASUREMENT_IDENTITIES[index][0])
                _eq(v, f"report.measurement.queue[{index}].resource", row.get("resource"), MEASUREMENT_IDENTITIES[index][1])
        for index, row in enumerate(measurement_rpc):
            obj = _validate_rpc(row, v, f"report.measurement.rpc[{index}]", rpc_high)
            if obj is not None and index < 4:
                _eq(v, f"report.measurement.rpc[{index}].method", obj.get("method"), RPC_METHODS[index])
        _validate_rpc_rates(
            measurement_rpc,
            phase="measurement",
            observed_ms=observed_rate_ms,
            v=v,
            path="report.measurement.rpc",
        )
        _validate_histogram(measurement.get("logical_total"), v, "report.measurement.logical_total", logical_high)
        _validate_histogram(measurement.get("queue_total"), v, "report.measurement.queue_total", logical_high)
        _validate_histogram(measurement.get("rpc_total"), v, "report.measurement.rpc_total", rpc_high)
        _validate_latency(measurement.get("latency"), v, "report.measurement.latency", logical_high, rpc_high)
        _validate_phase_histograms(measurement, v, "report.measurement")
        latency = measurement.get("latency")
        if isinstance(latency, dict):
            for field in ["logical_total", "queue_total", "rpc_total"]:
                _eq(v, f"report.measurement.latency.{field}", latency.get(field), measurement.get(field))
        txn_started_rate = _number(measurement.get("txn_started_per_second"), "report.measurement.txn_started_per_second", v, allow_none=True)
        txn_completed_rate = _number(measurement.get("txn_completed_per_second"), "report.measurement.txn_completed_per_second", v, allow_none=True)
        cutoff = _object(measurement.get("cutoff"), "report.measurement.cutoff", CUTOFF_KEYS, v)
        if cutoff is not None:
            configured_ns = cutoff.get("configured_ns")
            effective_ns = cutoff.get("effective_ns")
            if configured_ns is not None:
                _uint(configured_ns, "report.measurement.cutoff.configured_ns", v, positive=True)
                measurement_started = True
            if effective_ns is not None:
                _uint(effective_ns, "report.measurement.cutoff.effective_ns", v, positive=True)
            queued = _uint(cutoff.get("queued"), "report.measurement.cutoff.queued", v) or 0
            running = _uint(cutoff.get("running"), "report.measurement.cutoff.running", v) or 0
            if configured_ns is None and effective_ns is not None:
                v.error("report.measurement.cutoff", "effective_ns requires configured_ns")
            if configured_ns is not None and effective_ns is not None and effective_ns > configured_ns:
                v.error("report.measurement.cutoff.effective_ns", "must not exceed configured_ns")
            if config_info is not None and configured_ns is not None:
                try:
                    expected_ns = int(config_info["config"]["load"]["duration_ms"]) * 1_000_000
                except (KeyError, TypeError, ValueError):
                    expected_ns = None
                if expected_ns is not None:
                    _eq(v, "report.measurement.cutoff.configured_ns", configured_ns, expected_ns)
            expected_queued = _checked_sum(
                (row.get("cutoff_queued", 0) for row in measurement_rows if isinstance(row, dict) and type(row.get("cutoff_queued")) is int),
                v, "report.measurement.cutoff.queued",
            )
            expected_running = _checked_sum(
                (row.get("cutoff_running", 0) for row in measurement_rows if isinstance(row, dict) and type(row.get("cutoff_running")) is int),
                v, "report.measurement.cutoff.running",
            )
            _eq(v, "report.measurement.cutoff.queued", queued, expected_queued)
            _eq(v, "report.measurement.cutoff.running", running, expected_running)
        observed_ms = 0.0
        if timing is not None and isinstance(timing.get("observed_measurement_ms"), (int, float)) and not isinstance(timing.get("observed_measurement_ms"), bool):
            observed_ms = float(timing["observed_measurement_ms"])
        if cutoff is not None and cutoff.get("effective_ns") is not None:
            expected_observed = float(cutoff["effective_ns"]) / 1_000_000.0
            if abs(observed_ms - expected_observed) > 1e-5:
                v.error("report.timing.observed_measurement_ms", "does not match effective cutoff")
        elif timing is not None and timing.get("observed_measurement_ms") is not None:
            v.error("report.timing.observed_measurement_ms", "must be null before measurement starts")
        measurement_watch_totals = _validate_watch(
            measurement.get("watch"),
            config_info["config"] if config_info else {},
            observed_ms,
            v,
            "report.measurement.watch",
        )
        measurement_watch_events, measurement_watch_payload, measurement_watch_complete = measurement_watch_totals
        buckets = _array(measurement.get("buckets"), "report.measurement.buckets", v)
        bucket_rows: list[dict[str, Any]] = []
        if buckets is not None:
            expected_bucket_count = 0
            bucket_ns = 0
            configured_duration_ms = config_info["config"]["load"]["duration_ms"] if config_info else 0
            bucket_ms = config_info["config"]["report"]["bucket_ms"] if config_info else 0
            if bucket_ms:
                expected_bucket_count = (int(configured_duration_ms) + int(bucket_ms) - 1) // int(bucket_ms)
                bucket_ns = int(bucket_ms) * 1_000_000
            if len(buckets) != expected_bucket_count:
                v.error("report.measurement.buckets", f"must contain {expected_bucket_count} configured buckets")
            effective_ns = cutoff.get("effective_ns") if cutoff is not None else None
            for index, item in enumerate(buckets):
                row = _validate_bucket(item, v, f"report.measurement.buckets[{index}]", logical_high, rpc_high)
                if row is None:
                    continue
                bucket_rows.append(row)
                if effective_ns is None:
                    expected_start = expected_end = 0
                else:
                    expected_start = min(index * bucket_ns, int(effective_ns))
                    expected_end = min((index + 1) * bucket_ns, int(effective_ns))
                _eq(v, f"report.measurement.buckets[{index}].start_ns", row.get("start_ns"), expected_start)
                _eq(v, f"report.measurement.buckets[{index}].end_ns", row.get("end_ns"), expected_end)
            def bucket_sum(field: str) -> int:
                return sum(row.get(field, 0) for row in bucket_rows if type(row.get(field)) is int)
            sums = {
                "logical_offered": sum(row.get("offered", 0) for row in measurement_rows if isinstance(row, dict) and type(row.get("offered")) is int),
                "logical_started": sum(row.get("started_before_cutoff", 0) for row in measurement_rows if isinstance(row, dict) and type(row.get("started_before_cutoff")) is int),
                "logical_completed": sum((row.get("succeeded_before_cutoff", 0) + row.get("failed_before_cutoff", 0)) for row in measurement_rows if isinstance(row, dict) and type(row.get("succeeded_before_cutoff")) is int and type(row.get("failed_before_cutoff")) is int),
                "logical_errors": sum(row.get("failed_before_cutoff", 0) for row in measurement_rows if isinstance(row, dict) and type(row.get("failed_before_cutoff")) is int),
                "rpc_started": sum(row.get("started", 0) for row in measurement_rpc if isinstance(row, dict) and type(row.get("started")) is int),
                "rpc_completed": sum(row.get("completed", 0) for row in measurement_rpc if isinstance(row, dict) and type(row.get("completed")) is int),
                "txn_started": next((row.get("started", 0) for row in measurement_rpc if isinstance(row, dict) and row.get("method") == "txn" and type(row.get("started")) is int), 0),
                "txn_completed": next((row.get("completed", 0) for row in measurement_rpc if isinstance(row, dict) and row.get("method") == "txn" and type(row.get("completed")) is int), 0),
                "watch_events": measurement_watch_events,
                "watch_payload_bytes": measurement_watch_payload,
            }
            for field, expected in sums.items():
                if bucket_sum(field) != expected:
                    v.error(f"report.measurement.buckets.{field}", "bucket sum does not match measurement report")
            for field, total_field in [("logical_latency", "logical_total"), ("queue_latency", "queue_total"), ("rpc_latency", "rpc_total")]:
                _validate_histogram_total(
                    measurement.get(total_field),
                    (row.get(field) for row in bucket_rows),
                    v,
                    f"report.measurement.buckets.{field}",
                )
            if isinstance(measurement.get("rpc"), list):
                txn_rpc = next(
                    (
                        row
                        for row in measurement["rpc"]
                        if isinstance(row, dict) and row.get("method") == "txn"
                    ),
                    None,
                )
                if isinstance(txn_rpc, dict):
                    for field, expected in [
                        ("txn_started_per_second", txn_rpc.get("started_per_second")),
                        ("txn_completed_per_second", txn_rpc.get("completed_per_second")),
                    ]:
                        if not _rate_matches(measurement.get(field), expected):
                            v.error(
                                f"report.measurement.{field}",
                                "must match the measurement txn RPC rate",
                            )
        else:
            measurement_watch_events = measurement_watch_payload = 0
        _ = txn_started_rate, txn_completed_rate, measurement_started
    if drain is not None:
        drain_rows = _array(drain.get("logical"), "report.drain.logical", v) or []
        drain_queue = _array(drain.get("queue"), "report.drain.queue", v) or []
        drain_rpc = _array(drain.get("rpc"), "report.drain.rpc", v) or []
        if len(drain_rows) != 18:
            v.error("report.drain.logical", "must contain eighteen streams")
        if len(drain_queue) != 18:
            v.error("report.drain.queue", "must contain eighteen streams")
        if len(drain_rpc) != 4:
            v.error("report.drain.rpc", "must contain four methods")
        for index, row in enumerate(drain_rows):
            obj = _validate_logical(row, v, f"report.drain.logical[{index}]", logical_high, phase="drain")
            if obj is not None and index < len(MEASUREMENT_IDENTITIES):
                _eq(v, f"report.drain.logical[{index}].work_class", obj.get("work_class"), MEASUREMENT_IDENTITIES[index][0])
                _eq(v, f"report.drain.logical[{index}].resource", obj.get("resource"), MEASUREMENT_IDENTITIES[index][1])
                for key in ["planned", "offered", "accepted", "dropped_full", "dropped_closed", "missed_schedule", "not_offered_after_stop", "started_before_cutoff", "succeeded_before_cutoff", "failed_before_cutoff", "cutoff_queued", "cutoff_running"]:
                    if obj.get(key) != 0:
                        v.error(f"report.drain.logical[{index}].{key}", "must be zero in drain")
        for index, row in enumerate(drain_queue):
            _validate_queue(row, v, f"report.drain.queue[{index}]", logical_high)
            if isinstance(row, dict) and index < len(MEASUREMENT_IDENTITIES):
                _eq(v, f"report.drain.queue[{index}].work_class", row.get("work_class"), MEASUREMENT_IDENTITIES[index][0])
                _eq(v, f"report.drain.queue[{index}].resource", row.get("resource"), MEASUREMENT_IDENTITIES[index][1])
        for index, row in enumerate(drain_rpc):
            obj = _validate_rpc(row, v, f"report.drain.rpc[{index}]", rpc_high)
            if obj is not None and index < 4:
                _eq(v, f"report.drain.rpc[{index}].method", obj.get("method"), RPC_METHODS[index])
        _validate_histogram(drain.get("logical_total"), v, "report.drain.logical_total", logical_high)
        _validate_histogram(drain.get("queue_total"), v, "report.drain.queue_total", logical_high)
        _validate_histogram(drain.get("rpc_total"), v, "report.drain.rpc_total", rpc_high)
        _validate_phase_histograms(drain, v, "report.drain")
        _validate_watch(drain.get("watch"), config_info["config"] if config_info else {}, (float(timing.get("observed_measurement_ms")) if timing and isinstance(timing.get("observed_measurement_ms"), (int, float)) else 0.0), v, "report.drain.watch")
        _validate_rpc_rates(
            drain_rpc,
            phase="drain",
            observed_ms=None,
            v=v,
            path="report.drain.rpc",
        )
        _boolean(drain.get("timed_out"), "report.drain.timed_out", v)
    preparation_rows = preparation.get("logical", []) if isinstance(preparation, dict) else []
    plan_consistency_ok = _validate_plan_measurement_drain(
        root.get("plan"),
        measurement_rows,
        drain_rows,
        v,
    )
    planned_ok = _phase_conserved(preparation_rows, v, "report.preparation.logical") if isinstance(preparation_rows, list) else False
    planned_ok = (_phase_conserved(measurement_rows, v, "report.measurement.logical") and planned_ok and plan_consistency_ok) if isinstance(measurement_rows, list) else False
    offered_ok = planned_ok
    cohort_ok = _cohort_conserved(measurement_rows, v, "report.measurement.logical") if isinstance(measurement_rows, list) else False
    cohort_ok = (_cohort_conserved(preparation_rows, v, "report.preparation.logical") and cohort_ok) if isinstance(preparation_rows, list) else False
    unknown_ok = True
    for rows in [preparation_rows, measurement_rows]:
        for row in rows:
            if isinstance(row, dict):
                for unknown, failed in [("result_unknown_before_cutoff", "failed_before_cutoff"), ("result_unknown_after_cutoff", "failed_after_cutoff")]:
                    if type(row.get(unknown)) is int and type(row.get(failed)) is int and row[unknown] > row[failed]:
                        unknown_ok = False
    rpc_balanced = True
    if isinstance(preparation, dict) and isinstance(measurement, dict) and isinstance(drain, dict):
        for index, method in enumerate(RPC_METHODS):
            phases = [preparation.get("rpc", []), measurement.get("rpc", []), drain.get("rpc", [])]
            started = sum(rows[index].get("started", 0) for rows in phases if len(rows) > index and isinstance(rows[index], dict) and type(rows[index].get("started")) is int)
            completed = sum(rows[index].get("completed", 0) for rows in phases if len(rows) > index and isinstance(rows[index], dict) and type(rows[index].get("completed")) is int)
            if completed > started:
                v.error(f"report.rpc.{method}", "completed RPCs exceed started RPCs across phases")
                rpc_balanced = False
    no_active = cohort_ok and rpc_balanced
    accounting = _object(root.get("accounting"), "report.accounting", ACCOUNTING_KEYS, v)
    no_arithmetic = (
        _boolean(accounting.get("no_arithmetic_overflow"), "report.accounting.no_arithmetic_overflow", v)
        if accounting is not None else None
    )
    if no_arithmetic is False and status != "failed":
        v.error("report.accounting.no_arithmetic_overflow", "false requires a failed experiment")
    overflow = False
    for phase_obj in [preparation, measurement, drain]:
        if not isinstance(phase_obj, dict):
            continue
        for key in ["logical", "queue", "rpc"]:
            for row in phase_obj.get(key, []):
                if not isinstance(row, dict):
                    continue
                hists = []
                if key == "logical":
                    hists = [row.get("logical_latency"), row.get("queue_latency")]
                elif key == "queue":
                    hists = [row.get("latency")]
                else:
                    hists = [row.get("latency")]
                for hist in hists:
                    if isinstance(hist, dict) and type(hist.get("overflow")) is int and hist.get("overflow", 0) > 0:
                        overflow = True
        for key in ["logical_total", "queue_total", "rpc_total"]:
            hist = phase_obj.get(key)
            if isinstance(hist, dict) and type(hist.get("overflow")) is int and hist.get("overflow", 0) > 0:
                overflow = True
    observation_complete = measurement_watch_complete
    measurement_cutoff = measurement.get("cutoff") if isinstance(measurement, dict) else None
    observed_value = timing.get("observed_measurement_ms") if isinstance(timing, dict) else None
    observed_matches_config = (
        configured_measurement_ms is not None
        and isinstance(observed_value, (int, float))
        and not isinstance(observed_value, bool)
        and abs(float(observed_value) - float(configured_measurement_ms)) <= 1e-6
    )
    cutoff_matches_configured = (
        isinstance(measurement_cutoff, dict)
        and measurement_cutoff.get("effective_ns") == measurement_cutoff.get("configured_ns")
    )
    measurement_complete = (
        measurement_started
        and isinstance(timing, dict)
        and timing.get("observed_measurement_ms") is not None
        and cutoff_matches_configured
        and observed_matches_config
        and planned_ok
        and cohort_ok
        and no_active
        and no_arithmetic
    )
    if accounting is not None:
        expected_accounting = {
            "planned_conserved": planned_ok,
            "offered_conserved": offered_ok,
            "accepted_cohort_conserved": cohort_ok,
            "result_unknown_subset_failed": unknown_ok,
            "no_active_tokens": no_active,
            "measurement_complete": measurement_complete,
            "observation_complete": observation_complete,
            "histogram_overflow": overflow,
        }
        for key, expected in expected_accounting.items():
            actual = _boolean(accounting.get(key), f"report.accounting.{key}", v)
            if actual is not None:
                _eq(v, f"report.accounting.{key}", actual, expected)
    if experiment is not None and status is not None:
        expected_exit = {"completed": 0, "failed": 1, "interrupted": 130}.get(status)
        if expected_exit is not None and exit_code != expected_exit:
            v.error("exit_code", f"{status} report requires exit code {expected_exit}, got {exit_code}")
        phase = experiment.get("failure_phase")
        first_error = experiment.get("first_error")
        if status == "completed" and (phase is not None or first_error is not None):
            v.error("report.experiment", "completed report cannot carry failure context")
        if status == "failed" and (phase is None or first_error is None):
            v.error("report.experiment", "failed report requires failure_phase and first_error")
        if status == "completed":
            if not measurement_complete:
                v.error(
                    "report.experiment.status",
                    "completed report requires a complete measurement window",
                )
            if not cutoff_matches_configured:
                v.error(
                    "report.measurement.cutoff",
                    "completed report requires effective_ns == configured_ns",
                )
            if not observed_matches_config:
                v.error(
                    "report.timing.observed_measurement_ms",
                    "completed report must observe the configured measurement duration",
                )
    plan_checked = False
    if plan_doc is not None:
        before_plan_errors = len(v.errors)
        plan_root = _object(plan_doc, "plan", PLAN_OUTPUT_KEYS, v)
        if plan_root is not None:
            plan_config_info = _validate_config(plan_root.get("resolved_config"), v, "plan.resolved_config", runtime=False)
            _validate_plan_fields(plan_root, plan_config_info, v, "plan", report_plan=False)
            if config_info is not None and plan_config_info is not None:
                report_base = {key: config_info["config"][key] for key in BASE_CONFIG_KEYS}
                if report_base != plan_config_info["config"]:
                    v.error("plan.resolved_config", "does not match report resolved_config")
            if isinstance(root.get("plan"), dict):
                report_plan = root["plan"]
                for report_key, plan_key in [("resource_counts", "resource_counts"), ("periodic_plan", "periodic_plan"), ("pacing_tick_ms", "pacing_tick_ms"), ("estimated_measurement_memory_bytes", "estimated_measurement_memory_bytes")]:
                    if report_plan.get(report_key) != plan_root.get(plan_key):
                        v.error(f"report.plan.{report_key}", "does not match optional plan artifact")
                if report_plan.get("logical_streams") != plan_root.get("logical_plan", {}).get("streams"):
                    v.error("report.plan.logical_streams", "does not match optional plan artifact")
                if report_plan.get("update_rounding_deficit") != plan_root.get("logical_plan", {}).get("update_rounding_deficit"):
                    v.error("report.plan.update_rounding_deficit", "does not match optional plan artifact")
                if report_plan.get("read_rounding_deficit") != plan_root.get("logical_plan", {}).get("read_rounding_deficit"):
                    v.error("report.plan.read_rounding_deficit", "does not match optional plan artifact")
            plan_checked = len(v.errors) == before_plan_errors
    keyspace_checked = False
    keyspace_applicable = measurement_started and (
        isinstance(timing, dict) and timing.get("prepare_ms") is not None
    )
    keyspace_invalid = False
    if keyspace_doc is not None:
        before_keyspace_errors = len(v.errors)
        envelope_ok = _validate_keyspace(
            keyspace_doc,
            config_info,
            keyspace_applicable,
            v,
            "keyspace",
        )
        keyspace_checked = keyspace_applicable and envelope_ok
        keyspace_invalid = len(v.errors) > before_keyspace_errors
    v.check("schema", not any("unknown field" in error or "missing field" in error for error in v.errors))
    v.check("identity", not any(".work_class:" in error or ".resource:" in error or ".method:" in error for error in v.errors))
    v.check("conservation", planned_ok and offered_ok and cohort_ok and unknown_ok and rpc_balanced)
    v.check("arithmetic", v.arithmetic_valid)
    v.check("cutoff", not any("cutoff" in error for error in v.errors))
    v.check("buckets", not any(".buckets" in error for error in v.errors))
    v.check("rates", not any("per_second" in error or ".rate" in error for error in v.errors))
    v.check("histograms", not any("latency" in error or "histogram" in error for error in v.errors))
    v.check("exit", not any(error.startswith("exit_code:") for error in v.errors))
    evidence = {
        "plan": "checked" if plan_doc is not None and plan_checked else ("invalid" if plan_doc is not None else "not_provided"),
        "keyspace": (
            "not_provided"
            if keyspace_doc is None
            else "invalid"
            if keyspace_invalid
            else "checked"
            if keyspace_applicable and keyspace_checked
            else "not_applicable"
        ),
    }
    complete_evidence = not v.errors and status == "completed" and measurement_complete and plan_checked and keyspace_checked
    result = _diagnostics(v, status, complete_evidence, plan_checked, keyspace_checked)
    result["evidence"] = evidence
    return result


def _validate_keyspace(
    value: Any,
    config_info: dict[str, Any] | None,
    applicable: bool,
    v: Validator,
    path: str,
) -> bool:
    error_count = len(v.errors)
    obj = _object(value, path, {"header", "kvs", "count"}, v)
    if obj is None:
        return False
    if not isinstance(obj.get("header"), dict):
        v.error(f"{path}.header", "must be an object")
    if "kvs" in obj:
        kvs = _array(obj.get("kvs"), f"{path}.kvs", v)
    else:
        kvs = []
    if kvs is None:
        kvs = []
    if "count" in obj:
        count = _uint(obj.get("count"), f"{path}.count", v)
    else:
        count = len(kvs)
    if count is None:
        count = 0
    if count != len(kvs):
        v.error(f"{path}.count", "must equal kvs length")
    keys: list[bytes] = []
    for index, item in enumerate(kvs):
        item_path = f"{path}.kvs[{index}]"
        if not isinstance(item, dict):
            v.error(item_path, "must be an object")
            continue
        unknown = set(item) - {"key", "create_revision", "mod_revision", "version", "value", "lease"}
        for key in sorted(unknown):
            v.error(f"{item_path}.{key}", "unknown field")
        encoded = item.get("key")
        if not isinstance(encoded, str):
            v.error(f"{item_path}.key", "must be base64 text")
            continue
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError):
            v.error(f"{item_path}.key", "must be valid base64")
            continue
        keys.append(decoded)
        for field in ["create_revision", "mod_revision", "version"]:
            _uint(item.get(field), f"{item_path}.{field}", v, positive=field == "version")
        if "lease" in item:
            lease = _integer(item.get("lease"), f"{item_path}.lease", v)
            if applicable and lease is not None and lease != 0:
                v.error(f"{item_path}.lease", "native leases are not allowed")
    if applicable:
        if config_info is None:
            v.error(path, "config_info is unavailable for applicable keyspace validation")
        else:
            try:
                config = config_info["config"]
                counts = config_info["counts"]
                namespaces = int(config["topology"]["namespace_count"])
                expected_resources: set[bytes] = set()
                for kind in PREPARATION_RESOURCES:
                    count_value = int(counts[kind])
                    for object_id in range(count_value):
                        stem = RESOURCE_STEMS[kind]
                        name = f"{stem}-{object_id:05d}"
                        if kind in {"pods", "deployments", "replicasets", "daemonsets"}:
                            namespace = f"bench-ns-{object_id % min(count_value, namespaces):05d}"
                            key = f"{RESOURCE_PREFIXES[kind]}{namespace}/{name}"
                        else:
                            key = f"{RESOURCE_PREFIXES[kind]}{name}"
                        expected_resources.add(key.encode())
            except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
                v.error(path, f"config_info is invalid: {exc}")
            else:
                expected = expected_resources | {b"compact_rev_key"}
                actual = set(keys)
                if actual - expected:
                    v.error(f"{path}.kvs", f"contains {len(actual - expected)} unexpected keys")
                actual_resources = actual - {b"compact_rev_key"}
                if actual_resources != expected_resources:
                    missing = expected_resources - actual_resources
                    extra = actual_resources - expected_resources
                    if missing:
                        v.error(f"{path}.kvs", f"is missing {len(missing)} expected resource keys")
                    if extra:
                        v.error(f"{path}.kvs", f"contains {len(extra)} unexpected resource keys")
                if len(actual) not in {len(expected_resources), len(expected_resources) + 1}:
                    v.error(
                        f"{path}.kvs",
                        "does not contain the exact six-resource key count, with optional compact_rev_key",
                    )
    return len(v.errors) == error_count


def _diagnostics(v: Validator, status: str | None, complete_evidence: bool, plan_checked: bool, keyspace_checked: bool) -> dict[str, Any]:
    return {
        "report_valid": not v.errors,
        "experiment_status": status,
        "complete_evidence": complete_evidence,
        "errors": v.errors,
        "checks": v.checks,
        "evidence": {
            "plan": "checked" if plan_checked else "not_provided",
            "keyspace": "checked" if keyspace_checked else "not_provided",
        },
    }


def check_paths(report_path: str | Path, exit_code: int, plan_path: str | Path | None = None, keyspace_path: str | Path | None = None) -> dict[str, Any]:
    report = load_json(report_path)
    plan = load_json(plan_path) if plan_path is not None else None
    keyspace = load_json(keyspace_path) if keyspace_path is not None else None
    return _validate_report(report, exit_code, plan, keyspace)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a synthetic benchmark report offline")
    parser.add_argument("--report", required=True, help="run stdout JSON report")
    parser.add_argument("--exit-code", required=True, type=int, help="benchmark process exit code")
    parser.add_argument("--plan", help="optional plan stdout JSON")
    parser.add_argument("--keyspace", help="optional final etcdctl range JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
        result = check_paths(args.report, args.exit_code, args.plan, args.keyspace)
    except InputError as exc:
        print(json.dumps({"report_valid": False, "error": str(exc), "error_kind": "input"}, sort_keys=True))
        return 2
    except OSError as exc:
        print(json.dumps({"report_valid": False, "error": str(exc), "error_kind": "input"}, sort_keys=True))
        return 2
    except (TypeError, ValueError, KeyError, ZeroDivisionError) as exc:
        print(
            json.dumps(
                {
                    "report_valid": False,
                    "error": f"validator failure: {exc}",
                    "error_kind": "schema",
                },
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result.get("report_valid") else 1


if __name__ == "__main__":
    sys.exit(main())
