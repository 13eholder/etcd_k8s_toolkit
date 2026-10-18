#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import multiprocessing
import os
import signal
import socket
import subprocess
import stat
import sys
import tempfile
import textwrap
import threading
import time
import unittest
from unittest import mock
from pathlib import Path
from typing import Any
try:
    from . import check_report, run_synthetic
except ImportError:
    import check_report  # type: ignore[no-redef]
    import run_synthetic  # type: ignore[no-redef]



class _DrippingHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        try:
            while True:
                self.wfile.write(b"x" * 1024)
                self.wfile.flush()
                time.sleep(0.01)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class _BlockingHandler(BaseHTTPRequestHandler):
    started = threading.Event()
    release = threading.Event()

    def do_GET(self) -> None:
        type(self).started.set()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        type(self).release.wait()
        try:
            self.wfile.write(b"released")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, _format: str, *_args: Any) -> None:
        return

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "fixtures" / "report.json"
SMALL_CONFIG = HERE / "synthetic-small.json"


class CheckerBehaviorTests(unittest.TestCase):
    @staticmethod
    def _fixture() -> dict[str, Any]:
        return json.loads(FIXTURE.read_text(encoding="utf-8"))

    @staticmethod
    def _write_report(directory: Path, report: dict[str, Any], name: str = "report.json") -> Path:
        path = directory / name
        path.write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
        return path

    @staticmethod
    def _histogram(template: dict[str, Any], samples: int, *, overflow: int = 0) -> dict[str, Any]:
        result = dict(template)
        maximum = result["high_us"] + 1 if overflow else (1 if samples else None)
        result.update(
            sample_count=samples,
            overflow=overflow,
            max_us=maximum,
            exact_max_us=maximum,
            quantiles_unavailable=bool(overflow),
            p50_us=1 if samples and not overflow else None,
            p90_us=1 if samples and not overflow else None,
            p99_us=1 if samples and not overflow else None,
        )
        return result

    @classmethod
    def _terminal_report(cls, phase: str) -> dict[str, Any]:
        """One timed-out mutable attempt; drain starts before the cutoff."""
        report = cls._fixture()
        started = phase != "preparation"
        for name in ["preparation", "measurement", "drain"]:
            if name == "preparation" and started:
                continue
            section = report[name]
            for row in section["logical"]:
                for key, value in row.items():
                    if type(value) is int and key != "planned":
                        row[key] = 0
                row["first_error"] = None
                row["not_offered_after_stop"] = row["planned"] if name == "preparation" or not started else 0
                row["missed_schedule"] = row["planned"] if started and name == "measurement" else 0
                for field in ["logical_latency", "queue_latency"]:
                    row[field] = cls._histogram(row[field], 0)
            for row in section["queue"]:
                row["sample_count"] = 0
                row["latency"] = cls._histogram(row["latency"], 0)
            for row in section["rpc"]:
                for key, value in row.items():
                    if type(value) is int:
                        row[key] = 0
                row["started_per_second"] = 0.0 if started and name == "measurement" else None
                row["completed_per_second"] = 0.0 if started and name == "measurement" else None
                row["latency"] = cls._histogram(row["latency"], 0)
            for field in ["logical_total", "queue_total", "rpc_total"]:
                section[field] = cls._histogram(section[field], 0)
            for resource in section.get("resources", []):
                resource.update(created=0, failed=0)
        measurement = report["measurement"]
        for bucket in measurement["buckets"]:
            for key, value in bucket.items():
                if type(value) is int and key not in {"start_ns", "end_ns"}:
                    bucket[key] = 0
            if not started:
                bucket.update(start_ns=0, end_ns=0)
            for field in ["logical_latency", "queue_latency", "rpc_latency"]:
                bucket[field] = cls._histogram(bucket[field], 0)
            bucket["txn_started_per_second"] = 0.0 if started else None
            bucket["txn_completed_per_second"] = 0.0 if started else None
        for section in [measurement, report["drain"]]:
            watch = section["watch"]
            watch["observation_complete"] = started
            for root in watch["roots"]:
                if not started:
                    root.update(ready_streams=0, observable_stream_ms=0.0, observed_measurement_ms=0.0, coverage_ratio=None)
                root["observation_complete"] = started
                for stream in root["streams"]:
                    stream.update(events=0, payload_bytes=0, observation_complete=started)
                    if not started:
                        stream.update(ready=False, observable_duration_ms=0.0, disconnects=0, terminal=None)
        measurement["cutoff"]["queued"] = 0
        measurement["cutoff"]["running"] = 1 if phase == "drain" else 0
        if not started:
            measurement["cutoff"].update(configured_ns=None, effective_ns=None)
            report["timing"].update(observed_measurement_ms=None, drain_ms=None, watch_teardown_ms=None)
            report["experiment"].update(status="failed", failure_phase="preparation", first_error="controlled timeout")
        measurement["txn_started_per_second"] = 0.5 if started else None
        measurement["txn_completed_per_second"] = 0.5 if phase == "measurement" else (0.0 if started else None)
        report["accounting"].update(measurement_complete=started, observation_complete=started)

        cohort = report["preparation" if not started else "measurement"]["logical"][0]
        cohort.update(offered=1, accepted=1, started_before_cutoff=1)
        cohort["not_offered_after_stop"] = cohort["planned"] - 1 if not started else 0
        cohort["missed_schedule"] = cohort["planned"] - 1 if started else 0
        terminal = report[phase]["logical"][0]
        suffix = "after" if phase == "drain" else "before"
        terminal.update({
            f"failed_{suffix}_cutoff": 1,
            f"timeout_{suffix}_cutoff": 1,
            f"result_unknown_{suffix}_cutoff": 1,
        })
        terminal["logical_latency"] = cls._histogram(terminal["logical_latency"], 1)
        report[phase]["logical_total"] = copy.deepcopy(terminal["logical_latency"])
        cohort["queue_latency"] = cls._histogram(cohort["queue_latency"], 1)
        admission_phase = report["preparation" if not started else "measurement"]
        admission_phase["queue"][0].update(sample_count=1, latency=copy.deepcopy(cohort["queue_latency"]))
        admission_phase["queue_total"] = copy.deepcopy(cohort["queue_latency"])
        if phase == "drain":
            terminal["terminal_after_cutoff"] = 1
            for field in ["failed_after_cutoff", "timeout_after_cutoff", "result_unknown_after_cutoff", "terminal_after_cutoff"]:
                cohort[field] = terminal[field]
            cohort["cutoff_running"] = 1
        if not started:
            report["preparation"]["resources"][0]["failed"] = 1
        rpc = report[phase]["rpc"][1]
        rpc.update(completed=1, failed=1, timeout=1, result_unknown=1)
        rpc["latency"] = cls._histogram(rpc["latency"], 1)
        report[phase]["rpc_total"] = copy.deepcopy(rpc["latency"])
        admission_phase["rpc"][1]["started"] = 1
        if started:
            admission_phase["rpc"][1]["started_per_second"] = 0.5
            admission_phase["rpc"][1]["completed_per_second"] = 0.5 if phase == "measurement" else 0.0
            bucket = measurement["buckets"][0]
            bucket.update(logical_offered=1, logical_started=1, rpc_started=1, txn_started=1)
            bucket["queue_latency"] = copy.deepcopy(cohort["queue_latency"])
            bucket["txn_started_per_second"] = 1_000_000_000.0 / (bucket["end_ns"] - bucket["start_ns"])
            if phase == "measurement":
                bucket.update(logical_completed=1, logical_errors=1, rpc_completed=1, txn_completed=1)
                bucket["logical_latency"] = copy.deepcopy(terminal["logical_latency"])
                bucket["rpc_latency"] = copy.deepcopy(rpc["latency"])
                bucket["txn_completed_per_second"] = bucket["txn_started_per_second"]
        for field in ["logical_total", "queue_total", "rpc_total"]:
            measurement["latency"][field] = copy.deepcopy(measurement[field])
        return report

    def _check(self, report: dict[str, Any], *, plan: dict[str, Any] | None = None) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report_path = self._write_report(root, report)
            plan_path = self._write_report(root, plan, "plan.json") if plan is not None else None
            exit_code = {"completed": 0, "failed": 1, "interrupted": 130}[report["experiment"]["status"]]
            return check_report.check_paths(report_path, exit_code, plan_path)

    def test_timeout_unknown_terminal_counts_once_in_each_phase(self) -> None:
        for phase in ["preparation", "measurement", "drain"]:
            with self.subTest(phase=phase):
                report = self._terminal_report(phase)
                result = self._check(report)
                self.assertTrue(result["report_valid"], result["errors"])
                report[phase]["logical"][0]["logical_latency"]["sample_count"] += 1
                self.assertFalse(self._check(report)["report_valid"])

    def test_overflow_only_and_mixed_histograms_preserve_observations(self) -> None:
        for overflow_only in [False, True]:
            with self.subTest(overflow_only=overflow_only):
                report = self._terminal_report("drain")
                row = report["drain"]["logical"][0]
                samples = 0 if overflow_only else 1
                if not overflow_only:
                    row["failed_after_cutoff"] = 2
                    row["terminal_after_cutoff"] = 2
                    cohort = report["measurement"]["logical"][0]
                    cohort.update(accepted=2, offered=2, started_before_cutoff=2, failed_after_cutoff=2, terminal_after_cutoff=2, cutoff_running=2)
                    cohort["missed_schedule"] -= 1
                    cohort["queue_latency"] = self._histogram(cohort["queue_latency"], 2)
                    report["measurement"]["queue"][0].update(sample_count=2, latency=copy.deepcopy(cohort["queue_latency"]))
                    report["measurement"]["queue_total"] = copy.deepcopy(cohort["queue_latency"])
                    report["measurement"]["latency"]["queue_total"] = copy.deepcopy(cohort["queue_latency"])
                    report["measurement"]["buckets"][0].update(logical_offered=2, logical_started=2, queue_latency=copy.deepcopy(cohort["queue_latency"]))
                    report["measurement"]["cutoff"]["running"] = 2
                row["logical_latency"] = self._histogram(row["logical_latency"], samples, overflow=1)
                report["drain"]["logical_total"] = copy.deepcopy(row["logical_latency"])
                report["timing"]["drain_ms"] = 20_000.0
                report["accounting"]["histogram_overflow"] = True
                result = self._check(report)
                self.assertTrue(result["report_valid"], result["errors"])
                report["drain"]["logical_total"]["overflow"] += 1
                self.assertFalse(self._check(report)["report_valid"])

    def test_cross_cutoff_cohort_rejects_impossible_starts(self) -> None:
        report = self._terminal_report("drain")
        self.assertTrue(self._check(report)["report_valid"])
        for field in ["started_before_cutoff", "started_after_cutoff", "canceled_running"]:
            with self.subTest(field=field):
                invalid = copy.deepcopy(report)
                invalid["measurement"]["logical"][0][field] += 1
                self.assertFalse(self._check(invalid)["report_valid"])

    def test_histogram_copies_and_aggregates_reject_independent_mutations(self) -> None:
        for phase, field in [
            ("preparation", "queue"), ("measurement", "queue"), ("drain", "queue"),
            ("preparation", "logical_total"), ("measurement", "rpc_total"), ("drain", "logical_total"),
        ]:
            with self.subTest(phase=phase, field=field):
                report = self._terminal_report(phase)
                histogram = report[phase][field]
                if field == "queue":
                    histogram = histogram[0]["latency"]
                histogram["exact_max_us"] = 2
                histogram["max_us"] = 2
                self.assertFalse(self._check(report)["report_valid"])
        report = self._terminal_report("measurement")
        report["measurement"]["latency"]["logical_total"]["p50_us"] = 0
        self.assertFalse(self._check(report)["report_valid"])
        report = self._terminal_report("measurement")
        report["measurement"]["buckets"][0]["logical_latency"]["sample_count"] += 1
        self.assertFalse(self._check(report)["report_valid"])

    def test_canceled_cohort_does_not_create_completion_samples(self) -> None:
        for running in [False, True]:
            with self.subTest(running=running):
                report = self._terminal_report("measurement")
                measurement = report["measurement"]
                row = measurement["logical"][0]
                canceled = "canceled_running" if running else "canceled_queued"
                row.update(failed_before_cutoff=0, timeout_before_cutoff=0, result_unknown_before_cutoff=0)
                row[canceled] = 1
                report["drain"]["logical"][0][canceled] = 1
                row["logical_latency"] = self._histogram(row["logical_latency"], 0)
                row["started_before_cutoff"] = int(running)
                row["queue_latency"] = self._histogram(row["queue_latency"], int(running))
                row["cutoff_queued"] = int(not running)
                row["cutoff_running"] = int(running)
                measurement["cutoff"].update(queued=int(not running), running=int(running))
                measurement["queue"][0].update(sample_count=int(running), latency=copy.deepcopy(row["queue_latency"]))
                measurement["queue_total"] = copy.deepcopy(row["queue_latency"])
                measurement["logical_total"] = copy.deepcopy(row["logical_latency"])
                rpc = measurement["rpc"][1]
                rpc.update(started=int(running), completed=0, failed=0, timeout=0, result_unknown=0)
                rpc.update(started_per_second=0.5 if running else 0.0, completed_per_second=0.0)
                rpc["latency"] = self._histogram(rpc["latency"], 0)
                measurement["rpc_total"] = copy.deepcopy(rpc["latency"])
                measurement.update(txn_started_per_second=rpc["started_per_second"], txn_completed_per_second=0.0)
                bucket = measurement["buckets"][0]
                bucket.update(logical_started=int(running), logical_completed=0, logical_errors=0, rpc_started=int(running), rpc_completed=0, txn_started=int(running), txn_completed=0)
                bucket["txn_started_per_second"] = 1_000_000_000.0 / (bucket["end_ns"] - bucket["start_ns"]) if running else 0.0
                bucket["txn_completed_per_second"] = 0.0
                bucket["logical_latency"] = copy.deepcopy(row["logical_latency"])
                bucket["queue_latency"] = copy.deepcopy(row["queue_latency"])
                bucket["rpc_latency"] = copy.deepcopy(rpc["latency"])
                for field in ["logical_total", "queue_total", "rpc_total"]:
                    measurement["latency"][field] = copy.deepcopy(measurement[field])
                result = self._check(report)
                self.assertTrue(result["report_valid"], result["errors"])

    def test_integer_widths_and_arithmetic_failure_claim(self) -> None:
        report = self._terminal_report("measurement")
        scale = ((1 << 64) - 1) // 20
        report["resolved_config"]["load"]["read_ratio"] = {"numerator": scale, "denominator": 20 * scale}
        for stream in report["plan"]["logical_streams"]:
            if stream["work_class"] != "update":
                stream["rate"]["numerator"] *= scale
                stream["rate"]["denominator"] *= scale
        result = self._check(report)
        self.assertTrue(result["report_valid"], result["errors"])
        for field, value in [("started", 1 << 64), ("range_snapshot_revision", 1 << 63)]:
            invalid = self._fixture()
            invalid["measurement"]["rpc"][0][field] = value
            result = self._check(invalid)
            self.assertFalse(result["report_valid"])
            self.assertFalse(result["checks"]["arithmetic"])
        report = self._fixture()
        report["accounting"].update(no_arithmetic_overflow=False, measurement_complete=False)
        self.assertFalse(self._check(report)["report_valid"])
        report["experiment"].update(status="failed", failure_phase="execution", first_error="clock overflow")
        result = self._check(report)
        self.assertTrue(result["report_valid"], result["errors"])

    def test_plan_evidence_is_not_polluted_by_report_errors(self) -> None:
        report = self._fixture()
        plan = {
            "schema_version": 1,
            "resolved_config": {key: report["resolved_config"][key] for key in check_report.BASE_CONFIG_KEYS},
            "resource_counts": report["plan"]["resource_counts"],
            "logical_plan": {
                "streams": report["plan"]["logical_streams"],
                "update_rounding_deficit": report["plan"]["update_rounding_deficit"],
                "read_rounding_deficit": report["plan"]["read_rounding_deficit"],
            },
            "periodic_plan": report["plan"]["periodic_plan"],
            "pacing_tick_ms": report["plan"]["pacing_tick_ms"],
            "estimated_measurement_memory_bytes": report["plan"]["estimated_measurement_memory_bytes"],
        }
        report["measurement"]["rpc"][0]["started_per_second"] += 1
        result = self._check(report, plan=plan)
        self.assertFalse(result["report_valid"])
        self.assertEqual(result["evidence"]["plan"], "checked")
        plan["pacing_tick_ms"] = 2
        self.assertEqual(self._check(report, plan=plan)["evidence"]["plan"], "invalid")

    def test_accepted_report_is_valid(self) -> None:
        result = check_report.check_paths(FIXTURE, 0)
        self.assertTrue(result["report_valid"], result["errors"])
        self.assertEqual(result["experiment_status"], "completed")
        self.assertFalse(result["complete_evidence"])
        self.assertTrue(result["checks"]["conservation"])
    def test_hdr_quantile_may_reach_highest_equivalent_above_exact_max(self) -> None:
        report = self._fixture()
        histogram = report["preparation"]["logical"][0]["logical_latency"]
        self.assertGreater(histogram["p99_us"], histogram["exact_max_us"])
        result = check_report.check_paths(FIXTURE, 0)
        self.assertTrue(result["report_valid"], result["errors"])

    def test_hdr_quantile_above_equivalent_bucket_is_rejected(self) -> None:
        report = self._fixture()
        histogram = report["preparation"]["logical"][0]["logical_latency"]
        histogram["p99_us"] = histogram["exact_max_us"] + 256
        with tempfile.TemporaryDirectory() as temporary:
            path = self._write_report(Path(temporary), report)
            result = check_report.check_paths(path, 0)
        self.assertFalse(result["report_valid"])
        self.assertTrue(any("highest_equivalent" in error for error in result["errors"]))

    def test_counter_mutation_is_rejected(self) -> None:
        report = self._fixture()
        report["measurement"]["logical"][0]["offered"] += 1
        with tempfile.TemporaryDirectory() as temporary:
            path = self._write_report(Path(temporary), report)
            result = check_report.check_paths(path, 0)
        self.assertFalse(result["report_valid"])
        self.assertTrue(any("offered" in error or "planned" in error for error in result["errors"]))
        self.assertFalse(result["checks"]["conservation"])

    def test_bucket_mutation_is_rejected(self) -> None:
        report = self._fixture()
        report["measurement"]["buckets"][0]["logical_started"] += 1
        with tempfile.TemporaryDirectory() as temporary:
            path = self._write_report(Path(temporary), report)
            result = check_report.check_paths(path, 0)
        self.assertFalse(result["report_valid"])
        self.assertTrue(any("buckets" in error for error in result["errors"]))
        self.assertFalse(result["checks"]["buckets"])

    def test_exit_code_mismatch_is_rejected(self) -> None:
        result = check_report.check_paths(FIXTURE, 1)
        self.assertFalse(result["report_valid"])
        self.assertTrue(any(error.startswith("exit_code:") for error in result["errors"]))
        self.assertFalse(result["checks"]["exit"])

    def test_duplicate_json_key_is_input_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "duplicate.json"
            path.write_text('{"schema_version": 1, "schema_version": 1}', encoding="utf-8")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                return_code = check_report.main(["--report", str(path), "--exit-code", "0"])
        self.assertEqual(return_code, 2)
        diagnostic = json.loads(output.getvalue())
        self.assertEqual(diagnostic["error_kind"], "input")



class RunnerProcessBehaviorTests(unittest.TestCase):
    @staticmethod
    def _make_executable(path: Path, source: str) -> None:
        path.write_text(source, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    @staticmethod
    def _controlled_sources(run_body: str, *, plan_body: str | None = None) -> tuple[str, str]:
        report = json.loads(FIXTURE.read_text(encoding="utf-8"))
        base_keys = check_report.BASE_CONFIG_KEYS
        report_plan = report["plan"]
        plan = {
            "schema_version": 1,
            "resolved_config": {key: report["resolved_config"][key] for key in base_keys},
            "resource_counts": report_plan["resource_counts"],
            "logical_plan": {
                "streams": report_plan["logical_streams"],
                "update_rounding_deficit": report_plan["update_rounding_deficit"],
                "read_rounding_deficit": report_plan["read_rounding_deficit"],
            },
            "periodic_plan": report_plan["periodic_plan"],
            "pacing_tick_ms": report_plan["pacing_tick_ms"],
            "estimated_measurement_memory_bytes": report_plan["estimated_measurement_memory_bytes"],
        }
        report_literal = repr(json.dumps(report, separators=(",", ":")))
        plan_literal = repr(json.dumps(plan, separators=(",", ":")))
        if plan_body is None:
            plan_body = "    print(json.dumps(plan, separators=(',', ':')))"
        benchmark_source = f"""#!/usr/bin/env python3
import json
import sys
report = json.loads({report_literal})
plan = json.loads({plan_literal})
if sys.argv[1] == 'plan':
{plan_body}
else:
{run_body}
"""
        etcdctl_source = """#!/usr/bin/env python3
import json
import sys
if 'version' in sys.argv:
    print('etcdctl version: controlled-test')
elif '--help' in sys.argv:
    print('controlled help')
elif 'status' in sys.argv:
    print(json.dumps({'header': {'revision': 1}}))
else:
    print(json.dumps({'header': {'revision': 1}, 'kvs': [], 'count': 0}))
"""
        return benchmark_source, etcdctl_source

    @contextlib.contextmanager
    def _runner_files(self, benchmark_source: str, etcdctl_source: str):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            benchmark = root / "benchmark"
            etcdctl = root / "etcdctl"
            self._make_executable(benchmark, benchmark_source)
            self._make_executable(etcdctl, etcdctl_source)
            yield root, benchmark, etcdctl

    @staticmethod
    def _runner_arguments(
        benchmark: Path,
        etcdctl: Path,
        output_dir: Path,
        timeout: str,
        *,
        backend_metrics_url: str | None = None,
    ) -> list[str]:
        argv = [
            "--binary",
            str(benchmark),
            "--etcdctl",
            str(etcdctl),
            "--endpoint",
            "127.0.0.1:2379",
            "--config",
            str(SMALL_CONFIG),
            "--metrics-listen",
            "127.0.0.1:1",
            "--output-dir",
            str(output_dir),
            "--timeout-seconds",
            timeout,
        ]
        if backend_metrics_url is not None:
            argv.extend(["--backend-metrics-url", backend_metrics_url])
        return argv

    @classmethod
    def _invoke_runner(cls, benchmark: Path, etcdctl: Path, output_dir: Path, timeout: str, *, backend_metrics_url: str | None = None) -> int:
        return run_synthetic.main(cls._runner_arguments(
            benchmark, etcdctl, output_dir, timeout, backend_metrics_url=backend_metrics_url,
        ))

    @contextlib.contextmanager
    def _runner_process(self, benchmark: Path, etcdctl: Path, output_dir: Path, *, extra: list[str] | None = None):
        command = [
            sys.executable, str(HERE / "run_synthetic.py"),
            *self._runner_arguments(benchmark, etcdctl, output_dir, "5"),
            *(extra or []),
        ]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=15)

    @staticmethod
    def _listen_control(root: Path) -> socket.socket:
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(root / "control.sock"))
        listener.listen()
        listener.settimeout(10)
        return listener

    def test_plan_failure_or_spawn_failure_never_starts_run(self) -> None:
        run_body = "    from pathlib import Path\n    Path(__file__).with_name('run-started').write_text('unsafe')"
        for scenario in ["nonzero", "plan_spawn", "run_spawn"]:
            with self.subTest(scenario=scenario):
                plan_body = "    raise SystemExit(7)" if scenario == "nonzero" else None
                if scenario == "run_spawn":
                    plan_body = "    import os\n    os.chmod(sys.argv[0], 0o600)\n    print(json.dumps(plan))"
                benchmark_source, etcdctl_source = self._controlled_sources(run_body, plan_body=plan_body)
                if scenario == "plan_spawn":
                    etcdctl_source = """#!/usr/bin/env python3
import json, sys
from pathlib import Path
if 'version' in sys.argv:
    Path(__file__).with_name('benchmark').chmod(0o600)
    print('controlled version')
elif '--help' in sys.argv:
    print('controlled help')
else:
    print(json.dumps({'header': {'revision': 1}, 'kvs': [], 'count': 0}))
"""
                with self._runner_files(benchmark_source, etcdctl_source) as (root, benchmark, etcdctl):
                    output = root / "evidence"
                    self.assertEqual(self._invoke_runner(benchmark, etcdctl, output, "2"), 1)
                    self.assertFalse((root / "run-started").exists())
                    run = json.loads((output / "run-invocation.json").read_text())
                    self.assertFalse(run["executed"])
                    self.assertIsNone(run["exit_code"])
                    self.assertFalse((output / "run.stdout.json").exists())
                    check = json.loads((output / "check.json").read_text())
                    self.assertFalse(check["checked"])
                    self.assertFalse(check["report_valid"])

    def test_cancel_during_output_open_skips_launch(self) -> None:
        run_body = "    from pathlib import Path\n    Path(__file__).with_name('run-started').write_text('unsafe')"
        sources = self._controlled_sources(run_body)
        original_open = Path.open
        for stage in ["plan", "run"]:
            with self.subTest(stage=stage):
                with self._runner_files(*sources) as (root, benchmark, etcdctl):
                    output = root / "evidence"
                    delivered = threading.Event()

                    def open_with_interrupt(path: Path, *args: Any, **kwargs: Any):
                        stream = original_open(path, *args, **kwargs)
                        mode = args[0] if args else kwargs.get("mode", "r")
                        if path == output / f"{stage}.stdout.json" and mode == "wb":
                            handler = signal.getsignal(signal.SIGTERM)
                            self.assertTrue(callable(handler))

                            def observed_handler(signum: int, frame: Any) -> None:
                                handler(signum, frame)
                                delivered.set()

                            signal.signal(signal.SIGTERM, observed_handler)
                            os.kill(os.getpid(), signal.SIGTERM)
                            self.assertTrue(delivered.is_set())
                        return stream

                    with mock.patch.object(Path, "open", open_with_interrupt):
                        return_code = self._invoke_runner(benchmark, etcdctl, output, "2")
                    self.assertEqual(return_code, 130)
                    self.assertTrue(delivered.is_set())
                    invocation = json.loads((output / f"{stage}-invocation.json").read_text())
                    self.assertFalse(invocation["executed"])
                    self.assertIsNone(invocation["exit_code"])
                    self.assertFalse((root / "run-started").exists())
                    self.assertFalse((output / "run.stdout.json").exists())
                    self.assertFalse((output / "run.stderr.txt").exists())
                    check = json.loads((output / "check.json").read_text())
                    self.assertFalse(check["checked"])
                    self.assertFalse(check["report_valid"])

    def test_plan_signals_reap_child_and_skip_run(self) -> None:
        plan_body = textwrap.indent("""import os, signal, socket
from pathlib import Path
root = Path(sys.argv[sys.argv.index('--config') + 1]).parent.parent
signal.signal(signal.SIGINT, signal.SIG_DFL)
control = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
control.connect(str(root / 'control.sock'))
control.sendall((json.dumps({'pid': os.getpid()}) + '\\n').encode())
signal.pause()
""", "    ")
        run_body = "    from pathlib import Path\n    Path(__file__).with_name('run-started').write_text('unsafe')"
        for signum in [signal.SIGINT, signal.SIGTERM]:
            with self.subTest(signum=signum):
                sources = self._controlled_sources(run_body, plan_body=plan_body)
                with self._runner_files(*sources) as (root, benchmark, etcdctl):
                    output = root / "evidence"
                    with self._listen_control(root) as listener, self._runner_process(benchmark, etcdctl, output) as process:
                        connection, _ = listener.accept()
                        with connection, connection.makefile("rb") as events:
                            connection.settimeout(10)
                            child = json.loads(events.readline())
                            process.send_signal(signum)
                            _, stderr = process.communicate(timeout=15)
                    self.assertEqual(process.returncode, 130, stderr.decode())
                    self.assertFalse((root / "run-started").exists())
                    plan = json.loads((output / "plan-invocation.json").read_text())
                    self.assertEqual(plan["exit_code"], -signal.SIGINT)
                    self.assertEqual(plan["signal_sent"], ["SIGINT"])
                    run = json.loads((output / "run-invocation.json").read_text())
                    self.assertFalse(run["executed"])
                    self.assertFalse((output / "run.stdout.json").exists())
                    check = json.loads((output / "check.json").read_text())
                    self.assertFalse(check["checked"])
                    environment = json.loads((output / "environment.json").read_text())
                    self.assertIn(signal.Signals(signum).name, environment["signals_received"])
                    self.assertIn("metrics", environment["collection_skipped"])
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child["pid"], 0)

    def test_cancellation_after_successful_plan_prevents_run(self) -> None:
        plan_body = textwrap.indent("""import os, signal
signal.signal(signal.SIGINT, signal.SIG_IGN)
print(json.dumps(plan), flush=True)
os.kill(os.getppid(), signal.SIGINT)
""", "    ")
        run_body = "    from pathlib import Path\n    Path(__file__).with_name('run-started').write_text('unsafe')"
        sources = self._controlled_sources(run_body, plan_body=plan_body)
        with self._runner_files(*sources) as (root, benchmark, etcdctl):
            output = root / "evidence"
            with self._runner_process(benchmark, etcdctl, output) as process:
                _, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 130, stderr.decode())
            self.assertEqual(json.loads((output / "plan-invocation.json").read_text())["exit_code"], 0)
            self.assertFalse((root / "run-started").exists())
            self.assertFalse(json.loads((output / "run-invocation.json").read_text())["executed"])

    def test_sigterm_during_run_preserves_graceful_report_and_exit(self) -> None:
        run_body = textwrap.indent("""import os, signal, socket
from pathlib import Path
root = Path(sys.argv[sys.argv.index('--config') + 1]).parent.parent
control = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
control.connect(str(root / 'control.sock'))
def stop(_signum, _frame):
    control.sendall(b'SIGINT\\n')
    report['experiment'].update(status='interrupted', failure_phase='execution', first_error='controlled interrupt')
    print(json.dumps(report), flush=True)
    raise SystemExit(130)
signal.signal(signal.SIGINT, stop)
control.sendall((json.dumps({'pid': os.getpid()}) + '\\n').encode())
signal.pause()
""", "    ")
        with self._runner_files(*self._controlled_sources(run_body)) as (root, benchmark, etcdctl):
            output = root / "evidence"
            with self._listen_control(root) as listener, self._runner_process(benchmark, etcdctl, output) as process:
                connection, _ = listener.accept()
                with connection, connection.makefile("rb") as events:
                    connection.settimeout(10)
                    child = json.loads(events.readline())
                    process.send_signal(signal.SIGTERM)
                    _, stderr = process.communicate(timeout=15)
                    received = events.read()
            self.assertEqual(process.returncode, 130, stderr.decode())
            self.assertEqual(received, b"SIGINT\n")
            run = json.loads((output / "run-invocation.json").read_text())
            self.assertEqual(run["exit_code"], 130)
            self.assertEqual(run["signal_sent"], ["SIGINT"])
            self.assertEqual(run["received_signals"], ["SIGTERM"])
            self.assertTrue(json.loads((output / "check.json").read_text())["report_valid"])
            with self.assertRaises(ProcessLookupError):
                os.kill(child["pid"], 0)

    def test_watchdog_after_scheduled_interrupt_does_not_resend_sigint(self) -> None:
        run_body = textwrap.indent("""import os, signal, socket
from pathlib import Path
root = Path(sys.argv[sys.argv.index('--config') + 1]).parent.parent
control = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
control.connect(str(root / 'control.sock'))
def stop(_signum, _frame):
    control.sendall(b'SIGINT\\n')
signal.signal(signal.SIGINT, stop)
control.sendall((json.dumps({'pid': os.getpid()}) + '\\n').encode())
while True:
    signal.pause()
""", "    ")
        with self._runner_files(*self._controlled_sources(run_body)) as (root, benchmark, etcdctl):
            output = root / "evidence"
            extra = ["--interrupt-after-seconds", "1", "--timeout-seconds", "2"]
            with self._listen_control(root) as listener, self._runner_process(benchmark, etcdctl, output, extra=extra) as process:
                connection, _ = listener.accept()
                with connection, connection.makefile("rb") as events:
                    connection.settimeout(10)
                    child = json.loads(events.readline())
                    _, stderr = process.communicate(timeout=15)
                    received = events.read()
            self.assertEqual(process.returncode, 1, stderr.decode())
            self.assertEqual(received, b"SIGINT\n")
            run = json.loads((output / "run-invocation.json").read_text())
            self.assertTrue(run["watchdog_triggered"])
            self.assertTrue(run["forced_kill"])
            self.assertEqual(run["signal_sent"], ["SIGINT", "SIGKILL"])
            self.assertEqual(run["exit_code"], -signal.SIGKILL)
            with self.assertRaises(ProcessLookupError):
                os.kill(child["pid"], 0)

    @staticmethod
    def _start_server(handler: type[BaseHTTPRequestHandler]) -> tuple[ThreadingHTTPServer, threading.Thread]:
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, name="controlled-http-server", daemon=True)
        thread.start()
        return server, thread

    @staticmethod
    def _stop_server(server: ThreadingHTTPServer, thread: threading.Thread) -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3.0)

    def test_runner_watchdog_is_collection_failure_and_reaps_sampler(self) -> None:
        run_body = """    import signal
    def stop(_signum, _frame):
        raise SystemExit(130)
    signal.signal(signal.SIGINT, stop)
    signal.pause()
"""
        benchmark_source, etcdctl_source = self._controlled_sources(run_body)
        with self._runner_files(benchmark_source, etcdctl_source) as (root, benchmark, etcdctl):
            output_dir = root / "evidence"
            return_code = self._invoke_runner(benchmark, etcdctl, output_dir, "0.2")
            self.assertEqual(return_code, 1)
            invocation = json.loads((output_dir / "run-invocation.json").read_text(encoding="utf-8"))
            self.assertTrue(invocation["watchdog_triggered"])
            environment = json.loads((output_dir / "environment.json").read_text(encoding="utf-8"))
            self.assertTrue(any("watchdog" in error for error in environment["collection_errors"]))
            self.assertFalse(any(thread.name == "synthetic-evidence-sampler" for thread in threading.enumerate()))
            self.assertEqual(multiprocessing.active_children(), [])

    def test_http_scrape_timeout_is_bounded_and_reaped(self) -> None:
        server, thread = self._start_server(_DrippingHandler)
        previous_budget = run_synthetic.HTTP_SCRAPE_BUDGET_SECONDS
        budget = 0.2
        run_synthetic.HTTP_SCRAPE_BUDGET_SECONDS = budget
        try:
            with tempfile.TemporaryDirectory() as temporary:
                sampler = run_synthetic.MetricSampler(
                    threading.Event(),
                    Path(temporary) / "metrics.jsonl",
                    Path(temporary) / "process.jsonl",
                    os.getpid(),
                    f"http://127.0.0.1:{server.server_port}/metrics",
                    None,
                )
                output = io.StringIO()
                started = time.monotonic()
                sampler._sample_http(
                    output,
                    "backend",
                    f"http://127.0.0.1:{server.server_port}/metrics",
                    0.0,
                )
                elapsed = time.monotonic() - started
                record = json.loads(output.getvalue())
            self.assertEqual(record["outcome"], "timeout")
            self.assertLessEqual(
                elapsed,
                budget + run_synthetic.HTTP_SCRAPE_TERMINATE_GRACE_SECONDS + 1.0,
            )
            self.assertEqual(len(sampler.collection_errors), 1)
            self.assertEqual(multiprocessing.active_children(), [])
        finally:
            run_synthetic.HTTP_SCRAPE_BUDGET_SECONDS = previous_budget
            self._stop_server(server, thread)

    def test_http_scrape_stop_is_cancelled_without_collection_error(self) -> None:
        _BlockingHandler.started = threading.Event()
        _BlockingHandler.release = threading.Event()
        server, thread = self._start_server(_BlockingHandler)
        try:
            with tempfile.TemporaryDirectory() as temporary:
                stop_event = threading.Event()
                sampler = run_synthetic.MetricSampler(
                    stop_event,
                    Path(temporary) / "metrics.jsonl",
                    Path(temporary) / "process.jsonl",
                    os.getpid(),
                    f"http://127.0.0.1:{server.server_port}/metrics",
                    None,
                )
                output = io.StringIO()
                scrape_thread = threading.Thread(
                    target=sampler._sample_http,
                    args=(
                        output,
                        "backend",
                        f"http://127.0.0.1:{server.server_port}/metrics",
                        0.0,
                    ),
                    name="controlled-http-scrape",
                )
                scrape_thread.start()
                self.assertTrue(_BlockingHandler.started.wait(timeout=3.0))
                stop_event.set()
                scrape_thread.join(timeout=3.0)
                self.assertFalse(scrape_thread.is_alive())
                record = json.loads(output.getvalue())
            self.assertEqual(record["outcome"], "cancelled")
            self.assertNotIn("error", record)
            self.assertEqual(sampler.collection_errors, [])
            self.assertEqual(multiprocessing.active_children(), [])
        finally:
            _BlockingHandler.release.set()
            self._stop_server(server, thread)

    def test_runner_backend_scrape_timeout_is_collection_failure(self) -> None:
        server, thread = self._start_server(_DrippingHandler)
        previous_budget = run_synthetic.HTTP_SCRAPE_BUDGET_SECONDS
        run_synthetic.HTTP_SCRAPE_BUDGET_SECONDS = 0.2
        run_body = """    import signal
    def stop(_signum, _frame):
        raise SystemExit(130)
    signal.signal(signal.SIGINT, stop)
    signal.pause()
"""
        benchmark_source, etcdctl_source = self._controlled_sources(run_body)
        try:
            with self._runner_files(benchmark_source, etcdctl_source) as (root, benchmark, etcdctl):
                output_dir = root / "evidence"
                return_code = self._invoke_runner(
                    benchmark,
                    etcdctl,
                    output_dir,
                    "1.0",
                    backend_metrics_url=f"http://127.0.0.1:{server.server_port}/metrics",
                )
                self.assertEqual(return_code, 1)
                environment = json.loads((output_dir / "environment.json").read_text(encoding="utf-8"))
                self.assertTrue(any("backend metrics scrape timeout" in error for error in environment["collection_errors"]))
                records = [
                    json.loads(line)
                    for line in (output_dir / "metrics.jsonl").read_text(encoding="utf-8").splitlines()
                    if line
                ]
                self.assertTrue(any(record.get("target") == "backend" and record.get("outcome") == "timeout" for record in records))
                self.assertEqual(multiprocessing.active_children(), [])
        finally:
            run_synthetic.HTTP_SCRAPE_BUDGET_SECONDS = previous_budget
            self._stop_server(server, thread)

    def test_runner_retains_raw_artifacts_for_controlled_process(self) -> None:
        report = json.loads(FIXTURE.read_text(encoding="utf-8"))
        base_keys = check_report.BASE_CONFIG_KEYS
        report_plan = report["plan"]
        plan = {
            "schema_version": 1,
            "resolved_config": {key: report["resolved_config"][key] for key in base_keys},
            "resource_counts": report_plan["resource_counts"],
            "logical_plan": {
                "streams": report_plan["logical_streams"],
                "update_rounding_deficit": report_plan["update_rounding_deficit"],
                "read_rounding_deficit": report_plan["read_rounding_deficit"],
            },
            "periodic_plan": report_plan["periodic_plan"],
            "pacing_tick_ms": report_plan["pacing_tick_ms"],
            "estimated_measurement_memory_bytes": report_plan["estimated_measurement_memory_bytes"],
        }
        report_literal = repr(json.dumps(report, separators=(",", ":")))
        plan_literal = repr(json.dumps(plan, separators=(",", ":")))
        benchmark_source = f"""#!/usr/bin/env python3
import json
import sys
report = json.loads({report_literal})
plan = json.loads({plan_literal})
if sys.argv[1] == 'plan':
    print(json.dumps(plan, separators=(',', ':')))
else:
    print(json.dumps(report, separators=(',', ':')))
"""
        etcdctl_source = """#!/usr/bin/env python3
import json
import sys
if 'version' in sys.argv:
    print('etcdctl version: controlled-test')
elif '--help' in sys.argv:
    print('controlled help')
elif 'status' in sys.argv:
    print(json.dumps({'header': {'revision': 1}}))
else:
    print(json.dumps({'header': {'revision': 1}, 'kvs': [], 'count': 0}))
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            benchmark = root / "benchmark"
            etcdctl = root / "etcdctl"
            self._make_executable(benchmark, benchmark_source)
            self._make_executable(etcdctl, etcdctl_source)
            output_dir = root / "evidence"
            return_code = run_synthetic.main(
                [
                    "--binary",
                    str(benchmark),
                    "--etcdctl",
                    str(etcdctl),
                    "--endpoint",
                    "127.0.0.1:2379",
                    "--config",
                    str(SMALL_CONFIG),
                    "--metrics-listen",
                    "127.0.0.1:1",
                    "--output-dir",
                    str(output_dir),
                    "--timeout-seconds",
                    "5",
                ]
            )
            self.assertEqual(return_code, 1)
            self.assertEqual((output_dir / "input.json").read_bytes(), SMALL_CONFIG.read_bytes())
            for artifact in [
                "plan.stdout.json",
                "plan.stderr.txt",
                "plan-invocation.json",
                "run.stdout.json",
                "run.stderr.txt",
                "run-invocation.json",
                "metrics.jsonl",
                "process.jsonl",
                "environment.json",
                "check.json",
                "backend-status-before.stdout.txt",
                "backend-status-after.stdout.txt",
                "backend-keyspace-before.stdout.txt",
                "backend-keyspace-after.stdout.txt",
            ]:
                self.assertTrue((output_dir / artifact).is_file(), artifact)
            invocation = json.loads((output_dir / "run-invocation.json").read_text(encoding="utf-8"))
            self.assertIsInstance(invocation["argv"], list)
            self.assertEqual(invocation["argv"][1], "run")
            self.assertEqual(invocation["exit_code"], 0)
            self.assertFalse(invocation["watchdog_triggered"])
            self.assertFalse(invocation["forced_kill"])

    def test_runner_rejects_existing_output_directory_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary) / "existing"
            output_dir.mkdir()
            sentinel = output_dir / "sentinel"
            sentinel.write_text("keep", encoding="utf-8")
            return_code = run_synthetic.main(
                [
                    "--binary",
                    sys.executable,
                    "--etcdctl",
                    sys.executable,
                    "--endpoint",
                    "127.0.0.1:2379",
                    "--config",
                    str(SMALL_CONFIG),
                    "--metrics-listen",
                    "127.0.0.1:1",
                    "--output-dir",
                    str(output_dir),
                    "--timeout-seconds",
                    "1",
                ]
            )
            self.assertEqual(return_code, 2)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
