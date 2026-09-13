#!/usr/bin/env python3
from __future__ import annotations

import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import multiprocessing
import os
import signal
import stat
import sys
import tempfile
import threading
import time
import unittest
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

    def test_small_config_is_complete_and_exact(self) -> None:
        config = json.loads(SMALL_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(
            set(config),
            {
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
            },
        )
        self.assertEqual(config["topology"], {"node_count": 4, "pod_count": 80, "namespace_count": 1})
        self.assertEqual(config["controllers"], {"daemonset_count": 1, "pods_per_deployment": 4})


class RunnerProcessBehaviorTests(unittest.TestCase):
    @staticmethod
    def _make_executable(path: Path, source: str) -> None:
        path.write_text(source, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)

    @staticmethod
    def _controlled_sources(run_body: str) -> tuple[str, str]:
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
    def _invoke_runner(
        benchmark: Path,
        etcdctl: Path,
        output_dir: Path,
        timeout: str,
        *,
        backend_metrics_url: str | None = None,
    ) -> int:
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
        return run_synthetic.main(argv)

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
