#!/usr/bin/env python3
"""Run the benchmark and retain raw, bounded evidence.

The backend is an explicit operator-owned endpoint.  This runner never starts,
stops, clears, or reconfigures it; it only invokes the supplied benchmark,
etcdctl read-only commands, and the offline checker.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import shutil
import multiprocessing
import subprocess
import sys
import threading
import time
import tempfile
from pathlib import Path
from typing import Any, Sequence
from urllib import error as urllib_error
from urllib import request as urllib_request

try:
    from . import check_report
except ImportError:  # Executed directly from this directory.
    import check_report  # type: ignore[no-redef]


class InvocationError(Exception):
    """Invalid runner invocation or an output directory that cannot be used."""


class SignalState:
    def __init__(self) -> None:
        self.user_interrupt = False
        self.forwarded_to_child = False
        self.child: subprocess.Popen[bytes] | None = None
HTTP_SCRAPE_BUDGET_SECONDS = 5.0
HTTP_SCRAPE_POLL_SECONDS = 0.05
HTTP_SCRAPE_TERMINATE_GRACE_SECONDS = 0.25


def _http_fetch_worker(url: str, result_path: str) -> None:
    result: dict[str, Any] = {}
    try:
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        request = urllib_request.Request(url, method="GET")
        with opener.open(request, timeout=HTTP_SCRAPE_BUDGET_SECONDS) as response:
            body = response.read()
            result.update(
                {
                    "outcome": "ok",
                    "status": int(getattr(response, "status", response.getcode())),
                    "body": body.decode("utf-8", errors="replace"),
                    "body_bytes": len(body),
                }
            )
    except urllib_error.HTTPError as exc:
        result.update(
            {
                "outcome": "http_error",
                "error": f"{type(exc).__name__}: {exc}",
                "status": int(exc.code),
            }
        )
    except (OSError, urllib_error.URLError, ValueError, TypeError) as exc:
        result.update({"outcome": "http_error", "error": f"{type(exc).__name__}: {exc}"})
    except BaseException as exc:
        result.update({"outcome": "http_error", "error": f"{type(exc).__name__}: {exc}"})
    try:
        temporary_path = Path(f"{result_path}.tmp")
        _write_json(temporary_path, result)
        temporary_path.replace(result_path)
    except BaseException:
        pass


def _reap_http_process(process: multiprocessing.Process) -> None:
    if process.is_alive():
        try:
            process.terminate()
        except OSError:
            pass
        process.join(HTTP_SCRAPE_TERMINATE_GRACE_SECONDS)
    if process.is_alive():
        try:
            process.kill()
        except OSError:
            pass
        process.join()
    else:
        process.join()
    process.close()




class MetricSampler(threading.Thread):
    """Low-frequency HTTP and /proc sampler with explicit stop/join ownership."""

    def __init__(
        self,
        stop_event: threading.Event,
        metrics_path: Path,
        process_path: Path,
        pid: int,
        benchmark_url: str,
        backend_url: str | None,
    ) -> None:
        super().__init__(name="synthetic-evidence-sampler")
        self.stop_event = stop_event
        self.metrics_path = metrics_path
        self.process_path = process_path
        self.pid = pid
        self.benchmark_url = benchmark_url
        self.backend_url = backend_url
        self.error: str | None = None
        self.collection_errors: list[str] = []
        self.scrape_outcomes: dict[str, dict[str, int]] = {}

    def run(self) -> None:
        try:
            with self.metrics_path.open("w", encoding="utf-8") as metrics_file, self.process_path.open(
                "w", encoding="utf-8"
            ) as process_file:
                started = time.monotonic()
                while True:
                    elapsed = time.monotonic() - started
                    self._sample_http(metrics_file, "benchmark", self.benchmark_url, elapsed)
                    if self.backend_url:
                        self._sample_http(metrics_file, "backend", self.backend_url, elapsed)
                    self._sample_process(process_file, elapsed)
                    metrics_file.flush()
                    process_file.flush()
                    if self.stop_event.wait(1.0):
                        break
        except BaseException as exc:  # Preserve sampler failure in the caller's evidence.
            self.error = f"{type(exc).__name__}: {exc}"

    def _sample_http(self, output: Any, target: str, url: str, elapsed: float) -> None:
        record: dict[str, Any] = {
            "elapsed_seconds": elapsed,
            "target": target,
            "url": url,
        }
        outcome: str | None = None
        if self.stop_event.is_set():
            outcome = "cancelled"
        else:
            with tempfile.TemporaryDirectory(prefix="synthetic-http-") as temporary:
                result_path = Path(temporary) / "result.json"
                context = multiprocessing.get_context("spawn")
                process = context.Process(
                    target=_http_fetch_worker,
                    args=(url, str(result_path)),
                )
                try:
                    process.start()
                except (OSError, RuntimeError) as exc:
                    outcome = "http_error"
                    record["error"] = f"{type(exc).__name__}: {exc}"
                else:
                    deadline = time.monotonic() + HTTP_SCRAPE_BUDGET_SECONDS
                    while process.is_alive():
                        if self.stop_event.is_set():
                            outcome = "cancelled"
                            _reap_http_process(process)
                            break
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            outcome = "timeout"
                            _reap_http_process(process)
                            break
                        process.join(min(HTTP_SCRAPE_POLL_SECONDS, remaining))
                    if outcome is None:
                        _reap_http_process(process)
                        try:
                            child_result = json.loads(result_path.read_text(encoding="utf-8"))
                        except (OSError, UnicodeError, TypeError, ValueError) as exc:
                            child_result = {
                                "outcome": "http_error",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        if not isinstance(child_result, dict):
                            child_result = {
                                "outcome": "http_error",
                                "error": "HTTP worker returned a non-object result",
                            }
                        outcome = child_result.get("outcome")
                        if outcome not in {"ok", "http_error"}:
                            outcome = "http_error"
                            child_result = {
                                "outcome": outcome,
                                "error": "HTTP worker returned an invalid outcome",
                            }
                        for key in ["status", "body", "body_bytes", "error"]:
                            if key in child_result:
                                record[key] = child_result[key]
        record["outcome"] = outcome or "http_error"
        target_counts = self.scrape_outcomes.setdefault(target, {})
        target_counts[record["outcome"]] = target_counts.get(record["outcome"], 0) + 1
        if record["outcome"] == "timeout":
            record["error"] = f"HTTP scrape exceeded {HTTP_SCRAPE_BUDGET_SECONDS:.3f}s budget"
            self.collection_errors.append(f"{target} metrics scrape timeout")
        elif record["outcome"] == "http_error" and "error" not in record:
            record["error"] = "HTTP scrape worker failed without a diagnostic"
        if record["outcome"] == "cancelled":
            record.pop("error", None)
        output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")

    def _sample_process(self, output: Any, elapsed: float) -> None:
        record: dict[str, Any] = {
            "elapsed_seconds": elapsed,
            "pid": self.pid,
            "available": False,
            "units": {
                "elapsed_seconds": "seconds",
                "utime_ticks": "clock_ticks",
                "stime_ticks": "clock_ticks",
                "rss_pages": "pages",
                "cpu_user_seconds": "seconds",
                "cpu_system_seconds": "seconds",
                "rss_bytes": "bytes",
            },
        }
        try:
            stat_text = Path(f"/proc/{self.pid}/stat").read_text(encoding="utf-8")
            close = stat_text.rfind(")")
            if close < 0:
                raise ValueError("malformed /proc stat")
            fields = stat_text[close + 2 :].split()
            if len(fields) < 22:
                raise ValueError("short /proc stat")
            utime_ticks = int(fields[11])
            stime_ticks = int(fields[12])
            rss_pages = int(fields[21])
            ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
            record.update(
                {
                    "available": True,
                    "raw": {
                        "utime_ticks": utime_ticks,
                        "stime_ticks": stime_ticks,
                        "rss_pages": rss_pages,
                        "clock_ticks_per_second": ticks_per_second,
                        "page_size": page_size,
                    },
                    "cpu_user_seconds": utime_ticks / ticks_per_second,
                    "cpu_system_seconds": stime_ticks / ticks_per_second,
                    "rss_bytes": rss_pages * page_size,
                }
            )
        except (OSError, ValueError, TypeError) as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
        output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def _write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as output:
        json.dump(value, output, sort_keys=True, separators=(",", ":"))
        output.write("\n")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_executable(path_text: str, label: str) -> Path:
    path = Path(path_text).expanduser().resolve()
    if not path.is_file():
        raise InvocationError(f"{label} is not a regular file: {path}")
    if not os.access(path, os.X_OK):
        raise InvocationError(f"{label} is not executable: {path}")
    return path


def _require_file(path_text: str, label: str) -> Path:
    path = Path(path_text).expanduser().resolve()
    if not path.is_file():
        raise InvocationError(f"{label} is not a regular file: {path}")
    return path


def _send_group(process: subprocess.Popen[bytes], signum: signal.Signals) -> bool:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signum)
        else:
            process.send_signal(signum)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False


def _command_metadata(
    argv: Sequence[str],
    returncode: int | None,
    elapsed: float,
    stdout_path: Path,
    stderr_path: Path,
    *,
    signal_sent: list[str] | None = None,
    watchdog_triggered: bool = False,
    forced_kill: bool = False,
    error: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "argv": list(argv),
        "exit_code": returncode,
        "elapsed_seconds": elapsed,
        "stdout_bytes": stdout_path.stat().st_size if stdout_path.exists() else 0,
        "stderr_bytes": stderr_path.stat().st_size if stderr_path.exists() else 0,
        "signal_sent": signal_sent or [],
        "watchdog_triggered": watchdog_triggered,
        "forced_kill": forced_kill,
    }
    if error is not None:
        result["error"] = error
    return result


def _run_capture(
    argv: Sequence[str],
    output_dir: Path,
    stem: str,
    timeout: float,
    *,
    stdout_suffix: str = ".stdout.txt",
) -> dict[str, Any]:
    stdout_path = output_dir / f"{stem}{stdout_suffix}"
    stderr_path = output_dir / f"{stem}.stderr.txt"
    invocation_path = output_dir / f"{stem}-invocation.json"
    started = time.monotonic()
    returncode: int | None = None
    error: str | None = None
    forced_kill = False
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        try:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                close_fds=True,
                start_new_session=True,
            )
            try:
                returncode = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                forced_kill = True
                _send_group(process, signal.SIGKILL)
                returncode = process.wait()
                error = f"command exceeded timeout {timeout:.3f}s"
        except OSError as exc:
            error = f"{type(exc).__name__}: {exc}"
    metadata = _command_metadata(
        argv,
        returncode,
        time.monotonic() - started,
        stdout_path,
        stderr_path,
        forced_kill=forced_kill,
        error=error,
    )
    _write_json(invocation_path, metadata)
    return metadata


def _metrics_url(listen: str) -> str:
    if listen.startswith("http://") or listen.startswith("https://"):
        return listen.rstrip("/") + "/metrics"
    return f"http://{listen.rstrip('/')}/metrics"


def _run_benchmark(
    argv: Sequence[str],
    output_dir: Path,
    timeout_seconds: float,
    interrupt_after_seconds: float | None,
    metrics_listen: str,
    backend_metrics_url: str | None,
    signal_state: SignalState,
) -> dict[str, Any]:
    stdout_path = output_dir / "run.stdout.json"
    stderr_path = output_dir / "run.stderr.txt"
    invocation_path = output_dir / "run-invocation.json"
    started = time.monotonic()
    signal_state.forwarded_to_child = False
    signal_sent: list[str] = []
    watchdog_triggered = False
    forced_kill = False
    returncode: int | None = None
    error: str | None = None
    sampler_errors: list[str] = []
    sampler_stop = threading.Event()
    process: subprocess.Popen[bytes] | None = None
    sampler: MetricSampler | None = None
    grace_deadline: float | None = None
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        try:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                close_fds=True,
                start_new_session=True,
            )
            signal_state.child = process
            sampler = MetricSampler(
                sampler_stop,
                output_dir / "metrics.jsonl",
                output_dir / "process.jsonl",
                process.pid,
                _metrics_url(metrics_listen),
                backend_metrics_url,
            )
            sampler.start()
            while process.poll() is None:
                elapsed = time.monotonic() - started
                if signal_state.user_interrupt and "SIGINT" not in signal_sent:
                    if not signal_state.forwarded_to_child:
                        _send_group(process, signal.SIGINT)
                    signal_sent.append("SIGINT")
                if (
                    interrupt_after_seconds is not None
                    and elapsed >= interrupt_after_seconds
                    and "SIGINT" not in signal_sent
                ):
                    _send_group(process, signal.SIGINT)
                    signal_sent.append("SIGINT")
                if elapsed >= timeout_seconds and not watchdog_triggered:
                    watchdog_triggered = True
                    _send_group(process, signal.SIGINT)
                    signal_sent.append("SIGINT")
                    grace_deadline = time.monotonic() + min(5.0, max(0.1, timeout_seconds * 0.25))
                if (
                    grace_deadline is not None
                    and time.monotonic() >= grace_deadline
                    and process.poll() is None
                ):
                    forced_kill = True
                    _send_group(process, signal.SIGKILL)
                    break
                time.sleep(0.02)
            returncode = process.wait()
        except (OSError, RuntimeError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        finally:
            sampler_stop.set()
            if sampler is not None:
                sampler.join(
                    timeout=HTTP_SCRAPE_BUDGET_SECONDS
                    + HTTP_SCRAPE_TERMINATE_GRACE_SECONDS
                    + 1.0
                )
                if sampler.is_alive():
                    sampler_errors.append("metrics/process sampler did not join")
                elif sampler.error:
                    sampler_errors.append(sampler.error)
                sampler_errors.extend(sampler.collection_errors)
            if process is not None and process.poll() is None:
                _send_group(process, signal.SIGKILL)
                process.wait()
            signal_state.child = None
    if sampler_errors:
        sampler_error = "; ".join(sampler_errors)
        error = sampler_error if error is None else f"{error}; {sampler_error}"
    metadata = _command_metadata(
        argv,
        returncode,
        time.monotonic() - started,
        stdout_path,
        stderr_path,
        signal_sent=signal_sent,
        watchdog_triggered=watchdog_triggered,
        forced_kill=forced_kill,
        error=error,
    )
    metadata["collection_errors"] = sampler_errors
    metadata["metrics_scrape_outcomes"] = sampler.scrape_outcomes if sampler is not None else {}
    _write_json(invocation_path, metadata)
    return metadata


def _install_signal_handlers(state: SignalState) -> dict[int, Any]:
    previous: dict[int, Any] = {}

    def handler(signum: int, _frame: Any) -> None:
        state.user_interrupt = True
        if state.child is not None and not state.forwarded_to_child:
            _send_group(state.child, signal.SIGINT)
            state.forwarded_to_child = True

    for signum in [signal.SIGINT]:
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, handler)
    return previous


def _restore_signal_handlers(previous: dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _read_machine_memory() -> dict[str, Any]:
    result: dict[str, Any] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                result["mem_total_kib"] = int(line.split()[1])
                break
    except (OSError, ValueError, IndexError):
        result["error"] = "memory information unavailable"
    return result


def _environment(
    args: argparse.Namespace,
    binary: Path,
    etcdctl: Path,
    config: Path,
    output_dir: Path,
    version_invocation: dict[str, Any],
    help_invocation: dict[str, Any],
    collection_errors: list[str],
    scrape_outcomes: dict[str, dict[str, int]],
) -> dict[str, Any]:
    return {
        "os": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python": sys.version,
            "python_implementation": platform.python_implementation(),
            "cpu_count": os.cpu_count(),
            "memory": _read_machine_memory(),
        },
        "benchmark": {
            "binary": str(binary),
            "sha256": _sha256(binary),
        },
        "etcdctl": {
            "binary": str(etcdctl),
            "sha256": _sha256(etcdctl),
            "version_invocation": version_invocation,
            "help_invocation": help_invocation,
            "version_stdout": "etcdctl-version.stdout.txt",
            "version_stderr": "etcdctl-version.stderr.txt",
            "help_stdout": "etcdctl-help.stdout.txt",
            "help_stderr": "etcdctl-help.stderr.txt",
        },
        "tool": {
            "argv": list(sys.argv),
            "config": str(config),
            "output_dir": str(output_dir),
            "endpoint": args.endpoint,
            "metrics_listen": args.metrics_listen,
            "backend_metrics_url": args.backend_metrics_url,
            "timeout_seconds": args.timeout_seconds,
            "interrupt_after_seconds": args.interrupt_after_seconds,
        },
        "collection_errors": collection_errors,
        "metrics": {
            "scrape_budget_seconds": HTTP_SCRAPE_BUDGET_SECONDS,
            "targets": scrape_outcomes,
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture a synthetic benchmark run")
    parser.add_argument("--binary", required=True, help="absolute or resolvable benchmark executable")
    parser.add_argument("--etcdctl", required=True, help="etcdctl executable")
    parser.add_argument("--endpoint", required=True, help="explicit etcd endpoint")
    parser.add_argument("--config", required=True, help="complete synthetic JSON configuration")
    parser.add_argument("--metrics-listen", required=True, help="benchmark metrics host:port")
    parser.add_argument("--output-dir", required=True, help="new directory for raw evidence")
    parser.add_argument("--timeout-seconds", required=True, type=float)
    parser.add_argument("--backend-metrics-url")
    parser.add_argument("--interrupt-after-seconds", type=float)
    return parser


def _validate_args(args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    if not math_is_finite_positive(args.timeout_seconds):
        raise InvocationError("--timeout-seconds must be finite and greater than zero")
    if args.interrupt_after_seconds is not None and not math_is_finite_nonnegative(args.interrupt_after_seconds):
        raise InvocationError("--interrupt-after-seconds must be finite and non-negative")
    binary = _require_executable(args.binary, "--binary")
    etcdctl = _require_executable(args.etcdctl, "--etcdctl")
    config = _require_file(args.config, "--config")
    output_dir = Path(args.output_dir).expanduser().resolve()
    if output_dir.exists():
        raise InvocationError(f"--output-dir must name a new directory: {output_dir}")
    try:
        output_dir.mkdir()
    except OSError as exc:
        raise InvocationError(f"cannot create --output-dir {output_dir}: {exc}") from exc
    return binary, etcdctl, config, output_dir


def math_is_finite_positive(value: float) -> bool:
    return isinstance(value, (int, float)) and value > 0 and value != float("inf") and value != float("-inf") and value == value


def math_is_finite_nonnegative(value: float) -> bool:
    return isinstance(value, (int, float)) and value >= 0 and value != float("inf") and value != float("-inf") and value == value


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
        binary, etcdctl, config, output_dir = _validate_args(args)
        shutil.copyfile(config, output_dir / "input.json")
    except (InvocationError, OSError) as exc:
        print(f"run_synthetic: {exc}", file=sys.stderr)
        return 2

    collection_errors: list[str] = []
    command_timeout = max(1.0, min(float(args.timeout_seconds), 30.0))
    endpoint_args = [
        f"--endpoints={args.endpoint}",
        f"--command-timeout={command_timeout:.3f}s",
    ]
    version_invocation = _run_capture([str(etcdctl), "version"], output_dir, "etcdctl-version", command_timeout)
    help_invocation = _run_capture([str(etcdctl), "--help"], output_dir, "etcdctl-help", command_timeout)
    for label, invocation in [("etcdctl version", version_invocation), ("etcdctl help", help_invocation)]:
        if invocation.get("exit_code") != 0:
            collection_errors.append(f"{label} failed")

    status_before = _run_capture(
        [str(etcdctl), *endpoint_args, "endpoint", "status", "-w", "json"],
        output_dir,
        "backend-status-before",
        command_timeout,
    )
    keyspace_before = _run_capture(
        [str(etcdctl), *endpoint_args, "get", "", "--from-key", "--limit=1", "-w", "json"],
        output_dir,
        "backend-keyspace-before",
        command_timeout,
    )
    for label, invocation in [("backend status before", status_before), ("backend keyspace before", keyspace_before)]:
        if invocation.get("exit_code") != 0:
            collection_errors.append(f"{label} failed")

    state = SignalState()
    previous_handlers = _install_signal_handlers(state)
    try:
        plan = _run_capture(
            [str(binary), "plan", "--config", str(output_dir / "input.json")],
            output_dir,
            "plan",
            float(args.timeout_seconds),
            stdout_suffix=".stdout.json",
        )
        if plan.get("exit_code") != 0:
            collection_errors.append("benchmark plan failed")
        run = _run_benchmark(
            [
                str(binary),
                "run",
                "--config",
                str(output_dir / "input.json"),
                "--endpoint",
                args.endpoint,
                "--metrics-listen",
                args.metrics_listen,
            ],
            output_dir,
            float(args.timeout_seconds),
            args.interrupt_after_seconds,
            args.metrics_listen,
            args.backend_metrics_url,
            state,
        )
    finally:
        _restore_signal_handlers(previous_handlers)

    if run.get("watchdog_triggered"):
        collection_errors.append("benchmark watchdog triggered")
    if run.get("forced_kill"):
        collection_errors.append("benchmark process forcibly killed")
    run_collection_errors = run.get("collection_errors")
    if isinstance(run_collection_errors, list):
        collection_errors.extend(str(error) for error in run_collection_errors)

    status_after = _run_capture(
        [str(etcdctl), *endpoint_args, "endpoint", "status", "-w", "json"],
        output_dir,
        "backend-status-after",
        command_timeout,
    )
    keyspace_after = _run_capture(
        [
            str(etcdctl),
            *endpoint_args,
            "get",
            "",
            "--from-key",
            "--keys-only",
            "-w",
            "json",
        ],
        output_dir,
        "backend-keyspace-after",
        command_timeout,
    )
    for label, invocation in [("backend status after", status_after), ("backend keyspace after", keyspace_after)]:
        if invocation.get("exit_code") != 0:
            collection_errors.append(f"{label} failed")

    _write_json(output_dir / "environment.json", _environment(
        args,
        binary,
        etcdctl,
        config,
        output_dir,
        version_invocation,
        help_invocation,
        collection_errors,
        run.get("metrics_scrape_outcomes") if isinstance(run.get("metrics_scrape_outcomes"), dict) else {},
    ))

    check_result: dict[str, Any]
    try:
        plan_path: Path | None = (
            output_dir / "plan.stdout.json" if plan.get("exit_code") == 0 else None
        )
        keyspace_path = (
            output_dir / "backend-keyspace-after.stdout.txt"
            if keyspace_after.get("exit_code") == 0
            else None
        )
        check_result = check_report.check_paths(
            output_dir / "run.stdout.json",
            int(run["exit_code"]) if isinstance(run.get("exit_code"), int) else 255,
            plan_path,
            keyspace_path,
        )
    except (check_report.InputError, OSError, ValueError) as exc:
        check_result = {
            "report_valid": False,
            "experiment_status": None,
            "complete_evidence": False,
            "errors": [f"checker: {exc}"],
            "checks": {},
            "evidence": {"plan": "not_checked", "keyspace": "not_checked"},
        }
        collection_errors.append(f"checker failed: {exc}")
    _write_json(output_dir / "check.json", check_result)

    if not check_result.get("report_valid"):
        return 1
    if collection_errors:
        return 1
    return int(run["exit_code"]) if isinstance(run.get("exit_code"), int) else 1


if __name__ == "__main__":
    sys.exit(main())
