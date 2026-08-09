#!/usr/bin/env python3
"""Read-only summary tool for JSONL emitted by etcd_proxy_rs.

The proxy prints protobuf byte fields with Rust's Debug formatter.  This tool
decodes request keys and aggregates them without changing the input log.
"""

import argparse
import collections
import datetime as dt
import json
import re
import sys
from pathlib import Path


REQUEST_KEY_RE = re.compile(
    r"(?P<kind>RangeRequest|PutRequest|DeleteRangeRequest)"
    r" \{ key: \[(?P<key>[0-9, ]*)\]"
)
RANGE_RE = re.compile(r"RangeRequest \{ (?P<body>[^{}]*) \}")
COMPACT_RE = re.compile(r"CompactionRequest \{ revision: (?P<revision>\d+), physical: (?P<physical>true|false) \}")
LEASE_GRANT_RE = re.compile(r"LeaseGrantRequest \{ ttl: (?P<ttl>-?\d+), id: (?P<id>-?\d+) \}")
REQUEST_OP_RE = re.compile(r"Some\(Request(?P<kind>Range|Put|DeleteRange)\(")
COMPARE_RE = re.compile(
    r"Compare \{ result: (?P<result>\w+), target: (?P<target>\w+),.*?"
    r"target_union: Some\((?P<union>\w+)\((?P<value>[^)]*)\)\)"
)


def decode_bytes(text: str) -> str:
    """Decode the comma-separated decimal bytes used by Rust Debug output."""
    try:
        return bytes(int(number) for number in text.split(",") if number.strip()).decode(
            "utf-8", errors="backslashreplace"
        )
    except ValueError:
        return "<invalid byte sequence>"


def resource_prefix(key: str) -> str:
    """Collapse a Kubernetes storage key to its /registry/<resource> prefix."""
    parts = key.split("/")
    if len(parts) >= 3 and parts[1] == "registry":
        return f"/registry/{parts[2]}"
    if len(parts) >= 2 and parts[1]:
        return f"/{parts[1]}"
    return key or "/"


def print_counter(title: str, counter: collections.Counter, top: int) -> None:
    print(f"\n{title}")
    print("count\tvalue")
    for value, count in counter.most_common(top):
        print(f"{count}\t{value}")


def bracket_section(text: str, marker: str) -> str:
    """Return the balanced [] section immediately following marker, if present."""
    start = text.find(marker)
    if start == -1:
        return ""
    start += len(marker)
    depth = 1
    for index in range(start, len(text)):
        if text[index] == "[":
            depth += 1
        elif text[index] == "]":
            depth -= 1
            if depth == 0:
                return text[start:index]
    return ""


def text_field(body: str, field: str, default: str = "<missing>") -> str:
    match = re.search(rf"\b{re.escape(field)}: ([^, ]+)", body)
    return match.group(1) if match else default


def bytes_field(body: str, field: str) -> str:
    match = re.search(rf"\b{re.escape(field)}: \[([0-9, ]*)\]", body)
    return decode_bytes(match.group(1)) if match else "<missing>"


def transaction_pattern(request: str) -> str:
    compares = bracket_section(request, "compare: [")
    success = bracket_section(request, "success: [")
    failure = bracket_section(request, "failure: [")
    compare_parts = [
        f"{match.group('result')}/{match.group('target')}/"
        f"{match.group('union')}={match.group('value')}"
        for match in COMPARE_RE.finditer(compares)
    ]
    compare_summary = "+".join(compare_parts) if compare_parts else "none"
    success_summary = "+".join(match.group("kind") for match in REQUEST_OP_RE.finditer(success)) or "none"
    failure_summary = "+".join(match.group("kind") for match in REQUEST_OP_RE.finditer(failure)) or "none"
    return f"compare={compare_summary}; success={success_summary}; failure={failure_summary}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path, help="etcd proxy JSONL log to read")
    parser.add_argument("--top", type=int, default=20, help="rows per section (default: 20)")
    args = parser.parse_args()

    if args.top < 1:
        parser.error("--top must be positive")

    rpc_counts: collections.Counter[str] = collections.Counter()
    request_counts: collections.Counter[str] = collections.Counter()
    prefix_counts: collections.Counter[str] = collections.Counter()
    key_counts: collections.Counter[str] = collections.Counter()
    range_modes: collections.Counter[str] = collections.Counter()
    range_targets: collections.Counter[str] = collections.Counter()
    transaction_patterns: collections.Counter[str] = collections.Counter()
    compact_physical: collections.Counter[str] = collections.Counter()
    compact_revisions: list[int] = []
    lease_grant_patterns: collections.Counter[str] = collections.Counter()
    lease_grant_ttls: collections.Counter[str] = collections.Counter()
    lease_grant_id_modes: collections.Counter[str] = collections.Counter()
    total = invalid = 0
    first_timestamp = last_timestamp = None

    with args.log.open(encoding="utf-8") as log:
        for line_number, line in enumerate(log, start=1):
            if not line.strip():
                continue
            total += 1
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as error:
                invalid += 1
                print(f"warning: invalid JSON on line {line_number}: {error.msg}", file=sys.stderr)
                continue

            rpc_counts[entry.get("method", "<missing method>")] += 1
            timestamp = entry.get("timestamp_ms")
            if isinstance(timestamp, int):
                first_timestamp = timestamp if first_timestamp is None else min(first_timestamp, timestamp)
                last_timestamp = timestamp if last_timestamp is None else max(last_timestamp, timestamp)

            request = entry.get("request", "")
            if not isinstance(request, str):
                continue
            for match in REQUEST_KEY_RE.finditer(request):
                kind = match.group("kind")
                key = decode_bytes(match.group("key"))
                request_counts[kind] += 1
                prefix_counts[resource_prefix(key)] += 1
                key_counts[key] += 1

            for match in RANGE_RE.finditer(request):
                body = match.group("body")
                key = bytes_field(body, "key")
                range_end = bytes_field(body, "range_end")
                is_range = range_end != ""
                limit = text_field(body, "limit")
                revision = text_field(body, "revision")
                serializable = text_field(body, "serializable")
                keys_only = text_field(body, "keys_only")
                count_only = text_field(body, "count_only")
                sort_target = text_field(body, "sort_target")
                revision_mode = "latest" if revision == "0" else "historical"
                target_kind = "range" if is_range else "point"
                range_modes[
                    f"{target_kind}; limit={limit}; revision={revision_mode}; "
                    f"serializable={serializable}; keys_only={keys_only}; "
                    f"count_only={count_only}; sort_target={sort_target}"
                ] += 1
                target = key if not is_range else f"{key} .. {range_end}"
                range_targets[f"{target_kind}\t{target}\tlimit={limit}\trevision={revision}"] += 1

            if entry.get("method") == "/etcdserverpb.KV/Txn":
                transaction_patterns[transaction_pattern(request)] += 1

            for match in COMPACT_RE.finditer(request):
                compact_revisions.append(int(match.group("revision")))
                compact_physical[match.group("physical")] += 1

            for match in LEASE_GRANT_RE.finditer(request):
                ttl = match.group("ttl")
                lease_id = match.group("id")
                lease_grant_patterns[f"ttl={ttl}; id={lease_id}"] += 1
                lease_grant_ttls[ttl] += 1
                lease_grant_id_modes["auto-assigned (id=0)" if lease_id == "0" else "explicit id"] += 1

    print(f"input: {args.log}")
    print(f"JSON records: {total} (invalid: {invalid})")
    if first_timestamp is not None and last_timestamp is not None:
        first = dt.datetime.fromtimestamp(first_timestamp / 1000, tz=dt.timezone.utc)
        last = dt.datetime.fromtimestamp(last_timestamp / 1000, tz=dt.timezone.utc)
        print(f"time range (UTC): {first.isoformat()} to {last.isoformat()}")
        print(f"duration: {(last_timestamp - first_timestamp) / 1000:.1f} seconds")

    print_counter("Top-level etcd RPCs", rpc_counts, args.top)
    print_counter(
        "KV request objects containing a key", request_counts, args.top
    )
    print_counter("Decoded key prefixes", prefix_counts, args.top)
    print_counter("Decoded exact keys", key_counts, args.top)
    print_counter("Range parameter patterns", range_modes, args.top)
    print_counter("Decoded Range targets", range_targets, args.top)
    print_counter("Transaction patterns", transaction_patterns, args.top)
    if compact_revisions:
        print("\nCompact parameters")
        print(f"requests\t{len(compact_revisions)}")
        print(f"revision range\t{min(compact_revisions)} .. {max(compact_revisions)}")
        print_counter("Compact physical flag", compact_physical, args.top)
    if lease_grant_patterns:
        print_counter("LeaseGrant parameter patterns", lease_grant_patterns, args.top)
        print_counter("LeaseGrant TTLs", lease_grant_ttls, args.top)
        print_counter("LeaseGrant ID modes", lease_grant_id_modes, args.top)
    print(
        "\nNote: entries found inside TxnRequest include both success and failure "
        "branches. They describe possible transaction operations, not necessarily "
        "operations that executed."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
