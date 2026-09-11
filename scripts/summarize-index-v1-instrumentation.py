#!/usr/bin/env python3
"""Derive exact-window index and physical-disk metrics from raw samples."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from fractions import Fraction
from pathlib import Path
from typing import Any


COUNTERS = (
    "selected_bytes_total",
    "prepared_rows_total",
    "prepared_bytes_total",
    "projected_rows_total",
    "projected_bytes_total",
    "sealed_bytes_total",
    "checkpointed_source_positions_total",
    "checkpointed_source_payload_bytes_total",
)
GAUGES = (
    "local_next_offset",
    "local_tail",
    "lag_entries",
    "lag_oldest_age_milliseconds",
)
PROCESS_REQUIRED_FIELDS = (
    "interval_end_epoch_milliseconds",
    "interval_cpu_percent",
    "rss_kib",
    "threads",
    "read_bytes_total",
    "write_bytes_total",
    "cancelled_write_bytes_total",
    "mem_available_kib",
    "interval_seconds",
    "minor_faults_total",
    "major_faults_total",
    "voluntary_context_switches_total",
    "nonvoluntary_context_switches_total",
    "rchar_total",
    "wchar_total",
    "read_syscalls_total",
    "write_syscalls_total",
    "process_starttime_ticks",
)
PROCESS_COUNTERS = (
    "read_bytes_total",
    "write_bytes_total",
    "minor_faults_total",
    "major_faults_total",
    "voluntary_context_switches_total",
    "nonvoluntary_context_switches_total",
    "rchar_total",
    "wchar_total",
    "read_syscalls_total",
    "write_syscalls_total",
)
ROCKSDB_REQUIRED_METRICS = (
    "keldra_rocksdb_block_cache_capacity_bytes",
    "keldra_rocksdb_block_cache_usage_bytes",
    "keldra_rocksdb_block_cache_pinned_bytes",
    "keldra_rocksdb_write_buffer_capacity_bytes",
    "keldra_rocksdb_write_buffer_usage_bytes",
    "keldra_rocksdb_unavailable_properties",
    "keldra_rocksdb_active_memtable_bytes",
    "keldra_rocksdb_all_memtable_bytes",
    "keldra_rocksdb_table_reader_bytes",
    "keldra_rocksdb_pending_compaction_bytes",
    "keldra_rocksdb_immutable_memtables",
    "keldra_rocksdb_running_compactions",
    "keldra_rocksdb_running_flushes",
    "keldra_rocksdb_compaction_pending_column_families",
    "keldra_rocksdb_flush_pending_column_families",
    "keldra_rocksdb_actual_delayed_write_rate_bytes_per_second",
    "keldra_rocksdb_write_stopped",
    "keldra_rocksdb_background_errors",
    "keldra_rocksdb_write_stalled",
    "keldra_storage_wal_bytes",
)
GROUP_COMMIT_REQUIRED_FIELDS = (
    "attempts",
    "physical_commits",
    "commit_lane",
    "request_count",
    "operation_count",
    "inline_bytes",
    "failed_requests",
    "group_execute_started_epoch_milliseconds",
    "group_execute_ended_epoch_milliseconds",
    "admission_wait_sum_seconds",
    "admission_wait_max_seconds",
    "request_slot_wait_sum_seconds",
    "operation_slot_wait_sum_seconds",
    "inline_byte_slot_wait_sum_seconds",
    "enqueue_lock_wait_sum_seconds",
    "enqueue_to_group_sum_seconds",
    "enqueue_to_group_max_seconds",
    "lane_fence_wait_seconds",
    "lane_conflict_lock_wait_seconds",
    "physical_slot_wait_seconds",
    "physical_slots_active_at_acquire",
    "physical_slots_active_before_release",
    "physical_slots_peak_since_start_at_acquire",
    "physical_slots_peak_since_start_before_release",
    "physical_slot_count",
    "first_sequence_wait_seconds",
    "first_sequence_hold_seconds",
    "persistence_and_ordered_settlement_seconds",
    "primary_db_write_seconds",
    "completion_sequence_wait_seconds",
    "prior_retry_projection_db_write_seconds",
    "completion_projection_db_write_seconds",
    "ordered_frontier_wait_seconds",
    "completion_reorder_depth",
    "completion_ticket_lag",
    "prior_retry_projection_completions",
    "completion_projection_completions",
    "settlement_measured_component_sum_seconds",
    "commit_path_composite_seconds",
    "lane_queued_requests",
    "lane_peak_queued_requests_since_start",
    "total_queued_requests",
    "total_peak_queued_requests_since_start",
    "phase_complete",
    "physical_commit",
)
RESOURCE_MAXIMUM_GAP_MILLISECONDS = 3_000
ROCKSDB_MAXIMUM_GAP_MILLISECONDS = 30_000


def read_json_lines(path: str) -> list[dict[str, Any]]:
    with open(path, encoding="utf-8") as source:
        return [json.loads(line) for line in source if line.strip()]


def bracket(samples: list[dict[str, Any]], timestamp: int) -> tuple[dict[str, Any], dict[str, Any]]:
    before = [sample for sample in samples if sample["timestamp_unix_milliseconds"] <= timestamp]
    after = [sample for sample in samples if sample["timestamp_unix_milliseconds"] >= timestamp]
    if not before or not after:
        raise ValueError(f"telemetry does not bracket exact boundary {timestamp}")
    return before[-1], after[0]


def interpolate(
    before: dict[str, Any], after: dict[str, Any], key: str, timestamp: int
) -> Fraction:
    left = int(before[key])
    right = int(after[key])
    if right < left:
        raise ValueError(f"cumulative counter {key} regressed")
    left_at = int(before["timestamp_unix_milliseconds"])
    right_at = int(after["timestamp_unix_milliseconds"])
    if left_at == right_at:
        return Fraction(left)
    return Fraction(left) + Fraction((right - left) * (timestamp - left_at), right_at - left_at)


def pipeline_metrics(start: int, end: int, path: str) -> dict[str, Any]:
    if end <= start:
        raise ValueError("measurement window is nonpositive")
    samples = sorted(read_json_lines(path), key=lambda sample: sample["timestamp_unix_milliseconds"])
    required = ("timestamp_unix_milliseconds", *COUNTERS, *GAUGES)
    complete = [sample for sample in samples if all(sample.get(key) is not None for key in required)]
    if len(complete) < 2:
        raise ValueError("fewer than two complete v1 summary samples")
    for previous, current in zip(complete, complete[1:]):
        for key in COUNTERS:
            if int(current[key]) < int(previous[key]):
                raise ValueError(f"cumulative counter {key} regressed")
    start_before, start_after = bracket(complete, start)
    end_before, end_after = bracket(complete, end)
    elapsed_milliseconds = end - start
    seconds = elapsed_milliseconds / 1000

    def rate(key: str) -> float:
        delta = (
            interpolate(end_before, end_after, key, end)
            - interpolate(start_before, start_after, key, start)
        )
        return float(delta * 1000 / elapsed_milliseconds)

    observed = [sample for sample in complete if start <= sample["timestamp_unix_milliseconds"] <= end]
    gauge_samples = [start_before, *observed, end_after]
    return {
        "measurement": "v1-summary-exact-window-linear-interpolation",
        "samples": len(complete),
        "window": {"start": start, "end": end},
        "elapsed_seconds": seconds,
        "boundary_evidence": {
            "start_before": start_before["timestamp_unix_milliseconds"],
            "start_after": start_after["timestamp_unix_milliseconds"],
            "end_before": end_before["timestamp_unix_milliseconds"],
            "end_after": end_after["timestamp_unix_milliseconds"],
        },
        "indexed_physical_rows_per_second": rate("projected_rows_total"),
        "indexed_physical_rows_definition": (
            "durably published physical projection rows per second, linearly interpolated from "
            "cumulative projected_rows_total at the exact ingest-window boundaries"
        ),
        "prepared_physical_rows_per_second": rate("prepared_rows_total"),
        "unique_source_documents_per_second": None,
        "unique_source_documents_limitation": (
            "the workload report does not retain the successful operation indexes and source paths "
            "needed to count distinct acknowledged source documents within the window"
        ),
        "selected_bytes_per_second": rate("selected_bytes_total"),
        "prepared_bytes_per_second": rate("prepared_bytes_total"),
        "projected_bytes_per_second": rate("projected_bytes_total"),
        "sealed_bytes_per_second": rate("sealed_bytes_total"),
        "checkpointed_source_positions_per_second": rate("checkpointed_source_positions_total"),
        "checkpointed_source_payload_bytes_per_second": rate(
            "checkpointed_source_payload_bytes_total"
        ),
        "backlog": {
            "source": "nearest-bracketing authoritative-v1-summary-gauges",
            "start_observation": {key: start_before[key] for key in GAUGES}
            | {"timestamp_unix_milliseconds": start_before["timestamp_unix_milliseconds"]},
            "end_observation": {key: end_after[key] for key in GAUGES}
            | {"timestamp_unix_milliseconds": end_after["timestamp_unix_milliseconds"]},
            "maximum_lag_entries": max(sample["lag_entries"] for sample in gauge_samples),
            "maximum_lag_oldest_age_milliseconds": max(
                sample["lag_oldest_age_milliseconds"] for sample in gauge_samples
            ),
        },
    }


def disk_metrics(start: int, end: int, device: str, path: str) -> dict[str, Any]:
    if end <= start:
        raise ValueError("measurement window is nonpositive")
    with open(path, encoding="utf-8", newline="") as source:
        rows = [row for row in csv.DictReader(source, delimiter="\t") if row["device"] == device]
    rows.sort(key=lambda row: int(row["timestamp_unix_milliseconds"]))
    fields = (
        "reads_completed",
        "sectors_read",
        "writes_completed",
        "sectors_written",
        "write_milliseconds",
        "io_milliseconds",
        "weighted_io_milliseconds",
        "flushes_completed",
    )
    totals = {field: Fraction(0) for field in fields}
    overlap_total = 0
    intervals = 0
    for previous, current in zip(rows, rows[1:]):
        interval_start = int(previous["timestamp_unix_milliseconds"])
        interval_end = int(current["timestamp_unix_milliseconds"])
        overlap = min(interval_end, end) - max(interval_start, start)
        interval = interval_end - interval_start
        if overlap <= 0 or interval <= 0:
            continue
        fraction = Fraction(overlap, interval)
        for field in fields:
            delta = int(current[field]) - int(previous[field])
            if delta < 0:
                raise ValueError(f"diskstats counter {field} reset for {device}")
            totals[field] += delta * fraction
        overlap_total += overlap
        intervals += 1
    window = end - start
    if intervals == 0 or overlap_total < window:
        raise ValueError(f"diskstats samples do not cover the exact window for {device}")
    seconds = Fraction(overlap_total, 1000)
    return {
        "measurement": "prorated-adjacent-proc-diskstats",
        "device": device,
        "intervals": intervals,
        "observed_overlap_seconds": float(seconds),
        "measurement_window_seconds": window / 1000,
        "coverage_ratio": overlap_total / window,
        "read_iops": float(totals["reads_completed"] / seconds),
        "write_iops": float(totals["writes_completed"] / seconds),
        "flush_iops": float(totals["flushes_completed"] / seconds),
        "read_mib_per_second": float(totals["sectors_read"] * 512 / 1048576 / seconds),
        "write_mib_per_second": float(
            totals["sectors_written"] * 512 / 1048576 / seconds
        ),
        "busy_percent": float(totals["io_milliseconds"] / overlap_total * 100),
        "average_queue_depth": float(totals["weighted_io_milliseconds"] / overlap_total),
        "write_await_milliseconds": (
            float(totals["write_milliseconds"] / totals["writes_completed"])
            if totals["writes_completed"] > 0
            else 0
        ),
    }


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("cannot calculate a percentile without values")
    rank = (len(ordered) - 1) * quantile
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def distribution(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "sum": sum(values),
        "minimum": min(values),
        "mean": sum(values) / len(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "maximum": max(values),
    }


def coverage_evidence(
    start: int, end: int, timestamps: list[int], maximum_gap_milliseconds: int
) -> dict[str, Any]:
    timestamps = sorted(set(timestamps))
    before = [timestamp for timestamp in timestamps if timestamp <= start]
    after = [timestamp for timestamp in timestamps if timestamp >= end]
    start_bracket = before[-1] if before else None
    end_bracket = after[0] if after else None
    covered = (
        [timestamp for timestamp in timestamps if start_bracket <= timestamp <= end_bracket]
        if start_bracket is not None and end_bracket is not None
        else []
    )
    gaps = [right - left for left, right in zip(covered, covered[1:])]
    in_window = [timestamp for timestamp in timestamps if start <= timestamp <= end]
    maximum_observed_gap = max(gaps, default=None)
    cadence_complete = bool(gaps) and maximum_observed_gap <= maximum_gap_milliseconds
    return {
        "sample_count": len(timestamps),
        "distinct_sample_count": len(timestamps),
        "window": {"start": start, "end": end},
        "start_bracket": start_bracket,
        "end_bracket": end_bracket,
        "measurement_window_sample_count": len(in_window),
        "maximum_observed_gap_milliseconds": maximum_observed_gap,
        "maximum_allowed_gap_milliseconds": maximum_gap_milliseconds,
        "cadence_complete": cadence_complete,
        "complete": bool(before and after and in_window and cadence_complete),
    }


def overlapping_fraction(left: int, right: int, start: int, end: int) -> Fraction:
    interval = right - left
    overlap = min(right, end) - max(left, start)
    if interval <= 0 or overlap <= 0:
        return Fraction(0)
    return Fraction(overlap, interval)


def process_resource_summary(start: int, end: int, rows: list[dict[str, str]]) -> dict[str, Any]:
    rows = sorted(rows, key=lambda row: int(row["interval_end_epoch_milliseconds"]))
    identities = {int(row["process_starttime_ticks"]) for row in rows}
    if len(identities) != 1:
        raise ValueError("process resource samples contain more than one process identity")
    window_rows = [
        row for row in rows if start <= int(row["interval_end_epoch_milliseconds"]) <= end
    ]
    cpu_numerator = Fraction(0)
    cpu_seconds = Fraction(0)
    counter_deltas = {key: Fraction(0) for key in PROCESS_COUNTERS}
    cancelled_write_delta = Fraction(0)
    for previous, current in zip(rows, rows[1:]):
        left = int(previous["interval_end_epoch_milliseconds"])
        right = int(current["interval_end_epoch_milliseconds"])
        fraction = overlapping_fraction(left, right, start, end)
        if not fraction:
            continue
        overlap_seconds = Fraction(min(right, end) - max(left, start), 1000)
        cpu_numerator += Fraction(current["interval_cpu_percent"]) * overlap_seconds
        cpu_seconds += overlap_seconds
        for key in PROCESS_COUNTERS:
            delta = int(current[key]) - int(previous[key])
            if delta < 0:
                raise ValueError(f"process counter {key} regressed")
            counter_deltas[key] += delta * fraction
        cancelled_write_delta += (
            int(current["cancelled_write_bytes_total"])
            - int(previous["cancelled_write_bytes_total"])
        ) * fraction
    if not window_rows or cpu_seconds <= 0:
        raise ValueError("process samples do not provide a measured interval inside the window")
    return {
        "measurement": "exact-window-process-resource-summary",
        "time_weighted_cpu_percent": float(cpu_numerator / cpu_seconds),
        "sampled_peak_rss_bytes": max(int(row["rss_kib"]) for row in window_rows) * 1024,
        "sampled_peak_threads": max(int(row["threads"]) for row in window_rows),
        "sampled_minimum_host_mem_available_bytes": min(
            int(row["mem_available_kib"]) for row in window_rows
        )
        * 1024,
        "prorated_counter_deltas": {
            key.removesuffix("_total"): float(value) for key, value in counter_deltas.items()
        }
        | {"cancelled_write_bytes": float(cancelled_write_delta)},
        "attribution": (
            "counter deltas and interval CPU are prorated at boundary intervals; RSS, thread and "
            "available-memory values are sampled gauges whose endpoints fall inside the window"
        ),
    }


def parse_cpu(sample: dict[str, Any]) -> list[int]:
    line = next((line for line in sample["proc_stat"] if line.startswith("cpu ")), None)
    if line is None:
        raise ValueError("host sample has no aggregate cpu line")
    fields = [int(value) for value in line.split()[1:]]
    if len(fields) < 8:
        raise ValueError("host aggregate cpu line has fewer than eight fields")
    return fields[:8]


def parse_pressure(lines: list[str]) -> dict[str, dict[str, float]]:
    parsed: dict[str, dict[str, float]] = {}
    for line in lines:
        fields = line.split()
        if not fields:
            continue
        parsed[fields[0]] = {
            key: float(value) for key, value in (field.split("=", 1) for field in fields[1:])
        }
    return parsed


def host_missing_fields(samples: list[dict[str, Any]]) -> list[str]:
    required = (
        "timestamp_unix_milliseconds",
        "proc_stat",
        "loadavg",
        "pressure",
        "meminfo",
        "vmstat",
    )
    missing = {
        key for key in required if not samples or any(sample.get(key) is None for sample in samples)
    }
    for sample in samples:
        if sample.get("proc_stat") is not None and not any(
            line.startswith("cpu ") for line in sample["proc_stat"]
        ):
            missing.add("proc_stat.cpu")
        if sample.get("loadavg") is not None and len(sample["loadavg"].split()) < 4:
            missing.add("loadavg.runnable")
        pressure = sample.get("pressure") or {}
        for resource in ("cpu", "io", "memory"):
            scopes = parse_pressure(pressure.get(resource, []))
            if "some" not in scopes or any(
                key not in scopes.get("some", {}) for key in ("avg10", "avg60", "avg300")
            ):
                missing.add(f"pressure.{resource}.some")
        meminfo = sample.get("meminfo") or {}
        for key in ("MemAvailable", "SwapTotal", "SwapFree"):
            if key not in meminfo:
                missing.add(f"meminfo.{key}")
        vmstat = sample.get("vmstat") or {}
        for key in ("pgfault", "pgmajfault", "pswpin", "pswpout"):
            if key not in vmstat:
                missing.add(f"vmstat.{key}")
    return sorted(missing)


def host_resource_summary(start: int, end: int, samples: list[dict[str, Any]]) -> dict[str, Any]:
    samples = sorted(samples, key=lambda sample: int(sample["timestamp_unix_milliseconds"]))
    window_samples = [
        sample for sample in samples if start <= int(sample["timestamp_unix_milliseconds"]) <= end
    ]
    cpu_deltas = [Fraction(0) for _ in range(8)]
    vmstat_keys = ("pgfault", "pgmajfault", "pswpin", "pswpout")
    vmstat_deltas = {key: Fraction(0) for key in vmstat_keys}
    for previous, current in zip(samples, samples[1:]):
        left = int(previous["timestamp_unix_milliseconds"])
        right = int(current["timestamp_unix_milliseconds"])
        fraction = overlapping_fraction(left, right, start, end)
        if not fraction:
            continue
        previous_cpu = parse_cpu(previous)
        current_cpu = parse_cpu(current)
        for index, (old, new) in enumerate(zip(previous_cpu, current_cpu)):
            if new < old:
                raise ValueError("host aggregate CPU counters regressed")
            cpu_deltas[index] += (new - old) * fraction
        for key in vmstat_keys:
            old = int(previous["vmstat"].get(key, 0))
            new = int(current["vmstat"].get(key, 0))
            if new < old:
                raise ValueError(f"host vmstat counter {key} regressed")
            vmstat_deltas[key] += (new - old) * fraction
    total_cpu = sum(cpu_deltas)
    if not window_samples or total_cpu <= 0:
        raise ValueError("host samples do not provide CPU activity inside the window")
    idle = cpu_deltas[3]
    iowait = cpu_deltas[4]
    pressure_maxima: dict[str, float] = {}
    for sample in window_samples:
        for resource, lines in sample["pressure"].items():
            for scope, values in parse_pressure(lines).items():
                for key in ("avg10", "avg60", "avg300"):
                    name = f"{resource}_{scope}_{key}_percent"
                    pressure_maxima[name] = max(pressure_maxima.get(name, 0.0), values[key])
    load_values = [sample["loadavg"].split() for sample in window_samples]
    return {
        "measurement": "exact-window-host-resource-summary",
        "cpu_percent": {
            "busy_excluding_iowait": float((total_cpu - idle - iowait) * 100 / total_cpu),
            "idle": float(idle * 100 / total_cpu),
            "iowait": float(iowait * 100 / total_cpu),
            "steal": float(cpu_deltas[7] * 100 / total_cpu),
            "user_and_nice": float((cpu_deltas[0] + cpu_deltas[1]) * 100 / total_cpu),
            "system_irq_softirq": float(
                (cpu_deltas[2] + cpu_deltas[5] + cpu_deltas[6]) * 100 / total_cpu
            ),
        },
        "sampled_load": {
            "maximum_one_minute": max(float(values[0]) for values in load_values),
            "maximum_runnable_tasks": max(
                int(values[3].split("/", 1)[0]) for values in load_values
            ),
        },
        "sampled_memory": {
            "minimum_available_bytes": min(
                int(sample["meminfo"].get("MemAvailable", 0)) for sample in window_samples
            )
            * 1024,
            "maximum_swap_used_bytes": max(
                int(sample["meminfo"].get("SwapTotal", 0))
                - int(sample["meminfo"].get("SwapFree", 0))
                for sample in window_samples
            )
            * 1024,
        },
        "sampled_pressure_maxima": pressure_maxima,
        "prorated_vmstat_deltas": {key: float(value) for key, value in vmstat_deltas.items()},
        "attribution": (
            "CPU and vmstat deltas are prorated at boundary intervals; load, memory and PSI are "
            "sampled gauges whose timestamps fall inside the window"
        ),
    }


def coverage(start: int, end: int, kind: str, path: str) -> dict[str, Any]:
    if end <= start:
        raise ValueError("measurement window is nonpositive")
    if kind == "process-tsv":
        with open(path, encoding="utf-8", newline="") as source:
            rows = list(csv.DictReader(source, delimiter="\t"))
        missing = sorted(
            key
            for key in PROCESS_REQUIRED_FIELDS
            if not rows or any(row.get(key) in (None, "") for row in rows)
        )
        timestamps = [
            int(row["interval_end_epoch_milliseconds"])
            for row in rows
            if row.get("interval_end_epoch_milliseconds") not in (None, "")
        ]
        summary = None if missing else process_resource_summary(start, end, rows)
    elif kind == "host-jsonl":
        samples = read_json_lines(path)
        missing = host_missing_fields(samples)
        timestamps = [
            int(sample["timestamp_unix_milliseconds"])
            for sample in samples
            if sample.get("timestamp_unix_milliseconds") is not None
        ]
        summary = None if missing else host_resource_summary(start, end, samples)
    else:
        raise ValueError(f"unsupported coverage kind {kind}")
    evidence = coverage_evidence(start, end, timestamps, RESOURCE_MAXIMUM_GAP_MILLISECONDS)
    evidence.update(
        measurement="exact-window-sample-coverage",
        kind=kind,
        path=path,
        missing_required_fields=missing,
        measurement_window_summary=summary,
    )
    evidence["complete"] = evidence["complete"] and not missing and summary is not None
    return evidence


def parse_log_timestamp(line: str) -> int | None:
    first = line.split(maxsplit=1)[0]
    try:
        parsed = dt.datetime.fromisoformat(first.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(parsed.timestamp() * 1000)


def group_commit_metrics(start: int, end: int, path: Path) -> dict[str, Any]:
    number = re.compile(r"(?:^|\s)([a-z][a-z0-9_]*)=([0-9.eE+-]+)(?=\s|$)")
    boolean = re.compile(r"(?:^|\s)(phase_complete|physical_commit)=(true|false)(?=\s|$)")
    reason = re.compile(r'(?:^|\s)stop_reason="?([^"\s]+)"?')
    all_groups = 0
    contained: list[dict[str, float]] = []
    crossing = 0
    stop_reasons: dict[str, int] = {}
    missing_timestamps = 0
    if not path.is_file():
        return {
            "measurement": "completed-group-execution-window-distributions",
            "path": str(path),
            "available": False,
            "complete": False,
            "error": "sibling server.log is unavailable",
        }
    with path.open(encoding="utf-8") as source:
        for line in source:
            if "single-node mutation group completed" not in line:
                continue
            all_groups += 1
            values = {name: float(raw) for name, raw in number.findall(line)}
            values.update({name: float(raw == "true") for name, raw in boolean.findall(line)})
            begin = values.get("group_execute_started_epoch_milliseconds")
            finish = values.get("group_execute_ended_epoch_milliseconds")
            if begin is None or finish is None:
                missing_timestamps += 1
                continue
            if begin >= start and finish <= end:
                contained.append(values)
                match = reason.search(line)
                if match:
                    stop_reasons[match.group(1)] = stop_reasons.get(match.group(1), 0) + 1
            elif begin < end and finish > start:
                crossing += 1
    fields = sorted({name for group in contained for name in group})
    distributions = {
        name: distribution([group[name] for group in contained if name in group]) for name in fields
    }
    missing = sorted(set(GROUP_COMMIT_REQUIRED_FIELDS) - set(fields))
    return {
        "measurement": "completed-group-execution-window-distributions",
        "path": str(path),
        "available": True,
        "window": {"start": start, "end": end},
        "logged_group_count": all_groups,
        "fully_contained_group_count": len(contained),
        "window_crossing_group_count": crossing,
        "groups_missing_execution_timestamps": missing_timestamps,
        "missing_required_fields": missing,
        "stop_reason_counts": stop_reasons,
        "numeric_field_distributions": distributions,
        "since_start_peak_fields": [
            "lane_peak_queued_requests_since_start",
            "total_peak_queued_requests_since_start",
            "physical_slots_peak_since_start_at_acquire",
            "physical_slots_peak_since_start_before_release",
        ],
        "aggregation_semantics": (
            "each distribution contains only groups whose complete execution interval falls inside "
            "the measurement window; duration sums are concurrent group-seconds, not wall time; "
            "fields named since_start are process-lifetime high-water marks"
        ),
        "complete": bool(contained) and not missing and missing_timestamps == 0,
    }


def rocksdb_metrics(start: int, end: int, path: str) -> dict[str, Any]:
    runtime_samples: list[tuple[int, dict[str, float]]] = []
    current_sample: tuple[int, dict[str, float]] | None = None
    sample_line_count = 0
    metric = re.compile(r"(?:gauge\.)?(keldra_(?:rocksdb|storage)_[a-z0-9_]+)=([0-9.eE+-]+)")
    with open(path, encoding="utf-8") as source:
        for line in source:
            if "keldra_rocksdb_" not in line and "keldra_storage_wal_bytes" not in line:
                continue
            timestamp = parse_log_timestamp(line)
            if timestamp is None:
                continue
            sample_line_count += 1
            parsed_metrics = [(name, float(raw)) for name, raw in metric.findall(line)]
            if any(
                name == "keldra_rocksdb_block_cache_capacity_bytes"
                for name, _ in parsed_metrics
            ):
                if current_sample is not None:
                    runtime_samples.append(current_sample)
                current_sample = (timestamp, {})
            if current_sample is not None:
                current_sample[1].update(parsed_metrics)
    if current_sample is not None:
        runtime_samples.append(current_sample)
    runtime_sample_timestamps = [timestamp for timestamp, _ in runtime_samples]
    evidence = coverage_evidence(
        start, end, runtime_sample_timestamps, ROCKSDB_MAXIMUM_GAP_MILLISECONDS
    )
    in_window = [sample for timestamp, sample in runtime_samples if start <= timestamp <= end]
    present = {name for sample in in_window for name in sample}
    missing = sorted(set(ROCKSDB_REQUIRED_METRICS) - present)
    summaries = {
        name: {
            "sample_count": len(values),
            "first": values[0],
            "last": values[-1],
            "minimum": min(values),
            "maximum": max(values),
        }
        for name in sorted(present)
        if (values := [sample[name] for sample in in_window if name in sample])
    }
    maxima = {name: summary["maximum"] for name, summary in summaries.items()}
    in_window_runtime_samples = sum(
        start <= timestamp <= end for timestamp in runtime_sample_timestamps
    )
    incomplete_sample_metrics = sorted(
        name
        for name in ROCKSDB_REQUIRED_METRICS
        if summaries.get(name, {}).get("sample_count", 0) < in_window_runtime_samples
    )
    unavailable = maxima.get("keldra_rocksdb_unavailable_properties", 0) > 0
    group_commit = group_commit_metrics(start, end, Path(path).with_name("server.log"))
    evidence.update({
        "measurement": "rocksdb-runtime-exact-window-coverage",
        "path": path,
        "sample_line_count": sample_line_count,
        "distinct_runtime_sample_count": len(set(runtime_sample_timestamps)),
        "missing_required_metrics": missing,
        "metrics_not_present_in_every_runtime_sample": incomplete_sample_metrics,
        "reported_unavailable_properties": unavailable,
        "measurement_window_maxima": maxima,
        "measurement_window_metric_samples": summaries,
        "group_commit": group_commit,
    })
    evidence["complete"] = (
        evidence["complete"]
        and not missing
        and not incomplete_sample_metrics
        and not unavailable
        and group_commit["complete"]
    )
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    pipeline = subcommands.add_parser("pipeline")
    disk = subcommands.add_parser("disk")
    coverage_command = subcommands.add_parser("coverage")
    rocksdb = subcommands.add_parser("rocksdb")
    for command in (pipeline, disk, coverage_command, rocksdb):
        command.add_argument("--start-ms", required=True, type=int)
        command.add_argument("--end-ms", required=True, type=int)
        command.add_argument("--samples", required=True)
    disk.add_argument("--device", required=True)
    coverage_command.add_argument("--kind", choices=("process-tsv", "host-jsonl"), required=True)
    args = parser.parse_args()
    try:
        if args.command == "pipeline":
            result = pipeline_metrics(args.start_ms, args.end_ms, args.samples)
        elif args.command == "disk":
            result = disk_metrics(args.start_ms, args.end_ms, args.device, args.samples)
        elif args.command == "coverage":
            result = coverage(args.start_ms, args.end_ms, args.kind, args.samples)
        else:
            result = rocksdb_metrics(args.start_ms, args.end_ms, args.samples)
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
