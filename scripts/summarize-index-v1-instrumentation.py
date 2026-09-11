#!/usr/bin/env python3
"""Derive exact-window index and physical-disk metrics from raw samples."""

from __future__ import annotations

import argparse
import csv
import json
from fractions import Fraction
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


def main() -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    pipeline = subcommands.add_parser("pipeline")
    disk = subcommands.add_parser("disk")
    for command in (pipeline, disk):
        command.add_argument("--start-ms", required=True, type=int)
        command.add_argument("--end-ms", required=True, type=int)
        command.add_argument("--samples", required=True)
    disk.add_argument("--device", required=True)
    args = parser.parse_args()
    try:
        if args.command == "pipeline":
            result = pipeline_metrics(args.start_ms, args.end_ms, args.samples)
        else:
            result = disk_metrics(args.start_ms, args.end_ms, args.device, args.samples)
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
        raise SystemExit(str(error)) from error
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
