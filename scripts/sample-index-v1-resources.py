#!/usr/bin/env python3
"""Low-overhead, timestamped Linux process and host resource sampler."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any


PROCESS_FIELDS = (
    "timestamp_utc",
    "interval_end_epoch_milliseconds",
    "interval_cpu_percent",
    "rss_kib",
    "threads",
    "read_bytes_total",
    "write_bytes_total",
    "cancelled_write_bytes_total",
    "mem_available_kib",
    "interval_seconds",
    "interval_kernel_write_bytes",
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


def read_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition(":")
        if value:
            try:
                values[key] = int(value.strip().split()[0])
            except ValueError:
                continue
    return values


def process_snapshot(pid: int) -> dict[str, int]:
    root = Path("/proc") / str(pid)
    raw_stat = (root / "stat").read_text(encoding="utf-8")
    fields = raw_stat[raw_stat.rfind(")") + 2 :].split()
    status = read_key_values(root / "status")
    io = read_key_values(root / "io")
    meminfo = read_key_values(Path("/proc/meminfo"))
    return {
        "cpu_ticks": int(fields[11]) + int(fields[12]),
        "rss_kib": status.get("VmRSS", 0),
        "threads": status.get("Threads", int(fields[17])),
        "read_bytes_total": io.get("read_bytes", 0),
        "write_bytes_total": io.get("write_bytes", 0),
        "cancelled_write_bytes_total": io.get("cancelled_write_bytes", 0),
        "mem_available_kib": meminfo.get("MemAvailable", 0),
        "minor_faults_total": int(fields[7]),
        "major_faults_total": int(fields[9]),
        "voluntary_context_switches_total": status.get("voluntary_ctxt_switches", 0),
        "nonvoluntary_context_switches_total": status.get("nonvoluntary_ctxt_switches", 0),
        "rchar_total": io.get("rchar", 0),
        "wchar_total": io.get("wchar", 0),
        "read_syscalls_total": io.get("syscr", 0),
        "write_syscalls_total": io.get("syscw", 0),
        "process_starttime_ticks": int(fields[19]),
    }


def utc_timestamp(now_ns: int) -> str:
    return dt.datetime.fromtimestamp(now_ns / 1_000_000_000, dt.UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def require_same_process(previous: dict[str, int], current: dict[str, int], pid: int) -> None:
    if current["process_starttime_ticks"] != previous["process_starttime_ticks"]:
        raise RuntimeError(f"PID {pid} was reused while resource sampling was active")


def sample_process(pid: int, output: Path, interval: float) -> None:
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    previous_ns = time.time_ns()
    previous = process_snapshot(pid)
    with output.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=PROCESS_FIELDS, delimiter="\t")
        writer.writeheader()
        target.flush()
        while True:
            time.sleep(interval)
            now_ns = time.time_ns()
            try:
                current = process_snapshot(pid)
            except (FileNotFoundError, ProcessLookupError):
                return
            require_same_process(previous, current, pid)
            seconds = (now_ns - previous_ns) / 1_000_000_000
            row: dict[str, Any] = {
                "timestamp_utc": utc_timestamp(now_ns),
                "interval_end_epoch_milliseconds": now_ns // 1_000_000,
                "interval_cpu_percent": ((current["cpu_ticks"] - previous["cpu_ticks"]) / ticks_per_second / seconds * 100) if seconds > 0 else 0,
                "interval_seconds": seconds,
                "interval_kernel_write_bytes": max(0, current["write_bytes_total"] - previous["write_bytes_total"]),
            }
            row.update({key: current[key] for key in PROCESS_FIELDS if key in current})
            writer.writerow(row)
            target.flush()
            previous_ns, previous = now_ns, current


def read_lines(path: str) -> list[str]:
    return Path(path).read_text(encoding="utf-8").splitlines()


def sample_host(output: Path, interval: float) -> None:
    with output.open("w", encoding="utf-8") as target:
        while True:
            now_ns = time.time_ns()
            sample = {
                "timestamp_utc": utc_timestamp(now_ns),
                "timestamp_unix_milliseconds": now_ns // 1_000_000,
                "proc_stat": read_lines("/proc/stat"),
                "loadavg": Path("/proc/loadavg").read_text(encoding="utf-8").strip(),
                "pressure": {name: read_lines(f"/proc/pressure/{name}") for name in ("cpu", "io", "memory")},
                "meminfo": read_key_values(Path("/proc/meminfo")),
                "vmstat": {line.split()[0]: int(line.split()[1]) for line in read_lines("/proc/vmstat")},
            }
            target.write(json.dumps(sample, separators=(",", ":"), sort_keys=True) + "\n")
            target.flush()
            time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    process = subcommands.add_parser("process")
    process.add_argument("--pid", type=int, required=True)
    host = subcommands.add_parser("host")
    for command in (process, host):
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--interval-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if args.interval_seconds <= 0:
        raise SystemExit("interval must be positive")
    if args.command == "process":
        sample_process(args.pid, args.output, args.interval_seconds)
    else:
        sample_host(args.output, args.interval_seconds)


if __name__ == "__main__":
    main()
