#!/usr/bin/env python3
"""Summarise Anvil line-protocol performance traces and release-gate logs."""

from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path


def split_unescaped_space(line: str) -> tuple[str, str, str] | None:
    escaped = False
    for idx, ch in enumerate(line):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == " ":
            rest = line[idx + 1 :]
            try:
                fields, ts = rest.rsplit(" ", 1)
            except ValueError:
                return None
            return line[:idx], fields, ts
    return None


def unescape_key(value: str) -> str:
    out = []
    escaped = False
    for ch in value:
        if escaped:
            out.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        else:
            out.append(ch)
    if escaped:
        out.append("\\")
    return "".join(out)


def split_unescaped(value: str, sep: str) -> list[str]:
    parts = []
    start = 0
    escaped = False
    for idx, ch in enumerate(value):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == sep:
            parts.append(value[start:idx])
            start = idx + 1
    parts.append(value[start:])
    return parts


def parse_tags(head: str) -> tuple[str, dict[str, str]]:
    parts = split_unescaped(head, ",")
    measurement = unescape_key(parts[0])
    tags: dict[str, str] = {}
    for part in parts[1:]:
        key, _, value = part.partition("=")
        tags[unescape_key(key)] = unescape_key(value)
    return measurement, tags


def parse_fields(fields: str) -> dict[str, float | str]:
    parsed: dict[str, float | str] = {}
    for part in split_unescaped(fields, ","):
        key, _, value = part.partition("=")
        key = unescape_key(key)
        if value.endswith("u") or value.endswith("i"):
            try:
                parsed[key] = float(value[:-1])
            except ValueError:
                parsed[key] = value
        elif value.startswith('"') and value.endswith('"'):
            parsed[key] = value[1:-1].replace('\\"', '"').replace('\\\\', '\\')
        else:
            try:
                parsed[key] = float(value)
            except ValueError:
                parsed[key] = value
    return parsed


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def normalise_span(span: str) -> str:
    return re.sub(r" tx=.*$", " tx=*", span)


def print_group(title: str, groups: dict[str, list[float]], limit: int) -> None:
    if not groups:
        return
    rows = []
    for key, values in groups.items():
        rows.append(
            (
                max(values),
                sum(values),
                key,
                len(values),
                statistics.median(values),
                percentile(values, 95),
            )
        )
    rows.sort(reverse=True)
    print(f"\n{title}")
    print("max_ms\tsum_ms\tcount\tp50_ms\tp95_ms\tkey")
    for max_ms, sum_ms, key, count, p50, p95 in rows[:limit]:
        print(f"{max_ms:.3f}\t{sum_ms:.3f}\t{count}\t{p50:.3f}\t{p95:.3f}\t{key}")


def analyze_line_file(path: Path, limit: int) -> None:
    by_case: dict[str, list[float]] = defaultdict(list)
    by_request: dict[str, list[float]] = defaultdict(list)
    by_internal_span: dict[str, list[float]] = defaultdict(list)
    by_test_span: dict[str, list[float]] = defaultdict(list)

    for raw in path.read_text().splitlines():
        split = split_unescaped_space(raw)
        if split is None:
            continue
        head, fields_raw, _ts = split
        measurement, tags = parse_tags(head)
        fields = parse_fields(fields_raw)
        duration = fields.get("duration_ms")
        if not isinstance(duration, float):
            continue
        if measurement == "anvil_perf_case":
            by_case[tags.get("case", "unknown")].append(duration)
        elif measurement in {"anvil_request", "anvil_request_mux"}:
            key = f"{measurement} {tags.get('plane','?')} {tags.get('method','?')} {tags.get('path','?')} status={tags.get('status','?')}"
            by_request[key].append(duration)
        elif measurement == "anvil_internal_span":
            by_internal_span[normalise_span(tags.get("span", "unknown"))].append(duration)
        elif measurement == "anvil_test_span":
            by_test_span[tags.get("span", "unknown")].append(duration)

    print(f"Trace: {path}")
    print_group("Perf cases", by_case, limit)
    print_group("Request timings", by_request, limit)
    print_group("Internal spans", by_internal_span, limit)
    print_group("Test spans", by_test_span, limit)


def analyze_summary(path: Path) -> None:
    data = json.loads(path.read_text())
    print(f"Summary: {path}")
    for sample in sorted(data.get("samples", []), key=lambda s: s["duration_ms"], reverse=True):
        print(f"{sample['duration_ms']:.3f} ms\t{sample['name']}")


def analyze_coremeta_report(path: Path) -> None:
    data = json.loads(path.read_text())
    print(f"CoreMeta gate: {path}")
    print(
        f"profile={data.get('profile')} pass={data.get('pass')} "
        f"elapsed_ms={data.get('elapsed_ms')} manifest={data.get('gate_manifest_hash')}"
    )
    print("p95_ms\twork_per_op\tsamples\tscenario")
    for scenario in data.get("scenarios", []):
        print(
            f"{scenario['p95_ms']:.3f}\t"
            f"{scenario['logical_work_per_operation']:.2f}\t"
            f"{scenario['sample_count']}\t{scenario['name']}"
        )
    failed = [gate for gate in data.get("gates", []) if not gate.get("pass")]
    if failed:
        print("failed gates:")
        for gate in failed:
            print(
                f"{gate['name']} observed={gate.get('observed')} "
                f"expected={gate['expectation']} {gate['effective_threshold']} {gate['unit']}"
            )


def analyze_release_gate(path: Path, limit: int) -> None:
    text = path.read_text(errors="replace")
    started = len(re.findall(r"^     Running .+", text, re.MULTILINE))
    finished = re.findall(r"test result: .+ finished in ([0-9.]+)s", text)
    warnings = re.findall(r"test (.+?) has been running for over ([0-9]+) seconds", text)
    print(f"Release gate: {path}")
    print(f"test binaries started={started}")
    print(f"finished_groups={len(finished)} finished_seconds_sum={sum(map(float, finished)):.3f}")
    print(f"over_60s_warnings={len(warnings)}")
    if warnings:
        by_test: dict[str, int] = defaultdict(int)
        for name, _seconds in warnings:
            by_test[name] += 1
        print("slow warning counts:")
        for name, count in sorted(by_test.items(), key=lambda item: item[1], reverse=True)[:limit]:
            print(f"{count}\t{name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--line", type=Path, default=Path("target/anvil/perf/anvil.line"))
    parser.add_argument("--summary", type=Path, default=Path("target/anvil/perf/performance-summary.json"))
    parser.add_argument(
        "--coremeta-report",
        type=Path,
        default=Path("target/anvil/perf/coremeta/quick/report.json"),
    )
    parser.add_argument("--release-log", type=Path, default=Path("target/anvil/logs/release-gates.log"))
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    if args.summary.exists():
        analyze_summary(args.summary)
    if args.coremeta_report.exists():
        analyze_coremeta_report(args.coremeta_report)
    if args.line.exists():
        analyze_line_file(args.line, args.limit)
    if args.release_log.exists():
        analyze_release_gate(args.release_log, args.limit)


if __name__ == "__main__":
    main()
