#!/usr/bin/env python3
"""Validate CoreMeta performance-gate evidence after the benchmark exits."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Any


REPORT_SCHEMA = "anvil.perf.coremeta_gate_report.v1"
MANIFEST_SCHEMA = "anvil.perf.coremeta_gate_manifest.v1"
WORK_COUNTER_SOURCE = "RocksDB PerfContext EnableCount"
REQUIRED_SCENARIOS = frozenset(
    {
        "point_get_small",
        "point_get_large",
        "prefix_page_early_large",
        "prefix_page_deep_small",
        "prefix_page_deep_large",
        "bounded_list_scaled_page_large",
        "durable_single_row",
        "transactional_head_read_and_batch",
        "atomic_root_publication_small_table",
        "atomic_root_publication_large_table",
        "atomic_root_publication_multi_page_generation",
        "generation_inventory_small_table",
        "generation_inventory_large_table",
        "generation_inventory_captured_after_growth",
        "generation_catch_up_early_large",
        "generation_catch_up_deep_small",
        "generation_catch_up_deep_large",
        "generation_catch_up_multi_page_traversal",
    }
)
REQUIRED_COMPLEXITY_GATES = frozenset(
    {
        "point_get_work_is_table_size_independent",
        "deep_page_work_is_table_size_independent",
        "deep_page_work_is_comparable_to_early_page",
        "bounded_list_work_scales_with_page_size",
        "prefix_page_early_large_bounded_work",
        "prefix_page_deep_small_bounded_work",
        "prefix_page_deep_large_bounded_work",
        "bounded_list_scaled_page_large_bounded_work",
        "atomic_root_publication_work_is_table_size_independent",
        "atomic_root_publication_work_scales_with_generation_rows",
        "atomic_root_publication_small_table_bounded_work",
        "atomic_root_publication_large_table_bounded_work",
        "atomic_root_publication_multi_page_generation_bounded_work",
        "generation_inventory_work_is_table_size_independent",
        "captured_generation_inventory_work_stays_bounded_after_growth",
        "deep_catch_up_work_is_table_size_independent",
        "deep_catch_up_work_is_comparable_to_early_page",
        "multi_page_generation_work_scales_with_pages",
        "generation_inventory_small_table_bounded_work",
        "generation_inventory_large_table_bounded_work",
        "generation_inventory_captured_after_growth_bounded_work",
        "generation_catch_up_early_large_bounded_work",
        "generation_catch_up_deep_small_bounded_work",
        "generation_catch_up_deep_large_bounded_work",
        "generation_catch_up_multi_page_traversal_bounded_work",
    }
)
REQUIRED_CORRECTNESS_GATES = frozenset(
    {
        "rocksdb_work_counters_present",
        "generation_inventory_snapshot_is_immutable",
        "deep_catch_up_cursor_progress_is_exact",
        "multi_page_generation_cursor_progress_is_exact",
        "multi_page_generation_history_shape_is_exact",
        "multi_page_generation_uses_expected_page_count",
    }
) | frozenset(f"{name}_bounded_result" for name in REQUIRED_SCENARIOS)
REQUIRED_LATENCY_GATES = frozenset(
    {
        "point_get_small_p95",
        "point_get_large_p95",
        "prefix_page_early_large_p95",
        "prefix_page_deep_small_p95",
        "prefix_page_deep_large_p95",
        "bounded_list_scaled_page_large_p95",
        "durable_single_row_p95",
        "transactional_head_read_and_batch_p95",
    }
)
REQUIRED_GATES = (
    REQUIRED_COMPLEXITY_GATES | REQUIRED_CORRECTNESS_GATES | REQUIRED_LATENCY_GATES
)

REPORT_FIELDS = frozenset(
    {
        "schema",
        "pass",
        "profile",
        "dataset_id",
        "seed",
        "gate_manifest_hash",
        "gate_manifest_sha256",
        "profile_spec",
        "git_commit",
        "started_at_unix_ms",
        "elapsed_ms",
        "work_counter_source",
        "machine",
        "scenarios",
        "gates",
    }
)
MANIFEST_FIELDS = frozenset({"schema", "dataset_id", "seed", "profiles"})
PROFILE_FIELDS = frozenset(
    {
        "small_rows",
        "large_rows",
        "unrelated_rows",
        "batch_rows",
        "payload_bytes",
        "page_size",
        "scaling_page_size",
        "warmup_operations",
        "point_samples",
        "page_samples",
        "mutation_samples",
        "root_publication_warmup_operations",
        "root_publication_samples",
        "history_probe_warmup_operations",
        "history_probe_samples",
        "history_page_size",
        "history_max_page_bytes",
        "multi_page_generation_warmup_operations",
        "multi_page_generation_samples",
        "multi_page_generation_rows",
        "root_publication_history_rows_per_logical_mutation",
        "root_publication_history_fixed_rows",
        "thresholds",
    }
)
THRESHOLD_FIELDS = frozenset(
    {
        "point_get_p95_ms",
        "page_p95_floor_ms",
        "page_p95_point_multiplier",
        "durable_single_p95_ms",
        "transactional_head_p95_floor_ms",
        "transactional_head_single_multiplier",
        "point_work_growth_ratio",
        "deep_page_work_growth_ratio",
        "deep_to_early_work_ratio",
        "page_size_work_ratio_multiplier",
        "page_work_per_item",
        "page_fixed_work",
        "root_publication_work_growth_ratio",
        "root_publication_row_work_ratio_multiplier",
        "root_publication_work_per_mutation",
        "root_publication_fixed_work",
        "history_work_growth_ratio",
        "history_page_work_ratio_multiplier",
        "history_work_per_item",
        "history_fixed_work",
    }
)
SCENARIO_FIELDS = frozenset(
    {
        "name",
        "dataset_rows",
        "expected_items_per_operation",
        "returned_items",
        "item_count_mismatches",
        "sample_count",
        "latency_ns",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "throughput_per_second",
        "work",
        "logical_work_per_operation",
    }
)
WORK_COUNTER_FIELDS = frozenset(
    {
        "user_key_comparisons",
        "block_cache_hits",
        "block_reads",
        "block_read_bytes",
        "get_read_bytes",
        "get_from_memtable_count",
        "iterator_read_bytes",
        "internal_keys_skipped",
        "memtable_seeks",
        "memtable_nexts",
        "child_seeks",
        "write_wal_nanos",
        "write_memtable_nanos",
        "db_mutex_wait_nanos",
    }
)
LOGICAL_WORK_FIELDS = frozenset(
    {
        "user_key_comparisons",
        "block_reads",
        "get_from_memtable_count",
        "internal_keys_skipped",
        "memtable_seeks",
        "memtable_nexts",
        "child_seeks",
    }
)
MACHINE_FIELDS = frozenset(
    {"machine_class", "operating_system", "architecture", "logical_cpus", "hostname"}
)
GATE_FIELDS = frozenset(
    {
        "name",
        "category",
        "metric",
        "expectation",
        "observed",
        "configured_threshold",
        "effective_threshold",
        "unit",
        "pass",
        "detail",
    }
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--profile", choices=("quick", "release"), required=True)
    return parser.parse_args()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"CoreMeta performance report rejected: {message}")


def finite_number(value: Any) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (OverflowError, ValueError):
        return False


def nonnegative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def positive_integer(value: Any) -> bool:
    return nonnegative_integer(value) and value > 0


def require_fields(value: dict[str, Any], expected: frozenset[str], label: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    require(not missing, f"{label} fields are absent: {', '.join(missing)}")
    require(not unexpected, f"{label} fields are unexpected: {', '.join(unexpected)}")


def require_close(actual: Any, expected: float, label: str) -> None:
    require(finite_number(actual), f"{label} is not finite")
    require(
        math.isclose(float(actual), float(expected), rel_tol=1e-12, abs_tol=1e-12),
        f"{label} does not match benchmark evidence",
    )


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key {key!r}")
        value[key] = item
    return value


def reject_constant(value: str) -> None:
    raise ValueError(f"invalid JSON number {value}")


def load_json(raw: bytes, label: str) -> Any:
    try:
        return json.loads(
            raw,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:
        raise SystemExit(
            f"CoreMeta performance report rejected: invalid {label}: {error}"
        ) from error


def validate_profile(profile: Any, label: str) -> dict[str, Any]:
    require(isinstance(profile, dict), f"{label} must be an object")
    require_fields(profile, PROFILE_FIELDS, label)
    for field in PROFILE_FIELDS - {"thresholds"}:
        require(positive_integer(profile.get(field)), f"{label}.{field} must be positive")
    thresholds = profile.get("thresholds")
    require(isinstance(thresholds, dict), f"{label}.thresholds must be an object")
    require_fields(thresholds, THRESHOLD_FIELDS, f"{label}.thresholds")
    for field in THRESHOLD_FIELDS:
        value = thresholds.get(field)
        require(
            finite_number(value) and value > 0,
            f"{label}.thresholds.{field} must be finite and positive",
        )
    return profile


def scenario_contract(profile: dict[str, Any]) -> dict[str, tuple[int, int, int]]:
    mutation_rows = profile["warmup_operations"] + profile["mutation_samples"]
    publication_history_rows = (
        profile["multi_page_generation_rows"]
        * profile["root_publication_history_rows_per_logical_mutation"]
        + profile["root_publication_history_fixed_rows"]
    )
    return {
        "point_get_small": (profile["small_rows"], profile["point_samples"], 1),
        "point_get_large": (profile["large_rows"], profile["point_samples"], 1),
        "prefix_page_early_large": (
            profile["large_rows"],
            profile["page_samples"],
            profile["page_size"],
        ),
        "prefix_page_deep_small": (
            profile["small_rows"],
            profile["page_samples"],
            profile["page_size"],
        ),
        "prefix_page_deep_large": (
            profile["large_rows"],
            profile["page_samples"],
            profile["page_size"],
        ),
        "bounded_list_scaled_page_large": (
            profile["large_rows"],
            profile["page_samples"],
            profile["scaling_page_size"],
        ),
        "durable_single_row": (mutation_rows, profile["mutation_samples"], 1),
        "transactional_head_read_and_batch": (
            mutation_rows,
            profile["mutation_samples"],
            2,
        ),
        "atomic_root_publication_small_table": (
            profile["small_rows"],
            profile["root_publication_samples"],
            1,
        ),
        "atomic_root_publication_large_table": (
            profile["large_rows"],
            profile["root_publication_samples"],
            1,
        ),
        "atomic_root_publication_multi_page_generation": (
            profile["large_rows"],
            profile["multi_page_generation_samples"],
            profile["multi_page_generation_rows"],
        ),
        "generation_inventory_small_table": (
            profile["small_rows"],
            profile["history_probe_samples"],
            profile["history_page_size"],
        ),
        "generation_inventory_large_table": (
            profile["large_rows"],
            profile["history_probe_samples"],
            profile["history_page_size"],
        ),
        "generation_inventory_captured_after_growth": (
            profile["large_rows"],
            profile["history_probe_samples"],
            profile["history_page_size"],
        ),
        "generation_catch_up_early_large": (
            profile["large_rows"],
            profile["history_probe_samples"],
            profile["history_page_size"],
        ),
        "generation_catch_up_deep_small": (
            profile["small_rows"],
            profile["history_probe_samples"],
            profile["history_page_size"],
        ),
        "generation_catch_up_deep_large": (
            profile["large_rows"],
            profile["history_probe_samples"],
            profile["history_page_size"],
        ),
        "generation_catch_up_multi_page_traversal": (
            profile["large_rows"],
            profile["multi_page_generation_samples"],
            publication_history_rows,
        ),
    }


def percentile_ms(samples_ns: list[int], percentile: int) -> float:
    ordered = sorted(samples_ns)
    rank = max((len(ordered) * percentile + 99) // 100, 1)
    return ordered[min(rank, len(ordered)) - 1] / 1_000_000.0


def validate_scenarios(
    scenarios: Any, profile: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    require(isinstance(scenarios, list), "scenarios must be an array")
    require(
        all(isinstance(scenario, dict) for scenario in scenarios),
        "every scenario must be an object",
    )
    scenario_names = [scenario.get("name") for scenario in scenarios]
    require(
        all(isinstance(name, str) and name for name in scenario_names),
        "every scenario must have a non-empty name",
    )
    require(
        len(scenario_names) == len(set(scenario_names)),
        "scenario names must be unique",
    )
    actual_scenarios = set(scenario_names)
    missing_scenarios = sorted(REQUIRED_SCENARIOS - actual_scenarios)
    unexpected_scenarios = sorted(actual_scenarios - REQUIRED_SCENARIOS)
    require(
        not missing_scenarios,
        f"required scenarios are absent: {', '.join(missing_scenarios)}",
    )
    require(
        not unexpected_scenarios,
        f"unexpected scenarios are present: {', '.join(unexpected_scenarios)}",
    )

    expected_contract = scenario_contract(profile)
    by_name: dict[str, dict[str, Any]] = {}
    for scenario in scenarios:
        name = scenario["name"]
        require_fields(scenario, SCENARIO_FIELDS, f"scenario {name}")
        dataset_rows, sample_count, expected_items = expected_contract[name]
        require(
            scenario.get("dataset_rows") == dataset_rows,
            f"{name} dataset row count does not match its profile",
        )
        require(
            scenario.get("sample_count") == sample_count,
            f"{name} sample count does not match its profile",
        )
        require(
            scenario.get("expected_items_per_operation") == expected_items,
            f"{name} item bound does not match its profile",
        )
        require(
            scenario.get("returned_items") == sample_count * expected_items,
            f"{name} returned item total does not match its item bound",
        )
        require(
            nonnegative_integer(scenario.get("item_count_mismatches"))
            and scenario["item_count_mismatches"] == 0,
            f"{name} contains bounded-result mismatches",
        )

        samples = scenario.get("latency_ns")
        require(isinstance(samples, list), f"{name} has no raw latency evidence")
        require(
            len(samples) == sample_count,
            f"{name} raw sample count does not match summary",
        )
        require(
            all(nonnegative_integer(value) for value in samples),
            f"{name} has invalid latency samples",
        )
        require_close(scenario.get("p50_ms"), percentile_ms(samples, 50), f"{name} p50")
        require_close(scenario.get("p95_ms"), percentile_ms(samples, 95), f"{name} p95")
        require_close(scenario.get("p99_ms"), percentile_ms(samples, 99), f"{name} p99")
        total_ns = sum(samples)
        expected_throughput = (
            0.0 if total_ns == 0 else sample_count / (total_ns / 1_000_000_000.0)
        )
        require_close(
            scenario.get("throughput_per_second"),
            expected_throughput,
            f"{name} throughput",
        )

        work = scenario.get("work")
        require(isinstance(work, dict), f"{name} work evidence must be an object")
        require_fields(work, WORK_COUNTER_FIELDS, f"scenario {name} work")
        require(
            all(nonnegative_integer(work.get(field)) for field in WORK_COUNTER_FIELDS),
            f"{name} has invalid work counters",
        )
        logical_work = sum(work[field] for field in LOGICAL_WORK_FIELDS)
        require_close(
            scenario.get("logical_work_per_operation"),
            logical_work / sample_count,
            f"{name} logical work per operation",
        )
        by_name[name] = {"report": scenario, "logical_work": logical_work}
    return by_name


def build_gate_contracts(
    profile: dict[str, Any], scenarios: dict[str, dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    thresholds = profile["thresholds"]

    def report(name: str) -> dict[str, Any]:
        return scenarios[name]["report"]

    def work(name: str) -> float:
        return float(report(name)["logical_work_per_operation"])

    def ratio(numerator: float, denominator: float, name: str) -> float:
        require(denominator > 0, f"{name} has no denominator work evidence")
        return numerator / denominator

    def add(
        name: str,
        category: str,
        metric: str,
        expectation: str,
        observed: float,
        configured_threshold: float,
        effective_threshold: float,
        unit: str,
    ) -> None:
        require(name not in contracts, f"duplicate internal gate contract {name}")
        contracts[name] = {
            "category": category,
            "metric": metric,
            "expectation": expectation,
            "observed": observed,
            "configured_threshold": configured_threshold,
            "effective_threshold": effective_threshold,
            "unit": unit,
        }

    for name in REQUIRED_SCENARIOS:
        add(
            f"{name}_bounded_result",
            "correctness",
            "items_per_operation",
            "==",
            float(report(name)["item_count_mismatches"]),
            0.0,
            0.0,
            "operations",
        )

    read_work_scenarios = (
        "point_get_small",
        "point_get_large",
        "prefix_page_early_large",
        "prefix_page_deep_small",
        "prefix_page_deep_large",
        "bounded_list_scaled_page_large",
        "generation_inventory_small_table",
        "generation_inventory_large_table",
        "generation_inventory_captured_after_growth",
        "generation_catch_up_early_large",
        "generation_catch_up_deep_small",
        "generation_catch_up_deep_large",
        "generation_catch_up_multi_page_traversal",
    )
    add(
        "rocksdb_work_counters_present",
        "correctness",
        "logical_work",
        ">=",
        float(sum(scenarios[name]["logical_work"] for name in read_work_scenarios)),
        1.0,
        1.0,
        "count",
    )

    for name in ("point_get_small", "point_get_large"):
        limit = thresholds["point_get_p95_ms"]
        add(name + "_p95", "latency", "p95_ms", "<=", report(name)["p95_ms"], limit, limit, "ms")

    page_latency_limit = max(
        thresholds["page_p95_floor_ms"],
        report("point_get_large")["p95_ms"] * thresholds["page_p95_point_multiplier"],
    )
    for name in (
        "prefix_page_early_large",
        "prefix_page_deep_small",
        "prefix_page_deep_large",
        "bounded_list_scaled_page_large",
    ):
        add(
            name + "_p95",
            "latency",
            "p95_ms",
            "<=",
            report(name)["p95_ms"],
            thresholds["page_p95_floor_ms"],
            page_latency_limit,
            "ms",
        )

    durable_limit = thresholds["durable_single_p95_ms"]
    add(
        "durable_single_row_p95",
        "latency",
        "p95_ms",
        "<=",
        report("durable_single_row")["p95_ms"],
        durable_limit,
        durable_limit,
        "ms",
    )
    transactional_limit = max(
        thresholds["transactional_head_p95_floor_ms"],
        report("durable_single_row")["p95_ms"]
        * thresholds["transactional_head_single_multiplier"],
    )
    add(
        "transactional_head_read_and_batch_p95",
        "latency",
        "p95_ms",
        "<=",
        report("transactional_head_read_and_batch")["p95_ms"],
        thresholds["transactional_head_p95_floor_ms"],
        transactional_limit,
        "ms",
    )

    ratio_gates = (
        (
            "point_get_work_is_table_size_independent",
            "point_get_large",
            "point_get_small",
            thresholds["point_work_growth_ratio"],
        ),
        (
            "deep_page_work_is_table_size_independent",
            "prefix_page_deep_large",
            "prefix_page_deep_small",
            thresholds["deep_page_work_growth_ratio"],
        ),
        (
            "deep_page_work_is_comparable_to_early_page",
            "prefix_page_deep_large",
            "prefix_page_early_large",
            thresholds["deep_to_early_work_ratio"],
        ),
        (
            "bounded_list_work_scales_with_page_size",
            "bounded_list_scaled_page_large",
            "prefix_page_deep_large",
            profile["scaling_page_size"]
            / profile["page_size"]
            * thresholds["page_size_work_ratio_multiplier"],
        ),
        (
            "atomic_root_publication_work_is_table_size_independent",
            "atomic_root_publication_large_table",
            "atomic_root_publication_small_table",
            thresholds["root_publication_work_growth_ratio"],
        ),
        (
            "atomic_root_publication_work_scales_with_generation_rows",
            "atomic_root_publication_multi_page_generation",
            "atomic_root_publication_large_table",
            profile["multi_page_generation_rows"]
            * thresholds["root_publication_row_work_ratio_multiplier"],
        ),
        (
            "generation_inventory_work_is_table_size_independent",
            "generation_inventory_large_table",
            "generation_inventory_small_table",
            thresholds["history_work_growth_ratio"],
        ),
        (
            "captured_generation_inventory_work_stays_bounded_after_growth",
            "generation_inventory_captured_after_growth",
            "generation_inventory_large_table",
            thresholds["history_work_growth_ratio"],
        ),
        (
            "deep_catch_up_work_is_table_size_independent",
            "generation_catch_up_deep_large",
            "generation_catch_up_deep_small",
            thresholds["history_work_growth_ratio"],
        ),
        (
            "deep_catch_up_work_is_comparable_to_early_page",
            "generation_catch_up_deep_large",
            "generation_catch_up_early_large",
            thresholds["history_work_growth_ratio"],
        ),
    )
    for name, numerator, denominator, limit in ratio_gates:
        add(
            name,
            "complexity",
            "logical_work_per_operation_ratio",
            "<=",
            ratio(work(numerator), work(denominator), name),
            limit,
            limit,
            "ratio",
        )

    page_work_limits = {
        "prefix_page_early_large": profile["page_size"],
        "prefix_page_deep_small": profile["page_size"],
        "prefix_page_deep_large": profile["page_size"],
        "bounded_list_scaled_page_large": profile["scaling_page_size"],
    }
    for name, page_size in page_work_limits.items():
        limit = (
            page_size * thresholds["page_work_per_item"]
            + thresholds["page_fixed_work"]
        )
        add(
            name + "_bounded_work",
            "complexity",
            "logical_work_per_operation",
            "<=",
            work(name),
            limit,
            limit,
            "count/op",
        )

    publication_rows = {
        "atomic_root_publication_small_table": 1,
        "atomic_root_publication_large_table": 1,
        "atomic_root_publication_multi_page_generation": profile["multi_page_generation_rows"],
    }
    for name, mutation_rows in publication_rows.items():
        limit = (
            mutation_rows * thresholds["root_publication_work_per_mutation"]
            + thresholds["root_publication_fixed_work"]
        )
        add(
            name + "_bounded_work",
            "complexity",
            "logical_work_per_operation",
            "<=",
            work(name),
            limit,
            limit,
            "count/op",
        )

    add(
        "generation_inventory_snapshot_is_immutable",
        "correctness",
        "probe_mismatches",
        "==",
        0.0,
        0.0,
        0.0,
        "probes",
    )
    add(
        "deep_catch_up_cursor_progress_is_exact",
        "correctness",
        "cursor_mismatches",
        "==",
        0.0,
        0.0,
        0.0,
        "checks",
    )
    add(
        "multi_page_generation_cursor_progress_is_exact",
        "correctness",
        "cursor_mismatches",
        "==",
        0.0,
        0.0,
        0.0,
        "checks",
    )
    publication_history_rows = (
        profile["multi_page_generation_rows"]
        * profile["root_publication_history_rows_per_logical_mutation"]
        + profile["root_publication_history_fixed_rows"]
    )
    add(
        "multi_page_generation_history_shape_is_exact",
        "correctness",
        "history_mutations",
        "==",
        float(publication_history_rows),
        float(publication_history_rows),
        float(publication_history_rows),
        "mutations",
    )
    expected_pages = (
        publication_history_rows + profile["history_page_size"] - 1
    ) // profile["history_page_size"]
    add(
        "multi_page_generation_uses_expected_page_count",
        "correctness",
        "pages",
        "==",
        float(expected_pages),
        float(expected_pages),
        float(expected_pages),
        "pages",
    )

    traversal_ratio_limit = expected_pages * thresholds["history_page_work_ratio_multiplier"]
    add(
        "multi_page_generation_work_scales_with_pages",
        "complexity",
        "logical_work_per_operation_ratio",
        "<=",
        ratio(
            work("generation_catch_up_multi_page_traversal"),
            work("generation_catch_up_deep_large"),
            "multi_page_generation_work_scales_with_pages",
        ),
        traversal_ratio_limit,
        traversal_ratio_limit,
        "ratio",
    )

    history_page_limit = (
        profile["history_page_size"] * thresholds["history_work_per_item"]
        + thresholds["history_fixed_work"]
    )
    for name in (
        "generation_inventory_small_table",
        "generation_inventory_large_table",
        "generation_inventory_captured_after_growth",
        "generation_catch_up_early_large",
        "generation_catch_up_deep_small",
        "generation_catch_up_deep_large",
    ):
        add(
            name + "_bounded_work",
            "complexity",
            "logical_work_per_operation",
            "<=",
            work(name),
            history_page_limit,
            history_page_limit,
            "count/op",
        )

    traversal_work_limit = (
        publication_history_rows * thresholds["history_work_per_item"]
        + expected_pages * thresholds["history_fixed_work"]
    )
    add(
        "generation_catch_up_multi_page_traversal_bounded_work",
        "complexity",
        "logical_work_per_operation",
        "<=",
        work("generation_catch_up_multi_page_traversal"),
        traversal_work_limit,
        traversal_work_limit,
        "count/op",
    )

    require(set(contracts) == REQUIRED_GATES, "internal gate contract is incomplete")
    return contracts


def validate_gates(
    gates: Any, profile: dict[str, Any], scenarios: dict[str, dict[str, Any]]
) -> None:
    require(isinstance(gates, list) and gates, "gates must be a non-empty array")
    require(all(isinstance(gate, dict) for gate in gates), "every gate must be an object")
    gate_names = [gate.get("name") for gate in gates]
    require(
        all(isinstance(name, str) and name for name in gate_names),
        "every gate must have a non-empty name",
    )
    require(len(gate_names) == len(set(gate_names)), "gate names must be unique")
    contracts = build_gate_contracts(profile, scenarios)
    actual_names = set(gate_names)
    missing_gates = sorted(REQUIRED_GATES - actual_names)
    unexpected_gates = sorted(actual_names - REQUIRED_GATES)
    require(not missing_gates, f"required gates are absent: {', '.join(missing_gates)}")
    require(not unexpected_gates, f"unexpected gates are present: {', '.join(unexpected_gates)}")

    for gate in gates:
        name = gate["name"]
        require_fields(gate, GATE_FIELDS, f"gate {name}")
        contract = contracts[name]
        for field in ("category", "metric", "expectation", "unit"):
            require(
                gate.get(field) == contract[field],
                f"{name} {field} does not match the benchmark contract",
            )
        require_close(gate.get("observed"), contract["observed"], f"{name} observed value")
        require_close(
            gate.get("configured_threshold"),
            contract["configured_threshold"],
            f"{name} configured threshold",
        )
        require_close(
            gate.get("effective_threshold"),
            contract["effective_threshold"],
            f"{name} effective threshold",
        )
        require(
            isinstance(gate.get("detail"), str) and bool(gate["detail"].strip()),
            f"{name} detail is absent",
        )
        observed = float(gate["observed"])
        threshold = float(gate["effective_threshold"])
        expectation = gate["expectation"]
        calculated_pass = (
            observed <= threshold
            if expectation == "<="
            else observed >= threshold
            if expectation == ">="
            else abs(observed - threshold) < math.ulp(1.0)
        )
        require(gate.get("pass") is calculated_pass, f"{name} pass flag is inconsistent")
        require(calculated_pass, f"{name} failed")


def validate(
    report: dict[str, Any],
    manifest: dict[str, Any],
    manifest_bytes: bytes,
    expected_profile: str,
) -> None:
    require_fields(report, REPORT_FIELDS, "report")
    require_fields(manifest, MANIFEST_FIELDS, "gate manifest")
    require(report.get("schema") == REPORT_SCHEMA, "unexpected report schema")
    require(manifest.get("schema") == MANIFEST_SCHEMA, "unexpected gate manifest schema")
    require(report.get("profile") == expected_profile, "profile mismatch")
    require(report.get("pass") is True, "benchmark reported a failed gate")
    require(
        isinstance(report.get("git_commit"), str) and bool(report["git_commit"].strip()),
        "git commit is absent",
    )
    require(
        isinstance(report.get("gate_manifest_hash"), str)
        and re.fullmatch(r"blake3:[0-9a-f]{64}", report["gate_manifest_hash"]) is not None,
        "manifest BLAKE3 hash is invalid",
    )
    require(
        report.get("gate_manifest_sha256")
        == f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}",
        "copied manifest SHA-256 does not match the report",
    )
    require(
        isinstance(manifest.get("dataset_id"), str)
        and bool(manifest["dataset_id"].strip())
        and manifest["dataset_id"] == report.get("dataset_id"),
        "report dataset ID does not match copied manifest",
    )
    require(
        isinstance(manifest.get("seed"), str)
        and bool(manifest["seed"].strip())
        and manifest["seed"] == report.get("seed"),
        "report seed does not match copied manifest",
    )
    profiles = manifest.get("profiles")
    require(isinstance(profiles, dict), "gate manifest profiles must be an object")
    require(
        {"quick", "release"} <= set(profiles),
        "gate manifest must contain quick and release profiles",
    )
    profile = validate_profile(
        profiles.get(expected_profile), f"manifest profile {expected_profile}"
    )
    require(
        report.get("profile_spec") == profile,
        "report profile does not match copied manifest",
    )

    expected_commit = os.environ.get("ANVIL_EXPECTED_GIT_COMMIT")
    if not expected_commit and os.environ.get("GITHUB_ACTIONS") == "true":
        expected_commit = os.environ.get("GITHUB_SHA")
    if expected_commit:
        require(
            report.get("git_commit") == expected_commit,
            "CI report commit does not match checkout",
        )

    require(
        positive_integer(report.get("started_at_unix_ms")),
        "benchmark start timestamp is invalid",
    )
    require(
        nonnegative_integer(report.get("elapsed_ms")),
        "benchmark elapsed time is invalid",
    )
    require(
        report.get("work_counter_source") == WORK_COUNTER_SOURCE,
        "unexpected work-counter source",
    )
    machine = report.get("machine")
    require(isinstance(machine, dict), "machine evidence must be an object")
    require_fields(machine, MACHINE_FIELDS, "machine evidence")
    for field in MACHINE_FIELDS - {"logical_cpus"}:
        require(
            isinstance(machine.get(field), str) and bool(machine[field].strip()),
            f"machine evidence {field} is absent",
        )
    require(positive_integer(machine.get("logical_cpus")), "machine logical CPU count is invalid")

    scenarios = validate_scenarios(report.get("scenarios"), profile)
    validate_gates(report.get("gates"), profile, scenarios)


def write_step_summary(report: dict[str, Any], report_path: Path) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    with Path(summary_path).open("a", encoding="utf-8") as summary:
        summary.write("## CoreMeta performance gate\n\n")
        summary.write(f"Profile: `{report['profile']}`  \n")
        summary.write(f"Evidence: `{report_path}`  \n")
        summary.write(f"Manifest: `{report['gate_manifest_hash']}`\n\n")
        summary.write("| Scenario | p95 ms | work/op | samples |\n")
        summary.write("|---|---:|---:|---:|\n")
        for scenario in report["scenarios"]:
            summary.write(
                f"| {scenario['name']} | {scenario['p95_ms']:.3f} | "
                f"{scenario['logical_work_per_operation']:.2f} | "
                f"{scenario['sample_count']} |\n"
            )


def main() -> None:
    args = parse_args()
    require(args.report.is_file(), f"report does not exist: {args.report}")
    manifest_path = args.report.parent / "gate-manifest.json"
    require(manifest_path.is_file(), f"copied manifest does not exist: {manifest_path}")
    report = load_json(args.report.read_bytes(), "report JSON")
    manifest_bytes = manifest_path.read_bytes()
    manifest = load_json(manifest_bytes, "manifest JSON")
    require(isinstance(report, dict), "report root must be an object")
    require(isinstance(manifest, dict), "manifest root must be an object")
    validate(report, manifest, manifest_bytes, args.profile)
    write_step_summary(report, args.report)
    print(f"CoreMeta {args.profile} performance gate passed: {args.report}")


if __name__ == "__main__":
    main()
