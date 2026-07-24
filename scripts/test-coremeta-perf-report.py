#!/usr/bin/env python3
"""Regression tests for the machine-readable CoreMeta performance gate."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


REPO_ROOT = Path(__file__).resolve().parent.parent
CHECKER = REPO_ROOT / "scripts" / "check-coremeta-perf-report.py"
RUNNER = REPO_ROOT / "anvil-core" / "benches" / "coremeta_release_gate" / "runner.rs"
MANIFEST_PATH = REPO_ROOT / "ops" / "perf" / "coremeta-release-gate.json"
SCENARIOS = (
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
)
COMPLEXITY_GATES = (
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
)
CORRECTNESS_GATES = (
    "rocksdb_work_counters_present",
    "generation_inventory_snapshot_is_immutable",
    "deep_catch_up_cursor_progress_is_exact",
    "multi_page_generation_cursor_progress_is_exact",
    "multi_page_generation_history_shape_is_exact",
    "multi_page_generation_uses_expected_page_count",
) + tuple(f"{name}_bounded_result" for name in SCENARIOS)
LATENCY_GATES = (
    "point_get_small_p95",
    "point_get_large_p95",
    "prefix_page_early_large_p95",
    "prefix_page_deep_small_p95",
    "prefix_page_deep_large_p95",
    "bounded_list_scaled_page_large_p95",
    "durable_single_row_p95",
    "transactional_head_read_and_batch_p95",
)
WORK_COUNTERS = (
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
)


def load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("coremeta_report_checker", CHECKER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER_MODULE = load_checker()
BASE_MANIFEST = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def scenario_dimensions(profile: dict) -> dict[str, tuple[int, int, int]]:
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


def valid_report() -> dict:
    profile = copy.deepcopy(BASE_MANIFEST["profiles"]["quick"])
    scenarios = []
    facts = {}
    for name in SCENARIOS:
        dataset_rows, sample_count, expected_items = scenario_dimensions(profile)[name]
        latency_ns = [1000] * sample_count
        work = {field: 0 for field in WORK_COUNTERS}
        work["user_key_comparisons"] = sample_count * 2
        scenario = {
            "name": name,
            "dataset_rows": dataset_rows,
            "expected_items_per_operation": expected_items,
            "returned_items": sample_count * expected_items,
            "item_count_mismatches": 0,
            "sample_count": sample_count,
            "latency_ns": latency_ns,
            "p50_ms": 0.001,
            "p95_ms": 0.001,
            "p99_ms": 0.001,
            "throughput_per_second": 1_000_000.0,
            "work": work,
            "logical_work_per_operation": 2.0,
        }
        scenarios.append(scenario)
        facts[name] = {"report": scenario, "logical_work": sample_count * 2}

    gates = []
    contracts = CHECKER_MODULE.build_gate_contracts(profile, facts)
    for name in sorted(contracts):
        contract = contracts[name]
        observed = contract["observed"]
        threshold = contract["effective_threshold"]
        expectation = contract["expectation"]
        passed = (
            observed <= threshold
            if expectation == "<="
            else observed >= threshold
            if expectation == ">="
            else observed == threshold
        )
        gates.append(
            {
                "name": name,
                **contract,
                "pass": passed,
                "detail": "Synthetic passing evidence for checker regression tests.",
            }
        )

    return {
        "schema": "anvil.perf.coremeta_gate_report.v1",
        "profile": "quick",
        "pass": True,
        "git_commit": "test-commit",
        "gate_manifest_hash": "blake3:" + "0" * 64,
        "dataset_id": BASE_MANIFEST["dataset_id"],
        "seed": BASE_MANIFEST["seed"],
        "profile_spec": profile,
        "started_at_unix_ms": 1_750_000_000_000,
        "elapsed_ms": 1000,
        "work_counter_source": "RocksDB PerfContext EnableCount",
        "machine": {
            "machine_class": "test-runner",
            "operating_system": "linux",
            "architecture": "x86_64",
            "logical_cpus": 4,
            "hostname": "test-host",
        },
        "scenarios": scenarios,
        "gates": gates,
    }


def valid_manifest(report: dict) -> dict:
    manifest = copy.deepcopy(BASE_MANIFEST)
    manifest["dataset_id"] = report["dataset_id"]
    manifest["seed"] = report["seed"]
    manifest["profiles"]["quick"] = copy.deepcopy(report["profile_spec"])
    return manifest


class CoreMetaReportCheckerTests(unittest.TestCase):
    def run_checker(
        self,
        report: dict,
        manifest: dict | None = None,
        environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(prefix="anvil-coremeta-report-test-") as directory:
            path = Path(directory) / "report.json"
            report_to_write = copy.deepcopy(report)
            manifest_to_write = valid_manifest(report) if manifest is None else manifest
            manifest_bytes = json.dumps(manifest_to_write).encode()
            report_to_write["gate_manifest_sha256"] = (
                f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
            )
            path.write_text(json.dumps(report_to_write), encoding="utf-8")
            (path.parent / "gate-manifest.json").write_bytes(manifest_bytes)
            command_environment = os.environ.copy()
            command_environment.pop("GITHUB_ACTIONS", None)
            command_environment.pop("GITHUB_SHA", None)
            command_environment.pop("ANVIL_EXPECTED_GIT_COMMIT", None)
            if environment:
                command_environment.update(environment)
            return subprocess.run(
                [str(CHECKER), "--report", str(path), "--profile", "quick"],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
                env=command_environment,
            )

    def test_complete_passing_report_is_accepted(self) -> None:
        result = self.run_checker(valid_report())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_scenario_contract_matches_benchmark_runner(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        declared = set(re.findall(r'scenario\(scenarios,\s*"([^"]+)"\)', source))
        self.assertEqual(declared, set(SCENARIOS))
        self.assertEqual(CHECKER_MODULE.REQUIRED_SCENARIOS, set(SCENARIOS))

    def test_gate_contract_matches_benchmark_runner(self) -> None:
        source = RUNNER.read_text(encoding="utf-8")
        dynamic = {
            *(f"{name}_bounded_result" for name in SCENARIOS),
            *(f"{name}_p95" for name in SCENARIOS[:6]),
            *(f"{name}_bounded_work" for name in SCENARIOS[2:6]),
            *(f"{name}_bounded_work" for name in SCENARIOS[8:17]),
        }
        expected = set(COMPLEXITY_GATES) | set(CORRECTNESS_GATES) | set(LATENCY_GATES)
        self.assertEqual(CHECKER_MODULE.REQUIRED_GATES, expected)
        for name in expected - dynamic:
            self.assertIn(f'"{name}"', source)
        self.assertIn('&format!("{}_bounded_result", evidence.name)', source)
        self.assertIn('&format!("{}_p95", point.name)', source)
        self.assertIn('&format!("{}_bounded_work", page.name)', source)
        self.assertIn('&format!("{}_bounded_work", publication.name)', source)
        self.assertIn('&format!("{}_bounded_work", history_page.name)', source)
        self.assertIn("publication.expected_items_per_operation", source)
        self.assertNotIn("history_assertions.publication_small_mutations", source)
        self.assertNotIn("history_assertions.publication_large_mutations", source)

    def test_report_level_failure_is_rejected(self) -> None:
        report = valid_report()
        report["pass"] = False
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_missing_raw_samples_are_rejected(self) -> None:
        report = valid_report()
        del report["scenarios"][0]["latency_ns"]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_profile_sample_count_is_enforced(self) -> None:
        report = valid_report()
        report["scenarios"][0]["sample_count"] -= 1
        report["scenarios"][0]["latency_ns"].pop()
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("sample count does not match its profile", result.stderr)

    def test_latency_summary_must_match_raw_samples(self) -> None:
        report = valid_report()
        report["scenarios"][0]["p95_ms"] = 9.0
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("p95 does not match benchmark evidence", result.stderr)

    def test_work_summary_must_match_raw_counters(self) -> None:
        report = valid_report()
        report["scenarios"][0]["logical_work_per_operation"] = 0.0
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("logical work per operation", result.stderr)

    def test_legacy_scenario_subset_is_rejected(self) -> None:
        report = valid_report()
        report["scenarios"] = report["scenarios"][:8]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required scenarios are absent", result.stderr)

    def test_duplicate_scenario_name_is_rejected(self) -> None:
        report = valid_report()
        report["scenarios"][-1]["name"] = report["scenarios"][0]["name"]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("scenario names must be unique", result.stderr)

    def test_unexpected_scenario_is_rejected(self) -> None:
        report = valid_report()
        report["scenarios"].append(
            {**copy.deepcopy(report["scenarios"][0]), "name": "undeclared_scenario"}
        )
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected scenarios are present", result.stderr)

    def test_missing_complexity_gate_is_rejected(self) -> None:
        report = valid_report()
        missing = COMPLEXITY_GATES[0]
        report["gates"] = [gate for gate in report["gates"] if gate["name"] != missing]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required gates are absent", result.stderr)

    def test_missing_public_api_correctness_gate_is_rejected(self) -> None:
        report = valid_report()
        missing = "multi_page_generation_cursor_progress_is_exact"
        report["gates"] = [gate for gate in report["gates"] if gate["name"] != missing]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_duplicate_gate_name_is_rejected(self) -> None:
        report = valid_report()
        report["gates"][-1]["name"] = report["gates"][0]["name"]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("gate names must be unique", result.stderr)

    def test_unexpected_gate_is_rejected(self) -> None:
        report = valid_report()
        report["gates"].append(
            {**copy.deepcopy(report["gates"][0]), "name": "undeclared_gate"}
        )
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected gates are present", result.stderr)

    def test_gate_category_is_enforced(self) -> None:
        report = valid_report()
        report["gates"][0]["category"] = "latency"
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("category does not match", result.stderr)

    def test_gate_threshold_is_bound_to_manifest(self) -> None:
        report = valid_report()
        report["gates"][0]["effective_threshold"] += 1
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("effective threshold", result.stderr)

    def test_bounded_result_mismatch_is_rejected(self) -> None:
        report = valid_report()
        report["scenarios"][0]["item_count_mismatches"] = 1
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_publication_history_amplification_is_enforced(self) -> None:
        report = valid_report()
        scenario = next(
            item
            for item in report["scenarios"]
            if item["name"] == "generation_catch_up_multi_page_traversal"
        )
        scenario["expected_items_per_operation"] -= 1
        scenario["returned_items"] = (
            scenario["sample_count"] * scenario["expected_items_per_operation"]
        )
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("item bound does not match its profile", result.stderr)

    def test_manifest_profile_mismatch_is_rejected(self) -> None:
        report = valid_report()
        manifest = valid_manifest(report)
        manifest["profiles"]["quick"]["small_rows"] = 1
        result = self.run_checker(report, manifest)
        self.assertNotEqual(result.returncode, 0)

    def test_failed_individual_gate_is_rejected(self) -> None:
        report = valid_report()
        report["gates"][0]["pass"] = False
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_expected_ci_commit_is_enforced(self) -> None:
        report = valid_report()
        result = self.run_checker(
            report,
            environment={
                "GITHUB_ACTIONS": "true",
                "GITHUB_SHA": "expected-commit",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CI report commit does not match checkout", result.stderr)

    def test_unexpected_report_field_is_rejected(self) -> None:
        report = valid_report()
        report["unversioned_extension"] = True
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("report fields are unexpected", result.stderr)

    def test_duplicate_json_fields_are_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            CHECKER_MODULE.load_json(b'{"schema": "one", "schema": "two"}', "test")

    def test_nonfinite_json_numbers_are_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            CHECKER_MODULE.load_json(b'{"elapsed_ms": NaN}', "test")


if __name__ == "__main__":
    unittest.main()
