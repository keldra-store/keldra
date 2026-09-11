#!/usr/bin/env python3

import importlib.util
import csv
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "summarize-index-v1-instrumentation.py"
SPEC = importlib.util.spec_from_file_location("index_v1_instrumentation", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
SAMPLER_SCRIPT = Path(__file__).parents[1] / "sample-index-v1-resources.py"
SAMPLER_SPEC = importlib.util.spec_from_file_location("index_v1_resource_sampler", SAMPLER_SCRIPT)
SAMPLER = importlib.util.module_from_spec(SAMPLER_SPEC)
assert SAMPLER_SPEC.loader is not None
SAMPLER_SPEC.loader.exec_module(SAMPLER)
HARNESS_SCRIPT = Path(__file__).parents[1] / "qualify-index-v1-ssd-scale.sh"


class InstrumentationTests(unittest.TestCase):
    def test_disk_sampler_emits_tsv_delimiters_instead_of_literal_escapes(self):
        source = HARNESS_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('printf "\\t%s", $field', source)
        self.assertIn('printf "\\n"', source)
        self.assertNotIn('printf "\\\\t%s", $field', source)
        self.assertNotIn('printf "\\\\n"', source)

    def test_process_sampler_captures_pid_identity_and_cumulative_fields(self):
        sample = SAMPLER.process_snapshot(os.getpid())
        self.assertGreater(sample["process_starttime_ticks"], 0)
        self.assertIn("minor_faults_total", sample)
        self.assertIn("voluntary_context_switches_total", sample)
        self.assertIn("read_syscalls_total", sample)
        changed = dict(sample, process_starttime_ticks=sample["process_starttime_ticks"] + 1)
        with self.assertRaisesRegex(RuntimeError, "reused"):
            SAMPLER.require_same_process(sample, changed, os.getpid())

    def test_process_sampler_uses_monotonic_elapsed_time(self):
        calculated = {
            "timestamp_utc",
            "interval_end_epoch_milliseconds",
            "interval_cpu_percent",
            "interval_seconds",
            "interval_kernel_write_bytes",
        }
        snapshot = {key: 0 for key in SAMPLER.PROCESS_FIELDS if key not in calculated}
        snapshot.update(cpu_ticks=0, process_starttime_ticks=7)
        current = dict(snapshot, cpu_ticks=200, write_bytes_total=10)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "process.tsv"
            with (
                mock.patch.object(
                    SAMPLER,
                    "process_snapshot",
                    side_effect=[snapshot, current, FileNotFoundError()],
                ),
                mock.patch.object(SAMPLER.time, "sleep"),
                mock.patch.object(
                    SAMPLER.time,
                    "time_ns",
                    side_effect=[100_000_000_000, 50_000_000_000],
                ),
                mock.patch.object(
                    SAMPLER.time,
                    "monotonic_ns",
                    side_effect=[1_000_000_000, 3_000_000_000, 5_000_000_000],
                ),
            ):
                SAMPLER.sample_process(1, output, 1)
            with output.open(encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source, delimiter="\t"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(float(rows[0]["interval_seconds"]), 2)
        self.assertEqual(int(rows[0]["interval_end_epoch_milliseconds"]), 100_000)

    def test_process_coverage_validates_fields_cadence_and_surfaces_resources(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="") as process:
            fields = list(MODULE.PROCESS_REQUIRED_FIELDS)
            process.write("\t".join(fields) + "\n")
            for timestamp, counter in ((900, 0), (1100, 10), (1900, 50), (2100, 60)):
                values = {
                    "interval_end_epoch_milliseconds": timestamp,
                    "interval_cpu_percent": 200,
                    "rss_kib": 100 + counter,
                    "threads": 4,
                    "mem_available_kib": 1000 - counter,
                    "interval_seconds": 0.2,
                    "process_starttime_ticks": 7,
                    "cancelled_write_bytes_total": counter,
                }
                values.update({key: counter for key in MODULE.PROCESS_COUNTERS})
                process.write("\t".join(str(values[key]) for key in fields) + "\n")
            process.flush()
            result = MODULE.coverage(1000, 2000, "process-tsv", process.name)
        self.assertTrue(result["complete"])
        self.assertEqual(
            result["measurement_window_summary"]["time_weighted_cpu_percent"], 200
        )
        self.assertEqual(
            result["measurement_window_summary"]["prorated_counter_deltas"]["write_bytes"],
            50,
        )

    def test_process_coverage_rejects_missing_fields_and_sparse_cadence(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as process:
            process.write("interval_end_epoch_milliseconds\n0\n5000\n10000\n")
            process.flush()
            result = MODULE.coverage(1000, 9000, "process-tsv", process.name)
        self.assertFalse(result["complete"])
        self.assertFalse(result["cadence_complete"])
        self.assertIn("rss_kib", result["missing_required_fields"])

    def test_host_coverage_surfaces_cpu_memory_pressure_and_vmstat(self):
        samples = []
        for timestamp, counter in ((900, 0), (1100, 10), (1900, 50), (2100, 60)):
            samples.append(
                {
                    "timestamp_unix_milliseconds": timestamp,
                    "proc_stat": ["cpu " + " ".join([str(counter)] * 8)],
                    "loadavg": "1.0 0.5 0.25 2/100 1",
                    "pressure": {
                        name: ["some avg10=1.0 avg60=2.0 avg300=3.0 total=100"]
                        for name in ("cpu", "io", "memory")
                    },
                    "meminfo": {"MemAvailable": 1000 - counter, "SwapTotal": 100, "SwapFree": 90},
                    "vmstat": {
                        "pgfault": counter,
                        "pgmajfault": counter,
                        "pswpin": counter,
                        "pswpout": counter,
                    },
                }
            )
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as host:
            for sample in samples:
                host.write(json.dumps(sample) + "\n")
            host.flush()
            result = MODULE.coverage(1000, 2000, "host-jsonl", host.name)
        self.assertTrue(result["complete"])
        summary = result["measurement_window_summary"]
        self.assertEqual(summary["cpu_percent"]["busy_excluding_iowait"], 75)
        self.assertEqual(summary["prorated_vmstat_deltas"]["pgmajfault"], 50)
        self.assertEqual(summary["sampled_pressure_maxima"]["io_some_avg10_percent"], 1)

    def test_host_coverage_rejects_missing_nested_resource_fields(self):
        samples = [
            {
                "timestamp_unix_milliseconds": timestamp,
                "proc_stat": ["cpu " + " ".join([str(counter)] * 8)],
                "loadavg": "1.0 0.5 0.25 2/100 1",
                "pressure": {},
                "meminfo": {},
                "vmstat": {},
            }
            for timestamp, counter in ((900, 0), (1500, 10), (2100, 20))
        ]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as host:
            for sample in samples:
                host.write(json.dumps(sample) + "\n")
            host.flush()
            result = MODULE.coverage(1000, 2000, "host-jsonl", host.name)
        self.assertFalse(result["complete"])
        self.assertIn("pressure.io.some", result["missing_required_fields"])
        self.assertIn("meminfo.MemAvailable", result["missing_required_fields"])

    def test_rocksdb_diagnostics_report_coverage_and_window_maxima(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "rocksdb-runtime.log"
            with output.open("w", encoding="utf-8") as target:
                for timestamp, stalled in ((900, 0), (1500, 1), (2100, 0)):
                    seconds, milliseconds = divmod(timestamp, 1000)
                    stamp = f"1970-01-01T00:00:{seconds:02d}.{milliseconds:03d}Z"
                    values = {
                        name: stalled if name == "keldra_rocksdb_write_stalled" else 1
                        for name in MODULE.ROCKSDB_REQUIRED_METRICS
                    }
                    values["keldra_rocksdb_unavailable_properties"] = 0
                    target.write(
                        stamp
                        + " DEBUG "
                        + " ".join(f"gauge.{name}={value}" for name, value in values.items())
                        + "\n"
                    )
            group_values = {
                name: 1
                for name in MODULE.GROUP_COMMIT_REQUIRED_FIELDS
                if name not in ("phase_complete", "physical_commit")
            }
            group_values.update(
                group_execute_started_epoch_milliseconds=1200,
                group_execute_ended_epoch_milliseconds=1400,
            )
            (root / "server.log").write_text(
                "1970-01-01T00:00:01.400Z INFO "
                + " ".join(f"{name}={value}" for name, value in group_values.items())
                + ' phase_complete=true physical_commit=true stop_reason="max_operations" '
                + "single-node mutation group completed\n",
                encoding="utf-8",
            )
            result = MODULE.rocksdb_metrics(1000, 2000, str(output))
        self.assertTrue(result["complete"])
        self.assertEqual(result["measurement_window_maxima"]["keldra_rocksdb_write_stalled"], 1)
        self.assertEqual(result["group_commit"]["fully_contained_group_count"], 1)
        self.assertEqual(
            result["group_commit"]["numeric_field_distributions"]["physical_slot_count"]["p99"],
            1,
        )

    def test_rocksdb_coverage_requires_in_window_expected_metrics_and_group(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "rocksdb-runtime.log"
            output.write_text(
                "1970-01-01T00:00:00.900Z DEBUG gauge.keldra_rocksdb_block_cache_capacity_bytes=1\n"
                "1970-01-01T00:00:02.100Z DEBUG "
                "gauge.keldra_rocksdb_block_cache_capacity_bytes=1\n",
                encoding="utf-8",
            )
            result = MODULE.rocksdb_metrics(1000, 2000, str(output))
        self.assertFalse(result["complete"])
        self.assertEqual(result["measurement_window_sample_count"], 0)
        self.assertIn("keldra_rocksdb_write_stalled", result["missing_required_metrics"])

    def test_pipeline_rates_interpolate_exact_window_boundaries(self):
        samples = []
        for timestamp, multiplier in ((900, 0), (1100, 20), (1900, 100), (2100, 120)):
            sample = {"timestamp_unix_milliseconds": timestamp}
            for key in MODULE.COUNTERS:
                sample[key] = multiplier
            sample.update(
                local_next_offset=multiplier,
                local_tail=multiplier + 1,
                lag_entries=1,
                lag_oldest_age_milliseconds=2,
            )
            samples.append(sample)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as output:
            for sample in samples:
                output.write(json.dumps(sample) + "\n")
            output.flush()
            result = MODULE.pipeline_metrics(1000, 2000, output.name)
        self.assertEqual(result["elapsed_seconds"], 1)
        self.assertEqual(result["indexed_physical_rows_per_second"], 100)
        self.assertEqual(result["checkpointed_source_positions_per_second"], 100)
        self.assertIsNone(result["unique_source_documents_per_second"])
        self.assertEqual(result["boundary_evidence"]["start_before"], 900)
        self.assertEqual(result["boundary_evidence"]["end_after"], 2100)

    def test_pipeline_delta_does_not_lose_large_counter_precision(self):
        base = 2**60
        samples = []
        for timestamp, increment in ((900, 0), (1100, 20), (1900, 100), (2100, 120)):
            sample = {"timestamp_unix_milliseconds": timestamp}
            for key in MODULE.COUNTERS:
                sample[key] = base + increment
            sample.update(
                local_next_offset=base + increment,
                local_tail=base + increment + 1,
                lag_entries=1,
                lag_oldest_age_milliseconds=2,
            )
            samples.append(sample)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as output:
            for sample in samples:
                output.write(json.dumps(sample) + "\n")
            output.flush()
            result = MODULE.pipeline_metrics(1000, 2000, output.name)
        self.assertEqual(result["indexed_physical_rows_per_second"], 100)

    def test_pipeline_rejects_counter_reset(self):
        samples = []
        for timestamp, value in ((900, 10), (1100, 20), (1900, 5), (2100, 30)):
            sample = {"timestamp_unix_milliseconds": timestamp}
            for key in MODULE.COUNTERS:
                sample[key] = value
            sample.update(
                local_next_offset=value,
                local_tail=value + 1,
                lag_entries=1,
                lag_oldest_age_milliseconds=2,
            )
            samples.append(sample)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as output:
            for sample in samples:
                output.write(json.dumps(sample) + "\n")
            output.flush()
            with self.assertRaisesRegex(ValueError, "regressed"):
                MODULE.pipeline_metrics(1000, 2000, output.name)

    def test_diskstats_uses_512_byte_sectors_and_exact_overlap(self):
        header = (
            "timestamp_unix_milliseconds\tmajor\tminor\tdevice\treads_completed\t"
            "reads_merged\tsectors_read\tread_milliseconds\twrites_completed\t"
            "writes_merged\tsectors_written\twrite_milliseconds\tio_in_progress\t"
            "io_milliseconds\tweighted_io_milliseconds\tdiscards_completed\t"
            "discards_merged\tsectors_discarded\tdiscard_milliseconds\t"
            "flushes_completed\tflush_milliseconds\n"
        )
        rows = (
            "900\t8\t0\tsda\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n"
            "1100\t8\t0\tsda\t20\t0\t200\t0\t10\t0\t100\t50\t0\t100\t200\t0\t0\t0\t0\t2\t0\n"
            "1900\t8\t0\tsda\t100\t0\t1000\t0\t50\t0\t500\t250\t0\t500\t1000\t0\t0\t0\t0\t10\t0\n"
            "2100\t8\t0\tsda\t120\t0\t1200\t0\t60\t0\t600\t300\t0\t600\t1200\t0\t0\t0\t0\t12\t0\n"
        )
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as output:
            output.write(header + rows)
            output.flush()
            result = MODULE.disk_metrics(1000, 2000, "sda", output.name)
        self.assertEqual(result["read_iops"], 100)
        self.assertEqual(result["write_iops"], 50)
        self.assertEqual(result["flush_iops"], 10)
        self.assertAlmostEqual(result["read_mib_per_second"], 1000 * 512 / 1048576)
        self.assertEqual(result["busy_percent"], 50)
        self.assertEqual(result["average_queue_depth"], 1)
        self.assertEqual(result["write_await_milliseconds"], 5)
        json.dumps(result)


if __name__ == "__main__":
    unittest.main()
