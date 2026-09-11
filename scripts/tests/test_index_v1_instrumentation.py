#!/usr/bin/env python3

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path


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


class InstrumentationTests(unittest.TestCase):
    def test_process_sampler_captures_pid_identity_and_cumulative_fields(self):
        sample = SAMPLER.process_snapshot(os.getpid())
        self.assertGreater(sample["process_starttime_ticks"], 0)
        self.assertIn("minor_faults_total", sample)
        self.assertIn("voluntary_context_switches_total", sample)
        self.assertIn("read_syscalls_total", sample)
        changed = dict(sample, process_starttime_ticks=sample["process_starttime_ticks"] + 1)
        with self.assertRaisesRegex(RuntimeError, "reused"):
            SAMPLER.require_same_process(sample, changed, os.getpid())

    def test_process_and_host_coverage_require_both_window_brackets(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as process:
            process.write("interval_end_epoch_milliseconds\n900\n2100\n")
            process.flush()
            complete = MODULE.coverage(1000, 2000, "process-tsv", process.name)
            self.assertTrue(complete["complete"])
            missing = MODULE.coverage(800, 2000, "process-tsv", process.name)
            self.assertFalse(missing["complete"])
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as host:
            host.write('{"timestamp_unix_milliseconds":900}\n')
            host.write('{"timestamp_unix_milliseconds":2100}\n')
            host.flush()
            self.assertTrue(MODULE.coverage(1000, 2000, "host-jsonl", host.name)["complete"])

    def test_rocksdb_diagnostics_report_coverage_and_window_maxima(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as output:
            output.write("1970-01-01T00:00:00.900Z DEBUG gauge.keldra_rocksdb_write_stalled=0\n")
            output.write("1970-01-01T00:00:01.500Z DEBUG gauge.keldra_rocksdb_write_stalled=1\n")
            output.write("1970-01-01T00:00:02.100Z DEBUG gauge.keldra_rocksdb_write_stalled=0\n")
            output.flush()
            result = MODULE.rocksdb_metrics(1000, 2000, output.name)
        self.assertTrue(result["complete"])
        self.assertEqual(result["measurement_window_maxima"]["keldra_rocksdb_write_stalled"], 1)

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
