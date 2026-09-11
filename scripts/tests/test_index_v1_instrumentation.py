#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "summarize-index-v1-instrumentation.py"
SPEC = importlib.util.spec_from_file_location("index_v1_instrumentation", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class InstrumentationTests(unittest.TestCase):
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
