use std::time::Duration;

use anyhow::Result;
use hdrhistogram::Histogram;
use serde::Serialize;

const MAX_TRACKED_MICROSECONDS: u64 = 3_600_000_000;

pub(super) struct Latencies {
    histogram: Histogram<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct LatencyReport {
    pub samples: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl Latencies {
    pub(super) fn new() -> Result<Self> {
        Ok(Self {
            histogram: Histogram::new_with_bounds(1, MAX_TRACKED_MICROSECONDS, 3)?,
        })
    }

    pub(super) fn record(&mut self, duration: Duration) -> Result<()> {
        let micros = u64::try_from(duration.as_micros())?.clamp(1, MAX_TRACKED_MICROSECONDS);
        self.histogram.record(micros)?;
        Ok(())
    }

    pub(super) fn report(&self) -> LatencyReport {
        if self.histogram.is_empty() {
            return LatencyReport::default();
        }
        LatencyReport {
            samples: self.histogram.len(),
            p50_ms: self.histogram.value_at_quantile(0.50) as f64 / 1_000.0,
            p95_ms: self.histogram.value_at_quantile(0.95) as f64 / 1_000.0,
            p99_ms: self.histogram.value_at_quantile(0.99) as f64 / 1_000.0,
            max_ms: self.histogram.max() as f64 / 1_000.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_request_latency_percentiles_in_milliseconds() {
        let mut latency = Latencies::new().unwrap();
        for value in 1..=100 {
            latency.record(Duration::from_millis(value)).unwrap();
        }
        let report = latency.report();
        assert_eq!(report.samples, 100);
        assert!((49.0..=51.0).contains(&report.p50_ms));
        assert!((94.0..=96.0).contains(&report.p95_ms));
        assert!((98.0..=100.0).contains(&report.p99_ms));
    }
}
