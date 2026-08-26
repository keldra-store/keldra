use anyhow::{Result, ensure};
use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::Duration;

const MAX_TRACKED_MICROSECONDS: u64 = 3_600_000_000;

pub struct Latencies {
    histogram: Histogram<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LatencyReport {
    pub samples: u64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl Latencies {
    pub fn new() -> Result<Self> {
        Ok(Self {
            histogram: Histogram::new_with_bounds(1, MAX_TRACKED_MICROSECONDS, 3)?,
        })
    }

    pub fn record(&mut self, duration: Duration) -> Result<()> {
        let micros = u64::try_from(duration.as_micros())?.clamp(1, MAX_TRACKED_MICROSECONDS);
        self.histogram.record(micros)?;
        Ok(())
    }

    pub fn report(&self) -> LatencyReport {
        if self.histogram.is_empty() {
            return LatencyReport::default();
        }
        LatencyReport {
            samples: self.histogram.len(),
            p50_ms: millis(self.histogram.value_at_quantile(0.50)),
            p95_ms: millis(self.histogram.value_at_quantile(0.95)),
            p99_ms: millis(self.histogram.value_at_quantile(0.99)),
            max_ms: millis(self.histogram.max()),
        }
    }
}

pub fn validate_open_loop(rate_per_second: u64, max_in_flight: usize) -> Result<()> {
    ensure!(rate_per_second > 0, "query rate must be non-zero");
    ensure!(
        max_in_flight > 0,
        "maximum in-flight queries must be non-zero"
    );
    Ok(())
}

fn millis(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_hdr_percentiles_in_milliseconds() {
        let mut values = Latencies::new().unwrap();
        for millis in 1..=100 {
            values.record(Duration::from_millis(millis)).unwrap();
        }
        let report = values.report();
        assert_eq!(report.samples, 100);
        assert!((49.0..=51.0).contains(&report.p50_ms));
        assert!((94.0..=96.0).contains(&report.p95_ms));
        assert!((99.0..=101.0).contains(&report.max_ms));
    }

    #[test]
    fn rejects_closed_or_unbounded_scheduler_configuration() {
        assert!(validate_open_loop(0, 1).is_err());
        assert!(validate_open_loop(1, 0).is_err());
    }
}
