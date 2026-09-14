use super::{
    Canary, MAX_VISIBILITY_SAMPLE_FAILURE_DETAILS, MutationReport, VisibilitySampleFailure,
    bounded_error, data, index_client, marker_query, metrics::Latencies, progress::Counters,
};
use anyhow::{Context, Result, anyhow};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::Mutex,
    task::JoinSet,
    time::{Instant, sleep_until},
};
use tonic::transport::Channel;

pub(super) struct VisibilitySampleOutcome {
    pub(super) canary: Canary,
    pub(super) definition_position: usize,
    pub(super) definition_name: String,
    pub(super) started: bool,
    pub(super) successful_receipt_to_probe_start: Duration,
    pub(super) result: std::result::Result<Duration, VisibilityProbeFailure>,
}

#[derive(Debug)]
pub(super) struct VisibilityProbeFailure {
    pub(super) kind: VisibilityFailureKind,
    pub(super) error: anyhow::Error,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum VisibilityFailureKind {
    ObservationDeadline,
    RequestTimeout,
    RequestError,
}

pub(super) struct VisibilityQueryPacer {
    interval: Duration,
    next: Mutex<Instant>,
}

impl VisibilityQueryPacer {
    pub(super) fn new(rate_per_second: u64) -> Result<Self> {
        anyhow::ensure!(
            rate_per_second > 0,
            "visibility query rate must be non-zero"
        );
        Ok(Self {
            interval: Duration::from_secs_f64(1.0 / rate_per_second as f64),
            next: Mutex::new(Instant::now()),
        })
    }

    async fn wait_turn(&self, deadline: Instant) -> bool {
        let scheduled = {
            let mut next = self.next.lock().await;
            let scheduled = (*next).max(Instant::now());
            *next = scheduled + self.interval;
            scheduled
        };
        if scheduled >= deadline {
            return false;
        }
        sleep_until(scheduled).await;
        true
    }
}

impl VisibilityFailureKind {
    fn name(self) -> &'static str {
        match self {
            Self::ObservationDeadline => "observation-deadline",
            Self::RequestTimeout => "request-timeout",
            Self::RequestError => "request-error",
        }
    }
}

pub(super) struct MutationResponses {
    pub(super) report: MutationReport,
    pub(super) visibility_tasks: JoinSet<VisibilitySampleOutcome>,
    pub(super) successful_receipt_to_probe_start: Latencies,
    pub(super) probe_start_to_visibility: Latencies,
    pub(super) successful_receipt_to_visibility: Latencies,
    pub(super) counters: Arc<Counters>,
}

impl MutationResponses {
    pub(super) async fn finish_visibility(mut self) -> Result<MutationReport> {
        while let Some(sample) = self.visibility_tasks.join_next().await {
            let sample = sample.context("visibility task panicked")?;
            self.counters.visibility_probe_completed();
            if sample.started {
                self.report.visibility_probes_started += 1;
            }
            self.successful_receipt_to_probe_start
                .record(sample.successful_receipt_to_probe_start)?;
            match sample.result {
                Ok(elapsed) => {
                    self.report.visibility_probes_succeeded += 1;
                    self.counters.visibility_probe_succeeded();
                    self.successful_receipt_to_visibility.record(elapsed)?;
                    self.probe_start_to_visibility
                        .record(elapsed.saturating_sub(sample.successful_receipt_to_probe_start))?;
                }
                Err(error) => {
                    self.report.visibility_probes_failed += 1;
                    self.counters.visibility_probe_failed();
                    match error.kind {
                        VisibilityFailureKind::ObservationDeadline => {
                            self.report.visibility_probe_observation_deadlines += 1;
                        }
                        VisibilityFailureKind::RequestTimeout => {
                            self.report.visibility_probe_request_timeouts += 1;
                        }
                        VisibilityFailureKind::RequestError => {
                            self.report.visibility_probe_request_errors += 1;
                        }
                    }
                    if self.report.visibility_probe_failures.len()
                        < MAX_VISIBILITY_SAMPLE_FAILURE_DETAILS
                    {
                        self.report
                            .visibility_probe_failures
                            .push(VisibilitySampleFailure {
                                canary_id: sample.canary.id,
                                object_version: sample.canary.version,
                                definition_position: sample.definition_position,
                                definition_name: sample.definition_name,
                                failure_kind: error.kind.name(),
                                error: bounded_error(&format!("{:#}", error.error)),
                            });
                    } else {
                        self.report.visibility_probe_failures_omitted = self
                            .report
                            .visibility_probe_failures_omitted
                            .saturating_add(1);
                    }
                }
            }
        }
        self.report.successful_receipt_to_probe_start_delay =
            self.successful_receipt_to_probe_start.report();
        self.report.probe_start_to_query_visibility_latency =
            self.probe_start_to_visibility.report();
        self.report.successful_receipt_to_query_visibility_latency =
            self.successful_receipt_to_visibility.report();
        Ok(self.report)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn wait_canary(
    channel: &Channel,
    token: &str,
    bucket: &str,
    index_name: &str,
    canary: Canary,
    poll: Duration,
    request_timeout: Duration,
    observation_timeout: Duration,
    pacer: Arc<VisibilityQueryPacer>,
    permits: Arc<tokio::sync::Semaphore>,
) -> std::result::Result<Duration, VisibilityProbeFailure> {
    let deadline = canary.completed_at + observation_timeout;
    let mut client =
        index_client(channel.clone(), token).map_err(|error| VisibilityProbeFailure {
            kind: VisibilityFailureKind::RequestError,
            error,
        })?;
    let mut last_observation = "no query response completed".to_owned();
    loop {
        let slot_remaining = deadline.saturating_duration_since(Instant::now());
        let permit = match tokio::time::timeout(slot_remaining, permits.clone().acquire_owned())
            .await
        {
            Err(_) => {
                return Err(VisibilityProbeFailure {
                    kind: VisibilityFailureKind::ObservationDeadline,
                    error: anyhow!(
                        "canary {} was not visible on {index_name} within {:?}; its next query could not acquire a concurrency slot before the deadline; last observation: {last_observation}",
                        canary.id,
                        observation_timeout,
                    ),
                });
            }
            Ok(Err(error)) => {
                return Err(VisibilityProbeFailure {
                    kind: VisibilityFailureKind::RequestError,
                    error: anyhow!("visibility query concurrency authority closed: {error}"),
                });
            }
            Ok(Ok(permit)) => permit,
        };
        // Acquire concurrency before reserving a start time. Reversing this
        // order lets paced requests accumulate behind a saturated semaphore
        // and burst together when permits are released.
        if !pacer.wait_turn(deadline).await {
            return Err(VisibilityProbeFailure {
                kind: VisibilityFailureKind::ObservationDeadline,
                error: anyhow!(
                    "canary {} was not visible on {index_name} within {:?}; aggregate visibility query rate was {} per second; last observation: {last_observation}",
                    canary.id,
                    observation_timeout,
                    1.0 / pacer.interval.as_secs_f64(),
                ),
            });
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(VisibilityProbeFailure {
                kind: VisibilityFailureKind::ObservationDeadline,
                error: anyhow!(
                    "canary {} was not visible on {index_name} within {:?}; last observation: {last_observation}",
                    canary.id,
                    observation_timeout,
                ),
            });
        }
        let rpc_timeout = remaining.min(request_timeout);
        let response = tokio::time::timeout(
            rpc_timeout,
            marker_query(&mut client, bucket, index_name, canary.id),
        )
        .await;
        drop(permit);
        let response = match response {
            Err(_) if remaining <= request_timeout => {
                return Err(VisibilityProbeFailure {
                    kind: VisibilityFailureKind::ObservationDeadline,
                    error: anyhow!(
                        "canary {} was not visible on {index_name} within {:?}; final query consumed the remaining {:?}; last observation: {last_observation}",
                        canary.id,
                        observation_timeout,
                        remaining,
                    ),
                });
            }
            Err(_) => {
                return Err(VisibilityProbeFailure {
                    kind: VisibilityFailureKind::RequestTimeout,
                    error: anyhow!(
                        "canary {} query on {index_name} exceeded its {:?} per-request timeout",
                        canary.id,
                        rpc_timeout,
                    ),
                });
            }
            Ok(Err(error)) => {
                return Err(VisibilityProbeFailure {
                    kind: VisibilityFailureKind::RequestError,
                    error: error
                        .context(format!("canary {} query on {index_name} failed", canary.id)),
                });
            }
            Ok(Ok(response)) => response,
        };
        if response.hits.iter().any(|hit| {
            hit.object_version == canary.version
                && hit
                    .address
                    .as_ref()
                    .is_some_and(|address| address.path == data::marker_path(canary.id))
        }) {
            return Ok(Instant::now().saturating_duration_since(canary.completed_at));
        }
        last_observation = format!(
            "hits={}, commit_revision={}, max_source_lag_hint={}",
            response.hits.len(),
            response
                .freshness
                .as_ref()
                .map_or(0, |freshness| freshness.commit_revision),
            response.freshness.as_ref().map_or(0, |freshness| {
                freshness
                    .sources
                    .iter()
                    .map(|source| source.lag_hint)
                    .max()
                    .unwrap_or(0)
            }),
        );
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visibility_query_pacer_requires_an_explicit_positive_rate() {
        assert!(VisibilityQueryPacer::new(0).is_err());
        let pacer = VisibilityQueryPacer::new(20).unwrap();
        assert_eq!(pacer.interval, Duration::from_millis(50));
    }
}
