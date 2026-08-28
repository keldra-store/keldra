//! Black-box qualification for query latency under sustained index mutation.
//!
//! This executable uses only authenticated public Keldra APIs. It deliberately
//! creates multiple definitions over one bucket so every definition consumes
//! the same continuously mutating source journal.

#[path = "index_contention_qualification/config.rs"]
mod config;
#[path = "index_contention_qualification/data.rs"]
mod data;
#[path = "index_contention_qualification/metrics.rs"]
mod metrics;
#[path = "index_contention_qualification/progress.rs"]
mod progress;

use anyhow::{Context, Result, anyhow, bail, ensure};
use config::Config;
use data::CONTENT_TYPE;
use keldra_storage::v1::bulk_operation::Operation as BulkOperationValue;
use keldra_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use keldra_storage::v1::index_query::Query as QueryValue;
use keldra_storage::v1::index_service_client::IndexServiceClient;
use keldra_storage::v1::object_head::State as ObjectHeadState;
use keldra_storage::v1::{
    BulkOperation, BulkPutRequest, BulkWriteRequest, CreateBucketRequest, CreateIndexRequest,
    Durability, HeadObjectRequest, IndexPredicate, IndexPredicateExpression,
    IndexPredicateOperator, IndexQuery, IndexSourceFreshness, ObjectAddress, ObjectVersioning,
    QueryIndexRequest, QueryIndexResponse, TypedJsonIndexQuery,
};
use keldra_storage::{
    BearerToken, KeywordField, RawClient, TypedJsonIndexBuilder, UnsignedIntegerField,
    administration_client, connect_channel, exchange_client_credentials, object_client,
};
use metrics::{Latencies, LatencyReport};
use progress::Counters;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    started_unix_milliseconds: u128,
    completed_unix_milliseconds: u128,
    result: &'static str,
    configuration: config::PublicConfig,
    corpus_sha256: String,
    index_definition_ids: Vec<u64>,
    observed_source_node_ids: Vec<u64>,
    assignment_observability: &'static str,
    responsiveness_definition: &'static str,
    baseline: QueryPhaseReport,
    concurrent: QueryPhaseReport,
    mutations: MutationReport,
    drain_seconds: f64,
    post: QueryPhaseReport,
    correctness: CorrectnessReport,
    workload_validity: WorkloadValidityReport,
    responsiveness: ResponsivenessReport,
}

#[derive(Debug, Serialize)]
struct TerminalFailureReport {
    schema: &'static str,
    started_unix_milliseconds: u128,
    completed_unix_milliseconds: u128,
    result: &'static str,
    configuration: config::PublicConfig,
    failure: TerminalFailure,
}

#[derive(Debug, Serialize)]
struct TerminalFailure {
    stage: &'static str,
    error: String,
}

#[derive(Debug, Default, Serialize)]
struct QueryPhaseReport {
    scheduling_window_seconds: f64,
    completion_elapsed_seconds: f64,
    offered_schedules_per_second: f64,
    completed_queries_per_second: f64,
    offered_schedules: u64,
    completed: u64,
    dropped_schedules: u64,
    request_errors: u64,
    timeouts: u64,
    correctness_errors: u64,
    schedule_to_response: LatencyReport,
    dispatch_to_response: LatencyReport,
    scheduling_lateness: LatencyReport,
    minimum_commit_revision: Option<u64>,
    maximum_commit_revision: Option<u64>,
    maximum_source_lag_hint: u64,
    offered_definition_count: usize,
    queried_definition_count: usize,
}

#[derive(Debug, Default, Serialize)]
struct MutationReport {
    load_mode: &'static str,
    configured_operations_per_second: Option<f64>,
    elapsed_seconds: f64,
    offered_batches: u64,
    submitted_batches: u64,
    dropped_batches: u64,
    offered_operations: u64,
    accepted_batches: u64,
    accepted_operations: u64,
    accepted_bytes: u64,
    accepted_operations_per_second: f64,
    accepted_bytes_per_second: f64,
    request_errors: u64,
    queue_capacity: usize,
    minimum_sampled_queue_depth_while_producing: usize,
    queue_depth_samples: u64,
    nonempty_queue_sample_ratio: f64,
    queue_starvation_samples: u64,
    request_latency: LatencyReport,
    visibility_samples_requested: u64,
    visibility_samples_completed: u64,
    visibility_samples_skipped_busy: u64,
    visibility_sample_errors: u64,
    visibility_sample_failures: Vec<VisibilitySampleFailure>,
    visibility_sample_failures_omitted: u64,
    publication_visibility_lag: LatencyReport,
    visibility_definition: &'static str,
}

#[derive(Debug, Serialize)]
struct CorrectnessReport {
    passed: bool,
    stable_oracle_checked_on_every_completed_query: bool,
    final_canary_version_observed_by_every_definition: bool,
    exact_mutable_versions_verified_by_every_definition: bool,
    final_freshness_healthy_by_every_definition: bool,
    advisory_zero_lag_verified_by_every_definition: Option<bool>,
    zero_query_correctness_errors: bool,
}

#[derive(Debug, Serialize)]
struct WorkloadValidityReport {
    passed: bool,
    all_definitions_offered_in_every_phase: bool,
    sustained_nonempty_mutation_queue: bool,
    mutation_load_shape_valid: bool,
    mutation_requests_complete_and_successful: bool,
}

#[derive(Debug, Serialize)]
struct ResponsivenessReport {
    passed: bool,
    zero_query_request_errors_or_timeouts: bool,
    zero_dropped_schedules: bool,
    concurrent_query_p99_within_configured_limit: bool,
    publication_visibility_samples_complete: bool,
    publication_visibility_p99_within_configured_limit: bool,
}

#[derive(Debug, Serialize)]
struct VisibilitySampleFailure {
    canary_id: u64,
    object_version: u64,
    definition_position: usize,
    definition_name: String,
    error: String,
}

struct VisibilitySampleOutcome {
    canary: Canary,
    definition_position: usize,
    definition_name: String,
    result: Result<Duration>,
}

const MAX_VISIBILITY_SAMPLE_FAILURE_DETAILS: usize = 16;
const MAX_VISIBILITY_SAMPLE_ERROR_CHARS: usize = 512;

#[derive(Clone, Copy)]
struct MutationJob {
    sequence: u64,
}

struct MutationResult {
    operations: u64,
    bytes: u64,
    elapsed: Duration,
    canary: Option<Canary>,
}

#[derive(Default)]
struct MutationProducerReport {
    offered_batches: u64,
    submitted_batches: u64,
    dropped_batches: u64,
}

#[derive(Clone, Copy)]
struct Canary {
    id: u64,
    version: u64,
    accepted_at: Instant,
}

#[derive(Default)]
struct QueryOutcome {
    definition_position: usize,
    revision: u64,
    max_lag: u64,
    service: Duration,
    correctness_error: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let config = Arc::new(Config::from_env()?);
    let started_unix_milliseconds = unix_millis()?;
    match run_qualification(config.clone(), started_unix_milliseconds).await {
        Ok(report) => {
            write_report(config.output.as_deref(), &report)?;
            ensure!(
                report.result == "pass",
                "contention qualification failed; inspect JSON evidence"
            );
            Ok(())
        }
        Err(error) => {
            let report = TerminalFailureReport {
                schema: "keldra.index-contention-terminal-failure.v1",
                started_unix_milliseconds,
                completed_unix_milliseconds: unix_millis()?,
                result: "fail",
                configuration: config.public(),
                failure: TerminalFailure {
                    stage: "qualification_execution",
                    error: bounded_error(&format!("{error:#}")),
                },
            };
            write_report(config.output.as_deref(), &report)?;
            Err(error)
        }
    }
}

async fn run_qualification(config: Arc<Config>, started_unix_milliseconds: u128) -> Result<Report> {
    let corpus_sha256 =
        data::corpus_digest(config.seed, config.stable_records, config.mutable_records);
    let setup_channels = connect_all(&config.endpoints).await?;
    let token = exchange_client_credentials(
        setup_channels[0].clone(),
        config.client_id.clone(),
        config.client_secret.clone(),
    )
    .await?
    .access_token;
    setup(&config, &setup_channels[0], &token).await?;

    let definitions = create_definitions(&config, &setup_channels[0], &token).await?;
    let names = Arc::new(
        (0..config.definition_count)
            .map(data::index_name)
            .collect::<Vec<_>>(),
    );
    let expected = Arc::new(
        (0..config.stable_records)
            .map(data::stable_path)
            .collect::<BTreeSet<_>>(),
    );
    wait_all_ready(&config, &names, &expected, &token).await?;

    // Query and mutation transports are separately established so client-side
    // HTTP/2 flow control cannot manufacture server contention evidence.
    let query_channels = Arc::new(connect_all(&config.endpoints).await?);
    let mutation_channels = Arc::new(connect_all(&config.endpoints).await?);
    let visibility_channels = Arc::new(connect_all(&config.endpoints).await?);
    let counters = Counters::new().await?;
    let (stop_tx, stop_rx) = watch::channel(false);
    let progress_task = progress::start(config.progress_jsonl.clone(), counters.clone(), stop_rx);

    counters.phase("baseline").await;
    let baseline = run_query_phase(
        &config,
        &names,
        &expected,
        &query_channels,
        &token,
        config.baseline,
        counters.clone(),
    )
    .await?;
    counters.phase("concurrent").await;
    let mutation_task = tokio::spawn(run_mutations(
        config.clone(),
        mutation_channels,
        visibility_channels,
        token.clone(),
        counters.clone(),
    ));
    let concurrent = run_query_phase(
        &config,
        &names,
        &expected,
        &query_channels,
        &token,
        config.concurrent,
        counters.clone(),
    )
    .await?;
    counters.phase("drain").await;
    let drain_started = Instant::now();
    let mutations = tokio::time::timeout(config.drain_timeout, mutation_task)
        .await
        .context("mutation drain exceeded timeout")???;
    let final_canary = mutations.1;
    let mutation_report = mutations.0;
    let (final_visible, mut observed) = if let Some(canary) = final_canary {
        tokio::time::timeout(
            config.drain_timeout,
            wait_canary_on_all(&config, &names, &query_channels, &token, canary),
        )
        .await
        .context("final canary verification exceeded drain timeout")??
    } else {
        (false, BTreeSet::new())
    };
    let (final_state_verified, advisory_zero_lag, final_sources) = tokio::time::timeout(
        config.drain_timeout,
        verify_final_mutable_state(&config, &names, &query_channels, &token),
    )
    .await
    .context("final mutable verification exceeded drain timeout")??;
    observed.extend(final_sources);
    let drain_seconds = drain_started.elapsed().as_secs_f64();
    counters.phase("post").await;
    let post = run_query_phase(
        &config,
        &names,
        &expected,
        &query_channels,
        &token,
        config.post,
        counters.clone(),
    )
    .await?;
    counters.phase("complete").await;
    let _ = stop_tx.send(true);
    progress_task.await.context("progress task panicked")??;

    let zero_query_request_errors_or_timeouts = [&baseline, &concurrent, &post]
        .iter()
        .all(|p| p.request_errors + p.timeouts == 0);
    let zero_query_correctness_errors = [&baseline, &concurrent, &post]
        .iter()
        .all(|p| p.correctness_errors == 0);
    let zero_dropped_schedules = [&baseline, &concurrent, &post]
        .iter()
        .all(|p| p.dropped_schedules == 0);
    let mutation_requests_complete_and_successful = mutation_report.request_errors == 0
        && mutation_report.accepted_batches > 0
        && mutation_report.accepted_batches + mutation_report.request_errors
            == mutation_report.submitted_batches
        && mutation_report.accepted_operations > 0;
    let publication_visibility_samples_complete = mutation_report.visibility_samples_requested > 0
        && mutation_report.visibility_samples_completed
            == mutation_report.visibility_samples_requested
        && mutation_report.visibility_sample_errors == 0;
    let concurrent_p99_passed = config
        .max_concurrent_query_p99_ms
        .is_none_or(|maximum| concurrent.schedule_to_response.p99_ms <= maximum);
    let publication_visibility_p99_passed = config
        .max_publication_visibility_p99_ms
        .is_none_or(|maximum| mutation_report.publication_visibility_lag.p99_ms <= maximum);
    let all_definitions_offered = [&baseline, &concurrent, &post]
        .iter()
        .all(|phase| phase.offered_definition_count == config.definition_count);
    let sustained_nonempty_mutation_queue = mutation_report.queue_depth_samples > 0
        && mutation_report.minimum_sampled_queue_depth_while_producing > 0
        && mutation_report.queue_starvation_samples == 0;
    let mutation_load_shape_valid = if config.mutation_rate_operations_per_second.is_some() {
        mutation_report.dropped_batches == 0
            && mutation_report.offered_batches == mutation_report.submitted_batches
    } else {
        sustained_nonempty_mutation_queue
    };
    let correctness_passed = zero_query_correctness_errors && final_visible && final_state_verified;
    let workload_passed = all_definitions_offered
        && mutation_load_shape_valid
        && mutation_requests_complete_and_successful;
    let responsiveness_passed = zero_query_request_errors_or_timeouts
        && zero_dropped_schedules
        && concurrent_p99_passed
        && publication_visibility_samples_complete
        && publication_visibility_p99_passed;
    let report = Report {
        schema: "keldra.index-contention-qualification.v1",
        started_unix_milliseconds,
        completed_unix_milliseconds: unix_millis()?,
        result: if correctness_passed && workload_passed && responsiveness_passed {
            "pass"
        } else {
            "fail"
        },
        configuration: config.public(),
        corpus_sha256,
        index_definition_ids: definitions,
        observed_source_node_ids: observed.into_iter().collect(),
        assignment_observability: "public APIs expose source node IDs and placement epochs, but not builder-to-node assignments; definition_count is cluster-wide work and is not labeled per-node concurrency",
        responsiveness_definition: "every offered open-loop schedule completes within request_timeout with no scheduler drop, request error, timeout, or oracle mismatch; visibility probes use request_timeout per query and a separate observation timeout; optional concurrent-query and publication-visibility p99 gates are applied when configured",
        baseline,
        concurrent,
        mutations: mutation_report,
        drain_seconds,
        post,
        correctness: CorrectnessReport {
            passed: correctness_passed,
            stable_oracle_checked_on_every_completed_query: true,
            final_canary_version_observed_by_every_definition: final_visible,
            exact_mutable_versions_verified_by_every_definition: final_state_verified,
            final_freshness_healthy_by_every_definition: final_state_verified,
            advisory_zero_lag_verified_by_every_definition: advisory_zero_lag,
            zero_query_correctness_errors,
        },
        workload_validity: WorkloadValidityReport {
            passed: workload_passed,
            all_definitions_offered_in_every_phase: all_definitions_offered,
            sustained_nonempty_mutation_queue,
            mutation_load_shape_valid,
            mutation_requests_complete_and_successful,
        },
        responsiveness: ResponsivenessReport {
            passed: responsiveness_passed,
            zero_query_request_errors_or_timeouts,
            zero_dropped_schedules,
            concurrent_query_p99_within_configured_limit: concurrent_p99_passed,
            publication_visibility_samples_complete,
            publication_visibility_p99_within_configured_limit: publication_visibility_p99_passed,
        },
    };
    Ok(report)
}

fn write_report(path: Option<&std::path::Path>, report: &impl Serialize) -> Result<()> {
    let encoded = serde_json::to_vec_pretty(report)?;
    if let Some(path) = path {
        std::fs::write(path, &encoded).with_context(|| format!("write {}", path.display()))?;
    }
    println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    Ok(())
}

async fn setup(config: &Config, channel: &Channel, token: &str) -> Result<()> {
    let mut admin = administration_client(channel.clone(), token)?;
    admin
        .create_bucket(CreateBucketRequest {
            bucket: config.bucket.clone(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await
        .context("create contention bucket")?;
    let mut client = object_client(channel.clone(), token)?;
    let mut operations = Vec::new();
    for id in 0..config.stable_records {
        operations.push(put(
            config,
            data::stable_path(id),
            data::payload(config.seed, id, "stable", 0),
            format!("contention-initial-stable-{id}"),
        ));
    }
    for id in 0..config.mutable_records {
        operations.push(put(
            config,
            data::mutable_path(id),
            data::payload(config.seed, id, "mutable", 0),
            format!("contention-initial-mutable-{id}"),
        ));
    }
    for (batch, chunk) in operations.chunks(1_000).enumerate() {
        let outcomes = client
            .bulk_write(BulkWriteRequest {
                operations: chunk.to_vec(),
            })
            .await
            .with_context(|| format!("initial bulk batch {batch}"))?
            .into_inner()
            .outcomes;
        ensure!(outcomes.len() == chunk.len());
        for outcome in outcomes {
            ensure!(
                matches!(outcome.outcome, Some(BulkOutcomeValue::Receipt(_))),
                "initial write failed"
            );
        }
    }
    Ok(())
}

async fn create_definitions(config: &Config, channel: &Channel, token: &str) -> Result<Vec<u64>> {
    let mut client = index_client(channel.clone(), token)?;
    let mut ids = Vec::with_capacity(config.definition_count);
    for position in 0..config.definition_count {
        let name = data::index_name(position);
        let request: CreateIndexRequest = TypedJsonIndexBuilder::new(&config.bucket, &name)
            .path_prefix("contention/")
            .content_type(CONTENT_TYPE)
            .field(UnsignedIntegerField::single("record_id", "/record_id").exact())
            .field(KeywordField::single("class", "/class").exact())
            .field(UnsignedIntegerField::single("generation", "/generation").exact())
            .finish(format!("contention-create-{position}"))?;
        let definition = client
            .create_index(request)
            .await
            .with_context(|| format!("create index {name}"))?
            .into_inner();
        ensure!(definition.index_id != 0);
        ids.push(definition.index_id);
    }
    Ok(ids)
}

async fn wait_all_ready(
    config: &Config,
    names: &[String],
    expected: &BTreeSet<String>,
    token: &str,
) -> Result<()> {
    let deadline = Instant::now() + config.drain_timeout;
    for (position, name) in names.iter().enumerate() {
        let endpoint = &config.endpoints[position % config.endpoints.len()];
        let channel = connect_channel(endpoint)
            .await
            .map_err(|error| anyhow!("connect to {endpoint}: {error}"))?;
        let mut client = index_client(channel, token)?;
        loop {
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .min(config.request_timeout);
            if let Ok(Ok(response)) =
                tokio::time::timeout(remaining, stable_query(&mut client, &config.bucket, name))
                    .await
                && validate_stable(&response, expected).is_ok()
                && response
                    .freshness
                    .as_ref()
                    .is_some_and(|f| f.initial_build_complete && !f.rebuilding)
            {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "index {name} did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

async fn run_query_phase(
    config: &Config,
    names: &Arc<Vec<String>>,
    expected: &Arc<BTreeSet<String>>,
    channels: &Arc<Vec<Channel>>,
    token: &str,
    duration: Duration,
    counters: Arc<Counters>,
) -> Result<QueryPhaseReport> {
    let mut report = QueryPhaseReport::default();
    let mut end_to_end = Latencies::new()?;
    let mut service = Latencies::new()?;
    let mut lateness = Latencies::new()?;
    let permits = Arc::new(Semaphore::new(config.query_max_in_flight));
    let phase_start = Instant::now();
    let phase_end = phase_start + duration;
    let period_nanos = 1_000_000_000u64 / config.query_rate;
    ensure!(period_nanos > 0, "query rate exceeds scheduler resolution");
    let period = Duration::from_nanos(period_nanos);
    let mut tasks = JoinSet::new();
    let mut offered_definitions = BTreeSet::new();
    let mut queried_definitions = BTreeSet::new();
    let mut sequence = 0u32;
    loop {
        let intended = phase_start + period.saturating_mul(sequence);
        if intended >= phase_end {
            break;
        }
        tokio::time::sleep_until(intended).await;
        let dispatched = Instant::now();
        report.offered_schedules += 1;
        offered_definitions.insert(sequence as usize % names.len());
        counters.scheduled.fetch_add(1, Ordering::Relaxed);
        lateness.record(dispatched.saturating_duration_since(intended))?;
        if dispatched.saturating_duration_since(intended) >= period {
            report.dropped_schedules += 1;
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            sequence = sequence.checked_add(1).context("query schedule overflow")?;
            continue;
        }
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            report.dropped_schedules += 1;
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            sequence = sequence.checked_add(1).context("query schedule overflow")?;
            continue;
        };
        let definition_position = sequence as usize % names.len();
        let channel = channels[sequence as usize % channels.len()].clone();
        let name = names[definition_position].clone();
        let bucket = config.bucket.clone();
        let token = token.to_owned();
        let expected = expected.clone();
        let timeout = config.request_timeout;
        tasks.spawn(async move {
            let _permit = permit;
            let service_started = Instant::now();
            let result = tokio::time::timeout(timeout, async {
                let mut client = index_client(channel, &token)?;
                let response = stable_query(&mut client, &bucket, &name).await?;
                let correctness_error = validate_stable(&response, &expected).is_err();
                let freshness = response.freshness.context("query omitted freshness")?;
                Ok::<_, anyhow::Error>(QueryOutcome {
                    definition_position,
                    revision: freshness.commit_revision,
                    max_lag: freshness
                        .sources
                        .iter()
                        .map(|source| source.lag_hint)
                        .max()
                        .unwrap_or(0),
                    service: service_started.elapsed(),
                    correctness_error,
                })
            })
            .await;
            (intended, Instant::now(), result)
        });
        sequence = sequence.checked_add(1).context("query schedule overflow")?;
    }
    while let Some(joined) = tasks.join_next().await {
        let (intended, completed, result) = joined.context("query task panicked")?;
        match result {
            Err(_) => {
                report.timeouts += 1;
                counters.timeouts.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(_)) => {
                report.request_errors += 1;
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Ok(outcome)) => {
                report.completed += 1;
                queried_definitions.insert(outcome.definition_position);
                if outcome.correctness_error {
                    report.correctness_errors += 1;
                }
                report.minimum_commit_revision = Some(
                    report
                        .minimum_commit_revision
                        .map_or(outcome.revision, |v| v.min(outcome.revision)),
                );
                report.maximum_commit_revision = Some(
                    report
                        .maximum_commit_revision
                        .map_or(outcome.revision, |v| v.max(outcome.revision)),
                );
                report.maximum_source_lag_hint =
                    report.maximum_source_lag_hint.max(outcome.max_lag);
                end_to_end.record(completed.saturating_duration_since(intended))?;
                service.record(outcome.service)?;
                counters
                    .query_completed(
                        completed.saturating_duration_since(intended),
                        outcome.revision,
                        outcome.max_lag,
                    )
                    .await;
            }
        }
    }
    report.scheduling_window_seconds = duration.as_secs_f64();
    report.completion_elapsed_seconds = phase_start.elapsed().as_secs_f64();
    report.offered_schedules_per_second = report.offered_schedules as f64 / duration.as_secs_f64();
    report.completed_queries_per_second =
        report.completed as f64 / report.completion_elapsed_seconds;
    report.schedule_to_response = end_to_end.report();
    report.dispatch_to_response = service.report();
    report.scheduling_lateness = lateness.report();
    report.offered_definition_count = offered_definitions.len();
    report.queried_definition_count = queried_definitions.len();
    Ok(report)
}

async fn run_mutations(
    config: Arc<Config>,
    channels: Arc<Vec<Channel>>,
    visibility_channels: Arc<Vec<Channel>>,
    token: String,
    counters: Arc<Counters>,
) -> Result<(MutationReport, Option<Canary>)> {
    let mutation_started = Instant::now();
    let (job_tx, job_rx) = mpsc::channel(config.mutation_queue_depth);
    let sampler_tx = job_tx.clone();
    let receiver = Arc::new(Mutex::new(job_rx));
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let producing = Arc::new(AtomicUsize::new(1));
    let minimum_depth = Arc::new(AtomicUsize::new(config.mutation_queue_depth));
    let starvation = Arc::new(AtomicU64::new(0));
    let queue_samples = Arc::new(AtomicU64::new(0));
    let prefilled_batches = if config.mutation_rate_operations_per_second.is_none() {
        config.mutation_queue_depth as u64
    } else {
        0
    };
    for sequence in 0..prefilled_batches {
        job_tx
            .send(MutationJob { sequence })
            .await
            .context("prefill mutation queue")?;
    }
    let producer_config = config.clone();
    let producing_for_task = producing.clone();
    let producer = tokio::spawn(async move {
        let report = produce_mutation_jobs(&producer_config, job_tx, prefilled_batches).await;
        producing_for_task.store(0, Ordering::Release);
        report
    });
    tokio::task::yield_now().await;
    let sample_config = config.clone();
    let sample_producing = producing.clone();
    let sample_minimum = minimum_depth.clone();
    let sample_starvation = starvation.clone();
    let sampled = queue_samples.clone();
    let sampler = tokio::spawn(async move {
        while sample_producing.load(Ordering::Acquire) != 0 {
            let depth = sample_config
                .mutation_queue_depth
                .saturating_sub(sampler_tx.capacity());
            sample_minimum.fetch_min(depth, Ordering::Relaxed);
            sampled.fetch_add(1, Ordering::Relaxed);
            if depth == 0 {
                sample_starvation.fetch_add(1, Ordering::Relaxed);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let mut workers = JoinSet::new();
    for worker in 0..config.mutation_workers {
        let receiver = receiver.clone();
        let result_tx = result_tx.clone();
        let config = config.clone();
        let channel = channels[worker % channels.len()].clone();
        let token = token.clone();
        workers.spawn(async move {
            let mut client = object_client(channel, &token)?;
            loop {
                let Some(job) = receiver.lock().await.recv().await else {
                    break;
                };
                match execute_mutation(&config, &mut client, job).await {
                    Ok(result) => {
                        let _ = result_tx.send(Ok(result));
                    }
                    Err(error) => {
                        let _ = result_tx.send(Err(error));
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    drop(result_tx);
    let mut report = MutationReport {
        load_mode: if config.mutation_rate_operations_per_second.is_some() {
            "fixed-rate"
        } else {
            "saturated-queue"
        },
        configured_operations_per_second: config.mutation_rate_operations_per_second,
        queue_capacity: config.mutation_queue_depth,
        ..MutationReport::default()
    };
    let mut request_latency = Latencies::new()?;
    let mut visibility_latency = Latencies::new()?;
    let mut visibility_tasks = JoinSet::new();
    // Exactly one acceptance-to-visible probe may run at a time. This keeps
    // visibility evidence bounded and independent of definition count and
    // mutation throughput, so it cannot become the load under test.
    let visibility_permits = Arc::new(Semaphore::new(1));
    let mut visibility_sample_ordinal = 0_u64;
    let mut final_canary: Option<Canary> = None;
    while let Some(event) = result_rx.recv().await {
        match event {
            Err(_) => {
                report.request_errors += 1;
                counters.mutation_errors.fetch_add(1, Ordering::Relaxed);
            }
            Ok(result) => {
                report.accepted_batches += 1;
                report.accepted_operations += result.operations;
                report.accepted_bytes += result.bytes;
                counters
                    .mutations
                    .fetch_add(result.operations, Ordering::Relaxed);
                request_latency.record(result.elapsed)?;
                if let Some(canary) = result.canary {
                    if final_canary.is_none_or(|current| canary.id > current.id) {
                        final_canary = Some(canary);
                    }
                    if canary.id % config.visibility_sample_every_batches == 0 {
                        if let Ok(permit) = visibility_permits.clone().try_acquire_owned() {
                            report.visibility_samples_requested += 1;
                            let definition_position = visibility_definition_position(
                                visibility_sample_ordinal,
                                config.definition_count,
                            );
                            visibility_sample_ordinal = visibility_sample_ordinal.saturating_add(1);
                            let channel = visibility_channels
                                [definition_position % visibility_channels.len()]
                            .clone();
                            let name = data::index_name(definition_position);
                            let bucket = config.bucket.clone();
                            let token = token.clone();
                            let poll = config.visibility_poll;
                            let request_timeout = config.request_timeout;
                            let observation_timeout = config.visibility_observation_timeout;
                            visibility_tasks.spawn(async move {
                                let _permit = permit;
                                let result = wait_canary(
                                    &channel,
                                    &token,
                                    &bucket,
                                    &name,
                                    canary,
                                    poll,
                                    request_timeout,
                                    observation_timeout,
                                )
                                .await;
                                VisibilitySampleOutcome {
                                    canary,
                                    definition_position,
                                    definition_name: name,
                                    result,
                                }
                            });
                        } else {
                            report.visibility_samples_skipped_busy += 1;
                        }
                    }
                }
            }
        }
    }
    // The mutation workload ends when every submitted response has arrived.
    // Visibility probes measure indexing lag and must not dilute ingest rate.
    let mutation_elapsed = mutation_started.elapsed();
    let producer_report = producer.await.context("mutation producer panicked")??;
    report.offered_batches = producer_report.offered_batches;
    report.submitted_batches = producer_report.submitted_batches;
    report.dropped_batches = producer_report.dropped_batches;
    report.offered_operations = producer_report
        .offered_batches
        .saturating_mul((config.mutation_batch_size + 1) as u64);
    while let Some(worker) = workers.join_next().await {
        worker.context("mutation worker panicked")??;
    }
    sampler.await.context("queue sampler panicked")?;
    while let Some(sample) = visibility_tasks.join_next().await {
        let sample = sample.context("visibility task panicked")?;
        match sample.result {
            Ok(duration) => {
                report.visibility_samples_completed += 1;
                visibility_latency.record(duration)?;
            }
            Err(error) => {
                report.visibility_sample_errors += 1;
                if report.visibility_sample_failures.len() < MAX_VISIBILITY_SAMPLE_FAILURE_DETAILS {
                    report
                        .visibility_sample_failures
                        .push(VisibilitySampleFailure {
                            canary_id: sample.canary.id,
                            object_version: sample.canary.version,
                            definition_position: sample.definition_position,
                            definition_name: sample.definition_name,
                            error: bounded_error(&format!("{error:#}")),
                        });
                } else {
                    report.visibility_sample_failures_omitted =
                        report.visibility_sample_failures_omitted.saturating_add(1);
                }
            }
        }
    }
    report.minimum_sampled_queue_depth_while_producing = minimum_depth.load(Ordering::Relaxed);
    report.queue_starvation_samples = starvation.load(Ordering::Relaxed);
    report.queue_depth_samples = queue_samples.load(Ordering::Relaxed);
    report.nonempty_queue_sample_ratio = if report.queue_depth_samples == 0 {
        0.0
    } else {
        (report.queue_depth_samples - report.queue_starvation_samples) as f64
            / report.queue_depth_samples as f64
    };
    report.request_latency = request_latency.report();
    report.elapsed_seconds = mutation_elapsed.as_secs_f64();
    report.accepted_operations_per_second =
        report.accepted_operations as f64 / report.elapsed_seconds;
    report.accepted_bytes_per_second = report.accepted_bytes as f64 / report.elapsed_seconds;
    report.publication_visibility_lag = visibility_latency.report();
    report.visibility_definition = "publication_visibility_lag: receipt acceptance to first ordinary query hit with the exact canary object_version; samples rotate by sample ordinal across definitions, use a separate total observation timeout, and include polling-resolution delay";
    Ok((report, final_canary))
}

async fn produce_mutation_jobs(
    config: &Config,
    job_tx: mpsc::Sender<MutationJob>,
    prefilled_batches: u64,
) -> Result<MutationProducerReport> {
    let started = Instant::now();
    let deadline = started + config.concurrent;
    let mut report = MutationProducerReport {
        offered_batches: prefilled_batches,
        submitted_batches: prefilled_batches,
        dropped_batches: 0,
    };
    let mut sequence = prefilled_batches;
    if let Some(operation_rate) = config.mutation_rate_operations_per_second {
        return produce_fixed_rate_jobs(
            config.concurrent,
            config.mutation_batch_size,
            operation_rate,
            job_tx,
        )
        .await;
    } else {
        while Instant::now() < deadline {
            if job_tx.send(MutationJob { sequence }).await.is_err() {
                break;
            }
            report.offered_batches = report.offered_batches.saturating_add(1);
            report.submitted_batches = report.submitted_batches.saturating_add(1);
            sequence = sequence
                .checked_add(1)
                .context("mutation sequence overflow")?;
        }
    }
    Ok(report)
}

async fn produce_fixed_rate_jobs(
    duration: Duration,
    mutation_batch_size: usize,
    operation_rate: f64,
    job_tx: mpsc::Sender<MutationJob>,
) -> Result<MutationProducerReport> {
    let started = Instant::now();
    let deadline = started + duration;
    let batch_rate = operation_rate / (mutation_batch_size + 1) as f64;
    let schedule_interval = Duration::from_secs_f64(1.0 / batch_rate);
    let mut report = MutationProducerReport::default();
    let mut schedule_ordinal = 1_u64;
    loop {
        let scheduled = started + Duration::from_secs_f64(schedule_ordinal as f64 / batch_rate);
        if scheduled >= deadline {
            break;
        }
        tokio::time::sleep_until(scheduled).await;
        report.offered_batches = report.offered_batches.saturating_add(1);
        // A delayed producer records missed open-loop schedules instead of
        // manufacturing a catch-up burst that changes the offered workload.
        if Instant::now() >= scheduled + schedule_interval {
            report.dropped_batches = report.dropped_batches.saturating_add(1);
        } else {
            match job_tx.try_send(MutationJob {
                sequence: schedule_ordinal - 1,
            }) {
                Ok(()) => report.submitted_batches = report.submitted_batches.saturating_add(1),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    report.dropped_batches = report.dropped_batches.saturating_add(1)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    bail!("mutation worker queue closed during fixed-rate production")
                }
            }
        }
        schedule_ordinal = schedule_ordinal
            .checked_add(1)
            .context("mutation schedule overflow")?;
    }
    Ok(report)
}

async fn execute_mutation(
    config: &Config,
    client: &mut RawClient,
    job: MutationJob,
) -> Result<MutationResult> {
    let started = Instant::now();
    let mut operations = Vec::with_capacity(config.mutation_batch_size + 1);
    let mut bytes = 0u64;
    for offset in 0..config.mutation_batch_size {
        let ordinal = job
            .sequence
            .saturating_mul(config.mutation_batch_size as u64)
            .saturating_add(offset as u64);
        let id = ordinal % config.mutable_records;
        let payload = data::payload(config.seed, id, "mutable", job.sequence + 1);
        bytes = bytes.saturating_add(payload.len() as u64);
        operations.push(put(
            config,
            data::mutable_path(id),
            payload,
            format!("contention-mutation-{}-{offset}", job.sequence),
        ));
    }
    let marker_id = (1u64 << 63) | job.sequence;
    let marker_payload = data::payload(config.seed, marker_id, "marker", job.sequence);
    bytes = bytes.saturating_add(marker_payload.len() as u64);
    operations.push(put(
        config,
        data::marker_path(job.sequence),
        marker_payload,
        format!("contention-marker-{}", job.sequence),
    ));
    let response = client
        .bulk_write(BulkWriteRequest { operations })
        .await
        .context("mutation BulkWrite")?
        .into_inner();
    ensure!(
        response.outcomes.len() == config.mutation_batch_size + 1,
        "mutation outcome count mismatch"
    );
    let mut marker_version = None;
    for outcome in response.outcomes {
        let index = usize::try_from(outcome.index)?;
        match outcome.outcome.context("missing mutation outcome")? {
            BulkOutcomeValue::Receipt(receipt) => {
                ensure!(!receipt.deleted && receipt.version != 0);
                if index == config.mutation_batch_size {
                    marker_version = Some(receipt.version);
                }
            }
            BulkOutcomeValue::Failure(failure) => bail!(
                "mutation failed with code {}: {}",
                failure.code,
                failure.message
            ),
        }
    }
    let accepted_at = Instant::now();
    Ok(MutationResult {
        operations: (config.mutation_batch_size + 1) as u64,
        bytes,
        elapsed: accepted_at.saturating_duration_since(started),
        canary: Some(Canary {
            id: job.sequence,
            version: marker_version.context("marker receipt missing")?,
            accepted_at,
        }),
    })
}

async fn wait_canary(
    channel: &Channel,
    token: &str,
    bucket: &str,
    index_name: &str,
    canary: Canary,
    poll: Duration,
    request_timeout: Duration,
    observation_timeout: Duration,
) -> Result<Duration> {
    let deadline = canary.accepted_at + observation_timeout;
    let mut client = index_client(channel.clone(), token)?;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "canary {} was not visible on {index_name}",
            canary.id
        );
        let response = tokio::time::timeout(
            remaining.min(request_timeout),
            marker_query(&mut client, bucket, index_name, canary.id),
        )
        .await
        .context("canary query exceeded per-request timeout")??;
        if response.hits.iter().any(|hit| {
            hit.object_version == canary.version
                && hit
                    .address
                    .as_ref()
                    .is_some_and(|a| a.path == data::marker_path(canary.id))
        }) {
            return Ok(Instant::now().saturating_duration_since(canary.accepted_at));
        }
        ensure!(
            Instant::now() < deadline,
            "canary {} was not visible on {index_name}",
            canary.id
        );
        tokio::time::sleep(poll).await;
    }
}

async fn wait_canary_on_all(
    config: &Config,
    names: &[String],
    channels: &[Channel],
    token: &str,
    canary: Canary,
) -> Result<(bool, BTreeSet<u64>)> {
    let mut source_nodes = BTreeSet::new();
    for (position, name) in names.iter().enumerate() {
        wait_canary(
            &channels[position % channels.len()],
            token,
            &config.bucket,
            name,
            canary,
            config.visibility_poll,
            config.request_timeout,
            config.drain_timeout,
        )
        .await?;
        let mut client = index_client(channels[position % channels.len()].clone(), token)?;
        let response = marker_query(&mut client, &config.bucket, name, canary.id).await?;
        if let Some(freshness) = response.freshness {
            source_nodes.extend(
                freshness
                    .sources
                    .into_iter()
                    .map(|source| source.node_id)
                    .filter(|id| *id != 0),
            );
        }
    }
    Ok((true, source_nodes))
}

fn visibility_definition_position(sample_ordinal: u64, definition_count: usize) -> usize {
    (sample_ordinal as usize) % definition_count
}

fn bounded_error(error: &str) -> String {
    error
        .chars()
        .take(MAX_VISIBILITY_SAMPLE_ERROR_CHARS)
        .collect()
}

async fn verify_final_mutable_state(
    config: &Config,
    names: &[String],
    channels: &[Channel],
    token: &str,
) -> Result<(bool, Option<bool>, BTreeSet<u64>)> {
    let deadline = Instant::now() + config.drain_timeout;
    let mut authority = BTreeMap::new();
    let mut objects = object_client(channels[0].clone(), token)?;
    for id in 0..config.mutable_records {
        let path = data::mutable_path(id);
        let head = objects
            .head_object(HeadObjectRequest {
                address: Some(ObjectAddress {
                    tenant: config.tenant.clone(),
                    bucket: config.bucket.clone(),
                    path: path.clone(),
                }),
            })
            .await?
            .into_inner();
        let version = match head.state.context("mutable head omitted state")? {
            ObjectHeadState::Present(present) => present.version,
            _ => bail!("mutable authority path {path} is not present"),
        };
        authority.insert(path, version);
    }
    let mut nodes = BTreeSet::new();
    let mut all_observed_tails_available = true;
    for (position, name) in names.iter().enumerate() {
        let mut client = index_client(channels[position % channels.len()].clone(), token)?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "index {name} did not converge to authoritative mutable state and zero lag"
            );
            let response = tokio::time::timeout(
                remaining.min(config.request_timeout),
                class_query(&mut client, &config.bucket, name, "mutable", 1_000),
            )
            .await;
            if let Ok(Ok(response)) = response {
                let indexed = response
                    .hits
                    .iter()
                    .map(|hit| {
                        Ok((
                            hit.address
                                .as_ref()
                                .context("mutable query hit omitted address")?
                                .path
                                .clone(),
                            hit.object_version,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                let exact = indexed.len() == response.hits.len() && indexed == authority;
                if let Some(freshness) = response.freshness {
                    let source_ids = freshness
                        .sources
                        .iter()
                        .map(|source| source.node_id)
                        .collect::<BTreeSet<_>>();
                    let healthy = freshness.initial_build_complete
                        && !freshness.rebuilding
                        && freshness.sources.len() == config.endpoints.len()
                        && source_ids.len() == config.endpoints.len()
                        && freshness
                            .sources
                            .iter()
                            .all(|source| source.node_id != 0 && source.source_epoch.len() == 32);
                    let observed_tails_available = freshness
                        .sources
                        .iter()
                        .all(|source| source.observed_tail.is_some());
                    let no_observed_lag = freshness.sources.iter().all(source_has_no_observed_lag);
                    if exact && healthy && no_observed_lag {
                        all_observed_tails_available &= observed_tails_available;
                        nodes.extend(
                            freshness
                                .sources
                                .into_iter()
                                .map(|source| source.node_id)
                                .filter(|id| *id != 0),
                        );
                        break;
                    }
                }
            }
            tokio::time::sleep(config.visibility_poll).await;
        }
    }
    Ok((true, all_observed_tails_available.then_some(true), nodes))
}

fn source_has_no_observed_lag(source: &IndexSourceFreshness) -> bool {
    source.lag_hint == 0
        && source
            .observed_tail
            .is_none_or(|tail| tail.checked_add(1) == Some(source.indexed_next_offset))
}

fn put(config: &Config, path: String, bytes: Vec<u8>, command_id: String) -> BulkOperation {
    BulkOperation {
        operation: Some(BulkOperationValue::Put(BulkPutRequest {
            address: Some(ObjectAddress {
                tenant: config.tenant.clone(),
                bucket: config.bucket.clone(),
                path,
            }),
            bytes,
            content_type: CONTENT_TYPE.into(),
            command_id,
            durability: configured_durability(config) as i32,
        })),
    }
}

async fn stable_query(
    client: &mut IndexClient,
    bucket: &str,
    index_name: &str,
) -> Result<QueryIndexResponse> {
    class_query(client, bucket, index_name, "stable", 1_000).await
}

async fn class_query(
    client: &mut IndexClient,
    bucket: &str,
    index_name: &str,
    class: &str,
    limit: u32,
) -> Result<QueryIndexResponse> {
    query(
        client,
        bucket,
        index_name,
        "class",
        serde_json::to_vec(class)?,
        limit,
    )
    .await
}

async fn marker_query(
    client: &mut IndexClient,
    bucket: &str,
    index_name: &str,
    sequence: u64,
) -> Result<QueryIndexResponse> {
    let marker_id = (1u64 << 63) | sequence;
    query(
        client,
        bucket,
        index_name,
        "record_id",
        marker_id.to_string().into_bytes(),
        1,
    )
    .await
}

async fn query(
    client: &mut IndexClient,
    bucket: &str,
    index_name: &str,
    field: &str,
    value: Vec<u8>,
    limit: u32,
) -> Result<QueryIndexResponse> {
    client
        .query_index(QueryIndexRequest {
            bucket: bucket.into(),
            index_name: index_name.into(),
            query: Some(IndexQuery {
                query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
                    predicate: Some(IndexPredicateExpression::leaf(IndexPredicate {
                        field: field.into(),
                        operator: IndexPredicateOperator::Equal as i32,
                        values_json: vec![value],
                    })),
                    order: Vec::new(),
                    facets: Vec::new(),
                    aggregates: Vec::new(),
                })),
            }),
            limit,
            page_token: Vec::new(),
            tenant: String::new(),
            required_freshness: None,
        })
        .await
        .map(tonic::Response::into_inner)
        .map_err(Into::into)
}

fn validate_stable(response: &QueryIndexResponse, expected: &BTreeSet<String>) -> Result<()> {
    let actual = response
        .hits
        .iter()
        .map(|hit| {
            hit.address
                .as_ref()
                .map(|address| address.path.clone())
                .context("query hit omitted address")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    ensure!(
        actual.len() == response.hits.len(),
        "query returned duplicate stable paths"
    );
    ensure!(&actual == expected, "stable query oracle mismatch");
    ensure!(response.freshness.is_some(), "query omitted freshness");
    Ok(())
}

fn index_client(channel: Channel, token: &str) -> Result<IndexClient> {
    Ok(
        IndexServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(72 * 1024 * 1024)
            .max_decoding_message_size(72 * 1024 * 1024),
    )
}

fn configured_durability(config: &Config) -> Durability {
    match config.durability.as_str() {
        "REPLICATED" => Durability::Replicated,
        _ => Durability::Local,
    }
}

async fn connect_all(endpoints: &[String]) -> Result<Vec<Channel>> {
    let mut channels = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        channels.push(
            connect_channel(endpoint)
                .await
                .map_err(|error| anyhow!("connect to {endpoint}: {error}"))?,
        );
    }
    Ok(channels)
}

fn unix_millis() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_ids_do_not_overlap_small_corpus_ids() {
        assert_eq!((1u64 << 63) | 7, 9_223_372_036_854_775_815);
        assert!(data::marker_path(7).contains("0000000000000007"));
    }

    #[test]
    fn optional_observed_tail_never_manufactures_zero_lag() {
        let unavailable = IndexSourceFreshness {
            indexed_next_offset: 12,
            lag_hint: 0,
            observed_tail: None,
            ..Default::default()
        };
        assert!(source_has_no_observed_lag(&unavailable));
        assert!(unavailable.observed_tail.is_none());

        let current = IndexSourceFreshness {
            observed_tail: Some(11),
            ..unavailable.clone()
        };
        assert!(source_has_no_observed_lag(&current));

        let behind = IndexSourceFreshness {
            observed_tail: Some(12),
            lag_hint: 1,
            ..unavailable
        };
        assert!(!source_has_no_observed_lag(&behind));
    }

    #[test]
    fn visibility_samples_rotate_independently_of_canary_interval() {
        let positions = (0..20)
            .map(|ordinal| visibility_definition_position(ordinal, 16))
            .collect::<Vec<_>>();
        assert_eq!(&positions[..16], &(0..16).collect::<Vec<_>>());
        assert_eq!(&positions[16..], &[0, 1, 2, 3]);
    }

    #[test]
    fn visibility_failure_errors_are_bounded_on_character_boundaries() {
        let error = "é".repeat(MAX_VISIBILITY_SAMPLE_ERROR_CHARS + 10);
        let bounded = bounded_error(&error);
        assert_eq!(bounded.chars().count(), MAX_VISIBILITY_SAMPLE_ERROR_CHARS);
        assert!(error.starts_with(&bounded));
    }

    #[tokio::test]
    async fn fixed_rate_records_every_schedule_and_queue_drop() {
        let (job_tx, mut job_rx) = mpsc::channel(2);
        let report = produce_fixed_rate_jobs(Duration::from_millis(70), 32, 3_300.0, job_tx)
            .await
            .unwrap();
        assert_eq!(report.offered_batches, 6);
        assert_eq!(report.submitted_batches, 2);
        assert_eq!(report.dropped_batches, 4);
        assert_eq!(job_rx.recv().await.unwrap().sequence, 0);
        assert_eq!(job_rx.recv().await.unwrap().sequence, 1);
    }
}
