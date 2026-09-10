//! Black-box qualification for query latency under sustained index mutation.
//!
//! This executable uses only authenticated public Keldra APIs. It deliberately
//! creates multiple definitions over one bucket so every definition consumes
//! the same continuously mutating source journal.

#[path = "index_contention_qualification/capabilities.rs"]
mod capabilities;
#[path = "index_contention_qualification/config.rs"]
mod config;
#[path = "index_contention_qualification/data.rs"]
mod data;
#[path = "index_contention_qualification/metrics.rs"]
mod metrics;
#[path = "index_contention_qualification/progress.rs"]
mod progress;
#[cfg(test)]
#[path = "index_contention_qualification/tests.rs"]
mod tests;

use anyhow::{Context, Result, anyhow, bail, ensure};
use config::{Config, MutationWorkload};
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
    BearerToken, KeywordField, RawClient, TypedJsonIndexBuilder, administration_client,
    connect_channel, exchange_client_credentials, object_client,
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
const MAX_ACTIVE_QUERY_DEFINITIONS: usize = 1_024;

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    started_unix_milliseconds: u128,
    completed_unix_milliseconds: u128,
    result: &'static str,
    configuration: config::PublicConfig,
    corpus_sha256: String,
    index_definition_ids: Vec<u64>,
    definition_creation_seconds: f64,
    qualified_definition_activation_seconds: f64,
    qualified_definition_count: usize,
    physical_recipe_count: usize,
    observed_source_node_ids: Vec<u64>,
    assignment_observability: &'static str,
    responsiveness_definition: &'static str,
    baseline: QueryPhaseReport,
    concurrent: QueryPhaseReport,
    mutations: MutationReport,
    post_load: PostLoadReport,
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
    measurement_window_seconds: f64,
    total_until_all_terminal_seconds: f64,
    response_drain_seconds: f64,
    actual_scheduled_queries_per_second: f64,
    successful_queries_per_second: f64,
    scheduled_queries: u64,
    successful_queries: u64,
    successful_queries_in_window: u64,
    successful_queries_after_window: u64,
    scheduler_deadline_misses: u64,
    client_concurrency_rejections: u64,
    request_errors: u64,
    timeouts: u64,
    correctness_errors: u64,
    successful_schedule_to_response_latency: LatencyReport,
    successful_dispatch_to_response_latency: LatencyReport,
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
    target_data_operations_per_second: Option<f64>,
    measurement_window_started_unix_milliseconds: u128,
    measurement_window_ended_unix_milliseconds: u128,
    load_window_seconds: f64,
    total_until_all_terminal_seconds: f64,
    response_drain_seconds: f64,
    scheduled_batches: u64,
    undispatched_at_measurement_deadline_batches: u64,
    client_queue_enqueued_batches: u64,
    client_queue_dropped_batches: u64,
    scheduled_data_operations: u64,
    undispatched_at_measurement_deadline_data_operations: u64,
    client_queue_enqueued_data_operations: u64,
    fully_successful_batches: u64,
    structurally_valid_batches_with_operation_failures: u64,
    successful_data_operations: u64,
    successful_data_operations_in_window: u64,
    successful_data_operations_after_window: u64,
    successful_probe_operations: u64,
    failed_data_operations: u64,
    failed_probe_operations: u64,
    successful_data_payload_bytes: u64,
    successful_data_payload_bytes_in_window: u64,
    successful_data_payload_bytes_after_window: u64,
    successful_probe_payload_bytes: u64,
    indeterminate_data_operations: u64,
    indeterminate_probe_operations: u64,
    scheduled_data_operations_per_second: f64,
    client_queue_enqueued_data_operations_per_second: f64,
    successful_data_ingest_throughput_operations_per_second: f64,
    successful_data_ingest_throughput_payload_bytes_per_second: f64,
    indeterminate_batches: u64,
    failure_classes: Vec<MutationFailureClass>,
    failure_occurrences_omitted: u64,
    failure_diagnostics_definition: &'static str,
    queue_capacity: usize,
    minimum_sampled_client_queue_depth: usize,
    queue_depth_samples: u64,
    sampled_client_queue_nonempty_ratio: f64,
    sampled_client_queue_empty_count: u64,
    structurally_valid_bulk_write_dispatch_to_response_latency: LatencyReport,
    visibility_probes_planned: u64,
    visibility_probes_with_successful_receipts: u64,
    visibility_probes_started: u64,
    visibility_probes_succeeded: u64,
    visibility_probes_failed: u64,
    visibility_probe_failures: Vec<VisibilitySampleFailure>,
    visibility_probe_failures_omitted: u64,
    successful_receipt_to_probe_start_delay: LatencyReport,
    probe_start_to_query_visibility_latency: LatencyReport,
    successful_receipt_to_query_visibility_latency: LatencyReport,
    visibility_definition: &'static str,
}

#[derive(Debug, Default, Serialize)]
struct PostLoadReport {
    /// Non-overlapping wall time after the concurrent query phase has reached
    /// terminal responses, until correctness and convergence checks complete.
    elapsed_seconds: f64,
    outstanding_requests_and_sampled_visibility_seconds: f64,
    credential_refresh_seconds: f64,
    authoritative_state_read_seconds: f64,
    final_index_convergence_seconds: f64,
}

#[derive(Debug, Serialize)]
struct CorrectnessReport {
    passed: bool,
    stable_oracle_checked_on_every_structurally_valid_query_response: bool,
    exact_mutable_versions_verified_by_every_qualified_definition: bool,
    final_freshness_healthy_by_every_qualified_definition: bool,
    advisory_zero_lag_verified_by_every_qualified_definition: Option<bool>,
    zero_query_correctness_errors: bool,
}

#[derive(Debug, Serialize)]
struct WorkloadValidityReport {
    passed: bool,
    all_qualified_definitions_offered_in_every_phase: bool,
    sustained_nonempty_mutation_queue: bool,
    mutation_load_shape_valid: bool,
    all_enqueued_mutation_batches_reached_terminal_outcomes: bool,
    data_operation_outcomes_reconciled: bool,
    probe_operation_outcomes_reconciled: bool,
    zero_failed_mutation_operations: bool,
    successful_data_operations_observed: bool,
}

#[derive(Debug, Serialize)]
struct ResponsivenessReport {
    passed: bool,
    zero_query_request_errors_or_timeouts: bool,
    zero_query_scheduler_deadline_misses_or_client_concurrency_rejections: bool,
    concurrent_query_p99_within_configured_limit: bool,
    visibility_probe_population_complete: bool,
    successful_receipt_to_query_visibility_p99_within_configured_limit: bool,
}

#[derive(Debug, Serialize)]
struct VisibilitySampleFailure {
    canary_id: u64,
    object_version: u64,
    definition_position: usize,
    definition_name: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct MutationFailureClass {
    source: &'static str,
    code: i32,
    code_name: String,
    message: String,
    count: u64,
}

#[derive(Debug)]
struct MutationRequestFailure {
    classes: Vec<MutationFailureClass>,
}

impl MutationRequestFailure {
    fn one(source: &'static str, code: i32, code_name: String, message: String) -> Self {
        Self {
            classes: vec![MutationFailureClass {
                source,
                code,
                code_name,
                message: bounded_mutation_failure_message(&message),
                count: 1,
            }],
        }
    }
}

struct VisibilitySampleOutcome {
    canary: Canary,
    definition_position: usize,
    definition_name: String,
    successful_receipt_to_probe_start: Duration,
    result: Result<Duration>,
}

const MAX_VISIBILITY_SAMPLE_FAILURE_DETAILS: usize = 16;
const MAX_VISIBILITY_SAMPLE_ERROR_CHARS: usize = 512;
const MAX_MUTATION_FAILURE_CLASSES: usize = 8;
const MAX_MUTATION_FAILURE_MESSAGE_CHARS: usize = 512;

#[derive(Clone, Copy)]
struct MutationJob {
    sequence: u64,
}

struct MutationResult {
    successful_data_operations: u64,
    successful_probe_operations: u64,
    failed_data_operations: u64,
    failed_probe_operations: u64,
    successful_data_payload_bytes: u64,
    successful_probe_payload_bytes: u64,
    failures: Vec<MutationFailureClass>,
    elapsed: Duration,
    completed_at: Instant,
    canary: Option<Canary>,
}

#[derive(Default)]
struct MutationProducerReport {
    scheduled_batches: u64,
    undispatched_at_measurement_deadline_batches: u64,
    client_queue_enqueued_batches: u64,
    client_queue_dropped_batches: u64,
}

#[derive(Clone, Copy)]
struct Canary {
    id: u64,
    version: u64,
    completed_at: Instant,
    sample_eligible: bool,
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
    if std::env::var_os("KELDRA_INDEX_CONTENTION_CAPABILITY_ONLY").is_some() {
        let report = capabilities::run(&config, started_unix_milliseconds).await?;
        write_report(config.output.as_deref(), &report)?;
        return Ok(());
    }
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
                schema: "keldra.index-contention-terminal-failure.v2",
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
    let corpus_sha256 = data::corpus_digest(
        config.seed,
        config.stable_records,
        config.mutable_records,
        config.physical_recipe_count,
    );
    let setup_channels = connect_all(&config.endpoints).await?;
    let token = fresh_token(&config, &setup_channels[0]).await?;
    setup(&config, &setup_channels[0], &token).await?;

    let definition_creation_started = Instant::now();
    let definitions = create_definitions(&config, &setup_channels[0], &token).await?;
    let definition_creation_seconds = definition_creation_started.elapsed().as_secs_f64();
    let minimum_phase_schedules = config
        .query_rate
        .saturating_mul(
            config
                .baseline
                .min(config.concurrent)
                .min(config.post)
                .as_secs(),
        )
        .max(config.physical_recipe_count as u64) as usize;
    let names = Arc::new(
        qualification_definition_positions(
            config.definition_count,
            config.physical_recipe_count,
            MAX_ACTIVE_QUERY_DEFINITIONS.min(minimum_phase_schedules),
        )
        .into_iter()
        .map(data::index_name)
        .collect::<Vec<_>>(),
    );
    let expected = Arc::new(
        (0..config.stable_records)
            .map(data::stable_path)
            .collect::<BTreeSet<_>>(),
    );
    let definition_activation_started = Instant::now();
    wait_all_ready(&config, &names, &expected, &token).await?;
    let qualified_definition_activation_seconds =
        definition_activation_started.elapsed().as_secs_f64();
    let phase_token = fresh_token(&config, &setup_channels[0]).await?;

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
        &phase_token,
        config.baseline,
        Instant::now(),
        counters.clone(),
    )
    .await?;
    counters.phase("concurrent").await;
    let concurrent_started = Instant::now();
    let concurrent_started_unix_milliseconds = unix_millis()?;
    let mutation_task = tokio::spawn(run_mutations(
        config.clone(),
        mutation_channels,
        visibility_channels,
        phase_token.clone(),
        counters.clone(),
        concurrent_started,
        concurrent_started_unix_milliseconds,
    ));
    let concurrent = run_query_phase(
        &config,
        &names,
        &expected,
        &query_channels,
        &phase_token,
        config.concurrent,
        concurrent_started,
        counters.clone(),
    )
    .await?;
    counters.phase("drain").await;
    let post_load_started = Instant::now();
    let outstanding_started = Instant::now();
    let mutation_report = tokio::time::timeout(config.drain_timeout, mutation_task)
        .await
        .context("mutation drain exceeded timeout")???;
    let outstanding_requests_and_sampled_visibility_seconds =
        outstanding_started.elapsed().as_secs_f64();
    let credential_refresh_started = Instant::now();
    let verification_token = fresh_token(&config, &setup_channels[0]).await?;
    let credential_refresh_seconds = credential_refresh_started.elapsed().as_secs_f64();
    let authority_started = Instant::now();
    let authority =
        load_authoritative_mutable_state(&config, &query_channels, &verification_token).await?;
    let authoritative_state_read_seconds = authority_started.elapsed().as_secs_f64();
    let convergence_started = Instant::now();
    let (final_state_verified, advisory_zero_lag, final_sources) = tokio::time::timeout(
        config.drain_timeout,
        verify_final_mutable_state(
            &config,
            &names,
            &query_channels,
            &verification_token,
            authority,
        ),
    )
    .await
    .context("final mutable verification exceeded drain timeout")??;
    let final_index_convergence_seconds = convergence_started.elapsed().as_secs_f64();
    let observed = final_sources;
    let post_load = PostLoadReport {
        elapsed_seconds: post_load_started.elapsed().as_secs_f64(),
        outstanding_requests_and_sampled_visibility_seconds,
        credential_refresh_seconds,
        authoritative_state_read_seconds,
        final_index_convergence_seconds,
    };
    counters.phase("post").await;
    let post = run_query_phase(
        &config,
        &names,
        &expected,
        &query_channels,
        &verification_token,
        config.post,
        Instant::now(),
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
    let zero_query_scheduler_deadline_misses_or_client_concurrency_rejections =
        [&baseline, &concurrent, &post]
            .iter()
            .all(|p| p.scheduler_deadline_misses + p.client_concurrency_rejections == 0);
    let all_enqueued_mutation_batches_reached_terminal_outcomes = mutation_report
        .fully_successful_batches
        + mutation_report.structurally_valid_batches_with_operation_failures
        + mutation_report.indeterminate_batches
        == mutation_report.client_queue_enqueued_batches;
    let data_operation_outcomes_reconciled = mutation_report.successful_data_operations
        + mutation_report.failed_data_operations
        + mutation_report.indeterminate_data_operations
        == mutation_report.client_queue_enqueued_data_operations;
    let probe_operation_outcomes_reconciled = mutation_report.successful_probe_operations
        + mutation_report.failed_probe_operations
        + mutation_report.indeterminate_probe_operations
        == mutation_report.client_queue_enqueued_batches;
    let zero_failed_mutation_operations = mutation_report.failed_data_operations == 0
        && mutation_report.failed_probe_operations == 0
        && mutation_report.indeterminate_data_operations == 0
        && mutation_report.indeterminate_probe_operations == 0;
    let successful_data_operations_observed = mutation_report.successful_data_operations > 0;
    let visibility_probe_population_complete = mutation_report.visibility_probes_planned > 0
        && mutation_report.visibility_probes_with_successful_receipts
            == mutation_report.visibility_probes_planned
        && mutation_report.visibility_probes_started == mutation_report.visibility_probes_planned
        && mutation_report.visibility_probes_succeeded == mutation_report.visibility_probes_planned
        && mutation_report.visibility_probes_failed == 0;
    let concurrent_p99_passed = config
        .max_concurrent_query_p99_ms
        .is_none_or(|maximum| concurrent.successful_schedule_to_response_latency.p99_ms <= maximum);
    let successful_receipt_to_query_visibility_p99_passed = config
        .max_successful_receipt_to_query_visibility_p99_ms
        .is_none_or(|maximum| {
            mutation_report
                .successful_receipt_to_query_visibility_latency
                .p99_ms
                <= maximum
        });
    let all_qualified_definitions_offered = [&baseline, &concurrent, &post]
        .iter()
        .all(|phase| phase.offered_definition_count == names.len());
    let sustained_nonempty_mutation_queue = mutation_report.queue_depth_samples > 0
        && mutation_report.minimum_sampled_client_queue_depth > 0
        && mutation_report.sampled_client_queue_empty_count == 0;
    let mutation_load_shape_valid = if config.target_data_operations_per_second.is_some() {
        mutation_report.client_queue_dropped_batches == 0
            && mutation_report.undispatched_at_measurement_deadline_batches == 0
            && mutation_report.scheduled_batches == mutation_report.client_queue_enqueued_batches
    } else {
        sustained_nonempty_mutation_queue
    };
    let correctness_passed = zero_query_correctness_errors && final_state_verified;
    let workload_passed = all_qualified_definitions_offered
        && mutation_load_shape_valid
        && all_enqueued_mutation_batches_reached_terminal_outcomes
        && data_operation_outcomes_reconciled
        && probe_operation_outcomes_reconciled
        && zero_failed_mutation_operations
        && successful_data_operations_observed;
    let responsiveness_passed = zero_query_request_errors_or_timeouts
        && zero_query_scheduler_deadline_misses_or_client_concurrency_rejections
        && concurrent_p99_passed
        && visibility_probe_population_complete
        && successful_receipt_to_query_visibility_p99_passed;
    let report = Report {
        schema: "keldra.index-contention-qualification.v2",
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
        definition_creation_seconds,
        qualified_definition_activation_seconds,
        qualified_definition_count: names.len(),
        physical_recipe_count: config.physical_recipe_count,
        observed_source_node_ids: observed.into_iter().collect(),
        assignment_observability: "public APIs expose source node IDs and placement epochs, but not partition-producer assignments; logical definition and physical recipe counts are cluster-wide and are not labeled as per-node concurrency",
        responsiveness_definition: "every scheduled open-loop query completes within request_timeout with no scheduler deadline miss, client-concurrency rejection, request error, or timeout; oracle mismatches are reported by the separate correctness result; visibility probes use request_timeout per query and a separate observation timeout; optional concurrent-query and successful-receipt-to-query-visibility p99 gates are applied when configured",
        baseline,
        concurrent,
        mutations: mutation_report,
        post_load,
        post,
        correctness: CorrectnessReport {
            passed: correctness_passed,
            stable_oracle_checked_on_every_structurally_valid_query_response: true,
            exact_mutable_versions_verified_by_every_qualified_definition: final_state_verified,
            final_freshness_healthy_by_every_qualified_definition: final_state_verified,
            advisory_zero_lag_verified_by_every_qualified_definition: advisory_zero_lag,
            zero_query_correctness_errors,
        },
        workload_validity: WorkloadValidityReport {
            passed: workload_passed,
            all_qualified_definitions_offered_in_every_phase: all_qualified_definitions_offered,
            sustained_nonempty_mutation_queue,
            mutation_load_shape_valid,
            all_enqueued_mutation_batches_reached_terminal_outcomes,
            data_operation_outcomes_reconciled,
            probe_operation_outcomes_reconciled,
            zero_failed_mutation_operations,
            successful_data_operations_observed,
        },
        responsiveness: ResponsivenessReport {
            passed: responsiveness_passed,
            zero_query_request_errors_or_timeouts,
            zero_query_scheduler_deadline_misses_or_client_concurrency_rejections,
            concurrent_query_p99_within_configured_limit: concurrent_p99_passed,
            visibility_probe_population_complete,
            successful_receipt_to_query_visibility_p99_within_configured_limit:
                successful_receipt_to_query_visibility_p99_passed,
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
            data::payload(config.seed, id, "stable", 0, config.physical_recipe_count),
            format!("contention-initial-stable-{id}"),
        ));
    }
    for id in 0..config.mutable_records {
        operations.push(put(
            config,
            data::mutable_path(id),
            data::payload(config.seed, id, "mutable", 0, config.physical_recipe_count),
            format!("contention-initial-mutable-{id}"),
        ));
    }
    if config.mutation_workload == MutationWorkload::ProjectionPreserving {
        for ordinal in 0..data::PROJECTION_PRESERVING_MARKERS {
            let id = data::marker_id(ordinal);
            operations.push(put(
                config,
                data::marker_path(ordinal),
                data::payload_with_generations(
                    config.seed,
                    id,
                    "marker",
                    0,
                    0,
                    config.physical_recipe_count,
                ),
                format!("contention-initial-marker-{ordinal}"),
            ));
        }
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
        let recipe = physical_recipe(position, config.physical_recipe_count);
        let request: CreateIndexRequest = TypedJsonIndexBuilder::new(&config.bucket, &name)
            .path_prefix("contention/")
            .content_type(CONTENT_TYPE)
            .field(KeywordField::multi("probe", recipe_probe_pointer(recipe)).exact())
            .finish(format!("contention-create-{position}"))?;
        let definition = tokio::time::timeout(config.request_timeout, client.create_index(request))
            .await
            .with_context(|| format!("create index {name} exceeded request timeout"))?
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
    phase_start: Instant,
    counters: Arc<Counters>,
) -> Result<QueryPhaseReport> {
    let mut report = QueryPhaseReport::default();
    let mut end_to_end = Latencies::new()?;
    let mut service = Latencies::new()?;
    let mut lateness = Latencies::new()?;
    let permits = Arc::new(Semaphore::new(config.query_max_in_flight));
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
        report.scheduled_queries += 1;
        offered_definitions.insert(sequence as usize % names.len());
        counters.scheduled.fetch_add(1, Ordering::Relaxed);
        lateness.record(dispatched.saturating_duration_since(intended))?;
        if dispatched.saturating_duration_since(intended) >= period {
            report.scheduler_deadline_misses += 1;
            counters.dropped.fetch_add(1, Ordering::Relaxed);
            sequence = sequence.checked_add(1).context("query schedule overflow")?;
            continue;
        }
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            report.client_concurrency_rejections += 1;
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
                queried_definitions.insert(outcome.definition_position);
                if outcome.correctness_error {
                    report.correctness_errors += 1;
                } else {
                    report.successful_queries += 1;
                    if completed < phase_end {
                        report.successful_queries_in_window += 1;
                    } else {
                        report.successful_queries_after_window += 1;
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
    }
    report.measurement_window_seconds = duration.as_secs_f64();
    report.total_until_all_terminal_seconds = phase_start.elapsed().as_secs_f64();
    report.response_drain_seconds =
        (report.total_until_all_terminal_seconds - report.measurement_window_seconds).max(0.0);
    report.actual_scheduled_queries_per_second =
        report.scheduled_queries as f64 / report.measurement_window_seconds;
    report.successful_queries_per_second =
        report.successful_queries_in_window as f64 / report.measurement_window_seconds;
    report.successful_schedule_to_response_latency = end_to_end.report();
    report.successful_dispatch_to_response_latency = service.report();
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
    mutation_started: Instant,
    measurement_window_started_unix_milliseconds: u128,
) -> Result<MutationReport> {
    let mutation_window_ends = mutation_started + config.concurrent;
    let measurement_window_ended_unix_milliseconds =
        measurement_window_started_unix_milliseconds.saturating_add(config.concurrent.as_millis());
    let (job_tx, job_rx) = mpsc::channel(config.mutation_queue_depth);
    let sampler_tx = job_tx.clone();
    let receiver = Arc::new(Mutex::new(job_rx));
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let producing = Arc::new(AtomicUsize::new(1));
    let minimum_depth = Arc::new(AtomicUsize::new(config.mutation_queue_depth));
    let starvation = Arc::new(AtomicU64::new(0));
    let queue_samples = Arc::new(AtomicU64::new(0));
    let prefilled_batches = if config.target_data_operations_per_second.is_none() {
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
        let report = produce_mutation_jobs(
            &producer_config,
            job_tx,
            prefilled_batches,
            mutation_started,
            mutation_window_ends,
        )
        .await;
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
                let outcome = execute_mutation(&config, &mut client, job).await;
                let _ = result_tx.send((Instant::now(), job, outcome));
            }
            Ok::<(), anyhow::Error>(())
        });
    }
    drop(result_tx);
    let mut report = MutationReport {
        load_mode: if config.target_data_operations_per_second.is_some() {
            "fixed-rate"
        } else {
            "saturated-queue"
        },
        target_data_operations_per_second: config.target_data_operations_per_second,
        measurement_window_started_unix_milliseconds,
        measurement_window_ended_unix_milliseconds,
        load_window_seconds: config.concurrent.as_secs_f64(),
        queue_capacity: config.mutation_queue_depth,
        ..MutationReport::default()
    };
    let mut request_latency = Latencies::new()?;
    let mut successful_receipt_to_probe_start = Latencies::new()?;
    let mut probe_start_to_visibility = Latencies::new()?;
    let mut successful_receipt_to_visibility = Latencies::new()?;
    let mut visibility_tasks = JoinSet::new();
    // Bound observer traffic independently from workload queries. Every
    // predetermined sample waits for a permit and reports that wait separately,
    // so neither favorable censoring nor observer queueing is hidden.
    let visibility_permits = Arc::new(Semaphore::new(config.query_max_in_flight));
    let mut visibility_sample_ordinal = 0_u64;
    let mut last_terminal_response_at = mutation_started;
    while let Some((terminal_at, job, event)) = result_rx.recv().await {
        last_terminal_response_at = last_terminal_response_at.max(terminal_at);
        if job.sequence % config.visibility_sample_every_batches == 0 {
            report.visibility_probes_planned += 1;
        }
        match event {
            Err(failure) => {
                report.indeterminate_batches += 1;
                report.indeterminate_data_operations = report
                    .indeterminate_data_operations
                    .saturating_add(config.mutation_batch_size as u64);
                report.indeterminate_probe_operations =
                    report.indeterminate_probe_operations.saturating_add(1);
                counters.mutation_errors.fetch_add(1, Ordering::Relaxed);
                record_mutation_failure(&mut report, failure);
            }
            Ok(result) => {
                if result.failures.is_empty() {
                    report.fully_successful_batches += 1;
                } else {
                    report.structurally_valid_batches_with_operation_failures += 1;
                    counters.mutation_errors.fetch_add(1, Ordering::Relaxed);
                    record_mutation_failure(
                        &mut report,
                        MutationRequestFailure {
                            classes: result.failures.clone(),
                        },
                    );
                }
                report.successful_data_operations = report
                    .successful_data_operations
                    .saturating_add(result.successful_data_operations);
                report.failed_data_operations = report
                    .failed_data_operations
                    .saturating_add(result.failed_data_operations);
                report.failed_probe_operations = report
                    .failed_probe_operations
                    .saturating_add(result.failed_probe_operations);
                if result.completed_at < mutation_window_ends {
                    report.successful_data_operations_in_window = report
                        .successful_data_operations_in_window
                        .saturating_add(result.successful_data_operations);
                    report.successful_data_payload_bytes_in_window = report
                        .successful_data_payload_bytes_in_window
                        .saturating_add(result.successful_data_payload_bytes);
                } else {
                    report.successful_data_operations_after_window = report
                        .successful_data_operations_after_window
                        .saturating_add(result.successful_data_operations);
                    report.successful_data_payload_bytes_after_window = report
                        .successful_data_payload_bytes_after_window
                        .saturating_add(result.successful_data_payload_bytes);
                }
                report.successful_probe_operations = report
                    .successful_probe_operations
                    .saturating_add(result.successful_probe_operations);
                report.successful_data_payload_bytes = report
                    .successful_data_payload_bytes
                    .saturating_add(result.successful_data_payload_bytes);
                report.successful_probe_payload_bytes = report
                    .successful_probe_payload_bytes
                    .saturating_add(result.successful_probe_payload_bytes);
                counters
                    .mutations
                    .fetch_add(result.successful_data_operations, Ordering::Relaxed);
                request_latency.record(result.elapsed)?;
                if let Some(canary) = result.canary {
                    if canary.sample_eligible {
                        report.visibility_probes_with_successful_receipts += 1;
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
                        let permits = visibility_permits.clone();
                        visibility_tasks.spawn(async move {
                            let permit_result = permits.acquire_owned().await;
                            let probe_started = Instant::now();
                            let successful_receipt_to_probe_start =
                                probe_started.saturating_duration_since(canary.completed_at);
                            let result = match permit_result {
                                Ok(permit) => {
                                    let _permit = permit;
                                    wait_canary(
                                        &channel,
                                        &token,
                                        &bucket,
                                        &name,
                                        canary,
                                        poll,
                                        request_timeout,
                                        observation_timeout,
                                    )
                                    .await
                                }
                                Err(error) => Err(anyhow!("visibility semaphore closed: {error}")),
                            };
                            VisibilitySampleOutcome {
                                canary,
                                definition_position,
                                definition_name: name,
                                successful_receipt_to_probe_start,
                                result,
                            }
                        });
                    }
                }
            }
        }
    }
    // The mutation workload ends when every submitted response has arrived.
    // Visibility probes measure indexing lag and must not dilute ingest rate.
    let mutation_elapsed = mutation_started.elapsed();
    let producer_report = producer.await.context("mutation producer panicked")??;
    report.scheduled_batches = producer_report.scheduled_batches;
    report.undispatched_at_measurement_deadline_batches =
        producer_report.undispatched_at_measurement_deadline_batches;
    report.client_queue_enqueued_batches = producer_report.client_queue_enqueued_batches;
    report.client_queue_dropped_batches = producer_report.client_queue_dropped_batches;
    report.scheduled_data_operations = producer_report
        .scheduled_batches
        .saturating_mul(config.mutation_batch_size as u64);
    report.undispatched_at_measurement_deadline_data_operations = producer_report
        .undispatched_at_measurement_deadline_batches
        .saturating_mul(config.mutation_batch_size as u64);
    report.client_queue_enqueued_data_operations = producer_report
        .client_queue_enqueued_batches
        .saturating_mul(config.mutation_batch_size as u64);
    while let Some(worker) = workers.join_next().await {
        worker.context("mutation worker panicked")??;
    }
    sampler.await.context("queue sampler panicked")?;
    while let Some(sample) = visibility_tasks.join_next().await {
        let sample = sample.context("visibility task panicked")?;
        report.visibility_probes_started += 1;
        successful_receipt_to_probe_start.record(sample.successful_receipt_to_probe_start)?;
        match sample.result {
            Ok(successful_receipt_to_visible_duration) => {
                report.visibility_probes_succeeded += 1;
                successful_receipt_to_visibility.record(successful_receipt_to_visible_duration)?;
                probe_start_to_visibility.record(
                    successful_receipt_to_visible_duration
                        .saturating_sub(sample.successful_receipt_to_probe_start),
                )?;
            }
            Err(error) => {
                report.visibility_probes_failed += 1;
                if report.visibility_probe_failures.len() < MAX_VISIBILITY_SAMPLE_FAILURE_DETAILS {
                    report
                        .visibility_probe_failures
                        .push(VisibilitySampleFailure {
                            canary_id: sample.canary.id,
                            object_version: sample.canary.version,
                            definition_position: sample.definition_position,
                            definition_name: sample.definition_name,
                            error: bounded_error(&format!("{error:#}")),
                        });
                } else {
                    report.visibility_probe_failures_omitted =
                        report.visibility_probe_failures_omitted.saturating_add(1);
                }
            }
        }
    }
    report.minimum_sampled_client_queue_depth = minimum_depth.load(Ordering::Relaxed);
    report.sampled_client_queue_empty_count = starvation.load(Ordering::Relaxed);
    report.queue_depth_samples = queue_samples.load(Ordering::Relaxed);
    report.sampled_client_queue_nonempty_ratio = if report.queue_depth_samples == 0 {
        0.0
    } else {
        (report.queue_depth_samples - report.sampled_client_queue_empty_count) as f64
            / report.queue_depth_samples as f64
    };
    report.structurally_valid_bulk_write_dispatch_to_response_latency = request_latency.report();
    report.total_until_all_terminal_seconds = mutation_elapsed.as_secs_f64();
    report.response_drain_seconds = last_terminal_response_at
        .saturating_duration_since(mutation_window_ends)
        .as_secs_f64();
    report.scheduled_data_operations_per_second =
        report.scheduled_data_operations as f64 / report.load_window_seconds;
    report.client_queue_enqueued_data_operations_per_second =
        report.client_queue_enqueued_data_operations as f64 / report.load_window_seconds;
    report.successful_data_ingest_throughput_operations_per_second =
        report.successful_data_operations_in_window as f64 / report.load_window_seconds;
    report.successful_data_ingest_throughput_payload_bytes_per_second =
        report.successful_data_payload_bytes_in_window as f64 / report.load_window_seconds;
    report.failure_diagnostics_definition = "fully_successful_batches, structurally_valid_batches_with_operation_failures, and indeterminate_batches are disjoint terminal outcomes; indeterminate_batches had no structurally valid per-operation response, so their operation outcomes are indeterminate rather than falsely classified as failed; successful and failed operations come from structurally valid responses; successful sibling outcomes in a partial response remain counted; failure_classes retains bounded diagnostics; at most eight distinct classes are retained and failure_occurrences_omitted counts occurrences from additional classes";
    report.successful_receipt_to_probe_start_delay = successful_receipt_to_probe_start.report();
    report.probe_start_to_query_visibility_latency = probe_start_to_visibility.report();
    report.successful_receipt_to_query_visibility_latency =
        successful_receipt_to_visibility.report();
    report.visibility_definition = "successful_receipt_to_probe_start_delay measures local observer queueing; probe_start_to_query_visibility_latency measures active polling to the first ordinary-query hit with the exact object_version; successful_receipt_to_query_visibility_latency is their end-to-end sum from successful receipt; sampled probes use non-overwritten object paths, every predetermined successful probe receipt is retained, probes rotate across definitions, observer concurrency is bounded, and polling-resolution delay is included";
    Ok(report)
}

async fn produce_mutation_jobs(
    config: &Config,
    job_tx: mpsc::Sender<MutationJob>,
    prefilled_batches: u64,
    started: Instant,
    deadline: Instant,
) -> Result<MutationProducerReport> {
    let mut report = MutationProducerReport {
        scheduled_batches: prefilled_batches,
        undispatched_at_measurement_deadline_batches: 0,
        client_queue_enqueued_batches: prefilled_batches,
        client_queue_dropped_batches: 0,
    };
    let mut sequence = prefilled_batches;
    if let Some(operation_rate) = config.target_data_operations_per_second {
        return produce_fixed_rate_jobs(
            started,
            deadline,
            config.mutation_batch_size,
            operation_rate,
            job_tx,
        )
        .await;
    } else {
        while Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, job_tx.send(MutationJob { sequence })).await {
                Ok(Ok(())) => {
                    report.scheduled_batches = report.scheduled_batches.saturating_add(1);
                    report.client_queue_enqueued_batches =
                        report.client_queue_enqueued_batches.saturating_add(1);
                    sequence = sequence
                        .checked_add(1)
                        .context("mutation sequence overflow")?;
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }
    }
    Ok(report)
}

async fn produce_fixed_rate_jobs(
    started: Instant,
    deadline: Instant,
    mutation_batch_size: usize,
    operation_rate: f64,
    job_tx: mpsc::Sender<MutationJob>,
) -> Result<MutationProducerReport> {
    // The configured rate is the user data rate. The one marker operation per
    // batch is qualification overhead and must never dilute that target.
    let batch_rate = operation_rate / mutation_batch_size as f64;
    let mut report = MutationProducerReport::default();
    let mut schedule_ordinal = 1_u64;
    loop {
        let scheduled = started + Duration::from_secs_f64(schedule_ordinal as f64 / batch_rate);
        if scheduled >= deadline {
            break;
        }
        tokio::time::sleep_until(scheduled).await;
        // Tokio timers commonly coalesce sub-millisecond deadlines. Account
        // for every schedule which is due now instead of treating timer
        // granularity as server backpressure; the bounded channel remains the
        // authority which drops work the driver cannot actually offer.
        let now = Instant::now();
        if now >= deadline {
            while started + Duration::from_secs_f64(schedule_ordinal as f64 / batch_rate) < deadline
            {
                report.scheduled_batches = report.scheduled_batches.saturating_add(1);
                report.undispatched_at_measurement_deadline_batches = report
                    .undispatched_at_measurement_deadline_batches
                    .saturating_add(1);
                schedule_ordinal = schedule_ordinal
                    .checked_add(1)
                    .context("mutation schedule overflow")?;
            }
            break;
        }
        loop {
            let scheduled = started + Duration::from_secs_f64(schedule_ordinal as f64 / batch_rate);
            if scheduled >= deadline || scheduled > now {
                break;
            }
            report.scheduled_batches = report.scheduled_batches.saturating_add(1);
            match job_tx.try_send(MutationJob {
                sequence: schedule_ordinal - 1,
            }) {
                Ok(()) => {
                    report.client_queue_enqueued_batches =
                        report.client_queue_enqueued_batches.saturating_add(1)
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    report.client_queue_dropped_batches =
                        report.client_queue_dropped_batches.saturating_add(1)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    bail!("mutation worker queue closed during fixed-rate production")
                }
            }
            schedule_ordinal = schedule_ordinal
                .checked_add(1)
                .context("mutation schedule overflow")?;
        }
    }
    Ok(report)
}

async fn execute_mutation(
    config: &Config,
    client: &mut RawClient,
    job: MutationJob,
) -> std::result::Result<MutationResult, MutationRequestFailure> {
    let started = Instant::now();
    let mut operations = Vec::with_capacity(config.mutation_batch_size + 1);
    let mut operation_payload_bytes = Vec::with_capacity(config.mutation_batch_size + 1);
    for offset in 0..config.mutation_batch_size {
        let ordinal = job
            .sequence
            .saturating_mul(config.mutation_batch_size as u64)
            .saturating_add(offset as u64);
        let id = ordinal % config.mutable_records;
        let payload = match config.mutation_workload {
            MutationWorkload::MaterialChange => data::payload_at_least(
                config.seed,
                id,
                "mutable",
                job.sequence + 1,
                config.mutation_record_bytes,
                config.physical_recipe_count,
            ),
            MutationWorkload::ProjectionPreserving => data::payload_with_generations_at_least(
                config.seed,
                id,
                "mutable",
                0,
                job.sequence + 1,
                config.mutation_record_bytes,
                config.physical_recipe_count,
            ),
        };
        operation_payload_bytes.push(payload.len() as u64);
        operations.push(put(
            config,
            data::mutable_path(id),
            payload,
            format!("contention-mutation-{}-{offset}", job.sequence),
        ));
    }
    let sample_eligible = job.sequence % config.visibility_sample_every_batches == 0;
    let marker_ordinal = marker_ordinal(config.mutation_workload, job.sequence, sample_eligible);
    let marker_id = data::marker_id(marker_ordinal);
    let marker_payload = match config.mutation_workload {
        MutationWorkload::MaterialChange => data::payload(
            config.seed,
            marker_id,
            "marker",
            job.sequence,
            config.physical_recipe_count,
        ),
        MutationWorkload::ProjectionPreserving => data::payload_with_generations(
            config.seed,
            marker_id,
            "marker",
            0,
            job.sequence,
            config.physical_recipe_count,
        ),
    };
    let probe_bytes = marker_payload.len() as u64;
    operation_payload_bytes.push(probe_bytes);
    operations.push(put(
        config,
        data::marker_path(marker_ordinal),
        marker_payload,
        format!("contention-marker-{}", job.sequence),
    ));
    let response = match client.bulk_write(BulkWriteRequest { operations }).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            return Err(MutationRequestFailure::one(
                "rpc-status",
                status.code() as i32,
                format!("{:?}", status.code()),
                status.message().to_owned(),
            ));
        }
    };
    if response.outcomes.len() != config.mutation_batch_size + 1 {
        return Err(driver_mutation_failure(
            "outcome-count-mismatch",
            format!(
                "BulkWrite returned {} outcomes for {} operations",
                response.outcomes.len(),
                config.mutation_batch_size + 1
            ),
        ));
    }
    let mut marker_version = None;
    let mut failures = Vec::new();
    let mut seen = vec![false; operation_payload_bytes.len()];
    let mut successful_data_operations = 0_u64;
    let mut successful_probe_operations = 0_u64;
    let mut failed_data_operations = 0_u64;
    let mut failed_probe_operations = 0_u64;
    let mut successful_data_payload_bytes = 0_u64;
    let mut successful_probe_payload_bytes = 0_u64;
    for outcome in response.outcomes {
        let index = match usize::try_from(outcome.index) {
            Ok(index) => index,
            Err(error) => {
                return Err(driver_mutation_failure(
                    "invalid-outcome-index",
                    format!("BulkWrite outcome index is invalid: {error}"),
                ));
            }
        };
        if index >= operation_payload_bytes.len() {
            return Err(driver_mutation_failure(
                "outcome-index-out-of-range",
                format!("BulkWrite outcome index {index} is out of range"),
            ));
        }
        if std::mem::replace(&mut seen[index], true) {
            return Err(driver_mutation_failure(
                "duplicate-outcome-index",
                format!("BulkWrite returned duplicate outcome index {index}"),
            ));
        }
        let Some(outcome) = outcome.outcome else {
            return Err(driver_mutation_failure(
                "missing-outcome",
                format!("BulkWrite outcome {index} omitted its result"),
            ));
        };
        match outcome {
            BulkOutcomeValue::Receipt(receipt) => {
                if receipt.deleted || receipt.version == 0 {
                    return Err(driver_mutation_failure(
                        "invalid-receipt",
                        format!(
                            "BulkWrite outcome {index} returned deleted={} version={}",
                            receipt.deleted, receipt.version
                        ),
                    ));
                }
                if index == config.mutation_batch_size {
                    marker_version = Some(receipt.version);
                    successful_probe_operations += 1;
                    successful_probe_payload_bytes = successful_probe_payload_bytes
                        .saturating_add(operation_payload_bytes[index]);
                } else {
                    successful_data_operations += 1;
                    successful_data_payload_bytes = successful_data_payload_bytes
                        .saturating_add(operation_payload_bytes[index]);
                }
            }
            BulkOutcomeValue::Failure(failure) => {
                if index == config.mutation_batch_size {
                    failed_probe_operations += 1;
                } else {
                    failed_data_operations += 1;
                }
                failures.push(MutationFailureClass {
                    source: "outcome",
                    code: failure.code,
                    code_name: format!("{:?}", tonic::Code::from_i32(failure.code)),
                    message: bounded_mutation_failure_message(&failure.message),
                    count: 1,
                });
            }
        }
    }
    if seen.iter().any(|seen| !seen) {
        return Err(driver_mutation_failure(
            "missing-outcome-index",
            "BulkWrite response omitted one or more operation indexes".into(),
        ));
    }
    let completed_at = Instant::now();
    Ok(MutationResult {
        successful_data_operations,
        successful_probe_operations,
        failed_data_operations,
        failed_probe_operations,
        successful_data_payload_bytes: successful_data_payload_bytes,
        successful_probe_payload_bytes: successful_probe_payload_bytes,
        failures,
        elapsed: completed_at.saturating_duration_since(started),
        completed_at,
        canary: marker_version.map(|version| Canary {
            id: marker_ordinal,
            version,
            completed_at,
            sample_eligible,
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
    let deadline = canary.completed_at + observation_timeout;
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
            return Ok(Instant::now().saturating_duration_since(canary.completed_at));
        }
        ensure!(
            Instant::now() < deadline,
            "canary {} was not visible on {index_name}",
            canary.id
        );
        tokio::time::sleep(poll).await;
    }
}

fn visibility_definition_position(sample_ordinal: u64, definition_count: usize) -> usize {
    (sample_ordinal as usize) % definition_count
}

fn marker_ordinal(workload: MutationWorkload, sequence: u64, sample_eligible: bool) -> u64 {
    match workload {
        MutationWorkload::MaterialChange => sequence,
        MutationWorkload::ProjectionPreserving if sample_eligible => {
            data::PROJECTION_PRESERVING_MARKERS.saturating_add(sequence)
        }
        MutationWorkload::ProjectionPreserving => sequence % data::PROJECTION_PRESERVING_MARKERS,
    }
}

fn bounded_error(error: &str) -> String {
    error
        .chars()
        .take(MAX_VISIBILITY_SAMPLE_ERROR_CHARS)
        .collect()
}

fn bounded_mutation_failure_message(message: &str) -> String {
    message
        .chars()
        .take(MAX_MUTATION_FAILURE_MESSAGE_CHARS)
        .collect()
}

fn driver_mutation_failure(code_name: &'static str, message: String) -> MutationRequestFailure {
    MutationRequestFailure::one("driver-validation", -1, code_name.to_owned(), message)
}

fn record_mutation_failure(report: &mut MutationReport, failure: MutationRequestFailure) {
    for class in failure.classes {
        if let Some(existing) = report.failure_classes.iter_mut().find(|existing| {
            existing.source == class.source
                && existing.code == class.code
                && existing.code_name == class.code_name
                && existing.message == class.message
        }) {
            existing.count = existing.count.saturating_add(class.count);
        } else if report.failure_classes.len() < MAX_MUTATION_FAILURE_CLASSES {
            report.failure_classes.push(class);
        } else {
            report.failure_occurrences_omitted = report
                .failure_occurrences_omitted
                .saturating_add(class.count);
        }
    }
}

async fn load_authoritative_mutable_state(
    config: &Config,
    channels: &[Channel],
    token: &str,
) -> Result<Arc<BTreeMap<String, u64>>> {
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
    Ok(Arc::new(authority))
}

async fn verify_final_mutable_state(
    config: &Config,
    names: &[String],
    channels: &[Channel],
    token: &str,
    authority: Arc<BTreeMap<String, u64>>,
) -> Result<(bool, Option<bool>, BTreeSet<u64>)> {
    let deadline = Instant::now() + config.drain_timeout;
    let mut nodes = BTreeSet::new();
    let mut all_observed_tails_available = true;
    let mut next = names.iter().cloned().enumerate();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < config.query_max_in_flight {
            let Some((position, name)) = next.next() else {
                break;
            };
            let channel = channels[position % channels.len()].clone();
            let token = token.to_owned();
            let bucket = config.bucket.clone();
            let visibility_poll = config.visibility_poll;
            let request_timeout = config.request_timeout;
            let expected_source_count = config.endpoints.len();
            let authority = authority.clone();
            tasks.spawn(async move {
                verify_one_final_definition(
                    channel,
                    token,
                    bucket,
                    name,
                    authority,
                    deadline,
                    visibility_poll,
                    request_timeout,
                    expected_source_count,
                )
                .await
            });
        }
        let Some(completed) = tasks.join_next().await else {
            break;
        };
        let (observed_tails_available, source_nodes) =
            completed.context("final mutable verification task panicked")??;
        all_observed_tails_available &= observed_tails_available;
        nodes.extend(source_nodes);
    }
    Ok((true, all_observed_tails_available.then_some(true), nodes))
}

#[allow(clippy::too_many_arguments)]
async fn verify_one_final_definition(
    channel: Channel,
    token: String,
    bucket: String,
    name: String,
    authority: Arc<BTreeMap<String, u64>>,
    deadline: Instant,
    visibility_poll: Duration,
    request_timeout: Duration,
    expected_source_count: usize,
) -> Result<(bool, BTreeSet<u64>)> {
    let mut client = index_client(channel, &token)?;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "index {name} did not converge to authoritative mutable state and zero lag"
        );
        let response = tokio::time::timeout(
            remaining.min(request_timeout),
            class_query(&mut client, &bucket, &name, "mutable", 1_000),
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
            let exact = indexed.len() == response.hits.len() && indexed == *authority;
            if let Some(freshness) = response.freshness {
                let source_ids = freshness
                    .sources
                    .iter()
                    .map(|source| source.node_id)
                    .collect::<BTreeSet<_>>();
                let healthy = freshness.initial_build_complete
                    && !freshness.rebuilding
                    && freshness.sources.len() == expected_source_count
                    && source_ids.len() == expected_source_count
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
                    return Ok((observed_tails_available, source_ids));
                }
            }
        }
        tokio::time::sleep(visibility_poll).await;
    }
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
        "probe",
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
    let marker_id = data::marker_id(sequence);
    query(
        client,
        bucket,
        index_name,
        "probe",
        serde_json::to_vec(&data::marker_probe(marker_id))?,
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

async fn fresh_token(config: &Config, channel: &Channel) -> Result<String> {
    Ok(exchange_client_credentials(
        channel.clone(),
        config.client_id.clone(),
        config.client_secret.clone(),
    )
    .await?
    .access_token)
}

fn physical_recipe(position: usize, physical_recipe_count: usize) -> usize {
    position % physical_recipe_count
}

fn qualification_definition_positions(
    definition_count: usize,
    physical_recipe_count: usize,
    maximum: usize,
) -> Vec<usize> {
    if definition_count <= maximum {
        return (0..definition_count).collect();
    }
    let mut positions = (0..physical_recipe_count).collect::<BTreeSet<_>>();
    let remaining = maximum - physical_recipe_count;
    if remaining == 0 {
        return positions.into_iter().collect();
    }
    if remaining == 1 {
        positions.insert(definition_count - 1);
        return positions.into_iter().collect();
    }
    for ordinal in 0..remaining {
        positions.insert(
            physical_recipe_count
                + ordinal.saturating_mul(definition_count - 1 - physical_recipe_count)
                    / (remaining - 1),
        );
    }
    positions.into_iter().collect()
}

fn recipe_probe_pointer(recipe: usize) -> String {
    format!("/probes/{recipe:02}")
}

fn unix_millis() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}
