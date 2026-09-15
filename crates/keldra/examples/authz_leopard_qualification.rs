//! Public-API qualification for persistent Leopard userset indexing.
//!
//! The workload creates disjoint deep group trees, evaluates large exact-
//! revision batches, mutates and restores one canonical leaf edge, and checks
//! every configured replica endpoint. `prepare-rebuild` and `verify-rebuild`
//! split the same deterministic oracle around an externally controlled process
//! restart, replica replacement, or deletion of only disposable Leopard state.
//! The authority tuple column family must never be edited by that controller.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use keldra_storage::v1::authz_consistency::Requirement;
use keldra_storage::v1::{
    AuthzConsistency, AuthzScope, BindSchemaRequest, CheckPermissionsRequest, ExactRevision,
    MutateTuplesRequest, PermissionCheck, PutSchemaRequest,
};
use keldra_storage::{RawAuthzClient, authz_client, connect_channel, exchange_client_credentials};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio::time::Instant;

#[path = "authz_leopard_qualification/config.rs"]
mod config;
#[path = "authz_leopard_qualification/evidence.rs"]
mod evidence;
#[path = "authz_leopard_qualification/graph.rs"]
mod graph;
#[path = "authz_leopard_qualification/metrics.rs"]
mod metrics;

use config::{Config, Phase};
use evidence::ResourceEvidence;
use graph::{ExpectedCheck, Graph};
use metrics::{Latencies, LatencyReport};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RebuildState {
    schema: String,
    storage_tenant: String,
    realm: String,
    schema_id: String,
    roots: usize,
    depth: usize,
    fanout: usize,
    revision: u64,
}

#[derive(Debug, Serialize)]
struct GraphReport {
    roots: usize,
    depth: usize,
    fanout: usize,
    groups: usize,
    users: usize,
    canonical_direct_edges: usize,
    positive_oracle_checks: usize,
    negative_oracle_checks: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkReport {
    offered_batch_requests: u64,
    accepted_batch_requests: u64,
    failed_batch_requests: u64,
    offered_checks: u64,
    accepted_checks: u64,
    returned_evaluations: u64,
    correctness_failures: u64,
    elapsed_seconds: f64,
    accepted_batch_requests_per_second: f64,
    accepted_checks_per_second: f64,
    evaluation_results_per_second: f64,
    schedule_to_response_latency: LatencyReport,
}

#[derive(Debug, Serialize)]
struct CorrectnessReport {
    passed: bool,
    exact_revision_used_for_every_batch: bool,
    input_order_preserved: bool,
    nested_positive_and_negative_oracle_passed: bool,
    tuple_removal_became_denied: bool,
    unaffected_tree_remained_allowed: bool,
    stale_exact_revision_rejected_after_churn: bool,
    tuple_restore_became_allowed: bool,
    every_replica_endpoint_returned_the_exact_revision: bool,
    rebuild_verification_passed: Option<bool>,
}

#[derive(Debug, Serialize)]
struct QualificationReport {
    schema: &'static str,
    started_unix_milliseconds: u128,
    completed_unix_milliseconds: u128,
    result: &'static str,
    phase: Phase,
    configuration: Config,
    graph: GraphReport,
    authoritative_revision: u64,
    benchmark: Option<BenchmarkReport>,
    correctness: CorrectnessReport,
    resources_and_internal_work: Option<ResourceEvidence>,
    rate_definitions: RateDefinitions,
}

#[derive(Debug, Serialize)]
struct RateDefinitions {
    accepted_batch_requests_per_second: &'static str,
    accepted_checks_per_second: &'static str,
    evaluation_results_per_second: &'static str,
    latency: &'static str,
    internal_work: &'static str,
}

#[derive(Debug)]
struct BatchOutcome {
    latency: Duration,
    checks: usize,
    revision: u64,
    correctness_failures: u64,
}

#[derive(Default)]
struct ChurnEvidence {
    removed_denied: bool,
    unaffected_allowed: bool,
    stale_revision_rejected: bool,
    restored_allowed: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let started_unix_milliseconds = now_millis()?;
    let config = Config::from_env()?;
    let graph = graph::build(config.roots, config.depth, config.fanout)?;
    let mut clients = connect_clients(&config).await?;

    let (revision, benchmark, churn, replica_passed, rebuild_passed) = match config.phase {
        Phase::VerifyRebuild => {
            let state = read_state(&config.state_path)?;
            validate_state(&config, &state)?;
            let replica_passed =
                verify_every_endpoint(&mut clients, scope(&config), state.revision, &graph.checks)
                    .await?;
            (
                state.revision,
                None,
                ChurnEvidence::default(),
                replica_passed,
                Some(replica_passed),
            )
        }
        Phase::Full | Phase::PrepareRebuild => {
            let mut revision = create_graph(&mut clients[0], &config, &graph).await?;
            let replica_passed =
                verify_every_endpoint(&mut clients, scope(&config), revision, &graph.checks)
                    .await?;
            let benchmark = if config.phase == Phase::Full {
                Some(run_benchmark(&clients, &config, revision, &graph.checks).await?)
            } else {
                None
            };
            let churn = if config.phase == Phase::Full {
                let (next_revision, evidence) =
                    qualify_incremental_churn(&mut clients[0], &config, revision, &graph).await?;
                revision = next_revision;
                evidence
            } else {
                ChurnEvidence::default()
            };
            if config.phase == Phase::PrepareRebuild {
                write_state(
                    &config.state_path,
                    &RebuildState {
                        schema: "keldra.authz-leopard-rebuild-state.v1".into(),
                        storage_tenant: config.storage_tenant.clone(),
                        realm: config.realm.clone(),
                        schema_id: config.schema_id.clone(),
                        roots: config.roots,
                        depth: config.depth,
                        fanout: config.fanout,
                        revision,
                    },
                )?;
            }
            (revision, benchmark, churn, replica_passed, None)
        }
    };

    let resources = ResourceEvidence::load(
        config.resource_evidence_path.as_deref(),
        config.require_internal_telemetry,
    )?;
    let benchmark_passed = benchmark.as_ref().is_none_or(|report| {
        report.failed_batch_requests == 0
            && report.correctness_failures == 0
            && report.accepted_checks == report.offered_checks
            && report.returned_evaluations == report.accepted_checks
    });
    let churn_passed = config.phase != Phase::Full
        || (churn.removed_denied
            && churn.unaffected_allowed
            && churn.stale_revision_rejected
            && churn.restored_allowed);
    let correctness = CorrectnessReport {
        passed: benchmark_passed && churn_passed && replica_passed,
        exact_revision_used_for_every_batch: benchmark_passed,
        input_order_preserved: benchmark_passed,
        nested_positive_and_negative_oracle_passed: benchmark_passed && replica_passed,
        tuple_removal_became_denied: churn.removed_denied,
        unaffected_tree_remained_allowed: churn.unaffected_allowed,
        stale_exact_revision_rejected_after_churn: churn.stale_revision_rejected,
        tuple_restore_became_allowed: churn.restored_allowed,
        every_replica_endpoint_returned_the_exact_revision: replica_passed,
        rebuild_verification_passed: rebuild_passed,
    };
    let passed = correctness.passed && (!config.require_internal_telemetry || resources.is_some());
    let graph_report = GraphReport {
        roots: config.roots,
        depth: config.depth,
        fanout: config.fanout,
        groups: graph.groups,
        users: graph.users,
        canonical_direct_edges: graph.direct_edges,
        positive_oracle_checks: graph.checks.iter().filter(|check| check.allowed).count(),
        negative_oracle_checks: graph.checks.iter().filter(|check| !check.allowed).count(),
    };
    let report = QualificationReport {
        schema: "keldra.authz-leopard-qualification.v1",
        started_unix_milliseconds,
        completed_unix_milliseconds: now_millis()?,
        result: if passed { "pass" } else { "fail" },
        phase: config.phase,
        configuration: config,
        graph: graph_report,
        authoritative_revision: revision,
        benchmark,
        correctness,
        resources_and_internal_work: resources,
        rate_definitions: RateDefinitions {
            accepted_batch_requests_per_second: "successful CheckPermissions RPCs divided by the non-overlapping benchmark dispatch-to-terminal wall interval",
            accepted_checks_per_second: "ordered PermissionResult values returned by successful RPCs divided by the same wall interval",
            evaluation_results_per_second: "server-returned authorization decisions divided by the same wall interval; internal recursive steps are reported separately by Leopard telemetry",
            latency: "HDR histogram of client schedule-to-terminal-response time for each CheckPermissions RPC",
            internal_work: "exact OTLP counter deltas over the benchmark window; client-side graph size is never substituted for visited usersets, loaded edges, or DB prefix reads",
        },
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    ensure!(passed, "Leopard authorization qualification failed");
    Ok(())
}

async fn connect_clients(config: &Config) -> Result<Vec<RawAuthzClient>> {
    let mut channels = Vec::with_capacity(config.endpoints.len());
    for endpoint in &config.endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let token = exchange_client_credentials(
        channels[0].clone(),
        config.client_id.clone(),
        config.client_secret.clone(),
    )
    .await?
    .access_token;
    let mut clients = Vec::with_capacity(channels.len());
    for channel in channels {
        clients.push(authz_client(channel, &token)?);
    }
    Ok(clients)
}

async fn create_graph(client: &mut RawAuthzClient, config: &Config, graph: &Graph) -> Result<u64> {
    let published = client
        .put_schema(PutSchemaRequest {
            schema_id: config.schema_id.clone(),
            namespaces: graph::schema(),
        })
        .await?
        .into_inner();
    let schema_ref = published
        .schema_ref
        .context("PutSchema omitted schema reference")?;
    let bound = client
        .bind_schema(BindSchemaRequest {
            scope: Some(scope(config)),
            schema_ref: Some(schema_ref),
            expected_binding_generation: None,
        })
        .await?
        .into_inner();
    let mut revision = bound.revision;
    for (batch, mutations) in graph.mutations.chunks(1_000).enumerate() {
        let response = client
            .mutate_tuples(MutateTuplesRequest {
                scope: Some(scope(config)),
                operation_id: format!("leopard-load-{batch}"),
                expected_revision: Some(revision),
                mutations: mutations.to_vec(),
            })
            .await?
            .into_inner();
        revision = response.revision;
    }
    Ok(revision)
}

async fn run_benchmark(
    clients: &[RawAuthzClient],
    config: &Config,
    revision: u64,
    oracle: &[ExpectedCheck],
) -> Result<BenchmarkReport> {
    let checks = Arc::new(repeated_batch(oracle, config.checks_per_batch));
    let started = Instant::now();
    let mut jobs = JoinSet::new();
    let mut next = 0_usize;
    let mut accepted_batches = 0_u64;
    let mut accepted_checks = 0_u64;
    let mut failed_batches = 0_u64;
    let mut correctness_failures = 0_u64;
    let mut latencies = Latencies::new()?;

    while next < config.benchmark_batches || !jobs.is_empty() {
        while next < config.benchmark_batches && jobs.len() < config.max_in_flight_batches {
            let client = clients[next % clients.len()].clone();
            let scope = scope(config);
            let checks = checks.clone();
            let scheduled = Instant::now();
            jobs.spawn(
                async move { execute_batch(client, scope, revision, checks, scheduled).await },
            );
            next += 1;
        }
        let Some(result) = jobs.join_next().await else {
            break;
        };
        match result.context("join Leopard check batch")? {
            Ok(outcome) => {
                latencies.record(outcome.latency)?;
                accepted_batches += 1;
                accepted_checks += outcome.checks as u64;
                correctness_failures += outcome.correctness_failures;
                if outcome.revision != revision {
                    correctness_failures += 1;
                }
            }
            Err(_) => failed_batches += 1,
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let offered_checks = config
        .benchmark_batches
        .checked_mul(config.checks_per_batch)
        .context("offered check count overflow")? as u64;
    Ok(BenchmarkReport {
        offered_batch_requests: config.benchmark_batches as u64,
        accepted_batch_requests: accepted_batches,
        failed_batch_requests: failed_batches,
        offered_checks,
        accepted_checks,
        returned_evaluations: accepted_checks,
        correctness_failures,
        elapsed_seconds: elapsed,
        accepted_batch_requests_per_second: rate(accepted_batches, elapsed),
        accepted_checks_per_second: rate(accepted_checks, elapsed),
        evaluation_results_per_second: rate(accepted_checks, elapsed),
        schedule_to_response_latency: latencies.report(),
    })
}

async fn execute_batch(
    mut client: RawAuthzClient,
    scope: AuthzScope,
    revision: u64,
    checks: Arc<Vec<ExpectedCheck>>,
    scheduled: Instant,
) -> Result<BatchOutcome> {
    let response = client
        .check_permissions(CheckPermissionsRequest {
            scope: Some(scope),
            checks: checks
                .iter()
                .map(|expected| expected.check.clone())
                .collect(),
            consistency: Some(exact(revision)),
        })
        .await?
        .into_inner();
    ensure!(
        response.results.len() == checks.len(),
        "CheckPermissions changed result cardinality"
    );
    let correctness_failures = response
        .results
        .iter()
        .zip(checks.iter())
        .filter(|(actual, expected)| actual.allowed != expected.allowed)
        .count() as u64;
    Ok(BatchOutcome {
        latency: scheduled.elapsed(),
        checks: response.results.len(),
        revision: response.revision,
        correctness_failures,
    })
}

async fn qualify_incremental_churn(
    client: &mut RawAuthzClient,
    config: &Config,
    before_revision: u64,
    graph: &Graph,
) -> Result<(u64, ChurnEvidence)> {
    let removed = mutate_one(
        client,
        config,
        "leopard-churn-remove",
        before_revision,
        graph.churn_remove.clone(),
    )
    .await?;
    let removed_denied = check_one(
        client,
        config,
        removed,
        graph::permission_check(graph.churned_user.clone(), graph.churned_document.clone()),
    )
    .await?
    .is_some_and(|allowed| !allowed);
    let unaffected_allowed = check_one(
        client,
        config,
        removed,
        graph::permission_check(
            graph.unaffected_user.clone(),
            graph.unaffected_document.clone(),
        ),
    )
    .await?
    .is_some_and(|allowed| allowed);
    let stale_revision_rejected = check_one(
        client,
        config,
        before_revision,
        graph::permission_check(graph.churned_user.clone(), graph.churned_document.clone()),
    )
    .await
    .is_err();
    let restored = mutate_one(
        client,
        config,
        "leopard-churn-restore",
        removed,
        graph.churn_restore.clone(),
    )
    .await?;
    let restored_allowed = check_one(
        client,
        config,
        restored,
        graph::permission_check(graph.churned_user.clone(), graph.churned_document.clone()),
    )
    .await?
    .is_some_and(|allowed| allowed);
    Ok((
        restored,
        ChurnEvidence {
            removed_denied,
            unaffected_allowed,
            stale_revision_rejected,
            restored_allowed,
        },
    ))
}

async fn mutate_one(
    client: &mut RawAuthzClient,
    config: &Config,
    operation_id: &str,
    revision: u64,
    mutation: keldra_storage::v1::TupleMutation,
) -> Result<u64> {
    Ok(client
        .mutate_tuples(MutateTuplesRequest {
            scope: Some(scope(config)),
            operation_id: operation_id.into(),
            expected_revision: Some(revision),
            mutations: vec![mutation],
        })
        .await?
        .into_inner()
        .revision)
}

async fn check_one(
    client: &mut RawAuthzClient,
    config: &Config,
    revision: u64,
    check: PermissionCheck,
) -> Result<Option<bool>> {
    let response = client
        .check_permissions(CheckPermissionsRequest {
            scope: Some(scope(config)),
            checks: vec![check],
            consistency: Some(exact(revision)),
        })
        .await?
        .into_inner();
    ensure!(
        response.revision == revision,
        "authorization revision changed"
    );
    ensure!(
        response.results.len() == 1,
        "single check returned wrong cardinality"
    );
    Ok(response.results.first().map(|result| result.allowed))
}

async fn verify_every_endpoint(
    clients: &mut [RawAuthzClient],
    scope: AuthzScope,
    revision: u64,
    checks: &[ExpectedCheck],
) -> Result<bool> {
    let sample = repeated_batch(checks, checks.len().min(1_000));
    for client in clients {
        let outcome = execute_batch(
            client.clone(),
            scope.clone(),
            revision,
            Arc::new(sample.clone()),
            Instant::now(),
        )
        .await?;
        if outcome.revision != revision || outcome.correctness_failures != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn repeated_batch(oracle: &[ExpectedCheck], count: usize) -> Vec<ExpectedCheck> {
    let positives = oracle
        .iter()
        .filter(|check| check.allowed)
        .cloned()
        .collect::<Vec<_>>();
    let negatives = oracle
        .iter()
        .filter(|check| !check.allowed)
        .cloned()
        .collect::<Vec<_>>();
    (0..count)
        .map(|position| {
            if position % 10 == 9 && !negatives.is_empty() {
                negatives[(position / 10) % negatives.len()].clone()
            } else {
                positives[position % positives.len()].clone()
            }
        })
        .collect()
}

fn scope(config: &Config) -> AuthzScope {
    AuthzScope {
        storage_tenant: config.storage_tenant.clone(),
        realm: config.realm.clone(),
    }
}

fn exact(revision: u64) -> AuthzConsistency {
    AuthzConsistency {
        requirement: Some(Requirement::Exact(ExactRevision { revision })),
    }
}

fn write_state(path: &Path, state: &RebuildState) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(state)?;
    fs::write(path, bytes).with_context(|| format!("write rebuild state {}", path.display()))
}

fn read_state(path: &Path) -> Result<RebuildState> {
    let bytes = fs::read(path).with_context(|| format!("read rebuild state {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("decode rebuild state {}", path.display()))
}

fn validate_state(config: &Config, state: &RebuildState) -> Result<()> {
    ensure!(
        state.schema == "keldra.authz-leopard-rebuild-state.v1",
        "wrong rebuild state schema"
    );
    ensure!(
        state.storage_tenant == config.storage_tenant,
        "rebuild tenant changed"
    );
    ensure!(state.realm == config.realm, "rebuild realm changed");
    ensure!(
        state.schema_id == config.schema_id,
        "rebuild schema changed"
    );
    ensure!(state.roots == config.roots, "rebuild roots changed");
    ensure!(state.depth == config.depth, "rebuild depth changed");
    ensure!(state.fanout == config.fanout, "rebuild fanout changed");
    ensure!(state.revision > 0, "rebuild revision is zero");
    Ok(())
}

fn rate(count: u64, seconds: f64) -> f64 {
    if seconds > 0.0 {
        count as f64 / seconds
    } else {
        0.0
    }
}

fn now_millis() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_batch_contains_stable_positive_and_negative_oracles() {
        let graph = graph::build(1, 1, 2).unwrap();
        let first = repeated_batch(&graph.checks, 100);
        let second = repeated_batch(&graph.checks, 100);
        assert_eq!(
            first.iter().map(|check| check.allowed).collect::<Vec<_>>(),
            second.iter().map(|check| check.allowed).collect::<Vec<_>>()
        );
        assert_eq!(first.iter().filter(|check| check.allowed).count(), 90);
        assert_eq!(first.iter().filter(|check| !check.allowed).count(), 10);
    }

    #[test]
    fn exact_consistency_never_silently_becomes_latest() {
        let consistency = exact(42);
        assert!(matches!(
            consistency.requirement,
            Some(Requirement::Exact(ExactRevision { revision: 42 }))
        ));
    }
}
