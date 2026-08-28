//! Production-shaped public-API qualification for Keldra's 0.9 native segment builder.
//!
//! The generated corpus deliberately resembles a broad, small-JSON indexing
//! workload without embedding any private schema or source data.

#[path = "v06_index_resource_qualification/data.rs"]
mod data;
#[path = "v06_index_resource_qualification/incident.rs"]
mod incident;
#[path = "v06_index_resource_qualification/resource.rs"]
mod resource;
#[path = "v06_index_resource_qualification/singleton_probe.rs"]
mod singleton_probe;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use data::{PARTITION_COUNT, RecordFlavor};
use keldra_storage::v1::bulk_operation::Operation as BulkOperationValue;
use keldra_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use keldra_storage::v1::index_query::Query as QueryValue;
use keldra_storage::v1::index_service_client::IndexServiceClient;
use keldra_storage::v1::{
    BulkOperation, BulkPutRequest, BulkWriteRequest, CreateBucketRequest, CreateIndexRequest,
    DeleteRequest, Durability, IndexFreshness, IndexPredicate, IndexPredicateExpression,
    IndexPredicateOperator, IndexQuery, IndexSourceFreshness, ObjectAddress, ObjectVersioning,
    QueryIndexRequest, QueryIndexResponse, TypedJsonIndexQuery,
};
use keldra_storage::{
    BearerToken, BooleanField, FloatField, KeywordField, RawClient, TypedJsonIndexBuilder,
    UnsignedIntegerField, administration_client, connect_channel, exchange_client_credentials,
    object_client,
};
use resource::{Phase, ResourceMonitor, ResourceReport};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const CONTENT_TYPE: &str = "application/json";
const DEFAULT_RECORDS: u64 = 839_980;
const DEFAULT_MUTATIONS: u64 = 2_048;
const DEFAULT_BATCH_SIZE: usize = 1_000;
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_VERIFICATION_WORKERS: usize = 8;
const DEFAULT_SEED: u64 = 0x625d_54af_f989_97f3;
const QUERY_LIMIT: u32 = 1_000;
// Generated records use `record_id % PARTITION_COUNT`, so this partition is
// guaranteed empty and can carry freshness without scanning a real result set.
const FRESHNESS_PROBE_PARTITION: u64 = PARTITION_COUNT;
const BUILD_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const EXACT_VERIFICATION_TIMEOUT: Duration = Duration::from_secs(45 * 60);
const MIN_RELEASE_INGEST_OBJECTS_PER_SECOND: f64 = 3_000.0;
const MIN_RELEASE_INDEX_OBJECTS_PER_SECOND: f64 = 1_000.0;

#[derive(Clone, Debug)]
struct Config {
    endpoints: Vec<String>,
    tenant: Arc<str>,
    bucket: Arc<str>,
    client_id: String,
    client_secret: String,
    records: u64,
    mutation_count: u64,
    batch_size: usize,
    workers: usize,
    verification_workers: usize,
    seed: u64,
    resource_pids: Vec<u32>,
    resource_containers: Vec<String>,
    require_resource_targets: bool,
    configured_kind_budget_bytes: Option<u64>,
    configured_compaction_max_lanes: Option<usize>,
    configured_projection_max_lanes: Option<usize>,
    configured_rayon_workers: Option<usize>,
    max_anonymous_growth_bytes: Option<u64>,
    require_performance_targets: bool,
    evidence: EvidenceConfig,
    output: Option<PathBuf>,
    state_output: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct EvidenceConfig {
    source_commit: String,
    resolved_container_digest: String,
    native_architecture: String,
    container_platform: String,
    topology: String,
    node_count: usize,
    hardware_logical_cpus: usize,
    hardware_memory_bytes: u64,
    qualification_filesystem_total_bytes: u64,
    qualification_filesystem_available_bytes_at_start: u64,
    index_disk_cache_bytes_per_node: u64,
    index_memory_percent_per_node: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct Timings {
    ingest_seconds: f64,
    initial_build_seconds: f64,
    first_complete_commit_revision_seconds: f64,
    exact_verification_seconds: f64,
    cold_query_milliseconds: f64,
    warm_query_milliseconds: f64,
    mutation_seconds: f64,
    incremental_build_seconds: f64,
}

#[derive(Debug, Clone, Serialize)]
struct QualificationReport {
    schema: &'static str,
    records: u64,
    indexed_fields: usize,
    partitions: u64,
    ingest_workers: usize,
    verification_workers: usize,
    batch_size: usize,
    updated_objects: u64,
    deleted_objects: u64,
    final_live_objects: u64,
    configured_kind_budget_bytes: Option<u64>,
    configured_compaction_max_lanes: Option<usize>,
    configured_projection_max_lanes: Option<usize>,
    configured_rayon_workers: Option<usize>,
    max_anonymous_growth_bytes: Option<u64>,
    observed_peak_rss_growth_bytes: Option<u64>,
    observed_peak_anonymous_growth_bytes: Option<u64>,
    accepted_objects_per_second: f64,
    source_complete_objects_per_second: f64,
    initial_commit_revision: u64,
    final_commit_revision: u64,
    timings: Timings,
    production_query_regression: incident::IncidentReport,
    resources: Option<ResourceReport>,
    evidence: QualificationEvidence,
}

#[derive(Debug, Clone, Serialize)]
struct QualificationEvidence {
    source_commit: String,
    resolved_container_digest: String,
    native_architecture: String,
    container_platform: String,
    hardware: HardwareEvidence,
    corpus: CorpusEvidence,
    topology: TopologyEvidence,
    durability: DurabilityEvidence,
    execution: ExecutionEvidence,
    resource_configuration: ResourceConfigurationEvidence,
    timer_boundaries: TimerBoundaryEvidence,
    correctness: CorrectnessEvidence,
}

#[derive(Debug, Clone, Serialize)]
struct HardwareEvidence {
    logical_cpus: usize,
    memory_bytes: u64,
    qualification_filesystem_total_bytes: u64,
    qualification_filesystem_available_bytes_at_start: u64,
}

#[derive(Debug, Clone, Serialize)]
struct CorpusEvidence {
    identity: &'static str,
    initial_corpus_sha256: String,
    generator_seed_hex: String,
    records: u64,
    indexed_fields: usize,
}

#[derive(Debug, Clone, Serialize)]
struct TopologyEvidence {
    kind: String,
    node_count: usize,
    ingress_endpoint_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct DurabilityEvidence {
    initial_writes: &'static str,
    updates: &'static str,
    deletes: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct ExecutionEvidence {
    bulk_write_max_operations: usize,
    ingest_workers: usize,
    verification_workers: usize,
    partition_count: u64,
    query_limit: u32,
    configured_mutations: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ResourceConfigurationEvidence {
    index_disk_cache_bytes_per_node: u64,
    index_memory_percent_per_node: u64,
    builder_memory_bytes_per_kind_per_node: u64,
    compaction_max_lanes_per_kind: usize,
    projection_max_lanes_per_kind: usize,
    rayon_workers_per_node: usize,
    maximum_anonymous_growth_bytes: u64,
    resource_sample_interval_milliseconds: u64,
    monitored_target_count: usize,
    resource_targets_required: bool,
}

#[derive(Debug, Clone, Serialize)]
struct TimerBoundary {
    starts: &'static str,
    stops: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct TimerBoundaryEvidence {
    clock: &'static str,
    ingest_seconds: TimerBoundary,
    initial_build_seconds: TimerBoundary,
    first_complete_commit_revision_seconds: TimerBoundary,
    exact_verification_seconds: TimerBoundary,
    cold_query_milliseconds: TimerBoundary,
    warm_query_milliseconds: TimerBoundary,
    mutation_seconds: TimerBoundary,
    incremental_build_seconds: TimerBoundary,
}

#[derive(Debug, Clone, Serialize)]
struct CorrectnessEvidence {
    result: &'static str,
    source_complete_commit_revision_observed: bool,
    source_complete_sources_observed: usize,
    initial_exact_partition_verification: bool,
    final_exact_partition_verification: bool,
    update_and_delete_verification: bool,
    resource_limits_passed: bool,
    performance_targets_required: bool,
    performance_targets_passed: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct VerificationState {
    schema: String,
    tenant: String,
    bucket: String,
    records: u64,
    final_live_objects: u64,
    final_commit_revision: u64,
    final_result_sha256: String,
    source_count: usize,
}

#[derive(Debug)]
struct BatchResult {
    accepted: u64,
    receipts: Vec<(u64, u64)>,
}

#[derive(Clone, Copy, Debug)]
enum MutationMode {
    Initial,
    Update,
    Delete,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if let Some(path) = env::var_os("KELDRA_V09_RESOURCE_SINGLETON_PROBE_STATE_INPUT") {
        return singleton_probe::run(Path::new(&path)).await;
    }
    if let Some(path) = env::var_os("KELDRA_V06_RESOURCE_STATE_INPUT") {
        return verify_existing_state(Path::new(&path)).await;
    }
    let config = Config::from_env()?;
    // Hashing is intentionally outside every measured timer and resource
    // phase. It identifies the exact deterministic input without changing the
    // workload whose performance and memory use are under qualification.
    let initial_corpus_sha256 = initial_corpus_sha256(config.seed, config.records);
    let channels = connect_all(&config.endpoints).await?;
    let mut token = qualification_token(&config, channels[0].clone()).await?;

    let mut admin = administration_client(channels[0].clone(), &token)?;
    admin
        .create_bucket(CreateBucketRequest {
            bucket: config.bucket.to_string(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await
        .context("create qualification bucket")?;

    let mut index = index_client(channels[0].clone(), &token)?;
    let definition = index
        .create_index(qualification_index_request(&config.bucket)?)
        .await
        .context("create qualification index")?
        .into_inner();
    ensure!(definition.index_id != 0 && definition.version != 0);

    // Every independently bounded polling loop gets a fresh one-hour token.
    // A slow-but-valid loop can consume its full 30-minute allowance, so
    // sharing one token between two loops would recreate an accidental
    // one-hour qualification deadline.
    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    let baseline = wait_for_commit_revision(&mut index, &config.bucket, 0, None, None).await?;
    token = qualification_token(&config, channels[0].clone()).await?;
    let monitor = ResourceMonitor::start(
        &config.resource_pids,
        &config.resource_containers,
        Duration::from_millis(100),
        config.require_resource_targets,
    )?;

    let mut timings = Timings::default();
    set_phase(&monitor, Phase::Ingest);
    // This starts before the first request, so the measured time to a complete
    // commit_revision is conservatively no shorter than the RFC's first-accepted
    // object boundary.
    let first_object_started = Instant::now();
    let started = first_object_started;
    let initial = write_ranges(
        &config,
        &channels,
        &token,
        0,
        config.records,
        MutationMode::Initial,
        true,
    )
    .await?;
    timings.ingest_seconds = started.elapsed().as_secs_f64();
    ensure!(initial.accepted == config.records);
    let mut initial_versions = vec![0_u64; usize::try_from(config.records)?];
    for (record_id, version) in initial.receipts {
        let slot = initial_versions
            .get_mut(usize::try_from(record_id)?)
            .context("initial receipt was outside the generated corpus")?;
        ensure!(*slot == 0, "duplicate initial receipt");
        *slot = version;
    }
    ensure!(initial_versions.iter().all(|version| *version != 0));

    // A production-sized ingest can consume a substantial part of the
    // one-hour access-token lifetime. Start the independently bounded build
    // qualification with fresh credentials so authentication cannot become
    // its accidental wall-clock limit.
    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    set_phase(&monitor, Phase::InitialBuild);
    let started = Instant::now();
    let initial_response = wait_for_commit_revision(
        &mut index,
        &config.bucket,
        baseline.commit_revision(),
        Some(data::partition_paths(config.records, 7).len()),
        Some(config.endpoints.len()),
    )
    .await?;
    timings.initial_build_seconds = started.elapsed().as_secs_f64();
    timings.first_complete_commit_revision_seconds = first_object_started.elapsed().as_secs_f64();
    let initial_ready_commit_revision = freshness(&initial_response)?.commit_revision;
    let source_complete_commit_revision_observed =
        source_complete_freshness(freshness(&initial_response)?, config.endpoints.len());
    ensure!(
        source_complete_commit_revision_observed,
        "first complete commit_revision did not prove zero lag for every source"
    );
    let accepted_objects_per_second = config.records as f64 / timings.ingest_seconds;
    let source_complete_objects_per_second =
        config.records as f64 / timings.first_complete_commit_revision_seconds;
    if config.require_performance_targets {
        ensure!(
            accepted_objects_per_second >= MIN_RELEASE_INGEST_OBJECTS_PER_SECOND,
            "accepted object rate {accepted_objects_per_second:.3}/s is below the release target {MIN_RELEASE_INGEST_OBJECTS_PER_SECOND:.3}/s"
        );
        ensure!(
            source_complete_objects_per_second >= MIN_RELEASE_INDEX_OBJECTS_PER_SECOND,
            "source-complete index rate {source_complete_objects_per_second:.3}/s is below the release target {MIN_RELEASE_INDEX_OBJECTS_PER_SECOND:.3}/s"
        );
    }

    set_phase(&monitor, Phase::ColdQuery);
    let started = Instant::now();
    let cold = query_partition(&mut index, &config.bucket, 7).await?;
    timings.cold_query_milliseconds = started.elapsed().as_secs_f64() * 1_000.0;
    validate_partition_response(&cold, config.records, 7, &initial_versions, None, None)?;

    set_phase(&monitor, Phase::WarmQuery);
    let started = Instant::now();
    let warm = query_partition(&mut index, &config.bucket, 7).await?;
    timings.warm_query_milliseconds = started.elapsed().as_secs_f64() * 1_000.0;
    ensure!(hits_by_path(&cold)? == hits_by_path(&warm)?);

    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    set_phase(&monitor, Phase::IncidentQuery);
    let production_query_regression = incident::run(
        &mut index,
        &config.bucket,
        config.seed,
        config.records,
        &initial_versions,
        config.endpoints.len(),
    )
    .await?;

    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    set_phase(&monitor, Phase::WarmQuery);
    let started = Instant::now();
    let (initial_count, initial_commit_revision, _) = verify_every_partition(
        &index,
        &config.bucket,
        config.records,
        &initial_versions,
        None,
        None,
        config.verification_workers,
    )
    .await?;
    timings.exact_verification_seconds = started.elapsed().as_secs_f64();
    ensure!(initial_count == config.records);
    ensure!(initial_commit_revision >= initial_ready_commit_revision);

    // Mutation receives its own fresh access token. The independently bounded
    // incremental polling and verification loops renew again below.
    token = qualification_token(&config, channels[0].clone()).await?;

    let mutation_count = config.mutation_count.min(config.records / 2);
    let update_start = config.records.saturating_sub(mutation_count);
    let delete_end = update_start;
    let delete_start = delete_end.saturating_sub(mutation_count);

    set_phase(&monitor, Phase::Mutation);
    let started = Instant::now();
    let updates = write_ranges(
        &config,
        &channels,
        &token,
        update_start,
        config.records,
        MutationMode::Update,
        true,
    )
    .await?;
    let deletes = write_ranges(
        &config,
        &channels,
        &token,
        delete_start,
        delete_end,
        MutationMode::Delete,
        false,
    )
    .await?;
    timings.mutation_seconds = started.elapsed().as_secs_f64();
    ensure!(updates.accepted == mutation_count);
    ensure!(deletes.accepted == mutation_count);
    let expected_updates = updates.receipts.into_iter().collect::<BTreeMap<_, _>>();
    let deleted = (delete_start..delete_end).collect::<BTreeSet<_>>();

    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    set_phase(&monitor, Phase::IncrementalBuild);
    let started = Instant::now();
    let final_live = config.records - deleted.len() as u64;
    let _final_ready = wait_for_commit_revision(
        &mut index,
        &config.bucket,
        initial_commit_revision,
        None,
        Some(config.endpoints.len()),
    )
    .await?;
    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    let (final_count, final_commit_revision, final_result_sha256) = verify_every_partition(
        &index,
        &config.bucket,
        config.records,
        &initial_versions,
        Some(&expected_updates),
        Some(&deleted),
        config.verification_workers,
    )
    .await?;
    timings.incremental_build_seconds = started.elapsed().as_secs_f64();
    ensure!(final_count == final_live);

    let resources = match monitor {
        Some(monitor) => Some(monitor.finish().await),
        None => None,
    };
    if config.require_resource_targets {
        let resources = resources
            .as_ref()
            .context("required resource monitor was not started")?;
        ensure!(
            resources.final_reading.sampled_processes == resources.targets.len()
                && resources
                    .peaks
                    .values()
                    .all(|peak| peak.minimum_sampled_processes == resources.targets.len()),
            "one or more monitored Keldra processes disappeared during qualification"
        );
        let maximum_growth = config
            .max_anonymous_growth_bytes
            .context("required anonymous-memory growth limit was not configured")?;
        ensure!(
            resources.peak_anonymous_growth_bytes() <= maximum_growth,
            "anonymous memory grew by {} bytes, above the configured {}-byte qualification limit",
            resources.peak_anonymous_growth_bytes(),
            maximum_growth,
        );
    }
    let observed_peak_rss_growth_bytes = resources
        .as_ref()
        .map(ResourceReport::peak_rss_growth_bytes);
    let observed_peak_anonymous_growth_bytes = resources
        .as_ref()
        .map(ResourceReport::peak_anonymous_growth_bytes);
    let evidence = qualification_evidence(
        &config,
        initial_corpus_sha256,
        source_complete_commit_revision_observed,
    )?;
    let report = QualificationReport {
        schema: "keldra.index-resource-qualification.v2",
        records: config.records,
        indexed_fields: data::FIELD_COUNT,
        partitions: PARTITION_COUNT,
        ingest_workers: config.workers,
        verification_workers: config.verification_workers,
        batch_size: config.batch_size,
        updated_objects: expected_updates.len() as u64,
        deleted_objects: deleted.len() as u64,
        final_live_objects: final_live,
        configured_kind_budget_bytes: config.configured_kind_budget_bytes,
        configured_compaction_max_lanes: config.configured_compaction_max_lanes,
        configured_projection_max_lanes: config.configured_projection_max_lanes,
        configured_rayon_workers: config.configured_rayon_workers,
        max_anonymous_growth_bytes: config.max_anonymous_growth_bytes,
        observed_peak_rss_growth_bytes,
        observed_peak_anonymous_growth_bytes,
        accepted_objects_per_second,
        source_complete_objects_per_second,
        initial_commit_revision,
        final_commit_revision,
        timings,
        production_query_regression,
        resources,
        evidence,
    };
    if let Some(path) = &config.state_output {
        let state = VerificationState {
            schema: "keldra.index-resource-verification.v2".into(),
            tenant: config.tenant.to_string(),
            bucket: config.bucket.to_string(),
            records: config.records,
            final_live_objects: final_live,
            final_commit_revision,
            final_result_sha256,
            source_count: config.endpoints.len(),
        };
        std::fs::write(path, serde_json::to_vec_pretty(&state)?)
            .with_context(|| format!("write qualification state {}", path.display()))?;
    }
    let encoded = serde_json::to_vec_pretty(&report)?;
    if let Some(path) = &config.output {
        std::fs::write(path, &encoded)
            .with_context(|| format!("write qualification report {}", path.display()))?;
    }
    println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    Ok(())
}

fn qualification_index_request(bucket: &str) -> Result<CreateIndexRequest> {
    let record_id = UnsignedIntegerField::single("record_id", "/record_id")
        .exact()
        .order();
    let record_id_order = record_id.ascending();
    let modified_day = UnsignedIntegerField::single("modified_day", "/modified_day").order();
    let modified_day_order = modified_day.descending();

    Ok(TypedJsonIndexBuilder::new(bucket, "records-by-field")
        .path_prefix("records/")
        .content_type(CONTENT_TYPE)
        .field(record_id)
        .field(
            KeywordField::single("ecosystem", "/ecosystem")
                .exact()
                .facet(),
        )
        .field(KeywordField::single("package", "/package").exact())
        .field(KeywordField::single("severity", "/severity").exact())
        .field(BooleanField::single("active", "/active").exact().facet())
        .field(BooleanField::single("withdrawn", "/withdrawn").exact())
        .field(
            FloatField::single("score", "/score")
                .range()
                .order()
                .aggregate(),
        )
        .field(UnsignedIntegerField::single("published_day", "/published_day").exact())
        .field(modified_day)
        .field(UnsignedIntegerField::single("sequence", "/sequence").exact())
        .field(KeywordField::single("source", "/source").exact())
        .field(UnsignedIntegerField::single("partition", "/partition").exact())
        .physical_order([modified_day_order, record_id_order])
        .finish("v06-resource-create-index")?)
}

impl Config {
    fn from_env() -> Result<Self> {
        let records = number("KELDRA_V06_RESOURCE_RECORDS", DEFAULT_RECORDS)?;
        let mutation_count = number("KELDRA_V06_RESOURCE_MUTATIONS", DEFAULT_MUTATIONS)?;
        let batch_size = number("KELDRA_V06_RESOURCE_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
        let workers = number("KELDRA_V06_RESOURCE_WORKERS", DEFAULT_WORKERS)?;
        let verification_workers = number(
            "KELDRA_V06_RESOURCE_VERIFICATION_WORKERS",
            DEFAULT_VERIFICATION_WORKERS,
        )?;
        ensure!(records > 0, "record count must be non-zero");
        ensure!(batch_size > 0, "batch size must be non-zero");
        ensure!(workers > 0, "worker count must be non-zero");
        ensure!(
            verification_workers > 0,
            "verification worker count must be non-zero"
        );
        let endpoints = required("KELDRA_V06_RESOURCE_ENDPOINTS")?
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        ensure!(!endpoints.is_empty(), "at least one endpoint is required");
        let resource_pids = optional_list("KELDRA_V06_RESOURCE_PIDS")
            .into_iter()
            .map(|value| value.parse().context("invalid resource PID"))
            .collect::<Result<Vec<_>>>()?;
        let resource_containers = optional_list("KELDRA_V06_RESOURCE_CONTAINERS");
        let require_resource_targets = boolean("KELDRA_V06_REQUIRE_RESOURCE_TARGETS", true)?;
        let configured_kind_budget_bytes = optional_number("KELDRA_V06_KIND_BUDGET_BYTES")?;
        let configured_compaction_max_lanes =
            optional_number("KELDRA_V06_INDEX_COMPACTION_MAX_LANES")?;
        let configured_projection_max_lanes =
            optional_number("KELDRA_V06_INDEX_PROJECTION_MAX_LANES")?;
        let configured_rayon_workers = optional_number("KELDRA_V06_INDEX_RAYON_WORKERS")?;
        let max_anonymous_growth_bytes = optional_number("KELDRA_V06_MAX_ANONYMOUS_GROWTH_BYTES")?;
        let require_performance_targets = boolean("KELDRA_V09_REQUIRE_PERFORMANCE_TARGETS", false)?;
        let evidence = EvidenceConfig::from_env(endpoints.len())?;
        ensure!(
            configured_compaction_max_lanes.is_none_or(|lanes| lanes > 0),
            "KELDRA_V06_INDEX_COMPACTION_MAX_LANES must be non-zero when configured"
        );
        ensure!(
            configured_projection_max_lanes.is_none_or(|lanes| lanes > 0),
            "KELDRA_V06_INDEX_PROJECTION_MAX_LANES must be non-zero when configured"
        );
        if require_resource_targets {
            ensure!(
                !resource_pids.is_empty() || !resource_containers.is_empty(),
                "resource qualification requires a PID or container target"
            );
            ensure!(
                configured_kind_budget_bytes.is_some_and(|bytes| bytes > 0),
                "resource qualification requires a non-zero KELDRA_V06_KIND_BUDGET_BYTES"
            );
            ensure!(
                configured_rayon_workers.is_some_and(|workers| workers > 0),
                "resource qualification requires a non-zero KELDRA_V06_INDEX_RAYON_WORKERS"
            );
            ensure!(
                configured_projection_max_lanes == configured_rayon_workers,
                "resource qualification requires projection lanes to equal Rayon workers"
            );
            ensure!(
                max_anonymous_growth_bytes.is_some(),
                "resource qualification requires KELDRA_V06_MAX_ANONYMOUS_GROWTH_BYTES"
            );
        }
        Ok(Self {
            endpoints,
            tenant: required("KELDRA_V06_RESOURCE_TENANT")?.into(),
            bucket: required("KELDRA_V06_RESOURCE_BUCKET")?.into(),
            client_id: required("KELDRA_V06_RESOURCE_CLIENT_ID")?,
            client_secret: required("KELDRA_V06_RESOURCE_CLIENT_SECRET")?,
            records,
            mutation_count,
            batch_size,
            workers,
            verification_workers,
            seed: number("KELDRA_V06_RESOURCE_SEED", DEFAULT_SEED)?,
            resource_pids,
            resource_containers,
            require_resource_targets,
            configured_kind_budget_bytes,
            configured_compaction_max_lanes,
            configured_projection_max_lanes,
            configured_rayon_workers,
            max_anonymous_growth_bytes,
            require_performance_targets,
            evidence,
            output: env::var_os("KELDRA_V06_RESOURCE_OUTPUT").map(PathBuf::from),
            state_output: env::var_os("KELDRA_V06_RESOURCE_STATE_OUTPUT").map(PathBuf::from),
        })
    }
}

impl EvidenceConfig {
    fn from_env(ingress_endpoint_count: usize) -> Result<Self> {
        let source_commit = required("KELDRA_V09_EVIDENCE_SOURCE_COMMIT")?;
        ensure!(
            source_commit.len() == 40 && source_commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "KELDRA_V09_EVIDENCE_SOURCE_COMMIT must be a full Git commit ID"
        );
        let resolved_container_digest = required("KELDRA_V09_EVIDENCE_CONTAINER_DIGEST")?;
        let digest_hex = resolved_container_digest
            .strip_prefix("sha256:")
            .context("KELDRA_V09_EVIDENCE_CONTAINER_DIGEST must use sha256")?;
        ensure!(
            digest_hex.len() == 64 && digest_hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "KELDRA_V09_EVIDENCE_CONTAINER_DIGEST must be a complete sha256 digest"
        );
        let native_architecture = required("KELDRA_V09_EVIDENCE_NATIVE_ARCHITECTURE")?;
        ensure!(
            !native_architecture.trim().is_empty(),
            "native architecture evidence must not be empty"
        );
        let container_platform = required("KELDRA_V09_EVIDENCE_CONTAINER_PLATFORM")?;
        ensure!(
            matches!(container_platform.as_str(), "linux/amd64" | "linux/arm64"),
            "container platform evidence must be linux/amd64 or linux/arm64"
        );
        let topology = required("KELDRA_V09_EVIDENCE_TOPOLOGY")?;
        let node_count = required_number("KELDRA_V09_EVIDENCE_NODE_COUNT")?;
        ensure!(
            matches!(
                (topology.as_str(), node_count),
                ("single-node", 1) | ("three-node", 3)
            ),
            "topology evidence must describe the supported one- or three-node qualification"
        );
        ensure!(
            ingress_endpoint_count == node_count,
            "topology node count must equal the number of qualification ingress endpoints"
        );
        let hardware_logical_cpus = required_number("KELDRA_V09_EVIDENCE_HARDWARE_LOGICAL_CPUS")?;
        let hardware_memory_bytes = required_number("KELDRA_V09_EVIDENCE_HARDWARE_MEMORY_BYTES")?;
        let qualification_filesystem_total_bytes =
            required_number("KELDRA_V09_EVIDENCE_FILESYSTEM_TOTAL_BYTES")?;
        let qualification_filesystem_available_bytes_at_start =
            required_number("KELDRA_V09_EVIDENCE_FILESYSTEM_AVAILABLE_BYTES")?;
        let index_disk_cache_bytes_per_node =
            required_number("KELDRA_V09_EVIDENCE_INDEX_DISK_CACHE_BYTES_PER_NODE")?;
        let index_memory_percent_per_node =
            required_number("KELDRA_V09_EVIDENCE_INDEX_MEMORY_PERCENT_PER_NODE")?;
        ensure!(
            hardware_logical_cpus > 0
                && hardware_memory_bytes > 0
                && qualification_filesystem_total_bytes > 0
                && qualification_filesystem_available_bytes_at_start > 0
                && qualification_filesystem_available_bytes_at_start
                    <= qualification_filesystem_total_bytes
                && index_disk_cache_bytes_per_node > 0
                && (1..=100).contains(&index_memory_percent_per_node),
            "qualification hardware and resource evidence must be positive and bounded"
        );
        Ok(Self {
            source_commit,
            resolved_container_digest,
            native_architecture,
            container_platform,
            topology,
            node_count,
            hardware_logical_cpus,
            hardware_memory_bytes,
            qualification_filesystem_total_bytes,
            qualification_filesystem_available_bytes_at_start,
            index_disk_cache_bytes_per_node,
            index_memory_percent_per_node,
        })
    }
}

fn qualification_evidence(
    config: &Config,
    initial_corpus_sha256: String,
    source_complete_commit_revision_observed: bool,
) -> Result<QualificationEvidence> {
    let builder_memory_bytes_per_kind_per_node = config
        .configured_kind_budget_bytes
        .context("qualification evidence requires the configured per-kind memory budget")?;
    let compaction_max_lanes_per_kind = config
        .configured_compaction_max_lanes
        .context("qualification evidence requires the configured compaction lane limit")?;
    let projection_max_lanes_per_kind = config
        .configured_projection_max_lanes
        .context("qualification evidence requires the configured projection lane limit")?;
    let rayon_workers_per_node = config
        .configured_rayon_workers
        .context("qualification evidence requires the configured Rayon worker count")?;
    let maximum_anonymous_growth_bytes = config
        .max_anonymous_growth_bytes
        .context("qualification evidence requires the anonymous-memory growth limit")?;
    let evidence = &config.evidence;
    Ok(QualificationEvidence {
        source_commit: evidence.source_commit.clone(),
        resolved_container_digest: evidence.resolved_container_digest.clone(),
        native_architecture: evidence.native_architecture.clone(),
        container_platform: evidence.container_platform.clone(),
        hardware: HardwareEvidence {
            logical_cpus: evidence.hardware_logical_cpus,
            memory_bytes: evidence.hardware_memory_bytes,
            qualification_filesystem_total_bytes: evidence.qualification_filesystem_total_bytes,
            qualification_filesystem_available_bytes_at_start: evidence
                .qualification_filesystem_available_bytes_at_start,
        },
        corpus: CorpusEvidence {
            identity: "keldra.synthetic-index-resource.initial.v1",
            initial_corpus_sha256,
            generator_seed_hex: format!("0x{:016x}", config.seed),
            records: config.records,
            indexed_fields: data::FIELD_COUNT,
        },
        topology: TopologyEvidence {
            kind: evidence.topology.clone(),
            node_count: evidence.node_count,
            ingress_endpoint_count: config.endpoints.len(),
        },
        durability: DurabilityEvidence {
            initial_writes: "LOCAL",
            updates: "LOCAL",
            deletes: "LOCAL",
        },
        execution: ExecutionEvidence {
            bulk_write_max_operations: config.batch_size,
            ingest_workers: config.workers,
            verification_workers: config.verification_workers,
            partition_count: PARTITION_COUNT,
            query_limit: QUERY_LIMIT,
            configured_mutations: config.mutation_count,
        },
        resource_configuration: ResourceConfigurationEvidence {
            index_disk_cache_bytes_per_node: evidence.index_disk_cache_bytes_per_node,
            index_memory_percent_per_node: evidence.index_memory_percent_per_node,
            builder_memory_bytes_per_kind_per_node,
            compaction_max_lanes_per_kind,
            projection_max_lanes_per_kind,
            rayon_workers_per_node,
            maximum_anonymous_growth_bytes,
            resource_sample_interval_milliseconds: 100,
            monitored_target_count: config.resource_pids.len() + config.resource_containers.len(),
            resource_targets_required: config.require_resource_targets,
        },
        timer_boundaries: TimerBoundaryEvidence {
            clock: "tokio::time::Instant monotonic elapsed time",
            ingest_seconds: TimerBoundary {
                starts: "immediately before the first initial BulkWrite request",
                stops: "after every initial BulkWrite receipt is accepted",
            },
            initial_build_seconds: TimerBoundary {
                starts: "immediately before polling after every initial BulkWrite receipt is accepted",
                stops: "when one commit_revision proves zero lag through the observed tail of every topology source",
            },
            first_complete_commit_revision_seconds: TimerBoundary {
                starts: "immediately before the first initial BulkWrite request",
                stops: "when one commit_revision proves zero lag through the observed tail of every topology source",
            },
            exact_verification_seconds: TimerBoundary {
                starts: "before initial exact partition verification",
                stops: "after one complete commit_revision exactly matches every initial object",
            },
            cold_query_milliseconds: TimerBoundary {
                starts: "immediately before the first representative query request",
                stops: "when that query response is received",
            },
            warm_query_milliseconds: TimerBoundary {
                starts: "immediately before the repeated representative query request",
                stops: "when that query response is received",
            },
            mutation_seconds: TimerBoundary {
                starts: "immediately before the first update BulkWrite request",
                stops: "after every update and delete BulkWrite receipt is accepted",
            },
            incremental_build_seconds: TimerBoundary {
                starts: "immediately before polling after every update and delete receipt is accepted",
                stops: "after a newer complete commit_revision passes exact final verification",
            },
        },
        correctness: CorrectnessEvidence {
            result: "pass",
            source_complete_commit_revision_observed,
            source_complete_sources_observed: config.endpoints.len(),
            initial_exact_partition_verification: true,
            final_exact_partition_verification: true,
            update_and_delete_verification: true,
            resource_limits_passed: true,
            performance_targets_required: config.require_performance_targets,
            performance_targets_passed: config.require_performance_targets.then_some(true),
        },
    })
}

fn initial_corpus_sha256(seed: u64, records: u64) -> String {
    let mut hash = Sha256::new();
    hash.update(b"keldra.synthetic-index-resource.initial.v1\0");
    hash.update(seed.to_be_bytes());
    hash.update(records.to_be_bytes());
    hash.update(CONTENT_TYPE.as_bytes());
    for record_id in 0..records {
        let path = data::object_path(record_id);
        let payload = data::payload(seed, record_id, RecordFlavor::Initial);
        hash.update(record_id.to_be_bytes());
        hash.update(
            u64::try_from(path.len())
                .expect("generated path length fits u64")
                .to_be_bytes(),
        );
        hash.update(path.as_bytes());
        hash.update(
            u64::try_from(payload.len())
                .expect("generated payload length fits u64")
                .to_be_bytes(),
        );
        hash.update(payload);
    }
    format!("sha256:{}", hex::encode(hash.finalize()))
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

async fn qualification_token(config: &Config, channel: Channel) -> Result<String> {
    exchange_client_credentials(
        channel,
        config.client_id.clone(),
        config.client_secret.clone(),
    )
    .await
    .context("credential exchange failed")
    .map(|response| response.access_token)
}

fn index_client(channel: Channel, token: &str) -> Result<IndexClient> {
    Ok(
        IndexServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(72 * 1024 * 1024)
            .max_decoding_message_size(72 * 1024 * 1024),
    )
}

async fn write_ranges(
    config: &Config,
    channels: &[Channel],
    token: &str,
    start: u64,
    end: u64,
    mode: MutationMode,
    capture_versions: bool,
) -> Result<BatchResult> {
    if start >= end {
        return Ok(BatchResult {
            accepted: 0,
            receipts: Vec::new(),
        });
    }
    let next = Arc::new(std::sync::atomic::AtomicU64::new(start));
    let mut tasks = JoinSet::new();
    for worker in 0..config.workers {
        let client = object_client(channels[worker % channels.len()].clone(), token)?;
        let tenant = config.tenant.clone();
        let bucket = config.bucket.clone();
        let next = next.clone();
        let batch_size = config.batch_size;
        let seed = config.seed;
        tasks.spawn(async move {
            write_worker(
                client,
                tenant,
                bucket,
                next,
                end,
                batch_size,
                seed,
                mode,
                capture_versions,
            )
            .await
        });
    }
    let mut accepted = 0u64;
    let mut receipts = if capture_versions {
        Vec::with_capacity((end - start) as usize)
    } else {
        Vec::new()
    };
    while let Some(result) = tasks.join_next().await {
        let result = result.context("write worker panicked")??;
        accepted = accepted.saturating_add(result.accepted);
        receipts.extend(result.receipts);
    }
    receipts.sort_unstable_by_key(|(record_id, _)| *record_id);
    ensure!(accepted == end - start);
    if capture_versions {
        ensure!(receipts.len() as u64 == end - start);
    } else {
        ensure!(receipts.is_empty());
    }
    Ok(BatchResult { accepted, receipts })
}

#[allow(clippy::too_many_arguments)]
async fn write_worker(
    mut client: RawClient,
    tenant: Arc<str>,
    bucket: Arc<str>,
    next: Arc<std::sync::atomic::AtomicU64>,
    end: u64,
    batch_size: usize,
    seed: u64,
    mode: MutationMode,
    capture_versions: bool,
) -> Result<BatchResult> {
    use std::sync::atomic::Ordering;

    let mut receipts = Vec::new();
    let mut accepted = 0u64;
    loop {
        let batch_start = next.fetch_add(batch_size as u64, Ordering::Relaxed);
        if batch_start >= end {
            return Ok(BatchResult { accepted, receipts });
        }
        let batch_end = end.min(batch_start.saturating_add(batch_size as u64));
        let mut operations = Vec::with_capacity((batch_end - batch_start) as usize);
        for record_id in batch_start..batch_end {
            let address = Some(ObjectAddress {
                tenant: tenant.to_string(),
                bucket: bucket.to_string(),
                path: data::object_path(record_id),
            });
            let operation = match mode {
                MutationMode::Initial | MutationMode::Update => {
                    let flavor = if matches!(mode, MutationMode::Update) {
                        RecordFlavor::Updated
                    } else {
                        RecordFlavor::Initial
                    };
                    BulkOperationValue::Put(BulkPutRequest {
                        address,
                        bytes: data::payload(seed, record_id, flavor),
                        content_type: CONTENT_TYPE.into(),
                        command_id: command_id(mode, record_id),
                        durability: Durability::Local as i32,
                    })
                }
                MutationMode::Delete => BulkOperationValue::Delete(DeleteRequest {
                    address,
                    command_id: command_id(mode, record_id),
                    durability: Durability::Local as i32,
                }),
            };
            operations.push(BulkOperation {
                operation: Some(operation),
            });
        }
        let outcomes = client
            .bulk_write(BulkWriteRequest { operations })
            .await
            .context("BulkWrite failed")?
            .into_inner()
            .outcomes;
        ensure!(outcomes.len() as u64 == batch_end - batch_start);
        let mut seen = BTreeSet::new();
        for outcome in outcomes {
            let index = usize::try_from(outcome.index)?;
            ensure!(index < (batch_end - batch_start) as usize);
            ensure!(seen.insert(index), "duplicate BulkWrite outcome index");
            let record_id = batch_start + index as u64;
            let receipt = match outcome.outcome.context("missing BulkWrite outcome")? {
                BulkOutcomeValue::Receipt(receipt) => receipt,
                BulkOutcomeValue::Failure(failure) => bail!(
                    "BulkWrite record {record_id} failed with code {}: {}",
                    failure.code,
                    failure.message
                ),
            };
            ensure!(receipt.command_id == command_id(mode, record_id));
            ensure!(receipt.version != 0);
            ensure!(receipt.deleted == matches!(mode, MutationMode::Delete));
            accepted += 1;
            if capture_versions {
                receipts.push((record_id, receipt.version));
            }
        }
    }
}

fn command_id(mode: MutationMode, record_id: u64) -> String {
    let label = match mode {
        MutationMode::Initial => "initial",
        MutationMode::Update => "update",
        MutationMode::Delete => "delete",
    };
    format!("v06-resource-{label}-{record_id}")
}

async fn wait_for_commit_revision(
    client: &mut IndexClient,
    bucket: &str,
    after_commit_revision: u64,
    expected_partition_hits: Option<usize>,
    expected_complete_sources: Option<usize>,
) -> Result<QueryIndexResponse> {
    let deadline = Instant::now() + BUILD_TIMEOUT;
    loop {
        match query_partition(client, bucket, FRESHNESS_PROBE_PARTITION).await {
            Ok(probe)
                if commit_revision_is_ready(
                    &probe,
                    after_commit_revision,
                    expected_complete_sources,
                ) =>
            {
                let Some(expected_hits) = expected_partition_hits else {
                    return Ok(probe);
                };
                match query_partition(client, bucket, 7).await {
                    Ok(response) => {
                        if commit_revision_is_ready(
                            &response,
                            after_commit_revision,
                            expected_complete_sources,
                        ) {
                            ensure_expected_partition_hits(&response, 7, expected_hits)?;
                            return Ok(response);
                        }
                    }
                    Err(error) if retryable_error(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(_) => {}
            Err(error) if retryable_error(&error) => {}
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            bail!("index commit_revision did not become ready within {BUILD_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn commit_revision_is_ready(
    response: &QueryIndexResponse,
    after_commit_revision: u64,
    expected_complete_sources: Option<usize>,
) -> bool {
    freshness(response).is_ok_and(|value| {
        value.commit_revision > after_commit_revision
            && value.initial_build_complete
            && !value.rebuilding
            && expected_complete_sources
                .is_none_or(|expected| source_complete_freshness(value, expected))
    })
}

fn ensure_expected_partition_hits(
    response: &QueryIndexResponse,
    partition: u64,
    expected_hits: usize,
) -> Result<()> {
    ensure!(
        response.hits.len() == expected_hits,
        "ready index commit_revision {} returned {} hits for partition {partition}, expected {expected_hits}",
        response.commit_revision(),
        response.hits.len(),
    );
    Ok(())
}

async fn verify_every_partition(
    client: &IndexClient,
    bucket: &str,
    records: u64,
    initial_versions: &[u64],
    expected_updates: Option<&BTreeMap<u64, u64>>,
    deleted: Option<&BTreeSet<u64>>,
    workers: usize,
) -> Result<(u64, u64, String)> {
    let deadline = Instant::now() + EXACT_VERIFICATION_TIMEOUT;
    loop {
        let mut count = 0u64;
        let mut commit_revision = None;
        let mut partition_digests = BTreeMap::new();
        let mut complete = true;
        let mut next_partition = 0;
        let mut queries = JoinSet::new();
        while next_partition < PARTITION_COUNT && queries.len() < workers {
            spawn_partition_query(&mut queries, client, bucket, next_partition);
            next_partition += 1;
        }
        while let Some(joined) = queries.join_next().await {
            if Instant::now() >= deadline {
                queries.abort_all();
                while queries.join_next().await.is_some() {}
                bail!(
                    "exact partition verification did not converge before {EXACT_VERIFICATION_TIMEOUT:?}"
                );
            }
            let (partition, response) = joined.context("exact partition query task failed")?;
            let response = match response {
                Ok(response) => response,
                Err(error) if retryable_error(&error) => {
                    complete = false;
                    break;
                }
                Err(error) => return Err(error),
            };
            let response_commit_revision = freshness(&response)?.commit_revision;
            if commit_revision.is_some_and(|value| value != response_commit_revision) {
                complete = false;
                break;
            }
            commit_revision = Some(response_commit_revision);
            match validate_partition_response(
                &response,
                records,
                partition,
                initial_versions,
                expected_updates,
                deleted,
            ) {
                Ok(partition_count) => {
                    count += partition_count;
                    ensure!(
                        partition_digests
                            .insert(partition, partition_result_digest(partition, &response)?)
                            .is_none(),
                        "duplicate partition response"
                    );
                }
                Err(_) => {
                    complete = false;
                    break;
                }
            }
            if next_partition < PARTITION_COUNT {
                spawn_partition_query(&mut queries, client, bucket, next_partition);
                next_partition += 1;
            }
        }
        if !complete {
            queries.abort_all();
            while queries.join_next().await.is_some() {}
        }
        let expected = records - deleted.map_or(0, |values| values.len() as u64);
        if complete && count == expected {
            return Ok((
                count,
                commit_revision.context("verification returned no commit_revision")?,
                qualification_result_digest(&partition_digests)?,
            ));
        }
        if Instant::now() >= deadline {
            bail!(
                "exact partition verification did not converge before {EXACT_VERIFICATION_TIMEOUT:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn verify_existing_state(path: &Path) -> Result<()> {
    let state: VerificationState = serde_json::from_slice(
        &std::fs::read(path)
            .with_context(|| format!("read qualification state {}", path.display()))?,
    )
    .with_context(|| format!("parse qualification state {}", path.display()))?;
    ensure!(
        state.schema == "keldra.index-resource-verification.v2",
        "unsupported qualification state schema"
    );
    ensure!(state.records > 0, "qualification state has no records");
    ensure!(
        state.final_live_objects <= state.records,
        "qualification state has more live objects than records"
    );
    ensure!(
        state.final_commit_revision > 0,
        "qualification state has no final commit_revision"
    );
    ensure!(state.source_count > 0, "qualification state has no sources");
    validate_sha256(&state.final_result_sha256)?;

    let tenant = required("KELDRA_V06_RESOURCE_TENANT")?;
    let bucket = required("KELDRA_V06_RESOURCE_BUCKET")?;
    ensure!(
        tenant == state.tenant,
        "qualification state tenant mismatch"
    );
    ensure!(
        bucket == state.bucket,
        "qualification state bucket mismatch"
    );
    let client_id = required("KELDRA_V06_RESOURCE_CLIENT_ID")?;
    let client_secret = required("KELDRA_V06_RESOURCE_CLIENT_SECRET")?;
    let verification_workers = number(
        "KELDRA_V06_RESOURCE_VERIFICATION_WORKERS",
        DEFAULT_VERIFICATION_WORKERS,
    )?;
    ensure!(
        verification_workers > 0,
        "verification worker count must be non-zero"
    );
    let endpoints = required("KELDRA_V06_RESOURCE_ENDPOINTS")?
        .split(',')
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    ensure!(!endpoints.is_empty(), "at least one endpoint is required");
    let channels = connect_all(&endpoints).await?;

    for (endpoint, channel) in endpoints.iter().zip(channels) {
        let token =
            exchange_client_credentials(channel.clone(), client_id.clone(), client_secret.clone())
                .await
                .with_context(|| format!("credential exchange through {endpoint} failed"))?
                .access_token;
        let index = index_client(channel, &token)?;
        let (count, commit_revision, digest) = digest_every_partition(
            &index,
            &state.bucket,
            state.records,
            state.final_live_objects,
            state.source_count,
            verification_workers,
        )
        .await
        .with_context(|| format!("verify resource index through {endpoint}"))?;
        ensure!(
            count == state.final_live_objects,
            "resource index live-object count changed through {endpoint}"
        );
        ensure!(
            commit_revision >= state.final_commit_revision,
            "resource index commit_revision regressed through {endpoint}"
        );
        ensure!(
            digest == state.final_result_sha256,
            "resource index result digest changed through {endpoint}"
        );
        println!(
            "verified resource index through {endpoint}: commit_revision={commit_revision} objects={count} digest={digest}"
        );
    }
    Ok(())
}

async fn digest_every_partition(
    client: &IndexClient,
    bucket: &str,
    records: u64,
    expected_objects: u64,
    expected_sources: usize,
    workers: usize,
) -> Result<(u64, u64, String)> {
    let deadline = Instant::now() + EXACT_VERIFICATION_TIMEOUT;
    loop {
        let mut count = 0u64;
        let mut commit_revision = None;
        let mut partition_digests = BTreeMap::new();
        let mut complete = true;
        let mut next_partition = 0;
        let mut queries = JoinSet::new();
        while next_partition < PARTITION_COUNT && queries.len() < workers {
            spawn_partition_query(&mut queries, client, bucket, next_partition);
            next_partition += 1;
        }
        while let Some(joined) = queries.join_next().await {
            if Instant::now() >= deadline {
                queries.abort_all();
                while queries.join_next().await.is_some() {}
                bail!(
                    "persisted resource verification did not converge before {EXACT_VERIFICATION_TIMEOUT:?}"
                );
            }
            let (partition, response) =
                joined.context("resource verification query task failed")?;
            let response = match response {
                Ok(response) => response,
                Err(error) if retryable_error(&error) => {
                    complete = false;
                    break;
                }
                Err(error) => return Err(error),
            };
            let response_freshness = freshness(&response)?;
            if !response_freshness.initial_build_complete
                || response_freshness.rebuilding
                || !source_complete_freshness(response_freshness, expected_sources)
                || commit_revision.is_some_and(|value| value != response_freshness.commit_revision)
            {
                complete = false;
                break;
            }
            commit_revision = Some(response_freshness.commit_revision);
            match validate_digest_partition_response(&response, records, partition) {
                Ok((partition_count, partition_digest)) => {
                    count += partition_count;
                    ensure!(
                        partition_digests
                            .insert(partition, partition_digest)
                            .is_none(),
                        "duplicate partition response"
                    );
                }
                Err(_) => {
                    complete = false;
                    break;
                }
            }
            if next_partition < PARTITION_COUNT {
                spawn_partition_query(&mut queries, client, bucket, next_partition);
                next_partition += 1;
            }
        }
        if !complete {
            queries.abort_all();
            while queries.join_next().await.is_some() {}
        }
        if complete && count == expected_objects {
            return Ok((
                count,
                commit_revision.context("verification returned no commit_revision")?,
                qualification_result_digest(&partition_digests)?,
            ));
        }
        if Instant::now() >= deadline {
            bail!(
                "persisted resource verification did not converge before {EXACT_VERIFICATION_TIMEOUT:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn spawn_partition_query(
    queries: &mut JoinSet<(u64, Result<QueryIndexResponse>)>,
    client: &IndexClient,
    bucket: &str,
    partition: u64,
) {
    let mut client = client.clone();
    let bucket = bucket.to_owned();
    queries.spawn(async move {
        let response = query_partition(&mut client, &bucket, partition).await;
        (partition, response)
    });
}

fn validate_partition_response(
    response: &QueryIndexResponse,
    records: u64,
    partition: u64,
    initial_versions: &[u64],
    expected_updates: Option<&BTreeMap<u64, u64>>,
    deleted: Option<&BTreeSet<u64>>,
) -> Result<u64> {
    ensure!(response.next_page_token.is_empty());
    let mut paths = BTreeSet::new();
    let mut expected = 0u64;
    for record_id in (partition..records).step_by(PARTITION_COUNT as usize) {
        if deleted.is_some_and(|values| values.contains(&record_id)) {
            continue;
        }
        expected += 1;
    }
    for hit in &response.hits {
        let address = hit.address.as_ref().context("index hit omitted address")?;
        let record_id = parse_record_id(&address.path)?;
        ensure!(record_id < records && record_id % PARTITION_COUNT == partition);
        let initial_version = *initial_versions
            .get(usize::try_from(record_id)?)
            .context("index hit was outside the captured version set")?;
        let expected_version = expected_updates
            .and_then(|values| values.get(&record_id))
            .copied()
            .unwrap_or(initial_version);
        ensure!(hit.object_version == expected_version);
        ensure!(
            !deleted.is_some_and(|values| values.contains(&record_id)),
            "deleted record remained visible"
        );
        ensure!(paths.insert(address.path.as_str()), "duplicate index hit");
    }
    ensure!(response.hits.len() as u64 == expected);
    Ok(expected)
}

fn validate_digest_partition_response(
    response: &QueryIndexResponse,
    records: u64,
    partition: u64,
) -> Result<(u64, [u8; 32])> {
    ensure!(response.next_page_token.is_empty());
    let mut records_and_versions = Vec::with_capacity(response.hits.len());
    let mut seen = BTreeSet::new();
    for hit in &response.hits {
        let address = hit.address.as_ref().context("index hit omitted address")?;
        let record_id = parse_record_id(&address.path)?;
        ensure!(record_id < records && record_id % PARTITION_COUNT == partition);
        ensure!(hit.object_version != 0, "index hit omitted object version");
        ensure!(seen.insert(record_id), "duplicate index hit");
        records_and_versions.push((record_id, hit.object_version));
    }
    Ok((
        records_and_versions.len() as u64,
        digest_partition_records(partition, &records_and_versions),
    ))
}

fn partition_result_digest(partition: u64, response: &QueryIndexResponse) -> Result<[u8; 32]> {
    let records_and_versions = response
        .hits
        .iter()
        .map(|hit| {
            let address = hit.address.as_ref().context("index hit omitted address")?;
            Ok((parse_record_id(&address.path)?, hit.object_version))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(digest_partition_records(partition, &records_and_versions))
}

fn digest_partition_records(partition: u64, records_and_versions: &[(u64, u64)]) -> [u8; 32] {
    let mut canonical = records_and_versions.to_vec();
    canonical.sort_unstable();
    let mut hash = Sha256::new();
    hash.update(b"keldra.index-resource.partition-result.v1\0");
    hash.update(partition.to_be_bytes());
    hash.update((canonical.len() as u64).to_be_bytes());
    for (record_id, version) in canonical {
        hash.update(record_id.to_be_bytes());
        hash.update(version.to_be_bytes());
    }
    hash.finalize().into()
}

fn qualification_result_digest(partition_digests: &BTreeMap<u64, [u8; 32]>) -> Result<String> {
    ensure!(
        partition_digests.len() as u64 == PARTITION_COUNT,
        "qualification result omitted one or more partitions"
    );
    let mut hash = Sha256::new();
    hash.update(b"keldra.index-resource.complete-result.v1\0");
    hash.update(PARTITION_COUNT.to_be_bytes());
    for partition in 0..PARTITION_COUNT {
        let digest = partition_digests
            .get(&partition)
            .context("qualification result omitted a partition")?;
        hash.update(partition.to_be_bytes());
        hash.update(digest);
    }
    Ok(format!("sha256:{}", hex::encode(hash.finalize())))
}

fn validate_sha256(value: &str) -> Result<()> {
    let digest = value
        .strip_prefix("sha256:")
        .context("qualification result digest must use sha256")?;
    ensure!(
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "qualification result digest must be a complete sha256 digest"
    );
    Ok(())
}

async fn query_partition(
    client: &mut IndexClient,
    bucket: &str,
    partition: u64,
) -> Result<QueryIndexResponse> {
    client
        .query_index(QueryIndexRequest {
            bucket: bucket.into(),
            index_name: "records-by-field".into(),
            query: Some(IndexQuery {
                query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
                    predicate: Some(IndexPredicateExpression::leaf(IndexPredicate {
                        field: "partition".into(),
                        operator: IndexPredicateOperator::Equal as i32,
                        values_json: vec![partition.to_string().into_bytes()],
                    })),
                    order: Vec::new(),
                    facets: Vec::new(),
                    aggregates: Vec::new(),
                })),
            }),
            limit: QUERY_LIMIT,
            page_token: Vec::new(),
            tenant: String::new(),
            required_freshness: None,
        })
        .await
        .map(tonic::Response::into_inner)
        .map_err(Into::into)
}

fn freshness(response: &QueryIndexResponse) -> Result<&keldra_storage::v1::IndexFreshness> {
    response
        .freshness
        .as_ref()
        .context("index response omitted freshness")
}

fn source_complete_freshness(freshness: &IndexFreshness, expected_sources: usize) -> bool {
    let source_ids = freshness
        .sources
        .iter()
        .map(|source| source.node_id)
        .collect::<BTreeSet<_>>();
    expected_sources != 0
        && freshness.sources.len() == expected_sources
        && source_ids.len() == expected_sources
        && freshness.sources.iter().all(source_is_proven_current)
}

fn source_is_proven_current(source: &IndexSourceFreshness) -> bool {
    source.node_id != 0
        && source.source_epoch.len() == 32
        && source.lag_hint == 0
        && source.observed_tail.and_then(|tail| tail.checked_add(1))
            == Some(source.indexed_next_offset)
}

fn hits_by_path(response: &QueryIndexResponse) -> Result<BTreeMap<&str, u64>> {
    response
        .hits
        .iter()
        .map(|hit| {
            let address = hit.address.as_ref().context("index hit omitted address")?;
            Ok((address.path.as_str(), hit.object_version))
        })
        .collect()
}

fn parse_record_id(path: &str) -> Result<u64> {
    path.strip_prefix("records/")
        .and_then(|value| value.strip_suffix(".json"))
        .context("unexpected indexed path")?
        .parse()
        .context("invalid indexed record id")
}

fn retryable_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<tonic::Status>().is_some_and(|status| {
        matches!(
            status.code(),
            tonic::Code::Unavailable
                | tonic::Code::DeadlineExceeded
                | tonic::Code::NotFound
                | tonic::Code::FailedPrecondition
        )
    })
}

fn set_phase(monitor: &Option<ResourceMonitor>, phase: Phase) {
    if let Some(monitor) = monitor {
        monitor.set_phase(phase);
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).map_err(|_| anyhow!("{name} is required"))
}

fn required_number<T>(name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    required(name)?
        .parse()
        .with_context(|| format!("invalid {name}"))
}

fn optional_list(name: &str) -> Vec<String> {
    env::var(name)
        .ok()
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn number<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    env::var(name)
        .ok()
        .map(|value| value.parse().with_context(|| format!("invalid {name}")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn optional_number<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
{
    env::var(name)
        .ok()
        .map(|value| value.parse().with_context(|| format!("invalid {name}")))
        .transpose()
}

fn boolean(name: &str, default: bool) -> Result<bool> {
    match env::var(name).ok().as_deref() {
        None => Ok(default),
        Some("1" | "true" | "yes") => Ok(true),
        Some("0" | "false" | "no") => Ok(false),
        Some(_) => bail!("{name} must be true or false"),
    }
}

trait CommitRevisionResponse {
    fn commit_revision(&self) -> u64;
}

impl CommitRevisionResponse for QueryIndexResponse {
    fn commit_revision(&self) -> u64 {
        self.freshness
            .as_ref()
            .map_or(0, |value| value.commit_revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualification_definition_uses_typed_capability_specific_fields() {
        use keldra_storage::v1::IndexFieldCapability;
        use keldra_storage::v1::index_field::FieldType;
        use keldra_storage::v1::index_specification::Specification;

        let request = qualification_index_request("qualification").unwrap();
        let Specification::TypedJson(specification) =
            request.specification.unwrap().specification.unwrap()
        else {
            panic!("qualification definition was not Typed JSON")
        };
        let expected = [
            (
                "record_id",
                "unsigned",
                vec![IndexFieldCapability::Exact, IndexFieldCapability::Order],
            ),
            (
                "ecosystem",
                "keyword",
                vec![IndexFieldCapability::Exact, IndexFieldCapability::Facet],
            ),
            ("package", "keyword", vec![IndexFieldCapability::Exact]),
            ("severity", "keyword", vec![IndexFieldCapability::Exact]),
            (
                "active",
                "boolean",
                vec![IndexFieldCapability::Exact, IndexFieldCapability::Facet],
            ),
            ("withdrawn", "boolean", vec![IndexFieldCapability::Exact]),
            (
                "score",
                "float",
                vec![
                    IndexFieldCapability::Range,
                    IndexFieldCapability::Order,
                    IndexFieldCapability::Aggregate,
                ],
            ),
            (
                "published_day",
                "unsigned",
                vec![IndexFieldCapability::Exact],
            ),
            (
                "modified_day",
                "unsigned",
                vec![IndexFieldCapability::Order],
            ),
            ("sequence", "unsigned", vec![IndexFieldCapability::Exact]),
            ("source", "keyword", vec![IndexFieldCapability::Exact]),
            ("partition", "unsigned", vec![IndexFieldCapability::Exact]),
        ];

        assert_eq!(specification.fields.len(), expected.len());
        for (field, (name, kind, capabilities)) in specification.fields.iter().zip(expected) {
            let observed_kind = match field.field_type.as_ref().unwrap() {
                FieldType::Boolean(_) => "boolean",
                FieldType::UnsignedInteger(_) => "unsigned",
                FieldType::Float(_) => "float",
                FieldType::Keyword(_) => "keyword",
                other => panic!("unexpected qualification field type: {other:?}"),
            };
            assert_eq!(field.name, name);
            assert_eq!(observed_kind, kind);
            assert_eq!(
                field.capabilities,
                capabilities
                    .into_iter()
                    .map(|capability| capability as i32)
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(specification.physical_order, incident::physical_order());
    }

    #[test]
    fn record_path_parser_rejects_other_namespaces() {
        assert_eq!(parse_record_id("records/000000000042.json").unwrap(), 42);
        assert!(parse_record_id("other/000000000042.json").is_err());
        assert!(parse_record_id("records/not-a-number.json").is_err());
    }

    #[test]
    fn default_partitions_fit_one_exact_public_query_page() {
        let largest_partition = DEFAULT_RECORDS.div_ceil(PARTITION_COUNT);
        assert!(largest_partition <= u64::from(QUERY_LIMIT));
    }

    #[test]
    fn corpus_identity_hash_is_deterministic_and_input_sensitive() {
        assert_eq!(initial_corpus_sha256(42, 8), initial_corpus_sha256(42, 8));
        assert_ne!(initial_corpus_sha256(42, 8), initial_corpus_sha256(43, 8));
        assert_ne!(initial_corpus_sha256(42, 8), initial_corpus_sha256(42, 9));
    }

    #[test]
    fn source_complete_freshness_requires_every_unique_source_at_observed_tail() {
        let current = |node_id, indexed_next_offset| IndexSourceFreshness {
            node_id,
            source_epoch: vec![u8::try_from(node_id).unwrap(); 32],
            indexed_next_offset,
            observed_tail: Some(indexed_next_offset - 1),
            lag_hint: 0,
        };
        let mut freshness = IndexFreshness {
            sources: vec![current(1, 11), current(2, 21), current(3, 31)],
            ..Default::default()
        };
        assert!(source_complete_freshness(&freshness, 3));

        freshness.sources[2].lag_hint = 1;
        assert!(!source_complete_freshness(&freshness, 3));
        freshness.sources[2] = current(3, 31);
        freshness.sources[2].observed_tail = None;
        assert!(!source_complete_freshness(&freshness, 3));
        freshness.sources[2] = current(2, 31);
        assert!(!source_complete_freshness(&freshness, 3));
        assert!(!source_complete_freshness(&freshness, 2));
    }

    #[test]
    fn readiness_requires_a_new_complete_zero_lag_commit_revision() {
        let source = IndexSourceFreshness {
            node_id: 1,
            source_epoch: vec![1; 32],
            indexed_next_offset: 11,
            observed_tail: Some(10),
            lag_hint: 0,
        };
        let mut response = QueryIndexResponse {
            freshness: Some(IndexFreshness {
                commit_revision: 2,
                initial_build_complete: true,
                rebuilding: false,
                sources: vec![source],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(commit_revision_is_ready(&response, 1, Some(1)));
        assert!(!commit_revision_is_ready(&response, 2, Some(1)));
        assert!(!commit_revision_is_ready(&response, 1, Some(2)));

        response.freshness.as_mut().unwrap().sources[0].lag_hint = 1;
        assert!(!commit_revision_is_ready(&response, 1, Some(1)));
    }

    #[test]
    fn ready_partition_with_wrong_cardinality_fails_immediately() {
        let mut response = QueryIndexResponse {
            freshness: Some(IndexFreshness {
                commit_revision: 9,
                ..Default::default()
            }),
            ..Default::default()
        };

        ensure_expected_partition_hits(&response, 7, 0).unwrap();
        response.hits.push(Default::default());
        let error = ensure_expected_partition_hits(&response, 7, 2).unwrap_err();
        assert_eq!(
            error.to_string(),
            "ready index commit_revision 9 returned 1 hits for partition 7, expected 2"
        );
    }

    #[test]
    fn qualification_result_digest_is_order_independent_and_input_sensitive() {
        let ordered = digest_partition_records(7, &[(7, 11), (1_031, 12), (2_055, 13)]);
        let reordered = digest_partition_records(7, &[(2_055, 13), (7, 11), (1_031, 12)]);
        let changed_version = digest_partition_records(7, &[(7, 11), (1_031, 12), (2_055, 14)]);
        let changed_partition = digest_partition_records(8, &[(7, 11), (1_031, 12), (2_055, 13)]);
        assert_eq!(ordered, reordered);
        assert_ne!(ordered, changed_version);
        assert_ne!(ordered, changed_partition);

        let partitions = (0..PARTITION_COUNT)
            .map(|partition| (partition, digest_partition_records(partition, &[])))
            .collect::<BTreeMap<_, _>>();
        let complete = qualification_result_digest(&partitions).unwrap();
        validate_sha256(&complete).unwrap();

        let mut incomplete = partitions;
        incomplete.remove(&0);
        assert!(qualification_result_digest(&incomplete).is_err());
    }
}
