//! Production-shaped public-API qualification for Anvil's 0.6 index builder.
//!
//! The generated corpus deliberately resembles a broad, small-JSON indexing
//! workload without embedding any private schema or source data.

#[path = "v06_index_resource_qualification/data.rs"]
mod data;
#[path = "v06_index_resource_qualification/resource.rs"]
mod resource;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anvil_storage::v1::bulk_operation::Operation as BulkOperationValue;
use anvil_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_service_client::IndexServiceClient;
use anvil_storage::v1::index_specification::Specification as SpecificationValue;
use anvil_storage::v1::{
    BulkOperation, BulkPutRequest, BulkWriteRequest, CreateBucketRequest, CreateIndexRequest,
    DeleteRequest, Durability, IndexField, IndexPredicate, IndexPredicateOperator, IndexQuery,
    IndexSpecification, ObjectAddress, ObjectVersioning, QueryIndexRequest, QueryIndexResponse,
    TypedJsonIndexQuery, TypedJsonIndexSpec,
};
use anvil_storage::{
    BearerToken, RawClient, administration_client, connect_channel, exchange_client_credentials,
    object_client,
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use data::{PARTITION_COUNT, RecordFlavor};
use resource::{Phase, ResourceMonitor, ResourceReport};
use serde::Serialize;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const CONTENT_TYPE: &str = "application/json";
const DEFAULT_RECORDS: u64 = 839_980;
const DEFAULT_MUTATIONS: u64 = 2_048;
const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_WORKERS: usize = 4;
const DEFAULT_SEED: u64 = 0x625d_54af_f989_97f3;
const QUERY_LIMIT: u32 = 1_000;
const BUILD_TIMEOUT: Duration = Duration::from_secs(30 * 60);

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
    seed: u64,
    resource_pids: Vec<u32>,
    resource_containers: Vec<String>,
    require_resource_targets: bool,
    configured_kind_budget_bytes: Option<u64>,
    configured_compaction_max_lanes: Option<usize>,
    configured_rayon_workers: Option<usize>,
    max_anonymous_growth_bytes: Option<u64>,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct Timings {
    ingest_seconds: f64,
    initial_build_seconds: f64,
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
    batch_size: usize,
    updated_objects: u64,
    deleted_objects: u64,
    final_live_objects: u64,
    configured_kind_budget_bytes: Option<u64>,
    configured_compaction_max_lanes: Option<usize>,
    configured_rayon_workers: Option<usize>,
    max_anonymous_growth_bytes: Option<u64>,
    observed_peak_rss_growth_bytes: Option<u64>,
    observed_peak_anonymous_growth_bytes: Option<u64>,
    initial_generation: u64,
    final_generation: u64,
    timings: Timings,
    resources: Option<ResourceReport>,
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
    let config = Config::from_env()?;
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
        .create_index(CreateIndexRequest {
            bucket: config.bucket.to_string(),
            name: "records-by-field".into(),
            path_prefix: "records/".into(),
            content_type: CONTENT_TYPE.into(),
            specification: Some(IndexSpecification {
                specification: Some(SpecificationValue::TypedJson(TypedJsonIndexSpec {
                    fields: data::indexed_fields()
                        .into_iter()
                        .map(|(name, json_pointer)| IndexField {
                            name: name.into(),
                            json_pointer: json_pointer.into(),
                        })
                        .collect(),
                })),
            }),
            command_id: "v06-resource-create-index".into(),
        })
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
    let baseline = wait_for_generation(&mut index, &config.bucket, 0, None).await?;
    token = qualification_token(&config, channels[0].clone()).await?;
    let monitor = ResourceMonitor::start(
        &config.resource_pids,
        &config.resource_containers,
        Duration::from_millis(100),
        config.require_resource_targets,
    )?;

    let mut timings = Timings::default();
    set_phase(&monitor, Phase::Ingest);
    let started = Instant::now();
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
    let initial_response = wait_for_generation(
        &mut index,
        &config.bucket,
        baseline.generation(),
        Some(data::partition_paths(config.records, 7).len()),
    )
    .await?;
    timings.initial_build_seconds = started.elapsed().as_secs_f64();
    let initial_ready_generation = freshness(&initial_response)?.generation;

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
    let started = Instant::now();
    let (initial_count, initial_generation) = verify_every_partition(
        &mut index,
        &config.bucket,
        config.records,
        &initial_versions,
        None,
        None,
    )
    .await?;
    timings.exact_verification_seconds = started.elapsed().as_secs_f64();
    ensure!(initial_count == config.records);
    ensure!(initial_generation >= initial_ready_generation);

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
    let _final_ready =
        wait_for_generation(&mut index, &config.bucket, initial_generation, None).await?;
    token = qualification_token(&config, channels[0].clone()).await?;
    index = index_client(channels[0].clone(), &token)?;
    let (final_count, final_generation) = verify_every_partition(
        &mut index,
        &config.bucket,
        config.records,
        &initial_versions,
        Some(&expected_updates),
        Some(&deleted),
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
            "one or more monitored Anvil processes disappeared during qualification"
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
    let report = QualificationReport {
        schema: "anvil.index-resource-qualification.v1",
        records: config.records,
        indexed_fields: data::FIELD_COUNT,
        partitions: PARTITION_COUNT,
        ingest_workers: config.workers,
        batch_size: config.batch_size,
        updated_objects: expected_updates.len() as u64,
        deleted_objects: deleted.len() as u64,
        final_live_objects: final_live,
        configured_kind_budget_bytes: config.configured_kind_budget_bytes,
        configured_compaction_max_lanes: config.configured_compaction_max_lanes,
        configured_rayon_workers: config.configured_rayon_workers,
        max_anonymous_growth_bytes: config.max_anonymous_growth_bytes,
        observed_peak_rss_growth_bytes,
        observed_peak_anonymous_growth_bytes,
        initial_generation,
        final_generation,
        timings,
        resources,
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    if let Some(path) = config.output {
        std::fs::write(&path, &encoded)
            .with_context(|| format!("write qualification report {}", path.display()))?;
    }
    println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        let records = number("ANVIL_V06_RESOURCE_RECORDS", DEFAULT_RECORDS)?;
        let mutation_count = number("ANVIL_V06_RESOURCE_MUTATIONS", DEFAULT_MUTATIONS)?;
        let batch_size = number("ANVIL_V06_RESOURCE_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
        let workers = number("ANVIL_V06_RESOURCE_WORKERS", DEFAULT_WORKERS)?;
        ensure!(records > 0, "record count must be non-zero");
        ensure!(batch_size > 0, "batch size must be non-zero");
        ensure!(workers > 0, "worker count must be non-zero");
        let endpoints = required("ANVIL_V06_RESOURCE_ENDPOINTS")?
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        ensure!(!endpoints.is_empty(), "at least one endpoint is required");
        let resource_pids = optional_list("ANVIL_V06_RESOURCE_PIDS")
            .into_iter()
            .map(|value| value.parse().context("invalid resource PID"))
            .collect::<Result<Vec<_>>>()?;
        let resource_containers = optional_list("ANVIL_V06_RESOURCE_CONTAINERS");
        let require_resource_targets = boolean("ANVIL_V06_REQUIRE_RESOURCE_TARGETS", true)?;
        let configured_kind_budget_bytes = optional_number("ANVIL_V06_KIND_BUDGET_BYTES")?;
        let configured_compaction_max_lanes =
            optional_number("ANVIL_V06_INDEX_COMPACTION_MAX_LANES")?;
        let configured_rayon_workers = optional_number("ANVIL_V06_INDEX_RAYON_WORKERS")?;
        let max_anonymous_growth_bytes = optional_number("ANVIL_V06_MAX_ANONYMOUS_GROWTH_BYTES")?;
        ensure!(
            configured_compaction_max_lanes.is_none_or(|lanes| lanes > 0),
            "ANVIL_V06_INDEX_COMPACTION_MAX_LANES must be non-zero when configured"
        );
        if require_resource_targets {
            ensure!(
                !resource_pids.is_empty() || !resource_containers.is_empty(),
                "resource qualification requires a PID or container target"
            );
            ensure!(
                configured_kind_budget_bytes.is_some_and(|bytes| bytes > 0),
                "resource qualification requires a non-zero ANVIL_V06_KIND_BUDGET_BYTES"
            );
            ensure!(
                configured_rayon_workers.is_some_and(|workers| workers > 0),
                "resource qualification requires a non-zero ANVIL_V06_INDEX_RAYON_WORKERS"
            );
            ensure!(
                max_anonymous_growth_bytes.is_some(),
                "resource qualification requires ANVIL_V06_MAX_ANONYMOUS_GROWTH_BYTES"
            );
        }
        Ok(Self {
            endpoints,
            tenant: required("ANVIL_V06_RESOURCE_TENANT")?.into(),
            bucket: required("ANVIL_V06_RESOURCE_BUCKET")?.into(),
            client_id: required("ANVIL_V06_RESOURCE_CLIENT_ID")?,
            client_secret: required("ANVIL_V06_RESOURCE_CLIENT_SECRET")?,
            records,
            mutation_count,
            batch_size,
            workers,
            seed: number("ANVIL_V06_RESOURCE_SEED", DEFAULT_SEED)?,
            resource_pids,
            resource_containers,
            require_resource_targets,
            configured_kind_budget_bytes,
            configured_compaction_max_lanes,
            configured_rayon_workers,
            max_anonymous_growth_bytes,
            output: env::var_os("ANVIL_V06_RESOURCE_OUTPUT").map(PathBuf::from),
        })
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

async fn wait_for_generation(
    client: &mut IndexClient,
    bucket: &str,
    after_generation: u64,
    expected_partition_hits: Option<usize>,
) -> Result<QueryIndexResponse> {
    let deadline = Instant::now() + BUILD_TIMEOUT;
    let partition = 7;
    loop {
        match query_partition(client, bucket, partition).await {
            Ok(response)
                if freshness(&response).is_ok_and(|value| {
                    value.generation > after_generation
                        && value.initial_build_complete
                        && !value.rebuilding
                }) =>
            {
                if let Some(expected_hits) = expected_partition_hits {
                    if response.hits.len() != expected_hits {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
                return Ok(response);
            }
            Ok(_) => {}
            Err(error) if retryable_error(&error) => {}
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            bail!("index generation did not become ready within {BUILD_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn verify_every_partition(
    client: &mut IndexClient,
    bucket: &str,
    records: u64,
    initial_versions: &[u64],
    expected_updates: Option<&BTreeMap<u64, u64>>,
    deleted: Option<&BTreeSet<u64>>,
) -> Result<(u64, u64)> {
    let deadline = Instant::now() + BUILD_TIMEOUT;
    loop {
        let mut count = 0u64;
        let mut generation = None;
        let mut complete = true;
        for partition in 0..PARTITION_COUNT {
            let response = match query_partition(client, bucket, partition).await {
                Ok(response) => response,
                Err(error) if retryable_error(&error) => {
                    complete = false;
                    break;
                }
                Err(error) => return Err(error),
            };
            let response_generation = freshness(&response)?.generation;
            if generation.is_some_and(|value| value != response_generation) {
                complete = false;
                break;
            }
            generation = Some(response_generation);
            match validate_partition_response(
                &response,
                records,
                partition,
                initial_versions,
                expected_updates,
                deleted,
            ) {
                Ok(partition_count) => count += partition_count,
                Err(_) => {
                    complete = false;
                    break;
                }
            }
        }
        let expected = records - deleted.map_or(0, |values| values.len() as u64);
        if complete && count == expected {
            return Ok((
                count,
                generation.context("verification returned no generation")?,
            ));
        }
        if Instant::now() >= deadline {
            bail!("exact partition verification did not converge before timeout");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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
                    predicates: vec![IndexPredicate {
                        field: "partition".into(),
                        operator: IndexPredicateOperator::Equal as i32,
                        values_json: vec![partition.to_string().into_bytes()],
                    }],
                    order: Vec::new(),
                })),
            }),
            limit: QUERY_LIMIT,
            page_token: Vec::new(),
            tenant: String::new(),
        })
        .await
        .map(tonic::Response::into_inner)
        .map_err(Into::into)
}

fn freshness(response: &QueryIndexResponse) -> Result<&anvil_storage::v1::IndexFreshness> {
    response
        .freshness
        .as_ref()
        .context("index response omitted freshness")
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

trait GenerationResponse {
    fn generation(&self) -> u64;
}

impl GenerationResponse for QueryIndexResponse {
    fn generation(&self) -> u64 {
        self.freshness.as_ref().map_or(0, |value| value.generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
