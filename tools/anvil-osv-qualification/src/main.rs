use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use anvil_api::v1::administration_service_client::AdministrationServiceClient;
use anvil_api::v1::credential_service_client::CredentialServiceClient;
use anvil_api::v1::object_service_client::ObjectServiceClient;
use anvil_api::v1::{
    BucketPolicy, BulkOperation, BulkPutRequest, BulkWriteRequest, Durability,
    ExchangeClientCredentialsRequest, HeadObjectRequest, ListObjectsRequest, ObjectAddress,
    ObjectVersioning, SetBucketPolicyRequest, SetBucketVersioningRequest, bulk_operation,
    bulk_outcome, object_head,
};
use anvil_osv_qualification::{
    CorpusReport, DD_SCHEMA_SOURCE_COMMIT, ParsingReport, QUALIFICATION_SCHEMA,
    QualificationReport, ResultReport, RunCounts, SchemaShapeReport, SoftwareReport,
    TARGET_SECONDS, VerificationReport, WorkloadReport,
};
use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, ValueEnum};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use prost::Message as _;
use serde::{Serialize, Serializer, ser::SerializeMap as _};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, task::JoinSet};
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Channel, Endpoint},
};

const DD_OSV_BUCKET: &str = "dd-source-osv-raw";
const DEFAULT_SOURCE_URL: &str = "https://osv-vulnerabilities.storage.googleapis.com/all.zip";
const DEFAULT_SOURCE_CADENCE_HOURS: u16 = 6;
const CONTENT_TYPE_JSON: &str = "application/json";
const CONTENT_TYPE_SHARD: &str = "application/vnd.developer-defence.osv+ndjson+zstd";
const MAX_ARCHIVE_ENTRIES: usize = 2_000_000;
const MAX_DOCUMENT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_DECOMPRESSED_JSON_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_SHARD_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const MIN_SHARD_UNCOMPRESSED_BYTES: usize = 1024 * 1024;
const MAX_SHARD_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const SERVER_MAX_BULK_ITEMS: usize = 1_000;
const SERVER_MAX_BULK_ENCODED_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAXIMUM_BATCH_PAYLOAD_BYTES: usize = 60 * 1024 * 1024;
const CLIENT_MESSAGE_BYTES: usize = 72 * 1024 * 1024;
const PARSE_PROGRESS_INTERVAL: u64 = 25_000;
const CLIENT_PARTITIONS: &[&str] = &["cargo", "crates.io", "npm", "pypi"];

type Client = ObjectServiceClient<Channel>;
type AuthValue = MetadataValue<Ascii>;

#[derive(Debug, Parser)]
#[command(
    name = "anvil-osv-qualification",
    about = "Qualify Anvil 0.5 with Developer Defence's authoritative OSV shard schema"
)]
struct Args {
    /// URL of one clean Anvil 0.5 node, for example http://127.0.0.1:50051.
    #[arg(long)]
    endpoint: String,

    /// Durable application client ID used only to obtain a short-lived access token.
    #[arg(long)]
    client_id: String,

    /// Mode-0600 file containing only the durable application client secret.
    #[arg(long)]
    client_secret_file: PathBuf,

    #[arg(long)]
    tenant: String,

    #[arg(long, default_value = DD_OSV_BUCKET)]
    bucket: String,

    /// Local OSV ZIP corpus. The path is canonicalised before it is reported.
    #[arg(long)]
    corpus: PathBuf,

    /// Required lowercase SHA-256 pin for the exact ZIP bytes.
    #[arg(long)]
    corpus_sha256: String,

    /// Required YYYY-MM-DD identity from the acquired corpus, never the local clock.
    #[arg(long)]
    snapshot_day: String,

    /// Exact Anvil source revision being qualified.
    #[arg(long)]
    anvil_commit: String,

    /// Source URL embedded in the exact Developer Defence source-definition document.
    #[arg(long, default_value = DEFAULT_SOURCE_URL)]
    source_url: String,

    /// Source cadence embedded in the exact Developer Defence source definition.
    #[arg(long, default_value_t = DEFAULT_SOURCE_CADENCE_HOURS)]
    source_cadence_hours: u16,

    /// Replicated durability is unavailable in the single-node 0.5.0 gate.
    #[arg(long, value_enum, default_value = "local")]
    durability: DurabilityArgument,

    #[arg(long, default_value_t = 256)]
    batch_size: usize,

    #[arg(long, default_value_t = DEFAULT_MAXIMUM_BATCH_PAYLOAD_BYTES)]
    maximum_batch_payload_bytes: usize,

    /// Exact Developer Defence shard threshold; records themselves are never split.
    #[arg(long, default_value_t = DEFAULT_SHARD_UNCOMPRESSED_BYTES)]
    shard_uncompressed_bytes: usize,

    /// Maximum parallelism for shard compression and writes.
    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    #[arg(long, default_value_t = 64)]
    verification_concurrency: usize,

    /// Write the JSON report here as well as stdout.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Required acknowledgement; the tool also verifies the target is empty.
    #[arg(long)]
    confirm_clean_target: bool,
}

#[derive(Debug)]
struct RuntimeConfig {
    endpoint: String,
    tenant: String,
    bucket: String,
    corpus: PathBuf,
    corpus_path_display: String,
    corpus_sha256: String,
    corpus_bytes: u64,
    snapshot_day: String,
    snapshot_id: String,
    anvil_commit: String,
    source_url: String,
    source_cadence_hours: u16,
    durability: i32,
    durability_name: &'static str,
    batch_size: usize,
    maximum_batch_payload_bytes: usize,
    shard_uncompressed_bytes: usize,
    concurrency: usize,
    verification_concurrency: usize,
    output: Option<PathBuf>,
    auth: Option<AuthValue>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DurabilityArgument {
    Local,
    Replicated,
}

impl DurabilityArgument {
    fn api_value(self) -> i32 {
        match self {
            Self::Local => Durability::Local as i32,
            Self::Replicated => Durability::Replicated as i32,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Replicated => "replicated",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct SourceDefinition {
    schema: String,
    source_id: String,
    source_bucket: String,
    canonical_url: String,
    publisher: String,
    cadence_hours: u16,
    authentication_profile: String,
    downloaded_artifact_retention: String,
    redistribution_policy: String,
    enabled: bool,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
struct OsvSourceRecord {
    schema: String,
    source_id: String,
    source_record_id: String,
    record_identity_hash: String,
    content_sha256: String,
    ecosystem: String,
    package: String,
    normalised_ecosystem: String,
    normalised_package: String,
    modified_at: Option<String>,
    modified_day: String,
    published_at: Option<String>,
    withdrawn: bool,
    aliases: Vec<String>,
    summary: Option<String>,
    details: Option<String>,
    state: String,
    document: Value,
}

#[derive(Clone, Copy)]
struct ScopedDocument<'a> {
    base: &'a Map<String, Value>,
    affected: &'a [Value],
}

impl Serialize for ScopedDocument<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.base.len() + 1))?;
        let mut affected_written = false;
        for (key, value) in self.base {
            if !affected_written && key.as_str() > "affected" {
                map.serialize_entry("affected", self.affected)?;
                affected_written = true;
            }
            map.serialize_entry(key, value)?;
        }
        if !affected_written {
            map.serialize_entry("affected", self.affected)?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct OsvSourceRecordContent<'a> {
    schema: &'static str,
    source_id: &'static str,
    source_record_id: &'a str,
    ecosystem: &'a str,
    package: &'a str,
    normalised_ecosystem: &'a str,
    normalised_package: &'a str,
    modified_at: &'a Option<String>,
    published_at: &'a Option<String>,
    withdrawn: bool,
    aliases: &'a [String],
    summary: &'a Option<String>,
    details: &'a Option<String>,
    state: &'a str,
    document: ScopedDocument<'a>,
}

#[derive(Debug, PartialEq, Eq)]
struct PreparedRecord {
    encoded: Vec<u8>,
    normalised_ecosystem: String,
    modified_day: String,
}

#[derive(Debug)]
struct RecordJob {
    ecosystem: String,
    package: String,
    affected: Vec<Value>,
}

struct PreparedRecordJobs {
    document: Value,
    source_record_id: String,
    jobs: Vec<RecordJob>,
    has_unscoped: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct PreparedShard {
    partition: String,
    shard_index: u64,
    records_sha256: String,
    encoded_sha256: String,
    source_record_count: u64,
    uncompressed_bytes: u64,
    encoded_payload: Vec<u8>,
    ecosystems: Vec<String>,
    modified_day_min: String,
    modified_day_max: String,
}

#[derive(Clone, Debug, Serialize)]
struct OsvShardRef {
    partition: String,
    shard_index: u64,
    object_key: String,
    version_id: String,
    records_sha256: String,
    encoded_sha256: String,
    source_record_count: u64,
    uncompressed_bytes: u64,
    encoded_bytes: u64,
    ecosystems: Vec<String>,
    modified_day_min: String,
    modified_day_max: String,
}

#[derive(Debug, Serialize)]
struct OsvSnapshotManifest {
    schema: String,
    source_id: String,
    snapshot_id: String,
    snapshot_day: String,
    partition: String,
    content_digest: String,
    input_sha256: String,
    format: String,
    compression: String,
    source_record_count: u64,
    shard_count: u64,
    partitions: BTreeMap<String, u64>,
    shards: Vec<OsvShardRef>,
    state: String,
}

#[derive(Debug)]
enum PreparedObjectKind {
    SourceDefinition,
    Shard(PreparedShardDescriptor),
    Manifest,
}

#[derive(Debug)]
struct PreparedShardDescriptor {
    partition: String,
    shard_index: u64,
    records_sha256: String,
    encoded_sha256: String,
    source_record_count: u64,
    uncompressed_bytes: u64,
    ecosystems: Vec<String>,
    modified_day_min: String,
    modified_day_max: String,
}

#[derive(Debug)]
struct PreparedObject {
    path: String,
    payload: Vec<u8>,
    content_type: &'static str,
    command_id: String,
    kind: PreparedObjectKind,
}

#[derive(Clone, Debug)]
struct ExpectedObject {
    path: String,
    version: u64,
    content_length: u64,
    content_type: &'static str,
    content_blake3: [u8; 32],
}

#[derive(Debug)]
enum StoredObjectKind {
    SourceDefinition,
    Shard(OsvShardRef),
    Manifest,
}

#[derive(Debug)]
struct StoredObject {
    expected: ExpectedObject,
    kind: StoredObjectKind,
    replayed: bool,
}

#[derive(Debug)]
struct BatchWriteResult {
    objects: Vec<StoredObject>,
    payload_bytes: u64,
    latency: Duration,
}

#[derive(Debug, Default)]
struct DataPhaseResult {
    source_definition: Option<ExpectedObject>,
    shards: Vec<OsvShardRef>,
    expected: Vec<ExpectedObject>,
    source_definition_payload_bytes: u64,
    shard_payload_bytes: u64,
    request_count: u64,
    replayed: u64,
    latencies: Vec<Duration>,
}

struct ShardBuilder {
    partition: String,
    shard_index: u64,
    payload: Vec<u8>,
    source_record_count: u64,
    ecosystems: BTreeSet<String>,
    modified_day_min: Option<String>,
    modified_day_max: Option<String>,
}

impl ShardBuilder {
    fn new(partition: String, shard_index: u64, target_bytes: usize) -> Self {
        Self {
            partition,
            shard_index,
            payload: Vec::with_capacity(target_bytes),
            source_record_count: 0,
            ecosystems: BTreeSet::new(),
            modified_day_min: None,
            modified_day_max: None,
        }
    }

    fn would_exceed(&self, additional_bytes: usize, target_bytes: usize) -> bool {
        !self.payload.is_empty()
            && self.payload.len().saturating_add(additional_bytes) > target_bytes
    }

    fn push(&mut self, prepared: PreparedRecord) {
        self.payload.extend_from_slice(&prepared.encoded);
        self.payload.push(b'\n');
        self.source_record_count += 1;
        self.ecosystems.insert(prepared.normalised_ecosystem);
        update_min(&mut self.modified_day_min, &prepared.modified_day);
        update_max(&mut self.modified_day_max, &prepared.modified_day);
    }

    fn finish(self) -> Result<PreparedShard> {
        let records_sha256 = digest_bytes(&self.payload);
        let encoded_payload = zstd::stream::encode_all(Cursor::new(&self.payload), 6)?;
        let encoded_sha256 = digest_bytes(&encoded_payload);
        Ok(PreparedShard {
            partition: self.partition,
            shard_index: self.shard_index,
            records_sha256,
            encoded_sha256,
            source_record_count: self.source_record_count,
            uncompressed_bytes: self.payload.len() as u64,
            encoded_payload,
            ecosystems: self.ecosystems.into_iter().collect(),
            modified_day_min: self.modified_day_min.unwrap_or_else(|| "unknown".into()),
            modified_day_max: self.modified_day_max.unwrap_or_else(|| "unknown".into()),
        })
    }
}

struct ShardCompressor {
    worker_count: usize,
    output: mpsc::Sender<PreparedShard>,
    pending: Vec<thread::JoinHandle<Result<PreparedShard>>>,
    next_job_index: u64,
}

impl ShardCompressor {
    fn start(worker_count: usize, output: mpsc::Sender<PreparedShard>) -> Result<Self> {
        ensure!(
            worker_count > 0,
            "shard compression requires at least one worker"
        );
        Ok(Self {
            worker_count,
            output,
            pending: Vec::with_capacity(worker_count),
            next_job_index: 0,
        })
    }

    fn submit(&mut self, builder: ShardBuilder) -> Result<()> {
        let job_index = self.next_job_index;
        self.next_job_index += 1;
        let worker = match thread::Builder::new()
            .name(format!("osv-shard-compressor-{job_index}"))
            .spawn(move || builder.finish().context("compress OSV shard"))
        {
            Ok(worker) => worker,
            Err(error) => {
                // Existing jobs precede this one, so join the entire wave and
                // return their first input-order error before the spawn error.
                self.join_wave()?;
                return Err(error).context("start OSV shard compression worker");
            }
        };
        self.pending.push(worker);
        if self.pending.len() == self.worker_count {
            self.flush_wave()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush_wave()
    }

    fn flush_wave(&mut self) -> Result<()> {
        let shards = self.join_wave()?;
        for shard in shards {
            self.output.blocking_send(shard).map_err(|_| {
                anyhow::anyhow!("OSV shard consumer stopped before parsing completed")
            })?;
        }
        Ok(())
    }

    fn join_wave(&mut self) -> Result<Vec<PreparedShard>> {
        let workers = std::mem::take(&mut self.pending);
        let mut shards = Vec::with_capacity(workers.len());
        let mut first_error = None;
        for worker in workers {
            match worker.join() {
                Ok(Ok(shard)) => shards.push(shard),
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error =
                            Some(anyhow::anyhow!("OSV shard compression worker panicked"));
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(shards),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (mut config, client_id, client_secret) = validate_and_pin(args)?;
    let channel = Endpoint::from_shared(config.endpoint.clone())?
        .connect()
        .await
        .with_context(|| format!("connect to {}", config.endpoint))?;
    config.auth = Some(
        exchange_auth_value(channel.clone(), client_id, client_secret)
            .await
            .context("exchange OSV qualification client credentials")?,
    );
    let client = ObjectServiceClient::new(channel.clone())
        .max_encoding_message_size(CLIENT_MESSAGE_BYTES)
        .max_decoding_message_size(CLIENT_MESSAGE_BYTES);
    let versioning_changed = prepare_clean_bucket(&config, channel, client.clone()).await?;

    eprintln!(
        "event=osv_qualification_start corpus={} sha256={} snapshot_day={} snapshot_id={} archive_bytes={} shard_uncompressed_bytes={} batch_size={} maximum_batch_payload_bytes={} concurrency={} versioning_changed={}",
        config.corpus_path_display,
        config.corpus_sha256,
        config.snapshot_day,
        config.snapshot_id,
        config.corpus_bytes,
        config.shard_uncompressed_bytes,
        config.batch_size,
        config.maximum_batch_payload_bytes,
        config.concurrency,
        versioning_changed,
    );

    let ingest_started = Instant::now();
    let (mut data, parsing) = run_data_phase(&config, client.clone()).await?;
    ensure!(
        parsing.accepted_source_documents > 0,
        "the pinned corpus contains no accepted OSV source documents"
    );
    ensure!(
        !data.shards.is_empty(),
        "the pinned corpus produced no compressed source shards"
    );
    ensure!(
        data.shards
            .iter()
            .map(|shard| shard.source_record_count)
            .sum::<u64>()
            == parsing.normalised_source_records,
        "manifest shard record counts do not match parsed normalised records"
    );
    data.shards.sort_unstable_by(|left, right| {
        (&left.partition, left.shard_index, &left.records_sha256).cmp(&(
            &right.partition,
            right.shard_index,
            &right.records_sha256,
        ))
    });

    let manifest = snapshot_manifest(&config, parsing.normalised_source_records, data.shards);
    // DD derives its manifest mutation identity from the typed struct, while
    // its immutable JSON storage helper serializes through serde_json::Value.
    let manifest_sha256 = digest_bytes(&serde_json::to_vec(&manifest)?);
    let manifest_payload = dd_immutable_json_payload(&manifest)?;
    let manifest_result = send_batch(
        client.clone(),
        config.auth.clone(),
        Arc::from(config.tenant.as_str()),
        Arc::from(config.bucket.as_str()),
        config.durability,
        vec![PreparedObject {
            path: manifest_path(&config.snapshot_id),
            payload: manifest_payload,
            content_type: CONTENT_TYPE_JSON,
            command_id: command_id(&config.corpus_sha256, "manifest", &manifest_sha256),
            kind: PreparedObjectKind::Manifest,
        }],
    )
    .await
    .context("write authoritative OSV snapshot manifest")?;
    let ingest_elapsed = ingest_started.elapsed();

    let mut manifest_expected = None;
    let mut manifest_replayed = 0_u64;
    for stored in manifest_result.objects {
        ensure!(
            matches!(stored.kind, StoredObjectKind::Manifest),
            "manifest batch returned a non-manifest object"
        );
        manifest_replayed += u64::from(stored.replayed);
        manifest_expected = Some(stored.expected.clone());
        data.expected.push(stored.expected);
    }
    ensure!(
        manifest_expected.is_some(),
        "manifest batch omitted its object receipt"
    );

    let verification_started = Instant::now();
    let expected_object_count = data.expected.len() as u64;
    let verified = verify_current_objects(&config, client, &data.expected).await?;
    let verification_elapsed = verification_started.elapsed();

    let counts = RunCounts {
        source_documents: parsing.accepted_source_documents,
        normalised_source_records: parsing.normalised_source_records,
        shard_objects: data.expected.len().saturating_sub(2) as u64,
        source_definition_payload_bytes: data.source_definition_payload_bytes,
        shard_payload_bytes: data.shard_payload_bytes,
        manifest_payload_bytes: manifest_result.payload_bytes,
        data_bulk_requests: data.request_count,
        manifest_bulk_requests: 1,
        replayed_mutations: data.replayed.saturating_add(manifest_replayed),
    };
    let result = ResultReport::calculate(
        &counts,
        ingest_elapsed,
        &data.latencies,
        &[manifest_result.latency],
    );
    let passed = ingest_elapsed.as_secs_f64() <= TARGET_SECONDS
        && counts.replayed_mutations == 0
        && verified == expected_object_count;
    let report = QualificationReport {
        schema: QUALIFICATION_SCHEMA,
        measured: true,
        passed,
        target_seconds: TARGET_SECONDS,
        corpus: CorpusReport {
            path: &config.corpus_path_display,
            expected_sha256: &config.corpus_sha256,
            observed_sha256: &config.corpus_sha256,
            archive_bytes: config.corpus_bytes,
            snapshot_day: &config.snapshot_day,
            snapshot_id: &config.snapshot_id,
        },
        schema_shape: SchemaShapeReport::default(),
        software: SoftwareReport {
            anvil_commit: &config.anvil_commit,
            developer_defence_schema_source_commit: DD_SCHEMA_SOURCE_COMMIT,
        },
        workload: WorkloadReport {
            endpoint: &config.endpoint,
            tenant: &config.tenant,
            bucket: &config.bucket,
            source_url: &config.source_url,
            source_cadence_hours: config.source_cadence_hours,
            durability_class: config.durability_name,
            node_count: 1,
            batch_size_operations: config.batch_size,
            maximum_batch_payload_bytes: config.maximum_batch_payload_bytes,
            shard_uncompressed_target_bytes: config.shard_uncompressed_bytes,
            write_concurrency: config.concurrency,
            verification_concurrency: config.verification_concurrency,
            clean_target_verified: true,
        },
        parsing,
        result,
        verification: VerificationReport {
            expected_object_count,
            verified_object_count: verified,
            duration_seconds: verification_elapsed.as_secs_f64(),
        },
        limitations: vec![
            "The authoritative qualification shape is one source definition, immutable compressed content-addressed shards, and one immutable manifest; it deliberately does not create a raw object and mutable head for every upstream JSON document.",
            "Shard attributes are carried by the authoritative manifest. Anvil 0.5.0 receives no per-object user metadata and this tool creates no metadata sidecar.",
            "The required --snapshot-day and pinned archive hash determine the snapshot identity; the local clock never participates.",
            "Anvil 0.5 accepts exactly local or replicated durability: local performs the write and replicated is unavailable in this single-node qualification.",
            "Independent verification checks each current version, content length, content type, and BLAKE3 payload digest through HeadObject without downloading the payload again.",
        ],
    };
    emit_report(&report, config.output.as_deref())?;
    if !passed {
        bail!(
            "OSV qualification failed: elapsed={:.3}s target={TARGET_SECONDS:.3}s replayed={} verified={verified}/{expected_object_count}",
            ingest_elapsed.as_secs_f64(),
            counts.replayed_mutations,
        );
    }
    Ok(())
}

async fn prepare_clean_bucket(
    config: &RuntimeConfig,
    channel: Channel,
    mut client: Client,
) -> Result<bool> {
    let auth = config.auth.as_ref();
    let versioning = AdministrationServiceClient::new(channel)
        .set_bucket_versioning(request(
            SetBucketVersioningRequest {
                bucket: config.bucket.clone(),
                versioning: ObjectVersioning::Enabled as i32,
            },
            auth,
        ))
        .await
        .context("enable object versioning for the OSV qualification bucket")?
        .into_inner();
    ensure!(
        versioning.storage_tenant == config.tenant
            && versioning.bucket == config.bucket
            && versioning.versioning == ObjectVersioning::Enabled as i32,
        "bucket versioning response did not identify the requested enabled bucket"
    );
    let policy = client
        .set_bucket_policy(request(
            SetBucketPolicyRequest {
                tenant: config.tenant.clone(),
                bucket: config.bucket.clone(),
                policy: Some(BucketPolicy {
                    immutable_path_prefixes: vec![
                        "entities/source-definition".into(),
                        "shards/v1".into(),
                        "snapshots".into(),
                    ],
                    program_only_path_prefixes: Vec::new(),
                }),
            },
            auth,
        ))
        .await
        .context("install immutable OSV qualification paths")?
        .into_inner();
    ensure!(
        policy.immutable_path_prefixes
            == vec![
                String::from("entities/source-definition"),
                String::from("shards/v1"),
                String::from("snapshots"),
            ],
        "bucket policy response did not contain the exact qualification prefixes"
    );
    let page = client
        .list_objects(request(
            ListObjectsRequest {
                tenant: config.tenant.clone(),
                bucket: config.bucket.clone(),
                prefix: String::new(),
                start_after: None,
                limit: 1,
            },
            auth,
        ))
        .await
        .context("verify the OSV qualification bucket is empty")?
        .into_inner();
    ensure!(
        page.paths.is_empty() && !page.has_more,
        "qualification target is not empty; first path is {:?}",
        page.paths.first()
    );
    Ok(versioning.changed)
}

fn validate_and_pin(args: Args) -> Result<(RuntimeConfig, String, String)> {
    ensure!(
        args.confirm_clean_target,
        "--confirm-clean-target is required because this command writes the supplied tenant/bucket"
    );
    validate_canonical("tenant", &args.tenant)?;
    validate_canonical("bucket", &args.bucket)?;
    ensure!(
        args.bucket == DD_OSV_BUCKET,
        "the Developer Defence qualification bucket must be {DD_OSV_BUCKET}"
    );
    validate_snapshot_day(&args.snapshot_day)?;
    validate_git_commit("--anvil-commit", &args.anvil_commit)?;
    validate_canonical("client ID", &args.client_id)?;
    ensure!(
        args.source_url.starts_with("https://")
            && args.source_url.trim() == args.source_url
            && !args.source_url.chars().any(char::is_whitespace),
        "--source-url must be a canonical HTTPS URL"
    );
    ensure!(
        (1..=168).contains(&args.source_cadence_hours),
        "--source-cadence-hours must be between 1 and 168"
    );
    ensure!(
        matches!(args.durability, DurabilityArgument::Local),
        "Anvil 0.5.0 OSV qualification requires --durability local"
    );
    ensure!(
        (1..=SERVER_MAX_BULK_ITEMS).contains(&args.batch_size),
        "--batch-size must be between 1 and {SERVER_MAX_BULK_ITEMS}"
    );
    ensure!(
        (1..=SERVER_MAX_BULK_ENCODED_BYTES).contains(&args.maximum_batch_payload_bytes),
        "--maximum-batch-payload-bytes must be between 1 and {SERVER_MAX_BULK_ENCODED_BYTES}"
    );
    ensure!(
        (MIN_SHARD_UNCOMPRESSED_BYTES..=MAX_SHARD_UNCOMPRESSED_BYTES)
            .contains(&args.shard_uncompressed_bytes),
        "--shard-uncompressed-bytes must be between {MIN_SHARD_UNCOMPRESSED_BYTES} and {MAX_SHARD_UNCOMPRESSED_BYTES}"
    );
    ensure!(
        (1..=16).contains(&args.concurrency),
        "--concurrency must be between 1 and 16"
    );
    ensure!(
        args.verification_concurrency > 0,
        "--verification-concurrency must be non-zero"
    );
    validate_sha256("--corpus-sha256", &args.corpus_sha256)?;
    let corpus = args
        .corpus
        .canonicalize()
        .with_context(|| format!("canonicalise corpus path {}", args.corpus.display()))?;
    let metadata = corpus
        .metadata()
        .with_context(|| format!("stat corpus path {}", corpus.display()))?;
    ensure!(metadata.is_file(), "corpus path must name a regular file");
    let observed = sha256_file(&corpus)?;
    ensure!(
        observed == args.corpus_sha256,
        "corpus SHA-256 mismatch: expected {}, observed {observed}",
        args.corpus_sha256
    );
    let client_secret = read_client_secret(&args.client_secret_file)?;
    let snapshot_id = format!("osv-{}-{}", args.snapshot_day, &observed[..24]);
    Ok((
        RuntimeConfig {
            endpoint: args.endpoint,
            tenant: args.tenant,
            bucket: args.bucket,
            corpus_path_display: corpus.display().to_string(),
            corpus,
            corpus_sha256: observed,
            corpus_bytes: metadata.len(),
            snapshot_day: args.snapshot_day,
            snapshot_id,
            anvil_commit: args.anvil_commit,
            source_url: args.source_url,
            source_cadence_hours: args.source_cadence_hours,
            durability: args.durability.api_value(),
            durability_name: args.durability.name(),
            batch_size: args.batch_size,
            maximum_batch_payload_bytes: args.maximum_batch_payload_bytes,
            shard_uncompressed_bytes: args.shard_uncompressed_bytes,
            concurrency: args.concurrency,
            verification_concurrency: args.verification_concurrency,
            output: args.output,
            auth: None,
        },
        args.client_id,
        client_secret,
    ))
}

fn validate_canonical(name: &str, value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.trim() == value,
        "{name} must be non-empty and have no surrounding whitespace"
    );
    ensure!(!value.contains('\0'), "{name} must not contain NUL");
    Ok(())
}

fn validate_snapshot_day(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    ensure!(
        bytes.len() == 10
            && bytes[4] == b'-'
            && bytes[7] == b'-'
            && bytes
                .iter()
                .enumerate()
                .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit()),
        "--snapshot-day must use YYYY-MM-DD"
    );
    Ok(())
}

fn validate_sha256(name: &str, value: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{name} must be 64 lowercase hexadecimal characters"
    );
    Ok(())
}

fn validate_git_commit(name: &str, value: &str) -> Result<()> {
    ensure!(
        value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{name} must be an exact 40-character hexadecimal commit ID"
    );
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn read_client_secret(path: &Path) -> Result<String> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("stat client secret file {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "client secret path must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        ensure!(
            metadata.permissions().mode() & 0o7777 == 0o600,
            "client secret file must have mode 0600"
        );
    }
    ensure!(
        metadata.len() <= 4 * 1024,
        "client secret file exceeds 4096 bytes"
    );
    let secret = std::fs::read_to_string(path)
        .with_context(|| format!("read client secret file {}", path.display()))?;
    let secret = secret.trim();
    validate_client_secret_value(secret)?;
    Ok(secret.to_owned())
}

fn validate_client_secret_value(secret: &str) -> Result<()> {
    ensure!(
        (32..=4 * 1024).contains(&secret.len()),
        "client secret must contain between 32 and 4096 UTF-8 bytes"
    );
    Ok(())
}

async fn exchange_auth_value(
    channel: Channel,
    client_id: String,
    client_secret: String,
) -> Result<AuthValue> {
    let token = CredentialServiceClient::new(channel)
        .exchange_client_credentials(ExchangeClientCredentialsRequest {
            client_id,
            client_secret,
        })
        .await?
        .into_inner();
    ensure!(
        token.token_type == "Bearer",
        "credential exchange returned an unsupported token type"
    );
    ensure!(
        token.expires_in_seconds > 0,
        "credential exchange returned an already-expired token"
    );
    format!("Bearer {}", token.access_token)
        .parse()
        .context("access token contains bytes invalid for gRPC metadata")
}

async fn run_data_phase(
    config: &RuntimeConfig,
    client: Client,
) -> Result<(DataPhaseResult, ParsingReport)> {
    let (sender, mut receiver) = mpsc::channel(config.concurrency.saturating_mul(2).max(1));
    let corpus = config.corpus.clone();
    let target = config.shard_uncompressed_bytes;
    let compression_workers = config.concurrency;
    let parser = tokio::task::spawn_blocking(move || {
        parse_archive(corpus, target, compression_workers, sender)
    });
    let mut pending = JoinSet::new();
    let mut phase = DataPhaseResult::default();
    let mut batch = vec![source_definition_object(config)?];
    let mut batch_payload_bytes = batch[0].payload.len();

    while let Some(shard) = receiver.recv().await {
        let object = shard_object(config, shard)?;
        ensure!(
            object.payload.len() <= config.maximum_batch_payload_bytes,
            "encoded shard {} has {} bytes, exceeding --maximum-batch-payload-bytes {}; lower --shard-uncompressed-bytes",
            object.path,
            object.payload.len(),
            config.maximum_batch_payload_bytes
        );
        if batch_would_overflow(
            batch.len(),
            batch_payload_bytes,
            object.payload.len(),
            config.batch_size,
            config.maximum_batch_payload_bytes,
        ) {
            spawn_data_batch(config, &client, &mut pending, std::mem::take(&mut batch));
            batch_payload_bytes = 0;
            if pending.len() >= config.concurrency {
                collect_data_batch(pending.join_next().await, &mut phase)?;
            }
        }
        batch_payload_bytes = batch_payload_bytes
            .checked_add(object.payload.len())
            .context("data batch payload byte count overflowed")?;
        batch.push(object);
    }
    if !batch.is_empty() {
        spawn_data_batch(config, &client, &mut pending, batch);
    }
    while let Some(completed) = pending.join_next().await {
        collect_data_batch(Some(completed), &mut phase)?;
    }
    let parsing = parser.await.context("OSV parser task panicked")??;
    ensure!(
        phase.source_definition.is_some(),
        "data phase did not persist the source definition"
    );
    Ok((phase, parsing))
}

fn spawn_data_batch(
    config: &RuntimeConfig,
    client: &Client,
    pending: &mut JoinSet<Result<BatchWriteResult>>,
    objects: Vec<PreparedObject>,
) {
    let client = client.clone();
    let auth = config.auth.clone();
    let tenant = Arc::<str>::from(config.tenant.as_str());
    let bucket = Arc::<str>::from(config.bucket.as_str());
    let durability = config.durability;
    pending
        .spawn(async move { send_batch(client, auth, tenant, bucket, durability, objects).await });
}

fn collect_data_batch(
    completed: Option<Result<Result<BatchWriteResult>, tokio::task::JoinError>>,
    phase: &mut DataPhaseResult,
) -> Result<()> {
    let completed = completed
        .context("data request task set ended unexpectedly")?
        .context("data request task panicked")??;
    phase.request_count = phase.request_count.saturating_add(1);
    phase.latencies.push(completed.latency);
    for stored in completed.objects {
        phase.replayed = phase.replayed.saturating_add(u64::from(stored.replayed));
        match stored.kind {
            StoredObjectKind::SourceDefinition => {
                ensure!(
                    phase.source_definition.is_none(),
                    "source definition was persisted more than once"
                );
                phase.source_definition_payload_bytes = phase
                    .source_definition_payload_bytes
                    .saturating_add(stored.expected.content_length);
                phase.source_definition = Some(stored.expected.clone());
            }
            StoredObjectKind::Shard(shard) => {
                phase.shard_payload_bytes = phase
                    .shard_payload_bytes
                    .saturating_add(stored.expected.content_length);
                phase.shards.push(shard);
            }
            StoredObjectKind::Manifest => {
                bail!("manifest escaped into the independent data phase")
            }
        }
        phase.expected.push(stored.expected);
    }
    ensure!(
        completed.payload_bytes > 0,
        "BulkWrite reported an empty data batch"
    );
    Ok(())
}

async fn send_batch(
    mut client: Client,
    auth: Option<AuthValue>,
    tenant: Arc<str>,
    bucket: Arc<str>,
    durability: i32,
    objects: Vec<PreparedObject>,
) -> Result<BatchWriteResult> {
    ensure!(!objects.is_empty(), "cannot send an empty BulkWrite batch");
    let payload_bytes = objects.iter().try_fold(0_u64, |total, object| {
        total.checked_add(object.payload.len() as u64)
    });
    let payload_bytes = payload_bytes.context("BulkWrite payload byte count overflowed")?;
    let expected_commands = objects
        .iter()
        .map(|object| object.command_id.clone())
        .collect::<Vec<_>>();
    let operations = objects
        .iter()
        .map(|object| BulkOperation {
            operation: Some(bulk_operation::Operation::PutImmutable(BulkPutRequest {
                address: Some(address(&tenant, &bucket, &object.path)),
                bytes: object.payload.clone(),
                content_type: object.content_type.into(),
                command_id: object.command_id.clone(),
                durability,
            })),
        })
        .collect();
    let request_message = BulkWriteRequest { operations };
    ensure!(
        request_message.encoded_len() <= SERVER_MAX_BULK_ENCODED_BYTES,
        "BulkWrite protobuf encoding is {} bytes, exceeding the server limit {SERVER_MAX_BULK_ENCODED_BYTES}; lower --batch-size or --maximum-batch-payload-bytes",
        request_message.encoded_len()
    );
    let started = Instant::now();
    let response = client
        .bulk_write(request(request_message, auth.as_ref()))
        .await
        .context("OSV BulkWrite RPC failed")?
        .into_inner();
    let latency = started.elapsed();
    let receipts = ordered_receipts(response.outcomes, &expected_commands)?;
    let objects = objects
        .into_iter()
        .zip(receipts)
        .map(|(object, receipt)| {
            ensure!(receipt.version > 0, "BulkWrite receipt omitted its version");
            let content_blake3 = *blake3::hash(&object.payload).as_bytes();
            let encoded_bytes = object.payload.len() as u64;
            let kind = match object.kind {
                PreparedObjectKind::SourceDefinition => StoredObjectKind::SourceDefinition,
                PreparedObjectKind::Manifest => StoredObjectKind::Manifest,
                PreparedObjectKind::Shard(shard) => StoredObjectKind::Shard(OsvShardRef {
                    partition: shard.partition,
                    shard_index: shard.shard_index,
                    object_key: object.path.clone(),
                    version_id: receipt.version.to_string(),
                    records_sha256: shard.records_sha256,
                    encoded_sha256: shard.encoded_sha256,
                    source_record_count: shard.source_record_count,
                    uncompressed_bytes: shard.uncompressed_bytes,
                    encoded_bytes,
                    ecosystems: shard.ecosystems,
                    modified_day_min: shard.modified_day_min,
                    modified_day_max: shard.modified_day_max,
                }),
            };
            Ok(StoredObject {
                expected: ExpectedObject {
                    path: object.path,
                    version: receipt.version,
                    content_length: encoded_bytes,
                    content_type: object.content_type,
                    content_blake3,
                },
                kind,
                replayed: receipt.replayed,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BatchWriteResult {
        objects,
        payload_bytes,
        latency,
    })
}

fn ordered_receipts(
    outcomes: Vec<anvil_api::v1::BulkOutcome>,
    expected_commands: &[String],
) -> Result<Vec<anvil_api::v1::MutationReceipt>> {
    ensure!(
        outcomes.len() == expected_commands.len(),
        "BulkWrite returned {} outcomes for {} operations",
        outcomes.len(),
        expected_commands.len()
    );
    let mut ordered = std::iter::repeat_with(|| None)
        .take(expected_commands.len())
        .collect::<Vec<_>>();
    for outcome in outcomes {
        let index = outcome.index as usize;
        ensure!(
            index < ordered.len(),
            "BulkWrite returned out-of-range index {index}"
        );
        ensure!(
            ordered[index].is_none(),
            "BulkWrite returned duplicate index {index}"
        );
        let receipt = match outcome
            .outcome
            .context("BulkWrite outcome omitted its value")?
        {
            bulk_outcome::Outcome::Receipt(receipt) => receipt,
            bulk_outcome::Outcome::Failure(failure) => bail!(
                "BulkWrite operation {index} failed: code={} current_version={:?}: {}",
                failure.code,
                failure.current_version,
                failure.message
            ),
        };
        ensure!(
            receipt.command_id == expected_commands[index],
            "BulkWrite operation {index} returned command_id {:?}, expected {:?}",
            receipt.command_id,
            expected_commands[index]
        );
        ensure!(
            !receipt.deleted,
            "BulkWrite put operation {index} returned deleted=true"
        );
        ordered[index] = Some(receipt);
    }
    ordered
        .into_iter()
        .enumerate()
        .map(|(index, receipt)| receipt.with_context(|| format!("missing BulkWrite index {index}")))
        .collect()
}

fn source_definition_object(config: &RuntimeConfig) -> Result<PreparedObject> {
    let definition = SourceDefinition {
        schema: "developer-defence.source-definition.v2".into(),
        source_id: "osv".into(),
        source_bucket: DD_OSV_BUCKET.into(),
        canonical_url: config.source_url.clone(),
        publisher: "Google OSV".into(),
        cadence_hours: config.source_cadence_hours,
        authentication_profile: "public-https".into(),
        downloaded_artifact_retention: "ephemeral-until-shard-manifest-commit".into(),
        redistribution_policy: "record-level-upstream-rights".into(),
        enabled: true,
    };
    // Match DD's distinct mutation-identity and immutable-payload boundaries.
    let content_sha256 = digest_bytes(&serde_json::to_vec(&definition)?);
    let payload = dd_immutable_json_payload(&definition)?;
    Ok(PreparedObject {
        path: source_definition_path(),
        payload,
        content_type: CONTENT_TYPE_JSON,
        command_id: command_id(&config.corpus_sha256, "source-definition", &content_sha256),
        kind: PreparedObjectKind::SourceDefinition,
    })
}

fn shard_object(config: &RuntimeConfig, shard: PreparedShard) -> Result<PreparedObject> {
    ensure!(
        digest_bytes(&shard.encoded_payload) == shard.encoded_sha256,
        "encoded shard digest changed before persistence"
    );
    let path = shard_path(&shard.records_sha256);
    Ok(PreparedObject {
        path,
        command_id: command_id(&config.corpus_sha256, "shard", &shard.records_sha256),
        content_type: CONTENT_TYPE_SHARD,
        payload: shard.encoded_payload,
        kind: PreparedObjectKind::Shard(PreparedShardDescriptor {
            partition: shard.partition,
            shard_index: shard.shard_index,
            records_sha256: shard.records_sha256,
            encoded_sha256: shard.encoded_sha256,
            source_record_count: shard.source_record_count,
            uncompressed_bytes: shard.uncompressed_bytes,
            ecosystems: shard.ecosystems,
            modified_day_min: shard.modified_day_min,
            modified_day_max: shard.modified_day_max,
        }),
    })
}

fn snapshot_manifest(
    config: &RuntimeConfig,
    source_record_count: u64,
    shards: Vec<OsvShardRef>,
) -> OsvSnapshotManifest {
    let partitions = shards.iter().fold(BTreeMap::new(), |mut counts, shard| {
        *counts.entry(shard.partition.clone()).or_insert(0_u64) += 1;
        counts
    });
    OsvSnapshotManifest {
        schema: "developer-defence.osv-snapshot-manifest.v1".into(),
        source_id: "osv".into(),
        snapshot_id: config.snapshot_id.clone(),
        snapshot_day: config.snapshot_day.clone(),
        partition: "manifest".into(),
        content_digest: config.corpus_sha256.clone(),
        input_sha256: config.corpus_sha256.clone(),
        format: "developer-defence.osv-source-record.ndjson.v1".into(),
        compression: "zstd-6".into(),
        source_record_count,
        shard_count: shards.len() as u64,
        partitions,
        shards,
        state: "committed".into(),
    }
}

fn parse_archive(
    path: PathBuf,
    target_bytes: usize,
    compression_workers: usize,
    sender: mpsc::Sender<PreparedShard>,
) -> Result<ParsingReport> {
    let mut archive = zip::ZipArchive::new(File::open(path)?)?;
    ensure!(
        archive.len() <= MAX_ARCHIVE_ENTRIES,
        "OSV archive has {} entries, exceeding the {MAX_ARCHIVE_ENTRIES} entry bound",
        archive.len()
    );
    let mut report = ParsingReport {
        archive_entries: archive.len() as u64,
        ..ParsingReport::default()
    };
    let mut builders = BTreeMap::<String, ShardBuilder>::new();
    let mut next_indices = BTreeMap::<String, u64>::new();
    let mut compressor = ShardCompressor::start(compression_workers, sender)?;

    let parsing = (|| -> Result<ParsingReport> {
        for index in 0..archive.len() {
            let mut entry = archive.by_index(index)?;
            if entry.is_dir() || !entry.name().ends_with(".json") {
                continue;
            }
            report.json_documents += 1;
            if report
                .json_documents
                .is_multiple_of(PARSE_PROGRESS_INTERVAL)
            {
                eprintln!(
                    "event=osv_parse_progress json_documents={}",
                    report.json_documents
                );
            }
            if entry.size() > MAX_DOCUMENT_BYTES {
                report.oversized_documents += 1;
                continue;
            }
            let mut bytes = Vec::with_capacity(entry.size() as usize);
            entry
                .by_ref()
                .take(MAX_DOCUMENT_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
                report.oversized_documents += 1;
                continue;
            }
            report.decompressed_json_bytes = report
                .decompressed_json_bytes
                .checked_add(bytes.len() as u64)
                .context("OSV decompressed JSON byte count overflowed")?;
            ensure!(
                report.decompressed_json_bytes <= MAX_DECOMPRESSED_JSON_BYTES,
                "OSV decompressed JSON exceeds the {MAX_DECOMPRESSED_JSON_BYTES} byte bound"
            );
            let document = match serde_json::from_slice::<Value>(&bytes) {
                Ok(document) => document,
                Err(_) => {
                    report.malformed_documents += 1;
                    continue;
                }
            };
            let prepared = match prepare_record_jobs(document) {
                Ok(prepared) => prepared,
                Err(_) => {
                    report.malformed_documents += 1;
                    continue;
                }
            };
            let PreparedRecordJobs {
                document,
                source_record_id,
                jobs,
                has_unscoped,
            } = prepared;
            if has_unscoped {
                report.unscoped_documents += 1;
            }
            for job in jobs {
                let record = prepare_record(&document, &source_record_id, &job)
                    .context("prepare OSV source record")?;
                push_record_into_shards(
                    record,
                    target_bytes,
                    &mut compressor,
                    &mut builders,
                    &mut next_indices,
                )?;
                report.normalised_source_records += 1;
            }
            report.accepted_source_documents += 1;
        }
        for builder in builders.into_values() {
            compressor.submit(builder)?;
        }
        Ok(report)
    })();
    let compression = compressor.finish();
    match (parsing, compression) {
        (Ok(report), Ok(())) => Ok(report),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(compression_error)) => Err(error.context(format!(
            "OSV shard compression also failed: {compression_error:#}"
        ))),
    }
}

fn push_record_into_shards(
    prepared: PreparedRecord,
    target_bytes: usize,
    compressor: &mut ShardCompressor,
    builders: &mut BTreeMap<String, ShardBuilder>,
    next_indices: &mut BTreeMap<String, u64>,
) -> Result<()> {
    let partition = storage_partition(&prepared.normalised_ecosystem).to_string();
    if builders
        .get(&partition)
        .is_some_and(|builder| builder.would_exceed(prepared.encoded.len() + 1, target_bytes))
    {
        let completed = builders
            .remove(&partition)
            .expect("the partition builder was checked above");
        compressor.submit(completed)?;
    }
    let builder = builders.entry(partition.clone()).or_insert_with(|| {
        let index = next_indices.entry(partition.clone()).or_default();
        let builder = ShardBuilder::new(partition, *index, target_bytes);
        *index += 1;
        builder
    });
    builder.push(prepared);
    Ok(())
}

fn prepare_record_jobs(mut document: Value) -> Result<PreparedRecordJobs> {
    let source_record_id = document
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("OSV document has no id")?
        .to_string();
    let affected = match document
        .as_object_mut()
        .context("OSV document must be a JSON object")?
        .remove("affected")
    {
        Some(Value::Array(affected)) => affected,
        _ => Vec::new(),
    };
    let mut package_groups = BTreeMap::<(String, String), Vec<Value>>::new();
    let mut unscoped = Vec::new();
    for item in affected {
        match canonical_package_identity(&item) {
            Some(identity) => package_groups.entry(identity).or_default().push(item),
            None => unscoped.push(item),
        }
    }

    let has_unscoped = package_groups.is_empty() || !unscoped.is_empty();
    let mut jobs = package_groups
        .into_iter()
        .map(|((ecosystem, package), affected)| RecordJob {
            ecosystem,
            package,
            affected,
        })
        .collect::<Vec<_>>();
    if has_unscoped {
        jobs.push(RecordJob {
            ecosystem: "unscoped".into(),
            package: source_record_id.clone(),
            affected: unscoped,
        });
    }
    Ok(PreparedRecordJobs {
        document,
        source_record_id,
        jobs,
        has_unscoped,
    })
}

fn package_identity(affected: &Value) -> Option<(String, String)> {
    let package = affected.get("package")?.as_object()?;
    let ecosystem = package.get("ecosystem")?.as_str()?.trim().to_string();
    let name = package.get("name")?.as_str()?.trim().to_string();
    if ecosystem.is_empty() || name.is_empty() {
        return None;
    }
    Some((ecosystem, name))
}

fn canonical_package_identity(affected: &Value) -> Option<(String, String)> {
    let (ecosystem, package) = package_identity(affected)?;
    let lower_ecosystem = ecosystem.to_ascii_lowercase();
    let canonical_ecosystem = match lower_ecosystem.as_str() {
        "cargo" | "crates.io" => "crates.io".to_string(),
        _ => lower_ecosystem,
    };
    let canonical_package = normalize_package_name(&ecosystem, &package);
    Some((canonical_ecosystem, canonical_package))
}

fn normalize_package_name(ecosystem: &str, package: &str) -> String {
    match ecosystem.to_ascii_lowercase().as_str() {
        "pypi" => package
            .to_ascii_lowercase()
            .replace(['_', '.'], "-")
            .split('-')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("-"),
        "npm" => package.to_ascii_lowercase(),
        "cargo" | "crates.io" => package.to_string(),
        _ => package.to_string(),
    }
}

fn prepare_record(
    document: &Value,
    source_record_id: &str,
    job: &RecordJob,
) -> Result<PreparedRecord> {
    let RecordJob {
        ecosystem,
        package,
        affected,
    } = job;
    let base = document
        .as_object()
        .context("OSV document must be a JSON object")?;
    ensure!(
        !base.contains_key("affected"),
        "OSV base document still contains affected"
    );
    let scoped_document = ScopedDocument { base, affected };
    let normalised_ecosystem = ecosystem.trim().to_ascii_lowercase();
    let normalised_package = normalize_package_name(ecosystem, package);
    let record_identity_hash = digest_bytes(
        format!(
            "osv\0{}\0{normalised_ecosystem}\0{normalised_package}",
            source_record_id
        )
        .as_bytes(),
    );
    let modified_at = string_field(document, "modified");
    let modified_day = timestamp_day(modified_at.as_deref());
    let published_at = string_field(document, "published");
    let withdrawn = document
        .get("withdrawn")
        .is_some_and(|value| !value.is_null());
    let aliases = string_array(document, "aliases");
    let summary = string_field(document, "summary");
    let details = string_field(document, "details");
    let state = if withdrawn { "withdrawn" } else { "active" };
    let content = serde_json::to_vec(&OsvSourceRecordContent {
        schema: "developer-defence.osv-source-record.v1",
        source_id: "osv",
        source_record_id,
        ecosystem,
        package,
        normalised_ecosystem: &normalised_ecosystem,
        normalised_package: &normalised_package,
        modified_at: &modified_at,
        published_at: &published_at,
        withdrawn,
        aliases: &aliases,
        summary: &summary,
        details: &details,
        state,
        document: scoped_document,
    })?;
    let content_sha256 = digest_bytes(&content);
    let encoded = encode_final_record(
        &content,
        &record_identity_hash,
        &content_sha256,
        &modified_day,
    )?;
    Ok(PreparedRecord {
        encoded,
        normalised_ecosystem,
        modified_day,
    })
}

fn encode_final_record(
    content: &[u8],
    record_identity_hash: &str,
    content_sha256: &str,
    modified_day: &str,
) -> Result<Vec<u8>> {
    const ECOSYSTEM_FIELD: &[u8] = b",\"ecosystem\":";
    const PUBLISHED_AT_FIELD: &[u8] = b",\"published_at\":";

    let identity_offset = find_bytes(content, ECOSYSTEM_FIELD)
        .context("encoded OSV content has no ecosystem field boundary")?;
    let modified_day_offset = identity_offset
        + find_bytes(&content[identity_offset..], PUBLISHED_AT_FIELD)
            .context("encoded OSV content has no published_at field boundary")?;
    let mut encoded = Vec::with_capacity(content.len() + 256);
    encoded.extend_from_slice(&content[..identity_offset]);
    write_json_field(
        &mut encoded,
        b",\"record_identity_hash\":",
        record_identity_hash,
    )?;
    write_json_field(&mut encoded, b",\"content_sha256\":", content_sha256)?;
    encoded.extend_from_slice(&content[identity_offset..modified_day_offset]);
    write_json_field(&mut encoded, b",\"modified_day\":", modified_day)?;
    encoded.extend_from_slice(&content[modified_day_offset..]);
    Ok(encoded)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn write_json_field<T: Serialize + ?Sized>(
    encoded: &mut Vec<u8>,
    prefix: &[u8],
    value: &T,
) -> Result<()> {
    encoded.extend_from_slice(prefix);
    serde_json::to_writer(encoded, value)?;
    Ok(())
}

fn storage_partition(normalised_ecosystem: &str) -> &str {
    CLIENT_PARTITIONS
        .iter()
        .copied()
        .find(|candidate| *candidate == normalised_ecosystem)
        .unwrap_or("other")
}

fn string_field(document: &Value, field: &str) -> Option<String> {
    document
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn string_array(document: &Value, field: &str) -> Vec<String> {
    let mut values = document
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values
}

fn timestamp_day(value: Option<&str>) -> String {
    value
        .filter(|value| {
            let bytes = value.as_bytes();
            bytes.len() >= 10
                && bytes[4] == b'-'
                && bytes[7] == b'-'
                && bytes[..10]
                    .iter()
                    .enumerate()
                    .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
        })
        .map(|value| value[..10].to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn update_min(current: &mut Option<String>, candidate: &str) {
    if current.as_deref().is_none_or(|value| candidate < value) {
        *current = Some(candidate.to_string());
    }
}

fn update_max(current: &mut Option<String>, candidate: &str) {
    if current.as_deref().is_none_or(|value| candidate > value) {
        *current = Some(candidate.to_string());
    }
}

async fn verify_current_objects(
    config: &RuntimeConfig,
    client: Client,
    objects: &[ExpectedObject],
) -> Result<u64> {
    let tenant = Arc::<str>::from(config.tenant.as_str());
    let bucket = Arc::<str>::from(config.bucket.as_str());
    let auth = config.auth.clone();
    stream::iter(objects.iter())
        .map(|object| {
            let mut client = client.clone();
            let tenant = tenant.clone();
            let bucket = bucket.clone();
            let auth = auth.clone();
            async move {
                let head = client
                    .head_object(request(
                        HeadObjectRequest {
                            address: Some(address(&tenant, &bucket, &object.path)),
                        },
                        auth.as_ref(),
                    ))
                    .await
                    .with_context(|| format!("verify {}", object.path))?
                    .into_inner();
                let present = match head.state {
                    Some(object_head::State::Present(present)) => present,
                    Some(object_head::State::Deleted(deleted)) => {
                        bail!("{} is deleted at version {}", object.path, deleted.version)
                    }
                    Some(object_head::State::NeverExisted(_)) | None => {
                        bail!("{} does not exist", object.path)
                    }
                };
                ensure!(
                    present.version == object.version,
                    "{} is at version {}, expected {}",
                    object.path,
                    present.version,
                    object.version
                );
                ensure!(
                    present.content_length == object.content_length
                        && present.content_type == object.content_type
                        && present.content_hash.as_slice() == object.content_blake3,
                    "{} current metadata does not match the written payload",
                    object.path
                );
                Ok::<u64, anyhow::Error>(1)
            }
        })
        .buffer_unordered(config.verification_concurrency)
        .try_fold(0_u64, |total, count| async move {
            total
                .checked_add(count)
                .context("verified object count overflowed")
        })
        .await
}

fn batch_would_overflow(
    current_items: usize,
    current_bytes: usize,
    next_bytes: usize,
    maximum_items: usize,
    maximum_bytes: usize,
) -> bool {
    current_items >= maximum_items
        || current_bytes
            .checked_add(next_bytes)
            .is_none_or(|bytes| bytes > maximum_bytes)
}

fn request<T>(message: T, auth: Option<&AuthValue>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(auth) = auth {
        request.metadata_mut().insert("authorization", auth.clone());
    }
    request
}

fn address(tenant: &str, bucket: &str, path: &str) -> ObjectAddress {
    ObjectAddress {
        tenant: tenant.into(),
        bucket: bucket.into(),
        path: path.into(),
    }
}

fn source_definition_path() -> String {
    format!(
        "entities/source-definition/{}/current.json",
        digest_bytes(b"source-definition\0osv")
    )
}

fn shard_path(records_sha256: &str) -> String {
    format!(
        "shards/v1/{}/{}.ndjson.zst",
        &records_sha256[..2],
        records_sha256
    )
}

fn manifest_path(snapshot_id: &str) -> String {
    format!("snapshots/{snapshot_id}/manifest.json")
}

fn command_id(corpus_sha256: &str, phase: &str, identity: &str) -> String {
    format!("osv-qualification-v2:{corpus_sha256}:{phase}:{identity}")
}

fn digest_bytes(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

#[cfg(test)]
fn digest_json(value: &impl Serialize) -> Result<String> {
    let mut digest = Sha256::new();
    serde_json::to_writer(&mut digest, value)?;
    Ok(hex::encode(digest.finalize()))
}

fn dd_immutable_json_payload<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&serde_json::to_value(value)?)?)
}

fn emit_report(report: &QualificationReport<'_>, output: Option<&Path>) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(report).context("encode qualification report")?;
    println!("{}", String::from_utf8_lossy(&bytes));
    if let Some(path) = output {
        std::fs::write(path, &bytes)
            .with_context(|| format!("write qualification report {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct CloneReferenceContent<'a> {
        schema: &'static str,
        source_id: &'static str,
        source_record_id: &'a str,
        ecosystem: &'a str,
        package: &'a str,
        normalised_ecosystem: &'a str,
        normalised_package: &'a str,
        modified_at: &'a Option<String>,
        published_at: &'a Option<String>,
        withdrawn: bool,
        aliases: &'a [String],
        summary: &'a Option<String>,
        details: &'a Option<String>,
        state: &'a str,
        document: &'a Value,
    }

    fn prepare_records_serial(document: Value) -> Result<Vec<PreparedRecord>> {
        let PreparedRecordJobs {
            document,
            source_record_id,
            jobs,
            ..
        } = prepare_record_jobs(document)?;
        jobs.iter()
            .map(|job| prepare_record(&document, &source_record_id, job))
            .collect()
    }

    fn prepare_records_clone_reference(document: Value) -> Result<Vec<PreparedRecord>> {
        let PreparedRecordJobs {
            document,
            source_record_id,
            jobs,
            ..
        } = prepare_record_jobs(document)?;
        jobs.iter()
            .map(|job| prepare_record_clone_reference(&document, &source_record_id, job))
            .collect()
    }

    fn prepare_record_clone_reference(
        document: &Value,
        source_record_id: &str,
        job: &RecordJob,
    ) -> Result<PreparedRecord> {
        let mut scoped_document = document.clone();
        scoped_document
            .as_object_mut()
            .context("OSV document must be a JSON object")?
            .insert("affected".into(), Value::Array(job.affected.clone()));
        let normalised_ecosystem = job.ecosystem.trim().to_ascii_lowercase();
        let normalised_package = normalize_package_name(&job.ecosystem, &job.package);
        let record_identity_hash = digest_bytes(
            format!("osv\0{source_record_id}\0{normalised_ecosystem}\0{normalised_package}")
                .as_bytes(),
        );
        let modified_at = string_field(document, "modified");
        let modified_day = timestamp_day(modified_at.as_deref());
        let published_at = string_field(document, "published");
        let withdrawn = document
            .get("withdrawn")
            .is_some_and(|value| !value.is_null());
        let aliases = string_array(document, "aliases");
        let summary = string_field(document, "summary");
        let details = string_field(document, "details");
        let state = if withdrawn { "withdrawn" } else { "active" };
        let content_sha256 = digest_bytes(&serde_json::to_vec(&CloneReferenceContent {
            schema: "developer-defence.osv-source-record.v1",
            source_id: "osv",
            source_record_id,
            ecosystem: &job.ecosystem,
            package: &job.package,
            normalised_ecosystem: &normalised_ecosystem,
            normalised_package: &normalised_package,
            modified_at: &modified_at,
            published_at: &published_at,
            withdrawn,
            aliases: &aliases,
            summary: &summary,
            details: &details,
            state,
            document: &scoped_document,
        })?);
        let record = OsvSourceRecord {
            schema: "developer-defence.osv-source-record.v1".into(),
            source_id: "osv".into(),
            source_record_id: source_record_id.into(),
            record_identity_hash,
            content_sha256,
            ecosystem: job.ecosystem.clone(),
            package: job.package.clone(),
            normalised_ecosystem: normalised_ecosystem.clone(),
            normalised_package,
            modified_at,
            modified_day: modified_day.clone(),
            published_at,
            withdrawn,
            aliases,
            summary,
            details,
            state: state.into(),
            document: scoped_document,
        };
        Ok(PreparedRecord {
            encoded: serde_json::to_vec(&record)?,
            normalised_ecosystem,
            modified_day,
        })
    }

    fn decode_record(prepared: &PreparedRecord) -> OsvSourceRecord {
        serde_json::from_slice(&prepared.encoded).unwrap()
    }

    fn test_shard_builders() -> Vec<ShardBuilder> {
        (0..6)
            .map(|index| {
                let prepared = prepare_records_serial(serde_json::json!({
                    "id": format!("GHSA-compression-{index}"),
                    "modified": format!("2026-07-{:02}T12:00:00Z", index + 1),
                    "details": "deterministic shard compression",
                    "affected": [{
                        "package": {"ecosystem": "npm", "name": format!("example-{index}")},
                        "versions": ["1.0.0", "1.0.1"]
                    }]
                }))
                .unwrap()
                .remove(0);
                let mut builder = ShardBuilder::new("npm".into(), index, 1024);
                builder.push(prepared);
                builder
            })
            .collect()
    }

    #[test]
    fn parallel_shard_compression_is_byte_identical_to_serial() {
        let expected = test_shard_builders()
            .into_iter()
            .map(ShardBuilder::finish)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let builders = test_shard_builders();
        let (sender, mut receiver) = mpsc::channel(builders.len());
        let mut compressor = ShardCompressor::start(3, sender).unwrap();
        for builder in builders {
            compressor.submit(builder).unwrap();
        }
        compressor.finish().unwrap();

        let mut actual = Vec::new();
        while let Some(shard) = receiver.blocking_recv() {
            actual.push(shard);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn shard_compression_flushes_a_partial_wave_in_submission_order() {
        let expected = test_shard_builders()
            .into_iter()
            .take(5)
            .map(ShardBuilder::finish)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let builders = test_shard_builders();
        let (sender, mut receiver) = mpsc::channel(builders.len());
        let mut compressor = ShardCompressor::start(3, sender).unwrap();
        for builder in builders.into_iter().take(5) {
            compressor.submit(builder).unwrap();
        }
        assert_eq!(compressor.pending.len(), 2);
        compressor.finish().unwrap();

        let mut actual = Vec::new();
        while let Some(shard) = receiver.blocking_recv() {
            actual.push(shard);
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn shard_compression_rejects_zero_workers() {
        let (sender, _receiver) = mpsc::channel(1);
        let error = match ShardCompressor::start(0, sender) {
            Ok(_) => panic!("zero compression workers must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "shard compression requires at least one worker"
        );
    }

    #[test]
    fn shard_compression_never_retains_more_than_one_bounded_wave() {
        let builders = test_shard_builders();
        let (sender, _receiver) = mpsc::channel(builders.len());
        let mut compressor = ShardCompressor::start(3, sender).unwrap();
        for builder in builders {
            compressor.submit(builder).unwrap();
            assert!(compressor.pending.len() < compressor.worker_count);
        }
        compressor.finish().unwrap();
    }

    #[test]
    fn shard_compression_reports_a_stopped_consumer_without_hanging() {
        let builder = test_shard_builders().remove(0);
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let mut compressor = ShardCompressor::start(1, sender).unwrap();
        let error = compressor.submit(builder).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("OSV shard consumer stopped before parsing completed")
        );
    }

    #[test]
    fn borrowed_record_encoding_matches_clone_reference_for_mixed_scopes() {
        let document: Value = serde_json::from_str(
            r#"{
                "z": 1.0,
                "summary": "mixed package, \"quoted\", and unicode Δ records",
                "modified": "2026-07-14T12:00:00Z,\"published_at\":false",
                "id": "GHSA-mixed\\path,\"ecosystem\":false",
                "affected": [
                    {"package": {"ecosystem": "npm", "name": "Zed"}, "versions": ["1"]},
                    {"versions": ["unscoped"]},
                    {"package": {"ecosystem": "Cargo", "name": "crate-a"}, "versions": ["2"]},
                    {"package": {"ecosystem": "npm", "name": "zed"}, "versions": ["3"]}
                ],
                "a": true
            }"#,
        )
        .unwrap();
        let expected = prepare_records_clone_reference(document.clone()).unwrap();
        let actual = prepare_records_serial(document).unwrap();

        assert_eq!(actual, expected);
        let records = actual.iter().map(decode_record).collect::<Vec<_>>();
        assert_eq!(
            records
                .iter()
                .map(|record| record.ecosystem.as_str())
                .collect::<Vec<_>>(),
            ["crates.io", "npm", "unscoped"]
        );
        assert_eq!(records[1].document["affected"].as_array().unwrap().len(), 2);
        assert_eq!(records[2].document["affected"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn scoped_document_emits_affected_at_its_lexical_map_position() {
        let prepared = prepare_record_jobs(
            serde_json::from_str(
                r#"{"z":1.0,"middle":2,"id":"GHSA-lexical","affected":[{"versions":["1"]}],"a":1}"#,
            )
            .unwrap(),
        )
        .unwrap();
        let scoped = ScopedDocument {
            base: prepared.document.as_object().unwrap(),
            affected: &prepared.jobs[0].affected,
        };
        assert_eq!(
            serde_json::to_vec(&scoped).unwrap(),
            br#"{"a":1,"affected":[{"versions":["1"]}],"id":"GHSA-lexical","middle":2,"z":1.0}"#
        );
    }

    #[test]
    fn exact_developer_defence_transform_materialises_package_records() {
        let records = prepare_records_serial(serde_json::json!({
            "id": "GHSA-test",
            "modified": "2026-07-14T12:00:00Z",
            "aliases": ["CVE-2026-1", "CVE-2026-1"],
            "affected": [
                {"package": {"ecosystem": "npm", "name": "Example"}, "versions": ["1.0.0"]},
                {"package": {"ecosystem": "npm", "name": "example"}, "versions": ["1.0.1"]},
                {"package": {"ecosystem": "Go", "name": "example.org/module"}}
            ]
        }))
        .unwrap();

        assert_eq!(records.len(), 2);
        let npm = records
            .iter()
            .map(decode_record)
            .find(|record| record.ecosystem == "npm")
            .unwrap();
        assert_eq!(npm.normalised_package, "example");
        assert_eq!(npm.aliases, ["CVE-2026-1"]);
        assert_eq!(npm.modified_day, "2026-07-14");
        assert_eq!(npm.document["affected"].as_array().unwrap().len(), 2);
        let content = CloneReferenceContent {
            schema: "developer-defence.osv-source-record.v1",
            source_id: "osv",
            source_record_id: &npm.source_record_id,
            ecosystem: &npm.ecosystem,
            package: &npm.package,
            normalised_ecosystem: &npm.normalised_ecosystem,
            normalised_package: &npm.normalised_package,
            modified_at: &npm.modified_at,
            published_at: &npm.published_at,
            withdrawn: npm.withdrawn,
            aliases: &npm.aliases,
            summary: &npm.summary,
            details: &npm.details,
            state: &npm.state,
            document: &npm.document,
        };
        assert_eq!(
            npm.content_sha256,
            digest_bytes(&serde_json::to_vec(&content).unwrap())
        );
    }

    #[test]
    fn streaming_content_digest_matches_materialised_json_bytes() {
        let prepared = prepare_record_jobs(serde_json::json!({
            "z": 1.0,
            "published": "2026-07-13T12:00:00Z",
            "modified": "2026-07-14T12:00:00Z",
            "id": "GHSA-streaming-digest",
            "aliases": ["CVE-2026-1"],
            "affected": [{
                "package": {"ecosystem": "npm", "name": "Example"},
                "versions": ["1.0.0"]
            }],
            "a": true
        }))
        .unwrap();
        let job = &prepared.jobs[0];
        let normalised_ecosystem = job.ecosystem.trim().to_ascii_lowercase();
        let normalised_package = normalize_package_name(&job.ecosystem, &job.package);
        let modified_at = string_field(&prepared.document, "modified");
        let published_at = string_field(&prepared.document, "published");
        let aliases = string_array(&prepared.document, "aliases");
        let summary = string_field(&prepared.document, "summary");
        let details = string_field(&prepared.document, "details");
        let content = OsvSourceRecordContent {
            schema: "developer-defence.osv-source-record.v1",
            source_id: "osv",
            source_record_id: &prepared.source_record_id,
            ecosystem: &job.ecosystem,
            package: &job.package,
            normalised_ecosystem: &normalised_ecosystem,
            normalised_package: &normalised_package,
            modified_at: &modified_at,
            published_at: &published_at,
            withdrawn: false,
            aliases: &aliases,
            summary: &summary,
            details: &details,
            state: "active",
            document: ScopedDocument {
                base: prepared.document.as_object().unwrap(),
                affected: &job.affected,
            },
        };

        assert_eq!(
            digest_json(&content).unwrap(),
            digest_bytes(&serde_json::to_vec(&content).unwrap())
        );
    }

    #[test]
    fn pinned_normalised_transform_stabilises_key_order_and_number_spelling() {
        // The pinned DD shard schema intentionally uses serde_json here. JCS is
        // used only by DD's separate raw-document mirror, which this
        // authoritative shard qualification does not write.
        let first: Value =
            serde_json::from_str(r#"{"z":1.0,"id":"GHSA-canonical","a":1,"affected":[]}"#).unwrap();
        let second: Value =
            serde_json::from_str(r#"{"affected":[],"a":1,"id":"GHSA-canonical","z":1.0}"#).unwrap();
        let first = decode_record(&prepare_records_serial(first).unwrap().remove(0));
        let second = decode_record(&prepare_records_serial(second).unwrap().remove(0));
        assert_eq!(first, second);

        let scoped = serde_json::to_vec(&first.document).unwrap();
        assert_eq!(
            scoped,
            br#"{"a":1,"affected":[],"id":"GHSA-canonical","z":1.0}"#
        );
    }

    #[test]
    fn shard_encoding_is_content_addressed_zstd_6_ndjson() {
        let mut records = prepare_records_serial(serde_json::json!({
            "id": "GHSA-shard",
            "affected": [{"package": {"ecosystem": "npm", "name": "example"}}]
        }))
        .unwrap();
        let mut builder = ShardBuilder::new("npm".into(), 0, MIN_SHARD_UNCOMPRESSED_BYTES);
        builder.push(records.remove(0));
        let shard = builder.finish().unwrap();
        let decoded = zstd::stream::decode_all(Cursor::new(&shard.encoded_payload)).unwrap();

        assert_eq!(digest_bytes(&decoded), shard.records_sha256);
        assert_eq!(digest_bytes(&shard.encoded_payload), shard.encoded_sha256);
        assert_eq!(decoded.last(), Some(&b'\n'));
        assert_eq!(
            shard_path(&shard.records_sha256),
            format!(
                "shards/v1/{}/{}.ndjson.zst",
                &shard.records_sha256[..2],
                shard.records_sha256
            )
        );
    }

    #[test]
    fn source_definition_has_exact_schema_and_content_addressed_identity() {
        let path = source_definition_path();
        assert_eq!(
            path,
            format!(
                "entities/source-definition/{}/current.json",
                digest_bytes(b"source-definition\0osv")
            )
        );
        let definition = SourceDefinition {
            schema: "developer-defence.source-definition.v2".into(),
            source_id: "osv".into(),
            source_bucket: DD_OSV_BUCKET.into(),
            canonical_url: DEFAULT_SOURCE_URL.into(),
            publisher: "Google OSV".into(),
            cadence_hours: 6,
            authentication_profile: "public-https".into(),
            downloaded_artifact_retention: "ephemeral-until-shard-manifest-commit".into(),
            redistribution_policy: "record-level-upstream-rights".into(),
            enabled: true,
        };
        let value = serde_json::to_value(definition).unwrap();
        assert_eq!(value["schema"], "developer-defence.source-definition.v2");
        assert_eq!(value["authentication_profile"], "public-https");
        assert_eq!(value["source_bucket"], DD_OSV_BUCKET);
    }

    #[test]
    fn snapshot_identity_requires_an_explicit_canonical_day() {
        assert!(validate_snapshot_day("2026-07-18").is_ok());
        for invalid in ["", "20260718", "2026-7-18", " 2026-07-18"] {
            assert!(validate_snapshot_day(invalid).is_err());
        }
    }

    #[test]
    fn qualification_requires_an_exact_anvil_commit() {
        assert!(validate_git_commit("--anvil-commit", &"a".repeat(40)).is_ok());
        assert!(validate_git_commit("--anvil-commit", &"A".repeat(40)).is_ok());
        let short = "a".repeat(39);
        let non_hex = "g".repeat(40);
        for invalid in ["", "main", short.as_str(), non_hex.as_str()] {
            assert!(validate_git_commit("--anvil-commit", invalid).is_err());
        }
    }

    #[test]
    fn deterministic_batch_boundary_honours_items_and_payload() {
        assert!(!batch_would_overflow(2, 20, 10, 3, 30));
        assert!(batch_would_overflow(3, 20, 1, 3, 30));
        assert!(batch_would_overflow(2, 20, 11, 3, 30));
    }

    #[test]
    fn single_node_qualification_accepts_only_local_durability() {
        assert!(<DurabilityArgument as clap::ValueEnum>::from_str("local", false).is_ok());
        for rejected in ["replicated", "quorum", "anything", " local", "local "] {
            let parsed = <DurabilityArgument as clap::ValueEnum>::from_str(rejected, false);
            let accepted = matches!(parsed, Ok(DurabilityArgument::Local));
            assert!(!accepted, "unexpectedly accepted {rejected:?}");
        }
    }

    #[test]
    fn qualification_rejects_credentials_the_server_cannot_accept() {
        assert!(validate_client_secret_value(&"s".repeat(32)).is_ok());
        assert!(validate_client_secret_value(&"s".repeat(4 * 1024)).is_ok());
        assert!(validate_client_secret_value(&"s".repeat(31)).is_err());
        assert!(validate_client_secret_value(&"s".repeat(4 * 1024 + 1)).is_err());
    }
}
