use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anvil_api::v1::object_service_client::ObjectServiceClient;
use anvil_api::v1::{
    BulkOperation, BulkWriteRequest, HeadObjectRequest, ObjectAddress, PutObjectRequest,
    WriteCondition, bulk_operation, bulk_outcome, object_head, write_condition,
};
use anvil_osv_qualification::{
    CorpusReport, DD_SCHEMA_SOURCE_COMMIT, ParsingReport, QUALIFICATION_SCHEMA,
    QualificationReport, ResultReport, RunCounts, SchemaShapeReport, SoftwareReport,
    TARGET_SECONDS, VerificationReport, WorkloadReport,
};
use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{sync::mpsc, task::JoinSet};
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Channel, Endpoint},
};

const DD_OSV_BUCKET: &str = "dd-source-osv-raw";
const CONTENT_TYPE_JSON: &str = "application/json";
const MAX_ARCHIVE_ENTRIES: usize = 2_000_000;
const MAX_DOCUMENT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_DECOMPRESSED_JSON_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const SERVER_MAX_BULK_ITEMS: usize = 1_000;
const SERVER_MAX_BULK_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const CLIENT_MESSAGE_BYTES: usize = 72 * 1024 * 1024;
const PARSE_PROGRESS_INTERVAL: u64 = 25_000;

type Client = ObjectServiceClient<Channel>;
type AuthValue = MetadataValue<Ascii>;

#[derive(Debug, Parser)]
#[command(
    name = "anvil-osv-qualification",
    about = "Qualify Anvil 0.5 BulkWrite with Developer Defence's raw OSV schema"
)]
struct Args {
    /// URL of one clean Anvil 0.5 node, for example http://127.0.0.1:50051.
    #[arg(long)]
    endpoint: String,

    /// File containing only the bearer token. Omit for an explicitly insecure node.
    #[arg(long)]
    bearer_token_file: Option<PathBuf>,

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

    /// Exact Anvil source revision being qualified.
    #[arg(long)]
    anvil_commit: String,

    /// Exact `local` or `replicated`; 0.5.0 can qualify only `local`.
    #[arg(long)]
    durability_class: String,

    #[arg(long, default_value_t = 256)]
    batch_size: usize,

    #[arg(long, default_value_t = 3 * 1024 * 1024)]
    maximum_batch_payload_bytes: usize,

    #[arg(long, default_value_t = 4)]
    concurrency: usize,

    #[arg(long, default_value_t = 64)]
    verification_concurrency: usize,

    /// Write the JSON report here as well as stdout.
    #[arg(long)]
    output: Option<PathBuf>,

    /// Required acknowledgement that the tenant/bucket target contains no prior run.
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
    anvil_commit: String,
    durability_class: String,
    batch_size: usize,
    maximum_batch_payload_bytes: usize,
    concurrency: usize,
    verification_concurrency: usize,
    output: Option<PathBuf>,
    auth: Option<AuthValue>,
}

#[derive(Debug)]
struct LeafRecord {
    identity: String,
    raw_content_sha256: String,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct LeafBatch {
    records: Vec<LeafRecord>,
}

#[derive(Debug)]
struct HeadSeed {
    identity: String,
    raw_content_sha256: String,
    leaf_version: u64,
}

#[derive(Debug)]
struct HeadPut {
    seed: HeadSeed,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct ExpectedRecord {
    identity: String,
    leaf_version: u64,
    head_version: u64,
}

#[derive(Debug)]
struct LeafBatchResult {
    heads: Vec<HeadSeed>,
    payload_bytes: u64,
    replayed: u64,
    latency: Duration,
}

#[derive(Debug)]
struct HeadBatchResult {
    records: Vec<ExpectedRecord>,
    payload_bytes: u64,
    replayed: u64,
    latency: Duration,
}

#[derive(Debug)]
struct LeafPhaseResult {
    heads: Vec<HeadSeed>,
    parsing: ParsingReport,
    payload_bytes: u64,
    request_count: u64,
    replayed: u64,
    latencies: Vec<Duration>,
}

#[derive(Debug)]
struct HeadPhaseResult {
    records: Vec<ExpectedRecord>,
    payload_bytes: u64,
    request_count: u64,
    replayed: u64,
    latencies: Vec<Duration>,
}

#[derive(Serialize)]
struct RawHead<'a> {
    schema: &'static str,
    source: &'static str,
    record_identity_sha256: &'a str,
    raw_content_sha256: &'a str,
    raw_object_key: String,
    raw_object_version_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = validate_and_pin(args)?;
    let channel = Endpoint::from_shared(config.endpoint.clone())?
        .connect()
        .await
        .with_context(|| format!("connect to {}", config.endpoint))?;
    let client = ObjectServiceClient::new(channel)
        .max_encoding_message_size(CLIENT_MESSAGE_BYTES)
        .max_decoding_message_size(CLIENT_MESSAGE_BYTES);

    eprintln!(
        "event=osv_qualification_start corpus={} sha256={} archive_bytes={} batch_size={} concurrency={}",
        config.corpus_path_display,
        config.corpus_sha256,
        config.corpus_bytes,
        config.batch_size,
        config.concurrency,
    );
    let ingest_started = Instant::now();
    let leaf = run_leaf_phase(&config, client.clone()).await?;
    ensure!(
        leaf.parsing.accepted_source_records_n > 0,
        "the pinned corpus contains no accepted OSV source records"
    );
    ensure!(
        leaf.heads.len() as u64 == leaf.parsing.accepted_source_records_n,
        "leaf receipt count does not match accepted source records"
    );
    let head = run_head_phase(&config, client.clone(), leaf.heads).await?;
    let ingest_elapsed = ingest_started.elapsed();

    let verification_started = Instant::now();
    let verified = verify_current_versions(&config, client, &head.records).await?;
    let verification_elapsed = verification_started.elapsed();

    let counts = RunCounts {
        source_records: leaf.parsing.accepted_source_records_n,
        raw_payload_bytes: leaf.payload_bytes,
        head_payload_bytes: head.payload_bytes,
        leaf_bulk_requests: leaf.request_count,
        head_bulk_requests: head.request_count,
        replayed_mutations: leaf.replayed.saturating_add(head.replayed),
    };
    let result = ResultReport::calculate(&counts, ingest_elapsed, &leaf.latencies, &head.latencies);
    let expected_object_count = counts.logical_mutations();
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
            durability_class: &config.durability_class,
            node_count: 1,
            batch_size_operations: config.batch_size,
            maximum_batch_payload_bytes: config.maximum_batch_payload_bytes,
            write_concurrency: config.concurrency,
            verification_concurrency: config.verification_concurrency,
            clean_target_asserted: true,
        },
        parsing: leaf.parsing,
        result,
        verification: VerificationReport {
            expected_object_count,
            verified_object_count: verified,
            duration_seconds: verification_elapsed.as_secs_f64(),
        },
        limitations: vec![
            "This qualifies Developer Defence's raw OSV leaf/head persistence shape; normalised shard construction and snapshot publication are outside this storage gate.",
            "Anvil 0.5 accepts exactly local or replicated durability: local performs the write, replicated returns DURABILITY_UNAVAILABLE without mutation, and every other value is invalid. This single-node qualification uses local.",
            "Independent verification uses current object heads and exact receipt versions; it does not download payload bytes again.",
            "Anvil 0.5 has no list, count, or batch-head API: verification proves every expected exact version exists, but --confirm-clean-target remains the operator's assertion that no extra paths exist.",
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

fn validate_and_pin(args: Args) -> Result<RuntimeConfig> {
    ensure!(
        args.confirm_clean_target,
        "--confirm-clean-target is required because this command writes the supplied tenant/bucket"
    );
    validate_canonical("tenant", &args.tenant)?;
    validate_canonical("bucket", &args.bucket)?;
    validate_canonical("durability class", &args.durability_class)?;
    validate_canonical("Anvil commit", &args.anvil_commit)?;
    ensure!(
        (1..=SERVER_MAX_BULK_ITEMS).contains(&args.batch_size),
        "--batch-size must be between 1 and {SERVER_MAX_BULK_ITEMS}"
    );
    ensure!(
        (1..=SERVER_MAX_BULK_PAYLOAD_BYTES).contains(&args.maximum_batch_payload_bytes),
        "--maximum-batch-payload-bytes must be between 1 and {SERVER_MAX_BULK_PAYLOAD_BYTES}"
    );
    ensure!(args.concurrency > 0, "--concurrency must be non-zero");
    ensure!(
        args.verification_concurrency > 0,
        "--verification-concurrency must be non-zero"
    );
    ensure!(
        args.corpus_sha256.len() == 64
            && args
                .corpus_sha256
                .bytes()
                .all(|byte| { byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte) }),
        "--corpus-sha256 must be 64 lowercase hexadecimal characters"
    );
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
    let auth = args
        .bearer_token_file
        .as_deref()
        .map(read_auth_value)
        .transpose()?;
    Ok(RuntimeConfig {
        endpoint: args.endpoint,
        tenant: args.tenant,
        bucket: args.bucket,
        corpus_path_display: corpus.display().to_string(),
        corpus,
        corpus_sha256: observed,
        corpus_bytes: metadata.len(),
        anvil_commit: args.anvil_commit,
        durability_class: args.durability_class,
        batch_size: args.batch_size,
        maximum_batch_payload_bytes: args.maximum_batch_payload_bytes,
        concurrency: args.concurrency,
        verification_concurrency: args.verification_concurrency,
        output: args.output,
        auth,
    })
}

fn validate_canonical(name: &str, value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.trim() == value,
        "{name} must be non-empty and have no surrounding whitespace"
    );
    ensure!(!value.contains('\0'), "{name} must not contain NUL");
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

fn read_auth_value(path: &Path) -> Result<AuthValue> {
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("read bearer token file {}", path.display()))?;
    let token = token.trim();
    ensure!(!token.is_empty(), "bearer token file is empty");
    format!("Bearer {token}")
        .parse()
        .context("bearer token contains bytes invalid for gRPC metadata")
}

async fn run_leaf_phase(config: &RuntimeConfig, client: Client) -> Result<LeafPhaseResult> {
    let (sender, mut receiver) = mpsc::channel(config.concurrency.saturating_mul(2).max(1));
    let corpus = config.corpus.clone();
    let batch_size = config.batch_size;
    let maximum_batch_payload_bytes = config.maximum_batch_payload_bytes;
    let parser = tokio::task::spawn_blocking(move || {
        parse_archive(&corpus, batch_size, maximum_batch_payload_bytes, sender)
    });
    let mut pending = JoinSet::new();
    let mut heads = Vec::new();
    let mut payload_bytes = 0_u64;
    let mut request_count = 0_u64;
    let mut replayed = 0_u64;
    let mut latencies = Vec::new();

    while let Some(batch) = receiver.recv().await {
        if pending.len() >= config.concurrency {
            collect_leaf_batch(
                pending.join_next().await,
                &mut heads,
                &mut payload_bytes,
                &mut request_count,
                &mut replayed,
                &mut latencies,
            )?;
        }
        let client = client.clone();
        let auth = config.auth.clone();
        let tenant = Arc::<str>::from(config.tenant.as_str());
        let bucket = Arc::<str>::from(config.bucket.as_str());
        let durability = Arc::<str>::from(config.durability_class.as_str());
        let corpus_sha256 = Arc::<str>::from(config.corpus_sha256.as_str());
        pending.spawn(async move {
            send_leaf_batch(
                client,
                auth,
                tenant,
                bucket,
                durability,
                corpus_sha256,
                batch,
            )
            .await
        });
    }
    while let Some(completed) = pending.join_next().await {
        collect_leaf_batch(
            Some(completed),
            &mut heads,
            &mut payload_bytes,
            &mut request_count,
            &mut replayed,
            &mut latencies,
        )?;
    }
    let parsing = parser.await.context("OSV parser task panicked")??;
    Ok(LeafPhaseResult {
        heads,
        parsing,
        payload_bytes,
        request_count,
        replayed,
        latencies,
    })
}

fn collect_leaf_batch(
    completed: Option<Result<Result<LeafBatchResult>, tokio::task::JoinError>>,
    heads: &mut Vec<HeadSeed>,
    payload_bytes: &mut u64,
    request_count: &mut u64,
    replayed: &mut u64,
    latencies: &mut Vec<Duration>,
) -> Result<()> {
    let completed = completed
        .context("leaf request task set ended unexpectedly")?
        .context("leaf request task panicked")??;
    *payload_bytes = payload_bytes.saturating_add(completed.payload_bytes);
    *request_count = request_count.saturating_add(1);
    *replayed = replayed.saturating_add(completed.replayed);
    latencies.push(completed.latency);
    heads.extend(completed.heads);
    Ok(())
}

async fn send_leaf_batch(
    mut client: Client,
    auth: Option<AuthValue>,
    tenant: Arc<str>,
    bucket: Arc<str>,
    durability: Arc<str>,
    corpus_sha256: Arc<str>,
    batch: LeafBatch,
) -> Result<LeafBatchResult> {
    let payload_bytes = batch
        .records
        .iter()
        .try_fold(0_u64, |total, record| {
            total.checked_add(record.payload.len() as u64)
        })
        .context("raw payload byte count overflowed")?;
    let expected_commands = batch
        .records
        .iter()
        .map(|record| command_id(&corpus_sha256, "raw", &record.identity))
        .collect::<Vec<_>>();
    let operations = batch
        .records
        .iter()
        .zip(&expected_commands)
        .map(|(record, command_id)| BulkOperation {
            operation: Some(bulk_operation::Operation::Put(PutObjectRequest {
                address: Some(address(&tenant, &bucket, &raw_path(&record.identity))),
                bytes: record.payload.clone(),
                content_type: CONTENT_TYPE_JSON.into(),
                condition: Some(any_condition()),
                command_id: command_id.clone(),
                durability_class: durability.to_string(),
            })),
        })
        .collect();
    let started = Instant::now();
    let response = client
        .bulk_write(request(BulkWriteRequest { operations }, auth.as_ref()))
        .await
        .context("raw leaf BulkWrite RPC failed")?
        .into_inner();
    let latency = started.elapsed();
    let receipts = ordered_receipts(response.outcomes, &expected_commands)?;
    let mut replayed = 0_u64;
    let heads = batch
        .records
        .into_iter()
        .zip(receipts)
        .map(|(record, receipt)| {
            replayed += u64::from(receipt.replayed);
            ensure!(receipt.version > 0, "raw leaf receipt omitted its version");
            Ok(HeadSeed {
                identity: record.identity,
                raw_content_sha256: record.raw_content_sha256,
                leaf_version: receipt.version,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LeafBatchResult {
        heads,
        payload_bytes,
        replayed,
        latency,
    })
}

fn parse_archive(
    path: &Path,
    batch_size: usize,
    maximum_batch_payload_bytes: usize,
    sender: mpsc::Sender<LeafBatch>,
) -> Result<ParsingReport> {
    let mut archive = zip::ZipArchive::new(
        File::open(path).with_context(|| format!("open OSV archive {}", path.display()))?,
    )
    .context("open OSV ZIP structure")?;
    ensure!(
        archive.len() <= MAX_ARCHIVE_ENTRIES,
        "OSV archive has {} entries, exceeding the {MAX_ARCHIVE_ENTRIES} entry bound",
        archive.len()
    );
    let mut report = ParsingReport {
        archive_entries: archive.len() as u64,
        ..ParsingReport::default()
    };
    let mut batch = Vec::with_capacity(batch_size);
    let mut batch_payload_bytes = 0_usize;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .with_context(|| format!("open ZIP entry {index}"))?;
        if entry.is_dir() || !entry.name().ends_with(".json") {
            continue;
        }
        report.json_documents = report.json_documents.saturating_add(1);
        if report
            .json_documents
            .is_multiple_of(PARSE_PROGRESS_INTERVAL)
        {
            eprintln!(
                "event=osv_parse_progress json_documents={} accepted_records={}",
                report.json_documents, report.accepted_source_records_n
            );
        }
        if entry.size() > MAX_DOCUMENT_BYTES {
            report.oversized_documents = report.oversized_documents.saturating_add(1);
            continue;
        }
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        entry
            .by_ref()
            .take(MAX_DOCUMENT_BYTES + 1)
            .read_to_end(&mut bytes)
            .with_context(|| format!("read ZIP entry {}", entry.name()))?;
        if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            report.oversized_documents = report.oversized_documents.saturating_add(1);
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
        let Some(record) = prepare_raw_record(&bytes) else {
            report.malformed_documents = report.malformed_documents.saturating_add(1);
            continue;
        };
        if !batch.is_empty()
            && (batch.len() == batch_size
                || batch_payload_bytes
                    .checked_add(record.payload.len())
                    .is_none_or(|bytes| bytes > maximum_batch_payload_bytes))
        {
            send_leaf_batch_to_runtime(&sender, std::mem::take(&mut batch))?;
            batch = Vec::with_capacity(batch_size);
            batch_payload_bytes = 0;
        }
        batch_payload_bytes = batch_payload_bytes
            .checked_add(record.payload.len())
            .context("raw batch payload byte count overflowed")?;
        batch.push(record);
        report.accepted_source_records_n = report.accepted_source_records_n.saturating_add(1);
    }
    if !batch.is_empty() {
        send_leaf_batch_to_runtime(&sender, batch)?;
    }
    Ok(report)
}

fn prepare_raw_record(bytes: &[u8]) -> Option<LeafRecord> {
    let document = serde_json::from_slice::<Value>(bytes).ok()?;
    let source_record_id = document.get("id")?.as_str()?.trim().to_owned();
    if source_record_id.is_empty() {
        return None;
    }
    let payload = serde_jcs::to_vec(&document).ok()?;
    let identity = hex::encode(Sha256::digest(source_record_id.as_bytes()));
    let raw_content_sha256 = hex::encode(Sha256::digest(&payload));
    Some(LeafRecord {
        identity,
        raw_content_sha256,
        payload,
    })
}

fn send_leaf_batch_to_runtime(
    sender: &mpsc::Sender<LeafBatch>,
    records: Vec<LeafRecord>,
) -> Result<()> {
    sender
        .blocking_send(LeafBatch { records })
        .map_err(|_| anyhow::anyhow!("BulkWrite consumer stopped before OSV parsing completed"))
}

async fn run_head_phase(
    config: &RuntimeConfig,
    client: Client,
    heads: Vec<HeadSeed>,
) -> Result<HeadPhaseResult> {
    let mut pending = JoinSet::new();
    let mut current = Vec::with_capacity(config.batch_size);
    let mut current_payload_bytes = 0_usize;
    let mut records = Vec::with_capacity(heads.len());
    let mut payload_bytes = 0_u64;
    let mut request_count = 0_u64;
    let mut replayed = 0_u64;
    let mut latencies = Vec::new();

    for seed in heads {
        let payload = raw_head_payload(&seed)?;
        if !current.is_empty()
            && (current.len() == config.batch_size
                || current_payload_bytes
                    .checked_add(payload.len())
                    .is_none_or(|bytes| bytes > config.maximum_batch_payload_bytes))
        {
            if pending.len() >= config.concurrency {
                collect_head_batch(
                    pending.join_next().await,
                    &mut records,
                    &mut payload_bytes,
                    &mut request_count,
                    &mut replayed,
                    &mut latencies,
                )?;
            }
            spawn_head_batch(
                &mut pending,
                config,
                client.clone(),
                std::mem::take(&mut current),
            );
            current = Vec::with_capacity(config.batch_size);
            current_payload_bytes = 0;
        }
        current_payload_bytes = current_payload_bytes
            .checked_add(payload.len())
            .context("head batch payload byte count overflowed")?;
        current.push(HeadPut { seed, payload });
    }
    if !current.is_empty() {
        if pending.len() >= config.concurrency {
            collect_head_batch(
                pending.join_next().await,
                &mut records,
                &mut payload_bytes,
                &mut request_count,
                &mut replayed,
                &mut latencies,
            )?;
        }
        spawn_head_batch(&mut pending, config, client, current);
    }
    while let Some(completed) = pending.join_next().await {
        collect_head_batch(
            Some(completed),
            &mut records,
            &mut payload_bytes,
            &mut request_count,
            &mut replayed,
            &mut latencies,
        )?;
    }
    Ok(HeadPhaseResult {
        records,
        payload_bytes,
        request_count,
        replayed,
        latencies,
    })
}

fn spawn_head_batch(
    pending: &mut JoinSet<Result<HeadBatchResult>>,
    config: &RuntimeConfig,
    client: Client,
    puts: Vec<HeadPut>,
) {
    let auth = config.auth.clone();
    let tenant = Arc::<str>::from(config.tenant.as_str());
    let bucket = Arc::<str>::from(config.bucket.as_str());
    let durability = Arc::<str>::from(config.durability_class.as_str());
    let corpus_sha256 = Arc::<str>::from(config.corpus_sha256.as_str());
    pending.spawn(async move {
        send_head_batch(
            client,
            auth,
            tenant,
            bucket,
            durability,
            corpus_sha256,
            puts,
        )
        .await
    });
}

fn collect_head_batch(
    completed: Option<Result<Result<HeadBatchResult>, tokio::task::JoinError>>,
    records: &mut Vec<ExpectedRecord>,
    payload_bytes: &mut u64,
    request_count: &mut u64,
    replayed: &mut u64,
    latencies: &mut Vec<Duration>,
) -> Result<()> {
    let completed = completed
        .context("head request task set ended unexpectedly")?
        .context("head request task panicked")??;
    *payload_bytes = payload_bytes.saturating_add(completed.payload_bytes);
    *request_count = request_count.saturating_add(1);
    *replayed = replayed.saturating_add(completed.replayed);
    latencies.push(completed.latency);
    records.extend(completed.records);
    Ok(())
}

async fn send_head_batch(
    mut client: Client,
    auth: Option<AuthValue>,
    tenant: Arc<str>,
    bucket: Arc<str>,
    durability: Arc<str>,
    corpus_sha256: Arc<str>,
    puts: Vec<HeadPut>,
) -> Result<HeadBatchResult> {
    let payload_bytes = puts
        .iter()
        .try_fold(0_u64, |total, put| {
            total.checked_add(put.payload.len() as u64)
        })
        .context("head payload byte count overflowed")?;
    let expected_commands = puts
        .iter()
        .map(|put| command_id(&corpus_sha256, "head", &put.seed.identity))
        .collect::<Vec<_>>();
    let operations = puts
        .iter()
        .zip(&expected_commands)
        .map(|(put, command_id)| BulkOperation {
            operation: Some(bulk_operation::Operation::Put(PutObjectRequest {
                address: Some(address(&tenant, &bucket, &head_path(&put.seed.identity))),
                bytes: put.payload.clone(),
                content_type: CONTENT_TYPE_JSON.into(),
                condition: Some(absent_condition()),
                command_id: command_id.clone(),
                durability_class: durability.to_string(),
            })),
        })
        .collect();
    let started = Instant::now();
    let response = client
        .bulk_write(request(BulkWriteRequest { operations }, auth.as_ref()))
        .await
        .context("raw head BulkWrite RPC failed")?
        .into_inner();
    let latency = started.elapsed();
    let receipts = ordered_receipts(response.outcomes, &expected_commands)?;
    let mut replayed = 0_u64;
    let records = puts
        .into_iter()
        .zip(receipts)
        .map(|(put, receipt)| {
            replayed += u64::from(receipt.replayed);
            ensure!(receipt.version > 0, "raw head receipt omitted its version");
            Ok(ExpectedRecord {
                identity: put.seed.identity,
                leaf_version: put.seed.leaf_version,
                head_version: receipt.version,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(HeadBatchResult {
        records,
        payload_bytes,
        replayed,
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

fn raw_head_payload(seed: &HeadSeed) -> Result<Vec<u8>> {
    serde_jcs::to_vec(&RawHead {
        schema: "developer-defence.source-raw-record-head.v1",
        source: "osv",
        record_identity_sha256: &seed.identity,
        raw_content_sha256: &seed.raw_content_sha256,
        raw_object_key: raw_path(&seed.identity),
        raw_object_version_id: seed.leaf_version.to_string(),
    })
    .context("canonicalise Developer Defence raw head")
}

async fn verify_current_versions(
    config: &RuntimeConfig,
    client: Client,
    records: &[ExpectedRecord],
) -> Result<u64> {
    let tenant = Arc::<str>::from(config.tenant.as_str());
    let bucket = Arc::<str>::from(config.bucket.as_str());
    let auth = config.auth.clone();
    stream::iter(records.iter())
        .map(|record| {
            let mut leaf_client = client.clone();
            let mut head_client = client.clone();
            let tenant = tenant.clone();
            let bucket = bucket.clone();
            let auth = auth.clone();
            async move {
                let identity = record.identity.clone();
                let leaf_path = raw_path(&identity);
                let head_path = head_path(&identity);
                let leaf = leaf_client
                    .head_object(request(
                        HeadObjectRequest {
                            address: Some(address(&tenant, &bucket, &leaf_path)),
                        },
                        auth.as_ref(),
                    ))
                    .await
                    .with_context(|| format!("verify {leaf_path}"))?
                    .into_inner();
                verify_present_version(&leaf_path, leaf, record.leaf_version)?;
                let head = head_client
                    .head_object(request(
                        HeadObjectRequest {
                            address: Some(address(&tenant, &bucket, &head_path)),
                        },
                        auth.as_ref(),
                    ))
                    .await
                    .with_context(|| format!("verify {head_path}"))?
                    .into_inner();
                verify_present_version(&head_path, head, record.head_version)?;
                Ok::<u64, anyhow::Error>(2)
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

fn verify_present_version(
    path: &str,
    head: anvil_api::v1::ObjectHead,
    expected_version: u64,
) -> Result<()> {
    let version = match head.state {
        Some(object_head::State::Present(present)) => present.version,
        Some(object_head::State::Deleted(deleted)) => {
            bail!("{path} is deleted at version {}", deleted.version)
        }
        Some(object_head::State::NeverExisted(_)) | None => bail!("{path} does not exist"),
    };
    ensure!(
        version == expected_version,
        "{path} is at version {version}, expected {expected_version}"
    );
    Ok(())
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

fn any_condition() -> WriteCondition {
    WriteCondition {
        condition: Some(write_condition::Condition::Any(true)),
    }
}

fn absent_condition() -> WriteCondition {
    WriteCondition {
        condition: Some(write_condition::Condition::Absent(true)),
    }
}

fn raw_path(identity: &str) -> String {
    format!("raw/osv/{identity}/record.json")
}

fn head_path(identity: &str) -> String {
    format!("raw/osv/{identity}/current.json")
}

fn command_id(corpus_sha256: &str, phase: &str, identity: &str) -> String {
    format!("osv-qualification-v1:{corpus_sha256}:{phase}:{identity}")
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

    #[test]
    fn raw_record_uses_trimmed_id_for_identity_and_jcs_for_payload() {
        let record = prepare_raw_record(br#"{"id":"  OSV-TEST-1  ","z":2,"a":1}"#).unwrap();
        assert_eq!(record.identity, hex::encode(Sha256::digest(b"OSV-TEST-1")));
        assert_eq!(record.payload, br#"{"a":1,"id":"  OSV-TEST-1  ","z":2}"#);
        assert_eq!(
            record.raw_content_sha256,
            hex::encode(Sha256::digest(&record.payload))
        );
    }

    #[test]
    fn head_payload_matches_developer_defence_v1_shape() {
        let identity = "11".repeat(32);
        let raw_content_sha256 = "22".repeat(32);
        let payload = raw_head_payload(&HeadSeed {
            identity: identity.clone(),
            raw_content_sha256: raw_content_sha256.clone(),
            leaf_version: 42,
        })
        .unwrap();
        let expected = format!(
            "{{\"raw_content_sha256\":\"{raw_content_sha256}\",\"raw_object_key\":\"raw/osv/{identity}/record.json\",\"raw_object_version_id\":\"42\",\"record_identity_sha256\":\"{identity}\",\"schema\":\"developer-defence.source-raw-record-head.v1\",\"source\":\"osv\"}}"
        );
        assert_eq!(payload, expected.as_bytes());
    }
}
