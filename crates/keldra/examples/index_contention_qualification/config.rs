use anyhow::{Context, Result, ensure};
use serde::Serialize;
use std::{env, path::PathBuf, time::Duration};

const PREFIX: &str = "KELDRA_INDEX_CONTENTION_";

#[derive(Clone, Debug)]
pub struct Config {
    pub endpoints: Vec<String>,
    pub tenant: String,
    pub bucket: String,
    pub client_id: String,
    pub client_secret: String,
    pub server_source_commit: String,
    pub image: String,
    pub topology: String,
    pub durability: String,
    pub definition_count: usize,
    pub stable_records: u64,
    pub mutable_records: u64,
    pub seed: u64,
    pub mutation_workers: usize,
    pub mutation_batch_size: usize,
    pub mutation_record_bytes: usize,
    pub mutation_queue_depth: usize,
    pub mutation_rate_operations_per_second: Option<f64>,
    pub query_rate: u64,
    pub query_max_in_flight: usize,
    pub baseline: Duration,
    pub concurrent: Duration,
    pub post: Duration,
    pub request_timeout: Duration,
    pub drain_timeout: Duration,
    pub visibility_poll: Duration,
    pub visibility_observation_timeout: Duration,
    pub visibility_sample_every_batches: u64,
    pub max_concurrent_query_p99_ms: Option<f64>,
    pub max_publication_visibility_p99_ms: Option<f64>,
    pub output: Option<PathBuf>,
    pub progress_jsonl: Option<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PublicConfig {
    pub endpoints: Vec<String>,
    pub tenant: String,
    pub bucket: String,
    pub server_source_commit: String,
    pub image: String,
    pub topology: String,
    pub durability: String,
    pub definition_count: usize,
    pub stable_records: u64,
    pub mutable_records: u64,
    pub seed_hex: String,
    pub mutation_workers: usize,
    pub mutation_batch_size: usize,
    pub mutation_record_bytes: usize,
    pub mutation_queue_depth: usize,
    pub mutation_rate_operations_per_second: Option<f64>,
    pub query_rate_per_second: u64,
    pub query_max_in_flight: usize,
    pub baseline_seconds: u64,
    pub concurrent_seconds: u64,
    pub post_seconds: u64,
    pub request_timeout_milliseconds: u64,
    pub drain_timeout_seconds: u64,
    pub visibility_poll_milliseconds: u64,
    pub visibility_observation_timeout_seconds: u64,
    pub visibility_sample_every_batches: u64,
    pub max_concurrent_query_p99_ms: Option<f64>,
    pub max_publication_visibility_p99_ms: Option<f64>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let endpoints = required("ENDPOINTS")?
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let definition_count = number("DEFINITION_COUNT", 1)?;
        let stable_records = number("STABLE_RECORDS", 64)?;
        let mutable_records = number("MUTABLE_RECORDS", 256)?;
        let mutation_workers = number("MUTATION_WORKERS", 4)?;
        let mutation_batch_size = number("MUTATION_BATCH_SIZE", 32)?;
        let mutation_record_bytes = number("MUTATION_RECORD_BYTES", 0)?;
        let mutation_queue_depth = number("MUTATION_QUEUE_DEPTH", 32)?;
        let mutation_rate_operations_per_second =
            optional_positive("MUTATION_RATE_OPERATIONS_PER_SECOND")?;
        let query_rate = number("QUERY_RATE", 20)?;
        let query_max_in_flight = number("QUERY_MAX_IN_FLIGHT", 64)?;
        ensure!(!endpoints.is_empty(), "at least one endpoint is required");
        ensure!(
            (1..=64).contains(&definition_count),
            "definition count must be 1..=64"
        );
        ensure!(
            (1..=1_000).contains(&stable_records),
            "stable records must be 1..=1000"
        );
        ensure!(
            (1..=1_000).contains(&mutable_records),
            "mutable records must be 1..=1000 for exact final verification"
        );
        ensure!(mutation_workers > 0 && mutation_batch_size > 0);
        ensure!(
            mutation_record_bytes <= 64 * 1024 * 1024,
            "mutation record bytes must not exceed 64 MiB"
        );
        ensure!(
            mutation_queue_depth >= mutation_workers,
            "mutation queue depth must cover every worker"
        );
        let topology = required("TOPOLOGY")?;
        ensure!(
            matches!(topology.as_str(), "single-node" | "three-node"),
            "topology must be single-node or three-node"
        );
        let visibility_sample_every_batches = number("VISIBILITY_SAMPLE_EVERY_BATCHES", 16)?;
        ensure!(
            visibility_sample_every_batches > 0,
            "visibility sample interval must be non-zero"
        );
        let durability = required("DURABILITY")?.to_ascii_uppercase();
        ensure!(
            matches!(durability.as_str(), "LOCAL" | "REPLICATED"),
            "durability must be LOCAL or REPLICATED"
        );
        let max_concurrent_query_p99_ms =
            optional_bound("MAX_CONCURRENT_QUERY_P99_MILLISECONDS", 2_000.0)?;
        let max_publication_visibility_p99_ms =
            optional_bound("MAX_PUBLICATION_VISIBILITY_P99_MILLISECONDS", 30_000.0)?;
        ensure!(
            matches!(
                (topology.as_str(), durability.as_str()),
                ("single-node", "LOCAL") | ("three-node", "REPLICATED")
            ),
            "single-node requires LOCAL durability and three-node requires REPLICATED durability"
        );
        super::metrics::validate_open_loop(query_rate, query_max_in_flight)?;
        let server_source_commit = required("SERVER_SOURCE_COMMIT")?;
        ensure!(
            server_source_commit.len() == 40
                && server_source_commit.bytes().all(|b| b.is_ascii_hexdigit()),
            "server source commit must be a full Git commit ID"
        );
        let drain_timeout = seconds("DRAIN_TIMEOUT_SECONDS", 600)?;
        Ok(Self {
            endpoints,
            tenant: required("TENANT")?,
            bucket: required("BUCKET")?,
            client_id: required("CLIENT_ID")?,
            client_secret: required("CLIENT_SECRET")?,
            server_source_commit,
            image: required("IMAGE")?,
            topology,
            durability,
            definition_count,
            stable_records,
            mutable_records,
            seed: number("SEED", 0x6b65_6c64_7261_0016)?,
            mutation_workers,
            mutation_batch_size,
            mutation_record_bytes,
            mutation_queue_depth,
            mutation_rate_operations_per_second,
            query_rate,
            query_max_in_flight,
            baseline: seconds("BASELINE_SECONDS", 30)?,
            concurrent: seconds("CONCURRENT_SECONDS", 300)?,
            post: seconds("POST_SECONDS", 30)?,
            request_timeout: millis("REQUEST_TIMEOUT_MILLISECONDS", 30_000)?,
            drain_timeout,
            visibility_poll: millis("VISIBILITY_POLL_MILLISECONDS", 100)?,
            visibility_observation_timeout: seconds(
                "VISIBILITY_OBSERVATION_TIMEOUT_SECONDS",
                drain_timeout.as_secs(),
            )?,
            visibility_sample_every_batches,
            max_concurrent_query_p99_ms,
            max_publication_visibility_p99_ms,
            output: env::var_os(name("OUTPUT")).map(PathBuf::from),
            progress_jsonl: env::var_os(name("PROGRESS_JSONL")).map(PathBuf::from),
        })
    }

    pub fn public(&self) -> PublicConfig {
        PublicConfig {
            endpoints: self.endpoints.clone(),
            tenant: self.tenant.clone(),
            bucket: self.bucket.clone(),
            server_source_commit: self.server_source_commit.clone(),
            image: self.image.clone(),
            topology: self.topology.clone(),
            durability: self.durability.clone(),
            definition_count: self.definition_count,
            stable_records: self.stable_records,
            mutable_records: self.mutable_records,
            seed_hex: format!("0x{:016x}", self.seed),
            mutation_workers: self.mutation_workers,
            mutation_batch_size: self.mutation_batch_size,
            mutation_record_bytes: self.mutation_record_bytes,
            mutation_queue_depth: self.mutation_queue_depth,
            mutation_rate_operations_per_second: self.mutation_rate_operations_per_second,
            query_rate_per_second: self.query_rate,
            query_max_in_flight: self.query_max_in_flight,
            baseline_seconds: self.baseline.as_secs(),
            concurrent_seconds: self.concurrent.as_secs(),
            post_seconds: self.post.as_secs(),
            request_timeout_milliseconds: self.request_timeout.as_millis() as u64,
            drain_timeout_seconds: self.drain_timeout.as_secs(),
            visibility_poll_milliseconds: self.visibility_poll.as_millis() as u64,
            visibility_observation_timeout_seconds: self.visibility_observation_timeout.as_secs(),
            visibility_sample_every_batches: self.visibility_sample_every_batches,
            max_concurrent_query_p99_ms: self.max_concurrent_query_p99_ms,
            max_publication_visibility_p99_ms: self.max_publication_visibility_p99_ms,
        }
    }
}

fn name(suffix: &str) -> String {
    format!("{PREFIX}{suffix}")
}
fn required(suffix: &str) -> Result<String> {
    let key = name(suffix);
    env::var(&key).with_context(|| format!("{key} is required"))
}
fn number<T>(suffix: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + Copy,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let key = name(suffix);
    env::var(&key).map_or(Ok(default), |v| {
        v.parse().with_context(|| format!("invalid {key}"))
    })
}
fn seconds(suffix: &str, default: u64) -> Result<Duration> {
    let value = number(suffix, default)?;
    ensure!(value > 0, "{} must be non-zero", name(suffix));
    Ok(Duration::from_secs(value))
}
fn millis(suffix: &str, default: u64) -> Result<Duration> {
    let value = number(suffix, default)?;
    ensure!(value > 0, "{} must be non-zero", name(suffix));
    Ok(Duration::from_millis(value))
}

fn optional_bound(suffix: &str, default: f64) -> Result<Option<f64>> {
    let key = name(suffix);
    let value = env::var(&key).unwrap_or_else(|_| default.to_string());
    if value.eq_ignore_ascii_case("disabled") {
        return Ok(None);
    }
    let parsed: f64 = value.parse().with_context(|| format!("invalid {key}"))?;
    ensure!(
        parsed.is_finite() && parsed > 0.0,
        "{key} must be positive or 'disabled'"
    );
    Ok(Some(parsed))
}

fn optional_positive(suffix: &str) -> Result<Option<f64>> {
    let key = name(suffix);
    let Some(value) = env::var_os(&key) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{key} must be valid UTF-8"))?;
    if value.eq_ignore_ascii_case("disabled") {
        return Ok(None);
    }
    let parsed: f64 = value.parse().with_context(|| format!("invalid {key}"))?;
    ensure!(
        parsed.is_finite() && parsed > 0.0 && parsed <= 1_000_000.0,
        "{key} must be in 0..=1000000 or 'disabled'"
    );
    Ok(Some(parsed))
}
