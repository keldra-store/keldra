use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anvil::authentication::{JwtManager, RateLimitConfig, load_token_signing_key};
use anvil::observability::{Observability, ObservabilityConfig};
use anvil::{IndexRuntimeConfig, ServerConfig, serve};
use anvil_index::IndexKind;
use anyhow::{Context, Result};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "anvil-server", version, about = "Anvil 0.9 object server")]
struct Arguments {
    #[arg(long, env = "ANVIL_LISTEN", default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    #[arg(long, env = "ANVIL_PEER_LISTEN", default_value = "127.0.0.1:50052")]
    peer_listen: SocketAddr,

    #[arg(long, env = "ANVIL_PEER_ADVERTISE")]
    peer_advertise: Option<String>,

    /// Consume one operator-copied mode-0600 bundle to join an existing cluster.
    #[arg(long, env = "ANVIL_JOIN_BUNDLE")]
    join_bundle: Option<PathBuf>,

    #[arg(long, env = "ANVIL_DATA_DIR", default_value = "anvil-data")]
    data_dir: PathBuf,

    #[arg(long, env = "ANVIL_RUN_SYSTEM_BOOTSTRAP", default_value_t = false)]
    run_system_bootstrap: bool,

    #[arg(
        long,
        env = "ANVIL_SYSTEM_BOOTSTRAP_CREDENTIAL_OUTPUT",
        requires = "run_system_bootstrap"
    )]
    system_bootstrap_credential_output: Option<PathBuf>,

    #[arg(long, env = "ANVIL_NODE_ID", default_value_t = 1)]
    node_id: u16,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint: Option<String>,

    #[arg(long, env = "ANVIL_MAX_ATOMIC_COMMIT_ENTRIES", default_value_t = 4_096)]
    max_atomic_commit_entries: u32,

    #[arg(
        long,
        env = "ANVIL_MAX_ATOMIC_COMMIT_BYTES",
        default_value_t = 16 * 1024 * 1024_u64
    )]
    max_atomic_commit_bytes: u64,

    #[arg(
        long,
        env = "ANVIL_ATOMIC_PROGRAM_TIMEOUT_SECONDS",
        default_value = "30"
    )]
    atomic_program_timeout_seconds: NonZeroU64,

    /// Maximum wall time for one QueryIndex RPC; shorter client deadlines still win.
    #[arg(long, env = "ANVIL_INDEX_QUERY_TIMEOUT_SECONDS", default_value = "300")]
    index_query_timeout_seconds: NonZeroU64,

    #[arg(long, env = "ANVIL_TOKEN_SIGNING_KEY_FILE")]
    token_signing_key_file: PathBuf,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_GLOBAL_PER_SECOND",
        default_value = "10000"
    )]
    rate_limit_global_per_second: NonZeroU32,

    #[arg(long, env = "ANVIL_RATE_LIMIT_GLOBAL_BURST", default_value = "10000")]
    rate_limit_global_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_AUTHENTICATED_PER_SECOND",
        default_value = "1000"
    )]
    rate_limit_authenticated_per_second: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_AUTHENTICATED_BURST",
        default_value = "1000"
    )]
    rate_limit_authenticated_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_PER_MINUTE",
        default_value = "100"
    )]
    rate_limit_credential_global_per_minute: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_GLOBAL_BURST",
        default_value = "20"
    )]
    rate_limit_credential_global_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_PER_MINUTE",
        default_value = "10"
    )]
    rate_limit_credential_client_per_minute: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_CREDENTIAL_CLIENT_BURST",
        default_value = "3"
    )]
    rate_limit_credential_client_burst: NonZeroU32,

    #[arg(
        long,
        env = "ANVIL_RATE_LIMIT_KEYED_CLEANUP_INTERVAL",
        default_value = "1024"
    )]
    rate_limit_keyed_cleanup_interval: NonZeroU64,

    /// Shared disposable index disk-cache budget in bytes (default: 10 GiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_DISK_CACHE_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_DISK_CACHE_BYTES
    )]
    index_disk_cache_bytes: u64,

    /// Percentage of node memory bounding concurrent index block materialization (default: 10).
    #[arg(
        long,
        env = "ANVIL_INDEX_MEMORY_PERCENT",
        default_value_t = IndexRuntimeConfig::DEFAULT_MEMORY_PERCENT
    )]
    index_memory_percent: u8,

    /// Hard aggregate build/compaction heap budget for each index kind (default: 256 MiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_BUILDER_MEMORY_BYTES_PER_KIND",
        default_value_t = IndexRuntimeConfig::DEFAULT_BUILDER_MEMORY_BYTES_PER_KIND
    )]
    index_builder_memory_bytes_per_kind: u64,

    /// Path builder-memory override; absent uses the common per-kind fallback.
    #[arg(long, env = "ANVIL_INDEX_PATH_BUILDER_MEMORY_BYTES")]
    index_path_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Path compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_PATH_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_path_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_PATH_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_path_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_PATH_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_path_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_PATH_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_path_external_sort_chunk_bytes: u64,

    /// Metadata-filter builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_BUILDER_MEMORY_BYTES")]
    index_metadata_filter_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Metadata-filter compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_METADATA_FILTER_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_metadata_filter_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_metadata_filter_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_metadata_filter_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_metadata_filter_external_sort_chunk_bytes: u64,

    /// Typed-JSON builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_BUILDER_MEMORY_BYTES")]
    index_typed_json_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Typed-JSON compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_TYPED_JSON_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_typed_json_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_typed_json_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_typed_json_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_typed_json_external_sort_chunk_bytes: u64,

    /// Full-text builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_BUILDER_MEMORY_BYTES")]
    index_full_text_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Full-text compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_FULL_TEXT_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_full_text_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_full_text_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_full_text_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_full_text_external_sort_chunk_bytes: u64,

    /// Vector builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_VECTOR_BUILDER_MEMORY_BYTES")]
    index_vector_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Vector compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_VECTOR_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_vector_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_VECTOR_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_vector_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_VECTOR_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_vector_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_VECTOR_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_vector_external_sort_chunk_bytes: u64,

    /// Hybrid builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_HYBRID_BUILDER_MEMORY_BYTES")]
    index_hybrid_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Hybrid compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_HYBRID_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_hybrid_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_HYBRID_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_hybrid_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_HYBRID_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_hybrid_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_HYBRID_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_hybrid_external_sort_chunk_bytes: u64,

    /// Git-source builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_BUILDER_MEMORY_BYTES")]
    index_git_source_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Git-source compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_GIT_SOURCE_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_git_source_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_git_source_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_git_source_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_git_source_external_sort_chunk_bytes: u64,

    /// Tensor builder-memory override; absent uses the common fallback.
    #[arg(long, env = "ANVIL_INDEX_TENSOR_BUILDER_MEMORY_BYTES")]
    index_tensor_builder_memory_bytes: Option<u64>,

    /// Maximum parallel Tensor compaction lanes (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_TENSOR_COMPACTION_MAX_LANES",
        default_value_t = IndexRuntimeConfig::DEFAULT_COMPACTION_MAX_LANES
    )]
    index_tensor_compaction_max_lanes: u32,

    #[arg(long, env = "ANVIL_INDEX_TENSOR_PROJECTION_MAX_LANES", default_value_t = IndexRuntimeConfig::DEFAULT_PROJECTION_MAX_LANES)]
    index_tensor_projection_max_lanes: u32,
    #[arg(long, env = "ANVIL_INDEX_TENSOR_SOURCE_QUANTUM_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_SOURCE_QUANTUM_BYTES)]
    index_tensor_source_quantum_bytes: u64,
    #[arg(long, env = "ANVIL_INDEX_TENSOR_EXTERNAL_SORT_CHUNK_BYTES", default_value_t = IndexRuntimeConfig::DEFAULT_EXTERNAL_SORT_CHUNK_BYTES)]
    index_tensor_external_sort_chunk_bytes: u64,

    /// Threads in Anvil's process-owned index CPU pool (default: 4).
    #[arg(
        long,
        env = "ANVIL_INDEX_RAYON_WORKERS",
        default_value_t = IndexRuntimeConfig::DEFAULT_RAYON_WORKERS
    )]
    index_rayon_workers: u32,

    /// Maximum index queries executing concurrently on this node (default: 64).
    #[arg(
        long,
        env = "ANVIL_INDEX_QUERY_MAX_CONCURRENCY",
        default_value_t = IndexRuntimeConfig::DEFAULT_QUERY_MAX_CONCURRENCY
    )]
    index_query_max_concurrency: u32,

    /// Cache-read bytes processed by one query before it cooperatively yields (default: 4 MiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_QUERY_WORK_QUANTUM_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_QUERY_WORK_QUANTUM_BYTES
    )]
    index_query_work_quantum_bytes: u64,

    /// Hard working-memory budget shared by all index queries (default: 512 MiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_QUERY_MEMORY_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_QUERY_MEMORY_BYTES
    )]
    index_query_memory_bytes: u64,

    /// Fallback maximum segments retained in one size tier before builders compact (default: 64).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_SEGMENTS_PER_TIER",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_SEGMENTS_PER_TIER
    )]
    index_max_segments_per_tier: u32,

    /// Fallback maximum encoded unmerged bytes in one size tier (default: 1 GiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_UNMERGED_BYTES_PER_TIER",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_UNMERGED_BYTES_PER_TIER
    )]
    index_max_unmerged_bytes_per_tier: u64,

    #[arg(long, env = "ANVIL_INDEX_PATH_MAX_SEGMENTS_PER_TIER")]
    index_path_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_PATH_MAX_UNMERGED_BYTES_PER_TIER")]
    index_path_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_MAX_SEGMENTS_PER_TIER")]
    index_metadata_filter_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_METADATA_FILTER_MAX_UNMERGED_BYTES_PER_TIER")]
    index_metadata_filter_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_MAX_SEGMENTS_PER_TIER")]
    index_typed_json_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_TYPED_JSON_MAX_UNMERGED_BYTES_PER_TIER")]
    index_typed_json_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_MAX_SEGMENTS_PER_TIER")]
    index_full_text_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_FULL_TEXT_MAX_UNMERGED_BYTES_PER_TIER")]
    index_full_text_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_VECTOR_MAX_SEGMENTS_PER_TIER")]
    index_vector_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_VECTOR_MAX_UNMERGED_BYTES_PER_TIER")]
    index_vector_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_HYBRID_MAX_SEGMENTS_PER_TIER")]
    index_hybrid_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_HYBRID_MAX_UNMERGED_BYTES_PER_TIER")]
    index_hybrid_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_MAX_SEGMENTS_PER_TIER")]
    index_git_source_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_GIT_SOURCE_MAX_UNMERGED_BYTES_PER_TIER")]
    index_git_source_max_unmerged_bytes_per_tier: Option<u64>,
    #[arg(long, env = "ANVIL_INDEX_TENSOR_MAX_SEGMENTS_PER_TIER")]
    index_tensor_max_segments_per_tier: Option<u32>,
    #[arg(long, env = "ANVIL_INDEX_TENSOR_MAX_UNMERGED_BYTES_PER_TIER")]
    index_tensor_max_unmerged_bytes_per_tier: Option<u64>,

    /// Maximum generations retained per index, including current (default: 3).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_RETAINED_GENERATIONS",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_RETAINED_GENERATIONS
    )]
    index_max_retained_generations: u32,

    /// Maximum age of an obsolete index generation in hours (default: 24).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_GENERATION_AGE_HOURS",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_GENERATION_AGE_HOURS
    )]
    index_max_generation_age_hours: u64,

    /// Maximum authoritative bytes retained across all generations per index (default: 50 GiB).
    #[arg(
        long,
        env = "ANVIL_INDEX_MAX_RETAINED_GENERATION_BYTES",
        default_value_t = IndexRuntimeConfig::DEFAULT_MAX_RETAINED_GENERATION_BYTES
    )]
    index_max_retained_generation_bytes: u64,

    #[arg(long, env = "ANVIL_MAX_BLOB_BYTES", default_value_t = 16 * 1024 * 1024 * 1024_u64)]
    max_blob_bytes: u64,

    #[arg(
        long,
        env = "ANVIL_ERASURE_DATA_SHARDS",
        default_value_t = anvil_store::DEFAULT_ERASURE_DATA_SHARDS
    )]
    erasure_data_shards: u16,

    #[arg(
        long,
        env = "ANVIL_ERASURE_PARITY_SHARDS",
        default_value_t = anvil_store::DEFAULT_ERASURE_PARITY_SHARDS
    )]
    erasure_parity_shards: u16,

    #[arg(
        long,
        env = "ANVIL_ERASURE_STRIPE_UNIT_BYTES",
        default_value_t = anvil_store::DEFAULT_ERASURE_STRIPE_UNIT_BYTES
    )]
    erasure_stripe_unit_bytes: u32,

    #[arg(
        long,
        env = "ANVIL_AWAITING_PUBLISH_TTL_SECONDS",
        default_value_t = anvil_store::DEFAULT_AWAITING_PUBLISH_TTL_SECONDS
    )]
    awaiting_publish_ttl_seconds: u64,

    #[arg(
        long,
        env = "ANVIL_MUTATION_RECEIPT_RETENTION_SECONDS",
        default_value_t = anvil_store::DEFAULT_MUTATION_RECEIPT_RETENTION_SECONDS
    )]
    mutation_receipt_retention_seconds: u64,

    #[arg(
        long,
        env = "ANVIL_MAX_MUTATION_RECEIPT_ENTRIES",
        default_value_t = anvil_store::DEFAULT_MUTATION_RECEIPT_MAX_ENTRIES
    )]
    max_mutation_receipt_entries: u64,

    #[arg(
        long,
        env = "ANVIL_MAX_MUTATION_RECEIPT_BYTES",
        default_value_t = anvil_store::DEFAULT_MUTATION_RECEIPT_MAX_BYTES
    )]
    max_mutation_receipt_bytes: u64,

    /// Maximum retained entries in each node's ordered source journal.
    #[arg(
        long,
        env = "ANVIL_SOURCE_JOURNAL_MAX_ENTRIES",
        default_value_t = anvil_store::DEFAULT_WATCH_MAX_ENTRIES
    )]
    source_journal_max_entries: u64,

    /// Maximum retained logical bytes in each node's ordered source journal.
    #[arg(
        long,
        env = "ANVIL_SOURCE_JOURNAL_MAX_BYTES",
        default_value_t = anvil_store::DEFAULT_WATCH_MAX_BYTES
    )]
    source_journal_max_bytes: u64,
}

impl Arguments {
    fn erasure_profile(&self) -> Result<anvil_store::ErasureProfile> {
        anvil_store::ErasureProfile::new(
            self.erasure_data_shards,
            self.erasure_parity_shards,
            self.erasure_stripe_unit_bytes,
        )
        .context("validate erasure-code profile")
    }

    fn index_runtime_config(&self) -> Result<IndexRuntimeConfig> {
        let config = IndexRuntimeConfig::new(
            self.index_disk_cache_bytes,
            self.index_memory_percent,
            self.index_builder_memory_bytes_per_kind,
            self.index_rayon_workers,
            self.index_max_retained_generations,
            self.index_max_generation_age_hours,
            self.index_max_retained_generation_bytes,
        )
        .context("validate index runtime configuration")?
        .with_query_max_concurrency(self.index_query_max_concurrency)
        .and_then(|config| {
            config.with_query_work_quantum_bytes(self.index_query_work_quantum_bytes)
        })
        .and_then(|config| config.with_query_memory_bytes(self.index_query_memory_bytes))
        .context("validate index runtime configuration")?;
        let mut config = config;
        for (
            kind,
            memory,
            projection_lanes,
            source_quantum,
            sort_chunk,
            compaction_lanes,
            max_segments,
            max_bytes,
        ) in [
            (
                IndexKind::Path,
                self.index_path_builder_memory_bytes,
                self.index_path_projection_max_lanes,
                self.index_path_source_quantum_bytes,
                self.index_path_external_sort_chunk_bytes,
                self.index_path_compaction_max_lanes,
                self.index_path_max_segments_per_tier,
                self.index_path_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::MetadataFilter,
                self.index_metadata_filter_builder_memory_bytes,
                self.index_metadata_filter_projection_max_lanes,
                self.index_metadata_filter_source_quantum_bytes,
                self.index_metadata_filter_external_sort_chunk_bytes,
                self.index_metadata_filter_compaction_max_lanes,
                self.index_metadata_filter_max_segments_per_tier,
                self.index_metadata_filter_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::TypedJson,
                self.index_typed_json_builder_memory_bytes,
                self.index_typed_json_projection_max_lanes,
                self.index_typed_json_source_quantum_bytes,
                self.index_typed_json_external_sort_chunk_bytes,
                self.index_typed_json_compaction_max_lanes,
                self.index_typed_json_max_segments_per_tier,
                self.index_typed_json_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::FullText,
                self.index_full_text_builder_memory_bytes,
                self.index_full_text_projection_max_lanes,
                self.index_full_text_source_quantum_bytes,
                self.index_full_text_external_sort_chunk_bytes,
                self.index_full_text_compaction_max_lanes,
                self.index_full_text_max_segments_per_tier,
                self.index_full_text_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::Vector,
                self.index_vector_builder_memory_bytes,
                self.index_vector_projection_max_lanes,
                self.index_vector_source_quantum_bytes,
                self.index_vector_external_sort_chunk_bytes,
                self.index_vector_compaction_max_lanes,
                self.index_vector_max_segments_per_tier,
                self.index_vector_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::Hybrid,
                self.index_hybrid_builder_memory_bytes,
                self.index_hybrid_projection_max_lanes,
                self.index_hybrid_source_quantum_bytes,
                self.index_hybrid_external_sort_chunk_bytes,
                self.index_hybrid_compaction_max_lanes,
                self.index_hybrid_max_segments_per_tier,
                self.index_hybrid_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::GitSource,
                self.index_git_source_builder_memory_bytes,
                self.index_git_source_projection_max_lanes,
                self.index_git_source_source_quantum_bytes,
                self.index_git_source_external_sort_chunk_bytes,
                self.index_git_source_compaction_max_lanes,
                self.index_git_source_max_segments_per_tier,
                self.index_git_source_max_unmerged_bytes_per_tier,
            ),
            (
                IndexKind::Tensor,
                self.index_tensor_builder_memory_bytes,
                self.index_tensor_projection_max_lanes,
                self.index_tensor_source_quantum_bytes,
                self.index_tensor_external_sort_chunk_bytes,
                self.index_tensor_compaction_max_lanes,
                self.index_tensor_max_segments_per_tier,
                self.index_tensor_max_unmerged_bytes_per_tier,
            ),
        ] {
            config = config
                .with_kind_limits(
                    kind,
                    memory.unwrap_or(self.index_builder_memory_bytes_per_kind),
                    compaction_lanes,
                )
                .and_then(|config| config.with_kind_projection_max_lanes(kind, projection_lanes))
                .and_then(|config| config.with_kind_source_quantum_bytes(kind, source_quantum))
                .and_then(|config| config.with_kind_external_sort_chunk_bytes(kind, sort_chunk))
                .and_then(|config| {
                    config.with_kind_compaction_debt_limits(
                        kind,
                        max_segments.unwrap_or(self.index_max_segments_per_tier),
                        max_bytes.unwrap_or(self.index_max_unmerged_bytes_per_tier),
                    )
                })
                .context("validate index runtime configuration")?;
        }
        Ok(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let erasure_profile = arguments.erasure_profile()?;
    let index_runtime = arguments.index_runtime_config()?;
    let signing_key =
        load_token_signing_key(&arguments.token_signing_key_file).with_context(|| {
            format!(
                "load token signing key from {}",
                arguments.token_signing_key_file.display()
            )
        })?;
    let token_manager = JwtManager::new(signing_key).context("configure access tokens")?;
    let observability = Observability::init(ObservabilityConfig {
        node_id: arguments.node_id,
        otlp_endpoint: arguments.otlp_endpoint,
    })?;
    let server_result = serve(ServerConfig {
        listen: arguments.listen,
        peer_listen: arguments.peer_listen,
        peer_advertise: arguments.peer_advertise,
        join_bundle: arguments.join_bundle,
        data_dir: arguments.data_dir,
        run_system_bootstrap: arguments.run_system_bootstrap,
        system_bootstrap_credential_output: arguments.system_bootstrap_credential_output,
        node_id: arguments.node_id,
        max_atomic_commit_entries: arguments.max_atomic_commit_entries,
        max_atomic_commit_bytes: arguments.max_atomic_commit_bytes,
        atomic_program_timeout: std::time::Duration::from_secs(
            arguments.atomic_program_timeout_seconds.get(),
        ),
        index_query_timeout: std::time::Duration::from_secs(
            arguments.index_query_timeout_seconds.get(),
        ),
        token_manager,
        rate_limits: RateLimitConfig {
            global_per_second: arguments.rate_limit_global_per_second,
            global_burst: arguments.rate_limit_global_burst,
            authenticated_per_second: arguments.rate_limit_authenticated_per_second,
            authenticated_burst: arguments.rate_limit_authenticated_burst,
            credential_global_per_minute: arguments.rate_limit_credential_global_per_minute,
            credential_global_burst: arguments.rate_limit_credential_global_burst,
            credential_client_per_minute: arguments.rate_limit_credential_client_per_minute,
            credential_client_burst: arguments.rate_limit_credential_client_burst,
            keyed_cleanup_interval: arguments.rate_limit_keyed_cleanup_interval,
        },
        index_runtime,
        max_blob_bytes: arguments.max_blob_bytes,
        erasure_profile,
        awaiting_publish_ttl_seconds: arguments.awaiting_publish_ttl_seconds,
        mutation_receipt_retention_seconds: arguments.mutation_receipt_retention_seconds,
        max_mutation_receipt_entries: arguments.max_mutation_receipt_entries,
        max_mutation_receipt_bytes: arguments.max_mutation_receipt_bytes,
        source_journal_max_entries: arguments.source_journal_max_entries,
        source_journal_max_bytes: arguments.source_journal_max_bytes,
    })
    .await;
    let observability_result = observability.shutdown().await;
    match (server_result, observability_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(server_error), Ok(())) => Err(server_error),
        (Ok(()), Err(observability_error)) => Err(observability_error),
        (Err(server_error), Err(observability_error)) => {
            tracing::error!(
                error = %observability_error,
                "OpenTelemetry shutdown failed after the server stopped"
            );
            Err(server_error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;

    fn parse(extra: &[&str]) -> Arguments {
        let mut arguments = vec![
            "anvil-server",
            "--token-signing-key-file",
            "test-signing-key",
        ];
        arguments.extend_from_slice(extra);
        Arguments::try_parse_from(arguments).unwrap()
    }

    #[test]
    fn erasure_profile_defaults_to_two_plus_one() {
        let arguments = parse(&[]);
        let profile = arguments.erasure_profile().unwrap();
        assert_eq!(profile, anvil_store::ErasureProfile::default());
        assert_eq!(
            arguments.peer_listen,
            "127.0.0.1:50052".parse::<SocketAddr>().unwrap()
        );
        assert!(arguments.peer_advertise.is_none());
    }

    #[test]
    fn peer_listener_and_advertised_address_are_explicit_startup_options() {
        let arguments = parse(&[
            "--peer-listen",
            "0.0.0.0:60052",
            "--peer-advertise",
            "anvil-1.internal:60052",
        ]);
        assert_eq!(
            arguments.peer_listen,
            "0.0.0.0:60052".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            arguments.peer_advertise.as_deref(),
            Some("anvil-1.internal:60052")
        );
    }

    #[test]
    fn erasure_profile_accepts_a_valid_startup_override() {
        let profile = parse(&[
            "--erasure-data-shards",
            "4",
            "--erasure-parity-shards",
            "2",
            "--erasure-stripe-unit-bytes",
            "32768",
        ])
        .erasure_profile()
        .unwrap();
        assert_eq!(profile.data_shards(), 4);
        assert_eq!(profile.parity_shards(), 2);
        assert_eq!(profile.stripe_unit(), 32 * 1024);
    }

    #[test]
    fn erasure_profile_rejects_invalid_startup_geometry() {
        for extra in [
            vec!["--erasure-data-shards", "0"],
            vec!["--erasure-parity-shards", "0"],
            vec![
                "--erasure-data-shards",
                "255",
                "--erasure-parity-shards",
                "2",
            ],
            vec!["--erasure-stripe-unit-bytes", "0"],
        ] {
            let error = parse(&extra).erasure_profile().unwrap_err();
            assert!(error.to_string().contains("validate erasure-code profile"));
        }
    }

    #[test]
    fn index_query_timeout_is_independent_from_the_ordinary_request_maximum() {
        let defaults = parse(&[]);
        assert_eq!(defaults.atomic_program_timeout_seconds.get(), 30);
        assert_eq!(defaults.index_query_timeout_seconds.get(), 300);

        let configured = parse(&[
            "--atomic-program-timeout-seconds",
            "12",
            "--index-query-timeout-seconds",
            "600",
        ]);
        assert_eq!(configured.atomic_program_timeout_seconds.get(), 12);
        assert_eq!(configured.index_query_timeout_seconds.get(), 600);
    }

    #[test]
    fn index_runtime_defaults_are_wired_to_startup_configuration() {
        assert_eq!(
            parse(&[]).index_runtime_config().unwrap(),
            IndexRuntimeConfig::default()
        );
    }

    #[test]
    fn index_runtime_accepts_explicit_operator_limits() {
        let config = parse(&[
            "--index-disk-cache-bytes",
            "1048576",
            "--index-memory-percent",
            "25",
            "--index-builder-memory-bytes-per-kind",
            "33554432",
            "--index-path-builder-memory-bytes",
            "67108864",
            "--index-path-compaction-max-lanes",
            "3",
            "--index-path-projection-max-lanes",
            "2",
            "--index-path-source-quantum-bytes",
            "8388608",
            "--index-path-external-sort-chunk-bytes",
            "4194304",
            "--index-rayon-workers",
            "6",
            "--index-query-max-concurrency",
            "17",
            "--index-query-work-quantum-bytes",
            "1048576",
            "--index-query-memory-bytes",
            "268435456",
            "--index-max-segments-per-tier",
            "12",
            "--index-max-unmerged-bytes-per-tier",
            "10485760",
            "--index-path-max-segments-per-tier",
            "8",
            "--index-path-max-unmerged-bytes-per-tier",
            "5242880",
            "--index-max-retained-generations",
            "7",
            "--index-max-generation-age-hours",
            "48",
            "--index-max-retained-generation-bytes",
            "2097152",
        ])
        .index_runtime_config()
        .unwrap();
        assert_eq!(config.disk_cache_bytes(), 1_048_576);
        assert_eq!(config.memory_percent(), 25);
        assert_eq!(config.builder_memory_bytes_per_kind(), 33_554_432);
        assert_eq!(config.builder_memory_bytes(IndexKind::Path), 67_108_864);
        assert_eq!(config.compaction_max_lanes(IndexKind::Path), 3);
        assert_eq!(config.projection_max_lanes(IndexKind::Path), 2);
        assert_eq!(config.source_quantum_bytes(IndexKind::Path), 8_388_608);
        assert_eq!(config.external_sort_chunk_bytes(IndexKind::Path), 4_194_304);
        assert_eq!(
            config.builder_memory_bytes(IndexKind::TypedJson),
            33_554_432
        );
        assert_eq!(config.compaction_max_lanes(IndexKind::TypedJson), 4);
        assert_eq!(config.rayon_workers(), 6);
        assert_eq!(config.query_max_concurrency(), 17);
        assert_eq!(config.query_work_quantum_bytes(), 1_048_576);
        assert_eq!(config.query_memory_bytes(), 268_435_456);
        assert_eq!(config.max_segments_per_tier(IndexKind::Path), 8);
        assert_eq!(
            config.max_unmerged_bytes_per_tier(IndexKind::Path),
            5_242_880
        );
        assert_eq!(config.max_segments_per_tier(IndexKind::TypedJson), 12);
        assert_eq!(
            config.max_unmerged_bytes_per_tier(IndexKind::TypedJson),
            10_485_760
        );
        assert_eq!(config.max_retained_generations(), 7);
        assert_eq!(config.max_generation_age_hours(), 48);
        assert_eq!(config.max_retained_generation_bytes(), 2_097_152);
    }

    #[test]
    fn index_runtime_rejects_zero_and_out_of_range_limits() {
        for extra in [
            vec!["--index-disk-cache-bytes", "0"],
            vec!["--index-memory-percent", "0"],
            vec!["--index-memory-percent", "101"],
            vec!["--index-builder-memory-bytes-per-kind", "0"],
            vec!["--index-vector-builder-memory-bytes", "0"],
            vec!["--index-vector-projection-max-lanes", "0"],
            vec!["--index-vector-source-quantum-bytes", "0"],
            vec!["--index-vector-external-sort-chunk-bytes", "0"],
            vec!["--index-vector-compaction-max-lanes", "0"],
            vec!["--index-rayon-workers", "0"],
            vec!["--index-query-max-concurrency", "0"],
            vec!["--index-query-work-quantum-bytes", "0"],
            vec!["--index-query-memory-bytes", "0"],
            vec!["--index-max-segments-per-tier", "0"],
            vec!["--index-max-unmerged-bytes-per-tier", "0"],
            vec!["--index-vector-max-segments-per-tier", "0"],
            vec!["--index-vector-max-unmerged-bytes-per-tier", "0"],
            vec!["--index-max-retained-generations", "0"],
            vec!["--index-max-generation-age-hours", "0"],
            vec!["--index-max-retained-generation-bytes", "0"],
        ] {
            let error = parse(&extra).index_runtime_config().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("validate index runtime configuration")
            );
        }
    }

    #[test]
    fn index_runtime_defaults_and_meaning_are_visible_in_help() {
        let help = Arguments::command().render_long_help().to_string();
        for expected in [
            "--index-disk-cache-bytes",
            "default: 10 GiB",
            "--index-memory-percent",
            "default: 10",
            "--index-builder-memory-bytes-per-kind",
            "default: 256 MiB",
            "--index-path-builder-memory-bytes",
            "--index-path-compaction-max-lanes",
            "--index-path-projection-max-lanes",
            "--index-path-source-quantum-bytes",
            "--index-path-external-sort-chunk-bytes",
            "--index-tensor-builder-memory-bytes",
            "--index-tensor-compaction-max-lanes",
            "--index-tensor-projection-max-lanes",
            "--index-tensor-source-quantum-bytes",
            "--index-tensor-external-sort-chunk-bytes",
            "--index-rayon-workers",
            "--index-query-max-concurrency",
            "--index-query-work-quantum-bytes",
            "--index-query-memory-bytes",
            "default: 512 MiB",
            "default: 4",
            "--index-max-segments-per-tier",
            "default: 64",
            "--index-max-unmerged-bytes-per-tier",
            "default: 1 GiB",
            "--index-path-max-segments-per-tier",
            "--index-path-max-unmerged-bytes-per-tier",
            "--index-tensor-max-segments-per-tier",
            "--index-tensor-max-unmerged-bytes-per-tier",
            "--source-journal-max-entries",
            "--source-journal-max-bytes",
            "--index-max-retained-generations",
            "including current",
            "--index-max-generation-age-hours",
            "default: 24",
            "--index-max-retained-generation-bytes",
            "default: 50 GiB",
        ] {
            assert!(
                help.contains(expected),
                "help omitted `{expected}`:\n{help}"
            );
        }
    }
}
