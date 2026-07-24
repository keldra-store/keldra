//! CoreStore is the single durable storage boundary for Anvil.
//!
//! Feature code writes durable state through these primitives instead of owning
//! independent journal, metadata, or shard stores. The backend combines local
//! RocksDB metadata with the internal block, root, replication, anti-entropy, and
//! cross-region proxy services exposed by the same API surface.

mod block_shard;
#[cfg(feature = "coremeta-perf-gate")]
mod coremeta_perf_probe;
mod coremeta_quorum;
mod deterministic_proto;
mod encoding;
mod local;
mod local_format_writer;
mod manifest_proto;
mod meta;
mod pending_mutation;
mod root_proto;
mod storage_profile;
mod stream_event_hash_proto;
mod stream_segment;
mod transaction_manifest_proto;
mod types;

use std::future::Future;

use anyhow::Result;

#[cfg(feature = "coremeta-perf-gate")]
pub use coremeta_perf_probe::{
    CoreMetaCatchUpProbe, CoreMetaFrameProbe, CoreMetaGenerationProbe, CoreMetaInventoryProbe,
};
pub use coremeta_quorum::*;
pub(crate) use deterministic_proto::{
    decode_deterministic_proto, encode_deterministic_proto, protobuf_sha256_hex, sha256_digest,
    sha256_hex,
};
pub use encoding::*;
pub(crate) use local::CoreMetaRootPublication;
pub(crate) use local::commit_coremeta_batch_for_storage;
pub(crate) use local::decode_root_anchor_record;
pub(crate) use local::encode_root_anchor_record;
pub(crate) use local::record_corestore_trace_event;
pub(crate) use local::root_prepare_receipt_payload_hash;
pub(crate) use local::root_register_cohort_hash;
pub(crate) use local::shard_inventory_response;
pub(crate) use local::{
    CoreMetaPeerRoute, CoreMetaPeerTarget, CoreMetaWriteRoute, CurrentObjectMetadataPage,
    ObjectMetadataMutationGuard, ObjectMetadataPageCursor, ObjectMetadataPreconditionSnapshot,
    ObjectMetadataProjectionMutation, ObjectVersionsMetadataPage, PreparedObjectMetadataProjection,
    core_mutation_publication_attempt_id,
};
pub use local::{
    CoreMetaRecoverySnapshot, CorePipelineKeyring, CoreStore, CoreStoreAvailabilityError,
    CoreStoreCommitError, CoreStoreNodeIdentity, CoreStoreStartupRecovery,
    core_store_availability_error, is_core_store_unavailable, is_retryable_mutation_conflict,
    is_stream_head_mismatch,
};
pub(crate) use local::{decode_core_object_ref_target, encode_core_object_ref_target};
pub use local_format_writer::CoreFormatWriteReceipt;
pub(crate) use meta::core_meta_row_common_from_payload;
pub use meta::{
    CF_AUTHZ, CF_BOUNDARY, CF_INDEX_DEFS, CF_INDEX_ROWS, CF_INLINE_PAYLOADS, CF_LEASES_FENCES,
    CF_MATERIALISATION, CF_MESH, CF_META_VERSION, CF_OBJECT_HEADS, CF_OBJECT_VERSIONS,
    CF_OBSERVABILITY, CF_PERSONALDB, CF_REFCOUNTS, CF_REGISTRY, CF_ROOT_CACHE, CF_STREAM_HEADS,
    CF_STREAM_RECORDS, CF_TRANSACTIONS, CoreMetaBatchOp, CoreMetaBatchOpKind,
    CoreMetaEncodedOwnedRow, CoreMetaEncodedRow, CoreMetaEncodedRowsCursor,
    CoreMetaEncodedRowsPage, CoreMetaInlineOrLocatorProto, CoreMetaLocatorProto, CoreMetaRecord,
    CoreMetaRowCommonProto, CoreMetaStore, CoreMetaTuplePart, CoreMetaVisibilityState,
    TABLE_AUTHZ_HEAD_ROW, TABLE_AUTHZ_IDEMPOTENCY_RECEIPT_ROW, TABLE_AUTHZ_SCHEMA_ROW,
    TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW, TABLE_AUTHZ_TUPLE_PAGE_ROW,
    TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW, TABLE_BOUNDARY_MIGRATION_ROW,
    TABLE_BOUNDARY_SCHEMA_CURRENT_ROW, TABLE_BOUNDARY_SCHEMA_ROW, TABLE_BOUNDARY_VALUE_ROW,
    TABLE_BUCKET_CURRENT_BY_ID_ROW, TABLE_BUCKET_CURRENT_BY_NAME_ROW, TABLE_BUCKET_EVENT_HEAD_ROW,
    TABLE_BUCKET_ID_ALLOCATOR_ROW, TABLE_CONTROL_CURRENT_ROW, TABLE_CORE_FENCE_ROW,
    TABLE_DERIVED_INDEX_PROOF_ROW, TABLE_DIAGNOSTIC_ROW, TABLE_EXPLICIT_TRANSACTION_ROW,
    TABLE_GATEWAY_METADATA_ROW, TABLE_GATEWAY_MOUNT_ROUTE_ROW, TABLE_GIT_SOURCE_MANIFEST_ROW,
    TABLE_INDEX_DEFINITION_ROW, TABLE_INDEX_ROW, TABLE_INLINE_MANIFEST_BODY_ROW,
    TABLE_INLINE_PAYLOAD_ROW, TABLE_LANDED_BYTE_REF_ROW, TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
    TABLE_LOCAL_NODE_IDENTITY_ROW, TABLE_MANIFEST_CAS_CURRENT_ROW,
    TABLE_MATERIALISATION_CURSOR_ROW, TABLE_MESH_NODE_ROW, TABLE_MESH_PARTITION_ROW,
    TABLE_META_SCHEMA_VERSION_ROW, TABLE_MULTIPART_PART_CURRENT_ROW,
    TABLE_MULTIPART_UPLOAD_CURRENT_ROW, TABLE_NATIVE_IDEMPOTENCY_ROW,
    TABLE_NODE_SIGNING_KEYPAIR_ROW, TABLE_OBJECT_HEAD_ROW,
    TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW, TABLE_OBJECT_SHARD_REPAIR_ROW,
    TABLE_OBJECT_VERSION_META_ROW, TABLE_OBJECT_WATCH_CURSOR_ROW, TABLE_OBSERVABILITY_CURSOR_ROW,
    TABLE_OWNERSHIP_FENCE_ROW, TABLE_PARTITION_OWNER_ROW, TABLE_PENDING_MUTATION_ROW,
    TABLE_PERSONALDB_DATA_LOCATOR_ROW, TABLE_PERSONALDB_GROUP_ROW,
    TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW, TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
    TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW,
    TABLE_PERSONALDB_SIGNING_KEY_ROW, TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
    TABLE_PERSONALDB_WITNESS_RECEIPT_ROW, TABLE_QUORUM_PROFILE_CURRENT_ROW, TABLE_REFCOUNT_ROW,
    TABLE_REGISTRY_BLOB_LOCATOR_ROW, TABLE_REGISTRY_VERSION_ROW, TABLE_REPAIR_FINDING_HEAD_ROW,
    TABLE_REPAIR_FINDING_ID_ROW, TABLE_REPAIR_FINDING_ROW, TABLE_ROOT_CACHE_ROW,
    TABLE_ROOT_CATALOG_CURRENT_ROW, TABLE_ROOT_FAILOVER_CERTIFICATE_ROW,
    TABLE_ROOT_FAILOVER_VOTE_ROW, TABLE_STREAM_HEAD_ROW, TABLE_STREAM_IDEMPOTENCY_ROW,
    TABLE_STREAM_RECORD_INDEX_ROW, TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW, TABLE_TASK_CURRENT_ROW,
    TABLE_TASK_LEASE_ROW, TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW, TABLE_TRANSACTION_LOCATOR_ROW,
    TABLE_TRANSACTION_MANIFEST_BODY_ROW, TABLE_WATCH_CHECKPOINT_ROW, TABLE_WRITER_HEAD_ROW,
    TABLE_WRITER_SEGMENT_ROW, canonical_coremeta_cf_name, core_meta_column_families,
    core_meta_committed_row_common, core_meta_locator_from_manifest_locator,
    core_meta_locator_to_manifest_locator, core_meta_payload_digest, core_meta_pending_row_common,
    core_meta_record_tuple_key, core_meta_root_key_hash, core_meta_tuple_key,
    encode_core_meta_inline_payload_row, validate_coremeta_operation_key,
    validate_coremeta_operation_payload,
};
pub(crate) use meta::{
    CORE_META_MAX_INLINE_PAYLOAD_BYTES, CORE_META_MAX_VALUE_BYTES, replace_core_meta_row_common,
};
#[cfg(feature = "coremeta-perf-gate")]
pub use meta::{CoreMetaGetProbeEntry, reset_coremeta_get_probe, take_coremeta_get_probe};
pub use storage_profile::*;
pub(crate) use transaction_manifest_proto::{
    decode_manifest_locator_proto, encode_manifest_locator_proto,
};
pub use types::*;

pub trait CoreStoreBlockApi {
    fn write_logical_file(
        &self,
        request: WriteLogicalFileRequest,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send;

    fn write_logical_file_from_path(
        &self,
        request: WriteLogicalFilePathRequest,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send;

    fn read_logical_range(
        &self,
        request: ReadLogicalRangeRequest,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send;

    fn read_logical_range_chunks<F, Fut>(
        &self,
        request: ReadLogicalRangeRequest,
        chunk_size: usize,
        on_chunk: F,
    ) -> impl Future<Output = Result<()>> + Send
    where
        F: FnMut(Vec<u8>) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send;

    fn read_logical_file_manifest(
        &self,
        locator: CoreManifestLocator,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send;

    fn verify_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> impl Future<Output = Result<CoreLogicalFileVerificationReport>> + Send;
}

impl CoreStoreBlockApi for CoreStore {
    fn write_logical_file(
        &self,
        request: WriteLogicalFileRequest,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send {
        CoreStore::write_logical_file(self, request)
    }

    fn write_logical_file_from_path(
        &self,
        request: WriteLogicalFilePathRequest,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send {
        async move {
            Ok(
                CoreStore::write_logical_file_path_with_locator(self, request)
                    .await?
                    .manifest,
            )
        }
    }

    fn read_logical_range(
        &self,
        request: ReadLogicalRangeRequest,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send {
        CoreStore::read_logical_range(self, request)
    }

    fn read_logical_range_chunks<F, Fut>(
        &self,
        request: ReadLogicalRangeRequest,
        chunk_size: usize,
        on_chunk: F,
    ) -> impl Future<Output = Result<()>> + Send
    where
        F: FnMut(Vec<u8>) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        CoreStore::read_logical_range_chunks(self, request, chunk_size, on_chunk)
    }

    fn read_logical_file_manifest(
        &self,
        locator: CoreManifestLocator,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send {
        async move { CoreStore::read_logical_file_manifest(self, &locator).await }
    }

    fn verify_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> impl Future<Output = Result<CoreLogicalFileVerificationReport>> + Send {
        CoreStore::verify_logical_file_manifest(self, manifest)
    }
}
