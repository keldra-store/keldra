#![recursion_limit = "256"]

use std::{
    fs,
    path::{Path, PathBuf},
};

fn workspace_file(path: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf();
    fs::read_to_string(root.join(path)).unwrap_or_else(|err| panic!("read {path}: {err}"))
}

fn assert_contains_all(label: &str, source: &str, terms: &[&str]) {
    let missing = terms
        .iter()
        .copied()
        .filter(|term| !source.contains(term))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "{label} missing required terms: {missing:#?}"
    );
}

fn assert_contains_none(label: &str, source: &str, terms: &[&str]) {
    let present = terms
        .iter()
        .copied()
        .filter(|term| source.contains(term))
        .collect::<Vec<_>>();
    assert!(
        present.is_empty(),
        "{label} contains forbidden terms: {present:#?}"
    );
}

#[test]
fn object_refs_are_durable_protobuf_targets_not_json_manifests() {
    let source = format!(
        "{}\n{}\n{}\n{}\n{}",
        workspace_file("anvil-core/src/object_manager.rs"),
        workspace_file("anvil-core/src/object_manager/read.rs"),
        workspace_file("anvil-core/src/core_store/local_object_metadata.rs"),
        workspace_file("anvil-core/src/index_builder/helpers.rs"),
        workspace_file("anvil-core/src/persistence.rs"),
    );

    assert_contains_all(
        "object data target canonical path",
        &source,
        &[
            "anvil.core.object_data_target.v1",
            "encode_manifest_locator_proto(locator)",
            "decode_manifest_locator_proto",
            "encode_core_object_ref_target",
            "decode_core_object_ref_target",
            "\"kind\": \"object_ref\"",
            "shard_map_target",
            "optional_object_data_target_bytes",
        ],
    );
    assert_contains_none(
        "object ref JSON durable path",
        &source,
        &[
            "serde_json::from_value(value.clone())",
            "serde_json::to_value(object_ref)",
            "shard_map_json",
            "\"anvil.core.object_ref.v1\"",
        ],
    );
}

#[test]
fn metadata_journal_manifests_and_rows_are_protobuf_not_json_sidecars() {
    let source = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/metadata_journal.rs"),
        workspace_file("anvil-core/src/metadata_journal/helpers.rs"),
    );

    assert_contains_all(
        "object metadata canonical records",
        &source,
        &[
            "ObjectMetadataBodyProto",
            "PartitionManifestProto",
            "encode_partition_manifest(&manifest)",
            "ensure_deterministic_proto(&proto, input, \"partition manifest\")",
            "shard_map_target",
            "canonical_json_bytes",
        ],
    );
    assert_contains_none(
        "object metadata JSON durable manifests",
        &source,
        &[
            "serde_json::to_vec_pretty(&manifest)",
            "let manifest: PartitionManifest = serde_json::from_slice(input)?",
            "serde_json::to_vec(&unsigned)",
            "shard_map_json",
        ],
    );
}

#[test]
fn manifest_cas_stream_records_use_canonical_protobuf_wrappers() {
    let source = workspace_file("anvil-core/src/manifest_journal.rs");

    assert_contains_all(
        "manifest CAS canonical protobuf record",
        &source,
        &[
            "ManifestBodyProto",
            "ManifestCurrentRowProto",
            "canonical_json_bytes(&body.manifest)",
            "decode_canonical_json(&proto.manifest_json",
            "ensure_deterministic_proto(&proto, bytes, \"manifest CAS body\")",
        ],
    );
    assert_contains_none(
        "manifest CAS raw JSON record encoding",
        &source,
        &["manifest_json: serde_json::to_vec(&body.manifest)?"],
    );
}

#[test]
fn object_and_index_payloads_publish_corestore_locators_not_metadata_values() {
    let object_manager = workspace_file("anvil-core/src/object_manager.rs");
    let writer_segment = workspace_file("anvil-core/src/formats/writer.rs");
    let typed_segment = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/typed_field_segment.rs"),
        writer_segment
    );
    let writer_segment = workspace_file("anvil-core/src/formats/writer.rs");
    let vector_segment = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/vector_segment.rs"),
        writer_segment
    );
    let writer_segment = workspace_file("anvil-core/src/formats/writer.rs");
    let full_text_segment = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/full_text_segment.rs"),
        writer_segment
    );
    let metadata_helpers = workspace_file("anvil-core/src/metadata_journal/helpers.rs");

    assert_contains_all(
        "object blob payload CoreStore path",
        &object_manager,
        &[
            "stream_to_temp_file(data_stream)",
            "write_logical_file_path_with_locator(WriteLogicalFilePathRequest",
            "put_blob_with_storage_class",
            "source_path: temp_path.clone()",
            "source_hash: format!(\"sha256:{stream_hash}\")",
            "object_data_target_to_shard_map(&ObjectDataTarget::LogicalFile",
            "object_data_target_to_shard_map(&ObjectDataTarget::ObjectRef",
        ],
    );
    for (label, source) in [
        ("typed field segment", typed_segment.as_str()),
        ("vector segment", vector_segment.as_str()),
        ("full text segment", full_text_segment.as_str()),
    ] {
        assert_contains_all(
            label,
            source,
            &[
                "write_format_build_output(WriterBuildOutput",
                "encode_writer_segment(",
                "written_object_refs",
                "encode_core_object_ref_target",
            ],
        );
    }
    assert_contains_all(
        "metadata manifest and segment logical files",
        &metadata_helpers,
        &["write_logical_file_ref(WriteLogicalFileRequest"],
    );
}

#[test]
fn uploaded_object_bytes_are_staged_under_corestore_before_byte_pipeline() {
    let storage = workspace_file("anvil-core/src/storage.rs");
    let object_manager = workspace_file("anvil-core/src/object_manager.rs");
    let corestore = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/local_init_blob.rs"),
        workspace_file("anvil-core/src/core_store/local_logical_file_path.rs")
    );

    assert_contains_all(
        "storage scratch path is CoreStore staging",
        &storage,
        &[
            "const CORESTORE_STAGING_DIR: &str = \"staging\"",
            "core_store_staging_tmp_path(&storage_path)",
            ".join(CORESTORE_DIR)",
            ".join(CORESTORE_STAGING_DIR)",
            ".join(CORESTORE_TMP_DIR)",
        ],
    );
    assert_contains_all(
        "object upload keeps payload as staged file until CoreStore ingestion",
        &object_manager,
        &[
            "write_logical_file_path_with_locator(WriteLogicalFilePathRequest",
            "source_path: temp_path.clone()",
            "source_len: total_bytes_u64",
            "source_hash: format!(\"sha256:{stream_hash}\")",
        ],
    );
    assert_contains_all(
        "CoreStore path ingestion chunks from the staged file",
        &corestore,
        &[
            "write_logical_file_blocks_from_path",
            "fs::File::open(&request.source_path)",
            "file.read_exact(&mut chunk_bytes).await?",
            "CorePendingMutationPayload::Landed(&input.bytes)",
        ],
    );
}

#[test]
fn non_corestore_direct_persistence_paths_are_scratch_or_operator_exports() {
    let storage = workspace_file("anvil-core/src/storage.rs");
    let object_manager = workspace_file("anvil-core/src/object_manager.rs");
    let snapshot_builder = workspace_file("anvil-core/src/personaldb_snapshot_builder.rs");
    let snapshot_store = workspace_file("anvil-core/src/personaldb_snapshot_store.rs");
    let system_realm = workspace_file("anvil-core/src/system_realm.rs");
    let worker = workspace_file("anvil-core/src/worker.rs");
    let node_identity = workspace_file("anvil-core/src/node_identity.rs");

    assert_contains_all(
        "upload staging is Class C scratch",
        &storage,
        &[
            "staged_upload_scratch_path",
            "Class C scratch: callers must route durable bytes into CoreStore",
            ".join(CORESTORE_STAGING_DIR)",
            ".join(CORESTORE_TMP_DIR)",
        ],
    );
    assert_contains_all(
        "object manager routes object bytes to CoreStore",
        &object_manager,
        &[
            "write_logical_file_path_with_locator(WriteLogicalFilePathRequest",
            "put_blob_with_storage_class",
            "remove_temp_payload",
            "remove_temp_multipart_part",
        ],
    );
    assert_contains_none(
        "object manager direct durable filesystem writes",
        &object_manager,
        &[
            "File::create(",
            "OpenOptions::new()",
            "tokio::fs::write(",
            "std::fs::write(",
        ],
    );
    assert_contains_all(
        "PersonalDB snapshot builder uses scratch then CoreStore",
        &snapshot_builder,
        &[
            "NamedTempFile::new_in(storage.temp_dir_path())",
            "restore_snapshot_database_scratch",
            "write_personaldb_snapshot(",
        ],
    );
    assert_contains_all(
        "PersonalDB snapshots persist through CoreMeta locators",
        &snapshot_store,
        &[
            "write_personaldb_bytes_as_data_locator(",
            "read_personaldb_data_locator_bytes",
        ],
    );
    assert_contains_none(
        "PersonalDB snapshot store direct filesystem writes",
        &snapshot_store,
        &[
            "tokio::fs::write",
            "std::fs::write",
            "File::create",
            "OpenOptions",
        ],
    );
    assert_contains_all(
        "bootstrap credential is operator output outside storage",
        &system_realm,
        &[
            "reject_bootstrap_credential_output_path(config, output_path)?",
            "ensure_operator_path_outside_storage",
            "Operator export only",
        ],
    );
    assert_contains_all(
        "HF ingestion uses local cache then object upload",
        &worker,
        &[
            "tempfile::tempdir()?",
            "model files are durable after ObjectManager uploads to CoreStore",
            ".put_object(",
        ],
    );
    assert_contains_all(
        "node identity writes through CoreStore CoreMeta commit path",
        &node_identity,
        &[
            "CoreStore::new(storage.clone())",
            "commit_coremeta_root_groups(",
        ],
    );
    assert_contains_none(
        "node identity direct committed write bypass",
        &node_identity,
        &["write_local_committed_batch", "meta.put("],
    );
}

#[test]
fn object_current_heads_and_version_rows_use_separate_coremeta_column_families() {
    let object_metadata = format!(
        "{}\n{}\n{}",
        workspace_file("anvil-core/src/core_store/local_object_metadata.rs"),
        workspace_file("anvil-core/src/core_store/local_object_metadata/projections.rs"),
        workspace_file("anvil-core/src/core_store/local_object_metadata/mutation.rs"),
    );
    let meta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );

    assert_contains_all(
        "object head/version CoreMeta split",
        &(object_metadata + "\n" + &meta),
        &[
            "CF_OBJECT_HEADS",
            "TABLE_OBJECT_HEAD_ROW",
            "CF_OBJECT_VERSIONS",
            "TABLE_OBJECT_VERSION_META_ROW",
            "pub(super) fn object_current_key(",
            "b\"object-current\"",
            "pub(super) fn object_version_key(",
            "b\"object-version\"",
        ],
    );
}

#[test]
fn boundary_values_are_indexed_in_coremeta_boundary_rows() {
    let object_manager = workspace_file("anvil-core/src/object_manager.rs");
    let object_metadata = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/local_object_metadata.rs"),
        workspace_file("anvil-core/src/core_store/local_object_metadata/mutation.rs"),
    );
    let corestore = workspace_file("anvil-core/src/core_store/local_stream_control.rs");
    let proto = workspace_file("anvil-core/src/core_store/control_record_proto.rs");

    assert_contains_all(
        "object boundary value projection",
        &object_manager,
        &["object_write_boundary_values", "boundary_schema_bucket_key"],
    );
    assert_contains_all(
        "object metadata boundary projection",
        &object_metadata,
        &[
            "object_data_target_from_shard_map",
            "prepare_object_boundary_value_projections",
            "self.read_logical_file_manifest(locator).await?",
            "ancillary_boundary_value_key",
            "encode_boundary_value_row",
            "OwnedCoreMetaBatchOp::Put",
            "table_id: TABLE_BOUNDARY_VALUE_ROW",
        ],
    );
    assert_contains_all(
        "boundary value CoreMeta row",
        &corestore,
        &[
            "TABLE_BOUNDARY_VALUE_ROW",
            "boundary_value_coremeta_key",
            "encode_boundary_value_row",
        ],
    );
    assert_contains_all(
        "boundary value deterministic row encoding",
        &proto,
        &[
            "BoundaryValueRowProto",
            "anvil.core.boundary_value_row.v1",
            "ensure_det(&proto, bytes, \"boundary value row\")",
        ],
    );
}

#[test]
fn index_writer_internal_tables_use_binary_or_protobuf_records() {
    let source = format!(
        "{}\n{}\n{}",
        workspace_file("anvil-core/src/index_builder.rs"),
        workspace_file("anvil-core/src/typed_field_segment.rs"),
        workspace_file("anvil-core/src/full_text_segment.rs"),
    );

    assert_contains_all(
        "index internal binary/protobuf records",
        &source,
        &[
            "FullTextDocumentTableProto",
            "IndexDefinitionDigestProto",
            "StoredFieldsProto",
            "encode_stored_fields(&stored)",
            "ensure_deterministic_proto(&proto, bytes, \"typed field stored fields\")",
            "typed-row-binary-v1",
        ],
    );
    assert_contains_none(
        "index internal JSON durable tables",
        &source,
        &[
            "serde_json::to_vec(&owned_documents",
            "serde_json::to_vec(&stored)",
            "serde_json::from_slice(&stored_json)",
            "build_policy.to_string().as_bytes()",
        ],
    );
}

#[test]
fn vector_definition_and_provenance_hashes_are_canonical_protobuf() {
    let source = workspace_file("anvil-core/src/formats/vector.rs");

    assert_contains_all(
        "vector definition canonical hashes",
        &source,
        &[
            "VectorDefinitionProto",
            "VectorDefinitionFragmentProto",
            "VectorProvenanceProto",
            "vector_definition_hash(",
            "encode_proto(&proto)?",
            "canonical_json_bytes",
        ],
    );
    assert_contains_none(
        "vector definition ad-hoc JSON hashes",
        &source,
        &[
            "serde_json::to_vec(value)",
            "serde_json::Value::Object(provenance)",
            "provenance.insert(",
        ],
    );
}

#[test]
fn large_stream_records_are_index_rows_with_locators() {
    let source = workspace_file("anvil-core/src/core_store/local_roots_layout.rs");

    assert_contains_all(
        "stream large payload locator guard",
        &source,
        &[
            "CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES",
            "write_stream_record_payload(record).await?",
            "StoredStreamRecordIndexRow::new(record, None, Some(payload_locator))",
            "payload_locator",
        ],
    );
    assert_contains_none(
        "stream large payload inline shortcuts",
        &source,
        &["StoredStreamRecordIndexRow::new(record, Some(inline_payload_b64), Some("],
    );
}

#[test]
fn personaldb_control_envelope_and_certificate_records_are_not_json() {
    let control = workspace_file("anvil-core/src/personaldb_control.rs");
    let envelope = workspace_file("anvil-core/src/personaldb_envelope.rs");
    let submit = workspace_file("anvil-core/src/personaldb_submit.rs");
    let commit_store = workspace_file("anvil-core/src/personaldb_commit_store.rs");
    let catchup = workspace_file("anvil-core/src/personaldb_catchup.rs");
    let repair = workspace_file("anvil-core/src/personaldb_repair.rs");
    let format = workspace_file("anvil-core/src/formats/personaldb.rs");

    assert_contains_all(
        "personaldb control hashes use deterministic protobuf",
        &control,
        &[
            "PersonalDbGroupManifestHashProto",
            "PersonalDbSnapshotManifestHashProto",
            "PersonalDbCommitCertificateHashProto",
            "encode_deterministic_proto(",
            "commit_certificate_hash_proto(",
        ],
    );
    assert_contains_none(
        "personaldb control JSON hashes",
        &control,
        &[
            "serde_json::to_vec(&unsigned)",
            "serde_json::to_vec_pretty(&unsigned)",
            "canonical_json_bytes(&unsigned)",
        ],
    );

    assert_contains_all(
        "personaldb envelope hash uses deterministic protobuf",
        &envelope,
        &[
            "VerifiedMutationEnvelopeHashProto",
            "TableEffectHashProto",
            "RowMetadataDeltaHashProto",
            "encode_deterministic_proto(",
            "envelope_hash_proto(&unsigned)",
        ],
    );
    assert_contains_none(
        "personaldb envelope JSON hashes",
        &envelope,
        &[
            "serde_json::to_vec(&unsigned)",
            "serde_json::to_vec_pretty(&unsigned)",
            "canonical_json_bytes(&unsigned)",
        ],
    );

    assert_contains_all(
        "personaldb submit hashes voter acks as protobuf",
        &submit,
        &[
            "PersonalDbVoterAckHashSetProto",
            "PersonalDbVoterAckHashProto",
            "encode_deterministic_proto(&voter_acks_hash_proto(",
        ],
    );
    assert_contains_none(
        "personaldb submit JSON hash inputs",
        &submit,
        &["serde_json::to_vec(&request.voter_acks)"],
    );

    assert_contains_all(
        "personaldb commit certificates are deterministic protobuf bytes",
        &commit_store,
        &[
            "PersonalDbCommitCertificateProto",
            "encode_commit_certificate(certificate)",
            "decode_commit_certificate(&bytes)",
            "encode_core_object_ref_target(object_ref)",
            "decode_core_object_ref_target(target)",
        ],
    );
    assert_contains_none(
        "personaldb commit certificate JSON persistence",
        &commit_store,
        &[
            "serde_json::to_vec_pretty(certificate)",
            "serde_json::from_slice(&bytes)?",
            "CORE_OBJECT_REF_TARGET_PREFIX",
            "URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)",
        ],
    );

    assert_contains_all(
        "personaldb log and catchup use certificate bytes",
        &format,
        &["inline_certificate_bytes", "certificate_hash"],
    );
    assert_contains_all(
        "personaldb catchup decodes certificate bytes mechanically",
        &catchup,
        &[
            "record.inline_certificate_bytes.clone()",
            "encode_commit_certificate(&certificate)?",
            "decode_commit_certificate(&certificate_bytes)?",
        ],
    );
    assert_contains_all(
        "personaldb repair decodes inline certificate bytes mechanically",
        &repair,
        &[
            "decode_commit_certificate(&record.inline_certificate_bytes)",
            "PersonalDbLogChainRepairReason::InvalidCommitCertificate",
        ],
    );

    let combined = format!("{format}\n{catchup}\n{repair}");
    assert_contains_none(
        "personaldb log/catchup/repair JSON certificate paths",
        &combined,
        &[
            "inline_certificate_json",
            "certificate_json",
            "serde_json::from_slice::<PersonalDbCommitCertificate>",
        ],
    );
}

#[test]
fn coremeta_rocksdb_plane_defines_rfc_storage_model_tables_and_caps() {
    let meta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );
    let core_store_mod = workspace_file("anvil-core/src/core_store/mod.rs");
    let storage = workspace_file("anvil-core/src/storage.rs");

    assert_contains_all(
        "CoreMeta RFC 0007 table registry",
        &meta,
        &[
            "pub const CF_META_VERSION",
            "pub const CF_ROOT_CACHE",
            "pub const CF_TRANSACTIONS",
            "pub const CF_OBJECT_VERSIONS",
            "pub const CF_INLINE_PAYLOADS",
            "pub const CF_STREAM_HEADS",
            "pub const CF_STREAM_RECORDS",
            "pub const CF_INDEX_DEFS",
            "pub const CF_INDEX_ROWS",
            "pub const CF_BOUNDARY",
            "pub const CF_AUTHZ",
            "pub const CF_PERSONALDB",
            "pub const CF_REGISTRY",
            "pub const CF_MESH",
            "pub const CF_LEASES_FENCES",
            "pub const CF_MATERIALISATION",
            "pub const CF_REFCOUNTS",
            "pub const CF_OBSERVABILITY",
            "TABLE_META_SCHEMA_VERSION_ROW: u16 = 0x8001",
            "TABLE_PENDING_MUTATION_ROW: u16 = 0x8007",
            "TABLE_INLINE_PAYLOAD_ROW: u16 = 0x8103",
            "TABLE_MANIFEST_CAS_CURRENT_ROW: u16 = 0x8104",
            "TABLE_MULTIPART_UPLOAD_CURRENT_ROW: u16 = 0x8105",
            "TABLE_MULTIPART_PART_CURRENT_ROW: u16 = 0x8106",
            "TABLE_GATEWAY_METADATA_ROW: u16 = 0x8703",
            "TABLE_TASK_CURRENT_ROW: u16 = 0x8904",
            "CORE_META_MAX_VALUE_BYTES: usize = 64 * 1024",
            "CORE_META_MAX_INLINE_PAYLOAD_BYTES: usize = 32 * 1024",
            "CoreMetaValueEnvelope",
            "CoreMetaRowCommonProto",
            "CoreMetaLocatorProto",
        ],
    );
    assert_contains_all(
        "CoreMeta public row exports",
        &core_store_mod,
        &[
            "CF_META_VERSION",
            "CF_OBJECT_HEADS",
            "CoreMetaRecord",
            "CoreMetaStore",
            "TABLE_MANIFEST_CAS_CURRENT_ROW",
            "TABLE_MULTIPART_UPLOAD_CURRENT_ROW",
            "TABLE_MULTIPART_PART_CURRENT_ROW",
            "TABLE_GATEWAY_METADATA_ROW",
            "TABLE_TASK_CURRENT_ROW",
        ],
    );
    assert_contains_all(
        "CoreStore storage roots",
        &storage,
        &[
            "core_store_meta_path",
            "join(\"meta\")",
            "join(\"rocksdb\")",
            "core_store_blocks_path",
            "core_store_landed_bytes_path",
        ],
    );
    assert_contains_none(
        "CoreMeta forbidden metadata stores",
        &meta,
        &[
            "redb::",
            "sled::",
            "sqlite",
            "corestore/transactions/*.json",
            "corestore/manifests/*.json",
        ],
    );
}

#[test]
fn coremeta_tuple_keys_are_typed_and_prefix_scans_decode_tuple_parts() {
    let meta = format!(
        "{}\n{}\n{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/key.rs"),
        workspace_file("anvil-core/src/core_store/meta/scans.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );
    let helpers = workspace_file("anvil-core/src/core_store/local_key_helpers.rs");

    assert_contains_all(
        "CoreMeta typed tuple key implementation",
        &(meta.clone() + &helpers),
        &[
            "pub enum CoreMetaTuplePart",
            "pub fn core_meta_tuple_key(",
            "encode_tuple_part(&mut key, *part)?",
            "fn encode_escaped_value(",
            "fn decode_escaped_value(",
            "validate_core_meta_tuple_key(tuple_key)?",
            "CoreMetaTuplePart::Raw(part)",
            "core_meta_tuple_key(&parts)",
            "exclusive_prefix_successor(&prefix)",
            "set_iterate_lower_bound(start_key.clone())",
            "set_iterate_upper_bound(upper_bound)",
            "decode_core_meta_tuple_key(&key)?",
        ],
    );
    assert_contains_none(
        "removed length-delimited CoreMeta tuple layout",
        &meta,
        &[
            "CoreMetaTupleKey missing part_count",
            "CoreMetaTupleKey part has unsupported flags",
            "part_count.to_le_bytes()",
            "push_meta_tuple_part",
        ],
    );
}

#[test]
fn coremeta_inline_payloads_are_explicit_rows_not_large_metadata_values() {
    let meta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );

    assert_contains_all(
        "CoreMeta inline payload guard",
        &meta,
        &[
            "InlinePayloadRowProto",
            "raw_payload.len() > CORE_META_MAX_INLINE_PAYLOAD_BYTES",
            "raw_payload_length: raw_payload.len() as u64",
            "payload_bytes: raw_payload.to_vec()",
            "decode_inline_payload_row",
            "RocksDB compression",
        ],
    );
    assert_contains_none(
        "CoreMeta inline payload forbidden shortcuts",
        &meta,
        &[
            "put_inline_payload(&self, tuple_key: &[u8], raw_payload: &[u8]) -> Result<()> {\n        self.put(",
            "CORE_META_MAX_INLINE_PAYLOAD_BYTES + 1;\n        store.put_inline_payload",
        ],
    );
}

#[test]
fn explicit_transaction_rows_store_large_payloads_as_corestore_locators() {
    let rows = workspace_file("anvil-core/src/core_store/local_tx_rows.rs");

    assert_contains_all(
        "transaction staged payload locator guard",
        &rows,
        &[
            "CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES",
            "payload.len() <= CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES",
            "write_logical_file_with_locator(WriteLogicalFileRequest",
            "CoreTransactionPayloadRef::Locator",
            "core_meta_locator_from_manifest_locator(&write.locator)?",
            "read_transaction_payload_ref",
            "core_meta_locator_to_manifest_locator(&locator)?",
        ],
    );
    assert_contains_none(
        "transaction staged payload forbidden large inline shortcut",
        &rows,
        &[
            "if payload.len() <= CORE_META_MAX_VALUE_BYTES",
            "payload.len() <= CORE_META_MAX_INLINE_PAYLOAD_BYTES",
        ],
    );
}

#[test]
fn corestore_admission_uses_pending_rows_and_materialisation_rows_not_transaction_table_overload() {
    let admission = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/local_key_helpers.rs"),
        workspace_file("anvil-core/src/core_store/local_admission.rs"),
        workspace_file("anvil-core/src/core_store/local_admission/publication.rs"),
        workspace_file("anvil-core/src/core_store/local_admission/point_state.rs"),
        workspace_file("anvil-core/src/core_store/local_admission/finalisation.rs"),
    );

    assert_contains_all(
        "CoreStore admission table split",
        &admission,
        &[
            "TABLE_PENDING_MUTATION_ROW",
            "TABLE_MATERIALISATION_CURSOR_ROW",
            "CF_MATERIALISATION",
            "pub(super) fn admission_record_key(",
            "admission_record_key(admission_shard_hash, sequence)",
            "pub(super) fn admission_sequence_key(",
            "admission_sequence_key(admission_shard_hash)",
            "pub(super) fn admission_finalisation_key(",
            "admission_finalisation_key(key)",
        ],
    );
    assert_contains_none(
        "CoreStore admission table overload",
        &admission,
        &[
            "table_id: TABLE_TRANSACTION_ROW,\n                tuple_key: &admission_record_key",
            "CF_TRANSACTIONS,\n                TABLE_TRANSACTION_ROW,\n                &admission_sequence_key",
            "CF_TRANSACTIONS, TABLE_TRANSACTION_ROW, &index_key",
        ],
    );
}

#[test]
fn coremeta_common_root_identity_is_validated_and_hash_based() {
    let meta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );
    assert_contains_all(
        "CoreMeta common validation",
        &meta,
        &[
            "validate_coremeta_common_shape(&common)?",
            "CoreMeta row common visibility state must be specified",
            "validate_coremeta_hash(&common.root_key_hash, \"CoreMeta row common root key hash\")?",
            "CoreMeta row common rooted rows must use a non-zero root generation",
            "validate_coremeta_logical_id(&common.transaction_id",
        ],
    );

    let index_coremeta = workspace_file("anvil-core/src/index_coremeta.rs");
    assert_contains_all(
        "index CoreMeta root hashes",
        &index_coremeta,
        &[
            "core_meta_root_key_hash",
            "index_segment_root_key_hash(&record.index_id)",
            "index_definition_root_key_hash(record.tenant_id, record.bucket_id)",
        ],
    );
    assert_contains_none(
        "index CoreMeta non-hash roots",
        &index_coremeta,
        &[
            "root_key_hash: record.index_id.clone()",
            "root_key_hash: index_definition_root_key(",
        ],
    );
}

#[test]
fn corestore_source_has_no_custom_metadata_wal_or_sidecar_control_store() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .join("anvil-core/src/core_store");
    let mut files = Vec::new();
    collect_rs_files(&root, &mut files);
    let forbidden = [
        "WalFrame",
        "ANWAL",
        "ANVJRN",
        "core_wal",
        "custom metadata WAL",
        "corestore/transactions/*.json",
        "corestore/manifests/*.json",
        "core_store_root_register_path",
        "write_root_anchor_register_file",
        "read_root_anchor_generation_from_register",
        "redb::",
        "sled::",
    ];
    let mut violations = Vec::new();
    for file in files {
        let source =
            fs::read_to_string(&file).unwrap_or_else(|err| panic!("read {:?}: {err}", file));
        if source.contains("#[cfg(test)]") && file.to_string_lossy().contains("local_tests") {
            continue;
        }
        for term in forbidden {
            if source.contains(term) {
                violations.push(format!("{} contains {term}", file.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "CoreStore durable metadata must use RocksDB/CoreMeta, not a custom WAL or sidecar control store: {violations:#?}"
    );
}

#[test]
fn feature_metadata_current_state_uses_coremeta_rows_not_legacy_refs() {
    let index_coremeta = workspace_file("anvil-core/src/index_coremeta.rs");
    let index_journal = workspace_file("anvil-core/src/index_journal.rs");
    let authz_journal = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/authz_journal.rs"),
        workspace_file("anvil-core/src/authz_journal/projection.rs")
    );
    let authz_schema = workspace_file("anvil-core/src/authz_schema.rs");
    let authz_realm = workspace_file("anvil-core/src/authz_realm_schema.rs");
    let authz_payload = workspace_file("anvil-core/src/authz_coremeta_payload.rs");
    let authz_userset = workspace_file("anvil-core/src/authz_userset_index.rs");
    let diagnostics = workspace_file("anvil-core/src/diagnostic_store.rs");
    let repair = workspace_file("anvil-core/src/repair_finding.rs");

    assert_contains_all(
        "index current CoreMeta rows",
        &(index_coremeta.clone() + &index_journal),
        &[
            "TABLE_INDEX_DEFINITION_ROW",
            "IndexDefinitionCurrentCoreMetaRecord",
            "IndexDefinitionStateCoreMetaRecord",
            "write_index_current_coremeta_rows",
        ],
    );
    assert_contains_none("index current non-CoreMeta path", &index_journal, &[]);

    assert_contains_all(
        "authz schema CoreMeta rows",
        &(authz_schema.clone() + &authz_realm),
        &[
            "TABLE_AUTHZ_SCHEMA_ROW",
            "namespace_schema_tuple_key",
            "schema_revision_tuple_key",
            "schema_binding_tuple_key",
            "CoreStore::new(storage.clone()).await?",
            "read_coremeta_row(",
            "commit_coremeta_batch_for_storage(",
            "encode_authz_payload_row(",
            "decode_authz_payload_row(",
        ],
    );
    assert_contains_all(
        "authz schema large payload inline-or-locator path",
        &authz_payload,
        &[
            "CoreMetaInlineOrLocatorProto",
            "CORE_META_MAX_INLINE_PAYLOAD_BYTES",
            "WriterFamily::Authz.as_str().to_string()",
            "write_logical_file_with_locator(WriteLogicalFileRequest",
            "core_meta_locator_from_manifest_locator",
            "core_meta_locator_to_manifest_locator",
            "read_logical_range(ReadLogicalRangeRequest",
        ],
    );
    assert_contains_none(
        "authz schema non-CoreMeta path",
        &(authz_schema + &authz_realm),
        &[],
    );

    assert_contains_all(
        "authz userset CoreMeta row plus bounded format blob",
        &authz_userset,
        &[
            "TABLE_AUTHZ_TUPLE_PAGE_ROW",
            "derived_userset_index_tuple_key",
            "put_format_blob(",
            "WriterFamily::Authz",
            "core_object_ref_target",
        ],
    );
    assert_contains_all(
        "authz tuple current rows use inline-or-locator payload wrapper",
        &authz_journal,
        &[
            "AUTHZ_TUPLE_CURRENT_PAYLOAD_KIND",
            "encode_authz_payload_row(",
            "decode_authz_payload_row(",
            "TABLE_AUTHZ_TUPLE_OBJECT_CURRENT_ROW",
            "TABLE_AUTHZ_TUPLE_SUBJECT_CURRENT_ROW",
        ],
    );
    assert_contains_none(
        "authz userset legacy current ref mirror",
        &authz_userset,
        &[],
    );

    assert_contains_all(
        "diagnostic and repair CoreMeta rows",
        &(diagnostics.clone() + &repair),
        &[
            "TABLE_DIAGNOSTIC_ROW",
            "TABLE_REPAIR_FINDING_ROW",
            "diagnostic_tuple_key",
            "repair_finding_tuple_key",
        ],
    );
    assert_contains_none(
        "diagnostic and repair legacy object ref mirrors",
        &(diagnostics + &repair),
        &[],
    );
}

#[test]
fn low_level_byte_pipeline_uses_landed_bytes_before_final_shards() {
    let blob = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/local_init_blob.rs"),
        workspace_file("anvil-core/src/core_store/local_block_distribution.rs")
    );
    let roots = workspace_file("anvil-core/src/core_store/local_roots_layout.rs");

    assert_contains_all(
        "CoreStore landed-to-shard pipeline",
        &blob,
        &[
            "CorePendingMutationPayload::Landed(&input.bytes)",
            "let materialised_bytes = self.read_landed_bytes(&landed).await?",
            "encode_erasure_shards(materialised_bytes, profile)",
            "write_file_atomic(&shard_path, &shard_file).await?",
            "read_block_shard_file(",
        ],
    );
    assert_contains_none(
        "CoreStore direct logical bytes bypass",
        &roots,
        &["materialise_object_blob_bytes("],
    );
}

#[test]
fn bounded_small_payloads_use_coremeta_inline_payload_rows() {
    let meta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );
    let blob = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/local_init_blob.rs"),
        workspace_file("anvil-core/src/core_store/local_block_distribution.rs")
    );
    let keys = workspace_file("anvil-core/src/core_store/local_key_helpers.rs");

    assert_contains_all(
        "CoreMeta inline payload column family",
        &meta,
        &[
            "CF_INLINE_PAYLOADS",
            "TABLE_INLINE_PAYLOAD_ROW",
            "CORE_META_MAX_INLINE_PAYLOAD_BYTES",
            "InlinePayloadRowProto",
            "pub fn put_inline_payload",
            "pub fn get_inline_payload",
            "raw_payload_length",
        ],
    );
    assert_contains_all(
        "inline object body path",
        &blob,
        &[
            "put_inline_blob",
            "LOCAL_INLINE_PAYLOAD_PROFILE_ID",
            "CORE_META_MAX_INLINE_PAYLOAD_BYTES",
            "put_inline_payload",
            "read_inline_blob",
            "byte_pipeline.inline_payload",
        ],
    );
    assert_contains_all(
        "inline payload key and discriminator",
        &keys,
        &[
            "inline_payload_meta_key",
            "is_inline_object_ref",
            "LOCAL_INLINE_PAYLOAD_BLOCK_PREFIX",
        ],
    );
}

#[test]
fn low_level_manifests_and_shard_receipts_carry_boundary_summaries() {
    let types = workspace_file("anvil-core/src/core_store/types.rs");
    let block_shard = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/block_shard.rs"),
        workspace_file("anvil-core/src/core_store/local_init_blob.rs")
    );
    let manifest_proto = workspace_file("anvil-core/src/core_store/manifest_proto.rs");
    let transaction_proto =
        workspace_file("anvil-core/src/core_store/transaction_manifest_proto.rs");
    let root_proto = workspace_file("anvil-core/src/core_store/root_proto.rs");

    assert_contains_all(
        "boundary fields on manifest locator and shard summaries",
        &types,
        &[
            "pub boundary_summary_hash: String",
            "pub boundary_values_b64: String",
            "pub struct CoreShardReceiptSummary",
            "pub struct CoreLogicalShardRef",
        ],
    );
    assert_contains_all(
        "block shard boundary validation",
        &block_shard,
        &[
            "validate_boundary_summary_fields",
            "boundary_summary_hash(boundary_values)",
            "encode_boundary_values_b64",
            "ShardReceiptPayloadProto",
            "boundary_summary_hash: input.boundary_summary_hash.to_string()",
        ],
    );
    for (label, source) in [
        ("logical manifest proto", manifest_proto.as_str()),
        ("transaction manifest proto", transaction_proto.as_str()),
        ("root manifest proto", root_proto.as_str()),
    ] {
        assert_contains_all(
            label,
            source,
            &[
                "boundary_summary_hash: String",
                "boundary_values_b64: String",
                "boundary_summary_hash: value.boundary_summary_hash.clone()",
                "boundary_values_b64: value.boundary_values_b64.clone()",
            ],
        );
    }
}

#[test]
fn object_upload_scratch_files_are_not_the_durability_boundary() {
    let storage = workspace_file("anvil-core/src/storage.rs");

    assert_contains_all(
        "scratch upload path remains non-authoritative",
        &storage,
        &[
            "stream_to_temp_file",
            "non-authoritative scratch",
            "file.flush().await?",
            "\"temp_file_flush\"",
            "\"stream_to_temp_file finished\"",
        ],
    );
    assert_contains_none(
        "scratch upload must not fsync before CoreStore ingestion",
        &storage,
        &["\"temp_file_sync_all\"", "file.sync_all().await?"],
    );
}

#[test]
fn low_level_observability_names_cover_admission_pipeline_and_shards() {
    let io = workspace_file("anvil-core/src/core_store/local_io.rs");
    let admission = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/local_admission.rs"),
        workspace_file("anvil-core/src/core_store/local_admission/publication.rs"),
    );
    let perf = workspace_file("anvil-core/src/perf.rs");
    let blob = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/local_init_blob.rs"),
        workspace_file("anvil-core/src/core_store/local_block_distribution.rs")
    );

    assert_contains_all(
        "RFC 0007 low-level metric names",
        &(io.clone() + &admission + &blob + &perf),
        &[
            "anvil_admission_duration_ms",
            "anvil_landed_bytes_duration_ms",
            "anvil_rocksdb_write_batch_duration_ms",
            "anvil_byte_pipeline_stage_duration_ms",
            "anvil_block_write_duration_ms",
            "anvil_block_read_duration_ms",
            "admission.landed_fsync",
            "byte_pipeline.erasure_encode",
            "block.shard_write",
            "block.shard_fsync",
            "manifest.publish",
        ],
    );
}

#[test]
fn core_model_does_not_use_global_bucket_name_lookup_for_public_reads() {
    let object_reads = workspace_file("anvil-core/src/object_manager/read.rs");
    let bucket_journal = workspace_file("anvil-core/src/bucket_journal.rs");
    let tenancy = workspace_file("anvil-core/src/persistence/tenancy.rs");

    assert_contains_all(
        "tenant routed bucket reads",
        &object_reads,
        &[
            "Bucket reads require authenticated tenant claims or an explicit tenant route",
            "let tenant_id = route_tenant_id.or_else(|| claims.map(|claims| claims.tenant_id));",
            "let tenant_id = tenant_id.ok_or_else",
            "bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)",
        ],
    );
    assert_contains_none(
        "S3-era global bucket name read path",
        &(object_reads + &bucket_journal + &tenancy),
        &[
            "read_public_bucket_by_name",
            "read_current_bucket_by_name",
            "get_public_bucket_by_name",
            "set_bucket_public_access_by_name",
            "global_bucket_name_current_ref",
            "bucket_metadata_current/global/name/",
        ],
    );
}

#[test]
fn gateway_metadata_uses_coremeta_rows_not_legacy_refs() {
    let gateway = workspace_file("anvil-core/src/gateway_store.rs");
    let gateway_coremeta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/gateway_store/coremeta.rs"),
        workspace_file("anvil-core/src/gateway_store/metadata_rows.rs")
    );
    let meta = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/core_store/meta.rs"),
        workspace_file("anvil-core/src/core_store/meta/payload_validation.rs"),
    );
    let core_store_mod = workspace_file("anvil-core/src/core_store/mod.rs");

    assert_contains_all(
        "gateway CoreMeta metadata path",
        &(gateway.clone() + &gateway_coremeta + &meta + &core_store_mod),
        &[
            "CF_REGISTRY",
            "TABLE_GATEWAY_METADATA_ROW",
            "encode_gateway_metadata_row",
            "read_record_row",
            "put_record_row",
            "list_record_rows",
        ],
    );
    assert_contains_none("gateway legacy ref metadata path", &gateway, &[]);
}

#[test]
fn task_current_state_uses_coremeta_rows_not_legacy_refs() {
    let task = format!(
        "{}\n{}\n{}\n{}",
        workspace_file("anvil-core/src/task_journal.rs"),
        workspace_file("anvil-core/src/task_journal/model.rs"),
        workspace_file("anvil-core/src/task_journal/queue.rs"),
        workspace_file("anvil-core/src/task_journal/store.rs"),
    );

    assert_contains_all(
        "task current CoreMeta row path",
        &task,
        &[
            "TaskQueueRowProto",
            "TaskQueueRow",
            "TABLE_TASK_CURRENT_ROW",
            "CoreMutationPrecondition::CoreMetaRow",
            "scan_coremeta_prefix_page(",
            "CoreMutationBatch",
            "CoreMutationOperation::CoreMetaPut",
            "core_meta_payload_digest(TABLE_TASK_CURRENT_ROW, payload)",
        ],
    );
    assert_contains_none("task current legacy ref path", &task, &[]);
}

#[test]
fn metadata_transaction_projection_uses_exposed_stream_record_decoder() {
    let helpers = workspace_file("anvil-core/src/metadata_journal/helpers.rs");
    let projection = workspace_file("anvil-core/src/metadata_journal/transaction_projection.rs");

    assert_contains_all(
        "metadata transaction projection stream decoder",
        &(helpers + &projection),
        &[
            "pub(super) fn metadata_record_from_stream_record",
            "metadata_record_from_stream_record(record)?",
            "materialize_committed_object_metadata_transaction",
        ],
    );
}

#[test]
fn manifest_transactions_project_committed_current_rows() {
    let manifest = workspace_file("anvil-core/src/manifest_journal.rs");
    let service = workspace_file("anvil-core/src/services/transaction.rs");

    assert_contains_all(
        "manifest transaction projection",
        &(manifest + &service),
        &[
            "pub async fn materialize_committed_manifest_cas_transaction",
            "parse_manifest_cas_stream_id(stream_id)",
            "preconditions.push(current_update.precondition.clone())",
            "CoreMutationOperation::StreamAppend {",
            "CoreMutationOperation::CoreMetaPut {",
            "table_id: TABLE_MANIFEST_CAS_CURRENT_ROW",
            "manifest_current_row_key(",
            "read_manifest_current_row(&core_store",
            "committed without its current projection",
            "manifest_journal::materialize_committed_manifest_cas_transaction",
        ],
    );
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|err| panic!("read dir {:?}: {err}", dir)) {
        let entry = entry.expect("read directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn boundary_schemas_are_public_api_and_coremeta_rows_not_refs() {
    let core_proto = workspace_file("anvil-core/proto/anvil.proto");
    let rust_proto = workspace_file("clients/rust/proto/anvil.proto");
    let public_boundary_api = [
        "rpc PutBoundarySchema(PutBoundarySchemaRequest) returns (BoundarySchemaResponse);",
        "rpc GetBoundarySchema(GetBoundarySchemaRequest) returns (BoundarySchemaResponse);",
        "message BoundaryDimension",
        "message BoundarySource",
        "message BoundarySchemaRecord",
        "optional uint64 expected_generation = 2;",
    ];
    assert_contains_all(
        "boundary schema server API",
        &core_proto,
        &public_boundary_api,
    );
    assert_contains_all(
        "boundary schema Rust client API",
        &rust_proto,
        &public_boundary_api,
    );

    let stream_control = workspace_file("anvil-core/src/core_store/local_stream_control.rs");
    assert_contains_all(
        "boundary schema CoreMeta storage",
        &stream_control,
        &[
            "CF_BOUNDARY",
            "TABLE_BOUNDARY_SCHEMA_ROW",
            "TABLE_BOUNDARY_SCHEMA_CURRENT_ROW",
            "boundary_schema_coremeta_key",
            "boundary_schema_current_coremeta_key",
            "read_coremeta_row(",
            "CoreMetaBatchOpKind::Put(&bytes)",
            "commit_coremeta_root_groups(",
            "validate_boundary_schema(&schema, current_schema.as_ref(), input.expected_generation)",
        ],
    );
    assert_contains_none(
        "boundary schema ref bypass",
        &stream_control,
        &[
            "boundary_schema_ref_name",
            "decode_core_object_ref_target(&ref_value.target)",
            "write_control_logical_file_ref(\n                \"core_control\"",
        ],
    );

    let object_rpc = format!(
        "{}\n{}",
        workspace_file("anvil-core/src/services/object/rpc.rs"),
        workspace_file("anvil-core/src/services/object/boundary_rpc.rs"),
    );
    assert_contains_all(
        "boundary schema service implementation",
        &object_rpc,
        &[
            "async fn put_boundary_schema(",
            "async fn get_boundary_schema(",
            "require_bucket_scope(state, &claims, &req.bucket_name, AnvilAction::BucketWrite)",
            "require_bucket_scope(state, &claims, &req.bucket_name, AnvilAction::BucketRead)",
            "core_store.put_boundary_schema",
            "core_store.read_boundary_schema",
            "boundary_schema_bucket_key(claims.tenant_id, &bucket.name)",
        ],
    );

    let object_manager = workspace_file("anvil-core/src/object_manager.rs");
    assert_contains_all(
        "boundary schema lookup is scoped by Anvil storage tenant",
        &object_manager,
        &[
            "object_write_boundary_values_from_file(
        &self,
        tenant_id: i64",
            "boundary_schema_bucket_key(tenant_id, bucket_name)",
        ],
    );
}
