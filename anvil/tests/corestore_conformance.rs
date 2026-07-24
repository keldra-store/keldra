#![recursion_limit = "256"]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anvil::core_store::{
    AcquireFence, AppendStreamRecord, AuthzScopeRef, CF_INLINE_PAYLOADS,
    CORE_QUORUM_PROFILE_SCHEMA, CORE_ROOT_CATALOG_SCHEMA, CoreMetaRowCommonProto, CoreMetaStore,
    CoreMetaTuplePart, CoreMetaVisibilityState, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreMutationRootPublication, CoreQuorumProfile, CoreRootCatalog,
    CoreRootPartition, CoreStore, CoreTransactionState, GetBlob, PutBlob, ReadStream,
    SealStreamSegment, TABLE_INLINE_PAYLOAD_ROW, WatchRequest, core_meta_committed_row_common,
    core_meta_payload_digest, core_meta_tuple_key, encode_core_meta_inline_payload_row,
};
use anvil::formats::writer::WriterFamily;
use anvil::gateway_store::{
    GatewayMountMatchKind, GatewayMountRecord, GatewayMountState, put_gateway_mount_record,
    resolve_gateway_mount,
};
use anvil::storage::Storage;
use prost::Message;
use sha2::{Digest, Sha256};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf()
}

fn read_workspace_file(relative: &str) -> String {
    let path = workspace_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn workspace_relative_path(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .expect("path is under workspace")
        .to_string_lossy()
        .replace('\\', "/")
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn path_has_component_sequence(path: &Path, components: &[&str]) -> bool {
    let actual = path
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    actual
        .windows(components.len())
        .any(|window| window == components)
}

fn forbidden_final_sidecar_violations(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    files
        .into_iter()
        .filter_map(|path| {
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
            let forbidden_extension =
                matches!(extension, "json" | "jsonl" | "journal" | "manifest");
            let forbidden_suffix =
                file_name.ends_with(".sidecar") || file_name.ends_with(".wal.json");
            if forbidden_extension || forbidden_suffix {
                Some(rel)
            } else {
                None
            }
        })
        .collect()
}

fn legacy_stream_sidecar_violations(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    collect_files(root, &mut files);
    files
        .into_iter()
        .filter_map(|path| {
            let legacy_extension = format!("{}{}", "an", "stream");
            let has_legacy_extension = path
                .extension()
                .is_some_and(|extension| extension == legacy_extension.as_str());
            let has_legacy_data_dir = path_has_component_sequence(&path, &["streams", "data"]);
            if has_legacy_extension || has_legacy_data_dir {
                Some(
                    path.strip_prefix(root)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .replace('\\', "/"),
                )
            } else {
                None
            }
        })
        .collect()
}

#[derive(Clone, PartialEq, Message)]
struct TestCoreMetaPayload {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    body: Vec<u8>,
}

fn coremeta_test_payload(body_len: usize) -> Vec<u8> {
    let row = TestCoreMetaPayload {
        common: Some(CoreMetaRowCommonProto {
            realm_id: "test".to_string(),
            root_key_hash:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            root_generation: 1,
            transaction_id: "test".to_string(),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: 0,
            payload_schema_version: 1,
        }),
        schema: "anvil.core.stream_record_index.v1".to_string(),
        body: vec![0x5a; body_len],
    };
    row.encode_to_vec()
}

fn coremeta_test_tuple_key(part: &[u8]) -> Vec<u8> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Raw(part)])
        .expect("test tuple key uses canonical CoreMeta encoding")
}

fn block_shard_path(storage: &Storage, node_id: &str, block_id: &str, shard_index: u16) -> PathBuf {
    let block_path_hash = hex::encode(Sha256::digest(block_id.as_bytes()));
    storage
        .core_store_root_path()
        .join("blocks")
        .join("local-cache")
        .join("local-erasure-set")
        .join(node_id)
        .join("block-id")
        .join(&block_path_hash[0..2])
        .join(block_path_hash)
        .join(format!("shard-{shard_index:05}-{block_id}.anb"))
}

fn production_source(relative: &str) -> String {
    strip_cfg_test_modules(&read_workspace_file(relative))
}

fn production_sources(relatives: &[&str]) -> String {
    relatives
        .iter()
        .map(|relative| production_source(relative))
        .collect::<Vec<_>>()
        .join("\n")
}

fn protected_writer_source(relative: &str) -> String {
    match relative {
        "anvil-core/src/metadata_journal.rs" => production_sources(&[
            relative,
            "anvil-core/src/metadata_journal/helpers.rs",
            "anvil-core/src/metadata_journal/object_mutation.rs",
            "anvil-core/src/metadata_journal/transaction_projection.rs",
        ]),
        "anvil-core/src/task_journal.rs" => production_sources(&[
            relative,
            "anvil-core/src/task_journal/model.rs",
            "anvil-core/src/task_journal/queue.rs",
            "anvil-core/src/task_journal/store.rs",
        ]),
        _ => production_source(relative),
    }
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut out = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_test_module = false;
    let mut depth: i32 = 0;

    for line in source.lines() {
        let trimmed = line.trim_start();

        if skipping_test_module {
            depth += brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test && trimmed.starts_with("mod tests") {
            skipping_test_module = true;
            depth = brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            pending_cfg_test = false;
            continue;
        }

        if pending_cfg_test {
            out.push_str("#[cfg(test)]\n");
            pending_cfg_test = false;
        }

        out.push_str(line);
        out.push('\n');
    }

    out
}

fn brace_delta(line: &str) -> i32 {
    let opens = line.as_bytes().iter().filter(|byte| **byte == b'{').count() as i32;
    let closes = line.as_bytes().iter().filter(|byte| **byte == b'}').count() as i32;
    opens - closes
}

#[test]
fn rfc_0006_local_storage_guard_prevents_authoritative_feature_file_writes() {
    let source_root = workspace_root().join("anvil-core/src");
    let mut files = Vec::new();
    collect_rs_files(&source_root, &mut files);

    let allowed_direct_io = BTreeSet::from([
        // CoreStore owns local staging, shard, manifest, ref, watch and lock files.
        "anvil-core/src/core_store/local.rs",
        "anvil-core/src/core_store/local_admission.rs",
        "anvil-core/src/core_store/local_io.rs",
        "anvil-core/src/core_store/local_roots_layout.rs",
        // Root-register shards and crash-safe quarantine intents are CoreStore
        // recovery primitives, not feature-owned durable state.
        "anvil-core/src/core_store/local_coremeta_recovery/register_quarantine.rs",
        "anvil-core/src/core_store/local_root_register.rs",
        // External publication markers are feature-gated test control and are
        // expendable local operational state.
        "anvil-core/src/core_store/local_root_publication_test_control.rs",
        // Storage owns transient upload staging only; object payloads are moved into CoreStore.
        "anvil-core/src/storage.rs",
        // System realm bootstrap writes the first-admin credential file for the operator once;
        // system realm state itself is still committed through CoreStore-authz paths.
        "anvil-core/src/system_realm.rs",
        // Snapshot builder uses SQLite scratch files before writing snapshots through CoreStore.
        "anvil-core/src/personaldb_snapshot_builder.rs",
        // Performance tracing writes optional external telemetry files and never owns
        // authoritative Anvil state.
        "anvil-core/src/perf.rs",
        // HuggingFace target indexes are assembled in feature-writer scratch
        // before the resulting bytes enter CoreStore.
        "anvil-core/src/worker/hf_index.rs",
    ]);

    let forbidden_write_patterns = [
        "tokio::fs::write(",
        "std::fs::write(",
        "fs::write(",
        "tokio::fs::File::create(",
        "fs::File::create(",
        "File::create(",
        "OpenOptions::new(",
        "tokio::fs::rename(",
        "fs::rename(",
    ];

    let mut violations = Vec::new();
    for path in files {
        let relative = path
            .strip_prefix(workspace_root())
            .expect("source path is under workspace")
            .to_string_lossy()
            .replace('\\', "/");
        if relative.contains("/local_tests/") {
            continue;
        }
        let source = strip_cfg_test_modules(
            &fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {relative}: {error}")),
        );

        for (line_index, line) in source.lines().enumerate() {
            if forbidden_write_patterns
                .iter()
                .any(|pattern| line.contains(pattern))
                && !allowed_direct_io.contains(relative.as_str())
            {
                violations.push(format!("{}:{}: {}", relative, line_index + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "authoritative durable feature code must not write local files outside CoreStore-owned staging/cache/shard/scratch paths:\n{}",
        violations.join("\n")
    );
}

#[test]
fn rfc_0007_feature_state_uses_logical_files_not_raw_object_payload_writes() {
    let source_root = workspace_root().join("anvil-core/src");
    let mut files = Vec::new();
    collect_rs_files(&source_root, &mut files);

    let allowed_put_blob_files = BTreeSet::from([
        // CoreStore owns the raw object payload writer and uses it to assemble
        // object, stream, transaction, ref and logical-file primitives.
        "anvil-core/src/core_store/local.rs",
        // Object payload ingestion is the only feature writer allowed to call
        // the raw blob primitive directly. All format-aware feature files must
        // publish through write_logical_file.
        "anvil-core/src/object_manager.rs",
        "anvil-core/src/persistence.rs",
        "anvil-core/src/persistence/objects.rs",
    ]);

    let mut violations = Vec::new();
    for path in files {
        let relative = path
            .strip_prefix(workspace_root())
            .expect("source path is under workspace")
            .to_string_lossy()
            .replace('\\', "/");
        if relative.ends_with("/tests.rs") || relative.contains("/local_tests/") {
            continue;
        }
        let source = production_source(&relative);
        if allowed_put_blob_files.contains(relative.as_str()) {
            continue;
        }
        for (line_index, line) in source.lines().enumerate() {
            if line.contains(".put_blob(") {
                violations.push(format!("{}:{}: {}", relative, line_index + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "format-aware feature persistence must use CoreStore logical files instead of raw object payload writes:\n{}",
        violations.join("\n")
    );
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in
        fs::read_dir(dir).unwrap_or_else(|error| panic!("read dir {}: {error}", dir.display()))
    {
        let entry = entry.expect("read directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

fn count_manifest_sidecar_dirs(root: &Path) -> usize {
    let replicas = root.join("corestore").join("meta").join("replicas");
    let Ok(entries) = fs::read_dir(replicas) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| entry.path().join("manifests").exists())
        .count()
}

fn assert_source_contains_all(label: &str, source: &str, terms: &[&str]) {
    let missing = terms
        .iter()
        .copied()
        .filter(|term| !source.contains(term))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "{label} is missing required coverage terms: {missing:#?}"
    );
}

#[test]
fn rfc_0007_conformance_audit_metadata_rows_are_bounded() {
    const CF_OBJECT_VERSIONS: &str = "cf_object_versions";
    const CF_STREAM_RECORDS: &str = "cf_stream_records";
    const TABLE_OBJECT_VERSION_META_ROW: u16 = 0x8102;
    const TABLE_STREAM_RECORD_INDEX_ROW: u16 = 0x8202;
    const CORE_META_MAX_INLINE_PAYLOAD_BYTES: usize = 32 * 1024;
    const CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES: usize = 16 * 1024;
    const OBJECT_SIZED_PAYLOAD_BYTES: usize = 128 * 1024;

    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();

    let object_sized_payload = vec![0xa5; OBJECT_SIZED_PAYLOAD_BYTES];
    let err = store
        .put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &coremeta_test_tuple_key(b"object-version-row"),
            &object_sized_payload,
        )
        .expect_err("object metadata rows must reject object-sized payloads");
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected object metadata rejection: {err:#}"
    );

    let bounded_stream_row =
        coremeta_test_payload(CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES / 2);
    store
        .put(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &coremeta_test_tuple_key(b"stream-record-row"),
            &bounded_stream_row,
        )
        .expect("bounded stream metadata row commits");
    assert_eq!(
        store
            .get(
                CF_STREAM_RECORDS,
                TABLE_STREAM_RECORD_INDEX_ROW,
                &coremeta_test_tuple_key(b"stream-record-row"),
            )
            .unwrap()
            .expect("stream metadata row"),
        bounded_stream_row
    );

    let oversized_stream_row = vec![0x42; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES + 1];
    let err = store
        .put(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &coremeta_test_tuple_key(b"oversized-stream-record-row"),
            &oversized_stream_row,
        )
        .expect_err("stream metadata rows must stay below the bounded descriptor limit");
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected stream metadata rejection: {err:#}"
    );

    let inline_payload = vec![0x7a; CORE_META_MAX_INLINE_PAYLOAD_BYTES];
    store
        .put_inline_payload(
            &coremeta_test_tuple_key(b"inline-payload-ok"),
            &inline_payload,
        )
        .expect("bounded inline payload commits");
    assert_eq!(
        store
            .get_inline_payload(&coremeta_test_tuple_key(b"inline-payload-ok"))
            .unwrap()
            .expect("inline payload row"),
        inline_payload
    );

    let oversized_inline_payload = vec![0x8b; CORE_META_MAX_INLINE_PAYLOAD_BYTES + 1];
    let err = store
        .put_inline_payload(
            &coremeta_test_tuple_key(b"inline-payload-too-large"),
            &oversized_inline_payload,
        )
        .expect_err("inline payload rows must reject payloads above the dedicated inline cap");
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected inline payload rejection: {err:#}"
    );

    let highly_compressible_oversized_inline_payload =
        vec![0_u8; CORE_META_MAX_INLINE_PAYLOAD_BYTES * 4];
    let err = store
        .put_inline_payload(
            &coremeta_test_tuple_key(b"inline-payload-raw-bytes-not-compressed-size"),
            &highly_compressible_oversized_inline_payload,
        )
        .expect_err("inline cap must be enforced before any RocksDB compression");
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected raw-byte inline rejection: {err:#}"
    );

    let err = store
        .put(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            &coremeta_test_tuple_key(b"direct-inline-payload-too-large"),
            &oversized_inline_payload,
        )
        .expect_err("direct inline payload table writes must still enforce the inline cap");
    assert!(
        err.to_string().contains("inline payload") || err.to_string().contains("decode"),
        "unexpected direct inline table rejection: {err:#}"
    );
}

#[test]
fn rfc_0007_conformance_audit_s3_gateway_object_link_tests_are_corestore_backed() {
    let large_object_test =
        read_workspace_file("anvil/tests/s3_gateway_tests/public_private_large_object.rs");
    assert_source_contains_all(
        "S3 large object CoreStore backing test",
        &large_object_test,
        &[
            "test_s3_large_object_ranges_across_docker_cluster",
            ".put_object()",
            "S3 range GET across large CoreStore-backed object should succeed",
        ],
    );

    let native_object_test = read_workspace_file("anvil/tests/object_tests/reserved_head_core.rs");
    assert_source_contains_all(
        "native object CoreStore target test",
        &native_object_test,
        &[
            "test_object_payloads_are_corestore_backed_and_readable",
            "shard_map",
            "\"anvil.core.object_data_target.v1\"",
            "external object should record a CoreStore object data target",
        ],
    );

    let link_path_test = read_workspace_file("anvil/src/s3_gateway/tests.rs");
    assert_source_contains_all(
        "S3 object link CoreStore-backed payload test",
        &link_path_test,
        &[
            "seeded_local_object_link",
            ".object_manager",
            ".put_object(",
            "put_object_link",
            "object_link_get_and_head_follow_by_default_with_link_headers",
            "linked payload",
        ],
    );
}

#[test]
fn rfc_0007_conformance_audit_no_legacy_stream_sidecar_dependencies() {
    let source = read_workspace_file("anvil/tests/corestore_conformance.rs");
    let legacy_extension = format!(".{}{}", "an", "stream");
    let legacy_data_path = ["streams", "data"].join("/");

    assert!(
        !source.contains(&legacy_extension),
        "conformance tests must not require legacy stream files with extension {legacy_extension}"
    );
    assert!(
        !source.contains(&legacy_data_path),
        "conformance tests must not require legacy stream data paths named {legacy_data_path}"
    );
}

#[test]
fn rfc_0007_conformance_audit_no_legacy_stream_paths_in_sources() {
    let mut source_files = Vec::new();
    collect_rs_files(&workspace_root().join("anvil-core/src"), &mut source_files);
    collect_rs_files(&workspace_root().join("anvil/src"), &mut source_files);

    let legacy_extension = format!(".{}{}", "an", "stream");
    let legacy_data_path = ["streams", "data"].join("/");
    let mut violations = Vec::new();
    for path in source_files {
        let relative = workspace_relative_path(&path);
        let source = production_source(&relative);
        for (line_index, line) in source.lines().enumerate() {
            if line.contains(&legacy_extension) || line.contains(&legacy_data_path) {
                violations.push(format!("{}:{}: {}", relative, line_index + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "production code must not retain legacy stream sidecar path dependencies:\n{}",
        violations.join("\n")
    );
}

#[test]
fn rfc_0007_bucket_current_state_is_not_replayed_from_bucket_history() {
    let source = production_source("anvil-core/src/bucket_journal.rs");

    assert_source_contains_all(
        "bucket CoreStore current-state encoding",
        &source,
        &[
            "BucketCurrentRowProto",
            "BucketIdAllocatorRowProto",
            "BucketJournalBodyProto",
            "read_current_bucket_for_tenant_row",
            "read_bucket_id_allocator",
            "bucket_id_allocator_put",
            "encode_bucket_journal_body",
            "decode_bucket_journal_body",
        ],
    );

    for forbidden in [
        "read_current_buckets_from_stream",
        "serde_json::from_slice::<BucketJournalBody",
        "serde_json::to_vec(&BucketJournalBody",
    ] {
        assert!(
            !source.contains(forbidden),
            "bucket current state must not retain legacy JSON/stream replay path: {forbidden}"
        );
    }
}

#[test]
fn rfc_0007_task_live_state_is_not_replayed_from_task_audit_history() {
    let source = production_sources(&[
        "anvil-core/src/task_journal.rs",
        "anvil-core/src/task_journal/model.rs",
        "anvil-core/src/task_journal/queue.rs",
        "anvil-core/src/task_journal/store.rs",
    ]);

    assert_source_contains_all(
        "task CoreStore current-state encoding",
        &source,
        &[
            "TaskQueueRowProto",
            "TaskAuditProto",
            "QueueStore",
            "visible_snapshot",
            "encode_queue_row",
            "encode_task_audit",
            "TABLE_TASK_CURRENT_ROW",
            "CoreMutationOperation::CoreMetaPut",
        ],
    );

    for forbidden in [
        "let frames = read_task_journal_frames(storage).await?",
        "state.apply(body)",
        "serde_json::from_slice(&frame.body)",
        "serde_json::to_vec(&event)",
    ] {
        assert!(
            !source.contains(forbidden),
            "task live queue state must not retain stream replay/JSON current-state path: {forbidden}"
        );
    }
}

#[test]
fn rfc_0007_object_metadata_live_state_uses_coremeta_rows() {
    let metadata = production_sources(&[
        "anvil-core/src/metadata_journal.rs",
        "anvil-core/src/metadata_journal/object_mutation.rs",
        "anvil-core/src/metadata_journal/transaction_projection.rs",
    ]);
    let coremeta = production_sources(&[
        "anvil-core/src/core_store/local_object_metadata.rs",
        "anvil-core/src/core_store/local_object_metadata/mutation.rs",
        "anvil-core/src/core_store/local_object_metadata/projections.rs",
    ]);

    assert_source_contains_all(
        "object metadata live-state CoreMeta implementation",
        &metadata,
        &[
            "prepare_object_metadata_projection(",
            "ObjectMetadataProjectionMutation::Upsert",
            "ObjectMetadataProjectionMutation::DeleteVersion",
            "read_current_object_metadata(bucket, object_key)",
            "read_object_version_metadata(bucket, object_key, version_id)",
            "read_object_version_metadata_by_id(bucket, version_id)",
            "list_current_object_metadata(bucket)",
            "next_object_metadata_id(bucket)",
        ],
    );
    assert_source_contains_all(
        "object metadata CoreMeta row schema",
        &coremeta,
        &[
            "ObjectMetadataRowProto",
            "ObjectMetadataCounterProto",
            "CF_OBJECT_VERSIONS",
            "TABLE_OBJECT_VERSION_META_ROW",
            "object-id-counter",
            "object-version-id",
            "object-page-current",
            "object-page-version",
            "scan_coremeta_prefix_page",
            "scan_range_reverse_inclusive",
        ],
    );

    for forbidden in [
        "parse_current_object_ref_target",
        "current object ref points at missing metadata stream record",
        "record_object_metadata_mutation_id",
    ] {
        assert!(
            !metadata.contains(forbidden),
            "object metadata live state must not retain stream/segment replay path: {forbidden}"
        );
    }
    assert!(
        !coremeta.contains(".scan_prefix("),
        "object metadata projections must use point reads or bounded page/range reads"
    );

    let tests = read_workspace_file("anvil-core/src/metadata_journal/tests.rs");
    assert_source_contains_all(
        "object metadata live-state regression tests",
        &tests,
        &[
            "read_current_object_uses_coremeta_row",
            "live current-object reads are served from CoreMeta rows",
        ],
    );
}

#[test]
fn rfc_0007_root_catalog_hashing_uses_deterministic_protobuf() {
    let source = production_source("anvil-core/src/core_store/local_roots.rs");
    assert!(
        source.contains("encode_root_catalog_record(&unsigned)"),
        "root catalog hash input must be deterministic protobuf, not ad hoc JSON"
    );
    assert!(
        !source.contains("serde_json::to_vec(&unsigned)"),
        "root catalog hash input must not use serde_json"
    );
}

#[test]
fn rfc_0007_transaction_manifest_has_no_untyped_json_extensions() {
    let local = production_source("anvil-core/src/core_store/local.rs");
    let manifest_proto =
        production_source("anvil-core/src/core_store/transaction_manifest_proto.rs");

    assert_source_contains_all(
        "transaction manifest deterministic protobuf schema",
        &manifest_proto,
        &[
            "TransactionManifestHeaderProto",
            "TransactionManifestBodyProto",
            "CoreManifestLocatorProto",
        ],
    );

    for forbidden in [
        "ref_updates: Vec<serde_json::Value>",
        "tombstones: Vec<serde_json::Value>",
        "writer_checkpoints: Vec<serde_json::Value>",
        "boundary_schema_refs: Vec<serde_json::Value>",
        "ensure_no_untyped_transaction_extensions",
    ] {
        assert!(
            !local.contains(forbidden) && !manifest_proto.contains(forbidden),
            "transaction manifests must not retain untyped JSON extension slots: {forbidden}"
        );
    }
}

#[test]
fn rfc_0007_conformance_audit_no_custom_metadata_wal_path_remains() {
    let source = production_source("anvil-core/src/core_store/local.rs");
    let forbidden_terms = [
        format!("{}{}", "W", "AL"),
        format!("{}{}", "w", "al"),
        "AWF1".to_string(),
        "core_wal".to_string(),
        "rocksdb_admission_record".to_string(),
    ];
    let violations = forbidden_terms
        .iter()
        .filter(|term| source.contains(term.as_str()))
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "CoreStore must model admission as RocksDB-backed pending mutation rows, not a custom metadata log path: {violations:#?}"
    );
}

#[test]
fn rfc_0007_conformance_audit_reserved_paths_have_external_coverage() {
    let native = [
        read_workspace_file("anvil/tests/object_tests.rs"),
        read_workspace_file("anvil/tests/object_tests/reserved_head_core.rs"),
    ]
    .join("\n");
    assert_source_contains_all(
        "native reserved namespace tests",
        &native,
        &[
            "test_native_object_api_rejects_reserved_internal_namespaces",
            "UnauthorizedReservedNamespace",
            "PutObjectRequest",
            "GetObjectRequest",
            "HeadObjectRequest",
            "DeleteObjectRequest",
            "ListObjectsRequest",
            "ListObjectVersionsRequest",
            "CopyObjectRequest",
            "ComposeObjectRequest",
            "PatchJsonObjectRequest",
            "CompareAndSwapManifestRequest",
            "InitiateMultipartRequest",
            "CreateAppendStreamRequest",
            "AppendStreamRecordRequest",
            "SealAppendStreamSegmentRequest",
            "WatchPrefixRequest",
            "\"x-anvil-internal-write-token\"",
            "_anvil/authz/",
            "_anvil/personaldb/",
            "_anvil/git/",
        ],
    );

    let s3 = read_workspace_file("anvil/tests/s3_gateway_tests/public_private_large_object.rs");
    assert_source_contains_all(
        "S3 reserved namespace tests",
        &s3,
        &[
            "Reserved internal namespaces are never readable or writable through S3",
            "UnauthorizedReservedNamespace",
            ".get(&reserved_url)",
            ".head(&reserved_url)",
            "reqwest::header::RANGE",
            ".put_object()",
            ".list_objects_v2()",
            ".list_object_versions()",
            ".delete_object()",
            ".copy_object()",
            "internal_write_token",
            "_anvil/authz/",
            "_anvil/personaldb/",
            "_anvil/git/",
        ],
    );
}

#[test]
fn rfc_0007_conformance_audit_control_current_state_uses_coremeta_rows_and_protobuf() {
    let source = production_sources(&[
        "anvil-core/src/control_journal.rs",
        "anvil-core/src/control_journal/current.rs",
    ]);
    assert!(
        !source.contains("serde_json"),
        "control current state and history payloads must use deterministic protobuf, not JSON"
    );
    assert!(
        !source.contains("serde::{"),
        "control current state must not derive serde payload encodings"
    );
    assert_source_contains_all(
        "control current state CoreMeta row/protobuf implementation",
        &source,
        &[
            "pub async fn read_control_state",
            "scan_current_page",
            "encode_control_current_row",
            "decode_control_current_row",
            "TABLE_CONTROL_CURRENT_ROW",
            "ControlCurrentProto",
            "ControlEventProto",
            "decode_control_event_body(&record.payload)",
        ],
    );

    let tests = read_workspace_file("anvil-core/src/control_journal.rs");
    assert_source_contains_all(
        "control current state regression tests",
        &tests,
        &[
            "control_current_state_does_not_replay_control_history_stream",
            "control_current_rows_are_sufficient_without_control_history_stream",
            "control_state_reads_current_rows_and_keeps_history_for_watch",
        ],
    );
}

#[test]
fn rfc_0007_conformance_audit_authz_and_query_visibility_have_external_coverage() {
    let authz_helpers = production_source("anvil-core/src/services/auth/helpers.rs");
    assert_source_contains_all(
        "authz reserved realm validation",
        &authz_helpers,
        &[
            "validate_authz_realm_id",
            "is_reserved_authz_realm_id",
            "validate_public_authz_namespace",
            "is_reserved_authz_namespace",
            "UnauthorizedReservedNamespace",
            "starts_with(\"_anvil/\")",
            "anvil_mesh",
            "_system/",
        ],
    );

    let authz_tests = read_workspace_file("anvil/tests/auth_tests/object_lists_and_schemas.rs");
    assert_source_contains_all(
        "public AuthzService reserved realm tests",
        &authz_tests,
        &[
            "test_public_authz_apis_reject_reserved_system_realm_scope",
            "_anvil/system",
            "WriteAuthzTupleRequest",
            "WriteAuthzTuplesRequest",
            "ReadAuthzTuplesRequest",
            "CheckPermissionRequest",
            "CheckPermissionsRequest",
            "ListAuthzObjectsRequest",
            "ListAuthzSubjectsRequest",
            "BindAuthzSchemaRequest",
            "GetAuthzSchemaBindingRequest",
            "WatchAuthzTupleLogRequest",
            "UnauthorizedReservedNamespace",
        ],
    );

    let index_operations = [
        "anvil-core/src/services/index/operations.rs",
        "anvil-core/src/services/index/query.rs",
        "anvil-core/src/services/index/query_candidates.rs",
        "anvil-core/src/services/index/query_planner_adapter.rs",
        "anvil-core/src/object_manager/read.rs",
    ]
    .into_iter()
    .map(production_source)
    .collect::<Vec<_>>()
    .join("\n");
    assert_source_contains_all(
        "query/index authz visibility enforcement",
        &index_operations,
        &[
            "AuthzSegmentCandidateReader",
            "query_hit_visible",
            "validation::is_reserved_internal_key(object_key)",
            "tenant_reader.candidate_set(request.clone()).await",
            "IndexCapabilityMissing: object listing requires a planner-backed path/object-list candidate reader",
            "validation::is_reserved_internal_key(prefix)",
            "validation::is_reserved_internal_key(object_key)",
        ],
    );

    let typed_index_tests = read_workspace_file("anvil/tests/index_tests/typed_lifecycle.rs");
    assert_source_contains_all(
        "typed index reserved candidate regression test",
        &typed_index_tests,
        &[
            "test_typed_json_index_omits_reserved_internal_candidates",
            "_anvil/authz/query-candidate.json",
            "write_typed_field_segment",
            "query_index",
            "typed-visibility",
            "visible/query-candidate.json",
        ],
    );
}

#[test]
fn rfc_0006_protected_writers_use_commit_time_partition_preconditions() {
    let protected_writers = [
        "anvil-core/src/append_journal.rs",
        "anvil-core/src/authz_journal.rs",
        "anvil-core/src/bucket_journal.rs",
        "anvil-core/src/control_journal.rs",
        "anvil-core/src/hf_journal.rs",
        "anvil-core/src/index_diagnostic_journal.rs",
        "anvil-core/src/index_journal.rs",
        "anvil-core/src/manifest_journal.rs",
        "anvil-core/src/metadata_journal.rs",
        "anvil-core/src/model_journal.rs",
        "anvil-core/src/multipart_journal.rs",
        "anvil-core/src/task_journal.rs",
        "anvil-core/src/mesh_directory.rs",
        "anvil-core/src/mesh_lifecycle/topology_mutation.rs",
        "anvil-core/src/services/personaldb.rs",
    ];

    for relative in protected_writers {
        let source = protected_writer_source(relative);
        assert!(
            !source.contains("validate_partition_write("),
            "{relative} must not prevalidate a partition write and then perform a separate visible write"
        );
        assert!(
            source.contains("partition_write_precondition(")
                || source.contains("personaldb_group_write_precondition("),
            "{relative} must derive a commit-time precondition from the current partition owner"
        );
    }

    for relative in [
        "anvil-core/src/append_journal.rs",
        "anvil-core/src/authz_journal.rs",
        "anvil-core/src/bucket_journal.rs",
        "anvil-core/src/control_journal.rs",
        "anvil-core/src/hf_journal.rs",
        "anvil-core/src/index_diagnostic_journal.rs",
        "anvil-core/src/index_journal.rs",
        "anvil-core/src/manifest_journal.rs",
        "anvil-core/src/metadata_journal.rs",
        "anvil-core/src/model_journal.rs",
        "anvil-core/src/multipart_journal.rs",
        "anvil-core/src/task_journal.rs",
        "anvil-core/src/mesh_control_stream.rs",
        "anvil-core/src/mesh_lifecycle/topology_mutation.rs",
        "anvil-core/src/personaldb_heads.rs",
    ] {
        let source = protected_writer_source(relative);
        assert!(
            source.contains("CoreMutationBatch") && source.contains(".commit_mutation_batch"),
            "{relative} must make protected visible writes through CoreMutationBatch"
        );
    }
}

#[tokio::test]
async fn rfc_0006_root_catalog_is_signed_generationed_and_recoverable() {
    const KEY: &[u8] = b"rfc-0006-root-catalog-key";
    const ZERO_HASH: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root_segment = store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0006/system/mesh/root-segment/head".to_string(),
            bytes: vec![0x72; 128 * 1024],
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "rfc-0006-root-segment-1".to_string(),
        })
        .await
        .unwrap();
    let embedded_manifest = store.read_object_manifest(&root_segment).await.unwrap();

    let catalog = CoreRootCatalog {
        schema: CORE_ROOT_CATALOG_SCHEMA.to_string(),
        mesh_id: "mesh-rfc0006".to_string(),
        generation: 1,
        previous_hash: ZERO_HASH.to_string(),
        root_partitions: vec![CoreRootPartition {
            partition_id: "core.root.refs.0".to_string(),
            owner_node_id: "node-rfc0006".to_string(),
            fence: 1,
            placement_group: "root-pg-0".to_string(),
            embedded_head_segment_manifest: embedded_manifest,
        }],
        placement_catalog_ref: "core.ref:/system/placement/current".to_string(),
        stream_directory_ref: "core.ref:/system/streams/current".to_string(),
        authz_system_realm_ref: "core.ref:/system/authz/realm/current".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        signed_by: "node-rfc0006".to_string(),
        signature: String::new(),
    };

    let receipt = store
        .commit_root_catalog(catalog.clone(), KEY)
        .await
        .expect("genesis root catalog commits");
    assert_eq!(receipt.generation, 1);
    assert!(receipt.catalog_hash.starts_with("sha256:"));

    let latest = store
        .read_latest_root_catalog("mesh-rfc0006", KEY)
        .await
        .unwrap()
        .expect("latest root catalog");
    assert_eq!(latest.generation, 1);
    assert_eq!(latest.signed_by, "node-rfc0006");
    assert!(
        store
            .read_latest_root_catalog("mesh-rfc0006", b"wrong-key")
            .await
            .is_err(),
        "root catalog signatures must reject the wrong key"
    );
    assert!(
        store.commit_root_catalog(catalog, KEY).await.is_err(),
        "stale root catalog generations must be rejected"
    );
    assert_eq!(
        store
            .list_root_catalog_history_page("mesh-rfc0006", 0, 10)
            .await
            .unwrap()
            .records
            .len(),
        1
    );
}

#[tokio::test]
async fn rfc_0006_quorum_profile_requires_intersection_and_monotonic_epochs() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    let genesis = CoreQuorumProfile {
        schema: CORE_QUORUM_PROFILE_SCHEMA.to_string(),
        placement_group: "pg-rfc0006".to_string(),
        replica_count: 5,
        write_quorum: 3,
        read_quorum: 3,
        fence_quorum: 3,
        epoch: 1,
    };
    let receipt = store
        .commit_quorum_profile(genesis.clone())
        .await
        .expect("intersecting genesis quorum profile commits");
    assert_eq!(receipt.placement_group, "pg-rfc0006");
    assert_eq!(receipt.epoch, 1);
    assert!(receipt.profile_hash.starts_with("sha256:"));

    let latest = store
        .read_latest_quorum_profile("pg-rfc0006")
        .await
        .unwrap()
        .expect("latest quorum profile");
    assert_eq!(latest, genesis);

    let mut stale = latest.clone();
    stale.epoch = 1;
    assert!(
        store.commit_quorum_profile(stale).await.is_err(),
        "a new active epoch must immediately follow the current committed epoch"
    );

    let non_intersecting = CoreQuorumProfile {
        schema: CORE_QUORUM_PROFILE_SCHEMA.to_string(),
        placement_group: "pg-rfc0006-bad".to_string(),
        replica_count: 5,
        write_quorum: 2,
        read_quorum: 2,
        fence_quorum: 2,
        epoch: 1,
    };
    let err = store
        .commit_quorum_profile(non_intersecting)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("do not intersect"),
        "non-intersecting read/write/fence quorums must be rejected: {err:?}"
    );

    let next = CoreQuorumProfile { epoch: 2, ..latest };
    store
        .commit_quorum_profile(next)
        .await
        .expect("next quorum profile epoch commits");
    let history = store
        .list_quorum_profile_history_page("pg-rfc0006", 0, 10)
        .await
        .unwrap()
        .records;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].epoch, 1);
    assert_eq!(history[1].epoch, 2);
}

#[tokio::test]
async fn rfc_0006_corestore_transactions_gate_coremeta_stream_and_watch_visibility() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0006/tenant:t/bucket:b/object:alpha/payload".to_string(),
            bytes: br#"{"name":"alpha"}"#.to_vec(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "payload-alpha".to_string(),
        })
        .await
        .unwrap();
    let current_key = coremeta_test_tuple_key(b"tenant/t/bucket/b/object/alpha/current");
    let current_payload = encode_core_meta_inline_payload_row(
        &coremeta_test_payload(64),
        core_meta_committed_row_common("", "", 0, "", 0),
    )
    .unwrap();

    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-visible".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:rfc0006".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant:t/bucket:b",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: current_key.clone(),
                expected_payload_hash: None,
                require_absent: true,
                require_present: false,
            }],
            operations: vec![
                CoreMutationOperation::CoreMetaPut {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    cf: CF_INLINE_PAYLOADS.to_string(),
                    table_id: TABLE_INLINE_PAYLOAD_ROW,
                    tuple_key: current_key.clone(),
                    payload: current_payload.clone(),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: "object_metadata:t:b".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"alpha"}"#.to_vec(),
                    idempotency_key: Some("object-alpha-put".to_string()),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(receipt.visible_updates.len(), 2);
    assert_eq!(
        store
            .read_transaction("txn-rfc0006-visible")
            .await
            .unwrap()
            .expect("transaction")
            .state,
        CoreTransactionState::Committed
    );
    let current = CoreMetaStore::open(store.storage().core_store_meta_path())
        .unwrap()
        .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &current_key)
        .unwrap()
        .expect("current CoreMeta row");
    assert_eq!(current, current_payload);
    assert_eq!(
        store.get_blob(GetBlob { object_ref }).await.unwrap(),
        br#"{"name":"alpha"}"#
    );
    assert_eq!(
        store
            .read_stream(ReadStream {
                stream_id: "object_metadata:t:b".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_stream_ids_page("object_metadata:", None, 10)
            .await
            .unwrap(),
        vec!["object_metadata:t:b".to_string()],
        "CoreStore stream ids must be listed from RocksDB-backed stream metadata"
    );
    assert_eq!(
        store
            .watch(WatchRequest {
                stream_prefix: "object_metadata:t".to_string(),
                after_cursor: None,
                limit: 10,
            })
            .await
            .unwrap()
            .events
            .len(),
        1
    );

    let failed_payload = coremeta_test_payload(65);
    let failed = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-failed".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:rfc0006".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant:t/bucket:b",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: current_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(
                    TABLE_INLINE_PAYLOAD_ROW,
                    &failed_payload,
                )),
                require_absent: false,
                require_present: true,
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: "tenant:t/bucket:b".to_string(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: current_key.clone(),
                payload: failed_payload,
            }],
        })
        .await;
    assert!(failed.is_err());
    assert!(
        store
            .read_transaction("txn-rfc0006-failed")
            .await
            .unwrap()
            .is_none(),
        "failed mutation batches must not publish transaction records"
    );
    assert_eq!(
        CoreMetaStore::open(store.storage().core_store_meta_path())
            .unwrap()
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &current_key)
            .unwrap()
            .expect("current CoreMeta row"),
        current
    );
}

#[tokio::test]
async fn rfc_0007_coreobject_manifests_are_reconstructed_from_shard_placement() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0006/tenant:t/bucket:b/object:manifest/payload".to_string(),
            bytes: vec![0x6d; 128 * 1024],
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "payload-manifest-quorum".to_string(),
        })
        .await
        .unwrap();
    assert!(
        object_ref.manifest_ref.starts_with("core-manifest-sha256:"),
        "manifest_ref must be a CoreStore logical manifest identity, not a local file path"
    );
    assert!(
        count_manifest_sidecar_dirs(tmp.path()) == 0,
        "object manifests must not be stored as final sidecar JSON replicas"
    );

    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    assert_eq!(manifest.object_hash, object_ref.hash);
    assert_eq!(manifest.placements.len(), 6);

    for placement in manifest.placements.iter().take(2) {
        let shard_path = block_shard_path(
            &storage,
            &placement.node_id,
            &manifest.encoding.block_id,
            placement.shard_index,
        );
        fs::remove_file(shard_path).unwrap();
    }
    let degraded_manifest = store.read_object_manifest(&object_ref).await.unwrap();
    assert_eq!(
        degraded_manifest.placements.len(),
        4,
        "manifest reconstruction should use remaining erasure shard placement"
    );

    let placement = &manifest.placements[2];
    let shard_path = block_shard_path(
        &storage,
        &placement.node_id,
        &manifest.encoding.block_id,
        placement.shard_index,
    );
    fs::remove_file(shard_path).unwrap();
    assert!(
        store.read_object_manifest(&object_ref).await.is_err(),
        "manifest reconstruction must fail closed without enough erasure shard placement"
    );
}

#[tokio::test]
async fn rfc_0006_gateway_mounts_resolve_scope_before_route_handling() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let mount = GatewayMountRecord {
        schema: "anvil.gateway.mount.v1".to_string(),
        mount_id: "docker-west".to_string(),
        gateway: "docker".to_string(),
        hosts: vec!["registry.example.test".to_string()],
        path_prefixes: vec!["/".to_string(), "/v2/".to_string()],
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        anvil_storage_tenant_id: "storage-tenant-a".to_string(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: "storage-tenant-a".to_string(),
            authz_realm_id: "realm-a".to_string(),
        },
        tenant_id: "tenant-a".to_string(),
        registry_instance_id: "registry-a".to_string(),
        default_bucket: "packages".to_string(),
        repository_prefix: String::new(),
        state: GatewayMountState::Active,
        generation: 0,
        record_hash: String::new(),
    };
    assert_eq!(
        put_gateway_mount_record(&storage, mount, None)
            .await
            .unwrap(),
        1
    );

    let exact = resolve_gateway_mount(&storage, "registry.example.test", "/v2/team/api/tags/list")
        .await
        .unwrap()
        .expect("exact host mount");
    assert_eq!(exact.match_kind, GatewayMountMatchKind::ExactHostAlias);
    assert_eq!(exact.matched_path_prefix, "/v2/");
    assert_eq!(exact.record.authz_scope.authz_realm_id, "realm-a");

    let virtual_host = resolve_gateway_mount(
        &storage,
        "registry-a.tenant-a.eu-west-1.anvil-storage.com",
        "/v2/team/api/manifests/latest",
    )
    .await
    .unwrap()
    .expect("virtual host mount");
    assert_eq!(
        virtual_host.match_kind,
        GatewayMountMatchKind::VirtualHostRegional
    );
    assert_eq!(virtual_host.record.authz_scope.authz_realm_id, "realm-a");

    let path_style = resolve_gateway_mount(
        &storage,
        "eu-west-1.anvil-storage.com",
        "/tenant-a/_gateway/docker/registry-a/v2/team/api/blobs/sha256:abc",
    )
    .await
    .unwrap()
    .expect("path-style mount");
    assert_eq!(
        path_style.match_kind,
        GatewayMountMatchKind::PathStyleRegional
    );
    assert_eq!(
        path_style.matched_path_prefix,
        "/tenant-a/_gateway/docker/registry-a/"
    );

    assert!(
        resolve_gateway_mount(
            &storage,
            "eu-west-1.anvil-storage.com",
            "/tenant-a/_gateway/npm/registry-a/package",
        )
        .await
        .unwrap()
        .is_none(),
        "gateway kind and authz scope must come from the mount, not caller-supplied path text"
    );
}

#[tokio::test]
async fn rfc_0006_corestore_streams_are_chained_and_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    let first = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.created".to_string(),
            payload: br#"{"step":1}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-created-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(first.sequence, 1);
    assert!(!first.idempotent_replay);

    let replay = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.created".to_string(),
            payload: br#"{"step":1}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-created-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(replay.sequence, first.sequence);
    assert_eq!(replay.event_hash, first.event_hash);
    assert!(replay.idempotent_replay);

    let conflicting_replay = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.created".to_string(),
            payload: br#"{"step":99}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-created-1".to_string()),
        })
        .await;
    assert!(
        conflicting_replay.is_err(),
        "same idempotency key with different bytes must fail"
    );

    let second = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.updated".to_string(),
            payload: br#"{"step":2}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-updated-2".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(second.sequence, 2);

    let records = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].event_hash, first.event_hash);
    assert_eq!(records[1].previous_event_hash, records[0].event_hash);
    assert_eq!(records[1].event_hash, second.event_hash);
    assert_ne!(
        records[1].previous_event_hash, records[1].event_hash,
        "a stream record must chain to the prior event, not itself"
    );

    let watched = store
        .watch(WatchRequest {
            stream_prefix: "tenant:t/bucket:b".to_string(),
            after_cursor: Some(first.cursor),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(watched.events.len(), 1);
    assert_eq!(watched.events[0].cursor, second.cursor);
    assert_eq!(watched.events[0].previous_event_hash, records[0].event_hash);
    assert_eq!(watched.events[0].event_hash, records[1].event_hash);
    assert_eq!(watched.events[0].event_type, "audit.updated");
    assert_eq!(watched.events[0].transaction_id, None);
    assert_eq!(watched.events[0].payload_hash, records[1].payload_hash);
}

#[tokio::test]
async fn rfc_0006_sealed_stream_segments_use_binary_frame_and_stream_remains_open() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.created".to_string(),
            payload: br#"{"event":1}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("event-1".to_string()),
        })
        .await
        .unwrap();
    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.updated".to_string(),
            payload: br#"{"event":2}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("event-2".to_string()),
        })
        .await
        .unwrap();

    let sealed = store
        .seal_stream_segment(SealStreamSegment {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            through_sequence: Some(2),
            segment_kind: "audit".to_string(),
            mutation_id: "seal-events-1".to_string(),
        })
        .await
        .unwrap();
    let segment_bytes = store
        .get_blob(GetBlob {
            object_ref: sealed.object_ref.clone(),
        })
        .await
        .unwrap();
    assert!(
        segment_bytes.starts_with(b"ANSTRM\n\0"),
        "sealed stream segment must use the RFC binary frame magic"
    );
    assert!(
        segment_bytes
            .windows(8)
            .any(|window| window == b"ANSSIX1\0"),
        "sealed stream segment must include the RFC sparse sequence/time index"
    );
    let decoded = store.read_stream_segment(&sealed).await.unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].record_kind, "event.created");
    assert_eq!(decoded[1].record_kind, "event.updated");

    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.after_seal".to_string(),
            payload: br#"{"event":3}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("event-3".to_string()),
        })
        .await
        .unwrap();
    let records = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        3,
        "sealing must not close the logical stream"
    );
    assert_eq!(records[2].record_kind, "event.after_seal");
}

#[tokio::test]
async fn rfc_0007_corestore_runtime_does_not_write_final_json_control_sidecars() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0007/tenant:t/bucket:b/object:sidecar-proof/payload".to_string(),
            bytes: vec![0x44; 96 * 1024],
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "sidecar-proof-blob".to_string(),
        })
        .await
        .unwrap();
    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/rfc0007-final-sidecar-proof".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.created".to_string(),
            payload: vec![0x55; 32 * 1024],
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("rfc0007-final-sidecar-proof-1".to_string()),
        })
        .await
        .unwrap();

    let violations = forbidden_final_sidecar_violations(tmp.path());
    assert!(
        violations.is_empty(),
        "CoreStore runtime must not write final JSON/JSONL/custom journal control sidecars; only RocksDB/CoreMeta and byte-pipeline files are allowed:\n{}",
        violations.join("\n")
    );
}

#[tokio::test]
async fn rfc_0007_conformance_audit_stream_records_do_not_create_legacy_sidecars() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/rfc0007-sidecar-proof".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.created".to_string(),
            payload: vec![0x33; 32 * 1024],
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("rfc0007-sidecar-proof-1".to_string()),
        })
        .await
        .unwrap();
    store
        .seal_stream_segment(SealStreamSegment {
            stream_id: "tenant:t/bucket:b/rfc0007-sidecar-proof".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            through_sequence: Some(1),
            segment_kind: "audit".to_string(),
            mutation_id: "rfc0007-sidecar-proof-seal".to_string(),
        })
        .await
        .unwrap();

    let violations = legacy_stream_sidecar_violations(tmp.path());
    assert!(
        violations.is_empty(),
        "stream conformance paths must not create legacy stream sidecar files:\n{}",
        violations.join("\n")
    );

    let records = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/rfc0007-sidecar-proof".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn rfc_0006_fenced_mutations_use_authenticated_principal_not_request_owner_text() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let fence = store
        .acquire_fence(AcquireFence {
            fence_name: "tenant:t/bucket:b/object:secure".to_string(),
            authenticated_principal: "principal:legitimate-worker".to_string(),
            ttl_ms: 60_000,
        })
        .await
        .unwrap();

    let stale_owner = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-wrong-principal".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:impersonator".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant:t/bucket:b",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: vec![CoreMutationPrecondition::Fence {
                fence_name: "tenant:t/bucket:b/object:secure".to_string(),
                fence_token: fence.fence_token,
            }],
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: "object_metadata:t:b:secure".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"secure"}"#.to_vec(),
                idempotency_key: None,
            }],
        })
        .await;
    assert!(
        stale_owner.is_err(),
        "fenced mutation must derive owner from committed_by_principal and reject impersonation"
    );
    assert!(
        store
            .read_stream(ReadStream {
                stream_id: "object_metadata:t:b:secure".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty(),
        "failed fenced mutation must not publish protected stream records"
    );

    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-right-principal".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:legitimate-worker".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant:t/bucket:b",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: vec![CoreMutationPrecondition::Fence {
                fence_name: "tenant:t/bucket:b/object:secure".to_string(),
                fence_token: fence.fence_token,
            }],
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: "object_metadata:t:b:secure".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"secure"}"#.to_vec(),
                idempotency_key: None,
            }],
        })
        .await
        .unwrap();
}
