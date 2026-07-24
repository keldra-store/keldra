#![recursion_limit = "256"]

use std::{fs, path::PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf()
}

fn workspace_file(path: &str) -> String {
    fs::read_to_string(workspace_root().join(path))
        .unwrap_or_else(|err| panic!("read {path}: {err}"))
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
fn explicit_transaction_api_is_exposed_in_proto_and_rust_client() {
    let core_proto = workspace_file("anvil-core/proto/anvil.proto");
    let rust_proto = workspace_file("clients/rust/proto/anvil.proto");
    let public_transaction_api = [
        "service TransactionService {",
        "rpc BeginTransaction(BeginTransactionRequest) returns (BeginTransactionResponse);",
        "rpc CommitTransaction(CommitTransactionRequest) returns (WriteResponse);",
        "rpc RollbackTransaction(RollbackTransactionRequest) returns (RollbackTransactionResponse);",
        "rpc GetTransaction(GetTransactionRequest) returns (TransactionStatus);",
        "optional string transaction_id = 8;",
        "WriteState write_state = 9;",
        "WriteState write_state = 5;",
        "message TransactionScope",
        "message BoundaryValue",
        "repeated WritePrecondition preconditions = 4;",
        "repeated BoundaryValue boundary_values = 5;",
        "message CommitTransactionRequest",
        "message TransactionStatus",
    ];

    assert_contains_all(
        "explicit transaction server protobuf API",
        &core_proto,
        &public_transaction_api,
    );
    assert_contains_all(
        "explicit transaction Rust client protobuf API",
        &rust_proto,
        &public_transaction_api,
    );

    let client = workspace_file("clients/rust/src/lib.rs");
    assert_contains_all(
        "Rust transaction client surface",
        &client,
        &[
            "BeginTransactionRequest",
            "BeginTransactionResponse",
            "BoundaryValue",
            "CommitTransactionRequest",
            "GetTransactionRequest",
            "RollbackTransactionRequest",
            "TransactionScope",
            "TransactionStatus",
            "WriteResponse",
            "WriteState",
            "transaction_service_client",
            "pub fn native_context_with_transaction(",
            "pub fn transactions(",
            "pub async fn begin_transaction(",
            "pub async fn commit_transaction(",
            "pub async fn rollback_transaction(",
            "pub async fn get_transaction(",
        ],
    );
}

#[test]
fn transaction_boundary_hash_uses_deterministic_protobuf_not_json() {
    let service = workspace_file("anvil-core/src/services/transaction.rs");

    assert_contains_all(
        "transaction precondition boundary hash",
        &service,
        &[
            "#[derive(Clone, PartialEq, Message)]",
            "struct TransactionPreconditionsHashProto",
            "preconditions: Vec<WritePrecondition>",
            "boundary_values: Vec<BoundaryValue>",
            "fn transaction_preconditions_hash(",
            "encode_deterministic_proto(&input)",
            "anvil.transaction.preconditions.v1",
            "hasher.update(&(bytes.len() as u64).to_le_bytes())",
            "transaction_precondition_hash_includes_boundary_values",
        ],
    );
    assert_contains_none(
        "transaction precondition boundary hash",
        &service,
        &[
            "serde_json",
            "canonical_json",
            "to_vec(&input)",
            "format!(\"{:?}\"",
        ],
    );
}

#[test]
fn native_writes_accept_transaction_id_and_read_committed_source_gates_exist() {
    let object_rpc = format!(
        "{}\n{}\n{}\n{}\n{}",
        workspace_file("anvil-core/src/services/object/rpc.rs"),
        workspace_file("anvil-core/src/services/object/native_put_rpc.rs"),
        workspace_file("anvil-core/src/services/object/mutation_batch_rpc.rs"),
        workspace_file("anvil-core/src/services/object/batch_helpers.rs"),
        workspace_file("anvil-core/src/services/transaction.rs")
    );
    let object_rpc_with_shared_validation = format!(
        "{}\n{}",
        object_rpc,
        workspace_file("anvil-core/src/services/saga_reserved.rs")
    );
    assert_contains_all(
        "native write transaction id handling",
        &object_rpc_with_shared_validation,
        &[
            "pub(super) fn native_transaction_id(",
            "fn write_state_for_transaction(transaction_id: Option<&str>)",
            "transaction_id.trim().is_empty()",
            "ensure_mutation_batch_operations_supported",
            "prepare_mutation_batch_native_preconditions(",
            "WriteState::Staged as i32",
            "transaction_id: transaction_id.map(ToOwned::to_owned)",
            "delete_object_version(",
            "transaction_id,",
            "delete_object(",
            "watch_cursor = if transaction_id.is_some()",
            "write_state: write_state_for_transaction(transaction_id)",
            "verify_explicit_transaction_finalised",
            "WriteState::Finalised as i32",
        ],
    );
    assert_contains_all(
        "coordination-plane operations are explicit non-staged preconditions",
        &object_rpc,
        &[
            "is a coordination-plane operation; use CoordinationService",
            "mutation_batch_operation::Op::CheckpointTaskLease",
            "mutation_batch_operation::Op::CommitTaskLease",
            "Status::failed_precondition",
        ],
    );
    assert_contains_none(
        "native transaction implementation placeholders",
        &object_rpc,
        &["Status::unimplemented", "not implemented", "unimplemented!"],
    );

    let core_transactions = workspace_file("anvil-core/src/core_store/local_transactions.rs");
    let core_transaction_rows = workspace_file("anvil-core/src/core_store/local_tx_rows.rs");
    let core_transaction_visibility =
        workspace_file("anvil-core/src/core_store/local_transaction_visibility.rs");
    assert_contains_all(
        "CoreStore explicit transaction visibility",
        &format!(
            "{core_transactions}
{core_transaction_rows}
{core_transaction_visibility}"
        ),
        &[
            "begin_explicit_transaction",
            "stage_explicit_transaction_batch",
            "commit_explicit_transaction",
            "rollback_explicit_transaction",
            "read_explicit_transaction_for_principal",
            "validate_transaction_root_scope(&transaction)",
            "TransactionScopeMismatch",
            "filter_committed_stream_records",
            "transaction_makes_stream_record_visible",
            "coremeta_payload_visible_to_transaction_unlocked",
            "validate_explicit_transaction_commit_unlocked",
            "commit_explicit_transaction_rows_and_coremeta_updates_unlocked(&committed)",
            "let committed_transaction = self",
            "committed_transaction.committed_root_generation = Some(committed_root_generation)",
            "transaction_header_as_coremeta_op_unlocked",
            "borrow_owned_coremeta_batch_ops",
            "OwnedCoreMetaBatchOp::Delete",
            "core_meta_committed_row_common(",
            "delete_common_for_committed_transaction(",
            "validate_committed_coremeta_put_common(transaction, payload)",
            "committed_coremeta_payload_unlocked",
            "common.transaction_id != transaction.transaction_id",
            "Some(&batch.transaction_id)",
            "commit_coremeta_root_groups_prelocked(",
        ],
    );
    assert_contains_none(
        "CoreStore explicit transaction control path",
        &format!(
            "{core_transactions}
{core_transaction_rows}
{core_transaction_visibility}"
        ),
        &["corestore/transactions/*.json", "serde_json::"],
    );
}

#[test]
fn corestore_model_covers_read_committed_and_single_root_release_gates() {
    let model = workspace_file("crates/anvil-corestore-model/src/model.rs");
    let tests = workspace_file("crates/anvil-corestore-model/src/tests.rs");

    assert_contains_all(
        "CoreStore transaction model",
        &model,
        &[
            "METADATA_QUORUM",
            "PersistCommitCertificate",
            "visible_generations_have_persisted_certificates",
            "roots_reference_persisted_commit_evidence",
            "BeginTransaction(TxId, RootKey)",
            "StageMutation(TxId, RootKey)",
            "CommitTransaction(TxId)",
            "RollbackTransaction(TxId)",
            "TransactionStatus::ScopeMismatch",
            "rejected_scope_mismatches",
            "read_latest_committed",
            "read_at_generation",
            "staged_rows_are_invisible",
            "explicit_transactions_visible_only_on_their_root",
            "stale_owner_never_published",
            "BumpOwnerFence",
        ],
    );
    assert_contains_all(
        "CoreStore model regression tests",
        &tests,
        &[
            "model_explicit_transaction_scope_mismatch_rejected",
            "model_read_committed_dirty_read_forbidden",
            "model_read_committed_pinned_generation_repeatable",
            "model_committed_transaction_rows_stay_on_scoped_root",
            "model_stale_owner_rejected_by_fence",
            "model_commit_certificate_persisted_before_root",
        ],
    );

    let batch_helpers = workspace_file("anvil-core/src/services/object/batch_helpers.rs");
    assert_contains_all(
        "MutationBatch transaction preconditions",
        &batch_helpers,
        &[
            "prepare_mutation_batch_native_preconditions",
            "mutation_precondition_transaction",
            "mutation_batch_operation::Op::PutObject",
            "mutation_batch_operation::Op::DeleteObject",
            "mutation_batch_operation::Op::CompareAndSwapManifest",
            "prepare_native_mutation_precondition(",
        ],
    );

    let native_idempotency = workspace_file("anvil-core/src/native_idempotency.rs");
    assert_contains_all(
        "transaction-scoped native idempotency",
        &native_idempotency,
        &[
            "transaction_id: Option<String>",
            "record.transaction_id != context.transaction_id",
            "context.transaction_id.as_deref()",
            "hasher.update(transaction_id.as_bytes())",
            "native_idempotency_keys_are_scoped_by_transaction_id",
        ],
    );
}

#[test]
fn root_publication_requires_persisted_commit_evidence_and_fresh_owner_fence() {
    let local_roots = workspace_file("anvil-core/src/core_store/local_roots.rs");
    let root_publication = format!(
        "{}\n{}\n{}\n{}",
        workspace_file("anvil-core/src/core_store/local_roots_layout.rs"),
        workspace_file("anvil-core/src/core_store/local_root_publication.rs"),
        workspace_file("anvil-core/src/core_store/local_root_publication_evidence.rs"),
        workspace_file("anvil-core/src/core_store/local_root_failover.rs")
    );
    let root_proto = workspace_file("anvil-core/src/core_store/root_proto.rs");
    let transaction_proto =
        workspace_file("anvil-core/src/core_store/transaction_manifest_proto.rs");
    let local = workspace_file("anvil-core/src/core_store/local.rs");

    assert_contains_all(
        "CoreStore root/transaction records carry commit evidence",
        &(local.clone() + &root_proto + &transaction_proto),
        &[
            "core_meta_commit_certificate_hash",
            "certificate_persist_receipt_hashes",
        ],
    );
    assert_contains_all(
        "CoreStore root evidence validation",
        &local_roots,
        &[
            "CoreStore non-genesis root anchor must include commit evidence",
            "validate_certificate_persist_receipts(&anchor.certificate_persist_receipt_hashes)?",
            "CoreStore certificate persist receipt hashes must be sorted and unique",
        ],
    );
    assert_contains_all(
        "CoreStore root publication release gate",
        &root_publication,
        &[
            "CoreStore root anchor is missing CoreMeta commit certificate",
            "CoreStore root anchor references missing CoreMeta evidence",
            "core_meta_commit_certificate_hash: Some(outcome.certificate_hash.clone())",
            "certificate_persist_receipt_hashes: outcome.certificate_persist_receipt_hashes.clone()",
            "validate_transaction_manifest_record(&transaction_manifest, post_root_generation)?",
            "anchor.core_meta_commit_certificate_hash.as_deref()",
            "Some(certificate.certificate_hash.as_str())",
            "read_latest_root_anchor(&anchor.root_anchor_key)",
            "CoreStore root publication rejected stale owner fence",
        ],
    );
    assert_contains_none(
        "CoreStore root publication must not reuse pending-admission evidence",
        &root_publication,
        &["root_commit_evidence_for_records(records).await?"],
    );
    assert_contains_none(
        "CoreStore root publication stale cache path",
        &root_publication,
        &["read_cached_latest_root_anchor(&anchor.root_anchor_key)"],
    );
}

#[test]
fn public_cli_exposes_explicit_transaction_lifecycle() {
    let cli = workspace_file("anvil-cli/src/cli/transaction.rs");
    let main = workspace_file("anvil-cli/src/main.rs");
    assert_contains_all(
        "transaction CLI surface",
        &format!("{}\n{}", cli, main),
        &[
            "pub enum TransactionCommands",
            "Begin {",
            "Commit {",
            "Rollback {",
            "Get { transaction_id: String }",
            "begin_transaction(request)",
            "commit_transaction(request)",
            "rollback_transaction(request)",
            "get_transaction(request)",
            "Commands::Transaction { command }",
            "wait_for_finalization",
            "ConsistencyMode::Finalised as i32",
        ],
    );

    let object_cli = workspace_file("anvil-cli/src/cli/object.rs");
    let stream_cli = workspace_file("anvil-cli/src/cli/stream.rs");
    assert_contains_all(
        "object and stream CLIs can stage writes inside explicit transactions",
        &format!("{}\n{}", object_cli, stream_cli),
        &[
            "transaction_id: Option<String>",
            "native_mutation_context(
    _ctx: &Context,
    token: &str,
    _bucket_name: &str,
    tag: &str,
    transaction_id: Option<String>",
            "transaction_id,",
        ],
    );
}
