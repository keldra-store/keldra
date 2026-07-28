use crate::{
    authz_journal::{self, AuthzTupleFilter},
    core_store::{
        CF_AUTHZ, CoreMetaTuplePart, CoreObjectRef, CoreStore, GetBlob, PutBlob,
        TABLE_AUTHZ_TUPLE_PAGE_ROW, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::{
        hash32,
        writer::{WriterFamily, canonical_logical_file_id},
    },
    persistence::AuthzTupleRecord,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use chrono::Utc;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const AUTHZ_DERIVED_USERSET_INDEX_ROW_SCHEMA: &str = "anvil.authz.derived_userset_index_row.v2";
const AUTHZ_DERIVED_USERSET_INDEX_ROW_KIND: &str = "derived_userset_index";
const AUTHZ_SOURCE_HASH_DOMAIN: &[u8] = b"anvil.authz.source-record-chain.v1\0";

pub const DEFAULT_DERIVED_USERSET_INDEX_ID: &str = "derived-userset-primary";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzDerivedUsersetIndex {
    pub version: u16,
    pub tenant_id: i64,
    pub derived_index_id: String,
    pub processed_revision: u64,
    pub source_record_count: u64,
    pub source_records_hash: String,
    pub generation: u64,
    pub entries: Vec<AuthzDerivedUsersetEntry>,
    pub built_at: String,
    pub index_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzDerivedUsersetEntry {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzDerivedUsersetIndexProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    derived_index_id: String,
    #[prost(uint64, tag = "4")]
    processed_revision: u64,
    #[prost(uint64, tag = "5")]
    source_record_count: u64,
    #[prost(string, tag = "6")]
    source_records_hash: String,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(message, repeated, tag = "8")]
    entries: Vec<AuthzDerivedUsersetEntryProto>,
    #[prost(string, tag = "9")]
    built_at: String,
    #[prost(string, tag = "10")]
    index_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzDerivedUsersetIndexRowProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    derived_index_id: String,
    #[prost(uint64, tag = "5")]
    processed_revision: u64,
    #[prost(uint64, tag = "6")]
    generation: u64,
    #[prost(uint64, tag = "7")]
    writer_generation: u64,
    #[prost(string, tag = "8")]
    source_records_hash: String,
    #[prost(string, tag = "9")]
    index_hash: String,
    #[prost(string, tag = "10")]
    core_object_ref_target: String,
    #[prost(string, tag = "11")]
    built_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzDerivedUsersetEntryProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    object_id: String,
    #[prost(string, tag = "3")]
    relation: String,
    #[prost(string, tag = "4")]
    subject_kind: String,
    #[prost(string, tag = "5")]
    subject_id: String,
    #[prost(string, tag = "6")]
    caveat_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleRecordHashProto {
    #[prost(int64, tag = "1")]
    revision: i64,
    #[prost(uint32, tag = "2")]
    revision_ordinal: u32,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    namespace: String,
    #[prost(string, tag = "5")]
    object_id: String,
    #[prost(string, tag = "6")]
    relation: String,
    #[prost(string, tag = "7")]
    subject_kind: String,
    #[prost(string, tag = "8")]
    subject_id: String,
    #[prost(string, tag = "9")]
    caveat_hash: String,
    #[prost(string, tag = "10")]
    operation: String,
    #[prost(string, tag = "11")]
    written_by: String,
    #[prost(string, tag = "12")]
    reason: String,
    #[prost(string, tag = "13")]
    mutation_id: String,
    #[prost(string, tag = "14")]
    record_hash: String,
    #[prost(string, tag = "15")]
    written_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TupleViewKey {
    namespace: String,
    object_id: String,
    relation: String,
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UsersetRef {
    namespace: String,
    object_id: String,
    relation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SubjectRef {
    kind: String,
    id: String,
    caveat_hash: String,
}

struct AuthzDerivedUsersetIndexRow {
    tenant_id: i64,
    derived_index_id: String,
    processed_revision: u64,
    generation: u64,
    writer_generation: u64,
    source_records_hash: String,
    index_hash: String,
    core_object_ref_target: String,
    built_at: String,
}

impl From<&AuthzTupleRecord> for TupleViewKey {
    fn from(record: &AuthzTupleRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn lookup_derived_userset_index_at_revision(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: u64,
) -> Result<Option<bool>> {
    let Some(index) =
        read_derived_userset_index(storage, mvcc, tenant_id, derived_index_id).await?
    else {
        return Ok(None);
    };
    if index.processed_revision != revision {
        return Ok(None);
    }
    let needle = AuthzDerivedUsersetEntry {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: caveat_hash.to_string(),
    };
    Ok(Some(index.entries.binary_search(&needle).is_ok()))
}

pub async fn list_derived_userset_objects_at_revision(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    namespace: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: u64,
) -> Result<Option<Vec<String>>> {
    let Some(index) =
        read_derived_userset_index(storage, mvcc, tenant_id, derived_index_id).await?
    else {
        return Ok(None);
    };
    if index.processed_revision != revision {
        return Ok(None);
    }

    let objects = index
        .entries
        .iter()
        .filter(|entry| {
            entry.namespace == namespace
                && entry.relation == relation
                && entry.subject_kind == subject_kind
                && entry.subject_id == subject_id
                && entry.caveat_hash == caveat_hash
        })
        .map(|entry| entry.object_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(Some(objects))
}

pub async fn rebuild_derived_userset_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<AuthzDerivedUsersetIndex> {
    let index =
        build_expected_derived_userset_index(storage, mvcc, tenant_id, derived_index_id).await?;
    write_derived_userset_index(storage, mvcc, &index).await?;
    Ok(index)
}

pub async fn advance_derived_userset_index_from_batch(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    batch_records: &[AuthzTupleRecord],
) -> Result<AuthzDerivedUsersetIndex> {
    let Some(target_revision) = batch_records
        .iter()
        .map(|record| {
            if record.tenant_id != tenant_id {
                return Err(anyhow!("authorization userset batch tenant mismatch"));
            }
            u64::try_from(record.revision)
                .map_err(|_| anyhow!("authorization userset revision must be nonnegative"))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
    else {
        return read_derived_userset_index(storage, mvcc, tenant_id, derived_index_id)
            .await?
            .ok_or_else(|| anyhow!("authorization userset index does not exist"));
    };

    let existing = match read_derived_userset_index(storage, mvcc, tenant_id, derived_index_id)
        .await?
    {
        Some(existing) if existing.processed_revision + 1 >= target_revision => existing,
        _ => {
            let all_records =
                authz_journal::collect_authz_tuple_log_for_rebuild(mvcc, tenant_id, None).await?;
            let rebuilt =
                build_derived_userset_index_from_records(tenant_id, derived_index_id, all_records)?;
            write_derived_userset_index(storage, mvcc, &rebuilt).await?;
            return Ok(rebuilt);
        }
    };

    if existing.processed_revision >= target_revision {
        return Ok(existing);
    }

    let impacted = impacted_usersets_from_projection(
        mvcc,
        tenant_id,
        i64::try_from(target_revision)
            .map_err(|_| anyhow!("authorization userset revision exceeds supported range"))?,
        batch_records,
    )
    .await?;
    let source_record_count = existing
        .source_record_count
        .checked_add(u64::try_from(batch_records.len())?)
        .ok_or_else(|| anyhow!("authorization source record count overflow"))?;
    let source_records_hash =
        advance_source_records_hash(&existing.source_records_hash, batch_records)?;
    if impacted.is_empty() {
        let mut advanced = existing;
        advanced.processed_revision = target_revision;
        advanced.source_record_count = source_record_count;
        advanced.source_records_hash = source_records_hash;
        advanced.generation = target_revision;
        advanced.built_at = Utc::now().to_rfc3339();
        advanced.index_hash = hash_derived_userset_index(&advanced)?;
        write_derived_userset_index(storage, mvcc, &advanced).await?;
        return Ok(advanced);
    }

    let mut entries = existing
        .entries
        .into_iter()
        .filter(|entry| {
            !impacted.contains(&UsersetRef {
                namespace: entry.namespace.clone(),
                object_id: entry.object_id.clone(),
                relation: entry.relation.clone(),
            })
        })
        .collect::<BTreeSet<_>>();
    for userset in &impacted {
        for subject in collect_userset_subjects_from_projection(
            storage,
            mvcc,
            tenant_id,
            userset,
            i64::try_from(target_revision)
                .map_err(|_| anyhow!("authorization userset revision exceeds supported range"))?,
        )
        .await?
        {
            entries.insert(AuthzDerivedUsersetEntry {
                namespace: userset.namespace.clone(),
                object_id: userset.object_id.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
            });
        }
    }

    let mut advanced = AuthzDerivedUsersetIndex {
        version: 1,
        tenant_id,
        derived_index_id: derived_index_id.to_string(),
        processed_revision: target_revision,
        source_record_count,
        source_records_hash,
        generation: target_revision,
        entries: entries.into_iter().collect(),
        built_at: Utc::now().to_rfc3339(),
        index_hash: String::new(),
    };
    advanced.index_hash = hash_derived_userset_index(&advanced)?;
    validate_derived_userset_index(&advanced, tenant_id, derived_index_id)?;
    write_derived_userset_index(storage, mvcc, &advanced).await?;
    Ok(advanced)
}

pub async fn build_expected_derived_userset_index(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<AuthzDerivedUsersetIndex> {
    let records = authz_journal::collect_authz_tuple_log_for_rebuild(mvcc, tenant_id, None).await?;
    build_derived_userset_index_from_records(tenant_id, derived_index_id, records)
}

pub(crate) async fn build_expected_derived_userset_index_at_revision(
    _storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    revision: u64,
) -> Result<AuthzDerivedUsersetIndex> {
    let revision = i64::try_from(revision)
        .map_err(|_| anyhow!("authorization userset revision exceeds supported range"))?;
    let records =
        authz_journal::collect_authz_tuple_log_for_rebuild(mvcc, tenant_id, Some(revision)).await?;
    build_derived_userset_index_from_records(tenant_id, derived_index_id, records)
}

pub async fn read_derived_userset_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<Option<AuthzDerivedUsersetIndex>> {
    let snapshot = mvcc.runtime.applied_version()?;
    read_derived_userset_index_at_snapshot(storage, mvcc, tenant_id, derived_index_id, snapshot)
        .await
}

async fn read_derived_userset_index_at_snapshot(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    snapshot: u64,
) -> Result<Option<AuthzDerivedUsersetIndex>> {
    let Some(row) =
        read_derived_userset_index_row_at_snapshot(mvcc, tenant_id, derived_index_id, snapshot)?
    else {
        return Ok(None);
    };
    let store = CoreStore::new(storage.clone()).await?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&row.core_object_ref_target)?,
        })
        .await?;
    let index = decode_derived_userset_index(&bytes)?;
    validate_derived_userset_index(&index, tenant_id, derived_index_id)?;
    validate_derived_userset_index_row_matches(&row, &index)?;
    Ok(Some(index))
}

pub async fn write_derived_userset_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index: &AuthzDerivedUsersetIndex,
) -> Result<()> {
    write_derived_userset_index_with_predicates(storage, mvcc, index, &[], None).await
}

pub(crate) async fn write_derived_userset_index_with_predicates(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index: &AuthzDerivedUsersetIndex,
    additional_predicates: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
    task_publication_assignment: Option<crate::mvcc_worker_authority::AssignmentGuard>,
) -> Result<()> {
    validate_derived_userset_index(index, index.tenant_id, &index.derived_index_id)?;
    let bytes = encode_derived_userset_index(index)?;
    let stable_name =
        derived_userset_index_logical_file_id(index.tenant_id, &index.derived_index_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let writer_generation = index.generation.max(1);
    let logical_file_id = canonical_logical_file_id(
        WriterFamily::Authz,
        writer_generation,
        &stable_name,
        &hash32(&bytes),
    );
    let object_ref = store
        .put_format_blob(
            PutBlob {
                logical_name: logical_file_id,
                bytes,
                boundary_values: Vec::new(),
                mutation_id: format!(
                    "authz-derived-userset-index:{}:{}:{}",
                    index.tenant_id, index.derived_index_id, index.generation
                ),
                region_id: "local".to_string(),
            },
            WriterFamily::Authz,
        )
        .await?;
    write_derived_userset_index_row_mvcc(
        mvcc,
        index,
        writer_generation,
        &object_ref,
        additional_predicates,
        task_publication_assignment,
    )
    .await?;
    Ok(())
}

fn read_derived_userset_index_row_at_snapshot(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    snapshot: u64,
) -> Result<Option<AuthzDerivedUsersetIndexRow>> {
    let key = derived_userset_index_logical_key(tenant_id, derived_index_id)?;
    let Some(payload) = mvcc.runtime.read_at(&key, snapshot)?.map(|row| row.value) else {
        return Ok(None);
    };
    let row = decode_derived_userset_index_row(&payload)?;
    if row.tenant_id != tenant_id || row.derived_index_id != derived_index_id {
        return Err(anyhow!(
            "authorization userset index MVCC row scope mismatch"
        ));
    }
    Ok(Some(row))
}

async fn write_derived_userset_index_row_mvcc(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    index: &AuthzDerivedUsersetIndex,
    writer_generation: u64,
    object_ref: &CoreObjectRef,
    additional_predicates: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
    task_publication_assignment: Option<crate::mvcc_worker_authority::AssignmentGuard>,
) -> Result<()> {
    let row = AuthzDerivedUsersetIndexRow {
        tenant_id: index.tenant_id,
        derived_index_id: index.derived_index_id.clone(),
        processed_revision: index.processed_revision,
        generation: index.generation,
        writer_generation,
        source_records_hash: index.source_records_hash.clone(),
        index_hash: index.index_hash.clone(),
        core_object_ref_target: encode_core_object_ref_target(object_ref)?,
        built_at: index.built_at.clone(),
    };
    validate_derived_userset_index_row(&row)?;
    let logical_key = derived_userset_index_logical_key(index.tenant_id, &index.derived_index_id)?;
    let payload = encode_derived_userset_index_row(&row)?;
    if mvcc.read_latest_value(&logical_key)?.as_deref() == Some(payload.as_slice()) {
        return Ok(());
    }

    let assignment = if let Some(assignment) = task_publication_assignment {
        let task_queue_partition =
            crate::mvcc_worker_authority::work_partition_id("task-queue", "global")?;
        if assignment.partition_id != task_queue_partition {
            return Err(anyhow!(
                "authorization task publication requires the task queue assignment"
            ));
        }
        mvcc.validate_assignment(&assignment)?;
        assignment
    } else {
        mvcc.reconcile_authz_tuple_assignment(index.tenant_id)
            .await?
            .ok_or_else(|| {
                anyhow!("this node is not the assigned authorization userset index publisher")
            })?
    };
    let principal = format!("authz-derived-userset:tenant/{}", index.tenant_id);
    let now_unix_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            &principal,
            derived_userset_index_transaction_id(index),
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now_unix_ms,
        )
        .await?;
    let status = mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now_unix_ms)?;
    if status.state == "open" {
        let observed = mvcc
            .runtime
            .read_at(&logical_key, handle.snapshot_version)?;
        let predicate = observed
            .as_ref()
            .map(|row| {
                crate::mvcc_transaction::PredicateKind::ValueHash(
                    *blake3::hash(&row.value).as_bytes(),
                )
            })
            .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
        mvcc.stage_product_mutations(
            &handle.transaction_id,
            &principal,
            vec![crate::mvcc_product::ProductMutation::put(
                logical_key.clone(),
                payload,
            )],
            now_unix_ms,
        )?;
        mvcc.stage_predicate(
            &handle.transaction_id,
            &principal,
            logical_key,
            predicate,
            now_unix_ms,
        )?;
        for (key, predicate) in additional_predicates {
            mvcc.stage_predicate(
                &handle.transaction_id,
                &principal,
                key.clone(),
                predicate.clone(),
                now_unix_ms,
            )?;
        }
        mvcc.stage_assignment_guard(&handle.transaction_id, &principal, &assignment, now_unix_ms)?;
    }
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            &principal,
            u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => Err(anyhow!(
            "authorization userset publication aborted: {reason:?}"
        )),
    }
}

fn build_derived_userset_index_from_records(
    tenant_id: i64,
    derived_index_id: &str,
    mut records: Vec<AuthzTupleRecord>,
) -> Result<AuthzDerivedUsersetIndex> {
    records.sort_by_key(|record| record.revision);
    let source_record_count = records.len() as u64;
    let processed_revision = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0)
        .max(0) as u64;
    let source_records_hash = source_records_hash(&records)?;
    let current = current_tuple_map(records.clone());

    let mut usersets = BTreeSet::new();
    for record in current.values() {
        usersets.insert(UsersetRef {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
        });
    }

    let mut entries = BTreeSet::new();
    for userset in usersets {
        let mut visited = BTreeSet::new();
        for subject in expand_userset_subjects(&current, &userset, &mut visited)? {
            entries.insert(AuthzDerivedUsersetEntry {
                namespace: userset.namespace.clone(),
                object_id: userset.object_id.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
            });
        }
    }

    let mut index = AuthzDerivedUsersetIndex {
        version: 1,
        tenant_id,
        derived_index_id: derived_index_id.to_string(),
        processed_revision,
        source_record_count,
        source_records_hash,
        generation: processed_revision,
        entries: entries.into_iter().collect(),
        built_at: Utc::now().to_rfc3339(),
        index_hash: String::new(),
    };
    index.index_hash = hash_derived_userset_index(&index)?;
    Ok(index)
}

fn current_tuple_map(records: Vec<AuthzTupleRecord>) -> BTreeMap<TupleViewKey, AuthzTupleRecord> {
    let mut current = BTreeMap::new();
    for record in records {
        current.insert(TupleViewKey::from(&record), record);
    }
    current
}

async fn impacted_usersets_from_projection(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    revision: i64,
    batch_records: &[AuthzTupleRecord],
) -> Result<BTreeSet<UsersetRef>> {
    let mut impacted = BTreeSet::new();
    let mut stack = Vec::new();
    for record in batch_records {
        let userset = UsersetRef {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
        };
        if impacted.insert(userset.clone()) {
            stack.push(userset);
        }
    }

    while let Some(userset) = stack.pop() {
        if impacted.len() > 4_096 {
            return Err(anyhow!(
                "AuthzGraphNodeLimitExceeded: derived userset impact exceeds 4096 nodes"
            ));
        }
        let subject_id = format!(
            "{}/{}#{}",
            userset.namespace, userset.object_id, userset.relation
        );
        let filter = AuthzTupleFilter {
            subject_kind: Some("userset".to_string()),
            subject_id: Some(subject_id),
            caveat_hash: Some(String::new()),
            ..AuthzTupleFilter::default()
        };
        let mut after_tuple_key = None;
        loop {
            let page = authz_journal::page_current_authz_tuples(
                mvcc,
                tenant_id,
                &filter,
                revision,
                after_tuple_key.as_deref(),
                256,
            )
            .await
            .map_err(anyhow::Error::new)?;
            for record in page.records {
                let parent = UsersetRef {
                    namespace: record.namespace,
                    object_id: record.object_id,
                    relation: record.relation,
                };
                if impacted.insert(parent.clone()) {
                    stack.push(parent);
                }
            }
            let Some(next_tuple_key) = page.next_tuple_key else {
                break;
            };
            if after_tuple_key.as_ref() == Some(&next_tuple_key) {
                return Err(anyhow!(
                    "authorization userset impact page did not advance its continuation"
                ));
            }
            after_tuple_key = Some(next_tuple_key);
        }
    }
    Ok(impacted)
}

async fn collect_userset_subjects_from_projection(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    userset: &UsersetRef,
    revision: i64,
) -> Result<BTreeSet<SubjectRef>> {
    let mut subjects = BTreeSet::new();
    let mut after_position = None;
    loop {
        let page = authz_journal::list_current_authz_subjects_page(
            storage,
            mvcc,
            tenant_id,
            &userset.namespace,
            &userset.object_id,
            &userset.relation,
            None,
            revision,
            after_position.as_deref(),
            1_000,
        )
        .await?;
        subjects.extend(page.subjects.into_iter().map(|subject| SubjectRef {
            kind: subject.subject_kind,
            id: subject.subject_id,
            caveat_hash: subject.caveat_hash,
        }));
        let Some(next_position) = page.next_subject_position else {
            break;
        };
        if after_position.as_ref() == Some(&next_position) {
            return Err(anyhow!(
                "authorization userset subject page did not advance its continuation"
            ));
        }
        after_position = Some(next_position);
    }
    Ok(subjects)
}

fn expand_userset_subjects(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    userset: &UsersetRef,
    visited: &mut BTreeSet<UsersetRef>,
) -> Result<BTreeSet<SubjectRef>> {
    if !visited.insert(userset.clone()) {
        return Ok(BTreeSet::new());
    }

    let mut subjects = BTreeSet::new();
    for record in current.values() {
        if record.namespace != userset.namespace
            || record.object_id != userset.object_id
            || record.relation != userset.relation
            || record.operation != "add"
        {
            continue;
        }
        if record.subject_kind == "userset" {
            if !record.caveat_hash.is_empty() {
                continue;
            }
            let Some(next) = parse_userset_subject(&record.subject_id)? else {
                continue;
            };
            subjects.extend(expand_userset_subjects(current, &next, visited)?);
        } else {
            subjects.insert(SubjectRef {
                kind: record.subject_kind.clone(),
                id: record.subject_id.clone(),
                caveat_hash: record.caveat_hash.clone(),
            });
        }
    }

    visited.remove(userset);
    Ok(subjects)
}

fn parse_userset_subject(value: &str) -> Result<Option<UsersetRef>> {
    let Some((namespace, rest)) = value.split_once('/') else {
        return Ok(None);
    };
    let Some((object_id, relation)) = rest.rsplit_once('#') else {
        return Ok(None);
    };
    if namespace.is_empty()
        || object_id.is_empty()
        || relation.is_empty()
        || namespace.chars().any(char::is_control)
        || object_id.chars().any(char::is_control)
        || relation.chars().any(char::is_control)
    {
        return Err(anyhow!("invalid userset subject reference"));
    }
    Ok(Some(UsersetRef {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
    }))
}

fn validate_derived_userset_index(
    index: &AuthzDerivedUsersetIndex,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<()> {
    if index.version != 1 {
        return Err(anyhow!("unsupported authorization userset index version"));
    }
    if index.tenant_id != tenant_id || index.derived_index_id != derived_index_id {
        return Err(anyhow!("authorization userset index scope mismatch"));
    }
    if index.generation < index.processed_revision {
        return Err(anyhow!("authorization userset index generation is stale"));
    }
    let mut sorted = index.entries.clone();
    sorted.sort();
    sorted.dedup();
    if sorted != index.entries {
        return Err(anyhow!(
            "authorization userset index entries must be sorted and unique"
        ));
    }
    let expected = hash_derived_userset_index(index)?;
    if expected != index.index_hash {
        return Err(anyhow!("authorization userset index hash mismatch"));
    }
    Ok(())
}

fn validate_derived_userset_index_row(row: &AuthzDerivedUsersetIndexRow) -> Result<()> {
    validate_derived_userset_index_key(row.tenant_id, &row.derived_index_id)?;
    if row.generation < row.processed_revision {
        return Err(anyhow!(
            "authorization userset index MVCC row generation is stale"
        ));
    }
    if row.writer_generation == 0 {
        return Err(anyhow!(
            "authorization userset index MVCC row writer generation must be nonzero"
        ));
    }
    validate_hash_hex32(&row.source_records_hash, "source_records_hash")?;
    validate_hash_hex32(&row.index_hash, "index_hash")?;
    if !row.core_object_ref_target.starts_with("core-object-ref:") {
        return Err(anyhow!(
            "authorization userset index MVCC row must point at an immutable object"
        ));
    }
    if row.built_at.is_empty() {
        return Err(anyhow!(
            "authorization userset index MVCC row built_at must not be empty"
        ));
    }
    Ok(())
}

fn validate_derived_userset_index_row_matches(
    row: &AuthzDerivedUsersetIndexRow,
    index: &AuthzDerivedUsersetIndex,
) -> Result<()> {
    if row.tenant_id != index.tenant_id
        || row.derived_index_id != index.derived_index_id
        || row.processed_revision != index.processed_revision
        || row.generation != index.generation
        || row.writer_generation != index.generation.max(1)
        || row.source_records_hash != index.source_records_hash
        || row.index_hash != index.index_hash
        || row.built_at != index.built_at
    {
        return Err(anyhow!(
            "authorization userset index MVCC row does not match payload"
        ));
    }
    Ok(())
}

fn encode_derived_userset_index(index: &AuthzDerivedUsersetIndex) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&derived_userset_index_to_proto(
        index,
    )))
}

fn decode_derived_userset_index(bytes: &[u8]) -> Result<AuthzDerivedUsersetIndex> {
    derived_userset_index_from_proto(decode_deterministic_proto::<AuthzDerivedUsersetIndexProto>(
        bytes,
        "authorization derived userset index",
    )?)
}

fn encode_derived_userset_index_row(row: &AuthzDerivedUsersetIndexRow) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(
        &AuthzDerivedUsersetIndexRowProto {
            schema: AUTHZ_DERIVED_USERSET_INDEX_ROW_SCHEMA.to_string(),
            tenant_id: row.tenant_id,
            derived_index_id: row.derived_index_id.clone(),
            processed_revision: row.processed_revision,
            generation: row.generation,
            writer_generation: row.writer_generation,
            source_records_hash: row.source_records_hash.clone(),
            index_hash: row.index_hash.clone(),
            core_object_ref_target: row.core_object_ref_target.clone(),
            built_at: row.built_at.clone(),
        },
    ))
}

fn decode_derived_userset_index_row(bytes: &[u8]) -> Result<AuthzDerivedUsersetIndexRow> {
    let proto = decode_deterministic_proto::<AuthzDerivedUsersetIndexRowProto>(
        bytes,
        "authorization derived userset index row",
    )?;
    if proto.schema != AUTHZ_DERIVED_USERSET_INDEX_ROW_SCHEMA {
        return Err(anyhow!(
            "authorization derived userset index row schema mismatch"
        ));
    }
    let row = AuthzDerivedUsersetIndexRow {
        tenant_id: proto.tenant_id,
        derived_index_id: proto.derived_index_id,
        processed_revision: proto.processed_revision,
        generation: proto.generation,
        writer_generation: proto.writer_generation,
        source_records_hash: proto.source_records_hash,
        index_hash: proto.index_hash,
        core_object_ref_target: proto.core_object_ref_target,
        built_at: proto.built_at,
    };
    validate_derived_userset_index_row(&row)?;
    Ok(row)
}

fn derived_userset_index_transaction_id(index: &AuthzDerivedUsersetIndex) -> String {
    format!(
        "authz-derived-userset:{}:{}:{}:{}",
        index.tenant_id, index.derived_index_id, index.generation, index.index_hash
    )
}

fn derived_userset_index_to_proto(
    index: &AuthzDerivedUsersetIndex,
) -> AuthzDerivedUsersetIndexProto {
    AuthzDerivedUsersetIndexProto {
        version: u32::from(index.version),
        tenant_id: index.tenant_id,
        derived_index_id: index.derived_index_id.clone(),
        processed_revision: index.processed_revision,
        source_record_count: index.source_record_count,
        source_records_hash: index.source_records_hash.clone(),
        generation: index.generation,
        entries: index.entries.iter().map(userset_entry_to_proto).collect(),
        built_at: index.built_at.clone(),
        index_hash: index.index_hash.clone(),
    }
}

fn derived_userset_index_from_proto(
    proto: AuthzDerivedUsersetIndexProto,
) -> Result<AuthzDerivedUsersetIndex> {
    Ok(AuthzDerivedUsersetIndex {
        version: u16::try_from(proto.version)
            .map_err(|_| anyhow!("authorization userset index version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        derived_index_id: proto.derived_index_id,
        processed_revision: proto.processed_revision,
        source_record_count: proto.source_record_count,
        source_records_hash: proto.source_records_hash,
        generation: proto.generation,
        entries: proto
            .entries
            .into_iter()
            .map(userset_entry_from_proto)
            .collect(),
        built_at: proto.built_at,
        index_hash: proto.index_hash,
    })
}

fn userset_entry_to_proto(entry: &AuthzDerivedUsersetEntry) -> AuthzDerivedUsersetEntryProto {
    AuthzDerivedUsersetEntryProto {
        namespace: entry.namespace.clone(),
        object_id: entry.object_id.clone(),
        relation: entry.relation.clone(),
        subject_kind: entry.subject_kind.clone(),
        subject_id: entry.subject_id.clone(),
        caveat_hash: entry.caveat_hash.clone(),
    }
}

fn userset_entry_from_proto(proto: AuthzDerivedUsersetEntryProto) -> AuthzDerivedUsersetEntry {
    AuthzDerivedUsersetEntry {
        namespace: proto.namespace,
        object_id: proto.object_id,
        relation: proto.relation,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        caveat_hash: proto.caveat_hash,
    }
}

fn tuple_record_hash_to_proto(record: &AuthzTupleRecord) -> AuthzTupleRecordHashProto {
    AuthzTupleRecordHashProto {
        revision: record.revision,
        revision_ordinal: record.revision_ordinal,
        tenant_id: record.tenant_id,
        namespace: record.namespace.clone(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: record.subject_id.clone(),
        caveat_hash: record.caveat_hash.clone(),
        operation: record.operation.clone(),
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        mutation_id: record.mutation_id.to_string(),
        record_hash: record.record_hash.clone(),
        written_at: record.written_at.to_rfc3339(),
    }
}

fn hash_derived_userset_index(index: &AuthzDerivedUsersetIndex) -> Result<String> {
    let mut unsigned = index.clone();
    unsigned.index_hash.clear();
    Ok(hex::encode(hash32(&encode_derived_userset_index(
        &unsigned,
    )?)))
}

fn source_records_hash(records: &[AuthzTupleRecord]) -> Result<String> {
    let initial = hex::encode(hash32(AUTHZ_SOURCE_HASH_DOMAIN));
    advance_source_records_hash(&initial, records)
}

fn advance_source_records_hash(
    previous_hash: &str,
    records: &[AuthzTupleRecord],
) -> Result<String> {
    let mut state = hex::decode(previous_hash)
        .map_err(|_| anyhow!("authorization source records hash is not valid hex"))?;
    if state.len() != 32 {
        return Err(anyhow!(
            "authorization source records hash must contain 32 bytes"
        ));
    }
    let mut records = records.to_vec();
    records.sort_by(|left, right| {
        left.revision
            .cmp(&right.revision)
            .then(left.revision_ordinal.cmp(&right.revision_ordinal))
            .then(left.record_hash.cmp(&right.record_hash))
    });
    for record in records {
        let encoded = encode_deterministic_proto(&tuple_record_hash_to_proto(&record));
        let mut input =
            Vec::with_capacity(AUTHZ_SOURCE_HASH_DOMAIN.len() + state.len() + 8 + encoded.len());
        input.extend_from_slice(AUTHZ_SOURCE_HASH_DOMAIN);
        input.extend_from_slice(&state);
        input.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
        input.extend_from_slice(&encoded);
        state = hash32(&input).to_vec();
    }
    Ok(hex::encode(state))
}

fn derived_userset_index_logical_file_id(tenant_id: i64, derived_index_id: &str) -> Result<String> {
    validate_derived_userset_index_key(tenant_id, derived_index_id)?;
    Ok(format!(
        "authz-derived-userset-index/tenant-{tenant_id}/index-{derived_index_id}"
    ))
}

fn derived_userset_index_tuple_key(tenant_id: i64, derived_index_id: &str) -> Result<Vec<u8>> {
    validate_derived_userset_index_key(tenant_id, derived_index_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_DERIVED_USERSET_INDEX_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Utf8(derived_index_id),
    ])
}

fn derived_userset_index_logical_key(
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_AUTHZ,
        TABLE_AUTHZ_TUPLE_PAGE_ROW,
        &derived_userset_index_tuple_key(tenant_id, derived_index_id)?,
    )
}

fn validate_derived_userset_index_key(tenant_id: i64, derived_index_id: &str) -> Result<()> {
    if tenant_id < 0 {
        return Err(anyhow!(
            "authorization userset index tenant id must be nonnegative"
        ));
    }
    if derived_index_id.is_empty()
        || derived_index_id == "."
        || derived_index_id == ".."
        || derived_index_id.contains('/')
        || derived_index_id.contains('\\')
        || derived_index_id.contains(':')
        || derived_index_id.chars().any(char::is_control)
    {
        return Err(anyhow!("derived_index_id is not a safe component"));
    }
    Ok(())
}

fn validate_hash_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
}
