use crate::core_store::{
    CF_MATERIALISATION, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
    TABLE_WRITER_SEGMENT_ROW, core_meta_root_key_hash, core_meta_tuple_key,
    decode_deterministic_proto, encode_deterministic_proto, sha256_hex,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Weak},
};

mod head;

const WRITER_SEGMENT_ROW_SCHEMA: &str = "anvil.coremeta.writer_segment_locator.v1";
pub const WRITER_SEGMENT_PAGE_MAX: usize = 1000;

static WRITER_LOCKS: LazyLock<std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriterSegmentCatalogRecord {
    pub family: String,
    pub scope: String,
    pub segment_ref: String,
    pub core_object_ref_target: String,
    pub segment_hash: String,
    pub segment_length: u64,
    pub generation: u64,
    pub source_cursor: u64,
    pub created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterSegmentCatalogPage {
    pub records: Vec<WriterSegmentCatalogRecord>,
    pub next_generation: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
struct WriterSegmentCatalogRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    family: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(string, tag = "5")]
    segment_ref: String,
    #[prost(string, tag = "6")]
    core_object_ref_target: String,
    #[prost(string, tag = "7")]
    segment_hash: String,
    #[prost(uint64, tag = "8")]
    segment_length: u64,
    #[prost(uint64, tag = "9")]
    generation: u64,
    #[prost(uint64, tag = "10")]
    source_cursor: u64,
    #[prost(uint64, tag = "11")]
    created_at_unix_nanos: u64,
    #[prost(uint64, tag = "12")]
    publication_generation: u64,
}

pub async fn write_writer_segment_catalog_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    record: &WriterSegmentCatalogRecord,
    additional_preconditions: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
) -> Result<()> {
    write_writer_segment_catalog_record_with_assignment(
        mvcc,
        record,
        additional_preconditions,
        None,
    )
    .await
}

pub(crate) async fn write_writer_segment_catalog_record_with_assignment(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    record: &WriterSegmentCatalogRecord,
    additional_preconditions: &[(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )],
    assignment: Option<&crate::mvcc_worker_authority::AssignmentGuard>,
) -> Result<()> {
    validate_record(record)?;
    let write_lock = writer_lock(&record.family, &record.scope)?;
    let _guard = write_lock.lock().await;
    if let Some(existing) =
        read_record_at_generation(mvcc, &record.family, &record.scope, record.generation)?
    {
        if existing == *record {
            return Ok(());
        }
        bail!("writer segment generation already identifies a different segment");
    }

    let head_key = head::logical_key(&record.family, &record.scope)?;
    let current = mvcc
        .read_latest_value(&head_key)?
        .map(|payload| head::decode(&payload, &record.family, &record.scope))
        .transpose()?;
    let (mutations, mut predicates, transaction_id) = plan_mutation(record, current.as_ref())?;
    predicates.extend_from_slice(additional_preconditions);
    let principal = writer_realm(&record.family, &record.scope);
    let now_unix_ms = u64::try_from(chrono::Utc::now().timestamp_millis())
        .map_err(|_| anyhow!("writer segment publication timestamp predates Unix epoch"))?;
    if let Some(assignment) = assignment {
        mvcc.autocommit_product_mutations_with_predicates_and_assignment(
            &principal,
            &transaction_id,
            mutations,
            predicates,
            assignment,
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            now_unix_ms,
        )
        .await?;
    } else {
        mvcc.autocommit_product_mutations_with_predicates(
            &principal,
            &transaction_id,
            mutations,
            predicates,
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            now_unix_ms,
        )
        .await?;
    }
    Ok(())
}

fn plan_mutation(
    record: &WriterSegmentCatalogRecord,
    current: Option<&head::WriterHead>,
) -> Result<(
    Vec<crate::mvcc_product::ProductMutation>,
    Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    String,
)> {
    let publication_generation = current
        .map(|head| {
            head.publication_generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("writer publication generation overflow"))
        })
        .transpose()?
        .unwrap_or(1);
    let logical_head = current
        .filter(|head| head.record.generation > record.generation)
        .map(|head| &head.record)
        .unwrap_or(record);
    let segment_key = segment_logical_key(&record.family, &record.scope, record.generation)?;
    let head_key = head::logical_key(&record.family, &record.scope)?;
    let transaction_id = logical_transaction_id(record);
    let segment_payload = encode_record(record, publication_generation, &transaction_id)?;
    let head_payload = head::encode(
        logical_head,
        publication_generation,
        &transaction_id,
        logical_head
            .created_at_unix_nanos
            .max(record.created_at_unix_nanos),
    )?;
    Ok((
        vec![
            crate::mvcc_product::ProductMutation::put(segment_key.clone(), segment_payload),
            crate::mvcc_product::ProductMutation::put(head_key.clone(), head_payload),
        ],
        vec![
            (segment_key, crate::mvcc_transaction::PredicateKind::Absent),
            (
                head_key,
                current.map_or(crate::mvcc_transaction::PredicateKind::Absent, |head| {
                    crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(&head.payload).as_bytes(),
                    )
                }),
            ),
        ],
        transaction_id,
    ))
}

pub async fn read_writer_segment_catalog_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    family: &str,
    scope: &str,
    generation: u64,
    segment_ref: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    validate_scope_components(family, scope)?;
    if generation == 0 {
        bail!("writer segment generation must be nonzero");
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let Some(record) = read_record_at_generation_at(mvcc, family, scope, generation, snapshot)?
    else {
        return Ok(None);
    };
    validate_scope(&record, family, scope, generation, segment_ref)?;
    Ok(Some(record))
}

fn read_record_at_generation(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    family: &str,
    scope: &str,
    generation: u64,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    let Some(payload) = mvcc.read_latest_value(&segment_logical_key(family, scope, generation)?)?
    else {
        return Ok(None);
    };
    decode_record(&payload).map(Some)
}

fn read_record_at_generation_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    family: &str,
    scope: &str,
    generation: u64,
    snapshot: u64,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    mvcc.runtime
        .read_at(&segment_logical_key(family, scope, generation)?, snapshot)?
        .map(|row| decode_record(&row.value))
        .transpose()
}

pub async fn latest_writer_segment_catalog_record(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    family: &str,
    scope: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    let snapshot = mvcc.runtime.applied_version()?;
    Ok(mvcc
        .runtime
        .read_at(&head::logical_key(family, scope)?, snapshot)?
        .map(|row| head::decode(&row.value, family, scope))
        .transpose()?
        .map(|head| head.record))
}

pub async fn page_writer_segment_catalog_records(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    family: &str,
    scope: &str,
    after_generation: u64,
    through_generation: u64,
    limit: usize,
) -> Result<WriterSegmentCatalogPage> {
    validate_scope_components(family, scope)?;
    if limit == 0 || limit > WRITER_SEGMENT_PAGE_MAX {
        bail!("writer segment page limit must be between 1 and {WRITER_SEGMENT_PAGE_MAX}");
    }
    if through_generation <= after_generation {
        return Ok(WriterSegmentCatalogPage {
            records: Vec::new(),
            next_generation: None,
        });
    }
    let snapshot = mvcc.runtime.applied_version()?;
    let prefix = crate::mvcc_product::coremeta_application_prefix(
        CF_MATERIALISATION,
        &tuple_prefix(family, scope)?,
    )?;
    let mut records = mvcc
        .runtime
        .scan_table_prefix_at(TABLE_WRITER_SEGMENT_ROW, &prefix, snapshot)?
        .into_iter()
        .map(|(_, row)| {
            let record = decode_record(&row.value)?;
            if record.family != family || record.scope != scope {
                bail!("writer segment catalog row scope mismatch");
            }
            Ok(record)
        })
        .collect::<Result<Vec<_>>>()?;
    records.retain(|record| {
        record.generation > after_generation && record.generation <= through_generation
    });
    let scan_limit = limit
        .checked_add(1)
        .ok_or_else(|| anyhow!("writer segment page limit overflow"))?;
    records.truncate(scan_limit);
    let has_more = records.len() > limit;
    if has_more {
        records.truncate(limit);
    }
    let next_generation = has_more
        .then(|| records.last().map(|record| record.generation))
        .flatten();
    Ok(WriterSegmentCatalogPage {
        records,
        next_generation,
    })
}

fn encode_record(
    record: &WriterSegmentCatalogRecord,
    publication_generation: u64,
    publication_transaction_id: &str,
) -> Result<Vec<u8>> {
    validate_record(record)?;
    if publication_generation == 0 || publication_transaction_id.is_empty() {
        bail!("writer publication generation and transaction id must be present");
    }
    Ok(encode_deterministic_proto(
        &WriterSegmentCatalogRecordProto {
            common: Some(row_common(
                record,
                publication_generation,
                publication_transaction_id,
            )),
            schema: WRITER_SEGMENT_ROW_SCHEMA.to_string(),
            family: record.family.clone(),
            scope: record.scope.clone(),
            segment_ref: record.segment_ref.clone(),
            core_object_ref_target: record.core_object_ref_target.clone(),
            segment_hash: record.segment_hash.clone(),
            segment_length: record.segment_length,
            generation: record.generation,
            source_cursor: record.source_cursor,
            created_at_unix_nanos: record.created_at_unix_nanos,
            publication_generation,
        },
    ))
}

fn decode_record(bytes: &[u8]) -> Result<WriterSegmentCatalogRecord> {
    Ok(decode_record_with_publication(bytes)?.0)
}

fn decode_record_with_publication(
    bytes: &[u8],
) -> Result<(WriterSegmentCatalogRecord, u64, String)> {
    let proto = decode_deterministic_proto::<WriterSegmentCatalogRecordProto>(
        bytes,
        "writer segment catalog row",
    )?;
    if proto.schema != WRITER_SEGMENT_ROW_SCHEMA {
        bail!("writer segment catalog row has invalid schema");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("writer segment catalog row is missing CoreMeta common"))?;
    let record = WriterSegmentCatalogRecord {
        family: proto.family,
        scope: proto.scope,
        segment_ref: proto.segment_ref,
        core_object_ref_target: proto.core_object_ref_target,
        segment_hash: proto.segment_hash,
        segment_length: proto.segment_length,
        generation: proto.generation,
        source_cursor: proto.source_cursor,
        created_at_unix_nanos: proto.created_at_unix_nanos,
    };
    validate_record(&record)?;
    if proto.publication_generation == 0 {
        bail!("writer segment publication generation must be nonzero");
    }
    validate_common(&record, &common.transaction_id, common)?;
    Ok((
        record,
        proto.publication_generation,
        common.transaction_id.clone(),
    ))
}

fn validate_record(record: &WriterSegmentCatalogRecord) -> Result<()> {
    validate_scope_components(&record.family, &record.scope)?;
    require_nonempty(&record.segment_ref, "segment_ref")?;
    require_nonempty(&record.core_object_ref_target, "core_object_ref_target")?;
    if !record
        .core_object_ref_target
        .starts_with("core-object-ref:")
    {
        bail!("writer segment catalog row must carry a CoreStore object ref target");
    }
    validate_hex32(&record.segment_hash, "segment_hash")?;
    if record.segment_length == 0 {
        bail!("writer segment catalog row segment length must be nonzero");
    }
    if record.generation == 0 {
        bail!("writer segment catalog row generation must be nonzero");
    }
    Ok(())
}

fn validate_scope(
    record: &WriterSegmentCatalogRecord,
    family: &str,
    scope: &str,
    generation: u64,
    segment_ref: &str,
) -> Result<()> {
    if record.family != family
        || record.scope != scope
        || record.generation != generation
        || record.segment_ref != segment_ref
    {
        bail!("writer segment catalog row scope mismatch");
    }
    Ok(())
}

fn validate_scope_components(family: &str, scope: &str) -> Result<()> {
    require_nonempty(family, "family")?;
    require_nonempty(scope, "scope")
}

fn validate_common(
    record: &WriterSegmentCatalogRecord,
    publication_transaction_id: &str,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    validate_writer_common(
        &record.family,
        &record.scope,
        publication_transaction_id,
        record.created_at_unix_nanos,
        common,
    )
    .map_err(|_| anyhow!("writer segment catalog CoreMeta common mismatch"))
}

fn validate_writer_common(
    family: &str,
    scope: &str,
    transaction_id: &str,
    created_at_unix_nanos: u64,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.realm_id != writer_realm(family, scope)
        || common.root_key_hash != writer_root_key_hash(family, scope)
        || common.root_generation == 0
        || transaction_id.is_empty()
        || common.transaction_id != transaction_id
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.created_at_unix_nanos != created_at_unix_nanos
        || common.payload_schema_version != 1
    {
        bail!("writer CoreMeta common mismatch");
    }
    Ok(())
}

fn row_common(
    record: &WriterSegmentCatalogRecord,
    publication_generation: u64,
    publication_transaction_id: &str,
) -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: writer_realm(&record.family, &record.scope),
        root_key_hash: writer_root_key_hash(&record.family, &record.scope),
        root_generation: publication_generation,
        transaction_id: publication_transaction_id.to_string(),
        visibility_state: CoreMetaVisibilityState::Committed as i32,
        created_at_unix_nanos: record.created_at_unix_nanos,
        payload_schema_version: 1,
    }
}

fn tuple_key(family: &str, scope: &str, generation: u64) -> Result<Vec<u8>> {
    validate_scope_components(family, scope)?;
    if generation == 0 {
        bail!("writer segment generation must be nonzero");
    }
    let scope_hash = writer_scope_hash(family, scope);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(family),
        CoreMetaTuplePart::Hash(&scope_hash),
        CoreMetaTuplePart::U64(generation),
    ])
}

fn tuple_prefix(family: &str, scope: &str) -> Result<Vec<u8>> {
    validate_scope_components(family, scope)?;
    let scope_hash = writer_scope_hash(family, scope);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(family),
        CoreMetaTuplePart::Hash(&scope_hash),
    ])
}

fn segment_logical_key(
    family: &str,
    scope: &str,
    generation: u64,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    crate::mvcc_product::coremeta_logical_key(
        CF_MATERIALISATION,
        TABLE_WRITER_SEGMENT_ROW,
        &tuple_key(family, scope, generation)?,
    )
}

fn writer_lock(family: &str, scope: &str) -> Result<Arc<tokio::sync::Mutex<()>>> {
    let key = writer_scope_hash(family, scope);
    let mut locks = WRITER_LOCKS
        .lock()
        .map_err(|_| anyhow!("writer segment lock map is poisoned"))?;
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}

fn writer_scope_hash(family: &str, scope: &str) -> String {
    core_meta_root_key_hash(&format!("writer-scope/{family}/{scope}"))
}

fn writer_root_key_hash(family: &str, scope: &str) -> String {
    writer_scope_hash(family, scope)
}

fn writer_realm(family: &str, scope: &str) -> String {
    format!("writer/{family}/{scope}")
}

fn logical_transaction_id(record: &WriterSegmentCatalogRecord) -> String {
    let identity = format!(
        "{}/{}/{}/{}",
        record.family, record.scope, record.generation, record.segment_ref
    );
    format!("writer-segment:{}", sha256_hex(identity.as_bytes()))
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        bail!("writer segment catalog {field} must not be empty");
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("writer segment catalog {field} must contain a 32-byte hex digest");
    }
    Ok(())
}
