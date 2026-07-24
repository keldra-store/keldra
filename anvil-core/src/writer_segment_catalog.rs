use crate::{
    core_store::{
        CF_MATERIALISATION, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
        CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
        CoreMutationRootPublication, CoreStore, CoreTransactionState, TABLE_WRITER_HEAD_ROW,
        TABLE_WRITER_SEGMENT_ROW, core_meta_root_key_hash, core_meta_tuple_key,
        core_mutation_publication_attempt_id, decode_deterministic_proto,
        encode_deterministic_proto, sha256_hex,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Weak},
};

mod head;

#[cfg(test)]
use crate::core_store::{CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore};

const WRITER_SEGMENT_ROW_SCHEMA: &str = "anvil.coremeta.writer_segment_locator.v1";
pub const WRITER_SEGMENT_PAGE_MAX: usize = 1000;

static WRITER_LOCKS: LazyLock<std::sync::Mutex<BTreeMap<String, Weak<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
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
    storage: &Storage,
    record: &WriterSegmentCatalogRecord,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    validate_record(record)?;
    let write_lock = writer_lock(&record.family, &record.scope)?;
    let _guard = write_lock.lock().await;
    let store = CoreStore::new(storage.clone()).await?;

    if let Some(existing) =
        read_record_at_generation(&store, &record.family, &record.scope, record.generation)?
    {
        if existing == *record {
            return Ok(());
        }
        bail!("writer segment generation already identifies a different segment");
    }

    let current = head::read(&store, &record.family, &record.scope)?;
    let batch = mutation_batch(record, current.as_ref(), additional_preconditions)?;
    let receipt = store.commit_mutation_batch(batch).await?;
    if receipt.state != CoreTransactionState::Committed {
        bail!(
            "writer segment catalog publication {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        );
    }
    Ok(())
}

fn mutation_batch(
    record: &WriterSegmentCatalogRecord,
    current: Option<&head::WriterHead>,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<CoreMutationBatch> {
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
    let segment_key = tuple_key(&record.family, &record.scope, record.generation)?;
    let head_key = head::tuple_key(&record.family, &record.scope)?;
    let scope_partition = format!("writer-scope/{}/{}", record.family, record.scope);
    let mut preconditions = Vec::with_capacity(additional_preconditions.len() + 2);
    preconditions.push(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_MATERIALISATION.to_string(),
        table_id: TABLE_WRITER_SEGMENT_ROW,
        tuple_key: segment_key.clone(),
        expected_payload_hash: None,
        require_absent: true,
        require_present: false,
    });
    preconditions.push(head::precondition(&record.family, &record.scope, current)?);
    preconditions.extend_from_slice(additional_preconditions);
    let transaction_id =
        core_mutation_publication_attempt_id(&logical_transaction_id(record), &preconditions)?;
    let segment_payload = encode_record(record, publication_generation, &transaction_id)?;
    let head_payload = head::encode(
        logical_head,
        publication_generation,
        &transaction_id,
        logical_head
            .created_at_unix_nanos
            .max(record.created_at_unix_nanos),
    )?;
    let operations = vec![
        CoreMutationOperation::CoreMetaPut {
            partition_id: scope_partition.clone(),
            cf: CF_MATERIALISATION.to_string(),
            table_id: TABLE_WRITER_SEGMENT_ROW,
            tuple_key: segment_key,
            payload: segment_payload,
        },
        CoreMutationOperation::CoreMetaPut {
            partition_id: scope_partition.clone(),
            cf: CF_MATERIALISATION.to_string(),
            table_id: TABLE_WRITER_HEAD_ROW,
            tuple_key: head_key,
            payload: head_payload,
        },
    ];
    Ok(CoreMutationBatch {
        transaction_id,
        scope_partition: scope_partition.clone(),
        committed_by_principal: writer_realm(&record.family, &record.scope),
        root_publications: vec![
            CoreMutationRootPublication::new(
                scope_partition,
                crate::formats::writer::WriterFamily::CoreControl.as_str(),
            )
            .coordinator(),
        ],
        preconditions,
        operations,
    })
}

pub async fn read_writer_segment_catalog_record(
    storage: &Storage,
    family: &str,
    scope: &str,
    generation: u64,
    segment_ref: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    validate_scope_components(family, scope)?;
    if generation == 0 {
        bail!("writer segment generation must be nonzero");
    }
    let store = CoreStore::new(storage.clone()).await?;
    let Some(record) = read_record_at_generation(&store, family, scope, generation)? else {
        return Ok(None);
    };
    validate_scope(&record, family, scope, generation, segment_ref)?;
    Ok(Some(record))
}

fn read_record_at_generation(
    store: &CoreStore,
    family: &str,
    scope: &str,
    generation: u64,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    let Some(payload) = store.read_coremeta_row(
        CF_MATERIALISATION,
        TABLE_WRITER_SEGMENT_ROW,
        &tuple_key(family, scope, generation)?,
    )?
    else {
        return Ok(None);
    };
    decode_record(&payload).map(Some)
}

pub async fn latest_writer_segment_catalog_record(
    storage: &Storage,
    family: &str,
    scope: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    let store = CoreStore::new(storage.clone()).await?;
    Ok(head::read(&store, family, scope)?.map(|head| head.record))
}

pub async fn page_writer_segment_catalog_records(
    storage: &Storage,
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
    let scan_limit = limit
        .checked_add(1)
        .ok_or_else(|| anyhow!("writer segment page limit overflow"))?;
    let mut records = CoreStore::new(storage.clone())
        .await?
        .scan_coremeta_range_inclusive(
            CF_MATERIALISATION,
            TABLE_WRITER_SEGMENT_ROW,
            &tuple_key(family, scope, after_generation.saturating_add(1))?,
            &tuple_key(family, scope, through_generation)?,
            scan_limit,
        )?
        .into_iter()
        .map(|row| {
            let record = decode_record(&row.payload)?;
            if record.family != family || record.scope != scope {
                bail!("writer segment catalog row scope mismatch");
            }
            Ok(record)
        })
        .collect::<Result<Vec<_>>>()?;
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

#[cfg(test)]
pub(crate) fn test_overwrite_writer_segment_catalog_record(
    storage: &Storage,
    record: &WriterSegmentCatalogRecord,
) -> Result<()> {
    let existing = CoreMetaStore::open(storage.core_store_meta_path())?
        .get(
            CF_MATERIALISATION,
            TABLE_WRITER_SEGMENT_ROW,
            &tuple_key(&record.family, &record.scope, record.generation)?,
        )?
        .ok_or_else(|| anyhow!("writer segment test row is missing"))?;
    let (_, publication_generation, publication_transaction_id) =
        decode_record_with_publication(&existing)?;
    let payload = encode_record(record, publication_generation, &publication_transaction_id)?;
    let tuple_key = tuple_key(&record.family, &record.scope, record.generation)?;
    CoreMetaStore::open(storage.core_store_meta_path())?.write_batch(&[CoreMetaBatchOp {
        cf: CF_MATERIALISATION,
        table_id: TABLE_WRITER_SEGMENT_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    }])
}

#[cfg(test)]
pub(crate) fn test_writer_segment_mutation_batch(
    record: &WriterSegmentCatalogRecord,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<CoreMutationBatch> {
    mutation_batch(record, None, additional_preconditions)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn record(generation: u64) -> WriterSegmentCatalogRecord {
        WriterSegmentCatalogRecord {
            family: "test-writer".to_string(),
            scope: "tenant/42/index/main".to_string(),
            segment_ref: format!("segment:{generation}"),
            core_object_ref_target: format!("core-object-ref:test-{generation}"),
            segment_hash: format!("{generation:064x}"),
            segment_length: generation,
            generation,
            source_cursor: generation * 10,
            created_at_unix_nanos: generation * 100,
        }
    }

    #[tokio::test]
    async fn writer_head_is_a_transactional_point_projection() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1);
        let second = record(2);
        let third = record(3);

        write_writer_segment_catalog_record(&storage, &first, &[])
            .await
            .unwrap();
        write_writer_segment_catalog_record(&storage, &third, &[])
            .await
            .unwrap();
        write_writer_segment_catalog_record(&storage, &first, &[])
            .await
            .unwrap();
        write_writer_segment_catalog_record(&storage, &second, &[])
            .await
            .unwrap();

        assert_eq!(
            latest_writer_segment_catalog_record(&storage, &third.family, &third.scope)
                .await
                .unwrap(),
            Some(third.clone())
        );
        assert_eq!(
            read_writer_segment_catalog_record(
                &storage,
                &first.family,
                &first.scope,
                first.generation,
                &first.segment_ref,
            )
            .await
            .unwrap(),
            Some(first.clone())
        );
        let first_page = page_writer_segment_catalog_records(
            &storage,
            &first.family,
            &first.scope,
            0,
            u64::MAX,
            2,
        )
        .await
        .unwrap();
        assert_eq!(first_page.records, vec![first.clone(), second]);
        assert_eq!(first_page.next_generation, Some(2));
        let second_page = page_writer_segment_catalog_records(
            &storage,
            &first.family,
            &first.scope,
            first_page.next_generation.unwrap(),
            u64::MAX,
            2,
        )
        .await
        .unwrap();
        assert_eq!(second_page.records, vec![third]);
        assert_eq!(second_page.next_generation, None);

        let mut conflicting = first;
        conflicting.segment_ref = "segment:conflict".to_string();
        let error = write_writer_segment_catalog_record(&storage, &conflicting, &[])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("different segment"));
    }

    #[test]
    fn catalog_mutation_is_byte_identical_for_the_same_source_record() {
        let record = record(7);
        let first = mutation_batch(&record, None, &[]).unwrap();
        let replay = mutation_batch(&record, None, &[]).unwrap();
        assert_eq!(first, replay);
    }

    #[test]
    fn catalog_row_accepts_independent_physical_root_generation() {
        let record = record(7);
        let payload = encode_record(&record, 3, "tx-writer").unwrap();
        let mut common = crate::core_store::core_meta_row_common_from_payload(&payload).unwrap();
        common.root_generation = 91;
        let rebound = crate::core_store::replace_core_meta_row_common(&payload, &common).unwrap();

        let (decoded, catalog_generation, _) = decode_record_with_publication(&rebound).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(catalog_generation, 3);
        assert_ne!(common.root_generation, catalog_generation);
    }

    #[tokio::test]
    async fn stale_writer_head_snapshot_cannot_publish_a_segment() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1);
        let second = record(2);
        let third = record(3);

        write_writer_segment_catalog_record(&storage, &first, &[])
            .await
            .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let stale_head = head::read(&store, &first.family, &first.scope)
            .unwrap()
            .unwrap();
        let stale_batch = mutation_batch(&third, Some(&stale_head), &[]).unwrap();

        write_writer_segment_catalog_record(&storage, &second, &[])
            .await
            .unwrap();
        store.commit_mutation_batch(stale_batch).await.unwrap_err();

        assert_eq!(
            latest_writer_segment_catalog_record(&storage, &first.family, &first.scope)
                .await
                .unwrap(),
            Some(second)
        );
        assert_eq!(
            read_writer_segment_catalog_record(
                &storage,
                &third.family,
                &third.scope,
                third.generation,
                &third.segment_ref,
            )
            .await
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn caller_preconditions_are_enforced_with_catalog_publication() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = record(1);
        let second = record(2);
        let third = record(3);

        write_writer_segment_catalog_record(&storage, &first, &[])
            .await
            .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let first_head = head::read(&store, &first.family, &first.scope)
            .unwrap()
            .unwrap();
        let stale_caller_precondition =
            head::precondition(&first.family, &first.scope, Some(&first_head)).unwrap();
        write_writer_segment_catalog_record(&storage, &second, &[])
            .await
            .unwrap();

        write_writer_segment_catalog_record(&storage, &third, &[stale_caller_precondition])
            .await
            .unwrap_err();

        assert_eq!(
            latest_writer_segment_catalog_record(&storage, &first.family, &first.scope)
                .await
                .unwrap(),
            Some(second)
        );
        assert_eq!(
            read_writer_segment_catalog_record(
                &storage,
                &third.family,
                &third.scope,
                third.generation,
                &third.segment_ref,
            )
            .await
            .unwrap(),
            None
        );
    }
}
