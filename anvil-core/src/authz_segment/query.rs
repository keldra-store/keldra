use super::{
    AUTHZ_DELTA_CHECKPOINT_INTERVAL, AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY, TABLE_AUTHZ_LIST_SUBJECTS,
    TABLE_AUTHZ_TUPLE, authz_record_from_segment_record, authz_tuple_segment_scope,
    decode_authz_header_proto, decode_list_subjects_row, key_parts,
};
use crate::core_store::{CoreStore, decode_core_object_ref_target};
use crate::formats::{FileFamily, authz::TupleKey, segment::SegmentRecord};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use crate::writer_segment_catalog::{
    WriterSegmentCatalogRecord, page_writer_segment_catalog_records,
};
use crate::writer_segment_range::RangeAddressedWriterSegment;
use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HistoricalPermissionStats {
    pub segments_opened: usize,
    pub table_rows_visited: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoricalPermissionOutcome {
    pub allowed: bool,
    pub stats: HistoricalPermissionStats,
}

#[derive(Debug, Clone)]
pub(crate) struct HistoricalTupleOutcome {
    pub record: Option<AuthzTupleRecord>,
    pub stats: HistoricalPermissionStats,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_materialized_permission_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: u64,
) -> Result<HistoricalPermissionOutcome> {
    if revision == 0 {
        return Ok(HistoricalPermissionOutcome {
            allowed: false,
            stats: HistoricalPermissionStats::default(),
        });
    }

    let records = historical_segment_window(storage, tenant_id, revision).await?;
    let exact_key = key_parts(&[
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
    ]);
    let store = CoreStore::new(storage.clone()).await?;
    let mut allowed = false;
    let mut chain = SegmentChain::new(revision);
    let mut stats = HistoricalPermissionStats::default();

    for record in records {
        let segment = open_segment(&store, tenant_id, &record).await?;
        let header = decode_authz_header_proto(&segment.header)?;
        stats.segments_opened += 1;
        if !chain.accept(&record, &header)? {
            continue;
        }
        if header.segment_kind == "checkpoint" {
            allowed = false;
        }

        let directory = segment.read_body_table_directory().await?;
        let table =
            RangeAddressedWriterSegment::table_entry(&directory, TABLE_AUTHZ_LIST_SUBJECTS)?;
        let rows = segment
            .read_table_pages_matching_key_prefix(table, &exact_key)
            .await?;
        stats.table_rows_visited += rows.len();
        let mut exact_rows = rows.into_iter().filter(|row| row.key == exact_key);
        if let Some(row) = exact_rows.next() {
            if exact_rows.next().is_some() {
                bail!("authz historical permission index contains duplicate keys");
            }
            let row = decode_list_subjects_row(&row.value)?;
            if row.namespace != namespace
                || row.object_id != object_id
                || row.relation != relation
                || row.subject_kind != subject_kind
                || row.subject_id != subject_id
                || row.caveat_hash != caveat_hash
                || row.revision > header.generation
            {
                bail!("authz historical permission row scope mismatch");
            }
            allowed = match row.operation.as_str() {
                "add" => true,
                "remove" => false,
                _ => bail!("authz historical permission row operation is invalid"),
            };
        }
    }
    chain.finish()?;
    Ok(HistoricalPermissionOutcome { allowed, stats })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn lookup_materialized_tuple_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: u64,
) -> Result<HistoricalTupleOutcome> {
    if revision == 0 {
        return Ok(HistoricalTupleOutcome {
            record: None,
            stats: HistoricalPermissionStats::default(),
        });
    }

    let records = historical_segment_window(storage, tenant_id, revision).await?;
    let exact_key = TupleKey {
        namespace: namespace.as_bytes().to_vec(),
        object_id: object_id.as_bytes().to_vec(),
        relation: relation.as_bytes().to_vec(),
        subject_kind: subject_kind.as_bytes().to_vec(),
        subject_id: subject_id.as_bytes().to_vec(),
        caveat_hash: super::caveat_hash_from_string(caveat_hash)?,
    }
    .encode();
    let store = CoreStore::new(storage.clone()).await?;
    let mut current = None;
    let mut chain = SegmentChain::new(revision);
    let mut stats = HistoricalPermissionStats::default();

    for catalog_record in records {
        let segment = open_segment(&store, tenant_id, &catalog_record).await?;
        let header = decode_authz_header_proto(&segment.header)?;
        stats.segments_opened += 1;
        if !chain.accept(&catalog_record, &header)? {
            continue;
        }
        if header.segment_kind == "checkpoint" {
            current = None;
        }

        let directory = segment.read_body_table_directory().await?;
        let table = RangeAddressedWriterSegment::table_entry(&directory, TABLE_AUTHZ_TUPLE)?;
        let rows = segment
            .read_table_pages_matching_key_prefix(table, &exact_key)
            .await?;
        stats.table_rows_visited += rows.len();
        let mut exact = rows
            .into_iter()
            .filter(|row| row.key.starts_with(&exact_key))
            .map(|row| authz_record_from_segment_record(SegmentRecord::new(row.key, row.value)))
            .collect::<Result<Vec<_>>>()?;
        exact.sort_by_key(|record| (record.revision, record.revision_ordinal));
        for mut record in exact {
            let record_revision = u64::try_from(record.revision)
                .context("authorization tuple revision must be nonnegative")?;
            if record.namespace != namespace
                || record.object_id != object_id
                || record.relation != relation
                || record.subject_kind != subject_kind
                || record.subject_id != subject_id
                || record.caveat_hash != caveat_hash
                || record_revision > header.generation
            {
                bail!("authz historical tuple row scope mismatch");
            }
            record.tenant_id = tenant_id;
            match record.operation.as_str() {
                "add" => current = Some(record),
                "remove" => current = None,
                _ => bail!("authz historical tuple operation is invalid"),
            }
        }
    }
    chain.finish()?;
    Ok(HistoricalTupleOutcome {
        record: current,
        stats,
    })
}

async fn historical_segment_window(
    storage: &Storage,
    tenant_id: i64,
    revision: u64,
) -> Result<Vec<WriterSegmentCatalogRecord>> {
    let checkpoint_generation = checkpoint_generation(revision);
    let after_generation = checkpoint_generation.saturating_sub(1);
    let scope = authz_tuple_segment_scope(tenant_id)?;
    let page = page_writer_segment_catalog_records(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &scope,
        after_generation,
        revision,
        usize::try_from(AUTHZ_DELTA_CHECKPOINT_INTERVAL)
            .context("authorization checkpoint interval exceeds usize")?,
    )
    .await?;
    if page.next_generation.is_some() {
        bail!("AuthzRevisionUnavailable: historical segment window exceeds checkpoint bound");
    }
    Ok(page.records)
}

async fn open_segment(
    store: &CoreStore,
    tenant_id: i64,
    record: &WriterSegmentCatalogRecord,
) -> Result<RangeAddressedWriterSegment> {
    let segment = RangeAddressedWriterSegment::open_object_ref(
        store.clone(),
        decode_core_object_ref_target(&record.core_object_ref_target)?,
        FileFamily::AuthzTupleSegment,
    )
    .await?;
    let header = decode_authz_header_proto(&segment.header)?;
    if header.tenant_id != tenant_id.to_string()
        || header.generation != record.generation
        || header.source_stream_cursor != record.source_cursor
    {
        bail!("AuthzCandidateSetStale");
    }
    Ok(segment)
}

fn checkpoint_generation(revision: u64) -> u64 {
    if revision % AUTHZ_DELTA_CHECKPOINT_INTERVAL == 0 {
        revision
    } else {
        (revision / AUTHZ_DELTA_CHECKPOINT_INTERVAL) * AUTHZ_DELTA_CHECKPOINT_INTERVAL
    }
}

struct SegmentChain {
    requested_revision: u64,
    checkpoint_seen: bool,
    applied_generation: u64,
    source_stream_cursor: u64,
}

impl SegmentChain {
    fn new(requested_revision: u64) -> Self {
        Self {
            requested_revision,
            checkpoint_seen: false,
            applied_generation: 0,
            source_stream_cursor: 0,
        }
    }

    fn accept(
        &mut self,
        record: &WriterSegmentCatalogRecord,
        header: &super::AuthzSegmentHeader,
    ) -> Result<bool> {
        if header.generation > self.requested_revision {
            bail!("AuthzCandidateSetStale");
        }
        match header.segment_kind.as_str() {
            "checkpoint" => {
                if header.base_revision != 0
                    || !header.schema_replacement
                    || !header.relation_rule_replacement
                    || (self.checkpoint_seen
                        && (header.generation <= self.applied_generation
                            || header.source_stream_cursor < self.source_stream_cursor))
                {
                    bail!("AuthzCandidateSetStale");
                }
                self.checkpoint_seen = true;
            }
            "delta" => {
                if !self.checkpoint_seen {
                    return Ok(false);
                }
                if header.source_stream_cursor < self.source_stream_cursor
                    || header.base_revision != self.applied_generation
                    || header.generation != self.applied_generation.saturating_add(1)
                {
                    bail!("AuthzCandidateSetStale");
                }
            }
            _ => bail!("authz segment has unsupported segment kind"),
        }
        if record.generation != header.generation {
            bail!("AuthzCandidateSetStale");
        }
        self.applied_generation = header.generation;
        self.source_stream_cursor = header.source_stream_cursor;
        Ok(true)
    }

    fn finish(self) -> Result<()> {
        if !self.checkpoint_seen || self.applied_generation != self.requested_revision {
            bail!("AuthzRevisionUnavailable: materialized authorization revision is unavailable");
        }
        Ok(())
    }
}
