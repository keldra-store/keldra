use super::*;
use crate::core_store::{
    CF_MATERIALISATION, CF_OBJECT_HEADS, CoreMetaTuplePart,
    TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW, TABLE_WRITER_SEGMENT_ROW, core_meta_tuple_key,
};
use crate::mvcc_product::{ProductMutation, coremeta_logical_key};
use crate::mvcc_transaction::{LogicalKey, PredicateKind};

/// Immutable segment bodies and object payload bytes are deliberately absent
/// from this API. This module owns only mutable product projection rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataProductRowKind {
    ManifestPublication,
    WriterCatalogReference,
    CompactionState,
}

#[derive(Debug)]
pub(crate) struct MetadataMvccProjectionPlan {
    pub mutations: Vec<ProductMutation>,
    pub predicates: Vec<(LogicalKey, PredicateKind)>,
}

impl MetadataMvccProjectionPlan {
    pub fn new() -> Self {
        Self {
            mutations: Vec::new(),
            predicates: Vec::new(),
        }
    }

    pub fn observe_and_put(
        &mut self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        key: LogicalKey,
        payload: Vec<u8>,
    ) -> Result<()> {
        let predicate = mvcc
            .read_latest_value(&key)?
            .map(|current| PredicateKind::ValueHash(*blake3::hash(&current).as_bytes()))
            .unwrap_or(PredicateKind::Absent);
        self.predicates.push((key.clone(), predicate));
        self.mutations.push(ProductMutation::put(key, payload));
        Ok(())
    }

    pub fn observe_and_delete(
        &mut self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        key: LogicalKey,
    ) -> Result<()> {
        let predicate = mvcc
            .read_latest_value(&key)?
            .map(|current| PredicateKind::ValueHash(*blake3::hash(&current).as_bytes()))
            .unwrap_or(PredicateKind::Exists);
        self.predicates.push((key.clone(), predicate));
        self.mutations.push(ProductMutation::delete(key));
        Ok(())
    }

    pub fn stage(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        principal: &str,
        now_unix_ms: u64,
    ) -> Result<()> {
        mvcc.stage_product_mutations(transaction_id, principal, self.mutations, now_unix_ms)?;
        for (key, kind) in self.predicates {
            mvcc.stage_predicate(transaction_id, principal, key, kind, now_unix_ms)?;
        }
        Ok(())
    }

    pub async fn autocommit(
        self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        principal: &str,
        idempotency_key: &str,
        durability: crate::mvcc_transaction::DurabilityLevel,
        now_unix_ms: u64,
    ) -> Result<u64> {
        mvcc.autocommit_product_mutations_with_predicates(
            principal,
            idempotency_key,
            self.mutations,
            self.predicates,
            durability,
            now_unix_ms,
        )
        .await
    }
}

pub(crate) fn metadata_product_key(
    kind: MetadataProductRowKind,
    bucket: &Bucket,
    tuple_key: Option<&[u8]>,
) -> Result<LogicalKey> {
    match kind {
        MetadataProductRowKind::ManifestPublication => coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
            tuple_key.ok_or_else(|| anyhow!("manifest publication tuple key is required"))?,
        ),
        MetadataProductRowKind::WriterCatalogReference => coremeta_logical_key(
            CF_MATERIALISATION,
            TABLE_WRITER_SEGMENT_ROW,
            tuple_key.ok_or_else(|| anyhow!("writer catalog tuple key is required"))?,
        ),
        MetadataProductRowKind::CompactionState => coremeta_logical_key(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW,
            &core_meta_tuple_key(&[
                CoreMetaTuplePart::Utf8("object-metadata-compaction"),
                CoreMetaTuplePart::I64(bucket.tenant_id),
                CoreMetaTuplePart::I64(bucket.id),
            ])?,
        ),
    }
}

pub(crate) fn read_metadata_product_latest(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &LogicalKey,
) -> Result<Option<Vec<u8>>> {
    mvcc.read_latest_value(key)
}

pub(crate) fn read_metadata_product_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    key: &LogicalKey,
    snapshot: u64,
) -> Result<Option<Vec<u8>>> {
    Ok(mvcc.runtime.read_at(key, snapshot)?.map(|row| row.value))
}
