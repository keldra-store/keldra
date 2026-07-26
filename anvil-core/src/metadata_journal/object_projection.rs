use super::*;
use crate::core_store::{
    CF_OBJECT_HEADS, CF_OBJECT_VERSIONS, TABLE_OBJECT_HEAD_ROW, TABLE_OBJECT_VERSION_META_ROW,
    encode_object_metadata_counter_at_generation,
    encode_object_metadata_row_at_generation_for_transaction,
    encode_object_metadata_row_at_generation_with_delete_marker_for_transaction,
    object_current_history_key, object_current_key, object_current_page_key_for_object,
    object_id_counter_key, object_key_catalog_key, object_version_catalog_key,
    object_version_history_key, object_version_id_key, object_version_key,
    object_version_page_key_for_object,
};
use crate::mvcc_product::{ProductMutation, coremeta_logical_key};

#[derive(Debug, Clone)]
pub(crate) struct ObjectVersionSnapshot {
    pub object: Object,
    pub row_generation: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectProjectionSnapshot {
    pub snapshot_version: u64,
    pub projection_generation: u64,
    pub counter_max_id: i64,
    pub current: Option<Object>,
    pub original: Option<ObjectVersionSnapshot>,
    /// Latest surviving version at the transaction snapshot, excluding the
    /// version being deleted. Callers obtain this with one fixed MVCC scan.
    pub delete_current_successor: Option<Object>,
}

pub(crate) fn object_current_logical_key(
    bucket: &Bucket,
    object_key: &str,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    coremeta_logical_key(
        CF_OBJECT_HEADS,
        TABLE_OBJECT_HEAD_ROW,
        &object_current_key(bucket, object_key),
    )
}

pub(crate) fn object_version_logical_key(
    bucket: &Bucket,
    object_key: &str,
    version_id: uuid::Uuid,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    coremeta_logical_key(
        CF_OBJECT_VERSIONS,
        TABLE_OBJECT_VERSION_META_ROW,
        &object_version_key(bucket, object_key, version_id),
    )
}

pub(crate) fn object_id_counter_logical_key(
    bucket: &Bucket,
) -> Result<crate::mvcc_transaction::LogicalKey> {
    coremeta_logical_key(
        CF_OBJECT_VERSIONS,
        TABLE_OBJECT_VERSION_META_ROW,
        &object_id_counter_key(bucket),
    )
}

pub(crate) fn plan_object_upsert(
    bucket: &Bucket,
    object: &Object,
    snapshot: &ObjectProjectionSnapshot,
    transaction_id: &str,
) -> Result<Vec<ProductMutation>> {
    validate_projection_input(bucket, object, snapshot)?;
    let generation = snapshot.projection_generation;
    let payload = encode_object_projection_row(object, generation, transaction_id)?;
    let counter = encode_object_projection_counter(
        bucket,
        object.id.max(snapshot.counter_max_id),
        generation,
        transaction_id,
    )?;
    let mut mutations = vec![
        put(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            object_current_key(bucket, &object.key),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_key(bucket, &object.key, object.version_id),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_id_key(bucket, object.version_id),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            object_key_catalog_key(bucket, object),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_page_key_for_object(bucket, object, generation),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_catalog_key(bucket, object, generation),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            object_current_history_key(bucket, &object.key, generation, object.version_id),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_history_key(bucket, &object.key, object.version_id, generation),
            payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_id_counter_key(bucket),
            counter,
        )?,
    ];
    let page_key = object_current_page_key_for_object(bucket, object);
    mutations.push(if object.deleted_at.is_some() {
        delete(CF_OBJECT_HEADS, TABLE_OBJECT_HEAD_ROW, page_key)?
    } else {
        put(CF_OBJECT_HEADS, TABLE_OBJECT_HEAD_ROW, page_key, payload)?
    });
    Ok(mutations)
}

pub(crate) fn plan_object_delete_version(
    bucket: &Bucket,
    deletion: &Object,
    snapshot: &ObjectProjectionSnapshot,
    transaction_id: &str,
) -> Result<Vec<ProductMutation>> {
    validate_projection_input(bucket, deletion, snapshot)?;
    if deletion.deleted_at.is_none() {
        bail!("object version deletion projection requires deleted_at");
    }
    let original = snapshot
        .original
        .as_ref()
        .ok_or_else(|| anyhow!("object version metadata row missing at MVCC snapshot"))?;
    if original.object.key != deletion.key || original.object.version_id != deletion.version_id {
        bail!("object version deletion snapshot scope mismatch");
    }
    let generation = snapshot.projection_generation;
    let mut tombstone = deletion.clone();
    tombstone.record_hash = format!(
        "sha256:{}",
        crate::core_store::sha256_hex(tombstone.mutation_id.as_bytes())
    );
    let tombstone_payload =
        encode_object_projection_tombstone(&tombstone, generation, transaction_id)?;
    let counter = encode_object_projection_counter(
        bucket,
        deletion.id.max(snapshot.counter_max_id),
        generation,
        transaction_id,
    )?;
    let mut mutations = vec![
        delete(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_key(bucket, &deletion.key, deletion.version_id),
        )?,
        delete(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_id_key(bucket, deletion.version_id),
        )?,
        delete(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_page_key_for_object(bucket, &original.object, original.row_generation),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_version_history_key(bucket, &deletion.key, deletion.version_id, generation),
            tombstone_payload.clone(),
        )?,
        put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            object_id_counter_key(bucket),
            counter,
        )?,
    ];
    let deleted_is_current = snapshot.current.as_ref().is_some_and(|current| {
        current.key == deletion.key && current.version_id == deletion.version_id
    });
    if deleted_is_current {
        let current_key = object_current_key(bucket, &deletion.key);
        if let Some(successor) = snapshot.delete_current_successor.as_ref() {
            let successor_payload =
                encode_object_projection_row(successor, generation, transaction_id)?;
            mutations.push(put(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                current_key,
                successor_payload.clone(),
            )?);
            mutations.push(if successor.deleted_at.is_some() {
                delete(
                    CF_OBJECT_HEADS,
                    TABLE_OBJECT_HEAD_ROW,
                    object_current_page_key_for_object(bucket, successor),
                )?
            } else {
                put(
                    CF_OBJECT_HEADS,
                    TABLE_OBJECT_HEAD_ROW,
                    object_current_page_key_for_object(bucket, successor),
                    successor_payload.clone(),
                )?
            });
            mutations.push(put(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_history_key(bucket, &deletion.key, generation, successor.version_id),
                successor_payload,
            )?);
        } else {
            mutations.push(delete(CF_OBJECT_HEADS, TABLE_OBJECT_HEAD_ROW, current_key)?);
            mutations.push(delete(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_page_key_for_object(bucket, &original.object),
            )?);
            mutations.push(put(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_history_key(bucket, &deletion.key, generation, deletion.version_id),
                tombstone_payload,
            )?);
        }
    }
    Ok(mutations)
}

fn validate_projection_input(
    bucket: &Bucket,
    object: &Object,
    snapshot: &ObjectProjectionSnapshot,
) -> Result<()> {
    if object.tenant_id != bucket.tenant_id || object.bucket_id != bucket.id {
        bail!("object projection scope mismatch");
    }
    if snapshot.projection_generation == 0 || object.id <= 0 || snapshot.counter_max_id < 0 {
        bail!("object projection snapshot has invalid generation or object id");
    }
    Ok(())
}

pub(crate) fn encode_object_projection_row(
    object: &Object,
    projection_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation_for_transaction(
        object,
        projection_generation,
        transaction_id,
    )
}

pub(crate) fn encode_object_projection_tombstone(
    object: &Object,
    projection_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation_with_delete_marker_for_transaction(
        object,
        projection_generation,
        false,
        transaction_id,
    )
}

pub(crate) fn encode_object_projection_counter(
    bucket: &Bucket,
    max_id: i64,
    projection_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    encode_object_metadata_counter_at_generation(
        bucket,
        max_id,
        projection_generation,
        transaction_id,
    )
}

fn put(cf: &str, table: u16, tuple: Vec<u8>, payload: Vec<u8>) -> Result<ProductMutation> {
    Ok(ProductMutation::put(
        coremeta_logical_key(cf, table, &tuple)?,
        payload,
    ))
}

fn delete(cf: &str, table: u16, tuple: Vec<u8>) -> Result<ProductMutation> {
    Ok(ProductMutation::delete(coremeta_logical_key(
        cf, table, &tuple,
    )?))
}
