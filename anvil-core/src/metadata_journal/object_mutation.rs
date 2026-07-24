use super::*;
use crate::core_store::{
    CoreMutationBatchAdditions, CoreMutationRootPublication, CoreTransactionState,
    ObjectMetadataProjectionMutation, core_meta_root_key_hash,
};
use crate::formats::writer::WriterFamily;
use crate::persistence::ObjectWatchEvent;
use crate::watch_log;
use anyhow::bail;

const MAX_STREAM_HEAD_RETRIES: usize = 64;

#[cfg(test)]
pub(crate) async fn append_object_mutation(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
) -> Result<()> {
    append_object_mutation_inner(storage, bucket, object, mutation, 0, None, None, None).await
}

pub(crate) async fn append_object_mutation_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    append_object_mutation_with_permit_in_transaction(
        storage,
        bucket,
        object,
        mutation,
        permit,
        partition_owner_signing_key,
        None,
        None,
    )
    .await
}

pub(crate) async fn append_object_mutation_with_permit_in_transaction(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_object_mutation_inner(
        storage,
        bucket,
        object,
        mutation,
        permit.fence_token,
        Some(partition_precondition),
        transaction_id,
        transaction_principal,
    )
    .await
}

pub(crate) async fn append_object_put_mutations_with_permit_in_transaction(
    storage: &Storage,
    bucket: &Bucket,
    objects: &[Object],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
    additions: CoreMutationBatchAdditions,
) -> Result<()> {
    append_object_put_mutations_with_permit_inner(
        storage,
        bucket,
        objects,
        permit,
        partition_owner_signing_key,
        transaction_id,
        Some(transaction_principal),
        additions,
    )
    .await
}

pub(crate) async fn commit_object_put_mutations_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    objects: &[Object],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    additions: CoreMutationBatchAdditions,
) -> Result<()> {
    append_object_put_mutations_with_permit_inner(
        storage,
        bucket,
        objects,
        permit,
        partition_owner_signing_key,
        transaction_id,
        None,
        additions,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn append_object_put_mutations_with_permit_inner(
    storage: &Storage,
    bucket: &Bucket,
    objects: &[Object],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: Option<&str>,
    mut additions: CoreMutationBatchAdditions,
) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let _mutation_guard = core_store
        .acquire_object_metadata_mutation_lock(bucket)
        .await?;
    let scope_partition = hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    let explicit_transaction = match transaction_principal {
        Some(transaction_principal) => {
            let transaction = core_store
                .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
                .await?;
            if transaction.root_anchor_key != scope_partition {
                bail!("object metadata explicit transaction scope mismatch");
            }
            Some(transaction)
        }
        None => None,
    };
    let committed_by_principal = explicit_transaction
        .as_ref()
        .map(|transaction| transaction.committed_by_principal.clone())
        .unwrap_or_else(|| object_metadata_partition_principal(bucket));

    let metadata_stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let metadata_stream_precondition = core_store
        .stream_head_precondition_visible_to_transaction(
            &metadata_stream_id,
            explicit_transaction.as_ref(),
        )
        .await?;
    let watch_stream_id = watch_log::object_watch_stream_id(bucket.tenant_id, bucket.id);
    let watch_stream_precondition = core_store
        .stream_head_precondition_visible_to_transaction(
            &watch_stream_id,
            explicit_transaction.as_ref(),
        )
        .await?;
    let first_watch_sequence = stream_precondition_next_sequence(&watch_stream_precondition)?;

    let mut preconditions = vec![
        partition_precondition,
        metadata_stream_precondition,
        watch_stream_precondition,
    ];
    let mut operations = Vec::with_capacity(objects.len() * 16);
    for (index, object) in objects.iter().enumerate() {
        let projection = core_store
            .prepare_object_metadata_projection(
                bucket,
                object,
                ObjectMetadataProjectionMutation::Upsert,
                &scope_partition,
                transaction_id,
                explicit_transaction.as_ref(),
            )
            .await?;
        let event = object_watch_event(bucket, object, ObjectJournalMutation::Put);
        let sequence = first_watch_sequence
            .checked_add(index as u64)
            .ok_or_else(|| anyhow!("object watch stream sequence overflow"))?;
        let watch = watch_log::prepare_object_watch_append_at_sequence(
            bucket,
            object,
            &event,
            &scope_partition,
            &core_meta_root_key_hash(&scope_partition),
            Some(projection.root_generation),
            transaction_id,
            sequence,
            None,
        )?;
        preconditions.extend(watch.preconditions);
        operations.push(CoreMutationOperation::StreamAppend {
            partition_id: scope_partition.clone(),
            stream_id: metadata_stream_id.clone(),
            record_kind: ObjectJournalMutation::Put.object_record_kind().to_string(),
            payload: encode_object_version_body(&object_version_body(
                bucket,
                object,
                ObjectJournalMutation::Put,
                permit.fence_token,
            ))?,
            idempotency_key: Some(format!("object-metadata:{}:put", object.mutation_id)),
        });
        operations.extend(watch.operations);
        operations.extend(projection.operations);
    }
    preconditions.append(&mut additions.preconditions);
    operations.append(&mut additions.operations);
    let operations = coalesce_coremeta_operations_last_write_wins(operations);
    let mut root_publications = vec![CoreMutationRootPublication {
        root_anchor_key: scope_partition.clone(),
        writer_families: vec![
            WriterFamily::CoreControl.as_str().to_string(),
            WriterFamily::ObjectBlob.as_str().to_string(),
        ],
        transaction_coordinator: true,
    }];
    for publication in additions.root_publications {
        if publication.transaction_coordinator {
            bail!("object metadata batch addition cannot replace the coordinator root");
        }
        if root_publications
            .iter()
            .any(|current| current.root_anchor_key == publication.root_anchor_key)
        {
            bail!("object metadata batch addition duplicates a root publication");
        }
        root_publications.push(publication);
    }
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition,
        committed_by_principal,
        root_publications,
        preconditions,
        operations,
    };
    let receipt = if explicit_transaction.is_some() {
        core_store.stage_explicit_transaction_batch(batch).await?
    } else {
        core_store.commit_mutation_batch(batch).await?
    };
    let expected_state = if explicit_transaction.is_some() {
        CoreTransactionState::Open
    } else {
        CoreTransactionState::Committed
    };
    if receipt.state != expected_state {
        bail!(
            "object metadata mutation batch did not reach expected state {expected_state:?}: {}",
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation error")
        );
    }
    require_stream_update(&receipt.visible_updates, &metadata_stream_id)?;
    require_stream_update(&receipt.visible_updates, &watch_stream_id)?;
    Ok(())
}

fn coalesce_coremeta_operations_last_write_wins(
    operations: Vec<CoreMutationOperation>,
) -> Vec<CoreMutationOperation> {
    let mut last_coremeta_operation =
        std::collections::BTreeMap::<(String, u16, Vec<u8>), usize>::new();
    for (index, operation) in operations.iter().enumerate() {
        let key = match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                ..
            }
            | CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => Some((cf.clone(), *table_id, tuple_key.clone())),
            CoreMutationOperation::StreamAppend { .. } => None,
        };
        if let Some(key) = key {
            last_coremeta_operation.insert(key, index);
        }
    }

    operations
        .into_iter()
        .enumerate()
        .filter_map(|(index, operation)| {
            let keep = match &operation {
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                }
                | CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => last_coremeta_operation
                    .get(&(cf.clone(), *table_id, tuple_key.clone()))
                    .is_some_and(|last_index| *last_index == index),
                CoreMutationOperation::StreamAppend { .. } => true,
            };
            keep.then_some(operation)
        })
        .collect()
}

fn stream_precondition_next_sequence(precondition: &CoreMutationPrecondition) -> Result<u64> {
    let CoreMutationPrecondition::StreamHead {
        expected_last_sequence,
        ..
    } = precondition
    else {
        bail!("object stream precondition has wrong kind");
    };
    expected_last_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("object stream sequence overflow"))
}

pub(super) async fn append_object_mutation_inner(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let _mutation_guard = core_store
        .acquire_object_metadata_mutation_lock(bucket)
        .await?;
    for attempt in 0..MAX_STREAM_HEAD_RETRIES {
        let result = append_object_mutation_inner_once(
            storage,
            &core_store,
            bucket,
            object,
            mutation,
            fence_token,
            partition_precondition.clone(),
            transaction_id,
            transaction_principal,
        )
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(error)
                if is_stream_head_mismatch(&error) && attempt + 1 < MAX_STREAM_HEAD_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("metadata journal stream-head retry loop always returns")
}

#[allow(clippy::too_many_arguments)]
async fn append_object_mutation_inner_once(
    storage: &Storage,
    core_store: &CoreStore,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    explicit_transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    let scope_partition = hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    let implicit_transaction_id = format!(
        "object-metadata:{}:{}",
        object.mutation_id,
        mutation.event_name()
    );
    let transaction_id = explicit_transaction_id.unwrap_or(&implicit_transaction_id);
    let explicit_transaction = match explicit_transaction_id {
        Some(transaction_id) => {
            let principal = transaction_principal
                .ok_or_else(|| anyhow!("object metadata explicit transaction principal missing"))?;
            Some(
                core_store
                    .read_explicit_transaction_for_principal(transaction_id, principal)
                    .await?,
            )
        }
        None => None,
    };
    if explicit_transaction
        .as_ref()
        .is_some_and(|transaction| transaction.root_anchor_key != scope_partition)
    {
        bail!("object metadata explicit transaction scope mismatch");
    }
    let committed_by_principal = explicit_transaction
        .as_ref()
        .map(|transaction| transaction.committed_by_principal.clone())
        .unwrap_or_else(|| object_metadata_partition_principal(bucket));
    let projection_mutation = match mutation {
        ObjectJournalMutation::Put
        | ObjectJournalMutation::Copy
        | ObjectJournalMutation::DeleteMarker => ObjectMetadataProjectionMutation::Upsert,
        ObjectJournalMutation::DeleteVersion => ObjectMetadataProjectionMutation::DeleteVersion,
    };
    let projection = core_store
        .prepare_object_metadata_projection(
            bucket,
            object,
            projection_mutation,
            &scope_partition,
            transaction_id,
            explicit_transaction.as_ref(),
        )
        .await?;
    let metadata_stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let metadata_stream_precondition = core_store
        .stream_head_precondition_visible_to_transaction(
            &metadata_stream_id,
            explicit_transaction.as_ref(),
        )
        .await?;
    let event = object_watch_event(bucket, object, mutation);
    let watch = watch_log::prepare_object_watch_append(
        core_store,
        bucket,
        object,
        &event,
        &scope_partition,
        &core_meta_root_key_hash(&scope_partition),
        Some(projection.root_generation),
        transaction_id,
        explicit_transaction.as_ref(),
    )
    .await?;
    let object_payload =
        encode_object_version_body(&object_version_body(bucket, object, mutation, fence_token))?;
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.push(metadata_stream_precondition);
    preconditions.extend(watch.preconditions);
    let mut operations = Vec::with_capacity(3 + projection.operations.len());
    operations.push(CoreMutationOperation::StreamAppend {
        partition_id: scope_partition.clone(),
        stream_id: metadata_stream_id.clone(),
        record_kind: mutation.object_record_kind().to_string(),
        payload: object_payload,
        idempotency_key: Some(format!(
            "object-metadata:{}:{}",
            object.mutation_id,
            mutation.event_name()
        )),
    });
    operations.extend(watch.operations);
    operations.extend(projection.operations);
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: scope_partition.clone(),
        committed_by_principal,
        root_publications: vec![CoreMutationRootPublication {
            root_anchor_key: scope_partition,
            writer_families: vec![
                WriterFamily::CoreControl.as_str().to_string(),
                WriterFamily::ObjectBlob.as_str().to_string(),
            ],
            transaction_coordinator: true,
        }],
        preconditions,
        operations,
    };
    if explicit_transaction_id.is_some() {
        let receipt = core_store.stage_explicit_transaction_batch(batch).await?;
        if receipt.state != CoreTransactionState::Open {
            bail!("object metadata mutation was not staged in its explicit transaction");
        }
        require_stream_update(&receipt.visible_updates, &metadata_stream_id)?;
        require_stream_update(&receipt.visible_updates, &watch.stream_id)?;
        return Ok(());
    }
    let receipt = match core_store.commit_mutation_batch(batch).await {
        Ok(receipt) => receipt,
        Err(error) if error.to_string().contains("idempotency conflict") => {
            return Err(error.context(
                "object mutation idempotency identity conflicts with committed metadata",
            ));
        }
        Err(error) => return Err(error),
    };
    if receipt.state != CoreTransactionState::Committed {
        bail!(
            "object metadata mutation did not commit: {}",
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation error")
        );
    }
    let visible_metadata_sequence =
        require_stream_update(&receipt.visible_updates, &metadata_stream_id)?;
    let visible_watch_sequence = require_stream_update(&receipt.visible_updates, &watch.stream_id)?;
    require_committed_metadata_record(
        core_store,
        &metadata_stream_id,
        visible_metadata_sequence,
        object,
        mutation,
        bucket,
    )
    .await?;
    let projected_cursor = watch_log::exact_object_watch_cursor(
        storage,
        bucket.tenant_id,
        bucket.id,
        object.version_id,
        object.mutation_id,
    )
    .await?
    .ok_or_else(|| anyhow!("committed object mutation has no exact watch cursor"))?;
    if projected_cursor != u128::from(visible_watch_sequence) {
        bail!("committed object mutation watch cursor projection is inconsistent");
    }
    let watch_receipt =
        watch_log::committed_object_watch_receipt(storage, bucket, object, &event).await?;
    if watch_receipt.sequence != visible_watch_sequence {
        bail!("committed object mutation watch event sequence is inconsistent");
    }
    core_store
        .materialize_object_metadata_ancillary_projections(bucket, object, projection_mutation)
        .await?;
    Ok(())
}

async fn require_committed_metadata_record(
    core_store: &CoreStore,
    stream_id: &str,
    sequence: u64,
    object: &Object,
    mutation: ObjectJournalMutation,
    bucket: &Bucket,
) -> Result<()> {
    let record = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: sequence.saturating_sub(1),
            limit: 1,
        })
        .await?
        .into_iter()
        .find(|record| record.sequence == sequence);
    let Some(record) = record else {
        let transaction_id = format!(
            "object-metadata:{}:{}",
            object.mutation_id,
            mutation.event_name()
        );
        let transaction = core_store.read_transaction(&transaction_id).await?;
        bail!(
            "committed object mutation metadata record {stream_id}:{sequence} is not readable; transaction {transaction_id} is {}",
            transaction
                .as_ref()
                .map(|transaction| format!(
                    "{:?} with {} visible updates",
                    transaction.state,
                    transaction.visible_updates.len()
                ))
                .unwrap_or_else(|| "missing".to_string())
        );
    };
    let actual = metadata_record_from_stream_record(record)?;
    let expected_body = object_version_body(bucket, object, mutation, actual.body.fence_token);
    if actual.record_kind != ObjectMetadataRecordKind::from_str(mutation.object_record_kind())?
        || actual.body != expected_body
    {
        bail!("object mutation idempotency identity conflicts with committed metadata");
    }
    Ok(())
}

fn object_version_body(
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
) -> ObjectVersionBody {
    ObjectVersionBody {
        fence_token,
        id: object.id,
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        bucket_name: bucket.name.clone(),
        object_key: object.key.clone(),
        event: mutation.event_name().to_string(),
        kind: object.kind,
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        content_hash: object.content_hash.clone(),
        size: object.size,
        etag: object.etag.clone(),
        content_type: object.content_type.clone(),
        user_metadata_hash: object.user_metadata_hash.clone(),
        authz_revision: object.authz_revision,
        index_policy_snapshot: object.index_policy_snapshot.clone(),
        record_hash: object.record_hash.clone(),
        storage_class: object.storage_class.clone(),
        user_meta: object.user_meta.clone(),
        shard_map: object.shard_map.clone(),
        checksum: object.checksum.clone(),
        link: object.link.clone(),
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|timestamp| timestamp.to_rfc3339()),
    }
}

fn object_watch_event(
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
) -> ObjectWatchEvent {
    ObjectWatchEvent {
        id: 0,
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        key: object.key.clone(),
        event_type: mutation.watch_event_name().to_string(),
        version_id: Some(object.version_id),
        mutation_id: object.mutation_id,
        payload_hash: object.content_hash.clone(),
        etag: Some(object.etag.clone()),
        size: object.size,
        is_delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at,
    }
}

fn require_stream_update(
    updates: &[CoreTransactionUpdate],
    expected_stream_id: &str,
) -> Result<u64> {
    updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                stream_id,
                visible_sequence,
                ..
            } if stream_id == expected_stream_id => Some(*visible_sequence),
            _ => None,
        })
        .ok_or_else(|| anyhow!("object mutation is missing stream update {expected_stream_id}"))
}
