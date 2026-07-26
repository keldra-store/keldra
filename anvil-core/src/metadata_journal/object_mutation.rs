use super::*;
use crate::core_store::{
    CoreMutationBatchAdditions, CoreMutationPrecondition, CoreMutationRootPublication,
    ObjectMetadataProjectionMutation, core_meta_root_key_hash,
};
use crate::formats::writer::WriterFamily;
use crate::persistence::ObjectWatchEvent;
use crate::watch_log;
use anyhow::bail;

const MAX_STREAM_HEAD_RETRIES: usize = 64;

pub(crate) async fn append_object_mutation_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    append_object_mutation_with_permit_in_transaction(
        storage,
        Some(mvcc),
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
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
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
        mvcc,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
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
        Some(mvcc),
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    objects: &[Object],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    additions: CoreMutationBatchAdditions,
) -> Result<()> {
    append_object_put_mutations_with_permit_inner(
        storage,
        Some(mvcc),
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
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
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
    let committed_by_principal = transaction_principal
        .map(str::to_owned)
        .unwrap_or_else(|| object_metadata_partition_principal(bucket));

    let metadata_stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let metadata_stream_precondition = core_store
        .stream_head_precondition(&metadata_stream_id)
        .await?;
    let watch_stream_id = watch_log::object_watch_stream_id(bucket.tenant_id, bucket.id);
    let watch_stream_precondition = core_store
        .stream_head_precondition(&watch_stream_id)
        .await?;
    let first_watch_sequence = stream_precondition_next_sequence(&watch_stream_precondition)?;

    let mut preconditions = vec![
        partition_precondition,
        metadata_stream_precondition,
        watch_stream_precondition,
    ];
    let mut operations = Vec::with_capacity(objects.len() * 16);
    let mut projection_mutations = Vec::new();
    let mut projection_predicates = Vec::new();
    for (index, object) in objects.iter().enumerate() {
        let loaded = load_object_projection_snapshot(
            mvcc.ok_or_else(|| anyhow!("MVCC staging handle is required"))?,
            bucket,
            object,
            transaction_principal.map(|principal| (transaction_id, principal)),
        )?;
        projection_mutations.extend(plan_object_upsert(
            bucket,
            object,
            &loaded.snapshot,
            transaction_id,
        )?);
        projection_predicates.extend(loaded.predicates);
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
            Some(loaded.snapshot.projection_generation),
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
        committed_by_principal: committed_by_principal.clone(),
        root_publications,
        preconditions,
        operations,
    };
    let mvcc = mvcc.ok_or_else(|| anyhow!("MVCC staging handle is required"))?;
    let predicates = object_batch_precondition_predicates(&batch.preconditions)?;
    let event_plan = plan_metadata_events(
        mvcc,
        bucket,
        batch.operations,
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let mutations = event_plan.mutations;
    let mut predicates = predicates;
    predicates.push(event_plan.head_predicate);
    let mut mutations = mutations;
    mutations.extend(projection_mutations);
    predicates.extend(projection_predicates);
    if let Some(transaction_principal) = transaction_principal {
        mvcc.stage_product_mutations(
            transaction_id,
            transaction_principal,
            mutations,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )?;
        let now_unix_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        for (key, kind) in predicates {
            mvcc.stage_predicate(
                transaction_id,
                transaction_principal,
                key,
                kind,
                now_unix_ms,
            )?;
        }
        return Ok(());
    }
    mvcc.autocommit_product_mutations_with_predicates(
        &committed_by_principal,
        transaction_id,
        mutations,
        predicates,
        crate::mvcc_transaction::DurabilityLevel::Local,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )
    .await?;
    Ok(())
}

fn object_batch_precondition_predicates(
    batch_preconditions: &[CoreMutationPrecondition],
) -> Result<
    Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
> {
    use crate::mvcc_transaction::PredicateKind;

    let mut predicates = Vec::new();
    for precondition in batch_preconditions {
        let CoreMutationPrecondition::CoreMetaRow {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            require_absent: true,
            require_present: false,
        } = precondition
        else {
            continue;
        };
        if *table_id != crate::core_store::TABLE_NATIVE_IDEMPOTENCY_ROW
            || cf != crate::core_store::CF_TRANSACTIONS
            || expected_payload_hash.is_some()
        {
            continue;
        }
        let key = crate::mvcc_product::coremeta_logical_key(cf, *table_id, tuple_key)?;
        if !predicates.iter().any(|(current, _)| current == &key) {
            predicates.push((key, PredicateKind::Absent));
        }
    }
    Ok(predicates)
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
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
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
            mvcc,
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
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    core_store: &CoreStore,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    mvcc_transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    let scope_partition = hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    let implicit_transaction_id = format!(
        "object-metadata:{}:{}",
        object.mutation_id,
        mutation.event_name()
    );
    let transaction_id = mvcc_transaction_id.unwrap_or(&implicit_transaction_id);
    let committed_by_principal = transaction_principal
        .map(str::to_owned)
        .unwrap_or_else(|| object_metadata_partition_principal(bucket));
    let ancillary_projection_mutation = match mutation {
        ObjectJournalMutation::Put
        | ObjectJournalMutation::Copy
        | ObjectJournalMutation::DeleteMarker => ObjectMetadataProjectionMutation::Upsert,
        ObjectJournalMutation::DeleteVersion => ObjectMetadataProjectionMutation::DeleteVersion,
    };
    let mvcc = mvcc.ok_or_else(|| anyhow!("MVCC staging handle is required"))?;
    let loaded = load_object_projection_snapshot(
        mvcc,
        bucket,
        object,
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let projection_mutations = match mutation {
        ObjectJournalMutation::Put
        | ObjectJournalMutation::Copy
        | ObjectJournalMutation::DeleteMarker => {
            plan_object_upsert(bucket, object, &loaded.snapshot, transaction_id)?
        }
        ObjectJournalMutation::DeleteVersion => {
            plan_object_delete_version(bucket, object, &loaded.snapshot, transaction_id)?
        }
    };
    let projection_predicates = loaded.predicates;
    let metadata_stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let metadata_stream_precondition = core_store
        .stream_head_precondition(&metadata_stream_id)
        .await?;
    let event = object_watch_event(bucket, object, mutation);
    let watch = watch_log::prepare_object_watch_append(
        core_store,
        bucket,
        object,
        &event,
        &scope_partition,
        &core_meta_root_key_hash(&scope_partition),
        Some(loaded.snapshot.projection_generation),
        transaction_id,
    )
    .await?;
    let object_payload =
        encode_object_version_body(&object_version_body(bucket, object, mutation, fence_token))?;
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.push(metadata_stream_precondition);
    preconditions.extend(watch.preconditions);
    let mut operations = Vec::with_capacity(3);
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
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.to_string(),
        scope_partition: scope_partition.clone(),
        committed_by_principal: committed_by_principal.clone(),
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
    let predicates = object_batch_precondition_predicates(&batch.preconditions)?;
    let event_plan = plan_metadata_events(
        mvcc,
        bucket,
        batch.operations,
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let mut mutations = event_plan.mutations;
    mutations.extend(projection_mutations);
    let mut predicates = predicates;
    predicates.push(event_plan.head_predicate);
    predicates.extend(projection_predicates);
    if mvcc_transaction_id.is_some() {
        let transaction_principal = transaction_principal
            .ok_or_else(|| anyhow!("object metadata MVCC transaction principal missing"))?;
        mvcc.stage_product_mutations(
            transaction_id,
            transaction_principal,
            mutations,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )?;
        let now_unix_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
        for (key, kind) in predicates {
            mvcc.stage_predicate(
                transaction_id,
                transaction_principal,
                key,
                kind,
                now_unix_ms,
            )?;
        }
        return Ok(());
    }
    mvcc.autocommit_product_mutations_with_predicates(
        &committed_by_principal,
        transaction_id,
        mutations,
        predicates,
        crate::mvcc_transaction::DurabilityLevel::Local,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )
    .await?;
    core_store
        .materialize_object_metadata_ancillary_projections(
            bucket,
            object,
            ancillary_projection_mutation,
        )
        .await?;
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
