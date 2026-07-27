use super::*;
use crate::persistence::ObjectWatchEvent;
use crate::watch_log;

pub(crate) async fn append_object_mutation_with_permit(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    permit: &PartitionWritePermit,
) -> Result<()> {
    append_object_mutation_with_permit_in_transaction(
        storage,
        Some(mvcc),
        bucket,
        object,
        mutation,
        permit,
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
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    append_object_mutation_with_permit_in_transaction_and_audit(
        storage,
        mvcc,
        bucket,
        object,
        mutation,
        permit,
        transaction_id,
        transaction_principal,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_object_mutation_with_permit_in_transaction_and_audit(
    storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    permit: &PartitionWritePermit,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<()> {
    require_object_metadata_permit(bucket, permit)?;
    append_object_mutation_inner(
        storage,
        mvcc,
        bucket,
        object,
        mutation,
        permit.fence_token,
        transaction_id,
        transaction_principal,
        audit_event,
    )
    .await
}

pub(crate) async fn append_object_put_mutations_with_permit_in_transaction(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    objects: &[Object],
    permit: &PartitionWritePermit,
    transaction_id: &str,
    transaction_principal: &str,
    additions: crate::mvcc_product::ProductMutationPlan,
) -> Result<()> {
    append_object_put_mutations_with_permit_inner(
        storage,
        Some(mvcc),
        bucket,
        objects,
        permit,
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
    transaction_id: &str,
    additions: crate::mvcc_product::ProductMutationPlan,
) -> Result<()> {
    append_object_put_mutations_with_permit_inner(
        storage,
        Some(mvcc),
        bucket,
        objects,
        permit,
        transaction_id,
        None,
        additions,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn append_object_put_mutations_with_permit_inner(
    _storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    bucket: &Bucket,
    objects: &[Object],
    permit: &PartitionWritePermit,
    transaction_id: &str,
    transaction_principal: Option<&str>,
    mut additions: crate::mvcc_product::ProductMutationPlan,
) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    require_object_metadata_permit(bucket, permit)?;
    let mvcc = mvcc.ok_or_else(|| anyhow!("MVCC staging handle is required"))?;
    let scope_partition = hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    let committed_by_principal = transaction_principal
        .map(str::to_owned)
        .unwrap_or_else(|| object_metadata_partition_principal(bucket));

    let metadata_stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let mut operations = Vec::with_capacity(objects.len() * 16);
    let mut projection_mutations = Vec::new();
    let mut projection_predicates = Vec::new();
    let mut watch_events = Vec::with_capacity(objects.len());
    for object in objects {
        let loaded = load_object_projection_snapshot(
            mvcc,
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
        watch_events.push(object_watch_event(
            bucket,
            object,
            ObjectJournalMutation::Put,
        ));
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
    }
    let operations = coalesce_coremeta_operations_last_write_wins(operations);
    let event_plan = plan_metadata_events(
        mvcc,
        bucket,
        operations,
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let watch_inputs = objects.iter().zip(&watch_events).collect::<Vec<_>>();
    let watch_plan = watch_log::plan_object_watch_appends(
        mvcc,
        bucket,
        &watch_inputs,
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let mutations = event_plan.mutations;
    let mut predicates = additions.predicates;
    predicates.push(event_plan.head_predicate);
    predicates.extend(watch_plan.predicates);
    let mut mutations = mutations;
    mutations.extend(watch_plan.mutations);
    mutations.extend(projection_mutations);
    mutations.append(&mut additions.mutations);
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
        stage_object_metadata_assignment_guard(mvcc, bucket, transaction_id, transaction_principal)
            .await?;
        for event in additions.outbox_events {
            mvcc.open_transactions
                .add_stream_event(transaction_id, event, now_unix_ms)?;
        }
        return Ok(());
    }
    commit_object_metadata_plan(
        mvcc,
        bucket,
        &committed_by_principal,
        transaction_id,
        mutations,
        predicates,
        additions.outbox_events,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod active_path_contract {
    #[test]
    fn mvcc_object_mutation_path_has_no_per_transition_signing_dependency() {
        let source = include_str!("object_mutation.rs");
        let signing_key_parameter = ["partition_owner_", "signing_key"].concat();
        let signing_call = ["sign_", "manifest("].concat();
        assert!(!source.contains(&signing_key_parameter));
        assert!(!source.contains(&signing_call));
    }
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

pub(super) async fn append_object_mutation_inner(
    _storage: &Storage,
    mvcc: Option<&crate::mvcc_bootstrap::MvccSubsystem>,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
    audit_event: Option<&crate::tenant_audit::TenantAuditEvent>,
) -> Result<()> {
    let mvcc_transaction_id = transaction_id;
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
    let event = object_watch_event(bucket, object, mutation);
    let object_payload =
        encode_object_version_body(&object_version_body(bucket, object, mutation, fence_token))?;
    let mut operations = Vec::with_capacity(1);
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
    let event_plan = plan_metadata_events(
        mvcc,
        bucket,
        operations,
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let watch_plan = watch_log::plan_object_watch_appends(
        mvcc,
        bucket,
        &[(object, &event)],
        transaction_principal.map(|principal| (transaction_id, principal)),
    )?;
    let mut mutations = event_plan.mutations;
    mutations.extend(watch_plan.mutations);
    mutations.extend(projection_mutations);
    let mut outbox_events = Vec::new();
    if let Some(audit_event) = audit_event {
        let audit_plan = crate::tenant_audit::tenant_audit_mvcc_plan(
            audit_event,
            u64::try_from(object.id).unwrap_or(1),
            transaction_id,
        )?;
        mutations.extend(audit_plan.mutations);
        outbox_events.extend(audit_plan.outbox_events);
    }
    let mut predicates = vec![event_plan.head_predicate];
    predicates.extend(watch_plan.predicates);
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
        for event in outbox_events {
            mvcc.open_transactions.add_stream_event(
                transaction_id,
                event,
                u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
            )?;
        }
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
        stage_object_metadata_assignment_guard(mvcc, bucket, transaction_id, transaction_principal)
            .await?;
        return Ok(());
    }
    commit_object_metadata_plan(
        mvcc,
        bucket,
        &committed_by_principal,
        transaction_id,
        mutations,
        predicates,
        outbox_events,
    )
    .await?;
    Ok(())
}

async fn commit_object_metadata_plan(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    principal: &str,
    logical_idempotency_key: &str,
    mutations: Vec<crate::mvcc_product::ProductMutation>,
    predicates: Vec<(
        crate::mvcc_transaction::LogicalKey,
        crate::mvcc_transaction::PredicateKind,
    )>,
    outbox_events: Vec<crate::mvcc_outbox::StreamOutboxEvent>,
) -> Result<()> {
    let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            principal,
            logical_idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await?;
    mvcc.stage_product_mutations(&handle.transaction_id, principal, mutations, now)?;
    for predicate in predicates {
        mvcc.stage_predicate(
            &handle.transaction_id,
            principal,
            predicate.0,
            predicate.1,
            now,
        )?;
    }
    for event in outbox_events {
        mvcc.open_transactions
            .add_stream_event(&handle.transaction_id, event, now)?;
    }
    stage_object_metadata_assignment_guard(mvcc, bucket, &handle.transaction_id, principal).await?;
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            principal,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )
        .await?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => {
            Err(anyhow!("object metadata transaction aborted: {reason:?}"))
        }
    }
}

pub(super) async fn stage_object_metadata_assignment_guard(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    transaction_id: &str,
    principal: &str,
) -> Result<()> {
    let identity = format!("{}:{}", bucket.tenant_id, bucket.id);
    let assignment = mvcc
        .reconcile_work_assignment("object-metadata", &identity)
        .await?
        .ok_or_else(|| anyhow!("this node does not own the object metadata assignment"))?;
    mvcc.stage_assignment_guard(
        transaction_id,
        principal,
        &assignment,
        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    )
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
