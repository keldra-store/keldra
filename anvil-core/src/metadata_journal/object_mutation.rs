use super::*;
use crate::persistence::ObjectWatchEvent;

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
    let committed_by_principal = transaction_principal
        .map(str::to_owned)
        .unwrap_or_else(|| object_metadata_partition_principal(bucket));

    let mut projection_predicates = Vec::new();
    let mut mutation_fences = std::collections::BTreeMap::new();
    let mut projection_overlays = std::collections::BTreeMap::new();
    let mut journal_entries = Vec::with_capacity(objects.len());
    for object in objects {
        let loaded = load_object_projection_snapshot(
            mvcc,
            bucket,
            object,
            transaction_principal.map(|principal| (transaction_id, principal)),
        )?;
        projection_predicates.extend(loaded.predicates);
        for fence in plan_object_mutation_fences(
            bucket,
            object,
            loaded.snapshot.snapshot_version,
            transaction_id,
        )? {
            mutation_fences.insert(fence.key.clone(), fence);
        }
        if transaction_principal.is_some() {
            for projection in plan_object_upsert(bucket, object, &loaded.snapshot, transaction_id)?
            {
                projection_overlays.insert(projection.key.clone(), projection);
            }
        }
        let watch_event = object_watch_event(bucket, object, ObjectJournalMutation::Put);
        journal_entries.push(crate::object_journal_commit::ObjectJournalCommitEntry {
            object: object.clone(),
            metadata_record_kind: ObjectJournalMutation::Put.object_record_kind().to_string(),
            metadata_payload: encode_object_version_body(&object_version_body(
                bucket,
                object,
                ObjectJournalMutation::Put,
                permit.fence_token,
            ))?,
            watch_event,
            projection_snapshot: Some(loaded.snapshot),
        });
    }
    let journal_job = crate::object_journal_commit::ObjectJournalCommitJob::new(
        mvcc.cluster_id(),
        transaction_id,
        bucket.clone(),
        journal_entries,
    )?
    .canonical_bytes()?;
    let mut predicates = additions.predicates;
    let mut mutations = mutation_fences.into_values().collect::<Vec<_>>();
    mutations.append(&mut additions.mutations);
    predicates.extend(projection_predicates);
    if let Some(transaction_principal) = transaction_principal {
        mvcc.stage_product_mutations_with_read_overlays(
            transaction_id,
            transaction_principal,
            mutations,
            projection_overlays.into_values().collect(),
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
        for event in additions.outbox_events {
            mvcc.open_transactions
                .add_stream_event(transaction_id, event, now_unix_ms)?;
        }
        mvcc.open_transactions
            .add_job(transaction_id, journal_job, now_unix_ms)?;
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
        vec![journal_job],
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

#[cfg(test)]
mod committed_projection_tests {
    use super::*;
    use crate::core_store::decode_object_metadata_row;
    use chrono::Utc;

    #[test]
    fn committed_object_projection_uses_the_raft_cursor_without_a_foreground_counter_cas() {
        let bucket = Bucket {
            id: 11,
            tenant_id: 7,
            name: "objects".into(),
            region: "local".into(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let object = Object {
            id: 1,
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            key: "leases/a.json".into(),
            kind: Default::default(),
            content_hash: "payload-hash".into(),
            size: 4,
            etag: "payload-hash".into(),
            content_type: Some("application/json".into()),
            version_id,
            mutation_id,
            index_policy_snapshot: "policy".into(),
            user_metadata_hash: "metadata".into(),
            authz_revision: 1,
            record_hash: "record".into(),
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: Some("local".into()),
            user_meta: None,
            shard_map: None,
            checksum: None,
            link: None,
        };
        let snapshot = ObjectProjectionSnapshot {
            snapshot_version: 3,
            projection_generation: 4,
            counter_max_id: 3,
            current: None,
            original: None,
            delete_current_successor: None,
        };
        let entry = crate::object_journal_commit::ObjectJournalCommitEntry {
            object: object.clone(),
            metadata_record_kind: ObjectJournalMutation::Put.object_record_kind().into(),
            metadata_payload: encode_object_version_body(&object_version_body(
                &bucket,
                &object,
                ObjectJournalMutation::Put,
                1,
            ))
            .unwrap(),
            watch_event: object_watch_event(&bucket, &object, ObjectJournalMutation::Put),
            projection_snapshot: Some(snapshot.clone()),
        };
        let cursor = crate::object_journal_commit::commit_cursor(9, 0, 0).unwrap();
        let (committed, mutations) =
            prepare_committed_entry(&bucket, &entry, cursor, "transaction").unwrap();

        assert_eq!(committed.object.id, i64::try_from(cursor).unwrap());
        assert_eq!(committed.watch_event.id, i64::try_from(cursor).unwrap());
        assert_eq!(
            decode_object_version_body(&committed.metadata_payload)
                .unwrap()
                .id,
            i64::try_from(cursor).unwrap()
        );
        let projected = mutations
            .iter()
            .filter_map(|mutation| mutation.value.as_deref())
            .find_map(|payload| decode_object_metadata_row(payload).ok())
            .expect("committed mutations contain an object projection row");
        assert_eq!(projected.id, i64::try_from(cursor).unwrap());

        let fences =
            plan_object_mutation_fences(&bucket, &object, snapshot.snapshot_version, "transaction")
                .unwrap();
        assert_eq!(fences.len(), 2);
        assert_ne!(fences[0].key, fences[1].key);
        assert_ne!(
            fences[0].key,
            object_current_logical_key(&bucket, &object.key).unwrap()
        );
        assert_ne!(
            fences[1].key,
            object_version_logical_key(&bucket, &object.key, object.version_id).unwrap()
        );
        for fence in fences {
            let fenced = decode_object_metadata_row(
                fence
                    .value
                    .as_deref()
                    .expect("object mutation fences are certified writes"),
            )
            .unwrap();
            assert_eq!(fenced.mutation_id, object.mutation_id);
            assert_eq!(fenced.key, object.key);
        }
    }
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
    let projection_predicates = loaded.predicates;
    let projection_snapshot = loaded.snapshot;
    let projection_overlays = if mvcc_transaction_id.is_some() {
        match mutation {
            ObjectJournalMutation::Put
            | ObjectJournalMutation::Copy
            | ObjectJournalMutation::DeleteMarker => {
                plan_object_upsert(bucket, object, &projection_snapshot, transaction_id)?
            }
            ObjectJournalMutation::DeleteVersion => {
                plan_object_delete_version(bucket, object, &projection_snapshot, transaction_id)?
            }
        }
    } else {
        Vec::new()
    };
    let mut mutations = plan_object_mutation_fences(
        bucket,
        object,
        projection_snapshot.snapshot_version,
        transaction_id,
    )?;
    let event = object_watch_event(bucket, object, mutation);
    let object_payload =
        encode_object_version_body(&object_version_body(bucket, object, mutation, fence_token))?;
    let journal_job = crate::object_journal_commit::ObjectJournalCommitJob::new(
        mvcc.cluster_id(),
        transaction_id,
        bucket.clone(),
        vec![crate::object_journal_commit::ObjectJournalCommitEntry {
            object: object.clone(),
            metadata_record_kind: mutation.object_record_kind().to_string(),
            metadata_payload: object_payload,
            watch_event: event,
            projection_snapshot: Some(projection_snapshot),
        }],
    )?
    .canonical_bytes()?;
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
    let mut predicates = Vec::new();
    predicates.extend(projection_predicates);
    if mvcc_transaction_id.is_some() {
        let transaction_principal = transaction_principal
            .ok_or_else(|| anyhow!("object metadata MVCC transaction principal missing"))?;
        mvcc.stage_product_mutations_with_read_overlays(
            transaction_id,
            transaction_principal,
            mutations,
            projection_overlays,
            u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
        )?;
        for event in outbox_events {
            mvcc.open_transactions.add_stream_event(
                transaction_id,
                event,
                u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
            )?;
        }
        mvcc.open_transactions.add_job(
            transaction_id,
            journal_job,
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
    commit_object_metadata_plan(
        mvcc,
        bucket,
        &committed_by_principal,
        transaction_id,
        mutations,
        predicates,
        outbox_events,
        vec![journal_job],
    )
    .await?;
    Ok(())
}

pub(crate) fn prepare_committed_entry(
    bucket: &Bucket,
    entry: &crate::object_journal_commit::ObjectJournalCommitEntry,
    cursor: u64,
    transaction_id: &str,
) -> Result<(
    crate::object_journal_commit::ObjectJournalCommitEntry,
    Vec<crate::mvcc_product::ProductMutation>,
)> {
    if entry.projection_snapshot.is_none() {
        return Ok((entry.clone(), Vec::new()));
    }
    let committed_id =
        i64::try_from(cursor).context("object journal cursor exceeds committed object ID range")?;
    let mut committed = entry.clone();
    committed.object.id = committed_id;
    committed.watch_event.id = committed_id;

    let mut body = decode_object_version_body(&committed.metadata_payload)?;
    if body.tenant_id != bucket.tenant_id
        || body.bucket_id != bucket.id
        || body.object_key != committed.object.key
        || body.version_id != committed.object.version_id.to_string()
        || body.mutation_id != committed.object.mutation_id.to_string()
    {
        bail!("object journal commit metadata payload scope mismatch");
    }
    body.id = committed_id;
    committed.metadata_payload = encode_object_version_body(&body)?;

    let mut snapshot = committed
        .projection_snapshot
        .clone()
        .expect("projection presence checked above");
    snapshot.projection_generation = cursor;
    snapshot.counter_max_id = committed_id;
    let projection_mutations = match ObjectJournalMutation::from_event_name(&body.event)? {
        ObjectJournalMutation::Put
        | ObjectJournalMutation::Copy
        | ObjectJournalMutation::DeleteMarker => {
            plan_object_upsert(bucket, &committed.object, &snapshot, transaction_id)?
        }
        ObjectJournalMutation::DeleteVersion => {
            plan_object_delete_version(bucket, &committed.object, &snapshot, transaction_id)?
        }
    };
    Ok((committed, projection_mutations))
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
    materialisation_jobs: Vec<Vec<u8>>,
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
    for job in materialisation_jobs {
        mvcc.open_transactions
            .add_job(&handle.transaction_id, job, now)?;
    }
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

/// Fence the background compactor that publishes a materialized metadata manifest.
///
/// Foreground object mutations deliberately do not use this assignment: they
/// may enter through any cluster member and are ordered by MVCC certification.
pub(super) async fn stage_object_metadata_compaction_assignment_guard(
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
