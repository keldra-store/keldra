use super::*;

pub(super) fn core_submit_request(
    req: SubmitPersonalDbChangesetRequest,
) -> Result<CoreSubmitChangeset, Status> {
    let client_debug_metadata = if req.client_debug_metadata_json.trim().is_empty() {
        None
    } else {
        Some(
            serde_json::from_str(&req.client_debug_metadata_json)
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
        )
    };
    Ok(CoreSubmitChangeset {
        tenant_id: req.tenant_id,
        database_id: req.database_id,
        principal: req.principal,
        session_token: req.session_token,
        request_id: req.request_id,
        idempotency_key: req.idempotency_key,
        base_log_index: req.base_log_index,
        base_log_hash: req.base_log_hash,
        client_log_epoch: req.client_log_epoch,
        membership_epoch: req.membership_epoch,
        policy_epoch: req.policy_epoch,
        leader_replica_id: req.leader_replica_id,
        voter_acks: req
            .voter_acks
            .into_iter()
            .map(|ack| crate::personaldb_submit::PersonalDbVoterAck {
                replica_id: ack.replica_id,
                log_index: ack.log_index,
                log_hash: ack.log_hash,
                signature: ack.signature,
            })
            .collect(),
        changeset_payload_hash: req.changeset_payload_hash,
        changeset_bytes: req.changeset_bytes,
        client_debug_metadata,
    })
}

pub(super) fn group_manifest_record(
    manifest: PersonalDbGroupManifest,
) -> PersonalDbGroupManifestRecord {
    PersonalDbGroupManifestRecord {
        format_version: manifest.format_version.into(),
        tenant_id: manifest.tenant_id,
        database_id: manifest.database_id,
        schema_hash: manifest.schema_hash,
        genesis_hash: manifest.genesis_hash,
        created_at: manifest.created_at,
        created_by: manifest.created_by,
        consistency_policy: manifest.consistency_policy,
        object_layout_version: manifest.object_layout_version.into(),
        active_membership_epoch: manifest.active_membership_epoch,
        active_policy_epoch: manifest.active_policy_epoch,
        current_row_index_generation: manifest.current_row_index_generation,
        current_projection_generation: manifest.current_projection_generation,
        manifest_hash: manifest.manifest_hash.unwrap_or_default(),
        manifest_signature: manifest.manifest_signature.map(signature_envelope_record),
    }
}

pub(super) fn committed_head_record(
    head: PersonalDbCommittedHead,
) -> PersonalDbCommittedHeadRecord {
    PersonalDbCommittedHeadRecord {
        format_version: head.format_version.into(),
        tenant_id: head.tenant_id,
        database_id: head.database_id,
        log_index: head.log_index,
        log_hash: head.log_hash,
        segment_ref: head.segment_ref,
        row_index_generation: head.row_index_generation,
        policy_epoch: head.policy_epoch,
        membership_epoch: head.membership_epoch,
        schema_hash: head.schema_hash,
        updated_at: head.updated_at,
        updated_by_node: head.updated_by_node,
        head_hash: head.head_hash.unwrap_or_default(),
        head_signature: head.head_signature.map(signature_envelope_record),
    }
}

pub(super) fn snapshots_head_record(
    head: PersonalDbSnapshotsHead,
) -> PersonalDbSnapshotsHeadRecord {
    PersonalDbSnapshotsHeadRecord {
        format_version: head.format_version.into(),
        tenant_id: head.tenant_id,
        database_id: head.database_id,
        latest_snapshot_log_index: head.latest_snapshot_log_index,
        latest_snapshot_log_hash: head.latest_snapshot_log_hash,
        latest_snapshot_manifest_ref: head.latest_snapshot_manifest_ref,
        retained_snapshot_count: head.retained_snapshot_count,
        updated_at: head.updated_at,
        updated_by_node: head.updated_by_node,
        head_hash: head.head_hash.unwrap_or_default(),
        head_signature: head.head_signature.map(signature_envelope_record),
    }
}

pub(super) fn certificate_record(
    certificate: crate::personaldb_control::PersonalDbCommitCertificate,
) -> PersonalDbCommitCertificateRecord {
    PersonalDbCommitCertificateRecord {
        format_version: certificate.format_version.into(),
        tenant_id: certificate.tenant_id,
        database_id: certificate.database_id,
        log_index: certificate.log_index,
        previous_log_hash: certificate.previous_log_hash,
        entry_hash: certificate.entry_hash,
        changeset_payload_hash: certificate.changeset_payload_hash,
        verified_envelope_hash: certificate.verified_envelope_hash,
        client_log_epoch: certificate.client_log_epoch,
        membership_epoch: certificate.membership_epoch,
        policy_epoch: certificate.policy_epoch,
        leader_replica_id: certificate.leader_replica_id,
        voter_acks_hash: certificate.voter_acks_hash,
        authz_revision: certificate.authz_revision,
        witness_node_id: certificate.witness_node_id,
        witnessed_at: certificate.witnessed_at,
        certificate_hash: certificate.certificate_hash.unwrap_or_default(),
        witness_signature: certificate.witness_signature.map(signature_envelope_record),
    }
}

fn signature_envelope_record(
    envelope: personaldb_protocol::SignatureEnvelopeV1,
) -> SignatureEnvelopeV1 {
    crate::personaldb_signing::signature_envelope_to_proto(&envelope)
}

pub(super) fn log_record(
    record: crate::formats::personaldb::PersonalDbLogRecord,
) -> PersonalDbLogRecord {
    PersonalDbLogRecord {
        log_index: record.log_index,
        client_log_epoch: record.client_log_epoch,
        membership_epoch: record.membership_epoch,
        policy_epoch: record.policy_epoch,
        previous_log_hash: hex::encode(record.previous_log_hash),
        changeset_payload_hash: hex::encode(record.changeset_payload_hash),
        verified_envelope_hash: hex::encode(record.verified_envelope_hash),
        certificate_hash: hex::encode(record.certificate_hash),
        payload_ref: String::from_utf8_lossy(&record.payload_ref).into_owned(),
        certificate_ref: String::from_utf8_lossy(&record.certificate_ref).into_owned(),
        inline_certificate_bytes: record.inline_certificate_bytes,
        entry_hash: hex::encode(record.entry_hash),
    }
}

pub(super) fn catch_up_response(response: CoreCatchUpResponse) -> PersonalDbCatchUpResponse {
    match response {
        CoreCatchUpResponse::Entries(entries) => PersonalDbCatchUpResponse {
            snapshot_required: false,
            snapshot_reason: String::new(),
            committed_head: Some(committed_head_record(entries.committed_head)),
            snapshots_head: None,
            entries: entries
                .entries
                .into_iter()
                .map(|entry| PersonalDbCatchUpEntry {
                    log_record: Some(log_record(entry.record)),
                    changeset_bytes: entry.changeset_bytes,
                    certificate: Some(certificate_record(entry.certificate)),
                    certificate_bytes: entry.certificate_bytes,
                })
                .collect(),
            has_more: entries.has_more,
        },
        CoreCatchUpResponse::SnapshotRequired(snapshot) => PersonalDbCatchUpResponse {
            snapshot_required: true,
            snapshot_reason: snapshot_reason(snapshot.reason).to_string(),
            committed_head: snapshot.committed_head.map(committed_head_record),
            snapshots_head: snapshot.snapshots_head.map(snapshots_head_record),
            entries: Vec::new(),
            has_more: false,
        },
    }
}

pub(super) fn watch_response(event: PersonalDbGroupWatchEvent) -> WatchPersonalDbGroupResponse {
    let (low, high) = split_u128(event.cursor);
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let database_id = payload.database_id.clone();
    let log_index = payload.log_index;
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchPersonalDbGroupResponse {
        cursor_low: low,
        cursor_high: high,
        database_id: database_id.clone(),
        event_type: payload.event_type,
        log_index,
        log_hash: payload.log_hash,
        changeset_payload_hash: payload.changeset_payload_hash,
        certificate_hash: payload.certificate_hash,
        committed_head_hash: payload.committed_head_hash,
        authz_revision: event.authz_revision,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "personaldb_group",
            partition_family: "personaldb_group",
            partition_id: database_id.clone(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "personaldb_group".to_string(),
            object_ref: database_id,
            authz_revision: event.authz_revision,
            index_generation: 0,
            personaldb_log_index: log_index,
            payload_hash,
            emitted_at,
        })),
    }
}

pub(super) fn snapshot_reason(reason: PersonalDbSnapshotRestoreReason) -> &'static str {
    match reason {
        PersonalDbSnapshotRestoreReason::MissingCommittedHead => "missing_committed_head",
        PersonalDbSnapshotRestoreReason::DivergentReplica => "divergent_replica",
    }
}

pub(super) fn validate_claim_tenant(
    claim_tenant_id: i64,
    request_tenant_id: i64,
) -> Result<(), Status> {
    if request_tenant_id != claim_tenant_id {
        return Err(Status::permission_denied("Tenant scope mismatch"));
    }
    Ok(())
}

pub(super) fn validate_database_id(database_id: &str) -> Result<(), Status> {
    if database_id.is_empty() {
        return Err(Status::invalid_argument("database_id must not be empty"));
    }
    if database_id.contains('/') || database_id.contains("..") {
        return Err(Status::invalid_argument(
            "database_id contains unsafe characters",
        ));
    }
    Ok(())
}

pub(super) fn validate_projection_id(projection_id: &str) -> Result<(), Status> {
    if projection_id.is_empty() {
        return Err(Status::invalid_argument("projection_id must not be empty"));
    }
    if projection_id.contains('/') || projection_id.contains("..") {
        return Err(Status::invalid_argument(
            "projection_id contains unsafe characters",
        ));
    }
    Ok(())
}

pub(super) fn validate_projection_definition_scope(
    tenant_id: i64,
    database_id: &str,
    definition: &ProjectionDefinition,
) -> Result<(), Status> {
    if definition.tenant_id != tenant_id.to_string()
        || definition.database_id != database_id
        || definition.target_database_id != database_id
    {
        return Err(Status::invalid_argument(
            "PersonalDB projection definition scope mismatch",
        ));
    }
    Ok(())
}

pub(super) fn validate_hex32(value: &str, field: &'static str) -> Result<(), Status> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Status::invalid_argument(format!("{field} must be hex32")));
    }
    Ok(())
}

pub(super) fn hex32_status(value: &str, field: &'static str) -> Result<Hash32, Status> {
    validate_hex32(value, field)?;
    hex::decode(value)
        .map_err(|_| Status::invalid_argument(format!("{field} must be hex32")))?
        .try_into()
        .map_err(|_| Status::invalid_argument(format!("{field} must be hex32")))
}

pub(super) fn personaldb_resource(tenant_id: i64, database_id: &str) -> String {
    format!("tenant-{tenant_id}/{database_id}")
}

fn personaldb_relation_for_action(action: AnvilAction) -> Result<&'static str, Status> {
    match action {
        AnvilAction::PersonalDbRead => Ok("get_snapshot"),
        AnvilAction::PersonalDbWatch => Ok("watch"),
        AnvilAction::PersonalDbCommit
        | AnvilAction::PersonalDbInsert
        | AnvilAction::PersonalDbUpdate
        | AnvilAction::PersonalDbDelete => Ok("apply_changeset"),
        _ => Err(Status::invalid_argument(
            "action is not valid for PersonalDB group access",
        )),
    }
}

pub(super) async fn personaldb_access_allowed(
    storage: &crate::storage::Storage,
    claims: &auth::Claims,
    database_id: &str,
    action: AnvilAction,
) -> Result<bool, Status> {
    let relation = personaldb_relation_for_action(action)?;
    let object_id = access_control::personaldb_group_object_id(claims.tenant_id, database_id);
    access_control::system_realm_relationship_allows(
        storage,
        claims,
        crate::system_realm::SYSTEM_PERSONALDB_GROUP_NAMESPACE,
        &object_id,
        relation,
        None,
    )
    .await
    .map_err(internal_status)
}

pub(super) async fn personaldb_actor_access_allowed(
    storage: &crate::storage::Storage,
    actor: &PersonalDbCommitActor,
    database_id: &str,
    action: AnvilAction,
) -> Result<bool, Status> {
    let claims = auth::Claims {
        sub: actor.principal.clone(),
        exp: 0,
        tenant_id: actor.tenant_id,
        jti: None,
    };
    personaldb_access_allowed(storage, &claims, database_id, action).await
}

pub(super) fn personaldb_group_partition_family() -> &'static str {
    "personaldb_group"
}

pub(super) fn personaldb_group_partition_id(tenant_id: i64, database_id: &str) -> String {
    hex::encode(hash32(
        format!("personaldb_group\0{tenant_id}\0{database_id}").as_bytes(),
    ))
}

pub(super) fn personaldb_projection_resource(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> String {
    format!("tenant-{tenant_id}/{database_id}/projections/{projection_id}")
}

pub(super) async fn personaldb_projection_access_allowed(
    storage: &crate::storage::Storage,
    claims: &auth::Claims,
    database_id: &str,
    _projection_id: &str,
    action: AnvilAction,
) -> Result<bool, Status> {
    let relation = personaldb_relation_for_action(action)?;
    let object_id = access_control::personaldb_group_object_id(claims.tenant_id, database_id);
    access_control::system_realm_relationship_allows(
        storage,
        claims,
        crate::system_realm::SYSTEM_PERSONALDB_GROUP_NAMESPACE,
        &object_id,
        relation,
        None,
    )
    .await
    .map_err(internal_status)
}

pub(super) fn configured_personaldb_snapshot_policy(
    config: &crate::config::Config,
) -> PersonalDbSnapshotPolicy {
    let default = PersonalDbSnapshotPolicy::default();
    PersonalDbSnapshotPolicy {
        entry_threshold: if config.personaldb_snapshot_entry_threshold == 0 {
            default.entry_threshold
        } else {
            config.personaldb_snapshot_entry_threshold
        },
        payload_bytes_threshold: if config.personaldb_snapshot_payload_bytes_threshold == 0 {
            default.payload_bytes_threshold
        } else {
            config.personaldb_snapshot_payload_bytes_threshold
        },
    }
}

pub(super) fn nonzero_limit(limit: u32) -> Result<usize, Status> {
    const DEFAULT_LIMIT: usize = 100;
    const MAX_LIMIT: usize = 1000;

    let limit = if limit == 0 {
        DEFAULT_LIMIT
    } else {
        usize::try_from(limit)
            .map_err(|_| Status::invalid_argument("max_entries exceeds supported range"))?
    };
    if limit > MAX_LIMIT {
        return Err(Status::invalid_argument(format!(
            "max_entries must not exceed {MAX_LIMIT}"
        )));
    }
    Ok(limit)
}

#[cfg(test)]
mod limit_tests {
    use super::*;

    #[test]
    fn personaldb_catch_up_limit_is_bounded() {
        assert_eq!(nonzero_limit(0).unwrap(), 100);
        assert_eq!(nonzero_limit(1000).unwrap(), 1000);
        assert_eq!(
            nonzero_limit(1001).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn authz_revision_lag_is_not_reported_as_an_internal_failure() {
        let status = internal_status(anyhow::anyhow!(
            "AuthzRevisionUnavailable: requested revision is not materialized"
        ));
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }
}

pub(super) fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

pub(super) fn join_u128(low: u64, high: u64) -> u128 {
    (u128::from(high) << 64) | u128::from(low)
}

pub(super) fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

pub(super) fn internal_status(err: impl std::fmt::Display) -> Status {
    let message = format!("{err:#}");
    if message.contains(AnvilErrorCode::AuthzRevisionUnavailable.as_str()) {
        Status::failed_precondition(message)
    } else {
        Status::internal(message)
    }
}

pub(super) fn personaldb_ownership_status(err: impl std::fmt::Display) -> Status {
    Status::failed_precondition(format!(
        "PersonalDB group ownership fence is not current: {err}"
    ))
}

pub(super) fn projection_writeback_rejected(reason: &'static str) -> Status {
    Status::failed_precondition(format!(
        "{}: {reason}",
        AnvilErrorCode::PersonalDbProjectionWriteBackRejected
    ))
}

pub(super) fn projection_writeback_rejected_owned(reason: String) -> Status {
    Status::failed_precondition(format!(
        "{}: {reason}",
        AnvilErrorCode::PersonalDbProjectionWriteBackRejected
    ))
}

pub(super) fn single_projection_writeback_source(
    definition: &ProjectionDefinition,
) -> Result<String, Status> {
    let sources = definition
        .table_mappings
        .iter()
        .map(|mapping| mapping.source_database_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if sources.len() != 1 {
        return Err(projection_writeback_rejected(
            "projection write-back has ambiguous source database bindings",
        ));
    }
    Ok(sources.into_iter().next().expect("one source database"))
}

pub(super) fn submit_changeset_response(
    committed: CommittedPersonalDbChangeset,
) -> Response<SubmitPersonalDbChangesetResponse> {
    let (watch_cursor_low, watch_cursor_high) = split_u128(committed.watch_cursor);
    Response::new(SubmitPersonalDbChangesetResponse {
        log_index: committed.log_index,
        log_hash: committed.log_hash,
        changeset_payload_hash: committed.changeset_payload_hash,
        verified_envelope_hash: committed.verified_envelope_hash,
        certificate_hash: committed.certificate_hash,
        certificate: Some(certificate_record(committed.certificate)),
        committed_head: Some(committed_head_record(committed.committed_head)),
        watch_cursor_low,
        watch_cursor_high,
    })
}

pub(super) fn projection_response(
    definition: ProjectionDefinition,
) -> Result<PersonalDbProjectionResponse, Status> {
    Ok(PersonalDbProjectionResponse {
        projection_definition_json: serde_json::to_string(&definition).map_err(internal_status)?,
    })
}

pub(super) fn projection_watch_response(
    event: PersonalDbProjectionWatchEvent,
) -> WatchPersonalDbProjectionResponse {
    let (low, high) = split_u128(event.cursor);
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let database_id = payload.database_id.clone();
    let projection_id = payload.projection_id.clone();
    let projection_log_index = payload.projection_log_index;
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchPersonalDbProjectionResponse {
        cursor_low: low,
        cursor_high: high,
        database_id: database_id.clone(),
        projection_id: projection_id.clone(),
        event_type: payload.event_type,
        source_database_id: payload.source_database_id,
        source_log_index: payload.source_log_index,
        source_log_hash: payload.source_log_hash,
        projection_log_index,
        projection_log_hash: payload.projection_log_hash,
        definition_hash: payload.definition_hash,
        authz_revision: event.authz_revision,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "personaldb_projection",
            partition_family: "personaldb_projection",
            partition_id: format!("{database_id}/{projection_id}"),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "personaldb_projection".to_string(),
            object_ref: format!("{database_id}/{projection_id}"),
            authz_revision: event.authz_revision,
            index_generation: 0,
            personaldb_log_index: projection_log_index,
            payload_hash,
            emitted_at,
        })),
    }
}
