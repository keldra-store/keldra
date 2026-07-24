use crate::anvil_api::personal_db_service_server::PersonalDbService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control,
    anvil_personaldb_sqlite_changeset::iterate_changeset,
    auth, authz_journal,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace},
    core_store::CoreMutationPrecondition,
    error_codes::AnvilErrorCode,
    formats::{Hash32, hash32, personaldb::PersonalDbLogRecord as CorePersonalDbLogRecord},
    partition_fence::{
        PartitionOwnerStatus, PartitionRecoveryAcquire, PartitionWritePermit,
        acquire_partition_recovery, partition_write_precondition, publish_partition_ready,
        read_partition_owner,
    },
    permissions::AnvilAction,
    personaldb_catchup::{
        PersonalDbCatchUpRequest as CoreCatchUpRequest,
        PersonalDbCatchUpResponse as CoreCatchUpResponse, PersonalDbSnapshotRestoreReason,
        personaldb_catch_up,
    },
    personaldb_commit_store::{
        decode_commit_certificate, personaldb_commit_certificate_ref_name,
        write_personaldb_changeset_payload, write_personaldb_commit_certificate,
    },
    personaldb_control::{PersonalDbCommitCertificate, PersonalDbGroupManifest},
    personaldb_envelope::{
        PersonalDbEnvelopeDerivationInput, TableOperation, VerifiedMutationEnvelope,
        derive_verified_mutation_envelope,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, decode_committed_head,
        read_personaldb_committed_head, read_personaldb_group_manifest,
        write_personaldb_committed_head, write_personaldb_committed_head_with_preconditions,
        write_personaldb_group_manifest,
    },
    personaldb_projection::{
        ProjectionDefinition, WriteBackPolicy, list_projection_definitions_for_database,
        list_projection_definitions_for_source, read_projection_definition,
        write_projection_definition,
    },
    personaldb_projection_builder::{
        ProjectionAuthorizationCheck, ProjectionAuthorizationDecisions, ProjectionBuildInput,
        build_projection_changeset_with_authorization, collect_projection_authorization_checks,
    },
    personaldb_projection_writeback::{
        ProjectionWriteBackInput, build_projection_writeback_changeset,
    },
    personaldb_proposal_admission::{
        BeginWitnessSigningV1, PersonalDbAdmissionAuthority,
        ProposalAdmissionReservationIdentityV1, ProposalIdempotencyClaimIdentityV1,
        SignCertificateAndHeadV1, acknowledge_personaldb_witness_receipt,
        begin_personaldb_witness_signing, commit_personaldb_witnessed_proposal,
        derive_reservation_id, personaldb_group_leader_lease_id, reserve_personaldb_proposal,
        sign_personaldb_certificate_and_head_with_keyring,
    },
    personaldb_row_index::{PersonalDbRowIndexWrite, write_personaldb_row_index},
    personaldb_schema::{
        read_personaldb_schema_sql, validate_changeset_tables_registered, validate_schema_sql,
        write_personaldb_schema_sql,
    },
    personaldb_segment::{
        PersonalDbLogSegmentWrite, preview_personaldb_log_segment_ref, write_personaldb_log_segment,
    },
    personaldb_snapshot_builder::{
        PersonalDbSnapshotBuildRequest, PersonalDbSnapshotPolicy, maybe_build_personaldb_snapshot,
    },
    personaldb_submit::{
        SubmitPersonalDbChangeset as CoreSubmitChangeset, client_proposal_hash,
        default_max_changeset_size, validate_submit_personaldb_changeset,
    },
    personaldb_watch::{
        PersonalDbGroupWatchEvent, PersonalDbGroupWatchPayload, PersonalDbProjectionWatchEvent,
        PersonalDbProjectionWatchPayload, append_personaldb_group_watch_record,
        append_personaldb_projection_watch_record, list_personaldb_group_watch_event_page,
        list_personaldb_projection_watch_event_page,
    },
    services::watch_envelope::{self, WatchEnvelopeParts},
};
use personaldb_protocol::SignaturePurpose;
use tokio::sync::OwnedMutexGuard;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
struct PersonalDbCommitActor {
    tenant_id: i64,
    principal: String,
    bearer_token: Option<String>,
    require_public_commit_authorization: bool,
    require_admission_protocol: bool,
}

#[derive(Debug, Clone)]
struct CommittedPersonalDbChangeset {
    log_index: u64,
    log_hash: String,
    changeset_payload_hash: String,
    verified_envelope_hash: String,
    certificate_hash: String,
    certificate: PersonalDbCommitCertificate,
    committed_head: PersonalDbCommittedHead,
    watch_cursor: u128,
    authz_revision: u64,
}

#[tonic::async_trait]
impl PersonalDbService for AppState {
    type WatchPersonalDbGroupStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchPersonalDbGroupResponse, Status>> + Send>,
    >;
    type WatchPersonalDbProjectionStream = std::pin::Pin<
        Box<
            dyn futures_core::Stream<Item = Result<WatchPersonalDbProjectionResponse, Status>>
                + Send,
        >,
    >;

    async fn create_personal_db_group(
        &self,
        request: Request<CreatePersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroupResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_database_id(&req.database_id)?;
        validate_hex32(&req.schema_hash, "schema_hash")?;
        validate_hex32(&req.genesis_hash, "genesis_hash")?;
        validate_schema_sql(&req.schema_sql, &req.schema_hash)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        let resource = personaldb_resource(claims.tenant_id, &req.database_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::PersonalDbCreate,
            &resource,
        )
        .await?;
        let protocol_keyring = self.personaldb_protocol_keyring.as_ref();
        if read_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .is_some()
        {
            return Err(Status::already_exists("PersonalDB group already exists"));
        }
        self.persistence
            .ensure_personaldb_group_ownership_fence(claims.tenant_id, &req.database_id)
            .await
            .map_err(personaldb_ownership_status)?;

        let now = now_rfc3339();
        let manifest = PersonalDbGroupManifest {
            format_version: 2,
            tenant_id: claims.tenant_id.to_string(),
            database_id: req.database_id.clone(),
            schema_hash: req.schema_hash.clone(),
            genesis_hash: req.genesis_hash.clone(),
            created_at: now.clone(),
            created_by: claims.sub.clone(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 1,
            active_policy_epoch: 1,
            current_row_index_generation: 0,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
        .seal(protocol_keyring)
        .await
        .map_err(internal_status)?;
        write_personaldb_schema_sql(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            &req.schema_sql,
            &req.schema_hash,
        )
        .await
        .map_err(internal_status)?;
        write_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &manifest,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;

        let committed_head = PersonalDbCommittedHead {
            format_version: 2,
            tenant_id: claims.tenant_id.to_string(),
            database_id: req.database_id,
            log_index: 0,
            log_hash: manifest.genesis_hash.clone(),
            segment_ref: String::new(),
            row_index_generation: 0,
            policy_epoch: manifest.active_policy_epoch,
            membership_epoch: manifest.active_membership_epoch,
            schema_hash: manifest.schema_hash.clone(),
            updated_at: now,
            updated_by_node: claims.sub.clone(),
            head_hash: None,
            head_signature: None,
        }
        .seal(protocol_keyring)
        .await
        .map_err(internal_status)?;
        write_personaldb_committed_head(
            &self.storage,
            claims.tenant_id,
            &committed_head.database_id,
            &committed_head,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;
        access_control::grant_personaldb_group_defaults(
            &self.persistence,
            claims.tenant_id,
            &committed_head.database_id,
            &claims.sub,
            &claims.sub,
            "grant creator PersonalDB group owner",
        )
        .await
        .map_err(internal_status)?;

        Ok(Response::new(PersonalDbGroupResponse {
            manifest: Some(group_manifest_record(manifest)),
            committed_head: Some(committed_head_record(committed_head)),
        }))
    }

    async fn get_personal_db_group(
        &self,
        request: Request<GetPersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroupResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        if !personaldb_access_allowed(
            &self.storage,
            &claims,
            &req.database_id,
            AnvilAction::PersonalDbRead,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let manifest = read_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB group not found"))?;
        let committed_head = read_personaldb_committed_head(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;

        Ok(Response::new(PersonalDbGroupResponse {
            manifest: Some(group_manifest_record(manifest)),
            committed_head: committed_head.map(committed_head_record),
        }))
    }

    async fn create_personal_db_projection(
        &self,
        request: Request<CreatePersonalDbProjectionRequest>,
    ) -> Result<Response<PersonalDbProjectionResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let mut definition: ProjectionDefinition =
            serde_json::from_str(&req.projection_definition_json)
                .map_err(|err| Status::invalid_argument(err.to_string()))?;
        validate_projection_definition_scope(claims.tenant_id, &req.database_id, &definition)?;
        validate_projection_id(&definition.projection_id)?;
        let resource = personaldb_projection_resource(
            claims.tenant_id,
            &req.database_id,
            &definition.projection_id,
        );
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::PersonalDbCreate,
            &resource,
        )
        .await?;
        read_personaldb_group_manifest(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection group not found"))?;
        for source_database_id in &definition.source_database_ids {
            validate_database_id(source_database_id)?;
            read_personaldb_group_manifest(
                &self.storage,
                claims.tenant_id,
                source_database_id,
                self.personaldb_protocol_keyring.trust_store(),
            )
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::not_found("PersonalDB projection source group not found"))?;
        }
        if read_projection_definition(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            &definition.projection_id,
        )
        .await
        .map_err(internal_status)?
        .is_some()
        {
            return Err(Status::already_exists(
                "PersonalDB projection already exists",
            ));
        }
        definition.definition_hash = None;
        let definition = definition
            .seal()
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        write_projection_definition(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            &definition,
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(projection_response(definition)?))
    }

    async fn get_personal_db_projection(
        &self,
        request: Request<GetPersonalDbProjectionRequest>,
    ) -> Result<Response<PersonalDbProjectionResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        validate_projection_id(&req.projection_id)?;
        if !personaldb_projection_access_allowed(
            &self.storage,
            &claims,
            &req.database_id,
            &req.projection_id,
            AnvilAction::PersonalDbRead,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let definition = read_projection_definition(
            &self.storage,
            claims.tenant_id,
            &req.database_id,
            &req.projection_id,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection not found"))?;
        Ok(Response::new(projection_response(definition)?))
    }

    async fn submit_personal_db_changeset(
        &self,
        request: Request<SubmitPersonalDbChangesetRequest>,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let bearer_token = request_bearer_token(&request)?.to_string();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let core_request = core_submit_request(req)?;
        let actor = PersonalDbCommitActor {
            tenant_id: claims.tenant_id,
            principal: claims.sub.clone(),
            bearer_token: Some(bearer_token),
            require_public_commit_authorization: true,
            require_admission_protocol: true,
        };
        let projection_definitions = list_projection_definitions_for_database(
            &self.storage,
            claims.tenant_id,
            &core_request.database_id,
        )
        .await
        .map_err(internal_status)?;
        if !projection_definitions.is_empty() {
            return self
                .handle_personaldb_projection_writeback(core_request, actor, projection_definitions)
                .await;
        }
        let source_database_id = core_request.database_id.clone();
        let source_changeset_bytes = core_request.changeset_bytes.clone();
        let committed = self
            .commit_personaldb_changeset(core_request, actor)
            .await?;
        self.build_personaldb_projections_for_source_commit(
            claims.tenant_id,
            &source_database_id,
            &source_changeset_bytes,
            committed.log_index,
            &committed.log_hash,
            committed.authz_revision,
        )
        .await?;
        Ok(submit_changeset_response(committed))
    }

    async fn catch_up_personal_db(
        &self,
        request: Request<PersonalDbCatchUpRequest>,
    ) -> Result<Response<PersonalDbCatchUpResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        if !personaldb_access_allowed(
            &self.storage,
            &claims,
            &req.database_id,
            AnvilAction::PersonalDbRead,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let response = personaldb_catch_up(
            &self.storage,
            CoreCatchUpRequest {
                tenant_id: claims.tenant_id,
                database_id: req.database_id,
                principal: req.principal,
                replica_id: req.replica_id,
                have_log_index: req.have_log_index,
                have_log_hash: req.have_log_hash,
                max_entries: nonzero_limit(req.max_entries)?,
            },
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(catch_up_response(response)))
    }

    async fn watch_personal_db_group(
        &self,
        request: Request<WatchPersonalDbGroupRequest>,
    ) -> Result<Response<Self::WatchPersonalDbGroupStream>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        if !personaldb_access_allowed(
            &self.storage,
            &claims,
            &req.database_id,
            AnvilAction::PersonalDbWatch,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let stream_id = crate::personaldb_watch::personaldb_group_watch_stream_id(
            claims.tenant_id,
            &req.database_id,
        );
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let tenant_id = claims.tenant_id;
        let database_id = req.database_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match list_personaldb_group_watch_event_page(
                        &storage,
                        tenant_id,
                        &database_id,
                        last_cursor,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(internal_status(error))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx.send(Ok(watch_response(event))).await.is_err() {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }
                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchPersonalDbGroupStream
        ))
    }

    async fn watch_personal_db_projection(
        &self,
        request: Request<WatchPersonalDbProjectionRequest>,
    ) -> Result<Response<Self::WatchPersonalDbProjectionStream>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        validate_projection_id(&req.projection_id)?;
        if !personaldb_projection_access_allowed(
            &self.storage,
            &claims,
            &req.database_id,
            &req.projection_id,
            AnvilAction::PersonalDbWatch,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let stream_id = crate::personaldb_watch::personaldb_projection_watch_stream_id(
            claims.tenant_id,
            &req.database_id,
            &req.projection_id,
        );
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let tenant_id = claims.tenant_id;
        let database_id = req.database_id;
        let projection_id = req.projection_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match list_personaldb_projection_watch_event_page(
                        &storage,
                        tenant_id,
                        &database_id,
                        &projection_id,
                        last_cursor,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(internal_status(error))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx.send(Ok(projection_watch_response(event))).await.is_err() {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }
                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchPersonalDbProjectionStream
        ))
    }
}

impl AppState {
    fn personaldb_node_id(&self) -> String {
        if !self.config.node_id.is_empty() {
            return self.config.node_id.clone();
        }
        if !self.config.public_api_addr.is_empty() {
            return self.config.public_api_addr.clone();
        }
        if !self.config.api_listen_addr.is_empty() {
            return self.config.api_listen_addr.clone();
        }
        if !self.config.region.is_empty() {
            return self.config.region.clone();
        }
        "local-anvil-node".to_string()
    }

    async fn personaldb_commit_guard(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> OwnedMutexGuard<()> {
        let key = format!("{tenant_id}:{database_id}");
        let lock = {
            let mut locks = self.personaldb_commit_locks.lock().await;
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&key).and_then(std::sync::Weak::upgrade) {
                lock
            } else {
                let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(key, std::sync::Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    async fn acquire_personaldb_group_write_permit(
        &self,
        tenant_id: i64,
        database_id: &str,
        recovered_through_sequence: u64,
        recovered_manifest_hash: &str,
    ) -> Result<PartitionWritePermit, Status> {
        validate_hex32(recovered_manifest_hash, "recovered_manifest_hash")?;
        self.persistence
            .ensure_personaldb_group_ownership_fence(tenant_id, database_id)
            .await
            .map_err(personaldb_ownership_status)?;
        let partition_family = personaldb_group_partition_family().to_string();
        let partition_id = personaldb_group_partition_id(tenant_id, database_id);
        let owner_node_id = self.personaldb_node_id();
        let now_nanos = chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| Status::internal("partition owner timestamp overflow"))?;
        let recovering = acquire_partition_recovery(
            &self.storage,
            PartitionRecoveryAcquire {
                partition_family: partition_family.clone(),
                partition_id: partition_id.clone(),
                owner_node_id: owner_node_id.clone(),
                recovered_through_sequence,
                recovered_manifest_hash: recovered_manifest_hash.to_string(),
                now_nanos,
            },
            self.persistence.partition_owner_signing_key(),
        )
        .await
        .map_err(internal_status)?;
        if recovering.status == PartitionOwnerStatus::Ready {
            return recovering.write_permit().map_err(|err| {
                Status::failed_precondition(format!("PersonalDB partition is not writable: {err}"))
            });
        }
        let ready = publish_partition_ready(
            &self.storage,
            &partition_family,
            &partition_id,
            &owner_node_id,
            recovering.fence_token,
            recovered_through_sequence,
            recovered_manifest_hash,
            now_nanos.saturating_add(1),
            self.persistence.partition_owner_signing_key(),
        )
        .await
        .map_err(internal_status)?;
        ready.write_permit().map_err(|err| {
            Status::failed_precondition(format!("PersonalDB partition is not writable: {err}"))
        })
    }

    async fn personaldb_group_write_precondition(
        &self,
        permit: &PartitionWritePermit,
    ) -> Result<CoreMutationPrecondition, Status> {
        partition_write_precondition(
            &self.storage,
            permit,
            self.persistence.partition_owner_signing_key(),
        )
        .await
        .map_err(|err| {
            Status::failed_precondition(format!(
                "PersonalDB partition write fence is not current: {err}"
            ))
        })
    }

    async fn handle_personaldb_projection_writeback(
        &self,
        request: CoreSubmitChangeset,
        actor: PersonalDbCommitActor,
        definitions: Vec<ProjectionDefinition>,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        validate_claim_tenant(actor.tenant_id, request.tenant_id)?;
        validate_database_id(&request.database_id)?;
        if let Some(bearer_token) = actor.bearer_token.as_deref() {
            bind_personaldb_submit_session(&request, &actor, bearer_token)?;
        }
        if !personaldb_actor_access_allowed(
            &self.storage,
            &actor,
            &request.database_id,
            AnvilAction::PersonalDbCommit,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let validated = validate_submit_personaldb_changeset(request, default_max_changeset_size())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        iterate_changeset(&validated.request.changeset_bytes)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if definitions.len() != 1 {
            return Err(projection_writeback_rejected(
                "projection write-back has ambiguous projection bindings",
            ));
        }
        let definition = definitions.into_iter().next().ok_or_else(|| {
            projection_writeback_rejected("projection write-back binding missing")
        })?;
        match definition.writeback_policy {
            WriteBackPolicy::Deny => Err(projection_writeback_rejected(
                "projection write-back is denied by projection policy",
            )),
            WriteBackPolicy::AllowMappedColumns { .. } => {
                self.commit_personaldb_projection_writeback(validated.request, actor, definition)
                    .await
            }
        }
    }

    async fn commit_personaldb_projection_writeback(
        &self,
        request: CoreSubmitChangeset,
        actor: PersonalDbCommitActor,
        definition: ProjectionDefinition,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        let projection_manifest = read_personaldb_group_manifest(
            &self.storage,
            actor.tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection group not found"))?;
        let projection_head = read_personaldb_committed_head(
            &self.storage,
            actor.tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection head missing"))?;
        if projection_head.log_index != request.base_log_index
            || projection_head.log_hash != request.base_log_hash
        {
            return Err(projection_writeback_rejected(
                "projection write-back base does not match projection head",
            ));
        }
        let target_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            actor.tenant_id,
            &definition.database_id,
            &projection_manifest.schema_hash,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection schema SQL missing"))?;
        let source_database_id = single_projection_writeback_source(&definition)?;
        let source_manifest = read_personaldb_group_manifest(
            &self.storage,
            actor.tenant_id,
            &source_database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB source group not found"))?;
        let source_head = read_personaldb_committed_head(
            &self.storage,
            actor.tenant_id,
            &source_database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB source head missing"))?;
        let source_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            actor.tenant_id,
            &source_database_id,
            &source_manifest.schema_hash,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB source schema SQL missing"))?;
        let writeback = build_projection_writeback_changeset(ProjectionWriteBackInput {
            source_schema_sql: &source_schema_sql,
            target_schema_sql: &target_schema_sql,
            definition: &definition,
            projection_changeset_bytes: &request.changeset_bytes,
        })
        .map_err(|err| projection_writeback_rejected_owned(err.to_string()))?;
        if writeback.source_database_id != source_database_id {
            return Err(projection_writeback_rejected(
                "projection write-back source binding changed during translation",
            ));
        }
        let payload_hash = hash32(&writeback.changeset_bytes);
        let source_request = CoreSubmitChangeset {
            tenant_id: actor.tenant_id,
            database_id: source_database_id.clone(),
            principal: request.principal,
            session_token: request.session_token,
            request_id: format!(
                "projection-writeback:{}:{}",
                definition.projection_id, request.request_id
            ),
            idempotency_key: format!(
                "projection-writeback:{}:{}",
                definition.projection_id, request.idempotency_key
            ),
            base_log_index: source_head.log_index,
            base_log_hash: source_head.log_hash.clone(),
            client_log_epoch: source_head.log_index.saturating_add(1),
            membership_epoch: source_manifest.active_membership_epoch,
            policy_epoch: source_manifest.active_policy_epoch,
            leader_replica_id: request.leader_replica_id,
            voter_acks: vec![crate::personaldb_submit::PersonalDbVoterAck {
                replica_id: "projection-writeback".to_string(),
                log_index: source_head.log_index.saturating_add(1),
                log_hash: hex::encode(payload_hash),
                signature: "projection-writeback".to_string(),
            }],
            changeset_payload_hash: hex::encode(payload_hash),
            changeset_bytes: writeback.changeset_bytes,
            client_debug_metadata: request.client_debug_metadata,
        };
        let source_changeset_bytes = source_request.changeset_bytes.clone();
        let tenant_id = actor.tenant_id;
        let committed = self
            .commit_personaldb_changeset(source_request, actor)
            .await?;
        self.build_personaldb_projections_for_source_commit(
            tenant_id,
            &source_database_id,
            &source_changeset_bytes,
            committed.log_index,
            &committed.log_hash,
            committed.authz_revision,
        )
        .await?;
        Ok(submit_changeset_response(committed))
    }

    async fn commit_personaldb_changeset(
        &self,
        request: CoreSubmitChangeset,
        actor: PersonalDbCommitActor,
    ) -> Result<CommittedPersonalDbChangeset, Status> {
        validate_claim_tenant(actor.tenant_id, request.tenant_id)?;
        validate_database_id(&request.database_id)?;
        if actor.require_public_commit_authorization
            && !personaldb_actor_access_allowed(
                &self.storage,
                &actor,
                &request.database_id,
                AnvilAction::PersonalDbCommit,
            )
            .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }

        let validated = validate_submit_personaldb_changeset(request, default_max_changeset_size())
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        if let Some(bearer_token) = actor.bearer_token.as_deref() {
            bind_personaldb_submit_session(&validated.request, &actor, bearer_token)?;
        }
        let _commit_guard = self
            .personaldb_commit_guard(actor.tenant_id, &validated.request.database_id)
            .await;
        let protocol_keyring = self.personaldb_protocol_keyring.as_ref();
        let manifest = read_personaldb_group_manifest(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB group not found"))?;
        let previous_head = read_personaldb_committed_head(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB committed head missing"))?;

        if previous_head.log_index != validated.request.base_log_index
            || previous_head.log_hash != validated.request.base_log_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB base log position does not match committed head",
            ));
        }
        if manifest.active_membership_epoch != validated.request.membership_epoch
            || manifest.active_policy_epoch != validated.request.policy_epoch
            || previous_head.schema_hash != manifest.schema_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB submit epochs or schema do not match the active group",
            ));
        }
        let write_permit = self
            .acquire_personaldb_group_write_permit(
                actor.tenant_id,
                &validated.request.database_id,
                previous_head.log_index,
                &previous_head.log_hash,
            )
            .await?;
        let current_head_after_fence = read_personaldb_committed_head(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB committed head missing"))?;
        if current_head_after_fence.log_index != previous_head.log_index
            || current_head_after_fence.log_hash != previous_head.log_hash
            || current_head_after_fence.head_hash != previous_head.head_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB committed head changed during partition handoff",
            ));
        }
        let partition_owner = if actor.require_admission_protocol {
            Some(
                read_partition_owner(
                    &self.storage,
                    &write_permit.partition_family,
                    &write_permit.partition_id,
                    self.persistence.partition_owner_signing_key(),
                )
                .await
                .map_err(internal_status)?
                .ok_or_else(|| Status::failed_precondition("PersonalDB partition owner missing"))?,
            )
        } else {
            None
        };

        let changes = iterate_changeset(&validated.request.changeset_bytes)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let schema_sql = read_personaldb_schema_sql(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            &manifest.schema_hash,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB schema SQL missing"))?;
        validate_changeset_tables_registered(&changes, &schema_sql)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, actor.tenant_id)
            .await
            .map_err(internal_status)
            .and_then(|revision| {
                u64::try_from(revision)
                    .map_err(|_| Status::internal("Invalid authorization revision"))
            })?;
        let proposed_log_index = validated
            .request
            .base_log_index
            .checked_add(1)
            .ok_or_else(|| Status::failed_precondition("PersonalDB log index overflow"))?;
        let updated_at = chrono::Utc::now();
        let envelope = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
            tenant_id: actor.tenant_id,
            database_id: &validated.request.database_id,
            principal: &validated.request.principal,
            base_log_index: validated.request.base_log_index,
            proposed_log_index,
            changeset_payload_hash: validated.changeset_payload_hash,
            schema_hash: &manifest.schema_hash,
            policy_epoch: manifest.active_policy_epoch,
            authz_revision,
            changes: &changes,
            updated_at_nanos: updated_at
                .timestamp_nanos_opt()
                .ok_or_else(|| Status::internal("Invalid current timestamp"))?,
        })
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        authorize_personaldb_row_effects(&self.storage, &envelope, &actor).await?;
        let envelope_hash = envelope.envelope_hash32().map_err(internal_status)?;
        let previous_log_hash = hex32_status(&previous_head.log_hash, "committed head log hash")?;
        let schema_hash = hex32_status(&manifest.schema_hash, "schema hash")?;
        let admission_reservation = if actor.require_admission_protocol {
            let partition_owner = partition_owner
                .as_ref()
                .ok_or_else(|| Status::internal("PersonalDB partition owner not loaded"))?;
            let proposal_record = protocol_keyring
                .trust_record_for_purpose(SignaturePurpose::ProposalAdmission)
                .map_err(internal_status)?;
            let witness_record = protocol_keyring
                .trust_record_for_purpose(SignaturePurpose::Witness)
                .map_err(internal_status)?;
            let now_unix_seconds = updated_at.timestamp();
            let workflow_id = format!(
                "personaldb-submit:{}:{}",
                validated.request.database_id, validated.request.idempotency_key
            );
            let claim = ProposalIdempotencyClaimIdentityV1 {
                format_version: 1,
                tenant_id: actor.tenant_id.to_string(),
                application_id: actor.principal.clone(),
                operation_id: "personaldb.submit_changeset.v1".to_string(),
                request_id: validated.request.request_id.clone(),
                database_id: validated.request.database_id.clone(),
                client_proposal_hash_sha256: client_proposal_hash(&validated),
                changeset_payload_hash_sha256: validated.changeset_payload_hash,
                workflow_id: workflow_id.clone(),
                fencing_generation: write_permit.fence_token,
            };
            let claim_hash = claim.hash_sha256().map_err(internal_status)?;
            let authorization_receipt_sha256 = hash32(
                format!(
                    "anvil-personaldb-authorization-receipt-v1\0{}\0{}\0{}\0{}",
                    actor.tenant_id,
                    validated.request.database_id,
                    authz_revision,
                    hex::encode(envelope_hash)
                )
                .as_bytes(),
            );
            let identity = ProposalAdmissionReservationIdentityV1 {
                format_version: 1,
                reservation_id: derive_reservation_id(&validated.request.database_id, claim_hash)
                    .map_err(internal_status)?,
                database_id: validated.request.database_id.clone(),
                group_kind: "source".to_string(),
                proposer_id: actor.principal.clone(),
                client_proposal_hash_sha256: claim.client_proposal_hash_sha256,
                changeset_payload_hash_sha256: claim.changeset_payload_hash_sha256,
                expected_previous_log_index: previous_head.log_index,
                expected_previous_log_hash_sha256: previous_log_hash,
                membership_revision: manifest.active_membership_epoch,
                placement_epoch: u64::from(manifest.object_layout_version).max(1),
                client_log_epoch: validated.request.client_log_epoch,
                workflow_id,
                fencing_generation: write_permit.fence_token,
                leader_lease_id: personaldb_group_leader_lease_id(partition_owner),
                leader_lease_revision: partition_owner.recovery_epoch,
                authorization_receipt_sha256,
                authorization_revision: authz_revision,
                idempotency_claim_sha256: claim_hash,
                issued_at_unix_seconds: now_unix_seconds,
                expires_at_unix_seconds: now_unix_seconds + 300,
                selected_voter_ids: validated
                    .request
                    .voter_acks
                    .iter()
                    .map(|ack| ack.replica_id.clone())
                    .collect(),
                primary_server_id: write_permit.owner_node_id.clone(),
                proposal_admission_key_id: proposal_record.key_id.to_string(),
                proposal_admission_generation: proposal_record.key_generation.get(),
                witness_key_id: witness_record.key_id.to_string(),
                witness_key_generation: witness_record.key_generation.get(),
            };
            let authority = PersonalDbAdmissionAuthority {
                storage: &self.storage,
                trust_store: protocol_keyring.trust_store(),
                write_permit: &write_permit,
                partition_owner_signing_key: self.persistence.partition_owner_signing_key(),
                now_unix_seconds,
            };
            Some(
                reserve_personaldb_proposal(&authority, claim, identity)
                    .await
                    .map_err(internal_status)?,
            )
        } else {
            None
        };
        let payload_paths = write_personaldb_changeset_payload(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            proposed_log_index,
            validated.changeset_payload_hash,
            &validated.request.changeset_bytes,
        )
        .await
        .map_err(internal_status)?;
        let payload_ref = payload_paths.by_index_ref.clone().into_bytes();

        let provisional_record = CorePersonalDbLogRecord::new(
            proposed_log_index,
            validated.request.client_log_epoch,
            validated.request.membership_epoch,
            validated.request.policy_epoch,
            previous_log_hash,
            validated.changeset_payload_hash,
            envelope_hash,
            [0; 32],
            payload_ref.clone(),
            Vec::new(),
            Vec::new(),
        );
        let leader_replica_id = if actor.require_admission_protocol {
            write_permit.owner_node_id.clone()
        } else {
            validated.request.leader_replica_id.clone()
        };
        let witness_node_id = if actor.require_admission_protocol {
            write_permit.owner_node_id.clone()
        } else {
            actor.principal.clone()
        };
        let unsigned_certificate = PersonalDbCommitCertificate {
            format_version: 2,
            tenant_id: actor.tenant_id.to_string(),
            database_id: validated.request.database_id.clone(),
            log_index: proposed_log_index,
            previous_log_hash: hex::encode(previous_log_hash),
            entry_hash: hex::encode(provisional_record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            verified_envelope_hash: hex::encode(envelope_hash),
            client_log_epoch: validated.request.client_log_epoch,
            membership_epoch: validated.request.membership_epoch,
            policy_epoch: validated.request.policy_epoch,
            leader_replica_id,
            voter_acks_hash: hex::encode(validated.voter_acks_hash),
            authz_revision,
            witness_node_id,
            witnessed_at: now_rfc3339(),
            certificate_hash: None,
            witness_signature: None,
        };
        let row_index_records = envelope.row_index_upserts().map_err(internal_status)?;
        let row_index_generation = if row_index_records.is_empty() {
            previous_head.row_index_generation
        } else {
            previous_head
                .row_index_generation
                .checked_add(1)
                .ok_or_else(|| Status::failed_precondition("PersonalDB row index overflow"))?
        };
        let mut admission_terminal_request = None;
        let (certificate, record, committed_head) =
            if let Some(reservation) = admission_reservation.as_ref() {
                let certificate_hash_hex =
                    crate::personaldb_control::hash_commit_certificate(&unsigned_certificate)
                        .map_err(internal_status)?;
                let certificate_hash = hex32_status(&certificate_hash_hex, "certificate hash")?;
                let certificate_ref = personaldb_commit_certificate_ref_name(
                    actor.tenant_id,
                    &validated.request.database_id,
                    proposed_log_index,
                    &unsigned_certificate.entry_hash,
                )
                .map_err(internal_status)?;
                let record = CorePersonalDbLogRecord::new(
                    proposed_log_index,
                    validated.request.client_log_epoch,
                    validated.request.membership_epoch,
                    validated.request.policy_epoch,
                    previous_log_hash,
                    validated.changeset_payload_hash,
                    envelope_hash,
                    certificate_hash,
                    payload_ref.clone(),
                    certificate_ref.clone().into_bytes(),
                    Vec::new(),
                );
                let segment_ref = preview_personaldb_log_segment_ref(PersonalDbLogSegmentWrite {
                    tenant_id: actor.tenant_id,
                    database_id: &validated.request.database_id,
                    schema_hash,
                    source_fence_token: write_permit.fence_token,
                    records: std::slice::from_ref(&record),
                })
                .map_err(internal_status)?;
                let head_template = PersonalDbCommittedHead {
                    format_version: 2,
                    tenant_id: actor.tenant_id.to_string(),
                    database_id: validated.request.database_id.clone(),
                    log_index: proposed_log_index,
                    log_hash: hex::encode(record.entry_hash),
                    segment_ref: segment_ref.clone(),
                    row_index_generation,
                    policy_epoch: manifest.active_policy_epoch,
                    membership_epoch: manifest.active_membership_epoch,
                    schema_hash: manifest.schema_hash.clone(),
                    updated_at: now_rfc3339(),
                    updated_by_node: write_permit.owner_node_id.clone(),
                    head_hash: None,
                    head_signature: None,
                };
                let authority = PersonalDbAdmissionAuthority {
                    storage: &self.storage,
                    trust_store: protocol_keyring.trust_store(),
                    write_permit: &write_permit,
                    partition_owner_signing_key: self.persistence.partition_owner_signing_key(),
                    now_unix_seconds: updated_at.timestamp(),
                };
                let candidate = begin_personaldb_witness_signing(
                    &authority,
                    BeginWitnessSigningV1 {
                        tenant_id: actor.tenant_id,
                        reservation_id: reservation.identity.reservation_id.clone(),
                        expected_reservation_revision: reservation.reservation_revision,
                        unsigned_commit_certificate: unsigned_certificate,
                        head_template,
                        created_at_unix_seconds: updated_at.timestamp(),
                    },
                )
                .await
                .map_err(internal_status)?;
                let signing_request = SignCertificateAndHeadV1 {
                    reservation_id: candidate.reservation_id.clone(),
                    signing_reservation_revision: candidate.signing_reservation_revision,
                };
                admission_terminal_request = Some(signing_request.clone());
                let receipt = sign_personaldb_certificate_and_head_with_keyring(
                    &authority,
                    &signing_request,
                    protocol_keyring,
                )
                .await
                .map_err(internal_status)?;
                acknowledge_personaldb_witness_receipt(&authority, &signing_request)
                    .await
                    .map_err(internal_status)?;
                let certificate = decode_commit_certificate(&receipt.signed_commit_certificate)
                    .map_err(internal_status)?;
                let committed_head = decode_committed_head(&receipt.signed_committed_head)
                    .map_err(internal_status)?;
                let written_certificate_ref = write_personaldb_commit_certificate(
                    &self.storage,
                    actor.tenant_id,
                    &validated.request.database_id,
                    &certificate,
                    protocol_keyring.trust_store(),
                )
                .await
                .map_err(internal_status)?;
                if written_certificate_ref != certificate_ref {
                    return Err(Status::internal(
                        "PersonalDB witness certificate ref changed after signing",
                    ));
                }
                let written_segment_ref = write_personaldb_log_segment(
                    &self.storage,
                    PersonalDbLogSegmentWrite {
                        tenant_id: actor.tenant_id,
                        database_id: &validated.request.database_id,
                        schema_hash,
                        source_fence_token: write_permit.fence_token,
                        records: std::slice::from_ref(&record),
                    },
                )
                .await
                .map_err(internal_status)?;
                if written_segment_ref != segment_ref {
                    return Err(Status::internal(
                        "PersonalDB log segment ref changed after witness signing",
                    ));
                }
                (certificate, record, committed_head)
            } else {
                let certificate = unsigned_certificate
                    .seal(protocol_keyring)
                    .await
                    .map_err(internal_status)?;
                let certificate_ref = write_personaldb_commit_certificate(
                    &self.storage,
                    actor.tenant_id,
                    &validated.request.database_id,
                    &certificate,
                    protocol_keyring.trust_store(),
                )
                .await
                .map_err(internal_status)?;
                let certificate_hash = hex32_status(
                    certificate
                        .certificate_hash
                        .as_deref()
                        .ok_or_else(|| Status::internal("PersonalDB certificate hash missing"))?,
                    "certificate hash",
                )?;
                let record = CorePersonalDbLogRecord::new(
                    proposed_log_index,
                    validated.request.client_log_epoch,
                    validated.request.membership_epoch,
                    validated.request.policy_epoch,
                    previous_log_hash,
                    validated.changeset_payload_hash,
                    envelope_hash,
                    certificate_hash,
                    payload_ref,
                    certificate_ref.into_bytes(),
                    Vec::new(),
                );
                let segment_ref = write_personaldb_log_segment(
                    &self.storage,
                    PersonalDbLogSegmentWrite {
                        tenant_id: actor.tenant_id,
                        database_id: &validated.request.database_id,
                        schema_hash,
                        source_fence_token: write_permit.fence_token,
                        records: std::slice::from_ref(&record),
                    },
                )
                .await
                .map_err(internal_status)?;
                let committed_head = PersonalDbCommittedHead {
                    format_version: 2,
                    tenant_id: actor.tenant_id.to_string(),
                    database_id: validated.request.database_id.clone(),
                    log_index: proposed_log_index,
                    log_hash: hex::encode(record.entry_hash),
                    segment_ref: segment_ref.clone(),
                    row_index_generation,
                    policy_epoch: manifest.active_policy_epoch,
                    membership_epoch: manifest.active_membership_epoch,
                    schema_hash: manifest.schema_hash.clone(),
                    updated_at: now_rfc3339(),
                    updated_by_node: actor.principal.clone(),
                    head_hash: None,
                    head_signature: None,
                }
                .seal(protocol_keyring)
                .await
                .map_err(internal_status)?;
                (certificate, record, committed_head)
            };
        let certificate_hash = hex32_status(
            certificate
                .certificate_hash
                .as_deref()
                .ok_or_else(|| Status::internal("PersonalDB certificate hash missing"))?,
            "certificate hash",
        )?;
        if !row_index_records.is_empty() {
            write_personaldb_row_index(
                &self.storage,
                PersonalDbRowIndexWrite {
                    tenant_id: actor.tenant_id,
                    database_id: &validated.request.database_id,
                    generation: row_index_generation,
                    source_hash: record.entry_hash,
                    records: &row_index_records,
                },
            )
            .await
            .map_err(internal_status)?;
        }
        let write_precondition = self
            .personaldb_group_write_precondition(&write_permit)
            .await?;
        let current_head_before_publish = read_personaldb_committed_head(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB committed head missing"))?;
        if current_head_before_publish.log_index != previous_head.log_index
            || current_head_before_publish.log_hash != previous_head.log_hash
            || current_head_before_publish.head_hash != previous_head.head_hash
        {
            return Err(Status::failed_precondition(
                "PersonalDB committed head changed before publish",
            ));
        }
        write_personaldb_committed_head_with_preconditions(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            &committed_head,
            protocol_keyring.trust_store(),
            vec![write_precondition],
        )
        .await
        .map_err(internal_status)?;
        if let Some(signing_request) = admission_terminal_request.as_ref() {
            let authority = PersonalDbAdmissionAuthority {
                storage: &self.storage,
                trust_store: protocol_keyring.trust_store(),
                write_permit: &write_permit,
                partition_owner_signing_key: self.persistence.partition_owner_signing_key(),
                now_unix_seconds: updated_at.timestamp(),
            };
            commit_personaldb_witnessed_proposal(&authority, signing_request)
                .await
                .map_err(internal_status)?;
        }

        let watch_payload = PersonalDbGroupWatchPayload {
            database_id: validated.request.database_id.clone(),
            event_type: "commit".to_string(),
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            certificate_hash: hex::encode(certificate_hash),
            committed_head_hash: committed_head.head_hash.clone().unwrap_or_default(),
            emitted_at: now_rfc3339(),
        };
        let mutation_id = *uuid::Uuid::new_v4().as_bytes();
        maybe_build_personaldb_snapshot(
            &self.storage,
            PersonalDbSnapshotBuildRequest {
                tenant_id: actor.tenant_id,
                database_id: &validated.request.database_id,
                schema_sql: &schema_sql,
                created_by_node: &actor.principal,
                policy: configured_personaldb_snapshot_policy(&self.config),
            },
            protocol_keyring,
        )
        .await
        .map_err(internal_status)?;

        materialize_personaldb_row_owner_grants(&self.persistence, &envelope, &actor)
            .await
            .map_err(internal_status)?;

        let watch_cursor = append_personaldb_group_watch_record(
            &self.storage,
            actor.tenant_id,
            &validated.request.database_id,
            mutation_id,
            authz_revision,
            watch_payload.clone(),
        )
        .await
        .map_err(internal_status)?;
        Ok(CommittedPersonalDbChangeset {
            log_index: proposed_log_index,
            log_hash: hex::encode(record.entry_hash),
            changeset_payload_hash: hex::encode(validated.changeset_payload_hash),
            verified_envelope_hash: hex::encode(envelope_hash),
            certificate_hash: hex::encode(certificate_hash),
            certificate,
            committed_head,
            watch_cursor,
            authz_revision,
        })
    }

    async fn build_personaldb_projections_for_source_commit(
        &self,
        tenant_id: i64,
        source_database_id: &str,
        source_changeset_bytes: &[u8],
        source_log_index: u64,
        source_log_hash: &str,
        authz_revision: u64,
    ) -> Result<(), Status> {
        let source_manifest = read_personaldb_group_manifest(
            &self.storage,
            tenant_id,
            source_database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB source group not found"))?;
        let source_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            tenant_id,
            source_database_id,
            &source_manifest.schema_hash,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB source schema SQL missing"))?;
        let definitions =
            list_projection_definitions_for_source(&self.storage, tenant_id, source_database_id)
                .await
                .map_err(internal_status)?;
        for definition in definitions {
            self.build_one_personaldb_projection(
                tenant_id,
                source_database_id,
                &source_schema_sql,
                source_changeset_bytes,
                source_log_index,
                source_log_hash,
                authz_revision,
                &definition,
            )
            .await?;
        }
        Ok(())
    }

    async fn build_one_personaldb_projection(
        &self,
        tenant_id: i64,
        source_database_id: &str,
        source_schema_sql: &str,
        source_changeset_bytes: &[u8],
        source_log_index: u64,
        source_log_hash: &str,
        authz_revision: u64,
        definition: &ProjectionDefinition,
    ) -> Result<(), Status> {
        if definition.target_database_id != definition.database_id {
            return Err(Status::failed_precondition(
                "PersonalDB projection target database scope mismatch",
            ));
        }
        let target_manifest = read_personaldb_group_manifest(
            &self.storage,
            tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection group not found"))?;
        let target_head = read_personaldb_committed_head(
            &self.storage,
            tenant_id,
            &definition.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection head missing"))?;
        let target_schema_sql = read_personaldb_schema_sql(
            &self.storage,
            tenant_id,
            &definition.database_id,
            &target_manifest.schema_hash,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::failed_precondition("PersonalDB projection schema SQL missing"))?;
        let build_input = ProjectionBuildInput {
            source_database_id,
            source_schema_sql,
            target_schema_sql: &target_schema_sql,
            definition,
            source_changeset_bytes,
        };
        let authorization_checks =
            collect_projection_authorization_checks(build_input).map_err(internal_status)?;
        let authorization = self
            .evaluate_projection_authorization_checks(
                tenant_id,
                &definition.target_actor_or_scope,
                authorization_checks,
                authz_revision,
            )
            .await?;
        let Some(projection_changeset) =
            build_projection_changeset_with_authorization(build_input, &authorization)
                .map_err(internal_status)?
        else {
            return Ok(());
        };
        if projection_changeset.changeset_bytes.is_empty() {
            return Ok(());
        }
        let internal_actor = "anvil-projection-builder".to_string();
        let payload_hash = hash32(&projection_changeset.changeset_bytes);
        let projection_commit = self
            .commit_personaldb_changeset(
                CoreSubmitChangeset {
                    tenant_id,
                    database_id: definition.database_id.clone(),
                    principal: internal_actor.clone(),
                    session_token: "internal-projection-builder".to_string(),
                    request_id: format!(
                        "projection:{}:{}:{}",
                        source_database_id, source_log_index, definition.projection_id
                    ),
                    idempotency_key: format!(
                        "projection:{}:{}:{}",
                        source_database_id, source_log_hash, definition.projection_id
                    ),
                    base_log_index: target_head.log_index,
                    base_log_hash: target_head.log_hash,
                    client_log_epoch: target_head.log_index.saturating_add(1),
                    membership_epoch: target_manifest.active_membership_epoch,
                    policy_epoch: target_manifest.active_policy_epoch,
                    leader_replica_id: internal_actor.clone(),
                    voter_acks: vec![crate::personaldb_submit::PersonalDbVoterAck {
                        replica_id: internal_actor.clone(),
                        log_index: target_head.log_index.saturating_add(1),
                        log_hash: hex::encode(payload_hash),
                        signature: "internal-projection-builder".to_string(),
                    }],
                    changeset_payload_hash: hex::encode(payload_hash),
                    changeset_bytes: projection_changeset.changeset_bytes,
                    client_debug_metadata: None,
                },
                PersonalDbCommitActor {
                    tenant_id,
                    principal: internal_actor,
                    bearer_token: None,
                    require_public_commit_authorization: false,
                    require_admission_protocol: false,
                },
            )
            .await?;
        let payload = PersonalDbProjectionWatchPayload {
            database_id: definition.database_id.clone(),
            projection_id: definition.projection_id.clone(),
            event_type: "projection_committed".to_string(),
            source_database_id: source_database_id.to_string(),
            source_log_index,
            source_log_hash: source_log_hash.to_string(),
            projection_log_index: projection_commit.log_index,
            projection_log_hash: projection_commit.log_hash.clone(),
            definition_hash: definition.definition_hash.clone().unwrap_or_default(),
            emitted_at: now_rfc3339(),
        };
        let mutation_id = *uuid::Uuid::new_v4().as_bytes();
        let cursor = append_personaldb_projection_watch_record(
            &self.storage,
            tenant_id,
            &definition.database_id,
            &definition.projection_id,
            mutation_id,
            authz_revision,
            payload.clone(),
        )
        .await
        .map_err(internal_status)?;
        Ok(())
    }

    async fn evaluate_projection_authorization_checks(
        &self,
        tenant_id: i64,
        target_actor: &str,
        checks: std::collections::BTreeSet<ProjectionAuthorizationCheck>,
        authz_revision: u64,
    ) -> Result<ProjectionAuthorizationDecisions, Status> {
        let revision = i64::try_from(authz_revision)
            .map_err(|_| Status::internal("Invalid projection authorization revision"))?;
        let mut allowed = Vec::new();
        for check in checks {
            let scoped_namespace = encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, &check.namespace);
            let is_allowed = authz_journal::resolve_permission_at_revision(
                &self.storage,
                tenant_id,
                &scoped_namespace,
                &check.object_id,
                &check.relation,
                access_control::APP_SUBJECT_KIND,
                target_actor,
                "",
                revision,
            )
            .await
            .map_err(internal_status)?;
            if is_allowed {
                allowed.push(check);
            }
        }
        Ok(ProjectionAuthorizationDecisions::new(allowed))
    }
}

fn request_claims<T>(request: &Request<T>) -> Result<&auth::Claims, Status> {
    request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))
}

fn request_bearer_token<T>(request: &Request<T>) -> Result<&str, Status> {
    request
        .extensions()
        .get::<auth::AuthenticatedBearerToken>()
        .map(|token| token.0.as_str())
        .ok_or_else(|| Status::unauthenticated("Missing authenticated session token"))
}

fn bind_personaldb_submit_session(
    request: &CoreSubmitChangeset,
    actor: &PersonalDbCommitActor,
    bearer_token: &str,
) -> Result<(), Status> {
    if request.session_token != bearer_token {
        return Err(Status::unauthenticated(
            "PersonalDB session token does not match authenticated bearer",
        ));
    }
    if request.principal != actor.principal {
        return Err(Status::permission_denied(
            "PersonalDB principal does not match authenticated session",
        ));
    }
    Ok(())
}

async fn authorize_personaldb_row_effects(
    storage: &crate::storage::Storage,
    envelope: &VerifiedMutationEnvelope,
    actor: &PersonalDbCommitActor,
) -> Result<(), Status> {
    if !actor.require_public_commit_authorization {
        return Ok(());
    }

    for effect in &envelope.table_effects {
        let binding = &effect.source_resource_binding;
        let resource = personaldb_row_resource_id(actor.tenant_id, &envelope.database_id, binding);
        for permission in &effect.required_permissions {
            let revision = i64::try_from(envelope.authz_revision)
                .map_err(|_| Status::internal("Invalid PersonalDB authz revision"))?;
            let allowed = authz_journal::resolve_permission_at_revision(
                storage,
                actor.tenant_id,
                &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "personaldb_row"),
                &resource,
                permission,
                access_control::APP_SUBJECT_KIND,
                &actor.principal,
                "",
                revision,
            )
            .await
            .map_err(internal_status)?;
            if allowed || insert_effect_creates_owned_row(effect, actor) {
                continue;
            }
            return Err(Status::permission_denied(
                "PersonalDB row/resource mutation is not authorized",
            ));
        }
    }
    Ok(())
}

async fn materialize_personaldb_row_owner_grants(
    persistence: &crate::persistence::Persistence,
    envelope: &VerifiedMutationEnvelope,
    actor: &PersonalDbCommitActor,
) -> anyhow::Result<()> {
    let mut mutations = Vec::new();
    for row in &envelope.row_metadata_delta.upserts {
        if row.owner_principal.as_deref() != Some(actor.principal.as_str()) {
            continue;
        }
        let resource = format!(
            "tenant-{}/{}/{}/{}",
            actor.tenant_id, envelope.database_id, row.resource_type, row.resource_id
        );
        for relation in [
            "personaldb:insert",
            "personaldb:update",
            "personaldb:delete",
        ] {
            mutations.push(crate::persistence::AuthzTupleBatchMutation {
                namespace: encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "personaldb_row"),
                object_id: resource.clone(),
                relation: relation.to_string(),
                subject_kind: access_control::APP_SUBJECT_KIND.to_string(),
                subject_id: actor.principal.clone(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "PersonalDB row owner grant".to_string(),
            });
        }
    }
    if mutations.is_empty() {
        return Ok(());
    }
    persistence
        .write_authz_tuple_batch(actor.tenant_id, mutations, &actor.principal)
        .await?;
    Ok(())
}

fn insert_effect_creates_owned_row(
    effect: &crate::personaldb_envelope::TableEffect,
    actor: &PersonalDbCommitActor,
) -> bool {
    effect.operation == TableOperation::Insert
        && effect.source_resource_binding.owner_principal.as_deref()
            == Some(actor.principal.as_str())
}

fn personaldb_row_resource_id(
    tenant_id: i64,
    database_id: &str,
    binding: &crate::personaldb_envelope::ResourceBinding,
) -> String {
    format!(
        "tenant-{}/{}/{}/{}",
        tenant_id, database_id, binding.resource_type, binding.resource_id
    )
}

mod helpers;
use helpers::*;

#[cfg(test)]
mod tests;
