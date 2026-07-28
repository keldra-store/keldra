use super::*;

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
        let snapshot_version = self
            .mvcc
            .runtime
            .applied_version()
            .map_err(internal_status)?;
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        let mut transaction_id =
            crate::services::transaction_context::write_options_transaction_id(
                req.options.as_ref(),
            )?
            .map(ToOwned::to_owned);
        let internal_transaction = transaction_id.is_none();
        let mut transaction_principal = transaction_id
            .as_ref()
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let implicit_principal = crate::object_manager::transaction_principal_from_claims(&claims);
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
        let create_idempotency_key = format!(
            "personaldb-create:{}:{}:{}:{}",
            claims.tenant_id, req.database_id, req.schema_hash, req.genesis_hash
        );
        if transaction_id.is_none()
            && let Some(commit_version) = PersonalDbWritePlan::resolved_commit_version(
                &self.mvcc,
                &implicit_principal,
                &create_idempotency_key,
            )
            .await
            .map_err(internal_status)?
        {
            let manifest = read_personaldb_group_manifest(
                &self.storage,
                &self.mvcc,
                claims.tenant_id,
                &req.database_id,
                protocol_keyring.trust_store(),
                commit_version,
            )
            .await
            .map_err(internal_status)?
            .ok_or_else(|| Status::internal("Committed PersonalDB manifest is missing"))?;
            let committed_head = read_personaldb_committed_head_at_snapshot(
                &self.mvcc,
                claims.tenant_id,
                &req.database_id,
                protocol_keyring.trust_store(),
                commit_version,
            )
            .map_err(internal_status)?
            .ok_or_else(|| Status::internal("Committed PersonalDB head is missing"))?;
            return Ok(Response::new(PersonalDbGroupResponse {
                manifest: Some(group_manifest_record(manifest)),
                committed_head: Some(committed_head_record(committed_head)),
                write_state: WriteState::Committed as i32,
            }));
        }
        if internal_transaction {
            let now = u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("PersonalDB timestamp predates Unix epoch"))?;
            let handle = self
                .mvcc
                .open_transactions
                .begin(
                    self.mvcc.runtime.as_ref(),
                    self.mvcc.cluster_id().to_string(),
                    implicit_principal.clone(),
                    create_idempotency_key.clone(),
                    std::time::Duration::from_secs(300),
                    crate::mvcc_transaction::DurabilityLevel::Quorum,
                    crate::mvcc_transaction::ReadConsistency::Linearized,
                    now,
                )
                .await
                .map_err(internal_status)?;
            transaction_principal = Some(implicit_principal);
            transaction_id = Some(handle.transaction_id);
        }
        if read_personaldb_group_manifest(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            &req.database_id,
            protocol_keyring.trust_store(),
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .is_some()
        {
            return Err(Status::already_exists("PersonalDB group already exists"));
        }
        let assignment = self
            .personaldb_write_assignment(claims.tenant_id, &req.database_id)
            .await?;
        let root_generation = snapshot_version
            .checked_add(1)
            .ok_or_else(|| Status::internal("PersonalDB root generation overflow"))?;
        let mut write_plan = PersonalDbWritePlan::new(
            claims.tenant_id,
            &req.database_id,
            transaction_principal.as_deref().unwrap_or(&claims.sub),
            create_idempotency_key,
        )
        .map_err(internal_status)?
        .with_assignment_guard(assignment);

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
        prepare_and_stage_personaldb_schema_sql(
            &self.storage,
            &mut write_plan,
            claims.tenant_id,
            &req.database_id,
            root_generation,
            &req.schema_sql,
            &req.schema_hash,
        )
        .await
        .map_err(internal_status)?;
        prepare_and_stage_personaldb_group_manifest(
            &self.storage,
            &mut write_plan,
            claims.tenant_id,
            root_generation,
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
        prepare_and_stage_personaldb_committed_head(
            &self.storage,
            &mut write_plan,
            claims.tenant_id,
            &committed_head.database_id,
            root_generation,
            &committed_head,
            protocol_keyring.trust_store(),
        )
        .await
        .map_err(internal_status)?;
        stage_personaldb_committed_head_seed(
            &mut write_plan,
            claims.tenant_id,
            &committed_head.database_id,
            &committed_head,
            protocol_keyring.trust_store(),
        )
        .map_err(internal_status)?;
        if let Some(transaction_id) = transaction_id.as_deref() {
            write_plan
                .stage_into_transaction(
                    &self.mvcc,
                    transaction_id,
                    transaction_principal.as_deref().ok_or_else(|| {
                        Status::internal("missing PersonalDB transaction principal")
                    })?,
                    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                )
                .await
                .map_err(internal_status)?;
            let object_id = crate::access_control::personaldb_group_object_id(
                claims.tenant_id,
                &committed_head.database_id,
            );
            self.persistence
                .stage_authz_tuple_batch(
                    crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
                    vec![
                        crate::persistence::AuthzTupleBatchMutation {
                            namespace: crate::access_control::system_realm_namespace(
                                crate::system_realm::SYSTEM_PERSONALDB_GROUP_NAMESPACE,
                            ),
                            object_id: object_id.clone(),
                            relation: "parent_tenant".to_string(),
                            subject_kind: crate::system_realm::SYSTEM_STORAGE_TENANT_NAMESPACE
                                .to_string(),
                            subject_id: crate::access_control::storage_tenant_object_id(
                                claims.tenant_id,
                            ),
                            caveat_hash: String::new(),
                            operation: "add".to_string(),
                            reason: "grant creator PersonalDB group owner".to_string(),
                        },
                        crate::persistence::AuthzTupleBatchMutation {
                            namespace: crate::access_control::system_realm_namespace(
                                crate::system_realm::SYSTEM_PERSONALDB_GROUP_NAMESPACE,
                            ),
                            object_id,
                            relation: "owner".to_string(),
                            subject_kind: crate::access_control::APP_SUBJECT_KIND.to_string(),
                            subject_id: claims.sub.clone(),
                            caveat_hash: String::new(),
                            operation: "add".to_string(),
                            reason: "grant creator PersonalDB group owner".to_string(),
                        },
                    ],
                    &claims.sub,
                    transaction_id,
                    transaction_principal.as_deref().ok_or_else(|| {
                        Status::internal("missing PersonalDB transaction principal")
                    })?,
                    None,
                )
                .await
                .map_err(internal_status)?;
            if internal_transaction {
                let outcome = self
                    .mvcc
                    .open_transactions
                    .commit(
                        self.mvcc.runtime.as_ref(),
                        transaction_id,
                        transaction_principal.as_deref().ok_or_else(|| {
                            Status::internal("missing PersonalDB transaction principal")
                        })?,
                        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                    )
                    .await
                    .map_err(internal_status)?;
                if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
                    outcome.certification
                {
                    return Err(Status::aborted(format!(
                        "PersonalDB create transaction aborted: {reason:?}"
                    )));
                }
                crate::mvcc_fault_injection::hit(
                    crate::mvcc_fault_injection::FaultPoint::PersonalDbAfterCreateCommit,
                )
                .map_err(internal_status)?;
            }
            return Ok(Response::new(PersonalDbGroupResponse {
                manifest: Some(group_manifest_record(manifest)),
                committed_head: Some(committed_head_record(committed_head)),
                write_state: if internal_transaction {
                    WriteState::Committed as i32
                } else {
                    WriteState::Staged as i32
                },
            }));
        }
        unreachable!("PersonalDB create always has an explicit or internal transaction")
    }

    async fn get_personal_db_group(
        &self,
        request: Request<GetPersonalDbGroupRequest>,
    ) -> Result<Response<PersonalDbGroupResponse>, Status> {
        let snapshot_version = self
            .mvcc
            .runtime
            .applied_version()
            .map_err(internal_status)?;
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        if !personaldb_access_allowed(
            &self.storage,
            &self.mvcc,
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
            &self.mvcc,
            claims.tenant_id,
            &req.database_id,
            self.personaldb_protocol_keyring.trust_store(),
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB group not found"))?;
        let committed_head = read_personaldb_committed_head_mvcc(
            &self.mvcc,
            claims.tenant_id,
            &req.database_id,
            self.personaldb_protocol_keyring.trust_store(),
        )
        .map_err(internal_status)?;

        Ok(Response::new(PersonalDbGroupResponse {
            manifest: Some(group_manifest_record(manifest)),
            committed_head: committed_head.map(committed_head_record),
            write_state: WriteState::Committed as i32,
        }))
    }

    async fn create_personal_db_projection(
        &self,
        request: Request<CreatePersonalDbProjectionRequest>,
    ) -> Result<Response<PersonalDbProjectionResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        let transaction_id = crate::services::transaction_context::write_options_transaction_id(
            req.options.as_ref(),
        )?
        .map(ToOwned::to_owned);
        let transaction_principal = transaction_id
            .as_ref()
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let snapshot_version = match transaction_id.as_deref() {
            Some(transaction_id) => {
                let principal = transaction_principal
                    .as_deref()
                    .ok_or_else(|| Status::internal("missing transaction principal"))?;
                self.mvcc
                    .open_transactions
                    .binding(transaction_id, principal)
                    .map_err(internal_status)?;
                let handle = self
                    .mvcc
                    .open_transactions
                    .handle(transaction_id)
                    .map_err(internal_status)?;
                handle.snapshot_version
            }
            None => self
                .mvcc
                .runtime
                .applied_version()
                .map_err(internal_status)?,
        };
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
        let target_manifest = match transaction_id.as_deref() {
            Some(transaction_id) => {
                read_personaldb_group_manifest_in_transaction(
                    &self.storage,
                    &self.mvcc,
                    transaction_id,
                    transaction_principal
                        .as_deref()
                        .ok_or_else(|| Status::internal("missing transaction principal"))?,
                    claims.tenant_id,
                    &req.database_id,
                    self.personaldb_protocol_keyring.trust_store(),
                )
                .await
            }
            None => {
                read_personaldb_group_manifest(
                    &self.storage,
                    &self.mvcc,
                    claims.tenant_id,
                    &req.database_id,
                    self.personaldb_protocol_keyring.trust_store(),
                    snapshot_version,
                )
                .await
            }
        }
        .map_err(internal_status)?;
        target_manifest
            .ok_or_else(|| Status::not_found("PersonalDB projection group not found"))?;
        for source_database_id in &definition.source_database_ids {
            validate_database_id(source_database_id)?;
            let source_manifest = match transaction_id.as_deref() {
                Some(transaction_id) => {
                    read_personaldb_group_manifest_in_transaction(
                        &self.storage,
                        &self.mvcc,
                        transaction_id,
                        transaction_principal
                            .as_deref()
                            .ok_or_else(|| Status::internal("missing transaction principal"))?,
                        claims.tenant_id,
                        source_database_id,
                        self.personaldb_protocol_keyring.trust_store(),
                    )
                    .await
                }
                None => {
                    read_personaldb_group_manifest(
                        &self.storage,
                        &self.mvcc,
                        claims.tenant_id,
                        source_database_id,
                        self.personaldb_protocol_keyring.trust_store(),
                        snapshot_version,
                    )
                    .await
                }
            }
            .map_err(internal_status)?;
            source_manifest
                .ok_or_else(|| Status::not_found("PersonalDB projection source group not found"))?;
        }
        definition.definition_hash = None;
        let definition = definition
            .seal()
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let existing = match transaction_id.as_deref() {
            Some(transaction_id) => {
                read_projection_definition_in_transaction(
                    &self.storage,
                    &self.mvcc,
                    transaction_id,
                    transaction_principal
                        .as_deref()
                        .ok_or_else(|| Status::internal("missing transaction principal"))?,
                    claims.tenant_id,
                    &req.database_id,
                    &definition.projection_id,
                )
                .await
            }
            None => {
                read_projection_definition(
                    &self.storage,
                    &self.mvcc,
                    claims.tenant_id,
                    &req.database_id,
                    &definition.projection_id,
                    snapshot_version,
                )
                .await
            }
        }
        .map_err(internal_status)?;
        if let Some(existing) = existing {
            if transaction_id.is_none() && existing == definition {
                return Ok(Response::new(projection_response(
                    existing,
                    WriteState::Committed,
                )?));
            }
            return Err(Status::already_exists(
                "PersonalDB projection already exists",
            ));
        }
        let principal = transaction_principal
            .as_deref()
            .unwrap_or(claims.sub.as_str());
        let idempotency_key = format!(
            "personaldb-projection-create:{}:{}:{}",
            claims.tenant_id, req.database_id, definition.projection_id
        );
        let mut write_plan = PersonalDbWritePlan::new(
            claims.tenant_id,
            &req.database_id,
            principal,
            idempotency_key,
        )
        .map_err(internal_status)?;
        let root_generation = snapshot_version
            .checked_add(1)
            .ok_or_else(|| Status::internal("PersonalDB projection generation overflow"))?;
        prepare_and_stage_projection_definition(
            &self.storage,
            &mut write_plan,
            claims.tenant_id,
            &req.database_id,
            root_generation,
            &definition,
        )
        .await
        .map_err(internal_status)?;
        let write_state = if let Some(transaction_id) = transaction_id.as_deref() {
            write_plan
                .stage_into_transaction(
                    &self.mvcc,
                    transaction_id,
                    principal,
                    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
                )
                .await
                .map_err(internal_status)?;
            WriteState::Staged
        } else {
            write_plan
                .commit(&self.mvcc)
                .await
                .map_err(internal_status)?;
            WriteState::Committed
        };
        Ok(Response::new(projection_response(definition, write_state)?))
    }

    async fn get_personal_db_projection(
        &self,
        request: Request<GetPersonalDbProjectionRequest>,
    ) -> Result<Response<PersonalDbProjectionResponse>, Status> {
        let snapshot_version = self
            .mvcc
            .runtime
            .applied_version()
            .map_err(internal_status)?;
        let claims = request_claims(&request)?.clone();
        let req = request.into_inner();
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        validate_projection_id(&req.projection_id)?;
        if !personaldb_projection_access_allowed(
            &self.storage,
            &self.mvcc,
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
            &self.mvcc,
            claims.tenant_id,
            &req.database_id,
            &req.projection_id,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?
        .ok_or_else(|| Status::not_found("PersonalDB projection not found"))?;
        Ok(Response::new(projection_response(
            definition,
            WriteState::Committed,
        )?))
    }

    async fn submit_personal_db_changeset(
        &self,
        request: Request<SubmitPersonalDbChangesetRequest>,
    ) -> Result<Response<SubmitPersonalDbChangesetResponse>, Status> {
        let claims = request_claims(&request)?.clone();
        let bearer_token = request_bearer_token(&request)?.to_string();
        let req = request.into_inner();
        let transaction_id = crate::services::transaction_context::write_options_transaction_id(
            req.options.as_ref(),
        )?
        .map(ToOwned::to_owned);
        let transaction_principal = transaction_id
            .as_ref()
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let snapshot_version = match transaction_id.as_deref() {
            Some(transaction_id) => {
                let principal = transaction_principal
                    .as_deref()
                    .ok_or_else(|| Status::internal("missing transaction principal"))?;
                self.mvcc
                    .open_transactions
                    .binding(transaction_id, principal)
                    .map_err(internal_status)?;
                let handle = self
                    .mvcc
                    .open_transactions
                    .handle(transaction_id)
                    .map_err(internal_status)?;
                handle.snapshot_version
            }
            None => self
                .mvcc
                .runtime
                .applied_version()
                .map_err(internal_status)?,
        };
        validate_claim_tenant(claims.tenant_id, req.tenant_id)?;
        validate_database_id(&req.database_id)?;
        let core_request = core_submit_request(req)?;
        let actor =
            PersonalDbCommitActor::public(claims.tenant_id, claims.sub.clone(), bearer_token);
        let submit_idempotency_key = format!(
            "personaldb-submit:{}:{}",
            core_request.database_id, core_request.idempotency_key
        );
        if transaction_id.is_none()
            && let Some(commit_version) = PersonalDbWritePlan::resolved_commit_version(
                &self.mvcc,
                &actor.principal,
                &submit_idempotency_key,
            )
            .await
            .map_err(internal_status)?
        {
            let committed = self
                .reconstruct_personaldb_submit_retry(&core_request, commit_version)
                .await?;
            return Ok(submit_changeset_response(committed, WriteState::Committed));
        }
        let projection_definitions = list_projection_definitions_for_database(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            &core_request.database_id,
            snapshot_version,
        )
        .await
        .map_err(internal_status)?;
        if !projection_definitions.is_empty() {
            return self
                .handle_personaldb_projection_writeback(
                    core_request,
                    actor,
                    projection_definitions,
                    transaction_id
                        .as_deref()
                        .zip(transaction_principal.as_deref()),
                )
                .await;
        }
        let committed = self
            .commit_personaldb_changeset(
                core_request,
                actor,
                transaction_id
                    .as_deref()
                    .zip(transaction_principal.as_deref()),
                &[],
            )
            .await?;
        if transaction_id.is_some() {
            return Ok(submit_changeset_response(committed, WriteState::Staged));
        }
        Ok(submit_changeset_response(committed, WriteState::Committed))
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
            &self.mvcc,
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
            &self.mvcc,
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
            &self.mvcc,
            &claims,
            &req.database_id,
            AnvilAction::PersonalDbWatch,
        )
        .await?
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let mvcc = self.mvcc.clone();
        let tenant_id = claims.tenant_id;
        let database_id = req.database_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                let snapshot_version = match mvcc.runtime.applied_version() {
                    Ok(version) => version,
                    Err(error) => {
                        let _ = tx.send(Err(internal_status(error))).await;
                        return;
                    }
                };
                loop {
                    let page = match list_personaldb_group_watch_event_page(
                        &mvcc,
                        tenant_id,
                        &database_id,
                        last_cursor,
                        256,
                        snapshot_version,
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
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
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
            &self.mvcc,
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
        let mvcc = self.mvcc.clone();
        let tenant_id = claims.tenant_id;
        let database_id = req.database_id;
        let projection_id = req.projection_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                let snapshot_version = match mvcc.runtime.applied_version() {
                    Ok(version) => version,
                    Err(error) => {
                        let _ = tx.send(Err(internal_status(error))).await;
                        return;
                    }
                };
                loop {
                    let page = match list_personaldb_projection_watch_event_page(
                        &mvcc,
                        tenant_id,
                        &database_id,
                        &projection_id,
                        last_cursor,
                        256,
                        snapshot_version,
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
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchPersonalDbProjectionStream
        ))
    }
}
