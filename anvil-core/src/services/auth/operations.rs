use super::*;

impl AppState {
    pub(super) async fn write_authz_tuple_impl(
        &self,
        request: Request<WriteAuthzTupleRequest>,
    ) -> Result<Response<WriteAuthzTupleResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten()
            .map(ToOwned::to_owned);
        let _implicit_authz_write_guard = if transaction_id.is_none() {
            Some(
                crate::authz_head::tenant_write_lock(claims.tenant_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let input_hash = auth_mutation_input_hash("write-authz-tuple", &claims, &req);
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    req.context.as_ref(),
                    "write-authz-tuple",
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            return replay_auth_mutation_response::<WriteAuthzTupleResponse>(
                self,
                transaction,
                &input_hash,
            )
            .map(Response::new);
        }
        let effective_transaction_id = transaction_id
            .as_deref()
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction was not established"))?;
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(&claims);
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            req.context
                .as_ref()
                .map(|context| context.request_id.as_str())
                .unwrap_or("write-authz-tuple"),
            format!("authz-tuple:{}:{}", req.namespace, req.object_id),
            "authz.tuple.write",
            serde_json::json!({
                "relation": req.relation,
                "subject_kind": req.subject_kind,
                "subject_id": req.subject_id,
                "operation": req.operation,
            }),
        )?;
        let mutation = AuthzTupleMutation {
            namespace: req.namespace,
            object_id: req.object_id,
            relation: req.relation,
            subject_kind: req.subject_kind,
            subject_id: req.subject_id,
            caveat_hash: req.caveat_hash,
            operation: req.operation,
            reason: req.reason,
            scope: req.scope,
        };
        let operation = validate_authz_tuple_mutation(self, &claims, &mutation)
            .await?
            .to_string();
        let scope = resolve_authz_scope(&claims, mutation.scope.as_ref())?;
        let record = self
            .persistence
            .stage_authz_tuple_batch_with_tenant_audit(
                claims.tenant_id,
                vec![crate::persistence::AuthzTupleBatchMutation {
                    namespace: encode_realm_namespace(&scope.authz_realm_id, &mutation.namespace),
                    object_id: mutation.object_id,
                    relation: mutation.relation,
                    subject_kind: mutation.subject_kind.clone(),
                    subject_id: encode_userset_subject_realm(
                        &scope.authz_realm_id,
                        &mutation.subject_kind,
                        &mutation.subject_id,
                    ),
                    caveat_hash: mutation.caveat_hash,
                    operation,
                    reason: mutation.reason,
                }],
                &claims.sub,
                effective_transaction_id,
                &transaction_principal,
                None,
                Some(&audit_event),
            )
            .await
            .map_err(authz_tuple_write_status)?
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("staged tuple write returned no record"))?;

        let mut response = write_authz_tuple_response(&record)?;
        if implicit.is_none() {
            response.write_state = WriteState::Staged as i32;
            response.revision = 0;
            response.zookie.clear();
        } else {
            response.write_state = WriteState::Committed as i32;
            let now = u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("authorization mutation predates Unix epoch"))?;
            stage_auth_mutation_response(
                self,
                effective_transaction_id,
                &transaction_principal,
                AUTH_MUTATION_IMPLICIT_RESULT_KEY,
                input_hash,
                &response,
                now,
            )?;
            self.commit_implicit_auth_transaction(
                implicit
                    .as_ref()
                    .ok_or_else(|| Status::internal("implicit transaction disappeared"))?,
            )
            .await?;
        }
        Ok(Response::new(response))
    }

    pub(super) async fn write_authz_tuples_impl(
        &self,
        request: Request<WriteAuthzTuplesRequest>,
    ) -> Result<Response<WriteAuthzTuplesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten()
            .map(ToOwned::to_owned);
        let _implicit_authz_write_guard = if transaction_id.is_none() {
            Some(
                crate::authz_head::tenant_write_lock(claims.tenant_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let input_hash = auth_mutation_input_hash("write-authz-tuples", &claims, &req);
        if req.mutations.is_empty() {
            return Err(Status::invalid_argument(
                "mutations must contain at least one tuple",
            ));
        }
        if req.mutations.len() > 1000 {
            return Err(Status::invalid_argument(
                "mutations must contain no more than 1000 tuples",
            ));
        }
        for mutation in &req.mutations {
            validate_authz_tuple_mutation_shape(mutation)?;
        }
        let scope = resolve_batch_scope(&claims, req.scope.as_ref(), &req.mutations)?;
        validate_authz_batch_operation_id(req.operation_id.as_deref())?;
        let expected_revision = optional_expected_authz_revision(req.expected_revision)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzTupleWrite,
            &scope.authz_realm_id,
        )
        .await?;

        let mutations = req
            .mutations
            .iter()
            .map(|mutation| crate::persistence::AuthzTupleBatchMutation {
                namespace: encode_realm_namespace(&scope.authz_realm_id, &mutation.namespace),
                object_id: mutation.object_id.clone(),
                relation: mutation.relation.clone(),
                subject_id: encode_userset_subject_realm(
                    &scope.authz_realm_id,
                    &mutation.subject_kind,
                    &mutation.subject_id,
                ),
                subject_kind: mutation.subject_kind.clone(),
                caveat_hash: mutation.caveat_hash.clone(),
                operation: mutation.operation.clone(),
                reason: mutation.reason.clone(),
            })
            .collect::<Vec<_>>();
        let options = crate::persistence::AuthzTupleBatchWriteOptions {
            authz_realm_id: scope.authz_realm_id.clone(),
            operation_id: req.operation_id,
            expected_revision,
            schema_binding_precondition: None,
        };
        let operation_context = options
            .operation_id
            .as_deref()
            .map(|operation_id| credential_implicit_context(operation_id, operation_id));
        let implicit_context = req.context.as_ref().or(operation_context.as_ref());
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    implicit_context,
                    "write-authz-tuples",
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            return replay_auth_mutation_response::<WriteAuthzTuplesResponse>(
                self,
                transaction,
                &input_hash,
            )
            .map_err(|status| {
                if status.code() == tonic::Code::AlreadyExists {
                    Status::aborted("AuthzOperationConflict")
                } else {
                    status
                }
            })
            .map(Response::new);
        }
        let effective_transaction_id = transaction_id
            .as_deref()
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction was not established"))?;
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(&claims);
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            req.context
                .as_ref()
                .map(|context| context.request_id.as_str())
                .unwrap_or("write-authz-tuples"),
            format!("authz-realm:{}", scope.authz_realm_id),
            "authz.tuples.write",
            serde_json::json!({
                "operation_id": options.operation_id,
                "mutation_count": mutations.len(),
            }),
        )?;
        let records = self
            .persistence
            .stage_authz_tuple_batch_with_tenant_audit(
                claims.tenant_id,
                mutations,
                &claims.sub,
                effective_transaction_id,
                &transaction_principal,
                Some(&options),
                Some(&audit_event),
            )
            .await
            .map_err(authz_tuple_batch_write_status)?;
        let mut response = write_authz_tuple_batch_response(&records)?;
        if implicit.is_none() {
            response.write_state = WriteState::Staged as i32;
            response.revision = 0;
            response.zookie.clear();
            for result in &mut response.results {
                result.write_state = WriteState::Staged as i32;
                result.revision = 0;
                result.zookie.clear();
            }
        } else {
            response.write_state = WriteState::Committed as i32;
            for result in &mut response.results {
                result.write_state = WriteState::Committed as i32;
            }
            let now = u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("authorization mutation predates Unix epoch"))?;
            stage_auth_mutation_response(
                self,
                effective_transaction_id,
                &transaction_principal,
                AUTH_MUTATION_IMPLICIT_RESULT_KEY,
                input_hash,
                &response,
                now,
            )?;
            self.commit_implicit_auth_transaction(
                implicit
                    .as_ref()
                    .ok_or_else(|| Status::internal("implicit transaction disappeared"))?,
            )
            .await?;
        }
        Ok(Response::new(response))
    }

    pub(super) async fn read_authz_tuples_impl(
        &self,
        request: Request<ReadAuthzTuplesRequest>,
    ) -> Result<Response<ReadAuthzTuplesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_optional_public_authz_namespace(&req.namespace)?;
        validate_optional_tuple_field("object_id", &req.object_id)?;
        validate_optional_tuple_component("relation", &req.relation)?;
        validate_optional_tuple_component("subject_kind", &req.subject_kind)?;
        validate_optional_tuple_field("subject_id", &req.subject_id)?;
        validate_caveat_hash(&req.caveat_hash)?;
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;

        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzTupleRead,
            &scope.authz_realm_id,
        )
        .await?;
        let filter_hash = authz_page_filter_hash(
            "read_tuples",
            &[
                &scope.authz_realm_id,
                &req.namespace,
                &req.object_id,
                &req.relation,
                &req.subject_kind,
                &req.subject_id,
                &req.caveat_hash,
                if req.subject_kind.is_empty() {
                    "object_order"
                } else {
                    "subject_order"
                },
            ],
        );
        let page_size = normalize_page_size(req.page_size)?;
        let response_revision = match authz_page_token_revision(&req.page_token)? {
            Some(revision) => revision,
            None => {
                let consistency = AuthzConsistency::from_request(&req.consistency, &req.zookie)?;
                resolve_authz_response_revision(&self.mvcc, claims.tenant_id, consistency).await?
            }
        };
        let page_binding = AuthzPageBinding {
            tenant_id: claims.tenant_id,
            principal_id: &claims.sub,
            revision: response_revision,
            filter_hash: &filter_hash,
            page_size,
        };
        let page_token = parse_authz_page_token(
            &req.page_token,
            &page_binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        require_current_authz_list_revision(&self.mvcc, claims.tenant_id, response_revision)
            .await?;
        let after_tuple_key =
            decode_authz_page_position(page_token.as_ref().map(|token| token.position.as_str()))?;
        let page = authz_journal::page_current_authz_tuples(
            &self.mvcc,
            claims.tenant_id,
            &authz_journal::AuthzTupleFilter {
                realm_id: Some(scope.authz_realm_id.clone()),
                namespace: optional_filter_value(encode_optional_realm_namespace(
                    &scope.authz_realm_id,
                    &req.namespace,
                )),
                object_id: optional_filter_value(req.object_id),
                relation: optional_filter_value(req.relation),
                subject_id: optional_filter_value(encode_userset_subject_realm(
                    &scope.authz_realm_id,
                    &req.subject_kind,
                    &req.subject_id,
                )),
                subject_kind: optional_filter_value(req.subject_kind),
                caveat_hash: optional_filter_value(req.caveat_hash),
            },
            response_revision,
            after_tuple_key.as_deref(),
            page_size,
        )
        .await
        .map_err(authz_projection_page_status)?;
        let next_page_token = page
            .next_tuple_key
            .as_deref()
            .map(encode_authz_page_position)
            .transpose()?
            .as_deref()
            .map(|position| {
                encode_authz_page_token(&page_binding, position, self.config.jwt_secret.as_bytes())
            })
            .transpose()?
            .unwrap_or_default();

        Ok(Response::new(ReadAuthzTuplesResponse {
            tuples: page
                .records
                .into_iter()
                .map(|record| authz_tuple_response_for_realm(&record, &scope.authz_realm_id))
                .collect::<Result<Vec<_>, _>>()?,
            revision: revision_to_u64(response_revision)?,
            zookie: zookie(response_revision),
            next_page_token,
        }))
    }

    pub(super) async fn put_authz_schema_impl(
        &self,
        request: Request<PutAuthzSchemaRequest>,
    ) -> Result<Response<PutAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten()
            .map(ToOwned::to_owned);
        let _implicit_authz_write_guard = if transaction_id.is_none() {
            Some(
                crate::authz_head::tenant_write_lock(claims.tenant_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let input_hash = auth_mutation_input_hash("put-authz-schema", &claims, &req);
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(&claims);
        validate_storage_tenant(&claims, &req.anvil_storage_tenant_id)?;
        validate_tuple_component("schema_id", &req.schema_id)?;
        if req.namespaces.is_empty() {
            return Err(Status::invalid_argument(
                "namespaces must contain at least one schema",
            ));
        }
        for namespace in &req.namespaces {
            validate_public_authz_namespace(&namespace.namespace)?;
        }
        crate::authz_schema_contract::validate_schema_set(&req.namespaces)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaWrite,
            &format!("schema:{}", req.schema_id),
        )
        .await?;
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    req.context.as_ref(),
                    "put-authz-schema",
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            return replay_auth_mutation_response::<PutAuthzSchemaResponse>(
                self,
                transaction,
                &input_hash,
            )
            .map(Response::new);
        }
        let effective_transaction_id = transaction_id
            .as_deref()
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction was not established"))?;
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            req.context
                .as_ref()
                .map(|context| context.request_id.as_str())
                .unwrap_or("put-authz-schema"),
            format!("authz-schema:{}", req.schema_id),
            "authz.schema.put",
            serde_json::json!({ "namespace_count": req.namespaces.len() }),
        )?;
        let record = authz_realm_schema::put_schema_revision(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            &req.schema_id,
            req.namespaces,
            &claims.sub,
            &req.reason,
            Some(crate::authz_journal::AuthzTransactionBinding {
                transaction_id: effective_transaction_id,
                principal: &transaction_principal,
            }),
        )
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let authz_revision = if implicit.is_none() {
            0
        } else {
            record.authz_revision
        };
        let response = PutAuthzSchemaResponse {
            schema_ref: Some(schema_ref_response(&record.schema_ref)),
            authz_revision,
            zookie: zookie(u64_to_i64(authz_revision)?),
            write_state: if implicit.is_none() {
                WriteState::Staged as i32
            } else {
                WriteState::Committed as i32
            },
        };
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("authorization mutation predates Unix epoch"))?;
        stage_tenant_audit_in_transaction(
            self,
            effective_transaction_id,
            &transaction_principal,
            &audit_event,
            record.authz_revision,
            now,
        )?;
        if let Some(transaction) = &implicit {
            stage_auth_mutation_response(
                self,
                effective_transaction_id,
                &transaction_principal,
                AUTH_MUTATION_IMPLICIT_RESULT_KEY,
                input_hash,
                &response,
                now,
            )?;
            self.commit_implicit_auth_transaction(transaction).await?;
        }
        Ok(Response::new(response))
    }

    pub(super) async fn bind_authz_schema_impl(
        &self,
        request: Request<BindAuthzSchemaRequest>,
    ) -> Result<Response<BindAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten()
            .map(ToOwned::to_owned);
        let _implicit_authz_write_guard = if transaction_id.is_none() {
            Some(
                crate::authz_head::tenant_write_lock(claims.tenant_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let input_hash = auth_mutation_input_hash("bind-authz-schema", &claims, &req);
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(&claims);
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        let schema_ref = req
            .schema_ref
            .ok_or_else(|| Status::invalid_argument("schema_ref is required"))?;
        validate_tuple_component("schema_id", &schema_ref.schema_id)?;
        // Creating or rebinding a tenant authz realm is controlled by the
        // owning storage-tenant relation first. The realm row may not exist yet,
        // so checking the realm relation before seeding its parent_tenant tuple
        // would make first bind impossible without a non-Zanzibar bypass.
        access_control::require_storage_tenant_permission(
            &self.storage,
            &self.mvcc,
            &claims,
            "manage_tenant",
        )
        .await?;
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    req.context.as_ref(),
                    "bind-authz-schema",
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            return replay_auth_mutation_response::<BindAuthzSchemaResponse>(
                self,
                transaction,
                &input_hash,
            )
            .map(Response::new);
        }
        let effective_transaction_id = transaction_id
            .as_deref()
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction was not established"))?;
        access_control::stage_authz_realm_defaults(
            &self.persistence,
            claims.tenant_id,
            &scope.authz_realm_id,
            &claims.sub,
            &claims.sub,
            "grant creator authz realm owner",
            effective_transaction_id,
            &transaction_principal,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let binding = authz_realm_schema::bind_schema(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            &scope.authz_realm_id,
            authz_realm_schema::StoredSchemaRef {
                schema_id: schema_ref.schema_id,
                schema_revision: schema_ref.schema_revision,
                schema_digest: schema_ref.schema_digest,
            },
            req.expected_binding_generation,
            &claims.sub,
            &req.reason,
            Some(crate::authz_journal::AuthzTransactionBinding {
                transaction_id: effective_transaction_id,
                principal: &transaction_principal,
            }),
        )
        .await
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let authz_revision = if implicit.is_none() {
            0
        } else {
            binding.authz_revision
        };
        let response = BindAuthzSchemaResponse {
            scope: Some(scope),
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
            binding_generation: binding.binding_generation,
            authz_revision,
            zookie: zookie(u64_to_i64(authz_revision)?),
            write_state: if implicit.is_none() {
                WriteState::Staged as i32
            } else {
                WriteState::Committed as i32
            },
        };
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            req.context
                .as_ref()
                .map(|context| context.request_id.as_str())
                .unwrap_or("bind-authz-schema"),
            format!("authz-realm:{}", binding.realm_id),
            "authz.schema.bind",
            serde_json::json!({
                "schema_id": binding.schema_ref.schema_id,
                "schema_revision": binding.schema_ref.schema_revision,
                "binding_generation": binding.binding_generation,
            }),
        )?;
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("authorization mutation predates Unix epoch"))?;
        stage_tenant_audit_in_transaction(
            self,
            effective_transaction_id,
            &transaction_principal,
            &audit_event,
            binding.authz_revision,
            now,
        )?;
        if let Some(transaction) = &implicit {
            stage_auth_mutation_response(
                self,
                effective_transaction_id,
                &transaction_principal,
                AUTH_MUTATION_IMPLICIT_RESULT_KEY,
                input_hash,
                &response,
                now,
            )?;
            self.commit_implicit_auth_transaction(transaction).await?;
        }
        Ok(Response::new(response))
    }

    pub(super) async fn get_authz_schema_binding_impl(
        &self,
        request: Request<GetAuthzSchemaBindingRequest>,
    ) -> Result<Response<GetAuthzSchemaBindingResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let scope = resolve_authz_scope(&claims, req.scope.as_ref())?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaRead,
            &scope.authz_realm_id,
        )
        .await?;
        let binding = authz_realm_schema::read_schema_binding(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            &scope.authz_realm_id,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("schema binding not found"))?;
        Ok(Response::new(GetAuthzSchemaBindingResponse {
            scope: Some(scope),
            schema_ref: Some(schema_ref_response(&binding.schema_ref)),
            binding_generation: binding.binding_generation,
        }))
    }

    pub(super) async fn apply_authz_schema_impl(
        &self,
        request: Request<ApplyAuthzSchemaRequest>,
    ) -> Result<Response<ApplyAuthzSchemaResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let transaction_id = req
            .context
            .as_ref()
            .map(crate::services::transaction_context::public_context_transaction_id)
            .transpose()?
            .flatten()
            .map(ToOwned::to_owned);
        let _implicit_authz_write_guard = if transaction_id.is_none() {
            Some(
                crate::authz_head::tenant_write_lock(claims.tenant_id)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .lock_owned()
                    .await,
            )
        } else {
            None
        };
        let input_hash = auth_mutation_input_hash("apply-authz-schema", &claims, &req);
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(&claims);
        if req.namespaces.is_empty() {
            return Err(Status::invalid_argument(
                "namespaces must contain at least one schema",
            ));
        }
        for namespace in &req.namespaces {
            validate_public_authz_namespace(&namespace.namespace)?;
        }
        crate::authz_schema_contract::validate_schema_set(&req.namespaces)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::AuthzSchemaWrite,
            DEFAULT_AUTHZ_REALM_ID,
        )
        .await?;
        let implicit = if transaction_id.is_none() {
            Some(
                self.begin_implicit_auth_transaction(
                    &claims,
                    req.context.as_ref(),
                    "apply-authz-schema",
                )
                .await?,
            )
        } else {
            None
        };
        if let Some(transaction) = implicit.as_ref().filter(|transaction| transaction.replayed) {
            return replay_auth_mutation_response::<ApplyAuthzSchemaResponse>(
                self,
                transaction,
                &input_hash,
            )
            .map(Response::new);
        }
        let effective_transaction_id = transaction_id
            .as_deref()
            .or_else(|| {
                implicit
                    .as_ref()
                    .map(|transaction| transaction.transaction_id.as_str())
            })
            .ok_or_else(|| Status::internal("authorization transaction was not established"))?;
        access_control::stage_authz_realm_defaults(
            &self.persistence,
            claims.tenant_id,
            DEFAULT_AUTHZ_REALM_ID,
            &claims.sub,
            &claims.sub,
            "grant default authz realm owner",
            effective_transaction_id,
            &transaction_principal,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let binding = crate::authz_journal::AuthzTransactionBinding {
            transaction_id: effective_transaction_id,
            principal: &transaction_principal,
        };
        let record = authz_realm_schema::put_schema_revision(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            DEFAULT_AUTHZ_REALM_ID,
            req.namespaces,
            &claims.sub,
            &req.reason,
            Some(binding),
        )
        .await
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
        for namespace in &record.namespaces {
            authz_namespace_watch::stage_authz_namespace_watch_record(
                &self.mvcc,
                effective_transaction_id,
                &transaction_principal,
                claims.tenant_id,
                mutation_id_from_record_hash(&record.schema_ref.schema_digest),
                authz_namespace_watch::AuthzNamespaceWatchPayload {
                    namespace: namespace.namespace.clone(),
                    event_type: "schema_changed".to_string(),
                    authz_revision: record.authz_revision,
                    schema_hash: namespace.schema_hash.clone(),
                    invalidates_derived_usersets: true,
                    emitted_at: record.created_at.clone(),
                },
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        }
        authz_realm_schema::bind_schema(
            &self.storage,
            &self.mvcc,
            claims.tenant_id,
            DEFAULT_AUTHZ_REALM_ID,
            record.schema_ref.clone(),
            None,
            &claims.sub,
            &req.reason,
            Some(binding),
        )
        .await
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let response = ApplyAuthzSchemaResponse {
            namespaces: record.namespaces,
            schema_version: if implicit.is_some() {
                record.schema_ref.schema_revision
            } else {
                0
            },
            write_state: if implicit.is_some() {
                WriteState::Committed as i32
            } else {
                WriteState::Staged as i32
            },
        };
        let audit_event = crate::services::audit::build_tenant_audit_event(
            &claims,
            req.context
                .as_ref()
                .map(|context| context.request_id.as_str())
                .unwrap_or("apply-authz-schema"),
            format!("authz-realm:{DEFAULT_AUTHZ_REALM_ID}"),
            "authz.schema.apply",
            serde_json::json!({
                "schema_revision": record.schema_ref.schema_revision,
                "namespace_count": response.namespaces.len(),
            }),
        )?;
        let now = u64::try_from(chrono::Utc::now().timestamp_millis())
            .map_err(|_| Status::internal("authorization mutation predates Unix epoch"))?;
        stage_tenant_audit_in_transaction(
            self,
            effective_transaction_id,
            &transaction_principal,
            &audit_event,
            record.authz_revision,
            now,
        )?;
        if let Some(transaction) = &implicit {
            stage_auth_mutation_response(
                self,
                effective_transaction_id,
                &transaction_principal,
                AUTH_MUTATION_IMPLICIT_RESULT_KEY,
                input_hash,
                &response,
                now,
            )?;
            self.commit_implicit_auth_transaction(transaction).await?;
        }
        Ok(Response::new(response))
    }
}
