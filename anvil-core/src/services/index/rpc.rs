use super::*;

const INDEX_WATCH_SURFACE_0_4_0_LIMITATION: &str =
    "Index watch RPCs are unavailable in Anvil 0.4.0";

fn require_index_watch_surface() -> Result<(), Status> {
    Err(Status::unimplemented(INDEX_WATCH_SURFACE_0_4_0_LIMITATION))
}

fn index_write_transaction_id(options: Option<&WriteOptions>) -> Result<Option<&str>, Status> {
    crate::services::transaction_context::write_options_transaction_id(options)
}

struct IndexMutationTransaction {
    id: String,
    principal: String,
    internal: bool,
    replayed: bool,
    _authz_write_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

async fn begin_index_mutation(
    state: &AppState,
    claims: &auth::Claims,
    options: Option<&WriteOptions>,
    operation: &str,
) -> Result<IndexMutationTransaction, Status> {
    let principal = crate::object_manager::transaction_principal_from_claims(claims);
    if let Some(transaction_id) = index_write_transaction_id(options)? {
        state
            .mvcc
            .open_transactions
            .binding(transaction_id, &principal)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        return Ok(IndexMutationTransaction {
            id: transaction_id.to_string(),
            principal,
            internal: false,
            replayed: false,
            _authz_write_guard: None,
        });
    }
    let supplied = options
        .map(|options| options.idempotency_key.trim())
        .filter(|key| !key.is_empty());
    let idempotency_key = supplied.map_or_else(
        || {
            format!(
                "index:{}:{}:{operation}:{}",
                claims.tenant_id,
                claims.sub,
                uuid::Uuid::new_v4()
            )
        },
        |key| format!("index:{}:{}:{key}", claims.tenant_id, claims.sub),
    );
    let now = u64::try_from(chrono::Utc::now().timestamp_millis())
        .map_err(|_| Status::internal("index mutation timestamp predates Unix epoch"))?;
    // Index definition publication and its creator-owner tuple share one
    // atomic transaction. Hold the tenant authz revision's same-node guard
    // from snapshot selection through commit so implicit tuple/materializer
    // traffic cannot repeatedly invalidate this idempotent ensure operation.
    let authz_write_guard = crate::authz_head::tenant_write_lock(claims.tenant_id)
        .map_err(|error| Status::internal(error.to_string()))?
        .lock_owned()
        .await;
    let handle = state
        .mvcc
        .open_transactions
        .begin(
            state.mvcc.runtime.as_ref(),
            state.mvcc.cluster_id().to_string(),
            principal.clone(),
            idempotency_key,
            std::time::Duration::from_secs(300),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            crate::mvcc_transaction::ReadConsistency::Linearized,
            now,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    let status = state
        .mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if status.state == "committing" {
        let outcome = state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                &principal,
                now,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit index transaction aborted: {reason:?}"
            )));
        }
    } else if status.state == "aborted" {
        return Err(Status::aborted(
            "implicit index transaction previously aborted",
        ));
    }
    Ok(IndexMutationTransaction {
        id: handle.transaction_id,
        principal,
        internal: true,
        replayed: matches!(status.state, "committed" | "committing"),
        _authz_write_guard: Some(authz_write_guard),
    })
}

async fn commit_index_mutation(
    state: &AppState,
    transaction: &IndexMutationTransaction,
) -> Result<(), Status> {
    if !transaction.internal {
        return Ok(());
    }
    let outcome = state
        .mvcc
        .open_transactions
        .commit(
            state.mvcc.runtime.as_ref(),
            &transaction.id,
            &transaction.principal,
            u64::try_from(chrono::Utc::now().timestamp_millis())
                .map_err(|_| Status::internal("index commit predates Unix epoch"))?,
        )
        .await
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    match outcome.certification {
        crate::mvcc_transaction::CertificationResult::Committed { .. } => Ok(()),
        crate::mvcc_transaction::CertificationResult::Aborted { reason } => Err(Status::aborted(
            format!("implicit index transaction aborted: {reason:?}"),
        )),
    }
}

#[tonic::async_trait]
impl IndexService for AppState {
    type WatchIndexDefinitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexDefinitionResponse, Status>> + Send>,
    >;
    type WatchIndexPartitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexPartitionResponse, Status>> + Send>,
    >;

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let kind = concrete_index_kind(req.kind)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexCreate,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let selector = parse_json_field("selector_json", &req.selector_json)?;
        let extractor = parse_json_field("extractor_json", &req.extractor_json)?;
        let build_policy = parse_json_field("build_policy_json", &req.build_policy_json)?;
        validate_authorization_mode(&req.authorization_mode)?;
        validate_index_definition_shape(kind, &build_policy, &extractor, &self.config)?;
        let transaction =
            begin_index_mutation(self, &claims, req.options.as_ref(), "create").await?;
        if transaction.replayed {
            let index = self
                .persistence
                .get_index_definition(claims.tenant_id, bucket.id, &req.name)
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                .filter(|index| {
                    index.kind == kind.to_string()
                        && index.selector == selector
                        && index.extractor == extractor
                        && index.authorization_mode == req.authorization_mode
                        && index.build_policy == build_policy
                })
                .ok_or_else(|| {
                    Status::already_exists(
                        "index idempotency key was already used for different input",
                    )
                })?;
            return Ok(Response::new(IndexDefinitionResponse {
                index: Some(index_record(&bucket.name, index)?),
            }));
        }

        let mutation = crate::persistence::IndexDefinitionMutation::Create {
            name: req.name,
            kind: kind.to_string(),
            selector,
            extractor,
            authorization_mode: req.authorization_mode,
            build_policy,
        };
        let index = match self
            .persistence
            .apply_index_definition_mutation(
                &bucket,
                &mutation,
                Some(&transaction.id),
                Some(&transaction.principal),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            crate::persistence::IndexDefinitionMutationOutcome::Published { index, event } => {
                debug_assert_eq!(event.index_id, index.id);
                index
            }
            crate::persistence::IndexDefinitionMutationOutcome::AlreadyExists => {
                return Err(Status::already_exists("Index definition already exists"));
            }
            crate::persistence::IndexDefinitionMutationOutcome::NotFound
            | crate::persistence::IndexDefinitionMutationOutcome::KindChanged => {
                return Err(Status::internal("invalid create-index mutation outcome"));
            }
        };
        commit_index_mutation(self, &transaction).await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn update_index(
        &self,
        request: Request<UpdateIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexUpdate,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let selector = parse_json_field("selector_json", &req.selector_json)?;
        let extractor = parse_json_field("extractor_json", &req.extractor_json)?;
        let build_policy = parse_json_field("build_policy_json", &req.build_policy_json)?;
        validate_authorization_mode(&req.authorization_mode)?;
        let transaction =
            begin_index_mutation(self, &claims, req.options.as_ref(), "update").await?;
        let existing = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        validate_index_definition_shape(&existing.kind, &build_policy, &extractor, &self.config)?;
        if transaction.replayed {
            let index = self
                .persistence
                .get_index_definition(claims.tenant_id, bucket.id, &req.name)
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                .filter(|index| {
                    index.selector == selector
                        && index.extractor == extractor
                        && index.authorization_mode == req.authorization_mode
                        && index.build_policy == build_policy
                })
                .ok_or_else(|| {
                    Status::already_exists(
                        "index idempotency key was already used for different input",
                    )
                })?;
            return Ok(Response::new(IndexDefinitionResponse {
                index: Some(index_record(&bucket.name, index)?),
            }));
        }

        let mutation = crate::persistence::IndexDefinitionMutation::Update {
            name: req.name,
            expected_kind: existing.kind,
            selector,
            extractor,
            authorization_mode: req.authorization_mode,
            build_policy,
        };
        let index = match self
            .persistence
            .apply_index_definition_mutation(
                &bucket,
                &mutation,
                Some(&transaction.id),
                Some(&transaction.principal),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            crate::persistence::IndexDefinitionMutationOutcome::Published { index, event } => {
                debug_assert_eq!(event.index_id, index.id);
                index
            }
            crate::persistence::IndexDefinitionMutationOutcome::NotFound => {
                return Err(Status::not_found("Index definition not found"));
            }
            crate::persistence::IndexDefinitionMutationOutcome::KindChanged => {
                return Err(Status::aborted("Index definition changed during update"));
            }
            crate::persistence::IndexDefinitionMutationOutcome::AlreadyExists => {
                return Err(Status::internal("invalid update-index mutation outcome"));
            }
        };
        commit_index_mutation(self, &transaction).await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn disable_index(
        &self,
        request: Request<DisableIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexUpdate,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let transaction =
            begin_index_mutation(self, &claims, req.options.as_ref(), "disable").await?;
        if transaction.replayed {
            let index = self
                .persistence
                .get_index_definition(claims.tenant_id, bucket.id, &req.name)
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                .filter(|index| !index.enabled)
                .ok_or_else(|| {
                    Status::already_exists(
                        "index idempotency key was already used for different input",
                    )
                })?;
            return Ok(Response::new(IndexDefinitionResponse {
                index: Some(index_record(&bucket.name, index)?),
            }));
        }
        let mutation = crate::persistence::IndexDefinitionMutation::Disable { name: req.name };
        let index = match self
            .persistence
            .apply_index_definition_mutation(
                &bucket,
                &mutation,
                Some(&transaction.id),
                Some(&transaction.principal),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            crate::persistence::IndexDefinitionMutationOutcome::Published { index, event } => {
                debug_assert_eq!(event.index_id, index.id);
                index
            }
            crate::persistence::IndexDefinitionMutationOutcome::NotFound => {
                return Err(Status::not_found("Index definition not found"));
            }
            crate::persistence::IndexDefinitionMutationOutcome::AlreadyExists
            | crate::persistence::IndexDefinitionMutationOutcome::KindChanged => {
                return Err(Status::internal("invalid disable-index mutation outcome"));
            }
        };
        commit_index_mutation(self, &transaction).await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexDelete,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let transaction = begin_index_mutation(self, &claims, req.options.as_ref(), "drop").await?;
        if transaction.replayed {
            if self
                .persistence
                .get_index_definition(claims.tenant_id, bucket.id, &req.name)
                .await
                .map_err(|error| Status::internal(error.to_string()))?
                .is_some()
            {
                return Err(Status::already_exists(
                    "index idempotency key was already used for different input",
                ));
            }
            return Ok(Response::new(DropIndexResponse {}));
        }
        let mutation = crate::persistence::IndexDefinitionMutation::Drop { name: req.name };
        match self
            .persistence
            .apply_index_definition_mutation(
                &bucket,
                &mutation,
                Some(&transaction.id),
                Some(&transaction.principal),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            crate::persistence::IndexDefinitionMutationOutcome::Published { index, event } => {
                debug_assert_eq!(event.index_id, index.id);
            }
            crate::persistence::IndexDefinitionMutationOutcome::NotFound => {
                return Err(Status::not_found("Index definition not found"));
            }
            crate::persistence::IndexDefinitionMutationOutcome::AlreadyExists
            | crate::persistence::IndexDefinitionMutationOutcome::KindChanged => {
                return Err(Status::internal("invalid drop-index mutation outcome"));
            }
        }
        commit_index_mutation(self, &transaction).await?;
        Ok(Response::new(DropIndexResponse {}))
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let revision = index_journal::current_index_definition_collection_revision(
            &self.mvcc,
            claims.tenant_id,
            bucket.id,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        let include_disabled = req.include_disabled.to_string();
        let filters = [
            ("bucket_name", req.bucket_name.as_str()),
            ("include_disabled", include_disabled.as_str()),
        ];
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let revision_string = revision.to_string();
        let binding = crate::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.IndexService/ListIndexes",
            filters: &filters,
            principal_scope: &principal_scope,
            page_size,
            revision: &revision_string,
            sort: "name.asc",
        };
        let position = crate::services::collection_cursor::decode_page_token(
            req.page.as_ref(),
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?;
        let after_tuple_key =
            crate::services::collection_cursor::decode_binary_position(position.as_deref())?;
        let page = index_journal::page_current_index_definition_events(
            &self.mvcc,
            claims.tenant_id,
            bucket.id,
            req.include_disabled,
            revision,
            after_tuple_key.as_deref(),
            page_size,
        )
        .await
        .map_err(|error| Status::aborted(error.to_string()))?;
        let next_page_token = page
            .next_tuple_key
            .as_deref()
            .map(crate::services::collection_cursor::encode_binary_position)
            .transpose()?
            .map(|position| {
                crate::services::collection_cursor::encode_next_page_token(
                    &position,
                    &binding,
                    self.config.jwt_secret.as_bytes(),
                )
            })
            .transpose()?
            .unwrap_or_default();
        let indexes = page
            .events
            .into_iter()
            .map(|event| index_record_from_event(&event))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexesResponse {
            indexes,
            page: Some(PageResponse { next_page_token }),
        }))
    }

    async fn query_index(
        &self,
        request: Request<QueryIndexRequest>,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.index_name)?;
        let index_resource = format!("{}/{}", req.bucket_name, req.index_name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            &index_resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.index_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|index| index.enabled)
            .ok_or_else(|| Status::not_found("Index definition not found"))?;

        match index.kind.as_str() {
            "path" | "metadata_filter" => {
                self.query_metadata_backed_index(&claims, &bucket, &index, req)
                    .await
            }
            "typed_json" => {
                self.query_typed_json_index(&claims, &bucket, &index, req)
                    .await
            }
            "full_text" => {
                self.query_full_text_index(&claims, &bucket, &index, req)
                    .await
            }
            "vector" => self.query_vector_index(&claims, &bucket, &index, req).await,
            "hybrid" => self.query_hybrid_index(&claims, &bucket, &index, req).await,
            _ => Err(Status::failed_precondition("IndexDoesNotSupportQuery")),
        }
    }

    async fn query_spec(
        &self,
        request: Request<QuerySpecRequest>,
    ) -> Result<Response<QuerySpecResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let spec = AnvilQuerySpec::parse(&req.query_spec_json)?;
        let bucket_name = spec.scope.bucket_name.trim();
        if bucket_name.is_empty() {
            return Err(Status::invalid_argument(
                "QuerySpec scope.bucket_name is required",
            ));
        }
        if let Some(storage_tenant) = spec.scope.anvil_storage_tenant_id.as_deref()
            && !storage_tenant.is_empty()
            && storage_tenant != claims.tenant_id.to_string()
        {
            return Err(Status::permission_denied(
                "QuerySpec storage tenant does not match authenticated tenant",
            ));
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            bucket_name,
        )
        .await?;
        let bucket = self.get_index_bucket(claims.tenant_id, bucket_name).await?;
        let plan = self
            .plan_query_spec(&claims, &bucket, &spec, req.accept_degraded)
            .await?;
        let response = {
            if plan.typed_filter_index.is_some() {
                self.query_composite_query_spec(
                    &claims,
                    &bucket,
                    &plan,
                    &req.page_token,
                    req.lag_timeout_ms,
                )
                .await?
            } else {
                let query_req =
                    plan.single_query_request(&bucket.name, &req.page_token, req.lag_timeout_ms)?;
                match plan.index.kind.as_str() {
                    "path" | "metadata_filter" => {
                        self.query_metadata_backed_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "typed_json" => {
                        self.query_typed_json_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "full_text" => {
                        self.query_full_text_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "vector" => {
                        self.query_vector_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "hybrid" => {
                        self.query_hybrid_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    _ => return Err(Status::failed_precondition("IndexDoesNotSupportQuerySpec")),
                }
                .into_inner()
            }
        };
        if spec.consistency.allow_stale_index == Some(false) {
            if self
                .mvcc
                .runtime
                .local_store()
                .has_incomplete_object_materialisations()
                .map_err(|error| Status::internal(error.to_string()))?
            {
                return Err(Status::failed_precondition("ObjectMaterialisationPending"));
            }
            if !response.is_caught_up {
                return Err(Status::failed_precondition("IndexLagging"));
            }
        }

        Ok(Response::new(QuerySpecResponse {
            result: Some(response),
            canonical_query_hash: plan.canonical_query_hash,
            plan_json: plan.plan_json,
        }))
    }

    async fn watch_index_definition(
        &self,
        request: Request<WatchIndexDefinitionRequest>,
    ) -> Result<Response<Self::WatchIndexDefinitionStream>, Status> {
        require_index_watch_surface()?;
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexWatch,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let mvcc = std::sync::Arc::clone(&self.mvcc);
        let tenant_id = claims.tenant_id;
        let bucket_id = bucket.id;

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match index_journal::read_index_definition_event_page_mvcc(
                        &mvcc,
                        tenant_id,
                        bucket_id,
                        last_cursor,
                        256,
                    ) {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx
                            .send(index_definition_event_response(&event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchIndexDefinitionStream
        ))
    }

    async fn watch_index_partition(
        &self,
        request: Request<WatchIndexPartitionRequest>,
    ) -> Result<Response<Self::WatchIndexPartitionStream>, Status> {
        require_index_watch_surface()?;
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.index_name)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexWatch,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.index_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|index| index.enabled)
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let partition_id = if req.partition_id.trim().is_empty() {
            hex::encode(crate::formats::hash32(index_storage_id.as_bytes()))
        } else {
            validate_hex32(&req.partition_id, "partition_id")?;
            req.partition_id
        };
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let storage = self.storage.clone();
        let bucket_name = bucket.name.clone();
        let index_name = index.name.clone();
        let tenant_id = claims.tenant_id;
        let bucket_id = bucket.id;
        let mvcc = self.mvcc.clone();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(std::time::Duration::from_millis(250));
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match index_partition_watch::list_index_partition_watch_event_page(
                        &mvcc,
                        tenant_id,
                        bucket_id,
                        &index_storage_id,
                        &partition_id,
                        last_cursor,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx
                            .send(index_partition_event_response(
                                &bucket_name,
                                &index_name,
                                &index_storage_id,
                                &partition_id,
                                event,
                            ))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }
                poll.tick().await;
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchIndexPartitionStream
        ))
    }

    async fn list_index_diagnostics(
        &self,
        request: Request<ListIndexDiagnosticsRequest>,
    ) -> Result<Response<ListIndexDiagnosticsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.index_name.is_empty() {
            validate_index_name(&req.index_name)?;
        }
        if !req.severity.is_empty() {
            validate_diagnostic_severity(&req.severity)?;
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let page_size = crate::services::collection_cursor::page_size(req.page.as_ref())?;
        let filters = [
            ("bucket_name", req.bucket_name.as_str()),
            ("index_name", req.index_name.as_str()),
            ("severity", req.severity.as_str()),
        ];
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let revision = format!("append-only:{}:{}", claims.tenant_id, bucket.id);
        let binding = crate::services::collection_cursor::CollectionCursorBinding {
            service_method: "anvil.IndexService/ListIndexDiagnostics",
            filters: &filters,
            principal_scope: &principal_scope,
            page_size,
            revision: &revision,
            sort: "cursor.asc",
        };
        let after_cursor = crate::services::collection_cursor::decode_page_token(
            req.page.as_ref(),
            &binding,
            self.config.jwt_secret.as_bytes(),
        )?
        .map(|cursor| {
            cursor
                .parse::<i64>()
                .map_err(|_| Status::invalid_argument("invalid diagnostic cursor"))
        })
        .transpose()?
        .unwrap_or_default();
        let query_limit = i32::try_from(page_size + 1)
            .map_err(|_| Status::invalid_argument("page_size exceeds supported range"))?;
        let mut diagnostics = self
            .persistence
            .list_index_diagnostics(
                claims.tenant_id,
                bucket.id,
                &req.index_name,
                &req.severity,
                after_cursor,
                query_limit,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let has_more = diagnostics.len() > page_size;
        if has_more {
            diagnostics.truncate(page_size);
        }
        let next_page_token = if has_more {
            let position = diagnostics
                .last()
                .ok_or_else(|| Status::internal("diagnostic page is unexpectedly empty"))?
                .id
                .to_string();
            crate::services::collection_cursor::encode_next_page_token(
                &position,
                &binding,
                self.config.jwt_secret.as_bytes(),
            )?
        } else {
            String::new()
        };
        let diagnostics = diagnostics
            .into_iter()
            .map(index_diagnostic_record)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexDiagnosticsResponse {
            diagnostics,
            page: Some(PageResponse { next_page_token }),
        }))
    }
}
