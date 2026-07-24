use super::*;
use crate::formats::writer::WriterFamily;

impl AppState {
    pub(super) async fn latest_system_authz_revision_for_query(&self) -> Result<u64, Status> {
        crate::authz_journal::latest_authz_revision(
            &self.storage,
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))
        .map(|revision| revision.max(0) as u64)
    }

    pub(super) async fn plan_query_spec(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        spec: &AnvilQuerySpec,
        accept_degraded: bool,
    ) -> Result<QuerySpecPlan, Status> {
        let shape = spec.shape()?;
        if shape.can_relation.as_deref().unwrap_or("read") != "read" {
            return Err(Status::failed_precondition(
                "QuerySpec only supports read authorisation relation for index results",
            ));
        }
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let system_authz_revision = self.latest_system_authz_revision_for_query().await?;
        if let Some(min_authz_revision) = shape.min_authz_revision
            && latest_authz_revision.max(system_authz_revision) < min_authz_revision
        {
            return Err(Status::failed_precondition("AuthzRevisionLagging"));
        }
        let indexes = self
            .persistence
            .list_index_definitions(claims.tenant_id, bucket.id, false)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let selected = select_query_spec_indexes(&indexes, &shape, accept_degraded)?;
        let authorization_mode = selected.effective_authorization_mode().to_string();
        if selected.requires_object_authorization() && shape.can_relation.is_none() {
            return Err(Status::failed_precondition(
                "QuerySpec requires an explicit can predicate for protected resources",
            ));
        }
        let authz_scope = QueryAuthzScope::for_bucket(
            &self.config,
            claims,
            bucket,
            &authorization_mode,
            shape.can_relation.as_deref().unwrap_or("read"),
            shape.authz_scope.as_ref(),
            latest_authz_revision,
            system_authz_revision,
        );

        let plan_json = serde_json::json!({
            "schema": "anvil.query.plan.v1",
            "planner": if selected.typed_filter.is_some() {
                "primitive-index-intersection"
            } else {
                "primitive-index-single-stage"
            },
            "selected_index": {
                "name": selected.primary.name,
                "kind": selected.primary.kind,
                "version": selected.primary.version,
                "authorization_mode": selected.primary.authorization_mode,
            },
            "effective_authorization_mode": authorization_mode,
            "filter_index": selected.typed_filter.as_ref().map(|index| serde_json::json!({
                "name": index.name,
                "kind": index.kind,
                "version": index.version,
                "authorization_mode": index.authorization_mode,
            })),
            "source_kind": shape.source_kind,
            "path_prefix": shape.path_prefix,
            "boundary_predicate_count": shape.boundary_predicates.len(),
            "typed_predicate_count": shape.typed_predicates.len(),
            "typed_order_count": shape.typed_order.len(),
            "uses_full_text": shape.query_text.is_some(),
            "uses_vector": shape.query_vector.is_some(),
            "authz_relation": shape.can_relation,
            "authz_revision": latest_authz_revision,
            "authz_scope": authz_scope.trace_json(),
            "degraded": false,
        })
        .to_string();

        Ok(QuerySpecPlan {
            index: selected.primary,
            typed_filter_index: selected.typed_filter,
            authz_scope,
            canonical_query_hash: stable_json_hash(&spec.canonical_json()?),
            plan_json,
            query_text: shape.query_text.unwrap_or_default(),
            query_vector: shape.query_vector.unwrap_or_default(),
            phrase: shape.phrase,
            path_prefix: shape.path_prefix.unwrap_or_default(),
            boundary_predicates: shape.boundary_predicates,
            typed_predicates: shape.typed_predicates,
            typed_order: shape.typed_order,
            limit: shape.limit,
            require_caught_up_to_watch_cursor: shape
                .min_source_cursor
                .map(|cursor| cursor.to_string())
                .unwrap_or_default(),
        })
    }

    pub(super) async fn query_composite_query_spec(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        plan: &QuerySpecPlan,
        page_token: &str,
        lag_timeout_ms: u64,
    ) -> Result<QueryIndexResponse, Status> {
        let typed_filter = plan
            .typed_filter_index
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("IndexCapabilityMissing"))?;
        let requested_limit = query_limit(plan.limit);
        let candidate_limit = requested_limit.saturating_mul(50).max(1000);

        let primary_req = QueryIndexRequest {
            bucket_name: bucket.name.clone(),
            index_name: plan.index.name.clone(),
            query_text: plan.query_text.clone(),
            query_vector: plan.query_vector.clone(),
            limit: u32::try_from(candidate_limit).unwrap_or(u32::MAX),
            phrase: plan.phrase,
            path_prefix: String::new(),
            metadata_filters_json: String::new(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: plan.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        };
        let primary_response = match plan.index.kind.as_str() {
            "full_text" => {
                self.query_full_text_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            "vector" => {
                self.query_vector_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            "hybrid" => {
                self.query_hybrid_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            _ => return Err(Status::failed_precondition("IndexCapabilityMissing")),
        }
        .into_inner();

        let mut primary_by_key = BTreeMap::new();
        for hit in &primary_response.hits {
            primary_by_key
                .entry(hit.object_key.clone())
                .or_insert_with(|| hit.clone());
        }

        let typed_boundary_predicates_json = serde_json::to_string(&plan.boundary_predicates)
            .map_err(|e| Status::internal(e.to_string()))?;
        let typed_predicates_json = serde_json::to_string(&plan.typed_predicates)
            .map_err(|e| Status::internal(e.to_string()))?;
        let typed_order_json = serde_json::to_string(&plan.typed_order)
            .map_err(|e| Status::internal(e.to_string()))?;

        let typed_page_limit = requested_limit.max(1);
        let mut typed_page_token = page_token.to_string();
        let mut next_page_token = String::new();
        let mut hits = Vec::new();
        let mut typed_candidates_scanned = 0usize;
        let mut typed_index_generation = 0u64;
        let mut typed_authz_revision = 0u64;
        let mut typed_source_watch_cursor_high = 0u64;
        let mut typed_index_watch_cursor_applied = u64::MAX;
        let mut typed_is_caught_up = true;
        let mut typed_lag_record_count_hint = 0u64;
        let mut typed_scoring = serde_json::json!({
            "kind": "typed_json",
        });

        loop {
            let typed_req = QueryIndexRequest {
                bucket_name: bucket.name.clone(),
                index_name: typed_filter.name.clone(),
                query_text: String::new(),
                query_vector: Vec::new(),
                limit: u32::try_from(typed_page_limit).unwrap_or(u32::MAX),
                phrase: false,
                path_prefix: plan.path_prefix.clone(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: typed_boundary_predicates_json.clone(),
                typed_predicates_json: typed_predicates_json.clone(),
                typed_order_json: typed_order_json.clone(),
                page_token: typed_page_token.clone(),
                require_caught_up_to_watch_cursor: plan.require_caught_up_to_watch_cursor.clone(),
                lag_timeout_ms,
            };
            let typed_response = self
                .query_typed_json_index(claims, bucket, typed_filter, typed_req)
                .await?
                .into_inner();

            typed_index_generation = typed_index_generation.max(typed_response.index_generation);
            typed_authz_revision = typed_authz_revision.max(typed_response.authz_revision);
            typed_source_watch_cursor_high =
                typed_source_watch_cursor_high.max(typed_response.source_watch_cursor_high);
            typed_index_watch_cursor_applied =
                typed_index_watch_cursor_applied.min(typed_response.index_watch_cursor_applied);
            typed_is_caught_up &= typed_response.is_caught_up;
            typed_lag_record_count_hint =
                typed_lag_record_count_hint.max(typed_response.lag_record_count_hint);
            typed_scoring = serde_json::from_str(&typed_response.scoring_recipe_json)
                .unwrap_or_else(|_| serde_json::json!({ "kind": "typed_json" }));
            typed_candidates_scanned =
                typed_candidates_scanned.saturating_add(typed_response.hits.len());

            for typed_hit in &typed_response.hits {
                if let Some(hit) = primary_by_key.get(&typed_hit.object_key) {
                    hits.push(hit.clone());
                    if hits.len() >= requested_limit {
                        break;
                    }
                }
            }

            next_page_token = typed_response.next_page_token;
            if hits.len() >= requested_limit
                || next_page_token.is_empty()
                || typed_candidates_scanned >= candidate_limit
            {
                break;
            }
            typed_page_token = next_page_token.clone();
        }

        let primary_scoring: JsonValue =
            serde_json::from_str(&primary_response.scoring_recipe_json).unwrap_or_else(|_| {
                serde_json::json!({
                    "kind": plan.index.kind,
                })
            });
        if typed_index_watch_cursor_applied == u64::MAX {
            typed_index_watch_cursor_applied = 0;
        }

        Ok(QueryIndexResponse {
            hits,
            index_kind: primary_response.index_kind,
            index_generation: primary_response
                .index_generation
                .max(typed_index_generation),
            authz_revision: primary_response.authz_revision.max(typed_authz_revision),
            scoring_recipe_json: serde_json::json!({
                "kind": "query_spec_composite",
                "planner": "primitive-index-intersection",
                "primary_index": plan.index.name,
                "typed_filter_index": typed_filter.name,
                "typed_candidates_scanned": typed_candidates_scanned,
                "primary_scoring": primary_scoring,
                "typed_filter_scoring": typed_scoring,
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: primary_response
                .source_watch_cursor_high
                .max(typed_source_watch_cursor_high),
            index_watch_cursor_applied: primary_response
                .index_watch_cursor_applied
                .min(typed_index_watch_cursor_applied),
            is_caught_up: primary_response.is_caught_up && typed_is_caught_up,
            lag_record_count_hint: primary_response
                .lag_record_count_hint
                .max(typed_lag_record_count_hint),
        })
    }

    pub(super) fn index_page_token_signing_key(&self) -> Result<Vec<u8>, Status> {
        hex::decode(&self.config.anvil_secret_encryption_key)
            .map_err(|_| Status::internal("Invalid index page token signing key"))
    }

    pub(super) async fn index_page_token_boundary_hash(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
    ) -> Result<String, Status> {
        let bucket_key =
            crate::core_store::boundary_schema_bucket_key(claims.tenant_id, &bucket.name);
        let raw_generation_hash = self
            .core_store
            .read_boundary_schema_generation_hash(&bucket_key)
            .await
            .map_err(|e| Status::internal(format!("Read boundary schema generation: {e}")))?;
        Ok(stable_prefixed_json_hash(&serde_json::json!({
            "schema": "anvil.query.boundary_schema_generation_hash.v1",
            "bucket_key": bucket_key,
            "raw_generation_hash": raw_generation_hash,
        })))
    }

    pub(super) async fn query_full_text_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if req.query_text.trim().is_empty() {
            return Err(Status::invalid_argument("query_text is required"));
        }
        if !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_vector is not valid for full_text indexes",
            ));
        }
        ensure_planner_supported_query_shape("full_text", &req)?;
        ensure_no_direct_boundary_predicates(&req)?;
        let definition = full_text_definition(index)?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let query_terms = tokenize_text(&req.query_text, &definition.tokenizer)
            .into_iter()
            .map(|token| token.term.into_bytes())
            .collect::<Vec<_>>();
        let signing_key = self.index_page_token_signing_key()?;
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        let segment_ref = if let Some(token) = page_token.as_ref() {
            index_coremeta::index_segment_coremeta_record_for_family_generation(
                &self.storage,
                &index_storage_id,
                WriterFamily::FullText.as_str(),
                token.index_generation,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(|record| record.segment_ref)
            .ok_or_else(|| Status::invalid_argument("PageTokenScopeMismatch"))?
        } else {
            full_text_segment::latest_full_text_segment_ref(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::failed_precondition("IndexUnavailable"))?
        };
        let segment = full_text_segment::read_full_text_segment_terms(
            &self.storage,
            &segment_ref,
            &query_terms,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let reported_authz_revision = authz_revision.max(segment.header.authz_revision);
        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let system_authz_revision = self.latest_system_authz_revision_for_query().await?;
        let authz_scope = QueryAuthzScope::for_bucket(
            &self.config,
            claims,
            bucket,
            &index.authorization_mode,
            "read",
            None,
            authz_revision,
            system_authz_revision,
        );
        let authorized_labels = authz_label_filter_for_index_candidate_set(
            &index.authorization_mode,
            segment.header.authz_revision,
            reported_authz_revision,
        )?;
        let search_hits = search_query::query_full_text_segment(
            &segment,
            search_query::FullTextSegmentQuery {
                query: &req.query_text,
                tokenizer: &definition.tokenizer,
                positions_enabled: definition.positions_enabled,
                phrase: req.phrase,
                bm25: Bm25Config::default(),
                authorized_labels,
                limit: segment.postings.len().max(1),
            },
        )
        .map_err(full_text_query_status)?;
        let predicate_hash = score_based_predicate_hash("full_text", &req, &authz_scope)?;
        let order_hash = score_order_hash(&authz_scope);
        let boundary_schema_generation_hash =
            self.index_page_token_boundary_hash(claims, bucket).await?;
        let input_candidate_count = search_hits.len() as u64;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "full_text",
            &bucket.name,
            &index.name,
            segment.header.generation,
            segment.header.source_cursor,
            index.version.max(0) as u64,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
        );
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        let document_table =
            full_text_segment::decode_full_text_document_table(&segment.document_table)
                .map_err(|e| Status::internal(e.to_string()))?;
        let mut candidates = Vec::with_capacity(search_hits.len());
        for hit in search_hits {
            let Some(document_ref) = document_table.get(&(hit.document_id, hit.field_id)) else {
                continue;
            };
            candidates.push(IndexQueryHit {
                kind: index_kind,
                score: hit.score,
                object_key: document_ref.object_key.clone(),
                object_version_id: uuid::Uuid::from_bytes(hit.object_version_id).to_string(),
                document_id: hit.document_id,
                field_id: u32::from(hit.field_id),
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "matched_terms": hit.matched_terms,
                    "authz_label_hash": hex::encode(hit.authz_label_hash),
                })
                .to_string(),
            });
        }
        let planner_snapshot = PlannerCandidateSnapshot::from_index_query_hits(
            bucket,
            index,
            segment.header.generation,
            segment.header.source_cursor,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
            &segment_ref,
            &candidates,
        )?;
        let planner_result = execute_corestore_query_plan(
            &self.storage,
            claims,
            bucket,
            &index.authorization_mode,
            &authz_scope,
            &planner_snapshot,
            requested_limit.saturating_add(1),
        )
        .await?;
        let selected_object_ids =
            planner_snapshot.selected_object_ids(&planner_result.candidates)?;
        let mut selected_candidates = Vec::new();
        for hit in candidates {
            if !selected_object_ids.contains(&format!("{}/{}", bucket.name, hit.object_key)) {
                continue;
            }
            if self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &hit.object_key,
                    Some(&authz_scope),
                    authz_revision,
                )
                .await?
            {
                selected_candidates.push(hit);
            }
        }
        let mut candidates = selected_candidates;
        record_query_plan_metrics(
            "full_text",
            &index.authorization_mode,
            input_candidate_count,
            planner_result.metrics.boundary_candidate_count,
            planner_result.metrics.authz_candidate_count,
            planner_result.metrics.index_candidate_count,
            candidates.len() as u64,
        );
        candidates.sort_by(|left, right| {
            compare_score_hits(
                left.score,
                &left.object_version_id,
                right.score,
                &right.object_version_id,
            )
        });
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for hit in candidates {
                if score_after_cursor(hit.score, &hit.object_version_id, token)? {
                    filtered.push(hit);
                }
            }
            candidates = filtered;
        }
        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|hit| {
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        score_sort_values(hit.score, &hit.object_version_id),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            hits: candidates,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision: reported_authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "bm25",
                "k1": 1.2,
                "b": 0.75,
                "authz_scope": authz_scope.trace_json(),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment.header.source_cursor,
            is_caught_up: segment.header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment.header.source_cursor),
        }))
    }

    pub(super) async fn query_metadata_backed_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.trim().is_empty() {
            return Err(Status::invalid_argument(
                "query_text is not valid for path or metadata_filter indexes",
            ));
        }
        if !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_vector is not valid for path or metadata_filter indexes",
            ));
        }
        if index.kind == "metadata_filter" && req.metadata_filters_json.trim().is_empty() {
            return Err(Status::invalid_argument(
                "metadata_filters_json is required for metadata_filter indexes",
            ));
        }
        ensure_planner_supported_query_shape(&index.kind, &req)?;
        let filters = QueryFilters::from_request(&req)?;
        let boundary_predicates = BoundaryPredicate::parse_list(&req.boundary_predicates_json)?;
        if !boundary_predicates.is_empty() {
            return Err(Status::failed_precondition("IndexCapabilityMissing"));
        }
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let required_cursor = if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
            Some(required_cursor)
        } else {
            None
        };
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let signing_key = self.index_page_token_signing_key()?;
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        let segment_ref = if let Some(token) = page_token.as_ref() {
            Some(
                index_coremeta::index_segment_coremeta_record_for_family_generation(
                    &self.storage,
                    &index_storage_id,
                    WriterFamily::TypedMetadata.as_str(),
                    token.index_generation,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map(|record| record.segment_ref)
                .ok_or_else(|| Status::invalid_argument("PageTokenScopeMismatch"))?,
            )
        } else {
            typed_field_segment::latest_typed_field_segment_ref(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };
        let Some(segment_ref) = segment_ref else {
            if latest_cursor == 0 {
                return Ok(Response::new(QueryIndexResponse {
                    hits: Vec::new(),
                    index_kind,
                    index_generation: 0,
                    authz_revision: 0,
                    scoring_recipe_json: serde_json::json!({
                        "kind": index.kind.as_str(),
                        "score": "constant",
                        "source": "corestore_typed_field_segment",
                    })
                    .to_string(),
                    next_page_token: String::new(),
                    source_watch_cursor_high: 0,
                    index_watch_cursor_applied: 0,
                    is_caught_up: true,
                    lag_record_count_hint: 0,
                }));
            }
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        let segment_header =
            typed_field_segment::read_typed_field_segment_header(&self.storage, &segment_ref)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        if segment_header.source_kind != "object_metadata" {
            return Err(Status::failed_precondition(
                "MetadataBackedIndexSourceKindMismatch",
            ));
        }
        if required_cursor.is_some_and(|cursor| segment_header.source_cursor < cursor) {
            return Err(Status::failed_precondition("IndexLagging"));
        }
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let authz_revision = latest_authz_revision.max(0) as u64;
        let system_authz_revision = self.latest_system_authz_revision_for_query().await?;
        let authz_scope = QueryAuthzScope::for_bucket(
            &self.config,
            claims,
            bucket,
            &index.authorization_mode,
            "read",
            None,
            authz_revision,
            system_authz_revision,
        );
        let predicate_hash = metadata_backed_predicate_hash(&index.kind, &req, &authz_scope)?;
        let order_hash = object_key_order_hash(&authz_scope);
        let boundary_schema_generation_hash =
            self.index_page_token_boundary_hash(claims, bucket).await?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "metadata_backed",
            &bucket.name,
            &index.name,
            segment_header.generation,
            segment_header.source_cursor,
            index.version.max(0) as u64,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
        );
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }

        let candidate_entries = metadata_candidate_entries_from_value_index(
            &self.storage,
            &segment_ref,
            &req.path_prefix,
            &filters,
            segment_header.row_count,
        )
        .await?;
        let planner_snapshot = PlannerCandidateSnapshot::from_typed_value_entries(
            bucket,
            index,
            segment_header.generation,
            segment_header.source_cursor,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
            &segment_ref,
            &candidate_entries,
        )?;
        let planner_result = execute_corestore_query_plan(
            &self.storage,
            claims,
            bucket,
            &index.authorization_mode,
            &authz_scope,
            &planner_snapshot,
            requested_limit.saturating_add(1),
        )
        .await?;
        let selected_ordinals = planner_result
            .ranges
            .iter()
            .filter_map(|range| usize::try_from(range.logical_start).ok())
            .collect::<Vec<_>>();
        let selected_rows = typed_field_segment::read_typed_field_rows_by_ordinals(
            &self.storage,
            &segment_ref,
            selected_ordinals,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut rows = Vec::new();
        for row in &selected_rows {
            let object_ref = QueryObjectRef::from_typed_field_row(row)?;
            if !filters.matches(&object_ref)? {
                continue;
            }
            if self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    Some(&authz_scope),
                    authz_revision,
                )
                .await?
            {
                rows.push((row.source_identity.clone(), object_ref));
            }
        }
        record_query_plan_metrics(
            &index.kind,
            &index.authorization_mode,
            candidate_entries.len() as u64,
            planner_result.metrics.boundary_candidate_count,
            planner_result.metrics.authz_candidate_count,
            planner_result.metrics.index_candidate_count,
            rows.len() as u64,
        );

        rows.sort_by(|(left_source, left), (right_source, right)| {
            left.object_key
                .cmp(&right.object_key)
                .then(left_source.cmp(right_source))
        });
        if let Some(token) = page_token.as_ref() {
            rows.retain(|(source_identity, object_ref)| {
                object_key_after_cursor(
                    &object_ref.object_key,
                    source_identity,
                    &token.last_sort_values,
                    &token.last_source_identity,
                )
            });
        }

        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|(source_identity, object_ref)| {
                    let mut sort_values = BTreeMap::new();
                    sort_values.insert(
                        "object_key".to_string(),
                        JsonValue::String(object_ref.object_key.clone()),
                    );
                    IndexPageToken::for_cursor(&binding, source_identity.clone(), sort_values)
                        .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let mut hits = Vec::with_capacity(rows.len());
        for (_, object_ref) in rows {
            let metadata_json = serde_json::json!({
                "bucket_name": bucket.name,
                "user_metadata": object_ref.user_meta.clone(),
                "created_at_nanos": object_ref.created_at_nanos,
                "authz_revision": object_ref.authz_revision,
            })
            .to_string();
            hits.push(IndexQueryHit {
                kind: index_kind,
                score: 1.0,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json,
            });
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: segment_header.generation,
            authz_revision: authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": index.kind.as_str(),
                "score": "constant",
                "source": "corestore_typed_field_segment",
                "authz_scope": authz_scope.trace_json(),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment_header.source_cursor,
            is_caught_up: segment_header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment_header.source_cursor),
        }))
    }

    pub(super) async fn query_typed_json_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.trim().is_empty() || !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_text and query_vector are not valid for typed_json indexes",
            ));
        }
        let definition = TypedJsonIndexDefinition::from_index(index)?;
        ensure_planner_supported_query_shape("typed_json", &req)?;
        let predicates = TypedPredicate::parse_list(&req.typed_predicates_json)?;
        let boundary_predicates = BoundaryPredicate::parse_list(&req.boundary_predicates_json)?;
        let order = TypedOrder::parse_list(&req.typed_order_json, &definition.default_order)?;
        let requested_limit = query_limit(req.limit);
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let system_authz_revision = self.latest_system_authz_revision_for_query().await?;
        let authz_scope = QueryAuthzScope::for_bucket(
            &self.config,
            claims,
            bucket,
            &index.authorization_mode,
            "read",
            None,
            authz_revision,
            system_authz_revision,
        );
        let predicate_hash = typed_json_predicate_hash(&req, &authz_scope)?;
        let order_hash = typed_order_hash(&order, &authz_scope)?;

        let latest_cursor = if definition.source_kind == "append_record" {
            u64::try_from(
                crate::append_journal::append_record_source_cursor(
                    &self.storage,
                    claims.tenant_id,
                    bucket.id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            )
            .unwrap_or(u64::MAX)
        } else {
            self.persistence
                .latest_object_watch_cursor(claims.tenant_id, bucket.id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .max(0) as u64
        };
        let required_cursor = if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
            Some(required_cursor)
        } else {
            None
        };

        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let segment_ref =
            typed_field_segment::latest_typed_field_segment_ref(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::failed_precondition("TypedJsonIndexNotMaterialised"))?;
        let segment_header =
            typed_field_segment::read_typed_field_segment_header(&self.storage, &segment_ref)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        if segment_header.source_kind != definition.source_kind {
            return Err(Status::failed_precondition(
                "TypedJsonIndexSourceKindMismatch",
            ));
        }
        let expected_fields = definition
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        if segment_header
            .field_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != expected_fields
        {
            return Err(Status::failed_precondition(
                "TypedJsonIndexFieldSetMismatch",
            ));
        }
        if required_cursor.is_some_and(|cursor| segment_header.source_cursor < cursor) {
            return Err(Status::failed_precondition("IndexLagging"));
        }
        if predicates.is_empty() && boundary_predicates.is_empty() {
            return Err(Status::failed_precondition("IndexCapabilityMissing"));
        }
        let signing_key = self.index_page_token_signing_key()?;
        let boundary_schema_generation_hash =
            self.index_page_token_boundary_hash(claims, bucket).await?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "typed_json",
            &bucket.name,
            &index.name,
            segment_header.generation,
            segment_header.source_cursor,
            index.version.max(0) as u64,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }

        let mut candidate_entries = if predicates.is_empty() {
            boundary_candidate_entries_from_value_index(
                &self.storage,
                &segment_ref,
                &boundary_predicates,
                segment_header.row_count,
            )
            .await?
        } else {
            typed_json_candidate_entries_from_value_index(
                &self.storage,
                &segment_ref,
                &predicates,
                segment_header.row_count,
            )
            .await?
        };
        if !predicates.is_empty() && !boundary_predicates.is_empty() {
            let boundary_entries = boundary_candidate_entries_from_value_index(
                &self.storage,
                &segment_ref,
                &boundary_predicates,
                segment_header.row_count,
            )
            .await?;
            candidate_entries =
                intersect_typed_candidate_entries(candidate_entries, boundary_entries);
        }

        let planner_snapshot = PlannerCandidateSnapshot::from_typed_value_entries(
            bucket,
            index,
            segment_header.generation,
            segment_header.source_cursor,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
            &segment_ref,
            &candidate_entries,
        )?;
        let planner_result = execute_corestore_query_plan(
            &self.storage,
            claims,
            bucket,
            &index.authorization_mode,
            &authz_scope,
            &planner_snapshot,
            requested_limit.saturating_add(1),
        )
        .await?;
        let selected_ordinals = planner_result
            .ranges
            .iter()
            .filter_map(|range| usize::try_from(range.logical_start).ok())
            .collect::<Vec<_>>();
        let selected_rows = typed_field_segment::read_typed_field_rows_by_ordinals(
            &self.storage,
            &segment_ref,
            selected_ordinals,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut rows = Vec::new();
        for row in &selected_rows {
            let typed_row = TypedIndexRow::from_segment_row(row.clone());
            if !req.path_prefix.trim().is_empty()
                && !typed_row.object_key.starts_with(req.path_prefix.trim())
            {
                continue;
            }
            if !predicates
                .iter()
                .all(|predicate| predicate.matches(&typed_row))
            {
                continue;
            }
            if !boundary_predicates
                .iter()
                .all(|predicate| predicate.matches_row(&typed_row))
            {
                continue;
            }
            if self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &row.object_key,
                    Some(&authz_scope),
                    authz_revision,
                )
                .await?
            {
                rows.push(typed_row);
            }
        }
        record_query_plan_metrics(
            "typed_json",
            &index.authorization_mode,
            candidate_entries.len() as u64,
            planner_result.metrics.boundary_candidate_count,
            planner_result.metrics.authz_candidate_count,
            planner_result.metrics.index_candidate_count,
            rows.len() as u64,
        );

        rows.sort_by(|left, right| compare_typed_rows(left, right, &order));
        if let Some(token) = page_token.as_ref() {
            rows = rows
                .into_iter()
                .filter(|row| {
                    compare_typed_row_to_cursor(
                        row,
                        &token.last_sort_values,
                        &token.last_source_identity,
                        &order,
                    )
                    .is_gt()
                })
                .collect();
        }

        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|row| {
                    IndexPageToken::for_cursor(
                        &binding,
                        row.source_identity.clone(),
                        row.values.clone(),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let index_kind = index_kind_value_from_str(&index.kind)?;
        let hits = rows
            .into_iter()
            .map(|row| IndexQueryHit {
                kind: index_kind,
                score: 1.0,
                object_key: row.object_key,
                object_version_id: row.object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "typed_values": row.values,
                })
                .to_string(),
            })
            .collect();

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: segment_header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "typed_json",
                "score": "constant",
                "source": definition.source_kind,
                "authz_scope": authz_scope.trace_json(),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment_header.source_cursor,
            is_caught_up: segment_header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment_header.source_cursor),
        }))
    }

    pub(super) async fn query_hybrid_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if req.query_text.trim().is_empty() && req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_text or query_vector is required for hybrid indexes",
            ));
        }
        ensure_planner_supported_query_shape("hybrid", &req)?;
        ensure_no_direct_boundary_predicates(&req)?;

        let requested_limit = query_limit(req.limit);
        let internal_limit = internal_candidate_limit_for_request(&req, &index.authorization_mode);
        let filters = QueryFilters::from_request(&req)?;
        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let mut combined = BTreeMap::<[u8; 16], HybridAccum>::new();
        let mut generation = 0;
        let mut text_generation = 0;
        let mut vector_generation = 0;
        let mut index_authz_revision = 0;
        let mut applied_cursors = Vec::new();
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let has_text = !req.query_text.trim().is_empty();
        let has_vector = !req.query_vector.is_empty();

        if has_text {
            let definition = full_text_definition(index)?;
            let query_terms = tokenize_text(&req.query_text, &definition.tokenizer)
                .into_iter()
                .map(|token| token.term.into_bytes())
                .collect::<Vec<_>>();
            let Some(segment) = full_text_segment::read_latest_full_text_segment_terms(
                &self.storage,
                &index_storage_id,
                &query_terms,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            else {
                return Err(Status::failed_precondition("IndexUnavailable"));
            };
            text_generation = segment.header.generation;
            generation = generation.max(segment.header.generation);
            let reported_text_authz_revision = authz_revision.max(segment.header.authz_revision);
            index_authz_revision = index_authz_revision.max(segment.header.authz_revision);
            applied_cursors.push(segment.header.source_cursor);
            let authorized_labels = authz_label_filter_for_index_candidate_set(
                &index.authorization_mode,
                segment.header.authz_revision,
                reported_text_authz_revision,
            )?;
            let search_hits = search_query::query_full_text_segment(
                &segment,
                search_query::FullTextSegmentQuery {
                    query: &req.query_text,
                    tokenizer: &definition.tokenizer,
                    positions_enabled: definition.positions_enabled,
                    phrase: req.phrase,
                    bm25: Bm25Config::default(),
                    authorized_labels,
                    limit: score_index_candidate_limit(
                        internal_limit,
                        segment.postings.len() as u64,
                    ),
                },
            )
            .map_err(full_text_query_status)?;
            let document_table =
                full_text_segment::decode_full_text_document_table(&segment.document_table)
                    .map_err(|e| Status::internal(e.to_string()))?;
            for hit in search_hits {
                let Some(document_ref) = document_table.get(&(hit.document_id, hit.field_id))
                else {
                    continue;
                };
                let entry = combined
                    .entry(hit.object_version_id)
                    .or_insert_with(|| HybridAccum::new(hit.object_version_id));
                if entry.object_key.is_empty() {
                    entry.object_key = document_ref.object_key.clone();
                }
                entry.text_score += hit.score;
                entry.document_id = hit.document_id;
                entry.field_id = u32::from(hit.field_id);
            }
        }

        if has_vector {
            let Some(latest_record) =
                index_coremeta::latest_index_segment_coremeta_record_for_family(
                    &self.storage,
                    &index_storage_id,
                    WriterFamily::Vector.as_str(),
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            else {
                return Err(Status::failed_precondition("IndexUnavailable"));
            };
            let vector_header = vector_segment::read_vector_segment_header(
                &self.storage,
                &latest_record.segment_ref,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            if req.query_vector.len() != usize::from(vector_header.dimension) {
                return Err(Status::invalid_argument("query_vector dimension mismatch"));
            }
            vector_generation = vector_header.generation;
            generation = generation.max(vector_header.generation);
            let reported_vector_authz_revision = authz_revision.max(vector_header.authz_revision);
            index_authz_revision = index_authz_revision.max(vector_header.authz_revision);
            applied_cursors.push(vector_header.source_cursor);
            let metric = VectorMetric::from_name(&vector_header.metric)
                .map_err(|e| Status::internal(e.to_string()))?;
            let authorized_labels = authz_label_filter_for_index_candidate_set(
                &index.authorization_mode,
                vector_header.authz_revision,
                reported_vector_authz_revision,
            )?;
            let (_, search_hits) = vector_segment::query_vector_segment_ranges(
                &self.storage,
                &latest_record.segment_ref,
                &req.query_vector,
                metric,
                authorized_labels,
                score_index_candidate_limit(internal_limit, vector_header.vector_count),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            for hit in search_hits {
                let object_key =
                    Self::object_key_from_vector_source_id(bucket, &hit.source_id_binary)?;
                let entry = combined
                    .entry(hit.object_version_id)
                    .or_insert_with(|| HybridAccum::new(hit.object_version_id));
                if entry.object_key.is_empty() {
                    entry.object_key = object_key;
                }
                entry.vector_score = entry.vector_score.max(hit.score);
                entry.vector_id = hit.vector_id;
                entry.chunk_id = hit.chunk_id;
                entry.source_start = hit.source_start;
                entry.source_len = hit.source_len;
            }
        }

        let reported_authz_revision = authz_revision.max(index_authz_revision);
        let system_authz_revision = self.latest_system_authz_revision_for_query().await?;
        let authz_scope = QueryAuthzScope::for_bucket(
            &self.config,
            claims,
            bucket,
            &index.authorization_mode,
            "read",
            None,
            authz_revision,
            system_authz_revision,
        );

        let (text_weight, vector_weight, freshness_weight) = match (has_text, has_vector) {
            (true, true) => (0.55, 0.35, 0.10),
            (true, false) => (1.0, 0.0, 0.0),
            (false, true) => (0.0, 1.0, 0.0),
            (false, false) => unreachable!("validated above"),
        };
        let predicate_hash = score_based_predicate_hash("hybrid", &req, &authz_scope)?;
        let order_hash = score_order_hash(&authz_scope);
        let boundary_schema_generation_hash =
            self.index_page_token_boundary_hash(claims, bucket).await?;
        let root_generation = applied_cursors.iter().copied().min().unwrap_or(0);
        let input_candidate_count = combined.len() as u64;
        let mut candidates = Vec::new();
        for item in combined.into_values() {
            if item.object_key.is_empty() {
                continue;
            }
            let Some(object_ref) = self
                .query_object_ref_from_metadata(bucket, &item.object_key, item.object_version_id)
                .await?
            else {
                continue;
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            candidates.push(HybridCandidate { item, object_ref });
        }
        score_hybrid_candidates(
            &mut candidates,
            has_text,
            has_vector,
            text_weight,
            vector_weight,
            freshness_weight,
        );
        let planner_snapshot = PlannerCandidateSnapshot::from_hybrid_candidates(
            bucket,
            index,
            generation,
            root_generation,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
            &index_storage_id,
            &candidates,
        )?;
        let planner_result = execute_corestore_query_plan(
            &self.storage,
            claims,
            bucket,
            &index.authorization_mode,
            &authz_scope,
            &planner_snapshot,
            requested_limit.saturating_add(1),
        )
        .await?;
        let selected_object_ids =
            planner_snapshot.selected_object_ids(&planner_result.candidates)?;
        let mut selected_candidates = Vec::new();
        for candidate in candidates {
            if !selected_object_ids.contains(&format!(
                "{}/{}",
                bucket.name, candidate.object_ref.object_key
            )) {
                continue;
            }
            if self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &candidate.object_ref.object_key,
                    Some(&authz_scope),
                    authz_revision,
                )
                .await?
            {
                selected_candidates.push(candidate);
            }
        }
        let mut candidates = selected_candidates;
        record_query_plan_metrics(
            "hybrid",
            &index.authorization_mode,
            input_candidate_count,
            planner_result.metrics.boundary_candidate_count,
            planner_result.metrics.authz_candidate_count,
            planner_result.metrics.index_candidate_count,
            candidates.len() as u64,
        );
        candidates.sort_by(|left, right| {
            right
                .item
                .score
                .partial_cmp(&left.item.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    left.item
                        .object_version_id
                        .cmp(&right.item.object_version_id)
                })
        });
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "hybrid",
            &bucket.name,
            &index.name,
            generation,
            root_generation,
            index.version.max(0) as u64,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for candidate in candidates {
                if score_after_cursor(
                    candidate.item.score,
                    &candidate.object_ref.object_version_id,
                    token,
                )? {
                    filtered.push(candidate);
                }
            }
            candidates = filtered;
        }
        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|candidate| {
                    IndexPageToken::for_cursor(
                        &binding,
                        candidate.object_ref.object_version_id.clone(),
                        score_sort_values(
                            candidate.item.score,
                            &candidate.object_ref.object_version_id,
                        ),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let index_kind = index_kind_value_from_str(&index.kind)?;
        let mut hits = Vec::with_capacity(candidates.len().min(requested_limit));
        for candidate in candidates {
            let item = candidate.item;
            let object_ref = candidate.object_ref;
            hits.push(IndexQueryHit {
                kind: index_kind,
                score: item.score,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
                document_id: item.document_id,
                field_id: item.field_id,
                vector_id: item.vector_id,
                chunk_id: item.chunk_id,
                source_start: item.source_start,
                source_len: item.source_len,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "text_score": item.text_score,
                    "vector_score": item.vector_score,
                    "freshness_score": item.freshness_score,
                    "normalized_text_score": item.normalized_text_score,
                    "normalized_vector_score": item.normalized_vector_score,
                })
                .to_string(),
            });
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: generation,
            authz_revision: reported_authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "hybrid",
                "text_weight": text_weight,
                "vector_weight": vector_weight,
                "freshness_weight": freshness_weight,
                "index_generations": {
                    "full_text": text_generation,
                    "vector": vector_generation,
                    "max": generation
                },
                "authz_scope": authz_scope.trace_json(),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: applied_cursors.iter().copied().min().unwrap_or(0),
            is_caught_up: applied_cursors
                .iter()
                .copied()
                .min()
                .is_some_and(|cursor| cursor >= latest_cursor),
            lag_record_count_hint: latest_cursor
                .saturating_sub(applied_cursors.iter().copied().min().unwrap_or(0)),
        }))
    }

    pub(super) async fn query_vector_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.is_empty() {
            return Err(Status::invalid_argument(
                "query_text is not valid for vector indexes",
            ));
        }
        if req.query_vector.is_empty() {
            return Err(Status::invalid_argument("query_vector is required"));
        }
        ensure_planner_supported_query_shape("vector", &req)?;
        ensure_no_direct_boundary_predicates(&req)?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let signing_key = self.index_page_token_signing_key()?;
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        let segment_record = if let Some(token) = page_token.as_ref() {
            index_coremeta::index_segment_coremeta_record_for_family_generation(
                &self.storage,
                &index_storage_id,
                WriterFamily::Vector.as_str(),
                token.index_generation,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::invalid_argument("PageTokenScopeMismatch"))?
        } else {
            index_coremeta::latest_index_segment_coremeta_record_for_family(
                &self.storage,
                &index_storage_id,
                WriterFamily::Vector.as_str(),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition("IndexUnavailable"))?
        };
        let vector_header =
            vector_segment::read_vector_segment_header(&self.storage, &segment_record.segment_ref)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        if req.query_vector.len() != usize::from(vector_header.dimension) {
            return Err(Status::invalid_argument("query_vector dimension mismatch"));
        }
        let metric = VectorMetric::from_name(&vector_header.metric)
            .map_err(|e| Status::internal(e.to_string()))?;
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let reported_authz_revision = authz_revision.max(vector_header.authz_revision);
        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let authorized_labels = authz_label_filter_for_index_candidate_set(
            &index.authorization_mode,
            vector_header.authz_revision,
            authz_revision,
        )?;
        let system_authz_revision = self.latest_system_authz_revision_for_query().await?;
        let authz_scope = QueryAuthzScope::for_bucket(
            &self.config,
            claims,
            bucket,
            &index.authorization_mode,
            "read",
            None,
            authz_revision,
            system_authz_revision,
        );
        let predicate_hash = score_based_predicate_hash("vector", &req, &authz_scope)?;
        let order_hash = score_order_hash(&authz_scope);
        let boundary_schema_generation_hash =
            self.index_page_token_boundary_hash(claims, bucket).await?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "vector",
            &bucket.name,
            &index.name,
            vector_header.generation,
            vector_header.source_cursor,
            index.version.max(0) as u64,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
        );
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        let (_, search_hits) = vector_segment::query_vector_segment_ranges(
            &self.storage,
            &segment_record.segment_ref,
            &req.query_vector,
            metric,
            authorized_labels,
            score_index_candidate_limit(
                requested_limit.saturating_mul(20).max(requested_limit),
                vector_header.vector_count,
            ),
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let input_candidate_count = search_hits.len() as u64;
        let mut candidates = Vec::with_capacity(search_hits.len());
        for hit in search_hits {
            let object_key = Self::object_key_from_vector_source_id(bucket, &hit.source_id_binary)?;
            candidates.push(IndexQueryHit {
                kind: index_kind,
                score: hit.score,
                object_key,
                object_version_id: uuid::Uuid::from_bytes(hit.object_version_id).to_string(),
                document_id: 0,
                field_id: 0,
                vector_id: hit.vector_id,
                chunk_id: hit.chunk_id,
                source_start: hit.source_start,
                source_len: hit.source_len,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "metric": vector_header.metric,
                    "modality": vector_header.modality,
                })
                .to_string(),
            });
        }
        let planner_snapshot = PlannerCandidateSnapshot::from_index_query_hits(
            bucket,
            index,
            vector_header.generation,
            vector_header.source_cursor,
            authz_scope.revision_fence(),
            &authz_scope,
            predicate_hash.clone(),
            order_hash.clone(),
            boundary_schema_generation_hash.clone(),
            &segment_record.segment_ref,
            &candidates,
        )?;
        let planner_result = execute_corestore_query_plan(
            &self.storage,
            claims,
            bucket,
            &index.authorization_mode,
            &authz_scope,
            &planner_snapshot,
            requested_limit.saturating_add(1),
        )
        .await?;
        let selected_object_ids =
            planner_snapshot.selected_object_ids(&planner_result.candidates)?;
        let mut selected_candidates = Vec::new();
        for hit in candidates {
            if !selected_object_ids.contains(&format!("{}/{}", bucket.name, hit.object_key)) {
                continue;
            }
            if self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &hit.object_key,
                    Some(&authz_scope),
                    authz_revision,
                )
                .await?
            {
                selected_candidates.push(hit);
            }
        }
        let mut candidates = selected_candidates;
        record_query_plan_metrics(
            "vector",
            &index.authorization_mode,
            input_candidate_count,
            planner_result.metrics.boundary_candidate_count,
            planner_result.metrics.authz_candidate_count,
            planner_result.metrics.index_candidate_count,
            candidates.len() as u64,
        );
        candidates.sort_by(|left, right| {
            compare_score_hits(
                left.score,
                &left.object_version_id,
                right.score,
                &right.object_version_id,
            )
        });
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for hit in candidates {
                if score_after_cursor(hit.score, &hit.object_version_id, token)? {
                    filtered.push(hit);
                }
            }
            candidates = filtered;
        }

        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|hit| {
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        score_sort_values(hit.score, &hit.object_version_id),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            hits: candidates,
            index_kind,
            index_generation: vector_header.generation,
            authz_revision: reported_authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "vector",
                "metric": vector_header.metric,
                "max_candidate_multiplier": 20,
                "authz_scope": authz_scope.trace_json(),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: vector_header.source_cursor,
            is_caught_up: vector_header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(vector_header.source_cursor),
        }))
    }

    fn object_key_from_vector_source_id(
        bucket: &crate::persistence::Bucket,
        source_id_binary: &[u8],
    ) -> Result<String, Status> {
        let source = crate::core_store::SourceId::decode_binary(source_id_binary)
            .map_err(|e| Status::internal(format!("Invalid vector SourceId: {e}")))?;
        if source.kind != crate::core_store::SourceKind::ObjectCurrent {
            return Err(Status::internal("Vector SourceId is not an object source"));
        }
        let expected_prefix = format!("{}/{}/", bucket.tenant_id, bucket.name);
        source
            .resource_id
            .strip_prefix(&expected_prefix)
            .filter(|object_key| !object_key.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| Status::internal("Vector SourceId does not match query bucket"))
    }

    async fn query_object_ref_from_metadata(
        &self,
        bucket: &crate::persistence::Bucket,
        object_key: &str,
        object_version_id: [u8; 16],
    ) -> Result<Option<QueryObjectRef>, Status> {
        let version_id = uuid::Uuid::from_bytes(object_version_id);
        let object = crate::metadata_journal::read_object_version(
            &self.storage,
            bucket,
            &[],
            object_key,
            version_id,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .or_else(|| None);
        let object = match object {
            Some(object) => object,
            None => match crate::metadata_journal::read_current_object(
                &self.storage,
                bucket,
                &[],
                object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            {
                Some(object) => object,
                None => return Ok(None),
            },
        };
        Ok(Some(QueryObjectRef {
            object_version_id: object.version_id.to_string(),
            object_key: object.key,
            user_meta: object.user_meta,
            created_at_nanos: object.created_at.timestamp_nanos_opt().unwrap_or(0),
            authz_revision: object.authz_revision,
        }))
    }

    pub(super) async fn query_hit_visible(
        &self,
        claims: &auth::Claims,
        authorization_mode: &str,
        bucket_name: &str,
        object_key: &str,
        authz_scope: Option<&QueryAuthzScope>,
        authz_revision: u64,
    ) -> Result<bool, Status> {
        if validation::is_reserved_internal_key(object_key) {
            return Ok(false);
        }
        match authorization_mode {
            "inherit_object" => {
                if object_key.is_empty() {
                    return Ok(false);
                }
                let system_revision = if let Some(scope) = authz_scope {
                    i64::try_from(scope.system_revision)
                        .map_err(|_| Status::internal("Invalid system authz revision"))?
                } else {
                    crate::authz_journal::latest_authz_revision(
                        &self.storage,
                        crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
                    )
                    .await
                    .map_err(crate::services::authz_status::consistency_status)?
                };
                if let Some(scope) = authz_scope {
                    let revision = i64::try_from(authz_revision)
                        .map_err(|_| Status::internal("Invalid authz revision"))?;
                    if authz_journal::resolve_permission_at_revision(
                        &self.storage,
                        claims.tenant_id,
                        &scope.object_namespace,
                        &format!("{bucket_name}/{object_key}"),
                        &scope.relation,
                        access_control::APP_SUBJECT_KIND,
                        &claims.sub,
                        "",
                        revision,
                    )
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    {
                        return Ok(true);
                    }
                }

                let bucket = self.get_index_bucket(claims.tenant_id, bucket_name).await?;
                Ok(access_control::system_realm_relationship_allows(
                    &self.storage,
                    claims,
                    crate::system_realm::SYSTEM_OBJECT_NAMESPACE,
                    &access_control::object_object_id(&bucket, object_key),
                    "get",
                    Some(system_revision),
                )
                .await
                .map_err(crate::services::authz_status::consistency_status)?
                    || access_control::system_realm_relationship_allows(
                        &self.storage,
                        claims,
                        crate::system_realm::SYSTEM_BUCKET_NAMESPACE,
                        &access_control::bucket_object_id(&bucket),
                        "get_object",
                        Some(system_revision),
                    )
                    .await
                    .map_err(crate::services::authz_status::consistency_status)?)
            }
            "index_only" | "public" => Ok(true),
            _ => Ok(false),
        }
    }
}
