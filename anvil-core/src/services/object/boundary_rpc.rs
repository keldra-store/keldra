use super::*;

pub(super) async fn put_boundary_schema_rpc(
    state: &AppState,
    request: Request<PutBoundarySchemaRequest>,
) -> Result<Response<BoundarySchemaResponse>, Status> {
    let request_id = object_request_id(&request);
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    require_bucket_scope(state, &claims, &req.bucket_name, AnvilAction::BucketWrite).await?;
    let bucket =
        bucket_journal::read_current_bucket_mvcc(&state.mvcc, claims.tenant_id, &req.bucket_name)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

    let boundary_bucket_key =
        crate::core_store::boundary_schema_bucket_key(claims.tenant_id, &bucket.name);
    let requested_dimensions = req
        .dimensions
        .clone()
        .into_iter()
        .map(proto_boundary_dimension_to_core)
        .collect::<Result<Vec<_>, _>>()?;
    let requested_transaction_id = match req.transaction_id.as_deref() {
        Some(value) if value.trim().is_empty() => {
            return Err(Status::invalid_argument("transaction_id must not be empty"));
        }
        other => other,
    };
    let transaction_principal = crate::object_manager::transaction_principal_from_claims(&claims);
    let mutation_id = if req.mutation_id.trim().is_empty() {
        format!("boundary-schema:{request_id}")
    } else {
        req.mutation_id.clone()
    };
    let now = current_unix_millis_u64();
    let internal_transaction = requested_transaction_id.is_none();
    let transaction_id = if let Some(transaction_id) = requested_transaction_id {
        state
            .mvcc
            .open_transactions
            .binding(transaction_id, &transaction_principal)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        transaction_id.to_string()
    } else {
        let handle = state
            .mvcc
            .open_transactions
            .begin(
                state.mvcc.runtime.as_ref(),
                state.mvcc.cluster_id().to_string(),
                transaction_principal.clone(),
                format!("boundary-schema:{}:{}", boundary_bucket_key, mutation_id),
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
            .status(&handle.transaction_id, &transaction_principal, now)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if status.state == "committing" {
            let outcome = state
                .mvcc
                .open_transactions
                .commit(
                    state.mvcc.runtime.as_ref(),
                    &handle.transaction_id,
                    &transaction_principal,
                    now,
                )
                .await
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
                outcome.certification
            {
                return Err(Status::aborted(format!(
                    "implicit boundary schema transaction aborted: {reason:?}"
                )));
            }
        } else if status.state == "aborted" {
            return Err(Status::aborted(
                "implicit boundary schema transaction previously aborted",
            ));
        }
        if matches!(status.state, "committed" | "committing") {
            let schema = read_committed_boundary_schema(state, &boundary_bucket_key, None)?
                .ok_or_else(|| {
                    Status::failed_precondition("committed boundary schema outcome is unavailable")
                })?;
            let expected_result_generation = req
                .expected_generation
                .map(|generation| generation.saturating_add(1))
                .unwrap_or(1);
            if schema.generation != expected_result_generation
                || schema.dimensions != requested_dimensions
            {
                return Err(Status::already_exists(
                    "boundary schema mutation_id was already used for different input",
                ));
            }
            let schema_hash = boundary_schema_hash(&schema)
                .map_err(|error| Status::internal(error.to_string()))?;
            return Ok(Response::new(BoundarySchemaResponse {
                request_id,
                schema: Some(core_boundary_schema_to_proto(
                    &schema,
                    &bucket.name,
                    schema_hash,
                )),
            }));
        }
        handle.transaction_id
    };
    let current = read_boundary_schema_in_transaction(
        state,
        &boundary_bucket_key,
        &transaction_id,
        &transaction_principal,
    )?;
    let generation = match (current.as_ref(), req.expected_generation) {
        (None, None) => 1,
        (None, Some(_)) => {
            return Err(Status::failed_precondition(
                "BoundarySchemaGenerationConflict",
            ));
        }
        (Some(current), Some(expected)) if current.generation == expected => {
            current.generation.saturating_add(1)
        }
        (Some(_), Some(_)) => {
            return Err(Status::failed_precondition(
                "BoundarySchemaGenerationConflict",
            ));
        }
        (Some(_), None) => {
            return Err(Status::failed_precondition(
                "expected_generation is required when updating an existing boundary schema",
            ));
        }
    };

    let schema = crate::core_store::CoreBoundarySchema {
        schema: crate::core_store::CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
        bucket: boundary_bucket_key.clone(),
        generation,
        dimensions: requested_dimensions,
        created_at: chrono::Utc::now().to_rfc3339(),
    };
    let schema_hash = stage_boundary_schema_in_transaction(
        state,
        &schema,
        &transaction_id,
        &transaction_principal,
    )?;
    if internal_transaction {
        let outcome = state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &transaction_id,
                &transaction_principal,
                current_unix_millis_u64(),
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit boundary schema transaction aborted: {reason:?}"
            )));
        }
    }

    Ok(Response::new(BoundarySchemaResponse {
        request_id,
        schema: Some(core_boundary_schema_to_proto(
            &schema,
            &bucket.name,
            schema_hash,
        )),
    }))
}

fn read_boundary_schema_in_transaction(
    state: &AppState,
    boundary_bucket_key: &str,
    transaction_id: &str,
    principal: &str,
) -> Result<Option<crate::core_store::CoreBoundarySchema>, Status> {
    let tuple_key =
        crate::core_store::CoreStore::boundary_schema_current_tuple_key(boundary_bucket_key)
            .map_err(|error| Status::internal(error.to_string()))?;
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        crate::core_store::CF_BOUNDARY,
        crate::core_store::TABLE_BOUNDARY_SCHEMA_CURRENT_ROW,
        &tuple_key,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    let Some(bytes) = state
        .mvcc
        .read_transaction_value(transaction_id, principal, &logical_key)
        .map_err(|error| Status::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    crate::core_store::CoreStore::decode_boundary_schema_from_mvcc(&bytes)
        .map(Some)
        .map_err(|error| Status::internal(error.to_string()))
}

fn stage_boundary_schema_in_transaction(
    state: &AppState,
    schema: &crate::core_store::CoreBoundarySchema,
    transaction_id: &str,
    principal: &str,
) -> Result<String, Status> {
    let bytes = crate::core_store::CoreStore::encode_boundary_schema_for_mvcc(schema)
        .map_err(|error| Status::internal(error.to_string()))?;
    let generation_tuple_key = crate::core_store::CoreStore::boundary_schema_generation_tuple_key(
        &schema.bucket,
        schema.generation,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    let current_tuple_key =
        crate::core_store::CoreStore::boundary_schema_current_tuple_key(&schema.bucket)
            .map_err(|error| Status::internal(error.to_string()))?;
    let generation_key = crate::mvcc_product::coremeta_logical_key(
        crate::core_store::CF_BOUNDARY,
        crate::core_store::TABLE_BOUNDARY_SCHEMA_ROW,
        &generation_tuple_key,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    let current_key = crate::mvcc_product::coremeta_logical_key(
        crate::core_store::CF_BOUNDARY,
        crate::core_store::TABLE_BOUNDARY_SCHEMA_CURRENT_ROW,
        &current_tuple_key,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    state
        .mvcc
        .stage_product_mutations(
            transaction_id,
            principal,
            vec![
                crate::mvcc_product::ProductMutation::put(generation_key, bytes.clone()),
                crate::mvcc_product::ProductMutation::put(current_key, bytes.clone()),
            ],
            current_unix_millis_u64(),
        )
        .map_err(|error| Status::internal(error.to_string()))?;
    use sha2::Digest;
    Ok(format!("sha256:{:x}", sha2::Sha256::digest(&bytes)))
}

fn read_committed_boundary_schema(
    state: &AppState,
    boundary_bucket_key: &str,
    generation: Option<u64>,
) -> Result<Option<crate::core_store::CoreBoundarySchema>, Status> {
    let (table_id, tuple_key) = match generation {
        Some(generation) => (
            crate::core_store::TABLE_BOUNDARY_SCHEMA_ROW,
            crate::core_store::CoreStore::boundary_schema_generation_tuple_key(
                boundary_bucket_key,
                generation,
            ),
        ),
        None => (
            crate::core_store::TABLE_BOUNDARY_SCHEMA_CURRENT_ROW,
            crate::core_store::CoreStore::boundary_schema_current_tuple_key(boundary_bucket_key),
        ),
    };
    let tuple_key = tuple_key.map_err(|error| Status::internal(error.to_string()))?;
    let logical_key = crate::mvcc_product::coremeta_logical_key(
        crate::core_store::CF_BOUNDARY,
        table_id,
        &tuple_key,
    )
    .map_err(|error| Status::internal(error.to_string()))?;
    let Some(bytes) = state
        .mvcc
        .read_latest_value(&logical_key)
        .map_err(|error| Status::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    crate::core_store::CoreStore::decode_boundary_schema_from_mvcc(&bytes)
        .map(Some)
        .map_err(|error| Status::internal(error.to_string()))
}

pub(super) async fn get_boundary_schema_rpc(
    state: &AppState,
    request: Request<GetBoundarySchemaRequest>,
) -> Result<Response<BoundarySchemaResponse>, Status> {
    let request_id = object_request_id(&request);
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let _consistency = object_read_consistency(req.consistency.as_ref())?;
    require_bucket_scope(state, &claims, &req.bucket_name, AnvilAction::BucketRead).await?;
    let bucket =
        bucket_journal::read_current_bucket_mvcc(&state.mvcc, claims.tenant_id, &req.bucket_name)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let boundary_bucket_key =
        crate::core_store::boundary_schema_bucket_key(claims.tenant_id, &bucket.name);
    let schema = read_committed_boundary_schema(state, &boundary_bucket_key, req.generation)?
        .ok_or_else(|| Status::not_found("Boundary schema not found"))?;
    let schema_hash =
        boundary_schema_hash(&schema).map_err(|error| Status::internal(error.to_string()))?;

    Ok(Response::new(BoundarySchemaResponse {
        request_id,
        schema: Some(core_boundary_schema_to_proto(
            &schema,
            &bucket.name,
            schema_hash,
        )),
    }))
}

fn object_request_id<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<crate::middleware::AnvilRequestId>()
        .map(|request_id| request_id.0.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string())
}

fn proto_boundary_dimension_to_core(
    value: BoundaryDimension,
) -> Result<crate::core_store::CoreBoundaryDimension, Status> {
    Ok(crate::core_store::CoreBoundaryDimension {
        name: value.name,
        source: proto_boundary_source_to_core(
            value
                .source
                .ok_or_else(|| Status::invalid_argument("boundary dimension source is required"))?,
        )?,
        value_type: value.value_type,
        categories: value.categories,
        required: value.required,
        cardinality: value.cardinality,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity,
        compaction_scope: value.compaction_scope,
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds,
        deprecated: value.deprecated,
    })
}

fn proto_boundary_source_to_core(
    value: BoundarySource,
) -> Result<crate::core_store::CoreBoundarySource, Status> {
    match value.kind.as_str() {
        "user_metadata_json_pointer" => Ok(
            crate::core_store::CoreBoundarySource::UserMetadataJsonPointer {
                pointer: value.value,
            },
        ),
        "system_metadata" | "system_metadata_field" => {
            Ok(crate::core_store::CoreBoundarySource::SystemMetadataField { field: value.value })
        }
        "path_template" => Ok(crate::core_store::CoreBoundarySource::PathTemplate {
            template: value.value,
        }),
        "body_json_pointer" => Ok(crate::core_store::CoreBoundarySource::BodyJsonPointer {
            pointer: value.value,
            max_body_bytes: value.max_body_bytes,
        }),
        "writer_supplied" | "writer_supplied_boundary" => {
            let (writer_family, field) = value.value.split_once(':').ok_or_else(|| {
                Status::invalid_argument(
                    "writer_supplied boundary source value must be writer_family:field",
                )
            })?;
            Ok(
                crate::core_store::CoreBoundarySource::WriterSuppliedBoundary {
                    writer_family: writer_family.to_string(),
                    field: field.to_string(),
                },
            )
        }
        other => Err(Status::invalid_argument(format!(
            "unsupported boundary source kind {other}"
        ))),
    }
}

fn core_boundary_schema_to_proto(
    value: &crate::core_store::CoreBoundarySchema,
    bucket_name: &str,
    schema_hash: String,
) -> BoundarySchemaRecord {
    BoundarySchemaRecord {
        schema: value.schema.clone(),
        bucket_name: bucket_name.to_string(),
        generation: value.generation,
        dimensions: value
            .dimensions
            .iter()
            .map(core_boundary_dimension_to_proto)
            .collect(),
        created_at: value.created_at.clone(),
        schema_hash,
    }
}

fn core_boundary_dimension_to_proto(
    value: &crate::core_store::CoreBoundaryDimension,
) -> BoundaryDimension {
    BoundaryDimension {
        name: value.name.clone(),
        source: Some(core_boundary_source_to_proto(&value.source)),
        value_type: value.value_type.clone(),
        categories: value.categories.clone(),
        required: value.required,
        cardinality: value.cardinality.clone(),
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity.clone(),
        compaction_scope: value.compaction_scope.clone(),
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds.clone(),
        deprecated: value.deprecated,
    }
}

fn core_boundary_source_to_proto(value: &crate::core_store::CoreBoundarySource) -> BoundarySource {
    match value {
        crate::core_store::CoreBoundarySource::UserMetadataJsonPointer { pointer } => {
            BoundarySource {
                kind: "user_metadata_json_pointer".to_string(),
                value: pointer.clone(),
                max_body_bytes: 0,
            }
        }
        crate::core_store::CoreBoundarySource::SystemMetadataField { field } => BoundarySource {
            kind: "system_metadata_field".to_string(),
            value: field.clone(),
            max_body_bytes: 0,
        },
        crate::core_store::CoreBoundarySource::PathTemplate { template } => BoundarySource {
            kind: "path_template".to_string(),
            value: template.clone(),
            max_body_bytes: 0,
        },
        crate::core_store::CoreBoundarySource::BodyJsonPointer {
            pointer,
            max_body_bytes,
        } => BoundarySource {
            kind: "body_json_pointer".to_string(),
            value: pointer.clone(),
            max_body_bytes: *max_body_bytes,
        },
        crate::core_store::CoreBoundarySource::WriterSuppliedBoundary {
            writer_family,
            field,
        } => BoundarySource {
            kind: "writer_supplied_boundary".to_string(),
            value: format!("{writer_family}:{field}"),
            max_body_bytes: 0,
        },
    }
}

fn boundary_schema_hash(value: &crate::core_store::CoreBoundarySchema) -> anyhow::Result<String> {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    struct BoundarySchemaHashProto {
        #[prost(string, tag = "1")]
        schema: String,
        #[prost(string, tag = "2")]
        bucket: String,
        #[prost(uint64, tag = "3")]
        generation: u64,
        #[prost(string, repeated, tag = "4")]
        dimensions: Vec<String>,
        #[prost(string, tag = "5")]
        created_at: String,
    }

    let proto = BoundarySchemaHashProto {
        schema: value.schema.clone(),
        bucket: value.bucket.clone(),
        generation: value.generation,
        dimensions: value
            .dimensions
            .iter()
            .map(|dimension| {
                format!(
                    "{}\0{}\0{}\0{}\0{}",
                    dimension.name,
                    dimension.value_type,
                    dimension.categories.join(","),
                    dimension.required,
                    dimension.deprecated
                )
            })
            .collect(),
        created_at: value.created_at.clone(),
    };
    Ok(crate::core_store::protobuf_sha256_hex(&proto))
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct BoundaryMigrationRow {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    bucket_key: String,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    migration_id: String,
    #[prost(uint64, tag = "6")]
    from_generation: u64,
    #[prost(uint64, tag = "7")]
    to_generation: u64,
    #[prost(string, tag = "8")]
    mode: String,
    #[prost(string, tag = "9")]
    state: String,
    #[prost(uint64, tag = "10")]
    ranges_total: u64,
    #[prost(uint64, tag = "11")]
    ranges_done: u64,
    #[prost(string, tag = "12")]
    checkpoint_ref: String,
    #[prost(string, tag = "13")]
    last_error_code: String,
    #[prost(string, tag = "14")]
    last_error_message: String,
}

pub(super) async fn start_boundary_migration_rpc(
    state: &AppState,
    request: Request<StartBoundaryMigrationRequest>,
) -> Result<Response<WriteResponse>, Status> {
    let request_id = object_request_id(&request);
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    require_bucket_scope(state, &claims, &req.bucket_name, AnvilAction::BucketWrite).await?;
    let bucket =
        bucket_journal::read_current_bucket_mvcc(&state.mvcc, claims.tenant_id, &req.bucket_name)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let boundary_bucket_key =
        crate::core_store::boundary_schema_bucket_key(claims.tenant_id, &bucket.name);
    let current = read_committed_boundary_schema(state, &boundary_bucket_key, None)?
        .ok_or_else(|| Status::failed_precondition("Boundary schema not found"))?;
    if req.from_generation == 0
        || req.to_generation == 0
        || req.from_generation >= req.to_generation
        || req.to_generation > current.generation
    {
        return Err(Status::failed_precondition(
            "BoundaryMigrationRequired generation range is invalid for the current schema",
        ));
    }
    let mode = boundary_migration_mode_name(req.mode)?;
    let mutation_id = req
        .mutation_context
        .as_ref()
        .and_then(|context| {
            if context.idempotency_key.trim().is_empty() {
                None
            } else {
                Some(context.idempotency_key.clone())
            }
        })
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let migration_id = format!(
        "boundary-migration:{}:{}:{}:{}",
        claims.tenant_id, bucket.name, req.from_generation, req.to_generation
    );
    let row = BoundaryMigrationRow {
        common: Some(boundary_migration_row_common(
            &boundary_bucket_key,
            req.to_generation,
            &migration_id,
        )),
        schema: "anvil.boundary_migration.v1".to_string(),
        bucket_key: boundary_bucket_key.clone(),
        bucket_name: bucket.name.clone(),
        migration_id: migration_id.clone(),
        from_generation: req.from_generation,
        to_generation: req.to_generation,
        mode: mode.to_string(),
        state: "queued".to_string(),
        ranges_total: 0,
        ranges_done: 0,
        checkpoint_ref: format!("coremeta://boundary/{boundary_bucket_key}/{migration_id}"),
        last_error_code: String::new(),
        last_error_message: String::new(),
    };
    let requested_transaction_id =
        crate::services::transaction_context::native_context_transaction_id(
            req.mutation_context.as_ref(),
        )?;
    let transaction_principal = crate::object_manager::transaction_principal_from_claims(&claims);
    let internal_transaction = requested_transaction_id.is_none();
    let transaction_id = if let Some(transaction_id) = requested_transaction_id {
        state
            .mvcc
            .open_transactions
            .binding(transaction_id, &transaction_principal)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        transaction_id.to_string()
    } else {
        let now = current_unix_millis_u64();
        let handle = state
            .mvcc
            .open_transactions
            .begin(
                state.mvcc.runtime.as_ref(),
                state.mvcc.cluster_id().to_string(),
                transaction_principal.clone(),
                format!("boundary-migration:{}:{}", claims.tenant_id, mutation_id),
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
            .status(&handle.transaction_id, &transaction_principal, now)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if matches!(status.state, "committed" | "committing") {
            if status.state == "committing" {
                let outcome = state
                    .mvcc
                    .open_transactions
                    .commit(
                        state.mvcc.runtime.as_ref(),
                        &handle.transaction_id,
                        &transaction_principal,
                        now,
                    )
                    .await
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
                    outcome.certification
                {
                    return Err(Status::aborted(format!(
                        "implicit boundary migration transaction aborted: {reason:?}"
                    )));
                }
            }
            let existing = read_boundary_migration_row(state, &boundary_bucket_key, &migration_id)
                .await?
                .filter(|existing| existing == &row)
                .ok_or_else(|| {
                    Status::already_exists(
                        "boundary migration idempotency key was used for different input",
                    )
                })?;
            let _ = existing;
            return Ok(Response::new(WriteResponse {
                request_id,
                mutation_id,
                state: WriteState::Finalised as i32,
                root_generation: None,
                transaction_manifest_ref: None,
                idempotency_outcome: "replayed".to_string(),
                retry_after_hint: None,
                finalisation_error: None,
            }));
        }
        if status.state == "aborted" {
            return Err(Status::aborted(
                "implicit boundary migration transaction previously aborted",
            ));
        }
        handle.transaction_id
    };
    write_boundary_migration_row_in_transaction(
        state,
        &boundary_bucket_key,
        &migration_id,
        &row,
        &transaction_id,
        &transaction_principal,
    )
    .await?;
    if internal_transaction {
        let outcome = state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                &transaction_id,
                &transaction_principal,
                current_unix_millis_u64(),
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if let crate::mvcc_transaction::CertificationResult::Aborted { reason } =
            outcome.certification
        {
            return Err(Status::aborted(format!(
                "implicit boundary migration transaction aborted: {reason:?}"
            )));
        }
    }
    Ok(Response::new(WriteResponse {
        request_id,
        mutation_id,
        state: if internal_transaction {
            WriteState::Finalised as i32
        } else {
            WriteState::Staged as i32
        },
        root_generation: None,
        transaction_manifest_ref: None,
        idempotency_outcome: "accepted".to_string(),
        retry_after_hint: None,
        finalisation_error: None,
    }))
}

async fn write_boundary_migration_row_in_transaction(
    state: &AppState,
    boundary_bucket_key: &str,
    migration_id: &str,
    row: &BoundaryMigrationRow,
    transaction_id: &str,
    principal: &str,
) -> Result<(), Status> {
    let payload = crate::core_store::encode_deterministic_proto(row);
    let tuple_key = boundary_migration_tuple_key(boundary_bucket_key, migration_id)?;
    let logical_key = boundary_migration_logical_key(&tuple_key)?;
    let visible_in_transaction = state
        .mvcc
        .read_transaction_value(transaction_id, principal, &logical_key)
        .map_err(|error| Status::internal(error.to_string()))?;
    if visible_in_transaction.is_some() {
        return Err(Status::already_exists("BoundaryMigrationAlreadyExists"));
    }
    state
        .mvcc
        .stage_product_mutations(
            transaction_id,
            principal,
            vec![crate::mvcc_product::ProductMutation::put(
                logical_key,
                payload,
            )],
            current_unix_millis_u64(),
        )
        .map_err(|error| Status::internal(error.to_string()))
}

pub(super) async fn get_boundary_migration_rpc(
    state: &AppState,
    request: Request<GetBoundaryMigrationRequest>,
) -> Result<Response<BoundaryMigrationStatus>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    require_bucket_scope(state, &claims, &req.bucket_name, AnvilAction::BucketRead).await?;
    let bucket =
        bucket_journal::read_current_bucket_mvcc(&state.mvcc, claims.tenant_id, &req.bucket_name)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
    let boundary_bucket_key =
        crate::core_store::boundary_schema_bucket_key(claims.tenant_id, &bucket.name);
    let row = read_boundary_migration_row(state, &boundary_bucket_key, &req.migration_id)
        .await?
        .ok_or_else(|| Status::not_found("Boundary migration not found"))?;
    Ok(Response::new(boundary_migration_status(row)))
}

fn boundary_migration_row_common(
    boundary_bucket_key: &str,
    root_generation: u64,
    migration_id: &str,
) -> crate::core_store::CoreMetaRowCommonProto {
    crate::core_store::core_meta_committed_row_common(
        boundary_migration_realm_id(boundary_bucket_key),
        crate::core_store::core_meta_root_key_hash(&format!("boundary/{boundary_bucket_key}")),
        root_generation,
        migration_id.to_string(),
        current_unix_nanos_u64(),
    )
}

fn boundary_migration_realm_id(boundary_bucket_key: &str) -> String {
    boundary_bucket_key
        .split_once('/')
        .map(|(tenant, _)| format!("tenant/{tenant}"))
        .unwrap_or_else(|| "system".to_string())
}

fn current_unix_nanos_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn current_unix_millis_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

async fn read_boundary_migration_row(
    state: &AppState,
    boundary_bucket_key: &str,
    migration_id: &str,
) -> Result<Option<BoundaryMigrationRow>, Status> {
    let tuple_key = boundary_migration_tuple_key(boundary_bucket_key, migration_id)?;
    let logical_key = boundary_migration_logical_key(&tuple_key)?;
    let Some(visible) = state
        .mvcc
        .runtime
        .read_latest(&logical_key)
        .map_err(|error| Status::internal(error.to_string()))?
    else {
        return Ok(None);
    };
    decode_boundary_migration_row(&visible.value).map(Some)
}

fn decode_boundary_migration_row(bytes: &[u8]) -> Result<BoundaryMigrationRow, Status> {
    crate::core_store::decode_deterministic_proto::<BoundaryMigrationRow>(
        bytes,
        "boundary migration row",
    )
    .map_err(|error| Status::internal(error.to_string()))
}

fn boundary_migration_tuple_key(
    boundary_bucket_key: &str,
    migration_id: &str,
) -> Result<Vec<u8>, Status> {
    crate::core_store::core_meta_tuple_key(&[
        crate::core_store::CoreMetaTuplePart::Utf8("boundary_migration"),
        crate::core_store::CoreMetaTuplePart::Utf8(boundary_bucket_key),
        crate::core_store::CoreMetaTuplePart::Utf8(migration_id),
    ])
    .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn boundary_migration_logical_key(
    tuple_key: &[u8],
) -> Result<crate::mvcc_transaction::LogicalKey, Status> {
    crate::mvcc_product::coremeta_logical_key(
        crate::core_store::CF_BOUNDARY,
        crate::core_store::TABLE_BOUNDARY_MIGRATION_ROW,
        tuple_key,
    )
    .map_err(|error| Status::internal(error.to_string()))
}

fn boundary_migration_status(row: BoundaryMigrationRow) -> BoundaryMigrationStatus {
    BoundaryMigrationStatus {
        migration_id: row.migration_id,
        from_generation: row.from_generation,
        to_generation: row.to_generation,
        state: row.state,
        ranges_total: row.ranges_total,
        ranges_done: row.ranges_done,
        checkpoint_ref: row.checkpoint_ref,
        last_error: if row.last_error_code.is_empty() {
            None
        } else {
            Some(AnvilError {
                code: row.last_error_code,
                message: row.last_error_message,
            })
        },
    }
}

fn boundary_migration_mode_name(mode: i32) -> Result<&'static str, Status> {
    match mode {
        1 => Ok("reindex_only"),
        2 => Ok("rewrite_on_compaction"),
        3 => Ok("force_rewrite_now"),
        _ => Err(Status::invalid_argument(
            "boundary migration mode is required",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_schema_mvcc_rows_are_distinct_and_round_trip() {
        let schema = crate::core_store::CoreBoundarySchema {
            schema: crate::core_store::CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
            bucket: "1/releases".to_string(),
            generation: 2,
            dimensions: Vec::new(),
            created_at: "2026-07-26T00:00:00Z".to_string(),
        };
        let payload =
            crate::core_store::CoreStore::encode_boundary_schema_for_mvcc(&schema).unwrap();
        assert_eq!(
            crate::core_store::CoreStore::decode_boundary_schema_from_mvcc(&payload).unwrap(),
            schema
        );

        let generation_tuple =
            crate::core_store::CoreStore::boundary_schema_generation_tuple_key(&schema.bucket, 2)
                .unwrap();
        let current_tuple =
            crate::core_store::CoreStore::boundary_schema_current_tuple_key(&schema.bucket)
                .unwrap();
        let generation_key = crate::mvcc_product::coremeta_logical_key(
            crate::core_store::CF_BOUNDARY,
            crate::core_store::TABLE_BOUNDARY_SCHEMA_ROW,
            &generation_tuple,
        )
        .unwrap();
        let current_key = crate::mvcc_product::coremeta_logical_key(
            crate::core_store::CF_BOUNDARY,
            crate::core_store::TABLE_BOUNDARY_SCHEMA_CURRENT_ROW,
            &current_tuple,
        )
        .unwrap();
        assert_ne!(generation_key, current_key);
    }

    #[test]
    fn boundary_migration_rows_are_valid_coremeta_payloads() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = crate::core_store::CoreMetaStore::open(tmp.path()).unwrap();
        let bucket_key = "1/releases";
        let migration_id = "migration-1";
        let row = BoundaryMigrationRow {
            common: Some(boundary_migration_row_common(bucket_key, 2, migration_id)),
            schema: "anvil.boundary_migration.v1".to_string(),
            bucket_key: bucket_key.to_string(),
            bucket_name: "releases".to_string(),
            migration_id: migration_id.to_string(),
            from_generation: 1,
            to_generation: 2,
            mode: "reindex_only".to_string(),
            state: "queued".to_string(),
            ranges_total: 0,
            ranges_done: 0,
            checkpoint_ref: "coremeta://boundary/1/releases/migration-1".to_string(),
            last_error_code: String::new(),
            last_error_message: String::new(),
        };
        let payload = crate::core_store::encode_deterministic_proto(&row);
        let tuple_key = boundary_migration_tuple_key(bucket_key, migration_id).unwrap();

        meta.put(
            crate::core_store::CF_BOUNDARY,
            crate::core_store::TABLE_BOUNDARY_MIGRATION_ROW,
            &tuple_key,
            &payload,
        )
        .unwrap();

        let logical_key = boundary_migration_logical_key(&tuple_key).unwrap();
        assert_eq!(
            logical_key.table_id,
            crate::core_store::TABLE_BOUNDARY_MIGRATION_ROW
        );
        assert!(logical_key.application_key.ends_with(&tuple_key));
    }
}
