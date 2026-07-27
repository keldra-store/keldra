use super::*;
use crate::object_manager;

fn native_transaction_id(context: Option<&NativeMutationContext>) -> Result<Option<&str>, Status> {
    crate::services::transaction_context::native_context_transaction_id(context)
}

fn write_state_for_transaction(transaction_id: Option<&str>) -> i32 {
    if transaction_id.is_some() {
        WriteState::Staged as i32
    } else {
        WriteState::Finalised as i32
    }
}

type TailAppendStreamRpcStream = std::pin::Pin<
    Box<dyn futures_core::Stream<Item = Result<TailAppendStreamResponse, Status>> + Send>,
>;

pub(super) async fn create_append_stream_rpc(
    state: &AppState,
    request: Request<CreateAppendStreamRequest>,
) -> Result<Response<CreateAppendStreamResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_native_mutation_context(
        state,
        &claims,
        &req.bucket_name,
        req.mutation_context.as_ref(),
    )
    .await?;
    let requested_transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
    let target =
        NativeIdempotencyTarget::new("CreateAppendStream", &req.bucket_name, &req.stream_key);
    let (attempt, replay) = begin_native_mutation::<CreateAppendStreamResponse>(
        state,
        req.mutation_context.as_ref(),
        &target,
        &claims,
        AnvilAction::StreamCreate,
    )
    .await?;
    if let Some(response) = replay {
        return Ok(Response::new(response));
    }
    let transaction_principal = object_manager::transaction_principal_from_claims(&claims);
    let implicit_transaction = if requested_transaction_id.is_none() {
        Some(
            begin_implicit_native_transaction(
                state,
                req.mutation_context
                    .as_ref()
                    .expect("validated native mutation context"),
                &target,
                &claims,
            )
            .await?,
        )
    } else {
        None
    };
    let transaction_id = requested_transaction_id.or_else(|| {
        implicit_transaction
            .as_ref()
            .map(|handle| handle.transaction_id.as_str())
    });
    enforce_native_mutation_precondition(
        state,
        &claims,
        &req.bucket_name,
        &req.stream_key,
        req.mutation_context.as_ref(),
        AnvilAction::StreamCreate,
    )
    .await?;
    let result = state
        .object_manager
        .create_append_stream(
            &claims,
            &req.bucket_name,
            &req.stream_key,
            transaction_id,
            transaction_id.map(|_| transaction_principal.as_str()),
        )
        .await?;
    let authz_revision = latest_authz_revision(state, claims.tenant_id).await?;

    let response = CreateAppendStreamResponse {
        stream_id: result.stream_id.to_string(),
        version_id: result.stream_id.to_string(),
        mutation_id: result.receipt.mutation_id.to_string(),
        payload_hash: result.receipt.payload_hash,
        record_hash: result.receipt.record_hash,
        authz_revision,
        watch_cursor: if requested_transaction_id.is_some() {
            0
        } else {
            result.receipt.watch_cursor
        },
        write_state: write_state_for_transaction(requested_transaction_id),
    };
    if let Some(transaction) = implicit_transaction.as_ref() {
        stage_implicit_native_response(state, &attempt, &target, &response, transaction).await?;
        commit_implicit_native_transaction(state, transaction).await?;
    } else {
        complete_native_mutation(state, &attempt, &target, &response).await?;
    }
    Ok(Response::new(response))
}

pub(super) async fn append_stream_record_rpc(
    state: &AppState,
    request: Request<AppendStreamRecordRequest>,
) -> Result<Response<AppendStreamRecordResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_native_mutation_context(
        state,
        &claims,
        &req.bucket_name,
        req.mutation_context.as_ref(),
    )
    .await?;
    let requested_transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
    let target =
        NativeIdempotencyTarget::new("AppendStreamRecord", &req.bucket_name, &req.stream_key)
            .with_parameters(serde_json::json!({
                "stream_id": req.stream_id.clone(),
                "payload_hash": blake3::hash(&req.payload).to_hex().to_string()
            }));
    let (attempt, replay) = begin_native_mutation::<AppendStreamRecordResponse>(
        state,
        req.mutation_context.as_ref(),
        &target,
        &claims,
        AnvilAction::StreamAppend,
    )
    .await?;
    if let Some(response) = replay {
        return Ok(Response::new(response));
    }
    let implicit_transaction = if requested_transaction_id.is_none() {
        Some(
            begin_implicit_native_transaction(
                state,
                req.mutation_context
                    .as_ref()
                    .expect("validated native mutation context"),
                &target,
                &claims,
            )
            .await?,
        )
    } else {
        None
    };
    let transaction_id = requested_transaction_id.or_else(|| {
        implicit_transaction
            .as_ref()
            .map(|transaction| transaction.transaction_id.as_str())
    });
    enforce_native_mutation_precondition(
        state,
        &claims,
        &req.bucket_name,
        &req.stream_key,
        req.mutation_context.as_ref(),
        AnvilAction::StreamAppend,
    )
    .await?;
    enforce_write_precondition(state, &claims, req.precondition.as_ref()).await?;
    let stream_id = uuid::Uuid::parse_str(&req.stream_id)
        .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
    let user_metadata = parse_user_metadata_json(&req.user_metadata_json)?;
    let record = state
        .object_manager
        .append_stream_record(
            &claims,
            &req.bucket_name,
            &req.stream_key,
            stream_id,
            req.payload,
            req.content_type,
            user_metadata,
            transaction_id,
        )
        .await?;
    let authz_revision = latest_authz_revision(state, claims.tenant_id).await?;

    let response = AppendStreamRecordResponse {
        record_sequence: record.record_sequence,
        payload_hash: record.payload_hash,
        payload_size: record.payload_size,
        version_id: record.record_sequence.to_string(),
        mutation_id: record.receipt.mutation_id.to_string(),
        record_hash: record.receipt.record_hash,
        authz_revision,
        watch_cursor: if requested_transaction_id.is_some() {
            0
        } else {
            record.receipt.watch_cursor
        },
        content_type: record.content_type.unwrap_or_default(),
        user_metadata_json: json_object_string(record.user_metadata.as_ref()),
        write_state: write_state_for_transaction(requested_transaction_id),
    };
    if let Some(transaction) = implicit_transaction.as_ref() {
        stage_implicit_native_response(state, &attempt, &target, &response, transaction).await?;
        commit_implicit_native_transaction(state, transaction).await?;
    } else {
        complete_native_mutation(state, &attempt, &target, &response).await?;
    }
    Ok(Response::new(response))
}

pub(super) async fn read_append_stream_rpc(
    state: &AppState,
    request: Request<ReadAppendStreamRequest>,
) -> Result<Response<ReadAppendStreamResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let consistency_proto = effective_read_consistency(req.consistency.as_ref());
    let consistency = object_read_consistency(Some(&consistency_proto))?;
    let limit = if req.limit == 0 {
        100
    } else {
        req.limit.min(1000)
    };
    let token_binding = ObjectPageTokenBinding::for_stream(
        &claims,
        &req.bucket_name,
        &req.stream_key,
        &req.stream_id,
        limit,
        &consistency_proto,
    );
    let token = ObjectPageToken::decode(
        &req.page_token,
        &token_binding,
        state.config.jwt_secret.as_bytes(),
    )?;
    if token.is_some() && req.after_sequence != 0 {
        return Err(Status::invalid_argument("PageTokenScopeMismatch"));
    }
    let after_sequence = token
        .as_ref()
        .map(|token| token.last_sequence)
        .unwrap_or(req.after_sequence);
    let stream_id = uuid::Uuid::parse_str(&req.stream_id)
        .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
    let mut records = state
        .object_manager
        .read_append_stream_records(
            claims,
            &req.bucket_name,
            &req.stream_key,
            stream_id,
            after_sequence,
            limit.saturating_add(1),
            req.include_payload,
            consistency,
        )
        .await?;
    let next_page_token = if records.len() > limit as usize {
        let last_sequence = records
            .get(limit.saturating_sub(1) as usize)
            .map(|record| record.record_sequence)
            .unwrap_or(after_sequence);
        records.truncate(limit as usize);
        ObjectPageToken::for_sequence(&token_binding, last_sequence)
            .encode(state.config.jwt_secret.as_bytes())?
    } else {
        String::new()
    };
    let next_after_sequence = records
        .last()
        .map(|record| record.record_sequence)
        .unwrap_or(after_sequence);
    let records = records.into_iter().map(append_stream_record_info).collect();
    Ok(Response::new(ReadAppendStreamResponse {
        records,
        next_after_sequence,
        is_end: next_page_token.is_empty(),
        next_page_token,
    }))
}

pub(super) async fn tail_append_stream_rpc(
    state: &AppState,
    request: Request<TailAppendStreamRequest>,
) -> Result<Response<TailAppendStreamRpcStream>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    let stream_id = uuid::Uuid::parse_str(&req.stream_id)
        .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
    let (tx, rx) = mpsc::channel(32);
    let state = state.clone();
    let poll_interval =
        std::time::Duration::from_millis(u64::from(req.poll_interval_ms).clamp(100, 30_000));
    tokio::spawn(async move {
        let mut after_sequence = req.from_sequence.saturating_sub(1);
        loop {
            let records = state
                .object_manager
                .read_append_stream_records(
                    claims.clone(),
                    &req.bucket_name,
                    &req.stream_key,
                    stream_id,
                    after_sequence,
                    100,
                    req.include_payload,
                    crate::object_manager::ObjectReadConsistency::Latest,
                )
                .await;
            match records {
                Ok(records) if records.is_empty() => {
                    tokio::time::sleep(poll_interval).await;
                }
                Ok(records) => {
                    for record in records {
                        after_sequence = record.record_sequence;
                        if tx
                            .send(Ok(TailAppendStreamResponse {
                                record: Some(append_stream_record_info(record)),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            }
        }
    });
    Ok(Response::new(
        Box::pin(ReceiverStream::new(rx)) as TailAppendStreamRpcStream
    ))
}

pub(super) async fn seal_append_stream_segment_rpc(
    state: &AppState,
    request: Request<SealAppendStreamSegmentRequest>,
) -> Result<Response<SealAppendStreamSegmentResponse>, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let req = request.into_inner();
    validate_native_mutation_context(
        state,
        &claims,
        &req.bucket_name,
        req.mutation_context.as_ref(),
    )
    .await?;
    let requested_transaction_id = native_transaction_id(req.mutation_context.as_ref())?;
    let target =
        NativeIdempotencyTarget::new("SealAppendStreamSegment", &req.bucket_name, &req.stream_key)
            .with_parameters(serde_json::json!({ "stream_id": req.stream_id.clone() }));
    let (attempt, replay) = begin_native_mutation::<SealAppendStreamSegmentResponse>(
        state,
        req.mutation_context.as_ref(),
        &target,
        &claims,
        AnvilAction::StreamSealSegment,
    )
    .await?;
    if let Some(response) = replay {
        return Ok(Response::new(response));
    }
    let implicit_transaction = if requested_transaction_id.is_none() {
        Some(
            begin_implicit_native_transaction(
                state,
                req.mutation_context
                    .as_ref()
                    .expect("validated native mutation context"),
                &target,
                &claims,
            )
            .await?,
        )
    } else {
        None
    };
    let transaction_id = requested_transaction_id.or_else(|| {
        implicit_transaction
            .as_ref()
            .map(|transaction| transaction.transaction_id.as_str())
    });
    enforce_native_mutation_precondition(
        state,
        &claims,
        &req.bucket_name,
        &req.stream_key,
        req.mutation_context.as_ref(),
        AnvilAction::StreamSealSegment,
    )
    .await?;
    enforce_write_precondition(state, &claims, req.precondition.as_ref()).await?;
    let version_id = req.stream_id.clone();
    let stream_id = uuid::Uuid::parse_str(&req.stream_id)
        .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
    let transaction_principal =
        transaction_id.map(|_| object_manager::transaction_principal_from_claims(&claims));
    let sealed = state
        .object_manager
        .seal_append_stream_segment(
            &claims,
            &req.bucket_name,
            &req.stream_key,
            stream_id,
            transaction_id,
            transaction_principal.as_deref(),
        )
        .await?;
    let authz_revision = latest_authz_revision(state, claims.tenant_id).await?;

    let response = SealAppendStreamSegmentResponse {
        record_count: sealed.record_count,
        segment_hash: sealed.segment_hash.clone(),
        version_id,
        mutation_id: sealed.receipt.mutation_id.to_string(),
        payload_hash: sealed.segment_hash,
        record_hash: sealed.receipt.record_hash,
        authz_revision,
        watch_cursor: if requested_transaction_id.is_some() {
            0
        } else {
            sealed.receipt.watch_cursor
        },
        write_state: write_state_for_transaction(requested_transaction_id),
    };
    if let Some(transaction) = implicit_transaction.as_ref() {
        stage_implicit_native_response(state, &attempt, &target, &response, transaction).await?;
        commit_implicit_native_transaction(state, transaction).await?;
    } else {
        complete_native_mutation(state, &attempt, &target, &response).await?;
    }
    Ok(Response::new(response))
}
