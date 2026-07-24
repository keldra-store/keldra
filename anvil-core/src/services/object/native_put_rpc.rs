use super::rpc::{native_transaction_id, object_write_visibility, write_state_for_transaction};
use super::*;

pub(crate) async fn execute_native_put(
    state: &AppState,
    claims: auth::Claims,
    metadata: ObjectMetadata,
    data_stream: impl futures_core::Stream<Item = Result<Vec<u8>, Status>> + Unpin,
) -> Result<PutObjectResponse, Status> {
    let ObjectMetadata {
        bucket_name,
        object_key,
        mutation_context,
        content_type,
        user_metadata_json,
        storage_class,
    } = metadata;
    let user_metadata = parse_user_metadata_json(&user_metadata_json)?;
    validate_native_mutation_context(state, &claims, &bucket_name, mutation_context.as_ref())
        .await?;
    let transaction_id = native_transaction_id(mutation_context.as_ref())?;
    let write_visibility = object_write_visibility(mutation_context.as_ref())?;
    let target = NativeIdempotencyTarget::new("PutObject", &bucket_name, &object_key);
    let (attempt, replay) = begin_native_mutation::<PutObjectResponse>(
        state,
        mutation_context.as_ref(),
        &target,
        &claims,
        AnvilAction::ObjectWrite,
    )
    .await?;
    if let Some(response) = replay {
        return Ok(response);
    }
    enforce_native_mutation_precondition(
        state,
        &claims,
        &bucket_name,
        &object_key,
        mutation_context.as_ref(),
        AnvilAction::ObjectWrite,
    )
    .await?;

    let object = state
        .object_manager
        .put_object(
            &claims,
            &bucket_name,
            &object_key,
            data_stream,
            ObjectWriteOptions {
                content_type,
                user_metadata,
                transaction_id: transaction_id.map(ToOwned::to_owned),
                transaction_principal: transaction_id
                    .map(|_| crate::object_manager::transaction_principal_from_claims(&claims)),
                storage_class_id: storage_class,
                visibility: write_visibility,
            },
        )
        .await?;
    let watch_cursor = if transaction_id.is_some() || !write_visibility.requires_watch_visible() {
        0
    } else {
        object_watch_cursor(state, &object).await?
    };
    let response = PutObjectResponse {
        etag: object.etag,
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        payload_hash: object.content_hash,
        record_hash: object.record_hash,
        authz_revision: u64::try_from(object.authz_revision)
            .map_err(|_| Status::internal("Invalid authz revision"))?,
        index_policy_snapshot: object.index_policy_snapshot,
        watch_cursor,
        write_state: write_state_for_transaction(transaction_id),
    };
    complete_native_mutation(state, &attempt, &target, &response).await?;
    Ok(response)
}

pub(super) fn native_put_data_chunk(
    chunk_result: Result<PutObjectRequest, Status>,
) -> Result<Vec<u8>, Status> {
    match chunk_result? {
        PutObjectRequest {
            data: Some(put_object_request::Data::Chunk(bytes)),
        } => Ok(bytes),
        _ => Err(Status::invalid_argument(
            "PutObject metadata may appear only in the first chunk",
        )),
    }
}
