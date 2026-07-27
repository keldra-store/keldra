use super::rpc::{native_transaction_id, object_write_visibility, write_state_for_transaction};
use super::*;
use crate::mvcc_transaction::{DurabilityLevel, ReadConsistency};
use sha2::Digest as _;

pub(crate) async fn execute_native_put(
    state: &AppState,
    claims: auth::Claims,
    metadata: ObjectMetadata,
    mut data_stream: impl futures_core::Stream<Item = Result<Vec<u8>, Status>>
    + Unpin
    + Send
    + 'static,
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
    let requested_transaction_id = native_transaction_id(mutation_context.as_ref())?;
    let write_visibility = object_write_visibility(mutation_context.as_ref())?;
    let base_target = NativeIdempotencyTarget::new("PutObject", &bucket_name, &object_key);
    let attempt = lock_native_mutation(
        state,
        mutation_context.as_ref(),
        &base_target,
        &claims,
        AnvilAction::ObjectWrite,
    )
    .await?;
    let transaction_principal = crate::object_manager::transaction_principal_from_claims(&claims);
    let internal_transaction = requested_transaction_id.is_none();
    let internal_transaction_id;
    let transaction_id = if let Some(transaction_id) = requested_transaction_id {
        transaction_id
    } else {
        let context = mutation_context
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
        let handle = state
            .mvcc
            .open_transactions
            .begin(
                state.mvcc.runtime.as_ref(),
                state.config.mvcc_cluster_id.clone(),
                transaction_principal.clone(),
                super::native_mutation::implicit_native_transaction_key(context, &base_target)?,
                std::time::Duration::from_secs(300),
                configured_default_durability(&state.config.mvcc_default_durability)?,
                ReadConsistency::Linearized,
                current_unix_ms()?,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        internal_transaction_id = handle.transaction_id;
        &internal_transaction_id
    };

    state
        .mvcc
        .runtime
        .snapshot(ReadConsistency::Linearized)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut status = state
        .mvcc
        .open_transactions
        .status(transaction_id, &transaction_principal, current_unix_ms()?)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if native_idempotency::generic_result_exists(
        &state.mvcc,
        transaction_id,
        attempt.context(),
    )? {
        let (payload_hash, payload_size) = hash_native_payload(&mut data_stream).await?;
        let target = native_payload_target(base_target, &payload_hash, payload_size);
        return native_idempotency::load_generic_response(
            &state.mvcc,
            transaction_id,
            attempt.context(),
            &target,
        )?
        .ok_or_else(|| Status::data_loss("committed native put is missing its result"));
    }
    if status.state == "committing" {
        state
            .mvcc
            .open_transactions
            .commit(
                state.mvcc.runtime.as_ref(),
                transaction_id,
                &transaction_principal,
                current_unix_ms()?,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        status = state
            .mvcc
            .open_transactions
            .status(transaction_id, &transaction_principal, current_unix_ms()?)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
    }
    if status.state == "committed" {
        let (payload_hash, payload_size) = hash_native_payload(&mut data_stream).await?;
        let target = native_payload_target(base_target, &payload_hash, payload_size);
        return native_idempotency::load_generic_response(
            &state.mvcc,
            transaction_id,
            attempt.context(),
            &target,
        )?
        .or(
            native_idempotency::load_response(&state.mvcc, attempt.context(), &target)
                .await?,
        )
        .ok_or_else(|| Status::data_loss("committed native put is missing its response"));
    }
    if status.state != "open" {
        return Err(Status::aborted(format!(
            "native put transaction is {}",
            status.state
        )));
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
    let object_result = state
        .object_manager
        .put_object(
            &claims,
            &bucket_name,
            &object_key,
            data_stream,
            ObjectWriteOptions {
                content_type,
                user_metadata,
                transaction_id: Some(transaction_id.to_owned()),
                transaction_principal: Some(transaction_principal.clone()),
                storage_class_id: storage_class,
                visibility: write_visibility,
            },
        )
        .await;
    let object = object_result?;
    let target = native_payload_target(
        base_target,
        &object.content_hash,
        u64::try_from(object.size)
            .map_err(|_| Status::internal("object size is negative"))?,
    );
    let response = PutObjectResponse {
        etag: object.etag,
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        payload_hash: object.content_hash,
        record_hash: object.record_hash,
        authz_revision: u64::try_from(object.authz_revision)
            .map_err(|_| Status::internal("Invalid authz revision"))?,
        index_policy_snapshot: object.index_policy_snapshot,
        // Publication is part of applying this transaction. Its external
        // cursor is intentionally not guessed before the atomic commit.
        watch_cursor: 0,
        write_state: write_state_for_transaction(requested_transaction_id),
    };
    if internal_transaction {
        let transaction = super::native_mutation::ImplicitNativeTransaction {
            transaction_id: transaction_id.to_owned(),
            principal: transaction_principal,
        };
        super::native_mutation::stage_implicit_native_response(
            state,
            &attempt,
            &target,
            &response,
            &transaction,
        )
        .await?;
        super::native_mutation::commit_implicit_native_transaction(state, &transaction).await?;
    } else {
        complete_native_mutation(state, &attempt, &target, &response).await?;
    }
    Ok(response)
}

pub(super) fn native_payload_target(
    mut base: NativeIdempotencyTarget,
    payload_hash: &str,
    payload_size: u64,
) -> NativeIdempotencyTarget {
    let mut parameters = match base.parameters {
        serde_json::Value::Object(parameters) => parameters,
        serde_json::Value::Null => serde_json::Map::new(),
        parameters => {
            let mut wrapped = serde_json::Map::new();
            wrapped.insert("request".to_string(), parameters);
            wrapped
        }
    };
    parameters.insert(
        "payload_hash".to_string(),
        serde_json::Value::String(payload_hash.to_string()),
    );
    parameters.insert(
        "payload_size".to_string(),
        serde_json::Value::Number(payload_size.into()),
    );
    base.parameters = serde_json::Value::Object(parameters);
    base
}

pub(super) async fn hash_native_payload(
    stream: &mut (impl futures_core::Stream<Item = Result<Vec<u8>, Status>> + Unpin),
) -> Result<(String, u64), Status> {
    let mut digest = sha2::Sha256::new();
    let mut size = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        digest.update(&chunk);
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| Status::invalid_argument("payload size overflow"))?;
    }
    Ok((format!("sha256:{}", hex::encode(digest.finalize())), size))
}

pub(super) fn configured_default_durability(value: &str) -> Result<DurabilityLevel, Status> {
    match value.trim().to_ascii_lowercase().as_str() {
        "local" => Ok(DurabilityLevel::Local),
        "quorum" => Ok(DurabilityLevel::Quorum),
        "erasure" => Ok(DurabilityLevel::Erasure),
        _ => Err(Status::failed_precondition(
            "mvcc_default_durability must be local, quorum, or erasure",
        )),
    }
}

fn current_unix_ms() -> Result<u64, Status> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| Status::internal("system clock precedes Unix epoch"))?;
    u64::try_from(elapsed.as_millis()).map_err(|_| Status::internal("system time exceeds u64"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_write_durability_is_explicitly_configured() {
        assert_eq!(
            configured_default_durability("local").unwrap(),
            DurabilityLevel::Local
        );
        assert_eq!(
            configured_default_durability("QUORUM").unwrap(),
            DurabilityLevel::Quorum
        );
        assert_eq!(
            configured_default_durability("erasure").unwrap(),
            DurabilityLevel::Erasure
        );
        assert!(configured_default_durability("eventual").is_err());
    }

    #[test]
    fn payload_identity_extends_the_request_target() {
        let base = NativeIdempotencyTarget::new("UploadPart", "bucket", "object")
            .with_parameters(serde_json::json!({"upload_id": "upload", "part_number": 7}));
        let target = native_payload_target(base, "sha256:payload", 42);
        assert_eq!(target.parameters["upload_id"], "upload");
        assert_eq!(target.parameters["part_number"], 7);
        assert_eq!(target.parameters["payload_hash"], "sha256:payload");
        assert_eq!(target.parameters["payload_size"], 42);
    }
}
