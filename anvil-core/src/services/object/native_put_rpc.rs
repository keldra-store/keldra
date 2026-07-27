use super::rpc::{native_transaction_id, object_write_visibility, write_state_for_transaction};
use super::*;
use crate::mvcc_transaction::{DurabilityLevel, ReadConsistency};

pub(super) struct NativeScratchFile {
    path: std::path::PathBuf,
}

impl NativeScratchFile {
    pub(super) fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }

    pub(super) fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for NativeScratchFile {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to remove native mutation scratch file"
            );
        }
    }
}

pub(crate) async fn execute_native_put(
    state: &AppState,
    claims: auth::Claims,
    metadata: ObjectMetadata,
    data_stream: impl futures_core::Stream<Item = Result<Vec<u8>, Status>> + Unpin + Send + 'static,
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
    let (scratch_path, payload_size, payload_digest) = state
        .storage
        .stream_to_temp_file(data_stream)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
    let scratch = NativeScratchFile::new(scratch_path);
    let target = NativeIdempotencyTarget::new("PutObject", &bucket_name, &object_key)
        .with_parameters(serde_json::json!({
            "payload_hash": format!("sha256:{payload_digest}"),
            "payload_size": payload_size
        }));
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
                super::native_mutation::implicit_native_transaction_key(context, &target)?,
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

    let scratch_file = match tokio::fs::File::open(scratch.path()).await {
        Ok(file) => file,
        Err(error) => {
            return Err(Status::internal(error.to_string()));
        }
    };
    let replay_stream = tokio_util::io::ReaderStream::new(scratch_file).map(
        |result: Result<bytes::Bytes, std::io::Error>| {
            result
                .map(|bytes| bytes.to_vec())
                .map_err(|error| Status::internal(error.to_string()))
        },
    );
    let object_result = state
        .object_manager
        .put_object(
            &claims,
            &bucket_name,
            &object_key,
            replay_stream,
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

}
