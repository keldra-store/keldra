use super::rpc::{native_transaction_id, object_write_visibility, write_state_for_transaction};
use super::*;
use crate::{
    mvcc_transaction::{CertificationResult, DurabilityLevel, ReadConsistency},
    object_shard_manifest::PhysicalObjectShardManifest,
    shard_placement::{DistributedIngest, ShardPlacementPolicy},
    streaming_erasure::ErasureProfile,
};
use futures_util::TryStreamExt;
use tokio_util::io::StreamReader;

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
                implicit_transaction_idempotency_key(context, &bucket_name, &object_key),
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

    let mut data_stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<Vec<u8>, Status>> + Send>,
    > = Box::pin(data_stream);
    let prepared_ingest = {
        let binding = state
            .mvcc
            .open_transactions
            .binding(transaction_id, &transaction_principal)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if binding.cluster_id != state.config.mvcc_cluster_id {
            return Err(Status::failed_precondition(
                "transaction belongs to another cluster",
            ));
        }
        match binding.durability {
            DurabilityLevel::Local => {
                let reader_stream = data_stream
                    .by_ref()
                    .map_ok(std::io::Cursor::new)
                    .map_err(|status| std::io::Error::other(status.to_string()));
                let mut reader = StreamReader::new(reader_stream);
                let ingest = state
                    .mvcc
                    .local_objects
                    .persist(&mut reader)
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;
                state
                    .mvcc
                    .object_evidence
                    .record(&ingest.manifest.object_hash, ingest.evidence)
                    .map_err(|error| Status::internal(error.to_string()))?;
                state
                    .mvcc
                    .open_transactions
                    .add_manifest(
                        transaction_id,
                        &binding.cluster_id,
                        ingest.reference,
                        current_unix_ms()?,
                    )
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                data_stream = Box::pin(futures_util::stream::empty());
                Some(crate::object_manager::PreparedObjectIngest {
                    object_hash: ingest.manifest.object_hash.clone(),
                    object_length: ingest.manifest.object_length,
                    shard_map: serde_json::json!({
                        "schema": "anvil.mvcc.local_object_manifest.v1",
                        "manifest": ingest.manifest,
                    }),
                })
            }
            durability @ (DurabilityLevel::Quorum | DurabilityLevel::Erasure) => {
                let (candidates, tolerated_failure_domains, _) = state
                    .mvcc
                    .live_shard_placement()
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                if candidates.len() < 2 {
                    return Err(Status::failed_precondition(
                        "distributed object durability requires at least two shard targets",
                    ));
                }
                let parity_shards = tolerated_failure_domains.max(1).min(candidates.len() - 1);
                let profile = ErasureProfile {
                    data_shards: candidates.len() - parity_shards,
                    parity_shards,
                    shard_bytes: 256 * 1024,
                };
                let policy = ShardPlacementPolicy {
                    tolerated_failure_domains,
                };
                let object_identity = provisional_object_identity(
                    &binding.cluster_id,
                    transaction_id,
                    &bucket_name,
                    &object_key,
                );
                let plan = policy
                    .plan(object_identity, 1, profile, &candidates)
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                let reader_stream = data_stream
                    .by_ref()
                    .map_ok(std::io::Cursor::new)
                    .map_err(|status| std::io::Error::other(status.to_string()));
                let mut reader = StreamReader::new(reader_stream);
                let ingest = DistributedIngest::encode(
                    &state.mvcc.replication_client,
                    &plan,
                    policy,
                    profile,
                    durability,
                    &mut reader,
                    object_identity,
                    None,
                    1,
                )
                .await
                .map_err(|error| Status::unavailable(error.to_string()))?;
                let manifest = PhysicalObjectShardManifest::from_ingest(
                    &binding.cluster_id,
                    object_identity,
                    1,
                    profile.data_shards,
                    profile.parity_shards,
                    profile.shard_bytes,
                    &ingest,
                )
                .map_err(|error| Status::internal(error.to_string()))?;
                let manifest_reference = manifest
                    .reference()
                    .map_err(|error| Status::internal(error.to_string()))?;
                state
                    .mvcc
                    .object_evidence
                    .record_ingest(&ingest)
                    .map_err(|error| Status::internal(error.to_string()))?;
                state
                    .mvcc
                    .open_transactions
                    .add_manifest(
                        transaction_id,
                        &binding.cluster_id,
                        manifest_reference,
                        current_unix_ms()?,
                    )
                    .map_err(|error| Status::failed_precondition(error.to_string()))?;
                data_stream = Box::pin(futures_util::stream::empty());
                Some(crate::object_manager::PreparedObjectIngest {
                    object_hash: manifest.object_hash.clone(),
                    object_length: manifest.object_length,
                    shard_map: serde_json::json!({
                        "schema": "anvil.mvcc.object_shard_manifest.v1",
                        "manifest": manifest,
                    }),
                })
            }
        }
    };

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
                transaction_id: Some(transaction_id.to_owned()),
                transaction_principal: Some(transaction_principal.clone()),
                storage_class_id: storage_class,
                visibility: write_visibility,
                prepared_ingest,
            },
        )
        .await?;
    if internal_transaction {
        let outcome = state
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
        if let CertificationResult::Aborted { reason } = outcome.certification {
            return Err(Status::aborted(format!(
                "implicit MVCC transaction aborted: {reason:?}"
            )));
        }
    }
    let watch_cursor =
        if requested_transaction_id.is_some() || !write_visibility.requires_watch_visible() {
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
        write_state: write_state_for_transaction(requested_transaction_id),
    };
    complete_native_mutation(state, &attempt, &target, &response).await?;
    Ok(response)
}

fn configured_default_durability(value: &str) -> Result<DurabilityLevel, Status> {
    match value.trim().to_ascii_lowercase().as_str() {
        "local" => Ok(DurabilityLevel::Local),
        "quorum" => Ok(DurabilityLevel::Quorum),
        "erasure" => Ok(DurabilityLevel::Erasure),
        _ => Err(Status::failed_precondition(
            "mvcc_default_durability must be local, quorum, or erasure",
        )),
    }
}

fn implicit_transaction_idempotency_key(
    context: &NativeMutationContext,
    bucket_name: &str,
    object_key: &str,
) -> String {
    let mut hash = blake3::Hasher::new();
    for value in [
        "implicit-put-object",
        context.idempotency_key.as_str(),
        bucket_name,
        object_key,
    ] {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    format!("implicit-put:{}", hash.finalize().to_hex())
}

fn provisional_object_identity(
    cluster_id: &str,
    transaction_id: &str,
    bucket_name: &str,
    object_key: &str,
) -> uuid::Uuid {
    let mut hash = blake3::Hasher::new();
    for value in [cluster_id, transaction_id, bucket_name, object_key] {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    uuid::Uuid::from_bytes(bytes)
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
    fn implicit_transaction_identity_is_stable_for_retries_and_target_scoped() {
        let context = NativeMutationContext {
            idempotency_key: "request-7".into(),
            ..Default::default()
        };
        let first = implicit_transaction_idempotency_key(&context, "bucket", "one");
        assert_eq!(
            first,
            implicit_transaction_idempotency_key(&context, "bucket", "one")
        );
        assert_ne!(
            first,
            implicit_transaction_idempotency_key(&context, "bucket", "two")
        );
    }
}
