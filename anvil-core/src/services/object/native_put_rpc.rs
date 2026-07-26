use super::rpc::{native_transaction_id, object_write_visibility, write_state_for_transaction};
use super::*;
use crate::{
    mvcc_transaction::DurabilityLevel,
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

    let mut data_stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<Vec<u8>, Status>> + Send>,
    > = Box::pin(data_stream);
    let prepared_ingest = if let Some(transaction_id) = transaction_id {
        let binding = state
            .mvcc
            .open_transactions
            .binding(transaction_id, &claims.sub)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if binding.cluster_id != state.config.mvcc_cluster_id {
            return Err(Status::failed_precondition(
                "transaction belongs to another cluster",
            ));
        }
        match binding.durability {
            DurabilityLevel::Local => None,
            durability @ (DurabilityLevel::Quorum | DurabilityLevel::Erasure) => {
                let candidates = &state.mvcc.shard_candidates;
                if candidates.len() < 2 {
                    return Err(Status::failed_precondition(
                        "distributed object durability requires at least two shard targets",
                    ));
                }
                let parity_shards = state
                    .config
                    .mvcc_tolerated_failure_domains
                    .max(1)
                    .min(candidates.len() - 1);
                let profile = ErasureProfile {
                    data_shards: candidates.len() - parity_shards,
                    parity_shards,
                    shard_bytes: 256 * 1024,
                };
                let policy = ShardPlacementPolicy {
                    tolerated_failure_domains: state.config.mvcc_tolerated_failure_domains,
                };
                let object_identity = provisional_object_identity(
                    &binding.cluster_id,
                    transaction_id,
                    &bucket_name,
                    &object_key,
                );
                let plan = policy
                    .plan(object_identity, 1, profile, candidates)
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
    } else {
        None
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
                transaction_id: transaction_id.map(ToOwned::to_owned),
                transaction_principal: transaction_id
                    .map(|_| crate::object_manager::transaction_principal_from_claims(&claims)),
                storage_class_id: storage_class,
                visibility: write_visibility,
                prepared_ingest,
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
