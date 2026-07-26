use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::watch;

use crate::object_materialisation::ObjectMaterialisationJob;
use crate::{
    core_store::CoreBoundarySchema,
    local_object_store::LocalObjectManifest,
    object_manager::extract_object_boundary_values,
    object_shard_manifest::PhysicalObjectShardManifest,
    persistence::{Bucket, IndexDefinition, Object},
};
use crate::{
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{CertificationResult, DurabilityLevel, ReadConsistency},
    object_materialisation::{ObjectMaterialisationResult, ObjectMaterialisationState},
};

#[async_trait]
pub trait ObjectMaterialisationExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        job_id: &str,
        job: &ObjectMaterialisationJob,
        assignment: &crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Result<()>;
}

pub struct ObjectMaterialisationRunner<E> {
    mvcc: Arc<MvccSubsystem>,
    executor: Arc<E>,
    worker_id: String,
    lease_ms: u64,
    idle: Duration,
}

impl<E: ObjectMaterialisationExecutor> ObjectMaterialisationRunner<E> {
    pub fn new(mvcc: Arc<MvccSubsystem>, executor: Arc<E>, worker_id: String) -> Result<Self> {
        anyhow::ensure!(!worker_id.trim().is_empty(), "worker ID is required");
        Ok(Self {
            mvcc,
            executor,
            worker_id,
            lease_ms: 30_000,
            idle: Duration::from_millis(250),
        })
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.run_once(now_unix_ms()).await {
                tracing::warn!(%error, worker_id = %self.worker_id, "object materialisation attempt failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.idle) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    pub async fn run_once(&self, now_unix_ms: u64) -> Result<bool> {
        let Some((job_id, record)) = self
            .mvcc
            .runtime
            .local_store()
            .claim_object_materialisation_authorized(
                &self.worker_id,
                now_unix_ms,
                self.lease_ms,
                |record| {
                    self.mvcc
                        .claim_assignment(
                            "object-materialisation",
                            &record.job.target_logical_identity,
                        )
                        .ok()
                        .flatten()
                        .map(|guard| guard.lease_owner(&self.worker_id))
                },
            )?
        else {
            return Ok(false);
        };
        let guard = self
            .mvcc
            .claim_assignment(
                "object-materialisation",
                &record.job.target_logical_identity,
            )?
            .context("materialisation assignment changed after claim")?;
        let lease_owner = guard.lease_owner(&self.worker_id);
        if record.lease_owner.as_deref() != Some(&lease_owner) {
            anyhow::bail!("materialisation lease is not bound to current assignment");
        }
        let execution_timeout = Duration::from_millis(self.lease_ms.saturating_sub(1_000));
        let execution = tokio::time::timeout(
            execution_timeout,
            self.executor.execute(&job_id, &record.job, &guard),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "materialisation execution exceeded {} ms lease-safe timeout",
                execution_timeout.as_millis()
            )
        })
        .and_then(std::convert::identity);
        match execution {
            Ok(()) => {
                self.mvcc.validate_assignment(&guard)?;
                self.mvcc
                    .runtime
                    .local_store()
                    .complete_object_materialisation(&job_id, &lease_owner)?
            }
            Err(error) => {
                let shift = record.attempts.saturating_sub(1).min(10);
                let delay = 250_u64.saturating_mul(1_u64 << shift);
                self.mvcc
                    .runtime
                    .local_store()
                    .retry_object_materialisation(
                        &job_id,
                        &lease_owner,
                        now_unix_ms.saturating_add(delay),
                        &error.to_string(),
                    )?;
                return Err(error);
            }
        }
        Ok(true)
    }
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub struct MvccMaterialisationPublisher {
    mvcc: Arc<MvccSubsystem>,
}

pub fn object_materialisation_outbox_partition(cluster_id: &str) -> Result<(u64, String)> {
    anyhow::ensure!(!cluster_id.trim().is_empty(), "cluster ID is required");
    let stream_partition = format!("mvcc/{cluster_id}/object-materialisation");
    Ok((
        crate::mvcc_outbox::stream_partition_id(&stream_partition)?,
        stream_partition,
    ))
}

impl MvccMaterialisationPublisher {
    pub fn new(mvcc: Arc<MvccSubsystem>) -> Self {
        Self { mvcc }
    }

    pub async fn publish(&self, mut result: ObjectMaterialisationResult) -> Result<()> {
        anyhow::ensure!(
            result.cluster_id == self.mvcc.cluster_id(),
            "materialisation result belongs to another cluster"
        );
        result.state = ObjectMaterialisationState::Complete;
        let result_key = result.result_key()?;
        let status_key = result.status_key()?;
        if let Some(existing) = self.mvcc.runtime.local_store().read_latest(&status_key)?
            && existing.value == result.canonical_bytes()?
        {
            return Ok(());
        }
        let principal = "system/object-materialisation";
        let now = now_unix_ms();
        let handle = self
            .mvcc
            .open_transactions
            .begin(
                self.mvcc.runtime.as_ref(),
                result.cluster_id.clone(),
                principal,
                format!("object-materialisation:{}", result.job_id),
                Duration::from_secs(300),
                DurabilityLevel::Quorum,
                ReadConsistency::Linearized,
                now,
            )
            .await?;
        let bytes = result.canonical_bytes()?;
        self.mvcc.stage_product_mutations(
            &handle.transaction_id,
            principal,
            vec![
                ProductMutation::put(result_key, bytes.clone()),
                ProductMutation::put(status_key, bytes),
            ],
            now,
        )?;
        let (outbox_partition_id, stream_partition) =
            object_materialisation_outbox_partition(&result.cluster_id)?;
        let event_payload = serde_json::to_vec(&serde_json::json!({
            "schema": "anvil.mvcc.object-index-materialisation.v1",
            "cluster_id": result.cluster_id,
            "target_logical_identity": result.target_logical_identity,
            "job_id": result.job_id,
            "index_marker": result.index_marker,
        }))?;
        self.mvcc.open_transactions.add_stream_event(
            &handle.transaction_id,
            crate::mvcc_outbox::StreamOutboxEvent::new(
                outbox_partition_id,
                format!("mvcc/{}/events", result.cluster_id),
                stream_partition,
                "object.index-materialised",
                event_payload,
            )?,
            now,
        )?;
        let outcome = self
            .mvcc
            .open_transactions
            .commit(
                self.mvcc.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                now_unix_ms(),
            )
            .await?;
        anyhow::ensure!(
            matches!(outcome.certification, CertificationResult::Committed { .. }),
            "materialisation result transaction conflicted"
        );
        Ok(())
    }
}

pub struct MvccObjectMaterialisationExecutor {
    mvcc: Arc<MvccSubsystem>,
    publisher: MvccMaterialisationPublisher,
}

impl MvccObjectMaterialisationExecutor {
    pub fn new(mvcc: Arc<MvccSubsystem>) -> Self {
        Self {
            publisher: MvccMaterialisationPublisher::new(mvcc.clone()),
            mvcc,
        }
    }

    async fn payload(&self, job: &ObjectMaterialisationJob) -> Result<Vec<u8>> {
        let schema = job
            .representation
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let manifest = job
            .representation
            .get("manifest")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("materialisation representation has no manifest"))?;
        match schema {
            "anvil.mvcc.local_object_manifest.v1" => {
                let manifest: LocalObjectManifest = serde_json::from_value(manifest)?;
                anyhow::ensure!(
                    manifest.cluster_id == job.cluster_id,
                    "local representation belongs to another cluster"
                );
                self.mvcc
                    .local_objects
                    .read_range(&manifest, 0, manifest.object_length)
            }
            "anvil.mvcc.object_shard_manifest.v1" => {
                let manifest: PhysicalObjectShardManifest = serde_json::from_value(manifest)?;
                anyhow::ensure!(
                    manifest.cluster_id == job.cluster_id,
                    "shard representation belongs to another cluster"
                );
                let bytes = Arc::new(std::sync::Mutex::new(Vec::new()));
                manifest
                    .read_range_chunks(&self.mvcc.replication_client, 0, manifest.object_length, {
                        let bytes = bytes.clone();
                        move |chunk| {
                            let bytes = bytes.clone();
                            async move {
                                bytes.lock().unwrap().extend_from_slice(&chunk);
                                Ok(())
                            }
                        }
                    })
                    .await?;
                Ok(Arc::try_unwrap(bytes).unwrap().into_inner().unwrap())
            }
            _ => anyhow::bail!("unsupported MVCC materialisation representation"),
        }
    }
}

#[async_trait]
impl ObjectMaterialisationExecutor for MvccObjectMaterialisationExecutor {
    async fn execute(
        &self,
        job_id: &str,
        job: &ObjectMaterialisationJob,
        assignment: &crate::mvcc_worker_authority::AssignmentGuard,
    ) -> Result<()> {
        anyhow::ensure!(
            job.cluster_id == self.mvcc.cluster_id(),
            "materialisation job belongs to another cluster"
        );
        anyhow::ensure!(
            job.job_id()? == job_id,
            "materialisation job identity mismatch"
        );
        let payload = if job.requested_operations.extract_boundaries
            || job.requested_operations.maintain_indexes
        {
            tracing::debug!(job_id, "reading frozen object materialisation payload");
            self.payload(job).await?
        } else {
            Vec::new()
        };
        let boundaries = match &job.boundary_schema {
            Some(schema) if job.requested_operations.extract_boundaries => {
                let schema: CoreBoundarySchema = serde_json::from_value(schema.clone())?;
                extract_object_boundary_values(
                    &schema,
                    job.tenant_id,
                    &job.bucket_name,
                    &job.object_key,
                    job.content_type.as_deref(),
                    Some(&job.user_metadata),
                    job.payload_length,
                    &payload,
                )?
            }
            _ => Vec::new(),
        };
        let mut index_outcomes = Vec::new();
        if job.requested_operations.maintain_indexes {
            for definition in &job.frozen_index_definitions {
                anyhow::ensure!(
                    matches!(
                        definition.kind.as_str(),
                        "typed_json" | "full_text" | "vector"
                    ),
                    "MVCC materialisation for index kind `{}` is not available",
                    definition.kind
                );
            }
            let object: Object = serde_json::from_value(job.frozen_object.clone())?;
            let bucket = Bucket {
                id: job.bucket_id,
                tenant_id: job.tenant_id,
                name: job.bucket_name.clone(),
                region: String::new(),
                created_at: object.created_at,
                is_public_read: false,
            };
            for frozen in &job.frozen_index_definitions {
                self.mvcc.validate_assignment(assignment)?;
                tracing::debug!(
                    job_id,
                    index_id = frozen.id,
                    index_kind = %frozen.kind,
                    "building frozen object index"
                );
                let index = IndexDefinition {
                    id: frozen.id,
                    tenant_id: job.tenant_id,
                    bucket_id: job.bucket_id,
                    name: frozen.name.clone(),
                    kind: frozen.kind.clone(),
                    selector: frozen.selector.clone(),
                    extractor: frozen.extractor.clone(),
                    authorization_mode: frozen.authorization_mode.clone(),
                    build_policy: frozen.build_policy.clone(),
                    enabled: true,
                    version: frozen.version,
                    created_at: object.created_at,
                    updated_at: object.created_at,
                };
                let source = crate::index_builder::FrozenObjectIndexSource {
                    object: object.clone(),
                    payload: payload.clone(),
                    boundary_values: boundaries.clone(),
                    source_manifest_hash: job.source_manifest_hash.clone(),
                };
                let authority = crate::index_builder::IndexBuildAuthority::DirectRepair(
                    crate::index_builder::DirectRepairIndexBuildAuthority::new(&self.mvcc),
                );
                let outcome = match frozen.kind.as_str() {
                    "typed_json" => {
                        crate::index_builder::build_frozen_typed_json_index(
                            &self.mvcc.materialisation_storage,
                            &bucket,
                            &index,
                            self.mvcc.materialisation_signing_key.as_ref(),
                            u128::from(job.originating_snapshot_version),
                            &self.mvcc.local_node.node_id,
                            authority,
                            source,
                        )
                        .await?
                    }
                    "full_text" => {
                        crate::index_builder::build_frozen_full_text_index(
                            &self.mvcc.materialisation_storage,
                            &bucket,
                            &index,
                            self.mvcc.materialisation_signing_key.as_ref(),
                            u128::from(job.originating_snapshot_version),
                            &self.mvcc.local_node.node_id,
                            authority,
                            source,
                        )
                        .await?
                    }
                    "vector" => {
                        crate::index_builder::build_frozen_vector_index(
                            &self.mvcc.materialisation_storage,
                            &bucket,
                            &index,
                            self.mvcc.materialisation_signing_key.as_ref(),
                            u128::from(job.originating_snapshot_version),
                            &self.mvcc.local_node.node_id,
                            &self.mvcc.materialisation_embedding_providers,
                            authority,
                            source,
                        )
                        .await?
                    }
                    kind => anyhow::bail!(
                        "MVCC materialisation for index kind `{kind}` is not available"
                    ),
                };
                index_outcomes.push(serde_json::json!({
                    "index_id": frozen.id,
                    "index_version": frozen.version,
                    "kind": frozen.kind,
                    "generation": outcome.generation,
                    "segment_hashes": outcome.segment_hashes,
                }));
            }
        }
        self.mvcc.validate_assignment(assignment)?;
        self.publisher
            // Publishing is the final transactionally visible state transition.
            .publish(ObjectMaterialisationResult {
                schema: ObjectMaterialisationResult::SCHEMA.into(),
                cluster_id: job.cluster_id.clone(),
                target_logical_identity: job.target_logical_identity.clone(),
                job_id: job_id.to_string(),
                state: ObjectMaterialisationState::Complete,
                boundary_schema_hash: job.boundary_schema_hash.clone(),
                derived_boundaries: serde_json::to_value(boundaries)?,
                index_marker: serde_json::json!({
                    "requested": job.requested_operations.maintain_indexes,
                    "policy_snapshot": job.index_policy_snapshot,
                    "authz_revision": job.authz_revision,
                    "outcomes": index_outcomes,
                }),
                updated_at_unix_ms: now_unix_ms(),
            })
            .await
    }
}
