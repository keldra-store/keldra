//! Transaction-bound physical payload ingest and compatibility locators.
//!
//! Payload bytes are written directly to their final local representation or
//! erasure-coded shard holders. The returned locator is staged with the
//! caller's MVCC transaction; this module does not publish CoreMeta manifests.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncRead;
use tonic::Status;
use uuid::Uuid;

use crate::{
    core_store::{CoreCompressionDescriptor, CoreObjectEncoding, CoreObjectRef},
    local_object_store::LocalObjectManifest,
    mvcc_bootstrap::MvccSubsystem,
    mvcc_local_durability_upgrade::{LocalDurabilityUpgradeJob, LocalDurabilityUpgradeObject},
    mvcc_shard_repair::{MissingShardTarget, ShardMaintenanceKind, ShardRepairJob},
    mvcc_transaction::DurabilityLevel,
    object_shard_manifest::PhysicalObjectShardManifest,
    shard_placement::{DistributedIngest, ShardPlacementPolicy},
    streaming_erasure::ErasureProfile,
};

pub(crate) const MVCC_PHYSICAL_PAYLOAD_REF_PREFIX: &str = "anvil-mvcc-target:";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvccPhysicalPayloadLocator {
    Local(LocalObjectManifest),
    Shards(PhysicalObjectShardManifest),
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedMvccPhysicalPayload {
    pub object_hash: String,
    pub object_length: u64,
    pub locator: MvccPhysicalPayloadLocator,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PrepareMvccPhysicalPayload<'a> {
    pub transaction_id: &'a str,
    pub transaction_principal: &'a str,
    pub logical_scope: &'a str,
    pub logical_key: &'a str,
    pub prepared_at_unix_ms: u64,
}

impl PreparedMvccPhysicalPayload {
    pub fn shard_map(&self) -> JsonValue {
        encode_shard_map(&self.locator)
    }

    pub fn object_ref(&self) -> Result<CoreObjectRef> {
        core_object_ref_from_locator(&self.locator)
    }
}

/// Streams one payload into the transaction's requested durability
/// representation and stages only MVCC-owned references to it.
///
/// Shards remain provisional until the transaction is certified. An abort can
/// therefore leave unreferenced provisional shards for GC, but cannot publish
/// a legacy CoreMeta object or logical-file manifest.
pub(crate) async fn prepare_mvcc_physical_payload<R>(
    mvcc: &MvccSubsystem,
    reader: &mut R,
    request: PrepareMvccPhysicalPayload<'_>,
) -> std::result::Result<PreparedMvccPhysicalPayload, Status>
where
    R: AsyncRead + Unpin + Send,
{
    let binding = mvcc
        .open_transactions
        .binding(request.transaction_id, request.transaction_principal)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    if binding.cluster_id != mvcc.cluster_id() {
        return Err(Status::failed_precondition(
            "transaction belongs to another cluster",
        ));
    }
    let object_identity = provisional_payload_identity(
        &binding.cluster_id,
        request.transaction_id,
        request.logical_scope,
        request.logical_key,
    );

    let locator = match binding.durability {
        DurabilityLevel::Local => {
            let ingest = mvcc
                .local_objects
                .persist(reader)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            mvcc.object_evidence
                .record(&ingest.manifest.object_hash, ingest.evidence)
                .map_err(|error| Status::internal(error.to_string()))?;
            mvcc.open_transactions
                .add_manifest(
                    request.transaction_id,
                    &binding.cluster_id,
                    ingest.reference,
                    request.prepared_at_unix_ms,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            let upgrade = LocalDurabilityUpgradeJob {
                schema: LocalDurabilityUpgradeJob::SCHEMA.into(),
                cluster_id: binding.cluster_id.clone(),
                transaction_id: request.transaction_id.to_string(),
                commit_version: 0,
                bundle: None,
                target: DurabilityLevel::Erasure,
                objects: vec![LocalDurabilityUpgradeObject {
                    object_identity,
                    local_manifest: ingest.manifest.clone(),
                }],
                requested_at_unix_ms: request.prepared_at_unix_ms,
            };
            mvcc.open_transactions
                .add_job(
                    request.transaction_id,
                    upgrade
                        .canonical_bytes()
                        .map_err(|error| Status::internal(error.to_string()))?,
                    request.prepared_at_unix_ms,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            MvccPhysicalPayloadLocator::Local(ingest.manifest)
        }
        durability @ (DurabilityLevel::Quorum | DurabilityLevel::Erasure) => {
            let (candidates, tolerated_failure_domains, _) = mvcc
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
            let plan = policy
                .plan(object_identity, 1, profile, &candidates)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            let prepared_snapshot_version = mvcc
                .open_transactions
                .handle(request.transaction_id)
                .map_err(|error| Status::failed_precondition(error.to_string()))?
                .snapshot_version;
            let ingest = DistributedIngest::encode(
                &mvcc.replication_client,
                &plan,
                policy,
                profile,
                durability,
                reader,
                request.transaction_id,
                prepared_snapshot_version,
                request.prepared_at_unix_ms,
                true,
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
            if durability == DurabilityLevel::Quorum {
                stage_missing_shard_repair(mvcc, &binding.cluster_id, request, &plan, &manifest)?;
            }
            mvcc.object_evidence
                .record_ingest(&ingest)
                .map_err(|error| Status::internal(error.to_string()))?;
            mvcc.open_transactions
                .add_manifest(
                    request.transaction_id,
                    &binding.cluster_id,
                    manifest
                        .reference()
                        .map_err(|error| Status::internal(error.to_string()))?,
                    request.prepared_at_unix_ms,
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            crate::mvcc_shard_repair::stage_manifest_catalog_entry(
                mvcc,
                request.transaction_id,
                request.transaction_principal,
                &manifest,
                request.prepared_at_unix_ms,
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
            MvccPhysicalPayloadLocator::Shards(manifest)
        }
    };

    let (object_hash, object_length) = match &locator {
        MvccPhysicalPayloadLocator::Local(manifest) => {
            (manifest.object_hash.clone(), manifest.object_length)
        }
        MvccPhysicalPayloadLocator::Shards(manifest) => {
            (manifest.object_hash.clone(), manifest.object_length)
        }
    };
    Ok(PreparedMvccPhysicalPayload {
        object_hash,
        object_length,
        locator,
    })
}

fn stage_missing_shard_repair(
    mvcc: &MvccSubsystem,
    cluster_id: &str,
    request: PrepareMvccPhysicalPayload<'_>,
    plan: &crate::shard_placement::ShardPlacementPlan,
    manifest: &PhysicalObjectShardManifest,
) -> std::result::Result<(), Status> {
    let mut missing = Vec::new();
    for stripe_ordinal in 0..manifest.stripe_count {
        for (shard_ordinal, target) in plan.targets_by_ordinal.iter().enumerate() {
            let shard_ordinal = u16::try_from(shard_ordinal)
                .map_err(|_| Status::internal("shard ordinal exceeds u16"))?;
            if !manifest.placements.iter().any(|placement| {
                placement.stripe_ordinal == stripe_ordinal
                    && placement.shard_ordinal == shard_ordinal
            }) {
                missing.push(MissingShardTarget {
                    stripe_ordinal,
                    shard_ordinal,
                    target: target.clone(),
                });
            }
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    let repair = ShardRepairJob {
        schema: ShardRepairJob::SCHEMA.to_string(),
        cluster_id: cluster_id.to_string(),
        transaction_id: request.transaction_id.to_string(),
        kind: ShardMaintenanceKind::Repair,
        target_logical_identity: format!("cluster/{cluster_id}/object/{}", manifest.object_hash),
        source_manifest: manifest.clone(),
        source_manifest_hash: hex::encode(
            blake3::hash(
                &manifest
                    .canonical_bytes()
                    .map_err(|error| Status::internal(error.to_string()))?,
            )
            .as_bytes(),
        ),
        missing,
        retiring: Vec::new(),
        originating_snapshot_version: mvcc
            .open_transactions
            .handle(request.transaction_id)
            .map_err(|error| Status::failed_precondition(error.to_string()))?
            .snapshot_version,
        requested_at_unix_ms: request.prepared_at_unix_ms,
    };
    mvcc.open_transactions
        .add_job(
            request.transaction_id,
            repair
                .canonical_bytes()
                .map_err(|error| Status::internal(error.to_string()))?,
            request.prepared_at_unix_ms,
        )
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    Ok(())
}

pub(crate) fn encode_shard_map(locator: &MvccPhysicalPayloadLocator) -> JsonValue {
    match locator {
        MvccPhysicalPayloadLocator::Local(manifest) => serde_json::json!({
            "schema": "anvil.mvcc.local_object_manifest.v1",
            "manifest": manifest,
        }),
        MvccPhysicalPayloadLocator::Shards(manifest) => serde_json::json!({
            "schema": "anvil.mvcc.object_shard_manifest.v1",
            "manifest": manifest,
        }),
    }
}

pub(crate) fn decode_shard_map(value: &JsonValue) -> Result<Option<MvccPhysicalPayloadLocator>> {
    match value.get("schema").and_then(JsonValue::as_str) {
        Some("anvil.mvcc.local_object_manifest.v1") => {
            let manifest = serde_json::from_value(
                value
                    .get("manifest")
                    .cloned()
                    .context("MVCC local object manifest is missing")?,
            )?;
            Ok(Some(MvccPhysicalPayloadLocator::Local(manifest)))
        }
        Some("anvil.mvcc.object_shard_manifest.v1") => {
            let manifest = serde_json::from_value(
                value
                    .get("manifest")
                    .cloned()
                    .context("MVCC object shard manifest is missing")?,
            )?;
            Ok(Some(MvccPhysicalPayloadLocator::Shards(manifest)))
        }
        _ => Ok(None),
    }
}

pub(crate) fn core_object_ref_from_locator(
    locator: &MvccPhysicalPayloadLocator,
) -> Result<CoreObjectRef> {
    let (object_hash, object_length) = match locator {
        MvccPhysicalPayloadLocator::Local(manifest) => {
            (&manifest.object_hash, manifest.object_length)
        }
        MvccPhysicalPayloadLocator::Shards(manifest) => {
            (&manifest.object_hash, manifest.object_length)
        }
    };
    let representation = serde_json::to_vec(&encode_shard_map(locator))?;
    Ok(CoreObjectRef {
        hash: object_hash.to_string(),
        logical_size: object_length,
        manifest_ref: format!(
            "{MVCC_PHYSICAL_PAYLOAD_REF_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(representation)
        ),
        // Compatibility schemas still expose CoreObjectRef. The embedded MVCC
        // locator is authoritative; these legacy encoding fields are not used
        // to locate or read the payload.
        encoding: CoreObjectEncoding {
            block_id: String::new(),
            profile_id: "mvcc".to_string(),
            data_shards: 1,
            parity_shards: 0,
            minimum_read_shards: 1,
            minimum_write_ack_shards: 1,
            stripe_size: object_length,
            placement_scope: "cluster".to_string(),
            repair_priority: "normal".to_string(),
            stored_hash: object_hash.to_string(),
            compression: CoreCompressionDescriptor {
                algorithm: "none".to_string(),
                level: 0,
                uncompressed_length: object_length,
                compressed_length: object_length,
                dictionary_id: String::new(),
                descriptor_hash: String::new(),
            },
            encryption: "none".to_string(),
        },
        placements: Vec::new(),
    })
}

pub(crate) fn decode_core_object_ref_locator(
    object_ref: &CoreObjectRef,
) -> Result<Option<MvccPhysicalPayloadLocator>> {
    let Some(encoded) = object_ref
        .manifest_ref
        .strip_prefix(MVCC_PHYSICAL_PAYLOAD_REF_PREFIX)
    else {
        return Ok(None);
    };
    let representation = URL_SAFE_NO_PAD.decode(encoded)?;
    let shard_map: JsonValue = serde_json::from_slice(&representation)?;
    let locator = decode_shard_map(&shard_map)?
        .context("MVCC compatibility reference contains a legacy target")?;
    let (object_hash, object_length) = match &locator {
        MvccPhysicalPayloadLocator::Local(manifest) => {
            (&manifest.object_hash, manifest.object_length)
        }
        MvccPhysicalPayloadLocator::Shards(manifest) => {
            (&manifest.object_hash, manifest.object_length)
        }
    };
    if object_ref.hash.as_str() != object_hash.as_str() || object_ref.logical_size != object_length
    {
        bail!("MVCC object reference does not match its embedded locator");
    }
    Ok(Some(locator))
}

pub(crate) async fn read_mvcc_core_object_ref(
    mvcc: &MvccSubsystem,
    object_ref: &CoreObjectRef,
) -> Result<Option<Vec<u8>>> {
    let Some(locator) = decode_core_object_ref_locator(object_ref)? else {
        return Ok(None);
    };
    let bytes = match locator {
        MvccPhysicalPayloadLocator::Local(manifest) => {
            mvcc.local_objects
                .read_range(&manifest, 0, manifest.object_length)?
        }
        MvccPhysicalPayloadLocator::Shards(manifest) => {
            let output = Arc::new(Mutex::new(Vec::new()));
            let sink = output.clone();
            manifest
                .read_range_chunks(
                    &mvcc.replication_client,
                    0,
                    manifest.object_length,
                    move |chunk| {
                        let sink = sink.clone();
                        async move {
                            sink.lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .extend_from_slice(&chunk);
                            Ok(())
                        }
                    },
                )
                .await?;
            Arc::try_unwrap(output)
                .map_err(|_| anyhow::anyhow!("MVCC payload reader retained output"))?
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    };
    if object_ref.hash != format!("sha256:{}", hex::encode(Sha256::digest(&bytes)))
        || object_ref.logical_size != bytes.len() as u64
    {
        bail!("MVCC object reference verification failed");
    }
    Ok(Some(bytes))
}

fn provisional_payload_identity(
    cluster_id: &str,
    transaction_id: &str,
    logical_scope: &str,
    logical_key: &str,
) -> Uuid {
    let mut hash = blake3::Hasher::new();
    for value in [cluster_id, transaction_id, logical_scope, logical_key] {
        hash.update(&(value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc_transaction::NodeIncarnation;

    fn local_locator() -> MvccPhysicalPayloadLocator {
        MvccPhysicalPayloadLocator::Local(LocalObjectManifest {
            schema_version: 1,
            cluster_id: "cluster-a".to_string(),
            object_hash: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            object_length: 0,
            node: NodeIncarnation {
                node_id: "node-a".to_string(),
                incarnation: 1,
            },
            failure_domain: "rack-a".to_string(),
        })
    }

    #[test]
    fn compatibility_locator_round_trips_without_legacy_placements() {
        let locator = local_locator();
        let object_ref = core_object_ref_from_locator(&locator).unwrap();

        assert!(
            object_ref
                .manifest_ref
                .starts_with(MVCC_PHYSICAL_PAYLOAD_REF_PREFIX)
        );
        assert_eq!(object_ref.encoding.profile_id, "mvcc");
        assert!(object_ref.placements.is_empty());
        assert_eq!(
            decode_core_object_ref_locator(&object_ref).unwrap(),
            Some(locator)
        );
    }

    #[test]
    fn compatibility_locator_rejects_outer_identity_mismatch() {
        let mut object_ref = core_object_ref_from_locator(&local_locator()).unwrap();
        object_ref.logical_size = 1;

        assert!(decode_core_object_ref_locator(&object_ref).is_err());
    }

    #[test]
    fn erasure_locator_round_trips_as_an_mvcc_manifest() {
        let locator = MvccPhysicalPayloadLocator::Shards(PhysicalObjectShardManifest {
            schema_version: crate::object_shard_manifest::OBJECT_SHARD_MANIFEST_SCHEMA,
            cluster_id: "cluster-a".to_string(),
            object_identity: Uuid::from_bytes([7; 16]),
            object_hash: "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_string(),
            object_length: 0,
            encoding_generation: 1,
            data_shards: 1,
            parity_shards: 1,
            shard_bytes: 256 * 1024,
            stripe_count: 0,
            placements: Vec::new(),
        });
        let object_ref = core_object_ref_from_locator(&locator).unwrap();
        let shard_map = encode_shard_map(&locator);

        assert_eq!(
            shard_map.get("schema").and_then(JsonValue::as_str),
            Some("anvil.mvcc.object_shard_manifest.v1")
        );
        assert_eq!(
            decode_core_object_ref_locator(&object_ref).unwrap(),
            Some(locator)
        );
    }

    #[test]
    fn provisional_identity_is_stable_and_logically_scoped() {
        let first = provisional_payload_identity("cluster-a", "tx-a", "registry", "blob-a");
        assert_eq!(
            first,
            provisional_payload_identity("cluster-a", "tx-a", "registry", "blob-a")
        );
        assert_ne!(
            first,
            provisional_payload_identity("cluster-a", "tx-a", "objects", "blob-a")
        );
        assert_ne!(
            first,
            provisional_payload_identity("cluster-a", "tx-b", "registry", "blob-a")
        );
    }

    #[test]
    fn legacy_object_reference_is_not_claimed() {
        let mut object_ref = core_object_ref_from_locator(&local_locator()).unwrap();
        object_ref.manifest_ref = "corestore:legacy".to_string();

        assert_eq!(decode_core_object_ref_locator(&object_ref).unwrap(), None);
    }
}
