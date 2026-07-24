use super::*;
use crate::tasks::{
    RebalanceShardTaskOutcome, RebalanceShardTaskPayload, RepairedShardPlacement,
    UnresolvedShardPlacement,
};

pub(super) fn rebalance_payload_from_manifest(
    manifest: &CoreObjectManifest,
    encoded_manifest: &[u8],
) -> Result<RebalanceShardTaskPayload> {
    let object_ref = object_ref_from_object_manifest(manifest)?;
    let common = core_meta_row_common_from_payload(encoded_manifest)?;
    let expected_root_key_hash = object_manifest_root_key_hash(&object_ref.hash);
    if common.root_key_hash != expected_root_key_hash
        || common.root_generation == 0
        || common.transaction_id != manifest.mutation_id
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
    {
        bail!("CoreStore shard repair scheduler manifest publication identity is not canonical");
    }
    let payload = RebalanceShardTaskPayload {
        object_hash: object_ref.hash,
        logical_size: object_ref.logical_size,
        manifest_ref: object_ref.manifest_ref,
        block_id: object_ref.encoding.block_id,
        manifest_root_key_hash: common.root_key_hash,
        manifest_root_generation: common.root_generation,
        manifest_transaction_id: common.transaction_id,
        manifest_payload_digest: core_meta_payload_digest(
            TABLE_OBJECT_VERSION_META_ROW,
            encoded_manifest,
        ),
    };
    payload.validate()?;
    Ok(payload)
}

pub(super) fn repair_task_priority(repair_priority: &str) -> Result<i32> {
    match repair_priority {
        "urgent" => Ok(100),
        "high" => Ok(75),
        "normal" => Ok(50),
        "low" => Ok(25),
        other => bail!("CoreStore unsupported shard repair priority {other}"),
    }
}

pub(crate) struct PreparedShardRepair(PreparedShardRepairState);

enum PreparedShardRepairState {
    Complete(RebalanceShardTaskOutcome),
    Publish {
        canonical_manifest: CoreObjectManifest,
        effective_manifest: CoreObjectManifest,
        repair_finding_id: String,
        repaired: Vec<RepairedShard>,
        writer_family: WriterFamily,
        outcome: RebalanceShardTaskOutcome,
    },
}

impl CoreStore {
    pub(crate) async fn prepare_shard_repair_for_task(
        &self,
        payload: &RebalanceShardTaskPayload,
        repair_finding_id: &str,
    ) -> Result<PreparedShardRepair> {
        payload.validate()?;
        require_safe_repair_finding_id(repair_finding_id)?;
        let canonical_manifest = self.repair_task_manifest(payload)?;
        let mut manifest = canonical_manifest.clone();
        self.apply_shard_repair_overlays(&mut manifest)?;
        let writer_family = validate_repair_manifest_identity(&manifest)?;
        let profile = local_erasure_profile(&manifest.encoding.profile_id)?;
        let candidates = self.active_object_peer_placements().await?;
        let probes = self
            .probe_object_placements(&manifest, profile, &candidates)
            .await;
        let missing_indices = probes
            .iter()
            .filter(|probe| probe.state != PlacementProbeState::Present)
            .map(|probe| probe.placement.shard_index)
            .collect::<Vec<_>>();
        if missing_indices.is_empty() {
            return Ok(PreparedShardRepair(PreparedShardRepairState::Complete(
                RebalanceShardTaskOutcome::VerifiedHealthy,
            )));
        }

        let mut shards = vec![None; profile.total_shards()];
        for probe in &probes {
            if let Some(bytes) = &probe.bytes {
                shards[usize::from(probe.placement.shard_index)] = Some(bytes.clone());
            }
        }
        let present = shards.iter().filter(|shard| shard.is_some()).count();
        if present < profile.minimum_read_shards {
            return Err(CoreStoreAvailabilityError::ShardQuorumUnavailable {
                operation: "shard_repair",
                required: profile.minimum_read_shards,
                received: present,
                details: format!(
                    "object {} block {} does not have enough readable shards",
                    manifest.object_hash, manifest.encoding.block_id
                ),
            }
            .into());
        }

        reconstruct_all_shards(&mut shards, profile)?;
        verify_reconstructed_shards(&probes, &shards)?;
        let mut write_outcome = self
            .write_reconstructed_shards(payload, &manifest, profile, &candidates, &probes, &shards)
            .await?;
        write_outcome
            .repaired
            .sort_by_key(|repair| repair.expected.shard_index);
        write_outcome
            .unresolved
            .sort_by_key(|unresolved| unresolved.expected.shard_index);
        let accounted_indices = write_outcome
            .repaired
            .iter()
            .map(|repair| repair.expected.shard_index)
            .chain(
                write_outcome
                    .unresolved
                    .iter()
                    .map(|unresolved| unresolved.expected.shard_index),
            )
            .collect::<BTreeSet<_>>();
        let expected_indices = missing_indices.iter().copied().collect::<BTreeSet<_>>();
        if accounted_indices.len() != write_outcome.repaired.len() + write_outcome.unresolved.len()
            || accounted_indices != expected_indices
        {
            bail!("CoreStore shard repair did not account for every missing placement");
        }
        let repaired_outcome = write_outcome
            .repaired
            .iter()
            .map(|repair| RepairedShardPlacement {
                shard_index: repair.replacement.shard_index,
                replaced_node_id: repair.expected.node_id.clone(),
                replacement_node_id: repair.replacement.node_id.clone(),
                placement_generation: repair.replacement.generation,
            })
            .collect::<Vec<_>>();
        let unresolved = write_outcome
            .unresolved
            .into_iter()
            .map(|unresolved| UnresolvedShardPlacement {
                shard_index: unresolved.expected.shard_index,
                expected_node_id: unresolved.expected.node_id,
                reason: unresolved.reason,
            })
            .collect::<Vec<_>>();
        let outcome = if unresolved.is_empty() {
            RebalanceShardTaskOutcome::repaired(repaired_outcome)?
        } else {
            RebalanceShardTaskOutcome::retryable(repaired_outcome, unresolved)?
        };
        if write_outcome.repaired.is_empty() {
            return Ok(PreparedShardRepair(PreparedShardRepairState::Complete(
                outcome,
            )));
        }
        Ok(PreparedShardRepair(PreparedShardRepairState::Publish {
            canonical_manifest,
            effective_manifest: manifest,
            repair_finding_id: repair_finding_id.to_string(),
            repaired: write_outcome.repaired,
            writer_family,
            outcome,
        }))
    }

    pub(crate) async fn publish_prepared_shard_repair(
        &self,
        prepared: PreparedShardRepair,
        lease_precondition: CoreMutationPrecondition,
    ) -> Result<RebalanceShardTaskOutcome> {
        match prepared.0 {
            PreparedShardRepairState::Complete(outcome) => Ok(outcome),
            PreparedShardRepairState::Publish {
                canonical_manifest,
                effective_manifest,
                repair_finding_id,
                repaired,
                writer_family,
                outcome,
            } => {
                self.publish_shard_repair_overlays(
                    &canonical_manifest,
                    &effective_manifest,
                    &repair_finding_id,
                    &repaired,
                    writer_family,
                    lease_precondition,
                )
                .await?;
                Ok(outcome)
            }
        }
    }

    fn repair_task_manifest(
        &self,
        payload: &RebalanceShardTaskPayload,
    ) -> Result<CoreObjectManifest> {
        let expected_root_key_hash = object_manifest_root_key_hash(&payload.object_hash);
        if payload.manifest_root_key_hash != expected_root_key_hash {
            bail!("CoreStore shard repair task names a non-canonical manifest root");
        }
        let key = meta_tuple_key(&[
            b"object-manifest",
            payload.manifest_ref.as_bytes(),
            payload.block_id.as_bytes(),
        ]);
        let bytes = self
            .read_coremeta_row(CF_OBJECT_VERSIONS, TABLE_OBJECT_VERSION_META_ROW, &key)?
            .ok_or_else(|| {
                anyhow!(
                    "CoreStore shard repair requires manifest root {} generation {} for object {} block {}, but that exact manifest is not locally committed-visible",
                    payload.manifest_root_key_hash,
                    payload.manifest_root_generation,
                    payload.object_hash,
                    payload.block_id
                )
            })?;
        validate_repair_task_manifest_payload(payload, &bytes)?;
        let manifest = decode_object_manifest_record(&bytes)?;
        let object_ref = object_ref_from_object_manifest(&manifest)?;
        if object_ref.hash != payload.object_hash
            || object_ref.logical_size != payload.logical_size
            || object_ref.manifest_ref != payload.manifest_ref
            || object_ref.encoding.block_id != payload.block_id
            || manifest.mutation_id != payload.manifest_transaction_id
        {
            bail!("CoreStore shard repair task identity does not match its canonical manifest");
        }
        Ok(manifest)
    }
}

fn object_manifest_root_key_hash(object_hash: &str) -> String {
    core_meta_root_key_hash(&format!("object-manifest/{object_hash}"))
}

fn validate_repair_task_manifest_payload(
    payload: &RebalanceShardTaskPayload,
    encoded_manifest: &[u8],
) -> Result<()> {
    let common = core_meta_row_common_from_payload(encoded_manifest)?;
    let payload_digest = core_meta_payload_digest(TABLE_OBJECT_VERSION_META_ROW, encoded_manifest);
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.root_key_hash != payload.manifest_root_key_hash
        || common.root_generation != payload.manifest_root_generation
        || common.transaction_id != payload.manifest_transaction_id
        || payload_digest != payload.manifest_payload_digest
    {
        bail!(
            "CoreStore shard repair exact canonical manifest/root generation is not locally committed-visible"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repair_manifest() -> CoreObjectManifest {
        let hash = "a".repeat(64);
        let mut object_ref =
            CoreObjectRef::test_unlocated(format!("sha256:{hash}"), 42, encode_manifest_ref(&hash));
        object_ref.encoding.block_id = "block-a".to_string();
        CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: "mesh-a".to_string(),
            region_id: "r1".to_string(),
            object_hash: object_ref.hash,
            logical_size: object_ref.logical_size,
            logical_file_id: "lf_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_string(),
            logical_offset: 0,
            writer_family: WriterFamily::Stream.as_str().to_string(),
            encryption_algorithm: "none".to_string(),
            boundary_values: Vec::new(),
            encoding: object_ref.encoding,
            placements: Vec::new(),
            created_at: "2026-07-20T00:00:00+00:00".to_string(),
            mutation_id: "manifest-mutation-a".to_string(),
        }
    }

    #[test]
    fn repair_priority_is_bounded_and_explicit() {
        assert_eq!(repair_task_priority("urgent").unwrap(), 100);
        assert_eq!(repair_task_priority("high").unwrap(), 75);
        assert_eq!(repair_task_priority("normal").unwrap(), 50);
        assert_eq!(repair_task_priority("low").unwrap(), 25);
        assert!(repair_task_priority("background").is_err());
    }

    #[test]
    fn repair_payload_pins_the_exact_committed_manifest_generation() {
        let manifest = repair_manifest();
        let encoded = encode_object_manifest_record(&manifest).unwrap();
        let payload = rebalance_payload_from_manifest(&manifest, &encoded).unwrap();

        assert_eq!(payload.manifest_root_generation, 1);
        assert_eq!(payload.manifest_transaction_id, manifest.mutation_id);
        assert_eq!(
            payload.manifest_root_key_hash,
            object_manifest_root_key_hash(&manifest.object_hash)
        );
        validate_repair_task_manifest_payload(&payload, &encoded).unwrap();

        let mut stale = payload;
        stale.manifest_root_generation += 1;
        assert!(validate_repair_task_manifest_payload(&stale, &encoded).is_err());
    }
}
