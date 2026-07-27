#[cfg(test)]
use super::local_stream_control::control_record_proto::encode_object_manifest_record;
use super::*;
use crate::anvil_api::{
    ExchangeInventoryRequest, anti_entropy_internal_client::AntiEntropyInternalClient,
};
use crate::formats::hash32;
use crate::formats::writer::WriterFamily;
use crate::tasks::ShardRepairRetryReason;
use futures_util::{StreamExt, stream::FuturesUnordered};
use prost::Message;
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataValue;

#[path = "local_shard_recovery/repair_identity.rs"]
mod repair_identity;

use repair_identity::physical_shard_repair_operation_id;

const OBJECT_SHARD_REPAIR_SCHEMA: &str = "anvil.mvcc.object-shard-repair.v2";
const MVCC_OBJECT_SHARD_REPAIR_TABLE_ID: u16 = 0x8109;
const SHARD_INVENTORY_SCHEMA: &str = "anvil.core.shard_inventory.v1";
const SHARD_RECOVERY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, PartialEq, Message)]
struct ObjectShardRepairRowProto {
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    object_hash: String,
    #[prost(string, tag = "4")]
    block_id: String,
    #[prost(uint32, tag = "5")]
    shard_index: u32,
    #[prost(string, tag = "6")]
    node_id: String,
    #[prost(string, tag = "7")]
    region_id: String,
    #[prost(string, tag = "8")]
    cell_id: String,
    #[prost(string, tag = "9")]
    shard_hash: String,
    #[prost(uint64, tag = "10")]
    stored_size: u64,
    #[prost(uint64, tag = "11")]
    placement_generation: u64,
    #[prost(uint64, tag = "12")]
    placement_epoch: u64,
    #[prost(uint64, tag = "13")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "14")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "15")]
    signed_payload_hash: String,
    #[prost(string, tag = "16")]
    signature_algorithm: String,
    #[prost(bytes, tag = "17")]
    receipt_signature: Vec<u8>,
    #[prost(string, tag = "18")]
    repair_finding_id: String,
    #[prost(string, tag = "19")]
    replaced_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ShardInventoryDescriptor {
    schema: String,
    block_id: String,
    shard_index: u16,
    erasure_profile_id: String,
    placement_epoch: u64,
    shard_hash: String,
    boundary_summary_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShardInventoryState {
    Present,
    Missing,
    Divergent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlacementProbeState {
    Present,
    Repairable,
    Unavailable,
}

struct PlacementProbe {
    placement: CoreObjectPlacement,
    state: PlacementProbeState,
    bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepairedShard {
    expected: CoreObjectPlacement,
    replacement: CoreObjectPlacement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedShard {
    expected: CoreObjectPlacement,
    reason: ShardRepairRetryReason,
}

#[derive(Debug, Default)]
struct ShardRepairWriteOutcome {
    repaired: Vec<RepairedShard>,
    unresolved: Vec<UnresolvedShard>,
}

impl CoreStore {
    fn apply_shard_repair_overlays_mvcc(
        &self,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        manifest: &mut CoreObjectManifest,
        snapshot_version: u64,
    ) -> Result<()> {
        for placement in &mut manifest.placements {
            let tuple_key = object_shard_repair_key(
                &manifest.object_hash,
                &manifest.encoding.block_id,
                placement.shard_index,
            );
            let key = crate::mvcc_product::coremeta_logical_key(
                CF_OBJECT_VERSIONS,
                MVCC_OBJECT_SHARD_REPAIR_TABLE_ID,
                &tuple_key,
            )?;
            let Some(row) = mvcc.runtime.read_at(&key, snapshot_version)? else {
                continue;
            };
            let repair = decode_object_shard_repair_row(
                &manifest.object_hash,
                &manifest.encoding.block_id,
                placement.shard_index,
                &row.value,
            )?;
            if repair.placement_generation > placement.generation {
                *placement = placement_from_repair_row(repair)?;
            }
        }
        manifest
            .placements
            .sort_by_key(|placement| placement.shard_index);
        Ok(())
    }

    pub(crate) async fn shard_inventory_state(
        &self,
        encoded_descriptor: &str,
    ) -> Result<ShardInventoryState> {
        let descriptor = decode_shard_inventory_descriptor(encoded_descriptor)?;
        let request = CoreInternalGetShard {
            block_id: descriptor.block_id,
            shard_index: descriptor.shard_index,
            erasure_profile_id: descriptor.erasure_profile_id,
            placement_epoch: descriptor.placement_epoch,
            shard_hash: descriptor.shard_hash,
            boundary_summary_hash: Some(descriptor.boundary_summary_hash),
            range: None,
        };
        let profile = local_erasure_profile(&request.erasure_profile_id)?;
        let local_placement = self.internal_shard_placement(profile, request.shard_index);
        let path = self.shard_path(
            &local_placement.node_id,
            &request.block_id,
            request.shard_index,
        );
        if !path.exists() {
            return Ok(ShardInventoryState::Missing);
        }
        match self.read_internal_shard_range(request).await {
            Ok(_) => Ok(ShardInventoryState::Present),
            Err(_) => Ok(ShardInventoryState::Divergent),
        }
    }

    async fn probe_object_placements(
        &self,
        manifest: &CoreObjectManifest,
        profile: LocalErasureProfile,
        candidates: &[LocalShardPlacement],
    ) -> Vec<PlacementProbe> {
        let boundary_summary = match boundary_summary_hash(&manifest.boundary_values) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, "cannot build shard inventory boundary summary");
                return manifest
                    .placements
                    .iter()
                    .cloned()
                    .map(|placement| PlacementProbe {
                        placement,
                        state: PlacementProbeState::Unavailable,
                        bytes: None,
                    })
                    .collect();
            }
        };
        let boundary_values =
            encode_boundary_values_b64(&manifest.boundary_values).unwrap_or_default();
        let candidate_by_node = candidates
            .iter()
            .map(|candidate| (candidate.node_id.as_str(), candidate))
            .collect::<BTreeMap<_, _>>();
        let mut pending = FuturesUnordered::new();
        for placement in manifest.placements.iter().cloned() {
            let candidate = candidate_by_node.get(placement.node_id.as_str()).copied();
            let boundary_summary = boundary_summary.clone();
            let boundary_values = boundary_values.clone();
            let block_id = manifest.encoding.block_id.clone();
            pending.push(async move {
                let Some(candidate) = candidate else {
                    return PlacementProbe {
                        placement,
                        state: PlacementProbeState::Unavailable,
                        bytes: None,
                    };
                };
                let inventory = tokio::time::timeout(
                    SHARD_RECOVERY_PROBE_TIMEOUT,
                    self.exchange_shard_inventory(
                        candidate,
                        &block_id,
                        profile,
                        &placement,
                        &boundary_summary,
                    ),
                )
                .await;
                match inventory {
                    Ok(Ok(ShardInventoryState::Present)) => {
                        let read = tokio::time::timeout(
                            SHARD_RECOVERY_PROBE_TIMEOUT,
                            self.read_shard_from_placement(ReadShardFromPlacement {
                                block_id: &block_id,
                                profile,
                                placement: &placement,
                                boundary_summary_hash: &boundary_summary,
                                boundary_values_b64: &boundary_values,
                                range: None,
                                operation: "anti_entropy_repair_probe",
                            }),
                        )
                        .await;
                        placement_probe_after_present_inventory(placement, read)
                    }
                    Ok(Ok(ShardInventoryState::Missing | ShardInventoryState::Divergent)) => {
                        PlacementProbe {
                            placement,
                            state: PlacementProbeState::Repairable,
                            bytes: None,
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(
                            %error,
                            node_id = %placement.node_id,
                            shard_index = placement.shard_index,
                            "CoreStore shard inventory peer is unavailable"
                        );
                        PlacementProbe {
                            placement,
                            state: PlacementProbeState::Unavailable,
                            bytes: None,
                        }
                    }
                    Err(_) => PlacementProbe {
                        placement,
                        state: PlacementProbeState::Unavailable,
                        bytes: None,
                    },
                }
            });
        }
        let mut probes = Vec::with_capacity(manifest.placements.len());
        while let Some(probe) = pending.next().await {
            probes.push(probe);
        }
        probes.sort_by_key(|probe| probe.placement.shard_index);
        probes
    }

    async fn exchange_shard_inventory(
        &self,
        candidate: &LocalShardPlacement,
        block_id: &str,
        profile: LocalErasureProfile,
        placement: &CoreObjectPlacement,
        boundary_summary_hash: &str,
    ) -> Result<ShardInventoryState> {
        let descriptor = ShardInventoryDescriptor {
            schema: SHARD_INVENTORY_SCHEMA.to_string(),
            block_id: block_id.to_string(),
            shard_index: placement.shard_index,
            erasure_profile_id: profile.id.to_string(),
            placement_epoch: placement.placement_epoch,
            shard_hash: placement.shard_hash.clone(),
            boundary_summary_hash: boundary_summary_hash.to_string(),
        };
        let encoded = encode_shard_inventory_descriptor(&descriptor)?;
        if candidate.is_local || candidate.public_api_addr.trim().is_empty() {
            return self.shard_inventory_state(&encoded).await;
        }
        let bearer = self
            .node_identity
            .internal_bearer_token
            .as_deref()
            .ok_or_else(|| {
                anyhow!("CoreStore shard anti-entropy requires an internal bearer token")
            })?;
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        let request_body = ExchangeInventoryRequest {
            header: Some(self.internal_request_header("anti_entropy.exchange_inventory")?),
            namespace: "shard".to_string(),
            partition: encoded.clone(),
            inventory_hash: shard_inventory_hash(&encoded, ShardInventoryState::Present),
        };
        let diff = self
            .internal_grpc_request(
                &candidate.public_api_addr,
                "exchange shard inventory",
                move |channel| {
                    let mut client = AntiEntropyInternalClient::new(channel);
                    let mut request = tonic::Request::new(request_body.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .exchange_inventory(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            )
            .await?;
        if diff.missing_ids.iter().any(|id| id == &encoded) {
            Ok(ShardInventoryState::Missing)
        } else if diff.divergent_ids.iter().any(|id| id == &encoded) {
            Ok(ShardInventoryState::Divergent)
        } else if diff.inventory_hash
            == shard_inventory_hash(&encoded, ShardInventoryState::Present)
        {
            Ok(ShardInventoryState::Present)
        } else {
            bail!("CoreStore shard inventory response is not canonical")
        }
    }

    async fn write_reconstructed_shards(
        &self,
        task: &crate::tasks::RebalanceShardTaskPayload,
        manifest: &CoreObjectManifest,
        profile: LocalErasureProfile,
        candidates: &[LocalShardPlacement],
        probes: &[PlacementProbe],
        shards: &[Option<Vec<u8>>],
    ) -> Result<ShardRepairWriteOutcome> {
        let boundary_summary = boundary_summary_hash(&manifest.boundary_values)?;
        let boundary_values = encode_boundary_values_b64(&manifest.boundary_values)?;
        let mut effective = probes
            .iter()
            .map(|probe| probe.placement.clone())
            .collect::<Vec<_>>();
        let mut used_nodes = probes
            .iter()
            .filter(|probe| probe.state == PlacementProbeState::Present)
            .map(|probe| probe.placement.node_id.clone())
            .collect::<BTreeSet<_>>();
        let unavailable_nodes = probes
            .iter()
            .filter(|probe| probe.state == PlacementProbeState::Unavailable)
            .map(|probe| probe.placement.node_id.clone())
            .collect::<BTreeSet<_>>();
        let next_epoch = effective
            .iter()
            .map(|placement| placement.placement_epoch)
            .max()
            .unwrap_or(LOCAL_PLACEMENT_EPOCH)
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore shard repair placement epoch overflow"))?;
        let mut outcome = ShardRepairWriteOutcome::default();

        for probe in probes {
            if probe.state == PlacementProbeState::Present {
                continue;
            }
            let index = usize::from(probe.placement.shard_index);
            let shard = shards
                .get(index)
                .and_then(Option::as_ref)
                .ok_or_else(|| anyhow!("CoreStore repair left shard {index} absent"))?;
            let target = choose_repair_target(
                profile,
                candidates,
                &effective,
                &used_nodes,
                &unavailable_nodes,
                probe,
                &manifest.object_hash,
            );
            let Some(target) = target else {
                outcome.unresolved.push(UnresolvedShard {
                    expected: probe.placement.clone(),
                    reason: ShardRepairRetryReason::NoEligibleReplacementTarget,
                });
                continue;
            };
            let shard_hash = format!("sha256:{}", sha256_hex(shard));
            let next_generation =
                probe.placement.generation.checked_add(1).ok_or_else(|| {
                    anyhow!("CoreStore shard repair placement generation overflow")
                })?;
            let physical_operation_id = physical_shard_repair_operation_id(
                task,
                &probe.placement,
                target,
                next_epoch,
                next_generation,
            )?;
            let replacement = self
                .repair_shard_to_placement(RepairShardToPlacement {
                    logical_file_id: &manifest.logical_file_id,
                    block_id: &manifest.encoding.block_id,
                    shard_index: probe.placement.shard_index,
                    shard,
                    shard_hash: &shard_hash,
                    logical_offset: manifest.logical_offset,
                    profile,
                    placement: target,
                    placement_epoch: next_epoch,
                    generation: next_generation,
                    boundary_summary_hash: &boundary_summary,
                    boundary_values_b64: &boundary_values,
                    mutation_id: &manifest.mutation_id,
                    repair_finding_id: &physical_operation_id,
                    compression_algorithm: &manifest.encoding.compression.algorithm,
                    encryption_algorithm: &manifest.encryption_algorithm,
                    writer_family: &manifest.writer_family,
                })
                .await?;
            if let Some(slot) = effective
                .iter_mut()
                .find(|placement| placement.shard_index == replacement.shard_index)
            {
                *slot = replacement.clone();
            }
            used_nodes.insert(replacement.node_id.clone());
            outcome.repaired.push(RepairedShard {
                expected: probe.placement.clone(),
                replacement,
            });
        }
        Ok(outcome)
    }

    async fn publish_shard_repair_overlays(
        &self,
        canonical_manifest: &CoreObjectManifest,
        finding_id: &str,
        repaired: &[RepairedShard],
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        lease_predicate: (
            crate::mvcc_transaction::LogicalKey,
            crate::mvcc_transaction::PredicateKind,
        ),
    ) -> Result<()> {
        if repaired.is_empty() {
            bail!("CoreStore shard repair overlay publication has no repaired placements");
        }
        let mut transaction_identity = finding_id.as_bytes().to_vec();
        for repair in repaired {
            transaction_identity.extend_from_slice(&repair.expected.shard_index.to_be_bytes());
            transaction_identity.extend_from_slice(repair.replacement.shard_hash.as_bytes());
            transaction_identity.extend_from_slice(&repair.replacement.generation.to_be_bytes());
        }
        let transaction_id = format!(
            "shard-repair-overlay-{}",
            hex::encode(hash32(&transaction_identity))
        );
        let created_at = repaired
            .iter()
            .map(|repair| repair.replacement.written_at_unix_nanos)
            .max()
            .filter(|created_at| *created_at > 0)
            .ok_or_else(|| anyhow!("CoreStore shard repair receipts have no durable timestamp"))?;
        let mut mvcc_mutations = Vec::with_capacity(repaired.len());
        let mut mvcc_predicates = Vec::with_capacity(repaired.len() + 1);
        mvcc_predicates.push(lease_predicate);
        for repair in repaired {
            let placement = &repair.replacement;
            let key = object_shard_repair_key(
                &canonical_manifest.object_hash,
                &canonical_manifest.encoding.block_id,
                placement.shard_index,
            );
            let payload = encode_deterministic_proto(&ObjectShardRepairRowProto {
                schema: OBJECT_SHARD_REPAIR_SCHEMA.to_string(),
                object_hash: canonical_manifest.object_hash.clone(),
                block_id: canonical_manifest.encoding.block_id.clone(),
                shard_index: u32::from(placement.shard_index),
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash: placement.shard_hash.clone(),
                stored_size: placement.stored_size,
                placement_generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash.clone(),
                signature_algorithm: placement.signature_algorithm.clone(),
                receipt_signature: placement.receipt_signature.clone(),
                repair_finding_id: finding_id.to_string(),
                replaced_node_id: repair.expected.node_id.clone(),
            });
            let logical_key = crate::mvcc_product::coremeta_logical_key(
                CF_OBJECT_VERSIONS,
                MVCC_OBJECT_SHARD_REPAIR_TABLE_ID,
                &key,
            )?;
            let current = mvcc.read_latest_value(&logical_key)?;
            let canonical = canonical_manifest
                .placements
                .iter()
                .find(|candidate| candidate.shard_index == repair.expected.shard_index)
                .ok_or_else(|| anyhow!("canonical shard repair placement is missing"))?;
            let current_effective = current
                .as_deref()
                .map(|bytes| {
                    decode_object_shard_repair_row(
                        &canonical_manifest.object_hash,
                        &canonical_manifest.encoding.block_id,
                        repair.expected.shard_index,
                        bytes,
                    )
                    .and_then(placement_from_repair_row)
                })
                .transpose()?
                .filter(|overlay| overlay.generation > canonical.generation)
                .unwrap_or_else(|| canonical.clone());
            if current_effective != repair.expected {
                bail!("MVCC shard repair overlay changed after immutable shard preparation");
            }
            let predicate = current
                .map(|bytes| {
                    crate::mvcc_transaction::PredicateKind::ValueHash(
                        *blake3::hash(&bytes).as_bytes(),
                    )
                })
                .unwrap_or(crate::mvcc_transaction::PredicateKind::Absent);
            mvcc_predicates.push((logical_key.clone(), predicate));
            mvcc_mutations.push(crate::mvcc_product::ProductMutation::put(
                logical_key,
                payload.clone(),
            ));
        }
        mvcc.autocommit_product_mutations_with_predicates(
            "core_shard_repair",
            &transaction_id,
            mvcc_mutations,
            mvcc_predicates,
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            u64::try_from(created_at / 1_000_000).unwrap_or_default(),
        )
        .await?;
        Ok(())
    }
}

fn placement_probe_after_present_inventory(
    placement: CoreObjectPlacement,
    read: std::result::Result<Result<Vec<u8>>, tokio::time::error::Elapsed>,
) -> PlacementProbe {
    match read {
        Ok(Ok(bytes)) => PlacementProbe {
            placement,
            state: PlacementProbeState::Present,
            bytes: Some(bytes),
        },
        Ok(Err(error)) if is_core_store_unavailable(&error) => PlacementProbe {
            placement,
            state: PlacementProbeState::Unavailable,
            bytes: None,
        },
        Ok(Err(error)) => {
            tracing::warn!(
                %error,
                node_id = %placement.node_id,
                shard_index = placement.shard_index,
                "CoreStore shard inventory reported present but integrity verification failed"
            );
            PlacementProbe {
                placement,
                state: PlacementProbeState::Repairable,
                bytes: None,
            }
        }
        Err(_) => PlacementProbe {
            placement,
            state: PlacementProbeState::Unavailable,
            bytes: None,
        },
    }
}

fn validate_repair_manifest_identity(manifest: &CoreObjectManifest) -> Result<WriterFamily> {
    validate_logical_file_id(&manifest.logical_file_id, "repair manifest logical file id")?;
    validate_writer_family(&manifest.writer_family, "repair manifest writer family")?;
    validate_logical_id(&manifest.mutation_id, "repair manifest mutation id")?;
    validate_object_blob_pipeline_options(
        &manifest.encoding.compression.algorithm,
        &manifest.encryption_algorithm,
    )?;
    if manifest.encryption_algorithm != manifest.encoding.encryption {
        bail!("CoreStore repair manifest encryption identity mismatch");
    }
    WriterFamily::from_name(&manifest.writer_family)
        .ok_or_else(|| anyhow!("CoreStore repair manifest writer family is not registered"))
}

pub(crate) fn shard_inventory_response(
    encoded_descriptor: &str,
    state: ShardInventoryState,
) -> crate::anvil_api::InventoryDiff {
    let (missing_ids, divergent_ids) = match state {
        ShardInventoryState::Present => (Vec::new(), Vec::new()),
        ShardInventoryState::Missing => (vec![encoded_descriptor.to_string()], Vec::new()),
        ShardInventoryState::Divergent => (Vec::new(), vec![encoded_descriptor.to_string()]),
    };
    crate::anvil_api::InventoryDiff {
        missing_ids,
        divergent_ids,
        inventory_hash: shard_inventory_hash(encoded_descriptor, state),
    }
}

fn choose_repair_target<'a>(
    profile: LocalErasureProfile,
    candidates: &'a [LocalShardPlacement],
    effective: &[CoreObjectPlacement],
    used_nodes: &BTreeSet<String>,
    unavailable_nodes: &BTreeSet<String>,
    probe: &PlacementProbe,
    object_hash: &str,
) -> Option<&'a LocalShardPlacement> {
    let mut eligible = candidates
        .iter()
        .filter(|candidate| {
            !used_nodes.contains(&candidate.node_id)
                && !unavailable_nodes.contains(&candidate.node_id)
        })
        .collect::<Vec<_>>();
    if probe.state == PlacementProbeState::Repairable {
        eligible.sort_by(|left, right| {
            (left.node_id != probe.placement.node_id)
                .cmp(&(right.node_id != probe.placement.node_id))
                .then_with(|| {
                    rendezvous_score(object_hash, &right.node_id)
                        .cmp(&rendezvous_score(object_hash, &left.node_id))
                })
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
    } else {
        eligible.sort_by(|left, right| {
            rendezvous_score(object_hash, &right.node_id)
                .cmp(&rendezvous_score(object_hash, &left.node_id))
                .then_with(|| left.node_id.cmp(&right.node_id))
        });
    }
    eligible.into_iter().find(|candidate| {
        let mut proposed = effective
            .iter()
            .map(|placement| LocalShardPlacement {
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                failure_domain: placement.cell_id.clone(),
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: String::new(),
                is_local: false,
            })
            .collect::<Vec<_>>();
        let index = usize::from(probe.placement.shard_index);
        if index >= proposed.len() {
            return false;
        }
        proposed[index] = (*candidate).clone();
        validate_local_publish_placements(profile, &proposed).is_ok()
    })
}

fn verify_reconstructed_shards(
    probes: &[PlacementProbe],
    shards: &[Option<Vec<u8>>],
) -> Result<()> {
    for probe in probes {
        let index = usize::from(probe.placement.shard_index);
        let shard = shards
            .get(index)
            .and_then(Option::as_ref)
            .ok_or_else(|| anyhow!("CoreStore reconstructed shard {index} is missing"))?;
        let hash = format!("sha256:{}", sha256_hex(shard));
        if hash != probe.placement.shard_hash {
            bail!("CoreStore reconstructed shard {index} hash mismatch");
        }
    }
    Ok(())
}

fn object_shard_repair_key(object_hash: &str, block_id: &str, shard_index: u16) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-shard-repair",
        object_hash.as_bytes(),
        block_id.as_bytes(),
        &shard_index.to_be_bytes(),
    ])
}

fn decode_object_shard_repair_row(
    object_hash: &str,
    block_id: &str,
    shard_index: u16,
    payload: &[u8],
) -> Result<ObjectShardRepairRowProto> {
    let row = decode_deterministic_proto::<ObjectShardRepairRowProto>(
        payload,
        "object shard repair row",
    )?;
    if row.schema != OBJECT_SHARD_REPAIR_SCHEMA
        || row.object_hash != object_hash
        || row.block_id != block_id
        || row.shard_index != u32::from(shard_index)
    {
        bail!("CoreStore object shard repair row scope mismatch");
    }
    Ok(row)
}

fn placement_from_repair_row(row: ObjectShardRepairRowProto) -> Result<CoreObjectPlacement> {
    let shard_index = u16::try_from(row.shard_index)
        .map_err(|_| anyhow!("CoreStore object shard repair index exceeds u16"))?;
    if row.placement_generation < 2 || row.placement_epoch < 2 {
        bail!("CoreStore object shard repair row does not advance placement state");
    }
    Ok(CoreObjectPlacement {
        shard_index,
        node_id: row.node_id,
        region_id: row.region_id,
        cell_id: row.cell_id,
        shard_hash: row.shard_hash,
        stored_size: row.stored_size,
        generation: row.placement_generation,
        placement_epoch: row.placement_epoch,
        fsync_sequence: row.fsync_sequence,
        written_at_unix_nanos: row.written_at_unix_nanos,
        signed_payload_hash: row.signed_payload_hash,
        signature_algorithm: row.signature_algorithm,
        receipt_signature: row.receipt_signature,
    })
}

fn require_safe_repair_finding_id(finding_id: &str) -> Result<()> {
    if finding_id.is_empty()
        || finding_id.len() > 256
        || finding_id.contains('/')
        || finding_id.contains('\\')
        || finding_id.chars().any(char::is_control)
    {
        bail!("CoreStore shard repair finding id is not a safe bounded identity");
    }
    Ok(())
}

fn append_repair_placement_identity(bytes: &mut Vec<u8>, placement: &CoreObjectPlacement) {
    bytes.extend_from_slice(&placement.shard_index.to_be_bytes());
    append_repair_identity_component(bytes, placement.node_id.as_bytes());
    append_repair_identity_component(bytes, placement.region_id.as_bytes());
    append_repair_identity_component(bytes, placement.cell_id.as_bytes());
    append_repair_identity_component(bytes, placement.shard_hash.as_bytes());
    bytes.extend_from_slice(&placement.stored_size.to_be_bytes());
    bytes.extend_from_slice(&placement.generation.to_be_bytes());
    bytes.extend_from_slice(&placement.placement_epoch.to_be_bytes());
    bytes.extend_from_slice(&placement.fsync_sequence.to_be_bytes());
    bytes.extend_from_slice(&placement.written_at_unix_nanos.to_be_bytes());
    append_repair_identity_component(bytes, placement.signed_payload_hash.as_bytes());
    append_repair_identity_component(bytes, placement.signature_algorithm.as_bytes());
    append_repair_identity_component(bytes, &placement.receipt_signature);
}

fn append_repair_identity_component(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

fn rendezvous_score(scope: &str, node_id: &str) -> [u8; 32] {
    hash32(format!("anvil.equal_peer.repair.v1\0{scope}\0{node_id}").as_bytes())
}

fn encode_shard_inventory_descriptor(descriptor: &ShardInventoryDescriptor) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(descriptor)?))
}

fn decode_shard_inventory_descriptor(encoded: &str) -> Result<ShardInventoryDescriptor> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .context("decode shard inventory descriptor")?;
    let descriptor: ShardInventoryDescriptor = serde_json::from_slice(&bytes)?;
    if descriptor.schema != SHARD_INVENTORY_SCHEMA {
        bail!("CoreStore shard inventory descriptor schema mismatch");
    }
    validate_logical_id(&descriptor.block_id, "shard inventory block id")?;
    validate_hash(&descriptor.shard_hash, "shard inventory shard hash")?;
    validate_hash(
        &descriptor.boundary_summary_hash,
        "shard inventory boundary summary hash",
    )?;
    if descriptor.placement_epoch == 0 {
        bail!("CoreStore shard inventory placement epoch must be nonzero");
    }
    Ok(descriptor)
}

fn shard_inventory_hash(encoded_descriptor: &str, state: ShardInventoryState) -> String {
    let state = match state {
        ShardInventoryState::Present => "present",
        ShardInventoryState::Missing => "missing",
        ShardInventoryState::Divergent => "divergent",
    };
    format!(
        "sha256:{}",
        sha256_hex(format!("anvil.shard.inventory.v1\0{encoded_descriptor}\0{state}").as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(node_id: &str, cell_id: &str) -> LocalShardPlacement {
        LocalShardPlacement {
            node_id: node_id.to_string(),
            region_id: "r1".to_string(),
            cell_id: cell_id.to_string(),
            failure_domain: cell_id.to_string(),
            region_weight: 100,
            cell_weight: 100,
            public_api_addr: format!("http://{node_id}"),
            is_local: false,
        }
    }

    fn object_placement(shard_index: u16, node_id: &str, generation: u64) -> CoreObjectPlacement {
        CoreObjectPlacement {
            shard_index,
            node_id: node_id.to_string(),
            region_id: "r1".to_string(),
            cell_id: "a".to_string(),
            shard_hash: format!("sha256:{}", "1".repeat(64)),
            stored_size: 1,
            generation,
            placement_epoch: generation,
            fsync_sequence: 1,
            written_at_unix_nanos: 1,
            signed_payload_hash: format!("sha256:{}", "2".repeat(64)),
            signature_algorithm: "ed25519".to_string(),
            receipt_signature: vec![1],
        }
    }

    fn stream_repair_manifest() -> CoreObjectManifest {
        let hash = "a".repeat(64);
        let mut object_ref =
            CoreObjectRef::test_unlocated(format!("sha256:{hash}"), 42, encode_manifest_ref(&hash));
        object_ref.encoding.encryption = "aes_gcm_siv".to_string();
        CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: "mesh-a".to_string(),
            region_id: "r1".to_string(),
            object_hash: object_ref.hash,
            logical_size: object_ref.logical_size,
            logical_file_id: "lf_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_string(),
            logical_offset: 12_288,
            writer_family: WriterFamily::Stream.as_str().to_string(),
            encryption_algorithm: "aes_gcm_siv".to_string(),
            boundary_values: Vec::new(),
            encoding: object_ref.encoding,
            placements: Vec::new(),
            created_at: "2026-07-20T00:00:00+00:00".to_string(),
            mutation_id: "original-stream-mutation".to_string(),
        }
    }

    #[test]
    fn repair_requires_and_accepts_retained_stream_identity() {
        let manifest = stream_repair_manifest();

        assert_eq!(
            validate_repair_manifest_identity(&manifest).unwrap(),
            WriterFamily::Stream
        );

        let mut invalid = manifest;
        invalid.encryption_algorithm = "none".to_string();
        assert!(validate_repair_manifest_identity(&invalid).is_err());

        invalid = stream_repair_manifest();
        invalid.logical_file_id = "object_blob/repair".to_string();
        assert!(validate_repair_manifest_identity(&invalid).is_err());

        invalid = stream_repair_manifest();
        invalid.writer_family = "object_writer".to_string();
        assert!(validate_repair_manifest_identity(&invalid).is_err());
    }

    #[test]
    fn shard_inventory_descriptor_round_trips() {
        let descriptor = ShardInventoryDescriptor {
            schema: SHARD_INVENTORY_SCHEMA.to_string(),
            block_id: "blk_123".to_string(),
            shard_index: 3,
            erasure_profile_id: "ec-4-2".to_string(),
            placement_epoch: 2,
            shard_hash: format!("sha256:{}", "1".repeat(64)),
            boundary_summary_hash: format!("sha256:{}", "2".repeat(64)),
        };
        let encoded = encode_shard_inventory_descriptor(&descriptor).unwrap();
        assert_eq!(
            decode_shard_inventory_descriptor(&encoded).unwrap(),
            descriptor
        );
    }

    #[test]
    fn unavailable_shard_read_does_not_schedule_integrity_repair() {
        let placement = object_placement(2, "node-a", 1);
        let error: anyhow::Error = CoreStoreAvailabilityError::PeerUnavailable {
            operation: "anti_entropy_repair_probe".to_string(),
            endpoint: "http://node-a".to_string(),
            details: "peer restarting".to_string(),
        }
        .into();
        let probe = placement_probe_after_present_inventory(placement, Ok(Err(error)));
        assert_eq!(probe.state, PlacementProbeState::Unavailable);
        assert!(probe.bytes.is_none());
    }

    #[test]
    fn corrupt_shard_read_is_repairable_after_present_inventory() {
        let placement = object_placement(2, "node-a", 1);
        let probe = placement_probe_after_present_inventory(
            placement,
            Ok(Err(anyhow!("CoreStore remote shard hash mismatch"))),
        );
        assert_eq!(probe.state, PlacementProbeState::Repairable);
        assert!(probe.bytes.is_none());
    }

    #[test]
    fn repair_target_preserves_ec_4_2_spread() {
        let candidates = vec![
            candidate("n1", "a"),
            candidate("n2", "a"),
            candidate("n3", "b"),
            candidate("n4", "b"),
            candidate("n5", "c"),
            candidate("n6", "c"),
            candidate("n7", "d"),
        ];
        let effective = candidates[..6]
            .iter()
            .enumerate()
            .map(|(index, candidate)| CoreObjectPlacement {
                shard_index: index as u16,
                node_id: candidate.node_id.clone(),
                region_id: candidate.region_id.clone(),
                cell_id: candidate.cell_id.clone(),
                shard_hash: format!("sha256:{}", "1".repeat(64)),
                stored_size: 1,
                generation: 1,
                placement_epoch: 1,
                fsync_sequence: 1,
                written_at_unix_nanos: 1,
                signed_payload_hash: format!("sha256:{}", "2".repeat(64)),
                signature_algorithm: "ed25519".to_string(),
                receipt_signature: vec![1],
            })
            .collect::<Vec<_>>();
        let probe = PlacementProbe {
            placement: effective[0].clone(),
            state: PlacementProbeState::Unavailable,
            bytes: None,
        };
        let used = effective[1..]
            .iter()
            .map(|placement| placement.node_id.clone())
            .collect();
        let unavailable = BTreeSet::from(["n1".to_string()]);
        let selected = choose_repair_target(
            LOCAL_EC_4_2_PROFILE,
            &candidates,
            &effective,
            &used,
            &unavailable,
            &probe,
            "sha256:object",
        )
        .unwrap();
        assert_eq!(selected.node_id, "n7");
    }

    #[test]
    fn repairable_placement_prefers_its_original_active_node() {
        let candidates = vec![
            candidate("n1", "a"),
            candidate("n2", "a"),
            candidate("n3", "b"),
            candidate("n4", "b"),
            candidate("n5", "c"),
            candidate("n6", "c"),
        ];
        let effective = candidates
            .iter()
            .enumerate()
            .map(|(index, candidate)| CoreObjectPlacement {
                shard_index: index as u16,
                node_id: candidate.node_id.clone(),
                region_id: candidate.region_id.clone(),
                cell_id: candidate.cell_id.clone(),
                shard_hash: format!("sha256:{}", "1".repeat(64)),
                stored_size: 1,
                generation: 1,
                placement_epoch: 1,
                fsync_sequence: 1,
                written_at_unix_nanos: 1,
                signed_payload_hash: format!("sha256:{}", "2".repeat(64)),
                signature_algorithm: "ed25519".to_string(),
                receipt_signature: vec![1],
            })
            .collect::<Vec<_>>();
        let probe = PlacementProbe {
            placement: effective[0].clone(),
            state: PlacementProbeState::Repairable,
            bytes: None,
        };
        let used = effective[1..]
            .iter()
            .map(|placement| placement.node_id.clone())
            .collect();
        let selected = choose_repair_target(
            LOCAL_EC_4_2_PROFILE,
            &candidates,
            &effective,
            &used,
            &BTreeSet::new(),
            &probe,
            "sha256:object",
        )
        .unwrap();
        assert_eq!(selected.node_id, "n1");
    }
}
