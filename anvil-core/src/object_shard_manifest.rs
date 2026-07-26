//! Canonical physical manifests for MVCC object shard representations.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    mvcc_transaction::{NodeIncarnation, ObjectShardManifestReference},
    replication_client::{TonicReplicationStreamManager, object_shard_transfer_id},
    shard_placement::DistributedIngestResult,
};

pub const OBJECT_SHARD_MANIFEST_SCHEMA: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhysicalObjectShardManifest {
    pub schema_version: u16,
    pub cluster_id: String,
    pub object_identity: Uuid,
    pub object_hash: String,
    pub object_length: u64,
    pub encoding_generation: u64,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub shard_bytes: u64,
    pub stripe_count: u64,
    pub placements: Vec<PhysicalShardPlacement>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhysicalShardPlacement {
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
    pub payload_length: u64,
    pub payload_hash: [u8; 32],
    pub transfer_id: Uuid,
    pub node_id: String,
    pub node_incarnation: u64,
    pub failure_domain: String,
}

impl PhysicalObjectShardManifest {
    pub fn from_ingest(
        cluster_id: impl Into<String>,
        object_identity: Uuid,
        encoding_generation: u64,
        data_shards: usize,
        parity_shards: usize,
        shard_bytes: usize,
        result: &DistributedIngestResult,
    ) -> Result<Self> {
        let cluster_id = cluster_id.into();
        let data_shards = u16::try_from(data_shards).context("data shard count exceeds u16")?;
        let parity_shards =
            u16::try_from(parity_shards).context("parity shard count exceeds u16")?;
        let shard_bytes = u64::try_from(shard_bytes).context("shard size exceeds u64")?;
        let object_hash = format!("sha256:{}", hex::encode(result.encoded.content_hash));
        let mut placements = result
            .placements
            .iter()
            .map(|placement| {
                if placement.target.cluster_id != cluster_id {
                    bail!("physical shard placement belongs to another cluster");
                }
                Ok(PhysicalShardPlacement {
                    stripe_ordinal: placement.stripe_ordinal,
                    shard_ordinal: placement.shard_ordinal,
                    payload_length: placement.payload_length,
                    payload_hash: placement.payload_hash,
                    transfer_id: object_shard_transfer_id(
                        object_identity,
                        encoding_generation,
                        placement.stripe_ordinal,
                        placement.shard_ordinal,
                        placement.payload_hash,
                        placement.payload_length,
                    ),
                    node_id: placement.target.node.node_id.clone(),
                    node_incarnation: placement.target.node.incarnation,
                    failure_domain: placement.target.failure_domain.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        placements.sort_by_key(|placement| {
            (
                placement.stripe_ordinal,
                placement.shard_ordinal,
                placement.node_id.clone(),
                placement.node_incarnation,
            )
        });
        let manifest = Self {
            schema_version: OBJECT_SHARD_MANIFEST_SCHEMA,
            cluster_id,
            object_identity,
            object_hash,
            object_length: result.encoded.object_length,
            encoding_generation,
            data_shards,
            parity_shards,
            shard_bytes,
            stripe_count: result.encoded.stripe_count,
            placements,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != OBJECT_SHARD_MANIFEST_SCHEMA
            || self.cluster_id.trim().is_empty()
            || self.data_shards == 0
            || self.parity_shards == 0
            || self.shard_bytes == 0
        {
            bail!("invalid physical object shard manifest header");
        }
        let expected_per_stripe = usize::from(self.data_shards) + usize::from(self.parity_shards);
        for stripe in 0..self.stripe_count {
            let entries = self
                .placements
                .iter()
                .filter(|placement| placement.stripe_ordinal == stripe)
                .collect::<Vec<_>>();
            if entries.len() < usize::from(self.data_shards) {
                bail!("physical object manifest lacks enough shard placements");
            }
            let mut ordinals = std::collections::BTreeSet::new();
            for placement in entries {
                if usize::from(placement.shard_ordinal) >= expected_per_stripe
                    || !ordinals.insert(placement.shard_ordinal)
                {
                    bail!("physical object manifest has invalid or duplicate shard ordinal");
                }
            }
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn reference(&self) -> Result<ObjectShardManifestReference> {
        let bytes = self.canonical_bytes()?;
        Ok(ObjectShardManifestReference {
            object_hash: self.object_hash.clone(),
            manifest_hash: format!("sha256:{}", hex::encode(Sha256::digest(bytes))),
            object_length: self.object_length,
            encoding_generation: self.encoding_generation,
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
            stripe_count: self.stripe_count,
        })
    }

    pub async fn read_range_chunks<F, Fut>(
        &self,
        transport: &TonicReplicationStreamManager,
        start: u64,
        end_exclusive: u64,
        mut output: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        self.validate()?;
        if start > end_exclusive || end_exclusive > self.object_length {
            bail!("invalid MVCC object read range");
        }
        let codec = reed_solomon_erasure::galois_8::ReedSolomon::new(
            usize::from(self.data_shards),
            usize::from(self.parity_shards),
        )?;
        let mut object_hash = Sha256::new();
        let mut logical_offset = 0_u64;
        let total_shards = usize::from(self.data_shards) + usize::from(self.parity_shards);
        for stripe in 0..self.stripe_count {
            let mut shards = vec![None; total_shards];
            for placement in self
                .placements
                .iter()
                .filter(|placement| placement.stripe_ordinal == stripe)
            {
                let result = transport
                    .read_complete_transfer(
                        &self.cluster_id,
                        &NodeIncarnation {
                            node_id: placement.node_id.clone(),
                            incarnation: placement.node_incarnation,
                        },
                        placement.transfer_id,
                        placement.payload_length,
                        placement.payload_hash,
                    )
                    .await;
                let Ok(bytes) = result else {
                    continue;
                };
                if !shard_payload_matches(&bytes, placement.payload_hash) {
                    continue;
                }
                shards[usize::from(placement.shard_ordinal)] = Some(bytes);
            }
            if shards.iter().filter(|shard| shard.is_some()).count() < usize::from(self.data_shards)
            {
                bail!("not enough verified shards to reconstruct object stripe");
            }
            codec.reconstruct(&mut shards)?;
            for shard in shards.iter().take(usize::from(self.data_shards)) {
                let shard = shard
                    .as_deref()
                    .context("Reed-Solomon reconstruction omitted data shard")?;
                let remaining = self.object_length.saturating_sub(logical_offset);
                let logical_len = usize::try_from(remaining.min(shard.len() as u64))
                    .context("logical shard length exceeds address space")?;
                let logical = &shard[..logical_len];
                object_hash.update(logical);
                let shard_end = logical_offset + logical_len as u64;
                let selected_start = start.max(logical_offset);
                let selected_end = end_exclusive.min(shard_end);
                if selected_start < selected_end {
                    let relative_start = usize::try_from(selected_start - logical_offset)?;
                    let relative_end = usize::try_from(selected_end - logical_offset)?;
                    for chunk in logical[relative_start..relative_end].chunks(64 * 1024) {
                        output(chunk.to_vec()).await?;
                    }
                }
                logical_offset = shard_end;
            }
        }
        if logical_offset != self.object_length
            || format!("sha256:{}", hex::encode(object_hash.finalize())) != self.object_hash
        {
            bail!("reconstructed object content hash does not match manifest");
        }
        Ok(())
    }
}

fn shard_payload_matches(payload: &[u8], expected_hash: [u8; 32]) -> bool {
    *blake3::hash(payload).as_bytes() == expected_hash
}

#[cfg(test)]
mod tests {
    use crate::{
        mvcc_transaction::NodeIncarnation,
        shard_placement::{CompletedShardPlacement, ShardTarget},
        streaming_erasure::EncodedObject,
    };

    use super::*;

    #[test]
    fn manifest_is_canonical_and_cluster_bound() {
        let result = DistributedIngestResult {
            encoded: EncodedObject {
                content_hash: [7; 32],
                object_length: 3,
                stripe_count: 1,
            },
            evidence: Vec::new(),
            placements: (0..3)
                .map(|ordinal| CompletedShardPlacement {
                    stripe_ordinal: 0,
                    shard_ordinal: ordinal,
                    payload_length: 2,
                    payload_hash: [ordinal as u8; 32],
                    target: ShardTarget {
                        cluster_id: "cluster-a".into(),
                        node: NodeIncarnation {
                            node_id: format!("node-{ordinal}"),
                            incarnation: 1,
                        },
                        failure_domain: format!("zone-{ordinal}"),
                    },
                })
                .collect(),
        };
        let manifest = PhysicalObjectShardManifest::from_ingest(
            "cluster-a",
            Uuid::from_u128(9),
            1,
            2,
            1,
            2,
            &result,
        )
        .unwrap();
        assert_eq!(manifest.reference().unwrap(), manifest.reference().unwrap());
        assert!(
            PhysicalObjectShardManifest::from_ingest(
                "cluster-b",
                Uuid::from_u128(9),
                1,
                2,
                1,
                2,
                &result
            )
            .is_err()
        );
    }

    #[test]
    fn reconstruction_accepts_encoder_blake3_shard_hashes() {
        let codec = reed_solomon_erasure::galois_8::ReedSolomon::new(2, 1).unwrap();
        let mut encoded = vec![b"abcd".to_vec(), b"efgh".to_vec(), vec![0; 4]];
        codec.encode(&mut encoded).unwrap();
        let hashes = encoded
            .iter()
            .map(|payload| *blake3::hash(payload).as_bytes())
            .collect::<Vec<_>>();

        let mut available = encoded
            .iter()
            .zip(hashes)
            .map(|(payload, hash)| shard_payload_matches(payload, hash).then(|| payload.clone()))
            .collect::<Vec<_>>();
        available[0] = None;
        codec.reconstruct(&mut available).unwrap();

        assert_eq!(available[0].as_deref(), Some(b"abcd".as_slice()));
        assert_eq!(available[1].as_deref(), Some(b"efgh".as_slice()));
    }
}
