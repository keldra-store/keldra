use super::*;
use crate::anvil_api::RootPrepareReceipt;
use prost::Message;
use std::io::Write;

const ROOT_REGISTER_SHARD_MAGIC: &[u8; 8] = b"ANREGRT1";
const ROOT_REGISTER_SHARD_VERSION: u16 = 1;
const ROOT_REGISTER_FSYNC_SEQUENCE: u64 = 1;

#[derive(Clone, PartialEq, Message)]
struct RootRegisterShardHeaderProto {
    #[prost(string, tag = "1")]
    root_key_hash: String,
    #[prost(uint64, tag = "2")]
    root_generation: u64,
    #[prost(string, repeated, tag = "3")]
    register_cohort_nodes: Vec<String>,
    #[prost(string, tag = "4")]
    register_cohort_hash: String,
    #[prost(uint64, tag = "5")]
    placement_epoch: u64,
    #[prost(uint64, tag = "6")]
    created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RootRegisterShard {
    pub(crate) root_partition_id: u64,
    pub(crate) root_key_hash: String,
    pub(crate) root_generation: u64,
    pub(crate) shard_index: u16,
    pub(crate) register_cohort_nodes: Vec<String>,
    pub(crate) register_cohort_hash: String,
    pub(crate) placement_epoch: u64,
    pub(crate) created_at_unix_nanos: u64,
    pub(crate) root_anchor_record: Vec<u8>,
    pub(crate) root_anchor_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RootRegisterGenerationInspection {
    pub(super) root_key_hash: String,
    pub(super) root_generation: u64,
    pub(super) register_cohort_nodes: Vec<String>,
    pub(super) register_cohort_hash: String,
    pub(super) placement_epoch: u64,
    pub(super) root_anchor_record: Vec<u8>,
    pub(super) root_anchor_hash: String,
    pub(super) shard_indexes: BTreeSet<u16>,
}

impl RootRegisterGenerationInspection {
    pub(super) fn is_synthetic(&self) -> bool {
        self.register_cohort_nodes
            .iter()
            .all(|node_id| crate::mesh_lifecycle::is_synthetic_control_node_id(node_id))
    }
}

impl CoreStore {
    pub(crate) async fn persist_root_register_prepare(
        &self,
        replica_node_id: &str,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        cohort_node_ids: &[String],
        cohort_hash: &str,
        shard_index: u16,
        placement_epoch: u64,
    ) -> Result<RootPrepareReceipt> {
        validate_root_register_scope(
            anchor,
            anchor_bytes,
            expected_generation,
            cohort_node_ids,
            cohort_hash,
            shard_index,
            placement_epoch,
        )?;
        if cohort_node_ids
            .get(usize::from(shard_index))
            .map(String::as_str)
            != Some(replica_node_id)
        {
            bail!("CoreStore root-register shard index does not identify the target replica");
        }

        let shard = RootRegisterShard {
            root_partition_id: root_register_partition_id(anchor),
            root_key_hash: anchor.root_key_hash.clone(),
            root_generation: anchor.root_generation,
            shard_index,
            register_cohort_nodes: cohort_node_ids.to_vec(),
            register_cohort_hash: cohort_hash.to_string(),
            placement_epoch,
            created_at_unix_nanos: anchor.created_at_unix_nanos,
            root_anchor_record: anchor_bytes.to_vec(),
            root_anchor_hash: hash_root_anchor_record(anchor)?,
        };
        let encoded = encode_root_register_shard(&shard)?;
        let path = self.root_register_shard_path(&shard);
        persist_create_new_root_register_shard(path, encoded.clone()).await?;
        let persisted = read_root_register_shard_bytes(&encoded)?;
        if persisted != shard {
            bail!("CoreStore root-register shard failed durable read-back verification");
        }

        let mut receipt = RootPrepareReceipt {
            replica_node_id: replica_node_id.to_string(),
            root_key_hash: anchor.root_key_hash.clone(),
            expected_generation,
            post_generation: anchor.root_generation,
            new_root_hash: shard.root_anchor_hash,
            shard_index: u32::from(shard_index),
            register_cohort_hash: cohort_hash.to_string(),
            fsync_sequence: ROOT_REGISTER_FSYNC_SEQUENCE,
            signed_payload_hash: String::new(),
            signature: Vec::new(),
            placement_epoch,
        };
        receipt.signed_payload_hash = root_prepare_receipt_payload_hash(&receipt);
        receipt.signature = self.sign_internal_core_receipt(&receipt.signed_payload_hash)?;
        Ok(receipt)
    }

    pub(crate) async fn ensure_local_committed_root_register_shard(
        &self,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        cohort_node_ids: &[String],
        cohort_hash: &str,
        placement_epoch: u64,
    ) -> Result<()> {
        let Some(shard_index) = cohort_node_ids
            .iter()
            .position(|node_id| node_id == &self.node_identity.node_id)
        else {
            return Ok(());
        };
        let shard_index = u16::try_from(shard_index)
            .map_err(|_| anyhow!("root-register repair shard index overflow"))?;
        self.persist_root_register_prepare(
            &self.node_identity.node_id,
            anchor,
            anchor_bytes,
            expected_generation,
            cohort_node_ids,
            cohort_hash,
            shard_index,
            placement_epoch,
        )
        .await?;
        Ok(())
    }

    pub(super) fn root_register_shard_path(&self, shard: &RootRegisterShard) -> PathBuf {
        let hash_hex = shard
            .root_key_hash
            .strip_prefix("sha256:")
            .unwrap_or(&shard.root_key_hash);
        let prefix = hash_hex.get(..2).unwrap_or("00");
        self.storage
            .core_store_blocks_path()
            .join("register")
            .join(format!("{:020}", shard.root_partition_id))
            .join(prefix)
            .join(hash_hex)
            .join(format!("generation-{:020}", shard.root_generation))
            .join(format!("shard-{}.anr", shard.shard_index))
    }

    pub(crate) async fn read_exact_root_register_shard(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<RootRegisterShard>> {
        validate_hash(root_key_hash, "root-register read root key hash")?;
        let directory = self.root_register_generation_path(root_key_hash, generation)?;
        let root_key_hash = root_key_hash.to_string();
        let preferred_node_id = self.node_identity.node_id.clone();
        tokio::task::spawn_blocking(move || {
            read_root_register_generation(directory, &root_key_hash, generation, &preferred_node_id)
        })
        .await
        .map_err(|error| anyhow!("root-register read task failed: {error}"))?
    }

    pub(super) async fn inspect_root_register_generation(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<RootRegisterGenerationInspection>> {
        validate_hash(root_key_hash, "root-register inspection root key hash")?;
        let directory = self.root_register_generation_path(root_key_hash, generation)?;
        let root_key_hash = root_key_hash.to_string();
        tokio::task::spawn_blocking(move || {
            inspect_root_register_generation(directory, &root_key_hash, generation)
        })
        .await
        .map_err(|error| anyhow!("root-register inspection task failed: {error}"))?
    }

    pub(super) fn root_register_generation_path(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<PathBuf> {
        let hash_hex = root_key_hash
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow!("root-register root key hash has no sha256 prefix"))?;
        let prefix = hash_hex.get(..2).unwrap_or("00");
        Ok(self
            .storage
            .core_store_blocks_path()
            .join("register")
            .join(format!(
                "{:020}",
                root_register_partition_id_for_hash(root_key_hash)?
            ))
            .join(prefix)
            .join(hash_hex)
            .join(format!("generation-{generation:020}")))
    }
}

pub(crate) fn root_prepare_receipt_payload_hash(receipt: &RootPrepareReceipt) -> String {
    let mut unsigned = receipt.clone();
    unsigned.signed_payload_hash.clear();
    unsigned.signature.clear();
    domain_hash_bytes(
        "anvil.root.prepare_receipt.v1",
        &encode_deterministic_proto(&unsigned),
    )
}

pub(crate) fn root_register_cohort_hash(
    root_key_hash: &str,
    root_generation: u64,
    cohort_node_ids: &[String],
) -> String {
    let mut canonical = Vec::new();
    append_hash_part(&mut canonical, root_key_hash.as_bytes());
    append_hash_part(&mut canonical, &root_generation.to_le_bytes());
    for node_id in cohort_node_ids {
        append_hash_part(&mut canonical, node_id.as_bytes());
    }
    domain_hash_bytes("anvil.root.cohort.v1", &canonical)
}

fn validate_root_register_scope(
    anchor: &CoreRootAnchorRecord,
    anchor_bytes: &[u8],
    expected_generation: u64,
    cohort_node_ids: &[String],
    cohort_hash: &str,
    shard_index: u16,
    placement_epoch: u64,
) -> Result<()> {
    validate_root_anchor_record(anchor)?;
    if anchor.root_generation != expected_generation.saturating_add(1) {
        bail!("CoreStore root-register post generation mismatch");
    }
    if anchor_bytes != encode_root_anchor_record(anchor)? {
        bail!("CoreStore root-register anchor bytes are not canonical");
    }
    if cohort_node_ids.len() != 3 || usize::from(shard_index) >= cohort_node_ids.len() {
        bail!("CoreStore root-register-r3 requires exactly three indexed replicas");
    }
    let mut unique = cohort_node_ids.to_vec();
    unique.sort();
    unique.dedup();
    if unique.len() != cohort_node_ids.len() {
        bail!("CoreStore root-register cohort contains duplicate nodes");
    }
    for node_id in cohort_node_ids {
        validate_logical_id(node_id, "root-register cohort node id")?;
    }
    let expected_hash = root_register_cohort_hash(
        &anchor.root_key_hash,
        anchor.root_generation,
        cohort_node_ids,
    );
    if cohort_hash != expected_hash {
        bail!("CoreStore root-register cohort hash mismatch");
    }
    if placement_epoch == 0 {
        bail!("CoreStore root-register placement epoch must be nonzero");
    }
    Ok(())
}

fn root_register_partition_id(anchor: &CoreRootAnchorRecord) -> u64 {
    root_register_partition_id_for_hash(&anchor.root_key_hash).unwrap_or_default()
}

pub(super) fn root_register_partition_id_for_hash(root_hash: &str) -> Result<u64> {
    if root_hash == root_key_hash(core_transaction_root_anchor_key()) {
        return Ok(CORE_TRANSACTION_ROOT_PARTITION_ID);
    }
    decode_sha256_hash_bytes(root_hash)
        .map(|hash| u64::from_le_bytes(hash[..8].try_into().expect("eight-byte hash prefix")))
}

fn read_root_register_generation(
    directory: PathBuf,
    root_key_hash: &str,
    generation: u64,
    preferred_node_id: &str,
) -> Result<Option<RootRegisterShard>> {
    let Some(inspection) =
        inspect_root_register_generation(directory.clone(), root_key_hash, generation)?
    else {
        return Ok(None);
    };
    let entries = std::fs::read_dir(&directory)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("anr") {
            continue;
        }
        let shard = read_root_register_shard_bytes(&std::fs::read(&path)?)?;
        if shard
            .register_cohort_nodes
            .get(usize::from(shard.shard_index))
            .is_some_and(|node_id| node_id == preferred_node_id)
        {
            debug_assert_eq!(shard.root_anchor_hash, inspection.root_anchor_hash);
            return Ok(Some(shard));
        }
    }
    Ok(None)
}

pub(super) fn inspect_root_register_generation(
    directory: PathBuf,
    root_key_hash: &str,
    generation: u64,
) -> Result<Option<RootRegisterGenerationInspection>> {
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut shards = Vec::new();
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("anr") {
            continue;
        }
        let shard = read_root_register_shard_bytes(&std::fs::read(&path)?)?;
        if shard.root_key_hash != root_key_hash || shard.root_generation != generation {
            bail!("root-register shard path does not match its encoded scope");
        }
        shards.push(shard);
    }
    if shards.is_empty() {
        return Ok(None);
    }
    shards.sort_by_key(|shard| shard.shard_index);
    let reference = &shards[0];
    if shards.iter().skip(1).any(|shard| {
        shard.root_anchor_record != reference.root_anchor_record
            || shard.root_anchor_hash != reference.root_anchor_hash
            || shard.register_cohort_nodes != reference.register_cohort_nodes
            || shard.register_cohort_hash != reference.register_cohort_hash
            || shard.placement_epoch != reference.placement_epoch
    }) {
        bail!("root-register local generation contains conflicting shard records");
    }
    let shard_indexes = shards.iter().map(|shard| shard.shard_index).collect();
    Ok(Some(RootRegisterGenerationInspection {
        root_key_hash: reference.root_key_hash.clone(),
        root_generation: reference.root_generation,
        register_cohort_nodes: reference.register_cohort_nodes.clone(),
        register_cohort_hash: reference.register_cohort_hash.clone(),
        placement_epoch: reference.placement_epoch,
        root_anchor_record: reference.root_anchor_record.clone(),
        root_anchor_hash: reference.root_anchor_hash.clone(),
        shard_indexes,
    }))
}

pub(super) fn encode_root_register_shard(shard: &RootRegisterShard) -> Result<Vec<u8>> {
    validate_hash(&shard.root_key_hash, "root-register shard root key hash")?;
    validate_hash(
        &shard.root_anchor_hash,
        "root-register shard root anchor hash",
    )?;
    if shard.root_anchor_hash != format!("sha256:{}", sha256_hex(&shard.root_anchor_record)) {
        bail!("CoreStore root-register shard anchor hash mismatch");
    }
    let header = RootRegisterShardHeaderProto {
        root_key_hash: shard.root_key_hash.clone(),
        root_generation: shard.root_generation,
        register_cohort_nodes: shard.register_cohort_nodes.clone(),
        register_cohort_hash: shard.register_cohort_hash.clone(),
        placement_epoch: shard.placement_epoch,
        created_at_unix_nanos: shard.created_at_unix_nanos,
    };
    let header_bytes = encode_deterministic_proto(&header);
    let root_hash_bytes = decode_sha256_hash_bytes(&shard.root_key_hash)?;
    let mut encoded = Vec::with_capacity(
        ROOT_REGISTER_SHARD_MAGIC.len()
            + 2
            + 8
            + 32
            + 8
            + 2
            + 4
            + 8
            + header_bytes.len()
            + shard.root_anchor_record.len()
            + 4
            + 32,
    );
    encoded.extend_from_slice(ROOT_REGISTER_SHARD_MAGIC);
    encoded.extend_from_slice(&ROOT_REGISTER_SHARD_VERSION.to_le_bytes());
    encoded.extend_from_slice(&shard.root_partition_id.to_le_bytes());
    encoded.extend_from_slice(&root_hash_bytes);
    encoded.extend_from_slice(&shard.root_generation.to_le_bytes());
    encoded.extend_from_slice(&shard.shard_index.to_le_bytes());
    encoded.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
    encoded.extend_from_slice(&(shard.root_anchor_record.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&header_bytes);
    encoded.extend_from_slice(&shard.root_anchor_record);
    encoded.extend_from_slice(&crc32c(&encoded).to_le_bytes());
    let digest = sha256::digest_bytes(&encoded);
    encoded.extend_from_slice(&digest);
    Ok(encoded)
}

fn read_root_register_shard_bytes(bytes: &[u8]) -> Result<RootRegisterShard> {
    const FIXED_PREFIX: usize = 8 + 2 + 8 + 32 + 8 + 2 + 4 + 8;
    if bytes.len() < FIXED_PREFIX + 4 + 32 {
        bail!("CoreStore root-register shard is truncated");
    }
    let payload_end = bytes.len() - 32;
    let expected_digest = &bytes[payload_end..];
    if sha256::digest_bytes(&bytes[..payload_end]).as_slice() != expected_digest {
        bail!("CoreStore root-register shard sha256 mismatch");
    }
    let crc_offset = payload_end - 4;
    let expected_crc = u32::from_le_bytes(bytes[crc_offset..payload_end].try_into()?);
    if crc32c(&bytes[..crc_offset]) != expected_crc {
        bail!("CoreStore root-register shard crc32c mismatch");
    }

    let mut offset = 0usize;
    if take(bytes, &mut offset, 8)? != ROOT_REGISTER_SHARD_MAGIC {
        bail!("CoreStore root-register shard magic mismatch");
    }
    let version = u16::from_le_bytes(take(bytes, &mut offset, 2)?.try_into()?);
    if version != ROOT_REGISTER_SHARD_VERSION {
        bail!("CoreStore root-register shard version mismatch");
    }
    let root_partition_id = u64::from_le_bytes(take(bytes, &mut offset, 8)?.try_into()?);
    let root_hash_bytes: [u8; 32] = take(bytes, &mut offset, 32)?.try_into()?;
    let root_key_hash = format!("sha256:{}", hex::encode(root_hash_bytes));
    let root_generation = u64::from_le_bytes(take(bytes, &mut offset, 8)?.try_into()?);
    let shard_index = u16::from_le_bytes(take(bytes, &mut offset, 2)?.try_into()?);
    let header_len = u32::from_le_bytes(take(bytes, &mut offset, 4)?.try_into()?) as usize;
    let anchor_len = usize::try_from(u64::from_le_bytes(take(bytes, &mut offset, 8)?.try_into()?))?;
    let header_bytes = take(bytes, &mut offset, header_len)?;
    let header = decode_deterministic_proto::<RootRegisterShardHeaderProto>(
        header_bytes,
        "root-register shard header",
    )?;
    let root_anchor_record = take(bytes, &mut offset, anchor_len)?.to_vec();
    if offset != crc_offset {
        bail!("CoreStore root-register shard has trailing payload bytes");
    }
    if header.root_key_hash != root_key_hash || header.root_generation != root_generation {
        bail!("CoreStore root-register shard header scope mismatch");
    }
    if header.register_cohort_hash
        != root_register_cohort_hash(
            &root_key_hash,
            root_generation,
            &header.register_cohort_nodes,
        )
    {
        bail!("CoreStore root-register shard cohort hash mismatch");
    }
    Ok(RootRegisterShard {
        root_partition_id,
        root_key_hash,
        root_generation,
        shard_index,
        register_cohort_nodes: header.register_cohort_nodes,
        register_cohort_hash: header.register_cohort_hash,
        placement_epoch: header.placement_epoch,
        created_at_unix_nanos: header.created_at_unix_nanos,
        root_anchor_hash: format!("sha256:{}", sha256_hex(&root_anchor_record)),
        root_anchor_record,
    })
}

async fn persist_create_new_root_register_shard(path: PathBuf, bytes: Vec<u8>) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                if let Err(error) = file.write_all(&bytes).and_then(|_| file.sync_all()) {
                    drop(file);
                    let _ = std::fs::remove_file(&path);
                    return Err(error.into());
                }
                if let Some(parent) = path.parent() {
                    std::fs::File::open(parent)?.sync_all()?;
                }
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = std::fs::read(&path)?;
                if existing == bytes {
                    Ok(())
                } else {
                    bail!(
                        "CoreStore root-register generation is in doubt at {}",
                        path.display()
                    )
                }
            }
            Err(error) => Err(error.into()),
        }
    })
    .await
    .map_err(|error| anyhow!("CoreStore root-register write task failed: {error}"))?
}

fn append_hash_part(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow!("CoreStore root-register shard offset overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| anyhow!("CoreStore root-register shard is truncated"))?;
    *offset = end;
    Ok(value)
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

mod sha256 {
    use sha2::{Digest, Sha256};

    pub(super) fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_register_shard_round_trips_and_rejects_corruption() {
        let anchor_bytes = b"canonical-root-anchor".to_vec();
        let root_key_hash = format!("sha256:{}", sha256_hex(b"root-key"));
        let cohort = vec!["node-a".into(), "node-b".into(), "node-c".into()];
        let shard = RootRegisterShard {
            root_partition_id: 7,
            root_key_hash: root_key_hash.clone(),
            root_generation: 9,
            shard_index: 1,
            register_cohort_hash: root_register_cohort_hash(&root_key_hash, 9, &cohort),
            register_cohort_nodes: cohort,
            placement_epoch: 3,
            created_at_unix_nanos: 11,
            root_anchor_hash: format!("sha256:{}", sha256_hex(&anchor_bytes)),
            root_anchor_record: anchor_bytes,
        };
        let encoded = encode_root_register_shard(&shard).unwrap();
        assert_eq!(read_root_register_shard_bytes(&encoded).unwrap(), shard);

        let mut corrupt = encoded;
        corrupt[80] ^= 0x80;
        assert!(read_root_register_shard_bytes(&corrupt).is_err());
    }

    #[test]
    fn root_prepare_receipt_signature_scope_binds_placement_epoch() {
        let receipt = RootPrepareReceipt {
            replica_node_id: "node-a".into(),
            root_key_hash: format!("sha256:{}", sha256_hex(b"root-key")),
            expected_generation: 8,
            post_generation: 9,
            new_root_hash: format!("sha256:{}", sha256_hex(b"root-anchor")),
            shard_index: 0,
            register_cohort_hash: format!("sha256:{}", sha256_hex(b"cohort")),
            fsync_sequence: 1,
            signed_payload_hash: String::new(),
            signature: Vec::new(),
            placement_epoch: 3,
        };
        let mut different_epoch = receipt.clone();
        different_epoch.placement_epoch = 4;

        assert_ne!(
            root_prepare_receipt_payload_hash(&receipt),
            root_prepare_receipt_payload_hash(&different_epoch)
        );
    }

    #[test]
    fn exact_read_only_returns_the_preferred_nodes_encoded_slot() {
        let temporary = tempfile::tempdir().unwrap();
        let root_key_hash = format!("sha256:{}", sha256_hex(b"root-key"));
        let anchor_bytes = b"canonical-root-anchor".to_vec();
        let cohort = vec!["node-a".into(), "node-b".into(), "node-c".into()];
        for shard_index in 0..3_u16 {
            let shard = RootRegisterShard {
                root_partition_id: 7,
                root_key_hash: root_key_hash.clone(),
                root_generation: 9,
                shard_index,
                register_cohort_hash: root_register_cohort_hash(&root_key_hash, 9, &cohort),
                register_cohort_nodes: cohort.clone(),
                placement_epoch: 3,
                created_at_unix_nanos: 11,
                root_anchor_hash: format!("sha256:{}", sha256_hex(&anchor_bytes)),
                root_anchor_record: anchor_bytes.clone(),
            };
            std::fs::write(
                temporary.path().join(format!("shard-{shard_index}.anr")),
                encode_root_register_shard(&shard).unwrap(),
            )
            .unwrap();
        }

        assert!(
            read_root_register_generation(
                temporary.path().to_path_buf(),
                &root_key_hash,
                9,
                "node-outside-cohort",
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            read_root_register_generation(
                temporary.path().to_path_buf(),
                &root_key_hash,
                9,
                "node-b",
            )
            .unwrap()
            .unwrap()
            .shard_index,
            1
        );
    }

    #[tokio::test]
    async fn root_register_shard_repair_is_idempotent_and_conflicts_fail_closed() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("shard-0.anr");

        persist_create_new_root_register_shard(path.clone(), b"committed-shard".to_vec())
            .await
            .unwrap();
        persist_create_new_root_register_shard(path.clone(), b"committed-shard".to_vec())
            .await
            .unwrap();

        let error = persist_create_new_root_register_shard(path, b"conflicting-shard".to_vec())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("generation is in doubt"));
    }
}
