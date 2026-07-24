use super::local_stream_control::control_record_proto::encode_root_catalog_record;
use super::*;
use crate::core_store::{
    CoreMetaRowCommonProto, CoreMetaVisibilityState, core_meta_committed_row_common,
    decode_deterministic_proto, encode_deterministic_proto,
};

pub(super) fn core_transaction_root_anchor_key() -> &'static str {
    "system/core-control/0"
}

pub(super) fn root_key_hash(root_anchor_key: &str) -> String {
    descriptor_hash(&["anvil.root.key.v1", root_anchor_key])
}

#[derive(Clone, PartialEq, prost::Message)]
struct RootCacheRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(bytes, tag = "2")]
    root_anchor_record: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CoreGenesisMeshControlSegmentProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, repeated, tag = "2")]
    root_partitions: Vec<CoreGenesisPartitionProto>,
    #[prost(uint64, tag = "3")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CoreGenesisAuthzReservedSchemaSegmentProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    realm_id: String,
    #[prost(bool, tag = "3")]
    reserved: bool,
    #[prost(uint64, tag = "4")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CoreGenesisConfigProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, repeated, tag = "2")]
    initial_root_keys: Vec<String>,
    #[prost(message, repeated, tag = "3")]
    initial_partition_map: Vec<CoreGenesisPartitionProto>,
    #[prost(string, tag = "4")]
    mesh_control_segment_hash: String,
    #[prost(string, tag = "5")]
    authz_reserved_schema_segment_hash: String,
    #[prost(uint64, tag = "6")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CoreGenesisPartitionProto {
    #[prost(string, tag = "1")]
    root_anchor_key: String,
    #[prost(uint64, tag = "2")]
    root_partition_id: u64,
    #[prost(string, tag = "3")]
    owner_node_id: String,
    #[prost(uint64, tag = "4")]
    owner_epoch: u64,
    #[prost(uint64, tag = "5")]
    owner_fence: u64,
}

fn core_genesis_partition_to_proto(value: &CoreGenesisPartition) -> CoreGenesisPartitionProto {
    CoreGenesisPartitionProto {
        root_anchor_key: value.root_anchor_key.clone(),
        root_partition_id: value.root_partition_id,
        owner_node_id: value.owner_node_id.clone(),
        owner_epoch: value.owner_epoch,
        owner_fence: value.owner_fence,
    }
}

fn encode_genesis_mesh_control_segment(
    root_partitions: &[CoreGenesisPartition],
    created_at_unix_nanos: u64,
) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(
        &CoreGenesisMeshControlSegmentProto {
            schema: "anvil.core.genesis_mesh_control.v1".to_string(),
            root_partitions: root_partitions
                .iter()
                .map(core_genesis_partition_to_proto)
                .collect(),
            created_at_unix_nanos,
        },
    ))
}

fn encode_genesis_authz_reserved_schema_segment(
    realm_id: &str,
    created_at_unix_nanos: u64,
) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(
        &CoreGenesisAuthzReservedSchemaSegmentProto {
            schema: "anvil.core.genesis_authz_reserved_schema.v1".to_string(),
            realm_id: realm_id.to_string(),
            reserved: true,
            created_at_unix_nanos,
        },
    ))
}

pub(super) fn build_core_genesis_bundle(root_anchor_key: &str) -> Result<CoreGenesisBundle> {
    let initial_partition_map = vec![CoreGenesisPartition {
        root_anchor_key: root_anchor_key.to_string(),
        root_partition_id: CORE_TRANSACTION_ROOT_PARTITION_ID,
        owner_node_id: "genesis".to_string(),
        owner_epoch: 0,
        owner_fence: 0,
    }];
    let mesh_control_segment = encode_genesis_mesh_control_segment(&initial_partition_map, 0)?;
    let authz_reserved_schema_segment = encode_genesis_authz_reserved_schema_segment("system", 0)?;
    let genesis_config_hash = genesis_config_hash(
        root_anchor_key,
        &initial_partition_map,
        &mesh_control_segment,
        &authz_reserved_schema_segment,
    )?;
    Ok(CoreGenesisBundle {
        schema: "anvil.core.genesis_bundle.v1".to_string(),
        genesis_config_hash,
        mesh_control_segment,
        authz_reserved_schema_segment,
        initial_root_keys: vec![root_anchor_key.to_string()],
        initial_partition_map,
        created_at_unix_nanos: 0,
    })
}

fn genesis_config_hash(
    root_anchor_key: &str,
    initial_partition_map: &[CoreGenesisPartition],
    mesh_control_segment: &[u8],
    authz_reserved_schema_segment: &[u8],
) -> Result<String> {
    let config = CoreGenesisConfigProto {
        schema: "anvil.core.genesis_config.v1".to_string(),
        initial_root_keys: vec![root_anchor_key.to_string()],
        initial_partition_map: initial_partition_map
            .iter()
            .map(core_genesis_partition_to_proto)
            .collect(),
        mesh_control_segment_hash: domain_hash_bytes(
            "anvil.core.genesis_mesh_control.v1",
            mesh_control_segment,
        ),
        authz_reserved_schema_segment_hash: domain_hash_bytes(
            "anvil.core.genesis_authz_reserved_schema.v1",
            authz_reserved_schema_segment,
        ),
        created_at_unix_nanos: 0,
    };
    Ok(domain_hash_bytes(
        "anvil.root.genesis_config.v1",
        &encode_deterministic_proto(&config),
    ))
}

pub(super) fn validate_core_genesis_bundle(
    bundle: &CoreGenesisBundle,
    root_anchor_key: &str,
) -> Result<()> {
    if bundle.schema != "anvil.core.genesis_bundle.v1" {
        bail!("CoreStore genesis bundle has invalid schema");
    }
    validate_hash(&bundle.genesis_config_hash, "genesis config hash")?;
    if bundle.created_at_unix_nanos != 0 {
        bail!("CoreStore genesis bundle timestamp must be zero");
    }
    if bundle.mesh_control_segment.is_empty() || bundle.authz_reserved_schema_segment.is_empty() {
        bail!("CoreStore genesis bundle must include embedded mesh and authz segments");
    }
    if bundle.initial_root_keys != vec![root_anchor_key.to_string()] {
        bail!("CoreStore genesis bundle initial root keys mismatch");
    }
    if bundle.initial_partition_map.is_empty() {
        bail!("CoreStore genesis bundle must include an initial partition map");
    }
    let expected = build_core_genesis_bundle(root_anchor_key)?;
    if bundle != &expected {
        bail!("CoreStore genesis bundle does not match canonical bootstrap config");
    }
    Ok(())
}

pub(super) fn validate_root_anchor_record(anchor: &CoreRootAnchorRecord) -> Result<()> {
    if anchor.schema != "anvil.core.root_anchor.v1" {
        bail!("CoreStore root anchor has invalid schema");
    }
    validate_root_anchor_key(&anchor.root_anchor_key)?;
    let expected_root_key_hash = root_key_hash(&anchor.root_anchor_key);
    if anchor.root_key_hash != expected_root_key_hash {
        bail!("CoreStore root anchor key hash mismatch");
    }
    validate_hash(&anchor.root_key_hash, "root key hash")?;
    validate_hash(&anchor.previous_root_hash, "previous root hash")?;
    if anchor.root_state != "committed" {
        bail!("CoreStore root anchor state must be committed");
    }
    if anchor.root_generation == 0 {
        if anchor.root_anchor_key != core_transaction_root_anchor_key() {
            bail!("CoreStore generation-zero root anchor is reserved for system bootstrap");
        }
        if anchor.previous_root_hash != ZERO_HASH {
            bail!("CoreStore genesis root anchor previous hash must be zero");
        }
        if anchor.transaction_manifest.is_some() || anchor.checkpoint_manifest.is_some() {
            bail!("CoreStore genesis root anchor must not include manifest refs");
        }
        if anchor.core_meta_commit_certificate_hash.is_some()
            || !anchor.certificate_persist_receipt_hashes.is_empty()
        {
            bail!("CoreStore genesis root anchor must not include commit evidence");
        }
        if anchor.publisher_node_id != "genesis"
            || anchor.publisher_epoch != 0
            || anchor.partition_owner_fence != 0
            || anchor.created_at_unix_nanos != 0
        {
            bail!("CoreStore genesis root anchor must use canonical sentinel values");
        }
        if anchor.mutation_first.as_deref() != Some("genesis")
            || anchor.mutation_last.as_deref() != Some("genesis")
        {
            bail!("CoreStore genesis root anchor must use genesis mutation sentinels");
        }
        if anchor.writer_families
            != vec![
                "mesh_control".to_string(),
                "authz".to_string(),
                "core_control".to_string(),
            ]
        {
            bail!("CoreStore genesis root anchor writer families are invalid");
        }
        if anchor.manifest_count != 0 || anchor.final_block_count != 0 {
            bail!("CoreStore genesis root anchor manifest counters must be zero");
        }
        let bundle = anchor
            .genesis_bundle
            .as_ref()
            .ok_or_else(|| anyhow!("CoreStore genesis root anchor must include genesis bundle"))?;
        validate_core_genesis_bundle(bundle, &anchor.root_anchor_key)?;
        return Ok(());
    }

    if anchor.transaction_manifest.is_none() {
        bail!("CoreStore non-genesis root anchor must include a transaction manifest");
    }
    let certificate_hash = anchor
        .core_meta_commit_certificate_hash
        .as_deref()
        .ok_or_else(|| anyhow!("CoreStore non-genesis root anchor must include commit evidence"))?;
    validate_coremeta_digest(certificate_hash, "root anchor commit certificate hash")?;
    validate_certificate_persist_receipts(&anchor.certificate_persist_receipt_hashes)?;
    if anchor.genesis_bundle.is_some() {
        bail!("CoreStore non-genesis root anchor must not include a genesis bundle");
    }
    if anchor.publisher_node_id.is_empty() || anchor.publisher_node_id == "genesis" {
        bail!("CoreStore non-genesis root anchor publisher node id is invalid");
    }
    if anchor.publisher_epoch == 0 || anchor.partition_owner_fence == 0 {
        bail!("CoreStore root anchor publisher epoch and owner fence must be nonzero");
    }
    if anchor.created_at_unix_nanos == 0 {
        bail!("CoreStore non-genesis root anchor timestamp must be nonzero");
    }
    if let Some(locator) = &anchor.transaction_manifest {
        validate_manifest_locator(locator)?;
    }
    if let Some(locator) = &anchor.checkpoint_manifest {
        validate_manifest_locator(locator)?;
    }
    Ok(())
}

fn validate_root_anchor_key(root_anchor_key: &str) -> Result<()> {
    if root_anchor_key.is_empty()
        || root_anchor_key.len() > 1024
        || root_anchor_key.starts_with('/')
        || root_anchor_key.ends_with('/')
        || root_anchor_key
            .split('/')
            .any(|part| part.is_empty() || part.chars().any(char::is_control))
    {
        bail!("CoreStore root anchor key is not canonical");
    }
    Ok(())
}

pub(super) fn validate_transaction_manifest_record(
    transaction: &CoreTransactionManifestRecord,
    expected_root_generation: u64,
) -> Result<()> {
    if transaction.schema != "anvil.core.transaction_manifest.v1" {
        bail!("CoreStore transaction manifest has invalid schema");
    }
    if transaction.post_root_generation != expected_root_generation {
        bail!("CoreStore transaction manifest post_root_generation does not match root anchor");
    }
    if transaction.post_root_generation != transaction.pre_root_generation.saturating_add(1) {
        bail!("CoreStore transaction manifest root generations must be contiguous");
    }
    validate_hash(
        &transaction.root_key_hash,
        "transaction manifest root key hash",
    )?;
    match (
        transaction.coordinator_root_key_hash.as_deref(),
        transaction.coordinator_root_generation,
    ) {
        (Some(root_key_hash), Some(root_generation)) => {
            validate_hash(
                root_key_hash,
                "transaction manifest coordinator root key hash",
            )?;
            if root_generation == 0 {
                bail!("CoreStore transaction manifest coordinator generation must be nonzero");
            }
        }
        (None, None) => {}
        _ => {
            bail!(
                "CoreStore transaction manifest coordinator root hash and generation must be specified together"
            );
        }
    }
    if transaction.mutation_ids.is_empty() {
        bail!("CoreStore transaction manifest must include at least one mutation id");
    }
    let mut mutation_ids = transaction.mutation_ids.clone();
    mutation_ids.sort();
    mutation_ids.dedup();
    if mutation_ids != transaction.mutation_ids {
        bail!("CoreStore transaction manifest mutation ids must be sorted and unique");
    }
    let mut idempotency_hashes = transaction.idempotency_key_hashes.clone();
    idempotency_hashes.sort();
    idempotency_hashes.dedup();
    if idempotency_hashes != transaction.idempotency_key_hashes {
        bail!("CoreStore transaction manifest idempotency hashes must be sorted and unique");
    }
    for hash in &transaction.idempotency_key_hashes {
        validate_hash(hash, "transaction manifest idempotency key hash")?;
    }
    for locator in &transaction.logical_manifests {
        validate_manifest_locator(locator)?;
    }
    Ok(())
}

fn validate_certificate_persist_receipts(receipts: &[String]) -> Result<()> {
    if receipts.is_empty() {
        bail!("CoreStore commit evidence must include certificate persist receipts");
    }
    let mut sorted = receipts.to_vec();
    sorted.sort();
    sorted.dedup();
    if sorted.len() != receipts.len() || sorted != receipts {
        bail!("CoreStore certificate persist receipt hashes must be sorted and unique");
    }
    for receipt in receipts {
        validate_coremeta_digest(receipt, "certificate persist receipt hash")?;
    }
    Ok(())
}

fn validate_coremeta_digest(value: &str, label: &str) -> Result<()> {
    let Some((algorithm, digest)) = value.split_once(':') else {
        bail!("CoreStore {label} must be an algorithm:hex digest");
    };
    if !matches!(algorithm, "sha256" | "blake3")
        || digest.len() != 64
        || !digest.as_bytes().iter().all(u8::is_ascii_hexdigit)
    {
        bail!("CoreStore {label} must be a sha256 or blake3 digest");
    }
    Ok(())
}

pub(super) fn encode_transaction_manifest_record(
    transaction: &CoreTransactionManifestRecord,
) -> Result<Vec<u8>> {
    let header_proto = encode_transaction_manifest_header_proto(transaction)?;
    let body_proto = encode_transaction_manifest_body_proto(transaction)?;
    let mut out = Vec::with_capacity(
        CORE_TRANSACTION_MANIFEST_MAGIC.len()
            + 2
            + 4
            + 8
            + header_proto.len()
            + body_proto.len()
            + 4,
    );
    out.extend_from_slice(CORE_TRANSACTION_MANIFEST_MAGIC);
    out.extend_from_slice(&CORE_TRANSACTION_MANIFEST_VERSION.to_le_bytes());
    out.extend_from_slice(&(header_proto.len() as u32).to_le_bytes());
    out.extend_from_slice(&(body_proto.len() as u64).to_le_bytes());
    out.extend_from_slice(&header_proto);
    out.extend_from_slice(&body_proto);
    let mut crc_input = Vec::with_capacity(header_proto.len() + body_proto.len());
    crc_input.extend_from_slice(&header_proto);
    crc_input.extend_from_slice(&body_proto);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    Ok(out)
}

pub(super) fn decode_transaction_manifest_record(
    bytes: &[u8],
) -> Result<CoreTransactionManifestRecord> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_TRANSACTION_MANIFEST_MAGIC.len())?;
    if magic != CORE_TRANSACTION_MANIFEST_MAGIC {
        bail!("CoreStore transaction manifest has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_TRANSACTION_MANIFEST_VERSION {
        bail!("CoreStore transaction manifest has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let body_len = read_u64_le(bytes, &mut offset)? as usize;
    let header_proto = read_exact(bytes, &mut offset, header_len)?;
    let body_proto = read_exact(bytes, &mut offset, body_len)?;
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    if offset != bytes.len() {
        bail!("CoreStore transaction manifest has trailing bytes");
    }
    let mut crc_input = Vec::with_capacity(header_proto.len() + body_proto.len());
    crc_input.extend_from_slice(header_proto);
    crc_input.extend_from_slice(body_proto);
    if crc32c(&crc_input) != expected_crc {
        bail!("CoreStore transaction manifest checksum mismatch");
    }
    decode_transaction_manifest_proto(header_proto, body_proto)
}

pub(super) fn hash_root_anchor_record(anchor: &CoreRootAnchorRecord) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_root_anchor_record(anchor)?)
    ))
}

pub(crate) fn encode_root_anchor_record(anchor: &CoreRootAnchorRecord) -> Result<Vec<u8>> {
    validate_root_anchor_record(anchor)?;
    let (header_proto, body_proto) = encode_root_anchor_proto(anchor)?;
    let mut out = Vec::with_capacity(
        CORE_ROOT_ANCHOR_MAGIC.len() + 2 + 4 + 8 + header_proto.len() + body_proto.len() + 4 + 32,
    );
    out.extend_from_slice(CORE_ROOT_ANCHOR_MAGIC);
    out.extend_from_slice(&CORE_ROOT_ANCHOR_VERSION.to_le_bytes());
    out.extend_from_slice(&(header_proto.len() as u32).to_le_bytes());
    out.extend_from_slice(&(body_proto.len() as u64).to_le_bytes());
    out.extend_from_slice(&header_proto);
    out.extend_from_slice(&body_proto);
    out.extend_from_slice(&crc32c(&out).to_le_bytes());
    let file_hash = Sha256::digest(&out);
    out.extend_from_slice(&file_hash);
    Ok(out)
}

pub(super) fn encode_root_cache_row(anchor: &CoreRootAnchorRecord) -> Result<Vec<u8>> {
    let root_anchor_record = encode_root_anchor_record(anchor)?;
    let common_root_key_hash = if anchor.root_generation == 0 {
        String::new()
    } else {
        anchor.root_key_hash.clone()
    };
    Ok(encode_deterministic_proto(&RootCacheRowProto {
        common: Some(core_meta_committed_row_common(
            "system/root-cache",
            common_root_key_hash,
            anchor.root_generation,
            anchor.mutation_last.clone().unwrap_or_else(|| {
                format!(
                    "root-cache:{}:{}",
                    anchor.root_anchor_key, anchor.root_generation
                )
            }),
            anchor.created_at_unix_nanos,
        )),
        root_anchor_record,
    }))
}

pub(crate) fn decode_root_anchor_record(bytes: &[u8]) -> Result<CoreRootAnchorRecord> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_ROOT_ANCHOR_MAGIC.len())?;
    if magic != CORE_ROOT_ANCHOR_MAGIC {
        bail!("CoreStore root anchor has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_ROOT_ANCHOR_VERSION {
        bail!("CoreStore root anchor has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let body_len = read_u64_le(bytes, &mut offset)? as usize;
    let header_proto = read_exact(bytes, &mut offset, header_len)?;
    let body_proto = read_exact(bytes, &mut offset, body_len)?;
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    if crc32c(&bytes[..offset - 4]) != expected_crc {
        bail!("CoreStore root anchor checksum mismatch");
    }
    let hash_start = offset;
    let expected_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore root anchor has trailing bytes");
    }
    let actual_hash = Sha256::digest(&bytes[..hash_start]);
    let actual_hash: &[u8] = actual_hash.as_ref();
    if expected_hash != actual_hash {
        bail!("CoreStore root anchor file hash mismatch");
    }
    let anchor = decode_root_anchor_proto(header_proto, body_proto)?;
    validate_root_anchor_record(&anchor)?;
    Ok(anchor)
}

pub(super) fn decode_root_cache_row(bytes: &[u8]) -> Result<CoreRootAnchorRecord> {
    let row = decode_deterministic_proto::<RootCacheRowProto>(bytes, "root cache CoreMeta row")?;
    let common = row
        .common
        .ok_or_else(|| anyhow!("root cache CoreMeta row missing common metadata"))?;
    let anchor = decode_root_anchor_record(&row.root_anchor_record)?;
    if common.realm_id != "system/root-cache" {
        bail!("root cache CoreMeta row realm mismatch");
    }
    if anchor.root_generation == 0 {
        if !common.root_key_hash.is_empty() {
            bail!("genesis root cache CoreMeta row root must be empty");
        }
    } else if common.root_key_hash != anchor.root_key_hash {
        bail!("root cache CoreMeta row root mismatch");
    }
    if common.root_generation != anchor.root_generation {
        bail!("root cache CoreMeta row generation mismatch");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("root cache CoreMeta row is not committed");
    }
    Ok(anchor)
}

pub(super) fn domain_hash_bytes(domain: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_le_bytes());
    hasher.update(domain.as_bytes());
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub(super) fn decode_sha256_hash_bytes(hash: &str) -> Result<[u8; 32]> {
    let raw = strip_sha256_prefix(hash)?;
    let bytes = hex::decode(raw)?;
    Ok(bytes
        .try_into()
        .map_err(|_| anyhow!("CoreStore hash did not decode to 32 bytes"))?)
}

pub(super) fn root_catalog_region(catalog: &CoreRootCatalog) -> String {
    catalog
        .root_partitions
        .first()
        .map(|partition| partition.embedded_head_segment_manifest.region_id.clone())
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "local".to_string())
}

pub(super) fn validate_root_partition(partition: &CoreRootPartition) -> Result<()> {
    validate_logical_id(&partition.partition_id, "root partition id")?;
    validate_logical_id(&partition.owner_node_id, "root partition owner node id")?;
    validate_logical_id(&partition.placement_group, "root partition placement group")?;
    if partition.fence == 0 {
        bail!("CoreStore root partition fence must be nonzero");
    }
    if partition.embedded_head_segment_manifest.schema != CORE_OBJECT_MANIFEST_SCHEMA {
        bail!("CoreStore root partition embedded manifest has invalid schema");
    }
    if partition
        .embedded_head_segment_manifest
        .placements
        .is_empty()
    {
        bail!("CoreStore root partition embedded manifest must include placements");
    }
    Ok(())
}

pub(super) fn validate_quorum_profile(profile: &CoreQuorumProfile) -> Result<()> {
    validate_logical_id(&profile.placement_group, "placement group")?;
    if profile.schema != CORE_QUORUM_PROFILE_SCHEMA {
        bail!("CoreStore quorum profile has invalid schema");
    }
    if profile.epoch == 0 {
        bail!("CoreStore quorum profile epoch must be nonzero");
    }
    if profile.replica_count == 0 {
        bail!("CoreStore quorum profile replica_count must be nonzero");
    }
    validate_quorum_member("write_quorum", profile.write_quorum, profile.replica_count)?;
    validate_quorum_member("read_quorum", profile.read_quorum, profile.replica_count)?;
    validate_quorum_member("fence_quorum", profile.fence_quorum, profile.replica_count)?;
    require_quorum_intersection(
        "read_quorum",
        profile.read_quorum,
        "write_quorum",
        profile.write_quorum,
        profile.replica_count,
    )?;
    require_quorum_intersection(
        "fence_quorum",
        profile.fence_quorum,
        "write_quorum",
        profile.write_quorum,
        profile.replica_count,
    )?;
    require_quorum_intersection(
        "fence_quorum",
        profile.fence_quorum,
        "read_quorum",
        profile.read_quorum,
        profile.replica_count,
    )?;
    Ok(())
}

pub(super) fn validate_quorum_member(label: &str, value: u16, replica_count: u16) -> Result<()> {
    if value == 0 {
        bail!("CoreStore quorum profile {label} must be nonzero");
    }
    if value > replica_count {
        bail!(
            "CoreStore quorum profile {label} {} exceeds replica_count {}",
            value,
            replica_count
        );
    }
    Ok(())
}

pub(super) fn require_quorum_intersection(
    left_label: &str,
    left: u16,
    right_label: &str,
    right: u16,
    replica_count: u16,
) -> Result<()> {
    if u32::from(left) + u32::from(right) <= u32::from(replica_count) {
        bail!(
            "CoreStore quorum profile {left_label}/{right_label} do not intersect for replica_count {}",
            replica_count
        );
    }
    Ok(())
}

pub(super) fn hash_root_catalog(catalog: &CoreRootCatalog) -> Result<String> {
    let mut unsigned = catalog.clone();
    unsigned.signature.clear();
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_root_catalog_record(&unsigned)?)
    ))
}

pub(super) fn sign_root_catalog(signing_key: &[u8], catalog: &CoreRootCatalog) -> Result<String> {
    if signing_key.is_empty() {
        bail!("CoreStore root catalog signing key must not be empty");
    }
    let hash = hash_root_catalog(catalog)?;
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"core_root_catalog");
    mac.update(catalog.mesh_id.as_bytes());
    mac.update(&catalog.generation.to_le_bytes());
    mac.update(catalog.previous_hash.as_bytes());
    mac.update(catalog.signed_by.as_bytes());
    mac.update(hash.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

pub(super) fn verify_root_catalog(catalog: &CoreRootCatalog, signing_key: &[u8]) -> Result<()> {
    if catalog.schema != CORE_ROOT_CATALOG_SCHEMA {
        bail!("CoreStore root catalog has invalid schema");
    }
    if catalog.signature.is_empty() {
        bail!("CoreStore root catalog signature must not be empty");
    }
    if catalog.root_partitions.is_empty() {
        bail!("CoreStore root catalog must include root partitions");
    }
    for partition in &catalog.root_partitions {
        validate_root_partition(partition)?;
    }
    let expected = sign_root_catalog(signing_key, catalog)?;
    if catalog.signature != expected {
        bail!("CoreStore root catalog signature mismatch");
    }
    Ok(())
}
