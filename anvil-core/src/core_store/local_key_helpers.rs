use super::*;
use crate::core_store::{
    CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
    core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
    decode_deterministic_proto, encode_deterministic_proto,
};
use crate::node_signing::{NodeSigningKeypair, NodeVerifyingKey, validate_public_key_bytes};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct NodeSigningKeypairRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(bytes, tag = "2")]
    secret_key_bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct NodeReceiptSigningPublicKeyRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(bytes, tag = "3")]
    public_key_bytes: Vec<u8>,
    #[prost(string, tag = "4")]
    public_key_hash: String,
    #[prost(uint64, tag = "5")]
    updated_at_unix_nanos: u64,
}

pub(super) fn admission_record_key(admission_shard_hash: &str, sequence: u64) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-record",
        admission_shard_hash.as_bytes(),
        &sequence.to_be_bytes(),
    ])
}

pub(super) fn all_admission_records_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"admission-record"])
}

pub(super) fn admission_record_prefix(admission_shard_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"admission-record", admission_shard_hash.as_bytes()])
}

pub(super) fn admission_evidence_key(admission_shard_hash: &str, sequence: u64) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-evidence",
        admission_shard_hash.as_bytes(),
        &sequence.to_be_bytes(),
    ])
}

pub(super) fn admission_finalisation_key(key: &CorePendingMutationKey) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-finalisation",
        key.admission_shard_hash.as_bytes(),
        key.node_id.as_bytes(),
        &key.mutation_epoch.to_be_bytes(),
        &key.mutation_sequence.to_be_bytes(),
    ])
}

pub(super) fn admission_finalisation_record_key(key: &CorePendingMutationKey) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-finalisation-record",
        key.admission_shard_hash.as_bytes(),
        key.node_id.as_bytes(),
        &key.mutation_epoch.to_be_bytes(),
        &key.mutation_sequence.to_be_bytes(),
    ])
}

pub(super) fn admission_sequence_key(admission_shard_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"admission-sequence", admission_shard_hash.as_bytes()])
}

pub(super) fn admission_point_state_key(admission_shard_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"admission-point-state", admission_shard_hash.as_bytes()])
}

pub(super) fn admission_point_state_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"admission-point-state"])
}

pub(super) fn admission_mutation_head_key(
    admission_shard_hash: &str,
    mutation_id: &str,
) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-mutation-head",
        admission_shard_hash.as_bytes(),
        mutation_id.as_bytes(),
    ])
}

pub(super) fn admission_idempotency_head_key(
    admission_shard_hash: &str,
    idempotency_key_hash: &str,
) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-idempotency-head",
        admission_shard_hash.as_bytes(),
        idempotency_key_hash.as_bytes(),
    ])
}

pub(super) fn landed_byte_ref_key(admission_shard_hash: &str, landing_id: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"landed-byte",
        admission_shard_hash.as_bytes(),
        landing_id.as_bytes(),
    ])
}

pub(super) fn landed_byte_ref_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"landed-byte"])
}

pub(super) fn landed_byte_head_key(admission_shard_hash: &str, sha256: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"landed-byte-head",
        admission_shard_hash.as_bytes(),
        sha256.as_bytes(),
    ])
}

pub(super) fn landed_byte_head_prefix(admission_shard_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"landed-byte-head", admission_shard_hash.as_bytes()])
}

pub(super) fn object_manifest_meta_key(object_ref: &CoreObjectRef) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-manifest",
        object_ref.manifest_ref.as_bytes(),
        object_ref.encoding.block_id.as_bytes(),
    ])
}

pub(super) fn object_manifest_meta_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"object-manifest"])
}

pub(super) fn inline_payload_meta_key(object_ref: &CoreObjectRef) -> Vec<u8> {
    meta_tuple_key(&[
        b"inline-payload",
        object_ref.hash.as_bytes(),
        &object_ref.logical_size.to_be_bytes(),
        object_ref.manifest_ref.as_bytes(),
    ])
}

pub(super) fn is_inline_object_ref(object_ref: &CoreObjectRef) -> bool {
    object_ref.encoding.profile_id == LOCAL_INLINE_PAYLOAD_PROFILE_ID
        && object_ref.encoding.placement_scope == "coremeta-inline"
}

pub(super) fn local_inline_payload_block_id(hash_hex: &str) -> String {
    format!("{LOCAL_INLINE_PAYLOAD_BLOCK_PREFIX}:{hash_hex}")
}

pub(super) fn root_cache_key(root_anchor_key: &str) -> Vec<u8> {
    meta_tuple_key(&[b"root-anchor", root_anchor_key.as_bytes()])
}

pub(super) fn root_cache_hash_key(root_key_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"root-anchor-hash", root_key_hash.as_bytes()])
}

pub(super) fn root_cache_hash_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"root-anchor-hash"])
}

pub(super) fn root_anchor_generation_key(root_key_hash: &str, generation: u64) -> Vec<u8> {
    meta_tuple_key(&[
        b"root-anchor-generation",
        root_key_hash.as_bytes(),
        &generation.to_be_bytes(),
    ])
}

pub(super) fn root_anchor_generation_prefix(root_key_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"root-anchor-generation", root_key_hash.as_bytes()])
}

pub(super) fn stream_head_key(stream_id: &str) -> Vec<u8> {
    meta_tuple_bytewise_key(b"stream-head", stream_id.as_bytes(), &[])
}

pub(super) fn stream_head_prefix(stream_prefix: &str) -> Vec<u8> {
    meta_tuple_bytewise_key(b"stream-head", stream_prefix.as_bytes(), &[])
}

pub(super) fn stream_record_key(stream_id: &str, sequence: u64) -> Vec<u8> {
    meta_tuple_bytewise_key(
        b"stream-record",
        stream_id.as_bytes(),
        &[&sequence.to_be_bytes()],
    )
}

pub(super) fn stream_record_prefix(stream_id: &str) -> Vec<u8> {
    meta_tuple_bytewise_key(b"stream-record", stream_id.as_bytes(), &[])
}

pub(super) fn pending_transaction_stream_key(
    stream_id: &str,
    transaction_id: &str,
    ordinal: u64,
) -> Vec<u8> {
    meta_tuple_bytewise_key(
        b"transaction-pending-stream",
        stream_id.as_bytes(),
        &[transaction_id.as_bytes(), &ordinal.to_be_bytes()],
    )
}

pub(super) fn pending_transaction_stream_prefix(stream_prefix: &str) -> Vec<u8> {
    meta_tuple_bytewise_key(b"transaction-pending-stream", stream_prefix.as_bytes(), &[])
}

pub(super) fn stream_idempotency_key(stream_id: &str, idempotency_key_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"stream-idempotency",
        stream_id.as_bytes(),
        idempotency_key_hash.as_bytes(),
    ])
}

pub(super) fn node_signing_keypair_key() -> Vec<u8> {
    meta_tuple_key(&[b"node-signing-keypair"])
}

pub(super) fn node_receipt_signing_public_key_key(node_id: &str) -> Vec<u8> {
    meta_tuple_key(&[b"node-receipt-signing-public-key", node_id.as_bytes()])
}

pub(super) fn load_or_create_node_signing_keypair(
    meta: &CoreMetaStore,
) -> Result<NodeSigningKeypair> {
    let key = node_signing_keypair_key();
    // Node identity is required before publication visibility can initialise.
    if let Some(bytes) = meta.get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &key)? {
        let row = decode_node_signing_keypair_row(&bytes)?;
        return NodeSigningKeypair::from_secret_key_bytes(&row.secret_key_bytes)
            .context("CoreStore node signing keypair metadata is invalid");
    }

    let keypair = NodeSigningKeypair::generate()?;
    let row = NodeSigningKeypairRowProto {
        common: Some(core_meta_committed_row_common(
            "system/corestore",
            core_meta_root_key_hash("node-signing-keypair"),
            1,
            "node-signing-keypair",
            u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default(),
        )),
        secret_key_bytes: keypair.secret_key_bytes().to_vec(),
    };
    let bytes = encode_deterministic_proto(&row);
    meta.write_local_committed_batch(&[CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_NODE_SIGNING_KEYPAIR_ROW,
        tuple_key: &key,
        common: row.common.clone(),
        kind: CoreMetaBatchOpKind::Put(&bytes),
    }])?;
    Ok(keypair)
}

pub(super) fn node_receipt_signing_public_key_hash(public_key_bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"anvil.corestore.node_receipt_signing_public_key.v1");
    hasher.update([0]);
    hasher.update(public_key_bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub(super) fn node_admission_mutation_epoch(public_key_bytes: &[u8]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"anvil.corestore.admission_mutation_epoch.v1");
    hasher.update([0]);
    hasher.update(public_key_bytes);
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes).max(1)
}

pub(super) fn store_node_receipt_signing_public_key(
    meta: &CoreMetaStore,
    node_id: &str,
    public_key_bytes: &[u8],
) -> Result<String> {
    let public_key_hash = validate_node_receipt_signing_public_key(node_id, public_key_bytes)?;
    let key = node_receipt_signing_public_key_key(node_id);
    if let Some(existing_bytes) = meta.get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &key)? {
        let existing = decode_node_receipt_signing_public_key_row(node_id, &existing_bytes)?;
        if existing.public_key_hash == public_key_hash
            && existing.public_key_bytes == public_key_bytes
        {
            return Ok(public_key_hash);
        }
        bail!("CoreStore receipt signing identity replacement rejected for node {node_id}");
    }
    write_node_receipt_signing_public_key(meta, node_id, public_key_bytes, &public_key_hash)?;
    Ok(public_key_hash)
}

pub(super) fn seed_node_receipt_signing_public_key_if_absent(
    meta: &CoreMetaStore,
    node_id: &str,
    public_key_bytes: &[u8],
) -> Result<String> {
    let public_key_hash = validate_node_receipt_signing_public_key(node_id, public_key_bytes)?;
    let key = node_receipt_signing_public_key_key(node_id);
    if let Some(existing_bytes) = meta.get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &key)? {
        let existing = decode_node_receipt_signing_public_key_row(node_id, &existing_bytes)?;
        return Ok(existing.public_key_hash);
    }
    write_node_receipt_signing_public_key(meta, node_id, public_key_bytes, &public_key_hash)?;
    Ok(public_key_hash)
}

fn validate_node_receipt_signing_public_key(
    node_id: &str,
    public_key_bytes: &[u8],
) -> Result<String> {
    validate_logical_id(node_id, "node receipt signing public key node id")?;
    validate_public_key_bytes(public_key_bytes)
        .context("node receipt signing public key is invalid")?;
    Ok(node_receipt_signing_public_key_hash(public_key_bytes))
}

fn write_node_receipt_signing_public_key(
    meta: &CoreMetaStore,
    node_id: &str,
    public_key_bytes: &[u8],
    public_key_hash: &str,
) -> Result<()> {
    let key = node_receipt_signing_public_key_key(node_id);
    let updated_at_unix_nanos =
        u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default();
    let row = NodeReceiptSigningPublicKeyRowProto {
        // Public receipt keys are generation-zero bootstrap evidence. Making
        // them depend on a root whose receipts they are needed to verify would
        // create a circular visibility dependency during cluster formation.
        common: Some(core_meta_bootstrap_row_common(
            "system/corestore",
            updated_at_unix_nanos,
        )),
        node_id: node_id.to_string(),
        public_key_bytes: public_key_bytes.to_vec(),
        public_key_hash: public_key_hash.to_string(),
        updated_at_unix_nanos,
    };
    let bytes = encode_deterministic_proto(&row);
    meta.write_local_committed_batch(&[CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_NODE_SIGNING_KEYPAIR_ROW,
        tuple_key: &key,
        common: row.common.clone(),
        kind: CoreMetaBatchOpKind::Put(&bytes),
    }])?;
    Ok(())
}

pub(super) fn load_node_receipt_signing_public_key(
    meta: &CoreMetaStore,
    node_id: &str,
) -> Result<Option<NodeVerifyingKey>> {
    validate_logical_id(node_id, "node receipt signing public key node id")?;
    // Receipt verification keys bootstrap the visibility machinery they authenticate.
    let Some(bytes) = meta.get(
        CF_MESH,
        TABLE_NODE_SIGNING_KEYPAIR_ROW,
        &node_receipt_signing_public_key_key(node_id),
    )?
    else {
        return Ok(None);
    };
    let row = decode_node_receipt_signing_public_key_row(node_id, &bytes)?;
    Ok(Some(
        NodeVerifyingKey::from_bytes(&row.public_key_bytes)
            .context("stored node receipt signing public key is invalid")?,
    ))
}

fn decode_node_signing_keypair_row(bytes: &[u8]) -> Result<NodeSigningKeypairRowProto> {
    let row = decode_deterministic_proto::<NodeSigningKeypairRowProto>(
        bytes,
        "node signing keypair CoreMeta row",
    )?;
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("node signing keypair row missing CoreMeta common"))?;
    if common.realm_id != "system/corestore" {
        bail!("node signing keypair row realm mismatch");
    }
    if common.root_key_hash != core_meta_root_key_hash("node-signing-keypair") {
        bail!("node signing keypair row root mismatch");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("node signing keypair row is not committed");
    }
    if row.secret_key_bytes.is_empty() {
        bail!("node signing keypair row payload is empty");
    }
    Ok(row)
}

fn decode_node_receipt_signing_public_key_row(
    expected_node_id: &str,
    bytes: &[u8],
) -> Result<NodeReceiptSigningPublicKeyRowProto> {
    let row = decode_deterministic_proto::<NodeReceiptSigningPublicKeyRowProto>(
        bytes,
        "node receipt signing public key CoreMeta row",
    )?;
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("node receipt signing public key row missing CoreMeta common"))?;
    if common.realm_id != "system/corestore" {
        bail!("node receipt signing public key row realm mismatch");
    }
    if !common.root_key_hash.is_empty()
        || common.root_generation != 0
        || !common.transaction_id.is_empty()
    {
        bail!("node receipt signing public key row must be generation-zero bootstrap evidence");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("node receipt signing public key row is not committed");
    }
    if row.node_id != expected_node_id {
        bail!("node receipt signing public key row node id mismatch");
    }
    if row.public_key_bytes.is_empty() {
        bail!("node receipt signing public key row payload is empty");
    }
    validate_public_key_bytes(&row.public_key_bytes)
        .context("node receipt signing public key row payload is invalid")?;
    if row.public_key_hash != node_receipt_signing_public_key_hash(&row.public_key_bytes) {
        bail!("node receipt signing public key row hash mismatch");
    }
    Ok(row)
}

pub(super) fn meta_tuple_key(parts: &[&[u8]]) -> Vec<u8> {
    let parts = parts
        .iter()
        .map(|part| CoreMetaTuplePart::Raw(part))
        .collect::<Vec<_>>();
    core_meta_tuple_key(&parts).expect("CoreStore raw metadata tuple must be valid")
}

fn meta_tuple_bytewise_key(namespace: &[u8], value: &[u8], suffix: &[&[u8]]) -> Vec<u8> {
    let mut parts = Vec::with_capacity(1 + value.len() + suffix.len());
    parts.push(CoreMetaTuplePart::Raw(namespace));
    parts.extend(
        value
            .iter()
            .map(|byte| CoreMetaTuplePart::Raw(std::slice::from_ref(byte))),
    );
    parts.extend(suffix.iter().map(|part| CoreMetaTuplePart::Raw(part)));
    core_meta_tuple_key(&parts).expect("CoreStore bytewise metadata tuple must be valid")
}

pub(super) fn meta_tuple_utf8(parts: &[&str]) -> Vec<u8> {
    let parts = parts
        .iter()
        .map(|part| CoreMetaTuplePart::Utf8(part))
        .collect::<Vec<_>>();
    core_meta_tuple_key(&parts).expect("CoreStore UTF-8 metadata tuple must be valid")
}

pub(super) fn meta_tuple_u64(parts: &[u64]) -> Vec<u8> {
    let parts = parts
        .iter()
        .copied()
        .map(CoreMetaTuplePart::U64)
        .collect::<Vec<_>>();
    core_meta_tuple_key(&parts).expect("CoreStore u64 metadata tuple must be valid")
}

pub(super) fn decode_u64_le(bytes: &[u8], label: &str) -> Result<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow!("CoreStore {label} must be 8 bytes"))?;
    Ok(u64::from_le_bytes(array))
}

pub(super) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
