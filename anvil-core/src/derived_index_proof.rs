use crate::{
    core_store::{
        CF_INDEX_ROWS, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
        TABLE_DERIVED_INDEX_PROOF_ROW, core_meta_root_key_hash, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::{ProductMutation, coremeta_logical_key},
    mvcc_transaction::PredicateKind,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

const MAX_DERIVED_INDEX_SEGMENT_HASHES: usize = 1024;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedIndexProof {
    pub format_version: u16,
    pub index_id: String,
    pub index_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub source_watch_stream_id: String,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub segment_hashes: Vec<String>,
    pub built_by_node: String,
    pub built_at_nanos: i64,
    pub proof_hash: Option<String>,
    pub proof_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedIndexProofWrite {
    pub index_id: String,
    pub index_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub source_watch_stream_id: String,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub segment_hashes: Vec<String>,
    pub built_by_node: String,
    pub built_at_nanos: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedDerivedIndexProof {
    sealed: DerivedIndexProof,
}

#[derive(Clone, PartialEq, Message)]
struct DerivedIndexProofProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    index_id: String,
    #[prost(string, tag = "3")]
    index_kind: String,
    #[prost(string, tag = "4")]
    partition_family: String,
    #[prost(string, tag = "5")]
    partition_id: String,
    #[prost(string, tag = "6")]
    source_watch_stream_id: String,
    #[prost(string, tag = "7")]
    source_cursor: String,
    #[prost(string, tag = "8")]
    source_manifest_hash: String,
    #[prost(uint64, tag = "9")]
    generation: u64,
    #[prost(string, repeated, tag = "10")]
    segment_hashes: Vec<String>,
    #[prost(string, tag = "11")]
    built_by_node: String,
    #[prost(int64, tag = "12")]
    built_at_nanos: i64,
    #[prost(string, optional, tag = "13")]
    proof_hash: Option<String>,
    #[prost(string, optional, tag = "14")]
    proof_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct DerivedIndexProofRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    proof_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedIndexValidity {
    Valid,
    RebuildRequired,
}

impl DerivedIndexProof {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_proof(&self)?;
        let hash = hash_derived_index_proof(&self)?;
        let signature = sign_proof_hash(
            signing_key,
            &hash,
            &[
                &self.index_id,
                &self.partition_id,
                &self.source_manifest_hash,
                &self.generation.to_string(),
            ],
        )?;
        self.proof_hash = Some(hash);
        self.proof_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_proof(self)?;
        let expected_hash = hash_derived_index_proof(self)?;
        if self.proof_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("derived index proof hash mismatch"));
        }
        let expected_signature = sign_proof_hash(
            signing_key,
            &expected_hash,
            &[
                &self.index_id,
                &self.partition_id,
                &self.source_manifest_hash,
                &self.generation.to_string(),
            ],
        )?;
        if self.proof_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("derived index proof signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_derived_index_proof(proof: &DerivedIndexProof) -> Result<String> {
    let mut unsigned = proof.clone();
    unsigned.proof_hash = None;
    unsigned.proof_signature = None;
    Ok(hex::encode(hash32(&encode_derived_index_proof(&unsigned)?)))
}

pub(crate) fn prepare_derived_index_proof(
    proof: DerivedIndexProofWrite,
    signing_key: &[u8],
) -> Result<PreparedDerivedIndexProof> {
    validate_write(&proof)?;
    let sealed = DerivedIndexProof {
        format_version: 1,
        index_id: proof.index_id,
        index_kind: proof.index_kind,
        partition_family: proof.partition_family,
        partition_id: proof.partition_id,
        source_watch_stream_id: proof.source_watch_stream_id,
        source_cursor: proof.source_cursor,
        source_manifest_hash: proof.source_manifest_hash,
        generation: proof.generation,
        segment_hashes: proof.segment_hashes,
        built_by_node: proof.built_by_node,
        built_at_nanos: proof.built_at_nanos,
        proof_hash: None,
        proof_signature: None,
    }
    .seal(signing_key)?;
    Ok(PreparedDerivedIndexProof { sealed })
}

/// Stages the immutable proof and its movable head in the caller's MVCC
/// transaction. Both rows are protected by predicates derived from the
/// transaction's fixed snapshot, so a concurrent successor cannot be
/// overwritten.
pub(crate) fn stage_prepared_derived_index_proof(
    mvcc: &MvccSubsystem,
    transaction_id: &str,
    principal: &str,
    prepared: &PreparedDerivedIndexProof,
    now_unix_ms: u64,
) -> Result<DerivedIndexProof> {
    let proof = &prepared.sealed;
    let proof_hash = proof
        .proof_hash
        .as_deref()
        .ok_or_else(|| anyhow!("sealed derived index proof is missing proof hash"))?;
    let versioned = coremeta_logical_key(
        CF_INDEX_ROWS,
        TABLE_DERIVED_INDEX_PROOF_ROW,
        &versioned_proof_tuple_key(&proof.index_id, proof.generation, proof_hash)?,
    )?;
    let head = coremeta_logical_key(
        CF_INDEX_ROWS,
        TABLE_DERIVED_INDEX_PROOF_ROW,
        &head_proof_tuple_key(&proof.index_id)?,
    )?;
    let versioned_current = mvcc.read_transaction_value(transaction_id, principal, &versioned)?;
    if let Some(existing) = versioned_current.as_deref()
        && decode_derived_index_proof_row(existing)? != *proof
    {
        return Err(anyhow!(
            "derived index generation already identifies a different proof"
        ));
    }
    let head_current = mvcc.read_transaction_value(transaction_id, principal, &head)?;
    if let Some(existing) = head_current.as_deref() {
        let existing = decode_derived_index_proof_row(existing)?;
        if existing.generation > proof.generation {
            return Err(anyhow!("derived index proof head cannot move backwards"));
        }
        if existing.generation == proof.generation {
            if existing != *proof {
                return Err(anyhow!(
                    "derived index proof diverges at an existing generation"
                ));
            }
            return Ok(proof.clone());
        }
    }
    let predicate = |current: &Option<Vec<u8>>| match current {
        Some(payload) => PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()),
        None => PredicateKind::Absent,
    };
    mvcc.stage_product_mutations(
        transaction_id,
        principal,
        vec![
            ProductMutation::put(versioned.clone(), encode_derived_index_proof_row(proof)?),
            ProductMutation::put(head.clone(), encode_derived_index_proof_row(proof)?),
        ],
        now_unix_ms,
    )?;
    mvcc.stage_predicate(
        transaction_id,
        principal,
        versioned,
        predicate(&versioned_current),
        now_unix_ms,
    )?;
    mvcc.stage_predicate(
        transaction_id,
        principal,
        head,
        predicate(&head_current),
        now_unix_ms,
    )?;
    Ok(proof.clone())
}

pub fn read_latest_derived_index_proof_mvcc(
    mvcc: &MvccSubsystem,
    index_id: &str,
    signing_key: &[u8],
) -> Result<Option<DerivedIndexProof>> {
    let key = coremeta_logical_key(
        CF_INDEX_ROWS,
        TABLE_DERIVED_INDEX_PROOF_ROW,
        &head_proof_tuple_key(index_id)?,
    )?;
    let Some(payload) = mvcc.read_latest_value(&key)? else {
        return Ok(None);
    };
    let proof = decode_derived_index_proof_row(&payload)?;
    proof.verify(signing_key)?;
    if proof.index_id != index_id {
        return Err(anyhow!("derived index proof ref scope mismatch"));
    }
    Ok(Some(proof))
}

pub fn validate_derived_index_source(
    proof: &DerivedIndexProof,
    required_source_cursor: u128,
    expected_source_manifest_hash: &str,
    min_generation: u64,
    signing_key: &[u8],
) -> Result<DerivedIndexValidity> {
    proof.verify(signing_key)?;
    validate_hex32(
        expected_source_manifest_hash,
        "expected_source_manifest_hash",
    )?;
    if proof.source_manifest_hash != expected_source_manifest_hash
        || proof.source_cursor < required_source_cursor
        || proof.generation < min_generation
    {
        return Ok(DerivedIndexValidity::RebuildRequired);
    }
    Ok(DerivedIndexValidity::Valid)
}

fn validate_write(proof: &DerivedIndexProofWrite) -> Result<()> {
    let unsigned = DerivedIndexProof {
        format_version: 1,
        index_id: proof.index_id.clone(),
        index_kind: proof.index_kind.clone(),
        partition_family: proof.partition_family.clone(),
        partition_id: proof.partition_id.clone(),
        source_watch_stream_id: proof.source_watch_stream_id.clone(),
        source_cursor: proof.source_cursor,
        source_manifest_hash: proof.source_manifest_hash.clone(),
        generation: proof.generation,
        segment_hashes: proof.segment_hashes.clone(),
        built_by_node: proof.built_by_node.clone(),
        built_at_nanos: proof.built_at_nanos,
        proof_hash: None,
        proof_signature: None,
    };
    validate_unsigned_proof(&unsigned)
}

fn validate_unsigned_proof(proof: &DerivedIndexProof) -> Result<()> {
    if proof.format_version != 1 {
        return Err(anyhow!("unsupported derived index proof version"));
    }
    require_safe_component(&proof.index_id, "index_id")?;
    require_safe_component(&proof.index_kind, "index_kind")?;
    require_nonempty(&proof.partition_family, "partition_family")?;
    validate_hex32(&proof.partition_id, "partition_id")?;
    require_safe_component(&proof.source_watch_stream_id, "source_watch_stream_id")?;
    validate_hex32(&proof.source_manifest_hash, "source_manifest_hash")?;
    if proof.generation == 0 {
        return Err(anyhow!("derived index proof generation must be nonzero"));
    }
    if proof.segment_hashes.is_empty() {
        return Err(anyhow!("derived index proof must include segment hashes"));
    }
    if proof.segment_hashes.len() > MAX_DERIVED_INDEX_SEGMENT_HASHES {
        return Err(anyhow!(
            "derived index proof must contain no more than {MAX_DERIVED_INDEX_SEGMENT_HASHES} segment hashes"
        ));
    }
    for segment_hash in &proof.segment_hashes {
        validate_hex32(segment_hash, "segment_hash")?;
    }
    require_nonempty(&proof.built_by_node, "built_by_node")?;
    if proof.built_at_nanos < 0 {
        return Err(anyhow!("derived index proof timestamp must be nonnegative"));
    }
    Ok(())
}

fn sign_proof_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("derived index proof signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"derived_index_proof");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

fn encode_derived_index_proof(proof: &DerivedIndexProof) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&derived_index_proof_to_proto(
        proof,
    )))
}

fn decode_derived_index_proof(bytes: &[u8]) -> Result<DerivedIndexProof> {
    derived_index_proof_from_proto(decode_deterministic_proto::<DerivedIndexProofProto>(
        bytes,
        "derived index proof",
    )?)
}

fn encode_derived_index_proof_row(proof: &DerivedIndexProof) -> Result<Vec<u8>> {
    proof
        .proof_hash
        .as_ref()
        .ok_or_else(|| anyhow!("sealed derived index proof is missing proof hash"))?;
    Ok(encode_deterministic_proto(&DerivedIndexProofRowProto {
        common: Some(CoreMetaRowCommonProto {
            realm_id: proof.partition_family.clone(),
            root_key_hash: core_meta_root_key_hash(&format!(
                "derived-index-proof/{}/{}",
                proof.index_id, proof.partition_id
            )),
            root_generation: proof.generation,
            transaction_id: format!(
                "derived-index-proof:{}:{}",
                proof.index_id, proof.generation
            ),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: proof.built_at_nanos.max(0) as u64,
            payload_schema_version: 1,
        }),
        schema: "anvil.coremeta.derived_index_proof.v1".to_string(),
        proof_bytes: encode_derived_index_proof(proof)?,
    }))
}

fn decode_derived_index_proof_row(bytes: &[u8]) -> Result<DerivedIndexProof> {
    let row =
        decode_deterministic_proto::<DerivedIndexProofRowProto>(bytes, "derived index proof row")?;
    if row.schema != "anvil.coremeta.derived_index_proof.v1" {
        return Err(anyhow!("derived index proof row has invalid schema"));
    }
    row.common
        .as_ref()
        .ok_or_else(|| anyhow!("derived index proof row missing CoreMeta common"))?;
    decode_derived_index_proof(&row.proof_bytes)
}

fn derived_index_proof_to_proto(proof: &DerivedIndexProof) -> DerivedIndexProofProto {
    DerivedIndexProofProto {
        format_version: u32::from(proof.format_version),
        index_id: proof.index_id.clone(),
        index_kind: proof.index_kind.clone(),
        partition_family: proof.partition_family.clone(),
        partition_id: proof.partition_id.clone(),
        source_watch_stream_id: proof.source_watch_stream_id.clone(),
        source_cursor: proof.source_cursor.to_string(),
        source_manifest_hash: proof.source_manifest_hash.clone(),
        generation: proof.generation,
        segment_hashes: proof.segment_hashes.clone(),
        built_by_node: proof.built_by_node.clone(),
        built_at_nanos: proof.built_at_nanos,
        proof_hash: proof.proof_hash.clone(),
        proof_signature: proof.proof_signature.clone(),
    }
}

fn derived_index_proof_from_proto(proto: DerivedIndexProofProto) -> Result<DerivedIndexProof> {
    Ok(DerivedIndexProof {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("derived index proof version exceeds u16"))?,
        index_id: proto.index_id,
        index_kind: proto.index_kind,
        partition_family: proto.partition_family,
        partition_id: proto.partition_id,
        source_watch_stream_id: proto.source_watch_stream_id,
        source_cursor: proto
            .source_cursor
            .parse()
            .map_err(|_| anyhow!("derived index proof source_cursor is not u128"))?,
        source_manifest_hash: proto.source_manifest_hash,
        generation: proto.generation,
        segment_hashes: proto.segment_hashes,
        built_by_node: proto.built_by_node,
        built_at_nanos: proto.built_at_nanos,
        proof_hash: proto.proof_hash,
        proof_signature: proto.proof_signature,
    })
}

fn head_proof_tuple_key(index_id: &str) -> Result<Vec<u8>> {
    require_safe_component(index_id, "index_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("derived-index-proof"),
        CoreMetaTuplePart::Utf8(index_id),
        CoreMetaTuplePart::Utf8("head"),
    ])
}

fn versioned_proof_tuple_key(index_id: &str, generation: u64, proof_hash: &str) -> Result<Vec<u8>> {
    require_safe_component(index_id, "index_id")?;
    if generation == 0 {
        return Err(anyhow!("derived index proof generation must be nonzero"));
    }
    validate_hex32(proof_hash, "proof_hash")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("derived-index-proof"),
        CoreMetaTuplePart::Utf8(index_id),
        CoreMetaTuplePart::Utf8("generation"),
        CoreMetaTuplePart::U64(generation),
        CoreMetaTuplePart::Hash(proof_hash),
    ])
}
