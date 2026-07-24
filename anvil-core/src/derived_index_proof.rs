use crate::{
    core_store::{
        CF_INDEX_ROWS, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
        CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
        CoreMutationRootPublication, CoreStore, CoreTransactionState,
        TABLE_DERIVED_INDEX_PROOF_ROW, core_meta_payload_digest, core_meta_root_key_hash,
        core_meta_tuple_key, core_mutation_publication_attempt_id, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::hash32,
    storage::Storage,
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

pub async fn write_derived_index_proof(
    storage: &Storage,
    proof: DerivedIndexProofWrite,
    signing_key: &[u8],
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<DerivedIndexProof> {
    let prepared = prepare_derived_index_proof(proof, signing_key)?;
    publish_prepared_derived_index_proof(storage, &prepared, additional_preconditions).await
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

pub(crate) async fn publish_prepared_derived_index_proof(
    storage: &Storage,
    prepared: &PreparedDerivedIndexProof,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<DerivedIndexProof> {
    write_derived_index_proof_rows(storage, &prepared.sealed, additional_preconditions).await?;
    Ok(prepared.sealed.clone())
}

pub async fn read_latest_derived_index_proof(
    storage: &Storage,
    index_id: &str,
    signing_key: &[u8],
) -> Result<Option<DerivedIndexProof>> {
    let Some(proof) =
        read_derived_index_proof_row(storage, &head_proof_tuple_key(index_id)?).await?
    else {
        return Ok(None);
    };
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

async fn write_derived_index_proof_rows(
    storage: &Storage,
    proof: &DerivedIndexProof,
    additional_preconditions: &[CoreMutationPrecondition],
) -> Result<()> {
    let proof_hash = proof
        .proof_hash
        .as_deref()
        .ok_or_else(|| anyhow!("sealed derived index proof is missing proof hash"))?;
    let versioned_key = versioned_proof_tuple_key(&proof.index_id, proof.generation, proof_hash)?;
    let head_key = head_proof_tuple_key(&proof.index_id)?;
    let payload = encode_derived_index_proof_row(proof)?;
    let store = CoreStore::new(storage.clone()).await?;
    let versioned_current =
        store.read_coremeta_row(CF_INDEX_ROWS, TABLE_DERIVED_INDEX_PROOF_ROW, &versioned_key)?;
    if let Some(existing) = versioned_current.as_ref()
        && decode_derived_index_proof_row(existing)? != *proof
    {
        return Err(anyhow!(
            "derived index generation already identifies a different proof"
        ));
    }
    let head_current =
        store.read_coremeta_row(CF_INDEX_ROWS, TABLE_DERIVED_INDEX_PROOF_ROW, &head_key)?;
    if let Some(existing) = head_current.as_ref() {
        let existing_proof = decode_derived_index_proof_row(existing)?;
        if existing_proof.generation > proof.generation {
            return Err(anyhow!("derived index proof head cannot move backwards"));
        }
        if existing_proof.generation == proof.generation {
            if existing_proof != *proof {
                return Err(anyhow!(
                    "derived index proof diverges at an existing generation"
                ));
            }
            return Ok(());
        }
    }

    let row_precondition =
        |tuple_key: Vec<u8>, current: Option<&Vec<u8>>| CoreMutationPrecondition::CoreMetaRow {
            cf: CF_INDEX_ROWS.to_string(),
            table_id: TABLE_DERIVED_INDEX_PROOF_ROW,
            tuple_key,
            expected_payload_hash: current
                .map(|bytes| core_meta_payload_digest(TABLE_DERIVED_INDEX_PROOF_ROW, bytes)),
            require_absent: current.is_none(),
            require_present: current.is_some(),
        };
    let mut preconditions = vec![
        row_precondition(versioned_key.clone(), versioned_current.as_ref()),
        row_precondition(head_key.clone(), head_current.as_ref()),
    ];
    preconditions.extend_from_slice(additional_preconditions);
    let partition = format!(
        "derived-index-proof/{}/{}",
        proof.index_id, proof.partition_id
    );
    let logical_transaction_id = format!(
        "derived-index-proof:{}:{}:{}",
        proof.index_id, proof.generation, proof_hash
    );
    let transaction_id =
        core_mutation_publication_attempt_id(&logical_transaction_id, &preconditions)?;
    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition.clone(),
            committed_by_principal: format!("index-builder:{}", proof.built_by_node),
            root_publications: vec![
                CoreMutationRootPublication::with_writer_families(
                    partition.clone(),
                    vec![
                        crate::formats::writer::WriterFamily::CoreControl
                            .as_str()
                            .to_string(),
                        crate::formats::writer::WriterFamily::TypedMetadata
                            .as_str()
                            .to_string(),
                    ],
                )
                .coordinator(),
            ],
            preconditions,
            operations: vec![
                CoreMutationOperation::CoreMetaPut {
                    partition_id: partition.clone(),
                    cf: CF_INDEX_ROWS.to_string(),
                    table_id: TABLE_DERIVED_INDEX_PROOF_ROW,
                    tuple_key: versioned_key,
                    payload: payload.clone(),
                },
                CoreMutationOperation::CoreMetaPut {
                    partition_id: partition,
                    cf: CF_INDEX_ROWS.to_string(),
                    table_id: TABLE_DERIVED_INDEX_PROOF_ROW,
                    tuple_key: head_key,
                    payload,
                },
            ],
        })
        .await?;
    if receipt.state != CoreTransactionState::Committed {
        return Err(anyhow!(
            "derived index proof publication {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        ));
    }
    Ok(())
}

async fn read_derived_index_proof_row(
    storage: &Storage,
    tuple_key: &[u8],
) -> Result<Option<DerivedIndexProof>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(bytes) =
        store.read_coremeta_row(CF_INDEX_ROWS, TABLE_DERIVED_INDEX_PROOF_ROW, tuple_key)?
    else {
        return Ok(None);
    };
    Ok(Some(decode_derived_index_proof_row(&bytes)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_store::CoreMetaStore;
    use tempfile::tempdir;

    const KEY: &[u8] = b"derived index proof signing key";

    #[tokio::test]
    async fn derived_index_proof_writes_version_and_head() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let proof =
            write_derived_index_proof(&storage, proof(7, 42, hex::encode([8; 32])), KEY, &[])
                .await
                .unwrap();
        assert_eq!(proof.generation, 7);
        assert_eq!(proof.source_cursor, 42);
        let proof_hash = proof.proof_hash.as_deref().unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        assert!(
            meta.get(
                CF_INDEX_ROWS,
                TABLE_DERIVED_INDEX_PROOF_ROW,
                &versioned_proof_tuple_key("full-text-alpha", 7, proof_hash).unwrap()
            )
            .unwrap()
            .is_some()
        );
        assert_eq!(
            read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
                .await
                .unwrap()
                .unwrap(),
            proof
        );
    }

    #[test]
    fn derived_index_proof_retry_is_byte_identical() {
        let write = proof(7, 42, hex::encode([8; 32]));
        let first = prepare_derived_index_proof(write.clone(), KEY).unwrap();
        let replay = prepare_derived_index_proof(write, KEY).unwrap();

        assert_eq!(first.sealed, replay.sealed);
        assert_eq!(
            encode_derived_index_proof(&first.sealed).unwrap(),
            encode_derived_index_proof(&replay.sealed).unwrap()
        );
    }

    #[tokio::test]
    async fn derived_index_source_validation_requires_cursor_manifest_and_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let manifest_hash = hex::encode([8; 32]);
        let proof =
            write_derived_index_proof(&storage, proof(7, 42, manifest_hash.clone()), KEY, &[])
                .await
                .unwrap();
        assert_eq!(
            validate_derived_index_source(&proof, 42, &manifest_hash, 7, KEY).unwrap(),
            DerivedIndexValidity::Valid
        );
        assert_eq!(
            validate_derived_index_source(&proof, 43, &manifest_hash, 7, KEY).unwrap(),
            DerivedIndexValidity::RebuildRequired
        );
        assert_eq!(
            validate_derived_index_source(&proof, 42, &hex::encode([9; 32]), 7, KEY).unwrap(),
            DerivedIndexValidity::RebuildRequired
        );
        assert_eq!(
            validate_derived_index_source(&proof, 42, &manifest_hash, 8, KEY).unwrap(),
            DerivedIndexValidity::RebuildRequired
        );
    }

    #[tokio::test]
    async fn derived_index_proof_rejects_tamper_and_unsafe_inputs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_derived_index_proof(&storage, proof(7, 42, hex::encode([8; 32])), KEY, &[])
            .await
            .unwrap();
        let stored = read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let head_key = head_proof_tuple_key("full-text-alpha").unwrap();
        let mut value = meta
            .get(CF_INDEX_ROWS, TABLE_DERIVED_INDEX_PROOF_ROW, &head_key)
            .unwrap()
            .unwrap();
        *value.last_mut().expect("stored proof bytes are not empty") ^= 0x01;
        meta.put(
            CF_INDEX_ROWS,
            TABLE_DERIVED_INDEX_PROOF_ROW,
            &head_key,
            &value,
        )
        .unwrap();
        assert!(
            read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
                .await
                .is_err()
        );
        assert!(head_proof_tuple_key("../escape").is_err());
        let mut invalid = proof(7, 42, hex::encode([8; 32]));
        invalid.segment_hashes = Vec::new();
        assert!(
            write_derived_index_proof(&storage, invalid, KEY, &[])
                .await
                .is_err()
        );
        let mut oversized = proof(8, 43, hex::encode([8; 32]));
        oversized.segment_hashes = vec![hex::encode([3; 32]); MAX_DERIVED_INDEX_SEGMENT_HASHES + 1];
        assert!(
            write_derived_index_proof(&storage, oversized, KEY, &[])
                .await
                .is_err()
        );
        assert!(stored.verify(KEY).is_ok());
    }

    fn proof(
        generation: u64,
        source_cursor: u128,
        source_manifest_hash: String,
    ) -> DerivedIndexProofWrite {
        DerivedIndexProofWrite {
            index_id: "full-text-alpha".to_string(),
            index_kind: "full_text".to_string(),
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([2; 32]),
            source_watch_stream_id: "object-prefix".to_string(),
            source_cursor,
            source_manifest_hash,
            generation,
            segment_hashes: vec![hex::encode([3; 32]), hex::encode([4; 32])],
            built_by_node: "node-a".to_string(),
            built_at_nanos: 1000,
        }
    }
}
