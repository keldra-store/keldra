use anyhow::{Result, bail};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use super::deterministic_proto::encode_deterministic_proto;
use super::meta::{CoreMetaBatchOp, core_meta_payload_digest};

pub const CORE_META_DEFAULT_REPLICA_COUNT: usize = 3;
pub const CORE_META_DEFAULT_QUORUM: usize = 2;
pub const CORE_META_PENDING_BATCH_HASH_DOMAIN: &str = "anvil.coremeta.pending_batch.v1";
pub const CORE_META_PREPARE_RECEIPT_HASH_DOMAIN: &str = "anvil.coremeta.batch_receipt.v1";
pub const CORE_META_COMMITTED_BATCH_HASH_DOMAIN: &str = "anvil.coremeta.committed_batch.v1";
pub const CORE_META_COMMIT_CERTIFICATE_HASH_DOMAIN: &str = "anvil.coremeta.commit_certificate.v1";
pub const CORE_META_CERTIFICATE_PERSIST_RECEIPT_HASH_DOMAIN: &str =
    "anvil.coremeta.certificate_persist_receipt.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetaPendingBatchInput {
    pub root_key_hash: String,
    pub expected_root_generation: u64,
    pub post_root_generation: u64,
    pub transaction_id: String,
    pub row_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetaCommittedBatchInput {
    pub root_key_hash: String,
    pub expected_root_generation: u64,
    pub post_root_generation: u64,
    pub transaction_id: String,
    pub pending_batch_hash: String,
    pub committed_row_hashes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetaPrepareReceipt {
    pub replica_node_id: String,
    pub write_sequence: u64,
    pub pending_batch_hash: String,
    pub root_key_hash: String,
    pub expected_root_generation: u64,
    pub post_root_generation: u64,
    pub transaction_id: String,
    pub signed_payload_hash: String,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetaCommitCertificate {
    pub root_key_hash: String,
    pub expected_root_generation: u64,
    pub post_root_generation: u64,
    pub transaction_id: String,
    pub pending_batch_hash: String,
    pub prepare_receipts: Vec<CoreMetaPrepareReceipt>,
    pub certificate_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetaCertificatePersistReceipt {
    pub replica_node_id: String,
    pub write_sequence: u64,
    pub certificate_hash: String,
    pub committed_batch_hash: String,
    pub root_key_hash: String,
    pub post_root_generation: u64,
    pub transaction_id: String,
    pub signed_payload_hash: String,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetaQuorumProfile {
    pub profile_id: String,
    pub replica_count: usize,
    pub prepare_quorum: usize,
    pub certificate_persist_quorum: usize,
}

impl CoreMetaQuorumProfile {
    pub fn metadata_r3_q2() -> Self {
        Self {
            profile_id: "metadata-r3-q2".to_string(),
            replica_count: CORE_META_DEFAULT_REPLICA_COUNT,
            prepare_quorum: CORE_META_DEFAULT_QUORUM,
            certificate_persist_quorum: CORE_META_DEFAULT_QUORUM,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.replica_count == 0 {
            bail!("CoreMeta quorum profile must contain at least one replica");
        }
        if self.prepare_quorum == 0
            || self.certificate_persist_quorum == 0
            || self.prepare_quorum > self.replica_count
            || self.certificate_persist_quorum > self.replica_count
        {
            bail!("CoreMeta quorum profile has impossible quorum settings");
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaPendingBatchHashProto {
    #[prost(string, tag = "1")]
    root_key_hash: String,
    #[prost(uint64, tag = "2")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "3")]
    post_root_generation: u64,
    #[prost(string, tag = "4")]
    transaction_id: String,
    #[prost(string, repeated, tag = "5")]
    row_hashes: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaPrepareReceiptHashProto {
    #[prost(string, tag = "1")]
    replica_node_id: String,
    #[prost(uint64, tag = "2")]
    write_sequence: u64,
    #[prost(string, tag = "3")]
    pending_batch_hash: String,
    #[prost(string, tag = "4")]
    root_key_hash: String,
    #[prost(uint64, tag = "5")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "6")]
    post_root_generation: u64,
    #[prost(string, tag = "7")]
    transaction_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommitCertificateHashProto {
    #[prost(string, tag = "1")]
    root_key_hash: String,
    #[prost(uint64, tag = "2")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "3")]
    post_root_generation: u64,
    #[prost(string, tag = "4")]
    transaction_id: String,
    #[prost(string, tag = "5")]
    pending_batch_hash: String,
    #[prost(string, repeated, tag = "6")]
    prepare_receipt_hashes: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommittedBatchHashProto {
    #[prost(string, tag = "1")]
    root_key_hash: String,
    #[prost(uint64, tag = "2")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "3")]
    post_root_generation: u64,
    #[prost(string, tag = "4")]
    transaction_id: String,
    #[prost(string, tag = "5")]
    pending_batch_hash: String,
    #[prost(string, repeated, tag = "6")]
    row_hashes: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCertificatePersistReceiptHashProto {
    #[prost(string, tag = "1")]
    replica_node_id: String,
    #[prost(uint64, tag = "2")]
    write_sequence: u64,
    #[prost(string, tag = "3")]
    certificate_hash: String,
    #[prost(string, tag = "4")]
    committed_batch_hash: String,
    #[prost(string, tag = "5")]
    root_key_hash: String,
    #[prost(uint64, tag = "6")]
    post_root_generation: u64,
    #[prost(string, tag = "7")]
    transaction_id: String,
}

pub fn core_meta_row_hashes(ops: &[CoreMetaBatchOp<'_>]) -> Vec<String> {
    let mut row_hashes = ops
        .iter()
        .map(|op| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(op.cf.as_bytes());
            bytes.push(0);
            bytes.extend_from_slice(&op.table_id.to_le_bytes());
            bytes.extend_from_slice(op.tuple_key);
            bytes.push(0);
            match &op.kind {
                super::meta::CoreMetaBatchOpKind::Put(payload) => bytes
                    .extend_from_slice(core_meta_payload_digest(op.table_id, payload).as_bytes()),
                super::meta::CoreMetaBatchOpKind::Delete => bytes.extend_from_slice(b"delete"),
            }
            domain_hash_bytes("anvil.coremeta.row.v1", &[&bytes])
        })
        .collect::<Vec<_>>();
    row_hashes.sort();
    row_hashes.dedup();
    row_hashes
}

pub fn core_meta_encoded_row_hash(
    column_family: &str,
    core_meta_key: &[u8],
    value_envelope: &[u8],
) -> String {
    core_meta_encoded_row_hash_with_delete(column_family, core_meta_key, value_envelope, false)
}

pub fn core_meta_encoded_row_hash_with_delete(
    column_family: &str,
    core_meta_key: &[u8],
    value_envelope: &[u8],
    delete_marker: bool,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(column_family.len() as u64).to_le_bytes());
    bytes.extend_from_slice(column_family.as_bytes());
    bytes.extend_from_slice(&(core_meta_key.len() as u64).to_le_bytes());
    bytes.extend_from_slice(core_meta_key);
    bytes.push(u8::from(delete_marker));
    bytes.extend_from_slice(&(value_envelope.len() as u64).to_le_bytes());
    bytes.extend_from_slice(value_envelope);
    domain_hash_bytes("anvil.coremeta.row.v1", &[&bytes])
}

pub fn core_meta_encoded_row_hashes<'a>(
    rows: impl IntoIterator<Item = (&'a str, &'a [u8], &'a [u8])>,
) -> Vec<String> {
    let mut hashes = rows
        .into_iter()
        .map(|(cf, key, value)| core_meta_encoded_row_hash(cf, key, value))
        .collect::<Vec<_>>();
    hashes.sort();
    hashes.dedup();
    hashes
}

pub fn pending_batch_hash(input: &CoreMetaPendingBatchInput) -> Result<String> {
    let mut rows = input.row_hashes.clone();
    rows.sort();
    rows.dedup();
    let proto = CoreMetaPendingBatchHashProto {
        root_key_hash: input.root_key_hash.clone(),
        expected_root_generation: input.expected_root_generation,
        post_root_generation: input.post_root_generation,
        transaction_id: input.transaction_id.clone(),
        row_hashes: rows,
    };
    domain_hash_proto(CORE_META_PENDING_BATCH_HASH_DOMAIN, &proto)
}

pub fn committed_batch_hash(input: &CoreMetaCommittedBatchInput) -> Result<String> {
    let mut rows = input.committed_row_hashes.clone();
    rows.sort();
    rows.dedup();
    let proto = CoreMetaCommittedBatchHashProto {
        root_key_hash: input.root_key_hash.clone(),
        expected_root_generation: input.expected_root_generation,
        post_root_generation: input.post_root_generation,
        transaction_id: input.transaction_id.clone(),
        pending_batch_hash: input.pending_batch_hash.clone(),
        row_hashes: rows,
    };
    domain_hash_proto(CORE_META_COMMITTED_BATCH_HASH_DOMAIN, &proto)
}

pub fn prepare_receipt_payload_hash(receipt: &CoreMetaPrepareReceipt) -> Result<String> {
    domain_hash_proto(
        CORE_META_PREPARE_RECEIPT_HASH_DOMAIN,
        &CoreMetaPrepareReceiptHashProto {
            replica_node_id: receipt.replica_node_id.clone(),
            write_sequence: receipt.write_sequence,
            pending_batch_hash: receipt.pending_batch_hash.clone(),
            root_key_hash: receipt.root_key_hash.clone(),
            expected_root_generation: receipt.expected_root_generation,
            post_root_generation: receipt.post_root_generation,
            transaction_id: receipt.transaction_id.clone(),
        },
    )
}

pub fn build_commit_certificate(
    profile: &CoreMetaQuorumProfile,
    root_key_hash: String,
    expected_root_generation: u64,
    post_root_generation: u64,
    transaction_id: String,
    pending_batch_hash: String,
    mut prepare_receipts: Vec<CoreMetaPrepareReceipt>,
) -> Result<CoreMetaCommitCertificate> {
    profile.validate()?;
    validate_prepare_receipt_quorum(
        profile,
        &pending_batch_hash,
        &root_key_hash,
        expected_root_generation,
        post_root_generation,
        &transaction_id,
        &prepare_receipts,
    )?;
    prepare_receipts.sort_by(|a, b| a.replica_node_id.cmp(&b.replica_node_id));
    let mut cert = CoreMetaCommitCertificate {
        root_key_hash,
        expected_root_generation,
        post_root_generation,
        transaction_id,
        pending_batch_hash,
        prepare_receipts,
        certificate_hash: String::new(),
    };
    cert.certificate_hash = commit_certificate_hash(&cert)?;
    Ok(cert)
}

pub fn commit_certificate_hash(certificate: &CoreMetaCommitCertificate) -> Result<String> {
    let mut receipt_hashes = certificate
        .prepare_receipts
        .iter()
        .map(prepare_receipt_payload_hash)
        .collect::<Result<Vec<_>>>()?;
    receipt_hashes.sort();
    receipt_hashes.dedup();
    domain_hash_proto(
        CORE_META_COMMIT_CERTIFICATE_HASH_DOMAIN,
        &CoreMetaCommitCertificateHashProto {
            root_key_hash: certificate.root_key_hash.clone(),
            expected_root_generation: certificate.expected_root_generation,
            post_root_generation: certificate.post_root_generation,
            transaction_id: certificate.transaction_id.clone(),
            pending_batch_hash: certificate.pending_batch_hash.clone(),
            prepare_receipt_hashes: receipt_hashes,
        },
    )
}

pub fn certificate_persist_receipt_payload_hash(
    receipt: &CoreMetaCertificatePersistReceipt,
) -> Result<String> {
    domain_hash_proto(
        CORE_META_CERTIFICATE_PERSIST_RECEIPT_HASH_DOMAIN,
        &CoreMetaCertificatePersistReceiptHashProto {
            replica_node_id: receipt.replica_node_id.clone(),
            write_sequence: receipt.write_sequence,
            certificate_hash: receipt.certificate_hash.clone(),
            committed_batch_hash: receipt.committed_batch_hash.clone(),
            root_key_hash: receipt.root_key_hash.clone(),
            post_root_generation: receipt.post_root_generation,
            transaction_id: receipt.transaction_id.clone(),
        },
    )
}

pub fn validate_commit_certificate_with_verifier<F>(
    profile: &CoreMetaQuorumProfile,
    certificate: &CoreMetaCommitCertificate,
    mut verify_signature: F,
) -> Result<()>
where
    F: FnMut(&str, &str, &[u8]) -> Result<()>,
{
    profile.validate()?;
    let expected_certificate_hash = commit_certificate_hash(certificate)?;
    if certificate.certificate_hash != expected_certificate_hash {
        bail!("CoreMeta commit certificate hash mismatch");
    }
    validate_prepare_receipt_quorum_with_verifier(
        profile,
        &certificate.pending_batch_hash,
        &certificate.root_key_hash,
        certificate.expected_root_generation,
        certificate.post_root_generation,
        &certificate.transaction_id,
        &certificate.prepare_receipts,
        &mut verify_signature,
    )
}

pub fn validate_commit_evidence(
    profile: &CoreMetaQuorumProfile,
    certificate: &CoreMetaCommitCertificate,
    persist_receipts: &[CoreMetaCertificatePersistReceipt],
) -> Result<()> {
    validate_commit_evidence_with_verifier(profile, certificate, persist_receipts, |_, _, sig| {
        if sig.is_empty() {
            bail!("CoreMeta receipt signature must not be empty");
        }
        Ok(())
    })
}

pub fn validate_commit_evidence_with_verifier<F>(
    profile: &CoreMetaQuorumProfile,
    certificate: &CoreMetaCommitCertificate,
    persist_receipts: &[CoreMetaCertificatePersistReceipt],
    mut verify_signature: F,
) -> Result<()>
where
    F: FnMut(&str, &str, &[u8]) -> Result<()>,
{
    profile.validate()?;
    let expected_certificate_hash = commit_certificate_hash(certificate)?;
    if certificate.certificate_hash != expected_certificate_hash {
        bail!("CoreMeta commit certificate hash mismatch");
    }
    validate_prepare_receipt_quorum_with_verifier(
        profile,
        &certificate.pending_batch_hash,
        &certificate.root_key_hash,
        certificate.expected_root_generation,
        certificate.post_root_generation,
        &certificate.transaction_id,
        &certificate.prepare_receipts,
        &mut verify_signature,
    )?;
    let mut replicas = BTreeSet::new();
    for receipt in persist_receipts {
        if receipt.certificate_hash != certificate.certificate_hash
            || receipt.root_key_hash != certificate.root_key_hash
            || receipt.post_root_generation != certificate.post_root_generation
            || receipt.transaction_id != certificate.transaction_id
        {
            bail!("CoreMeta certificate persist receipt scope mismatch");
        }
        if receipt.signed_payload_hash != certificate_persist_receipt_payload_hash(receipt)? {
            bail!("CoreMeta certificate persist receipt payload hash mismatch");
        }
        verify_signature(
            &receipt.replica_node_id,
            &receipt.signed_payload_hash,
            &receipt.signature,
        )?;
        replicas.insert(receipt.replica_node_id.as_str());
    }
    if replicas.len() < profile.certificate_persist_quorum {
        bail!(
            "CoreMeta certificate persist quorum was not reached: profile={} required={} received={} replica_node_ids={:?}",
            profile.profile_id,
            profile.certificate_persist_quorum,
            replicas.len(),
            replicas
        );
    }
    Ok(())
}

fn validate_prepare_receipt_quorum(
    profile: &CoreMetaQuorumProfile,
    pending_batch_hash: &str,
    root_key_hash: &str,
    expected_root_generation: u64,
    post_root_generation: u64,
    transaction_id: &str,
    receipts: &[CoreMetaPrepareReceipt],
) -> Result<()> {
    validate_prepare_receipt_quorum_with_verifier(
        profile,
        pending_batch_hash,
        root_key_hash,
        expected_root_generation,
        post_root_generation,
        transaction_id,
        receipts,
        |_, _, sig| {
            if sig.is_empty() {
                bail!("CoreMeta prepare receipt signature must not be empty");
            }
            Ok(())
        },
    )
}

fn validate_prepare_receipt_quorum_with_verifier<F>(
    profile: &CoreMetaQuorumProfile,
    pending_batch_hash: &str,
    root_key_hash: &str,
    expected_root_generation: u64,
    post_root_generation: u64,
    transaction_id: &str,
    receipts: &[CoreMetaPrepareReceipt],
    mut verify_signature: F,
) -> Result<()>
where
    F: FnMut(&str, &str, &[u8]) -> Result<()>,
{
    let mut replicas = BTreeSet::new();
    for receipt in receipts {
        if receipt.pending_batch_hash != pending_batch_hash
            || receipt.root_key_hash != root_key_hash
            || receipt.expected_root_generation != expected_root_generation
            || receipt.post_root_generation != post_root_generation
            || receipt.transaction_id != transaction_id
        {
            bail!("CoreMeta prepare receipt scope mismatch");
        }
        if receipt.signed_payload_hash != prepare_receipt_payload_hash(receipt)? {
            bail!("CoreMeta prepare receipt payload hash mismatch");
        }
        verify_signature(
            &receipt.replica_node_id,
            &receipt.signed_payload_hash,
            &receipt.signature,
        )?;
        replicas.insert(receipt.replica_node_id.as_str());
    }
    if replicas.len() < profile.prepare_quorum {
        bail!("CoreMeta prepare quorum was not reached");
    }
    Ok(())
}

fn domain_hash_proto<M: Message>(domain: &str, message: &M) -> Result<String> {
    let bytes = encode_deterministic_proto(message);
    Ok(domain_hash_bytes(domain, &[&bytes]))
}

fn domain_hash_bytes(domain: &str, parts: &[&[u8]]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain.as_bytes());
    hasher.update(&[0]);
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

pub fn local_signed_prepare_receipt(
    replica_node_id: String,
    write_sequence: u64,
    pending_batch_hash: String,
    root_key_hash: String,
    expected_root_generation: u64,
    post_root_generation: u64,
    transaction_id: String,
) -> Result<CoreMetaPrepareReceipt> {
    let mut receipt = CoreMetaPrepareReceipt {
        replica_node_id,
        write_sequence,
        pending_batch_hash,
        root_key_hash,
        expected_root_generation,
        post_root_generation,
        transaction_id,
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash = prepare_receipt_payload_hash(&receipt)?;
    receipt.signature = receipt.signed_payload_hash.as_bytes().to_vec();
    Ok(receipt)
}

pub fn local_signed_certificate_persist_receipt(
    replica_node_id: String,
    write_sequence: u64,
    certificate_hash: String,
    committed_batch_hash: String,
    root_key_hash: String,
    post_root_generation: u64,
    transaction_id: String,
) -> Result<CoreMetaCertificatePersistReceipt> {
    let mut receipt = CoreMetaCertificatePersistReceipt {
        replica_node_id,
        write_sequence,
        certificate_hash,
        committed_batch_hash,
        root_key_hash,
        post_root_generation,
        transaction_id,
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash = certificate_persist_receipt_payload_hash(&receipt)?;
    receipt.signature = receipt.signed_payload_hash.as_bytes().to_vec();
    Ok(receipt)
}
