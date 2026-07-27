use crate::{
    core_store::encode_deterministic_proto,
    formats::{Hash32, hash32},
    personaldb_signing::PersonalDbProtocolKeyring,
};
use anyhow::{Result, anyhow};
use personaldb_protocol::{
    DatabaseId, ProtocolSignable, PublicKeyTrustStore, SignatureDomain, SignatureEnvelopeV1,
    SignatureMetadata, SignaturePurpose, SignatureScope, SigningPayload,
};
use prost::Message;

const PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX: &str = "personaldb_snapshot_object:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbGroupManifest {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub schema_hash: String,
    pub genesis_hash: String,
    pub created_at: String,
    pub created_by: String,
    pub consistency_policy: String,
    pub object_layout_version: u16,
    pub active_membership_epoch: u64,
    pub active_policy_epoch: u64,
    pub current_row_index_generation: u64,
    pub current_projection_generation: u64,
    pub manifest_hash: Option<String>,
    pub manifest_signature: Option<SignatureEnvelopeV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotManifest {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub log_hash: String,
    pub state_hash: String,
    pub schema_hash: String,
    pub snapshot_object_key: String,
    pub snapshot_object_hash: String,
    pub source_segment_start: u64,
    pub source_segment_end: u64,
    pub row_index_generation: u64,
    pub created_at: String,
    pub created_by_node: String,
    pub manifest_hash: Option<String>,
    pub manifest_signature: Option<SignatureEnvelopeV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCommitCertificate {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub previous_log_hash: String,
    pub entry_hash: String,
    pub changeset_payload_hash: String,
    pub verified_envelope_hash: String,
    pub client_log_epoch: u64,
    pub membership_epoch: u64,
    pub policy_epoch: u64,
    pub leader_replica_id: String,
    pub voter_acks_hash: String,
    pub authz_revision: u64,
    pub witness_node_id: String,
    pub witnessed_at: String,
    pub certificate_hash: Option<String>,
    pub witness_signature: Option<SignatureEnvelopeV1>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbGroupManifestHashProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(string, tag = "4")]
    schema_hash: String,
    #[prost(string, tag = "5")]
    genesis_hash: String,
    #[prost(string, tag = "6")]
    created_at: String,
    #[prost(string, tag = "7")]
    created_by: String,
    #[prost(string, tag = "8")]
    consistency_policy: String,
    #[prost(uint32, tag = "9")]
    object_layout_version: u32,
    #[prost(uint64, tag = "10")]
    active_membership_epoch: u64,
    #[prost(uint64, tag = "11")]
    active_policy_epoch: u64,
    #[prost(uint64, tag = "12")]
    current_row_index_generation: u64,
    #[prost(uint64, tag = "13")]
    current_projection_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSnapshotManifestHashProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    log_index: u64,
    #[prost(string, tag = "5")]
    log_hash: String,
    #[prost(string, tag = "6")]
    state_hash: String,
    #[prost(string, tag = "7")]
    schema_hash: String,
    #[prost(string, tag = "8")]
    snapshot_object_key: String,
    #[prost(string, tag = "9")]
    snapshot_object_hash: String,
    #[prost(uint64, tag = "10")]
    source_segment_start: u64,
    #[prost(uint64, tag = "11")]
    source_segment_end: u64,
    #[prost(uint64, tag = "12")]
    row_index_generation: u64,
    #[prost(string, tag = "13")]
    created_at: String,
    #[prost(string, tag = "14")]
    created_by_node: String,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbCommitCertificateHashProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    log_index: u64,
    #[prost(string, tag = "5")]
    previous_log_hash: String,
    #[prost(string, tag = "6")]
    entry_hash: String,
    #[prost(string, tag = "7")]
    changeset_payload_hash: String,
    #[prost(string, tag = "8")]
    verified_envelope_hash: String,
    #[prost(uint64, tag = "9")]
    client_log_epoch: u64,
    #[prost(uint64, tag = "10")]
    membership_epoch: u64,
    #[prost(uint64, tag = "11")]
    policy_epoch: u64,
    #[prost(string, tag = "12")]
    leader_replica_id: String,
    #[prost(string, tag = "13")]
    voter_acks_hash: String,
    #[prost(uint64, tag = "14")]
    authz_revision: u64,
    #[prost(string, tag = "15")]
    witness_node_id: String,
    #[prost(string, tag = "16")]
    witnessed_at: String,
}

impl PersonalDbGroupManifest {
    pub async fn seal(mut self, _keyring: &PersonalDbProtocolKeyring) -> Result<Self> {
        validate_group_manifest_unsigned(&self)?;
        require_unsealed(
            self.manifest_hash.as_ref(),
            self.manifest_signature.as_ref(),
            "personaldb group manifest",
        )?;
        let hash = group_manifest_hash_bytes(&self)?;
        self.manifest_hash = Some(hex::encode(hash));
        self.manifest_signature = None;
        Ok(self)
    }

    pub fn verify(&self, _trust_store: &PublicKeyTrustStore) -> Result<()> {
        validate_group_manifest_unsigned(self)?;
        let expected_hash = group_manifest_hash_bytes(self)?;
        if self.manifest_hash.as_deref() != Some(hex::encode(expected_hash).as_str()) {
            return Err(anyhow!("personaldb group manifest hash mismatch"));
        }
        Ok(())
    }
}

impl PersonalDbSnapshotManifest {
    pub async fn seal(mut self, _keyring: &PersonalDbProtocolKeyring) -> Result<Self> {
        validate_snapshot_manifest_unsigned(&self)?;
        require_unsealed(
            self.manifest_hash.as_ref(),
            self.manifest_signature.as_ref(),
            "personaldb snapshot manifest",
        )?;
        let hash = snapshot_manifest_hash_bytes(&self)?;
        self.manifest_hash = Some(hex::encode(hash));
        self.manifest_signature = None;
        Ok(self)
    }

    pub fn verify(&self, _trust_store: &PublicKeyTrustStore) -> Result<()> {
        validate_snapshot_manifest_unsigned(self)?;
        let expected_hash = snapshot_manifest_hash_bytes(self)?;
        if self.manifest_hash.as_deref() != Some(hex::encode(expected_hash).as_str()) {
            return Err(anyhow!("personaldb snapshot manifest hash mismatch"));
        }
        Ok(())
    }
}

impl PersonalDbCommitCertificate {
    pub async fn seal(mut self, _keyring: &PersonalDbProtocolKeyring) -> Result<Self> {
        validate_commit_certificate_unsigned(&self)?;
        require_unsealed(
            self.certificate_hash.as_ref(),
            self.witness_signature.as_ref(),
            "personaldb commit certificate",
        )?;
        let hash = commit_certificate_hash_bytes(&self)?;
        self.certificate_hash = Some(hex::encode(hash));
        self.witness_signature = None;
        Ok(self)
    }

    pub fn verify(&self, _trust_store: &PublicKeyTrustStore) -> Result<()> {
        validate_commit_certificate_unsigned(self)?;
        let expected_hash = commit_certificate_hash_bytes(self)?;
        if self.certificate_hash.as_deref() != Some(hex::encode(expected_hash).as_str()) {
            return Err(anyhow!("personaldb commit certificate hash mismatch"));
        }
        Ok(())
    }
}

impl ProtocolSignable for PersonalDbGroupManifest {
    fn signature_metadata(&self) -> SignatureMetadata {
        signature_metadata(
            SignaturePurpose::GroupControl,
            SignatureDomain::GroupManifest,
            &self.database_id,
            0,
        )
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(group_manifest_payload_hash(self))
    }
}

impl ProtocolSignable for PersonalDbSnapshotManifest {
    fn signature_metadata(&self) -> SignatureMetadata {
        signature_metadata(
            SignaturePurpose::Snapshot,
            SignatureDomain::SnapshotManifest,
            &self.database_id,
            self.log_index,
        )
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(snapshot_manifest_payload_hash(self))
    }
}

impl ProtocolSignable for PersonalDbCommitCertificate {
    fn signature_metadata(&self) -> SignatureMetadata {
        signature_metadata(
            SignaturePurpose::Witness,
            SignatureDomain::CommitCertificate,
            &self.database_id,
            self.log_index,
        )
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(commit_certificate_payload_hash(self))
    }
}

pub fn hash_group_manifest(manifest: &PersonalDbGroupManifest) -> Result<String> {
    Ok(hex::encode(group_manifest_hash_bytes(manifest)?))
}

pub fn hash_snapshot_manifest(manifest: &PersonalDbSnapshotManifest) -> Result<String> {
    Ok(hex::encode(snapshot_manifest_hash_bytes(manifest)?))
}

pub fn hash_commit_certificate(certificate: &PersonalDbCommitCertificate) -> Result<String> {
    Ok(hex::encode(commit_certificate_hash_bytes(certificate)?))
}

fn group_manifest_hash_bytes(manifest: &PersonalDbGroupManifest) -> Result<Hash32> {
    validate_group_manifest_unsigned(manifest)?;
    Ok(group_manifest_payload_hash(manifest))
}

fn snapshot_manifest_hash_bytes(manifest: &PersonalDbSnapshotManifest) -> Result<Hash32> {
    validate_snapshot_manifest_unsigned(manifest)?;
    Ok(snapshot_manifest_payload_hash(manifest))
}

fn commit_certificate_hash_bytes(certificate: &PersonalDbCommitCertificate) -> Result<Hash32> {
    validate_commit_certificate_unsigned(certificate)?;
    Ok(commit_certificate_payload_hash(certificate))
}

fn group_manifest_payload_hash(manifest: &PersonalDbGroupManifest) -> Hash32 {
    hash32(&encode_deterministic_proto(&group_manifest_hash_proto(
        manifest,
    )))
}

fn snapshot_manifest_payload_hash(manifest: &PersonalDbSnapshotManifest) -> Hash32 {
    hash32(&encode_deterministic_proto(&snapshot_manifest_hash_proto(
        manifest,
    )))
}

fn commit_certificate_payload_hash(certificate: &PersonalDbCommitCertificate) -> Hash32 {
    hash32(&encode_deterministic_proto(&commit_certificate_hash_proto(
        certificate,
    )))
}

fn signature_metadata(
    purpose: SignaturePurpose,
    domain: SignatureDomain,
    database_id: &str,
    log_index: u64,
) -> SignatureMetadata {
    SignatureMetadata::for_domain(purpose, domain, log_index).with_scope(
        SignatureScope::for_database_group(DatabaseId::new(database_id), database_id.to_string()),
    )
}

fn group_manifest_hash_proto(
    manifest: &PersonalDbGroupManifest,
) -> PersonalDbGroupManifestHashProto {
    PersonalDbGroupManifestHashProto {
        format_version: u32::from(manifest.format_version),
        tenant_id: manifest.tenant_id.clone(),
        database_id: manifest.database_id.clone(),
        schema_hash: manifest.schema_hash.clone(),
        genesis_hash: manifest.genesis_hash.clone(),
        created_at: manifest.created_at.clone(),
        created_by: manifest.created_by.clone(),
        consistency_policy: manifest.consistency_policy.clone(),
        object_layout_version: u32::from(manifest.object_layout_version),
        active_membership_epoch: manifest.active_membership_epoch,
        active_policy_epoch: manifest.active_policy_epoch,
        current_row_index_generation: manifest.current_row_index_generation,
        current_projection_generation: manifest.current_projection_generation,
    }
}

fn snapshot_manifest_hash_proto(
    manifest: &PersonalDbSnapshotManifest,
) -> PersonalDbSnapshotManifestHashProto {
    PersonalDbSnapshotManifestHashProto {
        format_version: u32::from(manifest.format_version),
        tenant_id: manifest.tenant_id.clone(),
        database_id: manifest.database_id.clone(),
        log_index: manifest.log_index,
        log_hash: manifest.log_hash.clone(),
        state_hash: manifest.state_hash.clone(),
        schema_hash: manifest.schema_hash.clone(),
        snapshot_object_key: manifest.snapshot_object_key.clone(),
        snapshot_object_hash: manifest.snapshot_object_hash.clone(),
        source_segment_start: manifest.source_segment_start,
        source_segment_end: manifest.source_segment_end,
        row_index_generation: manifest.row_index_generation,
        created_at: manifest.created_at.clone(),
        created_by_node: manifest.created_by_node.clone(),
    }
}

fn commit_certificate_hash_proto(
    certificate: &PersonalDbCommitCertificate,
) -> PersonalDbCommitCertificateHashProto {
    PersonalDbCommitCertificateHashProto {
        format_version: u32::from(certificate.format_version),
        tenant_id: certificate.tenant_id.clone(),
        database_id: certificate.database_id.clone(),
        log_index: certificate.log_index,
        previous_log_hash: certificate.previous_log_hash.clone(),
        entry_hash: certificate.entry_hash.clone(),
        changeset_payload_hash: certificate.changeset_payload_hash.clone(),
        verified_envelope_hash: certificate.verified_envelope_hash.clone(),
        client_log_epoch: certificate.client_log_epoch,
        membership_epoch: certificate.membership_epoch,
        policy_epoch: certificate.policy_epoch,
        leader_replica_id: certificate.leader_replica_id.clone(),
        voter_acks_hash: certificate.voter_acks_hash.clone(),
        authz_revision: certificate.authz_revision,
        witness_node_id: certificate.witness_node_id.clone(),
        witnessed_at: certificate.witnessed_at.clone(),
    }
}

pub(crate) fn validate_group_manifest_unsigned(manifest: &PersonalDbGroupManifest) -> Result<()> {
    if manifest.format_version != 2 {
        return Err(anyhow!("unsupported personaldb group manifest version"));
    }
    if manifest.consistency_policy != "StrictWitnessed" {
        return Err(anyhow!("unsupported personaldb consistency policy"));
    }
    validate_hex32(&manifest.schema_hash, "schema_hash")?;
    validate_hex32(&manifest.genesis_hash, "genesis_hash")?;
    require_nonempty(&manifest.tenant_id, "tenant_id")?;
    require_nonempty(&manifest.database_id, "database_id")?;
    require_nonempty(&manifest.created_by, "created_by")?;
    Ok(())
}

pub(crate) fn validate_snapshot_manifest_unsigned(
    manifest: &PersonalDbSnapshotManifest,
) -> Result<()> {
    if manifest.format_version != 1 {
        return Err(anyhow!("unsupported personaldb snapshot manifest version"));
    }
    validate_hex32(&manifest.log_hash, "log_hash")?;
    validate_hex32(&manifest.state_hash, "state_hash")?;
    validate_hex32(&manifest.schema_hash, "schema_hash")?;
    validate_hex32(&manifest.snapshot_object_hash, "snapshot_object_hash")?;
    require_nonempty(&manifest.tenant_id, "tenant_id")?;
    require_nonempty(&manifest.database_id, "database_id")?;
    require_corestore_ref(
        &manifest.snapshot_object_key,
        "snapshot_object_ref",
        PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX,
    )?;
    require_nonempty(&manifest.created_by_node, "created_by_node")?;
    if manifest.source_segment_start > manifest.source_segment_end {
        return Err(anyhow!("snapshot source segment range is invalid"));
    }
    Ok(())
}

pub(crate) fn validate_commit_certificate_unsigned(
    certificate: &PersonalDbCommitCertificate,
) -> Result<()> {
    if certificate.format_version != 2 {
        return Err(anyhow!("unsupported personaldb commit certificate version"));
    }
    validate_hex32(&certificate.previous_log_hash, "previous_log_hash")?;
    validate_hex32(&certificate.entry_hash, "entry_hash")?;
    validate_hex32(
        &certificate.changeset_payload_hash,
        "changeset_payload_hash",
    )?;
    validate_hex32(
        &certificate.verified_envelope_hash,
        "verified_envelope_hash",
    )?;
    validate_hex32(&certificate.voter_acks_hash, "voter_acks_hash")?;
    require_nonempty(&certificate.tenant_id, "tenant_id")?;
    require_nonempty(&certificate.database_id, "database_id")?;
    require_nonempty(&certificate.leader_replica_id, "leader_replica_id")?;
    require_nonempty(&certificate.witness_node_id, "witness_node_id")?;
    Ok(())
}

fn require_unsealed<T>(
    hash: Option<&String>,
    signature: Option<&T>,
    object_name: &'static str,
) -> Result<()> {
    if hash.is_some() || signature.is_some() {
        return Err(anyhow!("{object_name} is already sealed"));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be 32 bytes encoded as hex"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn require_corestore_ref(value: &str, field: &'static str, prefix: &str) -> Result<()> {
    require_nonempty(value, field)?;
    if !value.starts_with(prefix) {
        return Err(anyhow!("{field} must be a CoreStore/CoreMeta ref"));
    }
    if value.contains('/') || value.contains('\\') || value.chars().any(char::is_control) {
        return Err(anyhow!("{field} must not be a storage path"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::personaldb_protocol_keyring;
    use personaldb_protocol::signing_preimage;

    #[tokio::test]
    async fn group_manifest_seal_verify_and_tamper_reject() {
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_group_manifest().seal(&keyring).await.unwrap();
        manifest.verify(keyring.trust_store()).unwrap();
        assert_eq!(manifest.manifest_hash.as_deref().unwrap().len(), 64);
        assert_eq!(
            manifest
                .manifest_signature
                .as_ref()
                .unwrap()
                .signature
                .as_bytes()
                .len(),
            64
        );

        let mut tampered = manifest;
        tampered.active_policy_epoch += 1;
        assert!(tampered.verify(keyring.trust_store()).is_err());
    }

    #[tokio::test]
    async fn snapshot_manifest_seal_verify_and_tamper_reject() {
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_snapshot_manifest().seal(&keyring).await.unwrap();
        manifest.verify(keyring.trust_store()).unwrap();
        let mut tampered = manifest;
        tampered.snapshot_object_hash = hex::encode([7; 32]);
        assert!(tampered.verify(keyring.trust_store()).is_err());
    }

    #[tokio::test]
    async fn commit_certificate_seal_verify_and_tamper_reject() {
        let keyring = personaldb_protocol_keyring();
        let certificate = sample_commit_certificate().seal(&keyring).await.unwrap();
        certificate.verify(keyring.trust_store()).unwrap();
        assert_eq!(certificate.certificate_hash.as_deref().unwrap().len(), 64);

        let mut tampered = certificate;
        tampered.authz_revision += 1;
        assert!(tampered.verify(keyring.trust_store()).is_err());
    }

    #[tokio::test]
    async fn group_manifest_rejects_unsupported_policy() {
        let keyring = personaldb_protocol_keyring();
        let mut manifest = sample_group_manifest();
        manifest.consistency_policy = "EventuallyAccepted".to_string();
        assert!(manifest.seal(&keyring).await.is_err());
    }

    #[tokio::test]
    async fn signature_purpose_is_object_specific() {
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_group_manifest().seal(&keyring).await.unwrap();
        let mut certificate = sample_commit_certificate().seal(&keyring).await.unwrap();
        certificate.witness_signature = manifest.manifest_signature;
        assert!(certificate.verify(keyring.trust_store()).is_err());
    }

    #[tokio::test]
    async fn commit_certificate_ed25519_vector() {
        let keyring = personaldb_protocol_keyring();
        let unsigned = sample_commit_certificate();
        let unsigned_bytes = encode_deterministic_proto(&commit_certificate_hash_proto(&unsigned));
        let object_hash = commit_certificate_hash_bytes(&unsigned).unwrap();
        let metadata = unsigned.signature_metadata();
        let preimage = signing_preimage(
            metadata.domain,
            metadata.signed_payload_version,
            SigningPayload::Sha256Digest(object_hash),
        )
        .unwrap();
        let sealed = unsigned.seal(&keyring).await.unwrap();
        let envelope = sealed.witness_signature.as_ref().unwrap();
        let envelope_bytes = envelope.encode_deterministic().unwrap();

        assert_eq!(
            hex::encode(unsigned_bytes),
            "0802120674656e616e741a02646220012a40303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303030303240303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313031303130313a403032303230323032303230323032303230323032303230323032303230323032303230323032303230323032303230323032303230323032303230323032303242403033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303330333033303348015001580162077265706c6963616a403034303430343034303430343034303430343034303430343034303430343034303430343034303430343034303430343034303430343034303430343034303470017a046e6f646582011e323032362d30362d32305430303a30303a30302e3030303030303030305a"
        );
        assert_eq!(
            hex::encode(object_hash),
            "66ca6cbdd3c03c3da9bb67e259354f37756a781f98d7daafb33acaab89c74557"
        );
        assert_eq!(
            hex::encode(preimage.as_bytes()),
            "706572736f6e616c646200636f6d6d69742d6365727469666963617465000000000266ca6cbdd3c03c3da9bb67e259354f37756a781f98d7daafb33acaab89c74557"
        );
        assert_eq!(
            envelope.key_id.as_str(),
            "sha256:512ae918f6ee80cdfb87093abb416a47f64c01244b5a816a84e892825394f02e"
        );
        assert_eq!(
            hex::encode(envelope.signature.as_bytes()),
            "0bc12a16796f3dfb5a8516d6a3e7053a2448be3b7f1ed6ae75dfc15ecb48bb9707872556444d37aeb139fe8ac52af939347504ad876ccc9cfc6d87c917874109"
        );
        assert_eq!(
            hex::encode(envelope_bytes),
            "08011001180122477368613235363a353132616539313866366565383063646662383730393361626234313661343766363463303132343462356138313661383465383932383235333934663032652a400bc12a16796f3dfb5a8516d6a3e7053a2448be3b7f1ed6ae75dfc15ecb48bb9707872556444d37aeb139fe8ac52af939347504ad876ccc9cfc6d87c917874109"
        );
        sealed.verify(keyring.trust_store()).unwrap();
    }

    fn sample_group_manifest() -> PersonalDbGroupManifest {
        PersonalDbGroupManifest {
            format_version: 2,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            schema_hash: hex::encode([1; 32]),
            genesis_hash: hex::encode([2; 32]),
            created_at: "2026-06-20T00:00:00.000000000Z".to_string(),
            created_by: "principal".to_string(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 1,
            active_policy_epoch: 1,
            current_row_index_generation: 0,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
    }

    fn sample_snapshot_manifest() -> PersonalDbSnapshotManifest {
        PersonalDbSnapshotManifest {
            format_version: 1,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            log_index: 1000,
            log_hash: hex::encode([1; 32]),
            state_hash: hex::encode([2; 32]),
            schema_hash: hex::encode([3; 32]),
            snapshot_object_key: concat!(
                "personaldb_snapshot_object:tenant:tenant:database:db:",
                "log:00000000000000001000:",
                "state:0000000000000000000000000000000000000000000000000000000000000002"
            )
            .to_string(),
            snapshot_object_hash: hex::encode([4; 32]),
            source_segment_start: 1,
            source_segment_end: 1000,
            row_index_generation: 12,
            created_at: "2026-06-20T00:00:00.000000000Z".to_string(),
            created_by_node: "node".to_string(),
            manifest_hash: None,
            manifest_signature: None,
        }
    }

    fn sample_commit_certificate() -> PersonalDbCommitCertificate {
        PersonalDbCommitCertificate {
            format_version: 2,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            log_index: 1,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode([2; 32]),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 1,
            policy_epoch: 1,
            leader_replica_id: "replica".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 1,
            witness_node_id: "node".to_string(),
            witnessed_at: "2026-06-20T00:00:00.000000000Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
