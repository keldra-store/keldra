use crate::{
    personaldb_control::{
        PersonalDbCommitCertificate, PersonalDbGroupManifest, PersonalDbSnapshotManifest,
        validate_commit_certificate_unsigned, validate_group_manifest_unsigned,
        validate_snapshot_manifest_unsigned,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, PersonalDbSnapshotsHead, validate_committed_head_unsigned,
        validate_snapshots_head_unsigned,
    },
};
use anyhow::{Result, bail};
use personaldb_protocol::{
    PreparedProtocolSignature, ProtocolSignable, SignatureMetadata, SigningPayload,
};

const MAX_PERSONALDB_SIGNING_STRING_BYTES: usize = 4 * 1024;
const MAX_PERSONALDB_CANONICAL_SIGNING_BYTES: usize = 1024 * 1024;

/// A validated PersonalDB control object that Anvil may sign.
///
/// This type deliberately contains no transport or key-custody behavior. Providers receive the
/// typed object in process, after its unsigned shape and bounded strings have been validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbSigningObject {
    GroupManifest(PersonalDbGroupManifest),
    SnapshotManifest(PersonalDbSnapshotManifest),
    CommitCertificate(PersonalDbCommitCertificate),
    CommittedHead(PersonalDbCommittedHead),
    SnapshotsHead(PersonalDbSnapshotsHead),
    CanonicalProtocol(PreparedProtocolSignature),
}

impl PersonalDbSigningObject {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::GroupManifest(manifest) => validate_group_manifest_unsigned(manifest),
            Self::SnapshotManifest(manifest) => validate_snapshot_manifest_unsigned(manifest),
            Self::CommitCertificate(certificate) => {
                validate_commit_certificate_unsigned(certificate)
            }
            Self::CommittedHead(head) => validate_committed_head_unsigned(head),
            Self::SnapshotsHead(head) => validate_snapshots_head_unsigned(head),
            Self::CanonicalProtocol(prepared) => {
                if prepared.payload_len() > MAX_PERSONALDB_CANONICAL_SIGNING_BYTES {
                    bail!("canonical PersonalDB signing payload exceeds the protocol bound");
                }
                Ok(())
            }
        }?;
        self.validate_string_bounds()
    }

    pub fn metadata(&self) -> SignatureMetadata {
        self.signature_metadata()
    }

    fn validate_string_bounds(&self) -> Result<()> {
        let fields: &[(&str, &str)] = match self {
            Self::GroupManifest(manifest) => &[
                ("tenant_id", &manifest.tenant_id),
                ("database_id", &manifest.database_id),
                ("schema_hash", &manifest.schema_hash),
                ("genesis_hash", &manifest.genesis_hash),
                ("created_at", &manifest.created_at),
                ("created_by", &manifest.created_by),
                ("consistency_policy", &manifest.consistency_policy),
            ],
            Self::SnapshotManifest(manifest) => &[
                ("tenant_id", &manifest.tenant_id),
                ("database_id", &manifest.database_id),
                ("log_hash", &manifest.log_hash),
                ("state_sha256", &manifest.state_sha256),
                ("schema_hash", &manifest.schema_hash),
                ("snapshot_object_key", &manifest.snapshot_object_key),
                ("snapshot_object_sha256", &manifest.snapshot_object_sha256),
                ("created_at", &manifest.created_at),
                ("created_by_node", &manifest.created_by_node),
            ],
            Self::CommitCertificate(certificate) => &[
                ("tenant_id", &certificate.tenant_id),
                ("database_id", &certificate.database_id),
                ("previous_log_hash", &certificate.previous_log_hash),
                ("entry_hash", &certificate.entry_hash),
                (
                    "changeset_payload_hash",
                    &certificate.changeset_payload_hash,
                ),
                (
                    "verified_envelope_hash",
                    &certificate.verified_envelope_hash,
                ),
                ("leader_replica_id", &certificate.leader_replica_id),
                ("voter_acks_hash", &certificate.voter_acks_hash),
                ("witness_node_id", &certificate.witness_node_id),
                ("witnessed_at", &certificate.witnessed_at),
            ],
            Self::CommittedHead(head) => &[
                ("tenant_id", &head.tenant_id),
                ("database_id", &head.database_id),
                ("log_hash", &head.log_hash),
                ("segment_ref", &head.segment_ref),
                ("schema_hash", &head.schema_hash),
                ("updated_at", &head.updated_at),
                ("updated_by_node", &head.updated_by_node),
            ],
            Self::SnapshotsHead(head) => &[
                ("tenant_id", &head.tenant_id),
                ("database_id", &head.database_id),
                ("latest_snapshot_log_hash", &head.latest_snapshot_log_hash),
                (
                    "latest_snapshot_manifest_ref",
                    &head.latest_snapshot_manifest_ref,
                ),
                ("updated_at", &head.updated_at),
                ("updated_by_node", &head.updated_by_node),
            ],
            Self::CanonicalProtocol(_) => &[],
        };

        for (name, value) in fields {
            if value.len() > MAX_PERSONALDB_SIGNING_STRING_BYTES {
                bail!("PersonalDB signing field {name} exceeds the protocol bound");
            }
        }
        Ok(())
    }
}

impl ProtocolSignable for PersonalDbSigningObject {
    fn signature_metadata(&self) -> SignatureMetadata {
        match self {
            Self::GroupManifest(object) => object.signature_metadata(),
            Self::SnapshotManifest(object) => object.signature_metadata(),
            Self::CommitCertificate(object) => object.signature_metadata(),
            Self::CommittedHead(object) => object.signature_metadata(),
            Self::SnapshotsHead(object) => object.signature_metadata(),
            Self::CanonicalProtocol(object) => object.signature_metadata(),
        }
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        match self {
            Self::GroupManifest(object) => object.signing_payload(),
            Self::SnapshotManifest(object) => object.signing_payload(),
            Self::CommitCertificate(object) => object.signing_payload(),
            Self::CommittedHead(object) => object.signing_payload(),
            Self::SnapshotsHead(object) => object.signing_payload(),
            Self::CanonicalProtocol(object) => object.signing_payload(),
        }
    }
}
