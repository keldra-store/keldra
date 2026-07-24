use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    core_store::{
        CF_PERSONALDB, CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMutationBatch,
        CoreMutationBatchReceipt, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
        CoreTransactionState, TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
        TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
        core_meta_committed_row_common, core_meta_payload_digest, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
    partition_fence::{
        PartitionOwnerState, PartitionWritePermit, partition_write_precondition,
        read_partition_owner, validate_write_permit_for_state,
    },
    personaldb_commit_store::{decode_commit_certificate, encode_commit_certificate},
    personaldb_control::{PersonalDbCommitCertificate, validate_commit_certificate_unsigned},
    personaldb_coremeta::{
        personaldb_partition_id, personaldb_realm_id, personaldb_root_anchor_key,
        personaldb_root_key_hash, tenant_id_from_realm,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, decode_committed_head, encode_committed_head,
        personaldb_committed_head_precondition, read_personaldb_committed_head,
        validate_committed_head_unsigned,
    },
    personaldb_signing::{
        PersonalDbProtocolKeyring, signature_envelope_from_proto, signature_envelope_to_proto,
    },
    personaldb_signing_object::PersonalDbSigningObject,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use personaldb_protocol::{
    DatabaseId, KeyGeneration, KeyId, ProtocolSignable, ProtocolSigner, PublicKeyStatus,
    PublicKeyTrustRecord, PublicKeyTrustStore, SignatureDomain, SignatureEnvelopeV1,
    SignatureMetadata, SignaturePurpose, SignatureScope, SigningPayload,
};
use prost::Message;
use sha2::{Digest, Sha256};

const FORMAT_VERSION_V1: u32 = 1;
const MAX_ADMISSION_LIFETIME_SECONDS: i64 = 5 * 60;
const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_SIGNING_OBJECT_BYTES: usize = 64 * 1024;

const CLAIM_HASH_DOMAIN: &[u8] = b"anvil-proposal-idempotency-claim-v1\0";
const RESERVATION_ID_DOMAIN: &[u8] = b"anvil-proposal-admission-reservation-id-v1\0";
const RESERVATION_HASH_DOMAIN: &[u8] = b"anvil-proposal-admission-reservation-v1\0";
const SLOT_HASH_DOMAIN: &[u8] = b"anvil-proposal-admission-slot-v1\0";
const UNSIGNED_CERTIFICATE_HASH_DOMAIN: &[u8] = b"anvil-unsigned-commit-certificate-v2\0";
const HEAD_TEMPLATE_HASH_DOMAIN: &[u8] = b"anvil-witness-head-template-v1\0";
const UNSIGNED_HEAD_HASH_DOMAIN: &[u8] = b"anvil-unsigned-committed-head-v2\0";
const CANDIDATE_HASH_DOMAIN: &[u8] = b"anvil-witness-signing-candidate-v1\0";
const RECEIPT_HASH_DOMAIN: &[u8] = b"anvil-witness-dual-signing-receipt-v1\0";
const SIGNED_CERTIFICATE_HASH_DOMAIN: &[u8] = b"personaldb-commit-certificate-v2\0";
const SIGNED_HEAD_HASH_DOMAIN: &[u8] = b"personaldb-committed-head-v2\0";
const SIGNED_ADMISSION_HASH_DOMAIN: &[u8] = b"personaldb-proposal-admission-v1\0";

const CLAIM_KEY_PREFIX: &str = "proposal-idempotency-claim";
const SLOT_KEY_PREFIX: &str = "proposal-slot";
const RESERVATION_KEY_PREFIX: &str = "proposal-reservation";
const CANDIDATE_KEY_PREFIX: &str = "witness-candidate";
const RECEIPT_KEY_PREFIX: &str = "witness-signing";

pub const PERSONALDB_GROUP_PARTITION_FAMILY: &str = "personaldb_group";

mod codec;
mod storage_helpers;
mod validation;
use codec::*;
use storage_helpers::*;
use validation::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalIdempotencyClaimIdentityV1 {
    pub format_version: u32,
    pub tenant_id: String,
    pub application_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub database_id: String,
    pub client_proposal_hash_sha256: [u8; 32],
    pub changeset_payload_hash_sha256: [u8; 32],
    pub workflow_id: String,
    pub fencing_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ProposalAdmissionReservationStateV1 {
    Unspecified = 0,
    Reserved = 1,
    Signing = 2,
    Committed = 3,
    Revoked = 4,
    Expired = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ProposalAdmissionSlotStateV1 {
    Unspecified = 0,
    Reserved = 1,
    Signed = 2,
    Committed = 3,
    ReleasedUnsigned = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionSlotV1 {
    pub format_version: u32,
    pub database_id: String,
    pub next_log_index: u64,
    pub expected_previous_log_hash_sha256: [u8; 32],
    pub placement_epoch: u64,
    pub client_log_epoch: u64,
    pub fencing_generation: u64,
    pub slot_revision: u64,
    pub reservation_id: String,
    pub state: ProposalAdmissionSlotStateV1,
    pub witness_dual_signing_receipt_sha256: Option<[u8; 32]>,
    pub terminal_committed_head_sha256: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionReservationIdentityV1 {
    pub format_version: u32,
    pub reservation_id: String,
    pub database_id: String,
    pub group_kind: String,
    pub proposer_id: String,
    pub client_proposal_hash_sha256: [u8; 32],
    pub changeset_payload_hash_sha256: [u8; 32],
    pub expected_previous_log_index: u64,
    pub expected_previous_log_hash_sha256: [u8; 32],
    pub membership_revision: u64,
    pub placement_epoch: u64,
    pub client_log_epoch: u64,
    pub workflow_id: String,
    pub fencing_generation: u64,
    pub leader_lease_id: String,
    pub leader_lease_revision: u64,
    pub authorization_receipt_sha256: [u8; 32],
    pub authorization_revision: u64,
    pub idempotency_claim_sha256: [u8; 32],
    pub issued_at_unix_seconds: i64,
    pub expires_at_unix_seconds: i64,
    pub selected_voter_ids: Vec<String>,
    pub primary_server_id: String,
    pub proposal_admission_key_id: String,
    pub proposal_admission_generation: u64,
    pub witness_key_id: String,
    pub witness_key_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionReservationV1 {
    pub identity: ProposalAdmissionReservationIdentityV1,
    pub reservation_revision: u64,
    pub state: ProposalAdmissionReservationStateV1,
    pub candidate_entry_core_sha256: Option<[u8; 32]>,
    pub candidate_unsigned_certificate_sha256: Option<[u8; 32]>,
    pub candidate_head_template_sha256: Option<[u8; 32]>,
    pub candidate_unsigned_committed_head_sha256: Option<[u8; 32]>,
    pub witness_dual_signing_receipt_sha256: Option<[u8; 32]>,
    pub terminal_commit_certificate_sha256: Option<[u8; 32]>,
    pub terminal_committed_head_sha256: Option<[u8; 32]>,
    pub witness_signing_candidate_sha256: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessSigningCandidateV1 {
    pub format_version: u32,
    pub reservation_id: String,
    pub signing_reservation_revision: u64,
    pub database_id: String,
    pub next_log_index: u64,
    pub client_log_epoch: u64,
    pub fencing_generation: u64,
    pub witness_key_id: String,
    pub witness_key_generation: u64,
    pub unsigned_commit_certificate: Vec<u8>,
    pub head_template: Vec<u8>,
    pub candidate_entry_core_sha256: [u8; 32],
    pub candidate_unsigned_certificate_sha256: [u8; 32],
    pub candidate_head_template_sha256: [u8; 32],
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessDualSigningReceiptV1 {
    pub format_version: u32,
    pub reservation_id: String,
    pub signing_reservation_revision: u64,
    pub candidate_unsigned_certificate_sha256: [u8; 32],
    pub candidate_head_template_sha256: [u8; 32],
    pub witness_key_id: String,
    pub witness_key_generation: u64,
    pub certificate_signature_sha256: [u8; 32],
    pub signed_commit_certificate_sha256: [u8; 32],
    pub unsigned_committed_head_sha256: [u8; 32],
    pub head_signature_sha256: [u8; 32],
    pub signed_committed_head_sha256: [u8; 32],
    pub signed_at_unix_seconds: i64,
    pub signed_commit_certificate: Vec<u8>,
    pub signed_committed_head: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionV1 {
    pub format_version: u32,
    pub database_id: String,
    pub group_kind: String,
    pub proposer_id: String,
    pub client_proposal_hash_sha256: [u8; 32],
    pub workflow_id: String,
    pub fencing_generation: u64,
    pub authorization_revision: u64,
    pub idempotency_claim_sha256: [u8; 32],
    pub issued_at_unix_seconds: i64,
    pub expires_at_unix_seconds: i64,
    pub reservation_id: String,
    pub reservation_revision: u64,
    pub reservation_identity_sha256: [u8; 32],
    pub authorization_receipt_sha256: [u8; 32],
    pub selected_voter_ids: Vec<String>,
    pub primary_server_id: String,
    pub proposal_admission_key_id: String,
    pub proposal_admission_generation: u64,
    pub witness_key_id: String,
    pub witness_key_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedProposalAdmissionV1 {
    pub admission: ProposalAdmissionV1,
    pub proposal_admission_signature: SignatureEnvelopeV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProposalAdmissionReservationV1 {
    pub tenant_id: i64,
    pub reservation: ProposalAdmissionReservationV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginWitnessSigningV1 {
    pub tenant_id: i64,
    pub reservation_id: String,
    pub expected_reservation_revision: u64,
    pub unsigned_commit_certificate: PersonalDbCommitCertificate,
    pub head_template: PersonalDbCommittedHead,
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignCertificateAndHeadV1 {
    pub reservation_id: String,
    pub signing_reservation_revision: u64,
}

pub struct PersonalDbAdmissionAuthority<'a> {
    pub storage: &'a Storage,
    pub trust_store: &'a PublicKeyTrustStore,
    pub write_permit: &'a PartitionWritePermit,
    pub partition_owner_signing_key: &'a [u8],
    pub now_unix_seconds: i64,
}

impl ProposalIdempotencyClaimIdentityV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_claim(self)?;
        Ok(domain_hash(
            CLAIM_HASH_DOMAIN,
            &encode_deterministic_proto(&claim_to_proto(self)),
        ))
    }
}

impl ProposalAdmissionReservationIdentityV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_reservation_identity_shape(self)?;
        Ok(domain_hash(
            RESERVATION_HASH_DOMAIN,
            &encode_deterministic_proto(&reservation_identity_to_proto(self)),
        ))
    }
}

impl ProposalAdmissionSlotV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_slot(self)?;
        Ok(domain_hash(
            SLOT_HASH_DOMAIN,
            &encode_deterministic_proto(&slot_to_proto(self)),
        ))
    }
}

impl WitnessSigningCandidateV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_candidate(self)?;
        Ok(domain_hash(
            CANDIDATE_HASH_DOMAIN,
            &encode_deterministic_proto(&candidate_to_proto(self)),
        ))
    }
}

impl WitnessDualSigningReceiptV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_receipt_shape(self)?;
        Ok(domain_hash(
            RECEIPT_HASH_DOMAIN,
            &encode_deterministic_proto(&receipt_to_proto(self)),
        ))
    }
}

impl ProposalAdmissionV1 {
    pub fn encode_deterministic(&self) -> Result<Vec<u8>> {
        validate_admission_shape(self)?;
        Ok(encode_deterministic_proto(&admission_to_proto(self)))
    }
}

impl SignedProposalAdmissionV1 {
    pub fn encode_deterministic(&self) -> Result<Vec<u8>> {
        self.proposal_admission_signature.validate_shape()?;
        Ok(encode_deterministic_proto(&SignedProposalAdmissionProto {
            admission: Some(admission_to_proto(&self.admission)),
            proposal_admission_signature: Some(signature_envelope_to_proto(
                &self.proposal_admission_signature,
            )),
        }))
    }

    pub fn decode_deterministic(bytes: &[u8]) -> Result<Self> {
        let proto = decode_deterministic_proto::<SignedProposalAdmissionProto>(
            bytes,
            "signed proposal admission",
        )?;
        let admission = admission_from_proto(
            proto
                .admission
                .ok_or_else(|| anyhow!("signed proposal admission body missing"))?,
        )?;
        validate_admission_shape(&admission)?;
        let proposal_admission_signature = signature_envelope_from_proto(
            proto
                .proposal_admission_signature
                .ok_or_else(|| anyhow!("signed proposal admission signature missing"))?,
        )?;
        Ok(Self {
            admission,
            proposal_admission_signature,
        })
    }

    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        Ok(domain_hash(
            SIGNED_ADMISSION_HASH_DOMAIN,
            &self.encode_deterministic()?,
        ))
    }

    pub fn verify(
        &self,
        reservation: &ProposalAdmissionReservationV1,
        trust_store: &PublicKeyTrustStore,
    ) -> Result<()> {
        let expected = admission_from_reservation(reservation)?;
        if self.admission != expected {
            bail!("signed proposal admission does not match its reservation");
        }
        let signable = ProposalAdmissionSignable {
            admission: &self.admission,
            log_index: next_log_index(&reservation.identity)?,
        };
        trust_store.verify(&signable, &self.proposal_admission_signature)?;
        Ok(())
    }
}

struct ProposalAdmissionSignable<'a> {
    admission: &'a ProposalAdmissionV1,
    log_index: u64,
}

impl ProtocolSignable for ProposalAdmissionSignable<'_> {
    fn signature_metadata(&self) -> SignatureMetadata {
        SignatureMetadata::for_domain(
            SignaturePurpose::ProposalAdmission,
            SignatureDomain::ProposalAdmission,
            self.log_index,
        )
        .with_scope(SignatureScope::for_database_group(
            DatabaseId::new(&self.admission.database_id),
            self.admission.database_id.clone(),
        ))
        .requiring_key_generation(
            KeyGeneration::new(self.admission.proposal_admission_generation)
                .expect("validated proposal admission key generation"),
        )
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(
            Sha256::digest(encode_deterministic_proto(&admission_to_proto(
                self.admission,
            )))
            .into(),
        )
    }
}

struct RequiredGenerationSignable<'a> {
    object: &'a dyn ProtocolSignable,
    generation: KeyGeneration,
}

impl ProtocolSignable for RequiredGenerationSignable<'_> {
    fn signature_metadata(&self) -> SignatureMetadata {
        self.object
            .signature_metadata()
            .requiring_key_generation(self.generation)
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        self.object.signing_payload()
    }
}

enum WitnessSigningAuthority<'a> {
    ProtocolSigner(&'a dyn ProtocolSigner),
    Keyring(&'a PersonalDbProtocolKeyring),
}

impl WitnessSigningAuthority<'_> {
    fn validate_bound_signer(
        &self,
        expected_key_id: &str,
        expected_generation: u64,
        database_id: &str,
        log_index: u64,
    ) -> Result<()> {
        match self {
            Self::ProtocolSigner(signer) => validate_bound_signer(
                signer.trust_record(),
                SignaturePurpose::Witness,
                expected_key_id,
                expected_generation,
                database_id,
                log_index,
            ),
            Self::Keyring(keyring) => {
                let record = keyring.trust_record_for_purpose(SignaturePurpose::Witness)?;
                validate_bound_signer(
                    record,
                    SignaturePurpose::Witness,
                    expected_key_id,
                    expected_generation,
                    database_id,
                    log_index,
                )
            }
        }
    }

    async fn sign_certificate_with_required_generation(
        &self,
        certificate: &PersonalDbCommitCertificate,
        generation: KeyGeneration,
        trust_store: &PublicKeyTrustStore,
    ) -> Result<SignatureEnvelopeV1> {
        let signature = match self {
            Self::ProtocolSigner(signer) => {
                let signable = RequiredGenerationSignable {
                    object: certificate,
                    generation,
                };
                let signature = signer.sign(&signable)?;
                trust_store.verify(&signable, &signature)?;
                return Ok(signature);
            }
            Self::Keyring(keyring) => {
                keyring
                    .sign(PersonalDbSigningObject::CommitCertificate(
                        certificate.clone(),
                    ))
                    .await?
            }
        };
        let signable = RequiredGenerationSignable {
            object: certificate,
            generation,
        };
        trust_store.verify(&signable, &signature)?;
        Ok(signature)
    }

    async fn sign_head_with_required_generation(
        &self,
        head: &PersonalDbCommittedHead,
        generation: KeyGeneration,
        trust_store: &PublicKeyTrustStore,
    ) -> Result<SignatureEnvelopeV1> {
        let signature = match self {
            Self::ProtocolSigner(signer) => {
                let signable = RequiredGenerationSignable {
                    object: head,
                    generation,
                };
                let signature = signer.sign(&signable)?;
                trust_store.verify(&signable, &signature)?;
                return Ok(signature);
            }
            Self::Keyring(keyring) => {
                keyring
                    .sign(PersonalDbSigningObject::CommittedHead(head.clone()))
                    .await?
            }
        };
        let signable = RequiredGenerationSignable {
            object: head,
            generation,
        };
        trust_store.verify(&signable, &signature)?;
        Ok(signature)
    }
}

pub fn sign_proposal_admission(
    reservation: &ProposalAdmissionReservationV1,
    signer: &dyn ProtocolSigner,
    trust_store: &PublicKeyTrustStore,
) -> Result<SignedProposalAdmissionV1> {
    validate_reservation(reservation)?;
    if reservation.state != ProposalAdmissionReservationStateV1::Reserved {
        bail!("proposal admission can be signed only for a reserved reservation");
    }
    validate_bound_signer(
        signer.trust_record(),
        SignaturePurpose::ProposalAdmission,
        &reservation.identity.proposal_admission_key_id,
        reservation.identity.proposal_admission_generation,
        &reservation.identity.database_id,
        next_log_index(&reservation.identity)?,
    )?;
    let admission = admission_from_reservation(reservation)?;
    let signable = ProposalAdmissionSignable {
        admission: &admission,
        log_index: next_log_index(&reservation.identity)?,
    };
    let proposal_admission_signature = signer.sign(&signable)?;
    let signed = SignedProposalAdmissionV1 {
        admission,
        proposal_admission_signature,
    };
    signed.verify(reservation, trust_store)?;
    Ok(signed)
}

pub fn derive_reservation_id(
    database_id: &str,
    idempotency_claim_sha256: [u8; 32],
) -> Result<String> {
    validate_text(database_id, "database_id")?;
    let mut hasher = Sha256::new();
    hasher.update(RESERVATION_ID_DOMAIN);
    hasher.update(database_id.as_bytes());
    hasher.update([0]);
    hasher.update(idempotency_claim_sha256);
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

pub fn personaldb_group_partition_owner_id(tenant_id: i64, database_id: &str) -> Result<String> {
    validate_tenant_database(tenant_id, database_id)?;
    Ok(hex::encode(hash32(
        format!("{PERSONALDB_GROUP_PARTITION_FAMILY}\0{tenant_id}\0{database_id}").as_bytes(),
    )))
}

pub fn personaldb_group_leader_lease_id(owner: &PartitionOwnerState) -> String {
    format!(
        "partition-owner:{}:{}",
        owner.partition_family, owner.partition_id
    )
}

pub async fn reserve_personaldb_proposal(
    authority: &PersonalDbAdmissionAuthority<'_>,
    claim: ProposalIdempotencyClaimIdentityV1,
    identity: ProposalAdmissionReservationIdentityV1,
) -> Result<ProposalAdmissionReservationV1> {
    validate_claim(&claim)?;
    let tenant_id = parse_claim_tenant_id(&claim)?;
    if identity.database_id != claim.database_id
        || identity.client_proposal_hash_sha256 != claim.client_proposal_hash_sha256
        || identity.changeset_payload_hash_sha256 != claim.changeset_payload_hash_sha256
        || identity.workflow_id != claim.workflow_id
        || identity.fencing_generation != claim.fencing_generation
    {
        bail!("proposal reservation identity does not match its idempotency claim");
    }
    let claim_hash = claim.hash_sha256()?;
    if identity.idempotency_claim_sha256 != claim_hash {
        bail!("proposal reservation idempotency claim hash mismatch");
    }
    let expected_reservation_id = derive_reservation_id(&claim.database_id, claim_hash)?;
    if identity.reservation_id != expected_reservation_id {
        bail!("proposal reservation ID is not canonical");
    }

    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, &identity, &guard)?;
    validate_active_key_binding(
        authority.trust_store,
        &identity.proposal_admission_key_id,
        identity.proposal_admission_generation,
        SignaturePurpose::ProposalAdmission,
        &identity.database_id,
        next_log_index(&identity)?,
    )?;
    validate_active_key_binding(
        authority.trust_store,
        &identity.witness_key_id,
        identity.witness_key_generation,
        SignaturePurpose::Witness,
        &identity.database_id,
        next_log_index(&identity)?,
    )?;

    let reservation = ProposalAdmissionReservationV1 {
        identity,
        reservation_revision: 1,
        state: ProposalAdmissionReservationStateV1::Reserved,
        candidate_entry_core_sha256: None,
        candidate_unsigned_certificate_sha256: None,
        candidate_head_template_sha256: None,
        candidate_unsigned_committed_head_sha256: None,
        witness_dual_signing_receipt_sha256: None,
        terminal_commit_certificate_sha256: None,
        terminal_committed_head_sha256: None,
        witness_signing_candidate_sha256: None,
    };
    validate_reservation(&reservation)?;
    let slot = slot_from_reservation(&reservation)?;

    let claim_key = claim_key(&claim)?;
    let slot_key = slot_key(
        tenant_id,
        &slot.database_id,
        slot.next_log_index,
        slot.client_log_epoch,
    )?;
    let reservation_key = reservation_key(&reservation.identity.reservation_id)?;
    let existing_claim = read_claim_row(authority.storage, &claim_key)?;
    let existing_slot = read_slot_row(authority.storage, &slot_key)?;
    let existing_reservation = read_reservation_row(authority.storage, &reservation_key)?;
    if existing_claim.is_some() || existing_slot.is_some() || existing_reservation.is_some() {
        return exact_reservation_replay(
            tenant_id,
            &claim,
            &slot,
            &reservation,
            existing_claim,
            existing_slot,
            existing_reservation,
        );
    }

    let protocol_hash = reservation.identity.hash_sha256()?;
    let transaction_id = format!("personaldb-reserve:{}", hex::encode(protocol_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &claim.database_id).await?;
    let created_at_unix_nanos = unix_seconds_to_nanos(reservation.identity.issued_at_unix_seconds)?;
    let common = row_common(
        tenant_id,
        &claim.database_id,
        root_generation,
        &transaction_id,
        created_at_unix_nanos,
    );
    let claim_payload = encode_deterministic_proto(&ClaimRowProto {
        common: Some(common.clone()),
        claim: Some(claim_to_proto(&claim)),
    });
    let slot_payload = encode_deterministic_proto(&SlotRowProto {
        common: Some(common.clone()),
        slot: Some(slot_to_proto(&slot)),
    });
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common),
        reservation: Some(reservation_to_proto(&reservation)),
    });
    let preconditions = vec![
        guard.owner_precondition,
        guard.head_precondition,
        absent_precondition(TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW, claim_key.clone()),
        absent_precondition(TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, slot_key.clone()),
        absent_precondition(
            TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
            reservation_key.clone(),
        ),
    ];
    let operations = vec![
        put_operation(
            tenant_id,
            &claim.database_id,
            TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW,
            claim_key,
            claim_payload,
        ),
        put_operation(
            tenant_id,
            &claim.database_id,
            TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
            slot_key,
            slot_payload,
        ),
        put_operation(
            tenant_id,
            &claim.database_id,
            TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
            reservation_key,
            reservation_payload,
        ),
    ];
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &claim.database_id,
        &authority.write_permit.owner_node_id,
        preconditions,
        operations,
    )
    .await?;

    let stored = read_personaldb_proposal_reservation(
        authority.storage,
        &reservation.identity.reservation_id,
    )?
    .ok_or_else(|| anyhow!("proposal reservation commit produced no visible row"))?;
    if stored.tenant_id != tenant_id || stored.reservation != reservation {
        bail!("proposal reservation commit did not round-trip exactly");
    }
    Ok(stored.reservation)
}

pub async fn begin_personaldb_witness_signing(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: BeginWitnessSigningV1,
) -> Result<WitnessSigningCandidateV1> {
    validate_tenant_database(
        request.tenant_id,
        &request.unsigned_commit_certificate.database_id,
    )?;
    if request.created_at_unix_seconds != authority.now_unix_seconds {
        bail!("witness candidate creation time must equal the authority time");
    }
    validate_commit_certificate_unsigned(&request.unsigned_commit_certificate)?;
    validate_committed_head_unsigned(&request.head_template)?;
    require_unsigned_certificate(&request.unsigned_commit_certificate)?;
    require_unsigned_head(&request.head_template)?;

    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    if stored.tenant_id != request.tenant_id {
        bail!("witness candidate tenant does not match reservation");
    }
    let reservation = stored.reservation;
    let signing_revision = request
        .expected_reservation_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation revision overflow"))?;
    let is_initial = reservation.state == ProposalAdmissionReservationStateV1::Reserved
        && reservation.reservation_revision == request.expected_reservation_revision;
    let is_replay = reservation.state == ProposalAdmissionReservationStateV1::Signing
        && reservation.reservation_revision == signing_revision;
    if !is_initial && !is_replay {
        bail!("proposal reservation is not at the expected reserved revision");
    }
    let identity = &reservation.identity;
    let guard = load_group_guard(authority, request.tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, identity, &guard)?;
    validate_candidate_objects(
        request.tenant_id,
        identity,
        &request.unsigned_commit_certificate,
        &request.head_template,
    )?;

    let unsigned_commit_certificate =
        encode_commit_certificate(&request.unsigned_commit_certificate)?;
    let head_template = encode_committed_head(&request.head_template)?;
    ensure_bounded_object(&unsigned_commit_certificate, "unsigned commit certificate")?;
    ensure_bounded_object(&head_template, "witness head template")?;
    let entry_hash = decode_hex32(
        &request.unsigned_commit_certificate.entry_hash,
        "entry_hash",
    )?;
    let candidate_unsigned_certificate_sha256 = domain_hash(
        UNSIGNED_CERTIFICATE_HASH_DOMAIN,
        &unsigned_commit_certificate,
    );
    let candidate_head_template_sha256 = domain_hash(HEAD_TEMPLATE_HASH_DOMAIN, &head_template);
    let candidate = WitnessSigningCandidateV1 {
        format_version: FORMAT_VERSION_V1,
        reservation_id: request.reservation_id.clone(),
        signing_reservation_revision: signing_revision,
        database_id: identity.database_id.clone(),
        next_log_index: next_log_index(identity)?,
        client_log_epoch: identity.client_log_epoch,
        fencing_generation: identity.fencing_generation,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
        unsigned_commit_certificate,
        head_template,
        candidate_entry_core_sha256: entry_hash,
        candidate_unsigned_certificate_sha256,
        candidate_head_template_sha256,
        created_at_unix_seconds: request.created_at_unix_seconds,
    };
    validate_candidate(&candidate)?;
    let candidate_hash = candidate.hash_sha256()?;

    let mut signing_reservation = reservation.clone();
    signing_reservation.reservation_revision = candidate.signing_reservation_revision;
    signing_reservation.state = ProposalAdmissionReservationStateV1::Signing;
    signing_reservation.candidate_entry_core_sha256 = Some(candidate.candidate_entry_core_sha256);
    signing_reservation.candidate_unsigned_certificate_sha256 =
        Some(candidate.candidate_unsigned_certificate_sha256);
    signing_reservation.candidate_head_template_sha256 =
        Some(candidate.candidate_head_template_sha256);
    signing_reservation.witness_signing_candidate_sha256 = Some(candidate_hash);
    validate_reservation(&signing_reservation)?;

    let candidate_key = candidate_key(
        request.tenant_id,
        &candidate.database_id,
        candidate.next_log_index,
        candidate.client_log_epoch,
        &candidate.reservation_id,
    )?;
    let reservation_key = reservation_key(&candidate.reservation_id)?;
    let existing_candidate = read_candidate_row(authority.storage, &candidate_key)?;
    if let Some(existing) = existing_candidate {
        if existing == candidate {
            let current =
                read_personaldb_proposal_reservation(authority.storage, &candidate.reservation_id)?
                    .ok_or_else(|| anyhow!("witness candidate has no reservation"))?;
            if current.tenant_id == request.tenant_id && current.reservation == signing_reservation
            {
                return Ok(existing);
            }
        }
        bail!("altered witness candidate replay");
    }

    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let transaction_id = format!("personaldb-candidate:{}", hex::encode(candidate_hash));
    let root_generation =
        next_group_root_generation(authority.storage, request.tenant_id, &candidate.database_id)
            .await?;
    let common = row_common(
        request.tenant_id,
        &candidate.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(candidate.created_at_unix_seconds)?,
    );
    let candidate_payload = encode_deterministic_proto(&CandidateRowProto {
        common: Some(common.clone()),
        candidate: Some(candidate_to_proto(&candidate)),
    });
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common),
        reservation: Some(reservation_to_proto(&signing_reservation)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        request.tenant_id,
        &candidate.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition,
            guard.head_precondition,
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key.clone(),
                &current_reservation_payload,
            ),
            absent_precondition(
                TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
                candidate_key.clone(),
            ),
        ],
        vec![
            put_operation(
                request.tenant_id,
                &candidate.database_id,
                TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
                candidate_key,
                candidate_payload,
            ),
            put_operation(
                request.tenant_id,
                &candidate.database_id,
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                reservation_payload,
            ),
        ],
    )
    .await?;

    let stored_candidate = read_witness_signing_candidate(
        authority.storage,
        request.tenant_id,
        &candidate.database_id,
        candidate.next_log_index,
        candidate.client_log_epoch,
        &candidate.reservation_id,
    )?
    .ok_or_else(|| anyhow!("witness candidate commit produced no visible row"))?;
    if stored_candidate != candidate {
        bail!("witness candidate commit did not round-trip exactly");
    }
    Ok(stored_candidate)
}

pub async fn sign_personaldb_certificate_and_head(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
    signer: &dyn ProtocolSigner,
) -> Result<WitnessDualSigningReceiptV1> {
    sign_personaldb_certificate_and_head_with_authority(
        authority,
        request,
        WitnessSigningAuthority::ProtocolSigner(signer),
    )
    .await
}

pub async fn sign_personaldb_certificate_and_head_with_keyring(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
    keyring: &PersonalDbProtocolKeyring,
) -> Result<WitnessDualSigningReceiptV1> {
    sign_personaldb_certificate_and_head_with_authority(
        authority,
        request,
        WitnessSigningAuthority::Keyring(keyring),
    )
    .await
}

async fn sign_personaldb_certificate_and_head_with_authority(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
    signer: WitnessSigningAuthority<'_>,
) -> Result<WitnessDualSigningReceiptV1> {
    validate_reservation_id(&request.reservation_id)?;
    if request.signing_reservation_revision == 0 {
        bail!("signing reservation revision must be nonzero");
    }
    if let Some(existing) =
        read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
    {
        validate_receipt_request(&existing, request)?;
        validate_stored_receipt(authority, &existing)?;
        return Ok(existing);
    }

    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    let tenant_id = stored.tenant_id;
    let reservation = stored.reservation;
    if reservation.state != ProposalAdmissionReservationStateV1::Signing
        || reservation.reservation_revision != request.signing_reservation_revision
    {
        bail!("proposal reservation is not at the requested signing revision");
    }
    let identity = &reservation.identity;
    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, identity, &guard)?;
    let candidate = read_witness_signing_candidate(
        authority.storage,
        tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
        &identity.reservation_id,
    )?
    .ok_or_else(|| anyhow!("witness signing candidate not found"))?;
    validate_candidate_against_reservation(&candidate, &reservation)?;
    signer.validate_bound_signer(
        &identity.witness_key_id,
        identity.witness_key_generation,
        &identity.database_id,
        candidate.next_log_index,
    )?;

    let mut certificate = decode_commit_certificate(&candidate.unsigned_commit_certificate)
        .context("decode witness candidate commit certificate")?;
    let mut head = decode_committed_head(&candidate.head_template)
        .context("decode witness candidate committed-head template")?;
    require_unsigned_certificate(&certificate)?;
    require_unsigned_head(&head)?;
    validate_candidate_objects(tenant_id, identity, &certificate, &head)?;

    let generation = KeyGeneration::new(identity.witness_key_generation)?;
    let certificate_signature = signer
        .sign_certificate_with_required_generation(&certificate, generation, authority.trust_store)
        .await?;
    certificate.certificate_hash = Some(crate::personaldb_control::hash_commit_certificate(
        &certificate,
    )?);
    certificate.witness_signature = Some(certificate_signature.clone());
    let signed_commit_certificate = encode_commit_certificate(&certificate)?;

    let head_signature = signer
        .sign_head_with_required_generation(&head, generation, authority.trust_store)
        .await?;
    head.head_hash = Some(crate::personaldb_heads::hash_committed_head(&head)?);
    head.head_signature = Some(head_signature.clone());
    let signed_committed_head = encode_committed_head(&head)?;
    ensure_bounded_object(&signed_commit_certificate, "signed commit certificate")?;
    ensure_bounded_object(&signed_committed_head, "signed committed head")?;

    let receipt = WitnessDualSigningReceiptV1 {
        format_version: FORMAT_VERSION_V1,
        reservation_id: request.reservation_id.clone(),
        signing_reservation_revision: request.signing_reservation_revision,
        candidate_unsigned_certificate_sha256: candidate.candidate_unsigned_certificate_sha256,
        candidate_head_template_sha256: candidate.candidate_head_template_sha256,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
        certificate_signature_sha256: Sha256::digest(certificate_signature.signature.as_bytes())
            .into(),
        signed_commit_certificate_sha256: domain_hash(
            SIGNED_CERTIFICATE_HASH_DOMAIN,
            &signed_commit_certificate,
        ),
        unsigned_committed_head_sha256: domain_hash(
            UNSIGNED_HEAD_HASH_DOMAIN,
            &candidate.head_template,
        ),
        head_signature_sha256: Sha256::digest(head_signature.signature.as_bytes()).into(),
        signed_committed_head_sha256: domain_hash(SIGNED_HEAD_HASH_DOMAIN, &signed_committed_head),
        signed_at_unix_seconds: candidate.created_at_unix_seconds,
        signed_commit_certificate,
        signed_committed_head,
    };
    validate_receipt_against_candidate(&receipt, &candidate)?;
    store_witness_receipt_create_absent(authority, tenant_id, &candidate, &guard, &receipt).await?;
    Ok(
        read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
            .ok_or_else(|| anyhow!("witness receipt commit produced no visible row"))?,
    )
}

pub async fn acknowledge_personaldb_witness_receipt(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
) -> Result<ProposalAdmissionReservationV1> {
    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    let tenant_id = stored.tenant_id;
    let reservation = stored.reservation;
    let identity = &reservation.identity;
    let receipt = read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("witness dual-signing receipt not found"))?;
    validate_receipt_request(&receipt, request)?;
    validate_stored_receipt(authority, &receipt)?;
    let receipt_hash = receipt.hash_sha256()?;
    if reservation.witness_dual_signing_receipt_sha256 == Some(receipt_hash) {
        let slot = read_proposal_admission_slot(
            authority.storage,
            tenant_id,
            &identity.database_id,
            next_log_index(identity)?,
            identity.client_log_epoch,
        )?
        .ok_or_else(|| anyhow!("proposal slot not found"))?;
        if slot.state == ProposalAdmissionSlotStateV1::Signed
            && slot.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
        {
            return Ok(reservation);
        }
        bail!("proposal reservation receipt acknowledgement is incomplete");
    }
    if reservation.state != ProposalAdmissionReservationStateV1::Signing
        || reservation.reservation_revision != request.signing_reservation_revision
        || reservation.witness_dual_signing_receipt_sha256.is_some()
    {
        bail!("proposal reservation cannot acknowledge this witness receipt");
    }

    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, identity, &guard)?;
    let slot_key = slot_key(
        tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
    )?;
    let mut slot = read_slot_row(authority.storage, &slot_key)?
        .ok_or_else(|| anyhow!("proposal slot not found"))?;
    if slot.reservation_id != identity.reservation_id
        || slot.state != ProposalAdmissionSlotStateV1::Reserved
    {
        bail!("proposal slot is not reserved for this witness receipt");
    }
    slot.state = ProposalAdmissionSlotStateV1::Signed;
    slot.slot_revision = slot
        .slot_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal slot revision overflow"))?;
    slot.witness_dual_signing_receipt_sha256 = Some(receipt_hash);
    slot.terminal_committed_head_sha256 = Some(receipt.signed_committed_head_sha256);

    let mut acknowledged = reservation.clone();
    acknowledged.candidate_unsigned_committed_head_sha256 =
        Some(receipt.unsigned_committed_head_sha256);
    acknowledged.witness_dual_signing_receipt_sha256 = Some(receipt_hash);
    validate_reservation(&acknowledged)?;
    validate_slot(&slot)?;

    let reservation_key = reservation_key(&identity.reservation_id)?;
    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let current_slot_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
        &slot_key,
    )?
    .ok_or_else(|| anyhow!("proposal slot disappeared"))?;
    let receipt_key = receipt_key(&identity.reservation_id)?;
    let receipt_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
        &receipt_key,
    )?
    .ok_or_else(|| anyhow!("witness receipt disappeared"))?;
    let transaction_id = format!("personaldb-receipt-ack:{}", hex::encode(receipt_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &identity.database_id).await?;
    let common = row_common(
        tenant_id,
        &identity.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(authority.now_unix_seconds)?,
    );
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common.clone()),
        reservation: Some(reservation_to_proto(&acknowledged)),
    });
    let slot_payload = encode_deterministic_proto(&SlotRowProto {
        common: Some(common),
        slot: Some(slot_to_proto(&slot)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &identity.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition,
            guard.head_precondition,
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key.clone(),
                &current_reservation_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key.clone(),
                &current_slot_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
                receipt_key,
                &receipt_payload,
            ),
        ],
        vec![
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                reservation_payload,
            ),
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key,
                slot_payload,
            ),
        ],
    )
    .await?;
    Ok(
        read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
            .ok_or_else(|| anyhow!("acknowledged proposal reservation disappeared"))?
            .reservation,
    )
}

pub async fn commit_personaldb_witnessed_proposal(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
) -> Result<ProposalAdmissionReservationV1> {
    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    let tenant_id = stored.tenant_id;
    let reservation = stored.reservation;
    let identity = &reservation.identity;
    let receipt = read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("witness dual-signing receipt not found"))?;
    validate_receipt_request(&receipt, request)?;
    validate_stored_receipt(authority, &receipt)?;
    let receipt_hash = receipt.hash_sha256()?;
    let slot_key = slot_key(
        tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
    )?;
    let slot = read_slot_row(authority.storage, &slot_key)?
        .ok_or_else(|| anyhow!("proposal slot not found"))?;
    if reservation.state == ProposalAdmissionReservationStateV1::Committed {
        if reservation.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
            && reservation.terminal_commit_certificate_sha256
                == Some(receipt.signed_commit_certificate_sha256)
            && reservation.terminal_committed_head_sha256
                == Some(receipt.signed_committed_head_sha256)
            && slot.state == ProposalAdmissionSlotStateV1::Committed
            && slot.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
            && slot.terminal_committed_head_sha256 == Some(receipt.signed_committed_head_sha256)
        {
            return Ok(reservation);
        }
        bail!("proposal committed reservation is incomplete");
    }
    if reservation.state != ProposalAdmissionReservationStateV1::Signing
        || reservation.reservation_revision != request.signing_reservation_revision
        || reservation.witness_dual_signing_receipt_sha256 != Some(receipt_hash)
        || slot.reservation_id != identity.reservation_id
        || slot.state != ProposalAdmissionSlotStateV1::Signed
        || slot.witness_dual_signing_receipt_sha256 != Some(receipt_hash)
        || slot.terminal_committed_head_sha256 != Some(receipt.signed_committed_head_sha256)
    {
        bail!("proposal reservation cannot be committed from its current state");
    }

    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    if identity.fencing_generation != guard.owner.fence_token
        || identity.fencing_generation != authority.write_permit.fence_token
        || identity.primary_server_id != guard.owner.owner_node_id
        || identity.primary_server_id != authority.write_permit.owner_node_id
        || identity.leader_lease_id != personaldb_group_leader_lease_id(&guard.owner)
        || identity.leader_lease_revision != guard.owner.recovery_epoch
    {
        bail!("proposal reservation leader lease is stale at commit finalisation");
    }
    let committed_head = decode_committed_head(&receipt.signed_committed_head)?;
    let current_head_bytes = encode_committed_head(&guard.head)?;
    if guard.head != committed_head
        || domain_hash(SIGNED_HEAD_HASH_DOMAIN, &current_head_bytes)
            != receipt.signed_committed_head_sha256
    {
        bail!("proposal witnessed head is not the current committed head");
    }
    let committed_certificate = decode_commit_certificate(&receipt.signed_commit_certificate)?;
    if domain_hash(
        SIGNED_CERTIFICATE_HASH_DOMAIN,
        &encode_commit_certificate(&committed_certificate)?,
    ) != receipt.signed_commit_certificate_sha256
    {
        bail!("proposal witnessed certificate hash does not match receipt");
    }

    let terminal_revision = request
        .signing_reservation_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation revision overflow"))?;
    let mut committed_reservation = reservation.clone();
    committed_reservation.reservation_revision = terminal_revision;
    committed_reservation.state = ProposalAdmissionReservationStateV1::Committed;
    committed_reservation.terminal_commit_certificate_sha256 =
        Some(receipt.signed_commit_certificate_sha256);
    committed_reservation.terminal_committed_head_sha256 =
        Some(receipt.signed_committed_head_sha256);

    let mut committed_slot = slot.clone();
    committed_slot.state = ProposalAdmissionSlotStateV1::Committed;
    committed_slot.slot_revision = committed_slot
        .slot_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal slot revision overflow"))?;
    validate_reservation(&committed_reservation)?;
    validate_slot(&committed_slot)?;

    let reservation_key = reservation_key(&identity.reservation_id)?;
    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let current_slot_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
        &slot_key,
    )?
    .ok_or_else(|| anyhow!("proposal slot disappeared"))?;
    let receipt_key = receipt_key(&identity.reservation_id)?;
    let receipt_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
        &receipt_key,
    )?
    .ok_or_else(|| anyhow!("witness receipt disappeared"))?;
    let transaction_id = format!("personaldb-admission-commit:{}", hex::encode(receipt_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &identity.database_id).await?;
    let common = row_common(
        tenant_id,
        &identity.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(authority.now_unix_seconds)?,
    );
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common.clone()),
        reservation: Some(reservation_to_proto(&committed_reservation)),
    });
    let slot_payload = encode_deterministic_proto(&SlotRowProto {
        common: Some(common),
        slot: Some(slot_to_proto(&committed_slot)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &identity.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition,
            guard.head_precondition,
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key.clone(),
                &current_reservation_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key.clone(),
                &current_slot_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
                receipt_key,
                &receipt_payload,
            ),
        ],
        vec![
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                reservation_payload,
            ),
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key,
                slot_payload,
            ),
        ],
    )
    .await?;
    Ok(
        read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
            .ok_or_else(|| anyhow!("committed proposal reservation disappeared"))?
            .reservation,
    )
}

pub fn read_personaldb_proposal_reservation(
    storage: &Storage,
    reservation_id: &str,
) -> Result<Option<StoredProposalAdmissionReservationV1>> {
    let key = reservation_key(reservation_id)?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, &key)?
    else {
        return Ok(None);
    };
    let (common, reservation) = decode_reservation_row(&payload)?;
    let tenant_id = tenant_id_from_realm(&common.realm_id)?;
    validate_row_scope(&common, tenant_id, &reservation.identity.database_id)?;
    if reservation.identity.reservation_id != reservation_id {
        bail!("proposal reservation exact-key scope mismatch");
    }
    validate_reservation(&reservation)?;
    Ok(Some(StoredProposalAdmissionReservationV1 {
        tenant_id,
        reservation,
    }))
}

pub fn read_proposal_admission_slot(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
) -> Result<Option<ProposalAdmissionSlotV1>> {
    let key = slot_key(tenant_id, database_id, next_log_index, client_log_epoch)?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, &key)? else {
        return Ok(None);
    };
    let (common, slot) = decode_slot_row(&payload)?;
    validate_row_scope(&common, tenant_id, database_id)?;
    if slot.database_id != database_id
        || slot.next_log_index != next_log_index
        || slot.client_log_epoch != client_log_epoch
    {
        bail!("proposal slot exact-key scope mismatch");
    }
    validate_slot(&slot)?;
    Ok(Some(slot))
}

pub fn read_witness_signing_candidate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
    reservation_id: &str,
) -> Result<Option<WitnessSigningCandidateV1>> {
    let key = candidate_key(
        tenant_id,
        database_id,
        next_log_index,
        client_log_epoch,
        reservation_id,
    )?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, &key)? else {
        return Ok(None);
    };
    let (common, candidate) = decode_candidate_row(&payload)?;
    validate_row_scope(&common, tenant_id, database_id)?;
    if candidate.database_id != database_id
        || candidate.next_log_index != next_log_index
        || candidate.client_log_epoch != client_log_epoch
        || candidate.reservation_id != reservation_id
    {
        bail!("witness candidate exact-key scope mismatch");
    }
    validate_candidate(&candidate)?;
    Ok(Some(candidate))
}

pub fn read_witness_dual_signing_receipt(
    storage: &Storage,
    reservation_id: &str,
) -> Result<Option<WitnessDualSigningReceiptV1>> {
    let key = receipt_key(reservation_id)?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_WITNESS_RECEIPT_ROW, &key)? else {
        return Ok(None);
    };
    let (_common, receipt) = decode_receipt_row(&payload)?;
    if receipt.reservation_id != reservation_id {
        bail!("witness receipt exact-key scope mismatch");
    }
    validate_receipt_shape(&receipt)?;
    Ok(Some(receipt))
}

struct GroupGuard {
    owner: PartitionOwnerState,
    head: PersonalDbCommittedHead,
    owner_precondition: CoreMutationPrecondition,
    head_precondition: CoreMutationPrecondition,
}

async fn load_group_guard(
    authority: &PersonalDbAdmissionAuthority<'_>,
    tenant_id: i64,
    database_id: &str,
) -> Result<GroupGuard> {
    validate_tenant_database(tenant_id, database_id)?;
    let expected_partition_id = personaldb_group_partition_owner_id(tenant_id, database_id)?;
    if authority.write_permit.partition_family != PERSONALDB_GROUP_PARTITION_FAMILY
        || authority.write_permit.partition_id != expected_partition_id
    {
        bail!("PersonalDB admission write permit targets a different group");
    }
    let owner = read_partition_owner(
        authority.storage,
        &authority.write_permit.partition_family,
        &authority.write_permit.partition_id,
        authority.partition_owner_signing_key,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB group leader lease is absent"))?;
    validate_write_permit_for_state(&owner, authority.write_permit, true)
        .map_err(|error| anyhow!("{error}"))?;
    let owner_precondition = partition_write_precondition(
        authority.storage,
        authority.write_permit,
        authority.partition_owner_signing_key,
    )
    .await
    .map_err(|error| anyhow!("{error}"))?;
    let head = read_personaldb_committed_head(
        authority.storage,
        tenant_id,
        database_id,
        authority.trust_store,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB group committed head is absent"))?;
    let head_precondition =
        personaldb_committed_head_precondition(authority.storage, tenant_id, database_id)?;
    Ok(GroupGuard {
        owner,
        head,
        owner_precondition,
        head_precondition,
    })
}

fn validate_identity_against_guard(
    authority: &PersonalDbAdmissionAuthority<'_>,
    identity: &ProposalAdmissionReservationIdentityV1,
    guard: &GroupGuard,
) -> Result<()> {
    validate_reservation_identity_shape(identity)?;
    if identity.expected_previous_log_index != guard.head.log_index
        || identity.expected_previous_log_hash_sha256
            != decode_hex32(&guard.head.log_hash, "committed head log_hash")?
    {
        bail!("proposal reservation expected head is stale");
    }
    if identity.membership_revision != guard.head.membership_epoch {
        bail!("proposal reservation membership revision is stale");
    }
    if identity.fencing_generation != guard.owner.fence_token
        || identity.fencing_generation != authority.write_permit.fence_token
    {
        bail!("proposal reservation fencing generation is stale");
    }
    if identity.primary_server_id != guard.owner.owner_node_id
        || identity.primary_server_id != authority.write_permit.owner_node_id
    {
        bail!("proposal reservation primary does not own the group lease");
    }
    if identity.leader_lease_id != personaldb_group_leader_lease_id(&guard.owner)
        || identity.leader_lease_revision != guard.owner.recovery_epoch
    {
        bail!("proposal reservation leader lease is stale");
    }
    if identity.issued_at_unix_seconds > authority.now_unix_seconds
        || identity.expires_at_unix_seconds <= authority.now_unix_seconds
    {
        bail!("proposal reservation is outside its active lifetime");
    }
    Ok(())
}

fn validate_candidate_objects(
    tenant_id: i64,
    identity: &ProposalAdmissionReservationIdentityV1,
    certificate: &PersonalDbCommitCertificate,
    head: &PersonalDbCommittedHead,
) -> Result<()> {
    let tenant = tenant_id.to_string();
    let previous_hash = hex::encode(identity.expected_previous_log_hash_sha256);
    let next_index = next_log_index(identity)?;
    if certificate.tenant_id != tenant
        || certificate.database_id != identity.database_id
        || certificate.log_index != next_index
        || certificate.previous_log_hash != previous_hash
        || certificate.changeset_payload_hash != hex::encode(identity.changeset_payload_hash_sha256)
        || certificate.client_log_epoch != identity.client_log_epoch
        || certificate.membership_epoch != identity.membership_revision
        || certificate.leader_replica_id != identity.primary_server_id
        || certificate.authz_revision != identity.authorization_revision
    {
        bail!("witness candidate certificate does not match reservation");
    }
    if head.tenant_id != tenant
        || head.database_id != identity.database_id
        || head.log_index != next_index
        || head.log_hash != certificate.entry_hash
        || head.membership_epoch != identity.membership_revision
        || head.updated_by_node != identity.primary_server_id
    {
        bail!("witness candidate committed-head template does not match reservation");
    }
    Ok(())
}

fn validate_candidate_against_reservation(
    candidate: &WitnessSigningCandidateV1,
    reservation: &ProposalAdmissionReservationV1,
) -> Result<()> {
    validate_candidate(candidate)?;
    let identity = &reservation.identity;
    if candidate.reservation_id != identity.reservation_id
        || candidate.signing_reservation_revision != reservation.reservation_revision
        || candidate.database_id != identity.database_id
        || candidate.next_log_index != next_log_index(identity)?
        || candidate.client_log_epoch != identity.client_log_epoch
        || candidate.fencing_generation != identity.fencing_generation
        || candidate.witness_key_id != identity.witness_key_id
        || candidate.witness_key_generation != identity.witness_key_generation
        || reservation.candidate_entry_core_sha256 != Some(candidate.candidate_entry_core_sha256)
        || reservation.candidate_unsigned_certificate_sha256
            != Some(candidate.candidate_unsigned_certificate_sha256)
        || reservation.candidate_head_template_sha256
            != Some(candidate.candidate_head_template_sha256)
        || reservation.witness_signing_candidate_sha256 != Some(candidate.hash_sha256()?)
    {
        bail!("witness candidate does not match signing reservation");
    }
    Ok(())
}

fn validate_receipt_request(
    receipt: &WitnessDualSigningReceiptV1,
    request: &SignCertificateAndHeadV1,
) -> Result<()> {
    if receipt.reservation_id != request.reservation_id
        || receipt.signing_reservation_revision != request.signing_reservation_revision
    {
        bail!("altered witness signing replay");
    }
    Ok(())
}

fn validate_stored_receipt(
    authority: &PersonalDbAdmissionAuthority<'_>,
    receipt: &WitnessDualSigningReceiptV1,
) -> Result<()> {
    validate_receipt_shape(receipt)?;
    let stored = read_personaldb_proposal_reservation(authority.storage, &receipt.reservation_id)?
        .ok_or_else(|| anyhow!("witness receipt reservation not found"))?;
    let reservation = stored.reservation;
    let identity = &reservation.identity;
    let receipt_hash = receipt.hash_sha256()?;
    let signing_reservation_matches = reservation.state
        == ProposalAdmissionReservationStateV1::Signing
        && reservation.reservation_revision == receipt.signing_reservation_revision;
    let terminal_revision = receipt
        .signing_reservation_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation revision overflow"))?;
    let committed_reservation_matches = reservation.state
        == ProposalAdmissionReservationStateV1::Committed
        && reservation.reservation_revision == terminal_revision
        && reservation.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
        && reservation.terminal_commit_certificate_sha256
            == Some(receipt.signed_commit_certificate_sha256)
        && reservation.terminal_committed_head_sha256 == Some(receipt.signed_committed_head_sha256);
    if (!signing_reservation_matches && !committed_reservation_matches)
        || identity.witness_key_id != receipt.witness_key_id
        || identity.witness_key_generation != receipt.witness_key_generation
    {
        bail!("stored witness receipt does not match reservation");
    }
    let candidate = read_witness_signing_candidate(
        authority.storage,
        stored.tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
        &identity.reservation_id,
    )?
    .ok_or_else(|| anyhow!("witness receipt candidate not found"))?;
    validate_receipt_against_candidate(receipt, &candidate)?;
    let certificate = decode_commit_certificate(&receipt.signed_commit_certificate)?;
    let head = decode_committed_head(&receipt.signed_committed_head)?;
    certificate.verify(authority.trust_store)?;
    head.verify(authority.trust_store)?;
    Ok(())
}

fn validate_receipt_against_candidate(
    receipt: &WitnessDualSigningReceiptV1,
    candidate: &WitnessSigningCandidateV1,
) -> Result<()> {
    validate_receipt_shape(receipt)?;
    if receipt.reservation_id != candidate.reservation_id
        || receipt.signing_reservation_revision != candidate.signing_reservation_revision
        || receipt.candidate_unsigned_certificate_sha256
            != candidate.candidate_unsigned_certificate_sha256
        || receipt.candidate_head_template_sha256 != candidate.candidate_head_template_sha256
        || receipt.witness_key_id != candidate.witness_key_id
        || receipt.witness_key_generation != candidate.witness_key_generation
        || receipt.unsigned_committed_head_sha256
            != domain_hash(UNSIGNED_HEAD_HASH_DOMAIN, &candidate.head_template)
        || receipt.signed_commit_certificate_sha256
            != domain_hash(
                SIGNED_CERTIFICATE_HASH_DOMAIN,
                &receipt.signed_commit_certificate,
            )
        || receipt.signed_committed_head_sha256
            != domain_hash(SIGNED_HEAD_HASH_DOMAIN, &receipt.signed_committed_head)
    {
        bail!("witness receipt does not match immutable candidate");
    }
    let certificate = decode_commit_certificate(&receipt.signed_commit_certificate)?;
    let head = decode_committed_head(&receipt.signed_committed_head)?;
    let certificate_signature = certificate
        .witness_signature
        .as_ref()
        .ok_or_else(|| anyhow!("witness receipt certificate signature missing"))?;
    let head_signature = head
        .head_signature
        .as_ref()
        .ok_or_else(|| anyhow!("witness receipt head signature missing"))?;
    let certificate_signature_sha256: [u8; 32] =
        Sha256::digest(certificate_signature.signature.as_bytes()).into();
    let head_signature_sha256: [u8; 32] =
        Sha256::digest(head_signature.signature.as_bytes()).into();
    if receipt.certificate_signature_sha256 != certificate_signature_sha256
        || receipt.head_signature_sha256 != head_signature_sha256
    {
        bail!("witness receipt signature hash mismatch");
    }
    Ok(())
}

async fn store_witness_receipt_create_absent(
    authority: &PersonalDbAdmissionAuthority<'_>,
    tenant_id: i64,
    candidate: &WitnessSigningCandidateV1,
    guard: &GroupGuard,
    receipt: &WitnessDualSigningReceiptV1,
) -> Result<()> {
    validate_receipt_against_candidate(receipt, candidate)?;
    let receipt_key = receipt_key(&receipt.reservation_id)?;
    if let Some(existing) =
        read_witness_dual_signing_receipt(authority.storage, &receipt.reservation_id)?
    {
        if existing == *receipt {
            return Ok(());
        }
        bail!("altered witness receipt replay");
    }
    let reservation_key = reservation_key(&receipt.reservation_id)?;
    let candidate_key = candidate_key(
        tenant_id,
        &candidate.database_id,
        candidate.next_log_index,
        candidate.client_log_epoch,
        &candidate.reservation_id,
    )?;
    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let current_candidate_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
        &candidate_key,
    )?
    .ok_or_else(|| anyhow!("witness candidate disappeared"))?;
    let receipt_hash = receipt.hash_sha256()?;
    let transaction_id = format!("personaldb-witness:{}", hex::encode(receipt_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &candidate.database_id).await?;
    let common = row_common(
        tenant_id,
        &candidate.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(receipt.signed_at_unix_seconds)?,
    );
    let receipt_payload = encode_deterministic_proto(&ReceiptRowProto {
        common: Some(common),
        receipt: Some(receipt_to_proto(receipt)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &candidate.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition.clone(),
            guard.head_precondition.clone(),
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                &current_reservation_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
                candidate_key,
                &current_candidate_payload,
            ),
            absent_precondition(TABLE_PERSONALDB_WITNESS_RECEIPT_ROW, receipt_key.clone()),
        ],
        vec![put_operation(
            tenant_id,
            &candidate.database_id,
            TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
            receipt_key,
            receipt_payload,
        )],
    )
    .await
}

fn exact_reservation_replay(
    tenant_id: i64,
    claim: &ProposalIdempotencyClaimIdentityV1,
    slot: &ProposalAdmissionSlotV1,
    reservation: &ProposalAdmissionReservationV1,
    existing_claim: Option<ProposalIdempotencyClaimIdentityV1>,
    existing_slot: Option<ProposalAdmissionSlotV1>,
    existing_reservation: Option<(i64, ProposalAdmissionReservationV1)>,
) -> Result<ProposalAdmissionReservationV1> {
    if existing_claim.as_ref().is_some_and(|value| value != claim) {
        bail!("proposal idempotency claim conflict");
    }
    if existing_slot.as_ref().is_some_and(|value| value != slot) {
        bail!("proposal slot is occupied by another reservation");
    }
    if existing_reservation
        .as_ref()
        .is_some_and(|(stored_tenant, value)| *stored_tenant != tenant_id || value != reservation)
    {
        bail!("altered proposal reservation replay");
    }
    match (existing_claim, existing_slot, existing_reservation) {
        (Some(_), Some(_), Some((_, stored))) => Ok(stored),
        _ => bail!("partial proposal reservation state is a consistency fault"),
    }
}

fn admission_from_reservation(
    reservation: &ProposalAdmissionReservationV1,
) -> Result<ProposalAdmissionV1> {
    validate_reservation(reservation)?;
    let identity = &reservation.identity;
    let admission = ProposalAdmissionV1 {
        format_version: FORMAT_VERSION_V1,
        database_id: identity.database_id.clone(),
        group_kind: identity.group_kind.clone(),
        proposer_id: identity.proposer_id.clone(),
        client_proposal_hash_sha256: identity.client_proposal_hash_sha256,
        workflow_id: identity.workflow_id.clone(),
        fencing_generation: identity.fencing_generation,
        authorization_revision: identity.authorization_revision,
        idempotency_claim_sha256: identity.idempotency_claim_sha256,
        issued_at_unix_seconds: identity.issued_at_unix_seconds,
        expires_at_unix_seconds: identity.expires_at_unix_seconds,
        reservation_id: identity.reservation_id.clone(),
        reservation_revision: reservation.reservation_revision,
        reservation_identity_sha256: identity.hash_sha256()?,
        authorization_receipt_sha256: identity.authorization_receipt_sha256,
        selected_voter_ids: identity.selected_voter_ids.clone(),
        primary_server_id: identity.primary_server_id.clone(),
        proposal_admission_key_id: identity.proposal_admission_key_id.clone(),
        proposal_admission_generation: identity.proposal_admission_generation,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
    };
    validate_admission_shape(&admission)?;
    Ok(admission)
}

fn slot_from_reservation(
    reservation: &ProposalAdmissionReservationV1,
) -> Result<ProposalAdmissionSlotV1> {
    let identity = &reservation.identity;
    let slot = ProposalAdmissionSlotV1 {
        format_version: FORMAT_VERSION_V1,
        database_id: identity.database_id.clone(),
        next_log_index: next_log_index(identity)?,
        expected_previous_log_hash_sha256: identity.expected_previous_log_hash_sha256,
        placement_epoch: identity.placement_epoch,
        client_log_epoch: identity.client_log_epoch,
        fencing_generation: identity.fencing_generation,
        slot_revision: 1,
        reservation_id: identity.reservation_id.clone(),
        state: ProposalAdmissionSlotStateV1::Reserved,
        witness_dual_signing_receipt_sha256: None,
        terminal_committed_head_sha256: None,
    };
    validate_slot(&slot)?;
    Ok(slot)
}

#[cfg(test)]
mod tests;
