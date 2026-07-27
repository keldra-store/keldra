use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    personaldb_signing_object::PersonalDbSigningObject,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use personaldb_protocol::{
    KeyId, PublicKeyStatus, PublicKeyTrustRecord, PublicKeyTrustStore, SignatureEnvelopeV1,
    SignaturePurpose,
};
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

const SUPPORTED_PERSONALDB_SIGNER_PURPOSES: [SignaturePurpose; 3] = [
    SignaturePurpose::GroupControl,
    SignaturePurpose::Snapshot,
    SignaturePurpose::Witness,
];

/// An in-process, purpose-scoped source of PersonalDB protocol signatures.
///
/// Implementations own key custody. A storage-backed implementation can load and decrypt private
/// key material inside `sign`, create the signature, and discard the decrypted bytes before the
/// future completes.
#[async_trait]
pub trait PersonalDbSignerProvider: Send + Sync + fmt::Debug {
    fn purpose(&self) -> SignaturePurpose;
    fn key_id(&self) -> &KeyId;

    async fn sign(&self, object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1>;
}

/// Public trust and optional signing capabilities used by PersonalDB operations.
///
/// An empty keyring is the normal disabled state. Merely constructing Anvil does not require
/// PersonalDB keys; only an operation that explicitly needs a signature requires a provider.
#[derive(Clone)]
pub struct PersonalDbProtocolKeyring {
    trust_store: PublicKeyTrustStore,
    providers: Arc<HashMap<SignaturePurpose, Arc<dyn PersonalDbSignerProvider>>>,
}

impl Default for PersonalDbProtocolKeyring {
    fn default() -> Self {
        Self::disabled()
    }
}

impl fmt::Debug for PersonalDbProtocolKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let provider_key_ids = self
            .providers
            .iter()
            .map(|(purpose, provider)| (purpose, provider.key_id()))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("PersonalDbProtocolKeyring")
            .field("trust_store", &self.trust_store)
            .field("provider_key_ids", &provider_key_ids)
            .finish()
    }
}

impl PersonalDbProtocolKeyring {
    /// Builds a keyring from trust records and in-process providers.
    ///
    /// Both inputs may be empty. Trust records without providers support verification-only use.
    pub fn new(
        trust_records: impl IntoIterator<Item = PublicKeyTrustRecord>,
        providers: impl IntoIterator<Item = Arc<dyn PersonalDbSignerProvider>>,
    ) -> Result<Self> {
        let trust_store = PublicKeyTrustStore::from_records(trust_records)?;
        Self::from_providers(trust_store, providers)
    }

    /// Builds a keyring from an existing trust store and in-process providers.
    ///
    /// Providers are optional capabilities. A missing provider is reported only when an operation
    /// asks the keyring to sign for that purpose.
    pub fn from_providers(
        trust_store: PublicKeyTrustStore,
        providers: impl IntoIterator<Item = Arc<dyn PersonalDbSignerProvider>>,
    ) -> Result<Self> {
        let mut by_purpose = HashMap::new();
        let mut key_ids = HashSet::new();

        for provider in providers {
            let purpose = provider.purpose();
            if !SUPPORTED_PERSONALDB_SIGNER_PURPOSES.contains(&purpose) {
                bail!("PersonalDB {purpose} is not an Anvil control-object signing purpose");
            }

            let record = trust_store.get(provider.key_id()).ok_or_else(|| {
                anyhow!(
                    "PersonalDB {} signing provider key {} is absent from the trust store",
                    purpose,
                    provider.key_id()
                )
            })?;
            if record.status != PublicKeyStatus::Active {
                bail!(
                    "PersonalDB {} signing provider must use an active trust record",
                    purpose
                );
            }
            if record.purpose != purpose {
                bail!(
                    "PersonalDB {} signing provider key {} is trusted only for {}",
                    purpose,
                    record.key_id,
                    record.purpose
                );
            }
            if !key_ids.insert(record.key_id.clone()) {
                bail!(
                    "PersonalDB signing provider key {} is shared by more than one purpose",
                    record.key_id
                );
            }
            if by_purpose.insert(purpose, provider).is_some() {
                bail!(
                    "PersonalDB protocol keyring has more than one {} signing provider",
                    purpose
                );
            }
        }

        Ok(Self {
            trust_store,
            providers: Arc::new(by_purpose),
        })
    }

    pub fn disabled() -> Self {
        Self {
            trust_store: PublicKeyTrustStore::new(),
            providers: Arc::new(HashMap::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.providers.is_empty()
    }

    pub fn has_provider(&self, purpose: SignaturePurpose) -> bool {
        self.providers.contains_key(&purpose)
    }

    pub fn trust_store(&self) -> &PublicKeyTrustStore {
        &self.trust_store
    }

    pub(crate) fn trust_record_for_purpose(
        &self,
        purpose: SignaturePurpose,
    ) -> Result<&PublicKeyTrustRecord> {
        let provider = self.providers.get(&purpose).ok_or_else(|| {
            anyhow!("PersonalDB {purpose} signing provider is not configured for this operation")
        })?;
        self.trust_store.get(provider.key_id()).ok_or_else(|| {
            anyhow!(
                "PersonalDB {purpose} signing provider key {} is absent from the trust store",
                provider.key_id()
            )
        })
    }

    pub(crate) async fn sign(
        &self,
        object: PersonalDbSigningObject,
    ) -> Result<SignatureEnvelopeV1> {
        object.validate()?;
        let metadata = object.metadata();
        let provider = self.providers.get(&metadata.purpose).ok_or_else(|| {
            anyhow!(
                "PersonalDB {} signing provider is not configured for this operation",
                metadata.purpose
            )
        })?;
        let envelope = provider
            .sign(&object)
            .await
            .with_context(|| format!("PersonalDB {} signing failed", metadata.purpose))?;
        if &envelope.key_id != provider.key_id() {
            bail!(
                "PersonalDB {} signing provider returned key {}, expected {}",
                metadata.purpose,
                envelope.key_id,
                provider.key_id()
            );
        }
        self.trust_store.verify(&object, &envelope)?;
        Ok(envelope)
    }

    /// Builds a keyring around long-lived protocol signers for tests only.
    ///
    /// `ProtocolSigner` implementations may retain private key material. Production providers
    /// should implement [`PersonalDbSignerProvider`] and perform storage-backed key custody inside
    /// each `sign` call instead.
    #[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
    #[doc(hidden)]
    pub fn new_test_only(
        trust_store: PublicKeyTrustStore,
        signers: impl IntoIterator<Item = Arc<dyn personaldb_protocol::ProtocolSigner>>,
    ) -> Result<Self> {
        let providers = signers
            .into_iter()
            .map(|signer| {
                Arc::new(TestOnlyProtocolSignerProvider { signer })
                    as Arc<dyn PersonalDbSignerProvider>
            })
            .collect::<Vec<_>>();
        Self::from_providers(trust_store, providers)
    }
}

#[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
struct TestOnlyProtocolSignerProvider {
    signer: Arc<dyn personaldb_protocol::ProtocolSigner>,
}

#[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
impl fmt::Debug for TestOnlyProtocolSignerProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestOnlyProtocolSignerProvider")
            .field("trust_record", self.signer.trust_record())
            .finish()
    }
}

#[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
#[async_trait]
impl PersonalDbSignerProvider for TestOnlyProtocolSignerProvider {
    fn purpose(&self) -> SignaturePurpose {
        self.signer.trust_record().purpose
    }

    fn key_id(&self) -> &KeyId {
        &self.signer.trust_record().key_id
    }

    async fn sign(&self, object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1> {
        self.signer.sign(object).map_err(Into::into)
    }
}

pub(crate) fn signature_envelope_to_proto(
    envelope: &SignatureEnvelopeV1,
) -> WireSignatureEnvelopeV1 {
    WireSignatureEnvelopeV1 {
        format_version: envelope.format_version,
        hash_algorithm: envelope.hash_algorithm as i32,
        signature_algorithm: envelope.signature_algorithm as i32,
        key_id: envelope.key_id.to_string(),
        signature: envelope.signature.as_bytes().to_vec(),
    }
}

pub(crate) fn signature_envelope_from_proto(
    proto: WireSignatureEnvelopeV1,
) -> Result<SignatureEnvelopeV1> {
    SignatureEnvelopeV1::decode_canonical(&proto.encode_to_vec())
        .context("decode canonical PersonalDB signature envelope")
}

#[cfg(test)]
mod tests {
    use super::*;
    use personaldb_protocol::{
        Ed25519ProtocolSigner, KeyGeneration, KeyTrustPolicy, ProtocolSignable, ProtocolSigner,
        PublicKeyEnvelopeV1, SignatureDomain, SignatureMetadata, SigningPayload, signing_preimage,
    };
    use serde::Deserialize;
    use sha2::{Digest as _, Sha256};
    use std::str::FromStr as _;

    const SHARED_SIGNING_VECTOR_JSON: &str =
        include_str!("../tests/fixtures/protocol-signing-v1.json");

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SharedSigningVector {
        schema: String,
        id: String,
        private_key_seed_hex: String,
        public_key_hex: String,
        public_key_envelope_hex: String,
        key_id: String,
        purpose: String,
        domain: String,
        signed_payload_version: u32,
        key_generation: u64,
        log_index: u64,
        payload_utf8: String,
        payload_hex: String,
        payload_sha256_hex: String,
        signing_preimage_hex: String,
        signature_hex: String,
        signature_envelope_hex: String,
    }

    #[derive(Debug, Clone)]
    struct TestSignable {
        metadata: SignatureMetadata,
        payload: Vec<u8>,
    }

    impl TestSignable {
        fn witness(log_index: u64) -> Self {
            Self {
                metadata: SignatureMetadata::for_domain(
                    SignaturePurpose::Witness,
                    SignatureDomain::CommitCertificate,
                    log_index,
                ),
                payload: b"anvil canonical PersonalDB witness payload".to_vec(),
            }
        }
    }

    impl ProtocolSignable for TestSignable {
        fn signature_metadata(&self) -> SignatureMetadata {
            self.metadata.clone()
        }

        fn signing_payload(&self) -> SigningPayload<'_> {
            SigningPayload::ExactBytes(&self.payload)
        }
    }

    #[derive(Debug)]
    struct StaticEnvelopeProvider {
        purpose: SignaturePurpose,
        key_id: KeyId,
        envelope: Option<SignatureEnvelopeV1>,
    }

    #[async_trait]
    impl PersonalDbSignerProvider for StaticEnvelopeProvider {
        fn purpose(&self) -> SignaturePurpose {
            self.purpose
        }

        fn key_id(&self) -> &KeyId {
            &self.key_id
        }

        async fn sign(&self, _object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1> {
            self.envelope
                .clone()
                .ok_or_else(|| anyhow!("test provider has no signature"))
        }
    }

    #[test]
    fn shared_personaldb_signing_vector_crosses_anvil_wire_boundary() {
        let vector: SharedSigningVector = serde_json::from_str(SHARED_SIGNING_VECTOR_JSON).unwrap();
        assert_eq!(vector.schema, "personaldb.protocol-signing-vector.v1");
        assert_eq!(vector.id, "rfc8032-witness-commit-certificate-exact-bytes");
        let purpose = SignaturePurpose::from_str(&vector.purpose).unwrap();
        let domain = SignatureDomain::from_str(&vector.domain).unwrap();
        assert_eq!(
            domain.signed_payload_version(),
            vector.signed_payload_version
        );

        let payload = hex::decode(&vector.payload_hex).unwrap();
        assert_eq!(payload, vector.payload_utf8.as_bytes());
        assert_eq!(
            hex::encode(Sha256::digest(&payload)),
            vector.payload_sha256_hex
        );
        let preimage = signing_preimage(
            domain,
            vector.signed_payload_version,
            SigningPayload::ExactBytes(&payload),
        )
        .unwrap();
        assert_eq!(
            hex::encode(preimage.as_bytes()),
            vector.signing_preimage_hex
        );

        let mut private_key = hex::decode("302e020100300506032b657004220420").unwrap();
        private_key.extend(hex::decode(&vector.private_key_seed_hex).unwrap());
        let signer = Ed25519ProtocolSigner::from_pkcs8_der(
            &private_key,
            KeyTrustPolicy::new(
                KeyGeneration::new(vector.key_generation).unwrap(),
                purpose,
                vector.log_index,
            ),
        )
        .unwrap();
        assert_eq!(signer.trust_record().key_id.as_str(), vector.key_id);
        assert_eq!(
            hex::encode(signer.trust_record().public_key.as_bytes()),
            vector.public_key_hex
        );
        assert_eq!(
            hex::encode(
                PublicKeyEnvelopeV1::for_ed25519(signer.trust_record().public_key)
                    .encode_deterministic()
                    .unwrap()
            ),
            vector.public_key_envelope_hex
        );

        let object = TestSignable {
            metadata: SignatureMetadata::for_domain(purpose, domain, vector.log_index),
            payload,
        };
        let envelope = signer.sign(&object).unwrap();
        assert_eq!(
            hex::encode(envelope.signature.as_bytes()),
            vector.signature_hex
        );
        assert_eq!(
            hex::encode(envelope.encode_deterministic().unwrap()),
            vector.signature_envelope_hex
        );

        let wire = signature_envelope_to_proto(&envelope);
        let decoded = signature_envelope_from_proto(wire).unwrap();
        assert_eq!(decoded, envelope);
        let trust_store =
            PublicKeyTrustStore::from_records([signer.trust_record().clone()]).unwrap();
        trust_store.verify(&object, &decoded).unwrap();
    }

    #[test]
    fn wire_envelope_round_trips_canonically_and_rejects_malformed_shape() {
        let signer = signer(
            0x41,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let envelope = signer.sign(&TestSignable::witness(1)).unwrap();
        let wire = signature_envelope_to_proto(&envelope);

        assert_eq!(
            signature_envelope_from_proto(wire.clone()).unwrap(),
            envelope
        );
        assert_eq!(wire.format_version, 1);
        assert_eq!(wire.signature.len(), 64);

        let malformed = WireSignatureEnvelopeV1 {
            signature: vec![0; 63],
            ..wire
        };
        let error = signature_envelope_from_proto(malformed).unwrap_err();
        assert!(format!("{error:#}").contains("64 bytes"));
    }

    #[tokio::test]
    async fn disabled_keyring_is_valid_and_fails_only_when_signing_is_requested() {
        let keyring = PersonalDbProtocolKeyring::disabled();
        assert!(!keyring.is_enabled());
        assert!(keyring.trust_store().is_empty());

        let error = keyring
            .sign(PersonalDbSigningObject::CommitCertificate(
                sample_commit_certificate(),
            ))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("witness signing provider is not configured for this operation")
        );
    }

    #[test]
    fn trust_records_without_providers_support_verification_only_use() {
        let signer = signer(
            0x42,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let store = PublicKeyTrustStore::from_records([signer.trust_record().clone()]).unwrap();
        let keyring = PersonalDbProtocolKeyring::from_providers(
            store,
            std::iter::empty::<Arc<dyn PersonalDbSignerProvider>>(),
        )
        .unwrap();

        assert!(!keyring.is_enabled());
        assert_eq!(keyring.trust_store().len(), 1);
    }

    #[tokio::test]
    async fn one_configured_purpose_signs_without_requiring_other_purposes() {
        let witness = Arc::new(signer(
            0x43,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        )) as Arc<dyn ProtocolSigner>;
        let store = PublicKeyTrustStore::from_records([witness.trust_record().clone()]).unwrap();
        let keyring = PersonalDbProtocolKeyring::new_test_only(store, [witness]).unwrap();

        assert!(keyring.is_enabled());
        assert!(keyring.has_provider(SignaturePurpose::Witness));
        assert!(!keyring.has_provider(SignaturePurpose::Snapshot));
        keyring
            .sign(PersonalDbSigningObject::CommitCertificate(
                sample_commit_certificate(),
            ))
            .await
            .unwrap();
    }

    #[test]
    fn configured_provider_requires_an_active_matching_trust_record() {
        let retiring = signer(
            0x44,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0)
                .with_valid_until(10)
                .with_status(PublicKeyStatus::Retiring),
        );
        let retiring_provider = Arc::new(StaticEnvelopeProvider {
            purpose: SignaturePurpose::Witness,
            key_id: retiring.trust_record().key_id.clone(),
            envelope: None,
        }) as Arc<dyn PersonalDbSignerProvider>;
        let error =
            PersonalDbProtocolKeyring::new([retiring.trust_record().clone()], [retiring_provider])
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("must use an active trust record")
        );

        let witness = signer(
            0x45,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let wrong_purpose_provider = Arc::new(StaticEnvelopeProvider {
            purpose: SignaturePurpose::Snapshot,
            key_id: witness.trust_record().key_id.clone(),
            envelope: None,
        }) as Arc<dyn PersonalDbSignerProvider>;
        let error = PersonalDbProtocolKeyring::new(
            [witness.trust_record().clone()],
            [wrong_purpose_provider],
        )
        .unwrap_err();
        assert!(error.to_string().contains("trusted only for witness"));
    }

    #[test]
    fn configured_providers_reject_duplicate_purposes() {
        let first = signer(
            0x46,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let second = signer(
            0x47,
            KeyTrustPolicy::new(KeyGeneration::new(2).unwrap(), SignaturePurpose::Witness, 0),
        );
        let providers = [
            Arc::new(StaticEnvelopeProvider {
                purpose: SignaturePurpose::Witness,
                key_id: first.trust_record().key_id.clone(),
                envelope: None,
            }) as Arc<dyn PersonalDbSignerProvider>,
            Arc::new(StaticEnvelopeProvider {
                purpose: SignaturePurpose::Witness,
                key_id: second.trust_record().key_id.clone(),
                envelope: None,
            }) as Arc<dyn PersonalDbSignerProvider>,
        ];
        let error = PersonalDbProtocolKeyring::new(
            [first.trust_record().clone(), second.trust_record().clone()],
            providers,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("more than one witness signing provider")
        );
    }

    #[tokio::test]
    async fn returned_key_must_match_the_configured_provider() {
        let configured = signer(
            0x48,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let other = signer(
            0x49,
            KeyTrustPolicy::new(KeyGeneration::new(2).unwrap(), SignaturePurpose::Witness, 0),
        );
        let object = PersonalDbSigningObject::CommitCertificate(sample_commit_certificate());
        let wrong_envelope = other.sign(&object).unwrap();
        let provider = Arc::new(StaticEnvelopeProvider {
            purpose: SignaturePurpose::Witness,
            key_id: configured.trust_record().key_id.clone(),
            envelope: Some(wrong_envelope),
        }) as Arc<dyn PersonalDbSignerProvider>;
        let keyring = PersonalDbProtocolKeyring::new(
            [
                configured.trust_record().clone(),
                other.trust_record().clone(),
            ],
            [provider],
        )
        .unwrap();

        let error = keyring.sign(object).await.unwrap_err();
        assert!(error.to_string().contains("signing provider returned key"));
    }

    fn signer(seed: u8, policy: KeyTrustPolicy) -> Ed25519ProtocolSigner {
        Ed25519ProtocolSigner::from_pkcs8_der(&pkcs8(seed), policy).unwrap()
    }

    fn pkcs8(seed: u8) -> Vec<u8> {
        let mut bytes = hex::decode("302e020100300506032b657004220420").unwrap();
        bytes.extend([seed; 32]);
        bytes
    }

    fn sample_commit_certificate() -> crate::personaldb_control::PersonalDbCommitCertificate {
        crate::personaldb_control::PersonalDbCommitCertificate {
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
            witnessed_at: "2026-07-17T00:00:00Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
