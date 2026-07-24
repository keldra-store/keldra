//! In-process custody for PersonalDB protocol signing keys.
//!
//! Signing keys are Anvil control-plane state. They live in the System Realm's
//! CoreMeta namespace and are encrypted with the configured Anvil secret
//! [`EncryptionKeyring`]. Public read APIs deliberately return only trust and
//! audit metadata; decrypted PKCS#8 bytes exist only for the duration of a
//! typed protocol signing call.

use crate::{
    core_store::{
        CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRowCommonProto, CoreMetaTuplePart,
        CoreMetaVisibilityState, CoreStore, TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW,
        TABLE_PERSONALDB_SIGNING_KEY_ROW, commit_coremeta_batch_for_storage,
        core_meta_committed_row_common, core_meta_record_tuple_key, core_meta_root_key_hash,
        core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
    },
    crypto::EncryptionKeyring,
    personaldb_signing::{PersonalDbProtocolKeyring, PersonalDbSignerProvider},
    personaldb_signing_object::PersonalDbSigningObject,
    storage::Storage,
    system_realm::SYSTEM_REALM_ID,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use personaldb_protocol::{
    DatabaseId, Ed25519ProtocolSigner, Ed25519PublicKey, KeyGeneration, KeyId, LogIndex,
    PUBLIC_KEY_TRUST_RECORD_FORMAT_VERSION_V1, ProtocolSignable, ProtocolSigner as _,
    PublicKeyStatus, PublicKeyTrustRecord, PublicKeyTrustStore, SignatureAlgorithm,
    SignatureEnvelopeV1, SignaturePurpose,
};
use prost::Message;
use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const SIGNING_KEY_RECORD_FORMAT_VERSION: u32 = 1;
const SIGNING_KEY_RECORD_SCHEMA: &str = "anvil.system.personaldb_signing_key.v1";
const SIGNING_KEY_HEAD_SCHEMA: &str = "anvil.system.personaldb_signing_key_head.v1";
const SIGNING_KEY_NAMESPACE: &str = "personaldb-signing-key";
const SIGNING_KEY_PAGE_MAX: usize = 1_000;
const SIGNING_KEY_COUNT_MAX: usize = 4_096;
// CoreStore replaces this value and the empty transaction id before publication.
const CORE_META_PUBLICATION_GENERATION_PLACEHOLDER: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSigningKeyAuditMetadata {
    pub actor_id: String,
    pub operation_id: String,
    pub reason: Option<String>,
}

impl PersonalDbSigningKeyAuditMetadata {
    pub fn new(actor_id: impl Into<String>, operation_id: impl Into<String>) -> Self {
        Self {
            actor_id: actor_id.into(),
            operation_id: operation_id.into(),
            reason: None,
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    fn validate(&self) -> Result<()> {
        validate_audit_field(&self.actor_id, "actor id", 512)?;
        validate_audit_field(&self.operation_id, "operation id", 512)?;
        if let Some(reason) = &self.reason {
            validate_audit_field(reason, "reason", 4 * 1024)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSigningKeyPublicRecord {
    pub trust_record: PublicKeyTrustRecord,
    pub created_at_unix_nanos: u64,
    pub updated_at_unix_nanos: u64,
    pub created_audit: PersonalDbSigningKeyAuditMetadata,
    pub updated_audit: PersonalDbSigningKeyAuditMetadata,
    pub record_revision: u64,
}

#[derive(Debug, Clone)]
pub struct PersonalDbSigningKeyPage {
    pub records: Vec<PersonalDbSigningKeyPublicRecord>,
    pub next_tuple_key: Option<Vec<u8>>,
}

pub struct PersonalDbSigningKeyImport<'a> {
    pub trust_record: PublicKeyTrustRecord,
    pub private_key_pkcs8_der: &'a [u8],
    pub audit: PersonalDbSigningKeyAuditMetadata,
}

impl fmt::Debug for PersonalDbSigningKeyImport<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersonalDbSigningKeyImport")
            .field("trust_record", &self.trust_record)
            .field("private_key_pkcs8_der", &"[REDACTED]")
            .field("audit", &self.audit)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSigningKeyStatusUpdate {
    pub key_id: KeyId,
    pub expected_record_revision: u64,
    pub status: PublicKeyStatus,
    /// Exclusive upper boundary for the key's authority.
    pub valid_until_log_index: Option<LogIndex>,
    pub audit: PersonalDbSigningKeyAuditMetadata,
}

#[derive(Clone)]
pub struct PersonalDbSigningKeyStore {
    storage: Storage,
    core_store: CoreStore,
    encryption_keyring: Arc<EncryptionKeyring>,
    mutation_lock: Arc<Mutex<()>>,
}

impl fmt::Debug for PersonalDbSigningKeyStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersonalDbSigningKeyStore")
            .field("storage", &self.storage)
            .field("encryption_keyring", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl PersonalDbSigningKeyStore {
    pub fn new(core_store: CoreStore, encryption_keyring: Arc<EncryptionKeyring>) -> Self {
        Self {
            storage: core_store.storage().clone(),
            core_store,
            encryption_keyring,
            mutation_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Imports a new immutable key identity.
    ///
    /// Existing key IDs are never overwritten. Status and validity-boundary
    /// changes must use [`Self::set_status`].
    pub async fn import_key(
        &self,
        import: PersonalDbSigningKeyImport<'_>,
    ) -> Result<PersonalDbSigningKeyPublicRecord> {
        import.audit.validate()?;
        if import.private_key_pkcs8_der.is_empty() {
            bail!("PersonalDB signing private key must not be empty");
        }

        let trust_record = canonicalise_trust_record(import.trust_record)?;
        Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(
            import.private_key_pkcs8_der,
            trust_record.clone(),
        )
        .context("PersonalDB signing private key does not match its trust record")?;

        let encrypted_private_key_pkcs8_der = self
            .encryption_keyring
            .encrypt(import.private_key_pkcs8_der)
            .context("encrypt PersonalDB signing private key")?;
        let _guard = self.mutation_lock.lock().await;

        if self.load_stored_row(&trust_record.key_id)?.is_some() {
            bail!(
                "PersonalDB signing key {} already exists; key identity is immutable",
                trust_record.key_id
            );
        }
        let existing_records = self.list_public_records()?;
        if existing_records.len() >= SIGNING_KEY_COUNT_MAX {
            bail!(
                "PersonalDB signing key count has reached the configured maximum of {SIGNING_KEY_COUNT_MAX}"
            );
        }
        self.reject_conflicting_generation(&trust_record, &existing_records)?;

        let now = current_unix_nanos()?;
        let row = StoredSigningKey {
            public: PersonalDbSigningKeyPublicRecord {
                trust_record,
                created_at_unix_nanos: now,
                updated_at_unix_nanos: now,
                created_audit: import.audit.clone(),
                updated_audit: import.audit,
                record_revision: 1,
            },
            encrypted_private_key_pkcs8_der,
        };
        self.write_stored_row(&row).await?;
        Ok(row.public)
    }

    pub fn get_public_record(
        &self,
        key_id: &KeyId,
    ) -> Result<Option<PersonalDbSigningKeyPublicRecord>> {
        Ok(self.load_stored_row(key_id)?.map(|row| row.public))
    }

    pub fn list_public_records(&self) -> Result<Vec<PersonalDbSigningKeyPublicRecord>> {
        let revision = self.current_collection_revision()?;
        let mut records = Vec::new();
        let mut after_tuple_key = None;
        loop {
            let page = self.page_public_records(
                revision,
                after_tuple_key.as_deref(),
                SIGNING_KEY_PAGE_MAX,
            )?;
            records.extend(page.records);
            if records.len() > SIGNING_KEY_COUNT_MAX {
                bail!(
                    "PersonalDB signing key count exceeds the configured maximum of {SIGNING_KEY_COUNT_MAX}"
                );
            }
            let Some(next_tuple_key) = page.next_tuple_key else {
                return Ok(records);
            };
            after_tuple_key = Some(next_tuple_key);
        }
    }

    pub fn current_collection_revision(&self) -> Result<u64> {
        let Some(payload) = self.core_store.read_coremeta_row(
            CF_MESH,
            TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW,
            &signing_key_head_tuple_key()?,
        )?
        else {
            return Ok(0);
        };
        decode_signing_key_head(&payload)
    }

    pub fn page_public_records(
        &self,
        expected_revision: u64,
        after_tuple_key: Option<&[u8]>,
        page_size: usize,
    ) -> Result<PersonalDbSigningKeyPage> {
        if !(1..=SIGNING_KEY_PAGE_MAX).contains(&page_size) {
            bail!("PersonalDB signing key page size must be between 1 and {SIGNING_KEY_PAGE_MAX}");
        }
        if self.current_collection_revision()? != expected_revision {
            bail!("PersonalDB signing key collection revision changed");
        }
        let mut rows = self.core_store.scan_coremeta_prefix_page(
            CF_MESH,
            TABLE_PERSONALDB_SIGNING_KEY_ROW,
            &signing_key_tuple_prefix()?,
            after_tuple_key,
            page_size + 1,
        )?;
        let has_more = rows.len() > page_size;
        if has_more {
            rows.truncate(page_size);
        }
        let next_tuple_key = if has_more {
            Some(
                core_meta_record_tuple_key(
                    &rows
                        .last()
                        .ok_or_else(|| anyhow!("signing key page continuation has no last row"))?
                        .key,
                )?
                .to_vec(),
            )
        } else {
            None
        };
        let records = rows
            .into_iter()
            .map(|record| decode_stored_row(&record.payload).map(|row| row.public))
            .collect::<Result<Vec<_>>>()?;
        if self.current_collection_revision()? != expected_revision {
            bail!("PersonalDB signing key collection changed during page read");
        }
        Ok(PersonalDbSigningKeyPage {
            records,
            next_tuple_key,
        })
    }

    pub fn load_trust_records(&self) -> Result<Vec<PublicKeyTrustRecord>> {
        Ok(self
            .list_public_records()?
            .into_iter()
            .map(|record| record.trust_record)
            .collect())
    }

    pub fn load_trust_store(&self) -> Result<PublicKeyTrustStore> {
        PublicKeyTrustStore::from_records(self.load_trust_records()?)
            .context("build PersonalDB public-key trust store")
    }

    /// Builds the Ed25519-only runtime keyring from persisted System Realm keys.
    ///
    /// Every public trust record is retained for historical verification. At
    /// most one active signing provider is selected for each purpose: the key
    /// with the highest generation wins, while an equal-generation tie is
    /// rejected instead of being resolved by incidental storage order. An
    /// empty store produces the normal disabled keyring.
    pub fn load_protocol_keyring(&self) -> Result<PersonalDbProtocolKeyring> {
        let public_records = self.list_public_records()?;
        let trust_records = public_records
            .iter()
            .map(|record| record.trust_record.clone())
            .collect::<Vec<_>>();
        let mut selected = HashMap::<SignaturePurpose, &PublicKeyTrustRecord>::new();

        for record in public_records
            .iter()
            .map(|record| &record.trust_record)
            .filter(|record| record.status == PublicKeyStatus::Active)
        {
            match selected.get(&record.purpose) {
                None => {
                    selected.insert(record.purpose, record);
                }
                Some(current) if record.key_generation > current.key_generation => {
                    selected.insert(record.purpose, record);
                }
                Some(current) if record.key_generation == current.key_generation => {
                    bail!(
                        "PersonalDB {} signing purpose has ambiguous active generation {} keys {} and {}",
                        record.purpose,
                        record.key_generation,
                        current.key_id,
                        record.key_id
                    );
                }
                Some(_) => {}
            }
        }

        let mut selected = selected.into_values().collect::<Vec<_>>();
        selected.sort_by_key(|record| record.purpose as u16);
        let providers = selected
            .into_iter()
            .map(|record| {
                self.provider(&record.key_id)
                    .map(|provider| Arc::new(provider) as Arc<dyn PersonalDbSignerProvider>)
            })
            .collect::<Result<Vec<_>>>()?;
        PersonalDbProtocolKeyring::new(trust_records, providers)
            .context("build PersonalDB protocol keyring from persisted signing keys")
    }

    /// Applies a one-way status transition without changing key identity,
    /// purpose, scopes, generation, or private key material.
    pub async fn set_status(
        &self,
        update: PersonalDbSigningKeyStatusUpdate,
    ) -> Result<PersonalDbSigningKeyPublicRecord> {
        update.audit.validate()?;
        let _guard = self.mutation_lock.lock().await;
        let mut row = self
            .load_stored_row(&update.key_id)?
            .ok_or_else(|| anyhow!("unknown PersonalDB signing key {}", update.key_id))?;
        if row.public.record_revision != update.expected_record_revision {
            bail!(
                "PersonalDB signing key {} revision mismatch: expected {}, current {}",
                update.key_id,
                update.expected_record_revision,
                row.public.record_revision
            );
        }

        if row.public.trust_record.status == update.status {
            if row.public.trust_record.valid_until_log_index == update.valid_until_log_index {
                return Ok(row.public);
            }
            bail!(
                "PersonalDB signing key {} status update conflicts with its existing boundary",
                update.key_id
            );
        }
        validate_status_transition(row.public.trust_record.status, update.status)?;
        validate_non_extending_boundary(
            row.public.trust_record.valid_until_log_index,
            update.valid_until_log_index,
        )?;

        row.public.trust_record.status = update.status;
        row.public.trust_record.valid_until_log_index = update.valid_until_log_index;
        row.public
            .trust_record
            .validate()
            .context("PersonalDB signing key status update produces an invalid trust record")?;
        row.public.record_revision = row
            .public
            .record_revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("PersonalDB signing key record revision overflow"))?;
        let now = current_unix_nanos()?;
        row.public.updated_at_unix_nanos = now.max(
            row.public
                .updated_at_unix_nanos
                .checked_add(1)
                .ok_or_else(|| anyhow!("PersonalDB signing key timestamp overflow"))?,
        );
        row.public.updated_audit = update.audit;

        self.write_stored_row(&row).await?;
        Ok(row.public)
    }

    /// Signs one typed PersonalDB protocol object with a persisted key.
    ///
    /// The encrypted PKCS#8 value is decrypted only in this method. Its byte
    /// buffer is cleared on every return path, and the protocol signer is
    /// dropped before this method returns.
    pub fn sign(
        &self,
        key_id: &KeyId,
        signable: &dyn ProtocolSignable,
    ) -> Result<SignatureEnvelopeV1> {
        let row = self
            .load_stored_row(key_id)?
            .ok_or_else(|| anyhow!("unknown PersonalDB signing key {key_id}"))?;
        if row.public.trust_record.status != PublicKeyStatus::Active {
            bail!(
                "PersonalDB signing key {key_id} is {} and cannot produce new signatures",
                row.public.trust_record.status
            );
        }
        let decrypted_private_key = SensitiveBytes(
            self.encryption_keyring
                .decrypt(&row.encrypted_private_key_pkcs8_der)
                .context("decrypt PersonalDB signing private key")?,
        );
        let signer = Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(
            decrypted_private_key.as_slice(),
            row.public.trust_record,
        )
        .context("load PersonalDB signing private key")?;
        let result = signer
            .sign(signable)
            .context("sign PersonalDB protocol object");
        drop(signer);
        drop(decrypted_private_key);
        result
    }

    pub fn provider(&self, key_id: &KeyId) -> Result<StorageBackedPersonalDbSignerProvider> {
        let record = self
            .get_public_record(key_id)?
            .ok_or_else(|| anyhow!("unknown PersonalDB signing key {key_id}"))?;
        if record.trust_record.status != PublicKeyStatus::Active {
            bail!(
                "PersonalDB signing key {key_id} must be active before it is configured as a provider"
            );
        }
        Ok(StorageBackedPersonalDbSignerProvider {
            store: self.clone(),
            purpose: record.trust_record.purpose,
            key_id: key_id.clone(),
        })
    }

    fn reject_conflicting_generation(
        &self,
        candidate: &PublicKeyTrustRecord,
        existing_records: &[PersonalDbSigningKeyPublicRecord],
    ) -> Result<()> {
        for existing in existing_records {
            let existing = &existing.trust_record;
            if existing.purpose == candidate.purpose
                && existing.key_generation == candidate.key_generation
                && scopes_overlap(&existing.database_scopes, &candidate.database_scopes)
                && scopes_overlap(&existing.group_scopes, &candidate.group_scopes)
            {
                bail!(
                    "PersonalDB {} signing generation {} already has key {} for an overlapping scope",
                    candidate.purpose,
                    candidate.key_generation,
                    existing.key_id
                );
            }
        }
        Ok(())
    }

    fn load_stored_row(&self, key_id: &KeyId) -> Result<Option<StoredSigningKey>> {
        let key = signing_key_tuple_key(key_id)?;
        self.core_store
            .read_coremeta_row(CF_MESH, TABLE_PERSONALDB_SIGNING_KEY_ROW, &key)?
            .map(|payload| decode_stored_row(&payload))
            .transpose()
    }

    async fn write_stored_row(&self, row: &StoredSigningKey) -> Result<()> {
        validate_stored_row(row)?;
        let key_id = &row.public.trust_record.key_id;
        let mutation_id = signing_key_mutation_id(key_id, row.public.record_revision);
        let tuple_key = signing_key_tuple_key(key_id)?;
        let payload = encode_stored_row(row)?;
        let common = signing_key_publication_candidate_common(row);
        let collection_revision = self
            .current_collection_revision()?
            .checked_add(1)
            .ok_or_else(|| anyhow!("PersonalDB signing key collection revision overflow"))?;
        let head_tuple_key = signing_key_head_tuple_key()?;
        let head_payload = encode_signing_key_head(collection_revision)?;
        let key_op = CoreMetaBatchOp {
            cf: CF_MESH,
            table_id: TABLE_PERSONALDB_SIGNING_KEY_ROW,
            tuple_key: &tuple_key,
            common: Some(common),
            kind: CoreMetaBatchOpKind::Put(&payload),
        };
        let head_op = CoreMetaBatchOp {
            cf: CF_MESH,
            table_id: TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW,
            tuple_key: &head_tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&head_payload),
        };
        commit_coremeta_batch_for_storage(
            &self.storage,
            &mutation_id,
            &[key_op, head_op],
            &[
                crate::core_store::CoreMetaRootPublication::new(
                    signing_key_root_id(key_id),
                    crate::formats::writer::WriterFamily::PersonalDb,
                ),
                crate::core_store::CoreMetaRootPublication::new(
                    format!("{SYSTEM_REALM_ID}/{SIGNING_KEY_NAMESPACE}/head"),
                    crate::formats::writer::WriterFamily::PersonalDb,
                )
                .coordinator(),
            ],
        )
        .await
        .with_context(|| format!("persist PersonalDB signing key {key_id}"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct StorageBackedPersonalDbSignerProvider {
    store: PersonalDbSigningKeyStore,
    purpose: SignaturePurpose,
    key_id: KeyId,
}

impl fmt::Debug for StorageBackedPersonalDbSignerProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageBackedPersonalDbSignerProvider")
            .field("purpose", &self.purpose)
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl PersonalDbSignerProvider for StorageBackedPersonalDbSignerProvider {
    fn purpose(&self) -> SignaturePurpose {
        self.purpose
    }

    fn key_id(&self) -> &KeyId {
        &self.key_id
    }

    async fn sign(&self, object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1> {
        object.validate()?;
        if object.metadata().purpose != self.purpose {
            bail!(
                "PersonalDB {} provider cannot sign a {} object",
                self.purpose,
                object.metadata().purpose
            );
        }
        self.store.sign(&self.key_id, object)
    }
}

struct StoredSigningKey {
    public: PersonalDbSigningKeyPublicRecord,
    encrypted_private_key_pkcs8_der: Vec<u8>,
}

pub(crate) struct SensitiveBytes(Vec<u8>);

impl SensitiveBytes {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SensitiveBytes {
    fn drop(&mut self) {
        self.0.fill(0);
        std::hint::black_box(&mut self.0);
    }
}

#[derive(Clone, PartialEq, Message)]
struct SigningKeyAuditProto {
    #[prost(string, tag = "1")]
    actor_id: String,
    #[prost(string, tag = "2")]
    operation_id: String,
    #[prost(string, optional, tag = "3")]
    reason: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSigningKeyRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(uint32, tag = "3")]
    format_version: u32,
    #[prost(string, tag = "4")]
    key_id: String,
    #[prost(uint64, tag = "5")]
    key_generation: u64,
    #[prost(uint32, tag = "6")]
    signature_algorithm: u32,
    #[prost(uint32, tag = "7")]
    signature_purpose: u32,
    #[prost(string, repeated, tag = "8")]
    database_scopes: Vec<String>,
    #[prost(string, repeated, tag = "9")]
    group_scopes: Vec<String>,
    #[prost(uint64, tag = "10")]
    valid_from_log_index: u64,
    #[prost(uint64, optional, tag = "11")]
    valid_until_log_index: Option<u64>,
    #[prost(uint32, tag = "12")]
    status: u32,
    #[prost(bytes = "vec", tag = "13")]
    ed25519_public_key: Vec<u8>,
    #[prost(bytes = "vec", tag = "14")]
    encrypted_private_key_pkcs8_der: Vec<u8>,
    #[prost(uint64, tag = "15")]
    created_at_unix_nanos: u64,
    #[prost(uint64, tag = "16")]
    updated_at_unix_nanos: u64,
    #[prost(message, optional, tag = "17")]
    created_audit: Option<SigningKeyAuditProto>,
    #[prost(message, optional, tag = "18")]
    updated_audit: Option<SigningKeyAuditProto>,
    #[prost(uint64, tag = "19")]
    record_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSigningKeyHeadProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(uint64, tag = "3")]
    revision: u64,
}

fn encode_signing_key_head(revision: u64) -> Result<Vec<u8>> {
    if revision == 0 {
        bail!("PersonalDB signing key collection revision must be non-zero");
    }
    Ok(encode_deterministic_proto(&PersonalDbSigningKeyHeadProto {
        common: Some(core_meta_committed_row_common(
            SYSTEM_REALM_ID,
            signing_key_head_root_hash(),
            CORE_META_PUBLICATION_GENERATION_PLACEHOLDER,
            "",
            current_unix_nanos()?,
        )),
        schema: SIGNING_KEY_HEAD_SCHEMA.to_string(),
        revision,
    }))
}

fn decode_signing_key_head(bytes: &[u8]) -> Result<u64> {
    let proto = decode_deterministic_proto::<PersonalDbSigningKeyHeadProto>(
        bytes,
        "PersonalDB signing key collection head",
    )?;
    if proto.schema != SIGNING_KEY_HEAD_SCHEMA || proto.revision == 0 {
        bail!("PersonalDB signing key collection head is invalid");
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("PersonalDB signing key collection head has no CoreMeta common"))?;
    validate_signing_key_publication_common(
        &common,
        &signing_key_head_root_hash(),
        "PersonalDB signing key collection head",
    )?;
    Ok(proto.revision)
}

fn encode_stored_row(row: &StoredSigningKey) -> Result<Vec<u8>> {
    validate_stored_row(row)?;
    let trust = &row.public.trust_record;
    Ok(encode_deterministic_proto(&PersonalDbSigningKeyRowProto {
        common: Some(signing_key_publication_candidate_common(row)),
        schema: SIGNING_KEY_RECORD_SCHEMA.to_string(),
        format_version: PUBLIC_KEY_TRUST_RECORD_FORMAT_VERSION_V1,
        key_id: trust.key_id.as_str().to_string(),
        key_generation: trust.key_generation.get(),
        signature_algorithm: u32::from(trust.signature_algorithm as u16),
        signature_purpose: u32::from(trust.purpose as u16),
        database_scopes: trust
            .database_scopes
            .iter()
            .map(|database_id| database_id.0.clone())
            .collect(),
        group_scopes: trust.group_scopes.clone(),
        valid_from_log_index: trust.valid_from_log_index,
        valid_until_log_index: trust.valid_until_log_index,
        status: status_to_wire(trust.status),
        ed25519_public_key: trust.public_key.as_bytes().to_vec(),
        encrypted_private_key_pkcs8_der: row.encrypted_private_key_pkcs8_der.clone(),
        created_at_unix_nanos: row.public.created_at_unix_nanos,
        updated_at_unix_nanos: row.public.updated_at_unix_nanos,
        created_audit: Some(audit_to_proto(&row.public.created_audit)),
        updated_audit: Some(audit_to_proto(&row.public.updated_audit)),
        record_revision: row.public.record_revision,
    }))
}

fn decode_stored_row(bytes: &[u8]) -> Result<StoredSigningKey> {
    let proto = decode_deterministic_proto::<PersonalDbSigningKeyRowProto>(
        bytes,
        "PersonalDB signing key CoreMeta row",
    )?;
    if proto.schema != SIGNING_KEY_RECORD_SCHEMA {
        bail!("PersonalDB signing key CoreMeta row has an unsupported schema");
    }
    if proto.format_version != SIGNING_KEY_RECORD_FORMAT_VERSION {
        bail!(
            "PersonalDB signing key CoreMeta row has unsupported format version {}",
            proto.format_version
        );
    }
    let signature_algorithm = SignatureAlgorithm::try_from(
        u16::try_from(proto.signature_algorithm)
            .context("PersonalDB signing signature algorithm exceeds u16")?,
    )?;
    let purpose = SignaturePurpose::try_from(
        u16::try_from(proto.signature_purpose).context("PersonalDB signing purpose exceeds u16")?,
    )?;
    let trust_record = PublicKeyTrustRecord {
        format_version: SIGNING_KEY_RECORD_FORMAT_VERSION,
        signature_algorithm,
        key_id: KeyId::new(proto.key_id)?,
        key_generation: KeyGeneration::new(proto.key_generation)?,
        public_key: Ed25519PublicKey::try_from(proto.ed25519_public_key.as_slice())?,
        purpose,
        database_scopes: proto
            .database_scopes
            .into_iter()
            .map(DatabaseId::new)
            .collect(),
        group_scopes: proto.group_scopes,
        valid_from_log_index: proto.valid_from_log_index,
        valid_until_log_index: proto.valid_until_log_index,
        status: status_from_wire(proto.status)?,
    };
    let row = StoredSigningKey {
        public: PersonalDbSigningKeyPublicRecord {
            trust_record,
            created_at_unix_nanos: proto.created_at_unix_nanos,
            updated_at_unix_nanos: proto.updated_at_unix_nanos,
            created_audit: audit_from_proto(
                proto
                    .created_audit
                    .ok_or_else(|| anyhow!("PersonalDB signing key row has no creation audit"))?,
            )?,
            updated_audit: audit_from_proto(
                proto
                    .updated_audit
                    .ok_or_else(|| anyhow!("PersonalDB signing key row has no update audit"))?,
            )?,
            record_revision: proto.record_revision,
        },
        encrypted_private_key_pkcs8_der: proto.encrypted_private_key_pkcs8_der,
    };
    validate_stored_row_with_common(
        &row,
        proto
            .common
            .as_ref()
            .ok_or_else(|| anyhow!("PersonalDB signing key row has no CoreMeta common fields"))?,
    )?;
    Ok(row)
}

fn validate_stored_row(row: &StoredSigningKey) -> Result<()> {
    row.public.trust_record.validate()?;
    row.public.created_audit.validate()?;
    row.public.updated_audit.validate()?;
    if row.public.record_revision == 0 {
        bail!("PersonalDB signing key record revision must be non-zero");
    }
    if row.public.created_at_unix_nanos == 0
        || row.public.updated_at_unix_nanos < row.public.created_at_unix_nanos
    {
        bail!("PersonalDB signing key timestamps are invalid");
    }
    if row.encrypted_private_key_pkcs8_der.is_empty() {
        bail!("PersonalDB signing key encrypted private key is empty");
    }
    Ok(())
}

fn validate_stored_row_with_common(
    row: &StoredSigningKey,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    validate_stored_row(row)?;
    validate_signing_key_publication_common(
        common,
        &core_meta_root_key_hash(&signing_key_root_id(&row.public.trust_record.key_id)),
        "PersonalDB signing key row",
    )
}

fn signing_key_publication_candidate_common(row: &StoredSigningKey) -> CoreMetaRowCommonProto {
    let key_id = &row.public.trust_record.key_id;
    core_meta_committed_row_common(
        SYSTEM_REALM_ID,
        core_meta_root_key_hash(&signing_key_root_id(key_id)),
        CORE_META_PUBLICATION_GENERATION_PLACEHOLDER,
        "",
        row.public.updated_at_unix_nanos,
    )
}

fn validate_signing_key_publication_common(
    common: &CoreMetaRowCommonProto,
    expected_root_key_hash: &str,
    label: &str,
) -> Result<()> {
    let expected_shape = core_meta_committed_row_common(
        SYSTEM_REALM_ID,
        expected_root_key_hash,
        CORE_META_PUBLICATION_GENERATION_PLACEHOLDER,
        "",
        0,
    );
    if common.realm_id != expected_shape.realm_id
        || common.root_key_hash != expected_shape.root_key_hash
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.payload_schema_version != expected_shape.payload_schema_version
    {
        bail!("{label} has invalid rooted CoreMeta publication metadata");
    }
    Ok(())
}

fn signing_key_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(SYSTEM_REALM_ID),
        CoreMetaTuplePart::Utf8(SIGNING_KEY_NAMESPACE),
    ])
}

fn signing_key_tuple_key(key_id: &KeyId) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(SYSTEM_REALM_ID),
        CoreMetaTuplePart::Utf8(SIGNING_KEY_NAMESPACE),
        CoreMetaTuplePart::Utf8(key_id.as_str()),
    ])
}

fn signing_key_head_tuple_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(SYSTEM_REALM_ID),
        CoreMetaTuplePart::Utf8(SIGNING_KEY_NAMESPACE),
    ])
}

fn signing_key_head_root_hash() -> String {
    core_meta_root_key_hash(&format!("{SYSTEM_REALM_ID}/{SIGNING_KEY_NAMESPACE}/head"))
}

fn signing_key_root_id(key_id: &KeyId) -> String {
    format!("{SYSTEM_REALM_ID}/{SIGNING_KEY_NAMESPACE}/{key_id}")
}

fn signing_key_mutation_id(key_id: &KeyId, revision: u64) -> String {
    let digest = key_id.as_str().strip_prefix("sha256:").unwrap_or("invalid");
    format!("personaldb-signing-key-{digest}-{revision}")
}

fn canonicalise_trust_record(
    mut trust_record: PublicKeyTrustRecord,
) -> Result<PublicKeyTrustRecord> {
    trust_record.validate()?;
    if trust_record.signature_algorithm != SignatureAlgorithm::Ed25519 {
        bail!("PersonalDB signing key must use Ed25519");
    }
    trust_record
        .database_scopes
        .sort_by(|left, right| left.0.cmp(&right.0));
    trust_record.group_scopes.sort();
    trust_record.validate()?;
    Ok(trust_record)
}

fn validate_status_transition(from: PublicKeyStatus, to: PublicKeyStatus) -> Result<()> {
    let allowed = matches!(
        (from, to),
        (PublicKeyStatus::Active, PublicKeyStatus::Retiring)
            | (PublicKeyStatus::Active, PublicKeyStatus::RevokedFuture)
            | (PublicKeyStatus::Active, PublicKeyStatus::Compromised)
            | (PublicKeyStatus::Retiring, PublicKeyStatus::RevokedFuture)
            | (PublicKeyStatus::Retiring, PublicKeyStatus::Compromised)
            | (PublicKeyStatus::RevokedFuture, PublicKeyStatus::Compromised)
    );
    if !allowed {
        bail!("invalid PersonalDB signing key status transition {from} -> {to}");
    }
    Ok(())
}

fn validate_non_extending_boundary(
    current: Option<LogIndex>,
    next: Option<LogIndex>,
) -> Result<()> {
    let next = next.ok_or_else(|| {
        anyhow!("non-active PersonalDB signing keys require a valid-until log boundary")
    })?;
    if current.is_some_and(|current| next > current) {
        bail!("PersonalDB signing key status transition cannot extend its authority boundary");
    }
    Ok(())
}

fn scopes_overlap<T: PartialEq>(left: &[T], right: &[T]) -> bool {
    left.is_empty() || right.is_empty() || left.iter().any(|item| right.contains(item))
}

fn status_to_wire(status: PublicKeyStatus) -> u32 {
    match status {
        PublicKeyStatus::Active => 1,
        PublicKeyStatus::Retiring => 2,
        PublicKeyStatus::RevokedFuture => 3,
        PublicKeyStatus::Compromised => 4,
    }
}

fn status_from_wire(status: u32) -> Result<PublicKeyStatus> {
    match status {
        1 => Ok(PublicKeyStatus::Active),
        2 => Ok(PublicKeyStatus::Retiring),
        3 => Ok(PublicKeyStatus::RevokedFuture),
        4 => Ok(PublicKeyStatus::Compromised),
        _ => bail!("PersonalDB signing key row has unknown status {status}"),
    }
}

fn audit_to_proto(audit: &PersonalDbSigningKeyAuditMetadata) -> SigningKeyAuditProto {
    SigningKeyAuditProto {
        actor_id: audit.actor_id.clone(),
        operation_id: audit.operation_id.clone(),
        reason: audit.reason.clone(),
    }
}

fn audit_from_proto(proto: SigningKeyAuditProto) -> Result<PersonalDbSigningKeyAuditMetadata> {
    let audit = PersonalDbSigningKeyAuditMetadata {
        actor_id: proto.actor_id,
        operation_id: proto.operation_id,
        reason: proto.reason,
    };
    audit.validate()?;
    Ok(audit)
}

fn validate_audit_field(value: &str, label: &str, max_len: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_len
        || value.chars().any(|character| character.is_control())
    {
        bail!("PersonalDB signing key audit {label} is invalid");
    }
    Ok(())
}

fn current_unix_nanos() -> Result<u64> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos();
    u64::try_from(nanos).context("current Unix timestamp exceeds u64 nanoseconds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_store::CoreMetaStore;
    use personaldb_protocol::{
        KeyTrustPolicy, SignatureDomain, SignatureMetadata, SignatureScope, SigningPayload,
    };
    use tempfile::tempdir;

    #[derive(Debug)]
    struct TestSignable {
        payload: Vec<u8>,
        metadata: SignatureMetadata,
    }

    impl ProtocolSignable for TestSignable {
        fn signature_metadata(&self) -> SignatureMetadata {
            self.metadata.clone()
        }

        fn signing_payload(&self) -> SigningPayload<'_> {
            SigningPayload::ExactBytes(&self.payload)
        }
    }

    #[tokio::test]
    async fn encrypted_row_round_trips_without_exposing_private_key() {
        let directory = tempdir().unwrap();
        let storage = Storage::new_at(directory.path()).await.unwrap();
        let store = test_store(storage.clone()).await;
        let private_key = pkcs8(0x41);
        let trust_record = trust_record(
            &private_key,
            SignaturePurpose::Witness,
            1,
            vec![DatabaseId::new("database-a")],
            vec!["group-a".to_string()],
        );

        let imported = store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record.clone(),
                private_key_pkcs8_der: &private_key,
                audit: audit("import-1"),
            })
            .await
            .unwrap();

        let raw_payload = CoreMetaStore::open(storage.core_store_meta_path())
            .unwrap()
            .get(
                CF_MESH,
                TABLE_PERSONALDB_SIGNING_KEY_ROW,
                &signing_key_tuple_key(&trust_record.key_id).unwrap(),
            )
            .unwrap()
            .unwrap();
        assert!(
            !raw_payload
                .windows(private_key.len())
                .any(|window| window == private_key.as_slice())
        );
        let stored = decode_stored_row(&raw_payload).unwrap();
        assert_ne!(stored.encrypted_private_key_pkcs8_der, private_key);
        assert_eq!(stored.public, imported);
        assert_eq!(store.list_public_records().unwrap(), vec![imported.clone()]);

        let object = TestSignable {
            payload: b"admitted commit certificate".to_vec(),
            metadata: SignatureMetadata::for_domain(
                SignaturePurpose::Witness,
                SignatureDomain::CommitCertificate,
                7,
            )
            .with_scope(SignatureScope::for_database_group(
                DatabaseId::new("database-a"),
                "group-a",
            ))
            .requiring_key_generation(KeyGeneration::new(1).unwrap()),
        };
        let envelope = store.sign(&trust_record.key_id, &object).unwrap();
        store
            .load_trust_store()
            .unwrap()
            .verify(&object, &envelope)
            .unwrap();
    }

    #[tokio::test]
    async fn signing_key_pages_seek_ordered_rows_and_reject_stale_revisions() {
        let directory = tempdir().unwrap();
        let store = test_store(Storage::new_at(directory.path()).await.unwrap()).await;
        let mut imported = Vec::new();
        for (seed, generation) in [(0x31, 1), (0x32, 2), (0x33, 3)] {
            let private_key = pkcs8(seed);
            imported.push(
                store
                    .import_key(PersonalDbSigningKeyImport {
                        trust_record: trust_record(
                            &private_key,
                            SignaturePurpose::Witness,
                            generation,
                            Vec::new(),
                            Vec::new(),
                        ),
                        private_key_pkcs8_der: &private_key,
                        audit: audit(&format!("import-{generation}")),
                    })
                    .await
                    .unwrap(),
            );
        }

        let revision = store.current_collection_revision().unwrap();
        assert_eq!(revision, 3);
        let first = store.page_public_records(revision, None, 2).unwrap();
        assert_eq!(first.records.len(), 2);
        let second = store
            .page_public_records(revision, first.next_tuple_key.as_deref(), 2)
            .unwrap();
        assert_eq!(second.records.len(), 1);
        assert!(second.next_tuple_key.is_none());
        let mut paged = first.records;
        paged.extend(second.records);
        assert_eq!(paged, store.list_public_records().unwrap());

        store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id: imported[0].trust_record.key_id.clone(),
                expected_record_revision: 1,
                status: PublicKeyStatus::Retiring,
                valid_until_log_index: Some(10),
                audit: audit("retire"),
            })
            .await
            .unwrap();
        assert_eq!(store.current_collection_revision().unwrap(), 4);
        assert!(
            store
                .page_public_records(revision, None, 2)
                .unwrap_err()
                .to_string()
                .contains("revision changed")
        );
    }

    #[tokio::test]
    async fn import_rejects_mismatched_and_duplicate_key_material() {
        let directory = tempdir().unwrap();
        let store = test_store(Storage::new_at(directory.path()).await.unwrap()).await;
        let private_key = pkcs8(0x51);
        let trust_record = trust_record(
            &private_key,
            SignaturePurpose::Snapshot,
            1,
            Vec::new(),
            Vec::new(),
        );
        let wrong_private_key = pkcs8(0x52);

        let error = store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record.clone(),
                private_key_pkcs8_der: &wrong_private_key,
                audit: audit("mismatch"),
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("does not match"));
        assert!(store.list_public_records().unwrap().is_empty());

        store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record.clone(),
                private_key_pkcs8_der: &private_key,
                audit: audit("first"),
            })
            .await
            .unwrap();
        let error = store
            .import_key(PersonalDbSigningKeyImport {
                trust_record,
                private_key_pkcs8_der: &private_key,
                audit: audit("duplicate"),
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("already exists"));
    }

    #[tokio::test]
    async fn status_transitions_are_one_way_and_do_not_change_key_identity() {
        let directory = tempdir().unwrap();
        let store = test_store(Storage::new_at(directory.path()).await.unwrap()).await;
        let private_key = pkcs8(0x61);
        let trust_record = trust_record(
            &private_key,
            SignaturePurpose::ProposalAdmission,
            3,
            Vec::new(),
            Vec::new(),
        );
        store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record.clone(),
                private_key_pkcs8_der: &private_key,
                audit: audit("import"),
            })
            .await
            .unwrap();

        let retiring = store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id: trust_record.key_id.clone(),
                expected_record_revision: 1,
                status: PublicKeyStatus::Retiring,
                valid_until_log_index: Some(20),
                audit: audit("retire"),
            })
            .await
            .unwrap();
        assert_eq!(retiring.trust_record.key_id, trust_record.key_id);
        assert_eq!(
            retiring.trust_record.key_generation,
            trust_record.key_generation
        );
        assert_eq!(retiring.record_revision, 2);

        let object = TestSignable {
            payload: b"proposal admission".to_vec(),
            metadata: SignatureMetadata::for_domain(
                SignaturePurpose::ProposalAdmission,
                SignatureDomain::ProposalAdmission,
                10,
            )
            .requiring_key_generation(KeyGeneration::new(3).unwrap()),
        };
        let error = store.sign(&trust_record.key_id, &object).unwrap_err();
        assert!(format!("{error:#}").contains("retiring"));

        let error = store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id: trust_record.key_id.clone(),
                expected_record_revision: 1,
                status: PublicKeyStatus::RevokedFuture,
                valid_until_log_index: Some(18),
                audit: audit("stale-revoke"),
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("revision mismatch"));

        let error = store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id: trust_record.key_id.clone(),
                expected_record_revision: 2,
                status: PublicKeyStatus::Active,
                valid_until_log_index: None,
                audit: audit("reactivate"),
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("invalid"));

        let revoked = store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id: trust_record.key_id,
                expected_record_revision: 2,
                status: PublicKeyStatus::RevokedFuture,
                valid_until_log_index: Some(18),
                audit: audit("revoke"),
            })
            .await
            .unwrap();
        assert_eq!(revoked.trust_record.status, PublicKeyStatus::RevokedFuture);
        assert_eq!(revoked.trust_record.valid_until_log_index, Some(18));
        assert_eq!(revoked.record_revision, 3);
    }

    #[tokio::test]
    async fn overlapping_scope_cannot_reuse_a_purpose_generation() {
        let directory = tempdir().unwrap();
        let store = test_store(Storage::new_at(directory.path()).await.unwrap()).await;
        let first_private_key = pkcs8(0x71);
        let second_private_key = pkcs8(0x72);
        store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record(
                    &first_private_key,
                    SignaturePurpose::GroupControl,
                    4,
                    vec![DatabaseId::new("database-a")],
                    vec!["group-a".to_string()],
                ),
                private_key_pkcs8_der: &first_private_key,
                audit: audit("first"),
            })
            .await
            .unwrap();

        let error = store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record(
                    &second_private_key,
                    SignaturePurpose::GroupControl,
                    4,
                    vec![DatabaseId::new("database-a")],
                    vec!["group-a".to_string()],
                ),
                private_key_pkcs8_der: &second_private_key,
                audit: audit("second"),
            })
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("overlapping scope"));
    }

    #[tokio::test]
    async fn protocol_keyring_is_empty_or_selects_highest_active_generation() {
        let directory = tempdir().unwrap();
        let store = test_store(Storage::new_at(directory.path()).await.unwrap()).await;
        let empty = store.load_protocol_keyring().unwrap();
        assert!(!empty.is_enabled());
        assert!(empty.trust_store().is_empty());

        let first_private_key = pkcs8(0x81);
        let second_private_key = pkcs8(0x82);
        let first = trust_record(
            &first_private_key,
            SignaturePurpose::Witness,
            1,
            Vec::new(),
            Vec::new(),
        );
        let second = trust_record(
            &second_private_key,
            SignaturePurpose::Witness,
            2,
            Vec::new(),
            Vec::new(),
        );
        for (record, private_key, operation) in [
            (first.clone(), first_private_key.as_slice(), "first"),
            (second.clone(), second_private_key.as_slice(), "second"),
        ] {
            store
                .import_key(PersonalDbSigningKeyImport {
                    trust_record: record,
                    private_key_pkcs8_der: private_key,
                    audit: audit(operation),
                })
                .await
                .unwrap();
        }

        let keyring = store.load_protocol_keyring().unwrap();
        assert!(keyring.has_provider(SignaturePurpose::Witness));
        assert_eq!(keyring.trust_store().len(), 2);
        assert_eq!(
            keyring
                .trust_record_for_purpose(SignaturePurpose::Witness)
                .unwrap()
                .key_id,
            second.key_id
        );
    }

    #[tokio::test]
    async fn protocol_keyring_rejects_equal_generation_active_provider_ambiguity() {
        let directory = tempdir().unwrap();
        let store = test_store(Storage::new_at(directory.path()).await.unwrap()).await;
        let first_private_key = pkcs8(0x91);
        let second_private_key = pkcs8(0x92);
        for (private_key, database_scope, operation) in [
            (first_private_key.as_slice(), "database-a", "first"),
            (second_private_key.as_slice(), "database-b", "second"),
        ] {
            store
                .import_key(PersonalDbSigningKeyImport {
                    trust_record: trust_record(
                        private_key,
                        SignaturePurpose::Snapshot,
                        7,
                        vec![DatabaseId::new(database_scope)],
                        Vec::new(),
                    ),
                    private_key_pkcs8_der: private_key,
                    audit: audit(operation),
                })
                .await
                .unwrap();
        }

        let error = store.load_protocol_keyring().unwrap_err();
        assert!(format!("{error:#}").contains("ambiguous active generation 7"));
    }

    #[tokio::test]
    async fn corestore_publication_generation_advances_independently_of_signing_revisions() {
        let directory = tempdir().unwrap();
        let storage = Storage::new_at(directory.path()).await.unwrap();
        let store = test_store(storage.clone()).await;
        let private_key = pkcs8(0xa0);
        let trust_record = trust_record(
            &private_key,
            SignaturePurpose::Witness,
            9,
            Vec::new(),
            Vec::new(),
        );
        store
            .import_key(PersonalDbSigningKeyImport {
                trust_record: trust_record.clone(),
                private_key_pkcs8_der: &private_key,
                audit: audit("import-before-physical-gap"),
            })
            .await
            .unwrap();

        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let key_tuple = signing_key_tuple_key(&trust_record.key_id).unwrap();
        let head_tuple = signing_key_head_tuple_key().unwrap();
        let key_payload = meta
            .get(CF_MESH, TABLE_PERSONALDB_SIGNING_KEY_ROW, &key_tuple)
            .unwrap()
            .unwrap();
        let head_payload = meta
            .get(CF_MESH, TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW, &head_tuple)
            .unwrap()
            .unwrap();
        let operations = [
            CoreMetaBatchOp {
                cf: CF_MESH,
                table_id: TABLE_PERSONALDB_SIGNING_KEY_ROW,
                tuple_key: &key_tuple,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&key_payload),
            },
            CoreMetaBatchOp {
                cf: CF_MESH,
                table_id: TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW,
                tuple_key: &head_tuple,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&head_payload),
            },
        ];
        commit_coremeta_batch_for_storage(
            &storage,
            "personaldb-signing-key-test-physical-gap",
            &operations,
            &[
                crate::core_store::CoreMetaRootPublication::new(
                    signing_key_root_id(&trust_record.key_id),
                    crate::formats::writer::WriterFamily::PersonalDb,
                ),
                crate::core_store::CoreMetaRootPublication::new(
                    format!("{SYSTEM_REALM_ID}/{SIGNING_KEY_NAMESPACE}/head"),
                    crate::formats::writer::WriterFamily::PersonalDb,
                )
                .coordinator(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            store
                .get_public_record(&trust_record.key_id)
                .unwrap()
                .unwrap()
                .record_revision,
            1
        );
        assert_eq!(store.current_collection_revision().unwrap(), 1);
        store
            .set_status(PersonalDbSigningKeyStatusUpdate {
                key_id: trust_record.key_id.clone(),
                expected_record_revision: 1,
                status: PublicKeyStatus::Retiring,
                valid_until_log_index: Some(20),
                audit: audit("retire-after-physical-gap"),
            })
            .await
            .unwrap();

        let key_proto = decode_deterministic_proto::<PersonalDbSigningKeyRowProto>(
            &meta
                .get(CF_MESH, TABLE_PERSONALDB_SIGNING_KEY_ROW, &key_tuple)
                .unwrap()
                .unwrap(),
            "persisted PersonalDB signing key physical generation test row",
        )
        .unwrap();
        let head_proto = decode_deterministic_proto::<PersonalDbSigningKeyHeadProto>(
            &meta
                .get(CF_MESH, TABLE_PERSONALDB_SIGNING_KEY_HEAD_ROW, &head_tuple)
                .unwrap()
                .unwrap(),
            "persisted PersonalDB signing key physical generation test head",
        )
        .unwrap();
        assert_eq!(key_proto.record_revision, 2);
        assert_eq!(key_proto.common.unwrap().root_generation, 3);
        assert_eq!(head_proto.revision, 2);
        assert_eq!(head_proto.common.unwrap().root_generation, 3);
    }

    #[test]
    fn logical_signing_revisions_are_independent_from_physical_publication_common() {
        let private_key = pkcs8(0xa1);
        let audit = audit("logical-revision-41");
        let row = StoredSigningKey {
            public: PersonalDbSigningKeyPublicRecord {
                trust_record: trust_record(
                    &private_key,
                    SignaturePurpose::Witness,
                    17,
                    Vec::new(),
                    Vec::new(),
                ),
                created_at_unix_nanos: 100,
                updated_at_unix_nanos: 200,
                created_audit: audit.clone(),
                updated_audit: audit,
                record_revision: 41,
            },
            encrypted_private_key_pkcs8_der: vec![0x5a; 48],
        };
        let mut key_proto = decode_deterministic_proto::<PersonalDbSigningKeyRowProto>(
            &encode_stored_row(&row).unwrap(),
            "PersonalDB signing key test row",
        )
        .unwrap();
        {
            let key_common = key_proto.common.as_mut().unwrap();
            key_common.root_generation = 7;
            key_common.transaction_id = "physical-signing-key-transaction".to_string();
        }

        let decoded = decode_stored_row(&encode_deterministic_proto(&key_proto)).unwrap();
        let physical_key_generation = key_proto.common.as_ref().unwrap().root_generation;
        assert_eq!(decoded.public.record_revision, 41);
        assert_eq!(physical_key_generation, 7);
        assert_ne!(decoded.public.record_revision, physical_key_generation);

        let mut head_proto = decode_deterministic_proto::<PersonalDbSigningKeyHeadProto>(
            &encode_signing_key_head(83).unwrap(),
            "PersonalDB signing key test head",
        )
        .unwrap();
        {
            let head_common = head_proto.common.as_mut().unwrap();
            head_common.root_generation = 11;
            head_common.transaction_id = "physical-signing-head-transaction".to_string();
        }
        assert_eq!(
            decode_signing_key_head(&encode_deterministic_proto(&head_proto)).unwrap(),
            83
        );
        assert_ne!(
            head_proto.revision,
            head_proto.common.as_ref().unwrap().root_generation
        );

        let mut wrong_scope = key_proto.clone();
        wrong_scope.common.as_mut().unwrap().root_key_hash = signing_key_head_root_hash();
        assert!(decode_stored_row(&encode_deterministic_proto(&wrong_scope)).is_err());

        let mut pending = key_proto;
        pending.common.as_mut().unwrap().visibility_state = CoreMetaVisibilityState::Pending as i32;
        assert!(decode_stored_row(&encode_deterministic_proto(&pending)).is_err());
    }

    async fn test_store(storage: Storage) -> PersonalDbSigningKeyStore {
        let core_store = CoreStore::new(storage).await.unwrap();
        PersonalDbSigningKeyStore::new(
            core_store,
            Arc::new(
                EncryptionKeyring::new("test-key", vec![0xa5; 32])
                    .expect("test encryption key is valid"),
            ),
        )
    }

    fn audit(operation_id: &str) -> PersonalDbSigningKeyAuditMetadata {
        PersonalDbSigningKeyAuditMetadata::new("app:test-admin", operation_id)
            .with_reason("test key lifecycle operation")
    }

    fn trust_record(
        private_key: &[u8],
        purpose: SignaturePurpose,
        generation: u64,
        database_scopes: Vec<DatabaseId>,
        group_scopes: Vec<String>,
    ) -> PublicKeyTrustRecord {
        let policy = KeyTrustPolicy::new(KeyGeneration::new(generation).unwrap(), purpose, 0)
            .with_database_scopes(database_scopes)
            .with_group_scopes(group_scopes);
        Ed25519ProtocolSigner::from_pkcs8_der(private_key, policy)
            .unwrap()
            .trust_record()
            .clone()
    }

    fn pkcs8(seed: u8) -> Vec<u8> {
        let mut bytes = hex::decode("302e020100300506032b657004220420").unwrap();
        bytes.extend([seed; 32]);
        bytes
    }
}
