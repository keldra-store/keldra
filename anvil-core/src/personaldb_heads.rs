use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    core_store::{
        CoreMutationBatch, CoreMutationPrecondition, CoreMutationRootPublication, CoreStore,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::{hash32, writer::WriterFamily},
    personaldb_control::PersonalDbGroupManifest,
    personaldb_coremeta::{
        PersonalDbDataLocatorCoreMetaRow, PersonalDbGroupCoreMetaRow,
        personaldb_data_locator_precondition, personaldb_group_coremeta_put_operation,
        personaldb_partition_id, personaldb_payload_hash, personaldb_root_anchor_key,
        read_personaldb_data_locator_bytes, read_personaldb_data_locator_row,
        write_personaldb_bytes_as_data_locator_with_preconditions,
    },
    personaldb_signing::{
        PersonalDbProtocolKeyring, signature_envelope_from_proto, signature_envelope_to_proto,
    },
    personaldb_signing_object::PersonalDbSigningObject,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use personaldb_protocol::{
    DatabaseId, ProtocolSignable, PublicKeyTrustStore, SignatureDomain, SignatureEnvelopeV1,
    SignatureMetadata, SignaturePurpose, SignatureScope, SigningPayload,
};
use prost::Message;

const PERSONALDB_HEAD_DATA_PREFIX: &str = "personaldb_head:";
const PERSONALDB_LOG_SEGMENT_REF_PREFIX: &str = "personaldb_log_segment:";
const PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX: &str = "personaldb_snapshot_manifest:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbCommittedHead {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub log_hash: String,
    pub segment_ref: String,
    pub row_index_generation: u64,
    pub policy_epoch: u64,
    pub membership_epoch: u64,
    pub schema_hash: String,
    pub updated_at: String,
    pub updated_by_node: String,
    pub head_hash: Option<String>,
    pub head_signature: Option<SignatureEnvelopeV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotsHead {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub latest_snapshot_log_index: u64,
    pub latest_snapshot_log_hash: String,
    pub latest_snapshot_manifest_ref: String,
    pub retained_snapshot_count: u32,
    pub updated_at: String,
    pub updated_by_node: String,
    pub head_hash: Option<String>,
    pub head_signature: Option<SignatureEnvelopeV1>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbCommittedHeadProto {
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
    segment_ref: String,
    #[prost(uint64, tag = "7")]
    row_index_generation: u64,
    #[prost(uint64, tag = "8")]
    policy_epoch: u64,
    #[prost(uint64, tag = "9")]
    membership_epoch: u64,
    #[prost(string, tag = "10")]
    schema_hash: String,
    #[prost(string, tag = "11")]
    updated_at: String,
    #[prost(string, tag = "12")]
    updated_by_node: String,
    #[prost(string, optional, tag = "13")]
    head_hash: Option<String>,
    #[prost(message, optional, tag = "14")]
    head_signature: Option<WireSignatureEnvelopeV1>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSnapshotsHeadProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    latest_snapshot_log_index: u64,
    #[prost(string, tag = "5")]
    latest_snapshot_log_hash: String,
    #[prost(string, tag = "6")]
    latest_snapshot_manifest_ref: String,
    #[prost(uint32, tag = "7")]
    retained_snapshot_count: u32,
    #[prost(string, tag = "8")]
    updated_at: String,
    #[prost(string, tag = "9")]
    updated_by_node: String,
    #[prost(string, optional, tag = "10")]
    head_hash: Option<String>,
    #[prost(message, optional, tag = "11")]
    head_signature: Option<WireSignatureEnvelopeV1>,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbGroupManifestProto {
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
    #[prost(string, optional, tag = "14")]
    manifest_hash: Option<String>,
    #[prost(message, optional, tag = "15")]
    manifest_signature: Option<WireSignatureEnvelopeV1>,
}

impl PersonalDbCommittedHead {
    pub async fn seal(mut self, keyring: &PersonalDbProtocolKeyring) -> Result<Self> {
        validate_committed_head_unsigned(&self)?;
        require_unsealed(
            self.head_hash.as_ref(),
            self.head_signature.as_ref(),
            "personaldb committed head",
        )?;
        let hash = hash_committed_head(&self)?;
        let signature = keyring
            .sign(PersonalDbSigningObject::CommittedHead(self.clone()))
            .await?;
        self.head_hash = Some(hash);
        self.head_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, trust_store: &PublicKeyTrustStore) -> Result<()> {
        validate_committed_head_unsigned(self)?;
        let expected_hash = hash_committed_head(self)?;
        if self.head_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb committed head hash mismatch"));
        }
        let signature = self
            .head_signature
            .as_ref()
            .ok_or_else(|| anyhow!("personaldb committed head signature missing"))?;
        trust_store.verify(self, signature)?;
        Ok(())
    }
}

impl ProtocolSignable for PersonalDbCommittedHead {
    fn signature_metadata(&self) -> SignatureMetadata {
        SignatureMetadata::for_domain(
            SignaturePurpose::Witness,
            SignatureDomain::CommittedHead,
            self.log_index,
        )
        .with_scope(SignatureScope::for_database_group(
            DatabaseId::new(&self.database_id),
            self.database_id.clone(),
        ))
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(committed_head_payload_hash(self))
    }
}

impl PersonalDbSnapshotsHead {
    pub async fn seal(mut self, keyring: &PersonalDbProtocolKeyring) -> Result<Self> {
        validate_snapshots_head_unsigned(&self)?;
        require_unsealed(
            self.head_hash.as_ref(),
            self.head_signature.as_ref(),
            "personaldb snapshots head",
        )?;
        let hash = hash_snapshots_head(&self)?;
        let signature = keyring
            .sign(PersonalDbSigningObject::SnapshotsHead(self.clone()))
            .await?;
        self.head_hash = Some(hash);
        self.head_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, trust_store: &PublicKeyTrustStore) -> Result<()> {
        validate_snapshots_head_unsigned(self)?;
        let expected_hash = hash_snapshots_head(self)?;
        if self.head_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb snapshots head hash mismatch"));
        }
        let signature = self
            .head_signature
            .as_ref()
            .ok_or_else(|| anyhow!("personaldb snapshots head signature missing"))?;
        trust_store.verify(self, signature)?;
        Ok(())
    }
}

impl ProtocolSignable for PersonalDbSnapshotsHead {
    fn signature_metadata(&self) -> SignatureMetadata {
        SignatureMetadata::for_domain(
            SignaturePurpose::Snapshot,
            SignatureDomain::SnapshotsHead,
            self.latest_snapshot_log_index,
        )
        .with_scope(SignatureScope::for_database_group(
            DatabaseId::new(&self.database_id),
            self.database_id.clone(),
        ))
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(snapshots_head_payload_hash(self))
    }
}

pub fn hash_committed_head(head: &PersonalDbCommittedHead) -> Result<String> {
    validate_committed_head_unsigned(head)?;
    Ok(hex::encode(committed_head_payload_hash(head)))
}

pub fn hash_snapshots_head(head: &PersonalDbSnapshotsHead) -> Result<String> {
    validate_snapshots_head_unsigned(head)?;
    Ok(hex::encode(snapshots_head_payload_hash(head)))
}

pub async fn write_personaldb_group_manifest(
    storage: &Storage,
    tenant_id: i64,
    manifest: &PersonalDbGroupManifest,
    trust_store: &PublicKeyTrustStore,
) -> Result<()> {
    manifest.verify(trust_store)?;
    ensure_head_scope(
        tenant_id,
        &manifest.database_id,
        &manifest.tenant_id,
        &manifest.database_id,
    )?;
    write_head_record(
        storage,
        &personaldb_head_data_id(tenant_id, &manifest.database_id, "group_manifest")?,
        manifest,
    )
    .await
}

pub async fn read_personaldb_group_manifest(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbGroupManifest>> {
    let Some(manifest) = read_head_record::<PersonalDbGroupManifest>(
        storage,
        &personaldb_head_data_id(tenant_id, database_id, "group_manifest")?,
    )
    .await?
    else {
        return Ok(None);
    };
    manifest.verify(trust_store)?;
    ensure_head_scope(
        tenant_id,
        database_id,
        &manifest.tenant_id,
        &manifest.database_id,
    )?;
    Ok(Some(manifest))
}

pub async fn write_personaldb_committed_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    trust_store: &PublicKeyTrustStore,
) -> Result<()> {
    head.verify(trust_store)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    write_head_record(
        storage,
        &personaldb_head_data_id(tenant_id, database_id, "committed_head")?,
        head,
    )
    .await
}

pub async fn write_personaldb_committed_head_with_preconditions(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    trust_store: &PublicKeyTrustStore,
    preconditions: Vec<CoreMutationPrecondition>,
) -> Result<()> {
    head.verify(trust_store)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    write_head_record_with_preconditions(
        storage,
        &personaldb_head_data_id(tenant_id, database_id, "committed_head")?,
        head,
        preconditions,
    )
    .await
}

pub async fn read_personaldb_committed_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbCommittedHead>> {
    let Some(head) = read_head_record::<PersonalDbCommittedHead>(
        storage,
        &personaldb_head_data_id(tenant_id, database_id, "committed_head")?,
    )
    .await?
    else {
        return Ok(None);
    };
    head.verify(trust_store)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    Ok(Some(head))
}

pub(crate) fn personaldb_committed_head_precondition(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<CoreMutationPrecondition> {
    personaldb_data_locator_precondition(
        storage,
        tenant_id,
        database_id,
        &personaldb_head_data_id(tenant_id, database_id, "committed_head")?,
    )
}

pub async fn write_personaldb_snapshots_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbSnapshotsHead,
    trust_store: &PublicKeyTrustStore,
) -> Result<()> {
    head.verify(trust_store)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    write_head_record(
        storage,
        &personaldb_head_data_id(tenant_id, database_id, "snapshots_head")?,
        head,
    )
    .await
}

pub async fn read_personaldb_snapshots_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbSnapshotsHead>> {
    let Some(head) = read_head_record::<PersonalDbSnapshotsHead>(
        storage,
        &personaldb_head_data_id(tenant_id, database_id, "snapshots_head")?,
    )
    .await?
    else {
        return Ok(None);
    };
    head.verify(trust_store)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    Ok(Some(head))
}

pub(crate) fn validate_committed_head_unsigned(head: &PersonalDbCommittedHead) -> Result<()> {
    if head.format_version != 2 {
        return Err(anyhow!("unsupported personaldb committed head version"));
    }
    validate_hex32(&head.log_hash, "log_hash")?;
    validate_hex32(&head.schema_hash, "schema_hash")?;
    require_nonempty(&head.tenant_id, "tenant_id")?;
    require_nonempty(&head.database_id, "database_id")?;
    if head.log_index == 0 {
        if !head.segment_ref.is_empty() {
            return Err(anyhow!(
                "personaldb genesis committed head segment ref must be empty"
            ));
        }
    } else {
        require_corestore_ref(
            &head.segment_ref,
            "segment_ref",
            PERSONALDB_LOG_SEGMENT_REF_PREFIX,
        )?;
    }
    require_nonempty(&head.updated_at, "updated_at")?;
    require_nonempty(&head.updated_by_node, "updated_by_node")?;
    Ok(())
}

pub(crate) fn validate_snapshots_head_unsigned(head: &PersonalDbSnapshotsHead) -> Result<()> {
    if head.format_version != 2 {
        return Err(anyhow!("unsupported personaldb snapshots head version"));
    }
    validate_hex32(&head.latest_snapshot_log_hash, "latest_snapshot_log_hash")?;
    require_nonempty(&head.tenant_id, "tenant_id")?;
    require_nonempty(&head.database_id, "database_id")?;
    require_corestore_ref(
        &head.latest_snapshot_manifest_ref,
        "latest_snapshot_manifest_ref",
        PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX,
    )?;
    require_nonempty(&head.updated_at, "updated_at")?;
    require_nonempty(&head.updated_by_node, "updated_by_node")?;
    Ok(())
}

fn ensure_head_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    actual_tenant_id: &str,
    actual_database_id: &str,
) -> Result<()> {
    if actual_tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb head tenant scope mismatch"));
    }
    if actual_database_id != expected_database_id {
        return Err(anyhow!("personaldb head database scope mismatch"));
    }
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

async fn write_head_record<T: PersonalDbHeadRecordCodec>(
    storage: &Storage,
    data_id: &str,
    value: &T,
) -> Result<()> {
    write_head_record_with_preconditions(storage, data_id, value, Vec::new()).await
}

async fn write_head_record_with_preconditions<T: PersonalDbHeadRecordCodec>(
    storage: &Storage,
    data_id: &str,
    value: &T,
    preconditions: Vec<CoreMutationPrecondition>,
) -> Result<()> {
    let (tenant_id, database_id) = personaldb_head_scope(data_id)?;
    let encoded = value.encode_record()?;
    let payload_hash = personaldb_payload_hash(&encoded);
    let row = write_personaldb_bytes_as_data_locator_with_preconditions(
        storage,
        tenant_id,
        &database_id,
        data_id,
        value.data_kind(),
        value.writer_generation().max(1),
        encoded,
        payload_hash,
        vec![format!("kind:{}", value.data_kind())],
        format!("personaldb-head:{}", uuid::Uuid::new_v4().simple()),
        &preconditions,
    )
    .await?;
    if let Some(group_row) = value.group_coremeta_row(tenant_id, &database_id, &row)? {
        let scope_partition = personaldb_partition_id(tenant_id, &database_id);
        CoreStore::new(storage.clone())
            .await?
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: format!(
                    "personaldb-head-group:{}:{}",
                    group_row.group_id, group_row.generation
                ),
                scope_partition: scope_partition.clone(),
                committed_by_principal: "system:personaldb".to_string(),
                root_publications: vec![
                    CoreMutationRootPublication::new(
                        scope_partition,
                        WriterFamily::CoreControl.as_str(),
                    )
                    .coordinator(),
                    CoreMutationRootPublication::new(
                        personaldb_root_anchor_key(group_row.tenant_id, &group_row.group_id),
                        WriterFamily::PersonalDb.as_str(),
                    ),
                ],
                preconditions,
                operations: vec![personaldb_group_coremeta_put_operation(&group_row)?],
            })
            .await?;
    }
    Ok(())
}

async fn read_head_record<T: PersonalDbHeadRecordCodec>(
    storage: &Storage,
    data_id: &str,
) -> Result<Option<T>> {
    let (tenant_id, database_id) = personaldb_head_scope(data_id)?;
    let Some(row) =
        read_personaldb_data_locator_row(storage, tenant_id, &database_id, data_id).await?
    else {
        return Ok(None);
    };
    if row.data_kind != T::data_kind_static() {
        return Err(anyhow!("personaldb CoreMeta row has wrong record kind"));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    Ok(Some(T::decode_record(&bytes)?))
}

trait PersonalDbHeadRecordCodec: Sized {
    fn data_kind_static() -> &'static str;
    fn data_kind(&self) -> &'static str {
        Self::data_kind_static()
    }
    fn writer_generation(&self) -> u64;
    fn encode_record(&self) -> Result<Vec<u8>>;
    fn decode_record(bytes: &[u8]) -> Result<Self>;
    fn group_coremeta_row(
        &self,
        _tenant_id: i64,
        _database_id: &str,
        _locator: &PersonalDbDataLocatorCoreMetaRow,
    ) -> Result<Option<PersonalDbGroupCoreMetaRow>> {
        Ok(None)
    }
}

impl PersonalDbHeadRecordCodec for PersonalDbCommittedHead {
    fn data_kind_static() -> &'static str {
        "committed_head"
    }

    fn writer_generation(&self) -> u64 {
        self.log_index
    }

    fn encode_record(&self) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&committed_head_to_proto(self)))
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        committed_head_from_proto(decode_deterministic_proto::<PersonalDbCommittedHeadProto>(
            bytes,
            "personaldb committed head",
        )?)
    }
}

pub(crate) fn encode_committed_head(head: &PersonalDbCommittedHead) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&committed_head_to_proto(head)))
}

pub(crate) fn decode_committed_head(bytes: &[u8]) -> Result<PersonalDbCommittedHead> {
    committed_head_from_proto(decode_deterministic_proto::<PersonalDbCommittedHeadProto>(
        bytes,
        "personaldb committed head",
    )?)
}

impl PersonalDbHeadRecordCodec for PersonalDbSnapshotsHead {
    fn data_kind_static() -> &'static str {
        "snapshots_head"
    }

    fn writer_generation(&self) -> u64 {
        self.latest_snapshot_log_index
    }

    fn encode_record(&self) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&snapshots_head_to_proto(self)))
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        snapshots_head_from_proto(decode_deterministic_proto::<PersonalDbSnapshotsHeadProto>(
            bytes,
            "personaldb snapshots head",
        )?)
    }
}

impl PersonalDbHeadRecordCodec for PersonalDbGroupManifest {
    fn data_kind_static() -> &'static str {
        "group_manifest"
    }

    fn writer_generation(&self) -> u64 {
        self.active_policy_epoch
            .max(self.current_row_index_generation)
    }

    fn encode_record(&self) -> Result<Vec<u8>> {
        Ok(encode_deterministic_proto(&group_manifest_to_proto(self)))
    }

    fn decode_record(bytes: &[u8]) -> Result<Self> {
        group_manifest_from_proto(decode_deterministic_proto::<PersonalDbGroupManifestProto>(
            bytes,
            "personaldb group manifest",
        )?)
    }

    fn group_coremeta_row(
        &self,
        tenant_id: i64,
        database_id: &str,
        locator: &PersonalDbDataLocatorCoreMetaRow,
    ) -> Result<Option<PersonalDbGroupCoreMetaRow>> {
        Ok(Some(PersonalDbGroupCoreMetaRow {
            tenant_id,
            group_id: database_id.to_string(),
            generation: locator
                .root_generation
                .checked_add(1)
                .ok_or_else(|| anyhow!("personaldb group root generation overflow"))?,
            replica_set_hash: personaldb_payload_hash(self.created_by.as_bytes()),
            witness_policy_hash: personaldb_payload_hash(self.consistency_policy.as_bytes()),
            latest_commit: self.genesis_hash.clone(),
            snapshot_locator: None,
            transaction_id: locator.transaction_id.clone(),
            created_at_unix_nanos: locator.created_at_unix_nanos,
        }))
    }
}

fn committed_head_to_proto(head: &PersonalDbCommittedHead) -> PersonalDbCommittedHeadProto {
    PersonalDbCommittedHeadProto {
        format_version: u32::from(head.format_version),
        tenant_id: head.tenant_id.clone(),
        database_id: head.database_id.clone(),
        log_index: head.log_index,
        log_hash: head.log_hash.clone(),
        segment_ref: head.segment_ref.clone(),
        row_index_generation: head.row_index_generation,
        policy_epoch: head.policy_epoch,
        membership_epoch: head.membership_epoch,
        schema_hash: head.schema_hash.clone(),
        updated_at: head.updated_at.clone(),
        updated_by_node: head.updated_by_node.clone(),
        head_hash: head.head_hash.clone(),
        head_signature: head
            .head_signature
            .as_ref()
            .map(signature_envelope_to_proto),
    }
}

fn committed_head_from_proto(
    proto: PersonalDbCommittedHeadProto,
) -> Result<PersonalDbCommittedHead> {
    Ok(PersonalDbCommittedHead {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("personaldb committed head version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        log_index: proto.log_index,
        log_hash: proto.log_hash,
        segment_ref: proto.segment_ref,
        row_index_generation: proto.row_index_generation,
        policy_epoch: proto.policy_epoch,
        membership_epoch: proto.membership_epoch,
        schema_hash: proto.schema_hash,
        updated_at: proto.updated_at,
        updated_by_node: proto.updated_by_node,
        head_hash: proto.head_hash,
        head_signature: proto
            .head_signature
            .map(signature_envelope_from_proto)
            .transpose()?,
    })
}

fn snapshots_head_to_proto(head: &PersonalDbSnapshotsHead) -> PersonalDbSnapshotsHeadProto {
    PersonalDbSnapshotsHeadProto {
        format_version: u32::from(head.format_version),
        tenant_id: head.tenant_id.clone(),
        database_id: head.database_id.clone(),
        latest_snapshot_log_index: head.latest_snapshot_log_index,
        latest_snapshot_log_hash: head.latest_snapshot_log_hash.clone(),
        latest_snapshot_manifest_ref: head.latest_snapshot_manifest_ref.clone(),
        retained_snapshot_count: head.retained_snapshot_count,
        updated_at: head.updated_at.clone(),
        updated_by_node: head.updated_by_node.clone(),
        head_hash: head.head_hash.clone(),
        head_signature: head
            .head_signature
            .as_ref()
            .map(signature_envelope_to_proto),
    }
}

fn snapshots_head_from_proto(
    proto: PersonalDbSnapshotsHeadProto,
) -> Result<PersonalDbSnapshotsHead> {
    if proto.format_version != 2 {
        return Err(anyhow!("unsupported personaldb snapshots head version"));
    }
    Ok(PersonalDbSnapshotsHead {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("personaldb snapshots head version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        latest_snapshot_log_index: proto.latest_snapshot_log_index,
        latest_snapshot_log_hash: proto.latest_snapshot_log_hash,
        latest_snapshot_manifest_ref: proto.latest_snapshot_manifest_ref,
        retained_snapshot_count: proto.retained_snapshot_count,
        updated_at: proto.updated_at,
        updated_by_node: proto.updated_by_node,
        head_hash: proto.head_hash,
        head_signature: proto
            .head_signature
            .map(signature_envelope_from_proto)
            .transpose()?,
    })
}

fn group_manifest_to_proto(manifest: &PersonalDbGroupManifest) -> PersonalDbGroupManifestProto {
    PersonalDbGroupManifestProto {
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
        manifest_hash: manifest.manifest_hash.clone(),
        manifest_signature: manifest
            .manifest_signature
            .as_ref()
            .map(signature_envelope_to_proto),
    }
}

fn committed_head_payload_hash(head: &PersonalDbCommittedHead) -> [u8; 32] {
    let mut proto = committed_head_to_proto(head);
    proto.head_hash = None;
    proto.head_signature = None;
    hash32(&encode_deterministic_proto(&proto))
}

fn snapshots_head_payload_hash(head: &PersonalDbSnapshotsHead) -> [u8; 32] {
    let mut proto = snapshots_head_to_proto(head);
    proto.head_hash = None;
    proto.head_signature = None;
    hash32(&encode_deterministic_proto(&proto))
}

fn group_manifest_from_proto(
    proto: PersonalDbGroupManifestProto,
) -> Result<PersonalDbGroupManifest> {
    Ok(PersonalDbGroupManifest {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("personaldb group manifest version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        schema_hash: proto.schema_hash,
        genesis_hash: proto.genesis_hash,
        created_at: proto.created_at,
        created_by: proto.created_by,
        consistency_policy: proto.consistency_policy,
        object_layout_version: u16::try_from(proto.object_layout_version)
            .map_err(|_| anyhow!("personaldb group manifest layout version exceeds u16"))?,
        active_membership_epoch: proto.active_membership_epoch,
        active_policy_epoch: proto.active_policy_epoch,
        current_row_index_generation: proto.current_row_index_generation,
        current_projection_generation: proto.current_projection_generation,
        manifest_hash: proto.manifest_hash,
        manifest_signature: proto
            .manifest_signature
            .map(signature_envelope_from_proto)
            .transpose()?,
    })
}

fn personaldb_head_data_id(tenant_id: i64, database_id: &str, kind: &str) -> Result<String> {
    validate_head_scope_component(tenant_id, database_id)?;
    require_safe_component(kind, "personaldb head kind")?;
    Ok(format!(
        "{PERSONALDB_HEAD_DATA_PREFIX}tenant:{tenant_id}:database:{database_id}:{kind}"
    ))
}

fn personaldb_head_scope(data_id: &str) -> Result<(i64, String)> {
    let rest = data_id
        .strip_prefix(PERSONALDB_HEAD_DATA_PREFIX)
        .ok_or_else(|| anyhow!("personaldb CoreMeta head id has invalid prefix"))?;
    let parts = rest.split(':').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != "tenant" || parts[2] != "database" {
        return Err(anyhow!("personaldb CoreMeta head id has invalid shape"));
    }
    let tenant_id = parts[1]
        .parse::<i64>()
        .map_err(|_| anyhow!("personaldb CoreMeta head id tenant is invalid"))?;
    validate_head_scope_component(tenant_id, parts[3])?;
    require_safe_component(parts[4], "personaldb head kind")?;
    Ok((tenant_id, parts[3].to_string()))
}

fn validate_head_scope_component(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        return Err(anyhow!("personaldb tenant id must be nonnegative"));
    }
    require_safe_component(database_id, "database_id")
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::personaldb_protocol_keyring;
    use tempfile::tempdir;

    #[tokio::test]
    async fn committed_head_round_trips_via_coremeta_data_locator() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();
        let head = sample_committed_head().seal(&keyring).await.unwrap();

        write_personaldb_committed_head(&storage, 7, "db-alpha", &head, keyring.trust_store())
            .await
            .unwrap();

        let data_id = personaldb_head_data_id(7, "db-alpha", "committed_head").unwrap();
        let row = read_personaldb_data_locator_row(&storage, 7, "db-alpha", &data_id)
            .await
            .unwrap()
            .expect("committed head CoreMeta locator row exists");
        assert_eq!(row.data_kind, "committed_head");
        assert_eq!(row.generation, head.log_index);
        assert!(
            !read_personaldb_data_locator_bytes(&storage, &row)
                .await
                .unwrap()
                .is_empty()
        );

        let read = read_personaldb_committed_head(&storage, 7, "db-alpha", keyring.trust_store())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, head);
        assert_eq!(
            read.head_signature
                .as_ref()
                .unwrap()
                .signature
                .as_bytes()
                .len(),
            64
        );
        read.verify(keyring.trust_store()).unwrap();
    }

    #[tokio::test]
    async fn snapshots_head_round_trips_via_coremeta_data_locator() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();
        let head = sample_snapshots_head().seal(&keyring).await.unwrap();

        write_personaldb_snapshots_head(&storage, 7, "db-alpha", &head, keyring.trust_store())
            .await
            .unwrap();

        let data_id = personaldb_head_data_id(7, "db-alpha", "snapshots_head").unwrap();
        let row = read_personaldb_data_locator_row(&storage, 7, "db-alpha", &data_id)
            .await
            .unwrap()
            .expect("snapshots head CoreMeta locator row exists");
        assert_eq!(row.data_kind, "snapshots_head");
        assert_eq!(row.generation, head.latest_snapshot_log_index);

        let read = read_personaldb_snapshots_head(&storage, 7, "db-alpha", keyring.trust_store())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, head);
        let expected_hash = hash_snapshots_head(&read).unwrap();
        assert_eq!(read.head_hash.as_deref(), Some(expected_hash.as_str()));
        assert_eq!(
            read.signature_metadata().purpose,
            SignaturePurpose::Snapshot
        );
        assert_eq!(
            read.signature_metadata().domain,
            SignatureDomain::SnapshotsHead
        );
        assert_eq!(
            read.head_signature
                .as_ref()
                .unwrap()
                .signature
                .as_bytes()
                .len(),
            64
        );
        read.verify(keyring.trust_store()).unwrap();
    }

    #[tokio::test]
    async fn group_manifest_round_trips_via_coremeta_data_locator() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();
        let manifest = sample_group_manifest().seal(&keyring).await.unwrap();

        write_personaldb_group_manifest(&storage, 7, &manifest, keyring.trust_store())
            .await
            .unwrap();

        let data_id = personaldb_head_data_id(7, "db-alpha", "group_manifest").unwrap();
        let row = read_personaldb_data_locator_row(&storage, 7, "db-alpha", &data_id)
            .await
            .unwrap()
            .expect("group manifest CoreMeta locator row exists");
        assert_eq!(row.data_kind, "group_manifest");

        let read = read_personaldb_group_manifest(&storage, 7, "db-alpha", keyring.trust_store())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, manifest);
        assert_eq!(
            read.manifest_signature.unwrap().signature.as_bytes().len(),
            64,
            "raw Ed25519 signature must survive the CoreStore protobuf round trip"
        );
    }

    #[tokio::test]
    async fn missing_heads_return_none() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();

        assert!(
            read_personaldb_committed_head(&storage, 7, "db-alpha", keyring.trust_store())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_personaldb_snapshots_head(&storage, 7, "db-alpha", keyring.trust_store(),)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn snapshots_head_rejects_format_version_one_and_missing_signature() {
        let keyring = personaldb_protocol_keyring();
        let unsupported = PersonalDbSnapshotsHead {
            format_version: 1,
            ..sample_snapshots_head()
        };
        assert!(
            unsupported
                .seal(&keyring)
                .await
                .unwrap_err()
                .to_string()
                .contains("unsupported personaldb snapshots head version")
        );

        let mut unsigned = sample_snapshots_head();
        unsigned.head_hash = Some(hash_snapshots_head(&unsigned).unwrap());
        assert!(
            unsigned
                .verify(keyring.trust_store())
                .unwrap_err()
                .to_string()
                .contains("signature missing")
        );
    }

    #[tokio::test]
    async fn tampered_committed_head_is_rejected() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();
        let head = sample_committed_head().seal(&keyring).await.unwrap();
        write_personaldb_committed_head(&storage, 7, "db-alpha", &head, keyring.trust_store())
            .await
            .unwrap();

        let data_id = personaldb_head_data_id(7, "db-alpha", "committed_head").unwrap();
        let row = read_personaldb_data_locator_row(&storage, 7, "db-alpha", &data_id)
            .await
            .unwrap()
            .expect("committed head CoreMeta locator row exists");
        let mut value = read_personaldb_data_locator_bytes(&storage, &row)
            .await
            .unwrap();
        *value.last_mut().expect("stored head bytes are not empty") ^= 0x01;
        crate::personaldb_coremeta::write_personaldb_bytes_as_data_locator(
            &storage,
            7,
            "db-alpha",
            &data_id,
            "committed_head",
            row.generation + 1,
            value.clone(),
            personaldb_payload_hash(&value),
            vec!["kind:committed_head".to_string()],
            "personaldb-head-tamper".to_string(),
        )
        .await
        .unwrap();

        assert!(
            read_personaldb_committed_head(&storage, 7, "db-alpha", keyring.trust_store())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_hashes_and_scope_are_rejected() {
        let keyring = personaldb_protocol_keyring();
        let invalid_hash = PersonalDbCommittedHead {
            log_hash: "not-hex".to_string(),
            ..sample_committed_head()
        };
        assert!(invalid_hash.seal(&keyring).await.is_err());

        let wrong_scope = PersonalDbCommittedHead {
            database_id: "db-beta".to_string(),
            ..sample_committed_head()
        }
        .seal(&keyring)
        .await
        .unwrap();
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            write_personaldb_committed_head(
                &storage,
                7,
                "db-alpha",
                &wrong_scope,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );
        assert!(personaldb_head_data_id(7, "../escape", "committed_head").is_err());
    }

    fn sample_committed_head() -> PersonalDbCommittedHead {
        PersonalDbCommittedHead {
            format_version: 2,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 42,
            log_hash: hex::encode([1; 32]),
            segment_ref: concat!(
                "personaldb_log_segment:tenant:7:database:db-alpha:",
                "start:00000000000000000001:end:00000000000000000042:",
                "hash:0000000000000000000000000000000000000000000000000000000000000001"
            )
            .to_string(),
            row_index_generation: 3,
            policy_epoch: 5,
            membership_epoch: 8,
            schema_hash: hex::encode([2; 32]),
            updated_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            updated_by_node: "node-a".to_string(),
            head_hash: None,
            head_signature: None,
        }
    }

    fn sample_snapshots_head() -> PersonalDbSnapshotsHead {
        PersonalDbSnapshotsHead {
            format_version: 2,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            latest_snapshot_log_index: 1024,
            latest_snapshot_log_hash: hex::encode([3; 32]),
            latest_snapshot_manifest_ref: "personaldb_snapshot_manifest:tenant:7:database:db-alpha:log:00000000000000001024:state:0000000000000000000000000000000000000000000000000000000000000003".to_string(),
            retained_snapshot_count: 2,
            updated_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            updated_by_node: "node-a".to_string(),
            head_hash: None,
            head_signature: None,
        }
    }

    fn sample_group_manifest() -> PersonalDbGroupManifest {
        PersonalDbGroupManifest {
            format_version: 2,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            schema_hash: hex::encode([1; 32]),
            genesis_hash: hex::encode([2; 32]),
            created_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            created_by: "principal-a".to_string(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 8,
            active_policy_epoch: 5,
            current_row_index_generation: 3,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
    }
}
