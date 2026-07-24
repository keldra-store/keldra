use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    core_store::{CoreStore, decode_deterministic_proto, encode_deterministic_proto},
    formats::{Hash32, hash32},
    personaldb_control::PersonalDbCommitCertificate,
    personaldb_coremeta::{
        PersonalDbDataLocatorCoreMetaRow, personaldb_payload_hash,
        read_personaldb_data_locator_bytes, read_personaldb_data_locator_row,
        write_personaldb_bytes_as_data_locator, write_personaldb_data_locator_row,
    },
    personaldb_signing::{signature_envelope_from_proto, signature_envelope_to_proto},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use personaldb_protocol::PublicKeyTrustStore;
use prost::Message;

const PERSONALDB_CHANGESET_BY_INDEX_REF_PREFIX: &str = "personaldb_changeset_payload_by_index:";
const PERSONALDB_CHANGESET_BY_HASH_REF_PREFIX: &str = "personaldb_changeset_payload_by_hash:";
const PERSONALDB_COMMIT_CERTIFICATE_REF_PREFIX: &str = "personaldb_commit_certificate:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbChangesetPayloadRefs {
    pub by_index_ref: String,
    pub by_hash_ref: String,
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbCommitCertificateProto {
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
    #[prost(string, optional, tag = "17")]
    certificate_hash: Option<String>,
    #[prost(message, optional, tag = "18")]
    witness_signature: Option<WireSignatureEnvelopeV1>,
}

pub async fn write_personaldb_changeset_payload(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    expected_payload_hash: Hash32,
    changeset_bytes: &[u8],
) -> Result<PersonalDbChangesetPayloadRefs> {
    let actual_hash = hash32(changeset_bytes);
    if actual_hash != expected_payload_hash {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }

    let payload_hash_hex = hex::encode(expected_payload_hash);
    let by_hash_ref =
        personaldb_changeset_payload_by_hash_ref_name(tenant_id, database_id, &payload_hash_hex)?;
    let by_index_ref = personaldb_changeset_payload_by_index_ref_name(
        tenant_id,
        database_id,
        log_index,
        &payload_hash_hex,
    )?;

    let by_hash_row = write_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        database_id,
        &by_hash_ref,
        "changeset",
        log_index,
        changeset_bytes.to_vec(),
        payload_hash_hex.clone(),
        vec![format!("log_index:{log_index:020}")],
        format!(
            "personaldb-changeset-payload:{tenant_id}:{database_id}:{log_index}:{payload_hash_hex}"
        ),
    )
    .await?;
    let by_index_row = PersonalDbDataLocatorCoreMetaRow {
        tenant_id,
        group_id: database_id.to_string(),
        data_id: by_index_ref.clone(),
        data_kind: "changeset".to_string(),
        generation: log_index,
        root_generation: CoreStore::new(storage.clone())
            .await?
            .next_root_generation_for_anchor(
                &crate::personaldb_coremeta::personaldb_root_anchor_key(tenant_id, database_id),
            )
            .await?,
        sqlite_changeset_hash: payload_hash_hex,
        payload_locator: by_hash_row.payload_locator.clone(),
        projection_keys: by_hash_row.projection_keys.clone(),
        transaction_id: format!(
            "personaldb-changeset-index:{tenant_id}:{database_id}:{log_index}:{}",
            by_hash_row.sqlite_changeset_hash
        ),
        created_at_unix_nanos: by_hash_row.created_at_unix_nanos,
    };
    write_personaldb_data_locator_row(storage, &by_index_row, &[]).await?;

    Ok(PersonalDbChangesetPayloadRefs {
        by_index_ref,
        by_hash_ref,
    })
}

pub async fn read_personaldb_changeset_payload_by_hash(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let ref_name = personaldb_changeset_payload_by_hash_ref_name(
        tenant_id,
        database_id,
        &hex::encode(payload_hash),
    )?;
    read_personaldb_changeset_payload_ref(storage, &ref_name, payload_hash).await
}

pub async fn read_personaldb_changeset_payload_by_index(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let ref_name = personaldb_changeset_payload_by_index_ref_name(
        tenant_id,
        database_id,
        log_index,
        &hex::encode(payload_hash),
    )?;
    read_personaldb_changeset_payload_ref(storage, &ref_name, payload_hash).await
}

pub async fn read_personaldb_changeset_payload_ref(
    storage: &Storage,
    ref_name: &str,
    expected_payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let (tenant_id, database_id) = personaldb_ref_scope(ref_name)?;
    let Some(row) =
        read_personaldb_data_locator_row(storage, tenant_id, &database_id, ref_name).await?
    else {
        return Ok(None);
    };
    if row.data_kind != "changeset" {
        return Err(anyhow!("personaldb changeset locator has wrong data kind"));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    if hash32(&bytes) != expected_payload_hash {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }
    Ok(Some(bytes))
}

pub async fn write_personaldb_commit_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    certificate: &PersonalDbCommitCertificate,
    trust_store: &PublicKeyTrustStore,
) -> Result<String> {
    certificate.verify(trust_store)?;
    ensure_scope(
        tenant_id,
        database_id,
        &certificate.tenant_id,
        &certificate.database_id,
    )?;
    let ref_name = personaldb_commit_certificate_ref_name(
        tenant_id,
        database_id,
        certificate.log_index,
        &certificate.entry_hash,
    )?;
    let bytes = encode_commit_certificate(certificate)?;
    write_personaldb_bytes_as_data_locator(
        storage,
        tenant_id,
        database_id,
        &ref_name,
        "commit_certificate",
        certificate.log_index,
        bytes,
        personaldb_payload_hash(certificate.entry_hash.as_bytes()),
        vec![format!("entry_hash:{}", certificate.entry_hash)],
        format!(
            "personaldb-commit-certificate:{tenant_id}:{database_id}:{}:{}",
            certificate.log_index, certificate.entry_hash
        ),
    )
    .await?;
    Ok(ref_name)
}

pub async fn read_personaldb_commit_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    entry_hash: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbCommitCertificate>> {
    let ref_name =
        personaldb_commit_certificate_ref_name(tenant_id, database_id, log_index, entry_hash)?;
    read_personaldb_commit_certificate_ref(storage, &ref_name, trust_store).await
}

pub async fn read_personaldb_commit_certificate_ref(
    storage: &Storage,
    ref_name: &str,
    trust_store: &PublicKeyTrustStore,
) -> Result<Option<PersonalDbCommitCertificate>> {
    let (tenant_id, database_id) = personaldb_ref_scope(ref_name)?;
    let Some(row) =
        read_personaldb_data_locator_row(storage, tenant_id, &database_id, ref_name).await?
    else {
        return Ok(None);
    };
    if row.data_kind != "commit_certificate" {
        return Err(anyhow!(
            "personaldb commit certificate locator has wrong data kind"
        ));
    }
    let bytes = read_personaldb_data_locator_bytes(storage, &row).await?;
    let certificate = decode_commit_certificate(&bytes)?;
    certificate.verify(trust_store)?;
    Ok(Some(certificate))
}

pub(crate) fn encode_commit_certificate(
    certificate: &PersonalDbCommitCertificate,
) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&commit_certificate_to_proto(
        certificate,
    )))
}

pub(crate) fn decode_commit_certificate(bytes: &[u8]) -> Result<PersonalDbCommitCertificate> {
    commit_certificate_from_proto(decode_deterministic_proto::<
        PersonalDbCommitCertificateProto,
    >(bytes, "personaldb commit certificate")?)
}

fn commit_certificate_to_proto(
    certificate: &PersonalDbCommitCertificate,
) -> PersonalDbCommitCertificateProto {
    PersonalDbCommitCertificateProto {
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
        certificate_hash: certificate.certificate_hash.clone(),
        witness_signature: certificate
            .witness_signature
            .as_ref()
            .map(signature_envelope_to_proto),
    }
}

fn commit_certificate_from_proto(
    proto: PersonalDbCommitCertificateProto,
) -> Result<PersonalDbCommitCertificate> {
    Ok(PersonalDbCommitCertificate {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("personaldb commit certificate version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        log_index: proto.log_index,
        previous_log_hash: proto.previous_log_hash,
        entry_hash: proto.entry_hash,
        changeset_payload_hash: proto.changeset_payload_hash,
        verified_envelope_hash: proto.verified_envelope_hash,
        client_log_epoch: proto.client_log_epoch,
        membership_epoch: proto.membership_epoch,
        policy_epoch: proto.policy_epoch,
        leader_replica_id: proto.leader_replica_id,
        voter_acks_hash: proto.voter_acks_hash,
        authz_revision: proto.authz_revision,
        witness_node_id: proto.witness_node_id,
        witnessed_at: proto.witnessed_at,
        certificate_hash: proto.certificate_hash,
        witness_signature: proto
            .witness_signature
            .map(signature_envelope_from_proto)
            .transpose()?,
    })
}

pub fn personaldb_changeset_payload_by_index_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    payload_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(payload_hash, "personaldb changeset payload hash")?;
    Ok(format!(
        "{PERSONALDB_CHANGESET_BY_INDEX_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:hash:{payload_hash}"
    ))
}

pub fn personaldb_changeset_payload_by_hash_ref_name(
    tenant_id: i64,
    database_id: &str,
    payload_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(payload_hash, "personaldb changeset payload hash")?;
    Ok(format!(
        "{PERSONALDB_CHANGESET_BY_HASH_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:hash:{payload_hash}"
    ))
}

pub fn personaldb_commit_certificate_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    entry_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(entry_hash, "personaldb commit entry hash")?;
    Ok(format!(
        "{PERSONALDB_COMMIT_CERTIFICATE_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:entry:{entry_hash}"
    ))
}

fn ensure_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    actual_tenant_id: &str,
    actual_database_id: &str,
) -> Result<()> {
    if actual_tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb commit tenant scope mismatch"));
    }
    if actual_database_id != expected_database_id {
        return Err(anyhow!("personaldb commit database scope mismatch"));
    }
    Ok(())
}

fn validate_scope_component(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        return Err(anyhow!("personaldb tenant id must be nonnegative"));
    }
    if database_id.is_empty()
        || database_id == "."
        || database_id == ".."
        || database_id.contains('/')
        || database_id.contains('\\')
        || database_id.contains(':')
        || database_id.chars().any(char::is_control)
    {
        return Err(anyhow!("database_id is not a safe component"));
    }
    Ok(())
}

fn personaldb_ref_scope(ref_name: &str) -> Result<(i64, String)> {
    if ![
        PERSONALDB_CHANGESET_BY_INDEX_REF_PREFIX,
        PERSONALDB_CHANGESET_BY_HASH_REF_PREFIX,
        PERSONALDB_COMMIT_CERTIFICATE_REF_PREFIX,
    ]
    .iter()
    .any(|prefix| ref_name.starts_with(prefix))
    {
        return Err(anyhow!(
            "personaldb CoreMeta data id has unsupported ref prefix"
        ));
    }
    if ref_name.contains('/') || ref_name.contains('\\') || ref_name.chars().any(char::is_control) {
        return Err(anyhow!(
            "personaldb CoreMeta data id must not be a storage path"
        ));
    }
    let tenant_marker = "tenant:";
    let database_marker = ":database:";
    let tenant_start = ref_name
        .find(tenant_marker)
        .ok_or_else(|| anyhow!("personaldb CoreMeta data id is missing tenant"))?
        + tenant_marker.len();
    let database_marker_offset = ref_name[tenant_start..]
        .find(database_marker)
        .ok_or_else(|| anyhow!("personaldb CoreMeta data id is missing database"))?
        + tenant_start;
    let tenant_id = ref_name[tenant_start..database_marker_offset]
        .parse::<i64>()
        .map_err(|_| anyhow!("personaldb CoreMeta data id tenant is invalid"))?;
    let database_start = database_marker_offset + database_marker.len();
    let database_end = ref_name[database_start..]
        .find(':')
        .map(|offset| database_start + offset)
        .unwrap_or(ref_name.len());
    let database_id = ref_name[database_start..database_end].to_string();
    validate_scope_component(tenant_id, &database_id)?;
    Ok((tenant_id, database_id))
}

fn decode_hex32(value: &str, field: &'static str) -> Result<Hash32> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("{field} must be hex32"))?)
}

#[cfg(test)]
fn encode_core_object_ref_target(object_ref: &crate::core_store::CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

#[cfg(test)]
fn decode_core_object_ref_target(target: &str) -> Result<crate::core_store::CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::personaldb_protocol_keyring;
    use tempfile::tempdir;

    #[tokio::test]
    async fn changeset_payload_is_written_by_hash_and_index() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = b"sqlite changeset bytes";
        let payload_hash = hash32(payload);

        let refs =
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 42, payload_hash, payload)
                .await
                .unwrap();

        assert!(
            refs.by_index_ref
                .starts_with("personaldb_changeset_payload_by_index:tenant:9:database:db-alpha:")
        );
        assert!(
            refs.by_hash_ref
                .starts_with("personaldb_changeset_payload_by_hash:tenant:9:database:db-alpha:")
        );

        let by_hash =
            read_personaldb_changeset_payload_by_hash(&storage, 9, "db-alpha", payload_hash)
                .await
                .unwrap()
                .unwrap();
        let by_index =
            read_personaldb_changeset_payload_by_index(&storage, 9, "db-alpha", 42, payload_hash)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(by_hash, payload);
        assert_eq!(by_index, payload);
    }

    #[tokio::test]
    async fn changeset_payload_rejects_hash_mismatch_and_ref_collision() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload_hash = hash32(b"good");

        assert!(
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 1, payload_hash, b"bad")
                .await
                .is_err()
        );

        let refs =
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 1, payload_hash, b"good")
                .await
                .unwrap();
        write_personaldb_bytes_as_data_locator(
            &storage,
            9,
            "db-alpha",
            &refs.by_hash_ref,
            "changeset",
            2,
            b"corrupt".to_vec(),
            hex::encode(hash32(b"corrupt")),
            vec!["log_index:00000000000000000002".to_string()],
            "corrupt-changeset".to_string(),
        )
        .await
        .unwrap();
        assert!(
            read_personaldb_changeset_payload_by_hash(&storage, 9, "db-alpha", payload_hash)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn commit_certificate_round_trips_through_corestore_ref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();
        let certificate = sample_certificate().seal(&keyring).await.unwrap();

        let ref_name = write_personaldb_commit_certificate(
            &storage,
            9,
            "db-alpha",
            &certificate,
            keyring.trust_store(),
        )
        .await
        .unwrap();

        assert!(ref_name.starts_with(
            "personaldb_commit_certificate:tenant:9:database:db-alpha:log:00000000000000000042:"
        ));

        let read = read_personaldb_commit_certificate(
            &storage,
            9,
            "db-alpha",
            42,
            &certificate.entry_hash,
            keyring.trust_store(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read, certificate);
        assert_eq!(
            read.witness_signature.unwrap().signature.as_bytes().len(),
            64
        );
    }

    #[tokio::test]
    async fn commit_certificate_tamper_and_scope_mismatch_are_rejected() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let keyring = personaldb_protocol_keyring();
        let certificate = sample_certificate().seal(&keyring).await.unwrap();
        let ref_name = write_personaldb_commit_certificate(
            &storage,
            9,
            "db-alpha",
            &certificate,
            keyring.trust_store(),
        )
        .await
        .unwrap();

        let row = read_personaldb_data_locator_row(&storage, 9, "db-alpha", &ref_name)
            .await
            .unwrap()
            .expect("certificate locator exists");
        let mut value = read_personaldb_data_locator_bytes(&storage, &row)
            .await
            .unwrap();
        *value
            .last_mut()
            .expect("stored commit certificate bytes are not empty") ^= 0x01;
        write_personaldb_bytes_as_data_locator(
            &storage,
            9,
            "db-alpha",
            &ref_name,
            "commit_certificate",
            certificate.log_index + 1,
            value,
            personaldb_payload_hash(certificate.entry_hash.as_bytes()),
            vec![format!("entry_hash:{}", certificate.entry_hash)],
            "corrupt-certificate".to_string(),
        )
        .await
        .unwrap();
        assert!(
            read_personaldb_commit_certificate(
                &storage,
                9,
                "db-alpha",
                42,
                &certificate.entry_hash,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );

        let wrong_scope = PersonalDbCommitCertificate {
            database_id: "db-beta".to_string(),
            ..sample_certificate()
        }
        .seal(&keyring)
        .await
        .unwrap();
        assert!(
            write_personaldb_commit_certificate(
                &storage,
                9,
                "db-alpha",
                &wrong_scope,
                keyring.trust_store(),
            )
            .await
            .is_err()
        );
    }

    fn sample_certificate() -> PersonalDbCommitCertificate {
        PersonalDbCommitCertificate {
            format_version: 2,
            tenant_id: "9".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 42,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode(hash32(b"sqlite changeset bytes")),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 2,
            policy_epoch: 3,
            leader_replica_id: "leader-a".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 5,
            witness_node_id: "node-a".to_string(),
            witnessed_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
