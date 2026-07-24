use crate::core_store::{
    CF_AUTHZ, CoreMetaTuplePart, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
    TABLE_AUTHZ_HEAD_ROW, core_meta_committed_row_common, core_meta_payload_digest,
    core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
    encode_deterministic_proto, sha256_hex,
};
use crate::storage::Storage;
use anyhow::{Context, Result, bail};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Weak};

const AUTHZ_HEAD_SCHEMA: &str = "anvil.authz.head.v1";
const AUTHZ_HEAD_ROW_KIND: &str = "authz-head";
const ZERO_SHA256: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

static AUTHZ_WRITE_LOCKS: LazyLock<std::sync::Mutex<BTreeMap<i64, Weak<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthzHead {
    pub(crate) tenant_id: i64,
    pub(crate) committed_revision: u64,
    pub(crate) tuple_revision: u64,
    pub(crate) schema_revision: u64,
    pub(crate) derived_through_revision: u64,
    pub(crate) tuple_stream_head_hash: String,
    pub(crate) tuple_fence_token: u64,
    pub(crate) active_schema_bindings_hash: String,
    pub(crate) updated_at_unix_nanos: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthzHeadSnapshot {
    pub(crate) head: AuthzHead,
    expected_payload_hash: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum AuthzHeadMutation<'a> {
    TupleBatch {
        journal_payload: &'a [u8],
        fence_token: u64,
    },
    SchemaRevision,
    SchemaBinding {
        realm_id: &'a str,
        schema_id: &'a str,
        schema_revision: u64,
        schema_digest: &'a str,
        binding_generation: u64,
    },
}

#[derive(Clone, PartialEq, Message)]
struct AuthzHeadRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(uint64, tag = "4")]
    committed_revision: u64,
    #[prost(uint64, tag = "5")]
    tuple_revision: u64,
    #[prost(uint64, tag = "6")]
    schema_revision: u64,
    #[prost(uint64, tag = "7")]
    derived_through_revision: u64,
    #[prost(string, tag = "8")]
    tuple_stream_head_hash: String,
    #[prost(string, tag = "9")]
    active_schema_bindings_hash: String,
    #[prost(uint64, tag = "10")]
    updated_at_unix_nanos: u64,
    #[prost(uint64, tag = "11")]
    tuple_fence_token: u64,
}

pub(crate) fn tenant_write_lock(tenant_id: i64) -> Result<Arc<tokio::sync::Mutex<()>>> {
    validate_tenant_id(tenant_id)?;
    let mut locks = AUTHZ_WRITE_LOCKS
        .lock()
        .map_err(|_| anyhow::anyhow!("authorization write lock is poisoned"))?;
    if let Some(lock) = locks.get(&tenant_id).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(tenant_id, Arc::downgrade(&lock));
    Ok(lock)
}

pub(crate) async fn read(storage: &Storage, tenant_id: i64) -> Result<AuthzHeadSnapshot> {
    validate_tenant_id(tenant_id)?;
    let tuple_key = tuple_key(tenant_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let payload = store.read_coremeta_row(CF_AUTHZ, TABLE_AUTHZ_HEAD_ROW, &tuple_key)?;
    match payload {
        Some(payload) => Ok(AuthzHeadSnapshot {
            head: decode(&payload, tenant_id)?,
            expected_payload_hash: Some(core_meta_payload_digest(TABLE_AUTHZ_HEAD_ROW, &payload)),
        }),
        None => Ok(AuthzHeadSnapshot {
            head: initial(tenant_id),
            expected_payload_hash: None,
        }),
    }
}

pub(crate) fn advance(
    snapshot: &AuthzHeadSnapshot,
    transaction_id: &str,
    mutation: AuthzHeadMutation<'_>,
) -> Result<AuthzHead> {
    let revision = snapshot
        .head
        .committed_revision
        .checked_add(1)
        .context("authorization revision overflow")?;
    let mut head = snapshot.head.clone();
    head.committed_revision = revision;
    match mutation {
        AuthzHeadMutation::TupleBatch {
            journal_payload,
            fence_token,
        } => {
            head.tuple_revision = revision;
            head.tuple_stream_head_hash = canonical_sha256(journal_payload);
            head.tuple_fence_token = fence_token;
        }
        AuthzHeadMutation::SchemaRevision => {
            head.schema_revision = revision;
        }
        AuthzHeadMutation::SchemaBinding {
            realm_id,
            schema_id,
            schema_revision,
            schema_digest,
            binding_generation,
        } => {
            head.schema_revision = revision;
            head.active_schema_bindings_hash = next_binding_state_hash(
                &head.active_schema_bindings_hash,
                realm_id,
                schema_id,
                schema_revision,
                schema_digest,
                binding_generation,
            );
        }
    }
    head.updated_at_unix_nanos = current_unix_nanos()?;
    validate(&head)?;
    if transaction_id.is_empty() {
        bail!("authorization head transaction id must not be empty");
    }
    Ok(head)
}

pub(crate) fn precondition(snapshot: &AuthzHeadSnapshot) -> Result<CoreMutationPrecondition> {
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_AUTHZ.to_string(),
        table_id: TABLE_AUTHZ_HEAD_ROW,
        tuple_key: tuple_key(snapshot.head.tenant_id)?,
        expected_payload_hash: snapshot.expected_payload_hash.clone(),
        require_absent: snapshot.expected_payload_hash.is_none(),
        require_present: snapshot.expected_payload_hash.is_some(),
    })
}

pub(crate) fn put_operation(
    partition_id: &str,
    transaction_id: &str,
    head: &AuthzHead,
) -> Result<CoreMutationOperation> {
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: CF_AUTHZ.to_string(),
        table_id: TABLE_AUTHZ_HEAD_ROW,
        tuple_key: tuple_key(head.tenant_id)?,
        payload: encode(head, transaction_id)?,
    })
}

pub(crate) fn transaction_partition(tenant_id: i64) -> String {
    hex::encode(crate::authz_journal::authz_partition_id(tenant_id))
}

pub(crate) fn transaction_principal(tenant_id: i64) -> String {
    format!("partition-owner:authz_tuple:{tenant_id}")
}

pub(crate) fn root_anchor_key(tenant_id: i64) -> String {
    format!("authz/{tenant_id}")
}

fn initial(tenant_id: i64) -> AuthzHead {
    AuthzHead {
        tenant_id,
        committed_revision: 0,
        tuple_revision: 0,
        schema_revision: 0,
        derived_through_revision: 0,
        tuple_stream_head_hash: ZERO_SHA256.to_string(),
        tuple_fence_token: 0,
        active_schema_bindings_hash: ZERO_SHA256.to_string(),
        updated_at_unix_nanos: 0,
    }
}

fn encode(head: &AuthzHead, transaction_id: &str) -> Result<Vec<u8>> {
    validate(head)?;
    Ok(encode_deterministic_proto(&AuthzHeadRowProto {
        common: Some(core_meta_committed_row_common(
            format!("tenant/{}/authz", head.tenant_id),
            core_meta_root_key_hash(&root_anchor_key(head.tenant_id)),
            head.committed_revision,
            transaction_id,
            head.updated_at_unix_nanos,
        )),
        schema: AUTHZ_HEAD_SCHEMA.to_string(),
        tenant_id: head.tenant_id,
        committed_revision: head.committed_revision,
        tuple_revision: head.tuple_revision,
        schema_revision: head.schema_revision,
        derived_through_revision: head.derived_through_revision,
        tuple_stream_head_hash: head.tuple_stream_head_hash.clone(),
        tuple_fence_token: head.tuple_fence_token,
        active_schema_bindings_hash: head.active_schema_bindings_hash.clone(),
        updated_at_unix_nanos: head.updated_at_unix_nanos,
    }))
}

fn decode(payload: &[u8], expected_tenant_id: i64) -> Result<AuthzHead> {
    let proto = decode_deterministic_proto::<AuthzHeadRowProto>(payload, "authorization head")?;
    if proto.schema != AUTHZ_HEAD_SCHEMA {
        bail!("authorization head schema mismatch");
    }
    let common = proto
        .common
        .as_ref()
        .context("authorization head is missing CoreMeta common")?;
    let head = AuthzHead {
        tenant_id: proto.tenant_id,
        committed_revision: proto.committed_revision,
        tuple_revision: proto.tuple_revision,
        schema_revision: proto.schema_revision,
        derived_through_revision: proto.derived_through_revision,
        tuple_stream_head_hash: proto.tuple_stream_head_hash,
        tuple_fence_token: proto.tuple_fence_token,
        active_schema_bindings_hash: proto.active_schema_bindings_hash,
        updated_at_unix_nanos: proto.updated_at_unix_nanos,
    };
    validate(&head)?;
    if head.tenant_id != expected_tenant_id
        || common.realm_id != format!("tenant/{expected_tenant_id}/authz")
        || common.root_key_hash != core_meta_root_key_hash(&root_anchor_key(expected_tenant_id))
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.visibility_state_enum() != crate::core_store::CoreMetaVisibilityState::Committed
        || common.created_at_unix_nanos != head.updated_at_unix_nanos
    {
        bail!("authorization head scope metadata mismatch");
    }
    Ok(head)
}

fn validate(head: &AuthzHead) -> Result<()> {
    validate_tenant_id(head.tenant_id)?;
    if head.tuple_revision > head.committed_revision
        || head.schema_revision > head.committed_revision
        || head.derived_through_revision > head.committed_revision
    {
        bail!("authorization head revision watermarks are invalid");
    }
    validate_sha256(&head.tuple_stream_head_hash, "tuple stream head hash")?;
    validate_sha256(
        &head.active_schema_bindings_hash,
        "active schema bindings hash",
    )?;
    Ok(())
}

fn validate_tenant_id(tenant_id: i64) -> Result<()> {
    if tenant_id < 0 {
        bail!("authorization tenant id must be nonnegative");
    }
    Ok(())
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        bail!("authorization {label} must be a canonical sha256 hash");
    };
    if hex.len() != 64
        || !hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        bail!("authorization {label} must be a canonical sha256 hash");
    }
    Ok(())
}

fn tuple_key(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_HEAD_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn canonical_sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(bytes))
}

fn next_binding_state_hash(
    previous_hash: &str,
    realm_id: &str,
    schema_id: &str,
    schema_revision: u64,
    schema_digest: &str,
    binding_generation: u64,
) -> String {
    let mut bytes = Vec::new();
    for value in [previous_hash, realm_id, schema_id, schema_digest] {
        bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes.extend_from_slice(&schema_revision.to_be_bytes());
    bytes.extend_from_slice(&binding_generation.to_be_bytes());
    canonical_sha256(&bytes)
}

fn current_unix_nanos() -> Result<u64> {
    Ok(u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_nanos(),
    )
    .context("authorization head timestamp exceeds u64")?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authz_head_round_trip_preserves_point_state() {
        let snapshot = AuthzHeadSnapshot {
            head: initial(42),
            expected_payload_hash: None,
        };
        let head = advance(
            &snapshot,
            "tx-1",
            AuthzHeadMutation::TupleBatch {
                journal_payload: b"tuple batch",
                fence_token: 17,
            },
        )
        .unwrap();
        let payload = encode(&head, "tx-1").unwrap();
        assert_eq!(decode(&payload, 42).unwrap(), head);
        assert_eq!(head.committed_revision, 1);
        assert_eq!(head.tuple_revision, 1);
        assert_eq!(head.tuple_fence_token, 17);
        assert_eq!(head.schema_revision, 0);
    }

    #[test]
    fn binding_updates_form_a_deterministic_state_commitment() {
        let snapshot = AuthzHeadSnapshot {
            head: initial(7),
            expected_payload_hash: None,
        };
        let mutation = AuthzHeadMutation::SchemaBinding {
            realm_id: "default",
            schema_id: "main",
            schema_revision: 3,
            schema_digest: "blake3:0123456789abcdef",
            binding_generation: 2,
        };
        let left = advance(&snapshot, "tx", mutation).unwrap();
        let right = advance(&snapshot, "tx", mutation).unwrap();
        assert_eq!(
            left.active_schema_bindings_hash,
            right.active_schema_bindings_hash
        );
        assert_ne!(left.active_schema_bindings_hash, ZERO_SHA256);
    }

    #[test]
    fn authz_head_accepts_independent_physical_root_generation() {
        let snapshot = AuthzHeadSnapshot {
            head: initial(42),
            expected_payload_hash: None,
        };
        let head = advance(
            &snapshot,
            "tx-physical-generation",
            AuthzHeadMutation::TupleBatch {
                journal_payload: b"tuple batch",
                fence_token: 17,
            },
        )
        .unwrap();
        let payload = encode(&head, "tx-physical-generation").unwrap();
        let mut common = crate::core_store::core_meta_row_common_from_payload(&payload).unwrap();
        common.root_generation = 91;
        let rebound = crate::core_store::replace_core_meta_row_common(&payload, &common).unwrap();

        assert_eq!(decode(&rebound, 42).unwrap(), head);
        assert_ne!(common.root_generation, head.committed_revision);
    }
}
