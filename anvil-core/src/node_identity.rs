use anyhow::{Context, Result, bail};
use prost::Message;

use crate::core_store::{
    CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRootPublication, CoreMetaStore,
    CoreMetaTuplePart, CorePipelineKeyring, CoreStore, CoreStoreNodeIdentity,
    TABLE_LOCAL_NODE_IDENTITY_ROW, core_meta_committed_row_common, core_meta_root_key_hash,
    core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
};
use crate::storage::Storage;

const NODE_ID_PREFIX: &str = "node_";
const NODE_IDENTITY_SCHEMA: &str = "anvil.coremeta.local_node_identity.v1";
const NODE_IDENTITY_ROOT: &str = "mesh/node-identity/local";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIdentity {
    pub node_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct NodeIdentityProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    node_id: String,
}

pub async fn load_or_create_node_identity(
    storage_path: impl AsRef<std::path::Path>,
) -> Result<NodeIdentity> {
    load_or_create_node_identity_with_node_id(storage_path, None).await
}

pub async fn load_or_create_node_identity_with_node_id(
    storage_path: impl AsRef<std::path::Path>,
    requested_node_id: Option<&str>,
) -> Result<NodeIdentity> {
    load_or_create_node_identity_inner(storage_path, requested_node_id, None).await
}

pub(crate) async fn load_or_create_node_identity_with_core_store_configuration(
    storage_path: impl AsRef<std::path::Path>,
    requested_node_id: Option<&str>,
    pipeline_keyring: CorePipelineKeyring,
    node_identity: CoreStoreNodeIdentity,
) -> Result<NodeIdentity> {
    load_or_create_node_identity_inner(
        storage_path,
        requested_node_id,
        Some((pipeline_keyring, node_identity)),
    )
    .await
}

async fn load_or_create_node_identity_inner(
    storage_path: impl AsRef<std::path::Path>,
    requested_node_id: Option<&str>,
    core_store_configuration: Option<(CorePipelineKeyring, CoreStoreNodeIdentity)>,
) -> Result<NodeIdentity> {
    let storage = Storage::new_at(storage_path).await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let key = node_identity_key()?;

    // Process identity is needed before CoreStore can initialise receipt
    // verification, so this node-private bootstrap read intentionally bypasses
    // publication visibility.
    if let Some(bytes) = meta.get(CF_MESH, TABLE_LOCAL_NODE_IDENTITY_ROW, &key)? {
        let identity = decode_node_identity(&bytes)?;
        ensure_requested_identity_matches(&identity, requested_node_id)?;
        return Ok(identity);
    }

    let node_id = requested_node_id.map_or_else(generate_node_id, str::to_owned);
    validate_node_id(&node_id)?;
    let record = NodeIdentityProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(NODE_IDENTITY_ROOT),
            1,
            node_id.clone(),
            0,
        )),
        schema: NODE_IDENTITY_SCHEMA.to_string(),
        node_id,
    };
    let payload = encode_deterministic_proto(&record);
    let op = CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_LOCAL_NODE_IDENTITY_ROW,
        tuple_key: &key,
        common: record.common.clone(),
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    let core_store =
        if let Some((pipeline_keyring, mut core_store_identity)) = core_store_configuration {
            core_store_identity.node_id.clone_from(&record.node_id);
            CoreStore::new_with_pipeline_keyring_and_identity(
                storage.clone(),
                pipeline_keyring,
                core_store_identity,
                crate::core_store::CoreStoreStartupRecovery::Immediate,
            )
            .await?
        } else {
            CoreStore::new(storage.clone()).await?
        };
    core_store
        .commit_coremeta_root_groups(
            &record.node_id,
            &[op],
            &[CoreMetaRootPublication::new(
                NODE_IDENTITY_ROOT,
                crate::formats::writer::WriterFamily::MeshControl,
            )],
        )
        .await?;

    let stored = meta
        .get(CF_MESH, TABLE_LOCAL_NODE_IDENTITY_ROW, &key)?
        .context("CoreStore node identity row was not readable after write")?;
    decode_node_identity(&stored)
}

fn node_identity_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("node-identity"),
        CoreMetaTuplePart::Utf8("local"),
    ])
}

fn decode_node_identity(bytes: &[u8]) -> Result<NodeIdentity> {
    let record = decode_deterministic_proto::<NodeIdentityProto>(bytes, "node identity record")?;
    if record.schema != NODE_IDENTITY_SCHEMA {
        bail!("CoreStore node identity row has invalid schema");
    }
    record
        .common
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("CoreStore node identity row missing CoreMeta common"))?;
    validate_node_id(&record.node_id)?;
    Ok(NodeIdentity {
        node_id: record.node_id,
    })
}

fn ensure_requested_identity_matches(
    identity: &NodeIdentity,
    requested_node_id: Option<&str>,
) -> Result<()> {
    if let Some(requested_node_id) = requested_node_id
        && identity.node_id != requested_node_id
    {
        bail!(
            "configured node id {requested_node_id} does not match persisted node id {}",
            identity.node_id
        );
    }
    Ok(())
}

fn generate_node_id() -> String {
    format!("{NODE_ID_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

fn validate_node_id(node_id: &str) -> Result<()> {
    if node_id.is_empty() {
        bail!("CoreStore node identity is empty");
    }
    if node_id
        .chars()
        .any(|ch| ch == '/' || ch == '\0' || ch.is_control())
    {
        bail!("CoreStore node identity contains an invalid character");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn node_identity_is_created_in_coremeta_and_reloaded() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");

        let first = load_or_create_node_identity(&storage_path).await.unwrap();
        let second = load_or_create_node_identity(&storage_path).await.unwrap();

        assert!(
            storage_path
                .join("corestore")
                .join("meta")
                .join("rocksdb")
                .exists()
        );
        assert!(first.node_id.starts_with(NODE_ID_PREFIX));
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn configured_node_identity_must_match_the_persisted_identity() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");

        let identity = load_or_create_node_identity_with_node_id(&storage_path, Some("node-a"))
            .await
            .unwrap();
        assert_eq!(identity.node_id, "node-a");

        let error = load_or_create_node_identity_with_node_id(&storage_path, Some("node-b"))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match persisted node id")
        );
    }
}
