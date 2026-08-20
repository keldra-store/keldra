use keldra_api::v1::PersonalDbGroupKind as ApiGroupKind;
use keldra_consensus::NodeId;
use keldra_store::ObjectKey;
use personaldb_protocol::{DatabaseGroupKind, DatabaseId, Sha256Digest};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::authentication::Caller;
use crate::distributed_list::OriginalBearer;

pub(super) const STORAGE_FORMAT_VERSION: u32 = 1;
pub(super) const TRUST_BUNDLE_VERSION: u64 = 1;
pub(super) const MAX_ID_BYTES: usize = 128;
pub(super) const MAX_COMMAND_ID_BYTES: usize = 256;
pub(crate) const MANIFEST_ROOT_PREFIX: &str = "_keldra/personaldb/v1/";

#[derive(Clone)]
pub(super) struct GroupScope {
    pub(super) tenant: String,
    pub(super) bucket: String,
    pub(super) tenant_id: u64,
    pub(super) bucket_id: u64,
    pub(super) database_id: DatabaseId,
    pub(super) group_id: String,
    pub(super) caller: Caller,
    pub(super) bearer: OriginalBearer,
}

impl GroupScope {
    pub(super) fn placement_key(&self) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(18 + self.database_id.0.len().saturating_add(self.group_id.len()));
        key.push(STORAGE_FORMAT_VERSION as u8);
        key.extend_from_slice(&self.tenant_id.to_be_bytes());
        key.extend_from_slice(&self.bucket_id.to_be_bytes());
        key.extend_from_slice(self.database_id.0.as_bytes());
        key.push(0);
        key.extend_from_slice(self.group_id.as_bytes());
        key
    }

    pub(super) fn root_path(&self) -> String {
        format!(
            "_keldra/personaldb/v1/{}/{}",
            hex::encode(self.database_id.0.as_bytes()),
            hex::encode(self.group_id.as_bytes())
        )
    }

    pub(super) fn virtual_key(&self) -> Result<ObjectKey, Status> {
        ObjectKey::new(
            &self.tenant,
            &self.bucket,
            format!(
                "personaldb/{}/{}",
                hex::encode(self.database_id.0.as_bytes()),
                hex::encode(self.group_id.as_bytes())
            ),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))
    }

    pub(super) fn storage_key(&self, suffix: &str) -> Result<ObjectKey, Status> {
        ObjectKey::new(
            &self.tenant,
            &self.bucket,
            format!("{}/{}", self.root_path(), suffix),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GroupManifest {
    pub(super) storage_format_version: u32,
    pub(super) database_id: String,
    pub(super) group_id: String,
    pub(super) group_kind: StoredGroupKind,
    pub(super) schema_hash_sha256: [u8; 32],
    pub(super) projection_definition_hash_sha256: Option<[u8; 32]>,
    pub(super) trust_bundle_version: u64,
}

impl GroupManifest {
    pub(super) fn kind(&self) -> DatabaseGroupKind {
        self.group_kind.into()
    }

    pub(super) fn schema_hash(&self) -> Sha256Digest {
        Sha256Digest::from_bytes(self.schema_hash_sha256)
    }

    pub(super) fn projection_hash(&self) -> Option<Sha256Digest> {
        self.projection_definition_hash_sha256
            .map(Sha256Digest::from_bytes)
    }

    pub(super) fn validate_for(&self, scope: &GroupScope) -> Result<(), Status> {
        if self.storage_format_version != STORAGE_FORMAT_VERSION
            || self.database_id != scope.database_id.0
            || self.group_id != scope.group_id
            || self.trust_bundle_version != TRUST_BUNDLE_VERSION
        {
            return Err(Status::data_loss(
                "PersonalDB group manifest does not match its addressed group",
            ));
        }
        if self.kind() == DatabaseGroupKind::Projection
            && self.projection_definition_hash_sha256.is_none()
        {
            return Err(Status::data_loss(
                "PersonalDB projection manifest has no definition hash",
            ));
        }
        if self.kind() != DatabaseGroupKind::Projection
            && self.projection_definition_hash_sha256.is_some()
        {
            return Err(Status::data_loss(
                "non-projection PersonalDB manifest has a projection hash",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum StoredGroupKind {
    Source,
    Projection,
    Standalone,
}

impl From<StoredGroupKind> for DatabaseGroupKind {
    fn from(value: StoredGroupKind) -> Self {
        match value {
            StoredGroupKind::Source => Self::Source,
            StoredGroupKind::Projection => Self::Projection,
            StoredGroupKind::Standalone => Self::Standalone,
        }
    }
}

pub(super) fn parse_kind(value: i32) -> Result<StoredGroupKind, Status> {
    match ApiGroupKind::try_from(value)
        .map_err(|_| Status::invalid_argument("PersonalDB group kind is not recognized"))?
    {
        ApiGroupKind::Source => Ok(StoredGroupKind::Source),
        ApiGroupKind::Projection => Ok(StoredGroupKind::Projection),
        ApiGroupKind::Standalone => Ok(StoredGroupKind::Standalone),
        ApiGroupKind::Unspecified => Err(Status::invalid_argument(
            "PersonalDB group kind must be specified",
        )),
    }
}

pub(super) fn parse_scope_ids(
    bucket: &str,
    database_id: &str,
    group_id: &str,
) -> Result<(String, DatabaseId, String), Status> {
    validate_id("bucket", bucket)?;
    validate_id("database_id", database_id)?;
    validate_id("group_id", group_id)?;
    Ok((
        bucket.to_owned(),
        DatabaseId::new(database_id),
        group_id.to_owned(),
    ))
}

pub(super) fn validate_id(name: &'static str, value: &str) -> Result<(), Status> {
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || !value.is_ascii()
        || value.as_bytes().contains(&0)
    {
        return Err(Status::invalid_argument(format!(
            "{name} must be 1..={MAX_ID_BYTES} non-NUL ASCII bytes"
        )));
    }
    Ok(())
}

pub(super) fn validate_command_id(value: &str) -> Result<(), Status> {
    if value.is_empty() || value.len() > MAX_COMMAND_ID_BYTES || value.as_bytes().contains(&0) {
        return Err(Status::invalid_argument(format!(
            "command_id must be 1..={MAX_COMMAND_ID_BYTES} non-NUL bytes"
        )));
    }
    Ok(())
}

pub(super) fn storage_command_id(scope: &GroupScope, command_id: &str, operation: &str) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("keldra.personaldb/storage-command/v1");
    hasher.update(&scope.placement_key());
    hasher.update(command_id.as_bytes());
    hasher.update(operation.as_bytes());
    format!("pdb:{}", hasher.finalize().to_hex())
}

pub(super) fn digest(name: &'static str, value: &[u8]) -> Result<Sha256Digest, Status> {
    Sha256Digest::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("{name} must contain exactly 32 bytes")))
}

pub(super) fn primary_server_id(node: NodeId) -> String {
    format!("keldra-node-{}", node.0)
}

pub(super) fn manifest_path() -> &'static str {
    "manifest.json"
}

pub(super) fn projection_definition_path() -> &'static str {
    "projection/definition.pdb"
}

pub(super) fn head_path() -> &'static str {
    "heads/committed.pdb"
}

pub(super) fn entry_payload_path(index: u64) -> String {
    format!("log/payloads/{index:020}.changeset")
}

pub(super) fn entry_certificate_path(index: u64) -> String {
    format!("log/certificates/{index:020}.pdb")
}

pub(super) fn snapshot_manifest_path(snapshot_id: &str) -> String {
    format!("snapshots/manifests/{}.pdb", hex::encode(snapshot_id))
}

pub(super) fn snapshot_bytes_path(snapshot_id: &str) -> String {
    format!("snapshots/objects/{}.sqlite.zst", hex::encode(snapshot_id))
}

pub(crate) fn parse_manifest_object_path(path: &str) -> Result<(DatabaseId, String), Status> {
    let remainder = path
        .strip_prefix(MANIFEST_ROOT_PREFIX)
        .and_then(|path| path.strip_suffix("/manifest.json"))
        .ok_or_else(|| Status::data_loss("PersonalDB manifest path is malformed"))?;
    let (database_hex, group_hex) = remainder
        .split_once('/')
        .filter(|(_, group)| !group.contains('/'))
        .ok_or_else(|| Status::data_loss("PersonalDB manifest path is malformed"))?;
    let database_id = decode_path_id("database_id", database_hex)?;
    let group_id = decode_path_id("group_id", group_hex)?;
    Ok((DatabaseId::new(database_id), group_id))
}

fn decode_path_id(name: &'static str, encoded: &str) -> Result<String, Status> {
    let bytes = hex::decode(encoded)
        .map_err(|_| Status::data_loss("PersonalDB manifest path has invalid hexadecimal IDs"))?;
    let value = String::from_utf8(bytes)
        .map_err(|_| Status::data_loss("PersonalDB manifest path IDs are not UTF-8"))?;
    validate_id(name, &value)
        .map_err(|_| Status::data_loss("PersonalDB manifest path ID is invalid"))?;
    Ok(value)
}

pub(super) fn protocol_status(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(format!("invalid canonical PersonalDB value: {error}"))
}
