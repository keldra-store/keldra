use std::io::{self, Read};
use std::sync::Arc;

use rocksdb::{DB, WriteBatch};

use super::*;

const ARTIFACT_MANIFEST_FORMAT: u8 = 1;
const ARTIFACT_INSTALL_FORMAT: u8 = 1;
const COMPLETE_IDENTITY_KIND: u8 = 1;
const SHARD_IDENTITY_KIND: u8 = 2;
const UPLOAD_IDENTITY_KIND: u8 = 3;
const INLINE_LAYOUT: u8 = 1;
const CHUNKED_LAYOUT: u8 = 2;
const COMPLETE_INLINE_TAG: u8 = 1;
const COMPLETE_CHUNK_TAG: u8 = 2;
const SHARD_INLINE_TAG: u8 = 3;
const SHARD_CHUNK_TAG: u8 = 4;
const MANIFEST_KEY_TAG: u8 = 1;
const INSTALL_KEY_TAG: u8 = 1;
const MANIFEST_BYTES: usize = 1 + 1 + 1 + 8 + 4 + 4 + 32 + 32;
const INSTALL_BYTES: usize = MANIFEST_BYTES + 4 + 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ArtifactKind {
    Complete,
    Shard,
    Upload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ArtifactLayout {
    Inline,
    Chunked { chunk_count: u32 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ArtifactManifest {
    pub kind: ArtifactKind,
    pub encoded_length: u64,
    pub layout: ArtifactLayout,
    pub integrity: [u8; 32],
    pub storage_id: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArtifactInstall {
    manifest: ArtifactManifest,
    next_ordinal: u32,
    updated_at: u64,
}

impl ArtifactManifest {
    pub(super) fn complete(reference: &BlobRef) -> Result<Self, MutationError> {
        Self::complete_at(
            reference,
            artifact_storage_id(&complete_identity(reference)),
        )
    }

    pub(super) fn complete_at(
        reference: &BlobRef,
        storage_id: [u8; 32],
    ) -> Result<Self, MutationError> {
        Ok(Self {
            kind: ArtifactKind::Complete,
            encoded_length: reference.length,
            layout: layout_for_length(reference.length)?,
            integrity: reference.hash,
            storage_id,
        })
    }

    pub(super) fn uploaded_complete(
        reference: &BlobRef,
        storage_id: [u8; 32],
    ) -> Result<Self, MutationError> {
        Ok(Self {
            kind: ArtifactKind::Complete,
            encoded_length: reference.length,
            layout: ArtifactLayout::Chunked {
                chunk_count: chunk_count_for_length(reference.length)?,
            },
            integrity: reference.hash,
            storage_id,
        })
    }

    pub(super) fn upload(storage_id: [u8; 32]) -> Self {
        Self {
            kind: ArtifactKind::Upload,
            encoded_length: u64::MAX,
            layout: ArtifactLayout::Chunked {
                chunk_count: u32::MAX,
            },
            integrity: storage_id,
            storage_id,
        }
    }

    pub(super) fn shard(
        identity: &ShardIdentity,
        encoded_length: u64,
    ) -> Result<Self, MutationError> {
        Ok(Self {
            kind: ArtifactKind::Shard,
            encoded_length,
            layout: layout_for_length(encoded_length)?,
            integrity: *blake3::hash(&identity.encode()).as_bytes(),
            storage_id: artifact_storage_id(&identity.encode()),
        })
    }

    fn encode(&self) -> [u8; MANIFEST_BYTES] {
        let mut encoded = [0_u8; MANIFEST_BYTES];
        encoded[0] = ARTIFACT_MANIFEST_FORMAT;
        encoded[1] = match self.kind {
            ArtifactKind::Complete => COMPLETE_IDENTITY_KIND,
            ArtifactKind::Shard => SHARD_IDENTITY_KIND,
            ArtifactKind::Upload => UPLOAD_IDENTITY_KIND,
        };
        let (layout, chunk_count) = match self.layout {
            ArtifactLayout::Inline => (INLINE_LAYOUT, 0),
            ArtifactLayout::Chunked { chunk_count } => (CHUNKED_LAYOUT, chunk_count),
        };
        encoded[2] = layout;
        encoded[3..11].copy_from_slice(&self.encoded_length.to_be_bytes());
        encoded[11..15].copy_from_slice(&(PAYLOAD_ARTIFACT_CHUNK_BYTES as u32).to_be_bytes());
        encoded[15..19].copy_from_slice(&chunk_count.to_be_bytes());
        encoded[19..51].copy_from_slice(&self.integrity);
        encoded[51..].copy_from_slice(&self.storage_id);
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, MutationError> {
        if encoded.len() != MANIFEST_BYTES || encoded[0] != ARTIFACT_MANIFEST_FORMAT {
            return Err(artifact_storage("payload artifact manifest is malformed"));
        }
        let kind = match encoded[1] {
            COMPLETE_IDENTITY_KIND => ArtifactKind::Complete,
            SHARD_IDENTITY_KIND => ArtifactKind::Shard,
            UPLOAD_IDENTITY_KIND => ArtifactKind::Upload,
            _ => {
                return Err(artifact_storage(
                    "payload artifact manifest kind is invalid",
                ));
            }
        };
        let encoded_length = u64::from_be_bytes(encoded[3..11].try_into().unwrap());
        let chunk_bytes = u32::from_be_bytes(encoded[11..15].try_into().unwrap());
        if chunk_bytes as usize != PAYLOAD_ARTIFACT_CHUNK_BYTES {
            return Err(artifact_storage(
                "payload artifact chunk size is unsupported",
            ));
        }
        let chunk_count = u32::from_be_bytes(encoded[15..19].try_into().unwrap());
        let layout = match encoded[2] {
            INLINE_LAYOUT
                if chunk_count == 0 && encoded_length <= PAYLOAD_ARTIFACT_CHUNK_BYTES as u64 =>
            {
                ArtifactLayout::Inline
            }
            CHUNKED_LAYOUT
                if kind == ArtifactKind::Upload
                    && encoded_length == u64::MAX
                    && chunk_count == u32::MAX =>
            {
                ArtifactLayout::Chunked { chunk_count }
            }
            CHUNKED_LAYOUT if chunk_count == chunk_count_for_length(encoded_length)? => {
                ArtifactLayout::Chunked { chunk_count }
            }
            _ => return Err(artifact_storage("payload artifact layout is inconsistent")),
        };
        let integrity = encoded[19..51].try_into().unwrap();
        let storage_id = encoded[51..].try_into().unwrap();
        if kind == ArtifactKind::Upload && integrity != storage_id {
            return Err(artifact_storage(
                "upload installation storage identity is inconsistent",
            ));
        }
        Ok(Self {
            kind,
            encoded_length,
            layout,
            integrity,
            storage_id,
        })
    }
}

impl ArtifactInstall {
    fn encode(&self) -> [u8; INSTALL_BYTES] {
        let mut encoded = [0_u8; INSTALL_BYTES];
        encoded[..MANIFEST_BYTES].copy_from_slice(&self.manifest.encode());
        encoded[0] = ARTIFACT_INSTALL_FORMAT;
        encoded[MANIFEST_BYTES..MANIFEST_BYTES + 4]
            .copy_from_slice(&self.next_ordinal.to_be_bytes());
        encoded[MANIFEST_BYTES + 4..].copy_from_slice(&self.updated_at.to_be_bytes());
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, MutationError> {
        if encoded.len() != INSTALL_BYTES || encoded[0] != ARTIFACT_INSTALL_FORMAT {
            return Err(artifact_storage(
                "payload artifact installation is malformed",
            ));
        }
        let mut manifest = ArtifactManifest::decode(&encoded[..MANIFEST_BYTES])?;
        // The installation and manifest formats deliberately start at the same
        // version for storage format 0.15.
        manifest.kind = match encoded[1] {
            COMPLETE_IDENTITY_KIND => ArtifactKind::Complete,
            SHARD_IDENTITY_KIND => ArtifactKind::Shard,
            UPLOAD_IDENTITY_KIND => ArtifactKind::Upload,
            _ => {
                return Err(artifact_storage(
                    "payload artifact installation kind is invalid",
                ));
            }
        };
        let next_ordinal = u32::from_be_bytes(
            encoded[MANIFEST_BYTES..MANIFEST_BYTES + 4]
                .try_into()
                .unwrap(),
        );
        let updated_at = u64::from_be_bytes(encoded[MANIFEST_BYTES + 4..].try_into().unwrap());
        let maximum = match manifest.layout {
            ArtifactLayout::Inline => 1,
            ArtifactLayout::Chunked { chunk_count } => chunk_count,
        };
        if next_ordinal > maximum {
            return Err(artifact_storage(
                "payload artifact installation progress is invalid",
            ));
        }
        Ok(Self {
            manifest,
            next_ordinal,
            updated_at,
        })
    }
}

pub(super) fn complete_identity(reference: &BlobRef) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(40);
    encoded.extend_from_slice(&reference.hash);
    encoded.extend_from_slice(&reference.length.to_be_bytes());
    encoded
}

fn artifact_storage_id(identity: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"keldra-payload-storage-id-v1\0");
    hasher.update(identity);
    *hasher.finalize().as_bytes()
}

pub(super) fn is_inline_payload_artifact(reference: &BlobRef) -> bool {
    reference.length <= PAYLOAD_ARTIFACT_CHUNK_BYTES as u64
}

pub(super) fn validate_complete_artifact(
    reference: &BlobRef,
    bytes: &[u8],
) -> Result<(), MutationError> {
    if bytes.len() as u64 != reference.length || blake3::hash(bytes).as_bytes() != &reference.hash {
        return Err(artifact_storage(
            "complete payload failed length or hash verification",
        ));
    }
    Ok(())
}

pub(super) fn complete_inline_key(reference: &BlobRef) -> Vec<u8> {
    tagged_identity(
        COMPLETE_INLINE_TAG,
        &artifact_storage_id(&complete_identity(reference)),
    )
}

#[cfg(test)]
pub(super) fn complete_chunk_key(reference: &BlobRef, ordinal: u32) -> Vec<u8> {
    chunk_key(
        COMPLETE_CHUNK_TAG,
        &artifact_storage_id(&complete_identity(reference)),
        ordinal,
    )
}

#[cfg(test)]
pub(super) fn shard_inline_key(identity: &ShardIdentity) -> Vec<u8> {
    tagged_identity(SHARD_INLINE_TAG, &artifact_storage_id(&identity.encode()))
}

fn tagged_identity(tag: u8, identity: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + identity.len());
    key.push(tag);
    key.extend_from_slice(identity);
    key
}

fn chunk_key(tag: u8, identity: &[u8], ordinal: u32) -> Vec<u8> {
    let mut key = tagged_identity(tag, identity);
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn manifest_key(identity: &[u8]) -> Vec<u8> {
    tagged_identity(MANIFEST_KEY_TAG, identity)
}

fn install_key(identity: &[u8]) -> Vec<u8> {
    tagged_identity(INSTALL_KEY_TAG, identity)
}

fn layout_for_length(length: u64) -> Result<ArtifactLayout, MutationError> {
    if length <= PAYLOAD_ARTIFACT_CHUNK_BYTES as u64 {
        Ok(ArtifactLayout::Inline)
    } else {
        Ok(ArtifactLayout::Chunked {
            chunk_count: chunk_count_for_length(length)?,
        })
    }
}

fn chunk_count_for_length(length: u64) -> Result<u32, MutationError> {
    if length == 0 {
        return Ok(1);
    }
    let chunks = length
        .checked_add(PAYLOAD_ARTIFACT_CHUNK_BYTES as u64 - 1)
        .ok_or_else(|| artifact_storage("payload artifact chunk count overflow"))?
        / PAYLOAD_ARTIFACT_CHUNK_BYTES as u64;
    u32::try_from(chunks).map_err(|_| artifact_storage("payload artifact has too many chunks"))
}

fn artifact_storage(message: impl Into<String>) -> MutationError {
    MutationError::Storage(message.into())
}

impl Store {
    pub(super) fn stage_inline_complete_artifact(
        &self,
        batch: &mut WriteBatch,
        reference: &BlobRef,
        bytes: &[u8],
    ) -> Result<(), MutationError> {
        validate_complete_artifact(reference, bytes)?;
        let manifest = ArtifactManifest::complete(reference)?;
        if manifest.layout != ArtifactLayout::Inline {
            return Err(artifact_storage(
                "inline payload artifact exceeds the chunk threshold",
            ));
        }
        let identity = complete_identity(reference);
        batch.put_cf(
            self.cf(CF_PAYLOAD_ARTIFACTS)?,
            tagged_identity(COMPLETE_INLINE_TAG, &manifest.storage_id),
            bytes,
        );
        batch.put_cf(
            self.cf(CF_PAYLOAD_MANIFESTS)?,
            manifest_key(&identity),
            manifest.encode(),
        );
        tracing::debug!(
            payload.kind = "complete",
            payload.layout = "inline",
            payload.bytes = bytes.len(),
            monotonic_counter.keldra_payload_inline_values_total = 1_u64,
            monotonic_counter.keldra_payload_logical_bytes_total = bytes.len() as u64,
            "staged integrated payload value"
        );
        Ok(())
    }

    pub(super) fn read_complete_manifest(
        &self,
        reference: &BlobRef,
    ) -> Result<Option<ArtifactManifest>, MutationError> {
        let identity = complete_identity(reference);
        let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_PAYLOAD_MANIFESTS)?, manifest_key(&identity))
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let manifest = ArtifactManifest::decode(&encoded)?;
        if manifest.kind != ArtifactKind::Complete
            || manifest.encoded_length != reference.length
            || manifest.integrity != reference.hash
            || manifest.storage_id == [0; 32]
        {
            return Err(artifact_storage(
                "complete payload manifest contradicts its identity",
            ));
        }
        Ok(Some(manifest))
    }

    pub(super) fn read_shard_manifest(
        &self,
        identity: &ShardIdentity,
    ) -> Result<Option<ArtifactManifest>, MutationError> {
        let encoded_identity = identity.encode();
        let Some(encoded) = self
            .db
            .get_cf(
                self.cf(CF_PAYLOAD_MANIFESTS)?,
                manifest_key(&encoded_identity),
            )
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let manifest = ArtifactManifest::decode(&encoded)?;
        if manifest.kind != ArtifactKind::Shard
            || manifest.integrity != *blake3::hash(&encoded_identity).as_bytes()
        {
            return Err(artifact_storage("shard manifest contradicts its identity"));
        }
        Ok(Some(manifest))
    }

    pub(super) fn stage_artifact_delete(
        &self,
        batch: &mut WriteBatch,
        identity: &[u8],
        manifest: &ArtifactManifest,
    ) -> Result<(), MutationError> {
        self.stage_artifact_value_delete(batch, identity, manifest)?;
        batch.delete_cf(self.cf(CF_PAYLOAD_MANIFESTS)?, manifest_key(identity));
        batch.delete_cf(self.cf(CF_PAYLOAD_INSTALLS)?, install_key(identity));
        Ok(())
    }

    fn stage_artifact_value_delete(
        &self,
        batch: &mut WriteBatch,
        _identity: &[u8],
        manifest: &ArtifactManifest,
    ) -> Result<(), MutationError> {
        match (manifest.kind, manifest.layout) {
            (ArtifactKind::Complete, ArtifactLayout::Inline) => {
                batch.delete_cf(
                    self.cf(CF_PAYLOAD_ARTIFACTS)?,
                    tagged_identity(COMPLETE_INLINE_TAG, &manifest.storage_id),
                );
            }
            (ArtifactKind::Complete, ArtifactLayout::Chunked { .. }) => {
                stage_prefix_delete(
                    batch,
                    self.cf(CF_PAYLOAD_ARTIFACTS)?,
                    &tagged_identity(COMPLETE_CHUNK_TAG, &manifest.storage_id),
                )?;
            }
            (ArtifactKind::Shard, ArtifactLayout::Inline) => {
                batch.delete_cf(
                    self.cf(CF_PAYLOAD_ARTIFACTS)?,
                    tagged_identity(SHARD_INLINE_TAG, &manifest.storage_id),
                );
            }
            (ArtifactKind::Shard, ArtifactLayout::Chunked { .. }) => {
                stage_prefix_delete(
                    batch,
                    self.cf(CF_PAYLOAD_ARTIFACTS)?,
                    &tagged_identity(SHARD_CHUNK_TAG, &manifest.storage_id),
                )?;
            }
            (ArtifactKind::Upload, ArtifactLayout::Chunked { .. }) => {
                stage_prefix_delete(
                    batch,
                    self.cf(CF_PAYLOAD_ARTIFACTS)?,
                    &tagged_identity(COMPLETE_CHUNK_TAG, &manifest.storage_id),
                )?;
            }
            (ArtifactKind::Upload, ArtifactLayout::Inline) => {
                return Err(artifact_storage("upload artifact cannot use inline layout"));
            }
        }
        Ok(())
    }

    pub(super) fn reset_artifact_install(
        &self,
        batch: &mut WriteBatch,
        identity: &[u8],
        expected: &ArtifactManifest,
        now: u64,
    ) -> Result<(), MutationError> {
        let encoded = self
            .db
            .get_cf(self.cf(CF_PAYLOAD_INSTALLS)?, install_key(identity))
            .map_err(storage_error)?
            .ok_or_else(|| artifact_storage("payload artifact installation is missing"))?;
        let current = ArtifactInstall::decode(&encoded)?;
        if &current.manifest != expected {
            return Err(artifact_storage(
                "payload artifact installation changed while resetting",
            ));
        }
        self.stage_artifact_value_delete(batch, identity, expected)?;
        batch.delete_cf(self.cf(CF_PAYLOAD_MANIFESTS)?, manifest_key(identity));
        batch.put_cf(
            self.cf(CF_PAYLOAD_INSTALLS)?,
            install_key(identity),
            ArtifactInstall {
                manifest: expected.clone(),
                next_ordinal: 0,
                updated_at: now,
            }
            .encode(),
        );
        Ok(())
    }

    pub(super) fn read_artifact_install_manifest(
        &self,
        identity: &[u8],
    ) -> Result<Option<ArtifactManifest>, MutationError> {
        self.db
            .get_cf(self.cf(CF_PAYLOAD_INSTALLS)?, install_key(identity))
            .map_err(storage_error)?
            .map(|encoded| ArtifactInstall::decode(&encoded).map(|install| install.manifest))
            .transpose()
    }

    pub(super) fn read_artifact_install_state(
        &self,
        identity: &[u8],
    ) -> Result<Option<(ArtifactManifest, u32, u64)>, MutationError> {
        self.db
            .get_cf(self.cf(CF_PAYLOAD_INSTALLS)?, install_key(identity))
            .map_err(storage_error)?
            .map(|encoded| {
                ArtifactInstall::decode(&encoded)
                    .map(|install| (install.manifest, install.next_ordinal, install.updated_at))
            })
            .transpose()
    }

    pub(super) fn begin_artifact_install(
        &self,
        batch: &mut WriteBatch,
        identity: &[u8],
        manifest: ArtifactManifest,
        now: u64,
    ) -> Result<u32, MutationError> {
        if let Some(encoded) = self
            .db
            .get_cf(self.cf(CF_PAYLOAD_INSTALLS)?, install_key(identity))
            .map_err(storage_error)?
        {
            let current = ArtifactInstall::decode(&encoded)?;
            if current.manifest != manifest {
                return Err(artifact_storage(
                    "payload artifact installation contradicts an existing installation",
                ));
            }
            return Ok(current.next_ordinal);
        }
        let install = ArtifactInstall {
            manifest,
            next_ordinal: 0,
            updated_at: now,
        };
        batch.put_cf(
            self.cf(CF_PAYLOAD_INSTALLS)?,
            install_key(identity),
            install.encode(),
        );
        tracing::info!(
            payload.kind = ?install.manifest.kind,
            payload.bytes = install.manifest.encoded_length,
            payload.layout = ?install.manifest.layout,
            monotonic_counter.keldra_payload_installs_started_total = 1_u64,
            "started durable payload artifact installation"
        );
        Ok(0)
    }

    pub(super) fn advance_artifact_install(
        &self,
        identity: &[u8],
        expected: &ArtifactManifest,
        ordinal: u32,
        bytes: &[u8],
        now: u64,
        due_keys: Option<(&[u8], &[u8])>,
    ) -> Result<(), MutationError> {
        let key = install_key(identity);
        let encoded = self
            .db
            .get_cf(self.cf(CF_PAYLOAD_INSTALLS)?, &key)
            .map_err(storage_error)?
            .ok_or_else(|| artifact_storage("payload artifact installation is missing"))?;
        let current = ArtifactInstall::decode(&encoded)?;
        if &current.manifest != expected || current.next_ordinal != ordinal {
            return Err(artifact_storage(
                "payload artifact installation changed while writing",
            ));
        }
        let data_key = match (expected.kind, expected.layout) {
            (ArtifactKind::Complete, ArtifactLayout::Inline) if ordinal == 0 => {
                tagged_identity(COMPLETE_INLINE_TAG, &expected.storage_id)
            }
            (ArtifactKind::Complete, ArtifactLayout::Chunked { chunk_count })
                if ordinal < chunk_count =>
            {
                chunk_key(COMPLETE_CHUNK_TAG, &expected.storage_id, ordinal)
            }
            (ArtifactKind::Shard, ArtifactLayout::Inline) if ordinal == 0 => {
                tagged_identity(SHARD_INLINE_TAG, &expected.storage_id)
            }
            (ArtifactKind::Shard, ArtifactLayout::Chunked { chunk_count })
                if ordinal < chunk_count =>
            {
                chunk_key(SHARD_CHUNK_TAG, &expected.storage_id, ordinal)
            }
            (ArtifactKind::Upload, ArtifactLayout::Chunked { chunk_count })
                if ordinal < chunk_count =>
            {
                chunk_key(COMPLETE_CHUNK_TAG, &expected.storage_id, ordinal)
            }
            _ => {
                return Err(artifact_storage(
                    "payload artifact chunk ordinal is invalid",
                ));
            }
        };
        let next = ArtifactInstall {
            manifest: expected.clone(),
            next_ordinal: ordinal + 1,
            updated_at: now,
        };
        let mut batch = WriteBatch::default();
        batch.put_cf(self.cf(CF_PAYLOAD_ARTIFACTS)?, data_key, bytes);
        batch.put_cf(self.cf(CF_PAYLOAD_INSTALLS)?, &key, next.encode());
        if let Some((previous, replacement)) = due_keys {
            batch.delete_cf(self.cf(CF_BLOB_GC_DUE)?, previous);
            batch.put_cf(self.cf(CF_BLOB_GC_DUE)?, replacement, []);
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        tracing::debug!(
            payload.kind = ?expected.kind,
            payload.chunk_ordinal = ordinal,
            payload.chunk_bytes = bytes.len(),
            monotonic_counter.keldra_payload_chunk_values_total = 1_u64,
            monotonic_counter.keldra_payload_chunk_bytes_total = bytes.len() as u64,
            "persisted payload artifact chunk"
        );
        Ok(())
    }

    pub(super) fn finish_artifact_install(
        &self,
        batch: &mut WriteBatch,
        identity: &[u8],
        expected: &ArtifactManifest,
    ) -> Result<(), MutationError> {
        let key = install_key(identity);
        let encoded = self
            .db
            .get_cf(self.cf(CF_PAYLOAD_INSTALLS)?, &key)
            .map_err(storage_error)?
            .ok_or_else(|| artifact_storage("payload artifact installation is missing"))?;
        let current = ArtifactInstall::decode(&encoded)?;
        let completed = match expected.layout {
            ArtifactLayout::Inline => 1,
            ArtifactLayout::Chunked { chunk_count } => chunk_count,
        };
        if &current.manifest != expected || current.next_ordinal != completed {
            return Err(artifact_storage(
                "payload artifact installation is incomplete",
            ));
        }
        batch.put_cf(
            self.cf(CF_PAYLOAD_MANIFESTS)?,
            manifest_key(identity),
            expected.encode(),
        );
        batch.delete_cf(self.cf(CF_PAYLOAD_INSTALLS)?, key);
        tracing::info!(
            payload.kind = ?expected.kind,
            payload.bytes = expected.encoded_length,
            payload.layout = ?expected.layout,
            monotonic_counter.keldra_payload_installs_sealed_total = 1_u64,
            "sealed durable payload artifact installation"
        );
        Ok(())
    }

    pub(super) fn stage_uploaded_complete_manifest(
        &self,
        batch: &mut WriteBatch,
        upload_id: &[u8; 32],
        reference: &BlobRef,
        manifest: &ArtifactManifest,
    ) -> Result<(), MutationError> {
        let Some((install, next_ordinal, _)) = self.read_artifact_install_state(upload_id)? else {
            return Err(artifact_storage("pending upload installation is missing"));
        };
        if install != ArtifactManifest::upload(*upload_id)
            || manifest.kind != ArtifactKind::Complete
            || manifest.storage_id != *upload_id
            || manifest.encoded_length != reference.length
            || manifest.integrity != reference.hash
            || next_ordinal != chunk_count_for_length(reference.length)?
        {
            return Err(artifact_storage(
                "pending upload cannot be promoted to the complete manifest",
            ));
        }
        batch.put_cf(
            self.cf(CF_PAYLOAD_MANIFESTS)?,
            manifest_key(&complete_identity(reference)),
            manifest.encode(),
        );
        batch.delete_cf(self.cf(CF_PAYLOAD_INSTALLS)?, install_key(upload_id));
        Ok(())
    }
}

fn stage_prefix_delete(
    batch: &mut WriteBatch,
    cf: &rocksdb::ColumnFamily,
    prefix: &[u8],
) -> Result<(), MutationError> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            batch.delete_range_cf(cf, prefix, &end);
            return Ok(());
        }
    }
    Err(artifact_storage(
        "payload artifact key prefix has no successor",
    ))
}

#[derive(Debug)]
pub(crate) struct RocksArtifactReader {
    db: Arc<DB>,
    kind: ArtifactKind,
    manifest: ArtifactManifest,
    position: u64,
    cached_ordinal: Option<u32>,
    cached: Vec<u8>,
}

impl RocksArtifactReader {
    pub(super) fn new(db: Arc<DB>, manifest: ArtifactManifest) -> Self {
        Self {
            db,
            kind: manifest.kind,
            manifest,
            position: 0,
            cached_ordinal: None,
            cached: Vec::new(),
        }
    }

    fn value_key(&self, ordinal: u32) -> Result<Vec<u8>, io::Error> {
        match (self.kind, self.manifest.layout) {
            (ArtifactKind::Complete, ArtifactLayout::Inline) if ordinal == 0 => Ok(
                tagged_identity(COMPLETE_INLINE_TAG, &self.manifest.storage_id),
            ),
            (ArtifactKind::Complete, ArtifactLayout::Chunked { chunk_count })
                if ordinal < chunk_count =>
            {
                Ok(chunk_key(
                    COMPLETE_CHUNK_TAG,
                    &self.manifest.storage_id,
                    ordinal,
                ))
            }
            (ArtifactKind::Shard, ArtifactLayout::Inline) if ordinal == 0 => {
                Ok(tagged_identity(SHARD_INLINE_TAG, &self.manifest.storage_id))
            }
            (ArtifactKind::Shard, ArtifactLayout::Chunked { chunk_count })
                if ordinal < chunk_count =>
            {
                Ok(chunk_key(
                    SHARD_CHUNK_TAG,
                    &self.manifest.storage_id,
                    ordinal,
                ))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload artifact ordinal is outside its manifest",
            )),
        }
    }

    fn read_inner(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.position == self.manifest.encoded_length {
            return Ok(0);
        }
        let ordinal =
            u32::try_from(self.position / PAYLOAD_ARTIFACT_CHUNK_BYTES as u64).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "payload artifact ordinal overflow",
                )
            })?;
        if self.cached_ordinal != Some(ordinal) {
            let key = self.value_key(ordinal)?;
            let cf = self.db.cf_handle(CF_PAYLOAD_ARTIFACTS).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "payload artifact column family is missing",
                )
            })?;
            self.cached = self
                .db
                .get_cf(cf, key)
                .map_err(io::Error::other)?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "payload artifact chunk is missing",
                    )
                })?
                .to_vec();
            let chunk_start = u64::from(ordinal)
                .checked_mul(PAYLOAD_ARTIFACT_CHUNK_BYTES as u64)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "payload artifact chunk offset overflow",
                    )
                })?;
            let expected = usize::try_from(
                (self.manifest.encoded_length - chunk_start)
                    .min(PAYLOAD_ARTIFACT_CHUNK_BYTES as u64),
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "payload artifact chunk length does not fit usize",
                )
            })?;
            if self.cached.len() != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "payload artifact chunk has the wrong encoded length",
                ));
            }
            self.cached_ordinal = Some(ordinal);
        }
        let offset =
            usize::try_from(self.position % PAYLOAD_ARTIFACT_CHUNK_BYTES as u64).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "payload artifact offset overflow",
                )
            })?;
        let remaining =
            usize::try_from(self.manifest.encoded_length - self.position).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "payload artifact length does not fit usize",
                )
            })?;
        let count = output
            .len()
            .min(remaining)
            .min(self.cached.len().saturating_sub(offset));
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "payload artifact chunk is truncated",
            ));
        }
        output[..count].copy_from_slice(&self.cached[offset..offset + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Read for RocksArtifactReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.read_inner(output)
    }
}

#[cfg(test)]
mod tests {
    use super::super::journal_capacity::SourceJournalAdmission;
    use super::*;

    #[tokio::test]
    async fn complete_value_above_eight_mib_is_chunked_verified_and_collectable() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let bytes = vec![0x53; PAYLOAD_ARTIFACT_CHUNK_BYTES + 17];
        let reference = store.stage_blob(&bytes).await.unwrap();
        let manifest = store.read_complete_manifest(&reference).unwrap().unwrap();

        assert_eq!(manifest.layout, ArtifactLayout::Chunked { chunk_count: 2 });
        assert_eq!(store.read_blob_bytes(&reference).await.unwrap(), bytes);
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                    chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 0),
                )
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                    chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 1),
                )
                .unwrap()
                .is_some()
        );
        let updated_at = store
            .blob_reference_state(&reference)
            .unwrap()
            .unwrap()
            .updated_at;

        assert_eq!(
            store
                .collect_blob_garbage_at(updated_at + 1_000)
                .await
                .unwrap(),
            1
        );
        assert!(!store.contains_blob(&reference).await.unwrap());
    }

    #[tokio::test]
    async fn interrupted_chunk_install_is_durable_owned_and_age_gated_gc_reclaims_it() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let bytes = vec![0x6d; PAYLOAD_ARTIFACT_CHUNK_BYTES + 17];
        let reference = blob_reference_for_bytes(&bytes);
        let identity = complete_identity(&reference);
        let manifest = ArtifactManifest::complete(&reference).unwrap();
        let now = now_unix_millis().unwrap();

        {
            let store =
                Store::open(StoreOptions::new(&root, 1).with_awaiting_publish_ttl_seconds(1))
                    .await
                    .unwrap();
            assert_eq!(
                store
                    .begin_sealed_artifact_install_with_admission_wait(
                        &identity,
                        manifest.clone(),
                        now,
                        SourceJournalAdmission::Bounded,
                    )
                    .await
                    .unwrap(),
                0
            );
            store
                .advance_artifact_install(
                    &identity,
                    &manifest,
                    0,
                    &bytes[..PAYLOAD_ARTIFACT_CHUNK_BYTES],
                    now,
                    None,
                )
                .unwrap();
            assert!(
                store
                    .db
                    .get_cf(
                        store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                        complete_chunk_key(&reference, 0),
                    )
                    .unwrap()
                    .is_some()
            );
            assert!(store.read_complete_manifest(&reference).unwrap().is_none());
            assert!(
                store
                    .read_artifact_install_manifest(&identity)
                    .unwrap()
                    .is_some()
            );
        }

        let store = Store::open(StoreOptions::new(&root, 1).with_awaiting_publish_ttl_seconds(1))
            .await
            .unwrap();
        assert_eq!(store.collect_blob_garbage_at(now + 1_000).await.unwrap(), 1);
        assert!(store.blob_reference_state(&reference).unwrap().is_none());
        assert!(
            store
                .read_artifact_install_manifest(&identity)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                    complete_chunk_key(&reference, 0),
                )
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn chunk_with_extra_bytes_is_corruption_not_silently_truncated() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let bytes = vec![0x41; PAYLOAD_ARTIFACT_CHUNK_BYTES + 17];
        let reference = store.stage_blob(&bytes).await.unwrap();
        let manifest = store.read_complete_manifest(&reference).unwrap().unwrap();
        let key = chunk_key(COMPLETE_CHUNK_TAG, &manifest.storage_id, 1);
        let mut malformed = vec![0x41; 18];
        malformed[17] = 0xff;
        store
            .db
            .put_cf(store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(), key, malformed)
            .unwrap();

        let error = store.read_blob_bytes(&reference).await.unwrap_err();
        assert!(error.to_string().contains("wrong encoded length"));
    }

    #[tokio::test]
    async fn gc_never_discards_lifecycle_authority_when_payload_metadata_is_missing() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreOptions::new(temporary.path(), 1).with_awaiting_publish_ttl_seconds(1),
        )
        .await
        .unwrap();
        let bytes = vec![0x5c; 96 * 1024];
        let reference = store.stage_blob(&bytes).await.unwrap();
        let state = store.blob_reference_state(&reference).unwrap().unwrap();
        let identity = complete_identity(&reference);
        store
            .db
            .delete_cf(
                store.cf(CF_PAYLOAD_MANIFESTS).unwrap(),
                manifest_key(&identity),
            )
            .unwrap();

        let error = store
            .collect_blob_garbage_at(state.updated_at + 1_000)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("neither a sealed manifest"));
        assert!(store.blob_reference_state(&reference).unwrap().is_some());
        assert!(
            store
                .db
                .get_cf(
                    store.cf(CF_PAYLOAD_ARTIFACTS).unwrap(),
                    complete_inline_key(&reference),
                )
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn payload_without_lifecycle_is_corruption_not_an_adoptable_orphan() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let bytes = vec![0x27; 96 * 1024];
        let reference = store.stage_blob(&bytes).await.unwrap();
        store
            .db
            .delete_cf(
                store.cf(CF_BLOB_REFERENCES).unwrap(),
                blob_reference_key(&reference),
            )
            .unwrap();

        let error = store.stage_blob(&bytes).await.unwrap_err();

        assert!(error.to_string().contains("without lifecycle authority"));
        assert!(store.read_complete_manifest(&reference).unwrap().is_some());
    }
}
