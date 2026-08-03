//! Bounded typed payload state transfer for membership handoff.
//!
//! Artifact bytes remain in the ordinary inline/blob/shard planes. This module
//! only enumerates their existing lifecycle records and, after exact bytes
//! have been durably verified, installs that same lifecycle state.

use rocksdb::{Direction, IteratorMode, WriteOptions};
use serde::{Deserialize, Serialize};

use super::*;
use crate::ErasureCodec;

pub const MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS: u32 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
pub enum PayloadArtifactIdentity {
    Complete(BlobRef),
    Shard(ShardIdentity),
}

impl PayloadArtifactIdentity {
    fn key(&self) -> Vec<u8> {
        match self {
            Self::Complete(reference) => blob_reference_key(reference),
            Self::Shard(identity) => identity.encode().to_vec(),
        }
    }

    pub fn blob(&self) -> &BlobRef {
        match self {
            Self::Complete(reference) => reference,
            Self::Shard(identity) => identity.blob(),
        }
    }

    /// Canonical lifecycle-column-family key used only to merge bounded
    /// handoff pages from several nodes in their existing iterator order.
    pub fn handoff_order_key(&self) -> Vec<u8> {
        self.key()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadArtifactSnapshot {
    pub identity: PayloadArtifactIdentity,
    pub lifecycle: BlobReferenceState,
}

impl PayloadArtifactSnapshot {
    pub fn validate(&self) -> Result<(), MutationError> {
        validate_blob_reference_state(self.lifecycle)?;
        let key = self.identity.key();
        match &self.identity {
            PayloadArtifactIdentity::Complete(_) => {
                if key.len() != 40 {
                    return Err(storage_error("payload handoff has a malformed complete identity"));
                }
            }
            PayloadArtifactIdentity::Shard(_) => {
                ShardIdentity::decode(&key).map_err(storage_error)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadArtifactCursor(Vec<u8>);

impl PayloadArtifactCursor {
    pub fn from_key(key: Vec<u8>) -> Result<Self, MutationError> {
        decode_artifact_identity(&key)?;
        Ok(Self(key))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PayloadArtifactSnapshotPage {
    pub artifacts: Vec<PayloadArtifactSnapshot>,
    pub next_cursor: Option<PayloadArtifactCursor>,
}

impl Store {
    /// Enumerate the existing lifecycle column family without inventing a
    /// placement inventory. The cursor is the last canonical artifact key.
    pub fn export_payload_artifact_snapshots(
        &self,
        cursor: Option<&PayloadArtifactCursor>,
        max_records: u32,
    ) -> Result<PayloadArtifactSnapshotPage, MutationError> {
        if max_records == 0 || max_records > MAX_PAYLOAD_HANDOFF_EXPORT_RECORDS {
            return Err(storage_error("payload handoff page limit is invalid"));
        }
        if let Some(cursor) = cursor {
            decode_artifact_identity(&cursor.0)?;
        }
        let start = cursor.map_or(&[][..], |cursor| cursor.0.as_slice());
        let mut artifacts = Vec::with_capacity(max_records as usize);
        let mut last_key = None;
        let mut has_more = false;
        for entry in self.db.iterator_cf(
            self.cf(CF_BLOB_REFERENCES)?,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, encoded) = entry.map_err(storage_error)?;
            if cursor.is_some_and(|cursor| key.as_ref() <= cursor.0.as_slice()) {
                continue;
            }
            if artifacts.len() == max_records as usize {
                has_more = true;
                break;
            }
            let artifact = PayloadArtifactSnapshot {
                identity: decode_artifact_identity(&key)?,
                lifecycle: decode_blob_reference_state(&encoded)?,
            };
            artifact.validate()?;
            last_key = Some(key.to_vec());
            artifacts.push(artifact);
        }
        Ok(PayloadArtifactSnapshotPage {
            artifacts,
            next_cursor: has_more
                .then(|| PayloadArtifactCursor(last_key.expect("a full page has a last key"))),
        })
    }

    /// Replace the temporary seal reservation with the exact lifecycle state
    /// copied during handoff. The ordinary seal path must have durably written
    /// and verified the bytes first; a crash before this call leaves only a
    /// safe age-gated reservation, and retrying completes the same transfer.
    pub async fn install_payload_artifact_lifecycle(
        &self,
        codec: &ErasureCodec,
        artifact: &PayloadArtifactSnapshot,
    ) -> Result<(), MutationError> {
        artifact.validate()?;
        match &artifact.identity {
            PayloadArtifactIdentity::Complete(reference) => {
                if self.complete_copy_state(reference).await.map_err(storage_error)?
                    != PayloadArtifactState::Valid
                {
                    return Err(storage_error(
                        "payload handoff lifecycle cannot precede verified complete bytes",
                    ));
                }
            }
            PayloadArtifactIdentity::Shard(identity) => {
                self.validate_shard(codec, identity).map_err(storage_error)?;
            }
        }

        let _guard = self.commit_lock.lock().await;
        let key = artifact.identity.key();
        let present = match &artifact.identity {
            PayloadArtifactIdentity::Complete(reference) if is_small_blob(reference) => self
                .db
                .get_cf(self.cf(CF_SMALL_BLOBS)?, &key)
                .map_err(storage_error)?
                .is_some(),
            PayloadArtifactIdentity::Complete(reference) => {
                self.blobs.contains(reference).await.map_err(storage_error)?
            }
            PayloadArtifactIdentity::Shard(identity) => {
                self.contains_shard_artifact(identity).map_err(storage_error)?
            }
        };
        if !present {
            return Err(MutationError::BlobNotFound);
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_BLOB_REFERENCES)?,
                key,
                encode_blob_reference_state(artifact.lifecycle),
                &options,
            )
            .map_err(storage_error)
    }
}

fn decode_artifact_identity(key: &[u8]) -> Result<PayloadArtifactIdentity, MutationError> {
    match key.len() {
        40 => blob_reference_from_key(key).map(PayloadArtifactIdentity::Complete),
        44 => ShardIdentity::decode(key)
            .map(PayloadArtifactIdentity::Shard)
            .map_err(storage_error),
        _ => Err(storage_error("payload lifecycle key has a malformed identity")),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{ErasureProfile, StoreOptions};

    #[tokio::test]
    async fn handoff_preserves_exact_small_and_shard_lifecycle() {
        let source_dir = tempfile::tempdir().unwrap();
        let target_dir = tempfile::tempdir().unwrap();
        let source = Store::open(StoreOptions::new(source_dir.path(), 1)).await.unwrap();
        let target = Store::open(StoreOptions::new(target_dir.path(), 2)).await.unwrap();
        let small = source.stage_blob(b"small handoff").await.unwrap();
        let large_bytes = vec![7_u8; SMALL_BLOB_MAX_BYTES + 1];
        let large = source.stage_blob(&large_bytes).await.unwrap();
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        let mut shards = vec![Vec::new(); usize::from(codec.profile().total_shards())];
        source
            .encode_sealed_source(&codec, &large, &mut shards)
            .await
            .unwrap();
        let shard = ShardIdentity::new(large, 0);
        source
            .seal_shard(&codec, &shard, Cursor::new(&shards[0]))
            .await
            .unwrap();

        let page = source
            .export_payload_artifact_snapshots(None, 100)
            .unwrap();
        let small_snapshot = page
            .artifacts
            .iter()
            .find(|entry| entry.identity == PayloadArtifactIdentity::Complete(small.clone()))
            .unwrap();
        target
            .seal_small_copy(&small, b"small handoff")
            .await
            .unwrap();
        target
            .install_payload_artifact_lifecycle(&codec, small_snapshot)
            .await
            .unwrap();
        assert_eq!(target.blob_reference_state(&small).unwrap(), Some(small_snapshot.lifecycle));

        let shard_snapshot = page
            .artifacts
            .iter()
            .find(|entry| entry.identity == PayloadArtifactIdentity::Shard(shard.clone()))
            .unwrap();
        target
            .seal_shard(&codec, &shard, Cursor::new(&shards[0]))
            .await
            .unwrap();
        target
            .install_payload_artifact_lifecycle(&codec, shard_snapshot)
            .await
            .unwrap();
        assert_eq!(
            target.shard_reference_state(&shard).unwrap(),
            Some(shard_snapshot.lifecycle)
        );
    }

    #[tokio::test]
    async fn lifecycle_install_rejects_missing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(dir.path(), 1)).await.unwrap();
        let artifact = PayloadArtifactSnapshot {
            identity: PayloadArtifactIdentity::Complete(BlobRef {
                hash: *blake3::hash(b"missing").as_bytes(),
                length: 7,
            }),
            lifecycle: BlobReferenceState {
                ref_count: 9,
                flags: 0,
                created_at: 1,
                updated_at: 2,
            },
        };
        let codec = ErasureCodec::new(ErasureProfile::default()).unwrap();
        assert!(store
            .install_payload_artifact_lifecycle(&codec, &artifact)
            .await
            .is_err());
    }
}
