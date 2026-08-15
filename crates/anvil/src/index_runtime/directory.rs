//! Lazy cache-backed access to checked ranges in immutable v4 artifact objects.
//!
//! A pinned query or generation verifies each distinct ordinary-object pack
//! and exact version before the disposable content cache may materialise it.
//! Component ranges are resolved and bounded by the storage-neutral index
//! reader after the enclosing pack has been opened.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use anvil_index::{IndexError, v4::ArtifactPackReference};
use anvil_store::{BlobRef, ObjectKey, VersionId};

use crate::cluster_object_read::ClusterObjectReader;

use super::cache::{IndexCache, IndexFile, IndexSegmentId, IndexSlice};

#[derive(Clone)]
pub(crate) struct ManifestArtifactDirectory {
    cache: IndexCache,
    reader: ClusterObjectReader,
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    index_id: u64,
    verified: Arc<Mutex<BTreeSet<VerifiedArtifactObject>>>,
}

impl ManifestArtifactDirectory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        cache: IndexCache,
        reader: ClusterObjectReader,
        storage_tenant: String,
        bucket: String,
        tenant_id: u64,
        bucket_id: u64,
        index_id: u64,
    ) -> Result<Self, IndexError> {
        if tenant_id == 0 || bucket_id == 0 || index_id == 0 {
            return Err(IndexError::InvalidDefinition(
                "format-v4 artifact directory requires non-zero stable IDs".into(),
            ));
        }
        Ok(Self {
            cache,
            reader,
            storage_tenant,
            bucket,
            tenant_id,
            bucket_id,
            index_id,
            verified: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    /// Resolve and open one exact ordinary-object pack reference. Verification
    /// is retained only by this directory.
    pub(crate) async fn open(
        &self,
        pack: &ArtifactPackReference,
    ) -> Result<ManifestArtifactFile, IndexError> {
        pack.validate(self.index_id)?;
        let identity = VerifiedArtifactObject::from(pack);
        let verified = self
            .verified
            .lock()
            .map_err(|_| IndexError::Io("index artifact verification lock is poisoned".into()))?
            .contains(&identity);
        if !verified {
            self.verify(pack).await?;
            self.verified
                .lock()
                .map_err(|_| IndexError::Io("index artifact verification lock is poisoned".into()))?
                .insert(identity);
        }
        let object = IndexSegmentId::new(pack.object_content_hash, pack.object_length)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        Ok(ManifestArtifactFile {
            inner: self.cache.open(object),
        })
    }

    async fn verify(&self, pack: &ArtifactPackReference) -> Result<(), IndexError> {
        let key = ObjectKey::new(
            self.storage_tenant.clone(),
            self.bucket.clone(),
            pack.path.clone(),
        )
        .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        let snapshot = self
            .reader
            .current_snapshot_stable(&key, self.tenant_id, self.bucket_id)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?
            .ok_or_else(|| IndexError::FileNotFound(pack.path.clone()))?;
        if snapshot.tenant_id != self.tenant_id
            || snapshot.bucket_id != self.bucket_id
            || snapshot.exact_path != pack.path
        {
            return Err(IndexError::Integrity);
        }
        let version = snapshot
            .versions
            .iter()
            .find(|version| version.id == VersionId(pack.object_version))
            .ok_or_else(|| IndexError::FileNotFound(pack.path.clone()))?;
        let expected = BlobRef {
            hash: pack.object_content_hash,
            length: pack.object_length,
        };
        if version.deleted || version.blob.as_ref() != Some(&expected) {
            return Err(IndexError::Integrity);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct VerifiedArtifactObject {
    version: u64,
    content_hash: [u8; 32],
    length: u64,
}

impl From<&ArtifactPackReference> for VerifiedArtifactObject {
    fn from(pack: &ArtifactPackReference) -> Self {
        Self {
            version: pack.object_version,
            content_hash: pack.object_content_hash,
            length: pack.object_length,
        }
    }
}

pub(crate) struct ManifestArtifactFile {
    inner: IndexFile,
}

impl anvil_index::IndexFileRead for ManifestArtifactFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        anvil_index::IndexFileRead::read_at(&self.inner, offset, max_length).await
    }
}

impl anvil_index::v4::ArtifactDirectoryRead for ManifestArtifactDirectory {
    type File = ManifestArtifactFile;

    async fn open_artifact(&self, pack: &ArtifactPackReference) -> Result<Self::File, IndexError> {
        self.open(pack).await
    }
}
