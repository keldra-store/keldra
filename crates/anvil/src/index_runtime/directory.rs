//! Lazy cache-backed access to checked ranges in immutable v4 artifact objects.
//!
//! A pinned query or generation verifies each distinct ordinary-object path and
//! exact version before the disposable content cache may materialise it. Range
//! reads then remain bounded to the component named by the manifest.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use anvil_index::{IndexError, v4::ArtifactDescriptor};
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

    /// Resolve one exact ordinary-object reference, then open its checked
    /// component range. Verification is retained only by this directory.
    pub(crate) async fn open(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<ManifestArtifactFile, IndexError> {
        descriptor.validate(self.index_id)?;
        let identity = VerifiedArtifactObject::from(descriptor);
        let verified = self
            .verified
            .lock()
            .map_err(|_| IndexError::Io("index artifact verification lock is poisoned".into()))?
            .contains(&identity);
        if !verified {
            self.verify(descriptor).await?;
            self.verified
                .lock()
                .map_err(|_| IndexError::Io("index artifact verification lock is poisoned".into()))?
                .insert(identity);
        }
        let object = IndexSegmentId::new(descriptor.object_content_hash, descriptor.object_length)
            .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        Ok(ManifestArtifactFile {
            inner: self.cache.open(object),
            start: descriptor.offset,
            length: descriptor.encoded_length,
        })
    }

    async fn verify(&self, descriptor: &ArtifactDescriptor) -> Result<(), IndexError> {
        let key = ObjectKey::new(
            self.storage_tenant.clone(),
            self.bucket.clone(),
            descriptor.path.clone(),
        )
        .map_err(|error| IndexError::InvalidDefinition(error.to_string()))?;
        let snapshot = self
            .reader
            .current_snapshot_stable(&key, self.tenant_id, self.bucket_id)
            .await
            .map_err(|error| IndexError::Io(error.to_string()))?
            .ok_or_else(|| IndexError::FileNotFound(descriptor.path.clone()))?;
        if snapshot.tenant_id != self.tenant_id
            || snapshot.bucket_id != self.bucket_id
            || snapshot.exact_path != descriptor.path
        {
            return Err(IndexError::Integrity);
        }
        let version = snapshot
            .versions
            .iter()
            .find(|version| version.id == VersionId(descriptor.object_version))
            .ok_or_else(|| IndexError::FileNotFound(descriptor.path.clone()))?;
        let expected = BlobRef {
            hash: descriptor.object_content_hash,
            length: descriptor.object_length,
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

impl From<&ArtifactDescriptor> for VerifiedArtifactObject {
    fn from(descriptor: &ArtifactDescriptor) -> Self {
        Self {
            version: descriptor.object_version,
            content_hash: descriptor.object_content_hash,
            length: descriptor.object_length,
        }
    }
}

pub(crate) struct ManifestArtifactFile {
    inner: IndexFile,
    start: u64,
    length: u64,
}

impl anvil_index::IndexFileRead for ManifestArtifactFile {
    type Slice = IndexSlice;

    async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
        if offset >= self.length || max_length == 0 {
            return anvil_index::IndexFileRead::read_at(&self.inner, 0, 0).await;
        }
        let remaining = usize::try_from(self.length - offset).unwrap_or(usize::MAX);
        let physical = self
            .start
            .checked_add(offset)
            .ok_or(IndexError::OffsetOverflow)?;
        anvil_index::IndexFileRead::read_at(&self.inner, physical, max_length.min(remaining)).await
    }
}

impl anvil_index::v4::ArtifactDirectoryRead for ManifestArtifactDirectory {
    type File = ManifestArtifactFile;

    async fn open_artifact(
        &self,
        descriptor: &ArtifactDescriptor,
    ) -> Result<Self::File, IndexError> {
        self.open(descriptor).await
    }
}
