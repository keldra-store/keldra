//! Fsync-backed local object representations for `local` durability.

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

use crate::mvcc_transaction::{
    NodeIncarnation, ObjectDurabilityEvidence, ObjectShardManifestReference,
};

#[derive(Clone, Debug)]
pub struct LocalObjectStore {
    directory: Arc<PathBuf>,
    cluster_id: Arc<str>,
    node: NodeIncarnation,
    failure_domain: Arc<str>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalObjectManifest {
    pub schema_version: u16,
    pub cluster_id: String,
    pub object_hash: String,
    pub object_length: u64,
    pub node: NodeIncarnation,
    pub failure_domain: String,
}

pub struct LocalObjectIngestResult {
    pub manifest: LocalObjectManifest,
    pub reference: ObjectShardManifestReference,
    pub evidence: ObjectDurabilityEvidence,
}

impl LocalObjectStore {
    pub fn open(
        directory: impl AsRef<Path>,
        cluster_id: impl Into<Arc<str>>,
        node: NodeIncarnation,
        failure_domain: impl Into<Arc<str>>,
    ) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        let cluster_id = cluster_id.into();
        let failure_domain = failure_domain.into();
        if cluster_id.trim().is_empty()
            || node.node_id.trim().is_empty()
            || node.incarnation == 0
            || failure_domain.trim().is_empty()
        {
            bail!("local object store identity must be valid");
        }
        fs::create_dir_all(&directory)?;
        Ok(Self {
            directory: Arc::new(directory),
            cluster_id,
            node,
            failure_domain,
        })
    }

    pub async fn persist<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<LocalObjectIngestResult> {
        let temporary = self
            .directory
            .join(format!(".{}.local-object.part", Uuid::new_v4()));
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await?;
        let mut hash = Sha256::new();
        let mut length = 0_u64;
        let mut buffer = vec![0; 256 * 1024];
        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            tokio::io::AsyncWriteExt::write_all(&mut file, &buffer[..read]).await?;
            length = length
                .checked_add(read as u64)
                .context("local object length overflow")?;
        }
        file.sync_all().await?;
        drop(file);
        let object_hash = format!("sha256:{}", hex::encode(hash.finalize()));
        let final_path = self.path_for_hash(&object_hash)?;
        if final_path.exists() {
            verify_file(&final_path, &object_hash, length)?;
            fs::remove_file(&temporary)?;
        } else {
            fs::rename(&temporary, &final_path)?;
            sync_directory(&self.directory)?;
        }
        let manifest = LocalObjectManifest {
            schema_version: 1,
            cluster_id: self.cluster_id.to_string(),
            object_hash: object_hash.clone(),
            object_length: length,
            node: self.node.clone(),
            failure_domain: self.failure_domain.to_string(),
        };
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        let reference = ObjectShardManifestReference {
            object_hash: object_hash.clone(),
            manifest_hash: format!("sha256:{}", hex::encode(Sha256::digest(manifest_bytes))),
            object_length: length,
            encoding_generation: 1,
            data_shards: 1,
            parity_shards: 0,
            stripe_count: 1,
        };
        Ok(LocalObjectIngestResult {
            manifest,
            reference,
            evidence: ObjectDurabilityEvidence::LocalRepresentation {
                cluster_id: self.cluster_id.to_string(),
                object_hash,
                node: self.node.clone(),
                failure_domain: self.failure_domain.to_string(),
                complete: true,
                hash_verified: true,
                fsynced: true,
            },
        })
    }

    pub fn read_range(
        &self,
        manifest: &LocalObjectManifest,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<u8>> {
        if manifest.cluster_id != &*self.cluster_id
            || manifest.node != self.node
            || start > end_exclusive
            || end_exclusive > manifest.object_length
        {
            bail!("local object manifest or range is invalid for this store");
        }
        let path = self.path_for_hash(&manifest.object_hash)?;
        verify_file(&path, &manifest.object_hash, manifest.object_length)?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; usize::try_from(end_exclusive - start)?];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn path_for_hash(&self, object_hash: &str) -> Result<PathBuf> {
        let digest = object_hash
            .strip_prefix("sha256:")
            .context("local object hash must use sha256")?;
        if digest.len() != 64 || !digest.bytes().all(|value| value.is_ascii_hexdigit()) {
            bail!("local object hash is invalid");
        }
        Ok(self.directory.join(format!("{digest}.object")))
    }
}

fn verify_file(path: &Path, expected_hash: &str, expected_length: u64) -> Result<()> {
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected_length {
        bail!("local object representation length mismatch");
    }
    let mut hash = Sha256::new();
    let mut buffer = [0; 256 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    if format!("sha256:{}", hex::encode(hash.finalize())) != expected_hash {
        bail!("local object representation hash mismatch");
    }
    Ok(())
}

fn sync_directory(directory: &Path) -> Result<()> {
    OpenOptions::new().read(true).open(directory)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn retry_reuses_fsynced_hash_verified_representation() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalObjectStore::open(
            directory.path(),
            "cluster-a",
            NodeIncarnation {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            "zone-a",
        )
        .unwrap();
        let first = store
            .persist(&mut BufReader::new(&b"local bytes"[..]))
            .await
            .unwrap();
        let second = store
            .persist(&mut BufReader::new(&b"local bytes"[..]))
            .await
            .unwrap();
        assert_eq!(first.manifest, second.manifest);
        assert_eq!(store.read_range(&first.manifest, 6, 11).unwrap(), b"bytes");
    }
}
