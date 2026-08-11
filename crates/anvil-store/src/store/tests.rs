use rocksdb::WriteBatchIteratorCf;
use tempfile::TempDir;

use super::*;

async fn store() -> (TempDir, Store) {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    (temporary, store)
}

fn key(path: &str) -> ObjectKey {
    ObjectKey::new("tenant", "bucket", path).unwrap()
}

fn put(path: &str, bytes: &[u8], precondition: Precondition, command: &str) -> PutRequest {
    PutRequest {
        key: key(path),
        bytes: bytes.to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: match precondition {
            Precondition::Any => PutMode::Put,
            Precondition::Absent => PutMode::PutIfAbsent,
            Precondition::Version(version) => PutMode::PutIfVersion(version),
        },
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

fn immutable_put(path: &str, bytes: &[u8], command: &str) -> PutRequest {
    PutRequest {
        key: key(path),
        bytes: bytes.to_vec(),
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::PutImmutable,
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

fn publish(path: &str, blob: BlobRef, command: &str) -> PublishRequest {
    PublishRequest {
        key: key(path),
        blob,
        content_type: Some("application/octet-stream".into()),
        mode: PutMode::Put,
        command_id: Some(command.into()),
        durability: Durability::Local,
    }
}

fn blob_file_path(store: &Store, reference: &BlobRef) -> PathBuf {
    let hash = hex::encode(reference.hash);
    store.blobs.root().join(&hash[..2]).join(hash)
}

#[derive(Default)]
struct WalOperationCounter {
    puts: usize,
    deletes: usize,
    merges: usize,
    high_watermark_puts: usize,
    invalidation_metadata_puts: usize,
    receipt_metadata_puts: usize,
}

impl WriteBatchIteratorCf for WalOperationCounter {
    fn put_cf(&mut self, _cf_id: u32, key: &[u8], _value: &[u8]) {
        self.puts += 1;
        if key == VERSION_HIGH_WATERMARK_KEY {
            self.high_watermark_puts += 1;
        }
        if [
            LOCAL_INVALIDATION_OFFSET_KEY,
            LOCAL_INVALIDATION_SETTLED_KEY,
            LOCAL_INVALIDATION_FLOOR_KEY,
            LOCAL_INVALIDATION_COUNT_KEY,
            LOCAL_INVALIDATION_BYTES_KEY,
        ]
        .contains(&key)
        {
            self.invalidation_metadata_puts += 1;
        }
        if [MUTATION_RECEIPT_COUNT_KEY, MUTATION_RECEIPT_BYTES_KEY].contains(&key) {
            self.receipt_metadata_puts += 1;
        }
    }

    fn delete_cf(&mut self, _cf_id: u32, _key: &[u8]) {
        self.deletes += 1;
    }

    fn merge_cf(&mut self, _cf_id: u32, _key: &[u8], _value: &[u8]) {
        self.merges += 1;
    }
}

mod blob_lifecycle;
mod mutations;
mod reads_and_programs;
mod versioning;
