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

#[tokio::test]
async fn existing_volume_without_object_metadata_format_marker_is_rejected() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1);
    let store = Store::open(options.clone()).await.unwrap();
    store
        .db
        .delete_cf(
            store.cf(CF_METADATA).unwrap(),
            object_metadata_codec::OBJECT_METADATA_FORMAT_KEY,
        )
        .unwrap();
    drop(store);

    let error = Store::open(options).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no object metadata persistence format marker")
    );
}

#[tokio::test]
async fn existing_volume_with_unknown_object_metadata_format_is_rejected() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1);
    let store = Store::open(options.clone()).await.unwrap();
    store
        .db
        .put_cf(
            store.cf(CF_METADATA).unwrap(),
            object_metadata_codec::OBJECT_METADATA_FORMAT_KEY,
            [2],
        )
        .unwrap();
    drop(store);

    let error = Store::open(options).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("object metadata persistence format marker is unsupported")
    );
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
        if key == LOCAL_INVALIDATION_STATUS_KEY {
            self.invalidation_metadata_puts += 1;
        }
        if key == MUTATION_RECEIPT_STATUS_KEY {
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
mod path_lock_sets;
mod paths;
mod reads_and_programs;
mod rocksdb_memory;
mod versioning;
