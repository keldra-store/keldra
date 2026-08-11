use rocksdb::{DEFAULT_COLUMN_FAMILY_NAME, properties};

use super::*;

#[tokio::test]
async fn metadata_column_families_share_bounded_native_memory() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();

    assert!(store._metadata_memory.write_buffer_manager.enabled());
    assert_eq!(
        store
            ._metadata_memory
            .write_buffer_manager
            .get_buffer_size(),
        METADATA_WRITE_BUFFER_MANAGER_BYTES
    );
    assert_eq!(
        METADATA_BLOCK_CACHE_BYTES + METADATA_WRITE_BUFFER_MANAGER_BYTES,
        192 * 1024 * 1024
    );
    assert_eq!(
        METADATA_WRITE_BUFFER_MANAGER_BYTES / METADATA_COLUMN_FAMILY_WRITE_BUFFER_BYTES,
        8
    );

    for name in std::iter::once(DEFAULT_COLUMN_FAMILY_NAME).chain(COLUMN_FAMILIES.iter().copied()) {
        let column_family = store.db.cf_handle(name).unwrap();
        let capacity = store
            .db
            .property_int_value_cf(column_family, properties::BLOCK_CACHE_CAPACITY)
            .unwrap()
            .unwrap();
        assert_eq!(capacity, METADATA_BLOCK_CACHE_BYTES as u64, "{name}");
    }
}

#[tokio::test]
async fn bounded_metadata_options_reopen_an_existing_store() {
    let temporary = tempfile::tempdir().unwrap();
    {
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        store
            .db
            .put_cf(
                store.db.cf_handle(CF_METADATA).unwrap(),
                b"memory-test",
                b"ok",
            )
            .unwrap();
    }

    let reopened = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .db
            .get_cf(reopened.db.cf_handle(CF_METADATA).unwrap(), b"memory-test")
            .unwrap()
            .as_deref(),
        Some(b"ok".as_slice())
    );
    assert_eq!(
        reopened
            ._metadata_memory
            .write_buffer_manager
            .get_buffer_size(),
        METADATA_WRITE_BUFFER_MANAGER_BYTES
    );
}
