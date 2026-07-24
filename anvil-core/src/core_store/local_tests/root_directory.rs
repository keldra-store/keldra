use super::*;

#[tokio::test]
async fn committed_root_directory_is_bounded_and_resumable() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store.bootstrap_system_root_anchor().await.unwrap();

    let first = store
        .coremeta_root_directory_page("", 1, 1024 * 1024)
        .unwrap();
    assert_eq!(first.entries.len(), 1);
    assert!(!first.directory_complete);
    assert_eq!(first.next_root_key_hash, first.entries[0].root_key_hash);

    let final_page = store
        .coremeta_root_directory_page(&first.next_root_key_hash, 1, 1024 * 1024)
        .unwrap();
    assert!(final_page.entries.is_empty());
    assert!(final_page.directory_complete);
    assert!(final_page.next_root_key_hash.is_empty());
}

#[tokio::test]
async fn committed_root_directory_rejects_invalid_cursor_and_unbounded_requests() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    assert!(
        store
            .coremeta_root_directory_page("not-a-hash", 1, 1024)
            .is_err()
    );
    assert!(store.coremeta_root_directory_page("", 0, 1024).is_err());
    assert!(
        store
            .coremeta_root_directory_page("", CORE_META_MAX_SCAN_PAGE_ROWS + 1, 1024)
            .is_err()
    );
}
