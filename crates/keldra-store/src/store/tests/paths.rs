use std::ffi::OsStr;

use super::*;

#[test]
fn store_options_default_to_the_existing_root_layout() {
    let root = PathBuf::from("layout-root");
    let options = StoreOptions::new(&root, 7);

    assert_eq!(options.root, root);
    assert_eq!(options.metadata_directory, root.join("metadata"));
    assert_eq!(options.metadata_wal_directory, root.join("metadata"));
    assert_eq!(options.payload_directory, root.join("blobs"));
    assert_eq!(
        options.upload_spool_directory,
        root.join("blobs/.upload-spool")
    );
}

#[tokio::test]
async fn distinct_metadata_wal_and_payload_directories_survive_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let unused_root = temporary.path().join("unused-root");
    let metadata = temporary.path().join("metadata-db");
    let wal = temporary.path().join("metadata-wal");
    let payloads = temporary.path().join("payloads");
    let upload_spool = temporary.path().join("upload-spool");
    let options = StoreOptions::new(&unused_root, 1)
        .with_metadata_directory(&metadata)
        .with_metadata_wal_directory(&wal)
        .with_payload_directory(&payloads)
        .with_upload_spool(&upload_spool, 1024 * 1024);
    let bytes = vec![0x5a; SMALL_BLOB_MAX_BYTES + 1];
    let reference = blob_reference_for_bytes(&bytes);

    let store = Store::open(options.clone()).await.unwrap();
    store
        .put(put(
            "large",
            &bytes,
            Precondition::Absent,
            "separate-layout",
        ))
        .await
        .unwrap();

    assert!(metadata.join("CURRENT").is_file());
    assert!(
        std::fs::read_dir(&wal)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry.path().extension() == Some(OsStr::new("log")))
    );
    assert_eq!(store.blobs.root(), payloads);
    assert!(blob_file_path(&store, &reference).is_file());
    assert!(!unused_root.exists());
    drop(store);

    let reopened = Store::open(options).await.unwrap();
    assert_eq!(
        reopened.get(&key("large")).await.unwrap().unwrap().bytes,
        bytes
    );
    assert_eq!(reopened.blobs.root(), payloads);
}
