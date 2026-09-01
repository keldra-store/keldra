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
    assert_eq!(options.max_total_wal_bytes, 50 * 1024 * 1024 * 1024);
    assert_eq!(options.pending_upload_max_bytes, 16 * 1024 * 1024 * 1024);
}

#[tokio::test]
async fn fresh_store_refuses_a_nonempty_authoritative_payload_root() {
    let temporary = tempfile::tempdir().unwrap();
    let payloads = temporary.path().join("payloads");
    std::fs::create_dir_all(&payloads).unwrap();
    std::fs::write(payloads.join("unowned"), b"bytes").unwrap();
    let options =
        StoreOptions::new(temporary.path().join("store"), 1).with_payload_directory(&payloads);

    let error = Store::open(options)
        .await
        .err()
        .expect("nonempty fresh payload root must fail");

    assert!(
        error
            .to_string()
            .contains("refuses non-empty authoritative payload root")
    );
}

#[tokio::test]
async fn fresh_store_allows_only_current_authoritative_root_markers() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("state");
    let metadata = temporary.path().join("metadata");
    let wal = temporary.path().join("wal");
    let payloads = temporary.path().join("payloads");
    for (directory, marker) in [
        (&metadata, ".keldra-metadata-root-v1.json"),
        (&wal, ".keldra-metadata_wal-root-v1.json"),
        (&payloads, ".keldra-payload-root-v1.json"),
    ] {
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(directory.join(marker), b"current root authority").unwrap();
    }
    let options = StoreOptions::new(&root, 1)
        .with_metadata_directory(&metadata)
        .with_metadata_wal_directory(&wal)
        .with_payload_directory(&payloads)
        .with_pending_upload_max_bytes(1024 * 1024);

    let store = Store::open(options).await.unwrap();

    assert!(
        store
            .db
            .get_cf(store.cf(CF_METADATA).unwrap(), b"absent")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn existing_store_requires_its_integrated_payload_root() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("store");
    let options = StoreOptions::new(&root, 1);
    let store = Store::open(options.clone()).await.unwrap();
    drop(store);
    std::fs::remove_dir_all(root.join("blobs")).unwrap();

    let error = Store::open(options)
        .await
        .err()
        .expect("missing integrated payload root must fail");

    assert!(error.to_string().contains("payload root is unavailable"));
}

#[tokio::test]
async fn existing_store_without_the_integrated_format_marker_is_rejected() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1);
    let store = Store::open(options.clone()).await.unwrap();
    store
        .db
        .delete_cf(
            store.cf(CF_METADATA).unwrap(),
            INTEGRATED_PAYLOAD_STORAGE_FORMAT_KEY,
        )
        .unwrap();
    drop(store);

    let error = Store::open(options)
        .await
        .err()
        .expect("missing integrated storage marker must fail");

    assert!(
        error
            .to_string()
            .contains("has no integrated payload storage marker")
    );
}

#[tokio::test]
async fn existing_store_without_the_durable_mutation_record_marker_is_rejected() {
    let temporary = tempfile::tempdir().unwrap();
    let options = StoreOptions::new(temporary.path(), 1);
    let store = Store::open(options.clone()).await.unwrap();
    store
        .db
        .delete_cf(
            store.cf(CF_METADATA).unwrap(),
            DURABLE_MUTATION_RECORD_FORMAT_KEY,
        )
        .unwrap();
    drop(store);

    let error = Store::open(options)
        .await
        .err()
        .expect("missing durable mutation record marker must fail");

    assert!(
        error
            .to_string()
            .contains("has no durable mutation record format marker")
    );
}

#[tokio::test]
async fn pre_integrated_column_family_layout_is_rejected_without_migration() {
    let temporary = tempfile::tempdir().unwrap();
    let metadata = temporary.path().join("metadata");
    let payloads = temporary.path().join("blobs");
    std::fs::create_dir_all(&payloads).unwrap();
    let mut old_options = Options::default();
    old_options.create_if_missing(true);
    old_options.create_missing_column_families(true);
    let old = DB::open_cf(&old_options, &metadata, [CF_METADATA, "small_blobs"]).unwrap();
    drop(old);

    let error = Store::open(StoreOptions::new(temporary.path(), 1))
        .await
        .err()
        .expect("pre-integrated layout must fail");

    assert!(
        error
            .to_string()
            .contains("does not use the exact 0.15 integrated payload layout")
    );
}

#[tokio::test]
async fn distinct_metadata_wal_and_payload_directories_survive_reopen() {
    let temporary = tempfile::tempdir().unwrap();
    let unused_root = temporary.path().join("unused-root");
    let metadata = temporary.path().join("metadata-db");
    let wal = temporary.path().join("metadata-wal");
    let payloads = temporary.path().join("payloads");
    let options = StoreOptions::new(&unused_root, 1)
        .with_metadata_directory(&metadata)
        .with_metadata_wal_directory(&wal)
        .with_payload_directory(&payloads)
        .with_pending_upload_max_bytes(1024 * 1024);
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
    assert!(payloads.is_dir());
    assert!(store.read_complete_manifest(&reference).unwrap().is_some());
    let encoded_hash = hex::encode(reference.hash);
    assert!(
        !payloads
            .join(&encoded_hash[..2])
            .join(encoded_hash)
            .exists()
    );
    assert!(!unused_root.exists());
    drop(store);

    let reopened = Store::open(options).await.unwrap();
    assert_eq!(
        reopened.get(&key("large")).await.unwrap().unwrap().bytes,
        bytes
    );
    assert!(
        reopened
            .read_complete_manifest(&reference)
            .unwrap()
            .is_some()
    );
}
