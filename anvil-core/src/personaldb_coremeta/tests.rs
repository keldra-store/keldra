use super::*;
use crate::core_store::CoreManifestRef;
use tempfile::tempdir;

#[tokio::test]
async fn data_locator_pages_are_bounded_visible_and_seek_from_the_cursor() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    for (generation, data_id) in (1..).zip(["a", "b", "c"]) {
        let row = data_locator_row(4, "target", data_id, generation);
        write_personaldb_data_locator_row(&storage, &row, &[])
            .await
            .unwrap();
    }

    // This canonical candidate has the next root generation but no root
    // publication. Product pagination must skip it and continue to visible rows.
    let unpublished = data_locator_row(4, "target", "aa-hidden", 4);
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    meta.put(
        CF_PERSONALDB,
        TABLE_PERSONALDB_DATA_LOCATOR_ROW,
        &personaldb_data_locator_tuple_key(4, "target", "aa-hidden").unwrap(),
        &encode_data_locator_row(&unpublished).unwrap(),
    )
    .unwrap();
    assert!(
        read_personaldb_data_locator_row(&storage, 4, "target", "aa-hidden")
            .await
            .unwrap()
            .is_none()
    );

    let first = list_personaldb_data_locator_rows(&storage, 4, "target", None, 2)
        .await
        .unwrap();
    assert_eq!(
        first
            .rows
            .iter()
            .map(|row| row.data_id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let second = list_personaldb_data_locator_rows(
        &storage,
        4,
        "target",
        first.next_tuple_key.as_deref(),
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        second
            .rows
            .iter()
            .map(|row| row.data_id.as_str())
            .collect::<Vec<_>>(),
        vec!["c"]
    );
    assert!(second.next_tuple_key.is_none());
    assert!(
        list_personaldb_data_locator_rows(&storage, 4, "target", None, 0)
            .await
            .is_err()
    );
    assert!(
        list_personaldb_data_locator_rows(
            &storage,
            4,
            "target",
            None,
            PERSONALDB_DATA_LOCATOR_PAGE_MAX + 1,
        )
        .await
        .is_err()
    );
}

fn data_locator_row(
    tenant_id: i64,
    group_id: &str,
    data_id: &str,
    generation: u64,
) -> PersonalDbDataLocatorCoreMetaRow {
    let manifest_hash = format!("sha256:{}", hex::encode([7; 32]));
    PersonalDbDataLocatorCoreMetaRow {
        tenant_id,
        group_id: group_id.to_string(),
        data_id: data_id.to_string(),
        data_kind: "test".to_string(),
        generation,
        root_generation: generation,
        sqlite_changeset_hash: String::new(),
        payload_locator: CoreManifestLocator {
            manifest_ref: CoreManifestRef {
                logical_file_id: format!("locator-{group_id}-{data_id}"),
                writer_family: "personaldb".to_string(),
                writer_generation: generation,
                manifest_hash: manifest_hash.clone(),
            },
            manifest_encoding: "proto".to_string(),
            manifest_length: 0,
            manifest_hash,
            block_locators: Vec::new(),
        },
        projection_keys: Vec::new(),
        transaction_id: format!("locator-{group_id}-{data_id}"),
        created_at_unix_nanos: generation,
    }
}
