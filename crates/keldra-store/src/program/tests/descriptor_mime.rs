use super::*;

#[tokio::test]
async fn descriptor_mime_without_target_sidecar_provenance_remains_an_ordinary_object() {
    let (_temporary, store, _) = configured_store().await;
    let requested = ObjectPath::new("tenant", "bucket", "historical/descriptor-shaped").unwrap();
    let descriptor = crate::ObjectLinkDescriptor::new("historical/target").unwrap();
    let receipt = store
        .put(PutRequest {
            key: object_key(&requested).unwrap(),
            bytes: descriptor.encode(),
            content_type: Some(crate::OBJECT_LINK_CONTENT_TYPE.into()),
            mode: PutMode::Put,
            command_id: Some("historical-descriptor-mime".into()),
            durability: Durability::Local,
        })
        .await
        .unwrap();

    let bindings = store
        .resolve_program_alias_bindings(&[ExpandedProgramPath {
            path: requested.clone(),
            intent: keldra_atomic_program::ProgramPathIntent {
                get: true,
                put: false,
                delete: false,
            },
        }])
        .await
        .unwrap();

    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].requested_path, requested);
    assert_eq!(bindings[0].canonical_path, requested);
    assert!(bindings[0].descriptor_version.is_none());
    assert_eq!(
        bindings[0].canonical_version.as_ref().map(|v| v.id),
        Some(receipt.version)
    );
}
