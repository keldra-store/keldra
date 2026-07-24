use super::*;
use std::collections::HashSet;

#[tokio::test]
async fn concurrent_index_creates_allocate_distinct_ids_and_cursors() {
    let temp = tempdir().unwrap();
    let persistence = Persistence::new(&test_config(temp.path())).unwrap();
    let tenant = persistence
        .create_tenant("concurrent-index-tenant", "concurrent-index-tenant")
        .await
        .unwrap();
    let bucket = persistence
        .create_bucket(tenant.id, "concurrent-index-bucket", "test-region")
        .await
        .unwrap();

    let creates = (0..8).map(|ordinal| {
        let persistence = persistence.clone();
        let bucket = bucket.clone();
        async move {
            let mutation = IndexDefinitionMutation::Create {
                name: format!("index-{ordinal:02}"),
                kind: "typed_json".to_string(),
                selector: json!({"prefix": format!("items/{ordinal}/")}),
                extractor: json!({}),
                authorization_mode: "inherit_object".to_string(),
                build_policy: json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true}
                    ]
                }),
            };
            persistence
                .apply_index_definition_mutation(&bucket, &mutation, None, None)
                .await
        }
    });
    let outcomes = futures_util::future::join_all(creates).await;

    let mut index_ids = HashSet::new();
    let mut cursors = HashSet::new();
    for outcome in outcomes {
        let IndexDefinitionMutationOutcome::Published { index, event } = outcome.unwrap() else {
            panic!("every unique concurrent index create should publish");
        };
        assert!(index_ids.insert(index.id), "index IDs must be unique");
        assert!(cursors.insert(event.id), "event cursors must be unique");
    }
    assert_eq!(index_ids.len(), 8);
    assert_eq!(cursors.len(), 8);

    let definitions = persistence
        .list_index_definitions(tenant.id, bucket.id, true)
        .await
        .unwrap();
    assert_eq!(definitions.len(), 8);
    assert_eq!(
        definitions
            .iter()
            .map(|definition| definition.id)
            .collect::<HashSet<_>>()
            .len(),
        8
    );
    let events = persistence
        .list_index_definition_events(tenant.id, bucket.id, 0, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 8);
    assert_eq!(events.last().map(|event| event.id), Some(8));
}
