use super::*;
#[test]
fn selector_matches_prefix_and_content_type() {
    let object = object("docs/a.txt", Some("text/plain"));
    assert!(selector_matches(
        &serde_json::json!({"prefix": "docs/"}),
        &object
    ));
    assert!(!selector_matches(
        &serde_json::json!({"prefix": "logs/"}),
        &object
    ));
    assert!(selector_matches(
        &serde_json::json!({"content_type": "text/plain"}),
        &object
    ));
}

#[tokio::test]
async fn vector_text_extraction_uses_only_explicit_test_provider() {
    let production_definition =
        VectorIndexDefinition::from_json(&test_vector_definition("configured_provider", 4))
            .unwrap();
    let test_registry = EmbeddingProviderRegistry::for_tests(true);

    let missing_provider = extract_vectors(
        &production_definition.extractor,
        b"hello",
        &production_definition,
        &test_registry,
    )
    .await;
    assert!(missing_provider.vectors.is_empty());
    assert_eq!(
        missing_provider.diagnostics[0].code,
        "EmbeddingProviderNotConfigured"
    );

    let definition =
        VectorIndexDefinition::from_json(&test_vector_definition("test_only", 4)).unwrap();

    let disabled = extract_vectors(
        &definition.extractor,
        b"hello",
        &definition,
        &EmbeddingProviderRegistry::for_tests(false),
    )
    .await;
    assert!(disabled.vectors.is_empty());
    assert_eq!(
        disabled.diagnostics[0].code,
        "TestOnlyEmbeddingProviderDisabled"
    );

    let enabled =
        extract_vectors(&definition.extractor, b"hello", &definition, &test_registry).await;
    assert_eq!(enabled.vectors.len(), 1);
    assert_eq!(enabled.vectors[0].values.len(), 4);
}

#[tokio::test]
async fn vector_text_extraction_uses_configured_command_provider() {
    let mut definition_value = test_vector_definition("configured_provider", 4);
    definition_value["embedding"]["model_version"] = serde_json::json!("v1");
    let definition = VectorIndexDefinition::from_json(&definition_value).unwrap();
    let config = crate::config::Config {
            vector_embedding_providers_json: serde_json::json!({
                "providers": [{
                    "name": "configured_provider",
                    "kind": "command_json",
                    "command": "/bin/sh",
                    "args": [
                        "-c",
                        "cat >/dev/null; printf '%s' '{\"model_version\":\"v1\",\"vectors\":[{\"values\":[0.5,0.5,0.5,0.5],\"chunk_id\":7,\"source_start\":1,\"source_len\":5}]}'"
                    ],
                    "timeout_ms": 5000
                }]
            })
            .to_string(),
            ..crate::config::Config::default()
        };
    let registry = EmbeddingProviderRegistry::from_config(&config).unwrap();

    let extraction = extract_vectors(&definition.extractor, b"hello", &definition, &registry).await;

    assert!(
        extraction.diagnostics.is_empty(),
        "{:?}",
        extraction.diagnostics
    );
    assert_eq!(extraction.vectors.len(), 1);
    assert_eq!(extraction.vectors[0].chunk_id, 7);
    assert_eq!(extraction.vectors[0].source_start, 1);
    assert_eq!(extraction.vectors[0].source_len, 5);
    assert_eq!(extraction.vectors[0].values, vec![0.5, 0.5, 0.5, 0.5]);
}

fn test_vector_definition(provider: &str, dimension: u16) -> JsonValue {
    serde_json::json!({
        "schema": crate::formats::vector::VECTOR_INDEX_SCHEMA,
        "source": {"kind": "object_current", "prefix": "docs/"},
        "extractor": {"kind": "object_body_utf8"},
        "embedding": {
            "provider": provider,
            "model": "test-text-embedding",
            "dimension": dimension,
            "modality": "text",
            "normalisation": "unit_l2",
            "chunking": {"strategy": "whole_object"}
        },
        "ann": {
            "algorithm": "hnsw",
            "metric": "cosine"
        }
    })
}

#[test]
fn extractor_supports_body_key_and_content_type_fields() {
    let object = object("docs/a.txt", Some("text/plain"));
    let fields = extract_text_fields(
        &serde_json::json!({
            "fields": [
                {"source": "object_body_utf8"},
                {"source": "object_key"},
                {"source": "content_type"}
            ]
        }),
        &object,
        b"alpha body",
    );
    assert_eq!(
        fields
            .fields
            .into_iter()
            .map(|field| field.text)
            .collect::<Vec<_>>(),
        vec!["alpha body", "docs/a.txt", "text/plain"]
    );
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn extractor_supports_json_pointer_and_metadata_fields() {
    let mut object = object("docs/a.json", Some("application/json"));
    object.user_meta = Some(serde_json::json!({
        "owner": "alice",
        "nested": {"department": "legal"}
    }));
    let fields = extract_text_fields(
        &serde_json::json!({
            "fields": [
                {"source": "json_pointer", "pointer": "/summary"},
                {"source": "metadata_field", "field": "owner"},
                {"source": "metadata_field", "field": "/nested/department"}
            ]
        }),
        &object,
        br#"{"summary":"lease renewal due","ignored":true}"#,
    );
    assert_eq!(
        fields
            .fields
            .into_iter()
            .map(|field| field.text)
            .collect::<Vec<_>>(),
        vec!["lease renewal due", "alice", "legal"]
    );
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn extractor_reports_missing_json_pointer() {
    let object = object("docs/a.json", Some("application/json"));
    let fields = extract_text_fields(
        &serde_json::json!({"source": "json_pointer", "pointer": "/missing"}),
        &object,
        br#"{"summary":"present"}"#,
    );
    assert!(fields.fields.is_empty());
    assert_eq!(fields.diagnostics[0].code, "JsonPointerNotFound");
}

#[test]
fn extractor_supports_media_transcript_for_binary_payloads() {
    let object = object("media/audio/a.bin", Some("audio/mpeg"));
    let fields = extract_text_fields(
        &serde_json::json!({"source": "media_transcript"}),
        &object,
        b"\x00\x01audio payload",
    );
    assert_eq!(fields.fields.len(), 1);
    assert!(fields.fields[0].text.contains("Audio media object"));
    assert!(fields.fields[0].text.contains("media/audio/a.bin"));
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn extractor_supports_personaldb_table_column_rows() {
    let object = object("rows/items/1.json", Some("application/json"));
    let fields = extract_text_fields(
        &serde_json::json!({
            "source": "personaldb_table_column",
            "table": "items",
            "column": "name"
        }),
        &object,
        br#"{"table_name":"items","columns":{"id":1,"name":"alpha repair order"}}"#,
    );
    assert_eq!(fields.fields[0].text, "alpha repair order");
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn extractor_skips_non_matching_personaldb_table_column_rows() {
    let object = object("rows/items/1.json", Some("application/json"));
    let fields = extract_text_fields(
        &serde_json::json!({
            "source": "personaldb_table_column",
            "table": "invoices",
            "column": "name"
        }),
        &object,
        br#"{"table_name":"items","columns":{"id":1,"name":"alpha repair order"}}"#,
    );
    assert!(fields.fields.is_empty());
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn extractor_reports_missing_personaldb_table_column() {
    let object = object("rows/items/1.json", Some("application/json"));
    let fields = extract_text_fields(
        &serde_json::json!({
            "source": "personaldb_table_column",
            "column": "name"
        }),
        &object,
        br#"{"table_name":"items","columns":{"id":1}}"#,
    );
    assert!(fields.fields.is_empty());
    assert_eq!(fields.diagnostics[0].code, "PersonalDbTableColumnNotFound");
}

#[test]
fn extractor_supports_personaldb_table_column_json_pointer() {
    let object = object("rows/items/1.json", Some("application/json"));
    let fields = extract_text_fields(
        &serde_json::json!({
            "source": "personaldb_table_column",
            "table": "items",
            "column": "/new_values/name"
        }),
        &object,
        br#"{"table":"items","new_values":{"id":1,"name":"beta inspection note"}}"#,
    );
    assert_eq!(fields.fields[0].text, "beta inspection note");
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn extractor_supports_git_blob_text_as_utf8_payload() {
    let object = object("src/lib.rs", Some("text/plain"));
    let fields = extract_text_fields(
        &serde_json::json!({"source": "git_blob_text"}),
        &object,
        b"fn main() {}",
    );
    assert_eq!(fields.fields[0].text, "fn main() {}");
    assert!(fields.diagnostics.is_empty());
}

#[test]
fn typed_json_row_extracts_body_metadata_and_source_id() {
    let mut object = object("queue/item-1.json", Some("application/json"));
    object.user_meta = Some(serde_json::json!({"owner": "alice"}));
    object.authz_revision = 12;
    let bucket = Bucket {
        id: 1,
        tenant_id: 7,
        name: "jobs".to_string(),
        region: "local".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    };
    let index = index_definition(serde_json::json!({
        "source_kind": "object_current",
        "fields": [
            {"name": "state", "extractor": "/state"},
            {"name": "priority", "extractor": "object_body_json_pointer:/priority"},
            {"name": "owner", "extractor": "object_user_metadata_json_pointer:/owner"},
            {"name": "object_key", "extractor": "object_key"}
        ]
    }));
    let definition = parse_typed_json_build_definition(&index).unwrap();
    let row = typed_json_row_from_object(
        &bucket,
        &definition,
        &object,
        &serde_json::json!({"state": "pending", "priority": 10}),
    )
    .unwrap();

    assert_eq!(row.values["state"], "pending");
    assert_eq!(row.values["priority"], 10);
    assert_eq!(row.values["owner"], "alice");
    assert_eq!(row.values["object_key"], "queue/item-1.json");
    assert_eq!(row.authz_revision, 12);
    assert!(!row.source_id_binary.is_empty());
    assert!(row.encoded_values.contains_key("priority"));
}

#[test]
fn frozen_typed_json_extraction_uses_supplied_object_and_payload() {
    let mut object = object("queue/frozen.json", Some("application/json"));
    object.user_meta = Some(serde_json::json!({"owner": "frozen-owner"}));
    let bucket = Bucket {
        id: 1,
        tenant_id: 7,
        name: "jobs".to_string(),
        region: "local".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    };
    let index = index_definition(serde_json::json!({
        "source_kind": "object_current",
        "fields": [
            {"name": "state", "extractor": "/state"},
            {"name": "owner", "extractor": "object_user_metadata_json_pointer:/owner"}
        ]
    }));

    let row = extract_frozen_typed_json_row(&bucket, &index, &object, br#"{"state":"committed"}"#)
        .unwrap()
        .unwrap();

    assert_eq!(row.values["state"], "committed");
    assert_eq!(row.values["owner"], "frozen-owner");
    assert_eq!(row.object_version_id, object.version_id.to_string());
}

#[test]
fn typed_json_required_field_missing_fails_extraction() {
    let object = object("queue/item-1.json", Some("application/json"));
    let bucket = Bucket {
        id: 1,
        tenant_id: 7,
        name: "jobs".to_string(),
        region: "local".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    };
    let index = index_definition(serde_json::json!({
        "source_kind": "object_current",
        "fields": [
            {"name": "state", "extractor": "/state", "required": true}
        ]
    }));
    let definition = parse_typed_json_build_definition(&index).unwrap();
    let err = typed_json_row_from_object(&bucket, &definition, &object, &serde_json::json!({}))
        .unwrap_err();
    assert!(err.to_string().contains("required field missing"));
}

#[test]
fn typed_json_append_row_extracts_payload_and_metadata() {
    let bucket = Bucket {
        id: 1,
        tenant_id: 7,
        name: "events".to_string(),
        region: "local".to_string(),
        created_at: Utc::now(),
        is_public_read: false,
    };
    let stream = AppendStream {
        id: 3,
        tenant_id: 7,
        bucket_id: 1,
        bucket_name: "events".to_string(),
        stream_key: "audit".to_string(),
        stream_id: uuid::Uuid::from_bytes([3; 16]),
        created_at: Utc::now(),
        sealed_at: None,
        segment_hash: None,
    };
    let record = AppendStreamRecord {
        id: 4,
        stream_id: stream.id,
        record_sequence: 2,
        payload_hash: format!("sha256:{}", hex::encode([4u8; 32])),
        payload_object_ref: CoreObjectRef::test_unlocated(
            format!("sha256:{}", hex::encode([4u8; 32])),
            64,
            "manifest:event".to_string(),
        ),
        payload_size: 64,
        content_type: Some("application/json".to_string()),
        user_meta: Some(serde_json::json!({"actor": "alice"})),
        authenticated_principal: "tenant/7/principal/alice".to_string(),
        created_at: Utc::now(),
    };
    let index = index_definition(serde_json::json!({
        "source_kind": "append_record",
        "fields": [
            {"name": "stream", "extractor": "append_stream_key"},
            {"name": "sequence", "extractor": "append_record_sequence"},
            {"name": "state", "extractor": "append_payload_json_pointer:/state"},
            {"name": "actor", "extractor": "append_user_metadata_json_pointer:/actor"}
        ]
    }));
    let definition = parse_typed_json_build_definition(&index).unwrap();
    let row = typed_json_row_from_append_record(
        &bucket,
        &definition,
        &stream,
        &record,
        &serde_json::json!({"state": "sent"}),
    )
    .unwrap();

    assert_eq!(row.object_key, "audit");
    assert_eq!(row.values["stream"], "audit");
    assert_eq!(row.values["sequence"], 2);
    assert_eq!(row.values["state"], "sent");
    assert_eq!(row.values["actor"], "alice");
    assert!(!row.source_id_binary.is_empty());
}

fn object(key: &str, content_type: Option<&str>) -> Object {
    Object {
        id: 1,
        tenant_id: 1,
        bucket_id: 1,
        key: key.to_string(),
        kind: crate::object_links::ObjectEntryKind::Blob,
        content_hash: hex::encode([1; 32]),
        size: 10,
        etag: "etag".to_string(),
        content_type: content_type.map(ToOwned::to_owned),
        version_id: uuid::Uuid::from_bytes([1; 16]),
        mutation_id: uuid::Uuid::from_bytes([2; 16]),
        index_policy_snapshot: String::new(),
        user_metadata_hash: String::new(),
        authz_revision: 0,
        record_hash: String::new(),
        created_at: Utc::now(),
        deleted_at: None,
        storage_class: None,
        user_meta: None,
        shard_map: None,
        checksum: None,
        link: None,
    }
}

fn index_definition(build_policy: JsonValue) -> IndexDefinition {
    IndexDefinition {
        id: 1,
        tenant_id: 7,
        bucket_id: 1,
        name: "typed".to_string(),
        kind: "typed_json".to_string(),
        selector: JsonValue::Null,
        extractor: JsonValue::Null,
        authorization_mode: "inherit_object".to_string(),
        build_policy,
        enabled: true,
        version: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}
