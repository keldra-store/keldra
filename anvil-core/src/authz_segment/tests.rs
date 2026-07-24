use super::*;
use chrono::Utc;
use tempfile::tempdir;

mod bounded_history;
mod task_publication_fencing;

fn record(revision: i64, operation: &str) -> AuthzTupleRecord {
    AuthzTupleRecord {
        revision,
        revision_ordinal: 0,
        tenant_id: 7,
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        written_by: "node".to_string(),
        reason: "test".to_string(),
        mutation_id: uuid::Uuid::new_v4(),
        record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
        written_at: Utc::now(),
    }
}

fn tuple_record(revision: i64, object_id: &str, subject_id: &str) -> AuthzTupleRecord {
    AuthzTupleRecord {
        object_id: object_id.to_string(),
        subject_id: subject_id.to_string(),
        ..record(revision, "add")
    }
}

fn test_hash(ch: char) -> String {
    format!("blake3:{}", ch.to_string().repeat(64))
}

#[tokio::test]
async fn authz_checkpoint_stores_only_bounded_current_tuple_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let records = vec![record(2, "remove"), record(1, "add")];
    let segment_ref = write_authz_tuple_segment(&storage, 7, &records)
        .await
        .unwrap();
    assert_eq!(
        segment_ref,
        "authz_tuple_segment:tenant:7:generation:00000000000000000002"
    );

    let decoded = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decoded.header.partition_family, "authz_tuple");
    assert!(decoded.records.is_empty());
    assert_eq!(decoded.revision_checkpoints.len(), 1);
    assert_eq!(decoded.revision_checkpoints[0].tuple_record_count, 0);

    let latest = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .unwrap();
    assert!(latest.records.is_empty());
}

#[tokio::test]
async fn authz_tuple_segment_candidate_reader_returns_revision_scoped_doc_ids() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let records = [
        tuple_record(1, "alpha", "alice"),
        tuple_record(2, "beta", "bob"),
        tuple_record(3, "gamma", "alice"),
    ];
    for record in &records {
        crate::authz_journal::test_append_authz_tuple_record_unfenced(&storage, record)
            .await
            .unwrap();
    }
    crate::authz_journal::rebuild_authz_materialization_at_revision(&storage, 7, 3, 0)
        .await
        .unwrap();

    let scope = CandidateSetScope {
        root_key_hash: test_hash('0'),
        root_generation: 9,
        index_id: "index:documents".to_string(),
        index_generation: 4,
        authz_realm_id: "tenant:7".to_string(),
        authz_scope_hash: test_hash('1'),
        authz_object_namespace: "document".to_string(),
        authz_relation: "viewer".to_string(),
        authz_principal_hash: test_hash('2'),
        authz_revision: 3,
        boundary_schema_generation_hash: test_hash('3'),
        predicate_hash: test_hash('4'),
        order_hash: test_hash('5'),
    };
    let reader = AuthzSegmentCandidateReader::new(storage.clone(), 7);
    let partition_id = 44;
    let request = AuthzCandidateRequest {
        authz_scope: "tenant:7".to_string(),
        candidate_scope: scope,
        partition_id,
        subject: "user:alice".to_string(),
        relation: "viewer".to_string(),
        object_namespace: "document".to_string(),
        revision: 3,
        system_revision: 0,
        root_generation: 9,
    };

    let candidates = reader.candidate_set(request.clone()).await.unwrap();
    assert!(
        candidates.contains_doc_id(
            ObjectAuthzKey::realm_object("document", "alpha").doc_id(partition_id)
        )
    );
    assert!(
        candidates.contains_doc_id(
            ObjectAuthzKey::realm_object("document", "gamma").doc_id(partition_id)
        )
    );
    assert!(
        !candidates
            .contains_doc_id(ObjectAuthzKey::realm_object("document", "beta").doc_id(partition_id))
    );

    let decisions = reader
        .verify_page(
            request.clone(),
            vec![
                ObjectAuthzKey::realm_object("document", "alpha"),
                ObjectAuthzKey::realm_object("document", "beta"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        decisions
            .iter()
            .map(|decision| decision.allowed)
            .collect::<Vec<_>>(),
        vec![true, false]
    );

    let mut stale = request;
    stale.revision = 4;
    assert!(reader.candidate_set(stale).await.is_err());
}

#[tokio::test]
async fn authz_candidate_reader_merges_revisioned_tuple_segments() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple_record(1, "alpha", "alice"),
        tuple_record(2, "beta", "bob"),
        tuple_record(3, "gamma", "alice"),
    ] {
        crate::authz_journal::test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
        crate::authz_journal::materialize_authz_tuple_segment_at_revision(
            &storage,
            7,
            record.revision as u64,
            0,
        )
        .await
        .unwrap();
    }

    let latest = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .expect("materialized tuple writes should expose authz delta segments");
    assert_eq!(latest.header.generation, 3);
    assert_eq!(latest.header.segment_kind, "merged");
    assert_eq!(latest.records.len(), 3);
    assert_eq!(latest.revision_checkpoints.len(), 3);
    assert_eq!(latest.schema_descriptors.len(), 1);
    let catalog = crate::writer_segment_catalog::page_writer_segment_catalog_records(
        &storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(7).unwrap(),
        0,
        u64::MAX,
        10,
    )
    .await
    .unwrap()
    .records;
    assert_eq!(catalog.len(), 3);
    for (index, record) in catalog.into_iter().enumerate() {
        let segment =
            read_authz_tuple_segment_ref(&storage, 7, record.generation, &record.segment_ref)
                .await
                .unwrap()
                .unwrap();
        if index == 0 {
            assert_eq!(segment.header.segment_kind, "checkpoint");
            assert_eq!(segment.header.base_revision, 0);
            assert!(segment.header.schema_replacement);
            assert!(segment.header.relation_rule_replacement);
        } else {
            assert_eq!(segment.header.segment_kind, "delta");
            assert_eq!(segment.header.base_revision + 1, segment.header.generation);
            assert!(!segment.header.schema_replacement);
            assert!(!segment.header.relation_rule_replacement);
            assert!(segment.schema_descriptors.is_empty());
            assert!(segment.relation_rules.is_empty());
        }
        assert_eq!(segment.records.len(), 1);
        assert_eq!(segment.revision_checkpoints.len(), 1);
    }

    let scope = CandidateSetScope {
        root_key_hash: test_hash('0'),
        root_generation: 9,
        index_id: "index:documents".to_string(),
        index_generation: 4,
        authz_realm_id: "tenant:7".to_string(),
        authz_scope_hash: test_hash('1'),
        authz_object_namespace: "document".to_string(),
        authz_relation: "viewer".to_string(),
        authz_principal_hash: test_hash('2'),
        authz_revision: 3,
        boundary_schema_generation_hash: test_hash('3'),
        predicate_hash: test_hash('4'),
        order_hash: test_hash('5'),
    };
    let request = AuthzCandidateRequest {
        authz_scope: "tenant:7".to_string(),
        candidate_scope: scope,
        partition_id: 44,
        subject: "user:alice".to_string(),
        relation: "viewer".to_string(),
        object_namespace: "document".to_string(),
        revision: 3,
        system_revision: 0,
        root_generation: 9,
    };
    let reader = AuthzSegmentCandidateReader::new(storage.clone(), 7);

    let candidates = reader.candidate_set(request.clone()).await.unwrap();
    assert!(
        candidates.contains_doc_id(ObjectAuthzKey::realm_object("document", "alpha").doc_id(44))
    );
    assert!(
        candidates.contains_doc_id(ObjectAuthzKey::realm_object("document", "gamma").doc_id(44))
    );
    assert!(
        !candidates.contains_doc_id(ObjectAuthzKey::realm_object("document", "beta").doc_id(44))
    );

    let segment = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.generation, 3);

    let mut historical_request = request.clone();
    historical_request.revision = 2;
    historical_request.candidate_scope.authz_revision = 2;
    assert!(
        reader
            .candidate_set(historical_request.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("AuthzRevisionUnavailable")
    );
    let historical = reader
        .verify_page(
            historical_request,
            vec![
                ObjectAuthzKey::realm_object("document", "alpha"),
                ObjectAuthzKey::realm_object("document", "gamma"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        historical
            .iter()
            .map(|decision| decision.allowed)
            .collect::<Vec<_>>(),
        vec![true, false]
    );

    let mut stale_request = request;
    stale_request.revision = 4;
    assert!(reader.candidate_set(stale_request).await.is_err());
}

#[tokio::test]
async fn authz_candidate_reader_applies_remove_delta_without_changing_history() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple_record(1, "alpha", "alice"),
        tuple_record(2, "beta", "alice"),
        record(3, "remove"),
    ] {
        crate::authz_journal::test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
        crate::authz_journal::materialize_authz_tuple_segment_at_revision(
            &storage,
            7,
            u64::try_from(record.revision).unwrap(),
            0,
        )
        .await
        .unwrap();
    }

    let scope = CandidateSetScope {
        root_key_hash: test_hash('0'),
        root_generation: 9,
        index_id: "index:documents".to_string(),
        index_generation: 4,
        authz_realm_id: "tenant:7".to_string(),
        authz_scope_hash: test_hash('1'),
        authz_object_namespace: "document".to_string(),
        authz_relation: "viewer".to_string(),
        authz_principal_hash: test_hash('2'),
        authz_revision: 3,
        boundary_schema_generation_hash: test_hash('3'),
        predicate_hash: test_hash('4'),
        order_hash: test_hash('5'),
    };
    let request = AuthzCandidateRequest {
        authz_scope: "tenant:7".to_string(),
        candidate_scope: scope,
        partition_id: 44,
        subject: "user:alice".to_string(),
        relation: "viewer".to_string(),
        object_namespace: "document".to_string(),
        revision: 3,
        system_revision: 0,
        root_generation: 9,
    };
    let reader = AuthzSegmentCandidateReader::new(storage.clone(), 7);

    let current = reader.candidate_set(request.clone()).await.unwrap();
    assert!(!current.contains_doc_id(ObjectAuthzKey::realm_object("document", "alpha").doc_id(44)));
    assert!(current.contains_doc_id(ObjectAuthzKey::realm_object("document", "beta").doc_id(44)));

    let mut historical_request = request;
    historical_request.revision = 2;
    historical_request.candidate_scope.authz_revision = 2;
    assert!(
        reader
            .candidate_set(historical_request.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("AuthzRevisionUnavailable")
    );
    let historical = reader
        .verify_page(
            historical_request,
            vec![
                ObjectAuthzKey::realm_object("document", "alpha"),
                ObjectAuthzKey::realm_object("document", "beta"),
            ],
        )
        .await
        .unwrap();
    assert!(historical.iter().all(|decision| decision.allowed));
}
