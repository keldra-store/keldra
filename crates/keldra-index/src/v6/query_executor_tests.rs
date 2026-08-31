use super::*;
use crate::typed_json::{AggregateOperation, FieldType};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

struct Permit(usize);
impl super::super::QueryMemoryPermit for Permit {
    fn admitted_bytes(&self) -> usize {
        self.0
    }
}
fn credits(bytes: usize) -> QueryBlockCredits {
    QueryBlockCredits::from_query_permit(Box::new(Permit(bytes))).unwrap()
}

struct Loader {
    artifacts: BTreeMap<[u8; 32], Vec<u8>>,
    payload_loads: usize,
}
impl QueryArtifactLoader for Loader {
    fn query_artifact_size(
        &mut self,
        _kind: QueryArtifactKind,
        hash: [u8; 32],
    ) -> impl std::future::Future<Output = Result<usize, IndexError>> + Send {
        let size = self.artifacts.get(&hash).map(Vec::len);
        async move { size.ok_or(IndexError::Integrity) }
    }
    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, IndexError>> + Send {
        self.payload_loads += 1;
        let value = self.artifacts.get(&request.hash).cloned();
        async move { value.ok_or(IndexError::Integrity) }
    }
}

struct NoopWake;
impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}
fn ready<T>(future: impl std::future::Future<Output = T>) -> T {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("in-memory future unexpectedly yielded"),
    }
}
fn budget() -> Budget {
    Budget {
        limits: QueryExecutionLimits::default_for_memory(),
        evidence: QueryLoadEvidence::default(),
        heap_bytes: 0,
    }
}
fn partition(index: u64) -> ProjectionPartitionIdentity {
    ProjectionPartitionIdentity::new([1; 32], index, [2; 32], 3, 4, index).unwrap()
}
fn candidate(partition: ProjectionPartitionIdentity, covered: u64) -> QueryAdmissionCandidate {
    QueryAdmissionCandidate {
        partition,
        handoff_lineage_id: [7; 32],
        covered_through_source_position: covered,
        document: StableDocumentKey::from_bytes([8; 32]).unwrap(),
        material_source_version: covered,
        current_source_version: covered,
        source_path: "objects/candidate.json".into(),
        result_path: "results/candidate.json".into(),
        result_version: covered,
    }
}

#[test]
fn artifact_memory_refusal_happens_before_payload_loader() {
    let bytes = vec![1; 1024];
    let hash = *blake3::hash(&bytes).as_bytes();
    let mut loader = Loader {
        artifacts: [(hash, bytes)].into(),
        payload_loads: 0,
    };
    let mut credits = credits(512);
    let mut budget = budget();
    assert!(matches!(
        ready(load_pre_admitted(
            &mut loader,
            QueryArtifactKind::Block,
            hash,
            2048,
            &mut credits,
            &mut budget
        )),
        Err(IndexError::ResourceLimit { .. })
    ));
    assert_eq!(loader.payload_loads, 0);
}

#[test]
fn sequential_block_scans_reuse_transient_heap_credits() {
    let limits = QueryBlockLimits::default_for_memory();
    let recipe = RecipeIdentity::new([3; 32]).unwrap();
    let records = (0..64u32)
        .map(|value| super::super::QueryBlockRecord {
            key: value.to_be_bytes().to_vec(),
            value: vec![value as u8; 128],
        })
        .collect::<Vec<_>>();
    let mut encoding_credits = credits(1024 * 1024);
    let encoded = super::super::encode_query_block(
        QueryBlockKind::Point,
        recipe,
        &records,
        limits,
        &mut encoding_credits,
    )
    .unwrap();
    let hash = encoded.descriptor.hash;
    let mut loader = Loader {
        artifacts: [(hash, encoded.bytes)].into(),
        payload_loads: 0,
    };
    let resident_bytes = encoded.descriptor.records as usize * std::mem::size_of::<OwnedRecord>()
        + encoded.descriptor.encoded_bytes as usize;
    let admitted = resident_bytes + encoded.descriptor.encoded_bytes as usize * 2;
    let mut credits = credits(admitted);
    let initial = credits.remaining();
    let mut budget = budget();

    for _ in 0..64 {
        let (loaded, charged) = ready(load_block(
            &mut loader,
            &encoded.descriptor,
            limits,
            &mut credits,
            &mut budget,
        ))
        .unwrap();
        assert_eq!(loaded.len(), records.len());
        drop(loaded);
        budget.release_heap(&mut credits, charged).unwrap();
        assert_eq!(credits.remaining(), initial);
        assert_eq!(budget.heap_bytes, 0);
    }
}

#[test]
fn unequal_partition_roots_can_prove_one_common_cut() {
    let cut = QueryCommonCut {
        through_atomic_position: 20,
    };
    for (index, root_cut, next_newer) in [(1, 20, None), (2, 17, Some(21))] {
        let root = ProjectionQueryStreamRoot {
            stream_root_hash: [index as u8; 32],
            run_count: 1,
            first_sequence: 1,
            last_sequence: 1,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: root_cut,
        };
        PinnedPartitionQueryRoot {
            partition: partition(index),
            physical_catalog_generation: [4; 32],
            root,
            cut_proof: QueryRootCutProof {
                common_cut: cut,
                selected_stream_root_hash: root.stream_root_hash,
                next_newer_through_atomic_position: next_newer,
            },
            handoff_lineage_id: [5; 32],
        }
        .validate_at(cut)
        .unwrap();
    }
}

#[test]
fn handoff_dedup_selects_furthest_source_position() {
    let mut selected = BTreeMap::new();
    let mut credits = credits(4096);
    let mut budget = budget();
    select_handoff_candidate(
        &mut selected,
        candidate(partition(1), 9),
        &mut credits,
        &mut budget,
    )
    .unwrap();
    select_handoff_candidate(
        &mut selected,
        candidate(partition(2), 12),
        &mut credits,
        &mut budget,
    )
    .unwrap();
    assert_eq!(
        selected
            .values()
            .next()
            .unwrap()
            .covered_through_source_position,
        12
    );
}

#[test]
fn handoff_replacement_owns_exactly_one_candidate_charge() {
    let mut selected = BTreeMap::new();
    let mut credits = credits(16 * 1024);
    let mut budget = budget();
    let mut first = candidate(partition(1), 9);
    first.source_path = "a".into();
    first.result_path = "b".into();
    select_handoff_candidate(&mut selected, first.clone(), &mut credits, &mut budget).unwrap();
    assert_eq!(
        budget.heap_bytes,
        resident_selected_candidate_bytes(&first).unwrap()
    );

    let mut replacement = candidate(partition(2), 12);
    replacement.source_path = "objects/a-much-longer-source-path.json".into();
    replacement.result_path = "results/a-much-longer-result-path.json".into();
    select_handoff_candidate(
        &mut selected,
        replacement.clone(),
        &mut credits,
        &mut budget,
    )
    .unwrap();
    assert_eq!(
        budget.heap_bytes,
        resident_selected_candidate_bytes(&replacement).unwrap()
    );

    let ignored = candidate(partition(3), 8);
    select_handoff_candidate(&mut selected, ignored, &mut credits, &mut budget).unwrap();
    assert_eq!(
        budget.heap_bytes,
        resident_selected_candidate_bytes(&replacement).unwrap()
    );
}

#[test]
fn absent_predicate_matches_the_live_membership_universe_only() {
    let live = StableDocumentKey::from_bytes([1; 32]).unwrap();
    let deleted = StableDocumentKey::from_bytes([2; 32]).unwrap();
    let gate = |document, live| QueryDocumentGate {
        document,
        material_source_version: 1,
        current_source_version: 1,
        live,
        source_path: Some("objects/a.json".into()),
        result_path: Some("objects/a.json".into()),
        result_version: 1,
    };
    let gates = [(live, gate(live, true)), (deleted, gate(deleted, false))].into();
    assert_eq!(match_all_live_documents(&gates), [live].into());
}

#[test]
fn logical_order_tie_break_does_not_depend_on_handoff_partition() {
    let first = StableDocumentKey::from_bytes([1; 32]).unwrap();
    let second = StableDocumentKey::from_bytes([2; 32]).unwrap();
    let mut candidates = vec![
        QueryCandidate {
            partition: partition(1),
            document: second,
            material_source_version: 1,
        },
        QueryCandidate {
            partition: partition(99),
            document: first,
            material_source_version: 1,
        },
    ];
    order_candidates(&mut candidates, &[], &BTreeMap::new(), &BTreeMap::new()).unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.document)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
}

#[test]
fn unauthorized_values_cannot_leak_into_facets_or_aggregates() {
    let partition = partition(1);
    let admitted = QueryCandidate {
        partition,
        document: StableDocumentKey::from_bytes([10; 32]).unwrap(),
        material_source_version: 1,
    };
    let denied = StableDocumentKey::from_bytes([11; 32]).unwrap();
    let field_id = FieldId::new(0);
    let recipe = RecipeIdentity::new([9; 32]).unwrap();
    let field = FieldSchema {
        id: field_id,
        name: "value".into(),
        source_selector: "/value".into(),
        field_type: FieldType::Keyword,
        cardinality: Cardinality::Multi,
        allow_missing: true,
        allow_null: false,
        collation: crate::typed_json::Collation::BinaryUtf8,
        capabilities: FieldCapabilities::FACET.union(FieldCapabilities::AGGREGATE),
        analyzer: None,
        date_format: None,
    };
    let contracts = [(field_id, QueryFieldBinding { field, recipe })].into();
    let values = [
        (
            (partition, admitted.document, recipe),
            Some(vec![ScalarValue::String("visible".into())]),
        ),
        (
            (partition, denied, recipe),
            Some(vec![ScalarValue::String("secret".into())]),
        ),
    ]
    .into();
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let facets = facet_candidates(
        &[admitted],
        &[FacetRequest {
            field_id,
            limit: 10,
        }],
        &contracts,
        &values,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    assert_eq!(
        facets[0].buckets[0].value,
        ScalarValue::String("visible".into())
    );
    let aggregates = aggregate_candidates(
        &[admitted],
        &[AggregateRequest {
            field_id,
            operation: AggregateOperation::Count,
        }],
        &contracts,
        &values,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    assert_eq!(aggregates[0].contributing_count, 1);
}

#[test]
fn repeated_values_facet_once_but_aggregate_every_occurrence() {
    let partition = partition(1);
    let admitted = QueryCandidate {
        partition,
        document: StableDocumentKey::from_bytes([10; 32]).unwrap(),
        material_source_version: 1,
    };
    let field_id = FieldId::new(0);
    let recipe = RecipeIdentity::new([9; 32]).unwrap();
    let field = FieldSchema {
        id: field_id,
        name: "value".into(),
        source_selector: "/value".into(),
        field_type: FieldType::SignedInteger,
        cardinality: Cardinality::Multi,
        allow_missing: true,
        allow_null: false,
        collation: crate::typed_json::Collation::BinaryUtf8,
        capabilities: FieldCapabilities::FACET.union(FieldCapabilities::AGGREGATE),
        analyzer: None,
        date_format: None,
    };
    let contracts = [(field_id, QueryFieldBinding { field, recipe })].into();
    let values = [(
        (partition, admitted.document, recipe),
        Some(vec![ScalarValue::Signed(2), ScalarValue::Signed(2)]),
    )]
    .into();
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let facets = facet_candidates(
        &[admitted],
        &[FacetRequest {
            field_id,
            limit: 10,
        }],
        &contracts,
        &values,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    assert_eq!(facets[0].buckets.len(), 1);
    assert_eq!(facets[0].buckets[0].count, 1);

    let aggregates = aggregate_candidates(
        &[admitted],
        &[
            AggregateRequest {
                field_id,
                operation: AggregateOperation::Count,
            },
            AggregateRequest {
                field_id,
                operation: AggregateOperation::Sum,
            },
        ],
        &contracts,
        &values,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    assert_eq!(aggregates[0].contributing_count, 2);
    assert_eq!(aggregates[1].contributing_count, 2);
    assert_eq!(aggregates[1].value, Some(ScalarValue::Signed(4)));
}
