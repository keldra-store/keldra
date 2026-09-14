use super::*;
use crate::typed_json::{AggregateOperation, Cardinality, FieldCapabilities, FieldType};
use bytes::Bytes;
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
    artifacts: BTreeMap<[u8; 32], Bytes>,
    payload_loads: usize,
}
impl QueryArtifactLoader for Loader {
    fn load_query_artifact(
        &mut self,
        request: QueryArtifactLoad,
    ) -> impl std::future::Future<Output = Result<Bytes, IndexError>> + Send {
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
    Budget::new(QueryExecutionLimits::default_for_memory())
}

#[test]
fn canonical_run_directory_is_searched_without_materializing_an_index() {
    let first = RecipeIdentity::new([1; 32]).unwrap();
    let second = RecipeIdentity::new([2; 32]).unwrap();
    let block = |kind, recipe, key: u8, hash: u8| QueryBlockDescriptor {
        kind,
        recipe,
        minimum_key: vec![key],
        maximum_key: vec![key],
        hash: [hash; 32],
        encoded_bytes: 64,
        records: 1,
    };
    let run = ProjectionQueryRunDescriptor {
        partition: partition(1),
        physical_catalog_generation: [3; 32],
        sequence: 1,
        source_start_offset: 1,
        next_offset: 2,
        through_atomic_position: 1,
        blocks: vec![
            block(QueryBlockKind::Posting, first, 1, 1),
            block(QueryBlockKind::Gate, first, 1, 2),
            block(QueryBlockKind::Gate, first, 2, 3),
            block(QueryBlockKind::Gate, second, 1, 4),
        ],
    };
    run.validate(QueryBlockLimits::default_for_memory())
        .unwrap();

    let matching = matching_run_blocks(&run, QueryBlockKind::Gate, first);

    assert_eq!(matching.len(), 2);
    assert_eq!(matching[0].hash, [2; 32]);
    assert_eq!(matching[1].hash, [3; 32]);
    assert!(matching_run_blocks(&run, QueryBlockKind::Point, first).is_empty());
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
        canonical_source_path: None,
        result_path: "results/candidate.json".into(),
        result_version: covered,
    }
}

fn authorized(candidate: QueryCandidate) -> AuthorizedQueryCandidate {
    AuthorizedQueryCandidate {
        candidate: QueryAdmissionCandidate {
            partition: candidate.partition,
            handoff_lineage_id: [7; 32],
            covered_through_source_position: 1,
            document: candidate.document,
            material_source_version: candidate.material_source_version,
            current_source_version: candidate.material_source_version,
            source_path: "objects/candidate.json".into(),
            canonical_source_path: None,
            result_path: "results/candidate.json".into(),
            result_version: candidate.material_source_version,
        },
    }
}

#[test]
fn artifact_memory_refusal_happens_before_payload_loader() {
    let bytes = vec![1; 1024];
    let hash = *crate::profiled_blake3_hash!(&bytes).as_bytes();
    let mut loader = Loader {
        artifacts: [(hash, Bytes::from(bytes))].into(),
        payload_loads: 0,
    };
    let mut credits = credits(512);
    let mut budget = budget();
    assert!(matches!(
        ready(load_exact_pre_admitted(
            &mut loader,
            QueryArtifactKind::Block,
            hash,
            1024,
            &mut credits,
            &mut budget
        )),
        Err(IndexError::ResourceLimit { .. })
    ));
    assert_eq!(loader.payload_loads, 0);
    assert_eq!(credits.required_query_lease_bytes(), Some(1024));
}

#[test]
fn preverified_artifact_loader_is_not_rehashed() {
    let hash = [9; 32];
    let bytes = vec![1; 32];
    let artifact = Bytes::from(bytes.clone());
    let artifact_pointer = artifact.as_ptr();
    let mut loader = Loader {
        artifacts: [(hash, artifact)].into(),
        payload_loads: 0,
    };
    let mut credits = credits(bytes.len());
    let mut budget = budget();

    let loaded = ready(load_exact_pre_admitted(
        &mut loader,
        QueryArtifactKind::Block,
        hash,
        bytes.len(),
        &mut credits,
        &mut budget,
    ))
    .unwrap();
    assert_eq!(loaded, bytes);
    assert_eq!(loaded.as_ptr(), artifact_pointer);
    assert_eq!(loader.payload_loads, 1);
}

#[test]
fn logical_heap_limit_does_not_report_query_credit_exhaustion() {
    let mut limits = QueryExecutionLimits::default_for_memory();
    limits.maximum_heap_bytes = 8;
    let mut budget = Budget::new(limits);
    let mut credits = credits(1024);

    assert!(matches!(
        budget.reserve_heap(&mut credits, 9),
        Err(IndexError::ResourceLimit { .. })
    ));
    assert_eq!(credits.required_query_lease_bytes(), None);
    assert_eq!(credits.remaining(), 1024);
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
        artifacts: [(hash, Bytes::from(encoded.bytes))].into(),
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
        assert_eq!(budget.heap_bytes(), 0);
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
            stream_root_encoded_bytes: 1,
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
fn query_snapshot_identity_pins_the_exact_canonical_root_vector() {
    let cut = QueryCommonCut {
        through_atomic_position: 20,
    };
    let pin = |index: u64, root_hash: u8| {
        let root = ProjectionQueryStreamRoot {
            stream_root_hash: [root_hash; 32],
            stream_root_encoded_bytes: 1,
            run_count: 1,
            first_sequence: 1,
            last_sequence: 1,
            source_start_offset: 1,
            next_offset: 2,
            through_atomic_position: 20,
        };
        PinnedPartitionQueryRoot {
            partition: partition(index),
            physical_catalog_generation: [4; 32],
            root,
            cut_proof: QueryRootCutProof {
                common_cut: cut,
                selected_stream_root_hash: root.stream_root_hash,
                next_newer_through_atomic_position: None,
            },
            handoff_lineage_id: [5; 32],
        }
    };
    let first = pin(1, 7);
    let second = pin(2, 8);
    let identity = query_snapshot_identity(cut, &[first, second]).unwrap();

    assert_eq!(
        query_snapshot_identity(cut, &[first, second]).unwrap(),
        identity
    );
    assert_ne!(
        query_snapshot_identity(cut, &[first, pin(2, 9)]).unwrap(),
        identity
    );
    assert!(query_snapshot_identity(cut, &[second, first]).is_err());
}

#[test]
fn query_snapshot_binding_distinguishes_logical_definitions_on_the_same_roots() {
    let cut = QueryCommonCut {
        through_atomic_position: 20,
    };
    let root = ProjectionQueryStreamRoot {
        stream_root_hash: [7; 32],
        stream_root_encoded_bytes: 1,
        run_count: 1,
        first_sequence: 1,
        last_sequence: 1,
        source_start_offset: 1,
        next_offset: 2,
        through_atomic_position: 20,
    };
    let pin = PinnedPartitionQueryRoot {
        partition: partition(1),
        physical_catalog_generation: [4; 32],
        root,
        cut_proof: QueryRootCutProof {
            common_cut: cut,
            selected_stream_root_hash: root.stream_root_hash,
            next_newer_through_atomic_position: None,
        },
        handoff_lineage_id: [5; 32],
    };
    let identity = query_snapshot_identity(cut, &[pin]).unwrap();
    let snapshot = |logical_definition_version| ValidatedQuerySnapshot {
        identity,
        common_cut: cut,
        pins: vec![pin],
        logical: LogicalProjectionBinding {
            logical_index_id: 1,
            logical_definition_version,
            family_id: [1; 32],
            physical_catalog_generation: [4; 32],
            membership: RecipeIdentity::new([3; 32]).unwrap(),
            fields: Vec::new(),
        },
        catalog_lineage: vec![[4; 32]],
        recipe_catalog_proofs: Vec::new(),
        manifests: Vec::new(),
    };
    let first = snapshot(1);
    let second = snapshot(2);

    assert_eq!(first.identity(), second.identity());
    assert!(!first.has_same_binding(&second));
    assert!(first.matches_binding(
        &first.logical,
        &first.catalog_lineage,
        &first.recipe_catalog_proofs,
    ));
    assert!(!first.matches_binding(
        &second.logical,
        &second.catalog_lineage,
        &second.recipe_catalog_proofs,
    ));
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
        budget.heap_bytes(),
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
        budget.heap_bytes(),
        resident_selected_candidate_bytes(&replacement).unwrap()
    );

    let ignored = candidate(partition(3), 8);
    select_handoff_candidate(&mut selected, ignored, &mut credits, &mut budget).unwrap();
    assert_eq!(
        budget.heap_bytes(),
        resident_selected_candidate_bytes(&replacement).unwrap()
    );
}

struct BatchAdmission {
    calls: usize,
    reorder: bool,
}

impl QueryCandidateAdmission for BatchAdmission {
    fn admit_snapshot_current_authorized_batch(
        &mut self,
        contexts: Vec<QueryAdmissionContext>,
    ) -> impl std::future::Future<Output = Result<Vec<Option<AuthorizedQueryCandidate>>, IndexError>>
    + Send {
        self.calls += 1;
        let reorder = self.reorder;
        async move {
            let mut output = contexts
                .into_iter()
                .enumerate()
                .map(|(index, context)| {
                    (index % 3 != 1).then(|| AuthorizedQueryCandidate {
                        candidate: context.candidate,
                    })
                })
                .collect::<Vec<_>>();
            if reorder {
                output.swap(0, 2);
            }
            Ok(output)
        }
    }
}

fn selected_candidates(
    count: u8,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> BTreeMap<StableDocumentKey, QueryAdmissionCandidate> {
    let mut selected = BTreeMap::new();
    for index in 0..count {
        let mut value = candidate(partition(1), u64::from(index) + 1);
        value.document = StableDocumentKey::from_bytes([index + 1; 32]).unwrap();
        value.source_path = format!("objects/source-{index}.json");
        value.result_path = format!("objects/result-{index}.json");
        select_handoff_candidate(&mut selected, value, credits, budget).unwrap();
    }
    selected
}

#[test]
fn sixty_four_candidates_use_one_aligned_admission_batch_with_denials() {
    let mut credits = credits(1024 * 1024);
    let mut budget = budget();
    let selected = selected_candidates(64, &mut credits, &mut budget);
    let mut admission = BatchAdmission {
        calls: 0,
        reorder: false,
    };
    let (authorized, candidates) = ready(authorize_selected_candidates(
        &mut admission,
        selected,
        1,
        2,
        QueryCommonCut {
            through_atomic_position: 3,
        },
        &mut credits,
        &mut budget,
        None,
    ))
    .unwrap();

    assert_eq!(admission.calls, 1);
    assert_eq!(authorized.len(), 43);
    assert_eq!(candidates.len(), 43);
    for candidate in candidates {
        let index = usize::from(candidate.document.bytes()[0] - 1);
        assert_ne!(index % 3, 1);
        assert_eq!(
            authorized
                .get(&(candidate.partition, candidate.document))
                .unwrap()
                .candidate
                .document,
            candidate.document
        );
    }
}

#[test]
fn reordered_admission_batch_is_rejected() {
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let selected = selected_candidates(3, &mut credits, &mut budget);
    let mut admission = BatchAdmission {
        calls: 0,
        reorder: true,
    };
    assert!(matches!(
        ready(authorize_selected_candidates(
            &mut admission,
            selected,
            1,
            2,
            QueryCommonCut {
                through_atomic_position: 3,
            },
            &mut credits,
            &mut budget,
            None,
        )),
        Err(IndexError::Integrity)
    ));
}

#[test]
fn natural_order_admission_stops_after_requested_visible_candidates() {
    let mut credits = credits(1024 * 1024);
    let mut budget = budget();
    let selected = selected_candidates(64, &mut credits, &mut budget);
    let mut admission = BatchAdmission {
        calls: 0,
        reorder: false,
    };
    let (authorized, candidates) = ready(authorize_selected_candidates(
        &mut admission,
        selected,
        1,
        2,
        QueryCommonCut {
            through_atomic_position: 3,
        },
        &mut credits,
        &mut budget,
        Some(5),
    ))
    .unwrap();

    assert_eq!(admission.calls, 3);
    assert_eq!(authorized.len(), 5);
    assert_eq!(candidates.len(), 5);
    assert_eq!(candidates.last().unwrap().document.bytes()[0], 8);
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
        canonical_source_path: None,
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
    let candidates = [
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
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let mut collector = BoundedCandidateCollector::new(
        candidates.len(),
        &[],
        &BTreeMap::new(),
        None,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    let columns = PartitionValueColumns::new(Vec::new(), 0);
    for candidate in candidates {
        let authorized = authorized(candidate);
        budget
            .reserve_heap(
                &mut credits,
                resident_authorized_candidate_bytes(&authorized).unwrap(),
            )
            .unwrap();
        collector
            .observe(
                candidate,
                authorized,
                0,
                &columns,
                &mut credits,
                &mut budget,
            )
            .unwrap();
    }
    let (candidates, _) = collector.finish(&mut credits, &mut budget).unwrap();
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.candidate.document)
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
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let mut reducers = QueryValueReducers::new(
        &[FacetRequest {
            field_id,
            limit: 10,
        }],
        &[AggregateRequest {
            field_id,
            operation: AggregateOperation::Count,
        }],
        &contracts,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    let mut columns = PartitionValueColumns::new(vec![recipe], 0);
    columns
        .push(
            vec![Some(Some(vec![ScalarValue::String("visible".into())]))],
            0,
        )
        .unwrap();
    reducers
        .observe(
            0,
            &columns,
            &contracts,
            &ScalarSortKeyValueEncoder,
            &mut credits,
            &mut budget,
        )
        .unwrap();
    let (facets, aggregates) = reducers.finish(&mut credits, &mut budget).unwrap();
    assert_eq!(
        facets[0].buckets[0].value,
        ScalarValue::String("visible".into())
    );
    assert_eq!(aggregates[0].contributing_count, 1);
    let _ = admitted;
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
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let mut reducers = QueryValueReducers::new(
        &[FacetRequest {
            field_id,
            limit: 10,
        }],
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
        &mut credits,
        &mut budget,
    )
    .unwrap();
    let mut columns = PartitionValueColumns::new(vec![recipe], 0);
    columns
        .push(
            vec![Some(Some(vec![
                ScalarValue::Signed(2),
                ScalarValue::Signed(2),
            ]))],
            0,
        )
        .unwrap();
    reducers
        .observe(
            0,
            &columns,
            &contracts,
            &ScalarSortKeyValueEncoder,
            &mut credits,
            &mut budget,
        )
        .unwrap();
    let (facets, aggregates) = reducers.finish(&mut credits, &mut budget).unwrap();
    assert_eq!(facets[0].buckets.len(), 1);
    assert_eq!(facets[0].buckets[0].count, 1);
    assert_eq!(aggregates[0].contributing_count, 2);
    assert_eq!(aggregates[1].contributing_count, 2);
    assert_eq!(aggregates[1].value, Some(ScalarValue::Signed(4)));
    let _ = admitted;
}

struct DecimalPublicEncoder;

impl QueryPublicValueEncoder for DecimalPublicEncoder {
    fn encode_public_value(
        &self,
        _field: &FieldSchema,
        value: &ScalarValue,
    ) -> Result<Vec<u8>, IndexError> {
        match value {
            ScalarValue::Signed(value) => Ok(value.to_string().into_bytes()),
            _ => Err(IndexError::Integrity),
        }
    }
}

#[test]
fn facet_limit_uses_public_value_order_for_equal_counts() {
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
        capabilities: FieldCapabilities::FACET,
        analyzer: None,
        date_format: None,
    };
    let contracts = [(field_id, QueryFieldBinding { field, recipe })].into();
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let mut reducers = QueryValueReducers::new(
        &[FacetRequest { field_id, limit: 1 }],
        &[],
        &contracts,
        &mut credits,
        &mut budget,
    )
    .unwrap();
    let mut columns = PartitionValueColumns::new(vec![recipe], 0);
    columns
        .push(
            vec![
                Some(Some(vec![ScalarValue::Signed(2)])),
                Some(Some(vec![ScalarValue::Signed(10)])),
            ],
            0,
        )
        .unwrap();
    for row in 0..2 {
        reducers
            .observe(
                row,
                &columns,
                &contracts,
                &DecimalPublicEncoder,
                &mut credits,
                &mut budget,
            )
            .unwrap();
    }
    let (facets, _) = reducers.finish(&mut credits, &mut budget).unwrap();
    assert_eq!(facets[0].buckets.len(), 1);
    assert_eq!(facets[0].buckets[0].value, ScalarValue::Signed(10));
}

#[test]
fn explicit_search_after_is_exclusive_and_uses_document_tie_break() {
    let field_id = FieldId::new(0);
    let recipe = RecipeIdentity::new([9; 32]).unwrap();
    let field = FieldSchema {
        id: field_id,
        name: "value".into(),
        source_selector: "/value".into(),
        field_type: FieldType::SignedInteger,
        cardinality: Cardinality::Single,
        allow_missing: false,
        allow_null: false,
        collation: crate::typed_json::Collation::BinaryUtf8,
        capabilities: FieldCapabilities::ORDER,
        analyzer: None,
        date_format: None,
    };
    let contracts = [(field_id, QueryFieldBinding { field, recipe })].into();
    let first_document = StableDocumentKey::from_bytes([1; 32]).unwrap();
    let second_document = StableDocumentKey::from_bytes([2; 32]).unwrap();
    let third_document = StableDocumentKey::from_bytes([3; 32]).unwrap();
    let cursor = ExplicitQuerySearchAfter {
        values: vec![Some(ScalarValue::Signed(7))],
        document: first_document,
    };
    let mut credits = credits(64 * 1024);
    let mut budget = budget();
    let mut collector = BoundedCandidateCollector::new(
        1,
        &[OrderField {
            field_id,
            direction: crate::typed_json::OrderDirection::Ascending,
        }],
        &contracts,
        Some(&cursor),
        &mut credits,
        &mut budget,
    )
    .unwrap();
    let mut columns = PartitionValueColumns::new(vec![recipe], 0);
    columns
        .push(
            vec![
                Some(Some(vec![ScalarValue::Signed(7)])),
                Some(Some(vec![ScalarValue::Signed(7)])),
                Some(Some(vec![ScalarValue::Signed(8)])),
            ],
            0,
        )
        .unwrap();
    for (row, document) in [first_document, second_document, third_document]
        .into_iter()
        .enumerate()
    {
        let candidate = QueryCandidate {
            partition: partition(1),
            document,
            material_source_version: 1,
        };
        let authorized = authorized(candidate);
        budget
            .reserve_heap(
                &mut credits,
                resident_authorized_candidate_bytes(&authorized).unwrap(),
            )
            .unwrap();
        collector
            .observe(
                candidate,
                authorized,
                row,
                &columns,
                &mut credits,
                &mut budget,
            )
            .unwrap();
    }
    let (candidates, next) = collector.finish(&mut credits, &mut budget).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].candidate.document, second_document);
    assert_eq!(next.unwrap().document, second_document);
}
