use std::collections::BTreeSet;

use anvil_storage::v1::index_field::FieldType;
use anvil_storage::v1::{
    Durability, IndexFieldCapability, IndexFreshness, IndexSourceFreshness, QueryIndexResponse,
};

use super::{
    QueryValue, SpecificationValue, engine_cases, qualification_durability, retryable,
    retryable_transport, routed_responses_agree,
};

#[test]
fn public_matrix_covers_all_eight_kinds_and_real_pagination() {
    let cases = engine_cases();
    let kinds = cases
        .iter()
        .map(|case| {
            match case
                .specification
                .specification
                .as_ref()
                .expect("qualification specification")
            {
                SpecificationValue::Path(_) => "path",
                SpecificationValue::MetadataFilter(_) => "metadata_filter",
                SpecificationValue::TypedJson(_) => "typed_json",
                SpecificationValue::FullText(_) => "full_text",
                SpecificationValue::Vector(_) => "vector",
                SpecificationValue::Hybrid(_) => "hybrid",
                SpecificationValue::GitSource(_) => "git_source",
                SpecificationValue::Tensor(_) => "tensor",
            }
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(
        kinds,
        BTreeSet::from([
            "full_text",
            "git_source",
            "hybrid",
            "metadata_filter",
            "path",
            "tensor",
            "typed_json",
            "vector",
        ])
    );
    assert_eq!(cases.len(), kinds.len());
    for case in cases {
        assert!(
            case.documents.len() >= 3,
            "{} must exercise every three-node ingress source",
            case.name
        );
        assert!(!case.expected_paths.is_empty());
        assert!(case.expected_paths.contains(&case.replacement_hit_path));
        assert_ne!(case.replacement.0, case.delete_path);
        if let Some(SpecificationValue::TypedJson(specification)) =
            case.specification.specification.as_ref()
        {
            let Some(QueryValue::TypedJson(query)) = case.query.query.as_ref() else {
                panic!("Typed JSON qualification must issue a Typed JSON query");
            };
            assert!(!specification.physical_order.is_empty());
            assert_eq!(query.order, specification.physical_order);
        }
        let references_another_object = matches!(
            case.specification.specification.as_ref(),
            Some(SpecificationValue::GitSource(_) | SpecificationValue::Tensor(_))
        );
        assert_eq!(
            case.replacement_hit_path != case.replacement.0,
            references_another_object,
            "{} must point results at the correct ordinary object",
            case.name
        );
        assert_eq!(
            case.delete_hit_path != case.delete_path,
            references_another_object,
            "{} must delete the manifest for the correct ordinary result object",
            case.name
        );
        if !matches!(
            case.specification.specification,
            Some(SpecificationValue::Tensor(_))
        ) {
            assert!(case.expected_paths.len() > 1, "{} must paginate", case.name);
            assert!(case.expected_paths.contains(&case.delete_hit_path));
        } else {
            assert_eq!(
                case.expected_paths.len(),
                1,
                "{} is an exact lookup",
                case.name
            );
        }
    }
}

#[test]
fn typed_json_qualification_covers_every_type_and_capability() {
    let case = engine_cases()
        .into_iter()
        .find(|case| {
            matches!(
                case.specification.specification.as_ref(),
                Some(SpecificationValue::TypedJson(_))
            )
        })
        .expect("Typed JSON qualification case");
    let Some(SpecificationValue::TypedJson(specification)) = case.specification.specification
    else {
        unreachable!()
    };
    let types = specification
        .fields
        .iter()
        .map(
            |field| match field.field_type.as_ref().expect("field type") {
                FieldType::Boolean(_) => "boolean",
                FieldType::SignedInteger(_) => "signed",
                FieldType::UnsignedInteger(_) => "unsigned",
                FieldType::Float(_) => "float",
                FieldType::Keyword(_) => "keyword",
                FieldType::Text(_) => "text",
            },
        )
        .collect::<BTreeSet<_>>();
    let capabilities = specification
        .fields
        .iter()
        .flat_map(|field| field.capabilities.iter().copied())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        types,
        BTreeSet::from(["boolean", "float", "keyword", "signed", "text", "unsigned"])
    );
    assert_eq!(
        capabilities,
        BTreeSet::from([
            IndexFieldCapability::Exact as i32,
            IndexFieldCapability::Prefix as i32,
            IndexFieldCapability::Range as i32,
            IndexFieldCapability::Order as i32,
            IndexFieldCapability::Facet as i32,
            IndexFieldCapability::Aggregate as i32,
            IndexFieldCapability::FullText as i32,
        ])
    );
    assert!(specification.fields.iter().any(|field| {
        field.cardinality == anvil_storage::v1::IndexFieldCardinality::Multi as i32
    }));
}

#[test]
fn source_writes_request_only_satisfiable_topology_durability() {
    assert_eq!(qualification_durability(1), Durability::Local);
    assert_eq!(qualification_durability(3), Durability::Replicated);
}

fn routed_response() -> QueryIndexResponse {
    QueryIndexResponse {
        hits: Vec::new(),
        next_page_token: vec![1, 2, 3],
        facet_results: Vec::new(),
        aggregate_results: Vec::new(),
        freshness: Some(IndexFreshness {
            generation: 7,
            published_at: None,
            sources: vec![
                IndexSourceFreshness {
                    node_id: 1,
                    source_epoch: vec![1; 32],
                    indexed_next_offset: 11,
                    observed_tail: Some(12),
                    lag_hint: 1,
                },
                IndexSourceFreshness {
                    node_id: 2,
                    source_epoch: vec![2; 32],
                    indexed_next_offset: 21,
                    observed_tail: Some(22),
                    lag_hint: 1,
                },
            ],
            initial_build_complete: true,
            rebuilding: false,
            authorization_revision: 31,
            placement_term: 4,
            placement_index: 5,
            index_id: 41,
            definition_version: 3,
        }),
    }
}

fn assert_freshness_disagrees(mut mutate: impl FnMut(&mut QueryIndexResponse)) {
    let baseline = routed_response();
    let mut changed = baseline.clone();
    mutate(&mut changed);
    assert!(!routed_responses_agree(&[baseline, changed]));
}

#[test]
fn retryable_statuses_include_only_transport_timeout_cancellation() {
    assert!(retryable(&tonic::Status::unavailable("try another node")));
    assert!(retryable(&tonic::Status::deadline_exceeded(
        "request deadline exceeded"
    )));
    assert!(retryable(&tonic::Status::cancelled("Timeout expired")));

    assert!(!retryable(&tonic::Status::cancelled(
        "caller cancelled request"
    )));
    assert!(!retryable(&tonic::Status::invalid_argument(
        "invalid query"
    )));

    assert!(retryable_transport(&tonic::Status::unavailable(
        "serving fence expired"
    )));
    assert!(retryable_transport(&tonic::Status::deadline_exceeded(
        "unknown mutation outcome"
    )));
    assert!(retryable_transport(&tonic::Status::cancelled(
        "Timeout expired"
    )));
    assert!(!retryable_transport(&tonic::Status::not_found(
        "object missing"
    )));
    assert!(!retryable_transport(&tonic::Status::failed_precondition(
        "mutation rejected"
    )));
}

#[test]
fn routed_freshness_allows_only_live_source_observations_to_differ() {
    let baseline = routed_response();
    let mut changed = baseline.clone();
    let sources = &mut changed.freshness.as_mut().unwrap().sources;
    sources[0].observed_tail = Some(100);
    sources[0].lag_hint = 89;
    sources[1].observed_tail = None;
    sources[1].lag_hint = 0;

    assert!(routed_responses_agree(&[baseline, changed]));
}

#[test]
fn routed_freshness_requires_stable_identity_and_checkpoints() {
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().generation += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().published_at = Some(Default::default());
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().initial_build_complete = false;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().rebuilding = true;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().authorization_revision += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().placement_term += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().placement_index += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().index_id += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().definition_version += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().sources[0].node_id += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().sources[0]
            .source_epoch
            .push(9);
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().sources[0].indexed_next_offset += 1;
    });
    assert_freshness_disagrees(|response| {
        response.freshness.as_mut().unwrap().sources.swap(0, 1);
    });
}

#[test]
fn routed_responses_still_require_matching_results_and_freshness() {
    assert_freshness_disagrees(|response| response.next_page_token.push(4));
    assert_freshness_disagrees(|response| response.hits.push(Default::default()));
    assert_freshness_disagrees(|response| response.freshness = None);
}
