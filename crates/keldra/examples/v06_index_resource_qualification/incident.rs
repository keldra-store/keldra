//! Regression for the sparse, physically ordered query shape that motivated
//! the native segment format.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use keldra_storage::v1::index_query::Query as QueryValue;
use keldra_storage::v1::{
    IndexAggregateOperation, IndexAggregateRequest, IndexFacetRequest, IndexOrder,
    IndexOrderDirection, IndexPredicate, IndexPredicateOperator, IndexQuery, QueryIndexRequest,
    QueryIndexResponse, TypedJsonIndexQuery,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    IndexClient, RecordFlavor, data, freshness, parse_record_id, source_complete_freshness,
};

const INDEX_NAME: &str = "records-by-field";
const PAGE_SIZE: u32 = 999;
const INCIDENT_LIMIT: u32 = 4;

#[derive(Debug, Clone, Serialize)]
pub(super) struct IncidentReport {
    schema: &'static str,
    corpus_records: u64,
    index_id: u64,
    definition_version: u64,
    commit_revision: u64,
    physical_order: [&'static str; 2],
    incident_predicates: [&'static str; 3],
    limit_four: QueryEvidence,
    consecutive_pages: PaginationEvidence,
    zero_hit_sparse_conjunction: QueryEvidence,
    unselective_arbitrary_sort: QueryEvidence,
    exact_computations: ComputationEvidence,
}

#[derive(Debug, Clone, Serialize)]
struct QueryEvidence {
    returned_hits: usize,
    exact_order: bool,
    elapsed_milliseconds: f64,
    commit_revision: u64,
    result_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
struct PaginationEvidence {
    requested_page_size: u32,
    page_one_hits: usize,
    page_two_hits: usize,
    page_one_sha256: String,
    page_two_sha256: String,
    continuation_token_bytes: usize,
    page_two_used_page_one_token: bool,
    exact_order: bool,
    overlap: usize,
    page_one_elapsed_milliseconds: f64,
    page_two_elapsed_milliseconds: f64,
    commit_revision: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ComputationEvidence {
    returned_hits: usize,
    exact_order: bool,
    elapsed_milliseconds: f64,
    commit_revision: u64,
    result_sha256: String,
    matching_documents: usize,
}

#[derive(Debug, Deserialize)]
struct GeneratedRecord {
    record_id: u64,
    ecosystem: String,
    active: bool,
    withdrawn: bool,
    score: f64,
    modified_day: u64,
}

#[derive(Clone, Copy, Debug)]
struct ExpectedRecord {
    record_id: u64,
    modified_day: u64,
    score: f64,
}

pub(super) fn physical_order() -> Vec<IndexOrder> {
    vec![
        IndexOrder {
            field: "modified_day".into(),
            direction: IndexOrderDirection::Descending as i32,
        },
        IndexOrder {
            field: "record_id".into(),
            direction: IndexOrderDirection::Ascending as i32,
        },
    ]
}

pub(super) async fn run(
    client: &mut IndexClient,
    bucket: &str,
    seed: u64,
    records: u64,
    initial_versions: &[u64],
    expected_sources: usize,
) -> Result<IncidentReport> {
    let (incident_expected, arbitrary_expected) = expected_results(seed, records)?;
    ensure!(
        incident_expected.len() >= usize::try_from(PAGE_SIZE)? * 2,
        "qualification corpus has too few incident-query matches for two complete pages"
    );

    let incident_query = incident_query();
    let (limit_four, limit_four_elapsed) = execute(
        client,
        bucket,
        incident_query.clone(),
        INCIDENT_LIMIT,
        Vec::new(),
    )
    .await?;
    let limit_four_expected = &incident_expected[..usize::try_from(INCIDENT_LIMIT)?];
    let limit_four_ids = validate_response(
        &limit_four,
        limit_four_expected,
        initial_versions,
        expected_sources,
    )?;
    let (page_one, page_one_elapsed) = execute(
        client,
        bucket,
        incident_query.clone(),
        PAGE_SIZE,
        Vec::new(),
    )
    .await?;
    ensure!(
        !page_one.next_page_token.is_empty(),
        "first full incident-query page omitted its continuation token"
    );
    let page_token = page_one.next_page_token.clone();
    let page_width = usize::try_from(PAGE_SIZE)?;
    let page_one_ids = validate_response(
        &page_one,
        &incident_expected[..page_width],
        initial_versions,
        expected_sources,
    )?;
    let (page_two, page_two_elapsed) = execute(
        client,
        bucket,
        incident_query,
        PAGE_SIZE,
        page_token.clone(),
    )
    .await?;
    let page_two_ids = validate_response(
        &page_two,
        &incident_expected[page_width..page_width * 2],
        initial_versions,
        expected_sources,
    )?;
    let overlap = page_one_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .intersection(&page_two_ids.iter().copied().collect())
        .count();
    ensure!(overlap == 0, "consecutive incident-query pages overlap");
    ensure!(
        freshness(&page_one)?.commit_revision == freshness(&page_two)?.commit_revision,
        "consecutive incident-query pages crossed commit revisions"
    );

    let (zero_hit, zero_hit_elapsed) =
        execute(client, bucket, zero_hit_query(), INCIDENT_LIMIT, Vec::new()).await?;
    let zero_hit_ids = validate_response(&zero_hit, &[], initial_versions, expected_sources)?;
    ensure!(
        zero_hit.next_page_token.is_empty(),
        "zero-hit query returned a continuation token"
    );

    let (arbitrary, arbitrary_elapsed) = execute(
        client,
        bucket,
        arbitrary_sort_query(),
        INCIDENT_LIMIT,
        Vec::new(),
    )
    .await?;
    let arbitrary_ids = validate_response(
        &arbitrary,
        &arbitrary_expected,
        initial_versions,
        expected_sources,
    )?;

    // Keep the production-shaped limit-four query free of computation work so
    // its read-amplification evidence measures page selection. Exact facets and
    // aggregates deliberately visit the complete authorized match set and are
    // qualified separately.
    let (computations, computations_elapsed) = execute(
        client,
        bucket,
        computation_query(),
        INCIDENT_LIMIT,
        Vec::new(),
    )
    .await?;
    let computation_ids = validate_response(
        &computations,
        limit_four_expected,
        initial_versions,
        expected_sources,
    )?;
    validate_computations(&computations, incident_expected.len())?;

    let identity = freshness(&limit_four)?;
    for response in [&page_one, &page_two, &zero_hit, &arbitrary, &computations] {
        let observed = freshness(response)?;
        ensure!(
            observed.index_id == identity.index_id
                && observed.definition_version == identity.definition_version
                && observed.commit_revision == identity.commit_revision,
            "production-shaped query sequence crossed index identity or commit_revision"
        );
    }

    Ok(IncidentReport {
        schema: "keldra.index-production-query-regression.v2",
        corpus_records: records,
        index_id: identity.index_id,
        definition_version: identity.definition_version,
        commit_revision: identity.commit_revision,
        physical_order: ["modified_day DESC", "record_id ASC"],
        incident_predicates: [
            "withdrawn = false",
            "active = true",
            "ecosystem IN (cargo, npm, pypi)",
        ],
        limit_four: QueryEvidence {
            returned_hits: limit_four_ids.len(),
            exact_order: true,
            elapsed_milliseconds: limit_four_elapsed,
            commit_revision: freshness(&limit_four)?.commit_revision,
            result_sha256: result_sha256(&limit_four_ids),
        },
        consecutive_pages: PaginationEvidence {
            requested_page_size: PAGE_SIZE,
            page_one_hits: page_one_ids.len(),
            page_two_hits: page_two_ids.len(),
            page_one_sha256: result_sha256(&page_one_ids),
            page_two_sha256: result_sha256(&page_two_ids),
            continuation_token_bytes: page_token.len(),
            page_two_used_page_one_token: true,
            exact_order: true,
            overlap,
            page_one_elapsed_milliseconds: page_one_elapsed,
            page_two_elapsed_milliseconds: page_two_elapsed,
            commit_revision: freshness(&page_two)?.commit_revision,
        },
        zero_hit_sparse_conjunction: QueryEvidence {
            returned_hits: zero_hit_ids.len(),
            exact_order: true,
            elapsed_milliseconds: zero_hit_elapsed,
            commit_revision: freshness(&zero_hit)?.commit_revision,
            result_sha256: result_sha256(&zero_hit_ids),
        },
        unselective_arbitrary_sort: QueryEvidence {
            returned_hits: arbitrary_ids.len(),
            exact_order: true,
            elapsed_milliseconds: arbitrary_elapsed,
            commit_revision: freshness(&arbitrary)?.commit_revision,
            result_sha256: result_sha256(&arbitrary_ids),
        },
        exact_computations: ComputationEvidence {
            returned_hits: computation_ids.len(),
            exact_order: true,
            elapsed_milliseconds: computations_elapsed,
            commit_revision: freshness(&computations)?.commit_revision,
            result_sha256: result_sha256(&computation_ids),
            matching_documents: incident_expected.len(),
        },
    })
}

fn expected_results(seed: u64, records: u64) -> Result<(Vec<ExpectedRecord>, Vec<ExpectedRecord>)> {
    let mut incident = Vec::new();
    let mut arbitrary = Vec::with_capacity(usize::try_from(INCIDENT_LIMIT)?);
    for record_id in 0..records {
        let generated: GeneratedRecord =
            serde_json::from_slice(&data::payload(seed, record_id, RecordFlavor::Initial))
                .context("parse deterministic qualification record")?;
        ensure!(generated.record_id == record_id);
        let expected = ExpectedRecord {
            record_id,
            modified_day: generated.modified_day,
            score: generated.score,
        };
        if !generated.withdrawn
            && generated.active
            && matches!(generated.ecosystem.as_str(), "cargo" | "npm" | "pypi")
        {
            incident.push(expected);
        }
        arbitrary.push(expected);
        arbitrary.sort_unstable_by(arbitrary_order);
        arbitrary.truncate(usize::try_from(INCIDENT_LIMIT)?);
    }
    incident.sort_unstable_by(incident_order);
    Ok((incident, arbitrary))
}

fn incident_order(left: &ExpectedRecord, right: &ExpectedRecord) -> Ordering {
    right
        .modified_day
        .cmp(&left.modified_day)
        .then_with(|| left.record_id.cmp(&right.record_id))
}

fn arbitrary_order(left: &ExpectedRecord, right: &ExpectedRecord) -> Ordering {
    left.score
        .total_cmp(&right.score)
        .then_with(|| left.record_id.cmp(&right.record_id))
}

fn incident_query() -> TypedJsonIndexQuery {
    TypedJsonIndexQuery {
        predicates: incident_predicates(),
        order: physical_order(),
        facets: Vec::new(),
        aggregates: Vec::new(),
    }
}

fn computation_query() -> TypedJsonIndexQuery {
    let mut query = incident_query();
    query.facets.push(IndexFacetRequest {
        field: "active".into(),
        limit: 1,
    });
    query.aggregates.push(IndexAggregateRequest {
        field: "score".into(),
        operation: IndexAggregateOperation::Count as i32,
    });
    query
}

fn zero_hit_query() -> TypedJsonIndexQuery {
    let mut predicates = incident_predicates();
    predicates.push(predicate(
        "score",
        IndexPredicateOperator::LessThan,
        &["-1"],
    ));
    TypedJsonIndexQuery {
        predicates,
        order: physical_order(),
        facets: Vec::new(),
        aggregates: Vec::new(),
    }
}

fn arbitrary_sort_query() -> TypedJsonIndexQuery {
    TypedJsonIndexQuery {
        predicates: Vec::new(),
        order: vec![
            IndexOrder {
                field: "score".into(),
                direction: IndexOrderDirection::Ascending as i32,
            },
            IndexOrder {
                field: "record_id".into(),
                direction: IndexOrderDirection::Ascending as i32,
            },
        ],
        facets: Vec::new(),
        aggregates: Vec::new(),
    }
}

fn validate_computations(response: &QueryIndexResponse, matching_documents: usize) -> Result<()> {
    ensure!(response.facet_results.len() == 1);
    let facet = &response.facet_results[0];
    ensure!(facet.field == "active" && facet.buckets.len() == 1);
    ensure!(facet.buckets[0].value_json == b"true");
    ensure!(facet.buckets[0].count == u64::try_from(matching_documents)?);

    ensure!(response.aggregate_results.len() == 1);
    let aggregate = &response.aggregate_results[0];
    ensure!(aggregate.field == "score");
    ensure!(aggregate.operation == IndexAggregateOperation::Count as i32);
    ensure!(aggregate.contributing_count == u64::try_from(matching_documents)?);
    ensure!(aggregate.value_json.as_deref() == Some(matching_documents.to_string().as_bytes()));
    Ok(())
}

fn incident_predicates() -> Vec<IndexPredicate> {
    vec![
        predicate("withdrawn", IndexPredicateOperator::Equal, &["false"]),
        predicate("active", IndexPredicateOperator::Equal, &["true"]),
        predicate(
            "ecosystem",
            IndexPredicateOperator::In,
            &["\"cargo\"", "\"npm\"", "\"pypi\""],
        ),
    ]
}

fn predicate(
    field: &str,
    operator: IndexPredicateOperator,
    values_json: &[&str],
) -> IndexPredicate {
    IndexPredicate {
        field: field.into(),
        operator: operator as i32,
        values_json: values_json
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect(),
    }
}

async fn execute(
    client: &mut IndexClient,
    bucket: &str,
    query: TypedJsonIndexQuery,
    limit: u32,
    page_token: Vec<u8>,
) -> Result<(QueryIndexResponse, f64)> {
    let started = Instant::now();
    let response = client
        .query_index(QueryIndexRequest {
            bucket: bucket.into(),
            index_name: INDEX_NAME.into(),
            query: Some(IndexQuery {
                query: Some(QueryValue::TypedJson(query)),
            }),
            limit,
            page_token,
            tenant: String::new(),
            required_freshness: None,
        })
        .await
        .context("run production-shaped TypedJson query")?
        .into_inner();
    Ok((response, started.elapsed().as_secs_f64() * 1_000.0))
}

fn validate_response(
    response: &QueryIndexResponse,
    expected: &[ExpectedRecord],
    initial_versions: &[u64],
    expected_sources: usize,
) -> Result<Vec<u64>> {
    let observed = response
        .hits
        .iter()
        .map(|hit| {
            let address = hit.address.as_ref().context("index hit omitted address")?;
            let record_id = parse_record_id(&address.path)?;
            let expected_version = initial_versions
                .get(usize::try_from(record_id)?)
                .context("query returned a record outside the qualification corpus")?;
            ensure!(hit.object_version == *expected_version);
            Ok(record_id)
        })
        .collect::<Result<Vec<_>>>()?;
    let expected = expected
        .iter()
        .map(|record| record.record_id)
        .collect::<Vec<_>>();
    ensure!(
        observed == expected,
        "query result order or membership differed"
    );
    let response_freshness = freshness(response)?;
    ensure!(response_freshness.initial_build_complete);
    ensure!(
        source_complete_freshness(response_freshness, expected_sources),
        "query response did not prove complete zero-lag freshness"
    );
    Ok(observed)
}

fn result_sha256(record_ids: &[u64]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"keldra.index-production-query-result.v1\0");
    hash.update((record_ids.len() as u64).to_be_bytes());
    for record_id in record_ids {
        hash.update(record_id.to_be_bytes());
    }
    format!("sha256:{}", hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_selection_and_exact_computations_are_distinct_queries() {
        let selection = incident_query();
        assert!(selection.facets.is_empty());
        assert!(selection.aggregates.is_empty());

        let computations = computation_query();
        assert_eq!(computations.predicates, selection.predicates);
        assert_eq!(computations.order, selection.order);
        assert_eq!(computations.facets.len(), 1);
        assert_eq!(computations.facets[0].field, "active");
        assert_eq!(computations.facets[0].limit, 1);
        assert_eq!(computations.aggregates.len(), 1);
        assert_eq!(computations.aggregates[0].field, "score");
        assert_eq!(
            computations.aggregates[0].operation,
            IndexAggregateOperation::Count as i32
        );
    }
}
