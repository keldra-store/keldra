//! One isolated public equality query used by the bounded scale comparison.

use std::collections::BTreeSet;
use std::env;
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_service_client::IndexServiceClient;
use anvil_storage::v1::{
    IndexPredicate, IndexPredicateOperator, IndexQuery, IndexSourceFreshness, QueryIndexRequest,
    TypedJsonIndexQuery,
};
use anvil_storage::{BearerToken, connect_channel, exchange_client_credentials};
use anyhow::{Context, Result, anyhow, ensure};
use serde::{Deserialize, Serialize};

const INDEX_NAME: &str = "records-by-field";
const FIELD: &str = "record_id";
const VALUE_JSON: &[u8] = b"0";
const EXPECTED_PATH: &str = "records/000000000000.json";

#[derive(Deserialize)]
struct VerificationState {
    schema: String,
    tenant: String,
    bucket: String,
    records: u64,
    final_live_objects: u64,
    final_generation: u64,
    source_count: usize,
}

#[derive(Serialize)]
struct ProbeReport {
    schema: &'static str,
    endpoint: String,
    tenant: String,
    bucket: String,
    index_name: &'static str,
    field: &'static str,
    operator: &'static str,
    value_json: &'static str,
    expected_path: &'static str,
    started_at_unix_millis: u64,
    completed_at_unix_millis: u64,
    elapsed_milliseconds: f64,
    index_id: u64,
    definition_version: u64,
    generation: u64,
    placement_term: u64,
    placement_index: u64,
    source_count: usize,
    returned_hits: usize,
    object_version: u64,
}

pub(super) async fn run(state_path: &Path) -> Result<()> {
    let state: VerificationState = serde_json::from_slice(
        &std::fs::read(state_path)
            .with_context(|| format!("read qualification state {}", state_path.display()))?,
    )
    .with_context(|| format!("parse qualification state {}", state_path.display()))?;
    validate_state(&state)?;

    let tenant = required("ANVIL_V06_RESOURCE_TENANT")?;
    let bucket = required("ANVIL_V06_RESOURCE_BUCKET")?;
    ensure!(
        tenant == state.tenant,
        "qualification state tenant mismatch"
    );
    ensure!(
        bucket == state.bucket,
        "qualification state bucket mismatch"
    );
    let endpoints = required("ANVIL_V06_RESOURCE_ENDPOINTS")?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    ensure!(
        endpoints.len() == 1,
        "singleton probe requires exactly one ingress endpoint"
    );
    let endpoint = endpoints.into_iter().next().unwrap();
    let channel = connect_channel(&endpoint)
        .await
        .map_err(|error| anyhow!("connect to {endpoint}: {error}"))?;
    let token = exchange_client_credentials(
        channel.clone(),
        required("ANVIL_V06_RESOURCE_CLIENT_ID")?,
        required("ANVIL_V06_RESOURCE_CLIENT_SECRET")?,
    )
    .await
    .context("credential exchange failed")?
    .access_token;
    let mut client = IndexServiceClient::with_interceptor(channel, BearerToken::new(&token)?)
        .max_encoding_message_size(72 * 1024 * 1024)
        .max_decoding_message_size(72 * 1024 * 1024);

    let started_at_unix_millis = unix_millis()?;
    let started = Instant::now();
    let response = client
        .query_index(QueryIndexRequest {
            bucket: bucket.clone(),
            index_name: INDEX_NAME.into(),
            query: Some(IndexQuery {
                query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
                    predicates: vec![IndexPredicate {
                        field: FIELD.into(),
                        operator: IndexPredicateOperator::Equal as i32,
                        values_json: vec![VALUE_JSON.to_vec()],
                    }],
                    order: Vec::new(),
                    facets: Vec::new(),
                    aggregates: Vec::new(),
                })),
            }),
            // Two makes an accidental non-singleton result observable without
            // asking the server to retain a broad result set.
            limit: 2,
            page_token: Vec::new(),
            tenant: String::new(),
        })
        .await
        .context("singleton Typed JSON equality query failed")?
        .into_inner();
    let elapsed_milliseconds = started.elapsed().as_secs_f64() * 1_000.0;
    let completed_at_unix_millis = unix_millis()?;

    ensure!(response.next_page_token.is_empty());
    ensure!(
        response.hits.len() == 1,
        "singleton probe did not return exactly one hit"
    );
    let hit = &response.hits[0];
    let address = hit
        .address
        .as_ref()
        .context("singleton hit omitted address")?;
    ensure!(address.tenant == tenant && address.bucket == bucket);
    ensure!(
        address.path == EXPECTED_PATH,
        "singleton probe returned the wrong path"
    );
    ensure!(
        hit.object_version != 0,
        "singleton hit omitted object version"
    );
    let freshness = response
        .freshness
        .as_ref()
        .context("singleton response omitted freshness")?;
    ensure!(
        freshness.generation >= state.final_generation,
        "index generation regressed"
    );
    ensure!(freshness.initial_build_complete && !freshness.rebuilding);
    ensure!(
        freshness.index_id != 0
            && freshness.definition_version != 0
            && freshness.placement_term != 0
            && freshness.placement_index != 0
    );
    ensure!(source_complete(
        freshness.sources.as_slice(),
        state.source_count
    ));

    let report = ProbeReport {
        schema: "anvil.index-resource-singleton-probe.v1",
        endpoint,
        tenant,
        bucket,
        index_name: INDEX_NAME,
        field: FIELD,
        operator: "EQUAL",
        value_json: "0",
        expected_path: EXPECTED_PATH,
        started_at_unix_millis,
        completed_at_unix_millis,
        elapsed_milliseconds,
        index_id: freshness.index_id,
        definition_version: freshness.definition_version,
        generation: freshness.generation,
        placement_term: freshness.placement_term,
        placement_index: freshness.placement_index,
        source_count: freshness.sources.len(),
        returned_hits: response.hits.len(),
        object_version: hit.object_version,
    };
    let output = required("ANVIL_V09_RESOURCE_SINGLETON_PROBE_OUTPUT")?;
    let encoded = serde_json::to_vec_pretty(&report)?;
    std::fs::write(&output, &encoded).with_context(|| format!("write probe report {output}"))?;
    println!("{}", String::from_utf8(encoded).expect("JSON is UTF-8"));
    Ok(())
}

fn validate_state(state: &VerificationState) -> Result<()> {
    ensure!(state.schema == "anvil.index-resource-verification.v1");
    ensure!(state.records > 0 && state.final_live_objects <= state.records);
    ensure!(state.final_generation > 0 && state.source_count > 0);
    // This qualification workflow deletes a range immediately before its
    // equally-sized trailing update range. The inequality proves record zero
    // was neither deleted nor updated and therefore has one posting in the
    // final bounded segment set.
    let deleted = state.records - state.final_live_objects;
    ensure!(state.records > deleted.saturating_mul(2));
    Ok(())
}

fn source_complete(sources: &[IndexSourceFreshness], expected: usize) -> bool {
    let ids = sources
        .iter()
        .map(|source| source.node_id)
        .collect::<BTreeSet<_>>();
    expected != 0
        && sources.len() == expected
        && ids.len() == expected
        && sources.iter().all(|source| {
            source.node_id != 0
                && source.source_epoch.len() == 32
                && source.lag_hint == 0
                && source.observed_tail.and_then(|tail| tail.checked_add(1))
                    == Some(source.indexed_next_offset)
        })
}

fn required(name: &str) -> Result<String> {
    env::var(name).map_err(|_| anyhow!("{name} is required"))
}

fn unix_millis() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock precedes the Unix epoch")?
            .as_millis(),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singleton_state_requires_record_zero_to_be_untouched() {
        let state = |records, live| VerificationState {
            schema: "anvil.index-resource-verification.v1".into(),
            tenant: "tenant".into(),
            bucket: "bucket".into(),
            records,
            final_live_objects: live,
            final_generation: 1,
            source_count: 1,
        };
        assert!(validate_state(&state(16_384, 14_336)).is_ok());
        assert!(validate_state(&state(839_980, 837_932)).is_ok());
        assert!(validate_state(&state(2, 1)).is_err());
    }
}
