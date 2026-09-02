use super::{Config, IndexClient, fresh_token, index_client, put};
use anyhow::{Context, Result, ensure};
use keldra_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use keldra_storage::v1::index_query::Query as QueryValue;
use keldra_storage::v1::{
    BulkWriteRequest, CreateBucketRequest, IndexAggregateOperation, IndexAggregateRequest,
    IndexFacetRequest, IndexOrder, IndexOrderDirection, IndexPredicate, IndexPredicateExpression,
    IndexPredicateOperator, IndexQuery, ObjectVersioning, QueryIndexRequest, QueryIndexResponse,
    TextAnalyzer, TypedJsonIndexQuery,
};
use keldra_storage::{
    KeywordField, TextField, TypedJsonIndexBuilder, UnsignedIntegerField, administration_client,
    connect_channel,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, sleep};

const INDEX_NAME: &str = "v6-capabilities";

#[derive(Serialize)]
pub(super) struct CapabilityReport {
    schema: &'static str,
    started_unix_milliseconds: u128,
    completed_unix_milliseconds: u128,
    result: &'static str,
    configuration: super::config::PublicConfig,
    checks: [&'static str; 6],
}

pub(super) async fn run(
    config: &Config,
    started_unix_milliseconds: u128,
) -> Result<CapabilityReport> {
    ensure!(
        config.endpoints.len() == 1,
        "v6 capability preflight requires one endpoint"
    );
    let channel = connect_channel(&config.endpoints[0])
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let token = fresh_token(config, &channel).await?;
    let mut admin = administration_client(channel.clone(), &token)?;
    admin
        .create_bucket(CreateBucketRequest {
            bucket: config.bucket.clone(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await
        .context("create capability bucket")?;
    let mut objects = keldra_storage::object_client(channel.clone(), &token)?;
    let documents = [
        ("alpha", 1u64, "durable journal alpha"),
        ("beta", 2, "durable journal beta"),
        ("gamma", 3, "unrelated gamma"),
    ];
    let operations = documents
        .iter()
        .map(|(keyword, number, text)| {
            put(
                config,
                format!("capabilities/{keyword}.json"),
                serde_json::to_vec(&serde_json::json!({
                    "keyword": keyword,
                    "number": number,
                    "text": text,
                }))
                .expect("capability document is serializable"),
                format!("capability-put-{keyword}"),
            )
        })
        .collect();
    let outcomes = objects
        .bulk_write(BulkWriteRequest { operations })
        .await?
        .into_inner()
        .outcomes;
    ensure!(outcomes.len() == documents.len());
    ensure!(
        outcomes
            .iter()
            .all(|outcome| matches!(outcome.outcome, Some(BulkOutcomeValue::Receipt(_))))
    );

    let keyword = KeywordField::single("keyword", "/keyword")
        .exact()
        .prefix()
        .range()
        .order()
        .facet();
    let number = UnsignedIntegerField::single("number", "/number")
        .exact()
        .range()
        .order()
        .facet()
        .aggregate();
    let request = TypedJsonIndexBuilder::new(&config.bucket, INDEX_NAME)
        .path_prefix("capabilities/")
        .content_type(super::data::CONTENT_TYPE)
        .field(keyword)
        .field(number)
        .field(
            TextField::single("text", "/text")
                .analyzer(TextAnalyzer::UnicodeAlphanumericLowercase)
                .full_text(),
        )
        .finish("create-v6-capability-index")?;
    let mut client = index_client(channel, &token)?;
    tokio::time::timeout(config.request_timeout, client.create_index(request))
        .await
        .context("create capability index exceeded request timeout")??;

    let deadline = Instant::now() + config.drain_timeout;
    let mut last_query_failure = "capability query has not completed".to_owned();
    loop {
        match execute(
            &mut client,
            config,
            predicate("keyword", IndexPredicateOperator::Equal, &[b"\"beta\""]),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .await
        {
            Ok(response)
                if paths(&response)? == ["capabilities/beta.json"]
                    && response
                        .freshness
                        .as_ref()
                        .is_some_and(|value| value.initial_build_complete && !value.rebuilding) =>
            {
                break;
            }
            Ok(response) => {
                last_query_failure = format!(
                    "unexpected paths {:?} with freshness {:?}",
                    paths(&response)?,
                    response.freshness
                );
            }
            Err(error) => last_query_failure = format!("{error:#}"),
        }
        if Instant::now() >= deadline {
            anyhow::bail!("capability index did not become query-ready: {last_query_failure}");
        }
        sleep(config.visibility_poll).await;
    }

    let range = execute(
        &mut client,
        config,
        predicate(
            "number",
            IndexPredicateOperator::GreaterThanOrEqual,
            &[b"2"],
        ),
        vec![IndexOrder {
            field: "number".into(),
            direction: IndexOrderDirection::Descending as i32,
        }],
        Vec::new(),
        Vec::new(),
    )
    .await?;
    ensure!(
        paths(&range)? == ["capabilities/gamma.json", "capabilities/beta.json"],
        "range/order capability oracle mismatch"
    );

    let full_text = execute(
        &mut client,
        config,
        predicate("text", IndexPredicateOperator::FullText, &[b"\"durable\""]),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .await?;
    ensure!(
        path_set(&full_text)?
            == BTreeSet::from([
                "capabilities/alpha.json".to_owned(),
                "capabilities/beta.json".to_owned(),
            ]),
        "full-text capability oracle mismatch"
    );

    let computations = execute(
        &mut client,
        config,
        None,
        Vec::new(),
        vec![IndexFacetRequest {
            field: "keyword".into(),
            limit: 10,
        }],
        vec![IndexAggregateRequest {
            field: "number".into(),
            operation: IndexAggregateOperation::Sum as i32,
        }],
    )
    .await?;
    ensure!(computations.facet_results.len() == 1);
    let facet = &computations.facet_results[0];
    ensure!(facet.buckets.len() == 3);
    ensure!(facet.buckets.iter().all(|bucket| bucket.count == 1));
    ensure!(
        facet
            .buckets
            .iter()
            .map(|bucket| bucket.value_json.clone())
            .collect::<BTreeSet<_>>()
            == BTreeSet::from([
                b"\"alpha\"".to_vec(),
                b"\"beta\"".to_vec(),
                b"\"gamma\"".to_vec()
            ]),
        "facet capability oracle mismatch"
    );
    ensure!(computations.aggregate_results.len() == 1);
    let aggregate = &computations.aggregate_results[0];
    ensure!(aggregate.operation == IndexAggregateOperation::Sum as i32);
    ensure!(aggregate.contributing_count == 3);
    ensure!(aggregate.value_json.as_deref() == Some(b"6".as_slice()));

    Ok(CapabilityReport {
        schema: "keldra.index-v6-capability-qualification.v1",
        started_unix_milliseconds,
        completed_unix_milliseconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        result: "pass",
        configuration: config.public(),
        checks: ["exact", "range", "order", "facet", "aggregate", "full-text"],
    })
}

async fn execute(
    client: &mut IndexClient,
    config: &Config,
    predicate: Option<IndexPredicateExpression>,
    order: Vec<IndexOrder>,
    facets: Vec<IndexFacetRequest>,
    aggregates: Vec<IndexAggregateRequest>,
) -> Result<QueryIndexResponse> {
    tokio::time::timeout(
        config.request_timeout,
        client.query_index(QueryIndexRequest {
            bucket: config.bucket.clone(),
            index_name: INDEX_NAME.into(),
            query: Some(IndexQuery {
                query: Some(QueryValue::TypedJson(TypedJsonIndexQuery {
                    predicate,
                    order,
                    facets,
                    aggregates,
                })),
            }),
            limit: 100,
            page_token: Vec::new(),
            tenant: String::new(),
            required_freshness: None,
        }),
    )
    .await
    .context("capability query exceeded request timeout")?
    .map(tonic::Response::into_inner)
    .map_err(Into::into)
}

fn predicate(
    field: &str,
    operator: IndexPredicateOperator,
    values: &[&[u8]],
) -> Option<IndexPredicateExpression> {
    Some(IndexPredicateExpression::leaf(IndexPredicate {
        field: field.into(),
        operator: operator as i32,
        values_json: values.iter().map(|value| value.to_vec()).collect(),
    }))
}

fn paths(response: &QueryIndexResponse) -> Result<Vec<String>> {
    response
        .hits
        .iter()
        .map(|hit| {
            Ok(hit
                .address
                .as_ref()
                .context("capability hit omitted address")?
                .path
                .clone())
        })
        .collect()
}

fn path_set(response: &QueryIndexResponse) -> Result<BTreeSet<String>> {
    paths(response).map(|paths| paths.into_iter().collect())
}
