//! Public-API index qualification for one- and three-node Docker harnesses.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_service_client::IndexServiceClient;
use anvil_storage::v1::index_specification::Specification as SpecificationValue;
use anvil_storage::v1::put_header::Operation as PutOperationValue;
use anvil_storage::v1::{
    CreateApplicationRequest, CreateBucketRequest, CreateIndexRequest, DeleteRequest, Durability,
    FullTextField, FullTextIndexQuery, FullTextIndexSpec, GetIndexRequest, GitSourceIndexQuery,
    GitSourceIndexSpec, HybridIndexQuery, HybridIndexSpec, IndexField, IndexFreshness,
    IndexPredicate, IndexPredicateOperator, IndexQuery, IndexQueryHit, IndexSpecification,
    MetadataFilterIndexQuery, MetadataFilterIndexSpec, ObjectAddress, ObjectVersioning,
    PathIndexQuery, PathIndexSpec, PutHeader, PutOperation, QueryIndexRequest, QueryIndexResponse,
    SetBucketPublicReadRequest, TensorIndexQuery, TensorIndexSpec, TypedJsonIndexQuery,
    TypedJsonIndexSpec, VectorIndexQuery, VectorIndexSpec, VectorMetric,
};
use anvil_storage::{
    BearerToken, RawAdministrationClient, RawClient, administration_client, connect_channel,
    exchange_client_credentials, object_client, put_chunks,
};
use tokio::time::{Instant, sleep};
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Request};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const GENERATION_QUIET_WINDOW: Duration = Duration::from_secs(3);
const GENERATION_QUIET_LIMIT: Duration = Duration::from_secs(12);
const CONTENT_TYPE: &str = "application/json";

#[derive(Clone)]
struct EngineCase {
    bucket: &'static str,
    name: &'static str,
    specification: IndexSpecification,
    query: IndexQuery,
    documents: Vec<(&'static str, &'static [u8])>,
    expected_paths: Vec<&'static str>,
    replacement: (&'static str, &'static [u8]),
    delete_path: &'static str,
    expects_scores: bool,
    min_advanced_sources: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("ANVIL_INDEX_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "index qualification requires either one or three endpoints",
        ));
    }
    let tenant = required("ANVIL_INDEX_QUALIFICATION_TENANT")?;
    let client_id = required("ANVIL_INDEX_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("ANVIL_INDEX_QUALIFICATION_CLIENT_SECRET")?;

    let mut channels = Vec::new();
    for endpoint in &endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let token = exchange_client_credentials(channels[0].clone(), client_id, client_secret)
        .await?
        .access_token;
    let mut objects = channels
        .iter()
        .cloned()
        .map(|channel| object_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut administrators = channels
        .iter()
        .cloned()
        .map(|channel| administration_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut indexes = channels
        .iter()
        .cloned()
        .map(|channel| index_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;

    let cases = engine_cases();
    let mut definitions = Vec::new();
    let endpoint_count = endpoints.len();
    for (position, case) in cases.iter().enumerate() {
        create_bucket(&mut administrators[position % endpoint_count], case.bucket).await?;
        let definition = indexes[position % endpoint_count]
            .create_index(CreateIndexRequest {
                bucket: case.bucket.into(),
                name: case.name.into(),
                path_prefix: "docs/".into(),
                content_type: CONTENT_TYPE.into(),
                specification: Some(case.specification.clone()),
                command_id: format!("qualification-create-{}", case.name),
            })
            .await?
            .into_inner();
        if definition.index_id == 0 || definition.version == 0 {
            return Err(invalid("created index has an invalid identity"));
        }
        definitions.push(definition);
    }

    let mut baseline = Vec::new();
    for (case, definition) in cases.iter().zip(&definitions) {
        let expected_paths = BTreeSet::new();
        let responses = wait_for_queries(
            &mut indexes,
            request(case),
            definition.index_id,
            definition.version,
            0,
            0,
            &expected_paths,
            false,
            endpoints.len(),
        )
        .await?;
        baseline.push(require_freshness(&responses[0])?.clone());
    }

    let mut write_number = 0_u64;
    let object_client_count = objects.len();
    let source_durability = qualification_durability(endpoint_count);
    for case in &cases {
        for (path, bytes) in &case.documents {
            let client = &mut objects[(write_number as usize) % object_client_count];
            put_json(
                client,
                &tenant,
                case.bucket,
                path,
                bytes,
                &format!("qualification-write-{write_number}"),
                source_durability,
            )
            .await?;
            write_number += 1;
        }
    }

    let mut first_generations = Vec::new();
    for ((case, definition), before) in cases.iter().zip(&definitions).zip(&baseline) {
        let expected = case.expected_paths.iter().copied().collect::<BTreeSet<_>>();
        let responses = wait_for_queries(
            &mut indexes,
            request(case),
            definition.index_id,
            definition.version,
            before.generation,
            case.documents.len() as u64,
            &expected,
            case.expects_scores,
            endpoints.len(),
        )
        .await?;
        require_checkpoint_advance(
            before,
            require_freshness(&responses[0])?,
            case.min_advanced_sources.min(endpoints.len()),
        )?;
        first_generations.push(responses[0].clone());
    }

    for (case, expected_response) in cases.iter().zip(&first_generations) {
        let expected = case
            .expected_paths
            .iter()
            .map(|path| (*path).to_owned())
            .collect::<BTreeSet<_>>();
        for client in &mut indexes {
            let paged =
                collect_paginated_paths(client, case, require_freshness(expected_response)?)
                    .await?;
            if paged != expected {
                return Err(invalid(format!(
                    "{} pagination returned {paged:?}, expected {expected:?}",
                    case.name
                )));
            }
        }
    }

    qualify_zanzibar_denial(
        &mut administrators[0],
        channels[0].clone(),
        &tenant,
        &cases[0],
    )
    .await?;
    qualify_anonymous_query(
        &mut administrators[0],
        &channels,
        &tenant,
        &cases[0],
        &first_generations[0],
    )
    .await?;

    for (case_number, case) in cases.iter().enumerate() {
        let put_client = &mut objects[(write_number as usize) % object_client_count];
        put_json(
            put_client,
            &tenant,
            case.bucket,
            case.replacement.0,
            case.replacement.1,
            &format!("qualification-replace-{case_number}"),
            source_durability,
        )
        .await?;
        write_number += 1;

        let delete_client = &mut objects[(write_number as usize) % object_client_count];
        let receipt = delete_client
            .delete(DeleteRequest {
                address: Some(ObjectAddress {
                    tenant: tenant.clone(),
                    bucket: case.bucket.into(),
                    path: case.delete_path.into(),
                }),
                command_id: format!("qualification-delete-{case_number}"),
                durability: source_durability as i32,
            })
            .await?
            .into_inner();
        if !receipt.deleted || receipt.version == 0 {
            return Err(invalid("index source delete returned an invalid receipt"));
        }
        write_number += 1;
    }

    for ((case, definition), before) in cases.iter().zip(&definitions).zip(&first_generations) {
        let expected = case
            .expected_paths
            .iter()
            .copied()
            .filter(|path| *path != case.delete_path)
            .collect::<BTreeSet<_>>();
        let before_freshness = require_freshness(before)?;
        let before_replacement_version = hit_version(before, case.replacement.0)?;
        let responses = wait_for_queries(
            &mut indexes,
            request(case),
            definition.index_id,
            definition.version,
            before_freshness.generation,
            case.documents.len() as u64 + 2,
            &expected,
            case.expects_scores,
            endpoints.len(),
        )
        .await?;
        let after_replacement_version = hit_version(&responses[0], case.replacement.0)?;
        if after_replacement_version <= before_replacement_version {
            return Err(invalid(format!(
                "{} replacement remained on version {before_replacement_version}",
                case.name
            )));
        }
        require_checkpoint_advance(before_freshness, require_freshness(&responses[0])?, 1)?;
    }

    if env::var("ANVIL_INDEX_QUALIFICATION_REQUIRE_QUIESCENCE").is_ok_and(|value| value == "1") {
        require_generation_quiescence(&mut indexes[0], &cases[0]).await?;
    }

    println!(
        "index qualification passed on {} node(s): {} engines, {} put/delete mutations",
        endpoints.len(),
        cases.len(),
        write_number
    );
    Ok(())
}

async fn require_generation_quiescence(
    client: &mut IndexClient,
    case: &EngineCase,
) -> TestResult<()> {
    let deadline = Instant::now() + GENERATION_QUIET_LIMIT;
    let mut observed_generation = None;
    let mut stable_since = Instant::now();
    let mut advances = 0_u64;

    loop {
        let response = client.query_index(request(case)).await?.into_inner();
        let generation = require_freshness(&response)?.generation;
        if generation == 0 {
            return Err(invalid(
                "index generation disappeared while checking quiescence",
            ));
        }
        match observed_generation {
            Some(previous) if previous == generation => {}
            Some(_) => {
                observed_generation = Some(generation);
                stable_since = Instant::now();
                advances = advances.saturating_add(1);
            }
            None => {
                observed_generation = Some(generation);
                stable_since = Instant::now();
            }
        }

        if stable_since.elapsed() >= GENERATION_QUIET_WINDOW {
            println!(
                "index generation {generation} remained stable for {} seconds",
                GENERATION_QUIET_WINDOW.as_secs()
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "index generation did not quiesce without source mutations: \
                 observed {advances} advances in {} seconds (latest generation {generation})",
                GENERATION_QUIET_LIMIT.as_secs()
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn index_client(
    channel: Channel,
    token: &str,
) -> Result<IndexClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        IndexServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(72 * 1024 * 1024)
            .max_decoding_message_size(72 * 1024 * 1024),
    )
}

async fn create_bucket(client: &mut RawAdministrationClient, bucket: &str) -> TestResult<()> {
    let created = client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.into(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await?
        .into_inner();
    if created.bucket != bucket {
        return Err(invalid("bucket creation returned another bucket"));
    }
    Ok(())
}

async fn qualify_zanzibar_denial(
    administrator: &mut RawAdministrationClient,
    channel: Channel,
    tenant: &str,
    case: &EngineCase,
) -> TestResult<()> {
    let app_id = format!("{tenant}-index-denied");
    let client_id = format!("{tenant}-index-denied-client");
    let client_secret = "qualification-index-denied-secret-with-at-least-32-bytes";
    let credential = administrator
        .create_application(CreateApplicationRequest {
            app_id: app_id.clone(),
            client_id: client_id.clone(),
            client_secret: client_secret.into(),
        })
        .await?
        .into_inner();
    if credential.storage_tenant != tenant
        || credential.app_id != app_id
        || credential.client_id != client_id
        || !credential.active
    {
        return Err(invalid(
            "unprivileged application creation returned another identity",
        ));
    }
    let denied_token = exchange_client_credentials(channel.clone(), client_id, client_secret)
        .await?
        .access_token;
    let mut denied = index_client(channel, &denied_token)?;
    let status = denied
        .query_index(request(case))
        .await
        .expect_err("unprivileged application unexpectedly queried an index");
    if status.code() != Code::PermissionDenied {
        return Err(invalid(format!(
            "unprivileged index query returned {:?}, expected PermissionDenied",
            status.code()
        )));
    }
    Ok(())
}

async fn qualify_anonymous_query(
    administrator: &mut RawAdministrationClient,
    channels: &[Channel],
    tenant: &str,
    case: &EngineCase,
    expected: &QueryIndexResponse,
) -> TestResult<()> {
    let query = anonymous_request(case, tenant);
    let mut public = public_index_client(channels[0].clone());
    assert_status(
        public
            .query_index(query.clone())
            .await
            .expect_err("a private index unexpectedly allowed anonymous query")
            .code(),
        Code::PermissionDenied,
        "private anonymous index query",
    )?;

    let mut missing_tenant = query.clone();
    missing_tenant.tenant.clear();
    assert_status(
        public
            .query_index(missing_tenant)
            .await
            .expect_err("anonymous index query unexpectedly inferred a tenant")
            .code(),
        Code::InvalidArgument,
        "anonymous index query without tenant",
    )?;
    assert_status(
        public
            .get_index(GetIndexRequest {
                bucket: case.bucket.into(),
                name: case.name.into(),
            })
            .await
            .expect_err("anonymous index management unexpectedly succeeded")
            .code(),
        Code::Unauthenticated,
        "anonymous index management",
    )?;

    let mut invalid_request = Request::new(query.clone());
    invalid_request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer invalid-index-token")?,
    );
    assert_status(
        public
            .query_index(invalid_request)
            .await
            .expect_err("an invalid bearer unexpectedly degraded to anonymous")
            .code(),
        Code::Unauthenticated,
        "invalid index bearer",
    )?;

    administrator
        .set_bucket_public_read(SetBucketPublicReadRequest {
            bucket: case.bucket.into(),
            enabled: true,
        })
        .await?;
    let expected_paths = expected
        .hits
        .iter()
        .filter_map(hit_path)
        .collect::<BTreeSet<_>>();
    for channel in channels {
        let response = public_index_client(channel.clone())
            .query_index(query.clone())
            .await?
            .into_inner();
        let actual_paths = response
            .hits
            .iter()
            .filter_map(hit_path)
            .collect::<BTreeSet<_>>();
        if actual_paths != expected_paths || response.freshness.is_none() {
            return Err(invalid(format!(
                "anonymous index query returned {actual_paths:?}, expected {expected_paths:?}"
            )));
        }
    }

    administrator
        .set_bucket_public_read(SetBucketPublicReadRequest {
            bucket: case.bucket.into(),
            enabled: false,
        })
        .await?;
    assert_status(
        public_index_client(channels[0].clone())
            .query_index(query)
            .await
            .expect_err("revoked anonymous index query unexpectedly succeeded")
            .code(),
        Code::PermissionDenied,
        "revoked anonymous index query",
    )
}

fn public_index_client(channel: Channel) -> IndexServiceClient<Channel> {
    IndexServiceClient::new(channel)
        .max_encoding_message_size(72 * 1024 * 1024)
        .max_decoding_message_size(72 * 1024 * 1024)
}

fn anonymous_request(case: &EngineCase, tenant: &str) -> QueryIndexRequest {
    let mut request = request(case);
    request.tenant = tenant.into();
    request
}

fn assert_status(actual: Code, expected: Code, context: &str) -> TestResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(invalid(format!(
            "{context} returned {actual:?}, expected {expected:?}"
        )))
    }
}

async fn put_json(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    bytes: &[u8],
    command_id: &str,
    durability: Durability,
) -> TestResult<()> {
    let receipt = put_chunks(
        client,
        PutHeader {
            address: Some(ObjectAddress {
                tenant: tenant.into(),
                bucket: bucket.into(),
                path: path.into(),
            }),
            content_type: CONTENT_TYPE.into(),
            command_id: command_id.into(),
            durability: durability as i32,
            operation: Some(PutOperationValue::Put(PutOperation {})),
        },
        [bytes.to_vec()],
    )
    .await?;
    if receipt.version == 0 || receipt.deleted {
        return Err(invalid("index source write returned an invalid receipt"));
    }
    Ok(())
}

fn qualification_durability(endpoint_count: usize) -> Durability {
    if endpoint_count == 1 {
        Durability::Local
    } else {
        Durability::Replicated
    }
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_queries(
    clients: &mut [IndexClient],
    request: QueryIndexRequest,
    index_id: u64,
    definition_version: u64,
    after_generation: u64,
    indexed_objects: u64,
    expected_paths: &BTreeSet<&str>,
    expects_scores: bool,
    expected_sources: usize,
) -> TestResult<Vec<QueryIndexResponse>> {
    let deadline = Instant::now() + WAIT_LIMIT;
    let mut last = String::new();
    loop {
        let mut responses = Vec::with_capacity(clients.len());
        for client in clients.iter_mut() {
            match client.query_index(request.clone()).await {
                Ok(response) => responses.push(response.into_inner()),
                Err(status) if retryable(&status) => {
                    last = status.to_string();
                    responses.clear();
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if responses.len() == clients.len()
            && responses.iter().all(|response| {
                response_matches(
                    response,
                    index_id,
                    definition_version,
                    after_generation,
                    indexed_objects,
                    expected_paths,
                    expects_scores,
                    expected_sources,
                )
            })
            && routed_responses_agree(&responses)
        {
            return Ok(responses);
        }
        if !responses.is_empty() {
            last = responses
                .iter()
                .map(|response| {
                    let freshness = response.freshness.as_ref();
                    format!(
                        "generation={} hits={}",
                        freshness.map_or(0, |value| value.generation),
                        response.hits.len(),
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "index query did not converge before timeout; last error: {last}"
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn routed_responses_agree(responses: &[QueryIndexResponse]) -> bool {
    responses.windows(2).all(|pair| {
        pair[0].hits == pair[1].hits
            && pair[0].next_page_token == pair[1].next_page_token
            && stable_freshness_agrees(pair[0].freshness.as_ref(), pair[1].freshness.as_ref())
    })
}

fn stable_freshness_agrees(left: Option<&IndexFreshness>, right: Option<&IndexFreshness>) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return false;
    };
    let mut left = left.clone();
    let mut right = right.clone();
    for source in &mut left.sources {
        source.observed_tail = None;
        source.lag_hint = 0;
    }
    for source in &mut right.sources {
        source.observed_tail = None;
        source.lag_hint = 0;
    }
    left == right
}

fn response_matches(
    response: &QueryIndexResponse,
    index_id: u64,
    definition_version: u64,
    after_generation: u64,
    indexed_objects: u64,
    expected_paths: &BTreeSet<&str>,
    expects_scores: bool,
    expected_sources: usize,
) -> bool {
    let Some(freshness) = response.freshness.as_ref() else {
        return false;
    };
    if freshness.generation <= after_generation
        || freshness.published_at.is_none()
        || !freshness.initial_build_complete
        || freshness.rebuilding
        || freshness.index_id != index_id
        || freshness.definition_version != definition_version
        || freshness.placement_term == 0
        || freshness.placement_index == 0
        || freshness.sources.len() != expected_sources
        || freshness.authorization_revision == 0
    {
        return false;
    }
    let source_ids = freshness
        .sources
        .iter()
        .map(|source| source.node_id)
        .collect::<BTreeSet<_>>();
    if source_ids.len() != expected_sources
        || freshness
            .sources
            .iter()
            .any(|source| source.node_id == 0 || source.source_epoch.len() != 32)
    {
        return false;
    }
    let actual = response
        .hits
        .iter()
        .filter_map(hit_path)
        .collect::<BTreeSet<_>>();
    actual == *expected_paths
        && indexed_objects >= response.hits.len() as u64
        && response.hits.len() == expected_paths.len()
        && response.hits.iter().all(|hit| {
            hit.object_version != 0
                && hit.score.is_some() == expects_scores
                && hit.score.is_none_or(f32::is_finite)
        })
}

fn require_checkpoint_advance(
    before: &IndexFreshness,
    after: &IndexFreshness,
    minimum_sources: usize,
) -> TestResult<()> {
    let advanced_sources = after
        .sources
        .iter()
        .zip(&before.sources)
        .filter(|(new, old)| {
            new.node_id == old.node_id
                && new.source_epoch == old.source_epoch
                && new.indexed_next_offset > old.indexed_next_offset
        })
        .count();
    if after.generation <= before.generation
        || after.sources.len() != before.sources.len()
        || advanced_sources < minimum_sources
    {
        return Err(invalid(format!(
            "published index generation advanced {advanced_sources} cluster source checkpoints; expected at least {minimum_sources}"
        )));
    }
    Ok(())
}

fn require_freshness(response: &QueryIndexResponse) -> TestResult<&IndexFreshness> {
    response
        .freshness
        .as_ref()
        .ok_or_else(|| invalid("index response omitted freshness evidence"))
}

fn hit_path(hit: &IndexQueryHit) -> Option<&str> {
    hit.address.as_ref().map(|address| address.path.as_str())
}

fn hit_version(response: &QueryIndexResponse, path: &str) -> TestResult<u64> {
    response
        .hits
        .iter()
        .find(|hit| hit_path(hit) == Some(path))
        .map(|hit| hit.object_version)
        .ok_or_else(|| invalid(format!("index response omitted expected path {path}")))
}

fn request(case: &EngineCase) -> QueryIndexRequest {
    QueryIndexRequest {
        bucket: case.bucket.into(),
        index_name: case.name.into(),
        query: Some(case.query.clone()),
        limit: 100,
        page_token: Vec::new(),
        tenant: String::new(),
    }
}

async fn collect_paginated_paths(
    client: &mut IndexClient,
    case: &EngineCase,
    expected_freshness: &IndexFreshness,
) -> TestResult<BTreeSet<String>> {
    let mut request = request(case);
    request.limit = 1;
    let mut paths = BTreeSet::new();
    let mut previous_token = Vec::new();
    loop {
        request.page_token = previous_token.clone();
        let response = client.query_index(request.clone()).await?.into_inner();
        let freshness = require_freshness(&response)?;
        if !stable_freshness_agrees(Some(expected_freshness), Some(freshness)) {
            return Err(invalid(format!(
                "{} pagination changed its pinned generation",
                case.name
            )));
        }
        for hit in &response.hits {
            let path = hit_path(hit)
                .ok_or_else(|| invalid("paginated index hit omitted its object address"))?;
            if !paths.insert(path.to_owned()) {
                return Err(invalid(format!(
                    "{} pagination returned duplicate path {path}",
                    case.name
                )));
            }
        }
        if response.next_page_token.is_empty() {
            return Ok(paths);
        }
        if response.next_page_token == previous_token {
            return Err(invalid(format!(
                "{} pagination returned a non-advancing token",
                case.name
            )));
        }
        previous_token = response.next_page_token;
    }
}

fn retryable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::NotFound
            | tonic::Code::FailedPrecondition
    ) || (status.code() == tonic::Code::Cancelled && status.message() == "Timeout expired")
}

fn engine_cases() -> Vec<EngineCase> {
    vec![
        EngineCase {
            bucket: "index-journal-events",
            name: "paths",
            specification: specification(SpecificationValue::Path(PathIndexSpec {})),
            query: query(QueryValue::Path(PathIndexQuery {
                prefix: "docs/".into(),
                start_after: None,
            })),
            documents: vec![
                ("docs/a.json", br#"{"value":"a"}"#),
                ("docs/b.json", br#"{"value":"b"}"#),
                ("docs/c.json", br#"{"value":"c"}"#),
                ("docs/d.json", br#"{"value":"d"}"#),
                ("docs/e.json", br#"{"value":"e"}"#),
                ("docs/f.json", br#"{"value":"f"}"#),
                ("docs/g.json", br#"{"value":"g"}"#),
                ("docs/h.json", br#"{"value":"h"}"#),
                ("docs/i.json", br#"{"value":"i"}"#),
                ("docs/j.json", br#"{"value":"j"}"#),
                ("docs/k.json", br#"{"value":"k"}"#),
                ("docs/l.json", br#"{"value":"l"}"#),
            ],
            expected_paths: vec![
                "docs/a.json",
                "docs/b.json",
                "docs/c.json",
                "docs/d.json",
                "docs/e.json",
                "docs/f.json",
                "docs/g.json",
                "docs/h.json",
                "docs/i.json",
                "docs/j.json",
                "docs/k.json",
                "docs/l.json",
            ],
            replacement: ("docs/a.json", br#"{"value":"a-replaced"}"#),
            delete_path: "docs/b.json",
            expects_scores: false,
            min_advanced_sources: 3,
        },
        EngineCase {
            bucket: "index-typed-json",
            name: "active-documents",
            specification: specification(SpecificationValue::TypedJson(TypedJsonIndexSpec {
                fields: vec![IndexField {
                    name: "status".into(),
                    json_pointer: "/status".into(),
                }],
            })),
            query: query(QueryValue::TypedJson(TypedJsonIndexQuery {
                predicates: vec![IndexPredicate {
                    field: "status".into(),
                    operator: IndexPredicateOperator::Equal as i32,
                    values_json: vec![br#""active""#.to_vec()],
                }],
                order: Vec::new(),
            })),
            documents: vec![
                ("docs/active-a.json", br#"{"status":"active"}"#),
                ("docs/inactive.json", br#"{"status":"inactive"}"#),
                ("docs/active-b.json", br#"{"status":"active"}"#),
            ],
            expected_paths: vec!["docs/active-a.json", "docs/active-b.json"],
            replacement: (
                "docs/active-a.json",
                br#"{"status":"active","revision":2}"#,
            ),
            delete_path: "docs/active-b.json",
            expects_scores: false,
            min_advanced_sources: 1,
        },
        EngineCase {
            bucket: "index-object-metadata",
            name: "object-heads",
            specification: specification(SpecificationValue::MetadataFilter(
                MetadataFilterIndexSpec {
                    fields: vec!["path".into(), "content_type".into()],
                },
            )),
            query: query(QueryValue::MetadataFilter(MetadataFilterIndexQuery {
                predicates: vec![IndexPredicate {
                    field: "path".into(),
                    operator: IndexPredicateOperator::Prefix as i32,
                    values_json: vec![br#""docs/keep-""#.to_vec()],
                }],
            })),
            documents: vec![
                ("docs/keep-a.json", br#"{"value":"a"}"#),
                ("docs/drop.json", br#"{"value":"b"}"#),
                ("docs/keep-b.json", br#"{"value":"c"}"#),
            ],
            expected_paths: vec!["docs/keep-a.json", "docs/keep-b.json"],
            replacement: ("docs/keep-a.json", br#"{"value":"a-replaced"}"#),
            delete_path: "docs/keep-b.json",
            expects_scores: false,
            min_advanced_sources: 1,
        },
        EngineCase {
            bucket: "index-full-text",
            name: "search",
            specification: specification(SpecificationValue::FullText(FullTextIndexSpec {
                fields: vec![FullTextField {
                    name: "body".into(),
                    json_pointer: "/body".into(),
                }],
            })),
            query: query(QueryValue::FullText(FullTextIndexQuery {
                text: "durable journal".into(),
                phrase: true,
            })),
            documents: vec![
                (
                    "docs/journal-a.json",
                    br#"{"body":"durable journal delivery"}"#,
                ),
                ("docs/unrelated.json", br#"{"body":"another subject"}"#),
                ("docs/journal-b.json", br#"{"body":"a durable journal"}"#),
            ],
            expected_paths: vec!["docs/journal-a.json", "docs/journal-b.json"],
            replacement: (
                "docs/journal-a.json",
                br#"{"body":"durable journal replacement"}"#,
            ),
            delete_path: "docs/journal-b.json",
            expects_scores: true,
            min_advanced_sources: 1,
        },
        EngineCase {
            bucket: "index-vector",
            name: "embeddings",
            specification: specification(SpecificationValue::Vector(vector_spec())),
            query: query(QueryValue::Vector(VectorIndexQuery {
                values: vec![1.0, 0.0, 0.0],
            })),
            documents: semantic_documents(),
            expected_paths: semantic_paths(),
            replacement: (
                "docs/rust.json",
                br#"{"title":"rust search replacement","embedding":[0.9,0.1,0.0]}"#,
            ),
            delete_path: "docs/storage.json",
            expects_scores: true,
            min_advanced_sources: 1,
        },
        EngineCase {
            bucket: "index-hybrid",
            name: "semantic-text",
            specification: specification(SpecificationValue::Hybrid(HybridIndexSpec {
                full_text: Some(FullTextIndexSpec {
                    fields: vec![FullTextField {
                        name: "title".into(),
                        json_pointer: "/title".into(),
                    }],
                }),
                vector: Some(vector_spec()),
                full_text_weight: 0.0,
                vector_weight: 0.0,
            })),
            query: query(QueryValue::Hybrid(HybridIndexQuery {
                text: "rust search".into(),
                vector: vec![1.0, 0.0, 0.0],
            })),
            documents: semantic_documents(),
            expected_paths: semantic_paths(),
            replacement: (
                "docs/rust.json",
                br#"{"title":"rust search replacement","embedding":[0.9,0.1,0.0]}"#,
            ),
            delete_path: "docs/storage.json",
            expects_scores: true,
            min_advanced_sources: 1,
        },
        EngineCase {
            bucket: "index-git-source",
            name: "git-tree",
            specification: specification(SpecificationValue::GitSource(GitSourceIndexSpec {
                repository_id: "qualification-repository".into(),
            })),
            query: query(QueryValue::GitSource(GitSourceIndexQuery {
                commit_id: "qualification-commit".into(),
                tree_path: "src/".into(),
                prefix: true,
            })),
            documents: vec![
                (
                    "docs/git-lib.json",
                    br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"src/lib.rs","object_id":"1111111111111111111111111111111111111111","pack_path":"docs/git-lib.json","pack_version":1,"offset":0,"length":128}"#,
                ),
                (
                    "docs/git-main.json",
                    br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"src/main.rs","object_id":"2222222222222222222222222222222222222222","pack_path":"docs/git-main.json","pack_version":1,"offset":128,"length":256}"#,
                ),
            ],
            expected_paths: vec!["docs/git-lib.json", "docs/git-main.json"],
            replacement: (
                "docs/git-lib.json",
                br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"src/lib.rs","object_id":"3333333333333333333333333333333333333333","pack_path":"docs/git-lib.json","pack_version":2,"offset":256,"length":192}"#,
            ),
            delete_path: "docs/git-main.json",
            expects_scores: false,
            min_advanced_sources: 1,
        },
        EngineCase {
            bucket: "index-tensor",
            name: "model-tensors",
            specification: specification(SpecificationValue::Tensor(TensorIndexSpec {
                model_id: "qualification-model".into(),
            })),
            query: query(QueryValue::Tensor(TensorIndexQuery {
                tensor_name: "encoder.weight".into(),
            })),
            documents: vec![
                (
                    "docs/tensor-encoder.json",
                    br#"{"model_id":"qualification-model","tensor_name":"encoder.shadow","source_path":"docs/tensor-encoder.json","source_version":1,"offset":0,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
                (
                    "docs/tensor-decoder.json",
                    br#"{"model_id":"qualification-model","tensor_name":"decoder.bias","source_path":"docs/tensor-decoder.json","source_version":1,"offset":128,"length":32,"dtype":"f32","shape":[8]}"#,
                ),
                (
                    "docs/tensor-encoder-copy.json",
                    br#"{"model_id":"qualification-model","tensor_name":"encoder.weight","source_path":"docs/tensor-encoder-copy.json","source_version":1,"offset":160,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
            ],
            expected_paths: vec!["docs/tensor-encoder-copy.json"],
            replacement: (
                "docs/tensor-encoder-copy.json",
                br#"{"model_id":"qualification-model","tensor_name":"encoder.weight","source_path":"docs/tensor-encoder-copy.json","source_version":2,"offset":288,"length":128,"dtype":"f32","shape":[8,4]}"#,
            ),
            delete_path: "docs/tensor-decoder.json",
            expects_scores: false,
            min_advanced_sources: 1,
        },
    ]
}

fn semantic_documents() -> Vec<(&'static str, &'static [u8])> {
    vec![
        (
            "docs/rust.json",
            br#"{"title":"rust search","embedding":[1.0,0.0,0.0]}"#,
        ),
        (
            "docs/storage.json",
            br#"{"title":"durable storage","embedding":[0.8,0.2,0.0]}"#,
        ),
        (
            "docs/music.json",
            br#"{"title":"music library","embedding":[0.0,0.0,1.0]}"#,
        ),
    ]
}

fn semantic_paths() -> Vec<&'static str> {
    vec!["docs/music.json", "docs/rust.json", "docs/storage.json"]
}

fn vector_spec() -> VectorIndexSpec {
    VectorIndexSpec {
        json_pointer: "/embedding".into(),
        dimensions: 3,
        metric: VectorMetric::Cosine as i32,
        normalize: true,
    }
}

fn specification(value: SpecificationValue) -> IndexSpecification {
    IndexSpecification {
        specification: Some(value),
    }
}

fn query(value: QueryValue) -> IndexQuery {
    IndexQuery { query: Some(value) }
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use anvil_storage::v1::{Durability, IndexFreshness, IndexSourceFreshness, QueryIndexResponse};

    use super::{
        SpecificationValue, engine_cases, qualification_durability, retryable,
        routed_responses_agree,
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
            assert!(!case.expected_paths.is_empty());
            assert!(case.expected_paths.contains(&case.replacement.0));
            assert_ne!(case.replacement.0, case.delete_path);
            if !matches!(
                case.specification.specification,
                Some(SpecificationValue::Tensor(_))
            ) {
                assert!(case.expected_paths.len() > 1, "{} must paginate", case.name);
                assert!(case.expected_paths.contains(&case.delete_path));
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
    fn source_writes_request_only_satisfiable_topology_durability() {
        assert_eq!(qualification_durability(1), Durability::Local);
        assert_eq!(qualification_durability(3), Durability::Replicated);
    }

    fn routed_response() -> QueryIndexResponse {
        QueryIndexResponse {
            hits: Vec::new(),
            next_page_token: vec![1, 2, 3],
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
}
