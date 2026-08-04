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
    CreateBucketRequest, CreateIndexRequest, Durability, FullTextField, FullTextIndexQuery,
    FullTextIndexSpec, GitSourceIndexQuery, GitSourceIndexSpec, HybridIndexQuery, HybridIndexSpec,
    IndexField, IndexFreshness, IndexPredicate, IndexPredicateOperator, IndexQuery, IndexQueryHit,
    IndexSpecification, MetadataFilterIndexQuery, MetadataFilterIndexSpec, ObjectAddress,
    ObjectVersioning, PathIndexQuery, PathIndexSpec, PutHeader, PutOperation, QueryIndexRequest,
    QueryIndexResponse, TensorIndexQuery, TensorIndexSpec, TypedJsonIndexQuery, TypedJsonIndexSpec,
    VectorIndexQuery, VectorIndexSpec, VectorMetric,
};
use anvil_storage::{
    BearerToken, RawAdministrationClient, RawClient, administration_client, connect_channel,
    exchange_client_credentials, object_client, put_chunks,
};
use tokio::time::{Instant, sleep};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const CONTENT_TYPE: &str = "application/json";

#[derive(Clone)]
struct EngineCase {
    bucket: &'static str,
    name: &'static str,
    specification: IndexSpecification,
    query: IndexQuery,
    documents: Vec<(&'static str, &'static [u8])>,
    expected_paths: Vec<&'static str>,
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
            )
            .await?;
            write_number += 1;
        }
    }

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
    }

    println!(
        "index qualification passed on {} node(s): {} engines, {} writes",
        endpoints.len(),
        cases.len(),
        write_number
    );
    Ok(())
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

async fn put_json(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    bytes: &[u8],
    command_id: &str,
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
            durability: Durability::Replicated as i32,
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

fn request(case: &EngineCase) -> QueryIndexRequest {
    QueryIndexRequest {
        bucket: case.bucket.into(),
        index_name: case.name.into(),
        query: Some(case.query.clone()),
        limit: 100,
        page_token: Vec::new(),
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
                    br#"{"model_id":"qualification-model","tensor_name":"encoder.weight","source_path":"docs/tensor-encoder.json","source_version":1,"offset":0,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
                (
                    "docs/tensor-decoder.json",
                    br#"{"model_id":"qualification-model","tensor_name":"decoder.bias","source_path":"docs/tensor-decoder.json","source_version":1,"offset":128,"length":32,"dtype":"f32","shape":[8]}"#,
                ),
            ],
            expected_paths: vec!["docs/tensor-encoder.json"],
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
    use anvil_storage::v1::{IndexFreshness, IndexSourceFreshness, QueryIndexResponse};

    use super::{retryable, routed_responses_agree};

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
