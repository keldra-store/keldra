//! Public-API index qualification for one- and three-node Docker harnesses.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Duration;

use anvil_storage::v1::index_field::FieldType as IndexFieldType;
use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_service_client::IndexServiceClient;
use anvil_storage::v1::index_specification::Specification as SpecificationValue;
use anvil_storage::v1::put_header::Operation as PutOperationValue;
use anvil_storage::v1::{
    BooleanIndexField, CreateApplicationRequest, CreateBucketRequest, CreateIndexRequest,
    DeleteRequest, Durability, FloatIndexField, FullTextField, FullTextIndexQuery,
    FullTextIndexSpec, GetIndexRequest, GitSourceIndexQuery, GitSourceIndexSpec, HybridIndexQuery,
    HybridIndexSpec, IndexAggregateOperation, IndexAggregateRequest, IndexDefinition,
    IndexFacetRequest, IndexField, IndexFieldCapability, IndexFieldCardinality, IndexFreshness,
    IndexOrder, IndexOrderDirection, IndexPredicate, IndexPredicateOperator, IndexQuery,
    IndexQueryHit, IndexSpecification, KeywordIndexField, MetadataFilterIndexQuery,
    MetadataFilterIndexSpec, ObjectAddress, ObjectVersioning, PathIndexQuery, PathIndexSpec,
    PutHeader, PutOperation, QueryIndexRequest, QueryIndexResponse, RebuildIndexRequest,
    SetBucketPublicReadRequest, SignedIntegerIndexField, TensorIndexQuery, TensorIndexSpec,
    TextAnalyzer, TextIndexField, TypedJsonIndexQuery, TypedJsonIndexSpec,
    UnsignedIntegerIndexField, VectorIndexQuery, VectorIndexSpec, VectorMetric,
};
use anvil_storage::{
    BearerToken, RawAdministrationClient, RawClient, administration_client, connect_channel,
    exchange_client_credentials, object_client, put_chunks,
};
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, sleep};
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Request};

#[path = "cluster_index_qualification/definition_lifecycle.rs"]
mod definition_lifecycle;
#[path = "cluster_index_qualification/typed_capabilities.rs"]
mod typed_capabilities;
use typed_capabilities::{
    boolean_field, float_field, keyword_field, keyword_multi_field, signed_integer_field,
    signed_integer_multi_field, text_field, typed_json_order, unsigned_integer_field,
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const GENERATION_QUIET_WINDOW: Duration = Duration::from_secs(3);
const GENERATION_QUIET_LIMIT: Duration = Duration::from_secs(12);
const CONTENT_TYPE: &str = "application/json";
const COMPACTION_WAVES: usize = 5;
const VERIFICATION_STATE_SCHEMA: &str = "anvil.index-qualification-state.v1";

#[derive(Clone)]
struct EngineCase {
    bucket: &'static str,
    name: &'static str,
    specification: IndexSpecification,
    query: IndexQuery,
    documents: Vec<(&'static str, &'static [u8])>,
    expected_paths: Vec<&'static str>,
    replacement: (&'static str, &'static [u8]),
    replacement_hit_path: &'static str,
    delete_path: &'static str,
    delete_hit_path: &'static str,
    expects_scores: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct VerificationState {
    schema: String,
    tenant: String,
    source_count: usize,
    indexes: Vec<VerificationIndex>,
}

#[derive(Debug, Deserialize, Serialize)]
struct VerificationIndex {
    bucket: String,
    name: String,
    index_id: u64,
    definition_version: u64,
    generation: u64,
    placement_term: u64,
    placement_index: u64,
    sources: Vec<VerificationSource>,
    hits: Vec<VerificationHit>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct VerificationSource {
    node_id: u64,
    source_epoch: Vec<u8>,
    indexed_next_offset: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct VerificationHit {
    path: String,
    object_version: u64,
    score_bits: Option<u32>,
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
    let mut indexes = channels
        .iter()
        .cloned()
        .map(|channel| index_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let cases = engine_cases();
    if let Some(path) = env::var_os("ANVIL_INDEX_QUALIFICATION_STATE_INPUT") {
        verify_existing_state(Path::new(&path), &tenant, &cases, &mut indexes).await?;
        println!(
            "verified {} final index generations through {} endpoint(s)",
            cases.len(),
            endpoints.len()
        );
        return Ok(());
    }

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
        let expected_versions = BTreeMap::new();
        let responses = wait_for_queries(
            &mut indexes,
            request(case),
            definition.index_id,
            definition.version,
            0,
            0,
            &expected_paths,
            &expected_versions,
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
        if endpoint_count == 3 && case.documents.len() < endpoint_count {
            return Err(invalid(format!(
                "{} does not provide one source document per ingress node",
                case.name
            )));
        }
        for (document_number, (path, bytes)) in case.documents.iter().enumerate() {
            let client = &mut objects[document_number % object_client_count];
            put_index_document(
                client,
                &tenant,
                case,
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
        let expected_versions = BTreeMap::new();
        let responses = wait_for_queries(
            &mut indexes,
            request(case),
            definition.index_id,
            definition.version,
            before.generation,
            case.documents.len() as u64,
            &expected,
            &expected_versions,
            case.expects_scores,
            endpoints.len(),
        )
        .await?;
        require_physical_order(case, &responses, false)?;
        require_checkpoint_advance(before, require_freshness(&responses[0])?, endpoints.len())?;
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
            let paged_set = paged.iter().cloned().collect::<BTreeSet<_>>();
            if paged_set != expected || !physical_order_matches(case, &paged, false) {
                return Err(invalid(format!(
                    "{} pagination returned {paged:?}, expected {expected:?}",
                    case.name
                )));
            }
        }
    }

    let typed_position = cases
        .iter()
        .position(|case| {
            matches!(
                case.specification.specification.as_ref(),
                Some(SpecificationValue::TypedJson(_))
            )
        })
        .ok_or_else(|| invalid("index qualification omitted the Typed JSON engine"))?;
    typed_capabilities::qualify(
        &mut indexes,
        &tenant,
        &cases[typed_position],
        require_freshness(&first_generations[typed_position])?,
    )
    .await?;

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
        put_index_document(
            put_client,
            &tenant,
            case,
            case.bucket,
            case.replacement.0,
            case.replacement.1,
            &format!("qualification-replace-{case_number}"),
            source_durability,
        )
        .await?;
        write_number += 1;

        let delete_client = &mut objects[(write_number as usize) % object_client_count];
        delete_object(
            delete_client,
            DeleteRequest {
                address: Some(ObjectAddress {
                    tenant: tenant.clone(),
                    bucket: case.bucket.into(),
                    path: case.delete_path.into(),
                }),
                command_id: format!("qualification-delete-{case_number}"),
                durability: source_durability as i32,
            },
            "index source delete",
        )
        .await?;
        write_number += 1;
    }

    let mut mutation_generations = Vec::with_capacity(cases.len());
    for ((case, definition), before) in cases.iter().zip(&definitions).zip(&first_generations) {
        let expected = expected_after_primary_mutations(case);
        let expected_versions = BTreeMap::new();
        let before_freshness = require_freshness(before)?;
        let before_replacement_version = hit_version(before, case.replacement_hit_path)?;
        let responses = wait_for_queries(
            &mut indexes,
            request(case),
            definition.index_id,
            definition.version,
            before_freshness.generation,
            case.documents.len() as u64 + 2,
            &expected,
            &expected_versions,
            case.expects_scores,
            endpoints.len(),
        )
        .await?;
        let after_replacement_version = hit_version(&responses[0], case.replacement_hit_path)?;
        if after_replacement_version <= before_replacement_version {
            return Err(invalid(format!(
                "{} replacement remained on version {before_replacement_version}",
                case.name
            )));
        }
        require_checkpoint_advance(before_freshness, require_freshness(&responses[0])?, 1)?;
        mutation_generations.push(responses[0].clone());
    }

    let mut final_hit_versions = vec![0_u64; cases.len()];
    for wave in 0..COMPACTION_WAVES {
        for (case_number, case) in cases.iter().enumerate() {
            let put_client = &mut objects[(case_number + wave) % object_client_count];
            let hit_version = put_index_document(
                put_client,
                &tenant,
                case,
                case.bucket,
                case.replacement.0,
                case.replacement.1,
                &format!("qualification-compaction-{case_number}-{wave}"),
                source_durability,
            )
            .await?;
            final_hit_versions[case_number] = hit_version;
            write_number += 1;
        }

        let previous_generations = mutation_generations.clone();
        for (case_number, ((case, definition), before)) in cases
            .iter()
            .zip(&definitions)
            .zip(&previous_generations)
            .enumerate()
        {
            let expected = expected_after_primary_mutations(case);
            let expected_versions =
                BTreeMap::from([(case.replacement_hit_path, final_hit_versions[case_number])]);
            let before_freshness = require_freshness(before)?;
            let responses = wait_for_queries(
                &mut indexes,
                request(case),
                definition.index_id,
                definition.version,
                before_freshness.generation,
                case.documents.len() as u64 + 3 + wave as u64,
                &expected,
                &expected_versions,
                case.expects_scores,
                endpoints.len(),
            )
            .await?;
            require_checkpoint_advance(before_freshness, require_freshness(&responses[0])?, 1)?;
            mutation_generations[case_number] = responses[0].clone();
        }
    }

    let tensor_position = cases
        .iter()
        .position(|case| {
            matches!(
                case.specification.specification.as_ref(),
                Some(SpecificationValue::Tensor(_))
            )
        })
        .ok_or_else(|| invalid("index qualification omitted the Tensor engine"))?;
    let tensor_case = &cases[tensor_position];
    let before_tensor_delete = require_freshness(&mutation_generations[tensor_position])?.clone();
    let delete_client = &mut objects[(write_number as usize) % object_client_count];
    delete_object(
        delete_client,
        DeleteRequest {
            address: Some(ObjectAddress {
                tenant: tenant.clone(),
                bucket: tensor_case.bucket.into(),
                path: tensor_case.replacement.0.into(),
            }),
            command_id: "qualification-delete-tensor-result".into(),
            durability: source_durability as i32,
        },
        "tensor index source delete",
    )
    .await?;
    write_number += 1;
    let expected = BTreeSet::new();
    let expected_versions = BTreeMap::new();
    let responses = wait_for_queries(
        &mut indexes,
        request(tensor_case),
        definitions[tensor_position].index_id,
        definitions[tensor_position].version,
        before_tensor_delete.generation,
        tensor_case.documents.len() as u64 + 3,
        &expected,
        &expected_versions,
        tensor_case.expects_scores,
        endpoints.len(),
    )
    .await?;
    require_checkpoint_advance(&before_tensor_delete, require_freshness(&responses[0])?, 1)?;
    mutation_generations[tensor_position] = responses[0].clone();
    final_hit_versions[tensor_position] = 0;

    for position in 0..cases.len() {
        let (definition, response) = qualify_explicit_rebuild(
            &mut indexes,
            &cases[position],
            &definitions[position],
            &mutation_generations[position],
            endpoints.len(),
        )
        .await?;
        definitions[position] = definition;
        mutation_generations[position] = response;
    }

    let state_output = env::var_os("ANVIL_INDEX_QUALIFICATION_STATE_OUTPUT");
    if state_output.is_some()
        || env::var("ANVIL_INDEX_QUALIFICATION_REQUIRE_QUIESCENCE").is_ok_and(|value| value == "1")
    {
        require_generation_quiescence(&mut indexes[0], &cases).await?;
    }
    if let Some(path) = state_output {
        let final_responses = collect_final_responses(
            &mut indexes,
            &cases,
            &definitions,
            &final_hit_versions,
            tensor_position,
            endpoints.len(),
        )
        .await?;
        write_verification_state(
            Path::new(&path),
            &tenant,
            endpoints.len(),
            &cases,
            &definitions,
            &final_responses,
        )?;
    }

    definition_lifecycle::qualify(&mut administrators, &mut indexes, &cases).await?;

    println!(
        "index qualification passed on {} node(s): {} engines, {} public mutations, {} definition update/delete lifecycles, {} compaction waves",
        endpoints.len(),
        cases.len(),
        write_number,
        cases.len(),
        COMPACTION_WAVES,
    );
    Ok(())
}

async fn qualify_explicit_rebuild(
    clients: &mut [IndexClient],
    case: &EngineCase,
    definition: &IndexDefinition,
    before: &QueryIndexResponse,
    expected_sources: usize,
) -> TestResult<(IndexDefinition, QueryIndexResponse)> {
    let rebuilt = clients[0]
        .rebuild_index(RebuildIndexRequest {
            bucket: case.bucket.into(),
            name: case.name.into(),
            expected_version: definition.version,
            command_id: format!("qualification-rebuild-{}", case.name),
        })
        .await?
        .into_inner();
    if rebuilt.index_id != definition.index_id
        || rebuilt.bucket != definition.bucket
        || rebuilt.name != definition.name
        || rebuilt.path_prefix != definition.path_prefix
        || rebuilt.content_type != definition.content_type
        || rebuilt.kind != definition.kind
        || rebuilt.specification != definition.specification
        || rebuilt.version <= definition.version
    {
        return Err(invalid(format!(
            "{} rebuild changed its immutable definition or did not advance its version",
            case.name
        )));
    }

    let repeated = clients[0]
        .rebuild_index(RebuildIndexRequest {
            bucket: case.bucket.into(),
            name: case.name.into(),
            expected_version: rebuilt.version,
            command_id: format!("qualification-rebuild-rate-limit-{}", case.name),
        })
        .await;
    let repeated_error = match repeated {
        Ok(_) => {
            return Err(invalid(format!(
                "{} accepted a second explicit rebuild inside one hour",
                case.name
            )));
        }
        Err(status) => status,
    };
    if repeated_error.code() != tonic::Code::ResourceExhausted
        || !repeated_error
            .message()
            .contains("index rebuild is rate limited")
    {
        return Err(invalid(format!(
            "{} second explicit rebuild returned {repeated_error}",
            case.name
        )));
    }

    let expected = before
        .hits
        .iter()
        .filter_map(hit_path)
        .collect::<BTreeSet<_>>();
    let expected_versions = before
        .hits
        .iter()
        .filter_map(|hit| hit_path(hit).map(|path| (path, hit.object_version)))
        .collect::<BTreeMap<_, _>>();
    let responses = wait_for_queries(
        clients,
        request(case),
        rebuilt.index_id,
        rebuilt.version,
        require_freshness(before)?.generation,
        expected.len() as u64,
        &expected,
        &expected_versions,
        case.expects_scores,
        expected_sources,
    )
    .await?;
    require_physical_order(case, &responses, true)?;
    println!(
        "{} authorized explicit rebuild published version {} and its immediate retry was rate limited",
        case.name, rebuilt.version
    );
    Ok((rebuilt, responses[0].clone()))
}

async fn require_generation_quiescence(
    client: &mut IndexClient,
    cases: &[EngineCase],
) -> TestResult<()> {
    let deadline = Instant::now() + GENERATION_QUIET_LIMIT;
    let mut observed_generations = None;
    let mut stable_since = Instant::now();
    let mut advances = 0_u64;

    loop {
        let mut generations = Vec::with_capacity(cases.len());
        for case in cases {
            let response = client.query_index(request(case)).await?.into_inner();
            let generation = require_freshness(&response)?.generation;
            if generation == 0 {
                return Err(invalid(format!(
                    "{} generation disappeared while checking quiescence",
                    case.name
                )));
            }
            generations.push(generation);
        }
        match observed_generations.as_ref() {
            Some(previous) if previous == &generations => {}
            Some(_) => {
                observed_generations = Some(generations.clone());
                stable_since = Instant::now();
                advances = advances.saturating_add(1);
            }
            None => {
                observed_generations = Some(generations.clone());
                stable_since = Instant::now();
            }
        }

        if stable_since.elapsed() >= GENERATION_QUIET_WINDOW {
            println!(
                "all {} index generations remained stable for {} seconds",
                generations.len(),
                GENERATION_QUIET_WINDOW.as_secs()
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "index generations did not quiesce without source mutations: \
                 observed {advances} vector advances in {} seconds (latest {generations:?})",
                GENERATION_QUIET_LIMIT.as_secs()
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn collect_final_responses(
    indexes: &mut [IndexClient],
    cases: &[EngineCase],
    definitions: &[anvil_storage::v1::IndexDefinition],
    replacement_hit_versions: &[u64],
    tensor_position: usize,
    source_count: usize,
) -> TestResult<Vec<QueryIndexResponse>> {
    let mut final_responses = Vec::with_capacity(cases.len());
    for (position, ((case, definition), replacement_hit_version)) in cases
        .iter()
        .zip(definitions)
        .zip(replacement_hit_versions)
        .enumerate()
    {
        let expected = if position == tensor_position {
            BTreeSet::new()
        } else {
            expected_after_primary_mutations(case)
        };
        let expected_versions = if *replacement_hit_version == 0 {
            BTreeMap::new()
        } else {
            BTreeMap::from([(case.replacement_hit_path, *replacement_hit_version)])
        };
        let responses = wait_for_queries(
            indexes,
            request(case),
            definition.index_id,
            definition.version,
            0,
            u64::MAX,
            &expected,
            &expected_versions,
            case.expects_scores,
            source_count,
        )
        .await?;
        require_physical_order(case, &responses, true)?;
        final_responses.push(responses[0].clone());
    }
    Ok(final_responses)
}

fn expected_after_primary_mutations<'a>(case: &'a EngineCase) -> BTreeSet<&'a str> {
    case.expected_paths
        .iter()
        .copied()
        .filter(|path| *path != case.delete_hit_path)
        .collect()
}

fn expected_physical_order(case: &EngineCase, after_mutations: bool) -> Option<Vec<&str>> {
    let Some(SpecificationValue::TypedJson(specification)) =
        case.specification.specification.as_ref()
    else {
        return None;
    };
    (!specification.physical_order.is_empty()).then(|| {
        case.expected_paths
            .iter()
            .copied()
            .filter(|path| !after_mutations || *path != case.delete_hit_path)
            .collect()
    })
}

fn physical_order_matches(case: &EngineCase, actual: &[String], after_mutations: bool) -> bool {
    expected_physical_order(case, after_mutations)
        .is_none_or(|expected| actual.iter().map(String::as_str).eq(expected))
}

fn require_physical_order(
    case: &EngineCase,
    responses: &[QueryIndexResponse],
    after_mutations: bool,
) -> TestResult<()> {
    for response in responses {
        let actual = response
            .hits
            .iter()
            .filter_map(hit_path)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if actual.len() != response.hits.len()
            || !physical_order_matches(case, &actual, after_mutations)
        {
            return Err(invalid(format!(
                "{} did not return its declared physical order: {actual:?}",
                case.name
            )));
        }
    }
    Ok(())
}

fn write_verification_state(
    path: &Path,
    tenant: &str,
    source_count: usize,
    cases: &[EngineCase],
    definitions: &[anvil_storage::v1::IndexDefinition],
    responses: &[QueryIndexResponse],
) -> TestResult<()> {
    if cases.len() != definitions.len() || cases.len() != responses.len() {
        return Err(invalid(
            "index verification state inputs have different lengths",
        ));
    }
    let indexes = cases
        .iter()
        .zip(definitions)
        .zip(responses)
        .map(|((case, definition), response)| {
            let freshness = require_freshness(response)?;
            let mut sources = freshness
                .sources
                .iter()
                .map(|source| VerificationSource {
                    node_id: source.node_id,
                    source_epoch: source.source_epoch.clone(),
                    indexed_next_offset: source.indexed_next_offset,
                })
                .collect::<Vec<_>>();
            sources.sort_by_key(|source| source.node_id);
            let hits = verification_hits(response)?;
            Ok(VerificationIndex {
                bucket: case.bucket.into(),
                name: case.name.into(),
                index_id: definition.index_id,
                definition_version: definition.version,
                generation: freshness.generation,
                placement_term: freshness.placement_term,
                placement_index: freshness.placement_index,
                sources,
                hits,
            })
        })
        .collect::<TestResult<Vec<_>>>()?;
    let encoded = serde_json::to_vec_pretty(&VerificationState {
        schema: VERIFICATION_STATE_SCHEMA.into(),
        tenant: tenant.into(),
        source_count,
        indexes,
    })?;
    std::fs::write(path, encoded)?;
    Ok(())
}

async fn verify_existing_state(
    path: &Path,
    tenant: &str,
    cases: &[EngineCase],
    indexes: &mut [IndexClient],
) -> TestResult<()> {
    let state: VerificationState = serde_json::from_slice(&std::fs::read(path)?)?;
    if state.schema != VERIFICATION_STATE_SCHEMA
        || state.tenant != tenant
        || state.source_count != indexes.len()
        || state.indexes.len() != cases.len()
    {
        return Err(invalid(
            "index verification state does not match this cluster",
        ));
    }
    for (case, expected) in cases.iter().zip(&state.indexes) {
        if expected.bucket != case.bucket || expected.name != case.name {
            return Err(invalid(
                "index verification state order or identity changed",
            ));
        }
        let expected_paths = expected
            .hits
            .iter()
            .map(|hit| hit.path.as_str())
            .collect::<BTreeSet<_>>();
        let expected_versions = expected
            .hits
            .iter()
            .map(|hit| (hit.path.as_str(), hit.object_version))
            .collect::<BTreeMap<_, _>>();
        let responses = wait_for_queries(
            indexes,
            request(case),
            expected.index_id,
            expected.definition_version,
            expected.generation.saturating_sub(1),
            u64::MAX,
            &expected_paths,
            &expected_versions,
            case.expects_scores,
            state.source_count,
        )
        .await?;
        require_physical_order(case, &responses, true)?;
        for response in &responses {
            verify_response_state(response, expected)?;
        }
    }
    Ok(())
}

fn verify_response_state(
    response: &QueryIndexResponse,
    expected: &VerificationIndex,
) -> TestResult<()> {
    let freshness = require_freshness(response)?;
    let mut sources = freshness
        .sources
        .iter()
        .map(|source| VerificationSource {
            node_id: source.node_id,
            source_epoch: source.source_epoch.clone(),
            indexed_next_offset: source.indexed_next_offset,
        })
        .collect::<Vec<_>>();
    sources.sort_by_key(|source| source.node_id);
    if freshness.generation != expected.generation
        || freshness.index_id != expected.index_id
        || freshness.definition_version != expected.definition_version
        || freshness.placement_term != expected.placement_term
        || freshness.placement_index != expected.placement_index
        || !freshness.initial_build_complete
        || freshness.rebuilding
        || sources != expected.sources
        || verification_hits(response)? != expected.hits
    {
        return Err(invalid(format!(
            "{}:{} did not preserve its final complete generation {}",
            expected.bucket, expected.name, expected.generation
        )));
    }
    Ok(())
}

fn verification_hits(response: &QueryIndexResponse) -> TestResult<Vec<VerificationHit>> {
    let mut hits = response
        .hits
        .iter()
        .map(|hit| {
            Ok(VerificationHit {
                path: hit
                    .address
                    .as_ref()
                    .ok_or_else(|| invalid("index verification hit omitted its address"))?
                    .path
                    .clone(),
                object_version: hit.object_version,
                score_bits: hit.score.map(f32::to_bits),
            })
        })
        .collect::<TestResult<Vec<_>>>()?;
    hits.sort();
    Ok(hits)
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

async fn put_index_document(
    client: &mut RawClient,
    tenant: &str,
    case: &EngineCase,
    bucket: &str,
    path: &str,
    bytes: &[u8],
    command_id: &str,
    durability: Durability,
) -> TestResult<u64> {
    let mut manifest = serde_json::from_slice::<serde_json::Value>(bytes)?;
    let referenced = match case.specification.specification.as_ref() {
        Some(SpecificationValue::GitSource(_)) => Some(("pack_path", "pack_version")),
        Some(SpecificationValue::Tensor(_)) => Some(("source_path", "source_version")),
        _ => None,
    };
    let hit_version = if let Some((path_field, version_field)) = referenced {
        let referenced_path = manifest
            .get(path_field)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                invalid(format!(
                    "{case_name} omitted {path_field}",
                    case_name = case.name
                ))
            })?
            .to_owned();
        let payload = format!("qualification payload for {referenced_path}\n");
        let version = put_bytes(
            client,
            tenant,
            bucket,
            &referenced_path,
            payload.as_bytes(),
            "application/octet-stream",
            &format!("{command_id}-payload"),
            durability,
        )
        .await?;
        manifest[version_field] = serde_json::Value::from(version);
        version
    } else {
        0
    };
    let encoded = serde_json::to_vec(&manifest)?;
    let manifest_version = put_bytes(
        client,
        tenant,
        bucket,
        path,
        &encoded,
        CONTENT_TYPE,
        command_id,
        durability,
    )
    .await?;
    Ok(if hit_version == 0 {
        manifest_version
    } else {
        hit_version
    })
}

async fn put_bytes(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    bytes: &[u8],
    content_type: &str,
    command_id: &str,
    durability: Durability,
) -> TestResult<u64> {
    let receipt = put_chunks(
        client,
        PutHeader {
            address: Some(ObjectAddress {
                tenant: tenant.into(),
                bucket: bucket.into(),
                path: path.into(),
            }),
            content_type: content_type.into(),
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
    Ok(receipt.version)
}

async fn delete_object(
    client: &mut RawClient,
    request: DeleteRequest,
    context: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        match client.delete(request.clone()).await {
            Ok(response) => {
                let receipt = response.into_inner();
                if !receipt.deleted || receipt.version == 0 {
                    return Err(invalid(format!("{context} returned an invalid receipt")));
                }
                return Ok(());
            }
            Err(status) if retryable_transport(&status) && Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(status) => return Err(invalid(format!("{context} failed: {status}"))),
        }
    }
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
    expected_versions: &BTreeMap<&str, u64>,
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
                    expected_versions,
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
            && pair[0].facet_results == pair[1].facet_results
            && pair[0].aggregate_results == pair[1].aggregate_results
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
    expected_versions: &BTreeMap<&str, u64>,
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
        && expected_versions.iter().all(|(path, version)| {
            response
                .hits
                .iter()
                .any(|hit| hit_path(hit) == Some(*path) && hit.object_version == *version)
        })
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
) -> TestResult<Vec<String>> {
    let mut request = request(case);
    request.limit = 1;
    let deadline = Instant::now() + WAIT_LIMIT;
    let mut paths = Vec::new();
    let mut unique = BTreeSet::new();
    let mut previous_token = Vec::new();
    loop {
        request.page_token = previous_token.clone();
        let response = query_index_page(client, request.clone(), case.name, &deadline).await?;
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
            if !unique.insert(path.to_owned()) {
                return Err(invalid(format!(
                    "{} pagination returned duplicate path {path}",
                    case.name
                )));
            }
            paths.push(path.to_owned());
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

async fn query_index_page(
    client: &mut IndexClient,
    request: QueryIndexRequest,
    index_name: &str,
    deadline: &Instant,
) -> TestResult<QueryIndexResponse> {
    loop {
        match client.query_index(request.clone()).await {
            Ok(response) => return Ok(response.into_inner()),
            Err(status) if retryable_transport(&status) && Instant::now() < *deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(status) => {
                return Err(invalid(format!(
                    "{index_name} pagination query failed: {status}"
                )));
            }
        }
    }
}

fn retryable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::NotFound | tonic::Code::FailedPrecondition
    ) || retryable_transport(status)
}

fn retryable_transport(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
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
            replacement_hit_path: "docs/a.json",
            delete_path: "docs/b.json",
            delete_hit_path: "docs/b.json",
            expects_scores: false,
        },
        EngineCase {
            bucket: "index-typed-json",
            name: "active-documents",
            specification: specification(SpecificationValue::TypedJson(TypedJsonIndexSpec {
                fields: vec![
                    keyword_field(
                        "status",
                        "/status",
                        &[
                            IndexFieldCapability::Exact,
                            IndexFieldCapability::Prefix,
                            IndexFieldCapability::Range,
                            IndexFieldCapability::Facet,
                        ],
                    ),
                    signed_integer_field(
                        "modified_at",
                        "/modified_at",
                        &[
                            IndexFieldCapability::Exact,
                            IndexFieldCapability::Range,
                            IndexFieldCapability::Order,
                            IndexFieldCapability::Facet,
                            IndexFieldCapability::Aggregate,
                        ],
                    ),
                    keyword_field(
                        "source_record_id",
                        "/source_record_id",
                        &[
                            IndexFieldCapability::Exact,
                            IndexFieldCapability::Prefix,
                            IndexFieldCapability::Range,
                            IndexFieldCapability::Order,
                            IndexFieldCapability::Facet,
                        ],
                    ),
                    unsigned_integer_field(
                        "sequence",
                        "/sequence",
                        &[
                            IndexFieldCapability::Exact,
                            IndexFieldCapability::Range,
                            IndexFieldCapability::Facet,
                            IndexFieldCapability::Aggregate,
                        ],
                    ),
                    float_field(
                        "score",
                        "/score",
                        &[
                            IndexFieldCapability::Exact,
                            IndexFieldCapability::Range,
                            IndexFieldCapability::Order,
                            IndexFieldCapability::Facet,
                            IndexFieldCapability::Aggregate,
                        ],
                    ),
                    boolean_field(
                        "enabled",
                        "/enabled",
                        &[IndexFieldCapability::Exact, IndexFieldCapability::Facet],
                    ),
                    keyword_multi_field(
                        "labels",
                        "/labels",
                        &[IndexFieldCapability::Exact, IndexFieldCapability::Facet],
                    ),
                    signed_integer_multi_field(
                        "measurements",
                        "/measurements",
                        &[
                            IndexFieldCapability::Facet,
                            IndexFieldCapability::Aggregate,
                        ],
                    ),
                    text_field("summary", "/summary"),
                ],
                physical_order: typed_json_order(),
            })),
            query: query(QueryValue::TypedJson(TypedJsonIndexQuery {
                predicates: vec![IndexPredicate {
                    field: "status".into(),
                    operator: IndexPredicateOperator::Equal as i32,
                    values_json: vec![br#""active""#.to_vec()],
                }],
                order: typed_json_order(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
            documents: vec![
                ("docs/active-a.json", br#"{"status":"active","modified_at":100,"source_record_id":"b","sequence":1,"score":1.5,"enabled":true,"labels":["stable","stable","alpha"],"measurements":[1,1],"summary":"durable journal alpha"}"#),
                ("docs/inactive.json", br#"{"status":"inactive","modified_at":400,"source_record_id":"x","sequence":4,"score":9.0,"enabled":false,"labels":["archived"],"measurements":[9],"summary":"unrelated material"}"#),
                ("docs/active-b.json", br#"{"status":"active","modified_at":200,"source_record_id":"z","sequence":2,"score":2.5,"enabled":true,"labels":["stable","beta"],"measurements":[2,3],"summary":"durable journal beta"}"#),
                ("docs/active-c.json", br#"{"status":"active","modified_at":200,"source_record_id":"a","sequence":3,"score":3.5,"enabled":true,"labels":["stable","gamma"],"measurements":[4],"summary":"durable journal gamma"}"#),
            ],
            expected_paths: vec![
                "docs/active-c.json",
                "docs/active-b.json",
                "docs/active-a.json",
            ],
            replacement: (
                "docs/active-a.json",
                br#"{"status":"active","modified_at":100,"source_record_id":"b","sequence":1,"score":1.5,"enabled":true,"labels":["stable","stable","alpha"],"measurements":[1,1],"summary":"durable journal alpha","revision":2}"#,
            ),
            replacement_hit_path: "docs/active-a.json",
            delete_path: "docs/active-b.json",
            delete_hit_path: "docs/active-b.json",
            expects_scores: false,
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
            replacement_hit_path: "docs/keep-a.json",
            delete_path: "docs/keep-b.json",
            delete_hit_path: "docs/keep-b.json",
            expects_scores: false,
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
            replacement_hit_path: "docs/journal-a.json",
            delete_path: "docs/journal-b.json",
            delete_hit_path: "docs/journal-b.json",
            expects_scores: true,
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
            replacement_hit_path: "docs/rust.json",
            delete_path: "docs/storage.json",
            delete_hit_path: "docs/storage.json",
            expects_scores: true,
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
            replacement_hit_path: "docs/rust.json",
            delete_path: "docs/storage.json",
            delete_hit_path: "docs/storage.json",
            expects_scores: true,
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
                    br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"src/lib.rs","object_id":"1111111111111111111111111111111111111111","pack_path":"packs/git-lib.pack","pack_version":0,"offset":0,"length":128}"#,
                ),
                (
                    "docs/git-main.json",
                    br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"src/main.rs","object_id":"2222222222222222222222222222222222222222","pack_path":"packs/git-main.pack","pack_version":0,"offset":128,"length":256}"#,
                ),
                (
                    "docs/git-readme.json",
                    br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"README.md","object_id":"4444444444444444444444444444444444444444","pack_path":"packs/git-readme.pack","pack_version":0,"offset":384,"length":64}"#,
                ),
            ],
            expected_paths: vec!["packs/git-lib.pack", "packs/git-main.pack"],
            replacement: (
                "docs/git-lib.json",
                br#"{"repository_id":"qualification-repository","commit_id":"qualification-commit","tree_path":"src/lib.rs","object_id":"3333333333333333333333333333333333333333","pack_path":"packs/git-lib.pack","pack_version":0,"offset":256,"length":192}"#,
            ),
            replacement_hit_path: "packs/git-lib.pack",
            delete_path: "docs/git-main.json",
            delete_hit_path: "packs/git-main.pack",
            expects_scores: false,
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
                    br#"{"model_id":"qualification-model","tensor_name":"encoder.shadow","source_path":"tensors/encoder.bin","source_version":0,"offset":0,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
                (
                    "docs/tensor-decoder.json",
                    br#"{"model_id":"qualification-model","tensor_name":"decoder.bias","source_path":"tensors/decoder.bin","source_version":0,"offset":128,"length":32,"dtype":"f32","shape":[8]}"#,
                ),
                (
                    "docs/tensor-encoder-copy.json",
                    br#"{"model_id":"qualification-model","tensor_name":"encoder.weight","source_path":"tensors/encoder-copy.bin","source_version":0,"offset":160,"length":128,"dtype":"f32","shape":[8,4]}"#,
                ),
            ],
            expected_paths: vec!["tensors/encoder-copy.bin"],
            replacement: (
                "docs/tensor-encoder-copy.json",
                br#"{"model_id":"qualification-model","tensor_name":"encoder.weight","source_path":"tensors/encoder-copy.bin","source_version":0,"offset":288,"length":128,"dtype":"f32","shape":[8,4]}"#,
            ),
            replacement_hit_path: "tensors/encoder-copy.bin",
            delete_path: "docs/tensor-decoder.json",
            delete_hit_path: "tensors/decoder.bin",
            expects_scores: false,
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
#[path = "cluster_index_qualification/tests.rs"]
mod tests;
