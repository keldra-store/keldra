//! Public-API fixtures for online index reassignment and journal backpressure.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use anvil_storage::v1::bulk_operation::Operation as BulkOperationValue;
use anvil_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use anvil_storage::v1::index_field::FieldType as IndexFieldType;
use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_service_client::IndexServiceClient;
use anvil_storage::v1::index_specification::Specification as SpecificationValue;
use anvil_storage::v1::object_head::State as ObjectHeadState;
use anvil_storage::v1::put_header::Operation as PutOperationValue;
use anvil_storage::v1::{
    BulkOperation, BulkPutRequest, BulkWriteRequest, CreateBucketRequest, CreateIndexRequest,
    DeleteRequest, Durability, FullTextField, FullTextIndexQuery, FullTextIndexSpec,
    GitSourceIndexQuery, GitSourceIndexSpec, HeadObjectRequest, HybridIndexQuery, HybridIndexSpec,
    IndexField, IndexFieldCapability, IndexFieldCardinality, IndexPredicate,
    IndexPredicateOperator, IndexQuery, IndexSpecification, KeywordIndexField,
    MetadataFilterIndexQuery, MetadataFilterIndexSpec, MutationFailureCode, MutationReceipt,
    ObjectAddress, ObjectVersioning, PathIndexQuery, PathIndexSpec, PutHeader, PutOperation,
    QueryIndexRequest, TensorIndexQuery, TensorIndexSpec, TypedJsonIndexQuery, TypedJsonIndexSpec,
    VectorIndexQuery, VectorIndexSpec, VectorMetric,
};
use anvil_storage::{
    BearerToken, RawClient, administration_client, connect_channel, exchange_client_credentials,
    object_client, put_chunks,
};
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, sleep};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const MEMBERSHIP_FIXTURES: usize = 16;
const PRESSURE_PATH_SELECTION_ATTEMPTS: usize = 32;
const PRESSURE_PATH_SELECTION_TIMEOUT: Duration = Duration::from_secs(2);
const INDEX_NAME: &str = "paths";
const STATE_SCHEMA: &str = "anvil.index-recovery-qualification.v3";

struct RecoveryDocument {
    source_path: &'static str,
    result_path: &'static str,
    bytes: &'static [u8],
}

struct RecoveryCase {
    kind: &'static str,
    specification: IndexSpecification,
    query: IndexQuery,
    documents: [RecoveryDocument; 3],
}

#[derive(Debug, Deserialize, Serialize)]
struct FixtureState {
    bucket: String,
    index_id: u64,
    generation: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct KindFixtureState {
    bucket: String,
    engine_id: u64,
    generation: u64,
    placement_term: u64,
    placement_index: u64,
    kind: String,
}

#[derive(Clone, Copy)]
struct RecoveryEvidence {
    generation: u64,
    placement_term: u64,
    placement_index: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct MembershipState {
    schema: String,
    active_nodes_verified: usize,
    fixtures: Vec<FixtureState>,
    kind_fixtures: Vec<KindFixtureState>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PressureState {
    schema: String,
    fixture: FixtureState,
    next_candidate: usize,
    pressure_path: Option<String>,
    skipped_paths: Vec<String>,
    successful_mutations: u64,
    last_version: u64,
    pending_command_id: Option<String>,
}

struct Context {
    tenant: String,
    token: String,
    channels: Vec<Channel>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let mode = required("ANVIL_INDEX_RECOVERY_QUALIFICATION_MODE")?;
    let context = Context::connect().await?;
    let state_path = PathBuf::from(required("ANVIL_INDEX_RECOVERY_QUALIFICATION_STATE")?);
    match mode.as_str() {
        "membership-seed" => membership_seed(&context, state_path).await?,
        "membership-verify-two" => membership_verify(&context, state_path, 2).await?,
        "membership-verify-three" => membership_verify(&context, state_path, 3).await?,
        "pressure-seed" => pressure_seed(&context, state_path).await?,
        "pressure-write" => pressure_write(&context, state_path).await?,
        "pressure-assert-blocked" => pressure_assert_blocked(&context, state_path).await?,
        "pressure-verify" => pressure_verify(&context, state_path).await?,
        _ => return Err(invalid("unknown index recovery qualification mode")),
    }
    Ok(())
}

impl Context {
    async fn connect() -> TestResult<Self> {
        let endpoints = required("ANVIL_INDEX_RECOVERY_QUALIFICATION_ENDPOINTS")?
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Err(invalid("index recovery qualification requires an endpoint"));
        }
        let mut channels = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            channels.push(connect_channel(&endpoint).await?);
        }
        let tenant = required("ANVIL_INDEX_RECOVERY_QUALIFICATION_TENANT")?;
        let token = exchange_client_credentials(
            channels[0].clone(),
            required("ANVIL_INDEX_RECOVERY_QUALIFICATION_CLIENT_ID")?,
            required("ANVIL_INDEX_RECOVERY_QUALIFICATION_CLIENT_SECRET")?,
        )
        .await?
        .access_token;
        Ok(Self {
            tenant,
            token,
            channels,
        })
    }

    fn index_clients(&self) -> TestResult<Vec<IndexClient>> {
        self.channels
            .iter()
            .cloned()
            .map(|channel| {
                Ok(IndexServiceClient::with_interceptor(
                    channel,
                    BearerToken::new(&self.token)?,
                ))
            })
            .collect()
    }

    fn object_clients(&self) -> TestResult<Vec<RawClient>> {
        self.channels
            .iter()
            .cloned()
            .map(|channel| object_client(channel, &self.token).map_err(Into::into))
            .collect()
    }
}

async fn membership_seed(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 1 {
        return Err(invalid("membership seed requires the one-node topology"));
    }
    let mut administrator = administration_client(context.channels[0].clone(), &context.token)?;
    let mut indexes = context.index_clients()?;
    let mut objects = context.object_clients()?;
    let cases = recovery_cases();
    let path_case = recovery_case(&cases, "path")?;
    let mut fixtures = Vec::with_capacity(MEMBERSHIP_FIXTURES);
    for position in 0..MEMBERSHIP_FIXTURES {
        let bucket = format!("index-membership-{position:02}");
        create_bucket(&mut administrator, &bucket).await?;
        let definition =
            create_recovery_index(&mut indexes[0], &bucket, position, path_case).await?;
        let baseline = wait_for_recovery_case(
            &mut indexes,
            &bucket,
            path_case,
            definition.index_id,
            &BTreeSet::new(),
            0,
            1,
        )
        .await?
        .generation;
        let document = &path_case.documents[0];
        put_recovery_document(
            &mut objects[0],
            &context.tenant,
            &bucket,
            path_case,
            document,
            &format!("index-membership-before-{position}"),
        )
        .await?;
        let generation = wait_for_recovery_case(
            &mut indexes,
            &bucket,
            path_case,
            definition.index_id,
            &paths([document.result_path]),
            baseline,
            1,
        )
        .await?
        .generation;
        fixtures.push(FixtureState {
            bucket,
            index_id: definition.index_id,
            generation,
        });
    }
    // Keep the original 16 Path identities because the outer harness uses
    // them to prove an HRW reassignment published on each joining node. One
    // additional pre-growth fixture per remaining engine proves that the same
    // recovery/catch-up path is kind-independent without weakening that test.
    let mut kind_fixtures = Vec::with_capacity(cases.len() - 1);
    for (position, case) in cases.iter().filter(|case| case.kind != "path").enumerate() {
        let bucket = format!("index-membership-kind-{}", case.kind);
        create_bucket(&mut administrator, &bucket).await?;
        let definition =
            create_recovery_index(&mut indexes[0], &bucket, 100 + position, case).await?;
        let baseline = wait_for_recovery_case(
            &mut indexes,
            &bucket,
            case,
            definition.index_id,
            &BTreeSet::new(),
            0,
            1,
        )
        .await?
        .generation;
        let document = &case.documents[0];
        put_recovery_document(
            &mut objects[0],
            &context.tenant,
            &bucket,
            case,
            document,
            &format!("index-membership-kind-{}-before", case.kind),
        )
        .await?;
        let evidence = wait_for_recovery_case(
            &mut indexes,
            &bucket,
            case,
            definition.index_id,
            &paths([document.result_path]),
            baseline,
            1,
        )
        .await?;
        kind_fixtures.push(KindFixtureState {
            bucket,
            engine_id: definition.index_id,
            generation: evidence.generation,
            placement_term: evidence.placement_term,
            placement_index: evidence.placement_index,
            kind: case.kind.into(),
        });
    }
    write_state(
        state_path,
        &MembershipState {
            schema: STATE_SCHEMA.into(),
            active_nodes_verified: 1,
            fixtures,
            kind_fixtures,
        },
    )?;
    println!(
        "seeded {MEMBERSHIP_FIXTURES} HRW fixtures and one recovery fixture for every index kind"
    );
    Ok(())
}

async fn membership_verify(
    context: &Context,
    state_path: PathBuf,
    active_nodes: usize,
) -> TestResult<()> {
    if context.channels.len() != active_nodes || !matches!(active_nodes, 2 | 3) {
        return Err(invalid(format!(
            "membership verification requires exactly {active_nodes} endpoints"
        )));
    }
    let mut state: MembershipState = read_state(&state_path)?;
    require_schema(&state.schema)?;
    let cases = recovery_cases();
    require_membership_matrix(&state, &cases)?;
    if state.active_nodes_verified + 1 != active_nodes {
        return Err(invalid(format!(
            "membership state last verified {} ACTIVE node(s), cannot verify {active_nodes}",
            state.active_nodes_verified
        )));
    }
    let command_suffix = match active_nodes {
        2 => "after-two",
        3 => "after-three",
        _ => unreachable!("validated above"),
    };
    let path_case = recovery_case(&cases, "path")?;
    let mut indexes = context.index_clients()?;
    let mut objects = context.object_clients()?;
    for (position, fixture) in state.fixtures.iter().enumerate() {
        let before_paths = path_case.documents[..active_nodes - 1]
            .iter()
            .map(|document| document.result_path.to_owned())
            .collect();
        wait_for_recovery_case(
            &mut indexes,
            &fixture.bucket,
            path_case,
            fixture.index_id,
            &before_paths,
            0,
            active_nodes,
        )
        .await?;
        let document = &path_case.documents[active_nodes - 1];
        let ingress = position % objects.len();
        put_recovery_document(
            &mut objects[ingress],
            &context.tenant,
            &fixture.bucket,
            path_case,
            document,
            &format!("index-membership-{command_suffix}-{position}"),
        )
        .await?;
    }
    for (position, fixture) in state.kind_fixtures.iter().enumerate() {
        let case = recovery_case(&cases, &fixture.kind)?;
        let before_paths = expected_recovery_paths(case, active_nodes - 1);
        let assignment = wait_for_recovery_case(
            &mut indexes,
            &fixture.bucket,
            case,
            fixture.engine_id,
            &before_paths,
            0,
            active_nodes,
        )
        .await?;
        require_new_placement(
            fixture.placement_term,
            fixture.placement_index,
            assignment,
            case.kind,
        )?;
        let document = &case.documents[active_nodes - 1];
        let ingress = (MEMBERSHIP_FIXTURES + position) % objects.len();
        put_recovery_document(
            &mut objects[ingress],
            &context.tenant,
            &fixture.bucket,
            case,
            document,
            &format!("index-membership-kind-{}-{command_suffix}", case.kind),
        )
        .await?;
    }
    for fixture in &mut state.fixtures {
        let after_paths = expected_recovery_paths(path_case, active_nodes);
        fixture.generation = wait_for_recovery_case(
            &mut indexes,
            &fixture.bucket,
            path_case,
            fixture.index_id,
            &after_paths,
            fixture.generation,
            active_nodes,
        )
        .await?
        .generation;
    }
    for fixture in &mut state.kind_fixtures {
        let case = recovery_case(&cases, &fixture.kind)?;
        let after_paths = expected_recovery_paths(case, active_nodes);
        let evidence = wait_for_recovery_case(
            &mut indexes,
            &fixture.bucket,
            case,
            fixture.engine_id,
            &after_paths,
            fixture.generation,
            active_nodes,
        )
        .await?;
        fixture.generation = evidence.generation;
        fixture.placement_term = evidence.placement_term;
        fixture.placement_index = evidence.placement_index;
    }
    state.active_nodes_verified = active_nodes;
    write_state(&state_path, &state)?;
    println!(
        "verified {} HRW fixtures and all eight index kinds after online {active_nodes}-node assignment",
        state.fixtures.len(),
    );
    Ok(())
}

async fn pressure_seed(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 3 {
        return Err(invalid("journal-pressure seed requires three endpoints"));
    }
    let bucket = required("ANVIL_INDEX_RECOVERY_QUALIFICATION_BUCKET")?;
    let mut administrator = administration_client(context.channels[0].clone(), &context.token)?;
    create_bucket(&mut administrator, &bucket).await?;
    let mut indexes = context.index_clients()?;
    let mut objects = context.object_clients()?;
    let definition = create_path_index(&mut indexes[0], &bucket, 0).await?;
    let baseline = wait_for_paths(&mut indexes, &bucket, &BTreeSet::new(), 0).await?;
    put_path(
        &mut objects[0],
        &context.tenant,
        &bucket,
        "pressure/seed.json",
        "index-pressure-seed",
        br#"{"qualified":true}"#,
    )
    .await?;
    let generation = wait_for_paths(
        &mut indexes,
        &bucket,
        &paths(["pressure/seed.json"]),
        baseline,
    )
    .await?;
    write_state(
        state_path,
        &PressureState {
            schema: STATE_SCHEMA.into(),
            fixture: FixtureState {
                bucket,
                index_id: definition.index_id,
                generation,
            },
            next_candidate: 0,
            pressure_path: None,
            skipped_paths: Vec::new(),
            successful_mutations: 0,
            last_version: 0,
            pending_command_id: None,
        },
    )?;
    println!("seeded the journal-pressure Path index");
    Ok(())
}

async fn pressure_write(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 1 {
        return Err(invalid(
            "journal-pressure writer requires one selected ingress endpoint",
        ));
    }
    let release_path = PathBuf::from(required("ANVIL_INDEX_RECOVERY_QUALIFICATION_RELEASE")?);
    if release_path.exists() {
        return Err(invalid(
            "journal-pressure release marker already exists before the writer starts",
        ));
    }
    let mut state: PressureState = read_state(&state_path)?;
    require_schema(&state.schema)?;
    if state.pressure_path.is_some()
        || state.successful_mutations != 0
        || state.last_version != 0
        || state.pending_command_id.is_some()
    {
        return Err(invalid("journal-pressure state is not fresh"));
    }
    let mut objects = context.object_clients()?.remove(0);
    while state.next_candidate < PRESSURE_PATH_SELECTION_ATTEMPTS {
        let candidate = state.next_candidate;
        let path = format!("pressure/live-{candidate:02}.json");
        let command_id = format!("index-pressure-select-{candidate}");
        state.next_candidate += 1;
        match bulk_put_attempt(
            &mut objects,
            &context.tenant,
            &state.fixture.bucket,
            &path,
            &command_id,
            format!(r#"{{"mutation":0,"candidate":{candidate}}}"#).into_bytes(),
            Some(PRESSURE_PATH_SELECTION_TIMEOUT),
        )
        .await?
        {
            Some(receipt) => {
                state.pressure_path = Some(path);
                state.successful_mutations = 1;
                state.last_version = receipt.version;
                write_state(&state_path, &state)?;
                break;
            }
            None => {
                state.skipped_paths.push(path);
                write_state(&state_path, &state)?;
            }
        }
    }
    let pressure_path = state.pressure_path.clone().ok_or_else(|| {
        invalid(format!(
            "no live-coordinator path completed within {PRESSURE_PATH_SELECTION_ATTEMPTS} attempts"
        ))
    })?;
    loop {
        let mutation = state
            .successful_mutations
            .checked_add(1)
            .ok_or_else(|| invalid("journal-pressure mutation count is exhausted"))?;
        let command_id = format!("index-pressure-write-{mutation}");
        state.pending_command_id = Some(command_id.clone());
        write_state(&state_path, &state)?;
        let receipt = loop {
            if let Some(receipt) = bulk_put_attempt(
                &mut objects,
                &context.tenant,
                &state.fixture.bucket,
                &pressure_path,
                &command_id,
                format!(r#"{{"mutation":{mutation}}}"#).into_bytes(),
                None,
            )
            .await?
            {
                break receipt;
            }
            sleep(POLL_INTERVAL).await;
        };
        if receipt.version <= state.last_version {
            return Err(invalid(
                "journal-pressure mutation did not advance the object version",
            ));
        }
        state.successful_mutations = mutation;
        state.last_version = receipt.version;
        state.pending_command_id = None;
        write_state(&state_path, &state)?;
        if release_path.exists() {
            break;
        }
    }
    for (position, path) in state.skipped_paths.iter().enumerate() {
        bulk_delete_retry(
            &mut objects,
            &context.tenant,
            &state.fixture.bucket,
            path,
            &format!("index-pressure-cleanup-{position}"),
        )
        .await?;
    }
    println!(
        "journal-pressure writer resumed and committed version {} after {} mutations",
        state.last_version, state.successful_mutations
    );
    Ok(())
}

async fn pressure_assert_blocked(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 1 {
        return Err(invalid(
            "journal-pressure blocked assertion requires one live endpoint",
        ));
    }
    let state: PressureState = read_state(state_path)?;
    require_schema(&state.schema)?;
    let pressure_path = state
        .pressure_path
        .as_deref()
        .ok_or_else(|| invalid("journal-pressure state has no selected path"))?;
    if state.successful_mutations == 0
        || state.last_version == 0
        || state.pending_command_id.is_none()
    {
        return Err(invalid(
            "journal-pressure writer has no in-flight mutation to assert",
        ));
    }
    let mut objects = context.object_clients()?.remove(0);
    let head = objects
        .head_object(HeadObjectRequest {
            address: Some(address(
                &context.tenant,
                &state.fixture.bucket,
                pressure_path,
            )),
        })
        .await?
        .into_inner();
    match head.state {
        Some(ObjectHeadState::Present(present)) if present.version == state.last_version => {}
        Some(ObjectHeadState::Present(present)) => {
            return Err(invalid(format!(
                "pending journal-pressure mutation became visible as version {}, expected {}",
                present.version, state.last_version
            )));
        }
        _ => {
            return Err(invalid(
                "journal-pressure path was not present at its last committed version",
            ));
        }
    }
    println!(
        "proved pending mutation {} is absent while version {} remains current",
        state
            .pending_command_id
            .as_deref()
            .expect("validated above"),
        state.last_version
    );
    Ok(())
}

async fn pressure_verify(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 3 {
        return Err(invalid(
            "journal-pressure verification requires three endpoints",
        ));
    }
    let state: PressureState = read_state(state_path)?;
    require_schema(&state.schema)?;
    let pressure_path = state
        .pressure_path
        .as_deref()
        .ok_or_else(|| invalid("journal-pressure state has no selected path"))?;
    if state.successful_mutations < 2 || state.last_version == 0 {
        return Err(invalid(
            "journal-pressure writer did not commit its released mutation",
        ));
    }
    if state.pending_command_id.is_some() {
        return Err(invalid(
            "journal-pressure state still contains an in-flight mutation",
        ));
    }
    let mut indexes = context.index_clients()?;
    let expected = ["pressure/seed.json".to_owned(), pressure_path.to_owned()]
        .into_iter()
        .collect();
    wait_for_pressure_index(
        &mut indexes,
        &state.fixture.bucket,
        &expected,
        state.fixture.generation,
        pressure_path,
        state.last_version,
    )
    .await?;
    println!(
        "journal-pressure release reached an exact zero-lag generation at version {}",
        state.last_version
    );
    Ok(())
}

async fn create_bucket(
    client: &mut anvil_storage::RawAdministrationClient,
    bucket: &str,
) -> TestResult<()> {
    client
        .create_bucket(CreateBucketRequest {
            bucket: bucket.into(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await?;
    Ok(())
}

async fn create_recovery_index(
    client: &mut IndexClient,
    bucket: &str,
    command: usize,
    case: &RecoveryCase,
) -> TestResult<anvil_storage::v1::IndexDefinition> {
    let definition = client
        .create_index(CreateIndexRequest {
            bucket: bucket.into(),
            name: INDEX_NAME.into(),
            path_prefix: "docs/".into(),
            content_type: "application/json".into(),
            specification: Some(case.specification.clone()),
            command_id: format!("index-recovery-create-{}-{command}", case.kind),
        })
        .await?
        .into_inner();
    if definition.index_id == 0 || definition.version == 0 {
        return Err(invalid(format!(
            "{} recovery index has an invalid identity",
            case.kind
        )));
    }
    Ok(definition)
}

async fn create_path_index(
    client: &mut IndexClient,
    bucket: &str,
    command: usize,
) -> TestResult<anvil_storage::v1::IndexDefinition> {
    let definition = client
        .create_index(CreateIndexRequest {
            bucket: bucket.into(),
            name: INDEX_NAME.into(),
            path_prefix: String::new(),
            content_type: String::new(),
            specification: Some(IndexSpecification {
                specification: Some(SpecificationValue::Path(PathIndexSpec {})),
            }),
            command_id: format!("index-recovery-create-{command}"),
        })
        .await?
        .into_inner();
    if definition.index_id == 0 || definition.version == 0 {
        return Err(invalid("recovery index has an invalid identity"));
    }
    Ok(definition)
}

async fn put_recovery_document(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    case: &RecoveryCase,
    document: &RecoveryDocument,
    command_id: &str,
) -> TestResult<()> {
    let referenced = match case.kind {
        "git_source" => Some(("pack_path", "pack_version")),
        "tensor" => Some(("source_path", "source_version")),
        _ => None,
    };
    let bytes = if let Some((path_field, version_field)) = referenced {
        let mut manifest = serde_json::from_slice::<serde_json::Value>(document.bytes)?;
        if manifest.get(path_field).and_then(serde_json::Value::as_str)
            != Some(document.result_path)
        {
            return Err(invalid(format!(
                "{} recovery document has another projected result path",
                case.kind
            )));
        }
        let receipt = put_chunks(
            client,
            PutHeader {
                address: Some(address(tenant, bucket, document.result_path)),
                content_type: "application/octet-stream".into(),
                command_id: format!("{command_id}-payload"),
                durability: Durability::Local as i32,
                operation: Some(PutOperationValue::Put(PutOperation {})),
            },
            [format!("recovery payload for {}\n", document.result_path).into_bytes()],
        )
        .await?;
        if receipt.version == 0 || receipt.deleted {
            return Err(invalid(
                "recovery projected payload returned an invalid receipt",
            ));
        }
        manifest[version_field] = serde_json::Value::from(receipt.version);
        serde_json::to_vec(&manifest)?
    } else {
        document.bytes.to_vec()
    };
    put_path(
        client,
        tenant,
        bucket,
        document.source_path,
        command_id,
        &bytes,
    )
    .await
}

async fn put_path(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    command_id: &str,
    bytes: &[u8],
) -> TestResult<()> {
    put_chunks(
        client,
        PutHeader {
            address: Some(address(tenant, bucket, path)),
            content_type: "application/json".into(),
            command_id: command_id.into(),
            durability: Durability::Local as i32,
            operation: Some(PutOperationValue::Put(PutOperation {})),
        },
        [bytes.to_vec()],
    )
    .await?;
    Ok(())
}

async fn bulk_put_attempt(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    command_id: &str,
    bytes: Vec<u8>,
    timeout: Option<Duration>,
) -> TestResult<Option<MutationReceipt>> {
    let mut request = tonic::Request::new(BulkWriteRequest {
        operations: vec![BulkOperation {
            operation: Some(BulkOperationValue::Put(BulkPutRequest {
                address: Some(address(tenant, bucket, path)),
                bytes,
                content_type: "application/json".into(),
                command_id: command_id.into(),
                durability: Durability::Local as i32,
            })),
        }],
    });
    if let Some(timeout) = timeout {
        request.set_timeout(timeout);
    }
    let response = match client.bulk_write(request).await {
        Ok(response) => response.into_inner(),
        Err(status) if retryable(&status) => return Ok(None),
        Err(status) => return Err(status.into()),
    };
    let mut outcomes = response.outcomes.into_iter();
    let Some(outcome) = outcomes.next() else {
        return Err(invalid("journal-pressure BulkWrite omitted its outcome"));
    };
    if outcome.index != 0 || outcomes.next().is_some() {
        return Err(invalid(
            "journal-pressure BulkWrite returned invalid outcomes",
        ));
    }
    match outcome.outcome {
        Some(BulkOutcomeValue::Receipt(receipt)) => {
            if receipt.command_id != command_id || receipt.version == 0 || receipt.deleted {
                return Err(invalid(
                    "journal-pressure BulkWrite returned an invalid put receipt",
                ));
            }
            Ok(Some(receipt))
        }
        Some(BulkOutcomeValue::Failure(failure))
            if failure.code == MutationFailureCode::DurabilityUnavailable as i32 =>
        {
            Ok(None)
        }
        Some(BulkOutcomeValue::Failure(failure)) => Err(invalid(format!(
            "journal-pressure put failed non-retryably with code {}: {}",
            failure.code, failure.message
        ))),
        None => Err(invalid(
            "journal-pressure BulkWrite outcome omitted its result",
        )),
    }
}

async fn bulk_delete_retry(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    command_id: &str,
) -> TestResult<()> {
    let request = BulkWriteRequest {
        operations: vec![BulkOperation {
            operation: Some(BulkOperationValue::Delete(DeleteRequest {
                address: Some(address(tenant, bucket, path)),
                command_id: command_id.into(),
                durability: Durability::Local as i32,
            })),
        }],
    };
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        match client.bulk_write(request.clone()).await {
            Ok(response) => {
                let mut outcomes = response.into_inner().outcomes.into_iter();
                let Some(outcome) = outcomes.next() else {
                    return Err(invalid(
                        "journal-pressure cleanup BulkWrite omitted its outcome",
                    ));
                };
                if outcome.index != 0 || outcomes.next().is_some() {
                    return Err(invalid(
                        "journal-pressure cleanup BulkWrite returned invalid outcomes",
                    ));
                }
                match outcome.outcome {
                    Some(BulkOutcomeValue::Receipt(receipt))
                        if receipt.command_id == command_id
                            && receipt.version != 0
                            && receipt.deleted =>
                    {
                        return Ok(());
                    }
                    Some(BulkOutcomeValue::Failure(failure))
                        if failure.code == MutationFailureCode::DurabilityUnavailable as i32 => {}
                    Some(BulkOutcomeValue::Failure(failure)) => {
                        return Err(invalid(format!(
                            "journal-pressure cleanup failed with code {}: {}",
                            failure.code, failure.message
                        )));
                    }
                    _ => {
                        return Err(invalid(
                            "journal-pressure cleanup returned an invalid receipt",
                        ));
                    }
                }
                if Instant::now() >= deadline {
                    return Err(invalid(
                        "journal-pressure cleanup did not complete before its deadline",
                    ));
                }
                sleep(POLL_INTERVAL).await;
            }
            Err(status) if retryable(&status) && Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(status) => return Err(status.into()),
        }
    }
}

async fn wait_for_recovery_case(
    clients: &mut [IndexClient],
    bucket: &str,
    case: &RecoveryCase,
    index_id: u64,
    expected: &BTreeSet<String>,
    after_generation: u64,
    expected_sources: usize,
) -> TestResult<RecoveryEvidence> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut evidence: Option<RecoveryEvidence> = None;
        let mut ready = true;
        for client in clients.iter_mut() {
            match client.query_index(recovery_query(bucket, case)).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let freshness = response
                        .freshness
                        .ok_or_else(|| invalid("recovery index query omitted freshness"))?;
                    let paths = response
                        .hits
                        .iter()
                        .filter_map(|hit| hit.address.as_ref().map(|value| value.path.clone()))
                        .collect::<BTreeSet<_>>();
                    let source_ids = freshness
                        .sources
                        .iter()
                        .map(|source| source.node_id)
                        .collect::<BTreeSet<_>>();
                    let zero_lag = freshness.sources.len() == expected_sources
                        && source_ids.len() == expected_sources
                        && freshness.sources.iter().all(|source| {
                            source.node_id != 0
                                && source.source_epoch.len() == 32
                                && source.lag_hint == 0
                                && match source.observed_tail {
                                    Some(tail) => {
                                        tail.checked_add(1) == Some(source.indexed_next_offset)
                                    }
                                    None => source.indexed_next_offset == 0,
                                }
                        });
                    if paths != *expected
                        || response.hits.len() != expected.len()
                        || freshness.index_id != index_id
                        || freshness.definition_version == 0
                        || freshness.published_at.is_none()
                        || freshness.authorization_revision == 0
                        || freshness.placement_term == 0
                        || freshness.placement_index == 0
                        || freshness.generation <= after_generation
                        || !freshness.initial_build_complete
                        || freshness.rebuilding
                        || !zero_lag
                        || evidence.is_some_and(|value| {
                            value.generation != freshness.generation
                                || value.placement_term != freshness.placement_term
                                || value.placement_index != freshness.placement_index
                        })
                    {
                        ready = false;
                        break;
                    }
                    evidence = Some(RecoveryEvidence {
                        generation: freshness.generation,
                        placement_term: freshness.placement_term,
                        placement_index: freshness.placement_index,
                    });
                }
                Err(status) if retryable(&status) => {
                    ready = false;
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if ready && let Some(evidence) = evidence {
            return Ok(evidence);
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "{} recovery index did not converge to {} exact zero-lag results across {expected_sources} source journals",
                case.kind,
                expected.len()
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_paths(
    clients: &mut [IndexClient],
    bucket: &str,
    expected: &BTreeSet<String>,
    after_generation: u64,
) -> TestResult<u64> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut generation = u64::MAX;
        let mut ready = true;
        for client in clients.iter_mut() {
            match client.query_index(query(bucket)).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let freshness = response
                        .freshness
                        .ok_or_else(|| invalid("recovery index query omitted freshness"))?;
                    let paths = response
                        .hits
                        .iter()
                        .filter_map(|hit| hit.address.as_ref().map(|value| value.path.clone()))
                        .collect::<BTreeSet<_>>();
                    if &paths != expected
                        || response.hits.len() != expected.len()
                        || freshness.generation <= after_generation
                        || !freshness.initial_build_complete
                        || freshness.rebuilding
                    {
                        ready = false;
                        break;
                    }
                    generation = generation.min(freshness.generation);
                }
                Err(status) if retryable(&status) => {
                    ready = false;
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if ready && generation != u64::MAX {
            return Ok(generation);
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "recovery index did not converge to {} exact paths",
                expected.len()
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_pressure_index(
    clients: &mut [IndexClient],
    bucket: &str,
    expected: &BTreeSet<String>,
    after_generation: u64,
    pressure_path: &str,
    expected_version: u64,
) -> TestResult<()> {
    let expected_sources = clients.len();
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut ready = true;
        for client in clients.iter_mut() {
            match client.query_index(query(bucket)).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let freshness = response
                        .freshness
                        .ok_or_else(|| invalid("journal-pressure index omitted freshness"))?;
                    let paths = response
                        .hits
                        .iter()
                        .filter_map(|hit| hit.address.as_ref().map(|value| value.path.clone()))
                        .collect::<BTreeSet<_>>();
                    let expected_version_present = response.hits.iter().any(|hit| {
                        hit.address
                            .as_ref()
                            .is_some_and(|address| address.path == pressure_path)
                            && hit.object_version == expected_version
                    });
                    let source_ids = freshness
                        .sources
                        .iter()
                        .map(|source| source.node_id)
                        .collect::<BTreeSet<_>>();
                    let zero_lag = expected_sources != 0
                        && freshness.sources.len() == expected_sources
                        && source_ids.len() == expected_sources
                        && freshness.sources.iter().all(|source| {
                            source.node_id != 0
                                && source.source_epoch.len() == 32
                                && source.lag_hint == 0
                                && source.observed_tail.and_then(|tail| tail.checked_add(1))
                                    == Some(source.indexed_next_offset)
                        });
                    if paths != *expected
                        || response.hits.len() != expected.len()
                        || !expected_version_present
                        || freshness.generation <= after_generation
                        || !freshness.initial_build_complete
                        || freshness.rebuilding
                        || !zero_lag
                    {
                        ready = false;
                        break;
                    }
                }
                Err(status) if retryable(&status) => {
                    ready = false;
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "journal-pressure index did not converge to {} exact zero-lag paths",
                expected.len()
            )));
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn query(bucket: &str) -> QueryIndexRequest {
    QueryIndexRequest {
        bucket: bucket.into(),
        index_name: INDEX_NAME.into(),
        query: Some(IndexQuery {
            query: Some(QueryValue::Path(PathIndexQuery {
                prefix: String::new(),
                start_after: None,
            })),
        }),
        limit: 1_000,
        page_token: Vec::new(),
        tenant: String::new(),
    }
}

fn recovery_query(bucket: &str, case: &RecoveryCase) -> QueryIndexRequest {
    QueryIndexRequest {
        bucket: bucket.into(),
        index_name: INDEX_NAME.into(),
        query: Some(case.query.clone()),
        limit: 1_000,
        page_token: Vec::new(),
        tenant: String::new(),
    }
}

fn recovery_case<'a>(cases: &'a [RecoveryCase], kind: &str) -> TestResult<&'a RecoveryCase> {
    cases
        .iter()
        .find(|case| case.kind == kind)
        .ok_or_else(|| invalid(format!("unknown recovery index kind {kind}")))
}

fn require_membership_matrix(state: &MembershipState, cases: &[RecoveryCase]) -> TestResult<()> {
    let expected = cases
        .iter()
        .filter(|case| case.kind != "path")
        .map(|case| case.kind)
        .collect::<BTreeSet<_>>();
    let actual = state
        .kind_fixtures
        .iter()
        .map(|fixture| fixture.kind.as_str())
        .collect::<BTreeSet<_>>();
    if state.fixtures.len() != MEMBERSHIP_FIXTURES
        || state.kind_fixtures.len() != expected.len()
        || actual != expected
        || state.fixtures.iter().any(|fixture| fixture.index_id == 0)
        || state.kind_fixtures.iter().any(|fixture| {
            fixture.engine_id == 0 || fixture.placement_term == 0 || fixture.placement_index == 0
        })
    {
        return Err(invalid(
            "membership state does not contain the fixed HRW fixtures and all index kinds",
        ));
    }
    Ok(())
}

fn expected_recovery_paths(case: &RecoveryCase, stages: usize) -> BTreeSet<String> {
    case.documents[..stages]
        .iter()
        .map(|document| document.result_path.to_owned())
        .collect()
}

fn require_new_placement(
    previous_term: u64,
    previous_index: u64,
    current: RecoveryEvidence,
    kind: &str,
) -> TestResult<()> {
    if (current.placement_term, current.placement_index) <= (previous_term, previous_index) {
        return Err(invalid(format!(
            "{kind} recovery fixture did not adopt the new membership placement fence"
        )));
    }
    Ok(())
}

fn recovery_cases() -> Vec<RecoveryCase> {
    vec![
        RecoveryCase {
            kind: "path",
            specification: specification(SpecificationValue::Path(PathIndexSpec {})),
            query: index_query(QueryValue::Path(PathIndexQuery {
                prefix: "docs/".into(),
                start_after: None,
            })),
            documents: [
                document("docs/before.json", "docs/before.json", br#"{"stage":1}"#),
                document(
                    "docs/after-two.json",
                    "docs/after-two.json",
                    br#"{"stage":2}"#,
                ),
                document(
                    "docs/after-three.json",
                    "docs/after-three.json",
                    br#"{"stage":3}"#,
                ),
            ],
        },
        RecoveryCase {
            kind: "metadata_filter",
            specification: specification(SpecificationValue::MetadataFilter(
                MetadataFilterIndexSpec {
                    fields: vec!["path".into(), "content_type".into()],
                },
            )),
            query: index_query(QueryValue::MetadataFilter(MetadataFilterIndexQuery {
                predicates: vec![IndexPredicate {
                    field: "path".into(),
                    operator: IndexPredicateOperator::Prefix as i32,
                    values_json: vec![br#""docs/""#.to_vec()],
                }],
            })),
            documents: ordinary_documents(),
        },
        RecoveryCase {
            kind: "typed_json",
            specification: specification(SpecificationValue::TypedJson(TypedJsonIndexSpec {
                fields: vec![IndexField {
                    name: "state".into(),
                    json_pointer: "/state".into(),
                    cardinality: IndexFieldCardinality::Single as i32,
                    capabilities: vec![IndexFieldCapability::Exact as i32],
                    field_type: Some(IndexFieldType::Keyword(KeywordIndexField {})),
                }],
                physical_order: Vec::new(),
            })),
            query: index_query(QueryValue::TypedJson(TypedJsonIndexQuery {
                predicates: vec![IndexPredicate {
                    field: "state".into(),
                    operator: IndexPredicateOperator::Equal as i32,
                    values_json: vec![br#""active""#.to_vec()],
                }],
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
            documents: [
                document(
                    "docs/before.json",
                    "docs/before.json",
                    br#"{"state":"active","stage":1}"#,
                ),
                document(
                    "docs/after-two.json",
                    "docs/after-two.json",
                    br#"{"state":"active","stage":2}"#,
                ),
                document(
                    "docs/after-three.json",
                    "docs/after-three.json",
                    br#"{"state":"active","stage":3}"#,
                ),
            ],
        },
        RecoveryCase {
            kind: "full_text",
            specification: specification(SpecificationValue::FullText(FullTextIndexSpec {
                fields: vec![FullTextField {
                    name: "body".into(),
                    json_pointer: "/body".into(),
                }],
            })),
            query: index_query(QueryValue::FullText(FullTextIndexQuery {
                text: "durable journal".into(),
                phrase: true,
            })),
            documents: [
                document(
                    "docs/before.json",
                    "docs/before.json",
                    br#"{"body":"durable journal before"}"#,
                ),
                document(
                    "docs/after-two.json",
                    "docs/after-two.json",
                    br#"{"body":"durable journal after two"}"#,
                ),
                document(
                    "docs/after-three.json",
                    "docs/after-three.json",
                    br#"{"body":"durable journal after three"}"#,
                ),
            ],
        },
        RecoveryCase {
            kind: "vector",
            specification: specification(SpecificationValue::Vector(vector_spec())),
            query: index_query(QueryValue::Vector(VectorIndexQuery {
                values: vec![1.0, 0.0, 0.0],
            })),
            documents: semantic_documents(),
        },
        RecoveryCase {
            kind: "hybrid",
            specification: specification(SpecificationValue::Hybrid(HybridIndexSpec {
                full_text: Some(FullTextIndexSpec {
                    fields: vec![FullTextField {
                        name: "body".into(),
                        json_pointer: "/body".into(),
                    }],
                }),
                vector: Some(vector_spec()),
                full_text_weight: 0.0,
                vector_weight: 0.0,
            })),
            query: index_query(QueryValue::Hybrid(HybridIndexQuery {
                text: "durable journal".into(),
                vector: vec![1.0, 0.0, 0.0],
            })),
            documents: semantic_documents(),
        },
        RecoveryCase {
            kind: "git_source",
            specification: specification(SpecificationValue::GitSource(GitSourceIndexSpec {
                repository_id: "recovery-repository".into(),
            })),
            query: index_query(QueryValue::GitSource(GitSourceIndexQuery {
                commit_id: "recovery-commit".into(),
                tree_path: "src/".into(),
                prefix: true,
            })),
            documents: [
                document(
                    "docs/before.json",
                    "packs/before.pack",
                    br#"{"repository_id":"recovery-repository","commit_id":"recovery-commit","tree_path":"src/before.rs","object_id":"1111111111111111111111111111111111111111","pack_path":"packs/before.pack","pack_version":0,"offset":0,"length":64}"#,
                ),
                document(
                    "docs/after-two.json",
                    "packs/after-two.pack",
                    br#"{"repository_id":"recovery-repository","commit_id":"recovery-commit","tree_path":"src/after_two.rs","object_id":"2222222222222222222222222222222222222222","pack_path":"packs/after-two.pack","pack_version":0,"offset":64,"length":64}"#,
                ),
                document(
                    "docs/after-three.json",
                    "packs/after-three.pack",
                    br#"{"repository_id":"recovery-repository","commit_id":"recovery-commit","tree_path":"src/after_three.rs","object_id":"3333333333333333333333333333333333333333","pack_path":"packs/after-three.pack","pack_version":0,"offset":128,"length":64}"#,
                ),
            ],
        },
        RecoveryCase {
            kind: "tensor",
            specification: specification(SpecificationValue::Tensor(TensorIndexSpec {
                model_id: "recovery-model".into(),
            })),
            query: index_query(QueryValue::Tensor(TensorIndexQuery {
                tensor_name: "encoder.weight".into(),
            })),
            documents: [
                document(
                    "docs/before.json",
                    "tensors/before.bin",
                    br#"{"model_id":"recovery-model","tensor_name":"encoder.weight","source_path":"tensors/before.bin","source_version":0,"offset":0,"length":64,"dtype":"f32","shape":[4,4]}"#,
                ),
                document(
                    "docs/after-two.json",
                    "tensors/after-two.bin",
                    br#"{"model_id":"recovery-model","tensor_name":"encoder.weight","source_path":"tensors/after-two.bin","source_version":0,"offset":64,"length":64,"dtype":"f32","shape":[4,4]}"#,
                ),
                document(
                    "docs/after-three.json",
                    "tensors/after-three.bin",
                    br#"{"model_id":"recovery-model","tensor_name":"encoder.weight","source_path":"tensors/after-three.bin","source_version":0,"offset":128,"length":64,"dtype":"f32","shape":[4,4]}"#,
                ),
            ],
        },
    ]
}

const fn document(
    source_path: &'static str,
    result_path: &'static str,
    bytes: &'static [u8],
) -> RecoveryDocument {
    RecoveryDocument {
        source_path,
        result_path,
        bytes,
    }
}

fn ordinary_documents() -> [RecoveryDocument; 3] {
    [
        document("docs/before.json", "docs/before.json", br#"{"stage":1}"#),
        document(
            "docs/after-two.json",
            "docs/after-two.json",
            br#"{"stage":2}"#,
        ),
        document(
            "docs/after-three.json",
            "docs/after-three.json",
            br#"{"stage":3}"#,
        ),
    ]
}

fn semantic_documents() -> [RecoveryDocument; 3] {
    [
        document(
            "docs/before.json",
            "docs/before.json",
            br#"{"body":"durable journal before","embedding":[1.0,0.0,0.0]}"#,
        ),
        document(
            "docs/after-two.json",
            "docs/after-two.json",
            br#"{"body":"durable journal after two","embedding":[0.9,0.1,0.0]}"#,
        ),
        document(
            "docs/after-three.json",
            "docs/after-three.json",
            br#"{"body":"durable journal after three","embedding":[0.8,0.2,0.0]}"#,
        ),
    ]
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

fn index_query(value: QueryValue) -> IndexQuery {
    IndexQuery { query: Some(value) }
}

fn address(tenant: &str, bucket: &str, path: &str) -> ObjectAddress {
    ObjectAddress {
        tenant: tenant.into(),
        bucket: bucket.into(),
        path: path.into(),
    }
}

fn paths<const N: usize>(values: [&str; N]) -> BTreeSet<String> {
    values.into_iter().map(str::to_owned).collect()
}

fn retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::FailedPrecondition
    )
}

fn write_state(path: impl AsRef<std::path::Path>, state: &impl Serialize) -> TestResult<()> {
    let path = path.as_ref();
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(state)?)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

fn read_state<T: for<'de> Deserialize<'de>>(path: impl AsRef<std::path::Path>) -> TestResult<T> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn require_schema(schema: &str) -> TestResult<()> {
    if schema == STATE_SCHEMA {
        Ok(())
    } else {
        Err(invalid("index recovery state has another schema"))
    }
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
