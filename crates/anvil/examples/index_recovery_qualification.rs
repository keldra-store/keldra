//! Public-API fixtures for online index reassignment and retained-journal gaps.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use anvil_storage::v1::bulk_operation::Operation as BulkOperationValue;
use anvil_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use anvil_storage::v1::index_query::Query as QueryValue;
use anvil_storage::v1::index_service_client::IndexServiceClient;
use anvil_storage::v1::index_specification::Specification as SpecificationValue;
use anvil_storage::v1::put_header::Operation as PutOperationValue;
use anvil_storage::v1::watch_message::Message as WatchMessageValue;
use anvil_storage::v1::watch_prefix_request::Start as WatchStart;
use anvil_storage::v1::{
    BulkOperation, BulkPutRequest, BulkWriteRequest, CreateBucketRequest, CreateIndexRequest,
    Durability, IndexQuery, IndexSpecification, MutationFailureCode, ObjectAddress,
    ObjectVersioning, PathIndexQuery, PathIndexSpec, PutHeader, PutOperation, QueryIndexRequest,
    WatchNow, WatchPrefixRequest,
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
const GAP_BATCH: usize = 128;
const GAP_MAX_CANDIDATES: usize = 768;
const GAP_UNRELATED_RECORDS: usize = 4_096;
const GAP_SCOPED_HEAD_READ_LIMIT_PER_SOURCE: u64 = GAP_MAX_CANDIDATES as u64 + 8;
const INDEX_NAME: &str = "paths";
const STATE_SCHEMA: &str = "anvil.index-recovery-qualification.v1";

#[derive(Debug, Deserialize, Serialize)]
struct FixtureState {
    bucket: String,
    index_id: u64,
    generation: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct MembershipState {
    schema: String,
    active_nodes_verified: usize,
    fixtures: Vec<FixtureState>,
}

#[derive(Debug, Deserialize, Serialize)]
struct GapState {
    schema: String,
    fixture: FixtureState,
    unrelated_bucket: String,
    unrelated_objects: usize,
    max_scoped_head_reads_per_source: u64,
    expected_scoped_heads: usize,
    resume_token: Vec<u8>,
    next_candidate: usize,
    successful_paths: Vec<String>,
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
        "gap-seed" => gap_seed(&context, state_path).await?,
        "gap-write" => gap_write(&context, state_path).await?,
        "gap-verify" => gap_verify(&context, state_path).await?,
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
    let mut fixtures = Vec::with_capacity(MEMBERSHIP_FIXTURES);
    for position in 0..MEMBERSHIP_FIXTURES {
        let bucket = format!("index-membership-{position:02}");
        create_bucket(&mut administrator, &bucket).await?;
        let definition = create_path_index(&mut indexes[0], &bucket, position).await?;
        let baseline = wait_for_paths(&mut indexes, &bucket, &BTreeSet::new(), 0).await?;
        put_path(
            &mut objects[0],
            &context.tenant,
            &bucket,
            "docs/before.json",
            &format!("index-membership-before-{position}"),
        )
        .await?;
        let generation = wait_for_paths(
            &mut indexes,
            &bucket,
            &paths(["docs/before.json"]),
            baseline,
        )
        .await?;
        fixtures.push(FixtureState {
            bucket,
            index_id: definition.index_id,
            generation,
        });
    }
    write_state(
        state_path,
        &MembershipState {
            schema: STATE_SCHEMA.into(),
            active_nodes_verified: 1,
            fixtures,
        },
    )?;
    println!("seeded {MEMBERSHIP_FIXTURES} one-node index builders");
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
    if state.active_nodes_verified + 1 != active_nodes {
        return Err(invalid(format!(
            "membership state last verified {} ACTIVE node(s), cannot verify {active_nodes}",
            state.active_nodes_verified
        )));
    }
    let (new_path, command_suffix) = match active_nodes {
        2 => ("docs/after-two.json", "after-two"),
        3 => ("docs/after-three.json", "after-three"),
        _ => unreachable!("validated above"),
    };
    let before_paths = if active_nodes == 2 {
        paths(["docs/before.json"])
    } else {
        paths(["docs/after-two.json", "docs/before.json"])
    };
    let after_paths = if active_nodes == 2 {
        paths(["docs/after-two.json", "docs/before.json"])
    } else {
        paths([
            "docs/after-three.json",
            "docs/after-two.json",
            "docs/before.json",
        ])
    };
    let mut indexes = context.index_clients()?;
    let mut objects = context.object_clients()?;
    for (position, fixture) in state.fixtures.iter().enumerate() {
        wait_for_paths(&mut indexes, &fixture.bucket, &before_paths, 0).await?;
        let ingress = position % objects.len();
        put_path(
            &mut objects[ingress],
            &context.tenant,
            &fixture.bucket,
            new_path,
            &format!("index-membership-{command_suffix}-{position}"),
        )
        .await?;
    }
    for fixture in &mut state.fixtures {
        fixture.generation = wait_for_paths(
            &mut indexes,
            &fixture.bucket,
            &after_paths,
            fixture.generation,
        )
        .await?;
    }
    state.active_nodes_verified = active_nodes;
    write_state(&state_path, &state)?;
    println!(
        "verified {} pre-growth indexes after online {active_nodes}-node assignment",
        state.fixtures.len(),
    );
    Ok(())
}

async fn gap_seed(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 3 {
        return Err(invalid("journal-gap seed requires three endpoints"));
    }
    let bucket = required("ANVIL_INDEX_RECOVERY_QUALIFICATION_BUCKET")?;
    let mut administrator = administration_client(context.channels[0].clone(), &context.token)?;
    let unrelated_bucket = format!("{bucket}-unrelated");
    create_bucket(&mut administrator, &unrelated_bucket).await?;
    let mut unrelated_objects = context.object_clients()?.remove(0);
    let mut unrelated_accepted = 0_usize;
    for start in (0..GAP_UNRELATED_RECORDS).step_by(GAP_BATCH) {
        let end = (start + GAP_BATCH).min(GAP_UNRELATED_RECORDS);
        unrelated_accepted += bulk_put_paths(
            &mut unrelated_objects,
            &context.tenant,
            &unrelated_bucket,
            start,
            end,
            "unrelated",
            "index-gap-unrelated",
        )
        .await?
        .len();
    }
    if unrelated_accepted != GAP_UNRELATED_RECORDS {
        return Err(invalid(format!(
            "unrelated gap fixture accepted {unrelated_accepted} of {GAP_UNRELATED_RECORDS} objects"
        )));
    }
    create_bucket(&mut administrator, &bucket).await?;
    let mut indexes = context.index_clients()?;
    let mut objects = context.object_clients()?;
    let definition = create_path_index(&mut indexes[0], &bucket, 0).await?;
    let baseline = wait_for_paths(&mut indexes, &bucket, &BTreeSet::new(), 0).await?;
    put_path(
        &mut objects[0],
        &context.tenant,
        &bucket,
        "gap/seed.json",
        "index-gap-seed",
    )
    .await?;
    let generation =
        wait_for_paths(&mut indexes, &bucket, &paths(["gap/seed.json"]), baseline).await?;
    let resume_token =
        current_watch_checkpoint(&mut objects[0], address(&context.tenant, &bucket, "gap/"))
            .await?;
    write_state(
        state_path,
        &GapState {
            schema: STATE_SCHEMA.into(),
            fixture: FixtureState {
                bucket,
                index_id: definition.index_id,
                generation,
            },
            unrelated_bucket,
            unrelated_objects: GAP_UNRELATED_RECORDS,
            max_scoped_head_reads_per_source: GAP_SCOPED_HEAD_READ_LIMIT_PER_SOURCE,
            expected_scoped_heads: 1,
            resume_token,
            next_candidate: 0,
            successful_paths: Vec::new(),
        },
    )?;
    println!("seeded journal-gap index and captured its pre-gap cursor");
    Ok(())
}

async fn gap_write(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 1 {
        return Err(invalid(
            "journal-gap writer requires one selected ingress endpoint",
        ));
    }
    let mut state: GapState = read_state(&state_path)?;
    require_schema(&state.schema)?;
    let mut objects = context.object_clients()?.remove(0);
    while state.next_candidate < GAP_MAX_CANDIDATES {
        let end = (state.next_candidate + GAP_BATCH).min(GAP_MAX_CANDIDATES);
        let accepted = bulk_put_paths(
            &mut objects,
            &context.tenant,
            &state.fixture.bucket,
            state.next_candidate,
            end,
            "gap",
            "index-gap-write",
        )
        .await?;
        state.next_candidate = end;
        state.successful_paths.extend(accepted);
        state.expected_scoped_heads = state.successful_paths.len() + 1;
        write_state(&state_path, &state)?;
        if watch_cursor_expired(
            &mut objects,
            address(&context.tenant, &state.fixture.bucket, "gap/"),
            &state.resume_token,
        )
        .await?
        {
            println!(
                "proved a retained source-journal gap after {} accepted writes from {} candidates",
                state.successful_paths.len(),
                state.next_candidate
            );
            return Ok(());
        }
    }
    Err(invalid(
        "the pre-gap public cursor did not expire within the bounded write budget",
    ))
}

async fn gap_verify(context: &Context, state_path: PathBuf) -> TestResult<()> {
    if context.channels.len() != 3 {
        return Err(invalid("journal-gap verification requires three endpoints"));
    }
    let state: GapState = read_state(state_path)?;
    require_schema(&state.schema)?;
    if state.successful_paths.is_empty() {
        return Err(invalid("journal-gap state contains no post-cursor writes"));
    }
    if state.unrelated_bucket.is_empty()
        || state.unrelated_objects != GAP_UNRELATED_RECORDS
        || state.max_scoped_head_reads_per_source != GAP_SCOPED_HEAD_READ_LIMIT_PER_SOURCE
        || state.expected_scoped_heads != state.successful_paths.len() + 1
    {
        return Err(invalid("journal-gap unrelated-scope fixture is incomplete"));
    }
    let mut indexes = context.index_clients()?;
    let mut expected = paths(["gap/seed.json"]);
    expected.extend(state.successful_paths.iter().cloned());
    wait_for_paths(
        &mut indexes,
        &state.fixture.bucket,
        &expected,
        state.fixture.generation,
    )
    .await?;
    println!(
        "scoped rebuild recovered all {} live paths after a genuine journal gap",
        state.successful_paths.len() + 1
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

async fn put_path(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    path: &str,
    command_id: &str,
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
        [br#"{"qualified":true}"#.to_vec()],
    )
    .await?;
    Ok(())
}

async fn bulk_put_paths(
    client: &mut RawClient,
    tenant: &str,
    bucket: &str,
    start: usize,
    end: usize,
    path_prefix: &str,
    command_prefix: &str,
) -> TestResult<Vec<String>> {
    let request = BulkWriteRequest {
        operations: (start..end)
            .map(|position| BulkOperation {
                operation: Some(BulkOperationValue::Put(BulkPutRequest {
                    address: Some(address(
                        tenant,
                        bucket,
                        &format!("{path_prefix}/{position:06}.json"),
                    )),
                    bytes: br#"{"qualified":true}"#.to_vec(),
                    content_type: "application/json".into(),
                    command_id: format!("{command_prefix}-{position}"),
                    durability: Durability::Local as i32,
                })),
            })
            .collect(),
    };
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        match client.bulk_write(request.clone()).await {
            Ok(response) => {
                let outcomes = response.into_inner().outcomes;
                if outcomes.len() != end - start {
                    return Err(invalid("journal-gap BulkWrite omitted an outcome"));
                }
                let mut seen = BTreeSet::new();
                let mut accepted = Vec::new();
                for outcome in outcomes {
                    let index = usize::try_from(outcome.index)?;
                    if index >= end - start || !seen.insert(index) {
                        return Err(invalid(
                            "journal-gap BulkWrite returned an invalid outcome index",
                        ));
                    }
                    let candidate = start + index;
                    match outcome.outcome {
                        Some(BulkOutcomeValue::Receipt(receipt)) => {
                            if receipt.command_id != format!("{command_prefix}-{candidate}")
                                || receipt.version == 0
                                || receipt.deleted
                            {
                                return Err(invalid(
                                    "journal-gap BulkWrite returned an invalid receipt",
                                ));
                            }
                            accepted.push(format!("{path_prefix}/{candidate:06}.json"));
                        }
                        Some(BulkOutcomeValue::Failure(failure))
                            if failure.code
                                == MutationFailureCode::DurabilityUnavailable as i32 => {}
                        Some(BulkOutcomeValue::Failure(failure)) => {
                            return Err(invalid(format!(
                                "journal-gap candidate {candidate} failed non-retryably with code {}: {}",
                                failure.code, failure.message
                            )));
                        }
                        None => {
                            return Err(invalid(
                                "journal-gap BulkWrite outcome omitted its result",
                            ));
                        }
                    }
                }
                return Ok(accepted);
            }
            Err(status) if retryable(&status) && Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Err(status) => return Err(status.into()),
        }
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

async fn current_watch_checkpoint(
    client: &mut RawClient,
    prefix: ObjectAddress,
) -> TestResult<Vec<u8>> {
    let mut stream = client
        .watch_prefix(WatchPrefixRequest {
            prefix: Some(prefix),
            start: Some(WatchStart::Now(WatchNow {})),
        })
        .await?
        .into_inner();
    let message = tokio::time::timeout(WAIT_LIMIT, stream.message())
        .await??
        .ok_or_else(|| invalid("watch ended before its initial checkpoint"))?;
    match message.message {
        Some(WatchMessageValue::Checkpoint(checkpoint)) if !checkpoint.resume_token.is_empty() => {
            Ok(checkpoint.resume_token)
        }
        _ => Err(invalid("watch omitted its initial checkpoint")),
    }
}

async fn watch_cursor_expired(
    client: &mut RawClient,
    prefix: ObjectAddress,
    resume_token: &[u8],
) -> TestResult<bool> {
    let response = client
        .watch_prefix(WatchPrefixRequest {
            prefix: Some(prefix),
            start: Some(WatchStart::ResumeToken(resume_token.to_vec())),
        })
        .await;
    let mut stream = match response {
        Err(status) if resume_expired(&status) => return Ok(true),
        Err(status) if retryable(&status) => return Ok(false),
        Err(status) => return Err(status.into()),
        Ok(response) => response.into_inner(),
    };
    match tokio::time::timeout(Duration::from_secs(2), stream.message()).await {
        Ok(Err(status)) if resume_expired(&status) => Ok(true),
        Ok(Err(status)) if retryable(&status) => Ok(false),
        Ok(Err(status)) => Err(status.into()),
        Ok(Ok(_)) | Err(_) => Ok(false),
    }
}

fn resume_expired(status: &Status) -> bool {
    matches!(status.code(), Code::FailedPrecondition | Code::OutOfRange)
        && status.message() == "RESUME_EXPIRED"
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
    std::fs::write(path, serde_json::to_vec_pretty(state)?)?;
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
