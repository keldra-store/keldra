//! Public-API qualification for an authenticated atomic multi-object program.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use keldra_storage::v1::accounting_service_client::AccountingServiceClient;
use keldra_storage::v1::index_query::Query as IndexQueryValue;
use keldra_storage::v1::object_chunk::Value as ObjectChunkValue;
use keldra_storage::v1::object_head::State as ObjectHeadState;
use keldra_storage::v1::put_header::Operation as PutOperationValue;
use keldra_storage::v1::{
    AccountingMeasurementState, BucketPolicy, CreateBucketRequest, DisableAccountingRequest,
    Durability, EnableAccountingRequest, GetAccountingRequest, GetObjectRequest, HeadObjectRequest,
    IndexFreshnessRequirement, IndexQuery, InvokeProgramRequest, ObjectAddress, ObjectVersioning,
    PutHeader, PutImmutableOperation, QueryIndexRequest, SetBucketPolicyRequest,
    TypedJsonIndexQuery,
};
use keldra_storage::{
    BearerToken, BooleanField, KeywordField, PredicateExpression, RawClient, RawIndexClient,
    TypedJsonIndexBuilder, administration_client, connect_channel, exchange_client_credentials,
    index_client, object_client, put_chunks,
};
use serde_json::Value;
use tokio::time::{Instant, sleep};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type AccountingClient = AccountingServiceClient<InterceptedService<Channel, BearerToken>>;

const PROGRAM_PATH: &str = "_keldra/programs/atomic-program-qualification@1";
const TASK_ID: &str = "01J00000000000000000000000";
const TASK_PATH: &str = "atomic/tasks/01J00000000000000000000000.json";
const TASK_INDEX: &str = "worka-task-lookup";
const SECONDARY_PATH: &str = "atomic/secondary.json";
const REPLICA_WAIT_LIMIT: Duration = Duration::from_secs(90);
const REPLICA_POLL_INTERVAL: Duration = Duration::from_millis(100);
const PROGRAM: &[u8] = br#"{"schema_version":1,"documents":[{"name":"primary","path":{"tenant":"{tenant}","bucket":"{bucket}","path":"atomic/tasks/01J00000000000000000000000.json"},"cardinality":"one","access":"read_write","allow_initial_json":true},{"name":"secondary","path":{"tenant":"{tenant}","bucket":"{bucket}","path":"atomic/secondary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true}],"assertions":[],"operations":[{"kind":"set_value","target":{"document":{"slot":"primary","index":0},"pointer":"/schedulable"},"value":{"kind":"literal","value":true}},{"kind":"set_value","target":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"secondary-committed"}}],"returns":[{"name":"task_id","value":{"value":{"document":{"slot":"primary","index":0},"pointer":"/id"},"view":"current"}},{"name":"task_schedulable","value":{"value":{"document":{"slot":"primary","index":0},"pointer":"/schedulable"},"view":"current"}},{"name":"secondary_status","value":{"value":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"view":"current"}}],"caps":{"max_paths":2,"max_writes":2,"max_operations":4,"max_input_bytes":4096,"max_document_bytes":4096}}"#;

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("KELDRA_ATOMIC_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "atomic qualification requires either one or three endpoints",
        ));
    }
    let tenant = required("KELDRA_ATOMIC_QUALIFICATION_TENANT")?;
    let bucket = required("KELDRA_ATOMIC_QUALIFICATION_BUCKET")?;
    let client_id = required("KELDRA_ATOMIC_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("KELDRA_ATOMIC_QUALIFICATION_CLIENT_SECRET")?;
    let durability = if endpoints.len() == 1 {
        Durability::Local
    } else {
        Durability::Replicated
    };

    let mut channels = Vec::with_capacity(endpoints.len());
    for endpoint in &endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let token = exchange_client_credentials(channels[0].clone(), &client_id, &client_secret)
        .await?
        .access_token;
    let mut administrator = administration_client(channels[0].clone(), &token)?;
    administrator
        .create_bucket(CreateBucketRequest {
            bucket: bucket.clone(),
            versioning: ObjectVersioning::Unversioned as i32,
        })
        .await?;

    let mut indexes = channels
        .iter()
        .cloned()
        .map(|channel| index_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let task_index = create_task_index(&mut indexes[0], &bucket).await?;

    let mut objects = object_client(channels[0].clone(), &token)?;
    let program = String::from_utf8(PROGRAM.to_vec())?
        .replace("\"{bucket}\"", &serde_json::to_string(&bucket)?);
    put_chunks(
        &mut objects,
        PutHeader {
            address: Some(address(&tenant, &bucket, PROGRAM_PATH)),
            content_type: "application/json".into(),
            command_id: "atomic-program-qualification-program".into(),
            durability: durability as i32,
            operation: Some(PutOperationValue::PutImmutable(PutImmutableOperation {})),
        },
        [program.into_bytes()],
    )
    .await?;
    let program_hash = program_hash(&mut objects, &tenant, &bucket).await?;
    objects
        .set_bucket_policy(SetBucketPolicyRequest {
            tenant: tenant.clone(),
            bucket: bucket.clone(),
            policy: Some(BucketPolicy {
                immutable_path_prefixes: Vec::new(),
                program_only_path_prefixes: vec!["atomic".into()],
            }),
        })
        .await?;

    let mut accounting = channels
        .iter()
        .cloned()
        .map(|channel| accounting_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let accounting_definition = enable_atomic_accounting(&mut accounting[0], &bucket).await?;
    wait_for_complete_accounting_zero(&mut accounting, &bucket).await?;

    let input = invocation_input(&tenant, &bucket);
    let expected_accounting_bytes = expected_committed_payload_bytes()?;
    let first_invocation = objects.invoke_program(invocation(
        &tenant,
        &bucket,
        &program_hash,
        input.clone(),
        durability,
    ));
    let visibility = observe_all_or_nothing(channels.clone(), &token, &tenant, &bucket);
    let accounting_visibility =
        observe_atomic_accounting(&mut accounting, &bucket, expected_accounting_bytes);
    let (first, observed_pairs, accounting_bytes) =
        tokio::join!(first_invocation, visibility, accounting_visibility);
    let first = first?.into_inner();
    let observed_pairs = observed_pairs?;
    let accounting_bytes = accounting_bytes?;
    if first.replayed {
        return Err(invalid(
            "first atomic invocation unexpectedly reported replay",
        ));
    }
    if first.commit_log_index == 0 {
        return Err(invalid("first atomic invocation omitted its commit cursor"));
    }
    let atomic_through = first.commit_log_index;
    let first_output = first.output_json.clone();
    assert_output(&first_output)?;
    let first_receipts = receipt_versions(first.path_receipts)?;
    if observed_pairs.iter().any(|pair| pair != &first_receipts) {
        return Err(invalid(
            "atomic visibility observation did not match committed path receipts",
        ));
    }
    verify_task_index_eventually(
        &mut indexes,
        &tenant,
        &bucket,
        task_index.index_id,
        task_index.version,
        atomic_through,
        *first_receipts
            .get(TASK_PATH)
            .ok_or_else(|| invalid("atomic task receipt was absent"))?,
    )
    .await?;

    let replay = objects
        .invoke_program(invocation(
            &tenant,
            &bucket,
            &program_hash,
            input,
            durability,
        ))
        .await?
        .into_inner();
    if !replay.replayed {
        return Err(invalid(
            "repeat invocation did not report deterministic replay",
        ));
    }
    assert_output(&replay.output_json)?;
    let replay_receipts = receipt_versions(replay.path_receipts)?;
    if replay.output_json != first_output || replay_receipts != first_receipts {
        return Err(invalid(
            "replayed atomic invocation returned different output or path receipts",
        ));
    }
    let replay_accounting_bytes =
        observe_atomic_accounting(&mut accounting, &bucket, expected_accounting_bytes).await?;
    if replay_accounting_bytes != accounting_bytes {
        return Err(invalid(
            "replayed atomic invocation changed the logical accounting total",
        ));
    }

    for channel in channels.iter().skip(1) {
        let mut replica = object_client(channel.clone(), &token)?;
        verify_committed_pair_eventually(&mut replica, &tenant, &bucket, &first_receipts).await?;
    }
    verify_committed_pair_eventually(&mut objects, &tenant, &bucket, &first_receipts).await?;
    disable_atomic_accounting(&mut accounting[0], &bucket, accounting_definition.version).await?;

    println!(
        "atomic-program qualification passed on {} node(s): authenticated multi-object commit, Worka-shaped ULID plus schedulable Boolean conjunction on an all-source index generation covering the atomic commit cursor, {accounting_bytes} atomic logical bytes, and deterministic replay verified",
        endpoints.len(),
    );
    Ok(())
}

async fn create_task_index(
    client: &mut RawIndexClient,
    bucket: &str,
) -> TestResult<keldra_storage::v1::IndexDefinition> {
    let request = TypedJsonIndexBuilder::new(bucket, TASK_INDEX)
        .path_prefix("atomic/tasks/")
        .content_type("application/json")
        .field(KeywordField::single("id", "/id").exact())
        .field(BooleanField::single("schedulable", "/schedulable").exact())
        .finish("atomic-program-qualification-task-index")?;
    let definition = client.create_index(request).await?.into_inner();
    if definition.index_id == 0 || definition.version == 0 {
        return Err(invalid(
            "Worka-shaped task index returned an invalid identity",
        ));
    }
    Ok(definition)
}

async fn verify_task_index_eventually(
    clients: &mut [RawIndexClient],
    tenant: &str,
    bucket: &str,
    index_id: u64,
    definition_version: u64,
    atomic_through: u64,
    task_version: u64,
) -> TestResult<()> {
    let predicate = PredicateExpression::all([
        PredicateExpression::equal("id", TASK_ID)?,
        PredicateExpression::equal("schedulable", true)?,
    ])?;
    let request = QueryIndexRequest {
        bucket: bucket.into(),
        index_name: TASK_INDEX.into(),
        query: Some(IndexQuery {
            query: Some(IndexQueryValue::TypedJson(TypedJsonIndexQuery {
                predicate: Some(predicate.into_proto()),
                order: Vec::new(),
                facets: Vec::new(),
                aggregates: Vec::new(),
            })),
        }),
        limit: 1,
        page_token: Vec::new(),
        tenant: String::new(),
        required_freshness: Some(IndexFreshnessRequirement {
            sources: Vec::new(),
            atomic_through: Some(atomic_through),
        }),
    };
    let deadline = Instant::now() + REPLICA_WAIT_LIMIT;
    let expected_sources = clients.len();
    let mut last = String::new();
    loop {
        let mut complete = true;
        for client in clients.iter_mut() {
            match client.query_index(request.clone()).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let all_sources = response.freshness.as_ref().is_some_and(|freshness| {
                        let source_ids = freshness
                            .sources
                            .iter()
                            .map(|source| source.node_id)
                            .collect::<std::collections::BTreeSet<_>>();
                        freshness.index_id == index_id
                            && freshness.definition_version == definition_version
                            && freshness.commit_revision >= atomic_through
                            && freshness.initial_build_complete
                            && !freshness.rebuilding
                            && freshness.authorization_revision != 0
                            && freshness.placement_term != 0
                            && freshness.placement_index != 0
                            && freshness.sources.len() == expected_sources
                            && source_ids.len() == expected_sources
                            && freshness.sources.iter().all(|source| {
                                source.node_id != 0 && source.source_epoch.len() == 32
                            })
                    });
                    let positive_hit = response.hits.as_slice()
                        == [keldra_storage::v1::IndexQueryHit {
                            address: Some(address(tenant, bucket, TASK_PATH)),
                            object_version: task_version,
                            score: None,
                        }];
                    if !all_sources || !positive_hit || !response.next_page_token.is_empty() {
                        complete = false;
                        last = format!(
                            "all_sources={all_sources} hits={} next_page_token_bytes={}",
                            response.hits.len(),
                            response.next_page_token.len()
                        );
                        break;
                    }
                }
                Err(status) if retryable_index(&status) => {
                    complete = false;
                    last = status.to_string();
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if complete {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "Worka-shaped atomic task did not become a positive all-source index hit covering its atomic commit cursor through every endpoint: {last}"
            )));
        }
        sleep(REPLICA_POLL_INTERVAL).await;
    }
}

fn retryable_index(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::NotFound
    )
}

fn accounting_client(
    channel: Channel,
    token: &str,
) -> Result<AccountingClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(
        AccountingServiceClient::with_interceptor(channel, BearerToken::new(token)?)
            .max_encoding_message_size(72 * 1024 * 1024)
            .max_decoding_message_size(72 * 1024 * 1024),
    )
}

async fn enable_atomic_accounting(
    client: &mut AccountingClient,
    bucket: &str,
) -> TestResult<keldra_storage::v1::AccountingDefinition> {
    let definition = client
        .enable_accounting(EnableAccountingRequest {
            bucket: bucket.into(),
            path_prefix: "atomic".into(),
            command_id: "atomic-program-qualification-accounting-enable".into(),
        })
        .await?
        .into_inner();
    if definition.accounting_id == 0 || definition.version == 0 {
        return Err(invalid(
            "atomic accounting definition returned an invalid identity",
        ));
    }
    Ok(definition)
}

async fn disable_atomic_accounting(
    client: &mut AccountingClient,
    bucket: &str,
    expected_version: u64,
) -> TestResult<()> {
    let outcome = client
        .disable_accounting(DisableAccountingRequest {
            bucket: bucket.into(),
            path_prefix: "atomic".into(),
            expected_version,
            command_id: "atomic-program-qualification-accounting-disable".into(),
        })
        .await?
        .into_inner();
    if !outcome.disabled || outcome.tombstone_version == 0 {
        return Err(invalid(
            "atomic accounting disable returned an invalid outcome",
        ));
    }
    Ok(())
}

async fn wait_for_complete_accounting_zero(
    clients: &mut [AccountingClient],
    bucket: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + REPLICA_WAIT_LIMIT;
    let mut last = String::new();
    loop {
        let mut all_zero = true;
        for client in clients.iter_mut() {
            match client
                .get_accounting(GetAccountingRequest {
                    bucket: bucket.into(),
                    path_prefix: "atomic".into(),
                })
                .await
            {
                Ok(response) => {
                    let snapshot = response.into_inner();
                    let complete_zero = snapshot.logical.as_ref().is_some_and(|logical| {
                        logical
                            .visible_file_count
                            .as_ref()
                            .is_some_and(|measurement| {
                                measurement.state == AccountingMeasurementState::Present as i32
                                    && measurement.count == 0
                            })
                            && logical
                                .billable_logical_bytes
                                .as_ref()
                                .is_some_and(|measurement| measurement.bytes == 0)
                            && logical
                                .freshness
                                .as_ref()
                                .is_some_and(|freshness| freshness.complete)
                    });
                    if !complete_zero {
                        last = format!("latest atomic accounting snapshot: {snapshot:?}");
                        all_zero = false;
                    }
                }
                Err(status) if retryable_accounting(&status) => {
                    last = status.to_string();
                    all_zero = false;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if all_zero {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "atomic accounting did not establish a complete zero baseline: {last}"
            )));
        }
        sleep(REPLICA_POLL_INTERVAL).await;
    }
}

async fn observe_atomic_accounting(
    clients: &mut [AccountingClient],
    bucket: &str,
    expected_bytes: u64,
) -> TestResult<u64> {
    let deadline = Instant::now() + REPLICA_WAIT_LIMIT;
    let mut last = String::new();
    loop {
        let mut full_bytes = vec![None; clients.len()];
        for (position, client) in clients.iter_mut().enumerate() {
            match client
                .get_accounting(GetAccountingRequest {
                    bucket: bucket.into(),
                    path_prefix: "atomic".into(),
                })
                .await
            {
                Ok(response) => {
                    let snapshot = response.into_inner();
                    let Some(logical) = snapshot.logical.as_ref() else {
                        return Err(invalid("atomic accounting omitted logical usage"));
                    };
                    let complete = logical
                        .freshness
                        .as_ref()
                        .is_some_and(|freshness| freshness.complete);
                    let bytes = logical
                        .billable_logical_bytes
                        .as_ref()
                        .map(|measurement| measurement.bytes)
                        .unwrap_or(u64::MAX);
                    let file_count = logical
                        .visible_file_count
                        .as_ref()
                        .filter(|measurement| {
                            measurement.state == AccountingMeasurementState::Present as i32
                        })
                        .map(|measurement| measurement.count)
                        .unwrap_or(u64::MAX);
                    match (file_count, bytes) {
                        (0, 0) => {
                            last =
                                "latest complete atomic accounting snapshot remained zero".into();
                        }
                        (2, bytes) if bytes == expected_bytes && complete => {
                            full_bytes[position] = Some(bytes)
                        }
                        (2, bytes) if bytes == expected_bytes => {
                            last = format!(
                                "latest exact atomic accounting snapshot was incomplete: {snapshot:?}"
                            );
                        }
                        (objects, bytes) => {
                            return Err(invalid(format!(
                                "complete accounting snapshot exposed a partial atomic commit: objects={objects} logical_bytes={bytes}"
                            )));
                        }
                    }
                }
                Err(status) if retryable_accounting(&status) => last = status.to_string(),
                Err(status) => return Err(status.into()),
            }
        }
        if full_bytes.iter().all(Option::is_some) {
            let expected = full_bytes[0].expect("checked above");
            if full_bytes.iter().any(|bytes| *bytes != Some(expected)) {
                return Err(invalid(format!(
                    "atomic accounting endpoints disagreed on final logical bytes: {full_bytes:?}"
                )));
            }
            return Ok(expected);
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "atomic accounting did not converge to two objects on every endpoint: {last}"
            )));
        }
        sleep(REPLICA_POLL_INTERVAL).await;
    }
}

fn expected_committed_payload_bytes() -> TestResult<u64> {
    let primary = serde_json::to_vec(&serde_json::json!({
        "id": TASK_ID,
        "schedulable": true,
    }))?;
    let secondary = serde_json::to_vec(&serde_json::json!({
        "status": "secondary-committed",
    }))?;
    u64::try_from(primary.len() + secondary.len())
        .map_err(|_| invalid("atomic accounting payload length is exhausted"))
}

fn retryable_accounting(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::NotFound
    )
}

fn invocation(
    tenant: &str,
    bucket: &str,
    program_hash: &[u8],
    input_json: Vec<u8>,
    durability: Durability,
) -> InvokeProgramRequest {
    InvokeProgramRequest {
        program: Some(address(tenant, bucket, PROGRAM_PATH)),
        invocation_id: "atomic-program-qualification-invocation".into(),
        program_hash: program_hash.to_vec(),
        input_json,
        durability: durability as i32,
    }
}

async fn program_hash(objects: &mut RawClient, tenant: &str, bucket: &str) -> TestResult<Vec<u8>> {
    match objects
        .head_object(HeadObjectRequest {
            address: Some(address(tenant, bucket, PROGRAM_PATH)),
        })
        .await?
        .into_inner()
        .state
    {
        Some(ObjectHeadState::Present(present)) if present.content_hash.len() == 32 => {
            Ok(present.content_hash)
        }
        _ => Err(invalid("atomic program head omitted its content identity")),
    }
}

fn invocation_input(tenant: &str, bucket: &str) -> Vec<u8> {
    format!(
        r#"{{"bindings":{{"primary":[{{"path":{{"tenant":"{tenant}","bucket":"{bucket}","path":"{TASK_PATH}"}},"template_values":{{}},"expected_head":{{"kind":"absent"}},"initial_json":{{"id":"{TASK_ID}","schedulable":false}}}}],"secondary":[{{"path":{{"tenant":"{tenant}","bucket":"{bucket}","path":"{SECONDARY_PATH}"}},"template_values":{{}},"expected_head":{{"kind":"absent"}},"initial_json":{{"status":"uncommitted"}}}}]}}}}"#
    )
    .into_bytes()
}

fn assert_output(output: &[u8]) -> TestResult<()> {
    let actual: Value = serde_json::from_slice(output)?;
    let expected = serde_json::json!({
        "task_id": TASK_ID,
        "task_schedulable": true,
        "secondary_status": "secondary-committed",
    });
    if actual != expected {
        return Err(invalid("atomic program returned unexpected output"));
    }
    Ok(())
}

fn receipt_versions(
    receipts: Vec<keldra_storage::v1::ProgramPathReceipt>,
) -> TestResult<BTreeMap<String, u64>> {
    let versions = receipts
        .into_iter()
        .map(|receipt| {
            let address = receipt
                .address
                .ok_or_else(|| invalid("atomic path receipt omitted its address"))?;
            if receipt.deleted || receipt.version == 0 {
                return Err(invalid("atomic path receipt was not a live version"));
            }
            Ok((address.path, receipt.version))
        })
        .collect::<TestResult<BTreeMap<_, _>>>()?;
    if versions.len() != 2
        || !versions.contains_key(TASK_PATH)
        || !versions.contains_key(SECONDARY_PATH)
    {
        return Err(invalid(
            "atomic program did not commit exactly the two bound paths",
        ));
    }
    Ok(versions)
}

async fn observe_all_or_nothing(
    channels: Vec<tonic::transport::Channel>,
    token: &str,
    tenant: &str,
    bucket: &str,
) -> TestResult<Vec<BTreeMap<String, u64>>> {
    let mut clients = channels
        .into_iter()
        .map(|channel| object_client(channel, token))
        .collect::<Result<Vec<_>, _>>()?;
    let deadline = Instant::now() + REPLICA_WAIT_LIMIT;
    let mut complete = vec![None; clients.len()];

    loop {
        for (position, client) in clients.iter_mut().enumerate() {
            match observe_pair(client, tenant, bucket).await? {
                PairObservation::BothAbsent => {}
                PairObservation::BothPresent(versions) => complete[position] = Some(versions),
            }
        }
        if complete.iter().all(Option::is_some) {
            return Ok(complete
                .into_iter()
                .map(|value| value.expect("checked above"))
                .collect());
        }
        if Instant::now() >= deadline {
            return Err(invalid(
                "atomic paths did not become jointly visible on every public endpoint",
            ));
        }
        sleep(REPLICA_POLL_INTERVAL).await;
    }
}

enum PairObservation {
    BothAbsent,
    BothPresent(BTreeMap<String, u64>),
}

async fn observe_pair(
    objects: &mut RawClient,
    tenant: &str,
    bucket: &str,
) -> TestResult<PairObservation> {
    let primary = objects
        .head_object(HeadObjectRequest {
            address: Some(address(tenant, bucket, TASK_PATH)),
        })
        .await?
        .into_inner()
        .state;
    let secondary = objects
        .head_object(HeadObjectRequest {
            address: Some(address(tenant, bucket, SECONDARY_PATH)),
        })
        .await?
        .into_inner()
        .state;
    match (primary, secondary) {
        (Some(ObjectHeadState::NeverExisted(_)), Some(ObjectHeadState::NeverExisted(_))) => {
            Ok(PairObservation::BothAbsent)
        }
        (Some(ObjectHeadState::Present(primary)), Some(ObjectHeadState::Present(secondary)))
            if primary.version != 0 && secondary.version != 0 =>
        {
            Ok(PairObservation::BothPresent(BTreeMap::from([
                (TASK_PATH.into(), primary.version),
                (SECONDARY_PATH.into(), secondary.version),
            ])))
        }
        (primary, secondary) => Err(invalid(format!(
            "public endpoint exposed a partial atomic pair: primary={primary:?} secondary={secondary:?}"
        ))),
    }
}

async fn verify_committed_pair(
    objects: &mut RawClient,
    tenant: &str,
    bucket: &str,
    receipts: &BTreeMap<String, u64>,
) -> TestResult<()> {
    for (path, expected_version) in receipts {
        let head = objects
            .head_object(HeadObjectRequest {
                address: Some(address(tenant, bucket, path)),
            })
            .await?
            .into_inner();
        match head.state {
            Some(ObjectHeadState::Present(present)) if present.version == *expected_version => {}
            other => {
                return Err(invalid(format!(
                    "atomic path {path} has unexpected head: {other:?}"
                )));
            }
        }
        let bytes = read_all(objects, address(tenant, bucket, path)).await?;
        let value: Value = serde_json::from_slice(&bytes)?;
        let valid = if path == TASK_PATH {
            value.get("id").and_then(Value::as_str) == Some(TASK_ID)
                && value.get("schedulable").and_then(Value::as_bool) == Some(true)
        } else if path == SECONDARY_PATH {
            value.get("status").and_then(Value::as_str) == Some("secondary-committed")
        } else {
            false
        };
        if !valid {
            return Err(invalid(format!(
                "atomic path {path} has unexpected JSON state"
            )));
        }
    }
    Ok(())
}

async fn verify_committed_pair_eventually(
    objects: &mut RawClient,
    tenant: &str,
    bucket: &str,
    receipts: &BTreeMap<String, u64>,
) -> TestResult<()> {
    let deadline = Instant::now() + REPLICA_WAIT_LIMIT;
    let mut last_error = String::new();
    loop {
        match verify_committed_pair(objects, tenant, bucket, receipts).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = error.to_string(),
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "atomic committed pair did not become visible before deadline: {last_error}"
            )));
        }
        sleep(REPLICA_POLL_INTERVAL).await;
    }
}

async fn read_all(client: &mut RawClient, address: ObjectAddress) -> TestResult<Vec<u8>> {
    let mut stream = client
        .get_object(GetObjectRequest {
            address: Some(address),
            version: None,
        })
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.message().await? {
        if let Some(ObjectChunkValue::Bytes(value)) = chunk.value {
            bytes.extend_from_slice(&value);
        }
    }
    Ok(bytes)
}

fn address(tenant: &str, bucket: &str, path: &str) -> ObjectAddress {
    ObjectAddress {
        tenant: tenant.into(),
        bucket: bucket.into(),
        path: path.into(),
    }
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
