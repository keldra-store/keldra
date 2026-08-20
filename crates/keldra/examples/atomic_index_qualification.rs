//! Public-API qualification for all-or-nothing atomic-program index visibility.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use keldra_storage::v1::index_query::Query as QueryValue;
use keldra_storage::v1::index_service_client::IndexServiceClient;
use keldra_storage::v1::index_specification::Specification as SpecificationValue;
use keldra_storage::v1::object_head::State as ObjectHeadState;
use keldra_storage::v1::put_header::Operation as PutOperationValue;
use keldra_storage::v1::{
    BucketPolicy, CreateBucketRequest, CreateIndexRequest, Durability, HeadObjectRequest,
    IndexQuery, IndexSpecification, InvokeProgramRequest, ObjectAddress, ObjectVersioning,
    PathIndexQuery, PathIndexSpec, PutHeader, PutImmutableOperation, QueryIndexRequest,
    SetBucketPolicyRequest,
};
use keldra_storage::{
    BearerToken, administration_client, connect_channel, exchange_client_credentials,
    object_client, put_chunks,
};
use tokio::time::{Instant, sleep};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type IndexClient = IndexServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(90);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const INDEX_NAME: &str = "atomic-paths";
const PROGRAM_PATH: &str = "_keldra/programs/qualification@1";
const PRIMARY_PATH: &str = "atomic/primary.json";
const SECONDARY_PATH: &str = "atomic/secondary.json";
const PROGRAM: &[u8] = br#"{"schema_version":1,"documents":[{"name":"primary","path":{"tenant":"{tenant}","bucket":"{bucket}","path":"atomic/primary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true},{"name":"secondary","path":{"tenant":"{tenant}","bucket":"{bucket}","path":"atomic/secondary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true}],"assertions":[],"operations":[{"kind":"set_value","target":{"document":{"slot":"primary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"primary-committed"}},{"kind":"set_value","target":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"secondary-committed"}}],"returns":[{"name":"primary_status","value":{"value":{"document":{"slot":"primary","index":0},"pointer":"/status"},"view":"current"}},{"name":"secondary_status","value":{"value":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"view":"current"}}],"caps":{"max_paths":2,"max_writes":2,"max_operations":4,"max_input_bytes":4096,"max_document_bytes":4096}}"#;

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("ANVIL_ATOMIC_INDEX_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "atomic index qualification requires either one or three endpoints",
        ));
    }
    let tenant = required("ANVIL_ATOMIC_INDEX_QUALIFICATION_TENANT")?;
    let bucket = required("ANVIL_ATOMIC_INDEX_QUALIFICATION_BUCKET")?;
    let client_id = required("ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("ANVIL_ATOMIC_INDEX_QUALIFICATION_CLIENT_SECRET")?;

    let mut channels = Vec::with_capacity(endpoints.len());
    for endpoint in &endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    let token = exchange_client_credentials(channels[0].clone(), client_id, client_secret)
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
    let definition = indexes[0]
        .create_index(CreateIndexRequest {
            bucket: bucket.clone(),
            name: INDEX_NAME.into(),
            path_prefix: "atomic/".into(),
            content_type: "application/json".into(),
            specification: Some(IndexSpecification {
                specification: Some(SpecificationValue::Path(PathIndexSpec {})),
            }),
            command_id: "atomic-index-qualification-create".into(),
        })
        .await?
        .into_inner();
    if definition.index_id == 0 || definition.version == 0 {
        return Err(invalid(
            "atomic qualification index has an invalid identity",
        ));
    }
    let baseline_generation = wait_for_empty(&mut indexes, &bucket).await?;

    let durability = if endpoints.len() == 1 {
        Durability::Local
    } else {
        Durability::Replicated
    };
    let mut objects = object_client(channels[0].clone(), &token)?;
    let program = String::from_utf8(PROGRAM.to_vec())?
        .replace("\"{bucket}\"", &serde_json::to_string(&bucket)?);
    put_chunks(
        &mut objects,
        PutHeader {
            address: Some(address(&tenant, &bucket, PROGRAM_PATH)),
            content_type: "application/json".into(),
            command_id: "atomic-index-qualification-program".into(),
            durability: durability as i32,
            operation: Some(PutOperationValue::PutImmutable(PutImmutableOperation {})),
        },
        [program.into_bytes()],
    )
    .await?;
    let program_hash = match objects
        .head_object(HeadObjectRequest {
            address: Some(address(&tenant, &bucket, PROGRAM_PATH)),
        })
        .await?
        .into_inner()
        .state
    {
        Some(ObjectHeadState::Present(present)) if present.content_hash.len() == 32 => {
            present.content_hash
        }
        _ => return Err(invalid("atomic program head omitted its content identity")),
    };
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

    let input = format!(
        r#"{{"bindings":{{"primary":[{{"path":{{"tenant":"{tenant}","bucket":"{bucket}","path":"{PRIMARY_PATH}"}},"template_values":{{}},"expected_head":{{"kind":"absent"}},"initial_json":{{"status":"uncommitted"}}}}],"secondary":[{{"path":{{"tenant":"{tenant}","bucket":"{bucket}","path":"{SECONDARY_PATH}"}},"template_values":{{}},"expected_head":{{"kind":"absent"}},"initial_json":{{"status":"uncommitted"}}}}]}}}}"#
    );
    let invocation = objects.invoke_program(InvokeProgramRequest {
        program: Some(address(&tenant, &bucket, PROGRAM_PATH)),
        invocation_id: "atomic-index-qualification-invocation".into(),
        program_hash,
        input_json: input.into_bytes(),
        durability: durability as i32,
    });
    let observation = observe_all_or_nothing(&mut indexes, &bucket, baseline_generation);
    let (invoked, observed) = tokio::join!(invocation, observation);
    let invoked = invoked?.into_inner();
    let observed = observed?;

    if invoked.output_json
        != br#"{"primary_status":"primary-committed","secondary_status":"secondary-committed"}"#
    {
        return Err(invalid("atomic program returned unexpected output"));
    }
    let committed = invoked
        .path_receipts
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
    if committed != observed {
        return Err(invalid(format!(
            "indexed atomic versions {observed:?} differ from committed versions {committed:?}"
        )));
    }

    println!(
        "atomic-program index visibility passed on {} node(s): every observed generation contained zero or both paths",
        endpoints.len()
    );
    Ok(())
}

async fn wait_for_empty(clients: &mut [IndexClient], bucket: &str) -> TestResult<u64> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let mut generation = 0;
        let mut ready = true;
        for client in clients.iter_mut() {
            match client.query_index(query(bucket)).await {
                Ok(response) => {
                    let response = response.into_inner();
                    let freshness = response
                        .freshness
                        .ok_or_else(|| invalid("atomic index query omitted freshness"))?;
                    if !response.hits.is_empty()
                        || !freshness.initial_build_complete
                        || freshness.rebuilding
                    {
                        ready = false;
                        break;
                    }
                    generation = generation.max(freshness.generation);
                }
                Err(status) if retryable(&status) => {
                    ready = false;
                    break;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if ready && generation != 0 {
            return Ok(generation);
        }
        if Instant::now() >= deadline {
            return Err(invalid("atomic index baseline did not become ready"));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn observe_all_or_nothing(
    clients: &mut [IndexClient],
    bucket: &str,
    baseline_generation: u64,
) -> TestResult<BTreeMap<String, u64>> {
    let deadline = Instant::now() + WAIT_LIMIT;
    let mut complete = vec![None; clients.len()];
    loop {
        for (position, client) in clients.iter_mut().enumerate() {
            match client.query_index(query(bucket)).await {
                Ok(response) => {
                    let response = response.into_inner();
                    if response.hits.len() == 1 {
                        return Err(invalid(
                            "an index generation exposed only one atomic-program path",
                        ));
                    }
                    if response.hits.len() > 2 {
                        return Err(invalid("atomic index returned an unrelated path"));
                    }
                    if response.hits.len() == 2 {
                        let freshness = response
                            .freshness
                            .as_ref()
                            .ok_or_else(|| invalid("atomic index query omitted freshness"))?;
                        if freshness.generation <= baseline_generation
                            || !freshness.initial_build_complete
                            || freshness.rebuilding
                        {
                            return Err(invalid(
                                "atomic paths appeared in an incomplete generation",
                            ));
                        }
                        let versions = response
                            .hits
                            .into_iter()
                            .map(|hit| {
                                let path = hit
                                    .address
                                    .ok_or_else(|| invalid("atomic index hit omitted its address"))?
                                    .path;
                                if !matches!(path.as_str(), PRIMARY_PATH | SECONDARY_PATH)
                                    || hit.object_version == 0
                                {
                                    return Err(invalid(
                                        "atomic index hit had an invalid identity",
                                    ));
                                }
                                Ok((path, hit.object_version))
                            })
                            .collect::<TestResult<BTreeMap<_, _>>>()?;
                        if versions.len() != 2 {
                            return Err(invalid("atomic index returned duplicate paths"));
                        }
                        complete[position] = Some(versions);
                    }
                }
                Err(status) if retryable(&status) => {}
                Err(status) => return Err(status.into()),
            }
        }
        if complete.iter().all(Option::is_some) {
            let expected = complete[0].clone().expect("checked above");
            if complete.iter().flatten().all(|value| value == &expected) {
                return Ok(expected);
            }
            return Err(invalid(
                "query replicas disagreed on the committed atomic path versions",
            ));
        }
        if Instant::now() >= deadline {
            return Err(invalid("atomic index did not publish both committed paths"));
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
                prefix: "atomic/".into(),
                start_after: None,
            })),
        }),
        limit: 100,
        page_token: Vec::new(),
        tenant: String::new(),
    }
}

fn index_client(
    channel: Channel,
    token: &str,
) -> Result<IndexClient, tonic::metadata::errors::InvalidMetadataValue> {
    Ok(IndexServiceClient::with_interceptor(
        channel,
        BearerToken::new(token)?,
    ))
}

fn address(tenant: &str, bucket: &str, path: &str) -> ObjectAddress {
    ObjectAddress {
        tenant: tenant.into(),
        bucket: bucket.into(),
        path: path.into(),
    }
}

fn retryable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::NotFound
            | tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::FailedPrecondition
    )
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
