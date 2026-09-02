//! Public-API qualification for an authenticated atomic multi-object program.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use keldra_storage::v1::object_chunk::Value as ObjectChunkValue;
use keldra_storage::v1::object_head::State as ObjectHeadState;
use keldra_storage::v1::put_header::Operation as PutOperationValue;
use keldra_storage::v1::{
    BucketPolicy, CreateBucketRequest, Durability, GetObjectRequest, HeadObjectRequest,
    InvokeProgramRequest, ObjectAddress, ObjectVersioning, PutHeader, PutImmutableOperation,
    SetBucketPolicyRequest,
};
use keldra_storage::{
    RawClient, administration_client, connect_channel, exchange_client_credentials, object_client,
    put_chunks,
};
use serde_json::Value;
use tokio::time::{Instant, sleep};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const PROGRAM_PATH: &str = "_keldra/programs/atomic-program-qualification@1";
const PRIMARY_PATH: &str = "atomic/primary.json";
const SECONDARY_PATH: &str = "atomic/secondary.json";
const REPLICA_WAIT_LIMIT: Duration = Duration::from_secs(90);
const REPLICA_POLL_INTERVAL: Duration = Duration::from_millis(100);
const PROGRAM: &[u8] = br#"{"schema_version":1,"documents":[{"name":"primary","path":{"tenant":"{tenant}","bucket":"{bucket}","path":"atomic/primary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true},{"name":"secondary","path":{"tenant":"{tenant}","bucket":"{bucket}","path":"atomic/secondary.json"},"cardinality":"one","access":"read_write","allow_initial_json":true}],"assertions":[],"operations":[{"kind":"set_value","target":{"document":{"slot":"primary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"primary-committed"}},{"kind":"set_value","target":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"value":{"kind":"literal","value":"secondary-committed"}}],"returns":[{"name":"primary_status","value":{"value":{"document":{"slot":"primary","index":0},"pointer":"/status"},"view":"current"}},{"name":"secondary_status","value":{"value":{"document":{"slot":"secondary","index":0},"pointer":"/status"},"view":"current"}}],"caps":{"max_paths":2,"max_writes":2,"max_operations":4,"max_input_bytes":4096,"max_document_bytes":4096}}"#;

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

    let input = invocation_input(&tenant, &bucket);
    let first_invocation = objects.invoke_program(invocation(
        &tenant,
        &bucket,
        &program_hash,
        input.clone(),
        durability,
    ));
    let visibility = observe_all_or_nothing(channels.clone(), &token, &tenant, &bucket);
    let (first, observed_pairs) = tokio::join!(first_invocation, visibility);
    let first = first?.into_inner();
    let observed_pairs = observed_pairs?;
    if first.replayed {
        return Err(invalid(
            "first atomic invocation unexpectedly reported replay",
        ));
    }
    let first_output = first.output_json.clone();
    assert_output(&first_output)?;
    let first_receipts = receipt_versions(first.path_receipts)?;
    if observed_pairs.iter().any(|pair| pair != &first_receipts) {
        return Err(invalid(
            "atomic visibility observation did not match committed path receipts",
        ));
    }

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

    for channel in channels.iter().skip(1) {
        let mut replica = object_client(channel.clone(), &token)?;
        verify_committed_pair_eventually(&mut replica, &tenant, &bucket, &first_receipts).await?;
    }
    verify_committed_pair_eventually(&mut objects, &tenant, &bucket, &first_receipts).await?;

    println!(
        "atomic-program qualification passed on {} node(s): authenticated multi-object commit and deterministic replay verified",
        endpoints.len()
    );
    Ok(())
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
        r#"{{"bindings":{{"primary":[{{"path":{{"tenant":"{tenant}","bucket":"{bucket}","path":"{PRIMARY_PATH}"}},"template_values":{{}},"expected_head":{{"kind":"absent"}},"initial_json":{{"status":"uncommitted"}}}}],"secondary":[{{"path":{{"tenant":"{tenant}","bucket":"{bucket}","path":"{SECONDARY_PATH}"}},"template_values":{{}},"expected_head":{{"kind":"absent"}},"initial_json":{{"status":"uncommitted"}}}}]}}}}"#
    )
    .into_bytes()
}

fn assert_output(output: &[u8]) -> TestResult<()> {
    let actual: Value = serde_json::from_slice(output)?;
    let expected = serde_json::json!({
        "primary_status": "primary-committed",
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
        || !versions.contains_key(PRIMARY_PATH)
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
            address: Some(address(tenant, bucket, PRIMARY_PATH)),
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
                (PRIMARY_PATH.into(), primary.version),
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
        let expected = if path == PRIMARY_PATH {
            "primary-committed"
        } else {
            "secondary-committed"
        };
        if value.get("status").and_then(Value::as_str) != Some(expected) {
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
