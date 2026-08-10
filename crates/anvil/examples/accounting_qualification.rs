//! Public-API accounting qualification for one- and three-node Docker clusters.

use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use anvil_storage::v1::accounting_service_client::AccountingServiceClient;
use anvil_storage::v1::object_chunk::Value as ObjectChunkValue;
use anvil_storage::v1::put_header::Operation as PutOperationValue;
use anvil_storage::v1::{
    CreateBucketRequest, DeleteRequest, DisableAccountingRequest, Durability,
    EnableAccountingRequest, GetAccountingRequest, GetObjectRequest, ObjectAddress,
    ObjectVersioning, PutHeader, PutOperation,
};
use anvil_storage::{
    BearerToken, RawClient, administration_client, connect_channel, exchange_client_credentials,
    object_client, put_chunks,
};
use tokio::time::Instant;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
type AccountingClient = AccountingServiceClient<InterceptedService<Channel, BearerToken>>;

const WAIT_LIMIT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("ANVIL_ACCOUNTING_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "accounting qualification requires either one or three endpoints",
        ));
    }
    let tenant = required("ANVIL_ACCOUNTING_QUALIFICATION_TENANT")?;
    let bucket = required("ANVIL_ACCOUNTING_QUALIFICATION_BUCKET")?;
    let client_id = required("ANVIL_ACCOUNTING_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("ANVIL_ACCOUNTING_QUALIFICATION_CLIENT_SECRET")?;

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

    let mut accounting = channels
        .iter()
        .cloned()
        .map(|channel| accounting_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;
    let mut objects = channels
        .iter()
        .cloned()
        .map(|channel| object_client(channel, &token))
        .collect::<Result<Vec<_>, _>>()?;

    let bucket_definition = enable(&mut accounting[0], &bucket, "", "bucket").await?;
    let prefix_definition = enable(&mut accounting[0], &bucket, "billable", "prefix").await?;

    // Definition objects are authoritative immediately; each node discovers
    // the disposable traffic-meter assignment asynchronously.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Do not race the worker's cold current-head baseline. A complete zero
    // rollup proves both definitions have established their journal boundary;
    // every following object transition must then be applied incrementally.
    wait_for(&mut accounting, &bucket, "", 0, 0, 0, 0).await?;
    wait_for(&mut accounting, &bucket, "billable", 0, 0, 0, 0).await?;

    let mut expected_bytes = 0_u64;
    let mut addresses = Vec::new();
    for index in 0..objects.len() {
        let bytes = format!("accounting qualification payload {index}").into_bytes();
        expected_bytes += bytes.len() as u64;
        let address = ObjectAddress {
            tenant: tenant.clone(),
            bucket: bucket.clone(),
            path: format!("billable/node-{index}.bin"),
        };
        put_chunks(
            &mut objects[index],
            PutHeader {
                address: Some(address.clone()),
                content_type: "application/octet-stream".into(),
                command_id: format!("accounting-qualification-put-{index}"),
                durability: Durability::Local as i32,
                operation: Some(PutOperationValue::Put(PutOperation {})),
            },
            [bytes.clone()],
        )
        .await?;
        let reader = (index + 1) % objects.len();
        let returned = read_all(&mut objects[reader], address.clone()).await?;
        if returned != bytes {
            return Err(invalid("GetObject returned different qualification bytes"));
        }
        addresses.push(address);
    }

    wait_for(
        &mut accounting,
        &bucket,
        "billable",
        objects.len() as u64,
        expected_bytes,
        expected_bytes,
        expected_bytes,
    )
    .await?;

    for (index, address) in addresses.into_iter().enumerate() {
        let writer = (index + 2) % objects.len();
        objects[writer]
            .delete(DeleteRequest {
                address: Some(address),
                command_id: format!("accounting-qualification-delete-{index}"),
                durability: Durability::Local as i32,
            })
            .await?;
    }
    wait_for(
        &mut accounting,
        &bucket,
        "billable",
        0,
        0,
        expected_bytes,
        expected_bytes,
    )
    .await?;
    wait_for(
        &mut accounting,
        &bucket,
        "",
        0,
        0,
        expected_bytes,
        expected_bytes,
    )
    .await?;

    disable(
        &mut accounting[0],
        &bucket,
        "billable",
        prefix_definition.version,
    )
    .await?;
    disable(&mut accounting[0], &bucket, "", bucket_definition.version).await?;
    let error = accounting[0]
        .get_accounting(GetAccountingRequest {
            bucket: bucket.clone(),
            path_prefix: "billable".into(),
        })
        .await
        .unwrap_err();
    if error.code() != Code::NotFound {
        return Err(invalid(format!(
            "disabled accounting returned {:?}, expected NOT_FOUND",
            error.code()
        )));
    }

    println!(
        "accounting qualification passed on {} node(s): {expected_bytes} payload bytes",
        endpoints.len()
    );
    Ok(())
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

async fn enable(
    client: &mut AccountingClient,
    bucket: &str,
    prefix: &str,
    suffix: &str,
) -> TestResult<anvil_storage::v1::AccountingDefinition> {
    let definition = client
        .enable_accounting(EnableAccountingRequest {
            bucket: bucket.into(),
            path_prefix: prefix.into(),
            command_id: format!("accounting-qualification-enable-{suffix}"),
        })
        .await?
        .into_inner();
    if definition.accounting_id == 0 || definition.version == 0 {
        return Err(invalid("EnableAccounting returned an invalid identity"));
    }
    Ok(definition)
}

async fn disable(
    client: &mut AccountingClient,
    bucket: &str,
    prefix: &str,
    expected_version: u64,
) -> TestResult<()> {
    let response = client
        .disable_accounting(DisableAccountingRequest {
            bucket: bucket.into(),
            path_prefix: prefix.into(),
            expected_version,
            command_id: format!(
                "accounting-qualification-disable-{}",
                if prefix.is_empty() {
                    "bucket"
                } else {
                    "prefix"
                }
            ),
        })
        .await?
        .into_inner();
    if !response.disabled || response.tombstone_version == 0 {
        return Err(invalid("DisableAccounting returned an invalid outcome"));
    }
    Ok(())
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

#[allow(clippy::too_many_arguments)]
async fn wait_for(
    clients: &mut [AccountingClient],
    bucket: &str,
    prefix: &str,
    object_count: u64,
    logical_bytes: u64,
    minimum_inbound: u64,
    minimum_outbound: u64,
) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    let mut last = String::new();
    loop {
        let mut complete = true;
        for client in clients.iter_mut() {
            match client
                .get_accounting(GetAccountingRequest {
                    bucket: bucket.into(),
                    path_prefix: prefix.into(),
                })
                .await
            {
                Ok(response) => {
                    let snapshot = response.into_inner();
                    let matches = snapshot.object_count == object_count
                        && snapshot.logical_stored_bytes == logical_bytes
                        && snapshot.accepted_inbound_bytes >= minimum_inbound
                        && snapshot.served_outbound_bytes >= minimum_outbound
                        && snapshot
                            .freshness
                            .as_ref()
                            .is_some_and(|value| value.complete);
                    if !matches {
                        last = format!("latest accounting snapshot: {snapshot:?}");
                        complete = false;
                    }
                }
                Err(status) if retryable(&status) => {
                    last = status.to_string();
                    complete = false;
                }
                Err(status) => return Err(status.into()),
            }
        }
        if complete {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(invalid(format!("accounting did not converge: {last}")));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled | Code::NotFound
    )
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
