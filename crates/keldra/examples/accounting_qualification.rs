//! Public-API accounting qualification for one- and three-node Docker clusters.

use std::env;
use std::error::Error;
use std::io;
use std::time::Duration;

use keldra_storage::v1::accounting_service_client::AccountingServiceClient;
use keldra_storage::v1::object_chunk::Value as ObjectChunkValue;
use keldra_storage::v1::put_header::Operation as PutOperationValue;
use keldra_storage::v1::{
    AccountingMeasurementState, CreateBucketRequest, DeleteRequest, DeleteVersionRequest,
    DisableAccountingRequest, Durability, EnableAccountingRequest, GetAccountingRequest,
    GetObjectRequest, LinkObjectRequest, MutationReceipt, ObjectAddress, ObjectVersioning,
    PutHeader, PutOperation, UnlinkObjectRequest,
};
use keldra_storage::{
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
    let endpoints = required("KELDRA_ACCOUNTING_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "accounting qualification requires either one or three endpoints",
        ));
    }
    let tenant = required("KELDRA_ACCOUNTING_QUALIFICATION_TENANT")?;
    let bucket = required("KELDRA_ACCOUNTING_QUALIFICATION_BUCKET")?;
    let client_id = required("KELDRA_ACCOUNTING_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("KELDRA_ACCOUNTING_QUALIFICATION_CLIENT_SECRET")?;

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
    let node_count = objects.len();

    let bucket_definition = enable(&mut accounting[0], &bucket, "", "unversioned-bucket").await?;
    let prefix_definition = enable(
        &mut accounting[0],
        &bucket,
        "billable",
        "unversioned-prefix",
    )
    .await?;

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
    wait_for_retained_non_billable(&mut accounting, &bucket, "billable", expected_bytes).await?;
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

    let alias_target = ObjectAddress {
        tenant: tenant.clone(),
        bucket: bucket.clone(),
        path: "archive/alias-target.bin".into(),
    };
    let alias = ObjectAddress {
        tenant: tenant.clone(),
        bucket: bucket.clone(),
        path: "billable/alias.bin".into(),
    };
    put_payload(
        &mut objects[0],
        alias_target.clone(),
        b"0123456789",
        "accounting-qualification-alias-put-10",
    )
    .await?;
    link(
        &mut objects[1 % node_count],
        alias.clone(),
        alias_target.clone(),
        "accounting-qualification-alias-link",
    )
    .await?;
    wait_for(
        &mut accounting,
        &bucket,
        "billable",
        1,
        10,
        expected_bytes,
        expected_bytes,
    )
    .await?;
    wait_for(
        &mut accounting,
        &bucket,
        "",
        2,
        20,
        expected_bytes + 10,
        expected_bytes,
    )
    .await?;

    put_payload(
        &mut objects[2 % node_count],
        alias_target.clone(),
        b"01234567890123456789",
        "accounting-qualification-alias-put-20",
    )
    .await?;
    wait_for(
        &mut accounting,
        &bucket,
        "billable",
        1,
        20,
        expected_bytes,
        expected_bytes,
    )
    .await?;
    wait_for(
        &mut accounting,
        &bucket,
        "",
        2,
        40,
        expected_bytes + 30,
        expected_bytes,
    )
    .await?;

    unlink(
        &mut objects[0],
        alias,
        "accounting-qualification-alias-unlink",
    )
    .await?;
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
        1,
        20,
        expected_bytes + 30,
        expected_bytes,
    )
    .await?;

    let delete_command_id = "accounting-qualification-alias-delete";
    let deleted = objects[1 % node_count]
        .delete(DeleteRequest {
            address: Some(alias_target),
            command_id: delete_command_id.into(),
            durability: Durability::Local as i32,
        })
        .await?
        .into_inner();
    require_receipt(&deleted, delete_command_id, true)?;
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
        expected_bytes + 30,
        expected_bytes,
    )
    .await?;

    disable(
        &mut accounting[0],
        &bucket,
        "billable",
        prefix_definition.version,
        "unversioned-prefix",
    )
    .await?;
    disable(
        &mut accounting[0],
        &bucket,
        "",
        bucket_definition.version,
        "unversioned-bucket",
    )
    .await?;
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

    let versioned_bucket = format!("{bucket}-versioned");
    administrator
        .create_bucket(CreateBucketRequest {
            bucket: versioned_bucket.clone(),
            versioning: ObjectVersioning::Enabled as i32,
        })
        .await?;
    let versioned_bucket_definition = enable(
        &mut accounting[0],
        &versioned_bucket,
        "",
        "versioned-bucket",
    )
    .await?;
    let versioned_prefix_definition = enable(
        &mut accounting[0],
        &versioned_bucket,
        "billable",
        "versioned-prefix",
    )
    .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    wait_for(&mut accounting, &versioned_bucket, "", 0, 0, 0, 0).await?;
    wait_for(&mut accounting, &versioned_bucket, "billable", 0, 0, 0, 0).await?;

    let retained_target = ObjectAddress {
        tenant: tenant.clone(),
        bucket: versioned_bucket.clone(),
        path: "archive/retained-target.bin".into(),
    };
    let retained_alias = ObjectAddress {
        tenant,
        bucket: versioned_bucket.clone(),
        path: "billable/retained-alias.bin".into(),
    };
    let first_version = put_payload(
        &mut objects[0],
        retained_target.clone(),
        b"0123456789",
        "accounting-qualification-retained-put-10",
    )
    .await?;
    let second_version = put_payload(
        &mut objects[1 % node_count],
        retained_target.clone(),
        b"01234567890123456789",
        "accounting-qualification-retained-put-20",
    )
    .await?;
    if second_version <= first_version {
        return Err(invalid("versioned puts did not advance the object version"));
    }
    link(
        &mut objects[2 % node_count],
        retained_alias.clone(),
        retained_target.clone(),
        "accounting-qualification-retained-link",
    )
    .await?;
    wait_for(&mut accounting, &versioned_bucket, "billable", 1, 30, 0, 0).await?;
    wait_for(&mut accounting, &versioned_bucket, "", 2, 60, 30, 0).await?;

    let deleted_non_current = objects[0]
        .delete_version(DeleteVersionRequest {
            address: Some(retained_alias.clone()),
            version: first_version,
            durability: Durability::Local as i32,
        })
        .await?
        .into_inner();
    if !deleted_non_current.deleted || deleted_non_current.replacement_tombstone_version.is_some() {
        return Err(invalid(
            "DeleteVersion through the alias did not remove only the retained version",
        ));
    }
    wait_for(&mut accounting, &versioned_bucket, "billable", 1, 20, 0, 0).await?;
    wait_for(&mut accounting, &versioned_bucket, "", 2, 40, 30, 0).await?;

    unlink(
        &mut objects[1 % node_count],
        retained_alias,
        "accounting-qualification-retained-unlink",
    )
    .await?;
    wait_for(&mut accounting, &versioned_bucket, "billable", 0, 0, 0, 0).await?;
    wait_for(&mut accounting, &versioned_bucket, "", 1, 20, 30, 0).await?;

    let deleted_current = objects[2 % node_count]
        .delete_version(DeleteVersionRequest {
            address: Some(retained_target),
            version: second_version,
            durability: Durability::Local as i32,
        })
        .await?
        .into_inner();
    if !deleted_current.deleted || deleted_current.replacement_tombstone_version.is_none() {
        return Err(invalid(
            "DeleteVersion did not replace the current retained version with a tombstone",
        ));
    }
    wait_for(&mut accounting, &versioned_bucket, "billable", 0, 0, 0, 0).await?;
    wait_for(&mut accounting, &versioned_bucket, "", 0, 0, 30, 0).await?;

    disable(
        &mut accounting[0],
        &versioned_bucket,
        "billable",
        versioned_prefix_definition.version,
        "versioned-prefix",
    )
    .await?;
    disable(
        &mut accounting[0],
        &versioned_bucket,
        "",
        versioned_bucket_definition.version,
        "versioned-bucket",
    )
    .await?;

    println!(
        "accounting qualification passed on {} node(s): {expected_bytes} ordinary payload bytes plus alias and retained-lineage accounting",
        endpoints.len()
    );
    Ok(())
}

async fn put_payload(
    client: &mut RawClient,
    address: ObjectAddress,
    bytes: &[u8],
    command_id: &str,
) -> TestResult<u64> {
    let receipt = put_chunks(
        client,
        PutHeader {
            address: Some(address),
            content_type: "application/octet-stream".into(),
            command_id: command_id.into(),
            durability: Durability::Local as i32,
            operation: Some(PutOperationValue::Put(PutOperation {})),
        },
        [bytes.to_vec()],
    )
    .await?;
    require_receipt(&receipt, command_id, false)?;
    Ok(receipt.version)
}

async fn link(
    client: &mut RawClient,
    alias: ObjectAddress,
    target: ObjectAddress,
    command_id: &str,
) -> TestResult<()> {
    let receipt = client
        .link_object(LinkObjectRequest {
            link: Some(alias),
            target: Some(target),
            command_id: command_id.into(),
            durability: Durability::Local as i32,
        })
        .await?
        .into_inner();
    require_receipt(&receipt, command_id, false)
}

async fn unlink(client: &mut RawClient, alias: ObjectAddress, command_id: &str) -> TestResult<()> {
    let receipt = client
        .unlink_object(UnlinkObjectRequest {
            link: Some(alias),
            command_id: command_id.into(),
            durability: Durability::Local as i32,
        })
        .await?
        .into_inner();
    require_receipt(&receipt, command_id, true)
}

fn require_receipt(receipt: &MutationReceipt, command_id: &str, deleted: bool) -> TestResult<()> {
    if receipt.command_id != command_id
        || receipt.version == 0
        || receipt.deleted != deleted
        || receipt.replay_guarantee_expires_at.is_none()
    {
        return Err(invalid(format!(
            "mutation returned an invalid receipt for {command_id}"
        )));
    }
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
) -> TestResult<keldra_storage::v1::AccountingDefinition> {
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
    suffix: &str,
) -> TestResult<()> {
    let response = client
        .disable_accounting(DisableAccountingRequest {
            bucket: bucket.into(),
            path_prefix: prefix.into(),
            expected_version,
            command_id: format!("accounting-qualification-disable-{suffix}"),
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
                    let logical = snapshot.logical.as_ref();
                    let traffic = snapshot.traffic.as_ref();
                    let matches = logical.is_some_and(|usage| {
                        usage
                            .visible_file_count
                            .as_ref()
                            .is_some_and(|measurement| {
                                measurement.state == AccountingMeasurementState::Present as i32
                                    && measurement.count == object_count
                            })
                            && usage
                                .billable_logical_bytes
                                .as_ref()
                                .is_some_and(|measurement| {
                                    measurement.state == AccountingMeasurementState::Present as i32
                                        && measurement.bytes == logical_bytes
                                })
                            && usage
                                .retained_non_billable_logical_bytes
                                .as_ref()
                                .is_some_and(|measurement| {
                                    measurement.state == AccountingMeasurementState::Present as i32
                                })
                            && usage.freshness.as_ref().is_some_and(|value| value.complete)
                    }) && traffic.is_some_and(|usage| {
                        usage.accepted_inbound_bytes >= minimum_inbound
                            && usage.served_outbound_bytes >= minimum_outbound
                    }) && snapshot.tenant_physical_bytes.as_ref().is_some_and(
                        |measurement| {
                            measurement.state == AccountingMeasurementState::Unsupported as i32
                        },
                    );
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

async fn wait_for_retained_non_billable(
    clients: &mut [AccountingClient],
    bucket: &str,
    prefix: &str,
    expected_bytes: u64,
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
                    let matches = snapshot.logical.as_ref().is_some_and(|usage| {
                        usage
                            .retained_non_billable_logical_bytes
                            .as_ref()
                            .is_some_and(|measurement| {
                                measurement.state == AccountingMeasurementState::Present as i32
                                    && measurement.bytes == expected_bytes
                            })
                            && usage
                                .freshness
                                .as_ref()
                                .is_some_and(|freshness| freshness.complete)
                    });
                    if !matches {
                        last = format!("latest retained accounting snapshot: {snapshot:?}");
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
            return Err(invalid(format!(
                "retained non-billable accounting did not converge: {last}"
            )));
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
