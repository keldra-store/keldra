//! Public-API qualification for Zanzibar-governed anonymous bucket reads.

use std::env;
use std::error::Error;
use std::io;

use keldra_storage::v1::batch_get_outcome::Outcome as BatchOutcome;
use keldra_storage::v1::object_chunk::Value as ChunkValue;
use keldra_storage::v1::object_head::State as HeadState;
use keldra_storage::v1::object_service_client::ObjectServiceClient;
use keldra_storage::v1::put_header::Operation as PutOperationValue;
use keldra_storage::v1::{
    BatchGetRequest, CreateBucketRequest, Durability, GetObjectRequest, HeadObjectRequest,
    ListObjectVersionsRequest, ListObjectsRequest, ObjectAddress, ObjectVersioning, PutHeader,
    PutOperation, SetBucketPublicReadRequest,
};
use keldra_storage::{
    administration_client, connect_channel, exchange_client_credentials, object_client, put_chunks,
};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const CONTENT: &[u8] = b"public qualification payload";

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("ANVIL_PUBLIC_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(invalid("at least one qualification endpoint is required"));
    }
    let tenant = required("ANVIL_PUBLIC_QUALIFICATION_TENANT")?;
    let bucket = required("ANVIL_PUBLIC_QUALIFICATION_BUCKET")?;
    let client_id = required("ANVIL_PUBLIC_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("ANVIL_PUBLIC_QUALIFICATION_CLIENT_SECRET")?;
    let path = "public/readable.txt";
    let address = ObjectAddress {
        tenant: tenant.clone(),
        bucket: bucket.clone(),
        path: path.into(),
    };

    let channels = connect_all(&endpoints).await?;
    let token = exchange_client_credentials(channels[0].clone(), client_id, client_secret)
        .await?
        .access_token;
    let mut administrator = administration_client(channels[0].clone(), &token)?;
    let mut owner = object_client(channels[0].clone(), &token)?;

    administrator
        .create_bucket(CreateBucketRequest {
            bucket: bucket.clone(),
            versioning: ObjectVersioning::Enabled as i32,
        })
        .await?;
    let receipt = put_chunks(
        &mut owner,
        PutHeader {
            address: Some(address.clone()),
            content_type: "text/plain".into(),
            command_id: format!("public-qualification-put-{bucket}"),
            durability: Durability::Local as i32,
            operation: Some(PutOperationValue::Put(PutOperation {})),
        },
        [CONTENT.to_vec()],
    )
    .await?;

    assert_status(
        public(channels[0].clone())
            .head_object(HeadObjectRequest {
                address: Some(address.clone()),
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "private anonymous head",
    )?;

    administrator
        .set_bucket_public_read(SetBucketPublicReadRequest {
            bucket: bucket.clone(),
            enabled: true,
        })
        .await?;

    for channel in &channels {
        qualify_public_reads(channel.clone(), &tenant, &bucket, &address, receipt.version).await?;
    }
    qualify_invalid_bearer(channels[0].clone(), &address).await?;
    qualify_anonymous_write_denial(channels[0].clone(), &address).await?;

    administrator
        .set_bucket_public_read(SetBucketPublicReadRequest {
            bucket,
            enabled: false,
        })
        .await?;
    assert_status(
        public(channels[0].clone())
            .head_object(HeadObjectRequest {
                address: Some(address),
            })
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "revoked anonymous head",
    )?;

    println!(
        "public-read qualification passed on {} endpoint(s)",
        channels.len()
    );
    Ok(())
}

async fn connect_all(endpoints: &[String]) -> TestResult<Vec<Channel>> {
    let mut channels = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        channels.push(connect_channel(endpoint).await?);
    }
    Ok(channels)
}

async fn qualify_public_reads(
    channel: Channel,
    tenant: &str,
    bucket: &str,
    address: &ObjectAddress,
    version: u64,
) -> TestResult<()> {
    let mut client = public(channel);
    let head = client
        .head_object(HeadObjectRequest {
            address: Some(address.clone()),
        })
        .await?
        .into_inner();
    match head.state {
        Some(HeadState::Present(value))
            if value.version == version && value.content_length == CONTENT.len() as u64 => {}
        other => return Err(invalid(format!("unexpected public head: {other:?}"))),
    }

    let mut body = client
        .get_object(GetObjectRequest {
            address: Some(address.clone()),
            version: None,
        })
        .await?
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.message().await? {
        if let Some(ChunkValue::Bytes(chunk)) = chunk.value {
            bytes.extend_from_slice(&chunk);
        }
    }
    if bytes != CONTENT {
        return Err(invalid("public GetObject returned different bytes"));
    }

    let listed = client
        .list_objects(ListObjectsRequest {
            tenant: tenant.into(),
            bucket: bucket.into(),
            prefix: "public/".into(),
            start_after: None,
            limit: 100,
        })
        .await?
        .into_inner();
    if listed.paths != [address.path.clone()] {
        return Err(invalid("public ListObjects omitted the readable path"));
    }

    let versions = client
        .list_object_versions(ListObjectVersionsRequest {
            address: Some(address.clone()),
        })
        .await?
        .into_inner()
        .message()
        .await?;
    if versions.is_none() {
        return Err(invalid("public ListObjectVersions returned no version"));
    }

    let batch = client
        .batch_get(BatchGetRequest {
            objects: vec![GetObjectRequest {
                address: Some(address.clone()),
                version: None,
            }],
        })
        .await?
        .into_inner();
    if !matches!(
        batch.outcomes.as_slice(),
        [outcome] if matches!(outcome.outcome.as_ref(), Some(BatchOutcome::Object(_)))
    ) {
        return Err(invalid("public BatchGet did not return the object"));
    }
    Ok(())
}

async fn qualify_invalid_bearer(channel: Channel, address: &ObjectAddress) -> TestResult<()> {
    let mut request = Request::new(HeadObjectRequest {
        address: Some(address.clone()),
    });
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer invalid-public-token")?,
    );
    let error = public(channel).head_object(request).await.unwrap_err();
    assert_status(error.code(), Code::Unauthenticated, "invalid bearer")
}

async fn qualify_anonymous_write_denial(
    channel: Channel,
    address: &ObjectAddress,
) -> TestResult<()> {
    let error = public(channel)
        .start_put(PutHeader {
            address: Some(address.clone()),
            content_type: "text/plain".into(),
            command_id: "anonymous-write-must-fail".into(),
            durability: Durability::Local as i32,
            operation: Some(PutOperationValue::Put(PutOperation {})),
        })
        .await
        .unwrap_err();
    assert_status(error.code(), Code::Unauthenticated, "anonymous StartPut")
}

fn public(channel: Channel) -> ObjectServiceClient<Channel> {
    ObjectServiceClient::new(channel)
        .max_encoding_message_size(72 * 1024 * 1024)
        .max_decoding_message_size(72 * 1024 * 1024)
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

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
