//! Public S3 gateway qualification using the official AWS SDK for Rust.

use std::env;
use std::error::Error;
use std::io;

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const OBJECT_KEY: &str = "qualification/payload.bin";
const PAYLOAD: &[u8] = b"Keldra official AWS SDK qualification payload\0\x01\x02";

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoints = required("KELDRA_S3_QUALIFICATION_ENDPOINTS")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !matches!(endpoints.len(), 1 | 3) {
        return Err(invalid(
            "S3 qualification requires either one or three gateway endpoints",
        ));
    }
    let client_id = required("KELDRA_S3_QUALIFICATION_CLIENT_ID")?;
    let client_secret = required("KELDRA_S3_QUALIFICATION_CLIENT_SECRET")?;
    let bucket = required("KELDRA_S3_QUALIFICATION_BUCKET")?;
    let clients = endpoints
        .iter()
        .map(|endpoint| s3_client(endpoint, &client_id, &client_secret))
        .collect::<Vec<_>>();
    let count = clients.len();

    clients[0].create_bucket().bucket(&bucket).send().await?;
    clients[1 % count]
        .put_object()
        .bucket(&bucket)
        .key(OBJECT_KEY)
        .content_type("application/octet-stream")
        .body(ByteStream::from_static(PAYLOAD))
        .send()
        .await?;

    let head = clients[2 % count]
        .head_object()
        .bucket(&bucket)
        .key(OBJECT_KEY)
        .send()
        .await?;
    if head.content_length() != Some(PAYLOAD.len() as i64)
        || head.content_type() != Some("application/octet-stream")
    {
        return Err(invalid("HeadObject returned different object metadata"));
    }

    let downloaded = clients[0]
        .get_object()
        .bucket(&bucket)
        .key(OBJECT_KEY)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    if downloaded.as_ref() != PAYLOAD {
        return Err(invalid("GetObject returned different bytes"));
    }

    let listed = clients[1 % count]
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("qualification/")
        .send()
        .await?;
    let keys = listed
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect::<Vec<_>>();
    if keys != [OBJECT_KEY] {
        return Err(invalid(format!("ListObjectsV2 returned {keys:?}")));
    }

    clients[2 % count]
        .delete_object()
        .bucket(&bucket)
        .key(OBJECT_KEY)
        .send()
        .await?;
    if clients[0]
        .head_object()
        .bucket(&bucket)
        .key(OBJECT_KEY)
        .send()
        .await
        .is_ok()
    {
        return Err(invalid("DeleteObject left the object visible"));
    }

    println!(
        "S3 qualification passed on {} gateway endpoint(s): PUT/HEAD/GET/List/Delete",
        endpoints.len()
    );
    Ok(())
}

fn s3_client(endpoint: &str, client_id: &str, client_secret: &str) -> Client {
    let credentials = aws_sdk_s3::config::Credentials::new(
        client_id,
        client_secret,
        None,
        None,
        "keldra-qualification",
    );
    let configuration = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("eu-west-2"))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    Client::from_conf(configuration)
}

fn required(name: &str) -> TestResult<String> {
    env::var(name).map_err(|_| invalid(format!("{name} must be set")))
}

fn invalid(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
