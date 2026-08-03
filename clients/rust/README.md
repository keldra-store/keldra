# anvil-storage

The official Rust client for Anvil. It provides authenticated object upload
helpers and the complete generated Anvil 0.5 protocol, including object,
authorization, administration, cluster-wide index, and PersonalDB v0 clients.

## Install

```sh
cargo add anvil-storage@0.5.2
cargo add tokio --features macros,rt-multi-thread
```

## Connect and read an object head

Set `ANVIL_ENDPOINT`, `ANVIL_CLIENT_ID`, and `ANVIL_CLIENT_SECRET`, then run:

```rust,no_run
use anvil_storage::v1::{HeadObjectRequest, ObjectAddress, object_head};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = anvil_storage::connect_with_credentials(
        std::env::var("ANVIL_ENDPOINT")?,
        std::env::var("ANVIL_CLIENT_ID")?,
        std::env::var("ANVIL_CLIENT_SECRET")?,
    )
    .await?;

    let head = client
        .head_object(HeadObjectRequest {
            address: Some(ObjectAddress {
                tenant: "example".into(),
                bucket: "documents".into(),
                path: "reports/annual.pdf".into(),
            }),
        })
        .await?
        .into_inner();

    match head.state {
        Some(object_head::State::Present(object)) => {
            println!("object version: {}", object.version);
        }
        Some(object_head::State::Deleted(object)) => {
            println!("deleted at version: {}", object.version);
        }
        Some(object_head::State::NeverExisted(_)) => println!("object does not exist"),
        None => println!("no object state returned"),
    }

    Ok(())
}
```

Use an HTTPS endpoint when exchanging client credentials.

## Upload with compare-and-swap

`put_chunks` performs Anvil's `StartPut`, streaming `Put`, and `PutEnd` flow.
The operation below publishes only when version 41 is still current:

```rust,no_run
use anvil_storage::v1::{
    Durability, ObjectAddress, PutHeader, PutIfVersionOperation, put_header,
};

# async fn upload(
#     client: &mut anvil_storage::RawClient,
# ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let receipt = anvil_storage::put_chunks(
    client,
    PutHeader {
        address: Some(ObjectAddress {
            tenant: "example".into(),
            bucket: "documents".into(),
            path: "reports/annual.pdf".into(),
        }),
        content_type: "application/pdf".into(),
        command_id: "publish-annual-report".into(),
        durability: Durability::Local as i32,
        operation: Some(put_header::Operation::PutIfVersion(
            PutIfVersionOperation {
                expected_version: 41,
            },
        )),
    },
    vec![b"first chunk".to_vec(), b"second chunk".to_vec()],
)
.await?;

println!("published version: {}", receipt.version);
# Ok(())
# }
```

All generated clients and messages are available under `anvil_storage::v1`,
including `index_service_client::IndexServiceClient` and
`personal_db_service_client::PersonalDbServiceClient`. Use `connect_channel`
to share a transport and the public `BearerToken` interceptor to authenticate
those generated clients. `object_client`, `authz_client`, and
`administration_client` provide concise constructors for the core services.
