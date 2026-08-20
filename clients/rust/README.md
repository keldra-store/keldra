# keldra

The official Rust client for Keldra. It provides authenticated object upload
helpers and the complete generated Keldra 0.10 protocol, including object,
authorization, administration, cluster-wide index, accounting, and PersonalDB
clients.

## Install

```sh
cargo add keldra@0.10.0
cargo add tokio --features macros,rt-multi-thread
```

## Connect and read an object head

Set `ANVIL_ENDPOINT`, `ANVIL_CLIENT_ID`, and `ANVIL_CLIENT_SECRET`, then run:

```rust,no_run
use keldra::v1::{HeadObjectRequest, ObjectAddress, object_head};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = keldra::connect_with_credentials(
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

## Define a typed index safely

Typed JSON fields expose only capabilities valid for their logical type. The
builder below creates exact keyword postings and numeric order doc values; an
invalid combination such as Boolean range search cannot be constructed.

```rust,no_run
use keldra::{KeywordField, SignedIntegerField, TypedJsonIndexBuilder};

# fn definition() -> Result<keldra::v1::CreateIndexRequest, Box<dyn std::error::Error + Send + Sync>> {
let modified = SignedIntegerField::single("modified_at", "/modified_at")
    .range()
    .order();
let newest_first = modified.descending();

let request = TypedJsonIndexBuilder::new("documents", "published")
    .path_prefix("articles/")
    .content_type("application/json")
    .field(KeywordField::single("status", "/status").exact())
    .field(modified)
    .physical_order([newest_first])
    .finish("create-published-index")?;
# let _ = request;
# Ok(())
# }
```

Pass the request to `IndexServiceClient::create_index`. Query hits return the
ordinary object address and exact version; fetch selected source objects with
`GetObject` or `BatchGet`.

The concrete builders are `BooleanField`, `SignedIntegerField`,
`UnsignedIntegerField`, `FloatField`, `KeywordField`, and `TextField`. Their
available methods mirror Keldra's native capabilities: exact matching, keyword
prefix and range matching, numeric ranges, ordering, facets, numeric
aggregates, and analyzed full-text search. A field emits only the index
components required by the methods selected on its builder. Multi-valued
keyword and numeric fields retain source multiplicity for aggregate operations,
while one document contributes at most once to each distinct facet bucket.

## Upload with compare-and-swap

`put_chunks` performs Keldra's `StartPut`, streaming `Put`, and `PutEnd` flow.
The operation below publishes only when version 41 is still current:

```rust,no_run
use keldra::v1::{
    Durability, ObjectAddress, PutHeader, PutIfVersionOperation, put_header,
};

# async fn upload(
#     client: &mut keldra::RawClient,
# ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let receipt = keldra::put_chunks(
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

All generated clients and messages are available under `keldra::v1`.
Use `connect_channel` to share a transport; `object_client`, `index_client`,
`authz_client`, `administration_client`, and `personaldb_client` construct the
authenticated public service clients with the same bearer token.
