# keldra

The official Rust client for Keldra. It provides authenticated object upload
helpers and the complete generated Keldra 0.15 protocol, including object,
authorization, administration, cluster-wide index, accounting, and PersonalDB
clients.

## Install

```sh
cargo add keldra@0.15.0
cargo add tokio --features macros,rt-multi-thread
```

## Connect and read an object head

Set `KELDRA_ENDPOINT`, `KELDRA_CLIENT_ID`, and `KELDRA_CLIENT_SECRET`, then run:

```rust,no_run
use keldra::v1::{HeadObjectRequest, ObjectAddress, object_head};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = keldra::connect_with_credentials(
        std::env::var("KELDRA_ENDPOINT")?,
        std::env::var("KELDRA_CLIENT_ID")?,
        std::env::var("KELDRA_CLIENT_SECRET")?,
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

## Clone bytes or link a mutable name

`CloneObject` creates an independent destination version which initially shares
the source version's existing payload bytes. `LinkObject` instead creates a
transparent name for the same mutable target: writes through either path update
the target, and unlinking removes only the link.

```rust,no_run
use keldra::v1::{
    CloneObjectRequest, Durability, LinkObjectRequest, ObjectAddress,
    PutIfAbsentOperation, UnlinkObjectRequest, clone_object_request,
};

# async fn aliases(client: &mut keldra::RawClient) -> Result<(), tonic::Status> {
let target = ObjectAddress {
    tenant: "example".into(),
    bucket: "documents".into(),
    path: "reports/annual.pdf".into(),
};
let clone = ObjectAddress {
    path: "archives/annual.pdf".into(),
    ..target.clone()
};
client.clone_object(CloneObjectRequest {
    source: Some(target.clone()),
    source_version: 41,
    destination: Some(clone),
    command_id: "clone-annual-41".into(),
    durability: Durability::Replicated as i32,
    operation: Some(clone_object_request::Operation::PutIfAbsent(
        PutIfAbsentOperation {},
    )),
}).await?;

let link = ObjectAddress {
    path: "reports/latest.pdf".into(),
    ..target.clone()
};
client.link_object(LinkObjectRequest {
    link: Some(link.clone()),
    target: Some(target),
    command_id: "link-latest".into(),
    durability: Durability::Replicated as i32,
}).await?;
client.unlink_object(UnlinkObjectRequest {
    link: Some(link),
    command_id: "unlink-latest".into(),
    durability: Durability::Replicated as i32,
}).await?;
# Ok(())
# }
```

Clone and link require cluster protocol/storage capability `2/2`. Operators must
start 0.15 on fresh authoritative volumes and complete the documented explicit
capability activation before applications invoke them.

## Define a typed index safely

Typed JSON fields expose only capabilities valid for their logical type. The
builder below creates exact keyword postings and numeric order doc values; an
invalid combination such as Boolean range search cannot be constructed.

```rust,no_run
use keldra::{DateField, KeywordField, TypedJsonIndexBuilder};

# fn definition() -> Result<keldra::v1::CreateIndexRequest, Box<dyn std::error::Error + Send + Sync>> {
let modified = DateField::single("modified_at", "/modified_at")
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
`UnsignedIntegerField`, `FloatField`, `KeywordField`, `TextField`, and
`DateField`. Date fields use ISO-8601 by default; pass a pattern created by
`DateFormat::strftime(...)` to `DateField::format(...)` for one validated custom
format. Offset-less inputs are UTC and precision finer than milliseconds is
rejected. Date exact/range/order/facet operations use signed Unix epoch
milliseconds internally, while facet values are returned in the configured
format without storing source strings. Their
available methods mirror Keldra's native capabilities: exact matching, keyword
prefix and range matching, numeric ranges, ordering, facets, numeric
aggregates, and analyzed full-text search. A field emits only the index
components required by the methods selected on its builder. Multi-valued
keyword and numeric fields retain source multiplicity for aggregate operations,
while one document contributes at most once to each distinct facet bucket.

## Build bounded Boolean queries

`PredicateExpression` encodes canonical typed scalar values and constructs only
non-empty Boolean operators within Keldra's public 32-level/256-node bounds.
Facets, aggregates, ordering, pagination, and freshness remain ordinary public
query options and apply to the complete authorized Boolean match set.

```rust,no_run
use keldra::PredicateExpression;
use keldra::v1::{
    IndexAggregateOperation, IndexAggregateRequest, IndexFacetRequest,
    TypedJsonIndexQuery,
};

# fn query() -> Result<TypedJsonIndexQuery, Box<dyn std::error::Error + Send + Sync>> {
let runnable = PredicateExpression::all([
    PredicateExpression::any([
        PredicateExpression::equal("status", "pending")?,
        PredicateExpression::equal("status", "retryable")?,
    ])?,
    PredicateExpression::less_than_or_equal("due_at", 1_800_000_000_u64)?,
    PredicateExpression::exists("deleted_at")?.negated()?,
])?;

let query = TypedJsonIndexQuery {
    predicate: Some(runnable.into_proto()),
    order: vec![],
    facets: vec![IndexFacetRequest {
        field: "status".into(),
        limit: 10,
    }],
    aggregates: vec![IndexAggregateRequest {
        field: "duration".into(),
        operation: IndexAggregateOperation::Average as i32,
    }],
};
# Ok(query)
# }
```

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
