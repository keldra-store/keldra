# anvil-storage

Thin authenticated Rust transport for the breaking Anvil 0.5 API.

Production credential exchange must use an endpoint protected by TLS because
the exchange request contains the application's long-lived client secret.
The returned bearer token is valid for one hour.

```rust,no_run
use anvil_storage::v1::{HeadObjectRequest, ObjectAddress, object_head};

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut client = anvil_storage::connect_with_credentials(
    "https://anvil.example.com",
    "acme-reader-client",
    "a-high-entropy-client-secret-from-a-secret-store",
).await?;
let head = client
    .head_object(HeadObjectRequest {
        address: Some(ObjectAddress {
            tenant: "acme".into(),
            bucket: "documents".into(),
            path: "reports/annual.pdf".into(),
        }),
    })
    .await?
    .into_inner();
match head.state {
    Some(object_head::State::Present(present)) => {
        println!("present at version {}", present.version);
    }
    Some(object_head::State::Deleted(deleted)) => {
        println!("deleted at version {}", deleted.version);
    }
    Some(object_head::State::NeverExisted(_)) => println!("never existed"),
    None => return Err("server returned an empty object state".into()),
}
# Ok(())
# }
```

To exchange durable credentials and connect in one step:

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let client = anvil_storage::connect_with_credentials(
    "https://anvil.example.test",
    "developer-defence-client",
    std::env::var("ANVIL_CLIENT_SECRET")?,
)
.await?;
# let _ = client;
# Ok(())
# }
```

The client intentionally adds no domain orchestration. Use one-path CAS,
`BulkWrite`, or a pinned atomic program.

`Durability::Replicated` is a real typed request, never an alias for local
durability. A 0.5.0 node rejects it at `StartPut` with
`DURABILITY_UNAVAILABLE`, before accepting any upload bytes.

Buckets are `UNVERSIONED` by default. Choose `ENABLED` while creating a bucket,
or call the authorized `SetBucketVersioning` administration RPC later. Enabling
is permanent. Only an enabled bucket accepts `GetObjectRequest.version`,
per-item versions in `BatchGet`, `ListObjectVersions`, or `DeleteVersion`.
`ListObjectVersions` streams metadata from oldest to newest without payload
bytes.

`DeleteIfVersion` and `DeleteVersion` are deliberately different operations.
`DeleteIfVersion` compares the current head and publishes a new tombstone;
`DeleteVersion` permanently removes one retained version and does not act as a
CAS shorthand. Removing a non-current version leaves the head unchanged.
Removing the current live version replaces it with a fresh monotonic tombstone,
so an older retained version never becomes current again. The current tombstone
is the path's CAS/ABA fence and cannot be removed; the server returns gRPC
`FAILED_PRECONDITION` with
`CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED` in that case.

`ListObjects` is a unary, stateless page of current live paths. Its prefix is
literal UTF-8, an omitted/zero limit defaults to 100, and the maximum is 1,000.
When `has_more` is true, pass the response's last path back as the exclusive
`start_after` value. A later page is read committed and may observe writes or
deletes committed after the preceding page. Listing reveals child names, so it
requires Zanzibar bucket-wide `get_object` permission rather than an
exact-object grant.

Versioning is managed with the same bearer identity and Zanzibar checks as the
object APIs:

```rust,no_run
use anvil_storage::v1::{ObjectVersioning, SetBucketVersioningRequest};

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let channel = anvil_storage::connect_channel("https://anvil.example.test").await?;
let token = anvil_storage::exchange_client_credentials(
    channel.clone(),
    "developer-defence-client",
    std::env::var("ANVIL_CLIENT_SECRET")?,
)
.await?;
let mut administration =
    anvil_storage::administration_client(channel, &token.access_token)?;
administration
    .set_bucket_versioning(SetBucketVersioningRequest {
        bucket: "documents".into(),
        versioning: ObjectVersioning::Enabled as i32,
    })
    .await?;
# Ok(())
# }
```

Authorization uses the same authenticated channel; no second transport or
static administrator token exists:

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let channel = anvil_storage::connect_channel("https://anvil.example.test").await?;
let token = anvil_storage::exchange_client_credentials(
    channel.clone(),
    "developer-defence-client",
    std::env::var("ANVIL_CLIENT_SECRET")?,
)
.await?;
let mut authorization = anvil_storage::authz_client(channel, &token.access_token)?;
# let _ = &mut authorization;
# Ok(())
# }
```

Object, authorization, and administration clients all use the same 72-MiB
encode/decode bound. The generated raw clients and messages remain available
under `anvil_storage::v1`; these constructors add only bearer authentication
and consistent transport bounds.

A streamed write starts with one typed header, sends only token-bound chunks,
then explicitly publishes the sealed upload with `PutEnd`. The convenience
below also handles a zero-byte object correctly:

`content_type` is optional and limited to 512 UTF-8 bytes. `StartPut` rejects a
larger value before issuing a token or accepting payload bytes. The 64-MiB
`BulkWrite` limit covers the complete encoded operations—including addresses,
conditions, command IDs, content types, and protobuf framing—not just payloads.

```rust,no_run
use anvil_storage::v1::{
    Durability, ObjectAddress, PutHeader, PutIfVersionOperation, put_header,
};

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
# let mut client = anvil_storage::connect("https://anvil.example.test", "access-token").await?;
let receipt = anvil_storage::put_chunks(
    &mut client,
    PutHeader {
        address: Some(ObjectAddress {
            tenant: "acme".into(),
            bucket: "documents".into(),
            path: "reports/annual.pdf".into(),
        }),
        content_type: "application/pdf".into(),
        command_id: "publish-annual-report-2026".into(),
        durability: Durability::Local as i32,
        operation: Some(put_header::Operation::PutIfVersion(
            PutIfVersionOperation { expected_version: 41 },
        )),
    },
    vec![b"first bounded chunk".to_vec(), b"second chunk".to_vec()],
)
.await?;
println!("published version {}", receipt.version);
# Ok(())
# }
```

`exchange_client_credentials` is the only unauthenticated RPC. It sends the
long-lived client secret, so use a TLS endpoint in production. The resulting
bearer token lasts one hour; reconnect or exchange again after it expires.

A program definition is an ordinary immutable object at
`_anvil/programs/{name}@{version}`. Write it with `PutImmutableOperation`
through `StartPut` + `Put` + `PutEnd`. The normal path
authorization rules apply; there is no separate program registry API.
