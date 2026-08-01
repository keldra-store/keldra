# anvil-storage

Thin authenticated Rust transport for the breaking Anvil 0.5 API.

```rust,no_run
use anvil_storage::v1::{HeadObjectRequest, ObjectAddress, object_head};

# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let mut client = anvil_storage::connect("http://127.0.0.1:50051", "secret").await?;
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

The client intentionally adds no domain orchestration. Use one-path CAS,
`BulkWrite`, or a pinned atomic program.

A program definition is an ordinary immutable object at
`_anvil/programs/{name}@{version}`. Write it with `PutObject` or
`PublishObject` and an absent condition. The normal path authorization rules
apply; there is no separate program registry API.
