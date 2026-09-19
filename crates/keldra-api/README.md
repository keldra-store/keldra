# keldra-api

Generated Rust messages and gRPC clients for the Keldra 0.18 v1 protocol.

Most applications should depend on
[`keldra`](https://crates.io/crates/keldra), which adds
authenticated client constructors and upload helpers. Use `keldra-api` directly
when integrating the generated protocol types with a custom transport.

```rust
use keldra_api::v1::{HeadObjectRequest, ObjectAddress};

let request = HeadObjectRequest {
    address: Some(ObjectAddress {
        tenant: "example".into(),
        bucket: "documents".into(),
        path: "reports/annual.pdf".into(),
    }),
};

assert_eq!(request.address.unwrap().path, "reports/annual.pdf");
```

Object mutations default to `IndexingIntent::Standard`. A caller may select
`IndexingIntent::Realtime` on an individual mutation; its successful receipt
then carries an opaque, expiring `IndexVisibilityToken`. Supply one or more of
those tokens in `QueryIndexRequest.required_visibility_tokens` to wait for the
named mutations without claiming complete-prefix freshness. Mutation RPCs do
not wait for query visibility, so a later query deadline cannot hide a
successful durable write.
