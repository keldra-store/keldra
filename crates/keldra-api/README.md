# keldra-api

Generated Rust messages and gRPC clients for the Keldra 0.17 protocol.

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
