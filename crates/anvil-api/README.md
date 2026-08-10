# anvil-api

Generated Rust messages and gRPC clients for the Anvil 0.7 protocol.

Most applications should depend on
[`anvil-storage`](https://crates.io/crates/anvil-storage), which adds
authenticated client constructors and upload helpers. Use `anvil-api` directly
when integrating the generated protocol types with a custom transport.

```rust
use anvil_api::v1::{HeadObjectRequest, ObjectAddress};

let request = HeadObjectRequest {
    address: Some(ObjectAddress {
        tenant: "example".into(),
        bucket: "documents".into(),
        path: "reports/annual.pdf".into(),
    }),
};

assert_eq!(request.address.unwrap().path, "reports/annual.pdf");
```
