# Keldra clients

The official Rust client is published as
[`keldra`](https://crates.io/crates/keldra). It provides
authenticated access to Keldra's object, authorization, administration, bulk,
watch, atomic-program, cluster-wide index, accounting, and PersonalDB APIs.

See the [Rust client quickstart](rust/README.md) for a copy-and-paste example.

```sh
cargo test -p keldra
```

Protocol types are generated from the canonical Keldra protobuf API and exposed
through `keldra::v1`.
