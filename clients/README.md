# Anvil clients

The official Rust client is published as
[`anvil-storage`](https://crates.io/crates/anvil-storage). It provides
authenticated access to Anvil's object, authorization, administration, bulk,
watch, atomic-program, cluster-wide index, and PersonalDB v0 APIs.

See the [Rust client quickstart](rust/README.md) for a copy-and-paste example.

```sh
cargo test -p anvil-storage
```

Protocol types are generated from the canonical Anvil protobuf API and exposed
through `anvil_storage::v1`.
