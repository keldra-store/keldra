# Anvil Rust client

Anvil 0.5 ships one client package: the Rust crate in `clients/rust`, published
as `anvil-storage` when its API dependencies are publishable.

The client is a thin authenticated transport for the 0.5 object API. It exposes
exact-path reads, immutable and conditional writes, deletes, bulk operations,
and the pinned atomic-program invocation surface. It does not recreate the retired
transaction, admin, indexing, gateway, PersonalDB, Python, or TypeScript APIs.

```sh
cargo test -p anvil-storage
```

Protocol types come from the workspace `anvil-api` crate. There are no copied
client proto files and no proto-synchronisation script.
