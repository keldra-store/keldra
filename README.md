# Anvil

Anvil 0.5 is a small, versioned object store with an explicit opt-in layer for
bounded atomic application commands.

The storage kernel understands paths and opaque bytes. Every successful write
creates an immutable version and moves one current head. It supports exact
compare-and-swap, create-once namespaces, idempotent commands, independent bulk
writes, content-addressed streaming uploads, and same-snapshot batch reads.

Applications that need several exact paths to change together write a bounded
deterministic program as an immutable ordinary object below `_anvil/programs/`,
then explicitly invoke that pinned object. The singleton executor locks its
expanded paths locally in canonical order and evaluates against the latest
committed state. A compact Raft decision makes an already durable prepared
bundle visible. Raft never carries object bodies, path inventories, locks,
waiters, or program definitions.

## Implementation status

The 0.5 rewrite is under active development and is not release-qualified. The
object, authorization, atomic-program, and compact-consensus primitives have
unit coverage, but the production server does not yet have an approved
bootstrap/authentication path, `InvokeProgram` is not exposed, and
`WatchPrefix` is not implemented. The architecture gaps listed in section 16
of ANVIL-0009 remain deliberate blockers rather than being filled with implicit
defaults.

## Deliberate 0.5 break

There is no compatibility with the 0.4 transaction or MVCC API and no in-place
upgrade from its storage format. The following concepts are gone:

- `BeginTransaction`, `CommitTransaction`, and `RollbackTransaction`;
- transaction drafts and staged public writes;
- certification, read sets, range observations, and snapshot versions;
- arbitrary cross-partition atomic batches;
- payloads or product rows in Raft.

Export data through the old release and import it into a new 0.5 store.

## API layers

| Layer | Operations | Guarantee |
| --- | --- | --- |
| Opaque object core | `UploadBlob`, `PutObject`, `PublishObject`, `DeleteObject` | One immutable version and one CAS head move |
| Throughput | `BulkWrite`, `BatchGet` | Independent write outcomes; one read snapshot |
| Policy | create-once prefixes | Children can be created exactly once and never deleted |
| Atomic programs | immutable object under `_anvil/programs/`, then `InvokeProgram` | One bounded deterministic state transition orchestrated by the nominated executor |
| Workflows | application-owned saga | External-service coordination |

Uploading an MP3 is an ordinary blob operation. It does not invoke a program.
Only publishing its resulting blob reference changes a visible object head.

## Repository

- `crates/anvil-store`: opaque version/head storage, blobs, CAS, policies and bulk operations.
- `crates/anvil-atomic-program`: provisional bounded command engine; the 0.5
  program representation remains an explicit RFC decision.
- `crates/anvil-consensus`: compact executor nomination and publication decisions.
- `crates/anvil-api`: the single generated gRPC contract shared by server and clients.
- `anvil`: server transport and integration.
- `clients/rust`: thin authenticated Rust transport.
- `docs/rfcs/anvil_0009_atomic_programs.md`: complete 0.5 architecture and invariants.

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace
```

Anvil is licensed under Apache-2.0.
