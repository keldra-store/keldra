# Anvil 0.5.x known limitations

## First custom-realm binding in a multi-node 0.5.1 cluster

The first schema binding for a custom Zanzibar realm must atomically create
the realm binding and its protected-system ownership grant. Anvil 0.5.1 keeps
that guarantee on a one-node cluster, but rejects the first binding with
`UNAVAILABLE` when more than one node is active. Existing realms can be
rebound and used normally across the cluster. A later capability must add one
bounded cross-Zanzibar operation before enabling first binding on multi-node
clusters; 0.5.1 does not weaken the atomic ownership guarantee.

## Cluster lifecycle operations in 0.5.1

Anvil 0.5.1 supports genesis, authorized node preparation, learner catch-up,
typed ownership handoff, and activation for ordinary cluster formation. The
bounded Raft state machine also validates removal, capacity-reweight, and peer
certificate-overlap transitions, but 0.5.1 does not expose public operations
that start those transitions because their corresponding online ownership
handoff and live TLS-reload orchestration are not yet complete. Operators must
not submit those internal Raft commands directly.

There is no public drain or detailed cluster-health RPC in 0.5.1. Public
listener availability is the readiness boundary: Anvil binds it only after
membership, serving-fence, bootstrap, authorization, atomic recovery, and
ordered reference startup checks complete. Normal process termination stops
the public listener before the peer runtime and flushes the local store; it
does not remove the node from committed membership.

## Tenant-schema records in 0.5.1

The reserved distributed `TenantSchema` logical-record variant is not served
in 0.5.1. No public 0.5.1 operation creates or consumes that record type;
tenant names, buckets, object policies, credentials, and Zanzibar schemas use
their implemented typed records. A later capability that introduces tenant
schema metadata must add its explicit coordinator and handoff behavior first.

## Coordinator error detail in 0.5.1

Bulk writes correctly reject failed preconditions, but a per-item failure that
crosses the object-coordinator boundary is reported as `INVALID` rather than
the more specific `CONDITION_FAILED`. Other independent items in the same bulk
request retain their normal success or failure outcomes.

Deleting the current tombstone of a versioned object correctly returns
`FAILED_PRECONDITION` and leaves that tombstone unchanged, but the coordinator
path does not preserve the `CURRENT_TOMBSTONE_VERSION_CANNOT_BE_DELETED` text
prefix. Clients must use the gRPC status code rather than matching that text in
0.5.1.

## Per-object user metadata

Anvil 0.5.0 accepts the bounded `content_type` header but does not accept
arbitrary caller-defined metadata on an object version. Applications that need
descriptive or index input fields must currently carry them in their payload or
in an application-owned manifest.

## First large blob in a new hash prefix

Anvil 0.5.0 synchronizes a new large-blob file and its two-hex-digit prefix
directory, but does not synchronize the blob root after first creating that
prefix. A power loss in this window can therefore lose the first acknowledged
`LOCAL` blob in a new prefix. Initial creation of the blob root has the same
parent-directory durability limitation.

## Existing large-blob verification during deduplication

When a content-addressed large-blob path already exists, Anvil 0.5.0 discards
the incoming staged copy without first hashing the existing file. Publication
rejects a length mismatch and reads verify both length and BLAKE3, but a
same-length corrupted existing file can be accepted by a deduplicating write
and subsequently fail reads.

## Atomic preparation and the blob inactivity clock

Staging an atomic program's prepared output blobs and bundle sets their
ordinary blob `updated_at` timestamps before `CommitBatch` is proposed. An
unusually long delay in staging, waiting for the commit gate, or recovering an
earlier commit therefore reduces the effective post-commit replay and recovery
retention by the length of that delay.

The normal path is expected to take milliseconds. Anvil 0.5.0 does not refresh
the prepared blobs immediately before `CommitBatch`, and it does not add a
special lease, side store, or second lifecycle clock for atomic programs.

## Permanent deletion from versioned program-only paths

A version-enabled `PROGRAM_ONLY` path retains its historical payload versions.
Path policy correctly prohibits the ordinary `DeleteVersion` API from mutating
that path, and the 0.5.0 atomic-program DSL has no operation for deleting one
exact retained version. Operators therefore cannot permanently prune that
history in Anvil 0.5.0.
