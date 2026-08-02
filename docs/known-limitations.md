# Anvil 0.5.0 known limitations

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
