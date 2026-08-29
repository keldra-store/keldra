# RFC 0017: Date fields, zero-copy clones, and protected object links

Status: Implemented in 0.15.0

## 1. Scope

This RFC adds three public capabilities without introducing another byte plane:

1. Typed JSON date fields normalize configured textual dates to signed Unix
   epoch milliseconds and reuse the existing numeric index components.
2. A zero-copy clone creates an independent destination object version which
   references the exact source version's existing blob.
3. A protected object link is a transparent mutable path indirection to one
   canonical ordinary target path. Target mutation through either path changes
   the target. Removing the link removes only the link. A target with inbound
   links cannot be deleted, so a committed link can never dangle.

Clone and link are deliberately different:

| Operation | Destination has an independent head | Bytes initially shared | Later writes shared |
| --- | --- | --- | --- |
| Clone | yes | yes | no |
| Link | no | yes | yes |

A removed pre-Keldra implementation is semantic history only. Its persistence,
control-plane, and service code are not restored.

## 2. Date fields

### 2.1 Public definition

`DateIndexField` declares one input format. An absent format is ISO 8601. The
initial custom format is one validated strftime pattern; fallback format lists,
locale-dependent names, and named time zones are not supported.

An ISO 8601 value must contain a calendar date. A missing time means midnight.
A missing numeric offset means UTC. Custom patterns without an offset also use
UTC. Values with non-zero precision finer than one millisecond are rejected;
indexing never silently rounds an instant.

### 2.2 Physical and query representation

Only a signed `i64` Unix epoch millisecond value is stored in terms, points,
doc values, manifests, and query state. The source string is never retained in
an index artifact.

The compiled field schema retains the validated format once. Projection parses
source JSON strings through that format. Predicate literals are strings parsed
through the same format, then use the existing exact and range planner.

Date supports `EXACT`, `RANGE`, `ORDER`, and `FACET`. It does not initially
declare `AGGREGATE`, because the current capability admits SUM and AVERAGE as
well as COUNT/MINIMUM/MAXIMUM.

Date values remain signed integers throughout the native index. Public response
conversion uses the compiled schema already attached to the query plan to
format every exposed date scalar, including facet buckets, using the configured
pattern in UTC. Query hits currently return object identities rather than a
stored JSON source, so they require no payload rewrite; a future stored-field
surface must perform the same schema-directed conversion. At most 1,000 facet
buckets are formatted. No definition read or per-document string storage is
required.

Changing a field's date format changes the definition fingerprint and requires
the normal index rebuild.

## 3. Zero-copy clone

### 3.1 Contract

`CloneObject` names an exact immutable source version and a destination in the
same tenant and governed bucket. The destination uses the ordinary Put,
PutIfAbsent, or PutIfVersion mode, durability, and command identity.

The server authorizes an exact source read and destination write, resolves the
source version's complete `BlobRef`, proves that payload can satisfy the
destination publication, and atomically publishes a new destination version
while incrementing the blob reference. It never copies payload bytes.

The source and destination then have independent heads and version lineages.
Deleting or replacing either retires only that version's reference. Blob GC is
unchanged and can reclaim bytes only after every version reference is retired.

The command fingerprint binds source address and version, destination address,
put mode and expected version, durability, and copied content metadata. Replays
must return the original receipt; reusing a command with different input fails.

Cross-bucket and cross-tenant clone are outside this capability because payload
placement, encryption, governance, retention, authorization, and accounting may
differ. Those remain byte copies.

On a clustered topology the exact retained source version and destination may
have different object coordinators. Clone therefore uses the generalized
atomic authority described below: an exact retained `(path, version, BlobRef)`
precondition and destination `BlobRef` publication are one committed bundle.

## 4. Protected transparent links

### 4.1 Contract

A link path is a versioned internal descriptor containing its canonical target
path. It is not user payload and its content type is reserved. A link may target
only a present ordinary object in the same tenant and governed bucket.

Descriptor MIME and bytes alone are not trusted as link provenance. Resolution
requires both a per-version protected-origin marker, which only the sealed link
authority can set, and the protected target-local sidecar containing that exact
alias. Protected descriptor versions remain hidden after unlink, while
historical user objects which happened to use the newly reserved MIME remain
ordinary. New ordinary Put, Publish, and BulkWrite payloads cannot claim the
protected descriptor content type. Clone may preserve it from a historical
ordinary source, but the cloned version remains unmarked and ordinary; only the
sealed built-in transaction may create a protected descriptor version.

Creating a link through an existing link resolves that link first and records
the canonical ordinary target. Therefore committed state contains no chains and
cycles are impossible. Links to links, links to absent objects, and links to
reserved internal paths fail closed.

All ordinary reads and mutations addressed through a link transparently resolve
the canonical target. A write precondition applies to the target head. Deleting
or unlinking the link removes only its descriptor and inbound registration.
Deleting the target while any inbound link exists fails with FailedPrecondition.
There is no redirect mode and no dangling-link mode.

### 4.2 Authoritative inbound sidecar

Each canonical target has one bounded, sorted inbound-link sidecar in Store
metadata keyed by that exact target. It is replicated, snapshotted, repaired,
and updated under the same target lock and commit authority as the target head;
it is not an ordinary object or public content type. The sidecar prevents target
deletion and drives bounded watch and index fanout. A target supports at most
1,024 links in this release. Combined canonical link bytes remain bounded by
the ordinary atomic-program limits.

Link descriptor and target-local sidecar are changed through a fixed built-in
object transaction which reuses the generalized atomic prepare, Raft decision,
finalization, recovery, and receipt authority. Its operation enum is private
and closed; the server reconstructs every path intent, ordinary object
authorization, and precondition rather than accepting a caller-supplied
program definition.

Link creation conditions target presence, link-path absence, and the exact
previous sidecar revision and hash. Link removal conditions the exact descriptor
and sidecar state. Target deletion conditions an absent or empty sidecar.
There is no second link-specific commit authority and `InvokeProgram` cannot
invoke or impersonate the built-in transaction.

### 4.2.1 General atomic paths

`PROGRAM_ONLY` was required by the original atomic implementation because its
exact-path locks lived only in the singleton executor. Remote path authorities
had no reservation, so admitting an ordinary writer between prepare and Raft
commit could invalidate a committed batch or race its partial finalization.

This capability removes that implementation restriction rather than adding a
privileged bypass. Every affected path authority durably stages a bounded,
executor-fenced reservation before commit. Ordinary Put, Delete, CAS,
DeleteVersion, Clone, and Link mutations consult the same reservation under the
path's mutation lock. They wait or retry while the current executor owns an
uncommitted reservation, help or await finalization after commit, and clear it
only after Raft proves the invocation aborted or its executor nomination was
fenced. Read-only dependencies and exact retained-version dependencies are
reserved too.

Consequently caller-authored atomic programs may read or mutate any authorized
ordinary path while respecting its MUTABLE or IMMUTABLE policy. `PROGRAM_ONLY`
remains an optional policy for applications that intentionally prohibit
ordinary mutation; it is no longer required merely to participate in a
program. Reserved Keldra paths remain unavailable except for exact internal
records derived and validated by a built-in capability.

The released 0.14 cluster- and data-peer schemas are version 3 while this
capability uses exact schema 4 for both. Keldra 0.15 is also a clean storage
break under KELDRA-0018: nodes start on fresh authoritative volumes, and neither
mixed-binary operation nor an in-place 0.14 volume upgrade is supported. After
every fresh node runs the new binary, the cluster remains selected at protocol
and storage version 1. Each ACTIVE node then attests its expanded `1..=2`
support over mTLS to the Raft leader. An authorized operator inspects
`GetClusterCapabilities`; only when it reports no blocking ACTIVE nodes and a
quiescent atomic tail may the operator call `ActivateClusterCapabilities` with
the reported exact placement fence. Clone, link, and generalized path
reservations fail closed until Raft selects protocol and storage version 2.
New JOINING nodes likewise send their running binary's protocol and storage
ranges in the authenticated join request. The leader requires those ranges to
match the committed JOINING descriptor before handoff/promotion, and Raft
deterministically rechecks that the descriptor contains the selected pair in
the same `CompleteMembershipTransition(Add)` apply that makes the node ACTIVE.

A committed descriptor without its inbound registration, or an inbound entry
without its descriptor, is data loss. Reads and repair fail closed rather than
guessing which side won.

### 4.3 Resolution and races

Read resolution exact-reads the link descriptor, reads the target, then
revalidates the descriptor before returning. A concurrent unlink therefore has
a linearization order or causes a bounded retry.

A mutation through a link executes through the existing atomic-program
authority with an exact descriptor-version condition and the ordinary target
mutation. The built-in link transaction may publish an already staged exact
`BlobRef`, so streamed objects retain the ordinary object-size limit instead of
the atomic-program language's 16 MiB inline-input limit. It cannot mutate a
target after the addressing link was concurrently removed or retargeted. Links
are immutable bindings; changing a target is unlink plus link.

### 4.4 Authorization

Data authorization is evaluated against the resolved canonical target path.
The link grants no additional object permission and has no independent data
policy. Link creation additionally requires target read access and destination
put access; unlink requires destination delete access. This prevents namespace
squatting without changing target data permissions.

Authorization is repeated or pinned across resolution so a stale link or authz
revision cannot be used as a confused deputy.

### 4.5 Listing, watches, indexes, and accounting

Listing returns both the target path and every link path that independently
matches the list scope. Read surfaces addressed by a link return the target's
current version and metadata under the requested link identity.

Target mutations fan out bounded logical head changes for all registered link
paths. Public watches and indexes consequently observe each visible name while
payload projection remains content-addressed and cacheable. Candidate exactness
checks resolve the link and compare the canonical target version; they never
compare the descriptor's private version to the target version.

Object counts and logical bytes count every visible name. Physical storage
telemetry counts the shared blob once. A link descriptor and inbound sidecar
are internal metadata and are excluded from user payload byte accounting.

### 4.6 Recovery and garbage collection

The descriptor uses ordinary replicated object durability; its target-local
sidecar uses the target's replicated metadata lane. Both share atomic-program
recovery. A crash before commit exposes neither side. A crash after commit is
recovered from the existing program record. Lost responses are resolved by
deterministic command replay.

Links do not each increment the current target blob reference: they protect the
target object from deletion, and its ordinary versions own their blob
references. Replacing a target may retire its old blob normally because every
link follows the new head. After the last link is removed, target deletion and
ordinary version/blob GC proceed unchanged.

## 5. Public operations

The public API adds:

- `CloneObject(CloneObjectRequest)`
- `LinkObject(LinkObjectRequest)`
- `UnlinkObject(UnlinkObjectRequest)`

`LinkObject` accepts an existing target address, a new link address, durability,
and command ID. The initial release requires the link path to be absent.
`UnlinkObject` binds the exact link descriptor version and command ID.

Ordinary Get, Head, Put, Delete, version listing, batch reads, and bulk/atomic
mutations become link-aware. `DeleteObject` addressed to a link unlinks it; it
does not delete the target. Explicit `UnlinkObject` provides an unambiguous API
and receipt for applications which know they are operating on a link.
`DeleteVersion` through a link deletes a non-current retained target version;
it returns `FailedPrecondition` for the current target version because the
link itself is an inbound deletion blocker. It never unlinks the alias.

## 6. Required qualification

Date qualification covers default and custom parsing, offset normalization,
date-only UTC, exact/range/order/facet behavior, multi-value fields, null and
missing values, malformed patterns and values, millisecond precision, response
formatting, segment merge equivalence, every public endpoint, and mixed-node
schema rejection.

Clone qualification covers exact source versions, all destination put modes,
idempotent replay, source replacement/deletion, clone replacement/deletion,
reference counts and GC, local and replicated durability, authz, coordinator
fences, lost responses, and public Rust client use.

Link qualification covers creation through targets and links, direct canonical
storage, the 1,024-link bound, all transparent read/write operations, target
delete rejection, unlink and last-link behavior, concurrent unlink/write,
authorization, list/watch/index fanout, versioning, atomic failure injection,
crash/restart, lost responses, GC, accounting, and one-/three-node public-client
workflows.
