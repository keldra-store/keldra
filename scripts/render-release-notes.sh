#!/usr/bin/env bash
set -euo pipefail

version="$1"
record="$2"
image="$3"
manifest_digest="$4"

python3 "$(dirname "$0")/release-record.py" verify \
  --record "${record}" --version "${version}" \
  --commit "$(jq -r '.source_commit' "${record}")"

cat <<EOF
Keldra ${version} adds end-user authorization to index queries. An index can
bind its results to a custom Zanzibar realm, resource namespace, relation, and
object-path mapping; each query supplies the end-user subject. Keldra intersects
that check with the authenticated application's existing authority, evaluates
one pinned realm revision, and applies it before hits, pagination, facets, and
aggregates. Continuation tokens retain the authorization identity and revision.

Nested usersets are evaluated through disposable persistent Leopard forward and
reverse adjacency indexes backed by canonical authorization tuples. The indexes
are rebuilt from that authority when absent, and exact-revision qualification
covers nested membership, removal, restoration, replica reads, and rebuilds.

Mutation ingestion now uses conflict-scoped commit lanes so independent writes
can prepare and commit concurrently. V1 indexing pipelines independent
partition preparation, immutable staging, publication, and compaction work;
typed-JSON queries use bounded parallel execution plus retained snapshot,
directory, posting-block, and artifact reuse. These changes preserve ordering,
durability, and bounded-memory admission rather than weakening them for speed.

The overlapping-run compaction defect that could build a plan which failed its
own coverage validation is fixed by including the complete overlapping target
range. Integrity failures now name the failed invariant, halt only the affected
projection partition, report durable lag, and leave unrelated partitions
running instead of retrying the same doomed plan indefinitely.

Query continuations preserve their exact retained root vector independently of
disposable caches. Internal object-head and stored-version metadata now use
Keldra's compact binary v1 persistence format.

This is a clean persistence-format break. Keldra ${version} cannot open, migrate,
or reuse a Keldra 0.17 or earlier volume. Start it on fresh authoritative and
derived-index volumes; in-place upgrades, mixed 0.17/${version} clusters, and
predecessor format identities are unsupported.

Known limitation: one operator secret still controls JWT signing, durable
credential decryption, and PersonalDB identities, so those lifecycles rotate
together in this release.

The attached release record is the publication authority. It binds the source
commit, both OCI archives and runnable manifests, embedded server and CLI
binaries, exact package archives, and the exact three-node result.

- Image: \`${image}\`
- Multi-platform manifest digest: \`${manifest_digest}\`
- Release record: \`keldra-release-record-${version}.json\`
EOF
