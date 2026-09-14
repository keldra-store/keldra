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
Keldra ${version} makes v1 indexing and queries bounded, recoverable, and easier
to operate under sustained mixed workloads. Query continuations now preserve
their exact retained root vector independently of disposable caches, partition
work uses bounded concurrency, and compaction integrity failures are contained
and reported without silently freezing every index. Internal object head and
stored-version metadata now use Keldra's compact binary persistence format.

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
