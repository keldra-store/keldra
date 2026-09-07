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
Keldra ${version} introduces the v1 memory-first Typed JSON index architecture.
Equivalent logical definitions share physical extraction, immutable segment
artifacts, and publication work. Partition-owned hot ingress, bounded admission,
journal-backed recovery, and atomic published root vectors keep ingestion and
queries on one explicit consistency boundary.

This is a clean persistence and protocol break. Start ${version} on fresh
authoritative and derived-index volumes. In-place upgrades, mixed-version
clusters, and predecessor format identities are unsupported.

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
