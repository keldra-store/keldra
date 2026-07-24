---
title: Package Gateway Foundations
description: Model package artefacts on today's Anvil primitives while keeping future registry gateways truthful.
---

# Package Gateway Foundations

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/), [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), [Indexes: Path, Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/), [S3-Compatible Gateway](/tutorials/s3-gateway/), and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/). It assumes you know that objects are versioned, links are aliases rather than copies, indexes are derived data, and the public API is the tenant-facing surface.

Package and registry gateways are best understood as protocol adapters. A Docker-compatible registry, an npm registry, a Python package index, a Maven repository, or a Rust crate registry all speak different protocols, but they all need the same durable ideas: artefact bytes, package names, immutable versions, checksums, mutable channels such as `latest`, credentials, authorisation decisions, audit history, and searchable metadata. In Anvil those durable ideas should sit on storage tenants, buckets, objects, links, indexes, metadata, public policy scopes, and relationship authorisation. The gateway translates protocol details; it should not become a second storage engine or a second security model.

The important current boundary is that Anvil's public S3/static gateway is implemented, and Anvil's core contains foundational gateway record code for repositories, blobs, tags, upload sessions, credentials, mounts, access tokens, and audit records. The current repository does not expose a public Docker Registry v2, npm, PyPI, Maven, or Cargo protocol handler, and the public CLI has no `anvil package`, `anvil registry`, or package-gateway management command. Treat this page as a practical foundations tutorial: it shows how to model package artefacts today with the public Object, Link, Index, Bucket, Auth, and S3 surfaces, and it calls out where a future package gateway would take over.

Applications should call the public APIs directly when they own the publishing workflow. The `anvil` and `aws` commands below are manual helpers over currently implemented public surfaces; they are useful for proving the shape by hand, but they are not a package-manager client and they are not a substitute for API-level preconditions, idempotency, metadata, and retry handling.

For the broader concepts, read [Gateways](/learn/gateways/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Indexes and Query](/learn/indexes-and-query/), [Public CLI](/reference/public-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

This tutorial models a package gateway as ordinary Anvil data first and protocol compatibility second. You will store immutable artefacts, publish version manifests, move channels with links, build a typed catalogue, and decide what belongs on public delivery surfaces before thinking about gateway-specific request syntax.

Everything in this page is tenant-owned public-plane work. Use `anvil` or the public API for package objects, links, catalogue indexes, and public-read policy. Do not use `anvil-admin` to publish packages or move channels. Operators may configure gateway routing and tenant bootstrap, but package state belongs to the tenant.

## Separate the product model from the protocol

A package manager usually presents one convenient command such as "publish this package" or "install the latest version". Under that command are several separate state changes. The package archive is content-addressed and should be immutable. A version manifest records the package name, version, digest, media type, size, and any ecosystem metadata. A channel such as `latest`, `stable`, or `beta` is mutable and should move with a compare-and-swap check. A catalogue or search page is derived data over committed manifests. Pull and publish permissions are different authorisation decisions.

In today's public Anvil model you can represent those pieces directly:

| Package idea | Current Anvil primitive | Why it matters |
| --- | --- | --- |
| Artefact bytes | Object under a digest-named key | The key can be stable and content-addressed even though Anvil objects themselves are versioned. |
| Package version | JSON manifest object under a version key | Readers can inspect package metadata without downloading the artefact body. |
| Checksum | Digest stored in the key and manifest | Package integrity is explicit. Do not confuse it with Anvil's ETag or object version id. |
| Mutable channel | Object link such as `tags/latest` | The channel can move without copying bytes, and link generation is a CAS token. |
| Catalogue/search | Typed JSON index over manifest objects | Querying packages does not require scanning the bucket. |
| Public downloads | Dedicated public-read bucket or authenticated object reads | Public-read is deliberate policy, not a gateway bypass. |
| Existing object tooling | Native public API or S3 gateway | S3 moves bytes; it does not make Anvil an npm, PyPI, Maven, Cargo, or Docker registry today. |

This split also keeps future gateways honest. A Docker-style tag can map to a gateway tag record, but it is still a mutable pointer. An npm dist tarball can map to a blob record, but it is still immutable bytes plus a checksum. A PyPI simple-index page can be generated from manifests and indexes, but the manifest remains the source of truth.

## Start with narrow authority

Package publishing is a tenant data-plane workflow. Application publishers should use the public API, a public client, the public CLI helper, or the S3 gateway. They should not use the private admin API to write package objects, move `latest`, or avoid missing tenant grants. The admin API is for operators managing mesh, routing, repair, and system lifecycle; exposing it to make publishing easier would collapse the public/private boundary.

A production package bucket is usually dedicated to package artefacts because public-read is bucket-wide in the current public CLI. The tutorial examples continue to use the earlier `documents` bucket so the paths fit the previous pages, but do not copy that layout into a mixed private/public production bucket.

Use narrow grants. The exact grant commands depend on which app or principal publishes packages, but the shape should look like this rather than a wildcard grant:

| Need | Public policy action | Example resource |
| --- | --- | --- |
| Upload one immutable blob | `object:write` | `documents/packages/acme-widget/blobs/sha256/<digest>.tgz` |
| Upload one version manifest | `object:write` | `documents/packages/acme-widget/versions/1.0.0.json` |
| Move the `latest` channel | `object:write` | `documents/packages/acme-widget/tags/latest` |
| Read private artefacts | `object:read` | `documents/packages/acme-widget/blobs/sha256/<digest>.tgz` |
| List package prefixes | `object:list` | `documents` |
| Create the package catalogue index | `index:create` | `documents/package_catalog` |
| Query the package catalogue | `index:read` | `documents` |
| Make a dedicated package bucket public | `bucket:write` | `<package-bucket-name>` |

The current object listing and index reading checks are coarser than an ideal per-prefix model in places: object listing checks `object:list` on the bucket, and index list/query/diagnostics currently check `index:read` on the bucket. Keep that in mind when designing least-privilege package UIs. Relationship authorisation can further filter object reads and index hits when an index uses `inherit_object`, but it does not replace the coarse public policy action required to enter the API.

## Build an immutable artefact key

A package artefact is the byte stream a package manager eventually downloads: for example, a tarball, wheel, JAR, crate archive, container layer, or manifest file. The first rule is to make the content identity explicit before upload. In this example the digest is part of the object key and the manifest. That makes accidental overwrite visible in review, makes deduplication possible at the application level, and prepares the layout for a future package gateway that validates digests itself.

Create a small demo artefact and compute its SHA-256 digest:

```bash
mkdir -p package-demo
printf 'acme-widget version 1.0.0\n' > package-demo/acme-widget-1.0.0.tgz

PACKAGE_SHA="$(shasum -a 256 package-demo/acme-widget-1.0.0.tgz | awk '{print $1}')"
PACKAGE_SIZE="$(wc -c < package-demo/acme-widget-1.0.0.tgz | tr -d ' ')"
PACKAGE_KEY="packages/acme-widget/blobs/sha256/${PACKAGE_SHA}.tgz"

echo "sha256:${PACKAGE_SHA}"
echo "${PACKAGE_SIZE} bytes"
```

These commands do not contact Anvil. They prove only that the local file has a deterministic digest and that your shell has stored the key that later commands will use. They do not prove the digest has been enforced by Anvil, that the key is reserved against future overwrites, or that a package manager can install this file.

Upload the artefact as an ordinary object when your bucket, region placement, and grants are ready:

```bash
anvil --profile acme object put \
  package-demo/acme-widget-1.0.0.tgz \
  "s3://documents/${PACKAGE_KEY}"
```

This is a real public CLI command over the native Object API. A successful upload proves the active profile can write that exact object key and that Anvil committed a new current object version. It does not prove package-level immutability by itself. The current `object put` helper uses a normal write with no ETag or version precondition flag, and it may still hit the least-privilege bucket lookup gap described in [Buckets and Objects](/tutorials/buckets-and-objects/). Production publishers should use the public API or a client library so they can set idempotency keys, preconditions, content type, user metadata, and any fenced mutation context they require.

If the publisher is already S3-shaped, the same key can be written through the implemented S3-compatible gateway instead. This assumes the S3 app credentials from [S3-Compatible Gateway](/tutorials/s3-gateway/) are already exported as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. A successful command proves S3 signing and object upload through a gateway, not package-registry protocol support:

```bash
export ANVIL_S3_ENDPOINT="http://127.0.0.1:50051"
aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3 cp package-demo/acme-widget-1.0.0.tgz "s3://documents/${PACKAGE_KEY}"
```

Use one upload path as the source of truth in a real pipeline. Mixing native and S3 upload helpers is useful for smoke tests, but production pipelines should have one owner for object metadata, content type, idempotency, and retry behaviour.

## Write a version manifest as the package record

The artefact object stores bytes. A package version also needs a small metadata record that application code, index builders, and future gateway handlers can read quickly. Use JSON for this tutorial because Anvil can index typed JSON bodies today.

```bash
cat > package-demo/acme-widget-1.0.0.manifest.json <<EOF
{
  "schema": "example.package.manifest.v1",
  "name": "acme-widget",
  "version": "1.0.0",
  "media_type": "application/gzip",
  "digest": "sha256:${PACKAGE_SHA}",
  "size_bytes": ${PACKAGE_SIZE},
  "blob_key": "${PACKAGE_KEY}",
  "published_at": "2026-07-06T10:00:00Z"
}
EOF

anvil --profile acme object put \
  package-demo/acme-widget-1.0.0.manifest.json \
  s3://documents/packages/acme-widget/versions/1.0.0.json
```

The local `cat` command creates an application-level manifest; Anvil does not validate this schema for you. The `object put` command stores that manifest as another ordinary object version. If it succeeds, readers can fetch or index the manifest without downloading the package tarball. It does not prove the manifest digest matches the artefact object. Your publisher should check the digest before upload, and a future package gateway can enforce the digest when it commits a blob record.

Use object metadata for transport and operations metadata, not as the only package record. The public Object API can carry content type and structured user metadata, and the S3 gateway can carry `Content-Type` plus simple string `x-amz-meta-*` values. The current `anvil object put` helper does not expose those fields. Keep the version manifest as the canonical package metadata so it can be read, indexed, audited, and moved through links regardless of which upload surface wrote the bytes.

Do not treat the Anvil object ETag or version id as a replacement for `sha256:${PACKAGE_SHA}`. ETags and version ids are storage identities for object versions and preconditions. Package ecosystems need their own content digests because clients verify the package payload, not merely the storage mutation that wrote it.

## Move channels with links, not copies

A version path such as `packages/acme-widget/versions/1.0.0.json` should be stable. A channel path such as `packages/acme-widget/tags/latest` is intentionally mutable. In Anvil, model that mutable name as an object link.

```bash
anvil --profile acme object link create \
  s3://documents/packages/acme-widget/tags/latest \
  s3://documents/packages/acme-widget/versions/1.0.0.json \
  --resolution follow
```

A successful command proves the caller can write the link key, the target exists unless `--allow-dangling` was supplied, and Anvil created a link descriptor at generation `1`. It does not copy the manifest or the artefact body. Reads through the link follow to the target, while `anvil object link read` inspects the descriptor itself:

```bash
anvil --profile acme object link read \
  s3://documents/packages/acme-widget/tags/latest
```

When you publish `1.0.1`, upload a new blob and a new manifest under their immutable keys, then move the channel with a generation check:

```bash
anvil --profile acme object link update \
  s3://documents/packages/acme-widget/tags/latest \
  s3://documents/packages/acme-widget/versions/1.0.1.json \
  --expected-generation 1 \
  --resolution follow
```

The `--expected-generation` value is the link-level compare-and-swap token. If another publisher already moved `latest`, the update fails instead of silently overwriting their promotion. That is the behaviour you want for release channels. Read the link again, inspect the target, and decide whether your release should still move the channel.

The current public CLI can create and update links to a target key, but it does not expose a `--target-version` flag for pinned links. If a package manifest or approval record must point to a specific historical object version, use the public API or Rust client that exposes the version field.

## Build a package catalogue with a typed JSON index

A package catalogue is derived data. It should help users answer questions such as "which versions of `acme-widget` are published?" without making the index the source of truth. The manifest objects remain authoritative; the index can be rebuilt from them.

The following index selects manifest objects under `packages/` and extracts fields from the JSON body. The build policy uses `object_current`, which means the index sees the current version of each manifest object.

Read the JSON fields before running the command. `selector_json` is the build-time boundary: only source object keys under `packages/` are eligible. `extractor_json` is `{}` because current `typed_json` indexes put field definitions in `build_policy_json`. The build policy names each typed field and the JSON Pointer that extracts it from the manifest body. Query-time `typed_predicates_json` and `typed_order_json` later ask questions of those materialised fields; they do not change what the index stores.

```bash
anvil --profile acme index create documents package_catalog typed_json \
  --selector-json '{"prefix":"packages/"}' \
  --extractor-json '{}' \
  --build-policy-json '{"source_kind":"object_current","fields":[{"name":"name","extractor":"/name","required":true},{"name":"version","extractor":"/version","required":true},{"name":"published_at","extractor":"/published_at","required":true},{"name":"digest","extractor":"/digest","required":true},{"name":"blob_key","extractor":"/blob_key","required":true}],"default_order":[{"field":"published_at","direction":"desc"},{"field":"name","direction":"asc"},{"field":"version","direction":"asc"}]}' \
  --authorization-mode inherit_object
```

This calls `IndexService.CreateIndex`. A successful response proves the caller has `index:create` on `documents/package_catalog`, the bucket exists, the index kind is recognised, the selector and build policy JSON parsed, and Anvil stored an enabled index definition. It does not prove any manifests have been indexed yet. The builder still has to process source object changes, and local tutorials may still be blocked by region activation, upload metadata, or missing index grants.

Query the catalogue with the current array form for `typed_predicates_json` and `typed_order_json`:

```bash
anvil --profile acme index query documents package_catalog \
  --path-prefix packages/acme-widget/versions/ \
  --typed-predicates-json '[{"field":"name","op":"eq","value":"acme-widget"}]' \
  --typed-order-json '[{"field":"published_at","direction":"desc"},{"field":"version","direction":"asc"}]' \
  --limit 20
```

A successful query proves the typed predicate array and order array parsed, a materialised segment was available, and each returned hit was visible under the index's authorisation mode. Because the index uses `inherit_object`, the caller still needs object read visibility for the manifest object. If the command returns no rows, do not immediately conclude that no versions exist. The index may not have caught up, the selector may be wrong, the manifest JSON may have failed extraction, the JSON value types may not match the predicate, or the caller may not be allowed to see the manifest.

A package client should treat catalogue rows as discovery data. Before installing, read the manifest object and validate the artifact digest, size, signature/provenance fields, and policy that your package ecosystem requires. The typed index helps find candidates; it is not the package-verification engine.

Use diagnostics when rows are missing:

```bash
anvil --profile acme index diagnostics documents package_catalog \
  --severity error \
  --page-size 20
```

This proves the caller can read index diagnostics for the bucket and that Anvil can return any matching diagnostic records. For package manifests, common problems are invalid JSON, missing required fields, a JSON Pointer that does not match the manifest shape, or an index segment that has not been built. Diagnostics are evidence, not a repair by themselves.

SemVer ordering deserves special care. The example orders by `published_at`, not by the `version` string, because simple lexical order does not implement full SemVer precedence. If your product must answer "latest compatible version" according to an ecosystem's rules, keep that resolver in application logic or in a future gateway protocol handler that implements the ecosystem correctly.

## Decide what should be public

A package can be private, public, or mixed by product policy. In Anvil, public-read is explicit. If you make a bucket public, anyone who can reach the public surface can read matching data through supported read paths. That includes object names and metadata exposed by listing behaviour described in [Public Access](/tutorials/public-access/) and [S3-Compatible Gateway](/tutorials/s3-gateway/). Use a dedicated package bucket for public artefacts whenever possible.

The public CLI command is bucket-wide:

```bash
anvil --profile acme bucket set-public documents --allow true
```

This command calls the public Bucket API and requires `bucket:write` on `documents`. If it succeeds, it proves the caller deliberately changed bucket policy. It does not prove every package path is safe to publish, does not grant writes, does not expose the admin API, and does not remove copies already cached by package clients, CDNs, logs, or mirrors. Turn public-read off after a local test unless the whole bucket is meant to stay public:

```bash
anvil --profile acme bucket set-public documents --allow false
```

For private packages, keep the bucket private and give pull clients only the read/list scopes and relationship tuples they need. A pull credential should not be able to move `latest`, overwrite a manifest, create an index, or manage tenant apps. A publish credential should not automatically administer host aliases or bucket policy. Future package gateway credentials should resolve to ordinary Anvil principals and actions; they should not bypass the public policy and relationship checks.

## How this maps to the internal gateway foundation

The current core gateway foundation already models several registry-shaped records internally. Repository records identify a tenant, gateway family, registry instance, and repository. Blob records store a `sha256:<hex>` digest, media type, size, and CoreStore object reference; the internal helper verifies that the bytes match the digest and returns the existing record on an idempotent replay. Tag records are mutable digest pointers updated with an optional expected generation. Upload session records move through states such as `open`, `receiving`, `finalising`, `committed`, `aborted`, and `expired`, with expected digest, staged parts, received byte count, and idempotency hashes. Credential and access-token records support short-lived gateway bearer tokens, with the internal maximum token TTL currently capped at 900 seconds. Mount records describe host and path routing to a tenant, gateway family, registry instance, default bucket, and repository prefix. Audit records append gateway operations to an ordered stream with idempotency support.

Those records are useful foundations, but they are not a tenant-facing package registry yet. There is no current public service or CLI command to create a Docker mount, answer a Docker token challenge, accept an npm publish, generate a PyPI simple API page, serve Maven metadata, or implement Cargo's registry index. There is also no current public package-manager client configuration that can point `docker`, `npm`, `pip`, `mvn`, or `cargo` at Anvil and expect native registry behaviour.

That distinction is the main lesson of this page. The data model can be package-ready before every package protocol is implemented. You can already store artefacts, manifests, links, checksums, public/private policy, S3 uploads, and typed catalogue indexes on Anvil's public surfaces. When a package gateway is exposed later, it should translate external protocol calls into those same primitives rather than creating a parallel control plane.

## Current limitations to design around

Today's public surfaces let you model packages, but they do not enforce a complete package-registry contract. The public CLI upload helper cannot set content type or structured user metadata and does not expose write precondition flags. Digest validation for the manifest layout in this tutorial is application responsibility, not automatic storage validation. Version immutability is a convention unless your publisher uses preconditions, narrow grants, and review controls that prevent overwriting version keys. Object links give safe mutable channels, but the CLI cannot create pinned target-version links. The typed JSON index can catalogue manifests, but it does not implement ecosystem-specific version solving, dependency resolution, yanking policy, signature verification, provenance, vulnerability metadata, or SemVer precedence.

Gateway-specific implementation is also intentionally limited today. S3 and static object delivery exist; package protocol handlers, package gateway CLI commands, public mount management, registry-token challenge endpoints, and package upload sessions are not exposed. If a necessary workflow is not in the current public API or CLI, document it as a gap and keep the publishing path on ordinary Anvil objects until the gateway exists.

Keep the security boundary simple: public API and S3 gateway for tenant package publishing and pulling, public policy scopes plus relationship authorisation for decisions, object links for movable names, indexes for derived catalogues, and the private admin API only for operator lifecycle and repair.

## Success and failure cues

A package gateway design is on the right track when immutable artefact keys never move, manifests describe exactly what clients install, channel links move with generation checks, and catalogue indexes can be rebuilt from source objects. If a client sees an unexpected package, check the channel link generation, manifest body, typed catalogue row, and public-delivery policy before assuming the protocol adapter is wrong.

## Where to go next

Read [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) for safer channel movement, [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/) for catalogues, and [Public Access](/tutorials/public-access/) before serving artefacts anonymously. Operators planning a real package gateway should also read [Gateway Operations](/operators/gateway-operations/).
