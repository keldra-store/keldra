---
title: Contributor Architecture Guide
description: Practical rules for extending Anvil without bypassing CoreStore, weakening authorisation, or creating feature-specific durable storage.
---

# Contributor Architecture Guide

This page turns the architecture into implementation rules. It is meant for humans and automated coding agents changing Anvil.

## Start from the source record

Every feature needs a source record. For object writes the source record is the object head/version and payload locator. For relationship authorisation it is the schema and tuple revision. For PersonalDB it is the group head, changeset, commit certificate, and projection state. For mesh routing it is the region, cell, node, bucket locator, and control record.

Before adding code, name the source record and the root it belongs to. If you cannot name the source record, you are probably creating an unreviewable derived cache.

## Use CoreMeta for metadata

Metadata goes through CoreMeta. Add a table id and column-family mapping when the record is durable metadata. Encode internal records with deterministic protobuf. Keep public JSON as public protocol data, but do not create a JSON sidecar as the internal metadata store.

Use RocksDB directly only through the CoreMeta abstractions. Feature code should not open its own RocksDB database, SQLite file, JSON directory, or ad hoc journal as final storage.

## Use writer segments for specialised binary data

A full-text postings file, vector HNSW graph, typed field column, authz tuple segment, registry segment, and PersonalDB row index are all legitimate specialised formats. They become safe when they are writer segments stored through CoreStore, with CoreMeta locator rows and source cursor/generation evidence.

If the segment is small, it may inline. If it is larger, it goes through the byte pipeline. The writer should not care which final storage path is selected; it should produce a deterministic segment with enough range information for efficient reads.

## Keep authorisation in the request path

A query result is not safe because an index produced it. Every public/admin operation should authenticate a principal, identify the relevant resource, and ask the appropriate authorisation model. Query paths should prune with authorisation candidates where available and still run final visibility checks before returning hits.

Do not reintroduce string-scope bypasses for admin operations. The admin plane uses the built-in system realm. Tenant data uses tenant public policy and tenant relationship authorisation.

## Preserve staged visibility

Explicit transactions stage writes. Normal readers ignore staged rows. Rollback records an aborted outcome. Commit validates preconditions, commits bytes and metadata, persists evidence, publishes a root generation, and only then makes data visible.

Cluster-local MVCC transactions may span product tables and partitions. Keep all
of a workflow's atomic mutations in one transaction and reject attempts to cross
the cluster boundary.

## Prove the path from public API down to storage

A good change has at least one public-facing test or integration test that proves the intended user-visible behaviour. Lower-level tests are still useful for file format, encoding, and protocol invariants, but they should not be the only proof for a public operation.

For storage changes, add or update conformance checks that prove no durable sidecar path was introduced. For query changes, prove page tokens bind the caller and query shape, authz pruning or final checks are present, and stale generation combinations fail closed.
