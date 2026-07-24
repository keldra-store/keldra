---
title: Operators Overview
description: The operator book for running Anvil safely in production.
---

# Operators Overview

Running Anvil is different from using Anvil. A developer usually asks whether an object write, search query, watch, or gateway request behaves correctly for one tenant application. An operator is responsible for the mesh that makes those requests safe: the processes, ports, regions, cells, secrets, storage paths, credentials, background maintenance, repair evidence, releases, and incident decisions that sit behind the public API.

This section is written as an operator book rather than a command catalogue. Read it before the first production deployment, then keep it close during upgrades and incidents. The pages are ordered so that each chapter gives you the assumptions needed by the next one: first the deployment model and trust boundaries, then topology and bootstrap, then daily operation, then repair, capacity, hardening, incidents, and release gates.

If you are still learning the storage model, start with [Learn](/learn/overview/) first. If you are reviewing whether a release is structurally safe to deploy, read [Architecture Overview](/architecture/overview/), [CoreMeta and Blob Storage Layout](/architecture/storage-layout/), and [Release Architecture Status](/architecture/release-status/). If you want a hands-on walk-through of tenant setup and object traffic, use [Tutorials](/tutorials/overview/). If you need exact CLI syntax while following an operator chapter, use the [admin CLI reference](/reference/admin-cli/), [public CLI reference](/reference/public-cli/), and [authorisation action reference](/reference/authorisation-actions-and-resources/). The operator pages assume those references exist, but they try to explain why each operational step matters before showing any command.

## What Operators Own

An operator owns the boundary between tenant-facing service and internal control. The public listener accepts tenant traffic and enabled gateways, while also hosting internal node gRPC under a separate system-realm authorisation path. The private admin listener changes system state: tenants, topology, routing records, repairs, secret envelopes, and system-realm permissions. Network placement and credentials must keep tenant, node, and operator authority distinct even when tenant and node RPCs share one transport listener.

An operator also owns topology. A region is the placement and routing boundary visible to tenants. A cell is typically a rack, failure, or capacity boundary inside a region. A node is one Anvil server process with declared capabilities, including any in-process background responsibilities. Bucket placement, home-region routing, cross-region behaviour, host routing, and lifecycle state all depend on this topology being deliberate rather than accidental.

The durable centre is CoreStore. Objects, refs, streams, manifests, append records, authz records, routing records, index segments, watch checkpoints, gateway records, PersonalDB evidence, repair findings, and audit events are all meant to be recoverable through the CoreStore-backed model. Operating Anvil therefore means proving that source records are safe and that derived views such as indexes, projections, routing projections, and authorisation caches are current enough for their callers.

Operators do not publish tenant applications for users, but they do handle tenant handover. That means creating the storage tenant and first credential through the admin plane, then letting the tenant use the public plane for normal bucket, object, authz, index, link, and gateway work where the product has delegated those permissions. Admin credentials should not become the application's permanent publishing mechanism.

## Read the Chapters in Order

Begin with [Production Model](/operators/production-model/). It defines public, admin, and internal-node trust contexts; explains why Anvil is a mesh of equal server processes; and sets the vocabulary used by the rest of this book. Then read [Network and Ports](/operators/network-and-ports/) before exposing anything. A secure deployment starts with the right listener, credential, and committed endpoint, not with a later firewall clean-up.

Move next to [Topology Planning](/operators/topology-planning/) and [Deployment](/operators/deployment/). Topology decisions affect bucket placement and routing for the lifetime of data, while deployment turns those decisions into storage paths, committed node endpoints, lifecycle records, listeners, credentials, and first-boot behaviour. [Secrets and Key Management](/operators/secrets-and-key-management/) belongs before production traffic because server encryption keys, per-node credentials, signing identities, application secrets, and bearer tokens have different blast radii and rotation stories.

After the server is reachable, [Admin Plane](/operators/admin-plane/) and [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/) explain the handover from operator authority to tenant-owned work. Read them together. They separate system-realm operations from tenant-public operations and help avoid the common mistake of using an admin credential to do work that should belong to a tenant principal.

With tenants active, shift to the storage and evidence chapters. [CoreStore Operations](/operators/corestore-operations/) explains the durable substrate; [Backup and Recovery](/operators/backup-and-recovery/) turns that substrate into recoverable artefacts; and [Observability](/operators/observability/) describes the signals that show whether public requests, storage, authorisation, derived state, gateways, and audits agree. Process health alone is not enough evidence for Anvil: a process can answer requests while an index is stale, a watch consumer is behind, or a routing projection is wrong.

The derived-state chapters come next because they are where many production symptoms appear. [Index Operations](/operators/index-operations/) covers path, metadata, typed, full-text, vector, hybrid, and specialised indexes. [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/) explains cursor discipline for consumers that keep those views up to date. [Gateway Operations](/operators/gateway-operations/) covers protocol adapters such as S3 and static delivery without treating them as the core security model. [PersonalDB Operations](/operators/personaldb-operations/) covers local-first SQLite synchronisation evidence, projections, snapshots, and repair surfaces. The conceptual background for these chapters is in [CoreStore](/learn/corestore/), [Indexes and Query](/learn/indexes-and-query/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Gateways](/learn/gateways/), and [PersonalDB](/learn/personaldb/).

Finally, read the chapters that prepare you for change and failure. [Repair and Diagnostics](/operators/repair-and-diagnostics/) starts with read-only findings and explains when repair mutates state. [Capacity Planning](/operators/capacity-planning/) helps you budget for object count, bytes, indexes, vectors, watches, gateways, and PersonalDB groups rather than only disk space. [Upgrades and Rollbacks](/operators/upgrades-and-rollbacks/) treats a release as a storage and control-plane event. [Security Hardening](/operators/security-hardening/) brings together reserved namespaces, credentials, network exposure, system-realm authority, tenant realms, and gateway behaviour. [Incident Response](/operators/incident-response/) gives you an evidence-first response path, and [Release Readiness Checklist](/operators/release-readiness-checklist/) turns those expectations into a gate before shipping.

## How to Use This Book

For a new environment, read sequentially and write down your deployment decisions as you go: region names, cell boundaries, listener addresses, secret locations, backup boundary, admin bootstrap method, tenant handover policy, gateway exposure, and release gates. Those decisions become the assumptions behind your automation.

For an existing environment, use the chapter order as an audit path. If an index incident occurs, you may open [Index Operations](/operators/index-operations/) first, but you should still check [Observability](/operators/observability/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), [CoreStore Operations](/operators/corestore-operations/), and [Authorisation](/learn/authorisation/) before declaring the issue fixed. If a public object is visible unexpectedly, move through gateway configuration, public-read policy, tenant authz, and audit evidence rather than assuming it is only an S3 problem.

For release work, combine this book with the references. The operator chapters explain the operational intent. The references define current command and API shapes. When a chapter describes an ideal workflow but the current public surface is coarse or partial, the chapter should say so directly; do not fill the gap with direct storage edits, invented flags, or private scripts unless the deployment explicitly owns and audits that operational tooling.

## Current Public Surfaces and Gaps

The operator documentation is intentionally conservative about unsupported behaviour. Some workflows are still easier to describe conceptually than to drive end-to-end from a public CLI, especially around fine-grained lifecycle checkpoints, drain completion, derived repair surfaces, and newer gateway foundations. Treat those notes as operational constraints, not as invitations to bypass the API.

The safe posture is consistent across the book: keep the admin API private, use the public API for tenant-facing work, store durable state in CoreStore-backed records, make derived consumers checkpoint only after their work is durable, and prefer diagnostics before repair. Where current implementation or CLI exposure falls short of that model, the relevant chapter should call out the gap and point you back to the Learn and Reference pages rather than pretending the command exists.

## Daily operator loop

A useful daily check covers both planes and at least one derived view: public readiness, admin diagnostics, storage capacity, recent admin audit events, one tenant object smoke path, and index or watch lag for a known bucket. If those disagree, classify the problem before acting: source data missing, derived view stale, authorisation denied, routing wrong, or gateway translation broken.

Use tenant commands to prove tenant behaviour and admin commands to prove platform behaviour. A tenant support ticket should not require giving the application an admin credential. An operator topology change should not be made through a tenant app credential.
