# Leopard userset qualification

`authz_leopard_qualification` is the public `AuthzService` correctness and
performance workload for Keldra's disposable persistent Leopard indexes. It
does not inspect canonical tuple storage, and it never substitutes generated
graph cardinality for measured server work.

The default graph has 16 disjoint roots, five nested userset levels, fanout
four, and one user per leaf. Each document delegates `viewer` to one root
`group#member` userset. The qualification sends ordered batches containing
both reachable users and explicit outsiders at one exact tenant-wide
authorization revision. It then removes a leaf membership, proves that the
affected user is denied while another tree remains allowed, proves that the
old exact revision is rejected, restores the same canonical tuple, and proves
access returns.

Direct and nested userset membership is evaluated from each concrete subject
through the reverse adjacency index toward the requested userset. It therefore
loads the subject's ancestor paths, not every descendant branch under a large
root group. Forward adjacency remains available for schema rewrites whose
tuple-to-userset rule genuinely needs to discover target objects. The separate
forward and reverse counter deltas make that distinction observable.

Configure it with:

- `KELDRA_AUTHZ_LEOPARD_ENDPOINTS`: one or three comma-separated public gRPC
  endpoints.
- `KELDRA_AUTHZ_LEOPARD_CLIENT_ID` and
  `KELDRA_AUTHZ_LEOPARD_CLIENT_SECRET`: a qualification application already
  authorized to manage the named custom realm. These are never written into
  evidence.
- `KELDRA_AUTHZ_LEOPARD_TENANT`: the authenticated application's immutable
  storage tenant.
- `KELDRA_AUTHZ_LEOPARD_REALM` and `KELDRA_AUTHZ_LEOPARD_SCHEMA_ID`: unique
  qualification identities. Defaults are intended only for an empty isolated
  tenant.
- `KELDRA_AUTHZ_LEOPARD_ROOTS`, `KELDRA_AUTHZ_LEOPARD_DEPTH`, and
  `KELDRA_AUTHZ_LEOPARD_FANOUT`: graph shape.
- `KELDRA_AUTHZ_LEOPARD_CHECKS_PER_BATCH`: ordered decisions per public RPC,
  maximum 1,000.
- `KELDRA_AUTHZ_LEOPARD_BENCHMARK_BATCHES` and
  `KELDRA_AUTHZ_LEOPARD_MAX_IN_FLIGHT`: measured work and bounded concurrency.
- `KELDRA_AUTHZ_LEOPARD_RESOURCE_EVIDENCE`: JSON evidence assembled from exact
  before/after OTLP counter deltas and the host sampler over the same benchmark
  window.

Normal qualification requires resource/internal-work evidence. A diagnostic
run may set `KELDRA_AUTHZ_LEOPARD_REQUIRE_INTERNAL_TELEMETRY=false`, but that is
not release evidence.

## Result vocabulary

- `accepted_batch_requests_per_second` is successful `CheckPermissions` RPCs
  divided by dispatch-to-terminal wall time.
- `accepted_checks_per_second` is ordered `PermissionResult` values returned by
  successful RPCs divided by that same wall time. This is the externally useful
  authorization throughput.
- `evaluation_results_per_second` is the rate of decisions returned by the
  server. It is deliberately distinct from internal recursive evaluation
  steps.
- `schedule_to_response_latency` is an HDR histogram over complete public RPCs
  and reports p50, p95, p99, and maximum milliseconds.
- `leopard_visited_usersets`, `leopard_loaded_direct_edges`,
  `leopard_db_prefix_reads`, `leopard_cache_hits`, `leopard_cache_misses`,
  `leopard_adjacency_cache_hits`, `leopard_adjacency_cache_misses`,
  `leopard_forward_prefix_reads`, `leopard_forward_edges_loaded`,
  `leopard_reverse_prefix_reads`, `leopard_reverse_edges_loaded`,
  `leopard_evaluation_steps`, and `leopard_limit_failures` are exact server
  counter deltas. Their absence fails qualification.
- CPU is time-weighted process CPU percent. Memory is sampled peak process RSS.
  Storage reports read/write IOPS, read/write MiB/s, and device busy percent.

The resource evidence file uses schema
`keldra.authz-leopard-resource-evidence.v1` and contains exactly those names
plus `measurement_seconds`. The controller must align its sample window with
the benchmark phase; whole-process lifetime averages are not comparable.

## Replica and rebuild qualification

Every normal run sends exact-revision oracle batches through every configured
endpoint. To exercise restart, replica replacement, snapshot installation, or
reconstruction after deleting only disposable Leopard column-family state, use
two phases:

1. Run with `KELDRA_AUTHZ_LEOPARD_PHASE=prepare-rebuild`. The workload installs
   the graph, checks every endpoint, and writes the authoritative current
   revision and deterministic graph identity to
   `KELDRA_AUTHZ_LEOPARD_STATE_PATH`.
2. Let the topology harness perform exactly one supported recovery action. It
   must not edit or copy canonical tuple keys.
3. Run with `KELDRA_AUTHZ_LEOPARD_PHASE=verify-rebuild`, the same graph settings,
   and the same state path. The workload requires every endpoint to return the
   complete oracle at the exact saved revision.

The split-phase design keeps process control out of the client and works for
both a local restart and a real three-node handoff. Merely obtaining `latest`
after recovery is insufficient evidence because it could hide an unpinned or
partially rebuilt view.
