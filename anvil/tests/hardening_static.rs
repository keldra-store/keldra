#![recursion_limit = "256"]

use std::process::Command;

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil package has workspace parent")
        .to_path_buf()
}

fn production_rust_sources(relative_dirs: &[&str]) -> Vec<(std::path::PathBuf, String)> {
    let root = repo_root();
    let mut sources = Vec::new();
    for relative_dir in relative_dirs {
        collect_rust_sources(&root.join(relative_dir), &mut sources);
    }
    sources
        .into_iter()
        .map(|path| {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
            (
                path.strip_prefix(&root).unwrap().to_path_buf(),
                strip_cfg_test_modules(&source),
            )
        })
        .collect()
}

fn collect_rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("read dir entry").path();
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut out = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_test_module = false;
    let mut depth: i32 = 0;

    for line in source.lines() {
        let trimmed = line.trim_start();

        if skipping_test_module {
            depth += brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test && trimmed.starts_with("mod tests") {
            skipping_test_module = true;
            depth = brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            pending_cfg_test = false;
            continue;
        }

        if pending_cfg_test {
            out.push_str("#[cfg(test)]\n");
            pending_cfg_test = false;
        }

        out.push_str(line);
        out.push('\n');
    }

    out
}

fn brace_delta(line: &str) -> i32 {
    let opens = line.as_bytes().iter().filter(|byte| **byte == b'{').count() as i32;
    let closes = line.as_bytes().iter().filter(|byte| **byte == b'}').count() as i32;
    opens - closes
}

fn run_script(script: &str) {
    let output = Command::new(script)
        .current_dir(repo_root())
        .output()
        .unwrap_or_else(|err| panic!("failed to run {script}: {err}"));
    assert!(
        output.status.success(),
        "{script} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn docs_tenant_tutorials_do_not_use_admin_cli() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_cli_commands_exist() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_proto_fields_exist_for_snippets() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_no_known_pseudo_json_fields() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_index_examples_use_real_create_index_request_shape() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn release_blog_post_required_for_tag() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_blog_front_matter_version_matches_tag() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_notes_render_from_blog_post() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_notes_include_artifact_metadata() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_workflow_uses_shared_release_gates() {
    let root = repo_root();
    let release = std::fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    let ci = std::fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(
        root.join("scripts/release-gates.sh").exists(),
        "shared release gate script must exist"
    );
    assert!(
        release.contains("./scripts/release-gates.sh"),
        "release workflow must call the shared release gate script"
    );
    assert!(
        release.contains("./scripts/build-image.sh"),
        "release workflow must build the release image through scripts/build-image.sh"
    );
    assert!(
        release.contains("ANVIL_BUILD_PROFILE: release"),
        "release workflow must opt into release-profile image builds explicitly"
    );
    assert!(
        release.contains("platform: linux/amd64")
            && release.contains("arch: amd64")
            && !release.contains("platform: linux/arm64"),
        "release workflow must intentionally build only the amd64 release image"
    );
    assert!(
        release.contains("artifact: anvil-test-image-amd64")
            && !release.contains("anvil-test-image-arm64"),
        "release workflow must pass only the amd64 image artifact forward"
    );
    assert!(
        release.contains("docker tag \"$ANVIL_AMD64_IMAGE\" \"$image\"")
            && release.contains("docker push \"$image\"")
            && !release.contains("docker buildx imagetools create"),
        "release workflow must publish the tested amd64 image directly without a multi-arch manifest"
    );
    assert!(
        !release.contains("build-test-image-fast.sh"),
        "release workflow must not use the old test-only image build script name"
    );
    assert!(
        release.contains("cargo publish -p anvil-storage"),
        "release workflow must publish the anvil-storage Rust client crate"
    );
    assert!(
        release.contains("scripts/crate-version-exists.py"),
        "release workflow must skip cargo publish only when the exact crate version already exists"
    );
    assert!(
        release.contains("docker buildx imagetools inspect"),
        "release workflow must resolve the actual pushed Docker image digest"
    );
    assert!(
        !release.contains("sha256:<published-by-ghcr>"),
        "release workflow must not use placeholder Docker digests"
    );
    assert!(
        ci.contains("./scripts/release-gates.sh"),
        "PR CI workflow must call the shared release gate script"
    );
    let pr_docker_deferred =
        ci.contains("Docker image smoke deferred") && ci.contains("Docker integration deferred");
    if !pr_docker_deferred {
        assert!(
            ci.contains("./scripts/build-image.sh"),
            "PR CI workflow must build its Docker test image through scripts/build-image.sh"
        );
        assert!(
            ci.contains("ANVIL_BUILD_PROFILE: ci"),
            "PR CI workflow must use a fast ci-profile test image; release workflow owns release builds"
        );
        assert!(
            ci.contains("ANVIL_USE_NATIVE_CARGO"),
            "PR CI workflow must use native Cargo for the amd64 test image on Linux runners"
        );
        assert!(
            ci.contains("linux/amd64") && !ci.contains("linux/arm64"),
            "PR CI workflow must build only the amd64 Docker test image"
        );
        assert!(
            ci.contains("anvil-test-image-amd64") && !ci.contains("anvil-test-image-arm64"),
            "PR CI workflow must upload only the amd64 Docker test image artifact"
        );
    }
    assert!(
        !ci.contains("build-test-image-fast.sh"),
        "PR CI workflow must not use the old test-only image build script name"
    );
}

#[test]
fn public_proto_messages_do_not_reuse_admin_context() {
    let proto = std::fs::read_to_string(repo_root().join("anvil-core/proto/anvil.proto")).unwrap();
    let object_service = proto
        .split("service ObjectService {")
        .nth(1)
        .and_then(|tail| tail.split("\n}").next())
        .expect("ObjectService block");
    assert!(
        !object_service.contains("AdminMutationResponse"),
        "public ObjectService must not return AdminMutationResponse"
    );
    for message in [
        "CreateObjectLinkRequest",
        "UpdateObjectLinkRequest",
        "DeleteObjectLinkRequest",
        "CreateHostAliasRequest",
        "VerifyHostAliasRequest",
        "DeleteHostAliasRequest",
    ] {
        let start = proto
            .find(&format!("message {message} "))
            .unwrap_or_else(|| panic!("missing proto message {message}"));
        let body = &proto[start..];
        let end = body
            .find("\n}")
            .unwrap_or_else(|| panic!("unterminated proto message {message}"));
        let body = &body[..end];
        assert!(
            !body.contains("AdminRequestContext"),
            "{message} is public-facing and must not carry AdminRequestContext"
        );
        assert!(
            body.contains("PublicMutationContext")
                || message.starts_with("Read")
                || message.starts_with("List"),
            "{message} must use PublicMutationContext for public mutations"
        );
    }
}

#[test]
fn production_authorisation_has_no_scope_or_policy_bypass() {
    let forbidden = [
        "auth::is_authorized",
        "try_get_scopes_from_extensions",
        "scope_or_relationship_allows",
        "get_policies_for_app",
        ".grant_policy(",
        ".revoke_policy(",
        ".list_policies_for_app(",
        "claims.scopes",
        "scopes.iter()",
        "scopes.contains(",
        "\"*|*\"",
    ];
    let mut violations = Vec::new();
    for (path, mut source) in production_rust_sources(&[
        "anvil-core/src",
        "anvil/src",
        "anvil-cli/src",
        "clients/rust/src",
    ]) {
        if path == std::path::Path::new("anvil-core/src/core_store/local_coremeta_recovery.rs") {
            const PUBLICATION_SCOPE_CHECK: &str = "bundle.scopes.contains(scope)";
            assert_eq!(
                source.matches("scopes.contains(").count(),
                1,
                "CoreMeta recovery scope checks changed; review them before excluding any from the authorization scan"
            );
            assert!(
                source.contains(PUBLICATION_SCOPE_CHECK),
                "expected CoreMeta root-publication scope membership check is missing"
            );
            source = source.replacen(PUBLICATION_SCOPE_CHECK, "", 1);
        }
        for term in forbidden {
            if source.contains(term) {
                violations.push(format!("{} contains {term}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "production authorization must be Zanzibar-backed and must not use JWT scopes or legacy policy bypasses:\n{}",
        violations.join("\n")
    );
}

#[test]
fn tenant_read_actions_do_not_require_manage_tenant() {
    let source =
        std::fs::read_to_string(repo_root().join("anvil-core/src/access_control.rs")).unwrap();
    let action_allows = source
        .split("pub async fn action_allows")
        .nth(1)
        .and_then(|tail| tail.split("pub async fn require_action").next())
        .expect("action_allows body");
    let delegated = source
        .split("pub async fn delegated_relation_for_action")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub async fn write_delegated_action_tuple")
                .next()
        })
        .expect("delegated_relation_for_action body");

    for body in [action_allows, delegated] {
        for action in [
            "AnvilAction::AppRead",
            "AnvilAction::HfKeyRead",
            "AnvilAction::HfKeyList",
            "AnvilAction::HfIngestionRead",
            "AnvilAction::GitSourceRead",
            "AnvilAction::GitSourceWatch",
        ] {
            let at = body
                .find(action)
                .unwrap_or_else(|| panic!("{action} missing from tenant read authorization path"));
            let nearby = &body[at..body.len().min(at + 1800)];
            assert!(
                nearby.contains("\"read_tenant\""),
                "{action} must resolve through read_tenant, not manage_tenant"
            );
        }
    }
}

#[test]
fn admin_rpc_relation_mapping_covers_every_admin_rpc() {
    let proto = std::fs::read_to_string(repo_root().join("anvil-core/proto/anvil.proto")).unwrap();
    let admin_service = proto
        .split("service AdminService {")
        .nth(1)
        .and_then(|tail| tail.split("\n}").next())
        .expect("AdminService block");
    let proto_rpcs: std::collections::BTreeSet<_> = admin_service
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            line.strip_prefix("rpc ")
                .and_then(|tail| tail.split_once('('))
                .map(|(name, _)| name.to_string())
        })
        .collect();

    let mapping =
        std::fs::read_to_string(repo_root().join("anvil-core/src/services/admin/rpc_mapping.rs"))
            .unwrap();
    let mapped: std::collections::BTreeSet<_> = mapping
        .split('"')
        .skip(1)
        .step_by(2)
        .filter(|value| proto_rpcs.contains(*value))
        .map(ToOwned::to_owned)
        .collect();

    assert_eq!(
        proto_rpcs, mapped,
        "every admin RPC must have an explicit Zanzibar system-realm relation mapping"
    );
}

#[test]
fn bearer_claims_do_not_carry_authorisation_scopes() {
    let source = std::fs::read_to_string(repo_root().join("anvil-core/src/auth.rs")).unwrap();
    assert!(
        !source.contains("pub scopes") && !source.contains("pub scopes:"),
        "bearer JWT claims must identify the principal and storage tenant only"
    );
}

#[test]
fn access_token_request_does_not_accept_authorisation_scopes() {
    for relative in [
        "anvil-core/proto/anvil.proto",
        "clients/rust/proto/anvil.proto",
        "clients/python/src/anvil_storage_client/proto/anvil.proto",
        "clients/typescript/proto/anvil.proto",
    ] {
        let proto = std::fs::read_to_string(repo_root().join(relative)).unwrap();
        let request = proto
            .split("message GetAccessTokenRequest {")
            .nth(1)
            .and_then(|tail| tail.split("\n}").next())
            .unwrap_or_else(|| panic!("{relative} missing GetAccessTokenRequest"));
        assert!(
            !request.contains("scope"),
            "{relative} GetAccessTokenRequest must not accept requested scopes"
        );
    }
}

#[test]
fn public_actions_do_not_model_wildcards() {
    let source =
        std::fs::read_to_string(repo_root().join("anvil-core/src/permissions.rs")).unwrap();
    for forbidden in [
        "AnvilAction::All",
        "BucketAll",
        "ObjectAll",
        "HfKeyAll",
        "HfIngestionAll",
        "PolicyAll",
        "AuthzAll",
        "AppAll",
        "IndexAll",
        "StreamAll",
        "PersonalDbAll",
        "GitSourceAll",
        "RegistryAll",
        "MeshAll",
        "RepairAll",
        "CoordinationAll",
        "\"bucket:*\"",
        "\"object:*\"",
        "\"*:\"",
    ] {
        assert!(
            !source.contains(forbidden),
            "public authorisation action model must not retain wildcard action compatibility: {forbidden}"
        );
    }
}

#[test]
fn personaldb_snapshot_sqlite_files_are_scratch_only() {
    let source =
        std::fs::read_to_string(repo_root().join("anvil-core/src/personaldb_snapshot_builder.rs"))
            .unwrap();
    assert!(
        source.contains("NamedTempFile::new_in(storage.temp_dir_path())"),
        "PersonalDB snapshot builder must place restored SQLite workspaces in the storage temp area"
    );
    assert!(
        source.contains("write_personaldb_snapshot(")
            && source.contains("compressed_sqlite_bytes")
            && source.contains("remove_file(&temp_path)"),
        "PersonalDB durable snapshots must be compressed and written through the snapshot/CoreStore path, then remove scratch SQLite files"
    );
    let restore_start = source
        .find("async fn restore_snapshot_database_scratch")
        .expect("missing restore_snapshot_database_scratch");
    let restore_body = &source[restore_start..];
    let restore_body = restore_body
        .split(
            "
}
",
        )
        .next()
        .unwrap_or(restore_body);
    assert!(
        restore_body.contains("read_personaldb_snapshot_object")
            && restore_body.contains("tokio::fs::write(target_path"),
        "restore_snapshot_database_scratch may only hydrate scratch bytes from the durable snapshot object"
    );
}

#[test]
fn index_queries_use_zanzibar_final_visibility_not_label_authorisation() {
    let operations =
        std::fs::read_to_string(repo_root().join("anvil-core/src/services/index/operations.rs"))
            .unwrap();
    assert!(
        operations.contains("query_hit_visible")
            && operations.contains("system_realm_relationship_allows"),
        "index queries must final-check visible hits through the Zanzibar relationship engine"
    );
    assert!(
        !operations.contains("pub(super) async fn query_permission_filter"),
        "index queries must not keep a permission-filter shortcut alongside planner authz"
    );
    let adapter = std::fs::read_to_string(
        repo_root().join("anvil-core/src/services/index/query_planner_adapter.rs"),
    )
    .unwrap();
    assert!(
        adapter.contains("AuthzSegmentCandidateReader")
            && adapter.contains("tenant_reader.candidate_set(request.clone()).await"),
        "planner authz candidates must come from revision-bound authz writer segments"
    );
    assert!(
        !adapter.contains("list_current_authz_objects_at_revision"),
        "candidate authz filters must not use a direct-tuple-only list as a Zanzibar substitute"
    );

    let query = std::fs::read_to_string(repo_root().join("anvil-core/src/services/index/query.rs"))
        .unwrap();
    let label_filter_start = query
        .find("pub(super) fn authz_label_filter_for_index_candidate_set")
        .expect("missing authz_label_filter_for_index_candidate_set");
    let label_filter_body = &query[label_filter_start..];
    let label_filter_body = label_filter_body
        .split(
            "
}
",
        )
        .next()
        .unwrap_or(label_filter_body);
    assert!(
        label_filter_body.contains("Ok(None)"),
        "authz label filters must stay disabled unless they are proven Zanzibar-complete"
    );
}

#[test]
fn internal_proxy_authorisation_is_zanzibar_only() {
    let source =
        std::fs::read_to_string(repo_root().join("anvil-core/src/services/internal_proxy.rs"))
            .unwrap();
    assert!(
        source.contains("system_realm::check_admin_relation")
            && source.contains("SystemAdminRelation::ManageNodes"),
        "internal proxy must require the same system-realm node authority as CoreStore internal RPCs"
    );
    for forbidden in [
        "internal:proxy_object",
        "AnvilAction::InternalProxyObject",
        "access_control::require_action",
        "claims.tenant_id != 0",
        "claims.sub == \"internal\"",
        "claims.sub == \"internal-worker\"",
        "claims.scopes",
    ] {
        assert!(
            !source.contains(forbidden),
            "internal proxy must not retain string/scope/sub bypass: {forbidden}"
        );
    }
}

#[test]
fn raw_blob_and_shard_internal_paths_are_not_public_service_apis() {
    let proto = std::fs::read_to_string(repo_root().join("anvil-core/proto/anvil.proto")).unwrap();
    assert!(
        !proto.contains("rpc PutBlob("),
        "raw CoreStore blob writes must stay behind object/gateway APIs, not become public RPCs"
    );
    assert!(
        proto.contains("service BlockStoreInternal")
            && proto.contains("message InternalRequestHeader")
            && proto.contains("bytes signature = 6;"),
        "raw shard operations must remain internal signed-node RPCs"
    );

    let corestore_internal =
        std::fs::read_to_string(repo_root().join("anvil-core/src/services/corestore_internal.rs"))
            .unwrap();
    assert!(
        corestore_internal.contains("ensure_internal_node_request(self, &request).await?")
            && corestore_internal.contains("claims.sub != header.source_node_id")
            && corestore_internal.contains("verify_internal_core_receipt_signature"),
        "CoreStore internal services must authorise a signed node identity before touching shards or replicated metadata"
    );

    let local_internal_shards = std::fs::read_to_string(
        repo_root().join("anvil-core/src/core_store/local_internal_shards.rs"),
    )
    .unwrap();
    for method in [
        "pub(crate) async fn put_internal_shard",
        "pub(crate) async fn read_internal_shard_range",
        "pub(crate) async fn get_internal_shard_receipt",
    ] {
        assert!(
            local_internal_shards.contains(method),
            "raw internal shard method must not be publicly exported: {method}"
        );
    }
}

#[test]
fn cross_region_proxy_does_not_mint_magic_internal_principals() {
    let source =
        std::fs::read_to_string(repo_root().join("anvil/src/s3_gateway/proxy.rs")).unwrap();
    assert!(
        source.contains("corestore_internal_bearer_token"),
        "cross-region proxy must use the configured node/internal bearer token so the destination can authorise a real system-realm principal"
    );
    for forbidden in [
        "mint_token(\"internal\"",
        "mint_token(\"internal-worker\"",
        "claims.sub == \"internal\"",
        "internal:proxy_object",
    ] {
        assert!(
            !source.contains(forbidden),
            "cross-region proxy must not mint or recognise magic internal principals: {forbidden}"
        );
    }
}

#[test]
fn mesh_control_creates_system_realm_tuples_for_topology_objects() {
    let source =
        std::fs::read_to_string(repo_root().join("anvil-core/src/services/mesh_control.rs"))
            .unwrap();
    for required in [
        "grant_region_defaults",
        "grant_cell_defaults",
        "grant_node_defaults",
        "SystemAdminRelation::ManageRegions",
        "SystemAdminRelation::ManageNodes",
        "resolve_mesh_tenant_id",
        "require_mesh_bucket_manage",
    ] {
        assert!(
            source.contains(required),
            "mesh control creation must go through the system-realm topology model and seed required topology tuples: {required}"
        );
    }

    let transaction =
        std::fs::read_to_string(repo_root().join("anvil-core/src/services/transaction.rs"))
            .unwrap();
    for required in [
        "committed_topology_resources_from_transaction",
        "grant_region_defaults",
        "grant_cell_defaults",
        "grant_node_defaults",
    ] {
        assert!(
            transaction.contains(required),
            "explicit MeshControl transactions must seed the same system-realm topology tuples after commit: {required}"
        );
    }
}

#[test]
fn suspicious_json_control_paths_are_proto_wrapped_or_operator_only() {
    let checks = [
        (
            "anvil-core/src/mesh_directory.rs",
            [
                "payload_proto",
                "write_descriptor_projection",
                "control_payload_operator_json",
            ]
            .as_slice(),
            [".jsonl", ".wal.json", "File::create("].as_slice(),
        ),
        (
            "anvil-core/src/mesh_directory/record_proto.rs",
            [
                "DESCRIPTOR_FILE_EXTENSION: &str = \".pb\"",
                "descriptor_payload_proto",
                "encode_deterministic_proto",
            ]
            .as_slice(),
            [
                "DESCRIPTOR_FILE_EXTENSION: &str = \".json\"",
                "serde_json::to_vec_pretty",
            ]
            .as_slice(),
        ),
        (
            "anvil-core/src/metadata_journal.rs",
            [
                "ObjectMetadataBodyProto",
                "ObjectLinkTargetProto",
                "encode_deterministic_proto",
            ]
            .as_slice(),
            ["shard_map_json", "serde_json::to_vec_pretty"].as_slice(),
        ),
        (
            "anvil-core/src/manifest_journal.rs",
            [
                "ManifestBodyProto",
                "ManifestCurrentRowProto",
                "encode_deterministic_proto",
            ]
            .as_slice(),
            ["File::create(", ".wal.json", ".sidecar"].as_slice(),
        ),
        (
            "anvil-core/src/core_store/local_object_metadata.rs",
            [
                "ObjectMetadataRowProto",
                "shard_map_target",
                "shard_map_kind",
                "fn encode_deterministic",
            ]
            .as_slice(),
            ["shard_map_json", "serde_json::to_vec_pretty"].as_slice(),
        ),
    ];

    for (relative, required_terms, forbidden_terms) in checks {
        let source = std::fs::read_to_string(repo_root().join(relative)).unwrap();
        for required in required_terms {
            assert!(
                source.contains(required),
                "{relative} must keep internal control records in deterministic protobuf while allowing JSON only as public/operator payload"
            );
        }
        for forbidden in forbidden_terms {
            assert!(
                !source.contains(forbidden),
                "{relative} must not use forbidden durable JSON/control sidecar pattern: {forbidden}"
            );
        }
    }
}

#[test]
fn personaldb_signing_packaging_has_no_external_signer_sidecars() {
    let root = repo_root();
    let forbidden = [
        "anvil-personaldb-signer",
        "anvil-signer",
        "PERSONALDB_PROTOCOL_SIGNING_MANIFEST_PATH",
        "PERSONALDB_GROUP_CONTROL_SIGNER_SOCKET_DIR",
        "PERSONALDB_SNAPSHOT_SIGNER_SOCKET_DIR",
        "PERSONALDB_WITNESS_SIGNER_SOCKET_DIR",
        "start-personaldb-test-signers",
        "/run/anvil-signers",
    ];
    for relative in [
        "Cargo.toml",
        "anvil/Dockerfile",
        "anvil/Dockerfile.prebuilt",
        "anvil/build-and-run.sh",
        "anvil/docker-compose.yml",
        "anvil/tests/docker-compose.test.yml",
        "scripts/build-image.sh",
    ] {
        let packaging = std::fs::read_to_string(root.join(relative)).unwrap();
        for value in forbidden {
            assert!(
                !packaging.contains(value),
                "{relative} must not package external PersonalDB signer sidecar input {value}"
            );
        }
    }
}

#[test]
fn personaldb_protocol_is_a_public_exact_dependency() {
    let root = repo_root();
    for relative in ["anvil-core/Cargo.toml", "anvil-test-utils/Cargo.toml"] {
        let manifest = std::fs::read_to_string(root.join(relative)).unwrap();
        assert!(
            manifest.contains("personaldb-protocol = \"=0.1.0\""),
            "{relative} must pin the published PersonalDB protocol crate exactly"
        );
        for forbidden in ["ssh://", "git@github.com", "git ="] {
            assert!(
                !manifest.contains(forbidden),
                "{relative} must not require private Git access through {forbidden}"
            );
        }
    }
}

#[test]
fn personaldb_signing_admin_responses_never_contain_private_key_material() {
    let root = repo_root();
    for relative in [
        "anvil-core/proto/anvil.proto",
        "clients/rust/proto/anvil.proto",
        "clients/python/src/anvil_storage_client/proto/anvil.proto",
        "clients/typescript/proto/anvil.proto",
    ] {
        let proto = std::fs::read_to_string(root.join(relative)).unwrap();
        for message_name in [
            "PersonalDbSigningKeyRecord",
            "PersonalDbSigningKeyResponse",
            "ListPersonalDbSigningKeysResponse",
        ] {
            let message = proto
                .split(&format!("message {message_name} {{"))
                .nth(1)
                .and_then(|tail| tail.split("\n}").next())
                .unwrap_or_else(|| panic!("{relative} missing {message_name}"));
            assert!(
                !message.contains("private_key"),
                "{relative} {message_name} must not expose private signing material"
            );
        }
    }
}

#[test]
fn personaldb_signing_operator_docs_describe_optional_in_process_custody() {
    let root = repo_root();
    for relative in [
        "README.md",
        "documentation/content/operators/secrets-and-key-management.md",
    ] {
        let docs = std::fs::read_to_string(root.join(relative)).unwrap();
        for required in ["optional", "in-process"] {
            assert!(
                docs.contains(required),
                "{relative} must describe PersonalDB signing as {required}"
            );
        }
    }

    let operations = std::fs::read_to_string(
        root.join("documentation/content/operators/secrets-and-key-management.md"),
    )
    .unwrap();
    assert!(
        operations.contains("not prevent server startup"),
        "operator guidance must state that missing PersonalDB keys do not block startup"
    );
}
