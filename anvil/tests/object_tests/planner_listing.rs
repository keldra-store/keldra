use std::path::{Path, PathBuf};

fn workspace_file(path: &str) -> String {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = if manifest_dir.join("../anvil-core").exists() {
        manifest_dir
            .parent()
            .expect("manifest has workspace parent")
            .to_path_buf()
    } else {
        manifest_dir
    };
    std::fs::read_to_string(Path::new(&workspace_root).join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
}

fn function_body<'a>(source: &'a str, signature: &str, next_signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing function signature {signature}"));
    let tail = &source[start..];
    let end = tail
        .find(next_signature)
        .unwrap_or_else(|| panic!("missing following function signature {next_signature}"));
    &tail[..end]
}

#[test]
fn public_object_listing_fails_closed_when_planner_candidate_path_is_missing() {
    let source = workspace_file("anvil-core/src/object_manager/read.rs");
    let list_body = function_body(
        &source,
        "pub async fn list_objects_for_tenant(",
        "async fn planner_backed_object_listing(",
    );

    assert!(
        list_body.contains("authorized_bucket_reader_claims"),
        "ListObjects must first prove the caller can list the bucket through the normal authz gate"
    );
    assert!(
        list_body.contains("planner_backed_object_listing"),
        "ListObjects must enter the planner/candidate path instead of reading bucket metadata directly"
    );
    assert!(
        !list_body.contains("list_current_object_metadata"),
        "ListObjects must not fetch a broad current-object metadata set before planner/authz pruning"
    );
    assert!(
        !list_body.contains("filter_objects_visible_to_reader"),
        "ListObjects must not rely on broad fetch plus final visibility filtering as its normal query architecture"
    );

    let guard_body = function_body(
        &source,
        "async fn planner_backed_object_listing(",
        "pub async fn list_object_versions(",
    );
    assert!(
        guard_body.contains("execute_object_listing_plan")
            && guard_body.contains("list_current_object_metadata_page")
            && !guard_body.contains(".list_current_object_metadata(bucket"),
        "ListObjects must read object-list candidates only inside the planner-backed path"
    );
    assert!(
        !guard_body.contains("i32::MAX"),
        "ListObjects must keep source and result pages explicitly bounded"
    );
    assert!(
        guard_body.contains("if bucket_wide_read && !delimiter.is_empty()")
            && guard_body.contains("after_current_prefix"),
        "delimiter listings must seek past each emitted prefix instead of scanning its subtree"
    );
    let versions_body = function_body(
        &source,
        "pub async fn list_object_versions_for_tenant(",
        "async fn object_listing_authz_revision(",
    );
    assert!(
        versions_body.contains("list_object_versions_metadata_page")
            && !versions_body.contains("i32::MAX"),
        "ListObjectVersions must use bounded ordered source pages"
    );
    let planner_body = function_body(
        &source,
        "async fn execute_object_listing_plan(",
        "pub async fn current_object_for_write_precondition(",
    );
    assert!(
        planner_body.contains("CoreStoreQueryPlanner")
            && planner_body.contains("ObjectListingAuthzCandidateReader"),
        "object listing must use the shared planner and its revision-bound inherited/object authz candidate reader"
    );
}

#[test]
fn object_rpc_does_not_bypass_object_manager_listing_guard() {
    let source = workspace_file("anvil-core/src/services/object/rpc.rs");
    let list_body = function_body(
        &source,
        "async fn list_objects(",
        "async fn list_object_versions(",
    );

    assert!(
        list_body.contains(".list_objects_for_tenant("),
        "public ListObjects RPC must route through ObjectManager so the planner/authz guard is mandatory"
    );
    assert!(
        !list_body.contains("list_current_object_metadata"),
        "public ListObjects RPC must not call CoreStore metadata listing directly"
    );
}
