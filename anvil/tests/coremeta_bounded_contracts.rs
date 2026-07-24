use std::path::Path;

fn workspace_file(path: &str) -> String {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate must be inside the workspace");
    std::fs::read_to_string(workspace.join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
}

#[test]
fn coremeta_store_exposes_only_bounded_scan_contracts() {
    let source = workspace_file("anvil-core/src/core_store/meta.rs");

    assert!(!source.contains("pub fn scan_prefix("));
    assert!(!source.contains("scan_all_encoded_rows"));
    assert!(source.contains("pub fn scan_prefix_page("));
    assert!(source.contains("pub fn scan_encoded_rows_page("));
    assert!(source.contains("validate_scan_limit(limit)?"));
}

#[test]
fn owned_coremeta_surfaces_do_not_call_unbounded_scans() {
    for path in [
        "anvil-core/src/core_store/local_internal_coremeta.rs",
        "anvil-core/src/core_store/local_refcounts.rs",
        "anvil-core/src/core_store/local_admission.rs",
        "anvil-core/src/core_store/local_admission/point_state.rs",
        "anvil-core/src/authz_realm_schema.rs",
        "anvil-core/src/authz_schema.rs",
        "anvil-core/src/index_coremeta.rs",
    ] {
        let source = workspace_file(path);
        assert!(
            !source.contains(".scan_prefix("),
            "{path} retained an unbounded prefix scan"
        );
        assert!(
            !source.contains("scan_all_encoded_rows"),
            "{path} retained a full-store scan"
        );
        assert!(
            !source.contains("scan_coremeta_prefix("),
            "{path} retained the unbounded visibility-filtered scan wrapper"
        );
    }
}

#[test]
fn feature_collection_contracts_carry_cursor_and_limit() {
    let authz_schema = workspace_file("anvil-core/src/authz_schema.rs");
    let realm_schema = workspace_file("anvil-core/src/authz_realm_schema.rs");
    let index = workspace_file("anvil-core/src/index_coremeta.rs");

    for (label, source) in [
        ("namespace schemas", authz_schema),
        ("realm schemas", realm_schema),
        ("index segments", index),
    ] {
        assert!(
            source.contains("after_tuple_key: Option<&[u8]>")
                && source.contains("page_size: usize"),
            "{label} page contract must require a cursor and page size"
        );
        assert!(
            source.contains("page_size + 1"),
            "{label} page contract must bound continuation detection"
        );
    }

    let index = workspace_file("anvil-core/src/index_coremeta.rs");
    for point_key in [
        "index_segment_latest",
        "index_segment_family_latest",
        "index_segment_generation",
        "index_segment_ref",
    ] {
        assert!(
            index.contains(point_key),
            "index lookup must retain the {point_key} point row"
        );
    }
}
