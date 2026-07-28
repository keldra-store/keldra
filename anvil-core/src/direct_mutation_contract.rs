use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationClass {
    CanonicalMvcc,
    InternalCoreMeta,
    ConsensusControl,
}

#[test]
fn production_rocksdb_mutations_are_confined_to_reviewed_storage_boundaries() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let allowed = BTreeMap::from([
        (
            "anvil-core/src/mvcc_store/background_work.rs",
            MutationClass::CanonicalMvcc,
        ),
        (
            "anvil-core/src/mvcc_store/garbage_collection.rs",
            MutationClass::CanonicalMvcc,
        ),
        (
            "anvil-core/src/mvcc_store/store_access.rs",
            MutationClass::CanonicalMvcc,
        ),
        (
            "anvil-core/src/mvcc_open_transactions.rs",
            MutationClass::CanonicalMvcc,
        ),
        (
            "anvil-core/src/core_store/meta.rs",
            MutationClass::InternalCoreMeta,
        ),
        (
            "crates/anvil-mvcc-consensus/src/storage.rs",
            MutationClass::ConsensusControl,
        ),
    ]);
    let mut offenders = Vec::new();
    for root in [
        workspace.join("anvil-core/src"),
        workspace.join("crates/anvil-mvcc-consensus/src"),
    ] {
        inspect_tree(&workspace, &root, &allowed, &mut offenders);
    }
    assert!(
        offenders.is_empty(),
        "unclassified direct RocksDB mutation boundaries: {offenders:#?}"
    );
}

fn inspect_tree(
    workspace: &Path,
    directory: &Path,
    allowed: &BTreeMap<&str, MutationClass>,
    offenders: &mut Vec<String>,
) {
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            inspect_tree(workspace, &path, allowed, offenders);
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
            || is_test_source(&path)
        {
            continue;
        }
        let source = fs::read_to_string(&path).unwrap();
        let direct_mutation = [
            ".put_cf(",
            ".delete_cf(",
            ".put_cf_opt(",
            ".delete_cf_opt(",
            ".write_opt(",
        ]
        .iter()
        .any(|needle| source.contains(needle));
        if direct_mutation {
            let relative = path.strip_prefix(workspace).unwrap().to_string_lossy();
            if !allowed.contains_key(relative.as_ref()) {
                offenders.push(relative.into_owned());
            }
        }
    }
}

fn is_test_source(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
        || path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                name.ends_with("_tests.rs") || name == "direct_mutation_contract.rs"
            })
}
