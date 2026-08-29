//! Logical alias identities and their canonical exact-version fetch paths.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactSourcePath {
    pub(super) logical_path: String,
    pub(super) canonical_path: String,
    pub(super) version: u64,
    pub(super) deleted: bool,
}

pub(super) fn ordinary_journal_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    changes: &[IndexJournalChange],
) -> Vec<ExactSourcePath> {
    let mut paths = BTreeMap::<String, (String, u64, bool)>::new();
    for entry in changes {
        let (change_tenant_id, change_bucket_id, path, canonical, version, deleted) =
            match &entry.change {
                LocalChange::ObjectHead(change) if change.program_commit_cursor.is_none() => (
                    change.tenant_id,
                    change.bucket_id,
                    change.exact_path.as_str(),
                    change
                        .canonical_path
                        .as_deref()
                        .unwrap_or(change.exact_path.as_str()),
                    change.path_version.0,
                    change.kind == keldra_store::ObjectHeadChangeKind::Delete,
                ),
                LocalChange::RetainedVersionDeleted(change)
                    if change.resulting_head_version.is_some() =>
                {
                    (
                        change.tenant_id,
                        change.bucket_id,
                        change.exact_path.as_str(),
                        change.exact_path.as_str(),
                        change.resulting_head_version.unwrap().0,
                        false,
                    )
                }
                _ => continue,
            };
        if change_tenant_id == tenant_id
            && change_bucket_id == bucket_id
            && path_matches_prefix(path, path_prefix)
            && !contains_reserved_segment(path)
        {
            // A source journal, not the numeric VersionId, is the ordering
            // authority. Repeated mutations to one path coalesce to the last
            // record in this exact processed interval.
            paths.insert(path.to_owned(), (canonical.to_owned(), version, deleted));
        }
    }
    paths
        .into_iter()
        .map(
            |(logical_path, (canonical_path, version, deleted))| ExactSourcePath {
                logical_path,
                canonical_path,
                version,
                deleted,
            },
        )
        .collect()
}

pub(super) fn atomic_source_paths(
    tenant_id: u64,
    bucket_id: u64,
    path_prefix: &str,
    batch: &keldra_store::AtomicBatchPublished,
) -> Vec<ExactSourcePath> {
    // Atomic mutation descriptors are canonically sorted at authoritative
    // ingress. Preserve that order and coalesce adjacent duplicates without a
    // second tree allocation.
    let relevant_count = batch
        .mutations
        .iter()
        .filter(|mutation| {
            mutation.tenant_id == tenant_id
                && mutation.bucket_id == bucket_id
                && path_matches_prefix(&mutation.exact_path, path_prefix)
                && !contains_reserved_segment(&mutation.exact_path)
        })
        .count();
    let mut paths = Vec::<ExactSourcePath>::with_capacity(relevant_count);
    for mutation in &batch.mutations {
        if mutation.tenant_id == tenant_id
            && mutation.bucket_id == bucket_id
            && path_matches_prefix(&mutation.exact_path, path_prefix)
            && !contains_reserved_segment(&mutation.exact_path)
        {
            if let Some(last) = paths.last_mut()
                && last.logical_path == mutation.exact_path
            {
                if mutation.path_version.0 >= last.version {
                    last.canonical_path = mutation
                        .canonical_path
                        .clone()
                        .unwrap_or_else(|| mutation.exact_path.clone());
                    last.version = mutation.path_version.0;
                    last.deleted = mutation.deleted;
                }
            } else {
                paths.push(ExactSourcePath {
                    logical_path: mutation.exact_path.clone(),
                    canonical_path: mutation
                        .canonical_path
                        .clone()
                        .unwrap_or_else(|| mutation.exact_path.clone()),
                    version: mutation.path_version.0,
                    deleted: mutation.deleted,
                });
            }
        }
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(
        offset: u64,
        path: &str,
        canonical: Option<&str>,
        version: u64,
        deleted: bool,
    ) -> IndexJournalChange {
        IndexJournalChange {
            node: NodeId(1),
            change: LocalChange::ObjectHead(keldra_store::ObjectHeadChange {
                offset,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: path.into(),
                canonical_path: canonical.map(str::to_owned),
                path_version: VersionId(version),
                kind: if deleted {
                    keldra_store::ObjectHeadChangeKind::Delete
                } else {
                    keldra_store::ObjectHeadChangeKind::Put
                },
                program_commit_cursor: None,
                reference_deltas: Vec::new(),
                accounting_transition: None,
                definition_transition: None,
            }),
        }
    }

    #[test]
    fn ordinary_coalescing_uses_journal_order_not_version_magnitude() {
        assert_eq!(
            ordinary_journal_source_paths(
                2,
                3,
                "objects/",
                &[
                    change(8, "objects/current", None, 100, false),
                    change(9, "objects/current", None, 90, false),
                ],
            ),
            vec![ExactSourcePath {
                logical_path: "objects/current".into(),
                canonical_path: "objects/current".into(),
                version: 90,
                deleted: false,
            }],
        );
    }

    #[test]
    fn alias_changes_preserve_logical_identity_and_deletion() {
        for (deleted, version) in [(false, 90), (true, 91)] {
            assert_eq!(
                ordinary_journal_source_paths(
                    2,
                    3,
                    "objects/",
                    &[change(
                        8,
                        "objects/alias",
                        Some("objects/target"),
                        version,
                        deleted,
                    )],
                ),
                vec![ExactSourcePath {
                    logical_path: "objects/alias".into(),
                    canonical_path: "objects/target".into(),
                    version,
                    deleted,
                }],
            );
        }
    }

    #[test]
    fn atomic_alias_delete_retains_the_logical_delete_bit() {
        let batch = keldra_store::AtomicBatchPublished {
            offset: 1,
            cursor: 2,
            bundle_hash: keldra_store::PreparedBundleHash([3; 32]),
            affected_routes: vec![keldra_store::AtomicBatchRoute {
                tenant_id: 2,
                bucket_id: 3,
            }],
            mutations: vec![keldra_store::AtomicBatchMutation {
                source_id: keldra_store::SourceId {
                    node_id: 1,
                    source_epoch: [4; 32],
                },
                source_journal_position: 5,
                tenant_id: 2,
                bucket_id: 3,
                exact_path: "objects/alias".into(),
                canonical_path: Some("objects/target".into()),
                path_version: VersionId(9),
                deleted: true,
            }],
        };
        assert_eq!(
            atomic_source_paths(2, 3, "objects/", &batch),
            vec![ExactSourcePath {
                logical_path: "objects/alias".into(),
                canonical_path: "objects/target".into(),
                version: 9,
                deleted: true,
            }],
        );
    }
}
