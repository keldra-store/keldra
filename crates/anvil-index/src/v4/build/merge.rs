use std::collections::BTreeMap;

use crate::IndexError;

use super::super::ObjectIdentity;
use super::ProjectedSource;

#[derive(Clone, Debug, PartialEq)]
pub enum MergeMutation {
    Upsert(ProjectedSource),
    Delete(ObjectIdentity),
}

impl MergeMutation {
    fn identity(&self) -> &ObjectIdentity {
        match self {
            Self::Upsert(source) => &source.source_identity,
            Self::Delete(identity) => identity,
        }
    }

    fn validate(&self) -> Result<(), IndexError> {
        match self {
            Self::Upsert(source) => source.validate(),
            Self::Delete(identity) => identity.validate(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MergedSources {
    pub live: Vec<ProjectedSource>,
    /// Latest deleted source identities. These must remain in the locator
    /// state until a newer live version supersedes them; dropping them would
    /// permit an older segment to resurrect deleted results.
    pub tombstones: Vec<ObjectIdentity>,
}

/// Deterministically fold unordered source mutations. Highest source version
/// wins, while two different values at one `(path, version)` are corruption.
pub fn merge_mutations<I>(mutations: I) -> Result<MergedSources, IndexError>
where
    I: IntoIterator<Item = MergeMutation>,
{
    let mut latest = BTreeMap::<String, MergeMutation>::new();
    for mutation in mutations {
        mutation.validate()?;
        let identity = mutation.identity();
        match latest.get(&identity.path) {
            None => {
                latest.insert(identity.path.clone(), mutation);
            }
            Some(previous) if previous.identity().version < identity.version => {
                latest.insert(identity.path.clone(), mutation);
            }
            Some(previous) if previous.identity().version > identity.version => {}
            Some(previous) if previous == &mutation => {}
            Some(_) => {
                return Err(IndexError::InvalidFormat(
                    "conflicting source mutations at one version",
                ));
            }
        }
    }
    let mut output = MergedSources::default();
    for mutation in latest.into_values() {
        match mutation {
            MergeMutation::Upsert(source) => output.live.push(source),
            MergeMutation::Delete(identity) => output.tombstones.push(identity),
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v4::build::ProjectedRecord;

    fn source(path: &str, version: u64) -> ProjectedSource {
        ProjectedSource {
            source_identity: ObjectIdentity {
                path: path.into(),
                version,
            },
            records: vec![ProjectedRecord {
                result_identity: None,
                order_key: Vec::new(),
                terms: Vec::new(),
                columns: Vec::new(),
                stored_fields: None,
                vectors: Vec::new(),
                field_lengths: Vec::new(),
            }],
        }
    }

    #[test]
    fn tombstones_win_without_input_order_dependence() {
        let mutations = vec![
            MergeMutation::Upsert(source("a", 7)),
            MergeMutation::Delete(ObjectIdentity {
                path: "a".into(),
                version: 8,
            }),
            MergeMutation::Upsert(source("b", 2)),
        ];
        let forward = merge_mutations(mutations.clone()).unwrap();
        let reverse = merge_mutations(mutations.into_iter().rev()).unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(forward.live[0].source_identity.path, "b");
        assert_eq!(forward.tombstones[0].path, "a");
    }

    #[test]
    fn conflicting_equal_versions_fail_closed() {
        assert!(
            merge_mutations([
                MergeMutation::Upsert(source("a", 7)),
                MergeMutation::Delete(ObjectIdentity {
                    path: "a".into(),
                    version: 7,
                }),
            ])
            .is_err()
        );
    }
}
