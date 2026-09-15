//! Ordered, bounded source-version reads for one v1 preparation window.

use keldra_store::{MAX_OBJECT_RECORD_EXPORT_RECORDS, ObjectKey, Version, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;

use super::catalog::PhysicalCatalogRecipe;
use super::source::{IndexBuildObject, IndexSourceMutation};

#[derive(Clone, Copy)]
pub(super) struct ExactMutationRequest<'a> {
    pub(super) path: &'a str,
    pub(super) canonical_path: Option<&'a str>,
    pub(super) version: u64,
    pub(super) deleted: bool,
}

/// Loads the immutable versions named by one ordered producer window.
///
/// Tombstones do not require a descriptor read. Non-deleted entries use the
/// store's native exact-version batch boundary, while the returned mutations
/// retain the caller's ordering and exact identities.
pub(super) async fn load_exact_mutations(
    reader: &ClusterObjectReader,
    recipe: &PhysicalCatalogRecipe,
    requests: &[ExactMutationRequest<'_>],
    maximum_parallelism: usize,
) -> Result<Vec<IndexSourceMutation>, Status> {
    let mut sources = std::iter::repeat_with(|| None)
        .take(requests.len())
        .collect::<Vec<_>>();
    let reads = requests
        .iter()
        .enumerate()
        .filter(|(_, request)| !request.deleted)
        .map(|(index, request)| {
            let key = ObjectKey::new(&recipe.storage_tenant, &recipe.bucket, request.path)
                .map_err(|error| Status::data_loss(error.to_string()))?;
            Ok((index, key, VersionId(request.version)))
        })
        .collect::<Result<Vec<_>, Status>>()?;

    let mut pending = std::collections::VecDeque::from(
        reads
            .chunks(MAX_OBJECT_RECORD_EXPORT_RECORDS as usize)
            .map(<[_]>::to_vec)
            .collect::<Vec<_>>(),
    );
    let mut active = tokio::task::JoinSet::new();
    let maximum_parallelism = maximum_parallelism.max(1);
    while !pending.is_empty() || !active.is_empty() {
        while active.len() < maximum_parallelism {
            let Some(read_batch) = pending.pop_front() else {
                break;
            };
            let reader = reader.clone();
            let tenant_id = recipe.family.tenant_id;
            let bucket_id = recipe.family.bucket_id;
            active.spawn(async move {
                let keys = read_batch
                    .iter()
                    .map(|(_, key, _)| key.clone())
                    .collect::<Vec<_>>();
                let versions = read_batch
                    .iter()
                    .map(|(_, _, version)| *version)
                    .collect::<Vec<_>>();
                let selected = reader
                    .exact_versions_stable(&keys, &versions, tenant_id, bucket_id)
                    .await?;
                Ok::<_, Status>((read_batch, selected))
            });
        }
        if let Some(joined) = active.join_next().await {
            let (read_batch, selected) = joined.map_err(|error| {
                Status::internal(format!("v1 exact source read failed: {error}"))
            })??;
            if selected.len() != read_batch.len() {
                return Err(Status::data_loss(
                    "v1 exact source batch returned the wrong result count",
                ));
            }
            for ((index, _, _), selected) in read_batch.into_iter().zip(selected) {
                sources[index] = Some(source_from_version(requests[index], selected)?);
            }
        }
    }

    for (index, request) in requests.iter().copied().enumerate() {
        if request.deleted {
            sources[index] = Some(source_from_version(request, None)?);
        }
    }
    sources
        .into_iter()
        .map(|source| {
            source.ok_or_else(|| Status::data_loss("v1 exact source batch omitted a request"))
        })
        .collect()
}

fn source_from_version(
    request: ExactMutationRequest<'_>,
    selected: Option<Version>,
) -> Result<IndexSourceMutation, Status> {
    let identity = keldra_index::v1::ObjectIdentity {
        path: request.path.into(),
        version: request.version,
    };
    if request.deleted {
        return Ok(IndexSourceMutation::Remove {
            identity,
            canonical_path: request.canonical_path.map(str::to_owned),
        });
    }
    let selected =
        selected.ok_or_else(|| Status::failed_precondition("v1 exact source version is absent"))?;
    if selected.deleted || selected.id.0 != request.version {
        return Err(Status::data_loss("v1 exact source version mismatch"));
    }
    let blob = selected
        .blob
        .ok_or_else(|| Status::data_loss("v1 source blob is absent"))?;
    Ok(IndexSourceMutation::Upsert(IndexBuildObject {
        path: request.path.into(),
        canonical_path: request.canonical_path.map(str::to_owned),
        version: request.version,
        content_type: selected.content_type,
        content_hash: blob.hash,
        content_length: blob.length,
        committed_at_unix_millis: selected.committed_at_unix_millis,
    }))
}

#[cfg(test)]
mod tests {
    use keldra_store::{BlobRef, Version, VersionId};
    use tonic::Code;

    use super::{ExactMutationRequest, source_from_version};
    use crate::index_runtime::source::IndexSourceMutation;

    fn request(deleted: bool) -> ExactMutationRequest<'static> {
        ExactMutationRequest {
            path: "objects/exact.json",
            canonical_path: Some("objects/canonical.json"),
            version: 17,
            deleted,
        }
    }

    fn version(id: u64) -> Version {
        Version {
            id: VersionId(id),
            blob: Some(BlobRef {
                hash: [9; 32],
                length: 41,
            }),
            content_type: Some("application/json".into()),
            deleted: false,
            committed_at_unix_millis: 23,
            protected_link_descriptor: false,
        }
    }

    #[test]
    fn exact_source_conversion_preserves_requested_identity_and_metadata() {
        let source = source_from_version(request(false), Some(version(17))).unwrap();
        let IndexSourceMutation::Upsert(source) = source else {
            panic!("expected upsert")
        };
        assert_eq!(source.path, "objects/exact.json");
        assert_eq!(
            source.canonical_path.as_deref(),
            Some("objects/canonical.json")
        );
        assert_eq!(source.version, 17);
        assert_eq!(source.content_hash, [9; 32]);
        assert_eq!(source.content_length, 41);
        assert_eq!(source.committed_at_unix_millis, 23);
    }

    #[test]
    fn tombstone_does_not_require_a_selected_version() {
        let source = source_from_version(request(true), None).unwrap();
        let IndexSourceMutation::Remove {
            identity,
            canonical_path,
        } = source
        else {
            panic!("expected removal")
        };
        assert_eq!(identity.path, "objects/exact.json");
        assert_eq!(identity.version, 17);
        assert_eq!(canonical_path.as_deref(), Some("objects/canonical.json"));
    }

    #[test]
    fn exact_source_conversion_preserves_absent_and_mismatch_errors() {
        let absent = source_from_version(request(false), None).unwrap_err();
        assert_eq!(absent.code(), Code::FailedPrecondition);
        assert_eq!(absent.message(), "v1 exact source version is absent");

        let mismatch = source_from_version(request(false), Some(version(18))).unwrap_err();
        assert_eq!(mismatch.code(), Code::DataLoss);
        assert_eq!(mismatch.message(), "v1 exact source version mismatch");
    }
}
