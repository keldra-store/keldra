use std::collections::BTreeMap;

use keldra_index::v4::SegmentDescriptor;
use keldra_store::{BlobRef, ObjectKey, VersionId};
use tonic::Status;

use crate::cluster_object_read::ClusterObjectReader;
use crate::index_runtime::committed_view::LocatorPackOwnership;
use crate::index_service::StoredIndexDefinition;

use super::{CommitManifestReference, IndexCommitManifest, IndexPointerCasClass, LocatorRoot};
use crate::index_runtime::publication::IndexCurrentMutationGuard;

pub(super) async fn revalidate_candidate(
    reader: &ClusterObjectReader,
    definition: &StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    segments: &[SegmentDescriptor],
    locator_roots: &[LocatorRoot],
    rooted: Option<&IndexCommitManifest>,
    manifest: Option<&CommitManifestReference>,
    cas_class: IndexPointerCasClass,
    _guard: &IndexCurrentMutationGuard,
) -> Result<(), Status> {
    let rooted = rooted.map(manifest_packs).transpose()?.unwrap_or_default();
    // A rebuild's complete immutable graph is already protected and exactly
    // checked by its durable rebuild root. Re-reading that graph here would
    // make the serving CAS O(total index size).
    let packs = if cas_class == IndexPointerCasClass::Rebuild {
        BTreeMap::new()
    } else {
        unrooted_packs(
            candidate_packs(definition.index_id, segments, locator_roots)?,
            &rooted,
        )
    };
    for ((path, version), (hash, length)) in packs {
        revalidate_exact(
            reader,
            definition,
            tenant_id,
            bucket_id,
            path,
            VersionId(version),
            &BlobRef { hash, length },
            "index artifact pack",
        )
        .await?;
    }
    if let Some(reference) = manifest {
        reference
            .validate(definition.index_id)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        revalidate_exact(
            reader,
            definition,
            tenant_id,
            bucket_id,
            &reference.path,
            reference.object_version,
            &reference.blob,
            "index commit manifest",
        )
        .await?;
    }
    Ok(())
}

type ExactPacks<'a> = BTreeMap<(&'a str, u64), ([u8; 32], u64)>;

fn unrooted_packs<'a>(candidate: ExactPacks<'a>, rooted: &ExactPacks<'_>) -> ExactPacks<'a> {
    candidate
        .into_iter()
        .filter(|(identity, blob)| rooted.get(identity).is_none_or(|rooted| rooted != blob))
        .collect()
}

fn manifest_packs(manifest: &IndexCommitManifest) -> Result<ExactPacks<'_>, Status> {
    candidate_packs(
        manifest.index_id,
        &manifest.segments,
        &manifest.locator_roots,
    )
}

fn candidate_packs<'a>(
    index_id: u64,
    segments: &'a [SegmentDescriptor],
    locator_roots: &'a [LocatorRoot],
) -> Result<ExactPacks<'a>, Status> {
    let mut packs = ExactPacks::new();
    for pack in segments
        .iter()
        .flat_map(|segment| segment.packs.iter())
        .chain(
            locator_roots
                .iter()
                .flat_map(|root| match &root.pack_ownership {
                    LocatorPackOwnership::Segment => [].as_slice(),
                    LocatorPackOwnership::Standalone(packs) => packs.as_slice(),
                }),
        )
    {
        pack.validate(index_id)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let identity = (pack.object_content_hash, pack.object_length);
        if packs
            .insert((&pack.path, pack.object_version), identity)
            .is_some_and(|previous| previous != identity)
        {
            return Err(Status::data_loss(
                "one exact index pack version has conflicting identities",
            ));
        }
    }
    Ok(packs)
}

async fn revalidate_exact(
    reader: &ClusterObjectReader,
    definition: &StoredIndexDefinition,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
    version: VersionId,
    blob: &BlobRef,
    kind: &str,
) -> Result<(), Status> {
    let key = ObjectKey::new(&definition.tenant, &definition.bucket, path)
        .map_err(|error| Status::internal(error.to_string()))?;
    let opened = reader
        .open_stable(&key, tenant_id, bucket_id, Some(version))
        .await?
        .ok_or_else(|| Status::data_loss(format!("exact {kind} is absent")))?;
    if opened.version.id != version
        || opened.version.deleted
        || opened.version.blob.as_ref() != Some(blob)
    {
        return Err(Status::data_loss(format!(
            "exact {kind} differs from its committed reference"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_identity_includes_version_and_blob() {
        let first = ("pack", 7_u64, [1_u8; 32], 64_u64);
        let same_path_new_version = ("pack", 8_u64, [1_u8; 32], 64_u64);
        let same_version_new_blob = ("pack", 7_u64, [2_u8; 32], 64_u64);
        assert_ne!(
            (first.0, first.1),
            (same_path_new_version.0, same_path_new_version.1)
        );
        assert_ne!(
            (first.2, first.3),
            (same_version_new_blob.2, same_version_new_blob.3)
        );
    }

    #[test]
    fn unchanged_base_packs_are_not_revalidated() {
        let rooted = BTreeMap::from([
            (("base", 3), ([1; 32], 128)),
            (("retained", 4), ([2; 32], 256)),
        ]);
        let candidate = BTreeMap::from([
            (("base", 3), ([1; 32], 128)),
            (("retained", 4), ([9; 32], 256)),
            (("new", 5), ([3; 32], 512)),
        ]);
        assert_eq!(
            unrooted_packs(candidate, &rooted),
            BTreeMap::from([
                (("new", 5), ([3; 32], 512)),
                (("retained", 4), ([9; 32], 256)),
            ])
        );
    }
}
