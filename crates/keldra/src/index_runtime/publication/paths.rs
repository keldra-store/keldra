use tonic::Status;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ArtifactPathKind {
    Current,
    RebuildMutable,
    Immutable,
    ProjectionCurrent,
    ProjectionCatalogMutable,
    ProjectionImmutable,
    AccountingMutable,
}

impl ArtifactPathKind {
    pub(super) const fn is_current(self) -> bool {
        matches!(
            self,
            Self::Current | Self::ProjectionCurrent | Self::ProjectionCatalogMutable
        )
    }

    pub(super) const fn is_immutable(self) -> bool {
        matches!(self, Self::Immutable | Self::ProjectionImmutable)
    }
}

pub(super) fn parse_artifact_path(
    path: &str,
    expected_index: u64,
) -> Result<ArtifactPathKind, Status> {
    if crate::accounting::is_artifact_path(path, expected_index) {
        return Ok(ArtifactPathKind::AccountingMutable);
    }
    if path.starts_with("_keldra/index-projections/v6/") {
        if let Ok(parsed) = keldra_index::v6::parse_projection_artifact_path(path) {
            let routing_id = match (parsed.kind, parsed.partition, parsed.content_hash) {
                (keldra_index::v6::ProjectionArtifactKind::Current, Some(partition), None) => {
                    keldra_index::v6::projection_routing_id(partition)
                }
                (kind, None, Some(content_hash)) => {
                    keldra_index::v6::projection_artifact_routing_id(
                        parsed.family_id,
                        kind,
                        content_hash,
                    )
                    .map_err(|error| Status::invalid_argument(error.to_string()))?
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "projection artifact path has an invalid mutable/immutable identity",
                    ));
                }
            };
            if routing_id != expected_index {
                return Err(Status::invalid_argument(
                    "projection artifact routing identity does not match its canonical identity",
                ));
            }
            return Ok(match parsed.kind {
                keldra_index::v6::ProjectionArtifactKind::Current => {
                    ArtifactPathKind::ProjectionCurrent
                }
                keldra_index::v6::ProjectionArtifactKind::Pack
                | keldra_index::v6::ProjectionArtifactKind::StreamPage
                | keldra_index::v6::ProjectionArtifactKind::ComponentPage
                | keldra_index::v6::ProjectionArtifactKind::QueryRunPack
                | keldra_index::v6::ProjectionArtifactKind::QueryRunStreamPage
                | keldra_index::v6::ProjectionArtifactKind::Generation => {
                    ArtifactPathKind::ProjectionImmutable
                }
            });
        }
        let parsed = keldra_index::v6::parse_projection_catalog_path(path)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let routing_generation = parsed
            .physical_catalog_generation
            .unwrap_or(parsed.family_id);
        if keldra_index::v6::projection_catalog_routing_id(parsed.family_id, routing_generation)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            != expected_index
        {
            return Err(Status::invalid_argument(
                "projection catalog routing identity does not match its full identity",
            ));
        }
        return Ok(ArtifactPathKind::ProjectionCatalogMutable);
    }
    Err(Status::invalid_argument(
        "index artifact path is outside the format-v6 projection namespace",
    ))
}

fn parse_canonical_u64(value: &str) -> Option<u64> {
    let parsed = value.parse::<u64>().ok()?;
    (parsed != 0 && parsed.to_string() == value).then_some(parsed)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn index_definition_name(path: &str) -> Option<&str> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["_keldra", "indices", "v4", "definitions", name]
            if !name.is_empty()
                && name.len() <= 255
                && *name != "."
                && *name != ".."
                && !name.contains(['/', '\0']) =>
        {
            Some(name)
        }
        _ => None,
    }
}

pub(crate) fn manifest_path(index_id: u64, digest: [u8; 32]) -> String {
    format!(
        "_keldra/indices/v4/{index_id}/manifests/{}",
        hex::encode(digest)
    )
}

pub(crate) fn artifact_path(index_id: u64, digest: [u8; 32]) -> String {
    format!(
        "_keldra/indices/v4/{index_id}/artifacts/{}",
        hex::encode(digest)
    )
}

pub(crate) fn artifact_hash_from_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    let parts = path.split('/').collect::<Vec<_>>();
    let digest = match parts.as_slice() {
        [
            "_keldra",
            "indices",
            "v4",
            encoded_index,
            "artifacts",
            digest,
        ] if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest) => {
            *digest
        }
        _ => return None,
    };
    hex::decode(digest).ok()?.try_into().ok()
}

pub(super) fn immutable_content_hash_from_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    if let Some(hash) = artifact_hash_from_path(index_id, path) {
        return Some(hash);
    }
    let parts = path.split('/').collect::<Vec<_>>();
    let digest = match parts.as_slice() {
        [
            "_keldra",
            "indices",
            "v4",
            encoded_index,
            "manifests",
            digest,
        ] if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest) => {
            *digest
        }
        _ => return None,
    };
    hex::decode(digest).ok()?.try_into().ok()
}

pub(crate) fn manifest_hash_from_path(index_id: u64, path: &str) -> Option<[u8; 32]> {
    let hash = immutable_content_hash_from_path(index_id, path)?;
    is_manifest_artifact_path(index_id, path).then_some(hash)
}

pub(crate) fn is_manifest_artifact_path(index_id: u64, path: &str) -> bool {
    let parts = path.split('/').collect::<Vec<_>>();
    matches!(
        parts.as_slice(),
        ["_keldra", "indices", "v4", encoded_index, "manifests", digest]
            if parse_canonical_u64(encoded_index) == Some(index_id) && valid_digest(digest)
    )
}

pub(crate) fn current_path(index_id: u64) -> String {
    format!("_keldra/indices/v4/{index_id}/current")
}

pub(crate) fn rebuild_path(index_id: u64) -> String {
    format!("_keldra/indices/v4/{index_id}/rebuild")
}

pub(crate) fn is_index_recovery_path(path: &str, index_id: u64) -> bool {
    parse_artifact_path(path, index_id).is_ok()
}
