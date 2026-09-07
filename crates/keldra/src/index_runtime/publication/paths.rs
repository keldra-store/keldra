use tonic::Status;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ArtifactPathKind {
    ProjectionCurrent,
    ProjectionCatalogMutable,
    ProjectionImmutable,
    AccountingMutable,
}

impl ArtifactPathKind {
    pub(super) const fn is_current(self) -> bool {
        matches!(
            self,
            Self::ProjectionCurrent | Self::ProjectionCatalogMutable
        )
    }

    pub(super) const fn is_immutable(self) -> bool {
        matches!(self, Self::ProjectionImmutable)
    }
}

pub(super) fn parse_artifact_path(
    path: &str,
    expected_index: u64,
) -> Result<ArtifactPathKind, Status> {
    if crate::accounting::is_artifact_path(path, expected_index) {
        return Ok(ArtifactPathKind::AccountingMutable);
    }
    if path.starts_with("_keldra/index-projections/v1/") {
        if let Ok(parsed) = keldra_index::v1::parse_projection_artifact_path(path) {
            let routing_id = match (parsed.kind, parsed.partition, parsed.content_hash) {
                (keldra_index::v1::ProjectionArtifactKind::Current, Some(partition), None) => {
                    keldra_index::v1::projection_routing_id(partition)
                }
                (kind, None, Some(content_hash)) => {
                    keldra_index::v1::projection_artifact_routing_id(
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
                keldra_index::v1::ProjectionArtifactKind::Current => {
                    ArtifactPathKind::ProjectionCurrent
                }
                keldra_index::v1::ProjectionArtifactKind::Pack
                | keldra_index::v1::ProjectionArtifactKind::StreamPage
                | keldra_index::v1::ProjectionArtifactKind::ComponentPage
                | keldra_index::v1::ProjectionArtifactKind::QueryRunPack
                | keldra_index::v1::ProjectionArtifactKind::QueryRunStreamPage
                | keldra_index::v1::ProjectionArtifactKind::Generation => {
                    ArtifactPathKind::ProjectionImmutable
                }
            });
        }
        let parsed = keldra_index::v1::parse_projection_catalog_path(path)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        // The family directory and every generation-specific activation are
        // mutable state owned by one stable family lifecycle authority. The
        // physical generation in an activation path identifies catalog state;
        // it must not change routing or split that lifecycle across nodes.
        if keldra_index::v1::projection_catalog_routing_id(parsed.family_id, parsed.family_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?
            != expected_index
        {
            return Err(Status::invalid_argument(
                "projection catalog routing identity does not match its stable family authority",
            ));
        }
        return Ok(ArtifactPathKind::ProjectionCatalogMutable);
    }
    Err(Status::invalid_argument(
        "index artifact path is outside the format-v1 projection namespace",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_specific_activation_uses_stable_family_routing_authority() {
        let family_id = [7; 32];
        let physical_catalog_generation = [9; 32];
        let path = keldra_index::v1::projection_catalog_activation_path(
            family_id,
            physical_catalog_generation,
        );
        let family_routing =
            keldra_index::v1::projection_catalog_routing_id(family_id, family_id).unwrap();
        let generation_routing =
            keldra_index::v1::projection_catalog_routing_id(family_id, physical_catalog_generation)
                .unwrap();

        assert_ne!(family_routing, generation_routing);
        assert_eq!(
            parse_artifact_path(&path, family_routing).unwrap(),
            ArtifactPathKind::ProjectionCatalogMutable
        );
        assert!(parse_artifact_path(&path, generation_routing).is_err());
    }
}
