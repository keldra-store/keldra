use crate::IndexError;

use super::ProjectionPartitionIdentity;

const ROOT: &str = "_keldra/index-projections/v6";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionArtifactKind {
    Pack,
    StreamPage,
    ComponentPage,
    QueryRunPack,
    QueryRunStreamPage,
    Generation,
    Current,
}

/// Immutable artifacts are family-scoped. Only a mutable current has a
/// partition identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionArtifactPath {
    pub family_id: [u8; 32],
    pub partition: Option<ProjectionPartitionIdentity>,
    pub kind: ProjectionArtifactKind,
    pub content_hash: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionCatalogPathKind {
    PartitionDirectory,
    Activation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionCatalogPath {
    pub family_id: [u8; 32],
    pub physical_catalog_generation: Option<[u8; 32]>,
    pub kind: ProjectionCatalogPathKind,
}

pub fn projection_pack_path(partition: ProjectionPartitionIdentity, hash: [u8; 32]) -> String {
    immutable_path(partition.family_id, "packs", hash)
}

pub fn projection_stream_page_path(
    partition: ProjectionPartitionIdentity,
    hash: [u8; 32],
) -> String {
    immutable_path(partition.family_id, "stream-pages", hash)
}

pub fn projection_component_page_path(
    partition: ProjectionPartitionIdentity,
    hash: [u8; 32],
) -> String {
    immutable_path(partition.family_id, "component-pages", hash)
}

/// Query-ready immutable mini-run bytes. They are family-scoped content
/// addressed artifacts, never a mutable per-definition index.
pub fn projection_query_run_pack_path(
    partition: ProjectionPartitionIdentity,
    hash: [u8; 32],
) -> String {
    immutable_path(partition.family_id, "query-run-packs", hash)
}

pub fn projection_query_run_stream_page_path(
    partition: ProjectionPartitionIdentity,
    hash: [u8; 32],
) -> String {
    immutable_path(partition.family_id, "query-run-stream-pages", hash)
}

pub fn projection_generation_path(
    partition: ProjectionPartitionIdentity,
    hash: [u8; 32],
) -> String {
    immutable_path(partition.family_id, "generations", hash)
}

pub fn projection_current_path(partition: ProjectionPartitionIdentity) -> String {
    format!("{}/current", partition_root(partition))
}

/// The family directory is stable across physical catalog generations.
pub fn projection_family_directory_path(family_id: [u8; 32]) -> String {
    format!("{ROOT}/{}/partitions", encode_hash(family_id))
}

pub fn projection_catalog_activation_path(
    family_id: [u8; 32],
    physical_catalog_generation: [u8; 32],
) -> String {
    format!(
        "{ROOT}/{}/catalogs/{}/activation",
        encode_hash(family_id),
        encode_hash(physical_catalog_generation)
    )
}

pub fn parse_projection_catalog_path(path: &str) -> Result<ProjectionCatalogPath, IndexError> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["_keldra", "index-projections", "v6", family, "partitions"] => Ok(ProjectionCatalogPath {
            family_id: decode_nonzero_hash(family)?,
            physical_catalog_generation: None,
            kind: ProjectionCatalogPathKind::PartitionDirectory,
        }),
        [
            "_keldra",
            "index-projections",
            "v6",
            family,
            "catalogs",
            catalog,
            "activation",
        ] => Ok(ProjectionCatalogPath {
            family_id: decode_nonzero_hash(family)?,
            physical_catalog_generation: Some(decode_nonzero_hash(catalog)?),
            kind: ProjectionCatalogPathKind::Activation,
        }),
        _ => Err(IndexError::InvalidDefinition(
            "projection catalog path is not canonical".into(),
        )),
    }
}

pub fn projection_catalog_routing_id(
    family_id: [u8; 32],
    physical_catalog_generation: [u8; 32],
) -> Result<u64, IndexError> {
    routing_id(
        b"keldra-index-projection-catalog-routing-v1\0",
        &[&family_id, &physical_catalog_generation],
    )
}

/// Non-authoritative in-process serialization key for one immutable,
/// family-scoped content-addressed artifact. It deliberately excludes the
/// partition incarnation: immutable artifacts can be shared across handoff
/// lineage and catalog transitions. Only `projection_routing_id` may be used
/// to serialize a mutable partition-current CAS.
pub fn projection_artifact_routing_id(
    family_id: [u8; 32],
    kind: ProjectionArtifactKind,
    content_hash: [u8; 32],
) -> Result<u64, IndexError> {
    if family_id == [0; 32] || content_hash == [0; 32] {
        return Err(IndexError::InvalidDefinition(
            "projection artifact routing identity is zero".into(),
        ));
    }
    let class = match kind {
        ProjectionArtifactKind::Pack => b"pack".as_slice(),
        ProjectionArtifactKind::StreamPage => b"stream-page".as_slice(),
        ProjectionArtifactKind::ComponentPage => b"component-page".as_slice(),
        ProjectionArtifactKind::QueryRunPack => b"query-run-pack".as_slice(),
        ProjectionArtifactKind::QueryRunStreamPage => b"query-run-stream-page".as_slice(),
        ProjectionArtifactKind::Generation => b"generation".as_slice(),
        ProjectionArtifactKind::Current => {
            return Err(IndexError::InvalidDefinition(
                "mutable projection current has no artifact routing identity".into(),
            ));
        }
    };
    routing_id(
        b"keldra-index-projection-artifact-routing-v1\0",
        &[&family_id, class, &content_hash],
    )
}

/// Non-authoritative in-process serialization key for one exact partition.
pub fn projection_routing_id(partition: ProjectionPartitionIdentity) -> u64 {
    let mut bytes = Vec::with_capacity(104);
    encode_partition_bytes(&mut bytes, partition);
    routing_id(b"keldra-index-projection-partition-routing-v1\0", &[&bytes])
        .expect("a validated partition has nonzero routing material")
}

pub fn parse_projection_artifact_path(path: &str) -> Result<ProjectionArtifactPath, IndexError> {
    let parts = path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [
            "_keldra",
            "index-projections",
            "v6",
            family,
            "artifacts",
            class,
            hash,
        ] => {
            let kind = match *class {
                "packs" => ProjectionArtifactKind::Pack,
                "stream-pages" => ProjectionArtifactKind::StreamPage,
                "component-pages" => ProjectionArtifactKind::ComponentPage,
                "query-run-packs" => ProjectionArtifactKind::QueryRunPack,
                "query-run-stream-pages" => ProjectionArtifactKind::QueryRunStreamPage,
                "generations" => ProjectionArtifactKind::Generation,
                _ => {
                    return Err(IndexError::InvalidDefinition(
                        "projection artifact path class is invalid".into(),
                    ));
                }
            };
            Ok(ProjectionArtifactPath {
                family_id: decode_nonzero_hash(family)?,
                partition: None,
                kind,
                content_hash: Some(decode_nonzero_hash(hash)?),
            })
        }
        [
            "_keldra",
            "index-projections",
            "v6",
            family,
            "partitions",
            source_node,
            source_epoch,
            producer_node,
            placement_term,
            placement_index,
            "current",
        ] => {
            let partition = ProjectionPartitionIdentity::new(
                decode_nonzero_hash(family)?,
                decode_u64(source_node)?,
                decode_nonzero_hash(source_epoch)?,
                decode_u64(producer_node)?,
                decode_u64(placement_term)?,
                decode_u64(placement_index)?,
            )?;
            Ok(ProjectionArtifactPath {
                family_id: partition.family_id,
                partition: Some(partition),
                kind: ProjectionArtifactKind::Current,
                content_hash: None,
            })
        }
        _ => Err(IndexError::InvalidDefinition(
            "projection artifact path is not canonical".into(),
        )),
    }
}

fn immutable_path(family_id: [u8; 32], class: &str, hash: [u8; 32]) -> String {
    format!(
        "{ROOT}/{}/artifacts/{class}/{}",
        encode_hash(family_id),
        encode_hash(hash)
    )
}

fn partition_root(partition: ProjectionPartitionIdentity) -> String {
    format!(
        "{ROOT}/{}/partitions/{}/{}/{}/{}/{}",
        encode_hash(partition.family_id),
        partition.source_node,
        encode_hash(partition.source_epoch),
        partition.producer_node,
        partition.placement_term,
        partition.placement_index,
    )
}

fn encode_partition_bytes(out: &mut Vec<u8>, partition: ProjectionPartitionIdentity) {
    out.extend_from_slice(&partition.family_id);
    out.extend_from_slice(&partition.source_node.to_le_bytes());
    out.extend_from_slice(&partition.source_epoch);
    out.extend_from_slice(&partition.producer_node.to_le_bytes());
    out.extend_from_slice(&partition.placement_term.to_le_bytes());
    out.extend_from_slice(&partition.placement_index.to_le_bytes());
}

fn routing_id(domain: &[u8], values: &[&[u8]]) -> Result<u64, IndexError> {
    if values
        .iter()
        .any(|value| value.iter().all(|byte| *byte == 0))
    {
        return Err(IndexError::InvalidDefinition(
            "projection routing identity is zero".into(),
        ));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    for value in values {
        hasher.update(value);
    }
    let mut prefix = [0; 8];
    prefix.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    Ok(u64::from_be_bytes(prefix).max(1))
}

fn decode_u64(value: &str) -> Result<u64, IndexError> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(IndexError::InvalidDefinition(
            "projection partition integer is not canonical".into(),
        ));
    }
    let decoded: u64 = value.parse().map_err(|_| {
        IndexError::InvalidDefinition("projection partition integer is invalid".into())
    })?;
    if decoded == 0 {
        return Err(IndexError::InvalidDefinition(
            "projection partition integer is zero".into(),
        ));
    }
    Ok(decoded)
}

fn decode_nonzero_hash(value: &str) -> Result<[u8; 32], IndexError> {
    let hash = decode_hash(value)?;
    if hash == [0; 32] {
        return Err(IndexError::InvalidDefinition(
            "projection path identity is zero".into(),
        ));
    }
    Ok(hash)
}

fn decode_hash(value: &str) -> Result<[u8; 32], IndexError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(IndexError::InvalidDefinition(
            "projection digest is not canonical lowercase hex".into(),
        ));
    }
    let mut decoded = [0; 32];
    for (target, pair) in decoded.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *target = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(decoded)
}

fn encode_hash(hash: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in hash {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    value
}

fn nibble(byte: u8) -> Result<u8, IndexError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(IndexError::InvalidDefinition(
            "projection digest is invalid".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition() -> ProjectionPartitionIdentity {
        ProjectionPartitionIdentity::new([1; 32], 7, [2; 32], 7, 3, 4).unwrap()
    }

    #[test]
    fn immutable_artifacts_are_family_scoped_and_reusable() {
        let partition = partition();
        let mut successor = partition;
        successor.producer_node = 8;
        successor.placement_term = 4;
        successor.placement_index = 5;
        let hash = [6; 32];
        assert_eq!(
            projection_pack_path(partition, hash),
            projection_pack_path(successor, hash)
        );
        let parsed =
            parse_projection_artifact_path(&projection_pack_path(partition, hash)).unwrap();
        assert_eq!(parsed.family_id, partition.family_id);
        assert_eq!(parsed.partition, None);
        assert_eq!(parsed.content_hash, Some(hash));
    }

    #[test]
    fn only_current_is_partition_scoped() {
        let partition = partition();
        let parsed = parse_projection_artifact_path(&projection_current_path(partition)).unwrap();
        assert_eq!(parsed.partition, Some(partition));
        assert_eq!(parsed.kind, ProjectionArtifactKind::Current);
        assert_eq!(parsed.content_hash, None);
    }

    #[test]
    fn query_ready_artifacts_are_family_scoped_and_distinct_from_delta_packs() {
        let partition = partition();
        let hash = [9; 32];
        let run = parse_projection_artifact_path(&projection_query_run_pack_path(partition, hash))
            .unwrap();
        let stream =
            parse_projection_artifact_path(&projection_query_run_stream_page_path(partition, hash))
                .unwrap();
        assert_eq!(run.partition, None);
        assert_eq!(run.kind, ProjectionArtifactKind::QueryRunPack);
        assert_eq!(stream.kind, ProjectionArtifactKind::QueryRunStreamPage);
        assert_ne!(
            projection_query_run_pack_path(partition, hash),
            projection_pack_path(partition, hash)
        );
    }

    #[test]
    fn family_directory_is_stable_across_catalog_generations() {
        let family = [1; 32];
        let directory = projection_family_directory_path(family);
        assert_eq!(
            parse_projection_catalog_path(&directory).unwrap(),
            ProjectionCatalogPath {
                family_id: family,
                physical_catalog_generation: None,
                kind: ProjectionCatalogPathKind::PartitionDirectory,
            }
        );
        for catalog in [[2; 32], [3; 32]] {
            let activation = projection_catalog_activation_path(family, catalog);
            assert_eq!(
                parse_projection_catalog_path(&activation)
                    .unwrap()
                    .physical_catalog_generation,
                Some(catalog)
            );
        }
    }

    #[test]
    fn noncanonical_or_zero_components_are_rejected() {
        let partition = partition();
        assert!(
            parse_projection_artifact_path(
                &projection_current_path(partition).replace("/7/", "/07/")
            )
            .is_err()
        );
        assert!(parse_projection_artifact_path(&projection_pack_path(partition, [0; 32])).is_err());
    }
}
