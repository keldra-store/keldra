use crate::IndexError;

const ROOT: &str = "_keldra/index-projections/v5";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionArtifactKind {
    Pack,
    StreamPage,
    ComponentPage,
    Generation,
    Current,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionArtifactPath {
    pub family_id: [u8; 32],
    pub kind: ProjectionArtifactKind,
    pub content_hash: Option<[u8; 32]>,
}

pub fn projection_pack_path(family_id: [u8; 32], hash: [u8; 32]) -> String {
    immutable_path(family_id, "packs", hash)
}

pub fn projection_stream_page_path(family_id: [u8; 32], hash: [u8; 32]) -> String {
    immutable_path(family_id, "stream-pages", hash)
}

pub fn projection_component_page_path(family_id: [u8; 32], hash: [u8; 32]) -> String {
    immutable_path(family_id, "component-pages", hash)
}

pub fn projection_generation_path(family_id: [u8; 32], hash: [u8; 32]) -> String {
    immutable_path(family_id, "generations", hash)
}

pub fn projection_current_path(family_id: [u8; 32]) -> String {
    format!("{ROOT}/{}/current", encode_hash(family_id))
}

/// Non-authoritative scheduler key for one exact projection family.
///
/// Persistence and validation always use the full family identity embedded in
/// the canonical path. A collision here can therefore only serialize two
/// unrelated publication streams behind the same in-process gate.
pub fn projection_routing_id(family_id: [u8; 32]) -> u64 {
    let digest = blake3::hash(
        &[
            b"keldra-index-projection-routing-v1\0".as_slice(),
            family_id.as_slice(),
        ]
        .concat(),
    );
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_be_bytes(prefix).max(1)
}

pub fn parse_projection_artifact_path(path: &str) -> Result<ProjectionArtifactPath, IndexError> {
    let parts = path.split('/').collect::<Vec<_>>();
    let (family, kind, hash) = match parts.as_slice() {
        ["_keldra", "index-projections", "v5", family, "current"] => {
            (*family, ProjectionArtifactKind::Current, None)
        }
        ["_keldra", "index-projections", "v5", family, class, hash] => {
            let kind = match *class {
                "packs" => ProjectionArtifactKind::Pack,
                "stream-pages" => ProjectionArtifactKind::StreamPage,
                "component-pages" => ProjectionArtifactKind::ComponentPage,
                "generations" => ProjectionArtifactKind::Generation,
                _ => {
                    return Err(IndexError::InvalidDefinition(
                        "projection artifact path class is invalid".into(),
                    ));
                }
            };
            (*family, kind, Some(*hash))
        }
        _ => {
            return Err(IndexError::InvalidDefinition(
                "projection artifact path is not canonical".into(),
            ));
        }
    };
    let family_id = decode_hash(family)?;
    let content_hash = hash.map(decode_hash).transpose()?;
    if family_id == [0; 32] || content_hash == Some([0; 32]) {
        return Err(IndexError::InvalidDefinition(
            "projection artifact identity is zero".into(),
        ));
    }
    Ok(ProjectionArtifactPath {
        family_id,
        kind,
        content_hash,
    })
}

fn immutable_path(family_id: [u8; 32], class: &str, hash: [u8; 32]) -> String {
    format!(
        "{ROOT}/{}/{class}/{}",
        encode_hash(family_id),
        encode_hash(hash)
    )
}

fn decode_hash(value: &str) -> Result<[u8; 32], IndexError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(IndexError::InvalidDefinition(
            "projection artifact digest is not canonical lowercase hex".into(),
        ));
    }
    let mut decoded = [0_u8; 32];
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
            "projection artifact digest is invalid".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_v5_artifact_path_round_trips_exact_full_identities() {
        let family = [1; 32];
        let hash = [2; 32];
        for (path, kind, content_hash) in [
            (
                projection_pack_path(family, hash),
                ProjectionArtifactKind::Pack,
                Some(hash),
            ),
            (
                projection_stream_page_path(family, hash),
                ProjectionArtifactKind::StreamPage,
                Some(hash),
            ),
            (
                projection_component_page_path(family, hash),
                ProjectionArtifactKind::ComponentPage,
                Some(hash),
            ),
            (
                projection_generation_path(family, hash),
                ProjectionArtifactKind::Generation,
                Some(hash),
            ),
            (
                projection_current_path(family),
                ProjectionArtifactKind::Current,
                None,
            ),
        ] {
            assert_eq!(
                parse_projection_artifact_path(&path).unwrap(),
                ProjectionArtifactPath {
                    family_id: family,
                    kind,
                    content_hash,
                }
            );
        }
    }

    #[test]
    fn parser_rejects_aliases_truncation_zero_and_extra_segments() {
        let family = encode_hash([1; 32]);
        let hash = encode_hash([2; 32]);
        for path in [
            format!("{ROOT}/{family}/packs/{hash}/extra"),
            format!("{ROOT}/{family}/unknown/{hash}"),
            format!(
                "{ROOT}/{}/packs/{hash}",
                encode_hash([0xab; 32]).to_uppercase()
            ),
            format!("{ROOT}/{family}/packs/{}", &hash[..32]),
            projection_current_path([0; 32]),
            projection_pack_path([1; 32], [0; 32]),
        ] {
            assert!(parse_projection_artifact_path(&path).is_err(), "{path}");
        }
    }

    #[test]
    fn routing_identity_is_stable_nonzero_and_not_the_storage_authority() {
        assert_eq!(
            projection_routing_id([7; 32]),
            projection_routing_id([7; 32])
        );
        assert_ne!(projection_routing_id([7; 32]), 0);
        assert_ne!(
            projection_routing_id([7; 32]),
            projection_routing_id([8; 32])
        );
    }
}
