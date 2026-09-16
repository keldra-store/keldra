//! Shared physical-pack identities for format-v1 index artifacts.
//!
//! Logical component and query-block checksums remain stable semantic
//! identities.  A segment-local locator resolves those bytes through the
//! exact ordinary-object reference in its root-bound pack table.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::IndexError;

pub const ARTIFACT_PACK_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ArtifactPackLocator {
    pub ordinal: u32,
    pub offset: u64,
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub checksum: [u8; 32],
}

impl ArtifactPackLocator {
    pub fn resolve<'a>(
        &self,
        table: &'a ArtifactPackTable,
    ) -> Result<&'a ArtifactPackReference, IndexError> {
        self.validate()?;
        let pack = table.resolve(self.ordinal)?;
        let end = self
            .offset
            .checked_add(self.encoded_bytes)
            .ok_or(IndexError::OffsetOverflow)?;
        if end > pack.length {
            return Err(IndexError::Integrity);
        }
        Ok(pack)
    }

    pub fn range(&self) -> Result<std::ops::Range<usize>, IndexError> {
        let start = usize::try_from(self.offset).map_err(|_| IndexError::OffsetOverflow)?;
        let length = usize::try_from(self.encoded_bytes).map_err(|_| IndexError::OffsetOverflow)?;
        let end = start
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        Ok(start..end)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        if self.encoded_bytes == 0 || self.logical_bytes == 0 || self.checksum == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "artifact pack locator is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPackReference {
    pub ordinal: u32,
    pub canonical_path: Arc<str>,
    pub object_version: u64,
    pub hash: [u8; 32],
    pub length: u64,
}

impl ArtifactPackReference {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.canonical_path.is_empty()
            || !self
                .canonical_path
                .starts_with("_keldra/index-projections/v1/")
            || self.object_version == 0
            || self.hash == [0; 32]
            || self.length == 0
            || self.length > ARTIFACT_PACK_MAX_BYTES as u64
        {
            return Err(IndexError::InvalidDefinition(
                "artifact pack reference is invalid".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPackTable {
    entries: Arc<[ArtifactPackReference]>,
    memory_lease: super::SegmentMemoryLease,
}

impl ArtifactPackTable {
    pub fn empty() -> Self {
        Self {
            entries: Arc::from([]),
            memory_lease: super::SegmentMemoryLease::default(),
        }
    }

    pub fn new(entries: Vec<ArtifactPackReference>) -> Result<Self, IndexError> {
        let table = Self {
            entries: Arc::from(entries),
            memory_lease: super::SegmentMemoryLease::default(),
        };
        table.validate()?;
        Ok(table)
    }

    pub fn entries(&self) -> &[ArtifactPackReference] {
        &self.entries
    }

    pub fn has_memory_lease(&self) -> bool {
        self.memory_lease.is_attached()
    }
    pub fn attach_memory_lease(&self, lease: Arc<dyn Send + Sync + std::fmt::Debug>) -> bool {
        self.memory_lease.attach(lease)
    }
    pub fn resident_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>()
            + self.entries.len() * std::mem::size_of::<ArtifactPackReference>()
            + self
                .entries
                .iter()
                .map(|entry| entry.canonical_path.len() + 2 * std::mem::size_of::<usize>())
                .sum::<usize>()
    }

    pub fn resolve(&self, ordinal: u32) -> Result<&ArtifactPackReference, IndexError> {
        self.entries
            .get(ordinal as usize)
            .filter(|entry| entry.ordinal == ordinal)
            .ok_or(IndexError::Integrity)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        if self.entries.len() > u32::MAX as usize {
            return Err(IndexError::InvalidDefinition(
                "artifact pack table is unbounded".into(),
            ));
        }
        let mut paths = BTreeSet::new();
        for (ordinal, entry) in self.entries.iter().enumerate() {
            entry.validate()?;
            if entry.ordinal as usize != ordinal || !paths.insert(entry.canonical_path.as_ref()) {
                return Err(IndexError::InvalidDefinition(
                    "artifact pack table is not canonical".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnpublishedArtifactPack {
    pub ordinal: u32,
    pub hash: [u8; 32],
    pub bytes: Vec<u8>,
}

pub(crate) fn pack_whole_artifacts<T>(
    artifacts: Vec<(T, Vec<u8>, u64, [u8; 32])>,
) -> Result<(Vec<UnpublishedArtifactPack>, Vec<(T, ArtifactPackLocator)>), IndexError> {
    let mut packs = Vec::new();
    let mut located = Vec::with_capacity(artifacts.len());
    let mut bytes = Vec::new();
    let mut staged = Vec::new();
    for (identity, artifact, logical_bytes, checksum) in artifacts {
        if artifact.is_empty() || artifact.len() > ARTIFACT_PACK_MAX_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: artifact.len(),
                limit: ARTIFACT_PACK_MAX_BYTES,
            });
        }
        if !bytes.is_empty()
            && bytes
                .len()
                .checked_add(artifact.len())
                .is_none_or(|length| length > ARTIFACT_PACK_MAX_BYTES)
        {
            seal(&mut packs, &mut located, &mut bytes, &mut staged)?;
        }
        let offset = u64::try_from(bytes.len()).map_err(|_| IndexError::OffsetOverflow)?;
        bytes.extend_from_slice(&artifact);
        staged.push((
            identity,
            offset,
            artifact.len() as u64,
            logical_bytes,
            checksum,
        ));
    }
    if !bytes.is_empty() {
        seal(&mut packs, &mut located, &mut bytes, &mut staged)?;
    }
    Ok((packs, located))
}

type Staged<T> = (T, u64, u64, u64, [u8; 32]);

fn seal<T>(
    packs: &mut Vec<UnpublishedArtifactPack>,
    located: &mut Vec<(T, ArtifactPackLocator)>,
    bytes: &mut Vec<u8>,
    staged: &mut Vec<Staged<T>>,
) -> Result<(), IndexError> {
    let ordinal = u32::try_from(packs.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let complete = std::mem::take(bytes);
    let hash = *crate::profiled_blake3_hash!(&complete).as_bytes();
    for (identity, offset, encoded_bytes, logical_bytes, checksum) in std::mem::take(staged) {
        located.push((
            identity,
            ArtifactPackLocator {
                ordinal,
                offset,
                encoded_bytes,
                logical_bytes,
                checksum,
            },
        ));
    }
    packs.push(UnpublishedArtifactPack {
        ordinal,
        hash,
        bytes: complete,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(byte: u8, bytes: usize) -> (u8, Vec<u8>, u64, [u8; 32]) {
        (byte, vec![byte; bytes], bytes as u64, [byte; 32])
    }

    #[test]
    fn whole_artifacts_pack_deterministically_without_straddling() {
        let input = vec![
            artifact(1, ARTIFACT_PACK_MAX_BYTES - 1),
            artifact(2, 1),
            artifact(3, 1),
        ];
        let (packs, located) = pack_whole_artifacts(input).unwrap();
        assert_eq!(packs.len(), 2);
        assert_eq!(packs[0].bytes.len(), ARTIFACT_PACK_MAX_BYTES);
        assert_eq!(located[0].1.ordinal, 0);
        assert_eq!(located[0].1.offset, 0);
        assert_eq!(located[1].1.ordinal, 0);
        assert_eq!(located[1].1.offset, (ARTIFACT_PACK_MAX_BYTES - 1) as u64);
        assert_eq!(located[2].1.ordinal, 1);
        assert_eq!(located[2].1.offset, 0);
        let first_hash = packs[0].hash;
        let (again, _) = pack_whole_artifacts(vec![
            artifact(1, ARTIFACT_PACK_MAX_BYTES - 1),
            artifact(2, 1),
            artifact(3, 1),
        ])
        .unwrap();
        assert_eq!(again[0].hash, first_hash);
    }

    #[test]
    fn pack_table_rejects_noncanonical_and_out_of_range_locations() {
        let reference = ArtifactPackReference {
            ordinal: 0,
            canonical_path: "_keldra/index-projections/v1/f/artifacts/packs/a".into(),
            object_version: 7,
            hash: [1; 32],
            length: 8,
        };
        let table = ArtifactPackTable::new(vec![reference]).unwrap();
        let mut locator = ArtifactPackLocator {
            ordinal: 0,
            offset: 2,
            encoded_bytes: 6,
            logical_bytes: 6,
            checksum: [2; 32],
        };
        assert!(locator.resolve(&table).is_ok());
        locator.encoded_bytes = 7;
        assert_eq!(locator.resolve(&table), Err(IndexError::Integrity));

        let mut duplicate = table.entries()[0].clone();
        duplicate.ordinal = 1;
        assert!(ArtifactPackTable::new(vec![table.entries()[0].clone(), duplicate]).is_err());
    }

    #[test]
    fn oversized_whole_artifact_is_rejected() {
        assert!(matches!(
            pack_whole_artifacts(vec![artifact(1, ARTIFACT_PACK_MAX_BYTES + 1)]),
            Err(IndexError::ResourceLimit { .. })
        ));
    }
}
