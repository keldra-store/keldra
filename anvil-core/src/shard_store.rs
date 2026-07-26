//! Append-only storage for provisional and committed erasure shards.

use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"ANVSHD01";
const VERSION: u16 = 1;
const HEADER: usize = 101;
const TRAILER: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ShardKind {
    Data = 1,
    Parity = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardRecord {
    pub transaction_id: Uuid,
    pub object_identity: Uuid,
    pub encoding_generation: u64,
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
    pub shard_kind: ShardKind,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ShardIdentity {
    pub transaction_id: Uuid,
    pub object_identity: Uuid,
    pub encoding_generation: u64,
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
}

impl From<&ShardRecord> for ShardIdentity {
    fn from(record: &ShardRecord) -> Self {
        Self {
            transaction_id: record.transaction_id,
            object_identity: record.object_identity,
            encoding_generation: record.encoding_generation,
            stripe_ordinal: record.stripe_ordinal,
            shard_ordinal: record.shard_ordinal,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardLocation {
    pub segment_id: u64,
    pub payload_offset: u64,
    pub payload_length: u64,
    pub payload_hash: [u8; 32],
}

pub struct ShardSegment {
    id: u64,
    path: PathBuf,
    file: File,
}

impl ShardSegment {
    pub fn open(directory: impl AsRef<Path>, id: u64) -> Result<Self> {
        fs::create_dir_all(directory.as_ref())?;
        let path = directory.as_ref().join(format!("{id:020}.shards"));
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        recover_tail(&mut file)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self { id, path, file })
    }

    /// Returns only after the complete shard and its framing are fsynced.
    pub fn append(&mut self, record: &ShardRecord) -> Result<ShardLocation> {
        let location = append_record(&mut self.file, self.id, record)?;
        self.file.sync_data()?;
        Ok(location)
    }

    pub fn read(&mut self, location: &ShardLocation) -> Result<Vec<u8>> {
        if location.segment_id != self.id {
            bail!("shard belongs to another segment");
        }
        self.file.seek(SeekFrom::Start(location.payload_offset))?;
        let mut payload = vec![0; location.payload_length as usize];
        self.file.read_exact(&mut payload)?;
        if *blake3::hash(&payload).as_bytes() != location.payload_hash {
            bail!("shard hash mismatch");
        }
        Ok(payload)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rewrites this segment retaining exactly the shards authorised by a
    /// cluster-wide shard GC plan.
    ///
    /// The plan must already have proved manifest unreachability, replacement
    /// durability/application, rollback-window expiry, and absence of
    /// reader/repair/rebalance pins. This local append-only store intentionally
    /// cannot infer any of those distributed facts.
    pub fn compact_authorised(&mut self, retained: &BTreeSet<ShardIdentity>) -> Result<usize> {
        let records = read_records(&mut self.file)?;
        let removed = records
            .iter()
            .filter(|record| !retained.contains(&ShardIdentity::from(*record)))
            .count();
        if removed == 0 {
            return Ok(0);
        }
        let temporary_path = self.path.with_extension("shards.compacting");
        let mut replacement = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temporary_path)?;
        for record in records {
            if retained.contains(&ShardIdentity::from(&record)) {
                append_record(&mut replacement, self.id, &record)?;
            }
        }
        replacement.sync_all()?;
        fs::rename(&temporary_path, &self.path)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(removed)
    }
}

fn append_record(file: &mut File, segment_id: u64, record: &ShardRecord) -> Result<ShardLocation> {
    let hash = *blake3::hash(&record.payload).as_bytes();
    let offset = file.seek(SeekFrom::End(0))?;
    let mut bytes = Vec::with_capacity(HEADER + record.payload.len() + TRAILER);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(record.transaction_id.as_bytes());
    bytes.extend_from_slice(record.object_identity.as_bytes());
    bytes.extend_from_slice(&record.encoding_generation.to_be_bytes());
    bytes.extend_from_slice(&record.stripe_ordinal.to_be_bytes());
    bytes.extend_from_slice(&record.shard_ordinal.to_be_bytes());
    bytes.push(record.shard_kind as u8);
    bytes.extend_from_slice(&(record.payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&hash);
    debug_assert_eq!(bytes.len(), HEADER);
    bytes.extend_from_slice(&record.payload);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    file.write_all(&bytes)?;
    Ok(ShardLocation {
        segment_id,
        payload_offset: offset + HEADER as u64,
        payload_length: record.payload.len() as u64,
        payload_hash: hash,
    })
}

fn read_records(file: &mut File) -> Result<Vec<ShardRecord>> {
    recover_tail(file)?;
    let file_len = file.metadata()?.len();
    let mut offset = 0;
    let mut records = Vec::new();
    while offset < file_len {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0; HEADER];
        file.read_exact(&mut header)?;
        let payload_length = u64::from_be_bytes(header[61..69].try_into().unwrap());
        let mut payload = vec![0; payload_length as usize];
        file.read_exact(&mut payload)?;
        file.seek(SeekFrom::Current(TRAILER as i64))?;
        records.push(ShardRecord {
            transaction_id: Uuid::from_slice(&header[10..26])?,
            object_identity: Uuid::from_slice(&header[26..42])?,
            encoding_generation: u64::from_be_bytes(header[42..50].try_into().unwrap()),
            stripe_ordinal: u64::from_be_bytes(header[50..58].try_into().unwrap()),
            shard_ordinal: u16::from_be_bytes(header[58..60].try_into().unwrap()),
            shard_kind: match header[60] {
                1 => ShardKind::Data,
                2 => ShardKind::Parity,
                _ => bail!("invalid shard kind"),
            },
            payload,
        });
        offset = offset
            .checked_add(HEADER as u64 + payload_length + TRAILER as u64)
            .context("shard offset overflow")?;
    }
    Ok(records)
}

fn recover_tail(file: &mut File) -> Result<()> {
    let file_len = file.metadata()?.len();
    let mut offset = 0;
    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < (HEADER + TRAILER) as u64 {
            return truncate(file, offset);
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0; HEADER];
        file.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            bail!("invalid shard magic at {offset}");
        }
        if u16::from_be_bytes(header[8..10].try_into().unwrap()) != VERSION {
            bail!("unsupported shard format");
        }
        if !matches!(header[60], 1 | 2) {
            bail!("invalid shard kind");
        }
        let len = u64::from_be_bytes(header[61..69].try_into().unwrap());
        let record_len = (HEADER as u64)
            .checked_add(len)
            .and_then(|v| v.checked_add(TRAILER as u64))
            .context("shard length overflow")?;
        if record_len > remaining {
            return truncate(file, offset);
        }
        let mut body = vec![0; HEADER + len as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut body)?;
        if *blake3::hash(&body[HEADER..]).as_bytes() != header[69..101] {
            bail!("shard payload checksum mismatch at {offset}");
        }
        let mut checksum = [0; 32];
        file.read_exact(&mut checksum)?;
        if *blake3::hash(&body).as_bytes() != checksum {
            bail!("shard record checksum mismatch at {offset}");
        }
        offset += record_len;
    }
    Ok(())
}

fn truncate(file: &mut File, offset: u64) -> Result<()> {
    file.set_len(offset)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(payload: &[u8]) -> ShardRecord {
        ShardRecord {
            transaction_id: Uuid::new_v4(),
            object_identity: Uuid::new_v4(),
            encoding_generation: 1,
            stripe_ordinal: 3,
            shard_ordinal: 2,
            shard_kind: ShardKind::Parity,
            payload: payload.into(),
        }
    }

    #[test]
    fn append_reopens_and_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let location = ShardSegment::open(dir.path(), 1)
            .unwrap()
            .append(&record(b"shard"))
            .unwrap();
        assert_eq!(
            ShardSegment::open(dir.path(), 1)
                .unwrap()
                .read(&location)
                .unwrap(),
            b"shard"
        );
    }

    #[test]
    fn incomplete_tail_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&record(b"good")).unwrap();
        let good = segment.path().metadata().unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(segment.path())
            .unwrap()
            .write_all(b"ANV")
            .unwrap();
        drop(segment);
        assert_eq!(
            ShardSegment::open(dir.path(), 1)
                .unwrap()
                .path()
                .metadata()
                .unwrap()
                .len(),
            good
        );
    }

    #[test]
    fn corrupt_complete_shard_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&record(b"good")).unwrap();
        let mut file = OpenOptions::new().write(true).open(segment.path()).unwrap();
        file.seek(SeekFrom::Start(HEADER as u64)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        drop(segment);
        assert!(ShardSegment::open(dir.path(), 1).is_err());
    }

    #[test]
    fn authorised_compaction_keeps_pinned_provisional_or_committed_shards() {
        let dir = tempfile::tempdir().unwrap();
        let keep = record(b"visible or pinned");
        let remove = record(b"expired provisional or safely retired");
        let mut segment = ShardSegment::open(dir.path(), 1).unwrap();
        segment.append(&keep).unwrap();
        segment.append(&remove).unwrap();

        let retained = [ShardIdentity::from(&keep)].into_iter().collect();
        assert_eq!(segment.compact_authorised(&retained).unwrap(), 1);
        drop(segment);

        let mut reopened = ShardSegment::open(dir.path(), 1).unwrap();
        let records = read_records(&mut reopened.file).unwrap();
        assert_eq!(records, [keep]);
    }
}
