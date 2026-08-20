//! Versioned systematic Reed-Solomon shards for Keldra's large-byte plane.
//!
//! This module deliberately knows nothing about placement, acknowledgement,
//! reference counts, or peer transport. It transforms one verified complete
//! blob into self-validating shard streams and reconstructs that blob from any
//! sufficient set of valid shard chunks.

use std::io::{self, Read, Write};

use crc32c::{crc32c, crc32c_append};
use reed_solomon_erasure::galois_8::ReedSolomon;
use thiserror::Error;

use crate::BlobRef;

const SHARD_MAGIC: &[u8; 8] = b"ANVLSHRD";
/// On-disk identity and framing version for erasure-coded fragments.
pub const FRAGMENT_FORMAT_VERSION: u16 = 1;
const HEADER_BODY_BYTES: usize = 8 + 2 + 32 + 8 + 2;
const HEADER_BYTES: usize = HEADER_BODY_BYTES + 4;
const FRAME_PREFIX_BYTES: usize = 4 + 4;
const MAX_GALOIS_8_SHARDS: u16 = 256;

pub const DEFAULT_ERASURE_DATA_SHARDS: u16 = 2;
pub const DEFAULT_ERASURE_PARITY_SHARDS: u16 = 1;
pub const DEFAULT_ERASURE_STRIPE_UNIT_BYTES: u32 = 16 * 1024;

/// One immutable cluster erasure profile.
///
/// The profile is supplied by cluster configuration; it is intentionally not
/// repeated in every shard. `stripe_unit` is the number of bytes contributed
/// by each data shard to one stripe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErasureProfile {
    data_shards: u16,
    parity_shards: u16,
    stripe_unit: u32,
}

impl Default for ErasureProfile {
    fn default() -> Self {
        Self {
            data_shards: DEFAULT_ERASURE_DATA_SHARDS,
            parity_shards: DEFAULT_ERASURE_PARITY_SHARDS,
            stripe_unit: DEFAULT_ERASURE_STRIPE_UNIT_BYTES,
        }
    }
}

impl ErasureProfile {
    pub fn new(
        data_shards: u16,
        parity_shards: u16,
        stripe_unit: u32,
    ) -> Result<Self, ErasureError> {
        if data_shards == 0 {
            return Err(ErasureError::InvalidProfile(
                "data shard count must be greater than zero",
            ));
        }
        if parity_shards == 0 {
            return Err(ErasureError::InvalidProfile(
                "parity shard count must be greater than zero",
            ));
        }
        if data_shards
            .checked_add(parity_shards)
            .is_none_or(|total| total > MAX_GALOIS_8_SHARDS)
        {
            return Err(ErasureError::InvalidProfile(
                "data and parity shard counts must total at most 256",
            ));
        }
        if stripe_unit == 0 {
            return Err(ErasureError::InvalidProfile(
                "stripe unit must be greater than zero",
            ));
        }
        Ok(Self {
            data_shards,
            parity_shards,
            stripe_unit,
        })
    }

    pub const fn data_shards(self) -> u16 {
        self.data_shards
    }

    pub const fn parity_shards(self) -> u16 {
        self.parity_shards
    }

    pub const fn stripe_unit(self) -> u32 {
        self.stripe_unit
    }

    pub const fn total_shards(self) -> u16 {
        self.data_shards + self.parity_shards
    }

    fn stripe_width(self) -> u64 {
        u64::from(self.data_shards) * u64::from(self.stripe_unit)
    }

    fn stripe_count(self, blob_length: u64) -> u64 {
        if blob_length == 0 {
            0
        } else {
            1 + (blob_length - 1) / self.stripe_width()
        }
    }

    fn chunk_length(self, blob_length: u64, stripe: u64, ordinal: u16) -> usize {
        let stripe_offset = stripe * self.stripe_width();
        let remaining = blob_length
            .saturating_sub(stripe_offset)
            .min(self.stripe_width());
        let stripe_unit = u64::from(self.stripe_unit);
        let length = if ordinal < self.data_shards {
            remaining
                .saturating_sub(u64::from(ordinal) * stripe_unit)
                .min(stripe_unit)
        } else {
            // Every parity byte after the longest data chunk is deterministically
            // zero, so the padded tail need not be stored.
            remaining.min(stripe_unit)
        };
        usize::try_from(length).expect("a u32 stripe unit always fits usize on supported targets")
    }
}

/// A codec bound to one immutable cluster profile.
pub struct ErasureCodec {
    profile: ErasureProfile,
    reed_solomon: ReedSolomon,
}

impl ErasureCodec {
    pub fn new(profile: ErasureProfile) -> Result<Self, ErasureError> {
        let reed_solomon = ReedSolomon::new(
            usize::from(profile.data_shards),
            usize::from(profile.parity_shards),
        )
        .map_err(|error| ErasureError::Codec(error.to_string()))?;
        Ok(Self {
            profile,
            reed_solomon,
        })
    }

    pub const fn profile(&self) -> ErasureProfile {
        self.profile
    }

    /// Exact encoded byte length for one shard identity under this profile.
    /// This lets framed peer ingress reject excess bytes before they can grow
    /// an ordinary staging file without bound.
    pub fn encoded_shard_length(
        &self,
        expected: &BlobRef,
        ordinal: u16,
    ) -> Result<u64, ErasureError> {
        self.require_ordinal(ordinal)?;
        let stripes = self.profile.stripe_count(expected.length);
        let payload_bytes = if stripes == 0 {
            0_u128
        } else {
            u128::from(stripes - 1) * u128::from(self.profile.stripe_unit)
                + self
                    .profile
                    .chunk_length(expected.length, stripes - 1, ordinal) as u128
        };
        let encoded =
            HEADER_BYTES as u128 + u128::from(stripes) * FRAME_PREFIX_BYTES as u128 + payload_bytes;
        u64::try_from(encoded).map_err(|_| ErasureError::EncodedShardLengthOverflow { ordinal })
    }

    /// Encode a complete source into one writer per shard ordinal.
    ///
    /// The source must have exactly the hash and length in `expected`. Writers
    /// may contain partial data on error, so callers publish temporary files
    /// only after this method succeeds and they have synchronised each file.
    pub fn encode<R: Read, W: Write>(
        &self,
        mut source: R,
        expected: &BlobRef,
        shards: &mut [W],
    ) -> Result<(), ErasureError> {
        self.require_shard_count(shards.len())?;
        let header_bodies = self.write_headers(expected, shards)?;
        let stripe_unit = usize::try_from(self.profile.stripe_unit)
            .expect("a u32 stripe unit always fits usize on supported targets");
        let mut chunks = (0..usize::from(self.profile.total_shards()))
            .map(|_| vec![0_u8; stripe_unit])
            .collect::<Vec<_>>();
        let mut hasher = blake3::Hasher::new();
        let mut source_bytes = 0_u64;

        for stripe in 0..self.profile.stripe_count(expected.length) {
            for chunk in &mut chunks {
                chunk.fill(0);
            }
            for ordinal in 0..self.profile.data_shards {
                let length = self.profile.chunk_length(expected.length, stripe, ordinal);
                read_source_exact(
                    &mut source,
                    &mut chunks[usize::from(ordinal)][..length],
                    &mut hasher,
                    &mut source_bytes,
                    expected.length,
                )?;
            }
            self.reed_solomon
                .encode(&mut chunks)
                .map_err(|error| ErasureError::Codec(error.to_string()))?;
            for ordinal in 0..self.profile.total_shards() {
                let length = self.profile.chunk_length(expected.length, stripe, ordinal);
                write_chunk(
                    &mut shards[usize::from(ordinal)],
                    &header_bodies[usize::from(ordinal)],
                    ordinal,
                    stripe,
                    &chunks[usize::from(ordinal)][..length],
                )?;
            }
        }

        if read_one(&mut source)?.is_some() {
            return Err(ErasureError::SourceLengthMismatch {
                expected: expected.length,
                actual_at_least: expected.length.saturating_add(1),
            });
        }
        if hasher.finalize().as_bytes() != &expected.hash {
            return Err(ErasureError::BlobHashMismatch);
        }
        Ok(())
    }

    /// Verify the framing, identity, and CRC32C of one complete shard.
    pub fn validate_shard<R: Read>(
        &self,
        expected: &BlobRef,
        ordinal: u16,
        mut shard: R,
    ) -> Result<(), ErasureError> {
        self.require_ordinal(ordinal)?;
        let header_body = read_header(&mut shard, expected, ordinal)?;
        for stripe in 0..self.profile.stripe_count(expected.length) {
            let length = self.profile.chunk_length(expected.length, stripe, ordinal);
            read_chunk(&mut shard, &header_body, ordinal, stripe, length)?;
        }
        if read_one(&mut shard)?.is_some() {
            return Err(ErasureError::TrailingShardBytes { ordinal });
        }
        Ok(())
    }

    /// Reconstruct and verify the original blob from ordinal-addressed shards.
    ///
    /// A chunk with a bad CRC is treated as missing for that stripe. A malformed
    /// or truncated shard is unavailable from that point onward. `output` may
    /// contain partial data on error and should therefore be a temporary sink.
    pub fn reconstruct<R: Read, W: Write>(
        &self,
        expected: &BlobRef,
        shards: &mut [Option<R>],
        output: &mut W,
    ) -> Result<(), ErasureError> {
        self.require_shard_count(shards.len())?;
        let header_bodies = (0..self.profile.total_shards())
            .map(|ordinal| header_body(expected, ordinal))
            .collect::<Vec<_>>();

        for ordinal in 0..self.profile.total_shards() {
            let index = usize::from(ordinal);
            let valid = match shards[index].as_mut() {
                Some(shard) => read_header(shard, expected, ordinal).is_ok(),
                None => false,
            };
            if !valid {
                shards[index] = None;
            }
        }
        if shards.iter().flatten().count() < usize::from(self.profile.data_shards) {
            return Err(ErasureError::TooFewValidChunks {
                stripe: 0,
                required: self.profile.data_shards,
                available: u16::try_from(shards.iter().flatten().count()).unwrap_or(u16::MAX),
            });
        }

        let stripe_unit = usize::try_from(self.profile.stripe_unit)
            .expect("a u32 stripe unit always fits usize on supported targets");
        let mut hasher = blake3::Hasher::new();
        for stripe in 0..self.profile.stripe_count(expected.length) {
            let mut chunks = Vec::with_capacity(usize::from(self.profile.total_shards()));
            for ordinal in 0..self.profile.total_shards() {
                let index = usize::from(ordinal);
                let length = self.profile.chunk_length(expected.length, stripe, ordinal);
                let read = match shards[index].as_mut() {
                    Some(shard) => {
                        read_chunk(shard, &header_bodies[index], ordinal, stripe, length)
                    }
                    None => {
                        chunks.push(None);
                        continue;
                    }
                };
                match read {
                    Ok(mut chunk) => {
                        chunk.resize(stripe_unit, 0);
                        chunks.push(Some(chunk));
                    }
                    Err(ErasureError::ChunkChecksumMismatch { .. }) => chunks.push(None),
                    Err(_) => {
                        shards[index] = None;
                        chunks.push(None);
                    }
                }
            }
            let available = chunks.iter().flatten().count();
            if available < usize::from(self.profile.data_shards) {
                return Err(ErasureError::TooFewValidChunks {
                    stripe,
                    required: self.profile.data_shards,
                    available: u16::try_from(available).unwrap_or(u16::MAX),
                });
            }
            self.reed_solomon
                .reconstruct_data(&mut chunks)
                .map_err(|error| ErasureError::Codec(error.to_string()))?;
            for ordinal in 0..self.profile.data_shards {
                let length = self.profile.chunk_length(expected.length, stripe, ordinal);
                let data = chunks[usize::from(ordinal)].as_ref().ok_or_else(|| {
                    ErasureError::Codec("data shard was not reconstructed".into())
                })?;
                output.write_all(&data[..length])?;
                hasher.update(&data[..length]);
            }
        }
        if hasher.finalize().as_bytes() != &expected.hash {
            return Err(ErasureError::BlobHashMismatch);
        }
        Ok(())
    }

    /// Reconstruct from an ordinal-addressed subset of shard streams.
    ///
    /// This is the peer/coordinator-facing form of [`Self::reconstruct`]. It
    /// keeps shard identity explicit while constructing the codec's sparse
    /// ordinal vector, and rejects a repeated ordinal rather than silently
    /// choosing one copy.
    pub fn reconstruct_available<R: Read, W: Write>(
        &self,
        expected: &BlobRef,
        shards: impl IntoIterator<Item = (u16, R)>,
        output: &mut W,
    ) -> Result<(), ErasureError> {
        let mut ordinal_shards = std::iter::repeat_with(|| None)
            .take(usize::from(self.profile.total_shards()))
            .collect::<Vec<Option<R>>>();
        for (ordinal, shard) in shards {
            self.require_ordinal(ordinal)?;
            let slot = &mut ordinal_shards[usize::from(ordinal)];
            if slot.is_some() {
                return Err(ErasureError::DuplicateShardOrdinal { ordinal });
            }
            *slot = Some(shard);
        }
        self.reconstruct(expected, &mut ordinal_shards, output)
    }

    fn require_shard_count(&self, actual: usize) -> Result<(), ErasureError> {
        let expected = usize::from(self.profile.total_shards());
        if actual != expected {
            return Err(ErasureError::WrongShardCount { expected, actual });
        }
        Ok(())
    }

    fn require_ordinal(&self, ordinal: u16) -> Result<(), ErasureError> {
        if ordinal >= self.profile.total_shards() {
            return Err(ErasureError::InvalidShardOrdinal {
                ordinal,
                total: self.profile.total_shards(),
            });
        }
        Ok(())
    }

    fn write_headers<W: Write>(
        &self,
        expected: &BlobRef,
        shards: &mut [W],
    ) -> Result<Vec<[u8; HEADER_BODY_BYTES]>, ErasureError> {
        let mut bodies = Vec::with_capacity(shards.len());
        for (ordinal, shard) in shards.iter_mut().enumerate() {
            let ordinal = u16::try_from(ordinal).expect("profile limits shard ordinals to u16");
            let body = header_body(expected, ordinal);
            shard.write_all(&body)?;
            shard.write_all(&crc32c(&body).to_be_bytes())?;
            bodies.push(body);
        }
        Ok(bodies)
    }
}

fn header_body(reference: &BlobRef, ordinal: u16) -> [u8; HEADER_BODY_BYTES] {
    let mut body = [0_u8; HEADER_BODY_BYTES];
    let mut cursor = 0;
    body[cursor..cursor + SHARD_MAGIC.len()].copy_from_slice(SHARD_MAGIC);
    cursor += SHARD_MAGIC.len();
    body[cursor..cursor + 2].copy_from_slice(&FRAGMENT_FORMAT_VERSION.to_be_bytes());
    cursor += 2;
    body[cursor..cursor + reference.hash.len()].copy_from_slice(&reference.hash);
    cursor += reference.hash.len();
    body[cursor..cursor + 8].copy_from_slice(&reference.length.to_be_bytes());
    cursor += 8;
    body[cursor..cursor + 2].copy_from_slice(&ordinal.to_be_bytes());
    body
}

fn read_header<R: Read>(
    reader: &mut R,
    expected: &BlobRef,
    ordinal: u16,
) -> Result<[u8; HEADER_BODY_BYTES], ErasureError> {
    let mut encoded = [0_u8; HEADER_BYTES];
    reader.read_exact(&mut encoded)?;
    let mut body = [0_u8; HEADER_BODY_BYTES];
    body.copy_from_slice(&encoded[..HEADER_BODY_BYTES]);
    let stored_checksum = u32::from_be_bytes(
        encoded[HEADER_BODY_BYTES..]
            .try_into()
            .expect("header checksum has a fixed width"),
    );
    if crc32c(&body) != stored_checksum {
        return Err(ErasureError::HeaderChecksumMismatch { ordinal });
    }
    if &body[..SHARD_MAGIC.len()] != SHARD_MAGIC {
        return Err(ErasureError::InvalidShardHeader {
            ordinal,
            reason: "magic does not match",
        });
    }
    let version = u16::from_be_bytes(
        body[SHARD_MAGIC.len()..SHARD_MAGIC.len() + 2]
            .try_into()
            .expect("fragment version has a fixed width"),
    );
    if version != FRAGMENT_FORMAT_VERSION {
        return Err(ErasureError::UnsupportedFragmentFormat(version));
    }
    if body != header_body(expected, ordinal) {
        return Err(ErasureError::InvalidShardHeader {
            ordinal,
            reason: "content identity or ordinal does not match",
        });
    }
    Ok(body)
}

fn write_chunk<W: Write>(
    writer: &mut W,
    header_body: &[u8; HEADER_BODY_BYTES],
    ordinal: u16,
    stripe: u64,
    payload: &[u8],
) -> Result<(), ErasureError> {
    let length = u32::try_from(payload.len()).expect("a shard chunk is bounded by a u32 profile");
    let checksum = chunk_checksum(header_body, ordinal, stripe, length, payload);
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&checksum.to_be_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}

fn read_chunk<R: Read>(
    reader: &mut R,
    header_body: &[u8; HEADER_BODY_BYTES],
    ordinal: u16,
    stripe: u64,
    expected_length: usize,
) -> Result<Vec<u8>, ErasureError> {
    let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
    reader.read_exact(&mut prefix)?;
    let stored_length = u32::from_be_bytes(
        prefix[..4]
            .try_into()
            .expect("chunk length has a fixed width"),
    );
    if usize::try_from(stored_length).expect("u32 fits usize on supported targets")
        != expected_length
    {
        return Err(ErasureError::InvalidChunkLength {
            ordinal,
            stripe,
            expected: expected_length,
            actual: stored_length,
        });
    }
    let stored_checksum = u32::from_be_bytes(
        prefix[4..]
            .try_into()
            .expect("chunk checksum has a fixed width"),
    );
    let mut payload = vec![0_u8; expected_length];
    reader.read_exact(&mut payload)?;
    let expected_checksum = chunk_checksum(header_body, ordinal, stripe, stored_length, &payload);
    if stored_checksum != expected_checksum {
        return Err(ErasureError::ChunkChecksumMismatch { ordinal, stripe });
    }
    Ok(payload)
}

fn chunk_checksum(
    header_body: &[u8; HEADER_BODY_BYTES],
    ordinal: u16,
    stripe: u64,
    length: u32,
    payload: &[u8],
) -> u32 {
    let mut checksum = crc32c(header_body);
    checksum = crc32c_append(checksum, &ordinal.to_be_bytes());
    checksum = crc32c_append(checksum, &stripe.to_be_bytes());
    checksum = crc32c_append(checksum, &length.to_be_bytes());
    crc32c_append(checksum, payload)
}

fn read_source_exact<R: Read>(
    source: &mut R,
    mut destination: &mut [u8],
    hasher: &mut blake3::Hasher,
    source_bytes: &mut u64,
    expected_length: u64,
) -> Result<(), ErasureError> {
    while !destination.is_empty() {
        match source.read(destination) {
            Ok(0) => {
                return Err(ErasureError::SourceLengthMismatch {
                    expected: expected_length,
                    actual_at_least: *source_bytes,
                });
            }
            Ok(read) => {
                hasher.update(&destination[..read]);
                *source_bytes += read as u64;
                destination = &mut destination[read..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn read_one<R: Read>(reader: &mut R) -> Result<Option<u8>, io::Error> {
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(byte[0])),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

#[derive(Debug, Error)]
pub enum ErasureError {
    #[error("invalid erasure profile: {0}")]
    InvalidProfile(&'static str),
    #[error("expected {expected} shard streams, got {actual}")]
    WrongShardCount { expected: usize, actual: usize },
    #[error("shard ordinal {ordinal} is outside profile total {total}")]
    InvalidShardOrdinal { ordinal: u16, total: u16 },
    #[error("encoded shard {ordinal} length exceeds the supported u64 range")]
    EncodedShardLengthOverflow { ordinal: u16 },
    #[error("shard ordinal {ordinal} was supplied more than once")]
    DuplicateShardOrdinal { ordinal: u16 },
    #[error("unsupported fragment format {0}")]
    UnsupportedFragmentFormat(u16),
    #[error("shard {ordinal} header checksum does not match")]
    HeaderChecksumMismatch { ordinal: u16 },
    #[error("shard {ordinal} header is invalid: {reason}")]
    InvalidShardHeader { ordinal: u16, reason: &'static str },
    #[error("shard {ordinal} stripe {stripe} has chunk length {actual}, expected {expected}")]
    InvalidChunkLength {
        ordinal: u16,
        stripe: u64,
        expected: usize,
        actual: u32,
    },
    #[error("shard {ordinal} stripe {stripe} checksum does not match")]
    ChunkChecksumMismatch { ordinal: u16, stripe: u64 },
    #[error("shard {ordinal} contains trailing bytes")]
    TrailingShardBytes { ordinal: u16 },
    #[error("stripe {stripe} has {available} valid chunks; {required} data chunks are required")]
    TooFewValidChunks {
        stripe: u64,
        required: u16,
        available: u16,
    },
    #[error("source length differs: expected {expected}, observed at least {actual_at_least}")]
    SourceLengthMismatch { expected: u64, actual_at_least: u64 },
    #[error("blob BLAKE3 does not match its content identity")]
    BlobHashMismatch,
    #[error("Reed-Solomon codec failed: {0}")]
    Codec(String),
    #[error("shard I/O failed: {0}")]
    Io(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn reference(bytes: &[u8]) -> BlobRef {
        BlobRef {
            hash: *blake3::hash(bytes).as_bytes(),
            length: bytes.len() as u64,
        }
    }

    fn encode(profile: ErasureProfile, bytes: &[u8]) -> (ErasureCodec, BlobRef, Vec<Vec<u8>>) {
        let codec = ErasureCodec::new(profile).unwrap();
        let reference = reference(bytes);
        let mut shards = vec![Vec::new(); usize::from(profile.total_shards())];
        codec
            .encode(Cursor::new(bytes), &reference, &mut shards)
            .unwrap();
        (codec, reference, shards)
    }

    fn readers(shards: &[Vec<u8>]) -> Vec<Option<Cursor<Vec<u8>>>> {
        shards.iter().cloned().map(Cursor::new).map(Some).collect()
    }

    fn first_payload_offset() -> usize {
        HEADER_BYTES + FRAME_PREFIX_BYTES
    }

    #[test]
    fn default_profile_is_two_data_one_parity_with_sixteen_kibibyte_stripes() {
        let profile = ErasureProfile::default();
        assert_eq!(profile.data_shards(), 2);
        assert_eq!(profile.parity_shards(), 1);
        assert_eq!(profile.stripe_unit(), 16 * 1024);
    }

    #[test]
    fn profile_validates_galois_geometry_without_imposing_a_runtime_profile() {
        assert!(matches!(
            ErasureProfile::new(0, 1, 16 * 1024),
            Err(ErasureError::InvalidProfile(_))
        ));
        assert!(matches!(
            ErasureProfile::new(2, 0, 16 * 1024),
            Err(ErasureError::InvalidProfile(_))
        ));
        assert!(matches!(
            ErasureProfile::new(255, 2, 16 * 1024),
            Err(ErasureError::InvalidProfile(_))
        ));
        assert!(matches!(
            ErasureProfile::new(2, 1, 0),
            Err(ErasureError::InvalidProfile(_))
        ));

        let non_default = ErasureProfile::new(4, 2, 4 * 1024).unwrap();
        assert_eq!(non_default.data_shards(), 4);
        assert_eq!(non_default.parity_shards(), 2);
        assert_eq!(non_default.total_shards(), 6);
    }

    #[test]
    fn fragment_v1_golden_vector_is_frozen() {
        let profile = ErasureProfile::new(2, 1, 4).unwrap();
        let payload = (0_u8..10).collect::<Vec<_>>();
        let (_, _, shards) = encode(profile, &payload);

        assert_eq!(
            shards.iter().map(hex::encode).collect::<Vec<_>>(),
            [
                "414e564c53485244000187fcf07cac5be3c91735b34e535c67286e4e7a63bf152d95f2cf4cd1a244758b000000000000000a00008e9f3c2d0000000411a4e2a00001020300000002e68073d20809",
                "414e564c53485244000187fcf07cac5be3c91735b34e535c67286e4e7a63bf152d95f2cf4cd1a244758b000000000000000a00017cf4bf2e000000049b9711320405060700000000ac4c06ff",
                "414e564c53485244000187fcf07cac5be3c91735b34e535c67286e4e7a63bf152d95f2cf4cd1a244758b000000000000000a00026fa44cda00000004002f737508090a0b0000000258f47777181b",
            ]
        );
    }

    #[test]
    fn every_allowed_missing_set_reconstructs_for_multiple_profiles() {
        for profile in [
            ErasureProfile::new(2, 1, 7).unwrap(),
            ErasureProfile::new(3, 2, 5).unwrap(),
        ] {
            let payload = (0..97)
                .map(|index| (index * 37 % 251) as u8)
                .collect::<Vec<_>>();
            let (codec, reference, shards) = encode(profile, &payload);
            let total = usize::from(profile.total_shards());

            for missing in 0_usize..(1_usize << total) {
                if missing.count_ones() > u32::from(profile.parity_shards()) {
                    continue;
                }
                let mut available = readers(&shards);
                for (ordinal, shard) in available.iter_mut().enumerate() {
                    if missing & (1 << ordinal) != 0 {
                        *shard = None;
                    }
                }
                let mut reconstructed = Vec::new();
                codec
                    .reconstruct(&reference, &mut available, &mut reconstructed)
                    .unwrap();
                assert_eq!(reconstructed, payload);
            }
        }
    }

    #[test]
    fn crc_rejects_a_corrupt_chunk_and_reconstruction_treats_it_as_missing() {
        let profile = ErasureProfile::new(2, 1, 8).unwrap();
        let payload = (0..61).map(|value| value as u8).collect::<Vec<_>>();
        let (codec, reference, mut shards) = encode(profile, &payload);
        shards[0][first_payload_offset() + 2] ^= 0x80;

        assert!(matches!(
            codec.validate_shard(&reference, 0, Cursor::new(&shards[0])),
            Err(ErasureError::ChunkChecksumMismatch {
                ordinal: 0,
                stripe: 0
            })
        ));

        let mut available = readers(&shards);
        let mut reconstructed = Vec::new();
        codec
            .reconstruct(&reference, &mut available, &mut reconstructed)
            .unwrap();
        assert_eq!(reconstructed, payload);
    }

    #[test]
    fn separate_stripes_can_recover_from_different_corrupt_shards() {
        let profile = ErasureProfile::new(2, 1, 4).unwrap();
        let payload = (0_u8..24).collect::<Vec<_>>();
        let (codec, reference, mut shards) = encode(profile, &payload);
        let first_frame_bytes =
            FRAME_PREFIX_BYTES + usize::try_from(profile.stripe_unit()).unwrap();
        shards[0][first_payload_offset()] ^= 1;
        shards[1][first_payload_offset() + first_frame_bytes] ^= 1;

        let mut available = readers(&shards);
        let mut reconstructed = Vec::new();
        codec
            .reconstruct(&reference, &mut available, &mut reconstructed)
            .unwrap();
        assert_eq!(reconstructed, payload);
    }

    #[test]
    fn too_many_corrupt_chunks_in_one_stripe_fail_explicitly() {
        let profile = ErasureProfile::new(2, 1, 8).unwrap();
        let payload = (0_u8..32).collect::<Vec<_>>();
        let (codec, reference, mut shards) = encode(profile, &payload);
        shards[0][first_payload_offset()] ^= 1;
        shards[1][first_payload_offset()] ^= 1;

        let mut available = readers(&shards);
        let mut reconstructed = Vec::new();
        assert!(matches!(
            codec.reconstruct(&reference, &mut available, &mut reconstructed),
            Err(ErasureError::TooFewValidChunks {
                stripe: 0,
                required: 2,
                available: 1
            })
        ));
    }

    #[test]
    fn source_and_reconstructed_bytes_must_match_the_blob_identity() {
        let profile = ErasureProfile::new(2, 1, 8).unwrap();
        let codec = ErasureCodec::new(profile).unwrap();
        let expected = reference(b"expected payload");
        let mut shards = vec![Vec::new(); usize::from(profile.total_shards())];
        assert!(matches!(
            codec.encode(Cursor::new(b"different bytes!"), &expected, &mut shards),
            Err(ErasureError::BlobHashMismatch)
        ));
    }

    #[test]
    fn final_blake3_rejects_crc_consistent_wrong_data() {
        let profile = ErasureProfile::new(2, 1, 8).unwrap();
        let payload = b"expected payload";
        let (codec, reference, mut shards) = encode(profile, payload);

        let frame_offset = HEADER_BYTES;
        let payload_offset = first_payload_offset();
        shards[0][payload_offset] ^= 1;
        let length = u32::from_be_bytes(
            shards[0][frame_offset..frame_offset + 4]
                .try_into()
                .unwrap(),
        );
        let payload_end = payload_offset + usize::try_from(length).unwrap();
        let checksum = chunk_checksum(
            &header_body(&reference, 0),
            0,
            0,
            length,
            &shards[0][payload_offset..payload_end],
        );
        shards[0][frame_offset + 4..frame_offset + FRAME_PREFIX_BYTES]
            .copy_from_slice(&checksum.to_be_bytes());

        let mut available = readers(&shards);
        let mut reconstructed = Vec::new();
        assert!(matches!(
            codec.reconstruct(&reference, &mut available, &mut reconstructed),
            Err(ErasureError::BlobHashMismatch)
        ));
    }

    #[test]
    fn shard_validation_rejects_truncation_and_trailing_bytes() {
        let profile = ErasureProfile::new(2, 1, 8).unwrap();
        let (codec, reference, shards) = encode(profile, b"one complete payload");

        let mut truncated = shards[0].clone();
        truncated.pop();
        assert!(
            codec
                .validate_shard(&reference, 0, Cursor::new(truncated))
                .is_err()
        );

        let mut trailing = shards[0].clone();
        trailing.push(1);
        assert!(matches!(
            codec.validate_shard(&reference, 0, Cursor::new(trailing)),
            Err(ErasureError::TrailingShardBytes { ordinal: 0 })
        ));
    }
}
