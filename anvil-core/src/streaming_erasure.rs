//! Bounded-stripe Reed-Solomon encoding for distributed ingest.

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reed_solomon_erasure::galois_8::ReedSolomon;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

use crate::shard_store::ShardKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErasureProfile {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub shard_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct EncodedShard<'a> {
    pub provisional: bool,
    pub transaction_id: &'a str,
    pub prepared_snapshot_version: u64,
    pub prepared_at_unix_ms: u64,
    pub object_identity: Uuid,
    pub encoding_generation: u64,
    pub stripe_ordinal: u64,
    pub shard_ordinal: u16,
    pub kind: ShardKind,
    pub stripe_plaintext_length: usize,
    pub payload: &'a [u8],
    pub payload_hash: [u8; 32],
}

/// Implementations route shard ordinals to final target streams. Awaiting
/// `send` propagates target backpressure to the client reader.
#[async_trait]
pub trait ShardSink: Send {
    async fn send(&mut self, shard: EncodedShard<'_>) -> Result<()>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedObject {
    pub content_hash: [u8; 32],
    pub object_length: u64,
    pub stripe_count: u64,
}

pub struct StreamingErasureEncoder {
    profile: ErasureProfile,
    codec: ReedSolomon,
}

impl StreamingErasureEncoder {
    pub fn new(profile: ErasureProfile) -> Result<Self> {
        if profile.data_shards == 0 || profile.parity_shards == 0 || profile.shard_bytes == 0 {
            bail!("distributed erasure profile requires non-zero data, parity, and shard sizes");
        }
        profile
            .data_shards
            .checked_mul(profile.shard_bytes)
            .context("erasure stripe size overflow")?;
        let codec = ReedSolomon::new(profile.data_shards, profile.parity_shards)?;
        Ok(Self { profile, codec })
    }

    /// Holds only one stripe plus its parity shards, independent of object size.
    pub async fn encode<R: AsyncRead + Unpin + Send, S: ShardSink>(
        &self,
        reader: &mut R,
        transaction_id: &str,
        prepared_snapshot_version: u64,
        prepared_at_unix_ms: u64,
        provisional: bool,
        object_identity: Uuid,
        encoding_generation: u64,
        sink: &mut S,
    ) -> Result<EncodedObject> {
        let stripe_capacity = self.profile.data_shards * self.profile.shard_bytes;
        let mut stripe = vec![0; stripe_capacity];
        let mut object_hash = Sha256::new();
        let mut object_length = 0_u64;
        let mut stripe_ordinal = 0_u64;
        loop {
            let mut filled = 0;
            while filled < stripe_capacity {
                let read = reader.read(&mut stripe[filled..]).await?;
                if read == 0 {
                    break;
                }
                object_hash.update(&stripe[filled..filled + read]);
                object_length += read as u64;
                filled += read;
            }
            // An empty object still needs one durable physical stripe. Its
            // data and parity shards are all zeroes, while the logical length
            // remains zero and the content hash remains SHA-256(empty).
            // Exact non-empty stripe multiples must not gain an extra stripe.
            if filled == 0 && stripe_ordinal != 0 {
                break;
            }
            stripe[filled..].fill(0);
            let mut shards: Vec<Vec<u8>> = stripe
                .chunks_exact(self.profile.shard_bytes)
                .map(<[u8]>::to_vec)
                .collect();
            shards
                .extend((0..self.profile.parity_shards).map(|_| vec![0; self.profile.shard_bytes]));
            let encode_started_at = std::time::Instant::now();
            self.codec.encode(&mut shards)?;
            crate::perf::record_ingest_stripe_encode("ok", encode_started_at.elapsed());
            tracing::debug!(
                operation = "ingest.erasure_encode",
                stripe_ordinal,
                data_shards = self.profile.data_shards,
                parity_shards = self.profile.parity_shards,
                plaintext_bytes = filled,
                "encoded erasure stripe"
            );
            for (ordinal, payload) in shards.iter().enumerate() {
                sink.send(EncodedShard {
                    provisional,
                    transaction_id,
                    prepared_snapshot_version,
                    prepared_at_unix_ms,
                    object_identity,
                    encoding_generation,
                    stripe_ordinal,
                    shard_ordinal: ordinal.try_into().context("too many erasure shards")?,
                    kind: if ordinal < self.profile.data_shards {
                        ShardKind::Data
                    } else {
                        ShardKind::Parity
                    },
                    stripe_plaintext_length: filled,
                    payload,
                    payload_hash: *blake3::hash(payload).as_bytes(),
                })
                .await?;
            }
            tracing::debug!(
                operation = "ingest.stripe",
                stripe_ordinal,
                shard_count = shards.len(),
                plaintext_bytes = filled,
                "streamed complete erasure stripe"
            );
            stripe_ordinal += 1;
            if filled < stripe_capacity {
                break;
            }
        }
        Ok(EncodedObject {
            content_hash: object_hash.finalize().into(),
            object_length,
            stripe_count: stripe_ordinal,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[derive(Default)]
    struct Sink(Vec<(u64, u16, ShardKind, usize, Vec<u8>)>);
    #[async_trait]
    impl ShardSink for Sink {
        async fn send(&mut self, shard: EncodedShard<'_>) -> Result<()> {
            assert_eq!(shard.payload_hash, *blake3::hash(shard.payload).as_bytes());
            self.0.push((
                shard.stripe_ordinal,
                shard.shard_ordinal,
                shard.kind,
                shard.stripe_plaintext_length,
                shard.payload.to_vec(),
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn emits_complete_stripes_before_end_and_pads_only_the_tail() {
        let encoder = StreamingErasureEncoder::new(ErasureProfile {
            data_shards: 2,
            parity_shards: 1,
            shard_bytes: 4,
        })
        .unwrap();
        let input = b"abcdefghijk";
        let mut reader = Cursor::new(input);
        let mut sink = Sink::default();
        let result = encoder
            .encode(&mut reader, "tx", 1, 1, true, Uuid::new_v4(), 1, &mut sink)
            .await
            .unwrap();
        assert_eq!(result.object_length, 11);
        let expected_hash: [u8; 32] = Sha256::digest(input).into();
        assert_eq!(result.content_hash, expected_hash);
        assert_eq!(result.stripe_count, 2);
        assert_eq!(sink.0.len(), 6);
        assert_eq!(sink.0[0].4, b"abcd");
        assert_eq!(sink.0[1].4, b"efgh");
        assert_eq!(sink.0[3].4, b"ijk\0");
    }

    #[tokio::test]
    async fn empty_object_emits_one_durable_zero_length_stripe() {
        let encoder = StreamingErasureEncoder::new(ErasureProfile {
            data_shards: 2,
            parity_shards: 1,
            shard_bytes: 4,
        })
        .unwrap();
        let mut reader = std::io::Cursor::new(Vec::<u8>::new());
        let mut sink = Sink::default();
        let result = encoder
            .encode(&mut reader, "tx", 1, 1, true, Uuid::new_v4(), 1, &mut sink)
            .await
            .unwrap();

        assert_eq!(result.object_length, 0);
        let empty_hash: [u8; 32] = Sha256::digest([]).into();
        assert_eq!(result.content_hash, empty_hash);
        assert_eq!(result.stripe_count, 1);
        assert_eq!(sink.0.len(), 3);
        assert!(sink.0.iter().all(|entry| entry.3 == 0));
        assert!(sink.0.iter().all(|entry| entry.4.as_slice() == [0; 4]));
    }
}
