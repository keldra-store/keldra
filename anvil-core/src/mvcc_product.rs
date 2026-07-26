//! Stable product-facing logical keys and atomic transaction staging.
//!
//! Physical RocksDB column families and the retired root/publication scheme are
//! deliberately not transaction boundaries. They are retained in the key
//! encoding only as a schema namespace so two historical tables cannot alias.

use anyhow::{Result, bail};

use crate::{
    core_store::{
        CoreMutationOperation, TABLE_STREAM_RECORD_INDEX_ROW,
    },
    mvcc_bootstrap::MvccSubsystem,
    mvcc_open_transactions::StagedLogicalMutation,
    mvcc_transaction::LogicalKey,
};
use serde::Serialize;

const CORE_META_KEY_SCHEMA: &[u8] = b"anvil.mvcc.coremeta-key.v1";
const STREAM_KEY_SCHEMA: &[u8] = b"anvil.mvcc.stream-key.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductMutation {
    pub key: LogicalKey,
    pub value: Option<Vec<u8>>,
}

#[derive(Serialize)]
struct StreamRecordValue<'a> {
    schema: &'static str,
    stream_id: &'a str,
    record_kind: &'a str,
    idempotency_key: &'a str,
    payload: &'a [u8],
}

impl ProductMutation {
    pub fn put(key: LogicalKey, value: Vec<u8>) -> Self {
        Self {
            key,
            value: Some(value),
        }
    }

    pub fn delete(key: LogicalKey) -> Self {
        Self { key, value: None }
    }
}

/// Encode an ordinary CoreMeta row as an MVCC logical key.
pub fn coremeta_logical_key(cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<LogicalKey> {
    if cf.is_empty() {
        bail!("CoreMeta column-family namespace must not be empty");
    }
    let mut application_key = Vec::with_capacity(
        CORE_META_KEY_SCHEMA.len() + 2 + cf.len() + tuple_key.len(),
    );
    application_key.extend_from_slice(CORE_META_KEY_SCHEMA);
    push_len_prefixed(&mut application_key, cf.as_bytes())?;
    application_key.extend_from_slice(tuple_key);
    Ok(LogicalKey {
        table_id,
        application_key,
    })
}

/// Encode a stream position. Stream records and heads use their existing table
/// IDs; the application key remains ordered by stream ID and sequence.
pub fn stream_logical_key(
    table_id: u16,
    stream_id: &str,
    sequence: Option<u64>,
) -> Result<LogicalKey> {
    if stream_id.is_empty() {
        bail!("stream ID must not be empty");
    }
    let mut application_key =
        Vec::with_capacity(STREAM_KEY_SCHEMA.len() + 2 + stream_id.len() + 8);
    application_key.extend_from_slice(STREAM_KEY_SCHEMA);
    push_len_prefixed(&mut application_key, stream_id.as_bytes())?;
    if let Some(sequence) = sequence {
        application_key.extend_from_slice(&sequence.to_be_bytes());
    }
    Ok(LogicalKey {
        table_id,
        application_key,
    })
}

/// Translate logical CoreMeta and stream operations into their MVCC values.
///
/// Publication roots and physical partitions are intentionally absent. Stream
/// appends require an idempotency key, which is their stable logical record
/// identity before a commit version is assigned.
pub fn product_mutations_from_operations(
    operations: Vec<CoreMutationOperation>,
) -> Result<Vec<ProductMutation>> {
    operations
        .into_iter()
        .map(|operation| match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } => Ok(ProductMutation::put(
                coremeta_logical_key(&cf, table_id, &tuple_key)?,
                payload,
            )),
            CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => Ok(ProductMutation::delete(coremeta_logical_key(
                &cf, table_id, &tuple_key,
            )?)),
            CoreMutationOperation::StreamAppend {
                stream_id,
                record_kind,
                payload,
                idempotency_key,
                ..
            } => {
                let idempotency_key = idempotency_key
                    .ok_or_else(|| anyhow::anyhow!("MVCC stream append requires idempotency key"))?;
                let key = stream_logical_key(
                    TABLE_STREAM_RECORD_INDEX_ROW,
                    &stream_id,
                    Some(stable_stream_ordinal(&idempotency_key)),
                )?;
                let value = serde_json::to_vec(&StreamRecordValue {
                    schema: "anvil.mvcc.stream-record.v1",
                    stream_id: &stream_id,
                    record_kind: &record_kind,
                    idempotency_key: &idempotency_key,
                    payload: &payload,
                })?;
                Ok(ProductMutation::put(key, value))
            }
        })
        .collect()
}

impl MvccSubsystem {
    /// Observe and stage a complete product operation in one durable registry
    /// update. All reads use the transaction's original snapshot.
    pub fn stage_product_mutations(
        &self,
        transaction_id: &str,
        principal: &str,
        mutations: Vec<ProductMutation>,
        now_unix_ms: u64,
    ) -> Result<()> {
        let binding = self.open_transactions.binding(transaction_id, principal)?;
        let snapshot = self.open_transactions.handle(transaction_id)?.snapshot_version;
        let staged = mutations
            .into_iter()
            .map(|mutation| {
                let observed_version = self
                    .runtime
                    .read_at(&mutation.key, snapshot)?
                    .map(|row| row.commit_version);
                Ok(StagedLogicalMutation {
                    key: mutation.key,
                    observed_version,
                    value: mutation.value,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.open_transactions.stage_logical_mutations(
            transaction_id,
            principal,
            &binding.cluster_id,
            staged,
            now_unix_ms,
        )
    }
}

fn push_len_prefixed(output: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u16::try_from(value.len())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn stable_stream_ordinal(idempotency_key: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(idempotency_key.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 prefix is eight bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coremeta_keys_are_stable_and_column_family_namespaced() {
        let first = coremeta_logical_key("mesh", 0x8804, b"tenant/bucket").unwrap();
        let repeated = coremeta_logical_key("mesh", 0x8804, b"tenant/bucket").unwrap();
        let other_cf = coremeta_logical_key("registry", 0x8804, b"tenant/bucket").unwrap();
        assert_eq!(first, repeated);
        assert_ne!(first, other_cf);
        assert_eq!(first.table_id, 0x8804);
    }

    #[test]
    fn stream_sequences_preserve_numeric_order() {
        let earlier = stream_logical_key(0x8202, "events", Some(9)).unwrap();
        let later = stream_logical_key(0x8202, "events", Some(10)).unwrap();
        assert!(earlier.application_key < later.application_key);
    }
}
