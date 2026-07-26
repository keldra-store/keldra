//! Stable product-facing logical keys and atomic transaction staging.
//!
//! Physical RocksDB column families and the retired root/publication scheme are
//! deliberately not transaction boundaries. They are retained in the key
//! encoding only as a schema namespace so two historical tables cannot alias.

use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::{
    core_store::{CoreMutationOperation, TABLE_STREAM_RECORD_INDEX_ROW},
    mvcc_bootstrap::MvccSubsystem,
    mvcc_open_transactions::StagedLogicalMutation,
    mvcc_transaction::{CertificationResult, DurabilityLevel, LogicalKey, ReadConsistency},
};
use serde::{Deserialize, Serialize};

const CORE_META_KEY_SCHEMA: &[u8] = b"anvil.mvcc.coremeta-key.v1";
const STREAM_KEY_SCHEMA: &[u8] = b"anvil.mvcc.stream-key.v1";

pub fn stream_table_prefix() -> &'static [u8] {
    STREAM_KEY_SCHEMA
}

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

pub fn decode_stream_record_value(bytes: &[u8]) -> Result<(String, Vec<u8>)> {
    #[derive(Deserialize)]
    struct OwnedStreamRecordValue {
        schema: String,
        record_kind: String,
        payload: Vec<u8>,
    }
    let value: OwnedStreamRecordValue = serde_json::from_slice(bytes)?;
    if value.schema != "anvil.mvcc.stream-record.v1" {
        bail!("MVCC stream record has an unsupported schema");
    }
    Ok((value.record_kind, value.payload))
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
    let mut application_key =
        Vec::with_capacity(CORE_META_KEY_SCHEMA.len() + 2 + cf.len() + tuple_key.len());
    application_key.extend_from_slice(CORE_META_KEY_SCHEMA);
    push_len_prefixed(&mut application_key, cf.as_bytes())?;
    application_key.extend_from_slice(tuple_key);
    Ok(LogicalKey {
        table_id,
        application_key,
    })
}

pub fn coremeta_application_prefix(cf: &str, tuple_prefix: &[u8]) -> Result<Vec<u8>> {
    if cf.is_empty() {
        bail!("CoreMeta column-family namespace must not be empty");
    }
    let mut application_prefix =
        Vec::with_capacity(CORE_META_KEY_SCHEMA.len() + 2 + cf.len() + tuple_prefix.len());
    application_prefix.extend_from_slice(CORE_META_KEY_SCHEMA);
    push_len_prefixed(&mut application_prefix, cf.as_bytes())?;
    application_prefix.extend_from_slice(tuple_prefix);
    Ok(application_prefix)
}

pub fn coremeta_tuple_from_logical_key<'a>(
    key: &'a LogicalKey,
    expected_cf: &str,
) -> Result<&'a [u8]> {
    if !key.application_key.starts_with(CORE_META_KEY_SCHEMA) {
        bail!("logical key is not in the CoreMeta MVCC namespace");
    }
    let offset = CORE_META_KEY_SCHEMA.len();
    let length_bytes: [u8; 2] = key
        .application_key
        .get(offset..offset + 2)
        .context("CoreMeta MVCC key is missing its column-family length")?
        .try_into()?;
    let cf_length = usize::from(u16::from_be_bytes(length_bytes));
    let cf_start = offset + 2;
    let tuple_start = cf_start
        .checked_add(cf_length)
        .context("CoreMeta MVCC key column-family length overflow")?;
    let cf = key
        .application_key
        .get(cf_start..tuple_start)
        .context("CoreMeta MVCC key has a truncated column-family name")?;
    if cf != expected_cf.as_bytes() {
        bail!("CoreMeta MVCC key belongs to a different column family");
    }
    key.application_key
        .get(tuple_start..)
        .context("CoreMeta MVCC key is missing its tuple")
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
    let mut application_key = Vec::with_capacity(STREAM_KEY_SCHEMA.len() + 2 + stream_id.len() + 8);
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
                let idempotency_key = idempotency_key.ok_or_else(|| {
                    anyhow::anyhow!("MVCC stream append requires idempotency key")
                })?;
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
    pub fn stage_predicate(
        &self,
        transaction_id: &str,
        principal: &str,
        key: LogicalKey,
        kind: crate::mvcc_transaction::PredicateKind,
        now_unix_ms: u64,
    ) -> Result<()> {
        let binding = self.open_transactions.binding(transaction_id, principal)?;
        let snapshot = self
            .open_transactions
            .handle(transaction_id)?
            .snapshot_version;
        let visible = self.runtime.read_at(&key, snapshot)?;
        let satisfied = match &kind {
            crate::mvcc_transaction::PredicateKind::Unique
            | crate::mvcc_transaction::PredicateKind::Absent => visible.is_none(),
            crate::mvcc_transaction::PredicateKind::Exists => visible.is_some(),
            crate::mvcc_transaction::PredicateKind::ValueHash(expected) => visible
                .as_ref()
                .is_some_and(|row| blake3::hash(&row.value).as_bytes() == expected),
        };
        if !satisfied {
            bail!("MVCC transaction predicate is false at its snapshot");
        }
        self.open_transactions.add_predicate(
            transaction_id,
            &binding.cluster_id,
            key,
            kind,
            visible.map(|row| row.commit_version),
            now_unix_ms,
        )
    }

    /// Read the authoritative committed value at the node's latest applied MVCC
    /// version. Physical CoreStore projections are not consulted.
    pub fn read_latest_value(&self, key: &LogicalKey) -> Result<Option<Vec<u8>>> {
        Ok(self.runtime.read_latest(key)?.map(|visible| visible.value))
    }

    /// Execute an ordinary product mutation as a retry-stable MVCC transaction.
    ///
    /// The idempotency key deterministically selects the durable transaction
    /// draft. Retries of a committing or resolved draft resume certification
    /// instead of staging a second write.
    pub async fn autocommit_product_mutations(
        &self,
        principal: &str,
        idempotency_key: &str,
        mutations: Vec<ProductMutation>,
        durability: DurabilityLevel,
        now_unix_ms: u64,
    ) -> Result<u64> {
        self.autocommit_product_mutations_with_predicates(
            principal,
            idempotency_key,
            mutations,
            Vec::new(),
            durability,
            now_unix_ms,
        )
        .await
    }

    pub async fn autocommit_product_mutations_with_predicates(
        &self,
        principal: &str,
        idempotency_key: &str,
        mutations: Vec<ProductMutation>,
        predicates: Vec<(LogicalKey, crate::mvcc_transaction::PredicateKind)>,
        durability: DurabilityLevel,
        now_unix_ms: u64,
    ) -> Result<u64> {
        if mutations.is_empty() {
            bail!("MVCC autocommit requires at least one mutation");
        }
        let handle = self
            .open_transactions
            .begin(
                self.runtime.as_ref(),
                self.cluster_id(),
                principal,
                idempotency_key,
                Duration::from_secs(30),
                durability,
                ReadConsistency::Linearized,
                now_unix_ms,
            )
            .await?;
        let status =
            self.open_transactions
                .status(&handle.transaction_id, principal, now_unix_ms)?;
        if status.state == "open" {
            self.stage_product_mutations(
                &handle.transaction_id,
                principal,
                mutations,
                now_unix_ms,
            )?;
            for (key, kind) in predicates {
                self.stage_predicate(&handle.transaction_id, principal, key, kind, now_unix_ms)?;
            }
        }
        let outcome = self
            .open_transactions
            .commit(
                self.runtime.as_ref(),
                &handle.transaction_id,
                principal,
                now_unix_ms,
            )
            .await?;
        match outcome.certification {
            CertificationResult::Committed { commit_version } => Ok(commit_version),
            CertificationResult::Aborted { reason } => {
                bail!("MVCC autocommit transaction aborted: {reason:?}")
            }
        }
    }

    /// Read through this transaction's own write set, then through its fixed
    /// MVCC snapshot. A staged tombstone is returned as an absent value.
    pub fn read_transaction_value(
        &self,
        transaction_id: &str,
        principal: &str,
        key: &LogicalKey,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(staged) = self
            .open_transactions
            .staged_value(transaction_id, principal, key)?
        {
            return Ok(staged);
        }
        let snapshot = self
            .open_transactions
            .handle(transaction_id)?
            .snapshot_version;
        Ok(self
            .runtime
            .read_at(key, snapshot)?
            .map(|visible| visible.value))
    }

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
        let snapshot = self
            .open_transactions
            .handle(transaction_id)?
            .snapshot_version;
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
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("sha256 prefix is eight bytes"),
    )
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
