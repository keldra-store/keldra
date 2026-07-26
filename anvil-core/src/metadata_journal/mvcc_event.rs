use super::*;
use crate::core_store::{TABLE_STREAM_HEAD_ROW, TABLE_STREAM_RECORD_INDEX_ROW};
use crate::mvcc_product::{ProductMutation, stream_logical_key};
use crate::mvcc_transaction::{LogicalKey, PredicateKind};
use anyhow::bail;
use serde::{Deserialize, Serialize};

const METADATA_HEAD_SCHEMA: &str = "anvil.object-metadata.journal-head.v2";
const METADATA_EVENT_SCHEMA: &str = "anvil.object-metadata.journal-event.v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MetadataJournalHead {
    schema: String,
    pub last_sequence: u64,
    pub last_event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct MetadataJournalEvent {
    schema: String,
    pub partition_sequence: u64,
    pub previous_event_hash: String,
    pub event_hash: String,
    pub mutation_id: String,
    pub record_kind: String,
    pub payload_ref: String,
    pub payload: Vec<u8>,
}

pub(super) struct MetadataEventPlan {
    pub mutations: Vec<ProductMutation>,
    pub head_predicate: (LogicalKey, PredicateKind),
}

pub(super) fn plan_metadata_events(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    operations: Vec<CoreMutationOperation>,
    transaction: Option<(&str, &str)>,
) -> Result<MetadataEventPlan> {
    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let head_key = stream_logical_key(TABLE_STREAM_HEAD_ROW, &stream_id, None)?;
    let (current_payload, predicate_payload) =
        if let Some((transaction_id, principal)) = transaction {
            let snapshot = mvcc
                .open_transactions
                .handle(transaction_id)?
                .snapshot_version;
            (
                mvcc.read_transaction_value(transaction_id, principal, &head_key)?,
                mvcc.runtime
                    .read_at(&head_key, snapshot)?
                    .map(|row| row.value),
            )
        } else {
            let current = mvcc.read_latest_value(&head_key)?;
            (current.clone(), current)
        };
    let mut head = current_payload
        .as_deref()
        .map(decode_head)
        .transpose()?
        .unwrap_or(MetadataJournalHead {
            schema: METADATA_HEAD_SCHEMA.to_string(),
            last_sequence: 0,
            last_event_hash: String::new(),
        });
    let head_predicate = (
        head_key.clone(),
        predicate_payload
            .as_ref()
            .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
            .unwrap_or(PredicateKind::Absent),
    );
    let mut mutations = Vec::new();
    for operation in operations {
        match operation {
            CoreMutationOperation::StreamAppend {
                stream_id: operation_stream_id,
                record_kind,
                payload,
                idempotency_key,
                ..
            } if operation_stream_id == stream_id => {
                head.last_sequence = head
                    .last_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("object metadata journal sequence overflow"))?;
                let body = decode_object_version_body(&payload)?;
                let mutation_id = idempotency_key.unwrap_or(body.mutation_id);
                let payload_hash = hex::encode(hash32(&payload));
                let payload_ref = format!("inline:sha256:{payload_hash}");
                let event_hash = event_hash(
                    head.last_sequence,
                    &head.last_event_hash,
                    &mutation_id,
                    &record_kind,
                    &payload_ref,
                );
                let event = MetadataJournalEvent {
                    schema: METADATA_EVENT_SCHEMA.to_string(),
                    partition_sequence: head.last_sequence,
                    previous_event_hash: head.last_event_hash.clone(),
                    event_hash: event_hash.clone(),
                    mutation_id,
                    record_kind,
                    payload_ref,
                    payload,
                };
                head.last_event_hash = event_hash;
                mutations.push(ProductMutation::put(
                    stream_logical_key(
                        TABLE_STREAM_RECORD_INDEX_ROW,
                        &stream_id,
                        Some(head.last_sequence),
                    )?,
                    serde_json::to_vec(&event)?,
                ));
            }
            other => mutations.extend(crate::mvcc_product::product_mutations_from_operations(
                vec![other],
            )?),
        }
    }
    mutations.push(ProductMutation::put(head_key, serde_json::to_vec(&head)?));
    Ok(MetadataEventPlan {
        mutations,
        head_predicate,
    })
}

pub(super) fn decode_head(payload: &[u8]) -> Result<MetadataJournalHead> {
    let head: MetadataJournalHead = serde_json::from_slice(payload)?;
    if head.schema != METADATA_HEAD_SCHEMA {
        bail!("object metadata journal head has unsupported schema");
    }
    Ok(head)
}

pub(super) fn decode_event(payload: &[u8]) -> Result<MetadataJournalEvent> {
    let event: MetadataJournalEvent = serde_json::from_slice(payload)?;
    if event.schema != METADATA_EVENT_SCHEMA
        || event.partition_sequence == 0
        || event.payload_ref != format!("inline:sha256:{}", hex::encode(hash32(&event.payload)))
    {
        bail!("object metadata journal event is invalid");
    }
    Ok(event)
}

pub(super) fn validate_event_chain(
    events: &[MetadataJournalEvent],
    mut previous_sequence: u64,
    mut previous_hash: String,
) -> Result<()> {
    for event in events {
        if event.partition_sequence != previous_sequence.saturating_add(1)
            || event.previous_event_hash != previous_hash
            || event.event_hash
                != event_hash(
                    event.partition_sequence,
                    &event.previous_event_hash,
                    &event.mutation_id,
                    &event.record_kind,
                    &event.payload_ref,
                )
        {
            bail!("object metadata MVCC event hash chain is discontinuous");
        }
        previous_sequence = event.partition_sequence;
        previous_hash.clone_from(&event.event_hash);
    }
    Ok(())
}

fn event_hash(
    sequence: u64,
    previous_hash: &str,
    mutation_id: &str,
    record_kind: &str,
    payload_ref: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(METADATA_EVENT_SCHEMA.as_bytes());
    hasher.update(&sequence.to_be_bytes());
    for value in [previous_hash, mutation_id, record_kind, payload_ref] {
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}
