use super::local_tx_rows::OwnedCoreMetaBatchOp;
use super::*;
use crate::formats::writer::WriterFamily;

struct StreamBatchState {
    root_anchor_key: String,
    writer_family: WriterFamily,
    transaction_coordinator: bool,
    current_head_sequence: u64,
    physical_frontier_sequence: u64,
    planned_sequence: u64,
    previous_event_hash: String,
    new_records: Vec<StreamRecord>,
}

pub(super) async fn prepare_mutation_batch_operations(
    store: &CoreStore,
    batch: &CoreMutationBatch,
) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreTransactionUpdate>)> {
    let mut stream_states = initialise_stream_states(store, batch).await?;
    let mut visibility_cache = super::local_internal_coremeta::CoreMetaVisibilityCache::default();
    let mut owned_ops = Vec::new();
    let mut updates = vec![None; batch.operations.len()];

    for (operation_index, operation) in batch.operations.iter().enumerate() {
        match operation {
            CoreMutationOperation::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } => {
                let (op, update) = prepare_coremeta_put(
                    store,
                    batch,
                    &mut visibility_cache,
                    cf,
                    *table_id,
                    tuple_key,
                    payload,
                )?;
                if let Some(op) = op {
                    owned_ops.push(op);
                }
                updates[operation_index] = Some(update);
            }
            CoreMutationOperation::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => {
                let rooted_generation = store
                    .rooted_delete_generation_unlocked(batch, cf, *table_id, tuple_key)
                    .await?;
                let (op, update) = store.prepare_coremeta_delete_update_unlocked(
                    cf,
                    *table_id,
                    tuple_key,
                    batch.transaction_id.clone(),
                    rooted_generation,
                )?;
                owned_ops.push(op);
                updates[operation_index] = Some(update);
            }
            CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind,
                payload,
                idempotency_key,
            } => {
                let state = stream_states
                    .get_mut(stream_id)
                    .ok_or_else(|| anyhow!("CoreStore stream batch state is missing"))?;
                let expected_sequence = state
                    .planned_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))?;
                let existing = store
                    .read_stream_record_from_meta(stream_id, expected_sequence)
                    .await?;
                let record = match existing {
                    Some(record) => {
                        if !state.new_records.is_empty() {
                            bail!(
                                "CoreStore stream {stream_id} has a committed record after an unmaterialised batch operation"
                            );
                        }
                        validate_existing_stream_operation(
                            batch,
                            partition_id,
                            stream_id,
                            record_kind,
                            payload,
                            idempotency_key.as_deref(),
                            expected_sequence,
                            &state.previous_event_hash,
                            &record,
                        )?;
                        state.physical_frontier_sequence = expected_sequence;
                        record
                    }
                    None => {
                        if state.current_head_sequence > state.physical_frontier_sequence {
                            bail!(
                                "CoreStore stream {stream_id} advanced past an unmaterialised mutation batch operation"
                            );
                        }
                        let idempotency_key_hash = idempotency_key
                            .as_deref()
                            .map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
                        let record = build_stream_record_after_head(
                            AppendStreamRecord {
                                stream_id: stream_id.clone(),
                                partition_id: partition_id.clone(),
                                record_kind: record_kind.clone(),
                                payload: payload.clone(),
                                content_type: None,
                                user_metadata_json: "{}".to_string(),
                                fence: None,
                                transaction_id: Some(batch.transaction_id.clone()),
                                idempotency_key: idempotency_key.clone(),
                            },
                            idempotency_key_hash,
                            batch.committed_by_principal.clone(),
                            state.planned_sequence,
                            state.previous_event_hash.clone(),
                            now_rfc3339(),
                        )?;
                        state.new_records.push(record.clone());
                        record
                    }
                };
                state.planned_sequence = record.sequence;
                state.previous_event_hash = record.event_hash.clone();
                updates[operation_index] = Some(stream_update_from_record(record));
            }
        }
    }

    for (stream_id, state) in stream_states {
        if state.new_records.is_empty() {
            continue;
        }
        let root_generation = store
            .read_latest_root_anchor(&state.root_anchor_key)
            .await?
            .map_or(Ok(1), |anchor| {
                anchor
                    .root_generation
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("CoreMeta stream root generation overflow"))
            })?;
        let mut prepared = store
            .prepare_stream_metadata_rows_for_root(
                &stream_id,
                &state.new_records,
                &state.root_anchor_key,
                root_generation,
                &batch.transaction_id,
                state.writer_family,
                state.transaction_coordinator,
            )
            .await?;
        owned_ops.append(&mut prepared.owned_ops);
    }

    let updates = updates
        .into_iter()
        .enumerate()
        .map(|(index, update)| {
            update.ok_or_else(|| anyhow!("CoreStore mutation operation {index} was not prepared"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((owned_ops, updates))
}

async fn initialise_stream_states(
    store: &CoreStore,
    batch: &CoreMutationBatch,
) -> Result<BTreeMap<String, StreamBatchState>> {
    let mut stream_ids = BTreeSet::new();
    for operation in &batch.operations {
        let CoreMutationOperation::StreamAppend { stream_id, .. } = operation else {
            continue;
        };
        if stream_id == CORE_TRANSACTION_STREAM_ID {
            bail!("CoreStore mutation batch cannot append the transaction stream");
        }
        stream_ids.insert(stream_id.clone());
    }

    let mut states = BTreeMap::new();
    for stream_id in stream_ids {
        let current_head = store.read_stream_head_from_meta(&stream_id)?;
        let (current_sequence, current_hash) = current_head
            .as_ref()
            .map(|head| (head.last_sequence, head.last_event_hash.clone()))
            .unwrap_or_else(|| (0, ZERO_HASH.to_string()));
        let (base_sequence, base_hash) = stream_precondition(batch, &stream_id)?
            .unwrap_or_else(|| (current_sequence, current_hash.clone()));
        if current_sequence < base_sequence {
            bail!("CoreStore stream {stream_id} is behind its admitted mutation precondition");
        }
        if current_sequence == base_sequence && current_hash != base_hash {
            bail!("CoreStore stream {stream_id} precondition hash no longer matches");
        }

        let root_anchor_key =
            super::local_roots_layout::stream_coremeta_root_anchor_key(&stream_id);
        let stream_root_key_hash = root_key_hash(&root_anchor_key);
        let publication = batch
            .root_publications
            .iter()
            .find(|publication| root_key_hash(&publication.root_anchor_key) == stream_root_key_hash)
            .ok_or_else(|| {
                anyhow!(
                    "CoreMeta stream append does not declare canonical root {stream_root_key_hash}"
                )
            })?;
        let writer_family = publication
            .writer_families
            .iter()
            .find_map(|family| WriterFamily::from_name(family))
            .ok_or_else(|| anyhow!("CoreMeta stream root has no recognised writer family"))?;
        states.insert(
            stream_id,
            StreamBatchState {
                root_anchor_key,
                writer_family,
                transaction_coordinator: publication.transaction_coordinator,
                current_head_sequence: current_sequence,
                physical_frontier_sequence: base_sequence,
                planned_sequence: base_sequence,
                previous_event_hash: base_hash,
                new_records: Vec::new(),
            },
        );
    }
    Ok(states)
}

pub(super) fn stream_precondition(
    batch: &CoreMutationBatch,
    stream_id: &str,
) -> Result<Option<(u64, String)>> {
    let mut matched = batch
        .preconditions
        .iter()
        .filter_map(|precondition| match precondition {
            CoreMutationPrecondition::StreamHead {
                stream_id: candidate,
                expected_last_sequence,
                expected_last_event_hash,
            } if candidate == stream_id => {
                Some((*expected_last_sequence, expected_last_event_hash.clone()))
            }
            _ => None,
        });
    let first = matched.next();
    if matched.next().is_some() {
        bail!("CoreStore mutation batch declares a stream precondition more than once");
    }
    Ok(first)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_existing_stream_operation(
    batch: &CoreMutationBatch,
    partition_id: &str,
    stream_id: &str,
    record_kind: &str,
    payload: &[u8],
    idempotency_key: Option<&str>,
    expected_sequence: u64,
    expected_previous_event_hash: &str,
    record: &StreamRecord,
) -> Result<()> {
    let expected_idempotency_hash =
        idempotency_key.map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
    if record.stream_id != stream_id
        || record.partition_id != partition_id
        || record.sequence != expected_sequence
        || record.previous_event_hash != expected_previous_event_hash
        || record.record_kind != record_kind
        || record.payload != payload
        || record.content_type.is_some()
        || record.user_metadata_json != "{}"
        || record.authenticated_principal != batch.committed_by_principal
        || record.transaction_id.as_deref() != Some(batch.transaction_id.as_str())
        || record.idempotency_key_hash != expected_idempotency_hash
    {
        bail!(
            "CoreStore stream {stream_id} sequence {expected_sequence} conflicts with the admitted mutation batch"
        );
    }
    Ok(())
}

fn prepare_coremeta_put(
    store: &CoreStore,
    batch: &CoreMutationBatch,
    visibility_cache: &mut super::local_internal_coremeta::CoreMetaVisibilityCache,
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
    payload: &[u8],
) -> Result<(Option<OwnedCoreMetaBatchOp>, CoreTransactionUpdate)> {
    let cf = canonical_coremeta_cf_name(cf)?;
    let previous_payload =
        store.read_coremeta_row_with_visibility_cache(cf, table_id, tuple_key, visibility_cache)?;
    if previous_payload.as_deref() == Some(payload) {
        let common = core_meta_row_common_from_payload(payload)?;
        if !common.root_key_hash.is_empty() && common.transaction_id != batch.transaction_id {
            bail!("CoreMeta recovery payload is owned by another transaction");
        }
        let previous_payload_hash =
            matching_coremeta_precondition_hash(batch, cf, table_id, tuple_key)?;
        return Ok((
            None,
            CoreTransactionUpdate::CoreMetaPut {
                cf: cf.to_string(),
                table_id,
                tuple_key: tuple_key.to_vec(),
                previous_payload_hash,
                payload: payload.to_vec(),
                payload_hash: core_meta_payload_digest(table_id, payload),
            },
        ));
    }
    let (op, update) = store.prepare_coremeta_put_update_unlocked(
        cf,
        table_id,
        tuple_key,
        previous_payload,
        payload,
    )?;
    Ok((Some(op), update))
}

fn matching_coremeta_precondition_hash(
    batch: &CoreMutationBatch,
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> Result<Option<String>> {
    let mut hashes = batch
        .preconditions
        .iter()
        .filter_map(|precondition| match precondition {
            CoreMutationPrecondition::CoreMetaRow {
                cf: candidate_cf,
                table_id: candidate_table,
                tuple_key: candidate_key,
                expected_payload_hash,
                ..
            } if canonical_coremeta_cf_name(candidate_cf).ok() == Some(cf)
                && *candidate_table == table_id
                && candidate_key == tuple_key =>
            {
                Some(expected_payload_hash.clone())
            }
            CoreMutationPrecondition::CoreMetaLease {
                cf: candidate_cf,
                table_id: candidate_table,
                tuple_key: candidate_key,
                expected_payload_hash,
                ..
            } if canonical_coremeta_cf_name(candidate_cf).ok() == Some(cf)
                && *candidate_table == table_id
                && candidate_key == tuple_key =>
            {
                Some(Some(expected_payload_hash.clone()))
            }
            _ => None,
        });
    let first = hashes.next().flatten();
    if hashes.next().is_some() {
        bail!("CoreStore mutation batch declares a CoreMeta row precondition more than once");
    }
    Ok(first)
}

fn stream_update_from_record(record: StreamRecord) -> CoreTransactionUpdate {
    CoreTransactionUpdate::StreamAppend {
        partition_id: record.partition_id,
        stream_id: record.stream_id,
        record_kind: record.record_kind,
        payload: record.payload,
        idempotency_key_hash: record.idempotency_key_hash,
        visible_sequence: record.sequence,
        previous_event_hash: record.previous_event_hash,
        prepared_record_hash: record.event_hash,
        created_at: record.created_at,
    }
}
