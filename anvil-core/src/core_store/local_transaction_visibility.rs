use super::local_stream_control::control_record_proto::decode_stream_record_index_row;
use super::local_transactions::{
    transaction_lists_stream_record, transaction_lists_stream_record_identity,
    validate_core_meta_row_precondition,
};
use super::local_tx_rows::{CoreTransactionPreconditionRow, OwnedCoreMetaBatchOp};
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamWatchVisibility {
    Visible,
    Pending,
    TerminalInvisible,
}

impl CoreStore {
    pub(super) async fn stream_record_watch_visibility(
        &self,
        record: &StreamRecord,
    ) -> Result<StreamWatchVisibility> {
        if record.stream_id == CORE_TRANSACTION_STREAM_ID || record.transaction_id.is_none() {
            return Ok(StreamWatchVisibility::Visible);
        }
        let transaction_id = record
            .transaction_id
            .as_deref()
            .expect("transaction id was checked above");
        let Some(transaction) = self.read_transaction_unlocked(transaction_id).await? else {
            // Mutation operations are durable before their transaction commit
            // record. A watcher must retry this row rather than advancing past
            // data which may become visible after recovery completes the commit.
            return Ok(StreamWatchVisibility::Pending);
        };
        match transaction.state {
            CoreTransactionState::Committed => {
                if !transaction_lists_stream_record(&transaction, record)? {
                    let published = transaction
                        .visible_updates
                        .iter()
                        .filter_map(|update| match update {
                            CoreTransactionUpdate::StreamAppend {
                                stream_id,
                                visible_sequence,
                                prepared_record_hash,
                                ..
                            } => Some(format!(
                                "{stream_id}:{visible_sequence}:{prepared_record_hash}"
                            )),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    bail!(
                        "CoreStore committed transaction {} does not publish stream record {}:{}:{}; published stream records [{}]",
                        transaction_id,
                        record.stream_id,
                        record.sequence,
                        record.event_hash,
                        published
                    );
                }
                Ok(StreamWatchVisibility::Visible)
            }
            CoreTransactionState::Open | CoreTransactionState::Prepared => {
                Ok(StreamWatchVisibility::Pending)
            }
            CoreTransactionState::FinalisationFailed
            | CoreTransactionState::Aborted
            | CoreTransactionState::RolledBack
            | CoreTransactionState::Expired
            | CoreTransactionState::Failed => Ok(StreamWatchVisibility::TerminalInvisible),
        }
    }

    pub(super) async fn transaction_makes_stream_record_visible(
        &self,
        record: &StreamRecord,
        transaction_id: &str,
    ) -> Result<bool> {
        let Some(transaction) = self.read_transaction_unlocked(transaction_id).await? else {
            return Ok(false);
        };
        if transaction.state != CoreTransactionState::Committed {
            return Ok(false);
        }
        transaction_lists_stream_record(&transaction, record)
    }

    pub(super) async fn stream_record_identity_is_visible(
        &self,
        stream_id: &str,
        sequence: u64,
        event_hash: &str,
        transaction_id: Option<&str>,
    ) -> Result<bool> {
        let Some(transaction_id) = transaction_id else {
            return Ok(true);
        };
        let Some(transaction) = self.read_transaction_unlocked(transaction_id).await? else {
            return Ok(false);
        };
        Ok(transaction.state == CoreTransactionState::Committed
            && transaction_lists_stream_record_identity(
                &transaction,
                stream_id,
                sequence,
                event_hash,
            ))
    }

    pub(super) async fn filter_committed_stream_records(
        &self,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>> {
        let mut visible = Vec::with_capacity(records.len());
        for record in records {
            match self.stream_record_watch_visibility(&record).await? {
                StreamWatchVisibility::Visible => visible.push(record),
                StreamWatchVisibility::Pending | StreamWatchVisibility::TerminalInvisible => {}
            }
        }
        Ok(visible)
    }

    pub async fn read_coremeta_row_visible_to_transaction(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction_id: &str,
        principal: &str,
    ) -> Result<Option<Vec<u8>>> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        validate_coremeta_operation_key(cf, table_id, tuple_key)?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        if transaction.state != CoreTransactionState::Open {
            bail!("TransactionNotOpen");
        }
        self.coremeta_payload_visible_to_transaction_unlocked(cf, table_id, tuple_key, &transaction)
    }

    pub async fn read_stream_visible_to_transaction(
        &self,
        input: ReadStream,
        transaction_id: &str,
        principal: &str,
    ) -> Result<Vec<StreamRecord>> {
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        if transaction.state != CoreTransactionState::Open {
            bail!("TransactionNotOpen");
        }

        let records = self
            .read_stream_records_after(&input.stream_id, input.after_sequence, 0)
            .await?;
        let mut visible = Vec::new();
        for record in records {
            if let Some(record_transaction_id) = record.transaction_id.as_deref() {
                if record_transaction_id == transaction_id
                    && transaction_lists_stream_record(&transaction, &record)?
                {
                    visible.push(record);
                } else if self
                    .transaction_makes_stream_record_visible(&record, record_transaction_id)
                    .await?
                {
                    visible.push(record);
                }
            } else {
                visible.push(record);
            }
        }
        if input.limit > 0 {
            visible.truncate(input.limit);
        }
        Ok(visible)
    }

    pub(super) fn coremeta_payload_visible_to_transaction_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction: &CoreTransaction,
    ) -> Result<Option<Vec<u8>>> {
        let cf = canonical_coremeta_cf_name(cf)?;
        let mut current = self.committed_coremeta_payload_unlocked(cf, table_id, tuple_key)?;
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf: update_cf,
                    table_id: update_table_id,
                    tuple_key: update_tuple_key,
                    payload,
                    ..
                } => {
                    if canonical_coremeta_cf_name(update_cf)? == cf
                        && *update_table_id == table_id
                        && update_tuple_key == tuple_key
                    {
                        current = Some(payload.clone());
                    }
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf: update_cf,
                    table_id: update_table_id,
                    tuple_key: update_tuple_key,
                    ..
                } => {
                    if canonical_coremeta_cf_name(update_cf)? == cf
                        && *update_table_id == table_id
                        && update_tuple_key == tuple_key
                    {
                        current = None;
                    }
                }
                _ => {}
            }
        }
        Ok(current)
    }

    pub(super) fn committed_coremeta_payload_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let cf = canonical_coremeta_cf_name(cf)?;
        self.read_coremeta_row(cf, table_id, tuple_key)
    }

    pub(super) async fn validate_explicit_transaction_commit_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        let step_started_at = std::time::Instant::now();
        self.validate_explicit_transaction_stream_commits_unlocked(transaction)
            .await?;
        crate::emit_test_timing(
            format!(
                "core_store.commit_explicit_transaction validate_streams tx={}",
                transaction.transaction_id
            ),
            step_started_at.elapsed(),
        );
        let step_started_at = std::time::Instant::now();
        self.validate_explicit_transaction_coremeta_commits_unlocked(transaction)
            .await?;
        crate::emit_test_timing(
            format!(
                "core_store.commit_explicit_transaction validate_coremeta tx={}",
                transaction.transaction_id
            ),
            step_started_at.elapsed(),
        );
        Ok(())
    }

    pub(super) async fn validate_implicit_transaction_publication_unlocked(
        &self,
        transaction: &CoreTransaction,
        publication_generations: &BTreeMap<String, u64>,
    ) -> Result<()> {
        validate_transaction_root_scope(transaction)?;
        let coordinator_generation = transaction.committed_root_generation.ok_or_else(|| {
            anyhow!("CoreStore implicit transaction has no coordinator generation")
        })?;
        match publication_generations.get(&transaction.root_key_hash) {
            Some(generation) if *generation == coordinator_generation => {}
            Some(_) => {
                bail!(
                    "TransactionScopeMismatch: implicit transaction coordinator generation does not match publication"
                )
            }
            None => {
                bail!(
                    "TransactionScopeMismatch: implicit transaction coordinator root is not published"
                )
            }
        }

        self.validate_implicit_transaction_stream_publications_unlocked(
            transaction,
            publication_generations,
        )
        .await?;
        self.validate_implicit_transaction_coremeta_publications_unlocked(
            transaction,
            publication_generations,
        )
        .await
    }

    pub(super) fn prepare_coremeta_put_update_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        previous_payload: Option<Vec<u8>>,
        payload: &[u8],
    ) -> Result<(OwnedCoreMetaBatchOp, CoreTransactionUpdate)> {
        validate_coremeta_operation_payload(cf, table_id, tuple_key, payload)?;
        let cf = canonical_coremeta_cf_name(cf)?;
        let previous_payload_hash =
            previous_payload.map(|payload| core_meta_payload_digest(table_id, &payload));
        let payload_hash = core_meta_payload_digest(table_id, payload);
        let op = OwnedCoreMetaBatchOp::Put {
            cf,
            table_id,
            tuple_key: tuple_key.to_vec(),
            common: None,
            payload: payload.to_vec(),
        };
        let update = CoreTransactionUpdate::CoreMetaPut {
            cf: cf.to_string(),
            table_id,
            tuple_key: tuple_key.to_vec(),
            previous_payload_hash,
            payload: payload.to_vec(),
            payload_hash,
        };
        Ok((op, update))
    }

    pub(super) fn prepare_coremeta_delete_update_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction_id: String,
        rooted_generation: Option<u64>,
    ) -> Result<(OwnedCoreMetaBatchOp, CoreTransactionUpdate)> {
        validate_coremeta_operation_key(cf, table_id, tuple_key)?;
        let cf = canonical_coremeta_cf_name(cf)?;
        let current_payload = self.committed_coremeta_payload_unlocked(cf, table_id, tuple_key)?;
        let previous_payload_hash = current_payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(table_id, payload));
        let deleted_at_unix_nanos = current_unix_nanos_u64()?;
        let delete_common = current_payload
            .as_ref()
            .map(|payload| {
                core_meta_row_common_from_payload(payload).and_then(|common| {
                    let root_generation = if common.root_key_hash.is_empty() {
                        common.root_generation.saturating_add(1)
                    } else {
                        rooted_generation.ok_or_else(|| {
                            anyhow!("CoreMeta rooted delete has no bound publication generation")
                        })?
                    };
                    Ok(core_meta_committed_row_common(
                        common.realm_id,
                        common.root_key_hash,
                        root_generation,
                        transaction_id.clone(),
                        deleted_at_unix_nanos,
                    ))
                })
            })
            .transpose()?;
        let op = OwnedCoreMetaBatchOp::Delete {
            cf,
            table_id,
            tuple_key: tuple_key.to_vec(),
            common: delete_common,
        };
        let update = CoreTransactionUpdate::CoreMetaDelete {
            cf: cf.to_string(),
            table_id,
            tuple_key: tuple_key.to_vec(),
            previous_payload_hash,
        };
        Ok((op, update))
    }

    async fn validate_explicit_transaction_stream_commits_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        let mut updates_by_stream = BTreeMap::<String, BTreeMap<u64, StreamRecord>>::new();
        for update in &transaction.visible_updates {
            let Some(record) = staged_stream_record_from_update(transaction, update)? else {
                continue;
            };
            let previous = updates_by_stream
                .entry(record.stream_id.clone())
                .or_default()
                .insert(record.sequence, record);
            if previous.is_some() {
                bail!("TransactionConflict: transaction lists a stream sequence more than once");
            }
        }

        for (stream_id, updates) in updates_by_stream {
            let (mut previous_sequence, mut previous_event_hash) = self
                .read_stream_head_from_meta(&stream_id)?
                .map(|head| (head.last_sequence, head.last_event_hash))
                .unwrap_or_else(|| (0, ZERO_HASH.to_string()));
            for record in updates.into_values() {
                verify_stream_record_after_head(
                    &stream_id,
                    previous_sequence,
                    &previous_event_hash,
                    &record,
                )
                .map_err(|error| anyhow!("TransactionConflict: {error}"))?;
                previous_sequence = record.sequence;
                previous_event_hash = record.event_hash;
            }
        }
        Ok(())
    }

    async fn validate_implicit_transaction_stream_publications_unlocked(
        &self,
        transaction: &CoreTransaction,
        publication_generations: &BTreeMap<String, u64>,
    ) -> Result<()> {
        let mut unpublished_by_stream = BTreeMap::<String, BTreeMap<u64, StreamRecord>>::new();
        for update in &transaction.visible_updates {
            let Some(record) = publication_stream_record_from_update(transaction, update)? else {
                continue;
            };
            let root_key_hash =
                super::local_roots_layout::stream_coremeta_root_key_hash(&record.stream_id);
            // Staging the coordinator can temporarily hide participant rows
            // that recovery published earlier. Immutable root evidence still
            // proves whether this exact stream effect is already durable.
            if self
                .validate_existing_stream_publication_unlocked(
                    transaction,
                    &record,
                    &root_key_hash,
                    publication_generations.get(&root_key_hash).copied(),
                )
                .await?
            {
                continue;
            }
            if !publication_generations.contains_key(&root_key_hash) {
                bail!(
                    "TransactionScopeMismatch: implicit stream publication does not declare canonical root {root_key_hash}"
                );
            }
            let previous = unpublished_by_stream
                .entry(record.stream_id.clone())
                .or_default()
                .insert(record.sequence, record);
            if previous.is_some() {
                bail!("TransactionConflict: transaction lists a stream sequence more than once");
            }
        }

        for (stream_id, updates) in unpublished_by_stream {
            let first = updates
                .first_key_value()
                .map(|(_, record)| record)
                .ok_or_else(|| anyhow!("CoreStore unpublished stream update set is empty"))?;
            let expected_previous_sequence = first
                .sequence
                .checked_sub(1)
                .ok_or_else(|| anyhow!("CoreStore stream sequence must be nonzero"))?;
            let mut previous_sequence = expected_previous_sequence;
            let mut previous_event_hash = first.previous_event_hash.clone();
            for record in updates.into_values() {
                verify_stream_record_after_head(
                    &stream_id,
                    previous_sequence,
                    &previous_event_hash,
                    &record,
                )
                .map_err(|error| anyhow!("TransactionConflict: {error}"))?;
                previous_sequence = record.sequence;
                previous_event_hash = record.event_hash;
            }
        }
        Ok(())
    }

    async fn validate_existing_stream_publication_unlocked(
        &self,
        transaction: &CoreTransaction,
        expected: &StreamRecord,
        root_key_hash: &str,
        active_publication_generation: Option<u64>,
    ) -> Result<bool> {
        let Some(payload) = self.meta.get(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_key(&expected.stream_id, expected.sequence),
        )?
        else {
            return Ok(false);
        };
        // Use a physical read here because the coordinator header currently
        // being prepared can hide a previously published participant row.
        let common = core_meta_row_common_from_payload(&payload)?;
        if common.visibility_state_enum() != CoreMetaVisibilityState::Committed
            || common.root_key_hash != root_key_hash
            || common.root_generation == 0
        {
            return Ok(false);
        }
        if common.transaction_id != transaction.transaction_id {
            bail!(
                "TransactionScopeMismatch: materialised stream publication belongs to another transaction"
            );
        }
        let published = self.root_generation_is_published(
            root_key_hash,
            common.root_generation,
            &transaction.transaction_id,
        )?;
        if !published && active_publication_generation == Some(common.root_generation) {
            return Ok(false);
        }
        if !published {
            bail!(
                "TransactionConflict: materialised stream publication has no durable generation evidence"
            );
        }
        let actual = self
            .stream_record_from_index_row(decode_stream_record_index_row(&payload)?)
            .await?;
        if actual != *expected {
            bail!(
                "TransactionConflict: stream {} sequence {} changed after the admitted mutation",
                expected.stream_id,
                expected.sequence
            );
        }
        Ok(true)
    }

    async fn validate_explicit_transaction_coremeta_commits_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        let mut coremeta_visible = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        let mut coremeta_root_generation = None;
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                    payload,
                    payload_hash,
                } => {
                    validate_coremeta_operation_payload(cf, *table_id, tuple_key, payload)?;
                    let actual_hash = core_meta_payload_digest(*table_id, payload);
                    if &actual_hash != payload_hash {
                        bail!("TransactionConflict: staged CoreMeta payload hash mismatch");
                    }
                    let common = validate_committed_coremeta_put_common(transaction, payload)?;
                    match coremeta_root_generation {
                        Some(generation) if generation != common.root_generation => {
                            bail!(
                                "TransactionScopeMismatch: explicit transaction touches multiple root generations"
                            );
                        }
                        Some(_) => {}
                        None => coremeta_root_generation = Some(common.root_generation),
                    }
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let current_payload = match coremeta_visible.get(&key) {
                        Some(payload) => payload.clone(),
                        None => {
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                        }
                    };
                    let current_hash = current_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    if &current_hash != previous_payload_hash {
                        bail!("TransactionConflict: staged CoreMeta put preimage changed");
                    }
                    coremeta_visible.insert(key, Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                } => {
                    validate_coremeta_operation_key(cf, *table_id, tuple_key)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let (from_overlay, current_payload) = match coremeta_visible.get(&key) {
                        Some(payload) => (true, payload.clone()),
                        None => (
                            false,
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?,
                        ),
                    };
                    let current_hash = current_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    if &current_hash != previous_payload_hash {
                        bail!("TransactionConflict: staged CoreMeta delete preimage changed");
                    }
                    if let Some(payload) = current_payload.as_ref() {
                        if from_overlay {
                            if let Some(delete_generation) = delete_generation_from_visible_payload(
                                transaction,
                                payload,
                                from_overlay,
                            )? {
                                match coremeta_root_generation {
                                    Some(generation) if generation != delete_generation => {
                                        bail!(
                                            "TransactionScopeMismatch: explicit transaction touches multiple root generations"
                                        );
                                    }
                                    Some(_) => {}
                                    None => coremeta_root_generation = Some(delete_generation),
                                }
                            }
                        } else {
                            validate_delete_visible_payload_scope(transaction, payload)?;
                        }
                    }
                    coremeta_visible.insert(key, None);
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn validate_implicit_transaction_coremeta_publications_unlocked(
        &self,
        transaction: &CoreTransaction,
        publication_generations: &BTreeMap<String, u64>,
    ) -> Result<()> {
        let mut coremeta_visible = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                    payload,
                    payload_hash,
                } => {
                    validate_coremeta_operation_payload(cf, *table_id, tuple_key, payload)?;
                    let actual_hash = core_meta_payload_digest(*table_id, payload);
                    if &actual_hash != payload_hash {
                        bail!("TransactionConflict: staged CoreMeta payload hash mismatch");
                    }
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    if !coremeta_visible.contains_key(&key)
                        && self.exact_coremeta_put_is_published_unlocked(
                            transaction,
                            cf,
                            *table_id,
                            tuple_key,
                            payload,
                        )?
                    {
                        coremeta_visible.insert(key, Some(payload.clone()));
                        continue;
                    }
                    validate_implicit_coremeta_put_common(
                        transaction,
                        payload,
                        publication_generations,
                    )?;
                    let current_payload = match coremeta_visible.get(&key) {
                        Some(payload) => payload.clone(),
                        None => {
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                        }
                    };
                    let current_hash = current_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    if &current_hash != previous_payload_hash {
                        bail!("TransactionConflict: staged CoreMeta put preimage changed");
                    }
                    coremeta_visible.insert(key, Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                } => {
                    validate_coremeta_operation_key(cf, *table_id, tuple_key)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let (from_overlay, current_payload) = match coremeta_visible.get(&key) {
                        Some(payload) => (true, payload.clone()),
                        None => (
                            false,
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?,
                        ),
                    };
                    let current_hash = current_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    if &current_hash != previous_payload_hash {
                        bail!("TransactionConflict: staged CoreMeta delete preimage changed");
                    }
                    if let Some(payload) = current_payload.as_deref() {
                        validate_implicit_coremeta_delete_scope(
                            transaction,
                            payload,
                            from_overlay,
                            publication_generations,
                        )?;
                    }
                    coremeta_visible.insert(key, None);
                }
                CoreTransactionUpdate::StreamAppend { .. } => {}
            }
        }
        Ok(())
    }

    fn exact_coremeta_put_is_published_unlocked(
        &self,
        transaction: &CoreTransaction,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        expected_payload: &[u8],
    ) -> Result<bool> {
        let Some(actual_payload) = self.meta.get_named(cf, table_id, tuple_key)? else {
            return Ok(false);
        };
        if actual_payload != expected_payload {
            return Ok(false);
        }
        let common = core_meta_row_common_from_payload(expected_payload)?;
        if common.visibility_state_enum() != CoreMetaVisibilityState::Committed
            || common.root_key_hash.is_empty()
            || common.root_generation == 0
            || common.transaction_id != transaction.transaction_id
        {
            return Ok(false);
        }
        self.root_generation_is_published(
            &common.root_key_hash,
            common.root_generation,
            &transaction.transaction_id,
        )
    }

    pub(super) async fn commit_explicit_transaction_rows_and_coremeta_updates_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<CoreTransaction> {
        let step_started_at = std::time::Instant::now();
        let committed_root_generation = self
            .infer_explicit_transaction_commit_root_generation_unlocked(transaction)
            .await?;
        crate::emit_test_timing(
            format!(
                "core_store.commit_explicit_transaction infer_root_generation tx={}",
                transaction.transaction_id
            ),
            step_started_at.elapsed(),
        );
        let step_started_at = std::time::Instant::now();
        let mut committed_transaction = transaction.clone();
        committed_transaction.committed_root_generation = Some(committed_root_generation);
        let mut preconditions = self
            .read_transaction_preconditions_unlocked(&transaction.transaction_id)
            .await?;
        self.bind_explicit_transaction_preconditions(&committed_transaction, &mut preconditions)?;
        let mut owned_ops = self
            .complete_transaction_rows_as_coremeta_ops_unlocked(
                &committed_transaction,
                &preconditions,
            )
            .await?;
        let mut staged_streams = BTreeMap::<String, Vec<StreamRecord>>::new();
        for update in &transaction.visible_updates {
            if let Some(record) = staged_stream_record_from_update(transaction, update)? {
                staged_streams
                    .entry(record.stream_id.clone())
                    .or_default()
                    .push(record);
            }
        }
        for (stream_id, records) in &mut staged_streams {
            records.sort_by_key(|record| record.sequence);
            let mut prepared = self
                .prepare_stream_metadata_rows_for_root(
                    stream_id,
                    records,
                    &transaction.root_anchor_key,
                    committed_root_generation,
                    &transaction.transaction_id,
                    WriterFamily::Stream,
                    true,
                )
                .await?;
            owned_ops.append(&mut prepared.owned_ops);
        }
        let mut coremeta_batch_overlay = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        let mut final_coremeta_ops =
            BTreeMap::<(String, u16, Vec<u8>), OwnedCoreMetaBatchOp>::new();
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    validate_coremeta_operation_payload(cf, *table_id, tuple_key, payload)?;
                    let common = validate_committed_coremeta_put_common(transaction, payload)?;
                    if common.root_generation != committed_root_generation {
                        bail!(
                            "TransactionScopeMismatch: explicit transaction CoreMeta put generation does not match committed root generation"
                        );
                    }
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    final_coremeta_ops.insert(
                        key.clone(),
                        OwnedCoreMetaBatchOp::Put {
                            cf,
                            table_id: *table_id,
                            tuple_key: tuple_key.clone(),
                            payload: payload.clone(),
                            common: None,
                        },
                    );
                    coremeta_batch_overlay.insert(key, Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    validate_coremeta_operation_key(cf, *table_id, tuple_key)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let current_payload = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => payload.clone(),
                        None => {
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                        }
                    };
                    final_coremeta_ops.insert(
                        key.clone(),
                        OwnedCoreMetaBatchOp::Delete {
                            cf,
                            table_id: *table_id,
                            tuple_key: tuple_key.clone(),
                            common: Some(delete_common_for_committed_transaction(
                                transaction,
                                current_payload.as_deref(),
                                committed_root_generation,
                                current_unix_nanos_u64()?,
                            )?),
                        },
                    );
                    coremeta_batch_overlay.insert(key, None);
                }
                _ => {}
            }
        }
        owned_ops.extend(final_coremeta_ops.into_values());
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        crate::emit_test_timing(
            format!(
                "core_store.commit_explicit_transaction prepare_coremeta_ops tx={} ops={}",
                transaction.transaction_id,
                owned_ops.len()
            ),
            step_started_at.elapsed(),
        );
        let step_started_at = std::time::Instant::now();
        self.commit_coremeta_root_groups_prelocked(
            &committed_transaction.transaction_id,
            &ops,
            &[CoreMetaRootPublication::with_writer_families(
                committed_transaction.root_anchor_key.clone(),
                committed_transaction.writer_families.clone(),
            )
            .coordinator()],
        )
        .await?;
        crate::emit_test_timing(
            format!(
                "core_store.commit_explicit_transaction commit_coremeta tx={}",
                transaction.transaction_id
            ),
            step_started_at.elapsed(),
        );
        Ok(committed_transaction)
    }

    pub(super) async fn infer_explicit_transaction_commit_root_generation_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<u64> {
        if let Some(generation) = transaction
            .committed_root_generation
            .filter(|generation| *generation > 0)
        {
            return Ok(generation);
        }

        for update in &transaction.visible_updates {
            let CoreTransactionUpdate::CoreMetaPut { payload, .. } = update else {
                continue;
            };
            let common = core_meta_row_common_from_payload(payload)?;
            if common.root_key_hash != transaction.root_key_hash {
                bail!("TransactionScopeMismatch");
            }
            if common.root_generation != 0 {
                bail!(
                    "TransactionConflict: staged CoreMeta payload unexpectedly reserves a root generation"
                );
            }
        }

        let current_root_generation = self
            .read_latest_root_anchor(&transaction.root_anchor_key)
            .await?
            .map(|anchor| anchor.root_generation)
            .unwrap_or(0);
        current_root_generation
            .checked_add(1)
            .filter(|generation| *generation > 0)
            .ok_or_else(|| anyhow!("CoreStore explicit transaction root generation overflow"))
    }

    pub(super) async fn validate_mutation_preconditions_unlocked(
        &self,
        preconditions: &[CoreMutationPrecondition],
        committed_by_principal: &str,
        transaction_id: Option<&str>,
    ) -> Result<()> {
        let transaction = match transaction_id {
            Some(transaction_id) => Some(
                self.read_transaction_unlocked(transaction_id)
                    .await?
                    .ok_or_else(|| anyhow!("TransactionNotFound"))?,
            ),
            None => None,
        };
        self.validate_mutation_preconditions_against_transaction_unlocked(
            preconditions.iter(),
            committed_by_principal,
            transaction.as_ref(),
        )
        .await
    }

    pub(super) async fn validate_staged_transaction_preconditions_unlocked(
        &self,
        preconditions: &[CoreTransactionPreconditionRow],
        committed_by_principal: &str,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        let visible_update_count = u64::try_from(transaction.visible_updates.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
        let mut by_boundary = BTreeMap::<u64, Vec<&CoreMutationPrecondition>>::new();
        for persisted in preconditions {
            if persisted.visible_update_boundary > visible_update_count {
                bail!("CoreStore transaction precondition boundary exceeds staged update count");
            }
            if persisted.revalidate_at_publication {
                by_boundary
                    .entry(persisted.visible_update_boundary)
                    .or_default()
                    .push(&persisted.precondition);
            }
        }

        for (visible_update_boundary, boundary_preconditions) in by_boundary {
            let boundary = usize::try_from(visible_update_boundary).map_err(|_| {
                anyhow!("CoreStore transaction precondition boundary exceeds usize")
            })?;
            let mut transaction_at_boundary = transaction.clone();
            transaction_at_boundary.visible_updates.truncate(boundary);
            self.validate_mutation_preconditions_against_transaction_unlocked(
                boundary_preconditions.into_iter(),
                committed_by_principal,
                Some(&transaction_at_boundary),
            )
            .await?;
        }
        Ok(())
    }

    async fn validate_mutation_preconditions_against_transaction_unlocked<'a>(
        &self,
        preconditions: impl IntoIterator<Item = &'a CoreMutationPrecondition>,
        committed_by_principal: &str,
        transaction: Option<&CoreTransaction>,
    ) -> Result<()> {
        for precondition in preconditions {
            match precondition {
                CoreMutationPrecondition::Fence {
                    fence_name,
                    fence_token,
                } => {
                    self.validate_fence_precondition_unlocked(&CoreFencePrecondition {
                        fence_name: fence_name.clone(),
                        fence_token: *fence_token,
                        authenticated_principal: committed_by_principal.to_string(),
                    })
                    .await?;
                }
                CoreMutationPrecondition::CoreMetaRow {
                    cf,
                    table_id,
                    tuple_key,
                    expected_payload_hash,
                    require_absent,
                    require_present,
                } => {
                    let current = if let Some(transaction) = transaction {
                        self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            transaction,
                        )?
                    } else {
                        self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                    };
                    validate_core_meta_row_precondition(
                        current.as_deref(),
                        cf,
                        *table_id,
                        tuple_key,
                        expected_payload_hash.as_deref(),
                        *require_absent,
                        *require_present,
                    )?;
                }
                CoreMutationPrecondition::CoreMetaLease {
                    cf,
                    table_id,
                    tuple_key,
                    expected_payload_hash,
                    expires_at_unix_nanos,
                } => {
                    if *expires_at_unix_nanos == 0
                        || current_unix_nanos_u64()? >= *expires_at_unix_nanos
                    {
                        return Err(CoreStoreCommitError::CoreMetaRowPreconditionFailed {
                            cf: cf.clone(),
                            table_id: *table_id,
                            tuple_key_hex: hex::encode(tuple_key),
                            reason: "lease expired before commit admission".to_string(),
                        }
                        .into());
                    }
                    let current = if let Some(transaction) = transaction {
                        self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            transaction,
                        )?
                    } else {
                        self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                    };
                    validate_core_meta_row_precondition(
                        current.as_deref(),
                        cf,
                        *table_id,
                        tuple_key,
                        Some(expected_payload_hash),
                        false,
                        true,
                    )?;
                }
                CoreMutationPrecondition::StreamHead {
                    stream_id,
                    expected_last_sequence,
                    expected_last_event_hash,
                } => {
                    let (actual_sequence, actual_hash) =
                        self.stream_head_visible_to_transaction_unlocked(stream_id, transaction)?;
                    if actual_sequence != *expected_last_sequence
                        || actual_hash != *expected_last_event_hash
                    {
                        return Err(CoreStoreCommitError::StreamHeadMismatch {
                            stream_id: stream_id.clone(),
                            expected_last_sequence: *expected_last_sequence,
                            expected_last_event_hash: expected_last_event_hash.clone(),
                            actual_sequence,
                            actual_event_hash: actual_hash,
                        }
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn validate_source_watch_cursor_unlocked(&self, cursor: &str) -> Result<()> {
        let (stream_id, sequence) = parse_stream_cursor(cursor)?;
        let Some(record) = self
            .read_stream(ReadStream {
                stream_id,
                after_sequence: sequence.saturating_sub(1),
                limit: 1,
            })
            .await?
            .into_iter()
            .next()
        else {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        };
        if record.cursor != cursor {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        }
        Ok(())
    }

    pub(super) async fn validate_fence_precondition_unlocked(
        &self,
        precondition: &CoreFencePrecondition,
    ) -> Result<()> {
        validate_logical_id(&precondition.fence_name, "fence name")?;
        validate_logical_id(
            &precondition.authenticated_principal,
            "fence authenticated principal",
        )?;
        let Some(record) = super::local_stream_control::read_core_fence_current_row(
            self,
            &precondition.fence_name,
        )?
        else {
            bail!("CoreStore fence {} is not held", precondition.fence_name);
        };
        if record.owner_principal != precondition.authenticated_principal
            || record.fence_token != precondition.fence_token
            || record.expires_at_ms <= Utc::now().timestamp_millis()
        {
            bail!(
                "CoreStore fence {} precondition failed",
                precondition.fence_name
            );
        }
        Ok(())
    }
}

fn validate_committed_coremeta_put_common(
    transaction: &CoreTransaction,
    payload: &[u8],
) -> Result<CoreMetaRowCommonProto> {
    let common = core_meta_row_common_from_payload(payload)?;
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("TransactionConflict: staged CoreMeta payload must commit as visible");
    }
    if common.root_key_hash != transaction.root_key_hash {
        bail!("TransactionScopeMismatch");
    }
    if common.root_generation == 0 {
        bail!("TransactionConflict: staged CoreMeta payload has no root generation");
    }
    if common.transaction_id != transaction.transaction_id {
        bail!("TransactionScopeMismatch: staged CoreMeta payload transaction id mismatch");
    }
    Ok(common)
}

fn validate_implicit_coremeta_put_common(
    transaction: &CoreTransaction,
    payload: &[u8],
    publication_generations: &BTreeMap<String, u64>,
) -> Result<CoreMetaRowCommonProto> {
    let common = core_meta_row_common_from_payload(payload)?;
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("TransactionConflict: staged CoreMeta payload must commit as visible");
    }
    if common.root_key_hash.is_empty() {
        if common.root_generation != 0 {
            bail!("TransactionConflict: local CoreMeta payload has a root generation");
        }
        return Ok(common);
    }
    let Some(expected_generation) = publication_generations.get(&common.root_key_hash) else {
        bail!(
            "TransactionScopeMismatch: implicit CoreMeta payload references undeclared root {}",
            common.root_key_hash
        );
    };
    if common.root_generation != *expected_generation {
        bail!(
            "TransactionScopeMismatch: implicit CoreMeta payload generation does not match publication"
        );
    }
    if common.transaction_id != transaction.transaction_id {
        bail!("TransactionScopeMismatch: staged CoreMeta payload transaction id mismatch");
    }
    Ok(common)
}

fn validate_implicit_coremeta_delete_scope(
    transaction: &CoreTransaction,
    payload: &[u8],
    from_overlay: bool,
    publication_generations: &BTreeMap<String, u64>,
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    if common.root_key_hash.is_empty() {
        if common.root_generation != 0 {
            bail!("TransactionConflict: local CoreMeta payload has a root generation");
        }
        return Ok(());
    }
    let Some(publication_generation) = publication_generations.get(&common.root_key_hash) else {
        bail!(
            "TransactionScopeMismatch: implicit CoreMeta delete references undeclared root {}",
            common.root_key_hash
        );
    };
    if from_overlay {
        if common.transaction_id != transaction.transaction_id {
            bail!("TransactionScopeMismatch: staged CoreMeta delete overlay has another owner");
        }
        if common.root_generation != *publication_generation {
            bail!(
                "TransactionScopeMismatch: implicit CoreMeta delete generation does not match publication"
            );
        }
        return Ok(());
    }
    // A canonical row records the generation in which that row last changed,
    // not the current head of its root. An exact row precondition protects the
    // preimage while the root-register CAS proves the H -> H+1 publication.
    if common.root_generation == 0 || common.root_generation >= *publication_generation {
        bail!(
            "TransactionScopeMismatch: implicit CoreMeta delete generation does not match publication"
        );
    }
    Ok(())
}

fn delete_generation_from_visible_payload(
    transaction: &CoreTransaction,
    payload: &[u8],
    from_overlay: bool,
) -> Result<Option<u64>> {
    let common = core_meta_row_common_from_payload(payload)?;
    if common.root_key_hash.is_empty() {
        return Ok(None);
    }
    validate_delete_visible_common_scope(transaction, &common)?;
    if from_overlay {
        Ok(Some(common.root_generation))
    } else {
        common
            .root_generation
            .checked_add(1)
            .map(Some)
            .ok_or_else(|| anyhow!("CoreStore explicit transaction root generation overflow"))
    }
}

fn validate_delete_visible_payload_scope(
    transaction: &CoreTransaction,
    payload: &[u8],
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    validate_delete_visible_common_scope(transaction, &common)
}

fn validate_delete_visible_common_scope(
    transaction: &CoreTransaction,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.root_key_hash.is_empty() {
        return Ok(());
    }
    if common.root_key_hash != transaction.root_key_hash {
        bail!("TransactionScopeMismatch");
    }
    if common.root_generation == 0 {
        bail!("TransactionConflict: staged CoreMeta delete has no root generation");
    }
    Ok(())
}

fn delete_common_for_committed_transaction(
    transaction: &CoreTransaction,
    current_payload: Option<&[u8]>,
    committed_root_generation: u64,
    created_at_unix_nanos: u64,
) -> Result<CoreMetaRowCommonProto> {
    if committed_root_generation == 0 {
        bail!("CoreStore explicit transaction delete must use a non-zero root generation");
    }
    let realm_id = match current_payload {
        Some(payload) => {
            let common = core_meta_row_common_from_payload(payload)?;
            if !common.root_key_hash.is_empty() && common.root_key_hash != transaction.root_key_hash
            {
                bail!("TransactionScopeMismatch");
            }
            if !common.root_key_hash.is_empty() {
                common.realm_id
            } else {
                transaction.committed_by_principal.clone()
            }
        }
        None => transaction.committed_by_principal.clone(),
    };
    Ok(core_meta_committed_row_common(
        realm_id,
        transaction.root_key_hash.clone(),
        committed_root_generation,
        transaction.transaction_id.clone(),
        created_at_unix_nanos,
    ))
}
