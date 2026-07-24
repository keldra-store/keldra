use super::*;

impl CoreStore {
    pub(super) async fn publication_generation_bindings_unlocked(
        &self,
        transaction_id: &str,
        publications: &[CoreMutationRootPublication],
    ) -> Result<BTreeMap<String, u64>> {
        let mut bindings = BTreeMap::new();
        for publication in publications {
            let root_key_hash = root_key_hash(&publication.root_anchor_key);
            let generation = self
                .implicit_root_generation_unlocked(
                    transaction_id,
                    &publication.root_anchor_key,
                    None,
                )
                .await?;
            if bindings.insert(root_key_hash.clone(), generation).is_some() {
                bail!("CoreMeta mutation declares root {root_key_hash} more than once");
            }
        }
        Ok(bindings)
    }

    pub(super) async fn bind_mutation_batch_root_generations_unlocked(
        &self,
        batch: &mut CoreMutationBatch,
    ) -> Result<BTreeMap<String, u64>> {
        let bindings = self
            .publication_generation_bindings_unlocked(
                &batch.transaction_id,
                &batch.root_publications,
            )
            .await?;
        self.bind_mutation_batch_to_generations(batch, &bindings)?;
        Ok(bindings)
    }

    pub(super) fn bind_mutation_batch_to_generations(
        &self,
        batch: &mut CoreMutationBatch,
        bindings: &BTreeMap<String, u64>,
    ) -> Result<()> {
        for operation in &mut batch.operations {
            let CoreMutationOperation::CoreMetaPut { payload, .. } = operation else {
                continue;
            };
            let mut common = core_meta_row_common_from_payload(payload)?;
            if common.root_key_hash.is_empty() {
                continue;
            }
            common.root_generation = *bindings.get(&common.root_key_hash).ok_or_else(|| {
                anyhow!(
                    "CoreMeta mutation payload references undeclared root {}",
                    common.root_key_hash
                )
            })?;
            common.transaction_id = batch.transaction_id.clone();
            *payload = replace_core_meta_row_common(payload, &common)?;
        }
        Ok(())
    }

    pub(super) fn bind_encoded_rows_to_generations(
        &self,
        rows: &mut [CoreMetaEncodedOwnedRow],
        transaction_id: &str,
        bindings: &BTreeMap<String, u64>,
    ) -> Result<()> {
        for row in rows {
            if row.root_key_hash.is_empty() {
                continue;
            }
            let generation = *bindings.get(&row.root_key_hash).ok_or_else(|| {
                anyhow!(
                    "CoreMeta encoded row references undeclared root {}",
                    row.root_key_hash
                )
            })?;
            self.meta
                .rebind_encoded_row_publication(row, generation, transaction_id)?;
        }
        Ok(())
    }

    pub(super) fn bind_explicit_transaction_to_generation(
        &self,
        transaction: &mut CoreTransaction,
        root_generation: u64,
    ) -> Result<()> {
        if root_generation == 0 {
            bail!("CoreStore explicit transaction publication generation must be non-zero");
        }
        let mut staged_payload_hashes =
            BTreeMap::<(String, u16, Vec<u8>), Option<(String, String)>>::new();
        for update in &mut transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                    payload,
                    payload_hash,
                } => {
                    let key = (
                        canonical_coremeta_cf_name(cf)?.to_string(),
                        *table_id,
                        tuple_key.clone(),
                    );
                    rebind_staged_preimage_hash(
                        previous_payload_hash,
                        staged_payload_hashes.get(&key),
                    );
                    let unbound_payload_hash = payload_hash.clone();
                    let mut common = core_meta_row_common_from_payload(payload)?;
                    if common.root_key_hash != transaction.root_key_hash {
                        bail!("TransactionScopeMismatch");
                    }
                    common.root_generation = root_generation;
                    common.transaction_id = transaction.transaction_id.clone();
                    *payload = replace_core_meta_row_common(payload, &common)?;
                    *payload_hash = core_meta_payload_digest(*table_id, payload);
                    staged_payload_hashes
                        .insert(key, Some((unbound_payload_hash, payload_hash.clone())));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                } => {
                    let key = (
                        canonical_coremeta_cf_name(cf)?.to_string(),
                        *table_id,
                        tuple_key.clone(),
                    );
                    rebind_staged_preimage_hash(
                        previous_payload_hash,
                        staged_payload_hashes.get(&key),
                    );
                    staged_payload_hashes.insert(key, None);
                }
                CoreTransactionUpdate::StreamAppend { .. } => {}
            }
        }
        Ok(())
    }

    pub(super) fn bind_explicit_transaction_preconditions(
        &self,
        transaction: &CoreTransaction,
        preconditions: &mut [CoreTransactionPreconditionRow],
    ) -> Result<()> {
        for persisted in preconditions {
            let boundary = usize::try_from(persisted.visible_update_boundary).map_err(|_| {
                anyhow!("CoreStore transaction precondition boundary exceeds usize")
            })?;
            if boundary > transaction.visible_updates.len() {
                bail!("CoreStore transaction precondition boundary exceeds staged update count");
            }
            let CoreMutationPrecondition::CoreMetaRow {
                cf,
                table_id,
                tuple_key,
                expected_payload_hash,
                ..
            } = &mut persisted.precondition
            else {
                continue;
            };
            let canonical_cf = canonical_coremeta_cf_name(cf)?;
            let staged_value = transaction.visible_updates[..boundary]
                .iter()
                .rev()
                .find_map(|update| match update {
                    CoreTransactionUpdate::CoreMetaPut {
                        cf: update_cf,
                        table_id: update_table_id,
                        tuple_key: update_tuple_key,
                        payload_hash,
                        ..
                    } if canonical_coremeta_cf_name(update_cf).ok() == Some(canonical_cf)
                        && update_table_id == table_id
                        && update_tuple_key == tuple_key =>
                    {
                        Some(Some(payload_hash.clone()))
                    }
                    CoreTransactionUpdate::CoreMetaDelete {
                        cf: update_cf,
                        table_id: update_table_id,
                        tuple_key: update_tuple_key,
                        ..
                    } if canonical_coremeta_cf_name(update_cf).ok() == Some(canonical_cf)
                        && update_table_id == table_id
                        && update_tuple_key == tuple_key =>
                    {
                        Some(None)
                    }
                    _ => None,
                });
            if let Some(staged_value) = staged_value {
                *expected_payload_hash = staged_value;
            }
        }
        Ok(())
    }
}

fn rebind_staged_preimage_hash(
    previous_payload_hash: &mut Option<String>,
    staged_hashes: Option<&Option<(String, String)>>,
) {
    let Some(Some((unbound_hash, bound_hash))) = staged_hashes else {
        return;
    };
    if previous_payload_hash.as_ref() == Some(unbound_hash) {
        *previous_payload_hash = Some(bound_hash.clone());
    }
}
