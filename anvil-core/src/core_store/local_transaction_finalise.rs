use super::*;

impl CoreStore {
    pub async fn verify_explicit_transaction_finalised(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<u64> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        if transaction.state != CoreTransactionState::Committed {
            bail!("TransactionNotCommitted");
        }
        validate_transaction_root_scope(&transaction)?;
        let mut expected_coremeta = BTreeMap::<(String, u16, Vec<u8>), Option<String>>::new();
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::StreamAppend {
                    stream_id,
                    visible_sequence,
                    prepared_record_hash,
                    ..
                } => {
                    let Some(record) = self
                        .read_stream_record_from_meta(stream_id, *visible_sequence)
                        .await?
                    else {
                        bail!("TransactionFinalisationMissingStreamRecord");
                    };
                    if record.event_hash != *prepared_record_hash
                        || record.transaction_id.as_deref() != Some(transaction_id)
                    {
                        bail!("TransactionFinalisationStreamRecordMismatch");
                    }
                    if !self
                        .transaction_makes_stream_record_visible(&record, transaction_id)
                        .await?
                    {
                        bail!("TransactionFinalisationStreamRecordNotVisible");
                    }
                }
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload_hash,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    expected_coremeta.insert(
                        (cf.to_string(), *table_id, tuple_key.clone()),
                        Some(payload_hash.clone()),
                    );
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    expected_coremeta.insert((cf.to_string(), *table_id, tuple_key.clone()), None);
                }
            }
        }
        // Finalisation verifies the exact physical write outcome. A
        // publication-aware read could hide a malformed put or delete here.
        for ((cf, table_id, tuple_key), expected_hash) in expected_coremeta {
            match expected_hash {
                Some(expected_hash) => {
                    let current = self
                        .meta
                        .get_named(&cf, table_id, &tuple_key)?
                        .ok_or_else(|| anyhow!("TransactionFinalisationMissingCoreMetaRow"))?;
                    let current_hash = core_meta_payload_digest(table_id, &current);
                    if current_hash != expected_hash {
                        bail!("TransactionFinalisationCoreMetaPayloadMismatch");
                    }
                }
                None => {
                    if self.meta.get_named(&cf, table_id, &tuple_key)?.is_some() {
                        bail!("TransactionFinalisationCoreMetaDeleteStillVisible");
                    }
                }
            }
        }
        Ok(transaction.committed_root_generation.unwrap_or(0))
    }
}
