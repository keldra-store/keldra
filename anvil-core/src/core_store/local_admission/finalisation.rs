use super::*;

const LOCAL_FINALISATION_RETRY_LIMIT: usize = 16;

struct PreparedLocalFinalisation {
    observed_state: AdmissionPointState,
    owned_ops: Vec<OwnedCoreMetaBatchOp>,
}

impl CoreStore {
    pub(in crate::core_store::local) fn read_pending_mutation_finalisation_record(
        &self,
        key: &CorePendingMutationKey,
    ) -> Result<Option<CorePendingMutationFinalisationRecord>> {
        // Finalisation records are shard-local admission state. They must be
        // visible to retry/recovery before any product root is published.
        let row = self
            .meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_finalisation_record_key(key),
            )?
            .map(|bytes| decode_pending_mutation_finalisation_record(&bytes))
            .transpose()?;
        if row.as_ref().is_some_and(|row| {
            row.target.admission_shard().hash != key.admission_shard_hash
                || row.node_id != key.node_id
                || row.mutation_epoch != key.mutation_epoch
                || row.mutation_sequence != key.mutation_sequence
        }) {
            bail!("CoreStore pending mutation finalisation point record has invalid scope");
        }
        Ok(row)
    }

    pub(in crate::core_store::local) async fn mark_pending_mutation_finalised_unlocked(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
    ) -> Result<()> {
        self.mark_pending_mutation_finalised_with_result_unlocked(admission, state, None)
            .await
    }

    pub(in crate::core_store::local) async fn mark_pending_mutation_finalised_with_result_unlocked(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
        result: Option<CorePendingMutationFinalisationResult>,
    ) -> Result<()> {
        self.mark_pending_mutation_finalised_with_result_and_ops_unlocked(
            admission,
            state,
            result,
            Vec::new(),
        )
        .await
    }

    pub(in crate::core_store::local) async fn mark_pending_mutation_finalised_with_result_and_ops_unlocked(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
        result: Option<CorePendingMutationFinalisationResult>,
        preceding_ops: Vec<OwnedCoreMetaBatchOp>,
    ) -> Result<()> {
        let key = CorePendingMutationKey::from(admission);
        let result_hash = finalisation_result_hash(&result)?;
        if self.validate_idempotent_local_finalisation(admission, state, &result, &result_hash)? {
            return Ok(());
        }

        let finalisation = CorePendingMutationFinalisationRecord {
            schema: CORE_PENDING_MUTATION_FINALISATION_SCHEMA.to_string(),
            node_id: admission.node_id.clone(),
            mutation_epoch: admission.mutation_epoch,
            mutation_sequence: admission.sequence,
            mutation_id: admission.mutation_id.clone(),
            operation_family: admission.operation_family.clone(),
            writer_family: admission.writer_family.clone(),
            target: admission.target.clone(),
            boundary_values: admission.boundary_values.clone(),
            landed_bytes: admission.landed_bytes.clone(),
            state: state.to_string(),
            result,
            // The canonical control-stream owner assigns the timestamp. The
            // zero-valued proposal remains stable across routing retries.
            finalised_at_unix_nanos: 0,
        };
        if !preceding_ops.is_empty() {
            let ops = borrow_owned_coremeta_batch_ops(&preceding_ops);
            let has_rooted_rows = self
                .meta
                .encode_batch_ops(&ops)?
                .iter()
                .any(|row| !row.root_key_hash.is_empty());
            if has_rooted_rows {
                let publications = self.finalisation_root_publications(admission, &ops).await?;
                self.commit_or_resume_finalisation_roots(admission, &ops, &publications)
                    .await?;
            } else {
                self.meta.write_local_committed_batch(&ops)?;
            }
        }
        let finalisation = self
            .publish_pending_mutation_finalisation_transaction_record(&finalisation)
            .await?;
        let finalisation_payload = encode_pending_mutation_finalisation_record(&finalisation)?;

        let shard = admission.target.admission_shard();
        for attempt in 0..LOCAL_FINALISATION_RETRY_LIMIT {
            if self.validate_idempotent_local_finalisation(
                admission,
                state,
                &finalisation.result,
                &result_hash,
            )? {
                return Ok(());
            }
            let prepared = self.prepare_local_finalisation(
                admission,
                state,
                &result_hash,
                &finalisation,
                &finalisation_payload,
            )?;
            let borrowed = borrow_owned_coremeta_batch_ops(&prepared.owned_ops);
            let shard_guard = self
                .acquire_named_lock("admission-shard", &shard.hash)
                .await?;
            if self.load_admission_point_state_foreground(&shard.hash)? != prepared.observed_state {
                drop(shard_guard);
                tokio::time::sleep(admission_contention_retry_delay(&shard.hash, attempt)).await;
                continue;
            }
            if self
                .pending_mutation_finalisation_index_point(&key)?
                .is_some()
            {
                drop(shard_guard);
                if self.validate_idempotent_local_finalisation(
                    admission,
                    state,
                    &finalisation.result,
                    &result_hash,
                )? {
                    return Ok(());
                }
                bail!("CoreStore finalisation conflicts with existing shard-local state");
            }
            let result = self.meta.write_local_committed_batch(&borrowed);
            drop(shard_guard);
            result?;
            return Ok(());
        }
        bail!(
            "CoreStore admission shard {} remained contended after {} bounded finalisation attempts",
            shard.key,
            LOCAL_FINALISATION_RETRY_LIMIT
        )
    }

    fn prepare_local_finalisation(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
        result_hash: &str,
        finalisation: &CorePendingMutationFinalisationRecord,
        finalisation_payload: &[u8],
    ) -> Result<PreparedLocalFinalisation> {
        let shard = admission.target.admission_shard();
        let (_, _, stored_bytes) = self.validate_local_finalisation_source(admission)?;
        let observed_state = self.load_admission_point_state_foreground(&shard.hash)?;
        let next_oldest = if observed_state.oldest_pending_sequence == Some(admission.sequence)
            && observed_state.pending_rows > 1
        {
            Some(
                self.first_pending_mutation_after(&shard.hash, Some(admission.sequence))?
                    .ok_or_else(|| {
                        anyhow!("CoreStore admission oldest point has no shard-local successor")
                    })?
                    .record,
            )
        } else {
            None
        };

        let mut landed_updates = BTreeMap::<String, Option<LandedByteHead>>::new();
        let mut removed_landed_bytes = 0_u64;
        for landed in &admission.landed_bytes {
            self.verify_landed_bytes_ref_row(
                &shard.hash,
                &landed.landing_id,
                &admission.mutation_id,
                &landed.sha256,
                landed.length,
                &admission.boundary_values,
            )?;
            let current = match landed_updates.get(&landed.sha256) {
                Some(Some(head)) => head.clone(),
                Some(None) => bail!("CoreStore landed byte point head undercounts refs"),
                None => self
                    .read_landed_byte_head(&shard.hash, &landed.sha256)?
                    .ok_or_else(|| anyhow!("CoreStore landed byte point head is missing"))?,
            };
            let next = current.remove_reference(landed)?;
            if next.is_none() {
                removed_landed_bytes =
                    removed_landed_bytes
                        .checked_add(landed.length)
                        .ok_or_else(|| {
                            anyhow!("CoreStore landed byte finalisation counter overflow")
                        })?;
            }
            landed_updates.insert(landed.sha256.clone(), next);
        }
        let next_state = observed_state.after_finalisation(
            admission,
            stored_bytes,
            removed_landed_bytes,
            next_oldest.as_ref(),
        )?;
        let mutation_head = self
            .read_admission_mutation_head(&shard.hash, &admission.mutation_id)?
            .ok_or_else(|| anyhow!("CoreStore pending mutation point head is missing"))?;
        let finalised_mutation_head = mutation_head.finalised()?;
        let finalised_idempotency_head = admission
            .idempotency_key_hash
            .as_deref()
            .map(|key_hash| {
                self.read_admission_idempotency_head(&shard.hash, key_hash)?
                    .ok_or_else(|| anyhow!("CoreStore admission idempotency head is missing"))?
                    .finalised()
            })
            .transpose()?;
        let index = CorePendingMutationFinalisationIndexRow {
            schema: CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA.to_string(),
            admission_shard_hash: shard.hash.clone(),
            node_id: admission.node_id.clone(),
            mutation_epoch: admission.mutation_epoch,
            mutation_sequence: admission.sequence,
            mutation_id: admission.mutation_id.clone(),
            state: state.to_string(),
            result_hash: result_hash.to_string(),
        };
        let common = core_meta_committed_row_common(
            "system/local-admission",
            shard.hash.clone(),
            admission.sequence,
            admission.mutation_id.clone(),
            finalisation.finalised_at_unix_nanos,
        );
        let key = CorePendingMutationKey::from(admission);
        let mut owned_ops = vec![
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_finalisation_record_key(&key),
                payload: finalisation_payload.to_vec(),
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_finalisation_key(&key),
                payload: encode_pending_mutation_finalisation_index_row(&index)?,
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_point_state_key(&shard.hash),
                payload: encode_admission_point_state(
                    &shard.hash,
                    &next_state,
                    admission.sequence,
                    &admission.mutation_id,
                )?,
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_mutation_head_key(&shard.hash, &admission.mutation_id),
                payload: encode_admission_mutation_head(
                    &shard.hash,
                    &finalised_mutation_head,
                    admission.sequence,
                    &admission.mutation_id,
                )?,
                common: None,
            },
            OwnedCoreMetaBatchOp::Delete {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_PENDING_MUTATION_ROW,
                tuple_key: admission_record_key(&shard.hash, admission.sequence),
                common: Some(common.clone()),
            },
            OwnedCoreMetaBatchOp::Delete {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
                tuple_key: admission_evidence_key(&shard.hash, admission.sequence),
                common: Some(common.clone()),
            },
        ];
        if let Some(head) = finalised_idempotency_head.as_ref() {
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_idempotency_head_key(&shard.hash, &head.idempotency_key_hash),
                payload: encode_admission_idempotency_head(
                    &shard.hash,
                    head,
                    admission.sequence,
                    &admission.mutation_id,
                )?,
                common: None,
            });
        }
        for landed in &admission.landed_bytes {
            owned_ops.push(OwnedCoreMetaBatchOp::Delete {
                cf: CF_MATERIALISATION,
                table_id: TABLE_LANDED_BYTE_REF_ROW,
                tuple_key: landed_byte_ref_key(&shard.hash, &landed.landing_id),
                common: Some(common.clone()),
            });
        }
        for (sha256, head) in landed_updates {
            match head {
                Some(head) => owned_ops.push(OwnedCoreMetaBatchOp::Put {
                    cf: CF_MATERIALISATION,
                    table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                    tuple_key: landed_byte_head_key(&shard.hash, &sha256),
                    payload: encode_landed_byte_head(
                        &shard.hash,
                        &head,
                        admission.sequence,
                        &admission.mutation_id,
                    )?,
                    common: None,
                }),
                None => owned_ops.push(OwnedCoreMetaBatchOp::Delete {
                    cf: CF_MATERIALISATION,
                    table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                    tuple_key: landed_byte_head_key(&shard.hash, &sha256),
                    common: Some(common.clone()),
                }),
            }
        }
        Ok(PreparedLocalFinalisation {
            observed_state,
            owned_ops,
        })
    }

    fn validate_local_finalisation_source(
        &self,
        admission: &CorePendingMutationRecord,
    ) -> Result<(CorePendingMutationRecord, Vec<u8>, u64)> {
        let shard = admission.target.admission_shard();
        let (stored, payload, stored_bytes) = self
            .read_pending_mutation_at(&shard.hash, admission.sequence)?
            .ok_or_else(|| anyhow!("CoreStore pending mutation row is missing at finalisation"))?;
        let stored_hash = pending_mutation_request_hash(&stored, &payload)?;
        let head = self
            .read_admission_mutation_head(&shard.hash, &admission.mutation_id)?
            .ok_or_else(|| anyhow!("CoreStore pending mutation point head is missing"))?;
        if stored.mutation_id != admission.mutation_id
            || stored.target.admission_shard() != shard
            || pending_mutation_request_hash(admission, &payload)? != stored_hash
            || !head.is_active()
            || head.mutation_sequence != admission.sequence
            || head.request_hash != stored_hash
        {
            bail!("CoreStore pending mutation finalisation source/point mismatch");
        }
        if let Some(idempotency_key_hash) = admission.idempotency_key_hash.as_deref() {
            let idempotency = self
                .read_admission_idempotency_head(&shard.hash, idempotency_key_hash)?
                .ok_or_else(|| anyhow!("CoreStore admission idempotency head is missing"))?;
            if !idempotency.is_active()
                || idempotency.mutation_id != admission.mutation_id
                || idempotency.mutation_sequence != admission.sequence
                || idempotency.request_hash != admission_request_hash(admission)?
            {
                bail!("CoreStore admission idempotency head is inconsistent at finalisation");
            }
        }
        Ok((stored, payload, stored_bytes))
    }

    fn validate_idempotent_local_finalisation(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
        result: &Option<CorePendingMutationFinalisationResult>,
        result_hash: &str,
    ) -> Result<bool> {
        let key = CorePendingMutationKey::from(admission);
        let Some(existing) = self.pending_mutation_finalisation_index_point(&key)? else {
            return Ok(false);
        };
        if existing.mutation_id != admission.mutation_id
            || existing.state != state
            || existing.result_hash != result_hash
        {
            bail!("CoreStore pending mutation finalisation conflicts with existing point state");
        }
        let shard = admission.target.admission_shard();
        let mutation_head = self
            .read_admission_mutation_head(&shard.hash, &admission.mutation_id)?
            .ok_or_else(|| anyhow!("CoreStore finalised mutation is missing mutation head"))?;
        if !mutation_head.is_finalised()
            || mutation_head.mutation_sequence != admission.sequence
            || self
                .read_pending_mutation_at(&shard.hash, admission.sequence)?
                .is_some()
        {
            bail!("CoreStore idempotent finalisation point state is inconsistent");
        }
        let record = self
            .read_pending_mutation_finalisation_record(&key)?
            .ok_or_else(|| anyhow!("CoreStore finalisation index has no point record"))?;
        if record.result.as_ref().map(|_| ()) != result.as_ref().map(|_| ())
            || finalisation_result_hash(&record.result)? != result_hash
        {
            bail!("CoreStore finalisation record/index result mismatch");
        }
        Ok(true)
    }

    async fn finalisation_root_publications(
        &self,
        admission: &CorePendingMutationRecord,
        ops: &[CoreMetaBatchOp<'_>],
    ) -> Result<Vec<CoreMetaRootPublication>> {
        let CorePendingMutationTarget::MutationBatch {
            transaction_id,
            scope_partition,
            operation_count,
        } = &admission.target
        else {
            bail!("CoreStore preceding finalisation rows require a mutation-batch root scope");
        };
        let (stored, mut payload, _) = self.validate_local_finalisation_source(admission)?;
        if payload.is_empty() {
            let [landed] = stored.landed_bytes.as_slice() else {
                bail!("CoreStore landed mutation plan must have exactly one payload reference");
            };
            payload = self.read_landed_bytes(landed).await?;
        }
        let batch = decode_core_mutation_batch(&payload)?;
        if batch.transaction_id.as_str() != admission.mutation_id.as_str()
            || batch.transaction_id.as_str() != transaction_id.as_str()
            || batch.scope_partition.as_str() != scope_partition.as_str()
            || batch.operations.len() as u64 != *operation_count
        {
            bail!("CoreStore finalisation mutation plan does not match its admission target");
        }
        let publications =
            self.select_declared_publications_for_ops(ops, &batch.root_publications)?;
        if !publications.iter().any(|publication| {
            publication.root_anchor_key.as_str() == scope_partition.as_str()
                && publication.transaction_coordinator
        }) {
            bail!("CoreStore finalisation mutation plan is missing its coordinator root");
        }
        Ok(publications)
    }

    async fn commit_or_resume_finalisation_roots(
        &self,
        admission: &CorePendingMutationRecord,
        ops: &[CoreMetaBatchOp<'_>],
        publications: &[CoreMetaRootPublication],
    ) -> Result<()> {
        if let Some(intent) = self.read_root_publication_intent(&admission.mutation_id)? {
            self.validate_finalisation_publication_retry(admission, &intent, publications)?;
            self.resume_root_publication_intent(intent).await?;
            return Ok(());
        }

        self.commit_coremeta_root_groups_prelocked(&admission.mutation_id, ops, publications)
            .await?;
        Ok(())
    }

    fn validate_finalisation_publication_retry(
        &self,
        admission: &CorePendingMutationRecord,
        intent: &RootPublicationIntent,
        publications: &[CoreMetaRootPublication],
    ) -> Result<()> {
        if intent.transaction_id != admission.mutation_id
            || intent.publisher_node_id != self.node_identity.node_id
        {
            bail!("CoreStore finalisation publication intent has the wrong owner or transaction");
        }

        let expected = publications
            .iter()
            .map(|publication| (publication.root_key_hash(), publication))
            .collect::<BTreeMap<_, _>>();
        let persisted = intent
            .roots
            .iter()
            .map(|root| {
                (
                    root.publication.descriptor.root_key_hash(),
                    &root.publication.descriptor,
                )
            })
            .collect::<BTreeMap<_, _>>();
        if expected != persisted {
            bail!("CoreStore finalisation retry changed its canonical publication roots");
        }

        Ok(())
    }
}
