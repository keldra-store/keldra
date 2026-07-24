use super::*;

const ADMISSION_PUBLICATION_RETRY_LIMIT: usize = 16;

struct PreparedAdmissionPublication {
    observed_state: AdmissionPointState,
    record: CorePendingMutationRecord,
    owned_ops: Vec<OwnedCoreMetaBatchOp>,
    pending_bytes: u64,
    newly_landed_bytes: u64,
}

struct AdmissionRequestHashes {
    mutation: String,
    idempotency: String,
}

impl AdmissionRequestHashes {
    fn prepare(record: &CorePendingMutationRecord, payload: &[u8]) -> Result<Self> {
        Ok(Self {
            mutation: pending_mutation_request_hash(record, payload)?,
            idempotency: admission_request_hash(record)?,
        })
    }
}

impl CoreStore {
    pub(super) async fn append_core_pending_mutation_record(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: CorePendingMutationTarget,
        mutation_id: String,
        idempotency_key: Option<String>,
        landed_bytes: Vec<CorePendingLandedByte>,
        payload: &[u8],
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CoreAdmissionOutcome> {
        if payload.len() > CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
            bail!(
                "CoreStore pending mutation payload exceeds {} bytes",
                CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES
            );
        }
        let idempotency_key_hash =
            idempotency_key.map(|value| format!("sha256:{}", sha256_hex(value.as_bytes())));
        let shard = target.admission_shard();
        let created_at_unix_nanos = unix_timestamp_nanos();
        let base_record = CorePendingMutationRecord {
            schema: CORE_PENDING_MUTATION_RECORD_SCHEMA.to_string(),
            node_id: self.node_identity.node_id.clone(),
            mutation_epoch: self.admission_mutation_epoch,
            sequence: 0,
            mutation_id,
            idempotency_key_hash,
            anvil_storage_tenant_id: "local".to_string(),
            authz_scope: CorePendingAuthzScope {
                realm_id: "system".to_string(),
                revision: None,
            },
            operation_family: operation_family.to_string(),
            writer_family: writer_family.to_string(),
            target,
            precondition_fingerprints: Vec::new(),
            boundary_values,
            landed_bytes,
            created_at_unix_nanos,
        };

        let request_hashes = AdmissionRequestHashes::prepare(&base_record, payload)?;

        for attempt in 0..ADMISSION_PUBLICATION_RETRY_LIMIT {
            if let Some(existing) =
                self.resolve_admission_retry(&shard, &base_record, &request_hashes)?
            {
                return Ok(existing);
            }
            let prepared = self
                .prepare_admission_publication(&shard, &base_record, payload, &request_hashes)
                .await?;
            let borrowed = borrow_owned_coremeta_batch_ops(&prepared.owned_ops);
            let guard = self
                .acquire_named_lock("admission-shard", &shard.hash)
                .await?;
            let current_state = self.load_admission_point_state_foreground(&shard.hash)?;
            if current_state != prepared.observed_state {
                drop(guard);
                tokio::time::sleep(admission_contention_retry_delay(&shard.hash, attempt)).await;
                continue;
            }
            validate_admission_hard_capacity(
                &current_state,
                prepared.pending_bytes,
                prepared.newly_landed_bytes,
                CoreAdmissionCapacityLimits::production(),
            )?;
            let write_started_at = Instant::now();
            let result = self.meta.write_local_committed_batch(&borrowed);
            drop(guard);
            record_corestore_trace_event(
                "admission.rocksdb_write_batch",
                if result.is_ok() { "ok" } else { "error" },
            );
            crate::perf::record_duration(
                "anvil_rocksdb_write_batch_duration_ms",
                &[
                    ("node_id", self.node_identity.node_id.as_str()),
                    ("column_family_group", "local_admission"),
                    ("fsync_mode", "rocksdb_wal"),
                    ("status", if result.is_ok() { "ok" } else { "error" }),
                ],
                write_started_at.elapsed(),
            );
            result?;
            return Ok(CoreAdmissionOutcome::Pending(prepared.record));
        }
        bail!(
            "CoreStore admission shard {} remained contended after {} bounded publication attempts",
            shard.key,
            ADMISSION_PUBLICATION_RETRY_LIMIT
        )
    }

    async fn prepare_admission_publication(
        &self,
        shard: &CoreAdmissionShard,
        base_record: &CorePendingMutationRecord,
        payload: &[u8],
        request_hashes: &AdmissionRequestHashes,
    ) -> Result<PreparedAdmissionPublication> {
        let observed_state = self.load_admission_point_state_foreground(&shard.hash)?;
        let sequence = observed_state
            .last_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore pending mutation sequence overflow"))?;
        let mut record = base_record.clone();
        record.sequence = sequence;

        let payload_bytes = encode_stored_pending_mutation_row(&record, payload)?;
        let (landed_heads, newly_landed_bytes) =
            self.prepare_landed_head_updates(shard, &record)?;
        self.enforce_admission_capacity_for_state(
            &observed_state,
            payload_bytes.len() as u64,
            newly_landed_bytes,
            CoreAdmissionCapacityLimits::production(),
        )
        .await?;
        let next_state = observed_state.after_admission(
            &record,
            payload_bytes.len() as u64,
            newly_landed_bytes,
        )?;
        let mutation_head = AdmissionMutationHead::active(&record, request_hashes.mutation.clone());
        let idempotency_head = record.idempotency_key_hash.as_ref().map(|key_hash| {
            AdmissionIdempotencyHead::active(
                &record,
                key_hash.clone(),
                request_hashes.idempotency.clone(),
            )
        });
        let mut owned_ops = vec![
            OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_PENDING_MUTATION_ROW,
                tuple_key: admission_record_key(&shard.hash, sequence),
                payload: payload_bytes.clone(),
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_sequence_key(&shard.hash),
                payload: encode_admission_sequence_cursor_row(&shard.hash, sequence)?,
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_point_state_key(&shard.hash),
                payload: encode_admission_point_state(
                    &shard.hash,
                    &next_state,
                    sequence,
                    &record.mutation_id,
                )?,
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_mutation_head_key(&shard.hash, &record.mutation_id),
                payload: encode_admission_mutation_head(
                    &shard.hash,
                    &mutation_head,
                    sequence,
                    &record.mutation_id,
                )?,
                common: None,
            },
        ];
        if let Some(head) = idempotency_head.as_ref() {
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_idempotency_head_key(&shard.hash, &head.idempotency_key_hash),
                payload: encode_admission_idempotency_head(
                    &shard.hash,
                    head,
                    sequence,
                    &record.mutation_id,
                )?,
                common: None,
            });
        }
        for landed in &record.landed_bytes {
            let stored = CoreStoredLandedByteRef {
                schema: CORE_LANDED_BYTE_REF_SCHEMA.to_string(),
                admission_shard_hash: shard.hash.clone(),
                admission_sequence: sequence,
                landed: landed.clone(),
                mutation_id: record.mutation_id.clone(),
                boundary_values: record.boundary_values.clone(),
                created_at_unix_nanos: record.created_at_unix_nanos,
            };
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_LANDED_BYTE_REF_ROW,
                tuple_key: landed_byte_ref_key(&shard.hash, &landed.landing_id),
                payload: encode_landed_byte_ref_row(&stored)?,
                common: None,
            });
        }
        for head in landed_heads.values() {
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: landed_byte_head_key(&shard.hash, &head.sha256),
                payload: encode_landed_byte_head(&shard.hash, head, sequence, &record.mutation_id)?,
                common: None,
            });
        }

        let hash_input = encode_pending_mutation_hash_input(&record, payload)?;
        let admitted_payload_set_hash = admission_payload_set_hash(&owned_ops);
        let evidence_bytes =
            self.local_admission_evidence_bytes(&record, &hash_input, admitted_payload_set_hash)?;
        owned_ops.push(OwnedCoreMetaBatchOp::Put {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
            tuple_key: admission_evidence_key(&shard.hash, sequence),
            payload: evidence_bytes,
            common: None,
        });

        Ok(PreparedAdmissionPublication {
            observed_state,
            record,
            owned_ops,
            pending_bytes: payload_bytes.len() as u64,
            newly_landed_bytes,
        })
    }

    fn prepare_landed_head_updates(
        &self,
        shard: &CoreAdmissionShard,
        record: &CorePendingMutationRecord,
    ) -> Result<(BTreeMap<String, LandedByteHead>, u64)> {
        let mut landed_heads = BTreeMap::<String, LandedByteHead>::new();
        let mut newly_landed_bytes = 0_u64;
        for landed in &record.landed_bytes {
            // This collision check targets node-local admission staging before
            // any corresponding root publication exists.
            if self
                .meta
                .get(
                    CF_MATERIALISATION,
                    TABLE_LANDED_BYTE_REF_ROW,
                    &landed_byte_ref_key(&shard.hash, &landed.landing_id),
                )?
                .is_some()
            {
                bail!("CoreStore landed byte reference already exists without a mutation head");
            }
            let next = match landed_heads.get(&landed.sha256) {
                Some(head) => head.add_reference(landed)?,
                None => match self.read_landed_byte_head(&shard.hash, &landed.sha256)? {
                    Some(head) => head.add_reference(landed)?,
                    None => {
                        newly_landed_bytes = newly_landed_bytes
                            .checked_add(landed.length)
                            .ok_or_else(|| {
                                anyhow!("CoreStore landed byte admission counter overflow")
                            })?;
                        LandedByteHead::from_landed(landed)
                    }
                },
            };
            landed_heads.insert(landed.sha256.clone(), next);
        }
        Ok((landed_heads, newly_landed_bytes))
    }

    fn resolve_admission_retry(
        &self,
        shard: &CoreAdmissionShard,
        requested: &CorePendingMutationRecord,
        request_hashes: &AdmissionRequestHashes,
    ) -> Result<Option<CoreAdmissionOutcome>> {
        if let Some(head) =
            self.read_admission_mutation_head(&shard.hash, &requested.mutation_id)?
        {
            if head.request_hash != request_hashes.mutation {
                bail!("CoreStore pending mutation retry conflicts with the mutation point head");
            }
            return self.resolve_existing_admission_head(
                shard,
                &head.mutation_id,
                head.mutation_sequence,
                head.is_active(),
                Some(request_hashes.mutation.clone()),
                None,
            );
        }
        let Some(idempotency_key_hash) = requested.idempotency_key_hash.as_deref() else {
            return Ok(None);
        };
        let Some(head) = self.read_admission_idempotency_head(&shard.hash, idempotency_key_hash)?
        else {
            return Ok(None);
        };
        if head.request_hash != request_hashes.idempotency {
            bail!("CoreStore admission idempotency key conflicts with another request");
        }
        self.resolve_existing_admission_head(
            shard,
            &head.mutation_id,
            head.mutation_sequence,
            head.is_active(),
            None,
            Some(head.request_hash),
        )
    }

    fn resolve_existing_admission_head(
        &self,
        shard: &CoreAdmissionShard,
        mutation_id: &str,
        sequence: u64,
        is_active: bool,
        expected_mutation_hash: Option<String>,
        expected_admission_hash: Option<String>,
    ) -> Result<Option<CoreAdmissionOutcome>> {
        if !is_active {
            let key = CorePendingMutationKey {
                admission_shard_hash: shard.hash.clone(),
                node_id: self.node_identity.node_id.clone(),
                mutation_epoch: self.admission_mutation_epoch,
                mutation_sequence: sequence,
            };
            let index = self
                .pending_mutation_finalisation_index_point(&key)?
                .ok_or_else(|| anyhow!("CoreStore finalised admission head has no index row"))?;
            let finalisation = self
                .read_pending_mutation_finalisation_record(&key)?
                .ok_or_else(|| anyhow!("CoreStore finalised admission head has no point record"))?;
            if index.mutation_id != mutation_id
                || finalisation.mutation_id != mutation_id
                || index.state != finalisation.state
                || index.result_hash != finalisation_result_hash(&finalisation.result)?
            {
                bail!("CoreStore finalised admission point state is inconsistent");
            }
            return Ok(Some(CoreAdmissionOutcome::Finalised(finalisation)));
        }
        let (existing, existing_payload, _) = self
            .read_pending_mutation_at(&shard.hash, sequence)?
            .ok_or_else(|| anyhow!("CoreStore active admission head has no pending row"))?;
        if existing.mutation_id != mutation_id {
            bail!("CoreStore admission point head mutation id is inconsistent");
        }
        if let Some(expected_mutation_hash) = expected_mutation_hash
            && pending_mutation_request_hash(&existing, &existing_payload)?
                != expected_mutation_hash
        {
            bail!("CoreStore pending mutation retry row is inconsistent");
        }
        if let Some(expected_admission_hash) = expected_admission_hash
            && admission_request_hash(&existing)? != expected_admission_hash
        {
            bail!("CoreStore admission idempotency retry row is inconsistent");
        }
        let hash_input = encode_pending_mutation_hash_input(&existing, &existing_payload)?;
        self.verify_local_admission_evidence(&existing, &hash_input)?;
        Ok(Some(CoreAdmissionOutcome::Pending(existing)))
    }

    pub(in crate::core_store::local) fn local_admission_evidence_bytes(
        &self,
        record: &CorePendingMutationRecord,
        pending_mutation_hash_input: &[u8],
        admitted_payload_set_hash: String,
    ) -> Result<Vec<u8>> {
        let mut evidence = build_local_admission_evidence(
            record,
            pending_mutation_hash_input,
            admitted_payload_set_hash,
            unix_timestamp_nanos(),
            record.sequence,
        )?;
        evidence.local_receipt.source_signature =
            self.sign_core_receipt(&evidence.local_receipt.signed_payload_hash)?;
        evidence.source_signature = self.sign_core_receipt(&evidence.signed_payload_hash)?;
        validate_local_admission_evidence(&evidence)?;
        self.verify_core_admission_signature(
            &record.node_id,
            &evidence.local_receipt.signed_payload_hash,
            &evidence.local_receipt.source_signature,
        )?;
        self.verify_core_admission_signature(
            &record.node_id,
            &evidence.signed_payload_hash,
            &evidence.source_signature,
        )?;
        encode_local_admission_evidence(&evidence)
    }

    pub(in crate::core_store::local) fn verify_local_admission_evidence(
        &self,
        record: &CorePendingMutationRecord,
        pending_mutation_hash_input: &[u8],
    ) -> Result<CoreLocalAdmissionEvidence> {
        let shard = record.target.admission_shard();
        // Local admission evidence is staging proof consumed before rooted
        // publication, so visibility-aware application reads cannot expose it.
        let bytes = self
            .meta
            .get(
                CF_TRANSACTIONS,
                TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
                &admission_evidence_key(&shard.hash, record.sequence),
            )?
            .ok_or_else(|| {
                anyhow!(
                    "read CoreStore local admission evidence for shard {} sequence {}",
                    shard.key,
                    record.sequence
                )
            })?;
        self.verify_local_admission_evidence_payload(record, pending_mutation_hash_input, &bytes)
    }

    pub(in crate::core_store::local) fn verify_local_admission_evidence_payload(
        &self,
        record: &CorePendingMutationRecord,
        pending_mutation_hash_input: &[u8],
        bytes: &[u8],
    ) -> Result<CoreLocalAdmissionEvidence> {
        let evidence = decode_local_admission_evidence(bytes)?;
        validate_local_admission_evidence(&evidence)?;
        let expected_pending_mutation_hash = domain_hash_bytes(
            "anvil.admission.pending_mutation_hash_input.v1",
            pending_mutation_hash_input,
        );
        if evidence.local_receipt.pending_mutation_hash != expected_pending_mutation_hash {
            bail!("CoreStore local admission evidence pending mutation hash mismatch");
        }
        let expected_attempt = admission_attempt_id(record)?;
        if evidence.attempt_id != expected_attempt
            || evidence.local_receipt.attempt_id != expected_attempt
        {
            bail!("CoreStore local admission evidence attempt id mismatch");
        }
        self.verify_core_admission_signature(
            &record.node_id,
            &evidence.local_receipt.signed_payload_hash,
            &evidence.local_receipt.source_signature,
        )?;
        self.verify_core_admission_signature(
            &record.node_id,
            &evidence.signed_payload_hash,
            &evidence.source_signature,
        )?;
        Ok(evidence)
    }
}

fn admission_payload_set_hash(ops: &[OwnedCoreMetaBatchOp]) -> String {
    let mut bytes = Vec::new();
    for op in ops {
        match op {
            OwnedCoreMetaBatchOp::Put {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } => {
                bytes.push(1);
                append_hash_part(&mut bytes, cf.as_bytes());
                append_hash_part(&mut bytes, &table_id.to_be_bytes());
                append_hash_part(&mut bytes, tuple_key);
                append_hash_part(&mut bytes, payload);
            }
            OwnedCoreMetaBatchOp::Delete {
                cf,
                table_id,
                tuple_key,
                ..
            } => {
                bytes.push(0);
                append_hash_part(&mut bytes, cf.as_bytes());
                append_hash_part(&mut bytes, &table_id.to_be_bytes());
                append_hash_part(&mut bytes, tuple_key);
            }
        }
    }
    domain_hash_bytes("anvil.admission.payload-set.v1", &bytes)
}

fn append_hash_part(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u64).to_be_bytes());
    out.extend_from_slice(value);
}

fn validate_admission_hard_capacity(
    state: &AdmissionPointState,
    incoming_pending_bytes: u64,
    incoming_landed_bytes: u64,
    limits: CoreAdmissionCapacityLimits,
) -> Result<()> {
    if state.pending_rows.saturating_add(1) > limits.pending_mutation_hard_limit_rows
        || state.pending_bytes.saturating_add(incoming_pending_bytes)
            > limits.pending_mutation_hard_limit_bytes
        || state.landed_bytes.saturating_add(incoming_landed_bytes)
            > limits.landed_bytes_hard_limit_bytes
    {
        bail!(
            "{}: CoreStore admission shard hard capacity limit exceeded",
            AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str()
        );
    }
    if state
        .lag_seconds(unix_timestamp_nanos())
        .is_some_and(|lag| lag > limits.pending_mutation_hard_lag_seconds)
    {
        bail!(
            "{}: CoreStore admission shard hard lag limit exceeded",
            AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str()
        );
    }
    Ok(())
}
