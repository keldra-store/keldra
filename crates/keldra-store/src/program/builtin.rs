use super::*;

impl Store {
    pub async fn prepare_builtin_object_transaction(
        &self,
        plan: &BuiltInObjectTransactionPlan,
    ) -> Result<PreparedProgramBundle, ProgramStoreError> {
        let authority = ProgramBundleAuthority::BuiltInObjectTransaction {
            kind: plan.authority_kind,
            contract_version: plan.contract_version,
        };
        authority
            .validate(false)
            .map_err(|message| ProgramStoreError::InvalidBundle(message.into()))?;
        plan.participant_manifest
            .validate()
            .map_err(ProgramStoreError::InvalidBundle)?;
        validate_builtin_plan(plan)?;
        let source_encoded = serde_json::to_vec(plan).map_err(program_storage_error)?;
        let source_bundle_hash = PreparedBundleHash(tagged_hash(
            b"keldra.builtin-object-transaction.v1",
            &source_encoded,
        ));
        let committed_at_unix_millis = now_unix_millis().map_err(program_mutation_error)?;
        let mut allocated_versions = Vec::with_capacity(plan.writes.len());
        let mut writes = Vec::with_capacity(plan.writes.len());
        for write in &plan.writes {
            let participant = plan
                .participant_manifest
                .objects
                .iter()
                .find(|participant| participant.path == write.path)
                .ok_or_else(|| {
                    ProgramStoreError::InvalidBundle(
                        "built-in write has no exact manifest participant".into(),
                    )
                })?;
            if !participant.intent.put
                || participant.condition.observed_head().as_ref() != Some(&write.expected)
            {
                return Err(ProgramStoreError::InvalidBundle(
                    "built-in write intent or head condition differs from its manifest".into(),
                ));
            }
            let (blob, content_type, deleted) = match &write.payload {
                BuiltInWritePayload::Inline {
                    bytes,
                    content_type,
                } if !bytes.is_empty() && !content_type.is_empty() => (
                    Some(
                        self.stage_blob(bytes)
                            .await
                            .map_err(program_mutation_error)?,
                    ),
                    Some(content_type.clone()),
                    false,
                ),
                BuiltInWritePayload::ExistingReference(existing) => {
                    let source = plan
                        .participant_manifest
                        .objects
                        .get(existing.source_participant_index as usize)
                        .ok_or_else(|| {
                            ProgramStoreError::InvalidBundle(
                                "existing-reference source participant is out of bounds".into(),
                            )
                        })?;
                    let Some(expected) = source.condition.retained_version() else {
                        return Err(ProgramStoreError::InvalidBundle(
                            "existing-reference source is not an exact retained version".into(),
                        ));
                    };
                    if expected.deleted
                        || expected.blob.as_ref().is_none_or(|blob| {
                            blob.hash != existing.blob_hash || blob.length != existing.blob_length
                        })
                        || expected.content_type != existing.content_type
                    {
                        return Err(ProgramStoreError::InvalidBundle(
                            "existing-reference payload differs from its exact retained source"
                                .into(),
                        ));
                    }
                    (
                        Some(BlobRef {
                            hash: existing.blob_hash,
                            length: existing.blob_length,
                        }),
                        existing.content_type.clone(),
                        false,
                    )
                }
                BuiltInWritePayload::StagedReference {
                    blob_hash,
                    blob_length,
                    content_type,
                    upload_source_node_id,
                } if *blob_hash != [0; 32] && *upload_source_node_id != 0 => {
                    let reference = BlobRef {
                        hash: *blob_hash,
                        length: *blob_length,
                    };
                    if *upload_source_node_id == u64::from(self.node_id) {
                        self.open_blob(&reference)
                            .await
                            .map_err(program_mutation_error)?;
                    }
                    (Some(reference), content_type.clone(), false)
                }
                BuiltInWritePayload::Tombstone => (None, None, true),
                BuiltInWritePayload::Inline { .. } => {
                    return Err(ProgramStoreError::InvalidBundle(
                        "built-in inline write requires non-empty bytes and content type".into(),
                    ));
                }
                BuiltInWritePayload::StagedReference { .. } => {
                    return Err(ProgramStoreError::InvalidBundle(
                        "built-in staged reference requires a nonzero blob hash and upload source"
                            .into(),
                    ));
                }
            };
            let version_id = self.clock.next().map_err(program_storage_error)?;
            let protected_link_descriptor = plan.authority_kind == 2
                && !deleted
                && content_type.as_deref() == Some(crate::OBJECT_LINK_CONTENT_TYPE);
            allocated_versions.push(version_id);
            writes.push(PreparedVersionWrite {
                path: write.path.clone(),
                expected: write.expected.clone(),
                previous_version: write.previous_version.clone(),
                version: Version {
                    id: version_id,
                    blob,
                    content_type,
                    deleted,
                    committed_at_unix_millis,
                    protected_link_descriptor,
                },
            });
        }
        let record = StoredPreparedBundle {
            format: PREPARED_BUNDLE_FORMAT,
            source_bundle_hash,
            program_hash: ProgramHash([0; 32]),
            authority,
            participant_manifest: plan.participant_manifest.clone(),
            builtin_plan: Some(plan.clone()),
            alias_bindings: Vec::new(),
            alias_registry_transitions: Vec::new(),
            preconditions: plan.head_preconditions.clone(),
            writes,
            receipt: plan.receipt.clone(),
        };
        validate_prepared_record(&record)?;
        let bundle_bytes = serde_json::to_vec(&record).map_err(program_storage_error)?;
        let bundle = PreparedBundleRef::from(
            self.stage_blob(&bundle_bytes)
                .await
                .map_err(program_mutation_error)?,
        );
        let hash = PreparedBundleHash(bundle.hash);
        if let Some(allocated) = allocated_versions.into_iter().max() {
            let _commit_guard = self.lock_commit("builtin_object_transaction").await;
            let persisted = self.version_high_watermark()?.unwrap_or(VersionId(0));
            let mut batch = WriteBatch::default();
            batch.put_cf(
                self.program_cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&allocated.max(persisted)).map_err(program_storage_error)?,
            );
            self.write_program_batch(batch)?;
        }
        let durability = self.local_program_durability_evidence(bundle);
        let durability_evidence_hash = durability.hash()?;
        Ok(PreparedProgramBundle {
            hash,
            source_bundle_hash,
            program_hash: record.program_hash,
            authority,
            participant_manifest_hash: record
                .participant_manifest
                .hash()
                .map_err(ProgramStoreError::InvalidBundle)?,
            bundle,
            durability_evidence_hash,
            durability,
        })
    }
}
