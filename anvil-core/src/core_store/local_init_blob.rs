use super::local_stream_control::control_record_proto::{
    encode_object_manifest_record, object_manifest_root_generation,
};
use super::local_tx_rows::{OwnedCoreMetaBatchOp, borrow_owned_coremeta_batch_ops};
use super::*;
use crate::formats::{
    hash32,
    writer::{WriterFamily, canonical_logical_file_id},
};
use futures_util::{StreamExt, stream::FuturesUnordered};

struct PreparedInlineBlob {
    object_ref: CoreObjectRef,
    mutation_id: String,
    operations: Vec<OwnedCoreMetaBatchOp>,
    publication: CoreMetaRootPublication,
}

fn core_store_instance_registry_key(storage: &Storage) -> PathBuf {
    storage.core_store_root_path()
}

impl CoreStore {
    pub async fn new(storage: Storage) -> Result<Self> {
        if let Some(store) = Self::registered_for_storage(&storage) {
            return Ok(store);
        }
        Self::initialise_registered_store(
            storage,
            None,
            CoreStoreNodeIdentity::default(),
            false,
            CoreStoreStartupRecovery::Immediate,
        )
        .await
    }

    pub async fn new_with_pipeline_keyring(
        storage: Storage,
        pipeline_keyring: CorePipelineKeyring,
    ) -> Result<Self> {
        Self::initialise_registered_store(
            storage,
            Some(Arc::new(pipeline_keyring)),
            CoreStoreNodeIdentity::default(),
            true,
            CoreStoreStartupRecovery::Immediate,
        )
        .await
    }

    pub async fn new_with_pipeline_keyring_and_identity(
        storage: Storage,
        pipeline_keyring: CorePipelineKeyring,
        node_identity: CoreStoreNodeIdentity,
        startup_recovery: CoreStoreStartupRecovery,
    ) -> Result<Self> {
        Self::initialise_registered_store(
            storage,
            Some(Arc::new(pipeline_keyring)),
            node_identity,
            true,
            startup_recovery,
        )
        .await
    }

    async fn initialise_registered_store(
        storage: Storage,
        pipeline_keyring: Option<Arc<CorePipelineKeyring>>,
        node_identity: CoreStoreNodeIdentity,
        require_matching_configuration: bool,
        startup_recovery: CoreStoreStartupRecovery,
    ) -> Result<Self> {
        // Only one constructor may recover and publish a process-local store for
        // a storage root. Without the second registry check, concurrent callers
        // could replay a live admitted mutation while the first instance was
        // finalising it.
        let startup_recovery_lock = startup_recovery_lock(storage.core_store_root_path());
        let startup_guard = startup_recovery_lock.lock().await;
        if let Some(store) = Self::registered_for_storage(&storage) {
            if require_matching_configuration
                && (store.pipeline_keyring != pipeline_keyring
                    || store.node_identity != node_identity)
            {
                bail!("CoreStore is already open with different process configuration");
            }
            return Ok(store);
        }

        let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
        // Node signing identity is needed to verify publication evidence before
        // a visibility-aware CoreStore instance can finish initialising.
        let node_signing_keypair = Arc::new(load_or_create_node_signing_keypair(&meta)?);
        let receipt_signing_public_key = node_signing_keypair.public_key_bytes().to_vec();
        let admission_mutation_epoch = node_admission_mutation_epoch(&receipt_signing_public_key);
        store_node_receipt_signing_public_key(
            &meta,
            &node_identity.node_id,
            &receipt_signing_public_key,
        )?;
        // Before a mesh exists, the local quorum models control replicas with
        // synthetic identities. Their public verification keys are portable
        // bootstrap evidence, even though their private key is node-local. A
        // portable bootstrap can already bind them to the signer that produced
        // historical evidence, so startup must not replace that identity.
        for replica_node_id in local_control_node_ids() {
            seed_node_receipt_signing_public_key_if_absent(
                &meta,
                &replica_node_id,
                &receipt_signing_public_key,
            )?;
        }
        let storage_classes = CoreStorageClassCatalog::release_defaults();
        let mut store = Self {
            storage,
            meta,
            startup_recovery_lock: startup_recovery_lock.clone(),
            internal_channels: Arc::new(Mutex::new(BTreeMap::new())),
            coremeta_streams: Arc::new(Mutex::new(BTreeMap::new())),
            coremeta_recovery: Arc::new(
                super::local_coremeta_recovery::CoreMetaRecoveryState::default(),
            ),
            root_owner_failure_tracker: Arc::new(Mutex::new(
                super::local_root_failover::RootOwnerFailureTracker::default(),
            )),
            repair_task_scheduler: Arc::new(OnceLock::new()),
            pipeline_keyring,
            storage_classes,
            node_signing_keypair,
            admission_mutation_epoch,
            node_identity,
            startup_recovery_deferred: false,
        };
        store.ensure_layout().await?;
        store.bootstrap_system_root_anchor().await?;
        store.startup_recovery_deferred = startup_recovery == CoreStoreStartupRecovery::Distributed
            && (store.has_owned_pending_root_publication_intents()?
                || store.has_pending_mutations()?);
        if !store.startup_recovery_deferred {
            store.recover_root_publication_intents().await?;
            store.recover_pending_mutations(&startup_guard).await?;
        }
        store.register_process_instance();
        Ok(store)
    }

    pub fn startup_recovery_deferred(&self) -> bool {
        self.startup_recovery_deferred
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    fn registered_for_storage(storage: &Storage) -> Option<Self> {
        let key = core_store_instance_registry_key(storage);
        CORE_STORE_INSTANCE_REGISTRY
            .lock()
            .ok()
            .and_then(|registry| registry.get(&key).cloned())
    }

    fn register_process_instance(&self) {
        if let Ok(mut registry) = CORE_STORE_INSTANCE_REGISTRY.lock() {
            registry.insert(
                core_store_instance_registry_key(&self.storage),
                self.clone(),
            );
        }
    }

    #[cfg(test)]
    pub(super) fn unregister_process_instance_for_tests(&self) {
        if let Ok(mut registry) = CORE_STORE_INSTANCE_REGISTRY.lock() {
            registry.remove(&core_store_instance_registry_key(&self.storage));
        }
    }

    pub fn local_receipt_signing_public_key(&self) -> Vec<u8> {
        self.node_signing_keypair.public_key_bytes().to_vec()
    }

    pub(super) fn sign_core_receipt(&self, signed_payload_hash: &str) -> Result<Vec<u8>> {
        Ok(self
            .node_signing_keypair
            .sign(signed_payload_hash.as_bytes()))
    }

    pub(super) fn verify_core_receipt_signature(
        &self,
        node_id: &str,
        signed_payload_hash: &str,
        receipt_signature: &[u8],
    ) -> Result<()> {
        let public_key = if is_local_shard_node_id(node_id) || node_id == self.node_identity.node_id
        {
            self.node_signing_keypair.public_key()
        } else {
            // Receipt identity is required during startup and recovery before
            // the corresponding mesh root can be publication-visible.
            load_node_receipt_signing_public_key(&self.meta, node_id)?.ok_or_else(|| {
                anyhow!("CoreStore shard receipt references unknown node {node_id}")
            })?
        };
        public_key
            .verify(signed_payload_hash.as_bytes(), receipt_signature)
            .with_context(|| {
                format!("CoreStore shard receipt signature verification failed for node {node_id}")
            })
    }

    pub(super) fn verify_core_admission_signature(
        &self,
        node_id: &str,
        signed_payload_hash: &str,
        receipt_signature: &[u8],
    ) -> Result<()> {
        if node_id != self.node_identity.node_id {
            bail!("CoreStore admission receipt references unknown source node {node_id}");
        }
        self.node_signing_keypair
            .public_key()
            .verify(signed_payload_hash.as_bytes(), receipt_signature)
            .with_context(|| {
                format!(
                    "CoreStore admission receipt signature verification failed for source node {node_id}"
                )
            })
    }

    pub(super) fn verify_object_placement_receipt(
        &self,
        block_id: &str,
        profile_id: &str,
        placement: &CoreObjectPlacement,
        boundary_summary_hash: &str,
    ) -> Result<()> {
        let profile = local_erasure_profile(profile_id)?;
        if is_local_shard_node_id(&placement.node_id) {
            validate_local_shard_receipt_placement(
                profile,
                usize::from(placement.shard_index),
                &placement.node_id,
                &placement.region_id,
                &placement.cell_id,
            )?;
        } else {
            validate_logical_id(&placement.node_id, "shard receipt node id")?;
            validate_logical_id(&placement.region_id, "shard receipt region id")?;
            validate_logical_id(&placement.cell_id, "shard receipt cell id")?;
        }
        let expected = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id,
            shard_index: placement.shard_index,
            erasure_profile: profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: placement.placement_epoch,
            shard_length: placement.stored_size,
            shard_hash: &placement.shard_hash,
            fsync_sequence: placement.fsync_sequence,
            written_at_unix_nanos: placement.written_at_unix_nanos,
            boundary_summary_hash,
        });
        validate_shard_receipt_common(
            &placement.node_id,
            &placement.region_id,
            &placement.cell_id,
            &placement.shard_hash,
            placement.stored_size,
            placement.fsync_sequence,
            placement.written_at_unix_nanos,
            &placement.signed_payload_hash,
            &placement.signature_algorithm,
            &placement.receipt_signature,
            &expected,
        )?;
        self.verify_core_receipt_signature(
            &placement.node_id,
            &placement.signed_payload_hash,
            &placement.receipt_signature,
        )?;
        Ok(())
    }

    pub(super) fn verify_manifest_locator_receipts(
        &self,
        locator: &CoreManifestLocator,
    ) -> Result<()> {
        for block in &locator.block_locators {
            for receipt in &block.shard_receipts {
                self.verify_core_receipt_signature(
                    &receipt.node_id,
                    &receipt.signed_payload_hash,
                    &receipt.receipt_signature,
                )?;
            }
        }
        Ok(())
    }

    pub async fn put_blob(&self, input: PutBlob) -> Result<CoreObjectRef> {
        self.put_blob_with_storage_class(input, None).await
    }

    pub async fn put_blob_with_storage_class(
        &self,
        input: PutBlob,
        storage_class_id: Option<&str>,
    ) -> Result<CoreObjectRef> {
        let storage_class = self.select_storage_class(storage_class_id)?.clone();
        let profile = local_erasure_profile_from_byte_profile(&storage_class.byte_profile)?;
        self.put_blob_with_profile_and_encoding_policy(
            input,
            profile,
            &storage_class.byte_profile.compression,
            "none",
            WriterFamily::ObjectBlob.as_str(),
            storage_class.inline_payload_policy,
        )
        .await
    }

    pub async fn put_blobs_with_storage_classes(
        &self,
        inputs: Vec<(PutBlob, Option<String>)>,
    ) -> Result<Vec<CoreObjectRef>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let all_inline = inputs.iter().all(|(input, storage_class_id)| {
            self.select_storage_class(storage_class_id.as_deref())
                .is_ok_and(|storage_class| {
                    let policy = &storage_class.inline_payload_policy;
                    policy.enabled
                        && input.bytes.len() <= policy.effective_raw_payload_cap_bytes() as usize
                })
        });
        let payloads_are_unique = inputs
            .iter()
            .map(|(input, _)| sha256_hex(&input.bytes))
            .collect::<BTreeSet<_>>()
            .len()
            == inputs.len();

        if all_inline && payloads_are_unique {
            let mut prepared = Vec::with_capacity(inputs.len());
            for (input, storage_class_id) in inputs {
                let storage_class = self.select_storage_class(storage_class_id.as_deref())?;
                prepared.push(self.prepare_inline_blob(
                    input,
                    WriterFamily::ObjectBlob.as_str(),
                    storage_class.inline_payload_policy.clone(),
                )?);
            }
            return self.commit_prepared_inline_blobs(prepared).await;
        }

        let mut objects = Vec::with_capacity(inputs.len());
        for (input, storage_class_id) in inputs {
            objects.push(
                self.put_blob_with_storage_class(input, storage_class_id.as_deref())
                    .await?,
            );
        }
        Ok(objects)
    }

    pub(crate) async fn put_format_blob(
        &self,
        input: PutBlob,
        writer_family: WriterFamily,
    ) -> Result<CoreObjectRef> {
        let storage_class = self.default_storage_class()?.clone();
        let profile = local_erasure_profile_from_byte_profile(&storage_class.byte_profile)?;
        self.put_blob_with_profile_and_encoding_policy(
            input,
            profile,
            &storage_class.byte_profile.compression,
            "none",
            writer_family.as_str(),
            storage_class.inline_payload_policy,
        )
        .await
    }

    pub(super) async fn put_blob_with_profile(
        &self,
        input: PutBlob,
        profile: LocalErasureProfile,
    ) -> Result<CoreObjectRef> {
        self.put_blob_with_profile_and_encoding(
            input,
            profile,
            "none",
            WriterFamily::ObjectBlob.as_str(),
        )
        .await
    }

    pub(super) async fn put_logical_file_block_with_profile(
        &self,
        request: &WriteLogicalFileRequest,
        block_index: usize,
        logical_offset: u64,
        bytes: Vec<u8>,
        block_plain_hash: String,
        encryption_algorithm: String,
        profile: LocalErasureProfile,
    ) -> Result<CoreObjectRef> {
        let block_logical_file_id = canonical_logical_file_id(
            WriterFamily::from_name(&request.writer_family).ok_or_else(|| {
                anyhow!("CoreStore unknown writer family {}", request.writer_family)
            })?,
            request.generation,
            &format!("{}/block-{block_index:06}", request.logical_file_id),
            block_plain_hash.as_bytes(),
        );
        let input = PutBlob {
            logical_name: block_logical_file_id,
            bytes,
            boundary_values: request.boundary_values.clone(),
            region_id: request.region_id.clone(),
            mutation_id: format!("{}-block-{block_index:06}", request.mutation_id),
        };
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "put_blob")]);
        self.ensure_layout().await?;
        validate_logical_id(&input.logical_name, "blob logical name")?;
        validate_logical_id(&request.writer_family, "blob writer family")?;
        let stored_hash = format!("sha256:{}", sha256_hex(&input.bytes));
        let stored_len = input.bytes.len() as u64;
        let compression = none_compression_descriptor(&input.bytes);
        let admission = match self
            .admit_core_mutation_outcome(
                "object.put",
                &request.writer_family,
                CorePendingMutationTarget::ObjectPut {
                    logical_name: input.logical_name.clone(),
                    region_id: input.region_id.clone(),
                    erasure_profile_id: profile.id.to_string(),
                    encryption: encryption_algorithm.clone(),
                    block_plain_hash: block_plain_hash.clone(),
                    object_hash: stored_hash.clone(),
                    object_logical_size: stored_len,
                    compression: compression.clone(),
                    writer_generation: request.generation,
                    block_ordinal: block_index as u64,
                    logical_offset,
                },
                input.mutation_id.clone(),
                None,
                CorePendingMutationPayload::Landed(&input.bytes),
                input.boundary_values,
            )
            .await
            .with_context(|| {
                format!(
                    "admit CoreStore logical-file block mutation logical_file_id={} block_index={} mutation_id={}",
                    request.logical_file_id, block_index, input.mutation_id
                )
            })?
        {
            CoreAdmissionOutcome::Pending(admission) => admission,
            CoreAdmissionOutcome::Finalised(finalisation) => {
                return self.finalised_object_put_result(finalisation).await;
            }
        };
        let landed =
            admission.landed_bytes.first().cloned().ok_or_else(|| {
                anyhow!("CoreStore put_blob admission did not produce landed bytes")
            })?;
        let object_ref = match async {
            let materialised_bytes = self.read_landed_bytes(&landed).await?;
            let hash = strip_sha256_prefix(&landed.sha256)?.to_string();
            self.materialise_object_blob_bytes(
                &input.logical_name,
                request.generation,
                block_index as u64,
                logical_offset,
                &block_plain_hash,
                &hash,
                &materialised_bytes,
                &stored_hash,
                stored_len,
                compression,
                &admission.boundary_values,
                &admission.mutation_id,
                profile,
                &encryption_algorithm,
                &request.writer_family,
            )
            .await
            .with_context(|| {
                format!(
                    "materialise CoreStore logical-file block logical_file_id={} block_index={} mutation_id={}",
                    request.logical_file_id, block_index, admission.mutation_id
                )
            })
        }
        .await
        {
            Ok(object_ref) => object_ref,
            Err(error) => {
                if let Err(finalise_error) = self
                    .mark_pending_mutation_finalised_unlocked(&admission, "aborted")
                    .await
                {
                    return Err(error).with_context(|| {
                        format!(
                            "abort CoreStore logical-file block mutation mutation_id={} after materialisation failure also failed: {finalise_error:#}",
                            admission.mutation_id
                        )
                    });
                }
                return Err(error);
            }
        };
        self.mark_pending_mutation_finalised_with_result_unlocked(
            &admission,
            "committed",
            Some(CorePendingMutationFinalisationResult::ObjectRef(
                object_ref.clone(),
            )),
        )
            .await
            .with_context(|| {
                format!(
                    "finalise CoreStore logical-file block mutation logical_file_id={} block_index={} mutation_id={}",
                    request.logical_file_id, block_index, admission.mutation_id
                )
            })?;
        Ok(object_ref)
    }

    pub(super) async fn put_blob_with_profile_and_encoding(
        &self,
        input: PutBlob,
        profile: LocalErasureProfile,
        encryption_algorithm: &str,
        writer_family: &str,
    ) -> Result<CoreObjectRef> {
        let inline_policy = self.default_storage_class()?.inline_payload_policy.clone();
        self.put_blob_with_profile_and_encoding_policy(
            input,
            profile,
            "none",
            encryption_algorithm,
            writer_family,
            inline_policy,
        )
        .await
    }

    pub(super) async fn put_blob_with_profile_and_encoding_policy(
        &self,
        input: PutBlob,
        profile: LocalErasureProfile,
        compression_algorithm: &str,
        encryption_algorithm: &str,
        writer_family: &str,
        inline_policy: CoreInlinePayloadPolicy,
    ) -> Result<CoreObjectRef> {
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "put_blob")]);
        self.ensure_layout().await?;
        validate_logical_id(&input.logical_name, "blob logical name")?;
        validate_writer_family(writer_family, "blob writer family")?;
        validate_object_blob_pipeline_options(compression_algorithm, encryption_algorithm)?;
        inline_policy.validate()?;
        let inline_cap = inline_policy.max_raw_payload_bytes as usize;
        if inline_policy.enabled
            && writer_family != WriterFamily::CoreControl.as_str()
            && encryption_algorithm == "none"
            && input.bytes.len() <= inline_cap
        {
            return self
                .put_inline_blob(input, writer_family, inline_policy)
                .await;
        }
        let compression_started_at = Instant::now();
        let logical_hash = format!("sha256:{}", sha256_hex(&input.bytes));
        let logical_size = input.bytes.len() as u64;
        let (stored_bytes, compression) =
            encode_logical_file_source(compression_algorithm, input.bytes)?;
        record_byte_pipeline_stage_duration(
            "compress",
            writer_family,
            &compression.algorithm,
            encryption_algorithm,
            profile.id,
            compression_started_at.elapsed(),
        );
        crate::perf::record_compression_ratio(
            writer_family,
            &compression.algorithm,
            profile.id,
            logical_size,
            stored_bytes.len() as u64,
        );
        if compression.algorithm != "none" {
            record_corestore_trace_event("byte_pipeline.compress", "ok");
        }
        let stored_hash = format!("sha256:{}", sha256_hex(&stored_bytes));
        let writer = WriterFamily::from_name(writer_family)
            .ok_or_else(|| anyhow!("CoreStore writer family is not registered"))?;
        let logical_file_id = if is_canonical_logical_file_id(&input.logical_name) {
            input.logical_name.clone()
        } else {
            canonical_logical_file_id(writer, 0, &input.logical_name, &hash32(&stored_bytes))
        };
        let admission = match self
            .admit_core_mutation_outcome(
                "object.put",
                writer_family,
                CorePendingMutationTarget::ObjectPut {
                    logical_name: logical_file_id.clone(),
                    region_id: input.region_id.clone(),
                    erasure_profile_id: profile.id.to_string(),
                    encryption: encryption_algorithm.to_string(),
                    block_plain_hash: logical_hash.clone(),
                    object_hash: logical_hash.clone(),
                    object_logical_size: logical_size,
                    compression: compression.clone(),
                    writer_generation: 0_u64,
                    block_ordinal: 0_u64,
                    logical_offset: 0,
                },
                input.mutation_id.clone(),
                None,
                CorePendingMutationPayload::Landed(&stored_bytes),
                input.boundary_values,
            )
            .await?
        {
            CoreAdmissionOutcome::Pending(admission) => admission,
            CoreAdmissionOutcome::Finalised(finalisation) => {
                return self.finalised_object_put_result(finalisation).await;
            }
        };
        let landed =
            admission.landed_bytes.first().cloned().ok_or_else(|| {
                anyhow!("CoreStore put_blob admission did not produce landed bytes")
            })?;
        let object_ref = match async {
            let materialised_bytes = self.read_landed_bytes(&landed).await?;
            if landed.sha256 != stored_hash {
                bail!("CoreStore landed byte hash does not match encoded object hash");
            }
            let stored_hash_hex = strip_sha256_prefix(&landed.sha256)?.to_string();
            self.materialise_object_blob_bytes(
                &logical_file_id,
                0,
                0,
                0,
                &logical_hash,
                &stored_hash_hex,
                &materialised_bytes,
                &logical_hash,
                logical_size,
                compression,
                &admission.boundary_values,
                &admission.mutation_id,
                profile,
                encryption_algorithm,
                writer_family,
            )
            .await
        }
        .await
        {
            Ok(object_ref) => object_ref,
            Err(error) => {
                if let Err(finalise_error) = self
                    .mark_pending_mutation_finalised_unlocked(&admission, "aborted")
                    .await
                {
                    return Err(error).with_context(|| {
                        format!(
                            "abort CoreStore blob mutation mutation_id={} after materialisation failure also failed: {finalise_error:#}",
                            admission.mutation_id
                        )
                    });
                }
                return Err(error);
            }
        };
        self.mark_pending_mutation_finalised_with_result_unlocked(
            &admission,
            "committed",
            Some(CorePendingMutationFinalisationResult::ObjectRef(
                object_ref.clone(),
            )),
        )
        .await?;
        Ok(object_ref)
    }

    async fn finalised_object_put_result(
        &self,
        finalisation: CorePendingMutationFinalisationRecord,
    ) -> Result<CoreObjectRef> {
        if finalisation.state != "committed" || finalisation.operation_family != "object.put" {
            bail!(
                "CoreStore object mutation {} was finalised in state {}",
                finalisation.mutation_id,
                finalisation.state
            );
        }
        let CorePendingMutationTarget::ObjectPut {
            logical_name,
            region_id,
            erasure_profile_id,
            encryption,
            object_hash,
            object_logical_size,
            compression,
            logical_offset,
            ..
        } = &finalisation.target
        else {
            bail!("CoreStore finalised object mutation has a non-object target");
        };
        let Some(CorePendingMutationFinalisationResult::ObjectRef(object_ref)) =
            finalisation.result.as_ref()
        else {
            bail!("CoreStore committed object mutation has no object result");
        };
        let manifest = self.read_object_manifest(&object_ref).await?;
        if manifest.mutation_id != finalisation.mutation_id
            || manifest.logical_file_id != *logical_name
            || manifest.region_id != *region_id
            || manifest.writer_family != finalisation.writer_family
            || manifest.object_hash != *object_hash
            || manifest.logical_size != *object_logical_size
            || manifest.logical_offset != *logical_offset
            || manifest.encryption_algorithm != *encryption
            || manifest.encoding.profile_id != *erasure_profile_id
            || manifest.encoding.compression != *compression
            || manifest.boundary_values != finalisation.boundary_values
        {
            bail!("CoreStore finalised object result does not match its admitted mutation");
        }
        Ok(object_ref.clone())
    }

    pub(crate) async fn put_inline_blob(
        &self,
        input: PutBlob,
        writer_family: &str,
        inline_policy: CoreInlinePayloadPolicy,
    ) -> Result<CoreObjectRef> {
        let prepared = self.prepare_inline_blob(input, writer_family, inline_policy)?;
        let mutation_id = prepared.mutation_id.clone();
        let operations = borrow_owned_coremeta_batch_ops(&prepared.operations);
        self.commit_coremeta_root_groups(
            &mutation_id,
            &operations,
            std::slice::from_ref(&prepared.publication),
        )
        .await?;
        self.read_inline_blob(&prepared.object_ref)?;
        record_corestore_trace_event("byte_pipeline.inline_payload", "ok");
        Ok(prepared.object_ref)
    }

    fn prepare_inline_blob(
        &self,
        input: PutBlob,
        writer_family: &str,
        inline_policy: CoreInlinePayloadPolicy,
    ) -> Result<PreparedInlineBlob> {
        inline_policy.validate()?;
        let raw_cap =
            (inline_policy.max_raw_payload_bytes as usize).min(CORE_META_MAX_INLINE_PAYLOAD_BYTES);
        if input.bytes.len() > raw_cap {
            bail!(
                "CoreStore inline blob is {} bytes before RocksDB compression, exceeding {} bytes",
                input.bytes.len(),
                raw_cap
            );
        }
        validate_logical_id(&input.logical_name, "inline blob logical name")?;
        validate_writer_family(writer_family, "inline blob writer family")?;
        let writer = WriterFamily::from_name(writer_family)
            .ok_or_else(|| anyhow!("CoreStore writer family is not registered"))?;
        let logical_file_id = if is_canonical_logical_file_id(&input.logical_name) {
            input.logical_name.clone()
        } else {
            canonical_logical_file_id(writer, 0, &input.logical_name, &hash32(&input.bytes))
        };
        let hash = format!("sha256:{}", sha256_hex(&input.bytes));
        let hash_hex = strip_sha256_prefix(&hash)?;
        let block_id = local_inline_payload_block_id(hash_hex);
        let object_ref = CoreObjectRef {
            hash: hash.clone(),
            logical_size: input.bytes.len() as u64,
            manifest_ref: encode_manifest_ref_with_profile(
                hash_hex,
                LOCAL_INLINE_PAYLOAD_PROFILE_ID,
            ),
            encoding: CoreObjectEncoding {
                block_id,
                profile_id: LOCAL_INLINE_PAYLOAD_PROFILE_ID.to_string(),
                data_shards: 0,
                parity_shards: 0,
                minimum_read_shards: 0,
                minimum_write_ack_shards: 0,
                stripe_size: input.bytes.len() as u64,
                placement_scope: "coremeta-inline".to_string(),
                repair_priority: "metadata-quorum".to_string(),
                stored_hash: hash.clone(),
                compression: none_compression_descriptor(&input.bytes),
                encryption: "none".to_string(),
            },
            placements: Vec::new(),
        };
        let manifest = CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: "local".to_string(),
            region_id: input.region_id,
            object_hash: hash,
            logical_size: input.bytes.len() as u64,
            logical_file_id,
            logical_offset: 0,
            writer_family: writer_family.to_string(),
            encryption_algorithm: "none".to_string(),
            boundary_values: input.boundary_values,
            encoding: object_ref.encoding.clone(),
            placements: Vec::new(),
            created_at: Utc::now().to_rfc3339(),
            mutation_id: input.mutation_id,
        };
        let inline_key = inline_payload_meta_key(&object_ref);
        let manifest_key = object_manifest_meta_key(&object_ref);
        let root_anchor_key = object_manifest_root_anchor_key(&manifest.object_hash);
        let common = core_meta_committed_row_common(
            format!("mesh/{}/region/{}", manifest.mesh_id, manifest.region_id),
            core_meta_root_key_hash(&root_anchor_key),
            object_manifest_root_generation(manifest.logical_size),
            manifest.mutation_id.clone(),
            unix_timestamp_nanos(),
        );
        let inline_payload = put_inline_payload_row(&input.bytes, common.clone())?;
        if inline_payload.len() > inline_policy.absolute_encoded_record_max_bytes as usize {
            bail!(
                "CoreStore inline payload envelope is {} bytes, exceeding {} bytes",
                inline_payload.len(),
                inline_policy.absolute_encoded_record_max_bytes
            );
        }
        let manifest_payload = encode_object_manifest_record(&manifest)?;
        Ok(PreparedInlineBlob {
            object_ref,
            mutation_id: manifest.mutation_id,
            operations: vec![
                OwnedCoreMetaBatchOp::Put {
                    cf: CF_INLINE_PAYLOADS,
                    table_id: TABLE_INLINE_PAYLOAD_ROW,
                    tuple_key: inline_key,
                    common: Some(common),
                    payload: inline_payload,
                },
                OwnedCoreMetaBatchOp::Put {
                    cf: CF_OBJECT_VERSIONS,
                    table_id: TABLE_OBJECT_VERSION_META_ROW,
                    tuple_key: manifest_key,
                    common: None,
                    payload: manifest_payload,
                },
            ],
            publication: CoreMetaRootPublication::new(root_anchor_key, WriterFamily::ObjectBlob),
        })
    }

    async fn commit_prepared_inline_blobs(
        &self,
        prepared: Vec<PreparedInlineBlob>,
    ) -> Result<Vec<CoreObjectRef>> {
        let mut transaction_hasher = blake3::Hasher::new();
        let mut operations = Vec::with_capacity(prepared.len() * 2);
        let mut publications = Vec::with_capacity(prepared.len());
        let mut object_refs = Vec::with_capacity(prepared.len());
        for blob in prepared {
            transaction_hasher.update(blob.mutation_id.as_bytes());
            operations.extend(blob.operations);
            publications.push(blob.publication);
            object_refs.push(blob.object_ref);
        }
        if publications.len() > 1 {
            publications[0].transaction_coordinator = true;
        }
        let transaction_id = format!(
            "inline-blob-batch:{}",
            transaction_hasher.finalize().to_hex()
        );
        let operations = borrow_owned_coremeta_batch_ops(&operations);
        self.commit_coremeta_root_groups(&transaction_id, &operations, &publications)
            .await?;
        for object_ref in &object_refs {
            self.read_inline_blob(object_ref)?;
        }
        record_corestore_trace_event("byte_pipeline.inline_payload_batch", "ok");
        Ok(object_refs)
    }

    pub(super) async fn materialise_object_blob_bytes(
        &self,
        logical_file_id: &str,
        writer_generation: u64,
        block_ordinal: u64,
        logical_offset: u64,
        block_plain_hash: &str,
        stored_hash: &str,
        materialised_bytes: &[u8],
        object_hash: &str,
        object_logical_size: u64,
        compression: CoreCompressionDescriptor,
        boundary_values: &[CoreBoundaryValue],
        mutation_id: &str,
        profile: LocalErasureProfile,
        encryption_algorithm: &str,
        writer_family: &str,
    ) -> Result<CoreObjectRef> {
        validate_writer_family(writer_family, "materialised object writer family")?;
        if sha256_hex(materialised_bytes) != stored_hash {
            bail!("CoreStore object materialisation hash mismatch");
        }
        validate_hash(object_hash, "materialised object logical hash")?;
        if compression.compressed_length != materialised_bytes.len() as u64 {
            bail!("CoreStore object compression descriptor length does not match stored bytes");
        }
        if compression.uncompressed_length != object_logical_size {
            bail!("CoreStore object compression descriptor length does not match logical size");
        }
        let boundary_summary_hash = boundary_summary_hash(boundary_values)?;
        let boundary_values_b64 = encode_boundary_values_b64(boundary_values)?;
        let block_id =
            local_block_id_for_stored_block(profile.id, &format!("sha256:{stored_hash}"));
        record_corestore_trace_event("byte_pipeline.chunk", "ok");
        let erasure_started_at = Instant::now();
        let shards = encode_erasure_shards(materialised_bytes, profile)?;
        record_byte_pipeline_stage_duration(
            "erasure_encode",
            writer_family,
            "preencoded",
            encryption_algorithm,
            profile.id,
            erasure_started_at.elapsed(),
        );
        record_corestore_trace_event("byte_pipeline.erasure_encode", "ok");
        let placement_started_at = Instant::now();
        let placements = self
            .plan_publish_shard_placements(profile, boundary_values)
            .await?;
        record_byte_pipeline_stage_duration(
            "placement",
            writer_family,
            "preencoded",
            encryption_algorithm,
            profile.id,
            placement_started_at.elapsed(),
        );
        record_corestore_trace_event("placement.plan", "ok");
        let stripe_size = shards
            .iter()
            .map(|shard| (shard.len() as u64).saturating_mul(profile.data_shards as u64))
            .max()
            .unwrap_or(0);
        let block_id_ref = block_id.as_str();
        let boundary_summary_hash_ref = boundary_summary_hash.as_str();
        let boundary_values_b64_ref = boundary_values_b64.as_str();
        let compression_algorithm = compression.algorithm.as_str();
        let mut shard_writes = FuturesUnordered::new();
        for (shard_index, shard) in shards.iter().enumerate() {
            let placement = placements.get(shard_index).ok_or_else(|| {
                anyhow!("CoreStore missing local placement for shard {shard_index}")
            })?;
            let shard_hash = format!("sha256:{}", sha256_hex(shard));
            shard_writes.push(async move {
                let written = self
                    .write_shard_to_placement(WriteShardToPlacement {
                    logical_file_id,
                    block_id: block_id_ref,
                    shard_index: shard_index as u16,
                    shard,
                    shard_hash: &shard_hash,
                    logical_offset,
                    profile,
                    placement,
                    boundary_summary_hash: boundary_summary_hash_ref,
                    boundary_values_b64: boundary_values_b64_ref,
                    mutation_id,
                    compression_algorithm,
                    encryption_algorithm,
                    writer_family,
                })
                .await
                .with_context(|| {
                    format!(
                        "write CoreStore shard logical_file_id={} block_id={} shard_index={} node_id={}",
                        logical_file_id, block_id_ref, shard_index, placement.node_id
                    )
                })?;
                Ok::<_, anyhow::Error>(written)
            });
        }
        let mut object_placements = Vec::with_capacity(shards.len());
        let mut unavailable_shards = Vec::new();
        let mut non_availability_failure = None;
        while let Some(result) = shard_writes.next().await {
            match result {
                Ok(placement) => object_placements.push(placement),
                Err(error) if is_core_store_unavailable(&error) => {
                    unavailable_shards.push(format!("{error:#}"));
                }
                Err(error) => {
                    non_availability_failure.get_or_insert(error);
                }
            }
        }
        drop(shard_writes);
        if let Some(error) = non_availability_failure {
            return Err(error);
        }
        if object_placements.len() < profile.minimum_write_ack_shards {
            return Err(CoreStoreAvailabilityError::ShardQuorumUnavailable {
                operation: "object_write",
                required: profile.minimum_write_ack_shards,
                received: object_placements.len(),
                details: unavailable_shards.join("; "),
            }
            .into());
        }
        object_placements.sort_by_key(|placement| placement.shard_index);

        let object_ref = CoreObjectRef {
            hash: object_hash.to_string(),
            logical_size: object_logical_size,
            manifest_ref: encode_manifest_ref_with_profile(
                strip_sha256_prefix(object_hash)?,
                profile.id,
            ),
            encoding: CoreObjectEncoding {
                block_id,
                profile_id: profile.id.to_string(),
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                minimum_read_shards: profile.minimum_read_shards as u16,
                minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
                stripe_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                stored_hash: format!("sha256:{stored_hash}"),
                compression,
                encryption: encryption_algorithm.to_string(),
            },
            placements: object_placements,
        };
        let manifest = CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: self.node_identity.mesh_id.clone(),
            region_id: self.node_identity.region_id.clone(),
            object_hash: object_ref.hash.clone(),
            logical_size: object_ref.logical_size,
            logical_file_id: logical_file_id.to_string(),
            logical_offset,
            writer_family: writer_family.to_string(),
            encryption_algorithm: encryption_algorithm.to_string(),
            boundary_values: boundary_values.to_vec(),
            encoding: object_ref.encoding.clone(),
            placements: object_ref.placements.clone(),
            created_at: now_rfc3339(),
            mutation_id: mutation_id.to_string(),
        };
        let manifest_key = object_manifest_meta_key(&object_ref);
        let manifest_payload = encode_object_manifest_record(&manifest)?;
        self.commit_coremeta_root_groups(
            mutation_id,
            &[CoreMetaBatchOp {
                cf: CF_OBJECT_VERSIONS,
                table_id: TABLE_OBJECT_VERSION_META_ROW,
                tuple_key: &manifest_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&manifest_payload),
            }],
            &[CoreMetaRootPublication::new(
                object_manifest_root_anchor_key(&manifest.object_hash),
                WriterFamily::ObjectBlob,
            )],
        )
        .await?;
        Ok(object_ref)
    }

    pub async fn write_logical_file(
        &self,
        request: WriteLogicalFileRequest,
    ) -> Result<CoreLogicalFileManifest> {
        Ok(self
            .write_logical_file_with_locator(request)
            .await?
            .manifest)
    }

    pub async fn write_logical_file_with_locator(
        &self,
        mut request: WriteLogicalFileRequest,
    ) -> Result<CoreLogicalFileWrite> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "write_logical_file")],
        );
        validate_writer_family(&request.writer_family, "writer family")?;
        let family = WriterFamily::from_name(&request.writer_family)
            .ok_or_else(|| anyhow!("CoreStore writer family is not registered"))?;
        if !is_canonical_logical_file_id(&request.logical_file_id) {
            request.logical_file_id = canonical_logical_file_id(
                family,
                request.generation,
                &request.logical_file_id,
                &hash32(&request.source),
            );
        }
        validate_logical_file_id(&request.logical_file_id, "logical file id")?;
        validate_logical_id(&request.mutation_id, "logical file mutation id")?;
        let profile = local_erasure_profile(&request.pipeline_policy.erasure_profile_id)?;
        validate_pipeline_policy(&request.pipeline_policy, profile)?;

        let source = std::mem::take(&mut request.source);
        let plaintext_hash = format!("sha256:{}", sha256_hex(&source));
        let plaintext_len = source.len() as u64;
        let blocks = self
            .write_logical_file_blocks(&request, source, profile)
            .await
            .with_context(|| {
                format!(
                    "write CoreStore logical file blocks {}",
                    request.logical_file_id
                )
            })?;
        let manifest = logical_file_manifest_from_object_manifests(
            &request,
            &blocks,
            plaintext_hash,
            plaintext_len,
        )?;
        let locator = self
            .publish_logical_file_manifest(&manifest, &request.pipeline_policy)
            .await
            .with_context(|| {
                format!(
                    "publish CoreStore logical file manifest {}",
                    request.logical_file_id
                )
            })?;
        Ok(CoreLogicalFileWrite { manifest, locator })
    }

    pub(super) async fn write_logical_bytes_direct(
        &self,
        writer_family: &str,
        logical_file_id: String,
        generation: u64,
        source: Vec<u8>,
        mutation_id: String,
        region_id: String,
    ) -> Result<CoreManifestLocator> {
        let policy = self.pipeline_policy_for_storage_class(None)?;
        let write = self
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: writer_family.to_string(),
                generation,
                logical_file_id,
                source,
                range_hints: Vec::new(),
                pipeline_policy: policy,
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id,
                region_id,
            })
            .await?;
        Ok(write.locator)
    }

    pub(super) async fn write_logical_file_blocks(
        &self,
        request: &WriteLogicalFileRequest,
        source: Vec<u8>,
        profile: LocalErasureProfile,
    ) -> Result<Vec<MaterializedLogicalBlock>> {
        let target_block_size = usize::try_from(effective_target_block_size(
            &request.pipeline_policy,
            profile,
        ))
        .map_err(|_| anyhow!("CoreStore target_block_size exceeds usize"))?;
        let mut blocks = Vec::new();
        for (index, (start, end)) in
            logical_block_ranges_for_source(&source, request, target_block_size)?
                .into_iter()
                .enumerate()
        {
            let logical_offset = start as u64;
            let chunk = &source[start..end];
            let chunk_bytes = chunk.to_vec();
            let chunk_hash = format!("sha256:{}", sha256_hex(&chunk_bytes));
            let compression_started_at = Instant::now();
            let (stored_chunk, compression) =
                encode_logical_file_source(&request.pipeline_policy.compression, chunk_bytes)?;
            record_byte_pipeline_stage_duration(
                "compress",
                &request.writer_family,
                &request.pipeline_policy.compression,
                &request.pipeline_policy.encryption,
                profile.id,
                compression_started_at.elapsed(),
            );
            crate::perf::record_compression_ratio(
                &request.writer_family,
                &compression.algorithm,
                profile.id,
                chunk.len() as u64,
                stored_chunk.len() as u64,
            );
            if compression.algorithm != "none" {
                record_corestore_trace_event("byte_pipeline.compress", "ok");
            }
            let block_plain_hash = format!("sha256:{}", sha256_hex(&stored_chunk));
            let encryption_started_at = Instant::now();
            let pipeline_block = self.encrypt_pipeline_block(
                &request.pipeline_policy,
                &request.logical_file_id,
                index,
                logical_offset,
                chunk.len() as u64,
                &block_plain_hash,
                stored_chunk,
            )?;
            record_byte_pipeline_stage_duration(
                "encrypt",
                &request.writer_family,
                &request.pipeline_policy.compression,
                &pipeline_block.encryption.algorithm,
                profile.id,
                encryption_started_at.elapsed(),
            );
            record_corestore_trace_event("byte_pipeline.encrypt", "ok");
            let object_ref = self
                .put_logical_file_block_with_profile(
                    request,
                    index,
                    logical_offset,
                    pipeline_block.stored,
                    block_plain_hash,
                    pipeline_block.encryption.algorithm.clone(),
                    profile,
                )
                .await
                .with_context(|| {
                    format!(
                        "put CoreStore logical-file block logical_file_id={} block_index={}",
                        request.logical_file_id, index
                    )
                })?;
            let object_manifest = self.read_object_manifest(&object_ref).await.with_context(|| {
                format!(
                    "read object manifest for CoreStore logical-file block logical_file_id={} block_index={} manifest_ref={}",
                    request.logical_file_id, index, object_ref.manifest_ref
                )
            })?;
            blocks.push(MaterializedLogicalBlock {
                object_manifest,
                logical_offset,
                logical_length: chunk.len() as u64,
                compressed_length: compression.compressed_length,
                plaintext_hash: chunk_hash,
                compression,
                encryption: pipeline_block.encryption,
            });
        }
        Ok(blocks)
    }

    pub async fn write_logical_file_ref(
        &self,
        request: WriteLogicalFileRequest,
    ) -> Result<CoreObjectRef> {
        let write = self.write_logical_file_with_locator(request).await?;
        Ok(core_object_ref_from_logical_file_write(&write))
    }

    pub(super) async fn publish_logical_file_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
        policy: &CorePipelinePolicy,
    ) -> Result<CoreManifestLocator> {
        validate_logical_file_manifest_shape(manifest)?;
        let manifest_bytes = encode_logical_file_manifest_bytes(manifest)?;
        let manifest_hash = format!("sha256:{}", sha256_hex(&manifest_bytes));
        let manifest_logical_file_id = canonical_logical_file_id(
            WriterFamily::CoreControl,
            manifest.writer_generation,
            &format!("manifest:{}", manifest.logical_file_id),
            manifest_hash.as_bytes(),
        );
        let profile = local_erasure_profile(&policy.erasure_profile_id)?;
        let manifest_block_ref = self
            .put_blob_with_profile_and_encoding(
                PutBlob {
                    logical_name: manifest_logical_file_id,
                    bytes: manifest_bytes,
                    boundary_values: manifest_boundary_values(manifest),
                    region_id: "local".to_string(),
                    mutation_id: format!(
                        "manifest_{}",
                        sha256_hex(manifest.created_by_mutation_id.as_bytes())
                    ),
                },
                profile,
                "none",
                WriterFamily::CoreControl.as_str(),
            )
            .await?;
        record_corestore_trace_event("manifest.publish", "ok");
        manifest_locator_from_manifest_and_ref(manifest, &manifest_block_ref, &manifest_hash)
    }

    pub(super) async fn publish_inline_manifest_body(
        &self,
        writer_family: &str,
        logical_file_id: String,
        writer_generation: u64,
        body: Vec<u8>,
    ) -> Result<CoreManifestLocator> {
        let (locator, op) = self.prepare_inline_manifest_body(
            writer_family,
            logical_file_id,
            writer_generation,
            body,
        )?;
        let owned_ops = [op];
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        // The manifest hash is the immutable identity. It is not an independently
        // versioned root and therefore must not consume an arbitrary writer
        // generation as its first root generation.
        self.meta.write_local_committed_batch(&ops)?;
        Ok(locator)
    }

    pub(super) fn prepare_inline_manifest_body(
        &self,
        writer_family: &str,
        logical_file_id: String,
        writer_generation: u64,
        body: Vec<u8>,
    ) -> Result<(CoreManifestLocator, OwnedCoreMetaBatchOp)> {
        let locator = inline_manifest_locator_from_body(
            logical_file_id,
            writer_family.to_string(),
            writer_generation,
            &body,
        )?;
        let row = CoreInlineManifestBodyRow {
            schema: CORE_INLINE_MANIFEST_BODY_SCHEMA.to_string(),
            logical_file_id: locator.manifest_ref.logical_file_id.clone(),
            writer_family: locator.manifest_ref.writer_family.clone(),
            writer_generation: locator.manifest_ref.writer_generation,
            manifest_hash: locator.manifest_hash.clone(),
            manifest_encoding: locator.manifest_encoding.clone(),
            manifest_length: locator.manifest_length,
            body,
        };
        let key = inline_manifest_body_key(&locator.manifest_hash)?;
        let payload = encode_inline_manifest_body_row(&row)?;
        Ok((
            locator,
            OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_INLINE_MANIFEST_BODY_ROW,
                tuple_key: key,
                payload,
                common: None,
            },
        ))
    }

    pub(super) fn encrypt_pipeline_block(
        &self,
        policy: &CorePipelinePolicy,
        logical_file_id: &str,
        _block_index: usize,
        logical_offset: u64,
        logical_length: u64,
        plaintext_hash: &str,
        plaintext: Vec<u8>,
    ) -> Result<PipelineBlockBytes> {
        match policy.encryption.as_str() {
            "none" => {
                let ciphertext_hash = format!("sha256:{}", sha256_hex(&plaintext));
                Ok(PipelineBlockBytes {
                    stored: plaintext,
                    encryption: none_encryption_descriptor(plaintext_hash, &ciphertext_hash),
                })
            }
            "aes_gcm_siv" => {
                let keyring = self.pipeline_keyring.as_ref().ok_or_else(|| {
                    anyhow!(
                        "CoreStore aes_gcm_siv pipeline encryption requires a configured keyring"
                    )
                })?;
                let cipher = <Aes256GcmSiv as aes_gcm_siv::aead::KeyInit>::new_from_slice(
                    keyring.active_key()?,
                )
                .map_err(|err| anyhow!(err.to_string()))?;
                let nonce = Aes256GcmSiv::generate_nonce(&mut OsRng);
                let aad = pipeline_block_aad(
                    logical_file_id,
                    logical_offset,
                    logical_length,
                    plaintext_hash,
                );
                let ciphertext = cipher
                    .encrypt(
                        &nonce,
                        Payload {
                            msg: &plaintext,
                            aad: &aad,
                        },
                    )
                    .map_err(|err| anyhow!(err.to_string()))?;
                let aad_hash = format!("sha256:{}", sha256_hex(&aad));
                let ciphertext_hash = format!("sha256:{}", sha256_hex(&ciphertext));
                #[allow(deprecated)]
                let nonce_bytes = nonce.as_slice().to_vec();
                let descriptor_hash = encryption_descriptor_hash(
                    "aes_gcm_siv",
                    keyring.active_key_id(),
                    &nonce_bytes,
                    &aad_hash,
                    plaintext_hash,
                    &ciphertext_hash,
                );
                Ok(PipelineBlockBytes {
                    stored: ciphertext,
                    encryption: CoreEncryptionDescriptor {
                        algorithm: "aes_gcm_siv".to_string(),
                        key_id: keyring.active_key_id().to_string(),
                        nonce: nonce_bytes,
                        aad_hash,
                        plaintext_hash: plaintext_hash.to_string(),
                        ciphertext_hash,
                        descriptor_hash,
                    },
                })
            }
            other => bail!("CoreStore unsupported logical file encryption policy {other}"),
        }
    }

    pub(super) fn decrypt_pipeline_block(
        &self,
        logical_file_id: &str,
        block: &CoreLogicalBlockRef,
        stored: Vec<u8>,
    ) -> Result<Vec<u8>> {
        match block.encryption.algorithm.as_str() {
            "none" => {
                let actual_hash = format!("sha256:{}", sha256_hex(&stored));
                if actual_hash != block.encryption.ciphertext_hash {
                    bail!(
                        "CoreStore unencrypted block hash mismatch: expected {}, got {}",
                        block.encryption.ciphertext_hash,
                        actual_hash
                    );
                }
                Ok(stored)
            }
            "aes_gcm_siv" => {
                if block.encryption.nonce.len() != CORE_PIPELINE_NONCE_LEN {
                    bail!("CoreStore aes_gcm_siv block nonce has invalid length");
                }
                let keyring = self.pipeline_keyring.as_ref().ok_or_else(|| {
                    anyhow!(
                        "CoreStore aes_gcm_siv pipeline decryption requires a configured keyring"
                    )
                })?;
                let actual_ciphertext_hash = format!("sha256:{}", sha256_hex(&stored));
                if actual_ciphertext_hash != block.encryption.ciphertext_hash {
                    bail!(
                        "CoreStore encrypted block ciphertext hash mismatch: expected {}, got {}",
                        block.encryption.ciphertext_hash,
                        actual_ciphertext_hash
                    );
                }
                let aad = pipeline_block_aad(
                    logical_file_id,
                    block.logical_offset,
                    block.logical_length,
                    &block.encryption.plaintext_hash,
                );
                let aad_hash = format!("sha256:{}", sha256_hex(&aad));
                if aad_hash != block.encryption.aad_hash {
                    bail!("CoreStore encrypted block AAD hash mismatch");
                }
                let expected_descriptor_hash = encryption_descriptor_hash(
                    "aes_gcm_siv",
                    &block.encryption.key_id,
                    &block.encryption.nonce,
                    &block.encryption.aad_hash,
                    &block.encryption.plaintext_hash,
                    &block.encryption.ciphertext_hash,
                );
                if expected_descriptor_hash != block.encryption.descriptor_hash {
                    bail!("CoreStore encrypted block descriptor hash mismatch");
                }
                let cipher = <Aes256GcmSiv as aes_gcm_siv::aead::KeyInit>::new_from_slice(
                    keyring.key(&block.encryption.key_id)?,
                )
                .map_err(|err| anyhow!(err.to_string()))?;
                #[allow(deprecated)]
                let nonce = Nonce::from_slice(&block.encryption.nonce);
                let plaintext = cipher
                    .decrypt(
                        nonce,
                        Payload {
                            msg: &stored,
                            aad: &aad,
                        },
                    )
                    .map_err(|err| anyhow!(err.to_string()))?;
                let plaintext_hash = format!("sha256:{}", sha256_hex(&plaintext));
                if plaintext_hash != block.encryption.plaintext_hash {
                    bail!(
                        "CoreStore encrypted block plaintext hash mismatch: expected {}, got {}",
                        block.encryption.plaintext_hash,
                        plaintext_hash
                    );
                }
                Ok(plaintext)
            }
            other => bail!("CoreStore unsupported logical file encryption descriptor {other}"),
        }
    }

    pub(super) async fn write_control_logical_file_ref(
        &self,
        writer_family: &str,
        generation: u64,
        logical_file_id: String,
        bytes: Vec<u8>,
        mutation_id: String,
        region_id: String,
    ) -> Result<CoreObjectRef> {
        let family = WriterFamily::from_name(writer_family).ok_or_else(|| {
            anyhow!("unsupported control logical file writer family {writer_family}")
        })?;
        let canonical_logical_file_id =
            canonical_logical_file_id(family, generation, &logical_file_id, &hash32(&bytes));
        self.write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: writer_family.to_string(),
            generation,
            logical_file_id: canonical_logical_file_id,
            source: bytes,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id,
            region_id,
        })
        .await
    }

    pub async fn verify_logical_file_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> Result<CoreLogicalFileVerificationReport> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "verify_logical_file_manifest")],
        );
        let _plaintext = self.read_logical_file_plaintext(manifest).await?;
        Ok(CoreLogicalFileVerificationReport {
            verified: true,
            logical_file_id: manifest.logical_file_id.clone(),
            checked_blocks: manifest.blocks.len() as u64,
            checked_shards: manifest
                .blocks
                .iter()
                .map(|block| block.shards.len() as u64)
                .sum(),
            content_hash: manifest.content_hash.clone(),
        })
    }
}

fn object_manifest_root_anchor_key(object_hash: &str) -> String {
    format!("object-manifest/{object_hash}")
}

fn put_inline_payload_row(raw_payload: &[u8], common: CoreMetaRowCommonProto) -> Result<Vec<u8>> {
    encode_core_meta_inline_payload_row(raw_payload, common)
}
