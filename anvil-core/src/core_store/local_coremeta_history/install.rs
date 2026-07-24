use super::*;
use prost::Message;
use std::collections::BTreeSet;

const INSTALL_DESCRIPTOR_KIND: &str = "descriptor";
const INSTALL_MUTATION_KIND: &str = "mutation";
const INSTALL_CHUNK_KIND: &str = "mutation-chunk";
const INSTALL_MUTATION_KEY_KIND: &str = "mutation";
const INSTALL_CHUNK_KEY_KIND: &str = "mutation-chunk";
const INSTALL_MAX_MUTATION_CHUNKS: usize = 8;
const INSTALL_MAX_MUTATION_BYTES: usize =
    crate::core_store::meta::CORE_META_MAX_VALUE_BYTES + (16 * 1024);

#[derive(Clone, PartialEq, Message)]
struct StoredGenerationInstallProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    descriptor: Option<CoreMetaGenerationDescriptor>,
    #[prost(string, tag = "10")]
    kind: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredGenerationInstallMutationProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    generation: u64,
    #[prost(uint64, tag = "5")]
    ordinal: u64,
    #[prost(string, tag = "6")]
    mutation_hash: String,
    #[prost(uint64, tag = "7")]
    encoded_length: u64,
    #[prost(uint32, tag = "8")]
    chunk_count: u32,
    #[prost(string, tag = "10")]
    kind: String,
}

#[derive(Clone, PartialEq, Message)]
struct StoredGenerationInstallChunkProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    generation: u64,
    #[prost(uint64, tag = "5")]
    mutation_ordinal: u64,
    #[prost(uint32, tag = "6")]
    chunk_ordinal: u32,
    #[prost(uint32, tag = "7")]
    chunk_count: u32,
    #[prost(string, tag = "8")]
    mutation_hash: String,
    #[prost(bytes, tag = "9")]
    chunk_bytes: Vec<u8>,
    #[prost(string, tag = "10")]
    kind: String,
}

#[derive(Clone, PartialEq, Message)]
struct GenerationInstallSchemaProbe {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "10")]
    kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoreMetaGenerationInstallOutcome {
    StagedPartial {
        root_key_hash: String,
        generation: u64,
    },
    StagedComplete {
        root_key_hash: String,
        generation: u64,
        coordinator_root_key_hash: Option<String>,
        coordinator_root_generation: Option<u64>,
    },
    Published {
        root_key_hash: String,
        generation: u64,
    },
}

impl CoreStore {
    pub(in crate::core_store::local) fn staged_coremeta_recovery_root_hashes(
        &self,
    ) -> Result<BTreeSet<String>> {
        let mut roots = BTreeSet::new();
        let mut after = None;
        loop {
            let page = self.meta.scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_INSTALL_ROW,
                &[],
                after.as_deref(),
                CORE_META_MAX_SCAN_PAGE_ROWS,
            )?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                let probe = GenerationInstallSchemaProbe::decode(record.payload.as_slice())
                    .context("decode CoreMeta staged recovery row schema")?;
                if probe.schema != GENERATION_INSTALL_SCHEMA {
                    bail!("CoreMeta staged recovery row has an unknown schema");
                }
                if probe.kind == INSTALL_DESCRIPTOR_KIND {
                    roots.insert(decode_generation_install(&record.payload)?.root_key_hash);
                }
            }
            after = page
                .last()
                .map(|record| core_meta_record_tuple_key(&record.key).map(ToOwned::to_owned))
                .transpose()?;
            if page.len() < CORE_META_MAX_SCAN_PAGE_ROWS {
                break;
            }
        }
        Ok(roots)
    }

    /// Durably stages a bounded catch-up frame. Neither canonical rows nor the
    /// immutable history descriptor are written here. Root publication/CAS is
    /// the only transition that can make this generation visible.
    pub(crate) async fn install_coremeta_generation_frame(
        &self,
        frame: &CoreMetaBatchFrame,
    ) -> Result<CoreMetaGenerationInstallOutcome> {
        self.install_coremeta_generation_frame_with_authority(frame, None)
            .await
    }

    /// Installs a recovery frame selected by already-committed root authority.
    /// A conflicting unpublished candidate may be discarded, but immutable
    /// history is never replaced.
    pub(in crate::core_store::local) async fn install_committed_coremeta_generation_frame(
        &self,
        frame: &CoreMetaBatchFrame,
        committed_certificate_hash: &str,
        committed_publication_bundle: Option<&[u8]>,
    ) -> Result<CoreMetaGenerationInstallOutcome> {
        self.install_coremeta_generation_frame_with_authority(
            frame,
            Some((committed_certificate_hash, committed_publication_bundle)),
        )
        .await
    }

    async fn install_coremeta_generation_frame_with_authority(
        &self,
        frame: &CoreMetaBatchFrame,
        committed: Option<(&str, Option<&[u8]>)>,
    ) -> Result<CoreMetaGenerationInstallOutcome> {
        let descriptor = validate_catch_up_frame(frame)?;
        self.validate_install_descriptor_commit_evidence(descriptor)?;
        if let Some((certificate_hash, publication_bundle)) = committed {
            validate_committed_generation_descriptor(
                descriptor,
                certificate_hash,
                publication_bundle,
            )?;
        }
        if frame.mutations.len() as u64 == descriptor.mutation_count
            && frame.mutations.first().map(|mutation| mutation.ordinal) == Some(0)
        {
            verify_complete_generation(descriptor, &frame.mutations)?;
        }

        let lock_id = format!("{}:{}", descriptor.root_key_hash, descriptor.generation);
        let _guard = self
            .acquire_named_lock("coremeta-history-install", &lock_id)
            .await?;

        if let Some(published) =
            self.read_generation_descriptor(&descriptor.root_key_hash, descriptor.generation)?
        {
            if published != *descriptor {
                bail!("CoreMeta catch-up conflicts with a published immutable generation");
            }
            if !self.root_generation_is_published(
                &descriptor.root_key_hash,
                descriptor.generation,
                &descriptor.transaction_id,
            )? {
                bail!("CoreMeta immutable history exists without its matching root publication");
            }
            self.cleanup_published_generation_staging(descriptor)?;
            return Ok(published_outcome(descriptor));
        }

        if let Some((certificate_hash, publication_bundle)) = committed
            && let Some(staged) = self.read_generation_install_descriptor(
                &descriptor.root_key_hash,
                descriptor.generation,
            )?
            && validate_committed_generation_descriptor(
                &staged,
                certificate_hash,
                publication_bundle,
            )
            .is_err()
        {
            self.delete_staged_generation(&staged)?;
        }

        self.stage_generation_frame(descriptor, &frame.mutations)?;
        let Some(mutations) = self.try_read_complete_staged_generation(descriptor)? else {
            return Ok(CoreMetaGenerationInstallOutcome::StagedPartial {
                root_key_hash: descriptor.root_key_hash.clone(),
                generation: descriptor.generation,
            });
        };
        if let Err(error) = verify_complete_generation(descriptor, &mutations)
            .and_then(|()| self.validate_install_descriptor_commit_evidence(descriptor))
            .and_then(|()| self.validate_generation_install_predecessor(descriptor))
        {
            self.delete_staged_generation(descriptor)?;
            return Err(error);
        }

        Ok(staged_complete_outcome(descriptor))
    }

    /// Verifies that a complete staged group exactly matches one durable root
    /// publication intent. This method never publishes data. The caller must
    /// subsequently install the root evidence and execute the existing root
    /// CAS; retrying this validation after CAS reports `Published`.
    pub(crate) async fn validate_staged_coremeta_generation_group_for_publication(
        &self,
        scopes: &[(String, u64)],
    ) -> Result<Vec<CoreMetaGenerationInstallOutcome>> {
        if scopes.is_empty() {
            bail!("CoreMeta generation publication group must not be empty");
        }
        let mut ordered_scopes = scopes.to_vec();
        ordered_scopes.sort();
        if ordered_scopes.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("CoreMeta generation publication group contains duplicate scopes");
        }

        let mut guards = Vec::with_capacity(ordered_scopes.len());
        for (root_key_hash, generation) in &ordered_scopes {
            validate_history_root(root_key_hash)?;
            guards.push(
                self.acquire_named_lock(
                    "coremeta-history-install",
                    &format!("{root_key_hash}:{generation}"),
                )
                .await?,
            );
        }

        let result = self.validate_staged_generation_group_locked(&ordered_scopes);
        drop(guards);
        result
    }

    fn stage_generation_frame(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
        mutations: &[CoreMetaGenerationMutation],
    ) -> Result<()> {
        let mut inputs = Vec::new();
        match self
            .read_generation_install_descriptor(&descriptor.root_key_hash, descriptor.generation)?
        {
            Some(existing) if existing != *descriptor => {
                bail!("CoreMeta catch-up frame conflicts with its staged descriptor");
            }
            Some(_) => {}
            None => inputs.push(generation_install_input(descriptor)?),
        }

        for mutation in mutations {
            if let Some(existing) = self.read_staged_generation_mutation(
                &mutation.root_key_hash,
                mutation.generation,
                mutation.ordinal,
            )? {
                if existing != *mutation {
                    bail!("CoreMeta catch-up mutation conflicts with its staged value");
                }
                continue;
            }
            self.ensure_no_orphaned_install_chunks(
                &mutation.root_key_hash,
                mutation.generation,
                mutation.ordinal,
            )?;
            inputs.extend(generation_install_mutation_inputs(descriptor, mutation)?);
        }
        if inputs.is_empty() {
            return Ok(());
        }
        let rows = encode_history_rows(&self.meta, inputs)?;
        self.write_owned_install_rows(&rows)
    }

    fn validate_staged_generation_group_locked(
        &self,
        scopes: &[(String, u64)],
    ) -> Result<Vec<CoreMetaGenerationInstallOutcome>> {
        let mut generations = Vec::with_capacity(scopes.len());
        let mut published_count = 0usize;
        for (root_key_hash, generation) in scopes {
            if let Some(descriptor) = self.read_generation_descriptor(root_key_hash, *generation)? {
                self.validate_install_descriptor_commit_evidence(&descriptor)?;
                if !self.root_generation_is_published(
                    root_key_hash,
                    *generation,
                    &descriptor.transaction_id,
                )? {
                    bail!(
                        "CoreMeta immutable history exists without its matching root publication"
                    );
                }
                let mutations = self.read_complete_generation_mutations(&descriptor)?;
                verify_complete_generation(&descriptor, &mutations)?;
                generations.push((descriptor, mutations));
                published_count += 1;
                continue;
            }

            let descriptor = self
                .read_generation_install_descriptor(root_key_hash, *generation)?
                .ok_or_else(|| anyhow!("CoreMeta staged generation descriptor is missing"))?;
            self.validate_install_descriptor_commit_evidence(&descriptor)?;
            let mutations = self
                .try_read_complete_staged_generation(&descriptor)?
                .ok_or_else(|| anyhow!("CoreMeta staged generation is incomplete"))?;
            verify_complete_generation(&descriptor, &mutations)?;
            self.validate_generation_install_predecessor(&descriptor)?;
            generations.push((descriptor, mutations));
        }

        if published_count != 0 && published_count != generations.len() {
            bail!("CoreMeta generation group is only partially published");
        }
        validate_install_group(&generations)?;

        if published_count == generations.len() {
            let outcomes = generations
                .iter()
                .map(|(descriptor, _)| published_outcome(descriptor))
                .collect::<Vec<_>>();
            for (descriptor, _) in &generations {
                self.cleanup_published_generation_staging(descriptor)?;
            }
            return Ok(outcomes);
        }

        self.validate_generation_group_against_publication_intent(&generations)?;
        Ok(generations
            .iter()
            .map(|(descriptor, _)| staged_complete_outcome(descriptor))
            .collect())
    }

    fn validate_generation_group_against_publication_intent(
        &self,
        generations: &[(
            CoreMetaGenerationDescriptor,
            Vec<CoreMetaGenerationMutation>,
        )],
    ) -> Result<()> {
        let transaction_id = generations
            .first()
            .map(|(descriptor, _)| descriptor.transaction_id.as_str())
            .ok_or_else(|| anyhow!("CoreMeta generation publication group is empty"))?;
        if generations
            .iter()
            .any(|(descriptor, _)| descriptor.transaction_id != transaction_id)
        {
            bail!("CoreMeta generation publication group spans multiple transactions");
        }
        let intent = self
            .read_root_publication_intent(transaction_id)?
            .ok_or_else(|| anyhow!("CoreMeta generation has no durable root publication intent"))?;
        if intent.roots.len() != generations.len() {
            bail!("CoreMeta generation group does not contain every publication participant");
        }
        let coordinator = intent.coordinator_scope()?;
        let expected_publication_bundle =
            self.encode_coremeta_recovery_publication_bundle(&intent)?;
        let by_root = intent
            .roots
            .iter()
            .map(|root| (root.publication.descriptor.root_key_hash(), root))
            .collect::<BTreeMap<_, _>>();
        for (descriptor, mutations) in generations {
            let root = by_root.get(&descriptor.root_key_hash).ok_or_else(|| {
                anyhow!("CoreMeta staged generation is not in its publication intent")
            })?;
            let descriptor_coordinator = descriptor
                .coordinator_root_key_hash
                .clone()
                .zip(descriptor.coordinator_root_generation);
            if descriptor_coordinator != coordinator
                || descriptor.generation != root.publication.post_root_generation
                || descriptor.created_at_unix_nanos != intent.created_at_unix_nanos
                || descriptor.publication_bundle != expected_publication_bundle
            {
                bail!("CoreMeta staged generation publication scope mismatch");
            }
            let certificate =
                decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
                    &descriptor.commit_certificate,
                    "CoreMeta staged generation commit certificate",
                )?;
            if certificate.expected_root_generation != root.expected_root_generation
                || root
                    .certificate_hash
                    .as_ref()
                    .is_some_and(|hash| hash != &descriptor.certificate_hash)
            {
                bail!("CoreMeta staged generation certificate does not match its intent");
            }
            self.validate_generation_rows_match_intent(descriptor, mutations, &root.rows)?;
        }
        Ok(())
    }

    fn validate_generation_rows_match_intent(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
        mutations: &[CoreMetaGenerationMutation],
        intent_rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<()> {
        let mut candidate = self.coremeta_generation_mutations_as_owned(descriptor, mutations)?;
        sort_owned_rows(&mut candidate);
        let mut expected = intent_rows.to_vec();
        sort_owned_rows(&mut expected);
        if !same_physical_rows(&candidate, &expected) {
            bail!("CoreMeta staged generation rows do not match its publication intent");
        }
        Ok(())
    }

    pub(in crate::core_store::local) fn coremeta_generation_mutations_as_owned(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
        mutations: &[CoreMetaGenerationMutation],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let api_rows = mutations
            .iter()
            .map(|mutation| {
                mutation
                    .mutation
                    .as_ref()
                    .ok_or_else(|| anyhow!("CoreMeta staged generation mutation has no row"))
            })
            .collect::<Result<Vec<_>>>()?;
        let borrowed = api_rows
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.column_family.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        let delete_common = history_common(descriptor);
        let candidate =
            self.validate_and_own_coremeta_encoded_rows(&borrowed, Some(&delete_common))?;
        if candidate.iter().any(|row| {
            row.root_key_hash != descriptor.root_key_hash
                || row.root_generation != descriptor.generation
                || row.visibility_state != CoreMetaVisibilityState::Committed
                || is_coremeta_history_table(&row.core_meta_key)
        }) {
            bail!("CoreMeta staged generation contains an invalid canonical row scope");
        }
        Ok(candidate)
    }

    pub(in crate::core_store::local) fn read_complete_coremeta_generation_for_recovery(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<PreparedGenerationHistory>> {
        if let Some(descriptor) = self.read_generation_descriptor(root_key_hash, generation)? {
            let mutations = self.read_complete_generation_mutations(&descriptor)?;
            verify_complete_generation(&descriptor, &mutations)?;
            return Ok(Some(PreparedGenerationHistory {
                descriptor,
                mutations,
            }));
        }
        let Some(descriptor) =
            self.read_generation_install_descriptor(root_key_hash, generation)?
        else {
            return Ok(None);
        };
        let Some(mutations) = self.try_read_complete_staged_generation(&descriptor)? else {
            return Ok(None);
        };
        verify_complete_generation(&descriptor, &mutations)?;
        self.validate_install_descriptor_commit_evidence(&descriptor)?;
        Ok(Some(PreparedGenerationHistory {
            descriptor,
            mutations,
        }))
    }

    fn validate_install_descriptor_commit_evidence(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
    ) -> Result<()> {
        self.validate_descriptor_commit_evidence(descriptor)?;
        for evidence in &descriptor.certificate_persist_evidence {
            let api =
                decode_deterministic_proto::<crate::anvil_api::CoreMetaCertificatePersistReceipt>(
                    &evidence.evidence,
                    "CoreMeta generation install certificate persistence receipt",
                )?;
            let receipt = super::super::local_coremeta_quorum::api_persist_receipt_to_core(api)?;
            if receipt.committed_batch_hash != descriptor.committed_batch_hash {
                bail!("CoreMeta generation install evidence committed-batch hash mismatch");
            }
        }
        Ok(())
    }

    fn validate_generation_install_predecessor(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
    ) -> Result<()> {
        let certificate_api =
            decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
                &descriptor.commit_certificate,
                "CoreMeta generation predecessor certificate",
            )?;
        let latest = self
            .generation_history_bounds(&descriptor.root_key_hash)?
            .map_or(0, |(_, latest)| latest);
        if latest != certificate_api.expected_root_generation
            || latest.saturating_add(1) != descriptor.generation
        {
            bail!("CoreMeta generation install predecessor mismatch");
        }
        Ok(())
    }

    fn read_generation_install_descriptor(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<CoreMetaGenerationDescriptor>> {
        let key = generation_descriptor_key(root_key_hash, generation)?;
        self.meta
            .get(CF_TRANSACTIONS, TABLE_COREMETA_GENERATION_INSTALL_ROW, &key)?
            .map(|payload| decode_generation_install(&payload))
            .transpose()
    }

    fn read_staged_generation_mutation(
        &self,
        root_key_hash: &str,
        generation: u64,
        ordinal: u64,
    ) -> Result<Option<CoreMetaGenerationMutation>> {
        let key = generation_install_mutation_key(root_key_hash, generation, ordinal)?;
        let Some(payload) =
            self.meta
                .get(CF_TRANSACTIONS, TABLE_COREMETA_GENERATION_INSTALL_ROW, &key)?
        else {
            return Ok(None);
        };
        let header = decode_generation_install_mutation(&payload)?;
        let mut encoded = Vec::with_capacity(
            usize::try_from(header.encoded_length)
                .context("CoreMeta staged mutation length exceeds usize")?,
        );
        for chunk_ordinal in 0..header.chunk_count {
            let chunk_key =
                generation_install_chunk_key(root_key_hash, generation, ordinal, chunk_ordinal)?;
            let chunk_payload = self
                .meta
                .get(
                    CF_TRANSACTIONS,
                    TABLE_COREMETA_GENERATION_INSTALL_ROW,
                    &chunk_key,
                )?
                .ok_or_else(|| anyhow!("CoreMeta staged mutation chunk is missing"))?;
            let chunk = decode_generation_install_chunk(&chunk_payload)?;
            validate_install_chunk_for_mutation(&chunk, &header, chunk_ordinal)?;
            encoded.extend_from_slice(&chunk.chunk_bytes);
        }
        if encoded.len() as u64 != header.encoded_length
            || digest_bytes(&encoded) != header.mutation_hash
        {
            bail!("CoreMeta staged mutation payload identity mismatch");
        }
        let mutation = decode_deterministic_proto::<CoreMetaGenerationMutation>(
            &encoded,
            "CoreMeta staged generation mutation",
        )?;
        if mutation.root_key_hash != root_key_hash
            || mutation.generation != generation
            || mutation.ordinal != ordinal
        {
            bail!("CoreMeta staged mutation scope mismatch");
        }
        validate_api_mutation(
            mutation
                .mutation
                .as_ref()
                .ok_or_else(|| anyhow!("CoreMeta staged mutation has no row"))?,
        )?;
        Ok(Some(mutation))
    }

    fn try_read_complete_staged_generation(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
    ) -> Result<Option<Vec<CoreMetaGenerationMutation>>> {
        let count = usize::try_from(descriptor.mutation_count)
            .context("CoreMeta generation mutation count exceeds usize")?;
        if count == 0 || count > HISTORY_MAX_GENERATION_MUTATIONS {
            bail!("CoreMeta generation mutation count is outside supported bounds");
        }
        let mut mutations = Vec::with_capacity(count);
        for ordinal in 0..descriptor.mutation_count {
            let Some(mutation) = self.read_staged_generation_mutation(
                &descriptor.root_key_hash,
                descriptor.generation,
                ordinal,
            )?
            else {
                return Ok(None);
            };
            mutations.push(mutation);
        }
        Ok(Some(mutations))
    }

    fn ensure_no_orphaned_install_chunks(
        &self,
        root_key_hash: &str,
        generation: u64,
        ordinal: u64,
    ) -> Result<()> {
        let prefix = generation_install_chunk_prefix(root_key_hash, generation, ordinal)?;
        if !self
            .meta
            .scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_INSTALL_ROW,
                &prefix,
                None,
                1,
            )?
            .is_empty()
        {
            bail!("CoreMeta staged mutation has orphaned chunks");
        }
        Ok(())
    }

    fn cleanup_published_generation_staging(
        &self,
        published: &CoreMetaGenerationDescriptor,
    ) -> Result<()> {
        let Some(staged) = self
            .read_generation_install_descriptor(&published.root_key_hash, published.generation)?
        else {
            return Ok(());
        };
        if staged != *published {
            bail!("CoreMeta published generation conflicts with retained staging state");
        }
        self.delete_staged_generation(&staged)
    }

    fn delete_staged_generation(&self, descriptor: &CoreMetaGenerationDescriptor) -> Result<()> {
        let common = install_common(descriptor);
        let mut inputs = Vec::new();
        for ordinal in 0..descriptor.mutation_count {
            let header_key = generation_install_mutation_key(
                &descriptor.root_key_hash,
                descriptor.generation,
                ordinal,
            )?;
            if let Some(payload) = self.meta.get(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_INSTALL_ROW,
                &header_key,
            )? {
                let header = decode_generation_install_mutation(&payload)?;
                for chunk_ordinal in 0..header.chunk_count {
                    inputs.push(HistoryRowInput {
                        table_id: TABLE_COREMETA_GENERATION_INSTALL_ROW,
                        tuple_key: generation_install_chunk_key(
                            &descriptor.root_key_hash,
                            descriptor.generation,
                            ordinal,
                            chunk_ordinal,
                        )?,
                        kind: HistoryRowKind::Delete(common.clone()),
                    });
                }
                inputs.push(HistoryRowInput {
                    table_id: TABLE_COREMETA_GENERATION_INSTALL_ROW,
                    tuple_key: header_key,
                    kind: HistoryRowKind::Delete(common.clone()),
                });
            }
        }
        inputs.push(HistoryRowInput {
            table_id: TABLE_COREMETA_GENERATION_INSTALL_ROW,
            tuple_key: generation_descriptor_key(&descriptor.root_key_hash, descriptor.generation)?,
            kind: HistoryRowKind::Delete(common),
        });
        let rows = encode_history_rows(&self.meta, inputs)?;
        self.write_owned_install_rows(&rows)
    }

    fn write_owned_install_rows(&self, rows: &[CoreMetaEncodedOwnedRow]) -> Result<()> {
        if rows.iter().any(|row| {
            row.cf != CF_TRANSACTIONS
                || core_meta_record_table_id(&row.core_meta_key).map_or(true, |table_id| {
                    table_id != TABLE_COREMETA_GENERATION_INSTALL_ROW
                })
        }) {
            bail!("CoreMeta catch-up staging attempted to write outside its install table");
        }
        let borrowed = rows
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        self.write_coremeta_encoded_rows(&borrowed)
    }
}

fn generation_install_input(descriptor: &CoreMetaGenerationDescriptor) -> Result<HistoryRowInput> {
    let row = StoredGenerationInstallProto {
        common: Some(install_common(descriptor)),
        schema: GENERATION_INSTALL_SCHEMA.to_string(),
        descriptor: Some(descriptor.clone()),
        kind: INSTALL_DESCRIPTOR_KIND.to_string(),
    };
    Ok(HistoryRowInput {
        table_id: TABLE_COREMETA_GENERATION_INSTALL_ROW,
        tuple_key: generation_descriptor_key(&descriptor.root_key_hash, descriptor.generation)?,
        kind: HistoryRowKind::Put(encode_deterministic_proto(&row)),
    })
}

fn generation_install_mutation_inputs(
    descriptor: &CoreMetaGenerationDescriptor,
    mutation: &CoreMetaGenerationMutation,
) -> Result<Vec<HistoryRowInput>> {
    let encoded = encode_deterministic_proto(mutation);
    if encoded.is_empty() || encoded.len() > INSTALL_MAX_MUTATION_BYTES {
        bail!("CoreMeta staged mutation size is outside supported bounds");
    }
    let chunks = encoded
        .chunks(HISTORY_ENVELOPE_CHUNK_BYTES)
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if chunks.is_empty() || chunks.len() > INSTALL_MAX_MUTATION_CHUNKS {
        bail!("CoreMeta staged mutation chunk count is outside supported bounds");
    }
    let mutation_hash = digest_bytes(&encoded);
    let chunk_count = chunks.len() as u32;
    let common = install_common(descriptor);
    let header = StoredGenerationInstallMutationProto {
        common: Some(common.clone()),
        schema: GENERATION_INSTALL_SCHEMA.to_string(),
        root_key_hash: mutation.root_key_hash.clone(),
        generation: mutation.generation,
        ordinal: mutation.ordinal,
        mutation_hash: mutation_hash.clone(),
        encoded_length: encoded.len() as u64,
        chunk_count,
        kind: INSTALL_MUTATION_KIND.to_string(),
    };
    let mut inputs = vec![HistoryRowInput {
        table_id: TABLE_COREMETA_GENERATION_INSTALL_ROW,
        tuple_key: generation_install_mutation_key(
            &mutation.root_key_hash,
            mutation.generation,
            mutation.ordinal,
        )?,
        kind: HistoryRowKind::Put(encode_deterministic_proto(&header)),
    }];
    for (chunk_ordinal, chunk_bytes) in chunks.into_iter().enumerate() {
        let chunk = StoredGenerationInstallChunkProto {
            common: Some(common.clone()),
            schema: GENERATION_INSTALL_SCHEMA.to_string(),
            root_key_hash: mutation.root_key_hash.clone(),
            generation: mutation.generation,
            mutation_ordinal: mutation.ordinal,
            chunk_ordinal: chunk_ordinal as u32,
            chunk_count,
            mutation_hash: mutation_hash.clone(),
            chunk_bytes,
            kind: INSTALL_CHUNK_KIND.to_string(),
        };
        inputs.push(HistoryRowInput {
            table_id: TABLE_COREMETA_GENERATION_INSTALL_ROW,
            tuple_key: generation_install_chunk_key(
                &mutation.root_key_hash,
                mutation.generation,
                mutation.ordinal,
                chunk_ordinal as u32,
            )?,
            kind: HistoryRowKind::Put(encode_deterministic_proto(&chunk)),
        });
    }
    Ok(inputs)
}

pub(super) fn decode_generation_install(payload: &[u8]) -> Result<CoreMetaGenerationDescriptor> {
    let row = decode_deterministic_proto::<StoredGenerationInstallProto>(
        payload,
        "CoreMeta generation install descriptor",
    )?;
    if row.schema != GENERATION_INSTALL_SCHEMA || row.kind != INSTALL_DESCRIPTOR_KIND {
        bail!("CoreMeta generation install descriptor schema mismatch");
    }
    let descriptor = row
        .descriptor
        .ok_or_else(|| anyhow!("CoreMeta generation install descriptor payload is missing"))?;
    validate_descriptor(&descriptor)?;
    validate_install_common(
        row.common.as_ref(),
        &descriptor.root_key_hash,
        descriptor.generation,
    )?;
    let common = row.common.expect("validated install descriptor common");
    if common.transaction_id != descriptor.transaction_id
        || common.created_at_unix_nanos != descriptor.created_at_unix_nanos
    {
        bail!("CoreMeta generation install descriptor common metadata mismatch");
    }
    Ok(descriptor)
}

fn decode_generation_install_mutation(
    payload: &[u8],
) -> Result<StoredGenerationInstallMutationProto> {
    let row = decode_deterministic_proto::<StoredGenerationInstallMutationProto>(
        payload,
        "CoreMeta generation install mutation",
    )?;
    validate_install_mutation_header(&row)?;
    Ok(row)
}

fn decode_generation_install_chunk(payload: &[u8]) -> Result<StoredGenerationInstallChunkProto> {
    let row = decode_deterministic_proto::<StoredGenerationInstallChunkProto>(
        payload,
        "CoreMeta generation install chunk",
    )?;
    validate_install_chunk(&row)?;
    Ok(row)
}

fn validate_catch_up_frame(frame: &CoreMetaBatchFrame) -> Result<&CoreMetaGenerationDescriptor> {
    let descriptor = frame
        .descriptor
        .as_ref()
        .ok_or_else(|| anyhow!("CoreMeta install frame has no generation descriptor"))?;
    validate_descriptor(descriptor)?;
    if frame.frame_hash != catch_up_frame_hash(frame) {
        bail!("CoreMeta install frame hash mismatch");
    }
    if frame.mutations.is_empty() {
        bail!("CoreMeta install frame has no mutations");
    }
    let expected_bytes = descriptor.encoded_len() as u64
        + frame
            .mutations
            .iter()
            .map(|mutation| mutation.encoded_len() as u64)
            .sum::<u64>();
    if frame.encoded_bytes != expected_bytes {
        bail!("CoreMeta install frame encoded byte count mismatch");
    }
    let mut previous = None;
    for mutation in &frame.mutations {
        if mutation.root_key_hash != descriptor.root_key_hash
            || mutation.generation != descriptor.generation
            || mutation.ordinal >= descriptor.mutation_count
        {
            bail!("CoreMeta install frame mutation scope mismatch");
        }
        if previous.is_some_and(|ordinal| mutation.ordinal != ordinal + 1) {
            bail!("CoreMeta install frame mutation ordinals are not contiguous");
        }
        validate_api_mutation(
            mutation
                .mutation
                .as_ref()
                .ok_or_else(|| anyhow!("CoreMeta install frame mutation has no row"))?,
        )?;
        previous = Some(mutation.ordinal);
    }
    let last = previous.expect("non-empty mutation frame");
    if frame.generation_complete != (last + 1 == descriptor.mutation_count) {
        bail!("CoreMeta install frame generation completion marker is inconsistent");
    }
    if frame.history_complete
        && (!frame.generation_complete || descriptor.generation != frame.final_generation)
    {
        bail!("CoreMeta install frame history completion marker is inconsistent");
    }
    if frame.next_cursor.as_ref()
        != Some(&CoreMetaHistoryCursor {
            generation: descriptor.generation,
            ordinal: last,
        })
    {
        bail!("CoreMeta install frame cursor does not identify its final mutation");
    }
    Ok(descriptor)
}

fn validate_committed_generation_descriptor(
    descriptor: &CoreMetaGenerationDescriptor,
    committed_certificate_hash: &str,
    committed_publication_bundle: Option<&[u8]>,
) -> Result<()> {
    if descriptor.certificate_hash != committed_certificate_hash {
        bail!("CoreMeta recovery source does not contain the committed certificate");
    }
    if let Some(publication_bundle) = committed_publication_bundle
        && descriptor.publication_bundle != publication_bundle
    {
        bail!("CoreMeta recovery source does not contain the committed publication bundle");
    }
    Ok(())
}

fn validate_install_group(
    generations: &[(
        CoreMetaGenerationDescriptor,
        Vec<CoreMetaGenerationMutation>,
    )],
) -> Result<()> {
    if generations.len() == 1 {
        let descriptor = &generations[0].0;
        match (
            descriptor.coordinator_root_key_hash.as_deref(),
            descriptor.coordinator_root_generation,
        ) {
            (None, None) => {}
            (Some(root_key_hash), Some(generation))
                if root_key_hash == descriptor.root_key_hash
                    && generation == descriptor.generation => {}
            (Some(_), Some(_)) => {
                bail!(
                    "CoreMeta install does not contain the complete participant group for its coordinator"
                );
            }
            _ => {
                bail!("CoreMeta coordinator scope is incomplete");
            }
        }
        return Ok(());
    }
    let coordinator = generations[0]
        .0
        .coordinator_root_key_hash
        .as_deref()
        .zip(generations[0].0.coordinator_root_generation)
        .ok_or_else(|| anyhow!("multi-root CoreMeta install has no coordinator"))?;
    let transaction_id = generations[0].0.transaction_id.as_str();
    if generations.iter().any(|(descriptor, _)| {
        descriptor.transaction_id != transaction_id
            || descriptor.coordinator_root_key_hash.as_deref() != Some(coordinator.0)
            || descriptor.coordinator_root_generation != Some(coordinator.1)
    }) {
        bail!("CoreMeta install group does not share one transaction coordinator");
    }
    if !generations.iter().any(|(descriptor, _)| {
        descriptor.root_key_hash == coordinator.0 && descriptor.generation == coordinator.1
    }) {
        bail!("CoreMeta install group omits its coordinator generation");
    }
    Ok(())
}

fn validate_install_mutation_header(row: &StoredGenerationInstallMutationProto) -> Result<()> {
    if row.schema != GENERATION_INSTALL_SCHEMA || row.kind != INSTALL_MUTATION_KIND {
        bail!("CoreMeta generation install mutation schema mismatch");
    }
    validate_history_root(&row.root_key_hash)?;
    validate_sha256_digest(&row.mutation_hash, "staged mutation hash")?;
    if row.generation == 0
        || row.encoded_length == 0
        || row.encoded_length > INSTALL_MAX_MUTATION_BYTES as u64
        || row.chunk_count == 0
        || row.chunk_count as usize > INSTALL_MAX_MUTATION_CHUNKS
    {
        bail!("CoreMeta generation install mutation shape is invalid");
    }
    validate_install_common(row.common.as_ref(), &row.root_key_hash, row.generation)
}

fn validate_install_chunk(row: &StoredGenerationInstallChunkProto) -> Result<()> {
    if row.schema != GENERATION_INSTALL_SCHEMA || row.kind != INSTALL_CHUNK_KIND {
        bail!("CoreMeta generation install chunk schema mismatch");
    }
    validate_history_root(&row.root_key_hash)?;
    validate_sha256_digest(&row.mutation_hash, "staged mutation chunk hash")?;
    if row.generation == 0
        || row.chunk_count == 0
        || row.chunk_count as usize > INSTALL_MAX_MUTATION_CHUNKS
        || row.chunk_ordinal >= row.chunk_count
        || row.chunk_bytes.is_empty()
        || row.chunk_bytes.len() > HISTORY_ENVELOPE_CHUNK_BYTES
    {
        bail!("CoreMeta generation install chunk shape is invalid");
    }
    validate_install_common(row.common.as_ref(), &row.root_key_hash, row.generation)
}

fn validate_install_chunk_for_mutation(
    chunk: &StoredGenerationInstallChunkProto,
    header: &StoredGenerationInstallMutationProto,
    expected_ordinal: u32,
) -> Result<()> {
    if chunk.root_key_hash != header.root_key_hash
        || chunk.generation != header.generation
        || chunk.mutation_ordinal != header.ordinal
        || chunk.chunk_ordinal != expected_ordinal
        || chunk.chunk_count != header.chunk_count
        || chunk.mutation_hash != header.mutation_hash
    {
        bail!("CoreMeta staged mutation chunk scope mismatch");
    }
    Ok(())
}

fn validate_install_common(
    common: Option<&CoreMetaRowCommonProto>,
    root_key_hash: &str,
    generation: u64,
) -> Result<()> {
    let common = common.ok_or_else(|| anyhow!("CoreMeta generation install row has no common"))?;
    if common.root_key_hash != root_key_hash
        || common.root_generation != generation
        || common.transaction_id.is_empty()
        || common.created_at_unix_nanos == 0
        || common.visibility_state_enum() != CoreMetaVisibilityState::Pending
    {
        bail!("CoreMeta generation install common metadata mismatch");
    }
    Ok(())
}

fn install_common(descriptor: &CoreMetaGenerationDescriptor) -> CoreMetaRowCommonProto {
    core_meta_pending_row_common(
        format!("system/coremeta-history/{}", descriptor.root_key_hash),
        descriptor.root_key_hash.clone(),
        descriptor.generation,
        descriptor.transaction_id.clone(),
        descriptor.created_at_unix_nanos,
    )
}

fn staged_complete_outcome(
    descriptor: &CoreMetaGenerationDescriptor,
) -> CoreMetaGenerationInstallOutcome {
    CoreMetaGenerationInstallOutcome::StagedComplete {
        root_key_hash: descriptor.root_key_hash.clone(),
        generation: descriptor.generation,
        coordinator_root_key_hash: descriptor.coordinator_root_key_hash.clone(),
        coordinator_root_generation: descriptor.coordinator_root_generation,
    }
}

fn published_outcome(
    descriptor: &CoreMetaGenerationDescriptor,
) -> CoreMetaGenerationInstallOutcome {
    CoreMetaGenerationInstallOutcome::Published {
        root_key_hash: descriptor.root_key_hash.clone(),
        generation: descriptor.generation,
    }
}

fn generation_install_mutation_key(
    root_key_hash: &str,
    generation: u64,
    ordinal: u64,
) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Hash(root_key_hash),
        CoreMetaTuplePart::U64(generation),
        CoreMetaTuplePart::Utf8(INSTALL_MUTATION_KEY_KIND),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

fn generation_install_chunk_prefix(
    root_key_hash: &str,
    generation: u64,
    mutation_ordinal: u64,
) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Hash(root_key_hash),
        CoreMetaTuplePart::U64(generation),
        CoreMetaTuplePart::Utf8(INSTALL_CHUNK_KEY_KIND),
        CoreMetaTuplePart::U64(mutation_ordinal),
    ])
}

fn generation_install_chunk_key(
    root_key_hash: &str,
    generation: u64,
    mutation_ordinal: u64,
    chunk_ordinal: u32,
) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Hash(root_key_hash),
        CoreMetaTuplePart::U64(generation),
        CoreMetaTuplePart::Utf8(INSTALL_CHUNK_KEY_KIND),
        CoreMetaTuplePart::U64(mutation_ordinal),
        CoreMetaTuplePart::U64(u64::from(chunk_ordinal)),
    ])
}

fn is_coremeta_history_table(core_meta_key: &[u8]) -> bool {
    if core_meta_key.len() < 3 {
        return true;
    }
    matches!(
        u16::from_be_bytes([core_meta_key[1], core_meta_key[2]]),
        TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW
            | TABLE_COREMETA_GENERATION_MUTATION_ROW
            | TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW
            | TABLE_COREMETA_GENERATION_INSTALL_ROW
    )
}

fn sort_owned_rows(rows: &mut [CoreMetaEncodedOwnedRow]) {
    rows.sort_by(|left, right| {
        left.cf
            .cmp(&right.cf)
            .then_with(|| left.core_meta_key.cmp(&right.core_meta_key))
            .then_with(|| left.delete_marker.cmp(&right.delete_marker))
            .then_with(|| left.value_envelope.cmp(&right.value_envelope))
    });
}

fn same_physical_rows(left: &[CoreMetaEncodedOwnedRow], right: &[CoreMetaEncodedOwnedRow]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.cf == right.cf
                && left.core_meta_key == right.core_meta_key
                && left.value_envelope == right.value_envelope
                && left.delete_marker == right.delete_marker
                && left.root_key_hash == right.root_key_hash
                && left.root_generation == right.root_generation
                && left.visibility_state == right.visibility_state
        })
}

pub(super) fn validate_generation_install_payload(payload: &[u8]) -> Result<()> {
    let probe = GenerationInstallSchemaProbe::decode(payload)
        .context("decode CoreMeta generation install schema")?;
    if probe.schema != GENERATION_INSTALL_SCHEMA {
        bail!("CoreMeta generation install row has an unknown schema");
    }
    match probe.kind.as_str() {
        INSTALL_DESCRIPTOR_KIND => {
            let _ = decode_generation_install(payload)?;
        }
        INSTALL_MUTATION_KIND => {
            let _ = decode_generation_install_mutation(payload)?;
        }
        INSTALL_CHUNK_KIND => {
            let _ = decode_generation_install_chunk(payload)?;
        }
        _ => bail!("CoreMeta generation install row has an unknown kind"),
    }
    Ok(())
}
