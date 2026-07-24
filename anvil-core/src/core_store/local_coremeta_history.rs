use super::*;
use crate::anvil_api::{
    CoreMetaBatchFrame, CoreMetaCertificateEvidence, CoreMetaColumnFamilySummary,
    CoreMetaGenerationDescriptor, CoreMetaGenerationMutation, CoreMetaHistoryCursor,
    CoreMetaInventory, CoreMetaInventoryCursor, CoreMetaRowMutation,
};
use prost::Message;

pub(in crate::core_store) const TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW: u16 = 0x800c;
pub(in crate::core_store) const TABLE_COREMETA_GENERATION_MUTATION_ROW: u16 = 0x800d;
pub(in crate::core_store) const TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW: u16 = 0x800e;
pub(in crate::core_store) const TABLE_COREMETA_GENERATION_INSTALL_ROW: u16 = 0x800f;

const GENERATION_DESCRIPTOR_SCHEMA: &str = "anvil.coremeta.generation_descriptor.v1";
const GENERATION_MUTATION_SCHEMA: &str = "anvil.coremeta.generation_mutation.v1";
const GENERATION_ENVELOPE_CHUNK_SCHEMA: &str = "anvil.coremeta.generation_envelope_chunk.v1";
const GENERATION_INSTALL_SCHEMA: &str = "anvil.coremeta.generation_install.v1";
const HISTORY_ENVELOPE_CHUNK_BYTES: usize = 24 * 1024;
const HISTORY_MAX_ENVELOPE_CHUNKS: usize = 4;
const HISTORY_MAX_PAGE_ROWS: usize = CORE_META_MAX_SCAN_PAGE_ROWS;
const HISTORY_MAX_GENERATION_MUTATIONS: usize = 65_536;
const HISTORY_MIN_PAGE_BYTES: u64 = 128 * 1024;
const HISTORY_MAX_PAGE_BYTES: u64 = 16 * 1024 * 1024;

#[path = "local_coremeta_history/install.rs"]
mod install;
pub(crate) use install::CoreMetaGenerationInstallOutcome;

#[derive(Debug, Clone)]
pub(super) struct CoreMetaGenerationHistoryInput {
    pub(super) root_key_hash: String,
    pub(super) generation: u64,
    pub(super) transaction_id: String,
    pub(super) pending_batch_hash: String,
    pub(super) committed_batch_hash: String,
    pub(super) certificate_hash: String,
    pub(super) certificate_bytes: Vec<u8>,
    pub(super) certificate_persist_receipt_hashes: Vec<String>,
    pub(super) certificate_persist_receipt_bytes: Vec<Vec<u8>>,
    pub(super) coordinator_root_key_hash: Option<String>,
    pub(super) coordinator_root_generation: Option<u64>,
    pub(super) publication_bundle: Vec<u8>,
    pub(super) mutations: Vec<CoreMetaEncodedOwnedRow>,
    pub(super) created_at_unix_nanos: u64,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedGenerationHistory {
    pub(super) descriptor: CoreMetaGenerationDescriptor,
    pub(super) mutations: Vec<CoreMetaGenerationMutation>,
}

#[derive(Clone, PartialEq, Message)]
struct StoredGenerationDescriptorProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    descriptor: Option<CoreMetaGenerationDescriptor>,
}

#[derive(Clone, PartialEq, Message)]
struct StoredGenerationMutationProto {
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
    column_family: String,
    #[prost(bytes, tag = "7")]
    core_meta_key: Vec<u8>,
    #[prost(bool, tag = "8")]
    delete_marker: bool,
    #[prost(bytes, tag = "9")]
    inline_value_envelope: Vec<u8>,
    #[prost(string, tag = "10")]
    value_envelope_hash: String,
    #[prost(uint64, tag = "11")]
    value_envelope_bytes: u64,
    #[prost(uint32, tag = "12")]
    envelope_chunk_count: u32,
    #[prost(string, tag = "13")]
    row_hash: String,
    #[prost(uint64, tag = "14")]
    mutation_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
struct StoredGenerationEnvelopeChunkProto {
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
    value_envelope_hash: String,
    #[prost(bytes, tag = "9")]
    chunk_bytes: Vec<u8>,
}

impl CoreStore {
    /// Builds immutable history rows for the same RocksDB batch that publishes
    /// canonical rows and the root anchor. Returning an empty vector is an
    /// idempotent replay of an already verified generation.
    pub(super) fn prepare_coremeta_generation_history_rows(
        &self,
        input: CoreMetaGenerationHistoryInput,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let prepared = prepare_generation_history(input)?;
        self.validate_descriptor_commit_evidence(&prepared.descriptor)?;
        if let Some(existing) = self.read_generation_descriptor(
            &prepared.descriptor.root_key_hash,
            prepared.descriptor.generation,
        )? {
            if existing != prepared.descriptor {
                bail!(
                    "CoreMeta generation history is immutable: root={}, generation={}",
                    prepared.descriptor.root_key_hash,
                    prepared.descriptor.generation
                );
            }
            let mutations = self.read_complete_generation_mutations(&existing)?;
            verify_complete_generation(&existing, &mutations)?;
            return Ok(Vec::new());
        }

        self.ensure_generation_has_no_orphaned_rows(
            &prepared.descriptor.root_key_hash,
            prepared.descriptor.generation,
        )?;
        self.encode_generation_history_rows(&prepared)
    }

    /// Converts a durable publication intent into immutable generation history.
    /// Publication code must append these rows before its single atomic write.
    pub(super) fn coremeta_generation_history_rows_for_publication_intent(
        &self,
        intent: &super::local_root_publication_recovery::RootPublicationIntent,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let mut rows = Vec::new();
        let coordinator = intent.coordinator_scope()?;
        let publication_bundle = self.encode_coremeta_recovery_publication_bundle(intent)?;
        for root in &intent.roots {
            let certificate_hash = root.certificate_hash.as_deref().ok_or_else(|| {
                anyhow!(
                    "CoreMeta publication intent root {} has no certificate outcome",
                    root.publication.descriptor.root_key_hash()
                )
            })?;
            let evidence = self
                .read_coremeta_commit_evidence(certificate_hash)?
                .ok_or_else(|| {
                    anyhow!(
                        "CoreMeta publication intent certificate evidence is missing: {certificate_hash}"
                    )
                })?;
            let certificate =
                decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
                    &evidence.certificate_bytes,
                    "CoreMeta history commit certificate",
                )?;
            let root_key_hash = root.publication.descriptor.root_key_hash();
            if certificate.root_key_hash != root_key_hash
                || certificate.post_root_generation != root.publication.post_root_generation
                || certificate.transaction_id != intent.transaction_id
                || certificate.certificate_hash != evidence.certificate_hash
            {
                bail!("CoreMeta history certificate scope does not match publication intent");
            }
            rows.extend(
                self.prepare_coremeta_generation_history_rows(CoreMetaGenerationHistoryInput {
                    root_key_hash,
                    generation: root.publication.post_root_generation,
                    transaction_id: intent.transaction_id.clone(),
                    pending_batch_hash: certificate.pending_batch_hash,
                    committed_batch_hash: evidence.committed_batch_hash,
                    certificate_hash: evidence.certificate_hash,
                    certificate_bytes: evidence.certificate_bytes,
                    certificate_persist_receipt_hashes: evidence.certificate_persist_receipt_hashes,
                    certificate_persist_receipt_bytes: evidence.certificate_persist_receipt_bytes,
                    coordinator_root_key_hash: coordinator
                        .as_ref()
                        .map(|(root_key_hash, _)| root_key_hash.clone()),
                    coordinator_root_generation: coordinator
                        .as_ref()
                        .map(|(_, generation)| *generation),
                    publication_bundle: publication_bundle.clone(),
                    mutations: root.rows.clone(),
                    created_at_unix_nanos: intent.created_at_unix_nanos,
                })?,
            );
        }
        rows.sort_by(|left, right| {
            left.cf
                .cmp(&right.cf)
                .then_with(|| left.core_meta_key.cmp(&right.core_meta_key))
        });
        Ok(rows)
    }

    pub(in crate::core_store::local) fn validate_descriptor_commit_evidence(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
    ) -> Result<()> {
        validate_descriptor(descriptor)?;
        let certificate_api =
            decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
                &descriptor.commit_certificate,
                "CoreMeta history commit certificate",
            )?;
        let certificate =
            super::local_coremeta_quorum::api_commit_certificate_to_core(certificate_api)?;
        if certificate.root_key_hash != descriptor.root_key_hash
            || certificate.post_root_generation != descriptor.generation
            || certificate.transaction_id != descriptor.transaction_id
            || certificate.pending_batch_hash != descriptor.pending_batch_hash
            || certificate.certificate_hash != descriptor.certificate_hash
        {
            bail!("CoreMeta history certificate does not match its descriptor");
        }
        let receipts = descriptor
            .certificate_persist_evidence
            .iter()
            .map(|evidence| {
                let api = decode_deterministic_proto::<
                    crate::anvil_api::CoreMetaCertificatePersistReceipt,
                >(
                    &evidence.evidence,
                    "CoreMeta history certificate persistence receipt",
                )?;
                let receipt = super::local_coremeta_quorum::api_persist_receipt_to_core(api)?;
                let actual_hash = certificate_persist_receipt_payload_hash(&receipt)?;
                if actual_hash != evidence.evidence_hash {
                    bail!("CoreMeta history certificate persistence evidence hash mismatch");
                }
                Ok(receipt)
            })
            .collect::<Result<Vec<_>>>()?;
        let profile = self.default_coremeta_quorum_profile()?;
        validate_commit_evidence_with_verifier(
            &profile,
            &certificate,
            &receipts,
            |node_id, signed_payload_hash, signature| {
                self.verify_internal_core_receipt_signature(node_id, signed_payload_hash, signature)
            },
        )?;
        Ok(())
    }

    pub(crate) fn catch_up_coremeta_generation_history(
        &self,
        root_key_hash: &str,
        after: Option<&CoreMetaHistoryCursor>,
        through_generation: u64,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<Vec<CoreMetaBatchFrame>> {
        validate_history_root(root_key_hash)?;
        validate_page_bounds(max_rows, max_bytes, "CoreMeta catch-up")?;
        let bounds = self.generation_history_bounds(root_key_hash)?;
        let Some((retention_floor, latest_generation)) = bounds else {
            if after.is_some() || through_generation != 0 {
                bail!("CoreMeta catch-up cursor targets an empty history");
            }
            return Ok(vec![empty_catch_up_frame(root_key_hash)]);
        };
        let final_generation = capture_final_generation(through_generation, latest_generation)?;
        validate_history_cursor(after, retention_floor, final_generation)?;

        let mut generation = after.map_or(retention_floor, |cursor| cursor.generation);
        let mut after_ordinal = after.map(|cursor| cursor.ordinal);
        if let Some(cursor) = after {
            let descriptor = self
                .read_generation_descriptor(root_key_hash, cursor.generation)?
                .ok_or_else(|| anyhow!("CoreMeta catch-up cursor generation is not retained"))?;
            validate_descriptor(&descriptor)?;
            self.validate_descriptor_commit_evidence(&descriptor)?;
            if cursor.ordinal >= descriptor.mutation_count {
                bail!("CoreMeta catch-up cursor ordinal is outside its generation");
            }
            if cursor.ordinal + 1 == descriptor.mutation_count {
                generation = cursor.generation.saturating_add(1);
                after_ordinal = None;
            }
        }

        if generation > final_generation {
            return Ok(vec![completed_catch_up_frame(
                root_key_hash,
                final_generation,
                retention_floor,
                after.cloned(),
            )]);
        }

        let mut frames = Vec::new();
        let mut delivered_rows = 0usize;
        let mut delivered_bytes = 0u64;
        let mut last_cursor = after.cloned();
        while generation <= final_generation && delivered_rows < max_rows {
            let descriptor = self
                .read_generation_descriptor(root_key_hash, generation)?
                .ok_or_else(|| {
                    anyhow!(
                        "CoreMeta generation history has a gap at root={root_key_hash}, generation={generation}"
                    )
                })?;
            validate_descriptor(&descriptor)?;
            self.validate_descriptor_commit_evidence(&descriptor)?;
            let descriptor_bytes = descriptor.encoded_len() as u64;
            if delivered_bytes.saturating_add(descriptor_bytes) > max_bytes {
                if frames.is_empty() {
                    bail!("CoreMeta catch-up byte limit cannot fit a generation descriptor");
                }
                break;
            }

            let row_budget = max_rows - delivered_rows;
            let stored = self.read_generation_mutation_page(
                root_key_hash,
                generation,
                after_ordinal,
                row_budget,
            )?;
            if stored.is_empty() {
                bail!(
                    "CoreMeta generation descriptor has no retained mutation rows: root={root_key_hash}, generation={generation}"
                );
            }

            let mut mutations = Vec::new();
            let mut frame_bytes = descriptor_bytes;
            for mutation in stored {
                let encoded_bytes = mutation.encoded_len() as u64;
                if delivered_bytes
                    .saturating_add(frame_bytes)
                    .saturating_add(encoded_bytes)
                    > max_bytes
                {
                    break;
                }
                frame_bytes = frame_bytes.saturating_add(encoded_bytes);
                last_cursor = Some(CoreMetaHistoryCursor {
                    generation,
                    ordinal: mutation.ordinal,
                });
                mutations.push(mutation);
            }
            if mutations.is_empty() {
                if frames.is_empty() {
                    bail!("CoreMeta catch-up byte limit cannot fit one mutation");
                }
                break;
            }

            let last_ordinal = mutations.last().expect("non-empty mutation frame").ordinal;
            let generation_complete = last_ordinal + 1 == descriptor.mutation_count;
            let history_complete = generation_complete && generation == final_generation;
            let mut frame = CoreMetaBatchFrame {
                descriptor: Some(descriptor),
                mutations,
                next_cursor: last_cursor.clone(),
                generation_complete,
                history_complete,
                final_generation,
                retention_floor_generation: retention_floor,
                encoded_bytes: frame_bytes,
                frame_hash: String::new(),
            };
            frame.frame_hash = catch_up_frame_hash(&frame);
            delivered_rows += frame.mutations.len();
            delivered_bytes = delivered_bytes.saturating_add(frame_bytes);
            frames.push(frame);

            if history_complete || !generation_complete {
                break;
            }
            generation = generation.saturating_add(1);
            after_ordinal = None;
        }

        if frames.is_empty() {
            bail!("CoreMeta catch-up limits produced no progress");
        }
        Ok(frames)
    }

    pub(crate) fn coremeta_generation_inventory(
        &self,
        root_key_hash: &str,
        after: Option<&CoreMetaInventoryCursor>,
        through_generation: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<CoreMetaInventory> {
        validate_history_root(root_key_hash)?;
        validate_page_bounds(max_entries, max_bytes, "CoreMeta inventory")?;
        let Some((retention_floor, latest_generation)) =
            self.generation_history_bounds(root_key_hash)?
        else {
            if after.is_some() || through_generation != 0 {
                bail!("CoreMeta inventory cursor targets an empty history");
            }
            return Ok(empty_inventory(root_key_hash));
        };
        let final_generation = capture_final_generation(through_generation, latest_generation)?;
        validate_inventory_cursor(after, retention_floor, final_generation)?;

        let after_generation = after.map(|cursor| cursor.generation);
        if after_generation.is_some_and(|generation| generation >= final_generation) {
            return Ok(completed_inventory(
                root_key_hash,
                retention_floor,
                final_generation,
                after.cloned(),
            ));
        }
        let records =
            self.scan_generation_descriptors(root_key_hash, after_generation, max_entries)?;
        let mut descriptors = Vec::new();
        let mut encoded_bytes = 0u64;
        for descriptor in records {
            if descriptor.generation > final_generation {
                break;
            }
            self.validate_descriptor_commit_evidence(&descriptor)?;
            let bytes = descriptor.encoded_len() as u64;
            if encoded_bytes.saturating_add(bytes) > max_bytes {
                break;
            }
            encoded_bytes = encoded_bytes.saturating_add(bytes);
            descriptors.push(descriptor);
        }
        if descriptors.is_empty() {
            bail!("CoreMeta inventory limits produced no progress");
        }
        let last_generation = descriptors
            .last()
            .expect("non-empty descriptor page")
            .generation;
        let inventory_complete = last_generation == final_generation;
        let next_cursor = Some(CoreMetaInventoryCursor {
            generation: last_generation,
        });
        let page_hash = inventory_page_hash(root_key_hash, &descriptors);
        Ok(CoreMetaInventory {
            root_key_hash: root_key_hash.to_string(),
            descriptors,
            next_cursor,
            inventory_complete,
            retention_floor_generation: retention_floor,
            final_generation,
            page_hash,
            encoded_bytes,
        })
    }

    fn encode_generation_history_rows(
        &self,
        prepared: &PreparedGenerationHistory,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let descriptor = &prepared.descriptor;
        let common = history_common(descriptor);
        let descriptor_row = StoredGenerationDescriptorProto {
            common: Some(common.clone()),
            schema: GENERATION_DESCRIPTOR_SCHEMA.to_string(),
            descriptor: Some(descriptor.clone()),
        };
        let mut encoded_inputs = vec![HistoryRowInput {
            table_id: TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW,
            tuple_key: generation_descriptor_key(&descriptor.root_key_hash, descriptor.generation)?,
            kind: HistoryRowKind::Put(encode_deterministic_proto(&descriptor_row)),
        }];
        encoded_inputs.extend(generation_mutation_inputs(&common, &prepared.mutations)?);
        encode_history_rows(&self.meta, encoded_inputs)
    }

    pub(in crate::core_store::local) fn read_generation_descriptor(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<CoreMetaGenerationDescriptor>> {
        let key = generation_descriptor_key(root_key_hash, generation)?;
        self.meta
            .get(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW,
                &key,
            )?
            .map(|payload| decode_generation_descriptor(&payload))
            .transpose()
    }

    fn scan_generation_descriptors(
        &self,
        root_key_hash: &str,
        after_generation: Option<u64>,
        limit: usize,
    ) -> Result<Vec<CoreMetaGenerationDescriptor>> {
        let prefix = generation_descriptor_prefix(root_key_hash)?;
        let after = after_generation
            .map(|generation| generation_descriptor_key(root_key_hash, generation))
            .transpose()?;
        self.meta
            .scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW,
                &prefix,
                after.as_deref(),
                limit,
            )?
            .into_iter()
            .map(|record| decode_generation_descriptor(&record.payload))
            .collect()
    }

    pub(in crate::core_store::local) fn generation_history_bounds(
        &self,
        root_key_hash: &str,
    ) -> Result<Option<(u64, u64)>> {
        let first = self
            .scan_generation_descriptors(root_key_hash, None, 1)?
            .into_iter()
            .next();
        let Some(first) = first else {
            return Ok(None);
        };
        let start = generation_descriptor_key(root_key_hash, 0)?;
        let end = generation_descriptor_key(root_key_hash, u64::MAX)?;
        let latest = self
            .meta
            .scan_range_reverse_inclusive(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW,
                &start,
                &end,
                1,
            )?
            .into_iter()
            .next()
            .map(|record| decode_generation_descriptor(&record.payload))
            .transpose()?
            .ok_or_else(|| anyhow!("CoreMeta history lost its latest descriptor"))?;
        if first.root_key_hash != root_key_hash || latest.root_key_hash != root_key_hash {
            bail!("CoreMeta generation descriptor range escaped its root prefix");
        }
        Ok(Some((first.generation, latest.generation)))
    }

    fn read_generation_mutation_page(
        &self,
        root_key_hash: &str,
        generation: u64,
        after_ordinal: Option<u64>,
        limit: usize,
    ) -> Result<Vec<CoreMetaGenerationMutation>> {
        let prefix = generation_mutation_prefix(root_key_hash, generation)?;
        let after = after_ordinal
            .map(|ordinal| generation_mutation_key(root_key_hash, generation, ordinal))
            .transpose()?;
        self.meta
            .scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_MUTATION_ROW,
                &prefix,
                after.as_deref(),
                limit,
            )?
            .into_iter()
            .map(|record| {
                let stored = decode_generation_mutation(&record.payload)?;
                self.materialise_generation_mutation(stored)
            })
            .collect()
    }

    fn read_complete_generation_mutations(
        &self,
        descriptor: &CoreMetaGenerationDescriptor,
    ) -> Result<Vec<CoreMetaGenerationMutation>> {
        let count = usize::try_from(descriptor.mutation_count)
            .context("CoreMeta generation mutation count exceeds usize")?;
        if count == 0 || count > HISTORY_MAX_GENERATION_MUTATIONS {
            bail!("CoreMeta generation mutation count is outside supported bounds");
        }
        let mut mutations = Vec::with_capacity(count);
        let mut after = None;
        while mutations.len() < count {
            let page = self.read_generation_mutation_page(
                &descriptor.root_key_hash,
                descriptor.generation,
                after,
                (count - mutations.len()).min(HISTORY_MAX_PAGE_ROWS),
            )?;
            if page.is_empty() {
                bail!("CoreMeta generation history is incomplete");
            }
            after = page.last().map(|mutation| mutation.ordinal);
            mutations.extend(page);
        }
        Ok(mutations)
    }

    fn materialise_generation_mutation(
        &self,
        stored: StoredGenerationMutationProto,
    ) -> Result<CoreMetaGenerationMutation> {
        validate_stored_mutation(&stored)?;
        let value_envelope = if stored.envelope_chunk_count == 0 {
            stored.inline_value_envelope.clone()
        } else {
            let prefix = generation_envelope_chunk_prefix(
                &stored.root_key_hash,
                stored.generation,
                stored.ordinal,
            )?;
            let records = self.meta.scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW,
                &prefix,
                None,
                stored.envelope_chunk_count as usize,
            )?;
            if records.len() != stored.envelope_chunk_count as usize {
                bail!("CoreMeta generation mutation envelope chunks are incomplete");
            }
            let mut value = Vec::with_capacity(stored.value_envelope_bytes as usize);
            for (expected_ordinal, record) in records.into_iter().enumerate() {
                let chunk = decode_generation_envelope_chunk(&record.payload)?;
                validate_chunk_for_mutation(&chunk, &stored, expected_ordinal as u32)?;
                value.extend_from_slice(&chunk.chunk_bytes);
            }
            value
        };
        if value_envelope.len() as u64 != stored.value_envelope_bytes
            || digest_bytes(&value_envelope) != stored.value_envelope_hash
        {
            bail!("CoreMeta generation mutation envelope hash or length mismatch");
        }
        let row = CoreMetaRowMutation {
            column_family: stored.column_family,
            core_meta_key: stored.core_meta_key,
            value_envelope,
            row_hash: stored.row_hash,
            delete_marker: stored.delete_marker,
        };
        validate_api_mutation(&row)?;
        if mutation_size(&row) != stored.mutation_bytes {
            bail!("CoreMeta generation mutation byte count mismatch");
        }
        Ok(CoreMetaGenerationMutation {
            root_key_hash: stored.root_key_hash,
            generation: stored.generation,
            ordinal: stored.ordinal,
            mutation: Some(row),
        })
    }

    fn ensure_generation_has_no_orphaned_rows(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<()> {
        let mutation_prefix = generation_mutation_prefix(root_key_hash, generation)?;
        let chunk_prefix = core_meta_tuple_key(&[
            CoreMetaTuplePart::Hash(root_key_hash),
            CoreMetaTuplePart::U64(generation),
        ])?;
        let has_mutation = !self
            .meta
            .scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_MUTATION_ROW,
                &mutation_prefix,
                None,
                1,
            )?
            .is_empty();
        let has_chunk = !self
            .meta
            .scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW,
                &chunk_prefix,
                None,
                1,
            )?
            .is_empty();
        if has_mutation || has_chunk {
            bail!(
                "CoreMeta generation has orphaned immutable history rows: root={root_key_hash}, generation={generation}"
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn install_coremeta_generation_history_for_test(
        &self,
        input: CoreMetaGenerationHistoryInput,
    ) -> Result<()> {
        let rows = self.prepare_coremeta_generation_history_rows(input)?;
        if rows.is_empty() {
            return Ok(());
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

pub(in crate::core_store) fn validate_coremeta_history_row(
    table_id: u16,
    payload: &[u8],
) -> Result<()> {
    match table_id {
        TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW => {
            let _ = decode_generation_descriptor(payload)?;
        }
        TABLE_COREMETA_GENERATION_MUTATION_ROW => {
            let row = decode_generation_mutation(payload)?;
            validate_stored_mutation(&row)?;
        }
        TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW => {
            let row = decode_generation_envelope_chunk(payload)?;
            validate_stored_chunk(&row)?;
        }
        TABLE_COREMETA_GENERATION_INSTALL_ROW => {
            install::validate_generation_install_payload(payload)?;
        }
        _ => bail!("unknown CoreMeta generation history table {table_id:#06x}"),
    }
    Ok(())
}

pub(crate) fn verify_complete_generation(
    descriptor: &CoreMetaGenerationDescriptor,
    mutations: &[CoreMetaGenerationMutation],
) -> Result<()> {
    validate_descriptor(descriptor)?;
    if !descriptor.complete {
        bail!("CoreMeta generation descriptor is not complete");
    }
    if descriptor.mutation_count != mutations.len() as u64 {
        bail!("CoreMeta generation mutation count mismatch");
    }
    let mut mutation_bytes = 0u64;
    let mut row_hashes = Vec::with_capacity(mutations.len());
    let mut column_families = BTreeMap::<String, (u64, u64, Vec<(u64, String)>)>::new();
    for (ordinal, mutation) in mutations.iter().enumerate() {
        if mutation.root_key_hash != descriptor.root_key_hash
            || mutation.generation != descriptor.generation
            || mutation.ordinal != ordinal as u64
        {
            bail!("CoreMeta generation mutation ordering or scope mismatch");
        }
        let row = mutation
            .mutation
            .as_ref()
            .ok_or_else(|| anyhow!("CoreMeta generation mutation is missing its row"))?;
        validate_api_mutation(row)?;
        let row_bytes = mutation_size(row);
        mutation_bytes = mutation_bytes.saturating_add(row_bytes);
        row_hashes.push(row.row_hash.clone());
        let summary = column_families
            .entry(row.column_family.clone())
            .or_default();
        summary.0 = summary.0.saturating_add(1);
        summary.1 = summary.1.saturating_add(row_bytes);
        summary.2.push((ordinal as u64, row.row_hash.clone()));
    }
    if mutation_bytes != descriptor.mutation_bytes {
        bail!("CoreMeta generation mutation byte total mismatch");
    }
    let actual_hash = generation_hash(descriptor, &row_hashes);
    if actual_hash != descriptor.generation_hash {
        bail!("CoreMeta generation hash mismatch");
    }
    let actual_summaries = column_families
        .into_iter()
        .map(
            |(column_family, (mutation_count, mutation_bytes, entries))| {
                CoreMetaColumnFamilySummary {
                    slice_hash: column_family_slice_hash(&column_family, &entries),
                    column_family,
                    mutation_count,
                    mutation_bytes,
                }
            },
        )
        .collect::<Vec<_>>();
    if actual_summaries != descriptor.column_families {
        bail!("CoreMeta generation column-family summaries mismatch");
    }
    verify_descriptor_batch_identity(descriptor, &row_hashes)?;
    Ok(())
}

pub(super) fn prepare_generation_history(
    mut input: CoreMetaGenerationHistoryInput,
) -> Result<PreparedGenerationHistory> {
    validate_history_root(&input.root_key_hash)?;
    if input.generation == 0 {
        bail!("CoreMeta generation history cannot record generation zero");
    }
    validate_history_id(&input.transaction_id, "transaction id")?;
    validate_blake3_digest(&input.pending_batch_hash, "pending batch hash")?;
    validate_blake3_digest(&input.committed_batch_hash, "committed batch hash")?;
    validate_blake3_digest(&input.certificate_hash, "certificate hash")?;
    if input.certificate_bytes.is_empty() {
        bail!("CoreMeta generation history requires commit certificate bytes");
    }
    if input.created_at_unix_nanos == 0 {
        bail!("CoreMeta generation history requires a nonzero timestamp");
    }
    if input.publication_bundle.is_empty()
        || input.publication_bundle.len() > HISTORY_MAX_PAGE_BYTES as usize
    {
        bail!("CoreMeta generation history publication bundle is outside the bounded range");
    }
    validate_coordinator_scope(
        input.coordinator_root_key_hash.as_deref(),
        input.coordinator_root_generation,
    )?;
    let api_certificate = decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
        &input.certificate_bytes,
        "CoreMeta generation history commit certificate",
    )?;
    let core_certificate =
        super::local_coremeta_quorum::api_commit_certificate_to_core(api_certificate)?;
    if core_certificate.root_key_hash != input.root_key_hash
        || core_certificate.post_root_generation != input.generation
        || core_certificate.transaction_id != input.transaction_id
        || core_certificate.pending_batch_hash != input.pending_batch_hash
        || core_certificate.certificate_hash != input.certificate_hash
    {
        bail!("CoreMeta generation history commit certificate scope mismatch");
    }
    if input.certificate_persist_receipt_hashes.len()
        != input.certificate_persist_receipt_bytes.len()
    {
        bail!("CoreMeta certificate evidence hashes and payloads differ in length");
    }
    let mut evidence = input
        .certificate_persist_receipt_hashes
        .drain(..)
        .zip(input.certificate_persist_receipt_bytes.drain(..))
        .map(|(evidence_hash, evidence)| {
            validate_blake3_digest(&evidence_hash, "certificate persistence evidence hash")?;
            if evidence.is_empty() {
                bail!("CoreMeta certificate persistence evidence is empty");
            }
            Ok(CoreMetaCertificateEvidence {
                evidence_hash,
                evidence,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    evidence.sort_by(|left, right| left.evidence_hash.cmp(&right.evidence_hash));
    if evidence
        .windows(2)
        .any(|pair| pair[0].evidence_hash == pair[1].evidence_hash)
    {
        bail!("CoreMeta generation history has duplicate certificate evidence");
    }
    if input.mutations.is_empty() || input.mutations.len() > HISTORY_MAX_GENERATION_MUTATIONS {
        bail!("CoreMeta generation history mutation count is outside supported bounds");
    }
    input.mutations.sort_by(|left, right| {
        left.cf
            .cmp(&right.cf)
            .then_with(|| left.core_meta_key.cmp(&right.core_meta_key))
            .then_with(|| left.delete_marker.cmp(&right.delete_marker))
            .then_with(|| left.value_envelope.cmp(&right.value_envelope))
    });
    if input
        .mutations
        .windows(2)
        .any(|pair| pair[0].cf == pair[1].cf && pair[0].core_meta_key == pair[1].core_meta_key)
    {
        bail!("CoreMeta generation history cannot contain duplicate mutation targets");
    }

    let mut mutations = Vec::with_capacity(input.mutations.len());
    let mut mutation_bytes = 0u64;
    let mut row_hashes = Vec::with_capacity(input.mutations.len());
    let mut column_families = BTreeMap::<String, (u64, u64, Vec<(u64, String)>)>::new();
    for (ordinal, row) in input.mutations.into_iter().enumerate() {
        if row.root_key_hash != input.root_key_hash
            || row.root_generation != input.generation
            || row.visibility_state != CoreMetaVisibilityState::Committed
        {
            bail!("CoreMeta generation history mutation scope or visibility mismatch");
        }
        canonical_coremeta_cf_name(&row.cf)?;
        let _ = core_meta_record_tuple_key(&row.core_meta_key)?;
        let mutation = CoreMetaRowMutation {
            column_family: row.cf,
            core_meta_key: row.core_meta_key,
            value_envelope: row.value_envelope,
            row_hash: String::new(),
            delete_marker: row.delete_marker,
        };
        let mut mutation = mutation;
        mutation.row_hash = core_meta_encoded_row_hash_with_delete(
            &mutation.column_family,
            &mutation.core_meta_key,
            &mutation.value_envelope,
            mutation.delete_marker,
        );
        validate_api_mutation(&mutation)?;
        let row_bytes = mutation_size(&mutation);
        mutation_bytes = mutation_bytes.saturating_add(row_bytes);
        row_hashes.push(mutation.row_hash.clone());
        let summary = column_families
            .entry(mutation.column_family.clone())
            .or_default();
        summary.0 = summary.0.saturating_add(1);
        summary.1 = summary.1.saturating_add(row_bytes);
        summary.2.push((ordinal as u64, mutation.row_hash.clone()));
        mutations.push(CoreMetaGenerationMutation {
            root_key_hash: input.root_key_hash.clone(),
            generation: input.generation,
            ordinal: ordinal as u64,
            mutation: Some(mutation),
        });
    }

    let expected_pending_batch_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
        root_key_hash: input.root_key_hash.clone(),
        expected_root_generation: core_certificate.expected_root_generation,
        post_root_generation: input.generation,
        transaction_id: input.transaction_id.clone(),
        row_hashes: row_hashes.clone(),
    })?;
    if expected_pending_batch_hash != input.pending_batch_hash {
        bail!("CoreMeta generation history pending batch hash mismatch");
    }
    let expected_committed_batch_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
        root_key_hash: input.root_key_hash.clone(),
        expected_root_generation: core_certificate.expected_root_generation,
        post_root_generation: input.generation,
        transaction_id: input.transaction_id.clone(),
        pending_batch_hash: input.pending_batch_hash.clone(),
        committed_row_hashes: row_hashes.clone(),
    })?;
    if expected_committed_batch_hash != input.committed_batch_hash {
        bail!("CoreMeta generation history committed batch hash mismatch");
    }
    if crate::core_store::commit_certificate_hash(&core_certificate)? != input.certificate_hash {
        bail!("CoreMeta generation history certificate hash mismatch");
    }
    let column_families = column_families
        .into_iter()
        .map(
            |(column_family, (mutation_count, mutation_bytes, entries))| {
                CoreMetaColumnFamilySummary {
                    slice_hash: column_family_slice_hash(&column_family, &entries),
                    column_family,
                    mutation_count,
                    mutation_bytes,
                }
            },
        )
        .collect::<Vec<_>>();

    let mut descriptor = CoreMetaGenerationDescriptor {
        root_key_hash: input.root_key_hash,
        generation: input.generation,
        transaction_id: input.transaction_id,
        pending_batch_hash: input.pending_batch_hash,
        committed_batch_hash: input.committed_batch_hash,
        certificate_hash: input.certificate_hash,
        commit_certificate: input.certificate_bytes,
        certificate_persist_evidence: evidence,
        mutation_count: mutations.len() as u64,
        mutation_bytes,
        generation_hash: String::new(),
        complete: true,
        created_at_unix_nanos: input.created_at_unix_nanos,
        coordinator_root_key_hash: input.coordinator_root_key_hash,
        coordinator_root_generation: input.coordinator_root_generation,
        column_families,
        publication_bundle: input.publication_bundle,
    };
    descriptor.generation_hash = generation_hash(&descriptor, &row_hashes);
    verify_complete_generation(&descriptor, &mutations)?;
    Ok(PreparedGenerationHistory {
        descriptor,
        mutations,
    })
}

struct HistoryRowInput {
    table_id: u16,
    tuple_key: Vec<u8>,
    kind: HistoryRowKind,
}

enum HistoryRowKind {
    Put(Vec<u8>),
    Delete(CoreMetaRowCommonProto),
}

fn generation_mutation_inputs(
    common: &CoreMetaRowCommonProto,
    mutations: &[CoreMetaGenerationMutation],
) -> Result<Vec<HistoryRowInput>> {
    let mut inputs = Vec::new();
    for mutation in mutations {
        let row = mutation
            .mutation
            .as_ref()
            .ok_or_else(|| anyhow!("CoreMeta history mutation is missing its row"))?;
        let chunks =
            if row.delete_marker || row.value_envelope.len() <= HISTORY_ENVELOPE_CHUNK_BYTES {
                Vec::new()
            } else {
                row.value_envelope
                    .chunks(HISTORY_ENVELOPE_CHUNK_BYTES)
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            };
        if chunks.len() > HISTORY_MAX_ENVELOPE_CHUNKS {
            bail!("CoreMeta history mutation envelope requires too many chunks");
        }
        let envelope_hash = digest_bytes(&row.value_envelope);
        let stored = StoredGenerationMutationProto {
            common: Some(common.clone()),
            schema: GENERATION_MUTATION_SCHEMA.to_string(),
            root_key_hash: mutation.root_key_hash.clone(),
            generation: mutation.generation,
            ordinal: mutation.ordinal,
            column_family: row.column_family.clone(),
            core_meta_key: row.core_meta_key.clone(),
            delete_marker: row.delete_marker,
            inline_value_envelope: if chunks.is_empty() {
                row.value_envelope.clone()
            } else {
                Vec::new()
            },
            value_envelope_hash: envelope_hash.clone(),
            value_envelope_bytes: row.value_envelope.len() as u64,
            envelope_chunk_count: chunks.len() as u32,
            row_hash: row.row_hash.clone(),
            mutation_bytes: mutation_size(row),
        };
        inputs.push(HistoryRowInput {
            table_id: TABLE_COREMETA_GENERATION_MUTATION_ROW,
            tuple_key: generation_mutation_key(
                &mutation.root_key_hash,
                mutation.generation,
                mutation.ordinal,
            )?,
            kind: HistoryRowKind::Put(encode_deterministic_proto(&stored)),
        });
        for (chunk_ordinal, chunk_bytes) in chunks.into_iter().enumerate() {
            let chunk = StoredGenerationEnvelopeChunkProto {
                common: Some(common.clone()),
                schema: GENERATION_ENVELOPE_CHUNK_SCHEMA.to_string(),
                root_key_hash: mutation.root_key_hash.clone(),
                generation: mutation.generation,
                mutation_ordinal: mutation.ordinal,
                chunk_ordinal: chunk_ordinal as u32,
                chunk_count: stored.envelope_chunk_count,
                value_envelope_hash: envelope_hash.clone(),
                chunk_bytes,
            };
            inputs.push(HistoryRowInput {
                table_id: TABLE_COREMETA_GENERATION_ENVELOPE_CHUNK_ROW,
                tuple_key: generation_envelope_chunk_key(
                    &mutation.root_key_hash,
                    mutation.generation,
                    mutation.ordinal,
                    chunk_ordinal as u32,
                )?,
                kind: HistoryRowKind::Put(encode_deterministic_proto(&chunk)),
            });
        }
    }
    Ok(inputs)
}

fn encode_history_rows(
    meta: &CoreMetaStore,
    inputs: Vec<HistoryRowInput>,
) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
    let operations = inputs
        .iter()
        .map(|input| {
            let (common, kind) = match &input.kind {
                HistoryRowKind::Put(payload) => (None, CoreMetaBatchOpKind::Put(payload)),
                HistoryRowKind::Delete(common) => {
                    (Some(common.clone()), CoreMetaBatchOpKind::Delete)
                }
            };
            CoreMetaBatchOp {
                cf: CF_TRANSACTIONS,
                table_id: input.table_id,
                tuple_key: &input.tuple_key,
                common,
                kind,
            }
        })
        .collect::<Vec<_>>();
    meta.encode_batch_ops(&operations)
}

fn generation_descriptor_input(
    descriptor: &CoreMetaGenerationDescriptor,
) -> Result<HistoryRowInput> {
    let row = StoredGenerationDescriptorProto {
        common: Some(history_common(descriptor)),
        schema: GENERATION_DESCRIPTOR_SCHEMA.to_string(),
        descriptor: Some(descriptor.clone()),
    };
    Ok(HistoryRowInput {
        table_id: TABLE_COREMETA_GENERATION_DESCRIPTOR_ROW,
        tuple_key: generation_descriptor_key(&descriptor.root_key_hash, descriptor.generation)?,
        kind: HistoryRowKind::Put(encode_deterministic_proto(&row)),
    })
}

fn decode_generation_descriptor(payload: &[u8]) -> Result<CoreMetaGenerationDescriptor> {
    let row = decode_deterministic_proto::<StoredGenerationDescriptorProto>(
        payload,
        "CoreMeta generation descriptor",
    )?;
    if row.schema != GENERATION_DESCRIPTOR_SCHEMA {
        bail!("CoreMeta generation descriptor schema mismatch");
    }
    let descriptor = row
        .descriptor
        .ok_or_else(|| anyhow!("CoreMeta generation descriptor payload is missing"))?;
    validate_descriptor(&descriptor)?;
    validate_history_common(row.common.as_ref(), &descriptor)?;
    Ok(descriptor)
}

fn decode_generation_mutation(payload: &[u8]) -> Result<StoredGenerationMutationProto> {
    let row = decode_deterministic_proto::<StoredGenerationMutationProto>(
        payload,
        "CoreMeta generation mutation",
    )?;
    if row.schema != GENERATION_MUTATION_SCHEMA {
        bail!("CoreMeta generation mutation schema mismatch");
    }
    Ok(row)
}

fn decode_generation_envelope_chunk(payload: &[u8]) -> Result<StoredGenerationEnvelopeChunkProto> {
    let row = decode_deterministic_proto::<StoredGenerationEnvelopeChunkProto>(
        payload,
        "CoreMeta generation envelope chunk",
    )?;
    if row.schema != GENERATION_ENVELOPE_CHUNK_SCHEMA {
        bail!("CoreMeta generation envelope chunk schema mismatch");
    }
    Ok(row)
}

pub(in crate::core_store::local) fn validate_descriptor(
    descriptor: &CoreMetaGenerationDescriptor,
) -> Result<()> {
    validate_history_root(&descriptor.root_key_hash)?;
    if descriptor.generation == 0 || descriptor.mutation_count == 0 {
        bail!("CoreMeta generation descriptor generation/count must be nonzero");
    }
    if descriptor.mutation_count > HISTORY_MAX_GENERATION_MUTATIONS as u64 {
        bail!("CoreMeta generation descriptor mutation count exceeds the supported bound");
    }
    validate_history_id(&descriptor.transaction_id, "transaction id")?;
    validate_blake3_digest(&descriptor.pending_batch_hash, "pending batch hash")?;
    validate_blake3_digest(&descriptor.committed_batch_hash, "committed batch hash")?;
    validate_blake3_digest(&descriptor.certificate_hash, "certificate hash")?;
    validate_sha256_digest(&descriptor.generation_hash, "generation hash")?;
    if descriptor.commit_certificate.is_empty()
        || descriptor.publication_bundle.is_empty()
        || descriptor.publication_bundle.len() > HISTORY_MAX_PAGE_BYTES as usize
        || descriptor.created_at_unix_nanos == 0
        || !descriptor.complete
    {
        bail!("CoreMeta generation descriptor is incomplete");
    }
    validate_coordinator_scope(
        descriptor.coordinator_root_key_hash.as_deref(),
        descriptor.coordinator_root_generation,
    )?;
    if descriptor.column_families.is_empty() {
        bail!("CoreMeta generation descriptor has no column-family summaries");
    }
    let mut previous_family = None;
    let mut summary_count = 0u64;
    let mut summary_bytes = 0u64;
    for summary in &descriptor.column_families {
        canonical_coremeta_cf_name(&summary.column_family)?;
        validate_sha256_digest(&summary.slice_hash, "column-family slice hash")?;
        if summary.mutation_count == 0 {
            bail!("CoreMeta generation column-family summary has no mutations");
        }
        if previous_family.is_some_and(|family: &str| family >= summary.column_family.as_str()) {
            bail!("CoreMeta generation column-family summaries are not sorted/unique");
        }
        previous_family = Some(summary.column_family.as_str());
        summary_count = summary_count.saturating_add(summary.mutation_count);
        summary_bytes = summary_bytes.saturating_add(summary.mutation_bytes);
    }
    if summary_count != descriptor.mutation_count || summary_bytes != descriptor.mutation_bytes {
        bail!("CoreMeta generation column-family summary totals mismatch");
    }
    let mut previous = None;
    for evidence in &descriptor.certificate_persist_evidence {
        validate_blake3_digest(&evidence.evidence_hash, "certificate evidence hash")?;
        if evidence.evidence.is_empty() {
            bail!("CoreMeta generation descriptor certificate evidence is empty");
        }
        if previous.is_some_and(|value: &str| value >= evidence.evidence_hash.as_str()) {
            bail!("CoreMeta generation descriptor certificate evidence is not sorted/unique");
        }
        previous = Some(evidence.evidence_hash.as_str());
    }
    Ok(())
}

fn validate_coordinator_scope(root_key_hash: Option<&str>, generation: Option<u64>) -> Result<()> {
    match (root_key_hash, generation) {
        (None, None) => Ok(()),
        (Some(root_key_hash), Some(generation)) if generation > 0 => {
            validate_history_root(root_key_hash)
        }
        _ => bail!(
            "CoreMeta generation coordinator root hash and generation must both be present or absent"
        ),
    }
}

fn validate_stored_mutation(row: &StoredGenerationMutationProto) -> Result<()> {
    validate_history_root(&row.root_key_hash)?;
    if row.generation == 0 {
        bail!("CoreMeta generation mutation generation must be nonzero");
    }
    canonical_coremeta_cf_name(&row.column_family)?;
    let _ = core_meta_record_tuple_key(&row.core_meta_key)?;
    validate_sha256_digest(&row.value_envelope_hash, "value envelope hash")?;
    validate_blake3_digest(&row.row_hash, "row hash")?;
    if row.envelope_chunk_count as usize > HISTORY_MAX_ENVELOPE_CHUNKS {
        bail!("CoreMeta generation mutation chunk count exceeds its bound");
    }
    if row.delete_marker {
        if row.value_envelope_bytes != 0
            || !row.inline_value_envelope.is_empty()
            || row.envelope_chunk_count != 0
        {
            bail!("CoreMeta delete history mutation carries a value envelope");
        }
    } else if row.value_envelope_bytes == 0 {
        bail!("CoreMeta put history mutation has an empty value envelope");
    } else if row.envelope_chunk_count == 0 {
        if row.inline_value_envelope.len() as u64 != row.value_envelope_bytes {
            bail!("CoreMeta inline history envelope length mismatch");
        }
    } else if !row.inline_value_envelope.is_empty() {
        bail!("CoreMeta chunked history mutation also carries an inline envelope");
    }
    let descriptor = CoreMetaGenerationDescriptor {
        root_key_hash: row.root_key_hash.clone(),
        generation: row.generation,
        transaction_id: row
            .common
            .as_ref()
            .map(|common| common.transaction_id.clone())
            .unwrap_or_default(),
        complete: true,
        ..Default::default()
    };
    validate_history_common(row.common.as_ref(), &descriptor)
}

fn validate_stored_chunk(row: &StoredGenerationEnvelopeChunkProto) -> Result<()> {
    validate_history_root(&row.root_key_hash)?;
    validate_sha256_digest(&row.value_envelope_hash, "chunk envelope hash")?;
    if row.generation == 0
        || row.chunk_count == 0
        || row.chunk_count as usize > HISTORY_MAX_ENVELOPE_CHUNKS
        || row.chunk_ordinal >= row.chunk_count
        || row.chunk_bytes.is_empty()
        || row.chunk_bytes.len() > HISTORY_ENVELOPE_CHUNK_BYTES
    {
        bail!("CoreMeta generation envelope chunk shape is invalid");
    }
    let descriptor = CoreMetaGenerationDescriptor {
        root_key_hash: row.root_key_hash.clone(),
        generation: row.generation,
        transaction_id: row
            .common
            .as_ref()
            .map(|common| common.transaction_id.clone())
            .unwrap_or_default(),
        complete: true,
        ..Default::default()
    };
    validate_history_common(row.common.as_ref(), &descriptor)
}

fn validate_chunk_for_mutation(
    chunk: &StoredGenerationEnvelopeChunkProto,
    mutation: &StoredGenerationMutationProto,
    expected_ordinal: u32,
) -> Result<()> {
    validate_stored_chunk(chunk)?;
    if chunk.root_key_hash != mutation.root_key_hash
        || chunk.generation != mutation.generation
        || chunk.mutation_ordinal != mutation.ordinal
        || chunk.chunk_ordinal != expected_ordinal
        || chunk.chunk_count != mutation.envelope_chunk_count
        || chunk.value_envelope_hash != mutation.value_envelope_hash
    {
        bail!("CoreMeta generation envelope chunk scope mismatch");
    }
    Ok(())
}

fn validate_api_mutation(row: &CoreMetaRowMutation) -> Result<()> {
    canonical_coremeta_cf_name(&row.column_family)?;
    let _ = core_meta_record_tuple_key(&row.core_meta_key)?;
    if row.delete_marker && !row.value_envelope.is_empty() {
        bail!("CoreMeta delete mutation carries a value envelope");
    }
    if !row.delete_marker && row.value_envelope.is_empty() {
        bail!("CoreMeta put mutation has an empty value envelope");
    }
    let actual = core_meta_encoded_row_hash_with_delete(
        &row.column_family,
        &row.core_meta_key,
        &row.value_envelope,
        row.delete_marker,
    );
    if actual != row.row_hash {
        bail!("CoreMeta generation mutation row hash mismatch");
    }
    Ok(())
}

fn validate_history_common(
    common: Option<&CoreMetaRowCommonProto>,
    descriptor: &CoreMetaGenerationDescriptor,
) -> Result<()> {
    let common = common.ok_or_else(|| anyhow!("CoreMeta history row has no common metadata"))?;
    if common.root_key_hash != descriptor.root_key_hash
        || common.root_generation != descriptor.generation
        || common.transaction_id != descriptor.transaction_id
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.created_at_unix_nanos == 0
    {
        bail!("CoreMeta history row common metadata does not match its generation");
    }
    Ok(())
}

fn history_common(descriptor: &CoreMetaGenerationDescriptor) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("system/coremeta-history/{}", descriptor.root_key_hash),
        descriptor.root_key_hash.clone(),
        descriptor.generation,
        descriptor.transaction_id.clone(),
        descriptor.created_at_unix_nanos,
    )
}

fn generation_descriptor_prefix(root_key_hash: &str) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[CoreMetaTuplePart::Hash(root_key_hash)])
}

fn generation_descriptor_key(root_key_hash: &str, generation: u64) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Hash(root_key_hash),
        CoreMetaTuplePart::U64(generation),
    ])
}

fn generation_mutation_prefix(root_key_hash: &str, generation: u64) -> Result<Vec<u8>> {
    generation_descriptor_key(root_key_hash, generation)
}

fn generation_mutation_key(root_key_hash: &str, generation: u64, ordinal: u64) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Hash(root_key_hash),
        CoreMetaTuplePart::U64(generation),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

fn generation_envelope_chunk_prefix(
    root_key_hash: &str,
    generation: u64,
    mutation_ordinal: u64,
) -> Result<Vec<u8>> {
    generation_mutation_key(root_key_hash, generation, mutation_ordinal)
}

fn generation_envelope_chunk_key(
    root_key_hash: &str,
    generation: u64,
    mutation_ordinal: u64,
    chunk_ordinal: u32,
) -> Result<Vec<u8>> {
    validate_history_root(root_key_hash)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Hash(root_key_hash),
        CoreMetaTuplePart::U64(generation),
        CoreMetaTuplePart::U64(mutation_ordinal),
        CoreMetaTuplePart::U64(u64::from(chunk_ordinal)),
    ])
}

fn generation_hash(descriptor: &CoreMetaGenerationDescriptor, row_hashes: &[String]) -> String {
    let mut bytes = Vec::new();
    append_hash_part(&mut bytes, b"anvil.coremeta.generation.v1");
    append_hash_part(&mut bytes, descriptor.root_key_hash.as_bytes());
    append_hash_part(&mut bytes, &descriptor.generation.to_be_bytes());
    append_hash_part(&mut bytes, descriptor.transaction_id.as_bytes());
    append_hash_part(&mut bytes, descriptor.pending_batch_hash.as_bytes());
    append_hash_part(&mut bytes, descriptor.committed_batch_hash.as_bytes());
    append_hash_part(&mut bytes, descriptor.certificate_hash.as_bytes());
    append_hash_part(&mut bytes, &descriptor.commit_certificate);
    for evidence in &descriptor.certificate_persist_evidence {
        append_hash_part(&mut bytes, evidence.evidence_hash.as_bytes());
        append_hash_part(&mut bytes, &evidence.evidence);
    }
    append_hash_part(&mut bytes, &descriptor.mutation_count.to_be_bytes());
    append_hash_part(&mut bytes, &descriptor.mutation_bytes.to_be_bytes());
    append_hash_part(
        &mut bytes,
        descriptor
            .coordinator_root_key_hash
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    append_hash_part(
        &mut bytes,
        &descriptor
            .coordinator_root_generation
            .unwrap_or_default()
            .to_be_bytes(),
    );
    append_hash_part(&mut bytes, &descriptor.publication_bundle);
    for summary in &descriptor.column_families {
        append_hash_part(&mut bytes, summary.column_family.as_bytes());
        append_hash_part(&mut bytes, &summary.mutation_count.to_be_bytes());
        append_hash_part(&mut bytes, &summary.mutation_bytes.to_be_bytes());
        append_hash_part(&mut bytes, summary.slice_hash.as_bytes());
    }
    for (ordinal, row_hash) in row_hashes.iter().enumerate() {
        append_hash_part(&mut bytes, &(ordinal as u64).to_be_bytes());
        append_hash_part(&mut bytes, row_hash.as_bytes());
    }
    digest_bytes(&bytes)
}

fn column_family_slice_hash(column_family: &str, entries: &[(u64, String)]) -> String {
    let mut bytes = Vec::new();
    append_hash_part(&mut bytes, column_family.as_bytes());
    for (ordinal, row_hash) in entries {
        append_hash_part(&mut bytes, &ordinal.to_be_bytes());
        append_hash_part(&mut bytes, row_hash.as_bytes());
    }
    digest_domain("anvil.coremeta.generation.column_family.v1", &bytes)
}

fn verify_descriptor_batch_identity(
    descriptor: &CoreMetaGenerationDescriptor,
    row_hashes: &[String],
) -> Result<()> {
    let api_certificate = decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
        &descriptor.commit_certificate,
        "CoreMeta generation descriptor commit certificate",
    )?;
    let certificate =
        super::local_coremeta_quorum::api_commit_certificate_to_core(api_certificate)?;
    if certificate.root_key_hash != descriptor.root_key_hash
        || certificate.post_root_generation != descriptor.generation
        || certificate.transaction_id != descriptor.transaction_id
        || certificate.pending_batch_hash != descriptor.pending_batch_hash
        || certificate.certificate_hash != descriptor.certificate_hash
    {
        bail!("CoreMeta generation descriptor certificate scope mismatch");
    }
    let pending = pending_batch_hash(&CoreMetaPendingBatchInput {
        root_key_hash: descriptor.root_key_hash.clone(),
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: descriptor.generation,
        transaction_id: descriptor.transaction_id.clone(),
        row_hashes: row_hashes.to_vec(),
    })?;
    if pending != descriptor.pending_batch_hash {
        bail!("CoreMeta generation descriptor pending batch identity mismatch");
    }
    let committed = committed_batch_hash(&CoreMetaCommittedBatchInput {
        root_key_hash: descriptor.root_key_hash.clone(),
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: descriptor.generation,
        transaction_id: descriptor.transaction_id.clone(),
        pending_batch_hash: descriptor.pending_batch_hash.clone(),
        committed_row_hashes: row_hashes.to_vec(),
    })?;
    if committed != descriptor.committed_batch_hash {
        bail!("CoreMeta generation descriptor committed batch identity mismatch");
    }
    if crate::core_store::commit_certificate_hash(&certificate)? != descriptor.certificate_hash {
        bail!("CoreMeta generation descriptor certificate identity mismatch");
    }
    Ok(())
}

pub(super) fn catch_up_frame_hash(frame: &CoreMetaBatchFrame) -> String {
    let mut canonical = frame.clone();
    canonical.frame_hash.clear();
    digest_domain(
        "anvil.coremeta.catch_up_frame.v1",
        &canonical.encode_to_vec(),
    )
}

pub(in crate::core_store::local) fn inventory_page_hash(
    root_key_hash: &str,
    descriptors: &[CoreMetaGenerationDescriptor],
) -> String {
    let mut bytes = Vec::new();
    append_hash_part(&mut bytes, root_key_hash.as_bytes());
    for descriptor in descriptors {
        append_hash_part(&mut bytes, &descriptor.encode_to_vec());
    }
    digest_domain("anvil.coremeta.inventory_page.v1", &bytes)
}

fn mutation_size(row: &CoreMetaRowMutation) -> u64 {
    (row.column_family.len() as u64)
        .saturating_add(row.core_meta_key.len() as u64)
        .saturating_add(row.value_envelope.len() as u64)
        .saturating_add(1)
}

fn append_hash_part(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

fn digest_domain(domain: &str, value: &[u8]) -> String {
    let mut bytes = Vec::new();
    append_hash_part(&mut bytes, domain.as_bytes());
    append_hash_part(&mut bytes, value);
    digest_bytes(&bytes)
}

fn digest_bytes(value: &[u8]) -> String {
    format!("sha256:{}", sha256_hex(value))
}

fn validate_digest_family(value: &str, expected_algorithm: &str, label: &str) -> Result<()> {
    let Some((actual_algorithm, digest)) = value.split_once(':') else {
        bail!("CoreMeta history {label} must use algorithm:hex encoding");
    };
    if actual_algorithm != expected_algorithm
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("CoreMeta history {label} must be a {expected_algorithm} digest");
    }
    Ok(())
}

fn validate_sha256_digest(value: &str, label: &str) -> Result<()> {
    validate_digest_family(value, "sha256", label)
}

fn validate_blake3_digest(value: &str, label: &str) -> Result<()> {
    validate_digest_family(value, "blake3", label)
}

fn validate_history_root(root_key_hash: &str) -> Result<()> {
    validate_sha256_digest(root_key_hash, "root key hash")
}

fn validate_history_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 1024
        || value.chars().any(char::is_control)
        || value.trim() != value
    {
        bail!("CoreMeta history {label} is not canonical");
    }
    Ok(())
}

fn validate_page_bounds(rows: usize, bytes: u64, label: &str) -> Result<()> {
    if rows == 0 || rows > HISTORY_MAX_PAGE_ROWS {
        bail!("{label} row limit must be between 1 and {HISTORY_MAX_PAGE_ROWS}");
    }
    if !(HISTORY_MIN_PAGE_BYTES..=HISTORY_MAX_PAGE_BYTES).contains(&bytes) {
        bail!(
            "{label} byte limit must be between {HISTORY_MIN_PAGE_BYTES} and {HISTORY_MAX_PAGE_BYTES}"
        );
    }
    Ok(())
}

fn capture_final_generation(requested: u64, latest: u64) -> Result<u64> {
    if requested == 0 {
        return Ok(latest);
    }
    if requested > latest {
        bail!("CoreMeta history through_generation is newer than the latest retained generation");
    }
    Ok(requested)
}

fn validate_history_cursor(
    cursor: Option<&CoreMetaHistoryCursor>,
    retention_floor: u64,
    final_generation: u64,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.generation == 0 || cursor.generation > final_generation {
        bail!("CoreMeta catch-up cursor generation is outside the captured history");
    }
    if cursor.generation < retention_floor {
        bail!("CoreMeta catch-up cursor predates the retention floor");
    }
    Ok(())
}

fn validate_inventory_cursor(
    cursor: Option<&CoreMetaInventoryCursor>,
    retention_floor: u64,
    final_generation: u64,
) -> Result<()> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.generation == 0 || cursor.generation > final_generation {
        bail!("CoreMeta inventory cursor generation is outside the captured history");
    }
    if cursor.generation.saturating_add(1) < retention_floor {
        bail!("CoreMeta inventory cursor predates the retention floor");
    }
    Ok(())
}

fn empty_catch_up_frame(root_key_hash: &str) -> CoreMetaBatchFrame {
    let mut frame = CoreMetaBatchFrame {
        descriptor: None,
        mutations: Vec::new(),
        next_cursor: None,
        generation_complete: true,
        history_complete: true,
        final_generation: 0,
        retention_floor_generation: 0,
        encoded_bytes: 0,
        frame_hash: String::new(),
    };
    frame.frame_hash = digest_domain("anvil.coremeta.empty_catch_up.v1", root_key_hash.as_bytes());
    frame
}

fn completed_catch_up_frame(
    root_key_hash: &str,
    final_generation: u64,
    retention_floor: u64,
    cursor: Option<CoreMetaHistoryCursor>,
) -> CoreMetaBatchFrame {
    let mut frame = CoreMetaBatchFrame {
        descriptor: None,
        mutations: Vec::new(),
        next_cursor: cursor,
        generation_complete: true,
        history_complete: true,
        final_generation,
        retention_floor_generation: retention_floor,
        encoded_bytes: 0,
        frame_hash: String::new(),
    };
    frame.frame_hash = catch_up_frame_hash(&frame);
    frame
}

fn empty_inventory(root_key_hash: &str) -> CoreMetaInventory {
    CoreMetaInventory {
        root_key_hash: root_key_hash.to_string(),
        descriptors: Vec::new(),
        next_cursor: None,
        inventory_complete: true,
        retention_floor_generation: 0,
        final_generation: 0,
        page_hash: digest_domain(
            "anvil.coremeta.empty_inventory.v1",
            root_key_hash.as_bytes(),
        ),
        encoded_bytes: 0,
    }
}

fn completed_inventory(
    root_key_hash: &str,
    retention_floor: u64,
    final_generation: u64,
    cursor: Option<CoreMetaInventoryCursor>,
) -> CoreMetaInventory {
    CoreMetaInventory {
        root_key_hash: root_key_hash.to_string(),
        descriptors: Vec::new(),
        next_cursor: cursor,
        inventory_complete: true,
        retention_floor_generation: retention_floor,
        final_generation,
        page_hash: digest_domain(
            "anvil.coremeta.completed_inventory.v1",
            &final_generation.to_be_bytes(),
        ),
        encoded_bytes: 0,
    }
}
