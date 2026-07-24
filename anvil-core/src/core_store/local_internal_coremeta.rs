use super::*;
use prost::Message;

fn validate_coremeta_digest(value: &str, label: &str) -> Result<()> {
    let Some((algorithm, digest)) = value.split_once(':') else {
        bail!("{label} must use algorithm:hex encoding");
    };
    if !matches!(algorithm, "sha256" | "blake3")
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("{label} must be a sha256 or blake3 digest");
    }
    Ok(())
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommitEvidenceRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    certificate_hash: String,
    #[prost(string, tag = "3")]
    committed_batch_hash: String,
    #[prost(bytes, tag = "4")]
    certificate_bytes: Vec<u8>,
    #[prost(string, repeated, tag = "5")]
    certificate_persist_receipt_hashes: Vec<String>,
    #[prost(bytes = "vec", repeated, tag = "6")]
    certificate_persist_receipt_bytes: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaPendingBatchMarkerRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    pending_batch_hash: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "5")]
    post_root_generation: u64,
    #[prost(string, tag = "6")]
    transaction_id: String,
    #[prost(uint64, tag = "7")]
    core_meta_row_count: u64,
}

#[derive(Debug, Clone)]
pub(super) struct CoreMetaCommitEvidenceRecord {
    pub(super) certificate_hash: String,
    pub(super) committed_batch_hash: String,
    pub(super) certificate_bytes: Vec<u8>,
    pub(super) certificate_persist_receipt_hashes: Vec<String>,
    pub(super) certificate_persist_receipt_bytes: Vec<Vec<u8>>,
}

#[derive(Debug, Default)]
pub(super) struct CoreMetaVisibilityCache {
    decisions: BTreeMap<(String, u64, String), bool>,
}

impl CoreStore {
    pub(crate) fn encode_coremeta_batch_ops(
        &self,
        ops: &[CoreMetaBatchOp<'_>],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        self.meta.encode_batch_ops(ops)
    }

    pub(crate) fn read_coremeta_row(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.read_coremeta_row_with_visibility_cache(
            cf,
            table_id,
            tuple_key,
            &mut CoreMetaVisibilityCache::default(),
        )
    }

    pub(super) fn read_coremeta_row_with_visibility_cache(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
        visibility_cache: &mut CoreMetaVisibilityCache,
    ) -> Result<Option<Vec<u8>>> {
        let Some(payload) = self.meta.get(cf, table_id, tuple_key)? else {
            return Ok(None);
        };
        if self.coremeta_payload_is_committed_visible_with_cache(
            cf,
            table_id,
            &payload,
            visibility_cache,
        )? {
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn scan_coremeta_prefix_page(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
        after_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_visible_scan_limit(limit)?;
        let mut visible = Vec::with_capacity(limit);
        let mut physical_after = after_tuple_key.map(ToOwned::to_owned);
        let mut remaining_candidates = CORE_META_MAX_SCAN_PAGE_ROWS;

        while visible.len() < limit && remaining_candidates > 0 {
            let wanted = limit.saturating_sub(visible.len());
            let chunk_limit = wanted.max(64).min(remaining_candidates);
            let page = self.meta.scan_prefix_page(
                cf,
                table_id,
                tuple_prefix,
                physical_after.as_deref(),
                chunk_limit,
            )?;
            if page.is_empty() {
                return Ok(visible);
            }

            let physical_count = page.len();
            remaining_candidates = remaining_candidates.saturating_sub(physical_count);
            let next_physical_after = page
                .last()
                .map(|record| core_meta_record_tuple_key(&record.key).map(ToOwned::to_owned))
                .transpose()?;
            for record in page {
                if self.coremeta_payload_is_committed_visible(cf, table_id, &record.payload)? {
                    visible.push(record);
                    if visible.len() == limit {
                        return Ok(visible);
                    }
                }
            }
            if physical_count < chunk_limit {
                return Ok(visible);
            }
            physical_after = next_physical_after;
        }

        bail!("CoreMeta visible prefix page exhausted its bounded physical candidate budget")
    }

    pub(crate) fn scan_coremeta_prefix_reverse_page(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
        before_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_visible_scan_limit(limit)?;
        let mut visible = Vec::with_capacity(limit);
        let mut physical_before = before_tuple_key.map(ToOwned::to_owned);
        let mut remaining_candidates = CORE_META_MAX_SCAN_PAGE_ROWS;

        while visible.len() < limit && remaining_candidates > 0 {
            let wanted = limit.saturating_sub(visible.len());
            let chunk_limit = wanted.max(64).min(remaining_candidates);
            let page = self.meta.scan_prefix_reverse_page(
                cf,
                table_id,
                tuple_prefix,
                physical_before.as_deref(),
                chunk_limit,
            )?;
            if page.is_empty() {
                return Ok(visible);
            }

            let physical_count = page.len();
            remaining_candidates = remaining_candidates.saturating_sub(physical_count);
            let next_physical_before = page
                .last()
                .map(|record| core_meta_record_tuple_key(&record.key).map(ToOwned::to_owned))
                .transpose()?;
            for record in page {
                if self.coremeta_payload_is_committed_visible(cf, table_id, &record.payload)? {
                    visible.push(record);
                    if visible.len() == limit {
                        return Ok(visible);
                    }
                }
            }
            if physical_count < chunk_limit {
                return Ok(visible);
            }
            physical_before = next_physical_before;
        }

        bail!(
            "CoreMeta visible reverse prefix page exhausted its bounded physical candidate budget"
        )
    }

    pub(crate) fn scan_coremeta_range_inclusive(
        &self,
        cf: &'static str,
        table_id: u16,
        start_tuple_key: &[u8],
        end_tuple_key: &[u8],
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_visible_scan_limit(limit)?;
        let page = self.meta.scan_range_inclusive(
            cf,
            table_id,
            start_tuple_key,
            end_tuple_key,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )?;
        let physical_count = page.len();
        let visible = page
            .into_iter()
            .filter_map(|record| {
                match self.coremeta_payload_is_committed_visible(cf, table_id, &record.payload) {
                    Ok(true) => Some(Ok(record)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .take(limit)
            .collect::<Result<Vec<_>>>()?;
        if visible.len() < limit && physical_count == CORE_META_MAX_SCAN_PAGE_ROWS {
            bail!("CoreMeta visible range exhausted its bounded physical candidate budget");
        }
        Ok(visible)
    }

    pub(crate) fn write_coremeta_encoded_rows(
        &self,
        rows: &[CoreMetaEncodedRow<'_>],
    ) -> Result<()> {
        self.meta.write_encoded_rows(rows)
    }

    pub(crate) fn decode_coremeta_encoded_owned_row(
        &self,
        row: &CoreMetaEncodedOwnedRow,
    ) -> Result<(u16, Vec<u8>, Vec<u8>)> {
        CoreMetaStore::decode_encoded_owned_row(row)
    }

    pub(crate) fn validate_and_own_coremeta_encoded_rows(
        &self,
        rows: &[CoreMetaEncodedRow<'_>],
        delete_common: Option<&CoreMetaRowCommonProto>,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        self.meta.validate_and_own_encoded_rows(rows, delete_common)
    }

    pub fn export_coremeta_snapshot_rows_page(
        &self,
        after: Option<&crate::core_store::CoreMetaEncodedRowsCursor>,
        limit: usize,
    ) -> Result<crate::core_store::CoreMetaEncodedRowsPage> {
        self.meta.scan_encoded_rows_page(after, limit)
    }

    /// Export one bounded, point-in-time canonical bootstrap snapshot. The
    /// RocksDB snapshot is retained across every column family and visibility
    /// decision so a joining peer cannot receive a torn publication closure.
    pub fn export_portable_coremeta_bootstrap_rows(
        &self,
        max_physical_rows: usize,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        validate_visible_scan_limit(max_physical_rows)?;
        const PAGE_ROWS: usize = 512;

        let snapshot = self.meta.read_snapshot();
        let mut rows = Vec::new();
        let mut cursor = None;
        loop {
            let page = snapshot.scan_encoded_rows_page(cursor.as_ref(), PAGE_ROWS)?;
            if rows.len().saturating_add(page.rows.len()) > max_physical_rows {
                bail!(
                    "portable CoreMeta bootstrap snapshot exceeds its {max_physical_rows}-row physical scan budget"
                );
            }
            rows.extend(page.rows);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if cursor.as_ref().is_some_and(|current| {
                current.cf == next_cursor.cf
                    && current.core_meta_key.as_slice() >= next_cursor.core_meta_key.as_slice()
            }) {
                bail!("portable CoreMeta bootstrap cursor did not advance");
            }
            cursor = Some(next_cursor);
        }

        let mut visible = Vec::with_capacity(rows.len());
        let mut visibility_cache = CoreMetaVisibilityCache::default();
        for row in rows {
            if row.delete_marker || !is_portable_bootstrap_row(&row) {
                continue;
            }
            let cf = canonical_coremeta_cf_name(&row.cf)?;
            let (table_id, _, payload) = self.decode_coremeta_encoded_owned_row(&row)?;
            if self.coremeta_payload_is_committed_visible_from(
                &snapshot,
                cf,
                table_id,
                &payload,
                &mut visibility_cache,
            )? {
                visible.push(row);
            }
        }
        Ok(visible)
    }

    pub(crate) fn install_coremeta_snapshot_rows(
        &self,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<()> {
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

    pub fn install_portable_coremeta_bootstrap_rows(
        &self,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<()> {
        if rows.is_empty() {
            bail!("portable CoreMeta bootstrap snapshot must not be empty");
        }
        if rows
            .iter()
            .any(|row| row.delete_marker || !is_portable_bootstrap_row(row))
        {
            bail!("portable CoreMeta bootstrap snapshot contains node-local state");
        }

        let incoming_keys = rows
            .iter()
            .map(|row| (row.cf.clone(), row.core_meta_key.clone()))
            .collect::<BTreeSet<_>>();
        if incoming_keys.len() != rows.len() {
            bail!("portable CoreMeta bootstrap snapshot contains duplicate rows");
        }

        crate::mesh_lifecycle::validate_portable_lifecycle_topology_snapshot(self, rows)
            .map_err(anyhow::Error::from)?;

        // A joining node has already created generation-zero local bootstrap
        // state so its admin plane can start. Canonical bootstrap installation
        // replaces that portable state rather than merging two independently
        // signed root histories. Node-private and runtime-local rows survive.
        const MAX_EXISTING_BOOTSTRAP_ROWS: usize = 65_536;
        const MAX_BOOTSTRAP_REPLACEMENT_ROWS: usize = 16_384;
        let mut replacement = Vec::with_capacity(rows.len());
        let mut cursor = None;
        let mut examined = 0_usize;
        loop {
            let page = self.export_coremeta_snapshot_rows_page(cursor.as_ref(), 1_024)?;
            examined = examined.saturating_add(page.rows.len());
            if examined > MAX_EXISTING_BOOTSTRAP_ROWS {
                bail!(
                    "existing CoreMeta state exceeds the bounded portable bootstrap replacement budget"
                );
            }
            for existing in page.rows {
                if is_portable_bootstrap_row(&existing)
                    && !incoming_keys
                        .contains(&(existing.cf.clone(), existing.core_meta_key.clone()))
                {
                    replacement.push(CoreMetaEncodedOwnedRow {
                        cf: existing.cf,
                        core_meta_key: existing.core_meta_key,
                        value_envelope: Vec::new(),
                        delete_marker: true,
                        root_key_hash: String::new(),
                        root_generation: 0,
                        visibility_state: CoreMetaVisibilityState::Committed,
                    });
                }
            }
            if replacement.len().saturating_add(rows.len()) > MAX_BOOTSTRAP_REPLACEMENT_ROWS {
                bail!("portable CoreMeta bootstrap replacement exceeds its bounded row budget");
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        replacement.extend_from_slice(rows);

        let borrowed = replacement
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

    fn coremeta_payload_is_committed_visible(
        &self,
        cf: &'static str,
        table_id: u16,
        payload: &[u8],
    ) -> Result<bool> {
        self.coremeta_payload_is_committed_visible_with_cache(
            cf,
            table_id,
            payload,
            &mut CoreMetaVisibilityCache::default(),
        )
    }

    fn coremeta_payload_is_committed_visible_with_cache(
        &self,
        cf: &'static str,
        table_id: u16,
        payload: &[u8],
        visibility_cache: &mut CoreMetaVisibilityCache,
    ) -> Result<bool> {
        self.coremeta_payload_is_committed_visible_from(
            &self.meta,
            cf,
            table_id,
            payload,
            visibility_cache,
        )
    }

    fn coremeta_payload_is_committed_visible_from<R: CoreMetaReader>(
        &self,
        reader: &R,
        _cf: &'static str,
        _table_id: u16,
        payload: &[u8],
        visibility_cache: &mut CoreMetaVisibilityCache,
    ) -> Result<bool> {
        let common = core_meta_row_common_from_payload(payload)?;
        if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
            return Ok(false);
        }

        // Local-only rows are made durable by the local RocksDB batch and do
        // not participate in root publication.
        if common.root_key_hash.is_empty() {
            return Ok(common.root_generation == 0);
        }

        if common.root_generation == 0 || common.transaction_id.is_empty() {
            return Ok(false);
        }
        let decision_key = (
            common.root_key_hash.clone(),
            common.root_generation,
            common.transaction_id.clone(),
        );
        if let Some(decision) = visibility_cache.decisions.get(&decision_key) {
            return Ok(*decision);
        }
        if !self.root_generation_is_published_from(
            reader,
            &common.root_key_hash,
            common.root_generation,
            &common.transaction_id,
        )? {
            visibility_cache.decisions.insert(decision_key, false);
            return Ok(false);
        }

        // Implicit mutations have no explicit transaction header. When one
        // exists, it is the visibility coordinator and must itself be both
        // committed and published at its exact generation.
        let Some(header) =
            self.read_transaction_header_row_unlocked_from(reader, &common.transaction_id)?
        else {
            visibility_cache.decisions.insert(decision_key, true);
            return Ok(true);
        };
        if header.transaction.state != CoreTransactionState::Committed {
            visibility_cache.decisions.insert(decision_key, false);
            return Ok(false);
        }
        let Some(coordinator_generation) = header.transaction.committed_root_generation else {
            visibility_cache.decisions.insert(decision_key, false);
            return Ok(false);
        };
        let decision = self.root_generation_is_published_from(
            reader,
            &header.transaction.root_key_hash,
            coordinator_generation,
            &header.transaction.transaction_id,
        )?;
        visibility_cache.decisions.insert(decision_key, decision);
        Ok(decision)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn persist_coremeta_commit_evidence_at(
        &self,
        root_key_hash: &str,
        root_generation: u64,
        transaction_id: &str,
        certificate_hash: &str,
        committed_batch_hash: &str,
        certificate_bytes: Vec<u8>,
        certificate_persist_receipt_hashes: Vec<String>,
        certificate_persist_receipt_bytes: Vec<Vec<u8>>,
        created_at_unix_nanos: u64,
    ) -> Result<()> {
        let row = self.coremeta_commit_evidence_encoded_row_at(
            root_key_hash,
            root_generation,
            transaction_id,
            certificate_hash,
            committed_batch_hash,
            certificate_bytes,
            certificate_persist_receipt_hashes,
            certificate_persist_receipt_bytes,
            created_at_unix_nanos,
        )?;
        let borrowed = [CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        }];
        self.write_coremeta_encoded_rows(&borrowed)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn coremeta_commit_evidence_encoded_row_at(
        &self,
        root_key_hash: &str,
        root_generation: u64,
        transaction_id: &str,
        certificate_hash: &str,
        committed_batch_hash: &str,
        certificate_bytes: Vec<u8>,
        certificate_persist_receipt_hashes: Vec<String>,
        certificate_persist_receipt_bytes: Vec<Vec<u8>>,
        created_at_unix_nanos: u64,
    ) -> Result<CoreMetaEncodedOwnedRow> {
        if created_at_unix_nanos == 0 {
            bail!("CoreMeta commit evidence timestamp must be nonzero");
        }
        if certificate_persist_receipt_hashes.len() != certificate_persist_receipt_bytes.len() {
            bail!("CoreMeta certificate persistence evidence hashes and payloads differ in length");
        }
        let mut expected_hashes = certificate_persist_receipt_hashes;
        expected_hashes.sort();
        if expected_hashes.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("CoreMeta certificate persistence evidence contains a duplicate hash");
        }
        let mut certificate_persist_evidence = certificate_persist_receipt_bytes
            .into_iter()
            .map(|evidence| {
                let api_receipt =
                    decode_deterministic_proto::<
                        crate::anvil_api::CoreMetaCertificatePersistReceipt,
                    >(&evidence, "CoreMeta certificate persistence evidence")?;
                let receipt =
                    super::local_coremeta_quorum::api_persist_receipt_to_core(api_receipt)?;
                let evidence_hash = certificate_persist_receipt_payload_hash(&receipt)?;
                Ok((evidence_hash, evidence))
            })
            .collect::<Result<Vec<_>>>()?;
        certificate_persist_evidence.sort_by(|left, right| left.0.cmp(&right.0));
        if certificate_persist_evidence
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0)
        {
            bail!("CoreMeta certificate persistence evidence contains a duplicate hash");
        }
        let actual_hashes = certificate_persist_evidence
            .iter()
            .map(|(hash, _)| hash.clone())
            .collect::<Vec<_>>();
        if actual_hashes != expected_hashes {
            bail!("CoreMeta certificate persistence evidence hashes do not match their payloads");
        }
        let (certificate_persist_receipt_hashes, certificate_persist_receipt_bytes) =
            certificate_persist_evidence.into_iter().unzip();
        let payload = encode_deterministic_proto(&CoreMetaCommitEvidenceRowProto {
            common: Some(core_meta_committed_row_common(
                "system",
                root_key_hash,
                root_generation,
                transaction_id,
                created_at_unix_nanos,
            )),
            certificate_hash: certificate_hash.to_string(),
            committed_batch_hash: committed_batch_hash.to_string(),
            certificate_bytes,
            certificate_persist_receipt_hashes,
            certificate_persist_receipt_bytes,
        });
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("coremeta-commit-evidence"),
            CoreMetaTuplePart::Hash(certificate_hash),
        ])?;
        let ops = [CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }];
        self.meta
            .encode_batch_ops(&ops)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("CoreMeta commit evidence encoding produced no row"))
    }

    pub fn persist_coremeta_pending_batch_marker(
        &self,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        pending_batch_hash: &str,
        core_meta_row_count: usize,
    ) -> Result<()> {
        let row = self.coremeta_pending_batch_marker_encoded_row(
            root_key_hash,
            expected_root_generation,
            post_root_generation,
            transaction_id,
            pending_batch_hash,
            core_meta_row_count,
        )?;
        let borrowed = [CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        }];
        self.write_coremeta_encoded_rows(&borrowed)
    }

    pub(crate) fn coremeta_pending_batch_marker_encoded_row(
        &self,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        pending_batch_hash: &str,
        core_meta_row_count: usize,
    ) -> Result<CoreMetaEncodedOwnedRow> {
        validate_hash(root_key_hash, "CoreMeta pending batch root key hash")?;
        validate_coremeta_digest(pending_batch_hash, "CoreMeta pending batch hash")?;
        validate_logical_id(transaction_id, "CoreMeta pending batch transaction id")?;
        let payload = encode_deterministic_proto(&CoreMetaPendingBatchMarkerRowProto {
            common: Some(core_meta_pending_row_common(
                "system/coremeta-pending",
                root_key_hash,
                post_root_generation,
                transaction_id,
                unix_timestamp_nanos(),
            )),
            pending_batch_hash: pending_batch_hash.to_string(),
            root_key_hash: root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id: transaction_id.to_string(),
            core_meta_row_count: core_meta_row_count as u64,
        });
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("coremeta-pending-batch"),
            CoreMetaTuplePart::Hash(root_key_hash),
            CoreMetaTuplePart::U64(post_root_generation),
            CoreMetaTuplePart::Utf8(transaction_id),
            CoreMetaTuplePart::Hash(pending_batch_hash),
        ])?;
        let ops = [CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_TRANSACTION_LOCATOR_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }];
        self.meta
            .encode_batch_ops(&ops)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("CoreMeta pending batch marker encoding produced no row"))
    }

    pub(super) fn read_coremeta_commit_evidence(
        &self,
        certificate_hash: &str,
    ) -> Result<Option<CoreMetaCommitEvidenceRecord>> {
        self.read_coremeta_commit_evidence_from(&self.meta, certificate_hash)
    }

    pub(super) fn read_coremeta_commit_evidence_from<R: CoreMetaReader>(
        &self,
        reader: &R,
        certificate_hash: &str,
    ) -> Result<Option<CoreMetaCommitEvidenceRecord>> {
        validate_coremeta_digest(
            certificate_hash,
            "CoreMeta commit evidence certificate hash",
        )?;
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("coremeta-commit-evidence"),
            CoreMetaTuplePart::Hash(certificate_hash),
        ])?;
        let Some(payload) = reader.get(
            CF_TRANSACTIONS,
            TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW,
            &tuple_key,
        )?
        else {
            return Ok(None);
        };
        let row = decode_deterministic_proto::<CoreMetaCommitEvidenceRowProto>(
            &payload,
            "CoreMeta commit evidence row",
        )?;
        if row.certificate_hash != certificate_hash {
            bail!("CoreMeta commit evidence certificate hash scope mismatch");
        }
        validate_coremeta_digest(&row.committed_batch_hash, "CoreMeta committed batch hash")?;
        if row.certificate_bytes.is_empty() {
            bail!("CoreMeta commit evidence is missing certificate bytes");
        }
        for receipt_hash in &row.certificate_persist_receipt_hashes {
            validate_coremeta_digest(receipt_hash, "CoreMeta certificate persist receipt hash")?;
        }
        if row.certificate_persist_receipt_bytes.len()
            < row.certificate_persist_receipt_hashes.len()
        {
            bail!("CoreMeta commit evidence is missing certificate persistence receipt bytes");
        }
        Ok(Some(CoreMetaCommitEvidenceRecord {
            certificate_hash: row.certificate_hash,
            committed_batch_hash: row.committed_batch_hash,
            certificate_bytes: row.certificate_bytes,
            certificate_persist_receipt_hashes: row.certificate_persist_receipt_hashes,
            certificate_persist_receipt_bytes: row.certificate_persist_receipt_bytes,
        }))
    }
}

fn validate_visible_scan_limit(limit: usize) -> Result<()> {
    if !(1..=CORE_META_MAX_SCAN_PAGE_ROWS).contains(&limit) {
        bail!("CoreMeta visible scan limit must be between 1 and {CORE_META_MAX_SCAN_PAGE_ROWS}");
    }
    Ok(())
}

fn is_node_local_bootstrap_row(row: &CoreMetaEncodedOwnedRow) -> bool {
    if row.cf != CF_MESH {
        return false;
    }
    let Some(table_id) = encoded_coremeta_table_id(row) else {
        return false;
    };
    let Ok(tuple_key) = crate::core_store::core_meta_record_tuple_key(&row.core_meta_key) else {
        return false;
    };
    let expected_tuple = match table_id {
        TABLE_NODE_SIGNING_KEYPAIR_ROW => {
            core_meta_tuple_key(&[CoreMetaTuplePart::Raw(b"node-signing-keypair")])
        }
        TABLE_LOCAL_NODE_IDENTITY_ROW => core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("node-identity"),
            CoreMetaTuplePart::Utf8("local"),
        ]),
        _ => return false,
    };
    expected_tuple.is_ok_and(|local_tuple| tuple_key == local_tuple.as_slice())
}

fn is_runtime_local_bootstrap_row(row: &CoreMetaEncodedOwnedRow) -> bool {
    row.cf == CF_LEASES_FENCES
        || matches!(
            (row.cf.as_str(), encoded_coremeta_table_id(row)),
            (CF_MATERIALISATION, Some(TABLE_MATERIALISATION_CURSOR_ROW))
                | (
                    CF_MATERIALISATION,
                    Some(crate::core_store::TABLE_WRITER_SEGMENT_ROW)
                )
                | (
                    CF_MATERIALISATION,
                    Some(crate::core_store::TABLE_WRITER_HEAD_ROW)
                )
                | (
                    CF_TRANSACTIONS,
                    Some(local_coremeta_history::TABLE_COREMETA_GENERATION_INSTALL_ROW)
                )
                | (CF_TRANSACTIONS, Some(TABLE_ROOT_PUBLICATION_INTENT_ROW))
        )
}

fn is_portable_bootstrap_row(row: &CoreMetaEncodedOwnedRow) -> bool {
    !is_node_local_bootstrap_row(row) && !is_runtime_local_bootstrap_row(row)
}

fn encoded_coremeta_table_id(row: &CoreMetaEncodedOwnedRow) -> Option<u16> {
    (row.core_meta_key.len() >= 3)
        .then(|| u16::from_be_bytes([row.core_meta_key[1], row.core_meta_key[2]]))
}
