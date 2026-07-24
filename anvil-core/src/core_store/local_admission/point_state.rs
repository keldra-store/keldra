use super::*;
use crate::core_store::core_meta_record_tuple_key;
use prost::Message;

const ADMISSION_POINT_KIND: &str = "admission-accounting";
const ADMISSION_MUTATION_HEAD_KIND: &str = "admission-mutation-head";
const ADMISSION_IDEMPOTENCY_HEAD_KIND: &str = "admission-idempotency-head";
const LANDED_BYTE_HEAD_KIND: &str = "landed-byte-head";
const ADMISSION_MUTATION_ACTIVE: &str = "active";
const ADMISSION_MUTATION_FINALISED: &str = "finalised";
const ADMISSION_POINT_STABLE_READ_ATTEMPTS: usize = 16;
pub(super) const ADMISSION_RECOVERY_PAGE_ROWS: usize = 128;

#[derive(Clone, PartialEq, Message)]
struct AdmissionPointRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    point_kind: String,
    #[prost(uint64, tag = "4")]
    last_sequence: u64,
    #[prost(uint64, tag = "5")]
    pending_rows: u64,
    #[prost(uint64, tag = "6")]
    pending_bytes: u64,
    #[prost(uint64, tag = "7")]
    landed_bytes: u64,
    #[prost(uint64, optional, tag = "8")]
    oldest_pending_sequence: Option<u64>,
    #[prost(uint64, optional, tag = "9")]
    oldest_pending_created_at_unix_nanos: Option<u64>,
    #[prost(string, tag = "10")]
    mutation_id: String,
    #[prost(uint64, tag = "11")]
    mutation_sequence: u64,
    #[prost(string, tag = "12")]
    request_hash: String,
    #[prost(string, tag = "13")]
    mutation_state: String,
    #[prost(string, tag = "14")]
    landed_sha256: String,
    #[prost(string, tag = "15")]
    landed_relative_path: String,
    #[prost(uint64, tag = "16")]
    landed_length: u64,
    #[prost(uint64, tag = "17")]
    landed_reference_count: u64,
    #[prost(string, tag = "18")]
    admission_shard_hash: String,
    #[prost(string, tag = "19")]
    idempotency_key_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AdmissionPointState {
    pub(super) last_sequence: u64,
    pub(super) pending_rows: u64,
    pub(super) pending_bytes: u64,
    pub(super) landed_bytes: u64,
    pub(super) oldest_pending_sequence: Option<u64>,
    pub(super) oldest_pending_created_at_unix_nanos: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) struct AdmissionMutationHead {
    pub(super) mutation_id: String,
    pub(super) mutation_sequence: u64,
    pub(super) request_hash: String,
    pub(super) state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) struct AdmissionIdempotencyHead {
    pub(super) idempotency_key_hash: String,
    pub(super) mutation_id: String,
    pub(super) mutation_sequence: u64,
    pub(super) request_hash: String,
    pub(super) state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LandedByteHead {
    pub(super) sha256: String,
    pub(super) relative_path: String,
    pub(super) length: u64,
    pub(super) reference_count: u64,
}

pub(super) struct PendingMutationPageRow {
    pub(super) record: CorePendingMutationRecord,
    pub(super) inline_payload: Vec<u8>,
    pub(super) stored_bytes: u64,
    pub(super) tuple_key: Vec<u8>,
}

#[derive(Default)]
struct RecoveryShardState {
    pending_rows: u64,
    pending_bytes: u64,
    max_sequence: u64,
    oldest: Option<(u64, u64)>,
    landed_bytes: u64,
    landed_heads: BTreeMap<String, LandedByteHead>,
}

impl AdmissionPointState {
    pub(super) fn empty(last_sequence: u64) -> Self {
        Self {
            last_sequence,
            pending_rows: 0,
            pending_bytes: 0,
            landed_bytes: 0,
            oldest_pending_sequence: None,
            oldest_pending_created_at_unix_nanos: None,
        }
    }

    pub(super) fn after_admission(
        &self,
        record: &CorePendingMutationRecord,
        stored_bytes: u64,
        newly_landed_bytes: u64,
    ) -> Result<Self> {
        if record.sequence
            != self
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("CoreStore pending mutation sequence overflow"))?
        {
            bail!("CoreStore admission shard sequence is not contiguous");
        }
        let mut next = self.clone();
        next.last_sequence = record.sequence;
        next.pending_rows = next
            .pending_rows
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore admission pending row counter overflow"))?;
        next.pending_bytes = next
            .pending_bytes
            .checked_add(stored_bytes)
            .ok_or_else(|| anyhow!("CoreStore admission pending byte counter overflow"))?;
        next.landed_bytes = next
            .landed_bytes
            .checked_add(newly_landed_bytes)
            .ok_or_else(|| anyhow!("CoreStore admission landed byte counter overflow"))?;
        if self.pending_rows == 0 {
            next.oldest_pending_sequence = Some(record.sequence);
            next.oldest_pending_created_at_unix_nanos = Some(record.created_at_unix_nanos);
        }
        validate_admission_point_state(&next)?;
        Ok(next)
    }

    pub(super) fn after_finalisation(
        &self,
        record: &CorePendingMutationRecord,
        stored_bytes: u64,
        removed_landed_bytes: u64,
        next_oldest: Option<&CorePendingMutationRecord>,
    ) -> Result<Self> {
        if self.pending_rows == 0 {
            bail!("CoreStore admission point state cannot finalise an empty shard");
        }
        let mut next = self.clone();
        next.pending_rows = next
            .pending_rows
            .checked_sub(1)
            .ok_or_else(|| anyhow!("CoreStore admission pending row counter underflow"))?;
        next.pending_bytes = next
            .pending_bytes
            .checked_sub(stored_bytes)
            .ok_or_else(|| anyhow!("CoreStore admission pending byte counter underflow"))?;
        next.landed_bytes = next
            .landed_bytes
            .checked_sub(removed_landed_bytes)
            .ok_or_else(|| anyhow!("CoreStore admission landed byte counter underflow"))?;

        if self.oldest_pending_sequence == Some(record.sequence) {
            match (next.pending_rows, next_oldest) {
                (0, None) => {
                    next.oldest_pending_sequence = None;
                    next.oldest_pending_created_at_unix_nanos = None;
                }
                (0, Some(_)) => {
                    bail!("CoreStore admission oldest seek found a row for an empty shard");
                }
                (_, Some(oldest)) => {
                    if oldest.sequence <= record.sequence {
                        bail!("CoreStore admission oldest seek did not advance");
                    }
                    next.oldest_pending_sequence = Some(oldest.sequence);
                    next.oldest_pending_created_at_unix_nanos = Some(oldest.created_at_unix_nanos);
                }
                (_, None) => bail!("CoreStore admission oldest seek found no successor"),
            }
        } else if next_oldest.is_some() {
            bail!("CoreStore admission performed an unexpected oldest-row seek");
        }
        validate_admission_point_state(&next)?;
        Ok(next)
    }

    pub(super) fn lag_seconds(&self, now_unix_nanos: u64) -> Option<u64> {
        self.oldest_pending_created_at_unix_nanos
            .map(|created_at| now_unix_nanos.saturating_sub(created_at) / 1_000_000_000)
    }
}

impl AdmissionMutationHead {
    pub(super) fn active(record: &CorePendingMutationRecord, request_hash: String) -> Self {
        Self {
            mutation_id: record.mutation_id.clone(),
            mutation_sequence: record.sequence,
            request_hash,
            state: ADMISSION_MUTATION_ACTIVE.to_string(),
        }
    }

    pub(super) fn finalised(&self) -> Result<Self> {
        if !self.is_active() {
            bail!("CoreStore admission mutation head is not active");
        }
        let mut next = self.clone();
        next.state = ADMISSION_MUTATION_FINALISED.to_string();
        Ok(next)
    }

    pub(super) fn is_active(&self) -> bool {
        self.state == ADMISSION_MUTATION_ACTIVE
    }

    pub(super) fn is_finalised(&self) -> bool {
        self.state == ADMISSION_MUTATION_FINALISED
    }
}

impl AdmissionIdempotencyHead {
    pub(super) fn active(
        record: &CorePendingMutationRecord,
        idempotency_key_hash: String,
        request_hash: String,
    ) -> Self {
        Self {
            idempotency_key_hash,
            mutation_id: record.mutation_id.clone(),
            mutation_sequence: record.sequence,
            request_hash,
            state: ADMISSION_MUTATION_ACTIVE.to_string(),
        }
    }

    pub(super) fn finalised(&self) -> Result<Self> {
        if !self.is_active() {
            bail!("CoreStore admission idempotency head is not active");
        }
        let mut next = self.clone();
        next.state = ADMISSION_MUTATION_FINALISED.to_string();
        Ok(next)
    }

    pub(super) fn is_active(&self) -> bool {
        self.state == ADMISSION_MUTATION_ACTIVE
    }

    pub(super) fn is_finalised(&self) -> bool {
        self.state == ADMISSION_MUTATION_FINALISED
    }
}

impl LandedByteHead {
    pub(super) fn from_landed(landed: &CorePendingLandedByte) -> Self {
        Self {
            sha256: landed.sha256.clone(),
            relative_path: landed.relative_path.clone(),
            length: landed.length,
            reference_count: 1,
        }
    }

    pub(super) fn add_reference(&self, landed: &CorePendingLandedByte) -> Result<Self> {
        self.validate_descriptor(landed)?;
        let mut next = self.clone();
        next.reference_count = next
            .reference_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore landed byte reference counter overflow"))?;
        Ok(next)
    }

    pub(super) fn remove_reference(&self, landed: &CorePendingLandedByte) -> Result<Option<Self>> {
        self.validate_descriptor(landed)?;
        match self.reference_count {
            0 => bail!("CoreStore landed byte reference counter is zero"),
            1 => Ok(None),
            _ => {
                let mut next = self.clone();
                next.reference_count -= 1;
                Ok(Some(next))
            }
        }
    }

    fn validate_descriptor(&self, landed: &CorePendingLandedByte) -> Result<()> {
        if self.sha256 != landed.sha256
            || self.relative_path != landed.relative_path
            || self.length != landed.length
        {
            bail!("CoreStore landed byte point head descriptor mismatch");
        }
        Ok(())
    }
}

pub(super) fn encode_admission_point_state(
    admission_shard_hash: &str,
    state: &AdmissionPointState,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    validate_admission_point_state(state)?;
    encode_point_row(
        AdmissionPointRowProto {
            common: Some(point_common(
                admission_shard_hash,
                root_generation,
                transaction_id,
            )),
            schema: CORE_MATERIALISATION_CURSOR_SCHEMA.to_string(),
            point_kind: ADMISSION_POINT_KIND.to_string(),
            admission_shard_hash: admission_shard_hash.to_string(),
            last_sequence: state.last_sequence,
            pending_rows: state.pending_rows,
            pending_bytes: state.pending_bytes,
            landed_bytes: state.landed_bytes,
            oldest_pending_sequence: state.oldest_pending_sequence,
            oldest_pending_created_at_unix_nanos: state.oldest_pending_created_at_unix_nanos,
            ..Default::default()
        },
        "CoreStore admission point state",
    )
}

pub(super) fn encode_admission_mutation_head(
    admission_shard_hash: &str,
    head: &AdmissionMutationHead,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    validate_admission_mutation_head(head)?;
    encode_point_row(
        AdmissionPointRowProto {
            common: Some(point_common(
                admission_shard_hash,
                root_generation,
                transaction_id,
            )),
            schema: CORE_MATERIALISATION_CURSOR_SCHEMA.to_string(),
            point_kind: ADMISSION_MUTATION_HEAD_KIND.to_string(),
            admission_shard_hash: admission_shard_hash.to_string(),
            mutation_id: head.mutation_id.clone(),
            mutation_sequence: head.mutation_sequence,
            request_hash: head.request_hash.clone(),
            mutation_state: head.state.clone(),
            ..Default::default()
        },
        "CoreStore admission mutation head",
    )
}

pub(super) fn encode_admission_idempotency_head(
    admission_shard_hash: &str,
    head: &AdmissionIdempotencyHead,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    validate_admission_idempotency_head(head)?;
    encode_point_row(
        AdmissionPointRowProto {
            common: Some(point_common(
                admission_shard_hash,
                root_generation,
                transaction_id,
            )),
            schema: CORE_MATERIALISATION_CURSOR_SCHEMA.to_string(),
            point_kind: ADMISSION_IDEMPOTENCY_HEAD_KIND.to_string(),
            admission_shard_hash: admission_shard_hash.to_string(),
            idempotency_key_hash: head.idempotency_key_hash.clone(),
            mutation_id: head.mutation_id.clone(),
            mutation_sequence: head.mutation_sequence,
            request_hash: head.request_hash.clone(),
            mutation_state: head.state.clone(),
            ..Default::default()
        },
        "CoreStore admission idempotency head",
    )
}

pub(super) fn encode_landed_byte_head(
    admission_shard_hash: &str,
    head: &LandedByteHead,
    root_generation: u64,
    transaction_id: &str,
) -> Result<Vec<u8>> {
    validate_landed_byte_head(head)?;
    encode_point_row(
        AdmissionPointRowProto {
            common: Some(point_common(
                admission_shard_hash,
                root_generation,
                transaction_id,
            )),
            schema: CORE_MATERIALISATION_CURSOR_SCHEMA.to_string(),
            point_kind: LANDED_BYTE_HEAD_KIND.to_string(),
            admission_shard_hash: admission_shard_hash.to_string(),
            landed_sha256: head.sha256.clone(),
            landed_relative_path: head.relative_path.clone(),
            landed_length: head.length,
            landed_reference_count: head.reference_count,
            ..Default::default()
        },
        "CoreStore landed byte point head",
    )
}

pub(super) fn pending_mutation_request_hash(
    record: &CorePendingMutationRecord,
    inline_payload: &[u8],
) -> Result<String> {
    let mut normalized = record.clone();
    normalized.sequence = 0;
    normalized.created_at_unix_nanos = 0;
    let bytes = encode_pending_mutation_hash_input(&normalized, inline_payload)?;
    Ok(format!("sha256:{}", sha256_hex(&bytes)))
}

impl CoreStore {
    // Admission point, head, cursor, and pending rows are node-local staging
    // and recovery state. Raw reads are required before root publication.
    #[cfg(test)]
    pub(super) fn admission_accounting_totals_for_tests(
        &self,
    ) -> Result<(u64, u64, u64, Option<u64>)> {
        let mut totals = (0_u64, 0_u64, 0_u64, None::<u64>);
        let mut after = None;
        loop {
            let rows = self.meta.scan_prefix_page(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_point_state_prefix(),
                after.as_deref(),
                ADMISSION_RECOVERY_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let (_, state) = decode_admission_point_state_unscoped(&row.payload)?;
                totals.0 = totals.0.saturating_add(state.pending_rows);
                totals.1 = totals.1.saturating_add(state.pending_bytes);
                totals.2 = totals.2.saturating_add(state.landed_bytes);
                if let Some(lag) = state.lag_seconds(unix_timestamp_nanos()) {
                    totals.3 = Some(totals.3.map_or(lag, |current| current.max(lag)));
                }
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(|key| key.to_vec()))
                .transpose()?;
            if rows.len() < ADMISSION_RECOVERY_PAGE_ROWS {
                break;
            }
        }
        Ok(totals)
    }

    pub(super) fn read_admission_point_state(
        &self,
        admission_shard_hash: &str,
    ) -> Result<Option<AdmissionPointState>> {
        self.meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_point_state_key(admission_shard_hash),
            )?
            .map(|bytes| decode_admission_point_state(&bytes, admission_shard_hash))
            .transpose()
    }

    pub(super) fn load_admission_point_state_foreground(
        &self,
        admission_shard_hash: &str,
    ) -> Result<AdmissionPointState> {
        for _ in 0..ADMISSION_POINT_STABLE_READ_ATTEMPTS {
            let sequence_before = self.read_admission_sequence_cursor(admission_shard_hash)?;
            let observed = match self.read_admission_point_state(admission_shard_hash)? {
                Some(state) => {
                    if sequence_before != Some(state.last_sequence) {
                        Err(anyhow!(
                            "CoreStore admission sequence cursor and point state disagree"
                        ))
                    } else {
                        self.validate_admission_oldest_point(admission_shard_hash, &state)
                            .map(|()| state)
                    }
                }
                None => {
                    if self
                        .first_pending_mutation_after(admission_shard_hash, None)?
                        .is_some()
                    {
                        Err(anyhow!(
                            "CoreStore nonempty admission shard is missing point state; explicit startup repair is required"
                        ))
                    } else {
                        Ok(AdmissionPointState::empty(sequence_before.unwrap_or(0)))
                    }
                }
            };
            let sequence_after = self.read_admission_sequence_cursor(admission_shard_hash)?;
            if sequence_before != sequence_after {
                continue;
            }
            return observed;
        }
        bail!(
            "CoreStore admission point state remained unstable after {} bounded reads",
            ADMISSION_POINT_STABLE_READ_ATTEMPTS
        )
    }

    pub(in crate::core_store::local) fn read_admission_mutation_head(
        &self,
        admission_shard_hash: &str,
        mutation_id: &str,
    ) -> Result<Option<AdmissionMutationHead>> {
        let head = self
            .meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_mutation_head_key(admission_shard_hash, mutation_id),
            )?
            .map(|bytes| decode_admission_mutation_head(&bytes, admission_shard_hash))
            .transpose()?;
        if head
            .as_ref()
            .is_some_and(|head| head.mutation_id != mutation_id)
        {
            bail!("CoreStore admission mutation point head has invalid key scope");
        }
        Ok(head)
    }

    pub(in crate::core_store::local) fn read_admission_idempotency_head(
        &self,
        admission_shard_hash: &str,
        idempotency_key_hash: &str,
    ) -> Result<Option<AdmissionIdempotencyHead>> {
        let head = self
            .meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_idempotency_head_key(admission_shard_hash, idempotency_key_hash),
            )?
            .map(|bytes| decode_admission_idempotency_head(&bytes, admission_shard_hash))
            .transpose()?;
        if head
            .as_ref()
            .is_some_and(|head| head.idempotency_key_hash != idempotency_key_hash)
        {
            bail!("CoreStore admission idempotency point head has invalid key scope");
        }
        Ok(head)
    }

    pub(super) fn read_landed_byte_head(
        &self,
        admission_shard_hash: &str,
        sha256: &str,
    ) -> Result<Option<LandedByteHead>> {
        let head = self
            .meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &landed_byte_head_key(admission_shard_hash, sha256),
            )?
            .map(|bytes| decode_landed_byte_head(&bytes, admission_shard_hash))
            .transpose()?;
        if head.as_ref().is_some_and(|head| head.sha256 != sha256) {
            bail!("CoreStore landed byte point head has invalid key scope");
        }
        Ok(head)
    }

    pub(super) fn read_pending_mutation_at(
        &self,
        admission_shard_hash: &str,
        sequence: u64,
    ) -> Result<Option<(CorePendingMutationRecord, Vec<u8>, u64)>> {
        let Some(bytes) = self.meta.get(
            CF_TRANSACTIONS,
            TABLE_PENDING_MUTATION_ROW,
            &admission_record_key(admission_shard_hash, sequence),
        )?
        else {
            return Ok(None);
        };
        let stored_bytes = bytes.len() as u64;
        let (record, inline_payload) = decode_stored_pending_mutation_row(&bytes)?;
        if record.sequence != sequence
            || record.target.admission_shard().hash != admission_shard_hash
        {
            bail!("CoreStore pending mutation row key/shard/sequence mismatch");
        }
        Ok(Some((record, inline_payload, stored_bytes)))
    }

    pub(super) fn first_pending_mutation_after(
        &self,
        admission_shard_hash: &str,
        sequence: Option<u64>,
    ) -> Result<Option<PendingMutationPageRow>> {
        let after_key = sequence.map(|value| admission_record_key(admission_shard_hash, value));
        let mut rows =
            self.read_pending_mutation_page(admission_shard_hash, after_key.as_deref(), 1)?;
        Ok(rows.pop())
    }

    pub(super) fn read_pending_mutation_page(
        &self,
        admission_shard_hash: &str,
        after_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<PendingMutationPageRow>> {
        self.decode_pending_page(
            self.meta.scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &admission_record_prefix(admission_shard_hash),
                after_tuple_key,
                limit,
            )?,
            Some(admission_shard_hash),
        )
    }

    pub(super) fn read_all_pending_mutation_page(
        &self,
        after_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<PendingMutationPageRow>> {
        self.decode_pending_page(
            self.meta.scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &all_admission_records_prefix(),
                after_tuple_key,
                limit,
            )?,
            None,
        )
    }

    fn read_all_pending_mutation_page_from_snapshot(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        after_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<PendingMutationPageRow>> {
        self.decode_pending_page(
            snapshot.scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &all_admission_records_prefix(),
                after_tuple_key,
                limit,
            )?,
            None,
        )
    }

    fn decode_pending_page(
        &self,
        rows: Vec<CoreMetaRecord>,
        expected_shard_hash: Option<&str>,
    ) -> Result<Vec<PendingMutationPageRow>> {
        rows.into_iter()
            .map(|row| {
                let stored_bytes = row.payload.len() as u64;
                let tuple_key = core_meta_record_tuple_key(&row.key)?.to_vec();
                let (record, inline_payload) = decode_stored_pending_mutation_row(&row.payload)?;
                let shard_hash = record.target.admission_shard().hash;
                if expected_shard_hash.is_some_and(|expected| expected != shard_hash)
                    || tuple_key != admission_record_key(&shard_hash, record.sequence)
                {
                    bail!("CoreStore pending mutation page row key/shard/sequence mismatch");
                }
                Ok(PendingMutationPageRow {
                    record,
                    inline_payload,
                    stored_bytes,
                    tuple_key,
                })
            })
            .collect()
    }

    pub(super) fn pending_mutation_finalisation_index_point(
        &self,
        key: &CorePendingMutationKey,
    ) -> Result<Option<CorePendingMutationFinalisationIndexRow>> {
        let Some(bytes) = self.meta.get(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &admission_finalisation_key(key),
        )?
        else {
            return Ok(None);
        };
        let row = decode_pending_mutation_finalisation_index_row(&bytes)?;
        if row.schema != CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA
            || row.admission_shard_hash != key.admission_shard_hash
            || row.node_id != key.node_id
            || row.mutation_epoch != key.mutation_epoch
            || row.mutation_sequence != key.mutation_sequence
        {
            bail!("CoreStore pending mutation finalisation point row has invalid scope");
        }
        Ok(Some(row))
    }

    fn pending_mutation_finalisation_index_point_from_snapshot(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        key: &CorePendingMutationKey,
    ) -> Result<Option<CorePendingMutationFinalisationIndexRow>> {
        let Some(bytes) = snapshot.get(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &admission_finalisation_key(key),
        )?
        else {
            return Ok(None);
        };
        let row = decode_pending_mutation_finalisation_index_row(&bytes)?;
        if row.schema != CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA
            || row.admission_shard_hash != key.admission_shard_hash
            || row.node_id != key.node_id
            || row.mutation_epoch != key.mutation_epoch
            || row.mutation_sequence != key.mutation_sequence
        {
            bail!("CoreStore pending mutation finalisation point row has invalid scope");
        }
        Ok(Some(row))
    }

    pub(super) fn validate_admission_recovery_state(
        &self,
    ) -> Result<BTreeMap<String, (String, u64)>> {
        let snapshot = self.meta.read_snapshot();
        self.validate_admission_recovery_snapshot(&snapshot)
    }

    pub(in crate::core_store::local) fn validate_admission_recovery_snapshot(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
    ) -> Result<BTreeMap<String, (String, u64)>> {
        let mut shards = BTreeMap::<String, RecoveryShardState>::new();
        let mut referenced_paths = BTreeMap::<String, (String, u64)>::new();
        let mut seen_landing_ids = BTreeSet::new();
        let mut after = None;

        loop {
            let page = self.read_all_pending_mutation_page_from_snapshot(
                snapshot,
                after.as_deref(),
                ADMISSION_RECOVERY_PAGE_ROWS,
            )?;
            if page.is_empty() {
                break;
            }
            for row in &page {
                let shard_hash = row.record.target.admission_shard().hash;
                self.validate_recovery_pending_row(
                    snapshot,
                    &shard_hash,
                    row,
                    shards.entry(shard_hash.clone()).or_default(),
                    &mut seen_landing_ids,
                    &mut referenced_paths,
                )?;
            }
            after = page.last().map(|row| row.tuple_key.clone());
            if page.len() < ADMISSION_RECOVERY_PAGE_ROWS {
                break;
            }
        }

        self.validate_persisted_admission_shards(snapshot, &mut shards)?;
        Ok(referenced_paths)
    }

    fn validate_recovery_pending_row(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        shard_hash: &str,
        row: &PendingMutationPageRow,
        recovery: &mut RecoveryShardState,
        seen_landing_ids: &mut BTreeSet<String>,
        referenced_paths: &mut BTreeMap<String, (String, u64)>,
    ) -> Result<()> {
        let key = CorePendingMutationKey::from(&row.record);
        if self
            .pending_mutation_finalisation_index_point_from_snapshot(snapshot, &key)?
            .is_some()
        {
            bail!("CoreStore recovery found a finalised mutation with a live pending row");
        }
        let pending_hash_input =
            encode_pending_mutation_hash_input(&row.record, &row.inline_payload)?;
        self.verify_recovery_local_admission_evidence(snapshot, &row.record, &pending_hash_input)?;
        let mutation_request_hash =
            pending_mutation_request_hash(&row.record, &row.inline_payload)?;
        let mutation_head = self
            .read_recovery_admission_mutation_head(snapshot, shard_hash, &row.record.mutation_id)?
            .ok_or_else(|| anyhow!("CoreStore pending mutation is missing its mutation head"))?;
        if !mutation_head.is_active()
            || mutation_head.mutation_sequence != row.record.sequence
            || mutation_head.request_hash != mutation_request_hash
        {
            bail!("CoreStore pending mutation point head is inconsistent");
        }
        if let Some(idempotency_key_hash) = row.record.idempotency_key_hash.as_deref() {
            let idempotency_head = self
                .read_recovery_admission_idempotency_head(
                    snapshot,
                    shard_hash,
                    idempotency_key_hash,
                )?
                .ok_or_else(|| anyhow!("CoreStore pending mutation is missing idempotency head"))?;
            if !idempotency_head.is_active()
                || idempotency_head.mutation_id != row.record.mutation_id
                || idempotency_head.mutation_sequence != row.record.sequence
                || idempotency_head.request_hash != admission_request_hash(&row.record)?
            {
                bail!("CoreStore pending mutation idempotency head is inconsistent");
            }
        }
        recovery.pending_rows = recovery
            .pending_rows
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore recovery pending row counter overflow"))?;
        recovery.pending_bytes = recovery
            .pending_bytes
            .checked_add(row.stored_bytes)
            .ok_or_else(|| anyhow!("CoreStore recovery pending byte counter overflow"))?;
        recovery.max_sequence = recovery.max_sequence.max(row.record.sequence);
        recovery
            .oldest
            .get_or_insert((row.record.sequence, row.record.created_at_unix_nanos));

        for landed in &row.record.landed_bytes {
            if !seen_landing_ids.insert(landed.landing_id.clone()) {
                bail!("CoreStore recovery found a duplicate landed byte landing id");
            }
            self.verify_recovery_landed_bytes_ref_row(
                snapshot,
                shard_hash,
                &landed.landing_id,
                &row.record.mutation_id,
                &landed.sha256,
                landed.length,
                &row.record.boundary_values,
            )?;
            let next = match recovery.landed_heads.get(&landed.sha256) {
                Some(head) => head.add_reference(landed)?,
                None => LandedByteHead::from_landed(landed),
            };
            recovery.landed_heads.insert(landed.sha256.clone(), next);
            if let Some(existing) = referenced_paths.insert(
                landed.relative_path.clone(),
                (landed.sha256.clone(), landed.length),
            ) && existing != (landed.sha256.clone(), landed.length)
            {
                bail!("CoreStore recovery landed path descriptor mismatch");
            }
        }
        recovery.landed_bytes = recovery
            .landed_heads
            .values()
            .try_fold(0_u64, |total, head| total.checked_add(head.length))
            .ok_or_else(|| anyhow!("CoreStore recovery landed byte counter overflow"))?;
        Ok(())
    }

    fn verify_recovery_local_admission_evidence(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        record: &CorePendingMutationRecord,
        pending_mutation_hash_input: &[u8],
    ) -> Result<CoreLocalAdmissionEvidence> {
        let shard = record.target.admission_shard();
        let bytes = snapshot
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

    fn read_recovery_admission_mutation_head(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        admission_shard_hash: &str,
        mutation_id: &str,
    ) -> Result<Option<AdmissionMutationHead>> {
        let head = snapshot
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_mutation_head_key(admission_shard_hash, mutation_id),
            )?
            .map(|bytes| decode_admission_mutation_head(&bytes, admission_shard_hash))
            .transpose()?;
        if head
            .as_ref()
            .is_some_and(|head| head.mutation_id != mutation_id)
        {
            bail!("CoreStore admission mutation point head has invalid key scope");
        }
        Ok(head)
    }

    fn read_recovery_admission_idempotency_head(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        admission_shard_hash: &str,
        idempotency_key_hash: &str,
    ) -> Result<Option<AdmissionIdempotencyHead>> {
        let head = snapshot
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_idempotency_head_key(admission_shard_hash, idempotency_key_hash),
            )?
            .map(|bytes| decode_admission_idempotency_head(&bytes, admission_shard_hash))
            .transpose()?;
        if head
            .as_ref()
            .is_some_and(|head| head.idempotency_key_hash != idempotency_key_hash)
        {
            bail!("CoreStore admission idempotency point head has invalid key scope");
        }
        Ok(head)
    }

    #[allow(clippy::too_many_arguments)]
    fn verify_recovery_landed_bytes_ref_row(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        admission_shard_hash: &str,
        landing_id: &str,
        mutation_id: &str,
        sha256: &str,
        length: u64,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<()> {
        let landed_key = landed_byte_ref_key(admission_shard_hash, landing_id);
        let bytes = snapshot
            .get(CF_MATERIALISATION, TABLE_LANDED_BYTE_REF_ROW, &landed_key)?
            .ok_or_else(|| anyhow!("CoreStore landed byte CoreMeta row is missing"))?;
        self.verify_landed_bytes_ref_payload(
            admission_shard_hash,
            landing_id,
            mutation_id,
            sha256,
            length,
            boundary_values,
            &bytes,
        )
    }

    fn validate_persisted_admission_shards(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        expected: &mut BTreeMap<String, RecoveryShardState>,
    ) -> Result<()> {
        let mut after = None;
        loop {
            let rows = snapshot.scan_prefix_page(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_point_state_prefix(),
                after.as_deref(),
                ADMISSION_RECOVERY_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let (shard_hash, state) = decode_admission_point_state_unscoped(&row.payload)?;
                if core_meta_record_tuple_key(&row.key)?
                    != admission_point_state_key(&shard_hash).as_slice()
                {
                    bail!("CoreStore admission point state has invalid key scope");
                }
                let recovery = expected.remove(&shard_hash).unwrap_or_default();
                if self.read_admission_sequence_cursor_from_snapshot(snapshot, &shard_hash)?
                    != Some(state.last_sequence)
                    || state.last_sequence < recovery.max_sequence
                    || state.pending_rows != recovery.pending_rows
                    || state.pending_bytes != recovery.pending_bytes
                    || state.landed_bytes != recovery.landed_bytes
                    || state.oldest_pending_sequence != recovery.oldest.map(|value| value.0)
                    || state.oldest_pending_created_at_unix_nanos
                        != recovery.oldest.map(|value| value.1)
                {
                    bail!("CoreStore admission shard point state is inconsistent");
                }
                self.validate_landed_heads_for_recovery(
                    snapshot,
                    &shard_hash,
                    recovery.landed_heads,
                )?;
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(|key| key.to_vec()))
                .transpose()?;
            if rows.len() < ADMISSION_RECOVERY_PAGE_ROWS {
                break;
            }
        }
        if !expected.is_empty() {
            bail!("CoreStore pending rows exist for a shard without point state");
        }
        Ok(())
    }

    fn read_admission_sequence_cursor(&self, admission_shard_hash: &str) -> Result<Option<u64>> {
        self.meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_sequence_key(admission_shard_hash),
            )?
            .map(|bytes| decode_admission_sequence_cursor_row(&bytes, admission_shard_hash))
            .transpose()
    }

    fn read_admission_sequence_cursor_from_snapshot(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        admission_shard_hash: &str,
    ) -> Result<Option<u64>> {
        snapshot
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_sequence_key(admission_shard_hash),
            )?
            .map(|bytes| decode_admission_sequence_cursor_row(&bytes, admission_shard_hash))
            .transpose()
    }

    fn validate_admission_oldest_point(
        &self,
        admission_shard_hash: &str,
        state: &AdmissionPointState,
    ) -> Result<()> {
        validate_admission_point_state(state)?;
        match state.oldest_pending_sequence {
            Some(sequence) => {
                let (record, _, _) = self
                    .read_pending_mutation_at(admission_shard_hash, sequence)?
                    .ok_or_else(|| anyhow!("CoreStore admission oldest point row is missing"))?;
                if Some(record.created_at_unix_nanos) != state.oldest_pending_created_at_unix_nanos
                {
                    bail!("CoreStore admission oldest point timestamp mismatch");
                }
            }
            None => {
                if self
                    .first_pending_mutation_after(admission_shard_hash, None)?
                    .is_some()
                {
                    bail!("CoreStore empty admission point state has pending source rows");
                }
            }
        }
        Ok(())
    }

    fn validate_landed_heads_for_recovery(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        admission_shard_hash: &str,
        mut expected: BTreeMap<String, LandedByteHead>,
    ) -> Result<()> {
        let mut after = None;
        loop {
            let rows = snapshot.scan_prefix_page(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &landed_byte_head_prefix(admission_shard_hash),
                after.as_deref(),
                ADMISSION_RECOVERY_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let head = decode_landed_byte_head(&row.payload, admission_shard_hash)?;
                if core_meta_record_tuple_key(&row.key)?
                    != landed_byte_head_key(admission_shard_hash, &head.sha256).as_slice()
                {
                    bail!("CoreStore landed byte point head has invalid key scope");
                }
                let Some(expected_head) = expected.remove(&head.sha256) else {
                    bail!("CoreStore recovery found an unreferenced landed byte point head");
                };
                if head != expected_head {
                    bail!("CoreStore landed byte point head is inconsistent");
                }
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(|key| key.to_vec()))
                .transpose()?;
            if rows.len() < ADMISSION_RECOVERY_PAGE_ROWS {
                break;
            }
        }
        if !expected.is_empty() {
            bail!("CoreStore recovery found a missing landed byte point head");
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::core_store::local) fn install_admission_point_state_for_tests(
        &self,
    ) -> Result<()> {
        let mut rows_by_shard = BTreeMap::<String, Vec<PendingMutationPageRow>>::new();
        let mut after = None;
        loop {
            let page = self
                .read_all_pending_mutation_page(after.as_deref(), ADMISSION_RECOVERY_PAGE_ROWS)?;
            if page.is_empty() {
                break;
            }
            for row in page {
                rows_by_shard
                    .entry(row.record.target.admission_shard().hash)
                    .or_default()
                    .push(row);
            }
            after = rows_by_shard
                .values()
                .flat_map(|rows| rows.last())
                .max_by(|left, right| left.tuple_key.cmp(&right.tuple_key))
                .map(|row| row.tuple_key.clone());
        }
        for (shard_hash, rows) in rows_by_shard {
            self.install_admission_shard_state_for_tests(&shard_hash, &rows)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn install_admission_shard_state_for_tests(
        &self,
        shard_hash: &str,
        rows: &[PendingMutationPageRow],
    ) -> Result<()> {
        let mut state = AdmissionPointState::empty(0);
        let mut landed_heads = BTreeMap::<String, LandedByteHead>::new();
        let mut ops = Vec::<OwnedCoreMetaBatchOp>::new();
        for row in rows {
            state.pending_rows += 1;
            state.pending_bytes += row.stored_bytes;
            state.last_sequence = state.last_sequence.max(row.record.sequence);
            if state.oldest_pending_sequence.is_none() {
                state.oldest_pending_sequence = Some(row.record.sequence);
                state.oldest_pending_created_at_unix_nanos = Some(row.record.created_at_unix_nanos);
            }
            let mutation_request_hash =
                pending_mutation_request_hash(&row.record, &row.inline_payload)?;
            let mutation_head = AdmissionMutationHead::active(&row.record, mutation_request_hash);
            ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: admission_mutation_head_key(shard_hash, &row.record.mutation_id),
                payload: encode_admission_mutation_head(
                    shard_hash,
                    &mutation_head,
                    row.record.sequence,
                    "test-admission-state",
                )?,
                common: None,
            });
            if let Some(idempotency_key_hash) = row.record.idempotency_key_hash.clone() {
                let head = AdmissionIdempotencyHead::active(
                    &row.record,
                    idempotency_key_hash.clone(),
                    admission_request_hash(&row.record)?,
                );
                ops.push(OwnedCoreMetaBatchOp::Put {
                    cf: CF_MATERIALISATION,
                    table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                    tuple_key: admission_idempotency_head_key(shard_hash, &idempotency_key_hash),
                    payload: encode_admission_idempotency_head(
                        shard_hash,
                        &head,
                        row.record.sequence,
                        "test-admission-state",
                    )?,
                    common: None,
                });
            }
            for landed in &row.record.landed_bytes {
                let next = match landed_heads.get(&landed.sha256) {
                    Some(head) => head.add_reference(landed)?,
                    None => LandedByteHead::from_landed(landed),
                };
                landed_heads.insert(landed.sha256.clone(), next);
            }
        }
        state.landed_bytes = landed_heads
            .values()
            .try_fold(0_u64, |total, head| total.checked_add(head.length))
            .ok_or_else(|| anyhow!("test admission landed byte counter overflow"))?;
        if state.pending_rows == 0 {
            return Ok(());
        }
        ops.push(OwnedCoreMetaBatchOp::Put {
            cf: CF_MATERIALISATION,
            table_id: TABLE_MATERIALISATION_CURSOR_ROW,
            tuple_key: admission_sequence_key(shard_hash),
            payload: encode_admission_sequence_cursor_row(shard_hash, state.last_sequence)?,
            common: None,
        });
        ops.push(OwnedCoreMetaBatchOp::Put {
            cf: CF_MATERIALISATION,
            table_id: TABLE_MATERIALISATION_CURSOR_ROW,
            tuple_key: admission_point_state_key(shard_hash),
            payload: encode_admission_point_state(
                shard_hash,
                &state,
                state.last_sequence,
                "test-admission-state",
            )?,
            common: None,
        });
        for head in landed_heads.values() {
            ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: landed_byte_head_key(shard_hash, &head.sha256),
                payload: encode_landed_byte_head(
                    shard_hash,
                    head,
                    state.last_sequence,
                    "test-admission-state",
                )?,
                common: None,
            });
        }
        let borrowed = borrow_owned_coremeta_batch_ops(&ops);
        self.meta.write_local_committed_batch(&borrowed)
    }
}

fn point_common(
    admission_shard_hash: &str,
    root_generation: u64,
    transaction_id: &str,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        "system/local-admission",
        admission_shard_hash,
        root_generation,
        transaction_id,
        unix_timestamp_nanos(),
    )
}

fn encode_point_row(proto: AdmissionPointRowProto, label: &'static str) -> Result<Vec<u8>> {
    let bytes = encode_deterministic_proto(&proto);
    let decoded = decode_deterministic_proto::<AdmissionPointRowProto>(&bytes, label)?;
    if decoded != proto {
        bail!("{label} did not round-trip deterministically");
    }
    Ok(bytes)
}

fn decode_point_row(
    bytes: &[u8],
    label: &'static str,
    admission_shard_hash: &str,
) -> Result<AdmissionPointRowProto> {
    let proto = decode_deterministic_proto::<AdmissionPointRowProto>(bytes, label)?;
    if proto.schema != CORE_MATERIALISATION_CURSOR_SCHEMA {
        bail!("{label} has invalid schema");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("{label} is missing CoreMeta common"))?;
    if common.realm_id != "system/local-admission"
        || common.root_key_hash != admission_shard_hash
        || proto.admission_shard_hash != admission_shard_hash
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
    {
        bail!("{label} has invalid admission shard scope");
    }
    Ok(proto)
}

fn decode_admission_point_state_unscoped(bytes: &[u8]) -> Result<(String, AdmissionPointState)> {
    let proto = decode_deterministic_proto::<AdmissionPointRowProto>(
        bytes,
        "CoreStore admission point state",
    )?;
    let shard_hash = proto.admission_shard_hash.clone();
    let proto = decode_point_row(bytes, "CoreStore admission point state", &shard_hash)?;
    if proto.point_kind != ADMISSION_POINT_KIND {
        bail!("CoreStore admission point row has invalid kind");
    }
    let state = state_from_proto(&proto)?;
    Ok((shard_hash, state))
}

fn decode_admission_point_state(
    bytes: &[u8],
    admission_shard_hash: &str,
) -> Result<AdmissionPointState> {
    let proto = decode_point_row(
        bytes,
        "CoreStore admission point state",
        admission_shard_hash,
    )?;
    if proto.point_kind != ADMISSION_POINT_KIND {
        bail!("CoreStore admission point row has invalid kind");
    }
    state_from_proto(&proto)
}

fn state_from_proto(proto: &AdmissionPointRowProto) -> Result<AdmissionPointState> {
    let state = AdmissionPointState {
        last_sequence: proto.last_sequence,
        pending_rows: proto.pending_rows,
        pending_bytes: proto.pending_bytes,
        landed_bytes: proto.landed_bytes,
        oldest_pending_sequence: proto.oldest_pending_sequence,
        oldest_pending_created_at_unix_nanos: proto.oldest_pending_created_at_unix_nanos,
    };
    validate_admission_point_state(&state)?;
    Ok(state)
}

fn decode_admission_mutation_head(
    bytes: &[u8],
    admission_shard_hash: &str,
) -> Result<AdmissionMutationHead> {
    let proto = decode_point_row(
        bytes,
        "CoreStore admission mutation head",
        admission_shard_hash,
    )?;
    if proto.point_kind != ADMISSION_MUTATION_HEAD_KIND {
        bail!("CoreStore admission mutation head has invalid kind");
    }
    let head = AdmissionMutationHead {
        mutation_id: proto.mutation_id,
        mutation_sequence: proto.mutation_sequence,
        request_hash: proto.request_hash,
        state: proto.mutation_state,
    };
    validate_admission_mutation_head(&head)?;
    Ok(head)
}

fn decode_admission_idempotency_head(
    bytes: &[u8],
    admission_shard_hash: &str,
) -> Result<AdmissionIdempotencyHead> {
    let proto = decode_point_row(
        bytes,
        "CoreStore admission idempotency head",
        admission_shard_hash,
    )?;
    if proto.point_kind != ADMISSION_IDEMPOTENCY_HEAD_KIND {
        bail!("CoreStore admission idempotency head has invalid kind");
    }
    let head = AdmissionIdempotencyHead {
        idempotency_key_hash: proto.idempotency_key_hash,
        mutation_id: proto.mutation_id,
        mutation_sequence: proto.mutation_sequence,
        request_hash: proto.request_hash,
        state: proto.mutation_state,
    };
    validate_admission_idempotency_head(&head)?;
    Ok(head)
}

fn decode_landed_byte_head(bytes: &[u8], admission_shard_hash: &str) -> Result<LandedByteHead> {
    let proto = decode_point_row(
        bytes,
        "CoreStore landed byte point head",
        admission_shard_hash,
    )?;
    if proto.point_kind != LANDED_BYTE_HEAD_KIND {
        bail!("CoreStore landed byte point head has invalid kind");
    }
    let head = LandedByteHead {
        sha256: proto.landed_sha256,
        relative_path: proto.landed_relative_path,
        length: proto.landed_length,
        reference_count: proto.landed_reference_count,
    };
    validate_landed_byte_head(&head)?;
    Ok(head)
}

fn validate_admission_point_state(state: &AdmissionPointState) -> Result<()> {
    if state.pending_rows == 0 {
        if state.pending_bytes != 0
            || state.landed_bytes != 0
            || state.oldest_pending_sequence.is_some()
            || state.oldest_pending_created_at_unix_nanos.is_some()
        {
            bail!("CoreStore empty admission point state has nonempty accounting");
        }
        return Ok(());
    }
    let oldest_sequence = state
        .oldest_pending_sequence
        .ok_or_else(|| anyhow!("CoreStore admission point state is missing oldest sequence"))?;
    if state.oldest_pending_created_at_unix_nanos.is_none() {
        bail!("CoreStore admission point state is missing oldest timestamp");
    }
    if oldest_sequence == 0 || oldest_sequence > state.last_sequence {
        bail!("CoreStore admission point state has invalid oldest sequence");
    }
    if state.pending_bytes == 0 {
        bail!("CoreStore nonempty admission point state has zero pending bytes");
    }
    Ok(())
}

fn validate_admission_mutation_head(head: &AdmissionMutationHead) -> Result<()> {
    validate_logical_id(&head.mutation_id, "admission mutation head mutation id")?;
    validate_head_fields(head.mutation_sequence, &head.request_hash, &head.state)
}

fn validate_admission_idempotency_head(head: &AdmissionIdempotencyHead) -> Result<()> {
    validate_hash(
        &head.idempotency_key_hash,
        "admission idempotency head key hash",
    )?;
    validate_logical_id(&head.mutation_id, "admission idempotency head mutation id")?;
    validate_head_fields(head.mutation_sequence, &head.request_hash, &head.state)
}

fn validate_head_fields(sequence: u64, request_hash: &str, state: &str) -> Result<()> {
    if sequence == 0 {
        bail!("CoreStore admission point head sequence is zero");
    }
    validate_hash(request_hash, "admission point head request hash")?;
    if state != ADMISSION_MUTATION_ACTIVE && state != ADMISSION_MUTATION_FINALISED {
        bail!("CoreStore admission point head has invalid state");
    }
    Ok(())
}

fn validate_landed_byte_head(head: &LandedByteHead) -> Result<()> {
    validate_hash(&head.sha256, "landed byte point head hash")?;
    if head.relative_path.is_empty() {
        bail!("CoreStore landed byte point head path is empty");
    }
    if head.reference_count == 0 {
        bail!("CoreStore landed byte point head reference count is zero");
    }
    Ok(())
}
