use super::*;
use crate::ObjectMutation;
use crate::model::MAX_OBJECT_MUTATION_REFERENCE_DELTAS;
use crate::watch::{
    REFERENCE_PROOF_KEY_BYTES, ReferenceProof, decode_reference_proof, encode_reference_proof,
    reference_proof_key,
};

pub const MAX_REFERENCE_PROOF_EXPORT_RECORDS: u32 = 1_000;
pub const MAX_REFERENCE_PROOF_EXPORT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReferenceProofExportError {
    #[error("reference-proof export limits are invalid")]
    InvalidLimits,
    #[error("one reference proof requires {required_bytes} bytes, exceeding the page limit")]
    RecordTooLarge { required_bytes: u64 },
    #[error("reference-proof export storage failed: {0}")]
    Storage(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReferenceProofCursor {
    pub source: SourceId,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReferenceProofPage {
    pub proofs: Vec<ReferenceProof>,
    pub next_cursor: Option<ReferenceProofCursor>,
}

impl Store {
    /// Enumerates the retained fixed proof namespace in source/offset key
    /// order. It shares the existing journal column family and retention.
    pub fn export_reference_proofs(
        &self,
        cursor: Option<&ReferenceProofCursor>,
        max_records: u32,
        max_bytes: u64,
    ) -> Result<ReferenceProofPage, ReferenceProofExportError> {
        if max_records == 0
            || max_records > MAX_REFERENCE_PROOF_EXPORT_RECORDS
            || max_bytes == 0
            || max_bytes > MAX_REFERENCE_PROOF_EXPORT_BYTES
        {
            return Err(ReferenceProofExportError::InvalidLimits);
        }
        let after = cursor
            .map(|cursor| {
                validate_proof_coordinates(cursor.source, cursor.offset)
                    .map_err(export_storage)?;
                Ok(reference_proof_key(cursor.source, cursor.offset))
            })
            .transpose()?;
        let prefix = [crate::key::STORAGE_KEY_FORMAT_VERSION, 0xff];
        let start = after.as_ref().map_or(prefix.as_slice(), |key| key.as_slice());
        let mut proofs = Vec::with_capacity(max_records as usize);
        let mut encoded_bytes = 0_u64;
        let mut last = None;
        let mut more = false;
        for entry in self.db.iterator_cf(
            self.cf(CF_LOCAL_INVALIDATIONS).map_err(export_storage)?,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, encoded) = entry.map_err(export_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key.len() != REFERENCE_PROOF_KEY_BYTES {
                return Err(ReferenceProofExportError::Storage(
                    "reference-proof handoff key is malformed".into(),
                ));
            }
            if after.as_ref().is_some_and(|after| key.as_ref() <= after.as_slice()) {
                continue;
            }
            let proof = decode_reference_proof(&encoded).map_err(export_storage)?;
            validate_stored_proof(&proof).map_err(ReferenceProofExportError::Storage)?;
            if reference_proof_key(proof.source_id, proof.offset()).as_slice() != key.as_ref() {
                return Err(ReferenceProofExportError::Storage(
                    "reference-proof handoff key disagrees with its value".into(),
                ));
            }
            let proof_bytes = u64::try_from(
                serde_json::to_vec(&proof)
                    .map_err(export_storage)?
                    .len(),
            )
            .map_err(|_| {
                ReferenceProofExportError::Storage("reference-proof size overflow".into())
            })?;
            if proof_bytes > MAX_REFERENCE_PROOF_EXPORT_BYTES
                || (proofs.is_empty() && proof_bytes > max_bytes)
            {
                return Err(ReferenceProofExportError::RecordTooLarge {
                    required_bytes: proof_bytes,
                });
            }
            if proofs.len() == max_records as usize
                || encoded_bytes.saturating_add(proof_bytes) > max_bytes
            {
                more = true;
                break;
            }
            encoded_bytes += proof_bytes;
            last = Some(ReferenceProofCursor {
                source: proof.source_id,
                offset: proof.offset(),
            });
            proofs.push(proof);
        }
        Ok(ReferenceProofPage {
            proofs,
            next_cursor: more.then_some(last.expect("a full proof page has a cursor")),
        })
    }

    /// Installs one exact proof selected by a metadata read quorum. Reusing a
    /// source position for another fingerprint or path fails closed.
    pub async fn install_quorum_reconciled_reference_proof(
        &self,
        proof: &ReferenceProof,
    ) -> Result<bool, MutationError> {
        validate_stored_proof(proof).map_err(MutationError::InvalidObjectMutation)?;
        let _guard = self.commit_lock.lock().await;
        if let Some(existing) = self.read_reference_proof(proof.source_id, proof.offset())? {
            if existing == *proof {
                return Ok(true);
            }
            return Err(MutationError::ObjectMutationConflict);
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.cf(CF_LOCAL_INVALIDATIONS)?,
                reference_proof_key(proof.source_id, proof.offset()),
                encode_reference_proof(proof).map_err(storage_error)?,
                &options,
            )
            .map_err(storage_error)?;
        Ok(false)
    }

    /// Reads the one exact reference proof stored for a source-journal
    /// position. The key and typed value must agree; corruption fails closed.
    pub fn read_reference_proof(
        &self,
        source: SourceId,
        offset: u64,
    ) -> Result<Option<ReferenceProof>, MutationError> {
        validate_proof_coordinates(source, offset)?;
        let Some(encoded) = self
            .db
            .get_cf(
                self.cf(CF_LOCAL_INVALIDATIONS)?,
                reference_proof_key(source, offset),
            )
            .map_err(storage_error)?
        else {
            return Ok(None);
        };
        let proof = decode_reference_proof(&encoded).map_err(storage_error)?;
        validate_stored_proof(&proof).map_err(MutationError::Storage)?;
        if proof.source_id != source || proof.offset() != offset {
            return Err(MutationError::Storage(
                "reference proof key does not match its typed value".into(),
            ));
        }
        Ok(Some(proof))
    }

    /// Deletes a proof only when the durable typed value still exactly equals
    /// `expected`. A missing or replaced proof is left untouched.
    pub async fn delete_reference_proof_if_matches(
        &self,
        expected: &ReferenceProof,
    ) -> Result<bool, MutationError> {
        validate_stored_proof(expected).map_err(MutationError::InvalidObjectMutation)?;
        let _commit_guard = self.commit_lock.lock().await;
        let Some(actual) = self.read_reference_proof(expected.source_id, expected.offset())? else {
            return Ok(false);
        };
        if actual != *expected {
            return Ok(false);
        }
        let mut batch = WriteBatch::default();
        batch.delete_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)?,
            reference_proof_key(expected.source_id, expected.offset()),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        Ok(true)
    }

    /// Stages the exact proof derived from a validated typed object mutation.
    /// Existing exact evidence is an idempotent no-op; reuse of one source
    /// position for a different mutation is corruption and never overwrites.
    pub(super) fn stage_object_mutation_reference_proof(
        &self,
        batch: &mut WriteBatch,
        mutation: &ObjectMutation,
    ) -> Result<bool, MutationError> {
        let expected = proof_for_mutation(mutation)?;
        if let Some(existing) = self.read_reference_proof(expected.source_id, expected.offset())? {
            if existing != expected {
                return Err(MutationError::ObjectMutationConflict);
            }
            return Ok(false);
        }
        batch.put_cf(
            self.cf(CF_LOCAL_INVALIDATIONS)?,
            reference_proof_key(expected.source_id, expected.offset()),
            encode_reference_proof(&expected).map_err(storage_error)?,
        );
        Ok(true)
    }
}

fn export_storage(error: impl std::fmt::Display) -> ReferenceProofExportError {
    ReferenceProofExportError::Storage(error.to_string())
}

fn proof_for_mutation(mutation: &ObjectMutation) -> Result<ReferenceProof, MutationError> {
    mutation.validate()?;
    let proof = ReferenceProof::new(
        mutation.stamp.source_id,
        mutation.stamp.mutation_fingerprint,
        LocalChange::object_head(
            mutation.stamp.source_journal_position,
            mutation.tenant_id,
            mutation.bucket_id,
            mutation.exact_path.clone(),
            mutation.version.id,
            mutation.version.deleted,
            mutation.reference_deltas.clone(),
        ),
    );
    validate_stored_proof(&proof).map_err(MutationError::InvalidObjectMutation)?;
    Ok(proof)
}

fn validate_proof_coordinates(source: SourceId, offset: u64) -> Result<(), MutationError> {
    if source.node_id == 0 || source.source_epoch == [0; 32] || offset == 0 {
        return Err(MutationError::InvalidObjectMutation(
            "reference proof source identity or offset is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_stored_proof(proof: &ReferenceProof) -> Result<(), String> {
    if proof.source_id.node_id == 0
        || proof.source_id.source_epoch == [0; 32]
        || proof.offset() == 0
    {
        return Err("reference proof source identity or offset is invalid".into());
    }
    let LocalChange::ObjectHead(change) = &proof.change else {
        return Err("reference proof is not an object-head mutation".into());
    };
    if change.tenant_id == 0 || change.bucket_id == 0 || change.path_version.0 == 0 {
        return Err("reference proof stable identity or path version is invalid".into());
    }
    ObjectKey::new(
        "reference-proof",
        "reference-proof",
        change.exact_path.clone(),
    )
    .map_err(|error| format!("reference proof path is invalid: {error}"))?;
    if change.reference_deltas.len() > MAX_OBJECT_MUTATION_REFERENCE_DELTAS
        || change
            .reference_deltas
            .iter()
            .any(|delta| !matches!(delta.change, -1 | 1))
    {
        return Err("reference proof deltas are malformed".into());
    }
    for (index, delta) in change.reference_deltas.iter().enumerate() {
        if change.reference_deltas[..index]
            .iter()
            .any(|earlier| earlier.blob == delta.blob)
        {
            return Err("reference proof repeats one reference delta".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
