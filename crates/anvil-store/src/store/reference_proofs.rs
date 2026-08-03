use super::*;
use crate::ObjectMutation;
use crate::model::MAX_OBJECT_MUTATION_REFERENCE_DELTAS;
use crate::watch::{
    ReferenceProof, decode_reference_proof, encode_reference_proof, reference_proof_key,
};

impl Store {
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
