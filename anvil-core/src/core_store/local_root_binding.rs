use super::*;

impl CoreStore {
    pub(super) async fn publication_generation_bindings_unlocked(
        &self,
        transaction_id: &str,
        publications: &[CoreMutationRootPublication],
    ) -> Result<BTreeMap<String, u64>> {
        let mut bindings = BTreeMap::new();
        for publication in publications {
            let root_key_hash = root_key_hash(&publication.root_anchor_key);
            let generation = self
                .implicit_root_generation_unlocked(
                    transaction_id,
                    &publication.root_anchor_key,
                    None,
                )
                .await?;
            if bindings.insert(root_key_hash.clone(), generation).is_some() {
                bail!("CoreMeta mutation declares root {root_key_hash} more than once");
            }
        }
        Ok(bindings)
    }

    pub(super) async fn bind_mutation_batch_root_generations_unlocked(
        &self,
        batch: &mut CoreMutationBatch,
    ) -> Result<BTreeMap<String, u64>> {
        let bindings = self
            .publication_generation_bindings_unlocked(
                &batch.transaction_id,
                &batch.root_publications,
            )
            .await?;
        self.bind_mutation_batch_to_generations(batch, &bindings)?;
        Ok(bindings)
    }

    pub(super) fn bind_mutation_batch_to_generations(
        &self,
        batch: &mut CoreMutationBatch,
        bindings: &BTreeMap<String, u64>,
    ) -> Result<()> {
        for operation in &mut batch.operations {
            let CoreMutationOperation::CoreMetaPut { payload, .. } = operation else {
                continue;
            };
            let mut common = core_meta_row_common_from_payload(payload)?;
            if common.root_key_hash.is_empty() {
                continue;
            }
            common.root_generation = *bindings.get(&common.root_key_hash).ok_or_else(|| {
                anyhow!(
                    "CoreMeta mutation payload references undeclared root {}",
                    common.root_key_hash
                )
            })?;
            common.transaction_id = batch.transaction_id.clone();
            *payload = replace_core_meta_row_common(payload, &common)?;
        }
        Ok(())
    }

    pub(super) fn bind_encoded_rows_to_generations(
        &self,
        rows: &mut [CoreMetaEncodedOwnedRow],
        transaction_id: &str,
        bindings: &BTreeMap<String, u64>,
    ) -> Result<()> {
        for row in rows {
            if row.root_key_hash.is_empty() {
                continue;
            }
            let generation = *bindings.get(&row.root_key_hash).ok_or_else(|| {
                anyhow!(
                    "CoreMeta encoded row references undeclared root {}",
                    row.root_key_hash
                )
            })?;
            self.meta
                .rebind_encoded_row_publication(row, generation, transaction_id)?;
        }
        Ok(())
    }
}
