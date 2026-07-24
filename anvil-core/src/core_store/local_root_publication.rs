use super::local_root_failover::RootOwnerTerms;
use super::*;
use prost::Message;

const TRANSACTION_MANIFEST_BODY_SCHEMA: &str = "anvil.core.transaction_manifest_body.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoreMetaRootPublication {
    pub(crate) root_anchor_key: String,
    pub(crate) writer_families: Vec<String>,
    pub(crate) logical_manifests: Vec<CoreManifestLocator>,
    pub(crate) idempotency_key_hashes: Vec<String>,
    pub(crate) transaction_coordinator: bool,
}

impl From<&CoreMutationRootPublication> for CoreMetaRootPublication {
    fn from(publication: &CoreMutationRootPublication) -> Self {
        Self {
            root_anchor_key: publication.root_anchor_key.clone(),
            writer_families: publication.writer_families.clone(),
            logical_manifests: Vec::new(),
            idempotency_key_hashes: Vec::new(),
            transaction_coordinator: publication.transaction_coordinator,
        }
    }
}

pub(super) fn declared_coremeta_publications(
    publications: &[CoreMutationRootPublication],
) -> Result<Vec<CoreMetaRootPublication>> {
    let mut declared = BTreeMap::new();
    let mut coordinator_count = 0_usize;
    for publication in publications {
        let publication = CoreMetaRootPublication::from(publication);
        publication.validate()?;
        coordinator_count += usize::from(publication.transaction_coordinator);
        let root_key_hash = publication.root_key_hash();
        if declared
            .insert(root_key_hash.clone(), publication)
            .is_some()
        {
            bail!("CoreMeta mutation plan declares root {root_key_hash} more than once");
        }
    }
    if coordinator_count > 1 {
        bail!("CoreMeta mutation plan may name at most one transaction coordinator root");
    }
    Ok(declared.into_values().collect())
}

impl CoreMetaRootPublication {
    pub(crate) fn new(root_anchor_key: impl Into<String>, writer_family: WriterFamily) -> Self {
        Self::with_writer_families(root_anchor_key, vec![writer_family.as_str().to_string()])
    }

    pub(crate) fn with_writer_families(
        root_anchor_key: impl Into<String>,
        writer_families: Vec<String>,
    ) -> Self {
        Self {
            root_anchor_key: root_anchor_key.into(),
            writer_families,
            logical_manifests: Vec::new(),
            idempotency_key_hashes: Vec::new(),
            transaction_coordinator: false,
        }
    }

    pub(crate) fn coordinator(mut self) -> Self {
        self.transaction_coordinator = true;
        self
    }

    pub(crate) fn root_key_hash(&self) -> String {
        root_key_hash(&self.root_anchor_key)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_root_publication_key(&self.root_anchor_key)?;
        if self.writer_families.is_empty() {
            bail!("CoreMeta root publication must name at least one writer family");
        }
        let mut writer_families = self.writer_families.clone();
        writer_families.sort();
        writer_families.dedup();
        if writer_families != self.writer_families {
            bail!("CoreMeta root publication writer families must be sorted and unique");
        }
        for family in &self.writer_families {
            if WriterFamily::from_name(family).is_none() {
                bail!("CoreMeta root publication has unknown writer family {family}");
            }
        }
        let mut idempotency_hashes = self.idempotency_key_hashes.clone();
        idempotency_hashes.sort();
        idempotency_hashes.dedup();
        if idempotency_hashes != self.idempotency_key_hashes {
            bail!("CoreMeta root publication idempotency hashes must be sorted and unique");
        }
        for hash in &self.idempotency_key_hashes {
            validate_hash(hash, "CoreMeta root publication idempotency hash")?;
        }
        for locator in &self.logical_manifests {
            validate_manifest_locator(locator)?;
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Message)]
struct TransactionManifestBodyRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    manifest_hash: String,
    #[prost(bytes, tag = "4")]
    manifest_bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedRootPublication {
    pub(super) descriptor: CoreMetaRootPublication,
    pub(super) previous_root_hash: String,
    pub(super) transaction_manifest_locator: CoreManifestLocator,
    pub(super) transaction_manifest_row: CoreMetaEncodedOwnedRow,
    pub(super) post_root_generation: u64,
    pub(super) created_at_unix_nanos: u64,
}

impl CoreStore {
    pub(super) fn complete_implicit_stream_root_publications(
        batch: &mut CoreMutationBatch,
    ) -> Result<()> {
        let mut declared_by_hash = BTreeMap::new();
        for publication in &batch.root_publications {
            let root_key_hash = root_key_hash(&publication.root_anchor_key);
            if let Some(existing) =
                declared_by_hash.insert(root_key_hash.clone(), publication.root_anchor_key.clone())
                && existing != publication.root_anchor_key
            {
                bail!(
                    "CoreMeta mutation plan root hash collision between {existing} and {}",
                    publication.root_anchor_key
                );
            }
        }

        let mut missing = BTreeMap::new();
        for operation in &batch.operations {
            let CoreMutationOperation::StreamAppend { stream_id, .. } = operation else {
                continue;
            };
            let root_anchor_key =
                super::local_roots_layout::stream_coremeta_root_anchor_key(stream_id);
            let root_key_hash = root_key_hash(&root_anchor_key);
            match declared_by_hash.get(&root_key_hash) {
                Some(existing) if existing != &root_anchor_key => {
                    bail!(
                        "CoreMeta mutation plan root hash collision between {existing} and {root_anchor_key}"
                    );
                }
                Some(_) => {}
                None => {
                    missing.insert(root_key_hash, root_anchor_key);
                }
            }
        }

        batch.root_publications.extend(
            missing
                .into_values()
                .map(|root| CoreMutationRootPublication::new(root, WriterFamily::Stream.as_str())),
        );
        Ok(())
    }

    pub(super) fn validate_mutation_root_publications_unlocked(
        &self,
        batch: &CoreMutationBatch,
        scope_bound_streams: bool,
    ) -> Result<()> {
        validate_mutation_root_publication_header(batch)?;
        let selected = self.select_mutation_root_publications_unlocked(
            &batch.operations,
            &batch.root_publications,
            scope_bound_streams,
        )?;
        let used = selected
            .iter()
            .map(|publication| root_key_hash(&publication.root_anchor_key))
            .collect::<BTreeSet<_>>();
        if let Some(unused) = batch.root_publications.iter().find(|publication| {
            !publication.transaction_coordinator
                && !used.contains(&root_key_hash(&publication.root_anchor_key))
        }) {
            bail!(
                "CoreMeta mutation batch {} declares unused canonical root {}",
                batch.transaction_id,
                root_key_hash(&unused.root_anchor_key)
            );
        }
        Ok(())
    }

    pub(super) fn validate_admitted_mutation_root_publications(
        &self,
        batch: &CoreMutationBatch,
        scope_bound_streams: bool,
    ) -> Result<()> {
        validate_mutation_root_publication_header(batch)?;
        let declared = declared_coremeta_publications(&batch.root_publications)?
            .into_iter()
            .map(|publication| publication.root_key_hash())
            .collect::<BTreeSet<_>>();
        for operation in &batch.operations {
            let root_key_hash = match operation {
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    ..
                } => {
                    if scope_bound_streams {
                        root_key_hash(partition_id)
                    } else {
                        super::local_roots_layout::stream_coremeta_root_key_hash(stream_id)
                    }
                }
                CoreMutationOperation::CoreMetaPut { payload, .. } => {
                    core_meta_row_common_from_payload(payload)?.root_key_hash
                }
                CoreMutationOperation::CoreMetaDelete { .. } => {
                    // Admission already validated a delete against the row and
                    // its declared root. Recovery must not re-derive that
                    // immutable decision from newer materialised state: the row
                    // may have been changed or removed by a winning mutation.
                    continue;
                }
            };
            if !root_key_hash.is_empty() && !declared.contains(&root_key_hash) {
                bail!("CoreMeta mutation plan does not declare canonical root {root_key_hash}");
            }
        }
        Ok(())
    }

    pub(super) fn select_mutation_root_publications_unlocked(
        &self,
        operations: &[CoreMutationOperation],
        publications: &[CoreMutationRootPublication],
        scope_bound_streams: bool,
    ) -> Result<Vec<CoreMutationRootPublication>> {
        let declared = declared_coremeta_publications(publications)?
            .into_iter()
            .map(|publication| (publication.root_key_hash(), publication))
            .collect::<BTreeMap<_, _>>();
        let public_by_hash = publications
            .iter()
            .cloned()
            .map(|publication| (root_key_hash(&publication.root_anchor_key), publication))
            .collect::<BTreeMap<_, _>>();
        let mut used = BTreeSet::new();
        for operation in operations {
            let root_key_hash = match operation {
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    ..
                } => {
                    if scope_bound_streams {
                        root_key_hash(partition_id)
                    } else {
                        super::local_roots_layout::stream_coremeta_root_key_hash(stream_id)
                    }
                }
                CoreMutationOperation::CoreMetaPut { payload, .. } => {
                    core_meta_row_common_from_payload(payload)?.root_key_hash
                }
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    self.read_coremeta_row(cf, *table_id, tuple_key)?
                        .map(|payload| core_meta_row_common_from_payload(&payload))
                        .transpose()?
                        .map(|common| common.root_key_hash)
                        .unwrap_or_default()
                }
            };
            if root_key_hash.is_empty() {
                continue;
            }
            if !declared.contains_key(&root_key_hash) {
                bail!("CoreMeta mutation plan does not declare canonical root {root_key_hash}");
            }
            used.insert(root_key_hash);
        }
        used.into_iter()
            .map(|root_key_hash| {
                public_by_hash.get(&root_key_hash).cloned().ok_or_else(|| {
                    anyhow!("CoreMeta mutation plan lost canonical root {root_key_hash}")
                })
            })
            .collect()
    }

    pub(super) fn select_declared_publications_for_ops(
        &self,
        ops: &[CoreMetaBatchOp<'_>],
        declared: &[CoreMutationRootPublication],
    ) -> Result<Vec<CoreMetaRootPublication>> {
        let declared = declared_coremeta_publications(declared)?
            .into_iter()
            .map(|publication| (publication.root_key_hash(), publication))
            .collect::<BTreeMap<_, _>>();
        let rooted_hashes = self
            .meta
            .encode_batch_ops(ops)?
            .into_iter()
            .filter_map(|row| (!row.root_key_hash.is_empty()).then_some(row.root_key_hash))
            .collect::<BTreeSet<_>>();
        rooted_hashes
            .into_iter()
            .map(|root_key_hash| {
                declared.get(&root_key_hash).cloned().ok_or_else(|| {
                    anyhow!(
                        "CoreMeta mutation plan does not declare canonical root {root_key_hash}"
                    )
                })
            })
            .collect()
    }

    pub(super) async fn prepare_root_publication(
        &self,
        transaction_id: &str,
        descriptor: &CoreMetaRootPublication,
        post_root_generation: u64,
        coordinator_scope: Option<&(String, u64)>,
        created_at_unix_nanos: u64,
    ) -> Result<PreparedRootPublication> {
        descriptor.validate()?;
        validate_logical_id(transaction_id, "CoreMeta root publication transaction id")?;
        if created_at_unix_nanos == 0 {
            bail!("CoreMeta root publication timestamp must be nonzero");
        }
        let root_key_hash = descriptor.root_key_hash();
        let current_anchor = self
            .read_latest_root_anchor(&descriptor.root_anchor_key)
            .await?;
        self.ensure_root_publication_owner(current_anchor.as_ref())
            .await?;
        let pre_root_generation = current_anchor
            .as_ref()
            .map_or(0, |anchor| anchor.root_generation);
        if post_root_generation != pre_root_generation.saturating_add(1) {
            bail!(
                "CoreMeta root publication {} expected generation {}, got {}",
                descriptor.root_anchor_key,
                pre_root_generation.saturating_add(1),
                post_root_generation
            );
        }

        let mut mutation_ids = vec![transaction_id.to_string()];
        mutation_ids.sort();
        mutation_ids.dedup();
        let transaction_manifest = CoreTransactionManifestRecord {
            schema: "anvil.core.transaction_manifest.v1".to_string(),
            root_key_hash: root_key_hash.clone(),
            coordinator_root_key_hash: coordinator_scope.map(|(root, _)| root.clone()),
            coordinator_root_generation: coordinator_scope.map(|(_, generation)| *generation),
            mutation_ids,
            idempotency_key_hashes: descriptor.idempotency_key_hashes.clone(),
            pre_root_generation,
            post_root_generation,
            logical_manifests: descriptor.logical_manifests.clone(),
        };
        validate_transaction_manifest_record(&transaction_manifest, post_root_generation)?;
        let manifest_bytes = encode_transaction_manifest_record(&transaction_manifest)?;
        let stable_name = format!(
            "root-transaction-manifest:{}:{}",
            root_key_hash, transaction_id
        );
        let manifest_content_hash = sha256_hex(&manifest_bytes);
        let logical_file_id = canonical_logical_file_id(
            WriterFamily::CoreControl,
            post_root_generation,
            &stable_name,
            manifest_content_hash.as_bytes(),
        );
        let locator = inline_manifest_locator_from_body(
            logical_file_id,
            WriterFamily::CoreControl.as_str().to_string(),
            post_root_generation,
            &manifest_bytes,
        )?;
        let row = TransactionManifestBodyRowProto {
            common: Some(core_meta_committed_row_common(
                format!("root/{}", descriptor.root_anchor_key),
                root_key_hash.clone(),
                post_root_generation,
                transaction_id,
                created_at_unix_nanos,
            )),
            schema: TRANSACTION_MANIFEST_BODY_SCHEMA.to_string(),
            manifest_hash: locator.manifest_hash.clone(),
            manifest_bytes,
        };
        let payload = encode_deterministic_proto(&row);
        validate_transaction_manifest_body_row(&payload)?;
        let key = transaction_manifest_body_key(&locator.manifest_hash)?;
        let op = CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_TRANSACTION_MANIFEST_BODY_ROW,
            tuple_key: &key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        };
        let transaction_manifest_row = self
            .meta
            .encode_batch_ops(&[op])?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("CoreMeta transaction manifest row encoding produced no row"))?;

        Ok(PreparedRootPublication {
            descriptor: descriptor.clone(),
            previous_root_hash: current_anchor
                .as_ref()
                .map(hash_root_anchor_record)
                .transpose()?
                .unwrap_or_else(|| ZERO_HASH.to_string()),
            transaction_manifest_locator: locator,
            transaction_manifest_row,
            post_root_generation,
            created_at_unix_nanos,
        })
    }

    pub(super) fn prepared_root_anchor_for_publisher(
        &self,
        prepared: &PreparedRootPublication,
        outcome: &CoreMetaQuorumCommitOutcome,
        transaction_id: &str,
        publisher_node_id: &str,
    ) -> Result<CoreRootAnchorRecord> {
        validate_logical_id(publisher_node_id, "CoreMeta root publisher node id")?;
        if outcome.root_key_hash != prepared.descriptor.root_key_hash()
            || outcome.post_root_generation != prepared.post_root_generation
        {
            bail!("CoreMeta root publication outcome scope mismatch");
        }
        let owner_terms = self.root_owner_terms_for_publication(
            &outcome.root_key_hash,
            outcome.post_root_generation,
            publisher_node_id,
        )?;
        self.prepared_root_anchor_with_owner_terms(prepared, outcome, transaction_id, owner_terms)
    }

    pub(super) fn prepared_root_anchor_with_owner_terms(
        &self,
        prepared: &PreparedRootPublication,
        outcome: &CoreMetaQuorumCommitOutcome,
        transaction_id: &str,
        owner_terms: RootOwnerTerms,
    ) -> Result<CoreRootAnchorRecord> {
        if outcome.root_key_hash != prepared.descriptor.root_key_hash()
            || outcome.post_root_generation != prepared.post_root_generation
        {
            bail!("CoreMeta root publication outcome scope mismatch");
        }
        let final_block_count = prepared
            .descriptor
            .logical_manifests
            .iter()
            .flat_map(|locator| locator.block_locators.iter())
            .map(|block| u64::from(block.data_shards) + u64::from(block.parity_shards))
            .sum();
        Ok(CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: prepared.descriptor.root_anchor_key.clone(),
            root_key_hash: outcome.root_key_hash.clone(),
            root_generation: outcome.post_root_generation,
            previous_root_hash: prepared.previous_root_hash.clone(),
            transaction_manifest: Some(prepared.transaction_manifest_locator.clone()),
            checkpoint_manifest: None,
            core_meta_commit_certificate_hash: Some(outcome.certificate_hash.clone()),
            certificate_persist_receipt_hashes: outcome.certificate_persist_receipt_hashes.clone(),
            publisher_node_id: owner_terms.owner_node_id,
            publisher_epoch: owner_terms.owner_epoch,
            partition_owner_fence: owner_terms.owner_fence,
            created_at_unix_nanos: prepared.created_at_unix_nanos,
            root_state: "committed".to_string(),
            mutation_first: Some(transaction_id.to_string()),
            mutation_last: Some(transaction_id.to_string()),
            writer_families: prepared.descriptor.writer_families.clone(),
            manifest_count: prepared.descriptor.logical_manifests.len() as u64 + 1,
            final_block_count,
            genesis_bundle: None,
        })
    }

    pub(super) fn read_root_transaction_manifest(
        &self,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<CoreTransactionManifestRecord> {
        self.read_root_transaction_manifest_from(&self.meta, anchor)
    }

    fn read_root_transaction_manifest_from<R: CoreMetaReader>(
        &self,
        reader: &R,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<CoreTransactionManifestRecord> {
        let locator = anchor
            .transaction_manifest
            .as_ref()
            .ok_or_else(|| anyhow!("CoreStore root anchor is missing transaction manifest"))?;
        validate_manifest_locator(locator)?;
        let key = transaction_manifest_body_key(&locator.manifest_hash)?;
        // Manifest bodies are part of the root-cache proof used by the
        // publication filter itself, so a filtered read would recurse.
        let payload = reader
            .get(CF_TRANSACTIONS, TABLE_TRANSACTION_MANIFEST_BODY_ROW, &key)?
            .ok_or_else(|| anyhow!("CoreStore transaction manifest body row is missing"))?;
        let (common, manifest_bytes) = decode_transaction_manifest_body_row(&payload)?;
        if common.root_key_hash != anchor.root_key_hash
            || common.root_generation != anchor.root_generation
        {
            bail!("CoreStore transaction manifest body row root scope mismatch");
        }
        if manifest_bytes.len() as u64 != locator.manifest_length
            || format!("sha256:{}", sha256_hex(&manifest_bytes)) != locator.manifest_hash
        {
            bail!("CoreStore transaction manifest locator does not match body row");
        }
        let manifest = decode_transaction_manifest_record(&manifest_bytes)?;
        validate_transaction_manifest_record(&manifest, anchor.root_generation)?;
        if manifest.root_key_hash != anchor.root_key_hash {
            bail!("CoreStore transaction manifest root hash mismatch");
        }
        Ok(manifest)
    }

    pub(super) fn root_generation_is_published(
        &self,
        root_key_hash_value: &str,
        generation: u64,
        transaction_id: &str,
    ) -> Result<bool> {
        self.root_generation_is_published_from(
            &self.meta,
            root_key_hash_value,
            generation,
            transaction_id,
        )
    }

    pub(super) fn root_generation_is_published_from<R: CoreMetaReader>(
        &self,
        reader: &R,
        root_key_hash_value: &str,
        generation: u64,
        transaction_id: &str,
    ) -> Result<bool> {
        if root_key_hash_value.is_empty() {
            return Ok(generation == 0);
        }
        let Some(manifest) = self.root_generation_commit_manifest_from(
            reader,
            root_key_hash_value,
            generation,
            transaction_id,
        )?
        else {
            return Ok(false);
        };
        let Some(coordinator_root_key_hash) = manifest.coordinator_root_key_hash.as_deref() else {
            return Ok(true);
        };
        let coordinator_root_generation =
            manifest.coordinator_root_generation.ok_or_else(|| {
                anyhow!("CoreStore transaction manifest coordinator generation missing")
            })?;
        if coordinator_root_key_hash == root_key_hash_value
            && coordinator_root_generation == generation
        {
            return Ok(true);
        }
        let Some(coordinator_manifest) = self.root_generation_commit_manifest_from(
            reader,
            coordinator_root_key_hash,
            coordinator_root_generation,
            transaction_id,
        )?
        else {
            return Ok(false);
        };
        Ok(coordinator_manifest.coordinator_root_key_hash.as_deref()
            == Some(coordinator_root_key_hash)
            && coordinator_manifest.coordinator_root_generation
                == Some(coordinator_root_generation))
    }

    pub(super) fn root_generation_commit_manifest(
        &self,
        root_key_hash_value: &str,
        generation: u64,
        transaction_id: &str,
    ) -> Result<Option<CoreTransactionManifestRecord>> {
        self.root_generation_commit_manifest_from(
            &self.meta,
            root_key_hash_value,
            generation,
            transaction_id,
        )
    }

    fn root_generation_commit_manifest_from<R: CoreMetaReader>(
        &self,
        reader: &R,
        root_key_hash_value: &str,
        generation: u64,
        transaction_id: &str,
    ) -> Result<Option<CoreTransactionManifestRecord>> {
        // The root-cache generation row is the publication authority being
        // evaluated; it cannot depend on its own visibility decision.
        let Some(payload) = reader.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_key(root_key_hash_value, generation),
        )?
        else {
            return Ok(None);
        };
        let anchor = decode_root_cache_row(&payload)?;
        if anchor.root_key_hash != root_key_hash_value || anchor.root_generation != generation {
            bail!("CoreStore root publication row scope mismatch");
        }
        let certificate = self
            .validate_root_anchor_coremeta_commit_evidence_from(reader, &anchor)?
            .ok_or_else(|| anyhow!("CoreStore published root generation has no certificate"))?;
        let manifest = self.read_root_transaction_manifest_from(reader, &anchor)?;
        if certificate.transaction_id != transaction_id
            || !manifest.mutation_ids.iter().any(|id| id == transaction_id)
        {
            return Ok(None);
        }
        Ok(Some(manifest))
    }
}

pub(in crate::core_store) fn validate_transaction_manifest_body_row(payload: &[u8]) -> Result<()> {
    let (common, manifest_bytes) = decode_transaction_manifest_body_row(payload)?;
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.root_key_hash.is_empty()
        || common.root_generation == 0
        || common.transaction_id.is_empty()
    {
        bail!("CoreStore transaction manifest body row common metadata is invalid");
    }
    let manifest = decode_transaction_manifest_record(&manifest_bytes)?;
    validate_transaction_manifest_record(&manifest, common.root_generation)?;
    if manifest.root_key_hash != common.root_key_hash
        || !manifest
            .mutation_ids
            .iter()
            .any(|id| id == &common.transaction_id)
    {
        bail!("CoreStore transaction manifest body row scope mismatch");
    }
    Ok(())
}

pub(super) fn decode_transaction_manifest_body_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, Vec<u8>)> {
    let row = decode_deterministic_proto::<TransactionManifestBodyRowProto>(
        payload,
        "CoreStore transaction manifest body row",
    )?;
    if row.schema != TRANSACTION_MANIFEST_BODY_SCHEMA {
        bail!("CoreStore transaction manifest body row has invalid schema");
    }
    if row.manifest_bytes.is_empty()
        || row.manifest_hash != format!("sha256:{}", sha256_hex(&row.manifest_bytes))
    {
        bail!("CoreStore transaction manifest body row hash mismatch");
    }
    let common = row.common.ok_or_else(|| {
        anyhow!("CoreStore transaction manifest body row missing common metadata")
    })?;
    Ok((common, row.manifest_bytes))
}

pub(super) fn transaction_manifest_body_key(manifest_hash: &str) -> Result<Vec<u8>> {
    validate_hash(manifest_hash, "CoreStore transaction manifest body hash")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-transaction-manifest"),
        CoreMetaTuplePart::Hash(manifest_hash),
    ])
}

fn validate_mutation_root_publication_header(batch: &CoreMutationBatch) -> Result<()> {
    let coordinator = batch
        .root_publications
        .iter()
        .filter(|publication| publication.transaction_coordinator)
        .collect::<Vec<_>>();
    let [coordinator] = coordinator.as_slice() else {
        bail!("CoreMeta mutation batch must declare exactly one coordinator root");
    };
    if coordinator.root_anchor_key != batch.scope_partition {
        bail!("CoreMeta mutation batch coordinator root must equal its scope partition");
    }
    if !coordinator
        .writer_families
        .iter()
        .any(|family| family == WriterFamily::CoreControl.as_str())
    {
        bail!("CoreMeta mutation batch coordinator root must include core_control");
    }
    Ok(())
}

fn validate_root_publication_key(root_anchor_key: &str) -> Result<()> {
    if root_anchor_key.is_empty()
        || root_anchor_key.len() > 1024
        || root_anchor_key.starts_with('/')
        || root_anchor_key.ends_with('/')
        || root_anchor_key
            .split('/')
            .any(|part| part.is_empty() || part.chars().any(char::is_control))
    {
        bail!("CoreMeta root publication key is not canonical");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_append(stream_id: &str) -> CoreMutationOperation {
        CoreMutationOperation::StreamAppend {
            partition_id: "tenant/1/streams/0".to_string(),
            stream_id: stream_id.to_string(),
            record_kind: "test".to_string(),
            payload: Vec::new(),
            idempotency_key: None,
        }
    }

    #[test]
    fn implicit_stream_roots_are_completed_before_admission() {
        let mut batch = CoreMutationBatch {
            transaction_id: "stream-root-plan".to_string(),
            scope_partition: "tenant/1/streams/0".to_string(),
            committed_by_principal: "principal:test".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant/1/streams/0",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
            ],
            preconditions: Vec::new(),
            operations: vec![stream_append("z-stream"), stream_append("a-stream")],
        };

        CoreStore::complete_implicit_stream_root_publications(&mut batch).unwrap();

        let roots = batch
            .root_publications
            .iter()
            .map(|publication| publication.root_anchor_key.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            roots,
            BTreeSet::from(["stream/a-stream", "stream/z-stream", "tenant/1/streams/0",])
        );
        assert_eq!(
            batch
                .root_publications
                .iter()
                .filter(|publication| publication.transaction_coordinator)
                .count(),
            1
        );
    }

    #[test]
    fn existing_domain_owned_stream_root_is_not_duplicated() {
        let mut batch = CoreMutationBatch {
            transaction_id: "domain-stream-root-plan".to_string(),
            scope_partition: "tenant/1/streams/0".to_string(),
            committed_by_principal: "principal:test".to_string(),
            root_publications: vec![
                CoreMutationRootPublication::new(
                    "tenant/1/streams/0",
                    WriterFamily::CoreControl.as_str(),
                )
                .coordinator(),
                CoreMutationRootPublication::new(
                    "stream/domain-stream",
                    WriterFamily::TypedMetadata.as_str(),
                ),
            ],
            preconditions: Vec::new(),
            operations: vec![stream_append("domain-stream")],
        };

        CoreStore::complete_implicit_stream_root_publications(&mut batch).unwrap();

        assert_eq!(batch.root_publications.len(), 2);
        assert_eq!(
            batch.root_publications[1].writer_families,
            vec![WriterFamily::TypedMetadata.as_str().to_string()]
        );
    }
}
