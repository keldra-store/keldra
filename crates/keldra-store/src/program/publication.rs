use super::validation::{
    PreparedAliasPublication, prepared_alias_publications, publishes_physical_write,
};
use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AtomicBatchPublicationMarker {
    cursor: u64,
    bundle_hash: PreparedBundleHash,
}

/// A complete atomic delivery unit whose descriptors have been proven to be
/// the exact writes and logical alias observations in one sealed bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedAtomicBatchPublication {
    cursor: u64,
    bundle_hash: PreparedBundleHash,
    affected_routes: Vec<crate::AtomicBatchRoute>,
    mutations: Vec<crate::AtomicBatchMutation>,
    aliases: Vec<PreparedAliasPublication>,
}

impl SealedAtomicBatchPublication {
    pub fn from_prepared(
        cursor: u64,
        bundle_ref: PreparedBundleRef,
        bundle_hash: PreparedBundleHash,
        record: &PreparedProgramRecord,
        stages: &[ProgramPathStage],
        finalized: &[ProgramPathMutation],
        alias_finalized: &[ProgramAliasRegistryMutation],
    ) -> Result<Self, ProgramStoreError> {
        validate_prepared_record(record)?;
        let encoded = serde_json::to_vec(record).map_err(program_storage_error)?;
        if cursor == 0
            || bundle_ref.length != encoded.len() as u64
            || bundle_ref.hash != bundle_hash.0
            || *blake3::hash(&encoded).as_bytes() != bundle_ref.hash
            || stages.len() != record.writes.len()
            || finalized.len() != stages.len()
        {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }
        for ((stage, mutation), write) in stages.iter().zip(finalized).zip(&record.writes) {
            stage.validate()?;
            mutation.validate()?;
            if stage.bundle_hash != bundle_hash
                || stage.program_hash != record.program_hash
                || stage.authority != record.authority
                || stage.participant_manifest_hash
                    != record.participant_manifest_hash(bundle_hash)?
                || stage.path != write.path
                || stage.expected != write.expected
                || stage.previous_version != write.previous_version
                || stage.version != write.version
                || mutation.commit_cursor != cursor
                || mutation.stage != *stage
            {
                return Err(ProgramStoreError::PreparedBundleMismatch);
            }
        }
        validate_alias_registry_finalizations(
            cursor,
            bundle_hash,
            record,
            stages,
            alias_finalized,
        )?;
        let published_physical = finalized
            .iter()
            .filter(|mutation| publishes_physical_write(record, &mutation.stage.path))
            .cloned()
            .collect::<Vec<_>>();
        let (mut affected_routes, mutations) = atomic_batch_descriptors(&published_physical);
        let aliases = prepared_alias_publications(record)?;
        affected_routes.extend(aliases.iter().map(|alias| crate::AtomicBatchRoute {
            tenant_id: alias.identity.tenant_id.0,
            bucket_id: alias.identity.bucket_id.0,
        }));
        affected_routes.sort_unstable();
        affected_routes.dedup();
        let publication = Self {
            cursor,
            bundle_hash,
            affected_routes,
            mutations,
            aliases,
        };
        publication.validate_bound()?;
        Ok(publication)
    }

    fn validate_bound(&self) -> Result<(), ProgramStoreError> {
        if self.mutations.is_empty() && self.aliases.is_empty() {
            return Ok(());
        }
        let mut mutations = self.mutations.clone();
        mutations.extend(self.aliases.iter().map(|alias| crate::AtomicBatchMutation {
            tenant_id: alias.identity.tenant_id.0,
            bucket_id: alias.identity.bucket_id.0,
            exact_path: alias.requested_path.clone(),
            canonical_path: Some(alias.canonical_path.clone()),
            path_version: alias.canonical_version,
            deleted: alias.deleted,
            source_id: crate::SourceId {
                node_id: u16::MAX,
                source_epoch: [u8::MAX; 32],
            },
            source_journal_position: u64::MAX,
        }));
        mutations.sort_unstable();
        let event = crate::LocalChange::atomic_batch_published(
            u64::MAX,
            self.cursor,
            self.bundle_hash,
            self.affected_routes.clone(),
            mutations,
        );
        let crate::LocalChange::AtomicBatchPublished(batch) = &event else {
            unreachable!("atomic constructor returned another change kind");
        };
        batch
            .validate()
            .map_err(|message| ProgramStoreError::InvalidBundle(message.into()))?;
        let bytes = crate::watch::encoded_change_len(&event).map_err(program_storage_error)?;
        if bytes > crate::MAX_ATOMIC_BATCH_PUBLISHED_BYTES {
            return Err(ProgramStoreError::InvalidBundle(format!(
                "atomic batch publication requires {bytes} bytes; maximum is {}",
                crate::MAX_ATOMIC_BATCH_PUBLISHED_BYTES
            )));
        }
        Ok(())
    }
}

fn validate_alias_registry_finalizations(
    cursor: u64,
    bundle_hash: PreparedBundleHash,
    record: &PreparedProgramRecord,
    path_stages: &[ProgramPathStage],
    finalized: &[ProgramAliasRegistryMutation],
) -> Result<(), ProgramStoreError> {
    let writes = record.alias_registry_writes()?;
    if writes.len() != finalized.len() {
        return Err(ProgramStoreError::PreparedBundleMismatch);
    }
    let begin_cursor = path_stages
        .first()
        .map(|stage| stage.begin_cursor)
        .or_else(|| {
            finalized
                .first()
                .map(|mutation| mutation.stage.begin_cursor)
        });
    for ((target, expected, replacement_aliases), mutation) in writes.into_iter().zip(finalized) {
        mutation.validate()?;
        let stage = &mutation.stage;
        if Some(stage.begin_cursor) != begin_cursor
            || stage.bundle_hash != bundle_hash
            || stage.program_hash != record.program_hash
            || stage.authority != record.authority
            || stage.participant_manifest_hash != record.participant_manifest_hash(bundle_hash)?
            || stage.tenant_id != target.tenant_id
            || stage.bucket_id != target.bucket_id
            || stage.target != target.path
            || stage.expected.as_ref() != expected
            || &stage.replacement_aliases != replacement_aliases
            || mutation.commit_cursor != cursor
        {
            return Err(ProgramStoreError::PreparedBundleMismatch);
        }
    }
    Ok(())
}

impl Store {
    /// Publish logical aliases and the complete derived-consumer delivery unit
    /// in one synced batch after every distributed physical path is durable.
    pub async fn publish_atomic_batch(
        &self,
        publication: SealedAtomicBatchPublication,
    ) -> Result<bool, ProgramStoreError> {
        publication.validate_bound()?;
        if publication.mutations.is_empty() && publication.aliases.is_empty() {
            return Ok(false);
        }
        let cursor = publication.cursor;
        let bundle_hash = publication.bundle_hash;
        let expected = AtomicBatchPublicationMarker {
            cursor,
            bundle_hash,
        };
        loop {
            let commit_guard = self.lock_commit("atomic_program").await;
            if let Some(existing) = self.read_program_json::<AtomicBatchPublicationMarker>(
                CF_METADATA,
                ATOMIC_BATCH_PUBLISHED_KEY,
            )? {
                if existing == expected {
                    return Ok(false);
                }
                if existing.cursor >= cursor {
                    return Err(ProgramStoreError::CommitCorruption { cursor });
                }
            }
            let journal = self
                .local_watch_status()
                .map_err(|error| ProgramStoreError::Storage(error.to_string()))?;
            let mut changes = Vec::with_capacity(publication.aliases.len() + 1);
            let mut mutations = publication.mutations.clone();
            for (index, alias) in publication.aliases.iter().enumerate() {
                let position = journal
                    .tail
                    .checked_add(u64::try_from(index).map_err(|_| {
                        ProgramStoreError::Storage("alias journal position is exhausted".into())
                    })?)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| {
                        ProgramStoreError::Storage("alias journal position is exhausted".into())
                    })?;
                changes.push(PendingLocalChange::AliasObjectHead {
                    identity: alias.identity,
                    exact_path: alias.requested_path.clone(),
                    canonical_path: alias.canonical_path.clone(),
                    path_version: alias.canonical_version,
                    deleted: alias.deleted,
                    program_commit_cursor: Some(cursor),
                });
                mutations.push(crate::AtomicBatchMutation {
                    tenant_id: alias.identity.tenant_id.0,
                    bucket_id: alias.identity.bucket_id.0,
                    exact_path: alias.requested_path.clone(),
                    canonical_path: Some(alias.canonical_path.clone()),
                    path_version: alias.canonical_version,
                    deleted: alias.deleted,
                    source_id: journal.source_id,
                    source_journal_position: position,
                });
            }
            mutations.sort_unstable();
            changes.push(PendingLocalChange::AtomicBatchPublished {
                cursor,
                bundle_hash,
                affected_routes: publication.affected_routes.clone(),
                mutations,
            });
            let mut batch = WriteBatch::default();
            let attempt = self.stage_local_changes(
                &mut batch,
                &changes,
                LocalReferenceEffects::NoReferenceEffects,
            );
            match attempt {
                Ok(()) => {
                    batch.put_cf(
                        self.program_cf(CF_METADATA)?,
                        ATOMIC_BATCH_PUBLISHED_KEY,
                        serde_json::to_vec(&expected).map_err(program_storage_error)?,
                    );
                    self.write_program_batch(batch)?;
                    self.notify_local_invalidations();
                    return Ok(true);
                }
                Err(MutationError::SourceJournalCapacity) => {
                    drop(commit_guard);
                    self.wait_for_mutation_capacity().await;
                }
                Err(error) => return Err(program_mutation_error(error)),
            }
        }
    }
}

fn atomic_batch_descriptors(
    finalized: &[ProgramPathMutation],
) -> (
    Vec<crate::AtomicBatchRoute>,
    Vec<crate::AtomicBatchMutation>,
) {
    let mut affected_routes = finalized
        .iter()
        .map(|mutation| crate::AtomicBatchRoute {
            tenant_id: mutation.stage.tenant_id,
            bucket_id: mutation.stage.bucket_id,
        })
        .collect::<Vec<_>>();
    affected_routes.sort_unstable();
    affected_routes.dedup();
    let mut mutations = finalized
        .iter()
        .map(|mutation| crate::AtomicBatchMutation {
            tenant_id: mutation.stage.tenant_id,
            bucket_id: mutation.stage.bucket_id,
            exact_path: mutation.stage.path.path.clone(),
            canonical_path: None,
            path_version: mutation.stage.version.id,
            deleted: mutation.stage.version.deleted,
            source_id: mutation.stamp.source_id,
            source_journal_position: mutation.stamp.source_journal_position,
        })
        .collect::<Vec<_>>();
    mutations.sort_unstable();
    (affected_routes, mutations)
}
