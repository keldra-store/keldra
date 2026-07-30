impl MvccStore {
    /// Enumerates compact-Raft partitions required by durable background work.
    /// Delivered/completed rows no longer require assignment coverage.
    pub fn required_background_work_partitions(&self) -> Result<BTreeSet<u64>> {
        let mut partitions = BTreeSet::new();
        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        for (prefix_suffix, kind) in [
            (b"object-job/".as_slice(), "object-materialisation"),
            (b"shard-repair/".as_slice(), "shard-repair"),
            (b"local-upgrade/".as_slice(), "local-durability-upgrade"),
            (b"index-finalization/".as_slice(), "index-finalization"),
            (
                b"personaldb-postcommit/".as_slice(),
                "personaldb-postcommit",
            ),
            (
                b"git-source-postcommit/".as_slice(),
                "git-source-postcommit",
            ),
            (
                b"hf-ingestion-postcommit/".as_slice(),
                "hf-ingestion-postcommit",
            ),
            (
                b"bucket-locator-finalization/".as_slice(),
                "bucket-locator-finalization",
            ),
            (
                b"object-link-finalization/".as_slice(),
                "object-link-finalization",
            ),
        ] {
            let prefix = self.key(prefix_suffix);
            for row in self.db.iterator_cf(
                materialisation_cf,
                IteratorMode::From(&prefix, Direction::Forward),
            ) {
                let (key, value) = row?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let logical_identity = if kind == "object-materialisation" {
                    let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
                    if record.state == ObjectMaterialisationState::Complete {
                        continue;
                    }
                    record.job.assignment_logical_identity()
                } else if kind == "shard-repair" {
                    let record: ShardRepairRecord = serde_json::from_slice(&value)?;
                    if record.state == ShardRepairState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity
                } else if kind == "index-finalization" {
                    let record: IndexFinalizationRecord = serde_json::from_slice(&value)?;
                    if record.state == IndexFinalizationState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity()
                } else if kind == "personaldb-postcommit" {
                    let record: PersonalDbPostCommitRecord = serde_json::from_slice(&value)?;
                    if record.state == PersonalDbPostCommitState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity()
                } else if kind == "git-source-postcommit" {
                    let record: GitSourcePostCommitRecord = serde_json::from_slice(&value)?;
                    if record.state == GitSourcePostCommitState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity()
                } else if kind == "hf-ingestion-postcommit" {
                    let record: HfIngestionPostCommitRecord = serde_json::from_slice(&value)?;
                    if record.state == HfIngestionPostCommitState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity()
                } else if kind == "bucket-locator-finalization" {
                    let record: BucketLocatorFinalizationRecord = serde_json::from_slice(&value)?;
                    if record.state == BucketLocatorFinalizationState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity()
                } else if kind == "object-link-finalization" {
                    let record: ObjectLinkFinalizationRecord = serde_json::from_slice(&value)?;
                    if record.state == ObjectLinkFinalizationState::Complete {
                        continue;
                    }
                    record.job.target_logical_identity()
                } else {
                    let record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&value)?;
                    if record.state == LocalDurabilityUpgradeState::Complete {
                        continue;
                    }
                    format!("transaction/{}", record.job.transaction_id)
                };
                partitions.insert(crate::mvcc_worker_authority::work_partition_id(
                    kind,
                    &logical_identity,
                )?);
            }
        }
        let outbox_cf = self.cf(CF_OUTBOX)?;
        let prefix = self.key(b"event/");
        for row in self
            .db
            .iterator_cf(outbox_cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: OutboxRecord = serde_json::from_slice(&value)?;
            if record.state == OutboxState::Delivered {
                continue;
            }
            partitions.insert(
                crate::mvcc_outbox::StreamOutboxEvent::decode(&record.payload)?.partition_id,
            );
        }
        Ok(partitions)
    }

    pub fn pinned_local_upgrade_assignments(
        &self,
    ) -> Result<std::collections::BTreeMap<u64, crate::mvcc_transaction::NodeIncarnation>> {
        let mut assignments = std::collections::BTreeMap::new();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"local-upgrade/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&value)?;
            if record.state == LocalDurabilityUpgradeState::Complete {
                continue;
            }
            let holder = record.job.local_holder()?;
            let partition_id = crate::mvcc_worker_authority::work_partition_id(
                "local-durability-upgrade",
                &format!("transaction/{}", record.job.transaction_id),
            )?;
            if assignments
                .insert(partition_id, holder.clone())
                .is_some_and(|existing| existing != holder)
            {
                bail!("local durability upgrade partition names conflicting holders");
            }
        }
        Ok(assignments)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = MVCC_COLUMN_FAMILIES
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
        let db = DB::open_cf_descriptors(&options, path.as_ref(), descriptors)
            .with_context(|| format!("open MVCC RocksDB at {}", path.as_ref().display()))?;
        Self::from_db(Arc::new(db), "cluster")
    }

    pub fn from_db(db: Arc<DB>, cluster_id: &str) -> Result<Self> {
        if cluster_id.is_empty() {
            bail!("MVCC store cluster ID is required");
        }
        for name in MVCC_COLUMN_FAMILIES {
            if db.cf_handle(name).is_none() {
                bail!("missing MVCC RocksDB column family {name}");
            }
        }
        let mut scope = Vec::with_capacity(4 + cluster_id.len());
        scope.extend_from_slice(&(cluster_id.len() as u32).to_be_bytes());
        scope.extend_from_slice(cluster_id.as_bytes());
        Ok(Self {
            db,
            cluster_id: cluster_id.to_string(),
            scope,
            decision_transition: Arc::new(Mutex::new(())),
            materialisation_transition: Arc::new(Mutex::new(())),
            outbox_transition: Arc::new(Mutex::new(())),
        })
    }

    /// Capture all cluster-scoped MVCC column families at one RocksDB sequence.
    ///
    /// The transition locks are held only while the RocksDB snapshot is
    /// created. Export iteration then proceeds without stopping transaction
    /// application or background workers.
    pub fn export_checkpoint(&self) -> Result<MvccCheckpoint> {
        let decision_transition = self.decision_transition.lock().unwrap();
        let materialisation_transition = self.materialisation_transition.lock().unwrap();
        let outbox_transition = self.outbox_transition.lock().unwrap();
        let snapshot = self.db.snapshot();
        drop(outbox_transition);
        drop(materialisation_transition);
        drop(decision_transition);

        let mut column_families = Vec::with_capacity(MVCC_COLUMN_FAMILIES.len());
        for name in MVCC_COLUMN_FAMILIES {
            let cf = self.cf(name)?;
            let mut entries = Vec::new();
            for row in snapshot.iterator_cf(cf, IteratorMode::From(&self.scope, Direction::Forward))
            {
                let (key, value) = row?;
                if !key.starts_with(&self.scope) {
                    break;
                }
                let relative_key = self.unscoped(&key)?.to_vec();
                if name == CF_META && relative_key == INSTALLED_CHECKPOINT_KEY {
                    continue;
                }
                entries.push(MvccCheckpointEntry {
                    key: relative_key,
                    value: value.to_vec(),
                });
            }
            column_families.push(MvccCheckpointColumnFamily {
                name: name.to_string(),
                entries,
            });
        }

        let meta = &column_families[3];
        let checkpoint = MvccCheckpoint {
            format_version: MVCC_CHECKPOINT_FORMAT_VERSION,
            cluster_id: self.cluster_id.clone(),
            decision_watermark: checkpoint_meta_version(meta, DECISION_WATERMARK_KEY)?,
            applied_version: checkpoint_meta_version(meta, APPLIED_VERSION_KEY)?,
            gc_watermark: checkpoint_meta_version(meta, GC_WATERMARK_KEY)?,
            column_families,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub fn export_checkpoint_bytes(&self) -> Result<Vec<u8>> {
        self.export_checkpoint()?.encode()
    }

    /// Atomically install a checkpoint into a clean replacement's cluster
    /// scope.
    ///
    /// A successful retry is detected by a donor-independent content identity.
    /// A later retry may install a different checkpoint only when none of its
    /// watermarks moves local state backwards. This lets preparation resume
    /// after the first checkpoint was applied but the learner admission did
    /// not complete.
    pub fn install_checkpoint(
        &self,
        checkpoint: &MvccCheckpoint,
    ) -> Result<MvccCheckpointInstallOutcome> {
        checkpoint.validate()?;
        if checkpoint.cluster_id != self.cluster_id {
            bail!("MVCC checkpoint belongs to another cluster");
        }
        let checkpoint_id = checkpoint.identity()?;
        let _decision_transition = self.decision_transition.lock().unwrap();
        let _materialisation_transition = self.materialisation_transition.lock().unwrap();
        let _outbox_transition = self.outbox_transition.lock().unwrap();
        let meta_cf = self.cf(CF_META)?;
        let marker_key = self.key(INSTALLED_CHECKPOINT_KEY);
        let current_decision_watermark = self.decision_watermark()?;
        let current_applied_version = self.applied_version()?;
        let current_gc_watermark = self.gc_watermark()?;

        if let Some(bytes) = self.db.get_cf(meta_cf, &marker_key)? {
            let marker: InstalledMvccCheckpoint = serde_json::from_slice(&bytes)
                .context("decode installed MVCC checkpoint marker")?;
            if marker.format_version != MVCC_CHECKPOINT_FORMAT_VERSION
                || marker.cluster_id != self.cluster_id
            {
                bail!("installed MVCC checkpoint marker is invalid");
            }
            if marker.checkpoint_id == checkpoint_id
                && marker.decision_watermark == checkpoint.decision_watermark
            {
                if current_decision_watermark < checkpoint.decision_watermark {
                    bail!("installed MVCC checkpoint watermark regressed");
                }
                return Ok(MvccCheckpointInstallOutcome::Replayed);
            }
        }

        if checkpoint.decision_watermark < current_decision_watermark
            || checkpoint.applied_version < current_applied_version
            || checkpoint.gc_watermark < current_gc_watermark
        {
            bail!("MVCC checkpoint installation cannot move local watermarks backwards");
        }

        let mut batch = WriteBatch::default();
        for name in MVCC_COLUMN_FAMILIES {
            let cf = self.cf(name)?;
            for row in self
                .db
                .iterator_cf(cf, IteratorMode::From(&self.scope, Direction::Forward))
            {
                let (key, _) = row?;
                if !key.starts_with(&self.scope) {
                    break;
                }
                batch.delete_cf(cf, key);
            }
        }
        for column in &checkpoint.column_families {
            let cf = self.cf(&column.name)?;
            for entry in &column.entries {
                batch.put_cf(cf, self.key(&entry.key), &entry.value);
            }
        }
        batch.put_cf(
            meta_cf,
            marker_key,
            serde_json::to_vec(&InstalledMvccCheckpoint {
                format_version: MVCC_CHECKPOINT_FORMAT_VERSION,
                cluster_id: self.cluster_id.clone(),
                checkpoint_id,
                decision_watermark: checkpoint.decision_watermark,
            })?,
        );
        self.db.write_opt(batch, &durable_write_options())?;

        if self.decision_watermark()? != checkpoint.decision_watermark
            || self.applied_version()? != checkpoint.applied_version
            || self.gc_watermark()? != checkpoint.gc_watermark
        {
            bail!("installed MVCC checkpoint watermark verification failed");
        }
        Ok(MvccCheckpointInstallOutcome::Installed)
    }

    pub fn install_checkpoint_bytes(&self, bytes: &[u8]) -> Result<MvccCheckpointInstallOutcome> {
        self.install_checkpoint(&MvccCheckpoint::decode(bytes)?)
    }

    /// Atomically applies a certified bundle and advances the applied version.
    ///
    /// Application must follow certification order. Replaying the same bundle
    /// at the same version is a no-op; using that version for different content
    /// is rejected.
    pub fn apply_certified_bundle(
        &self,
        commit_version: CommitVersion,
        bundle: &TransactionBundle,
    ) -> Result<ApplyOutcome> {
        self.apply_certified_bundle_at_decision(commit_version, bundle, None)
    }

    pub fn apply_certified_bundle_and_advance(
        &self,
        commit_version: CommitVersion,
        bundle: &TransactionBundle,
        decision_position: CommitVersion,
    ) -> Result<ApplyOutcome> {
        self.apply_certified_bundle_at_decision(commit_version, bundle, Some(decision_position))
    }

    fn apply_certified_bundle_at_decision(
        &self,
        commit_version: CommitVersion,
        bundle: &TransactionBundle,
        decision_position: Option<CommitVersion>,
    ) -> Result<ApplyOutcome> {
        let _decision_transition = self.decision_transition.lock().unwrap();
        if bundle.cluster_id != self.cluster_id {
            bail!("transaction bundle belongs to another cluster");
        }
        let current_decision_watermark = decision_position
            .map(|position| self.validate_decision_position_unlocked(position))
            .transpose()?;
        let identity = bundle.identity()?.hash;
        let applied_key = self.key(&commit_version.to_be_bytes());
        let applied_cf = self.cf(CF_APPLIED)?;
        if let Some(existing) = self.db.get_cf(applied_cf, &applied_key)? {
            if existing.as_slice() == identity.as_bytes() {
                if let Some(position) = decision_position {
                    self.advance_decision_watermark_unlocked(position)?;
                }
                return Ok(ApplyOutcome::Replayed);
            }
            bail!("commit version {commit_version} was already applied with another bundle");
        }

        if let (Some(position), Some(current)) = (decision_position, current_decision_watermark) {
            if position <= current {
                bail!(
                    "MVCC decision position {position} was already processed without commit version {commit_version}"
                );
            }
            if commit_version != position {
                bail!(
                    "unseen commit version {commit_version} cannot be applied at decision position {position}"
                );
            }
        }
        let applied_version = self.applied_version()?;
        if commit_version <= applied_version {
            bail!(
                "cannot apply unseen version {commit_version} below applied version {applied_version}"
            );
        }

        let versions_cf = self.cf(CF_VERSIONS)?;
        let heads_cf = self.cf(CF_HEADS)?;
        let meta_cf = self.cf(CF_META)?;
        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        let outbox_cf = self.cf(CF_OUTBOX)?;
        let mut committed_writes = bundle.writes.clone();
        // Object projection facts are derived in canonical job/entry order at
        // apply time. Multiple mutations in one certified transaction may
        // intentionally advance the same projection row; retain the final
        // derived value without masking overlap with an explicitly certified
        // bundle write (the duplicate check below still rejects that).
        let mut committed_object_writes = BTreeMap::new();
        let mut journal_job_ordinal = 0;
        for encoded_job in &bundle.materialisation_jobs {
            let schema = serde_json::from_slice::<serde_json::Value>(encoded_job)?
                .get("schema")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if !schema
                .as_deref()
                .is_some_and(crate::object_journal_commit::ObjectJournalCommitJob::is_schema)
            {
                continue;
            }
            let job = crate::object_journal_commit::ObjectJournalCommitJob::decode(encoded_job)?;
            for mutation in job.committed_mutations(commit_version, journal_job_ordinal)? {
                let write = match mutation.value {
                    Some(value) => WriteOperation::Put {
                        key: mutation.key,
                        value,
                    },
                    None => WriteOperation::Delete { key: mutation.key },
                };
                committed_object_writes.insert(write.key().clone(), write);
            }
            journal_job_ordinal += 1;
        }
        committed_writes.extend(committed_object_writes.into_values());
        committed_writes.sort_by(|left, right| left.key().cmp(right.key()));
        for pair in committed_writes.windows(2) {
            if pair[0].key() == pair[1].key() {
                bail!("certified bundle derives duplicate committed object journal keys");
            }
        }
        let mut batch = WriteBatch::default();
        for write in &committed_writes {
            let key = write.key();
            let logical_key = self.key(&encode_logical_key(key)?);
            let versioned_key = self.key(&encode_versioned_key(key, commit_version)?);
            let row = match write {
                WriteOperation::Put { value, .. } => encode_value(value),
                WriteOperation::Delete { .. } => vec![TOMBSTONE],
            };
            batch.put_cf(versions_cf, versioned_key, row);
            batch.put_cf(heads_cf, logical_key, commit_version.to_be_bytes());
        }
        for encoded_job in &bundle.materialisation_jobs {
            let schema = serde_json::from_slice::<serde_json::Value>(encoded_job)?
                .get("schema")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            if schema.as_deref() == Some(ShardRepairJob::SCHEMA) {
                let job = ShardRepairJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!("shard repair job belongs to another transaction or cluster");
                }
                let key = self.key(format!("shard-repair/{}", job.job_id()?).as_bytes());
                let record = serde_json::to_vec(&ShardRepairRecord::pending(job))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("shard repair job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema.as_deref() == Some(LocalDurabilityUpgradeJob::SCHEMA) {
                let mut job: LocalDurabilityUpgradeJob = serde_json::from_slice(encoded_job)?;
                job.validate()?;
                if job.cluster_id != self.cluster_id
                    || job.transaction_id != bundle.transaction_id
                    || job.commit_version != 0
                    || job.bundle.is_some()
                {
                    bail!("local durability upgrade intent is not valid for this commit");
                }
                let job_id = job.job_id()?;
                job.commit_version = commit_version;
                job.bundle = Some(bundle.identity()?);
                let key = self.key(format!("local-upgrade/{job_id}").as_bytes());
                let record = serde_json::to_vec(&LocalDurabilityUpgradeRecord::pending(job)?)?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("local durability upgrade job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema.as_deref() == Some(IndexFinalizationJob::SCHEMA) {
                let job = IndexFinalizationJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!("index finalization job belongs to another transaction or cluster");
                }
                let key = self.key(format!("index-finalization/{}", job.job_id()?).as_bytes());
                let record =
                    serde_json::to_vec(&IndexFinalizationRecord::pending(job, commit_version))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("index finalization job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema.as_deref() == Some(PersonalDbPostCommitJob::SCHEMA) {
                let job = PersonalDbPostCommitJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!("PersonalDB postcommit job belongs to another transaction or cluster");
                }
                let key = self.key(format!("personaldb-postcommit/{}", job.job_id()?).as_bytes());
                let record =
                    serde_json::to_vec(&PersonalDbPostCommitRecord::pending(job, commit_version))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("PersonalDB postcommit job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema.as_deref() == Some(GitSourcePostCommitJob::SCHEMA) {
                let job = GitSourcePostCommitJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!("GitSource postcommit job belongs to another transaction or cluster");
                }
                let key = self.key(format!("git-source-postcommit/{}", job.job_id()?).as_bytes());
                let record =
                    serde_json::to_vec(&GitSourcePostCommitRecord::pending(job, commit_version))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("GitSource postcommit job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema.as_deref() == Some(HfIngestionPostCommitJob::SCHEMA) {
                let job = HfIngestionPostCommitJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!(
                        "Hugging Face ingestion postcommit job belongs to another transaction or cluster"
                    );
                }
                let key = self.key(format!("hf-ingestion-postcommit/{}", job.job_id()?).as_bytes());
                let record =
                    serde_json::to_vec(&HfIngestionPostCommitRecord::pending(job, commit_version))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("Hugging Face ingestion postcommit job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema
                .as_deref()
                .is_some_and(crate::object_journal_commit::ObjectJournalCommitJob::is_schema)
            {
                // Object journal facts were installed into this same RocksDB
                // batch above using the Raft-assigned commit version.
                continue;
            }
            if schema.as_deref() == Some(ObjectLinkFinalizationJob::SCHEMA) {
                let job = ObjectLinkFinalizationJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!("object-link finalization job belongs to another transaction or cluster");
                }
                let key =
                    self.key(format!("object-link-finalization/{}", job.job_id()?).as_bytes());
                let record = serde_json::to_vec(&ObjectLinkFinalizationRecord::pending(
                    job,
                    commit_version,
                ))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("object-link finalization job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            if schema.as_deref() == Some(BucketLocatorFinalizationJob::SCHEMA) {
                let job = BucketLocatorFinalizationJob::decode(encoded_job)?;
                if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id
                {
                    bail!(
                        "bucket locator finalization job belongs to another transaction or cluster"
                    );
                }
                let key =
                    self.key(format!("bucket-locator-finalization/{}", job.job_id()?).as_bytes());
                let record = serde_json::to_vec(&BucketLocatorFinalizationRecord::pending(
                    job,
                    commit_version,
                ))?;
                if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                    && existing.as_slice() != record.as_slice()
                {
                    bail!("bucket locator finalization job identity collision");
                }
                batch.put_cf(materialisation_cf, key, record);
                continue;
            }
            let job = ObjectMaterialisationJob::decode(encoded_job)?;
            if job.cluster_id != self.cluster_id || job.transaction_id != bundle.transaction_id {
                bail!("materialisation job belongs to another transaction or cluster");
            }
            let key = self.key(format!("object-job/{}", job.job_id()?).as_bytes());
            let record = serde_json::to_vec(&ObjectMaterialisationRecord::pending(job))?;
            if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                && existing.as_slice() != record.as_slice()
            {
                bail!("materialisation job identity collision");
            }
            batch.put_cf(materialisation_cf, key, record);
        }
        for (ordinal, payload) in bundle.outbox_events.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).context("too many outbox events in bundle")?;
            let record = OutboxRecord {
                event_id: outbox_event_id(&bundle.transaction_id, ordinal, payload),
                transaction_id: bundle.transaction_id.clone(),
                commit_version,
                ordinal,
                payload: payload.clone(),
                state: OutboxState::Pending,
                attempts: 0,
                created_unix_ms: current_unix_ms(),
                next_attempt_unix_ms: 0,
                last_error: None,
                lease_owner: None,
                lease_expires_unix_ms: None,
            };
            batch.put_cf(
                outbox_cf,
                self.key(&outbox_event_key(commit_version, ordinal)),
                serde_json::to_vec(&record)?,
            );
        }
        for result in &bundle.idempotency_results {
            let key =
                self.idempotency_result_key(&bundle.transaction_id, &result.namespace, &result.key);
            let record = CommittedIdempotencyResult {
                transaction_id: bundle.transaction_id.clone(),
                commit_version,
                result: result.clone(),
            };
            let encoded = serde_json::to_vec(&record)?;
            if let Some(existing) = self.db.get_cf(materialisation_cf, &key)?
                && existing.as_slice() != encoded.as_slice()
            {
                bail!("committed idempotency result identity collision");
            }
            batch.put_cf(materialisation_cf, key, encoded);
        }
        batch.put_cf(applied_cf, applied_key, identity.as_bytes());
        batch.put_cf(
            meta_cf,
            self.key(APPLIED_VERSION_KEY),
            commit_version.to_be_bytes(),
        );
        if let Some(position) = decision_position {
            batch.put_cf(
                meta_cf,
                self.key(DECISION_WATERMARK_KEY),
                position.to_be_bytes(),
            );
        }
        #[cfg(any(test, debug_assertions))]
        crate::mvcc_fault_injection::hit_for_transaction(
            crate::mvcc_fault_injection::FaultPoint::MvccBatchWrite,
            &bundle.transaction_id,
        )?;
        self.db.write_opt(batch, &durable_write_options())?;
        Ok(ApplyOutcome::Applied)
    }

    pub fn committed_idempotency_result(
        &self,
        transaction_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<Option<CommittedIdempotencyResult>> {
        if transaction_id.trim().is_empty() || namespace.trim().is_empty() || key.trim().is_empty()
        {
            bail!("committed idempotency result identity must be non-empty");
        }
        let bytes = self.db.get_cf(
            self.cf(CF_MATERIALISATION)?,
            self.idempotency_result_key(transaction_id, namespace, key),
        )?;
        let record = bytes
            .map(|bytes| serde_json::from_slice::<CommittedIdempotencyResult>(&bytes))
            .transpose()?;
        if let Some(record) = &record
            && (record.transaction_id != transaction_id
                || record.result.namespace != namespace
                || record.result.key != key)
        {
            bail!("committed idempotency result key does not match its record");
        }
        Ok(record)
    }

    pub fn outbox_records_after(
        &self,
        commit_version: CommitVersion,
        limit: usize,
    ) -> Result<Vec<OutboxRecord>> {
        if limit == 0 {
            bail!("outbox page limit must be non-zero");
        }
        let cf = self.cf(CF_OUTBOX)?;
        let seek = self.key(&outbox_event_key(commit_version.saturating_add(1), 0));
        let prefix = self.key(b"event/");
        let mut records = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            records.push(serde_json::from_slice(&value)?);
            if records.len() == limit {
                break;
            }
        }
        Ok(records)
    }

    pub fn claim_outbox(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<OutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("outbox worker and lease must be non-empty");
        }
        self.claim_outbox_where(worker_id, now_unix_ms, lease_ms, |_| true)
    }

    pub fn claim_outbox_where(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&OutboxRecord) -> bool,
    ) -> Result<Option<OutboxRecord>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("outbox worker and lease must be non-empty");
        }
        let _transition = self.outbox_transition.lock().unwrap();
        let cf = self.cf(CF_OUTBOX)?;
        let prefix = self.key(b"event/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: OutboxRecord = serde_json::from_slice(&value)?;
            let claimable = (record.state == OutboxState::Pending
                && record.next_attempt_unix_ms <= now_unix_ms)
                || (record.state == OutboxState::Running
                    && record
                        .lease_expires_unix_ms
                        .is_some_and(|deadline| deadline <= now_unix_ms));
            if !claimable {
                continue;
            }
            if !eligible(&record) {
                continue;
            }
            record.state = OutboxState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("outbox lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            return Ok(Some(record));
        }
        Ok(None)
    }

    pub fn retry_outbox(
        &self,
        record: &OutboxRecord,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        let _transition = self.outbox_transition.lock().unwrap();
        let cf = self.cf(CF_OUTBOX)?;
        let key = self.key(&outbox_event_key(record.commit_version, record.ordinal));
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("outbox event not found")?;
        let mut current: OutboxRecord = serde_json::from_slice(&bytes)?;
        if current.event_id != record.event_id
            || current.state != OutboxState::Running
            || current.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("outbox event is not leased by this worker");
        }
        current.state = OutboxState::Pending;
        current.next_attempt_unix_ms = next_attempt_unix_ms;
        current.last_error = Some(error.to_string());
        current.lease_owner = None;
        current.lease_expires_unix_ms = None;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&current)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    pub fn rebind_outbox_lease(
        &self,
        record: &OutboxRecord,
        current_owner: &str,
        assignment_owner: &str,
    ) -> Result<OutboxRecord> {
        let _transition = self.outbox_transition.lock().unwrap();
        let cf = self.cf(CF_OUTBOX)?;
        let key = self.key(&outbox_event_key(record.commit_version, record.ordinal));
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("outbox event not found")?;
        let mut current: OutboxRecord = serde_json::from_slice(&bytes)?;
        if current.event_id != record.event_id
            || current.state != OutboxState::Running
            || current.lease_owner.as_deref() != Some(current_owner)
        {
            bail!("outbox lease changed before assignment binding");
        }
        current.lease_owner = Some(assignment_owner.to_string());
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&current)?,
            &durable_write_options(),
        )?;
        Ok(current)
    }

    pub fn outbox_backlog(&self, now_unix_ms: u64) -> Result<(u64, u64, u64)> {
        let records = self.outbox_records_after(0, usize::MAX)?;
        let mut count = 0u64;
        let mut oldest_age_ms = 0u64;
        let mut failures = 0u64;
        for record in records {
            if record.state == OutboxState::Delivered {
                continue;
            }
            count = count.saturating_add(1);
            oldest_age_ms = oldest_age_ms.max(now_unix_ms.saturating_sub(record.created_unix_ms));
            failures = failures.saturating_add(u64::from(record.last_error.is_some()));
        }
        Ok((count, oldest_age_ms, failures))
    }

    pub fn complete_outbox(&self, record: &OutboxRecord, worker_id: &str) -> Result<()> {
        self.complete_outbox_at(record, worker_id, 0)
    }

    pub fn complete_outbox_at(
        &self,
        record: &OutboxRecord,
        worker_id: &str,
        now_unix_ms: u64,
    ) -> Result<()> {
        let _transition = self.outbox_transition.lock().unwrap();
        let cf = self.cf(CF_OUTBOX)?;
        let key = self.key(&outbox_event_key(record.commit_version, record.ordinal));
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("outbox event not found")?;
        let mut current: OutboxRecord = serde_json::from_slice(&bytes)?;
        if current.event_id != record.event_id {
            bail!("outbox event identity mismatch");
        }
        if current.state == OutboxState::Delivered {
            return Ok(());
        }
        if current.state != OutboxState::Running
            || current.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("outbox event is not leased by this worker");
        }
        if now_unix_ms != 0
            && current
                .lease_expires_unix_ms
                .is_none_or(|expires| expires <= now_unix_ms)
        {
            bail!("outbox lease expired before durable downstream ACK");
        }
        current.state = OutboxState::Delivered;
        current.last_error = None;
        current.lease_owner = None;
        current.lease_expires_unix_ms = None;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&current)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    pub fn read_at(
        &self,
        key: &LogicalKey,
        snapshot_version: CommitVersion,
    ) -> Result<Option<VisibleRow>> {
        Ok(self.read_point_at(key, snapshot_version)?.into_visible())
    }

    /// Reads the complete point state at `snapshot_version`, retaining a
    /// tombstone's commit version for MVCC conflict observations.
    pub fn read_point_at(
        &self,
        key: &LogicalKey,
        snapshot_version: CommitVersion,
    ) -> Result<PointSnapshot> {
        let gc_watermark = self.gc_watermark()?;
        if snapshot_version < gc_watermark {
            bail!("snapshot {snapshot_version} is below local GC watermark {gc_watermark}");
        }
        if snapshot_version > self.readable_version()? {
            bail!(
                "snapshot {snapshot_version} is above local readable version {}",
                self.readable_version()?
            );
        }
        let prefix = self.key(&encode_logical_key(key)?);
        let seek = self.key(&encode_versioned_key(key, snapshot_version)?);
        let versions_cf = self.cf(CF_VERSIONS)?;
        let mut rows = self
            .db
            .iterator_cf(versions_cf, IteratorMode::From(&seek, Direction::Forward));
        let Some(row) = rows.next() else {
            return Ok(PointSnapshot::Unwritten);
        };
        let (encoded_key, encoded_value) = row?;
        if !encoded_key.starts_with(&prefix) {
            return Ok(PointSnapshot::Unwritten);
        }
        let version = decode_versioned_key(self.unscoped(&encoded_key)?)?.1;
        decode_point_snapshot(version, &encoded_value)
    }

    pub fn read_latest(&self, key: &LogicalKey) -> Result<Option<VisibleRow>> {
        let heads_cf = self.cf(CF_HEADS)?;
        let Some(head) = self
            .db
            .get_cf(heads_cf, self.key(&encode_logical_key(key)?))?
        else {
            return Ok(None);
        };
        let head_version = decode_u64(&head, "MVCC head")?;
        let readable_version = self.readable_version()?;
        if head_version > readable_version {
            bail!(
                "MVCC head version {head_version} is above local readable version {readable_version}"
            );
        }
        self.read_at(key, readable_version)
    }

    pub fn scan_table_prefix_at(
        &self,
        table_id: u16,
        application_prefix: &[u8],
        snapshot_version: CommitVersion,
    ) -> Result<Vec<(LogicalKey, VisibleRow)>> {
        self.scan_table_prefix_at_bounded(
            table_id,
            application_prefix,
            snapshot_version,
            usize::MAX,
        )
    }

    /// Scans at most `max_rows` visible rows from one table/application prefix.
    ///
    /// The bound is applied while iterating RocksDB heads, before callers
    /// decode or retain application payloads. This is the primitive for admin
    /// and control-plane list operations whose result sizes must be capped.
    pub fn scan_table_prefix_at_bounded(
        &self,
        table_id: u16,
        application_prefix: &[u8],
        snapshot_version: CommitVersion,
        max_rows: usize,
    ) -> Result<Vec<(LogicalKey, VisibleRow)>> {
        let gc_watermark = self.gc_watermark()?;
        if snapshot_version < gc_watermark {
            bail!("snapshot {snapshot_version} is below local GC watermark {gc_watermark}");
        }
        let readable_version = self.readable_version()?;
        if snapshot_version > readable_version {
            bail!("snapshot {snapshot_version} is above local readable version {readable_version}");
        }
        if max_rows == 0 {
            return Ok(Vec::new());
        }

        let heads_cf = self.cf(CF_HEADS)?;
        let mut visible = Vec::new();
        for row in self.db.iterator_cf(
            heads_cf,
            IteratorMode::From(&self.scope, Direction::Forward),
        ) {
            let (encoded_key, _) = row?;
            if !encoded_key.starts_with(&self.scope) {
                break;
            }
            let key = decode_logical_key(self.unscoped(&encoded_key)?)?;
            if key.table_id != table_id || !key.application_key.starts_with(application_prefix) {
                continue;
            }
            if let Some(row) = self.read_at(&key, snapshot_version)? {
                visible.push((key, row));
                if visible.len() == max_rows {
                    break;
                }
            }
        }
        Ok(visible)
    }

    pub fn applied_version(&self) -> Result<CommitVersion> {
        self.read_meta_version(APPLIED_VERSION_KEY)
    }

    pub fn readable_version(&self) -> Result<CommitVersion> {
        Ok(self.applied_version()?.max(self.decision_watermark()?))
    }

    pub fn gc_watermark(&self) -> Result<CommitVersion> {
        self.read_meta_version(GC_WATERMARK_KEY)
    }

    pub fn decision_watermark(&self) -> Result<CommitVersion> {
        self.read_meta_version(DECISION_WATERMARK_KEY)
    }

    pub fn advance_decision_watermark(&self, position: CommitVersion) -> Result<()> {
        let _decision_transition = self.decision_transition.lock().unwrap();
        self.advance_decision_watermark_unlocked(position)
    }

    fn advance_decision_watermark_unlocked(&self, position: CommitVersion) -> Result<()> {
        let current = self.validate_decision_position_unlocked(position)?;
        if position <= current {
            return Ok(());
        }
        self.db.put_cf_opt(
            self.cf(CF_META)?,
            self.key(DECISION_WATERMARK_KEY),
            position.to_be_bytes(),
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn validate_decision_position_unlocked(
        &self,
        position: CommitVersion,
    ) -> Result<CommitVersion> {
        let current = self.decision_watermark()?;
        let expected = current.saturating_add(1);
        if position > expected {
            bail!(
                "MVCC decision gap: local watermark is {current}, expected decision {expected}, found {position}"
            );
        }
        Ok(current)
    }
}
