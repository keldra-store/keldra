impl MvccStore {
    /// Removes obsolete history below a consensus-approved watermark.
    ///
    /// The newest version at or below the watermark is retained as the
    /// visibility anchor, including tombstones. All newer versions remain.
    /// Delivered outbox events and completed jobs are removed only when their
    /// commit/snapshot coordinate is strictly below the watermark. Pending or
    /// leased work is a hard pin and causes collection to fail.
    pub fn garbage_collect(&self, safe_watermark: CommitVersion) -> Result<usize> {
        let started_at = std::time::Instant::now();
        let _materialisation_transition = self.materialisation_transition.lock().unwrap();
        let _outbox_transition = self.outbox_transition.lock().unwrap();
        let current = self.gc_watermark()?;
        if safe_watermark < current {
            bail!("GC watermark cannot move backwards");
        }
        if safe_watermark > self.readable_version()? {
            bail!("GC watermark cannot exceed the readable version");
        }
        if let Some(oldest_pin) = self.unfinished_work_pins()?.all().into_iter().next()
            && oldest_pin < safe_watermark
        {
            bail!("GC watermark {safe_watermark} exceeds unfinished work pin {oldest_pin}");
        }

        let versions_cf = self.cf(CF_VERSIONS)?;
        let applied_cf = self.cf(CF_APPLIED)?;
        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        let outbox_cf = self.cf(CF_OUTBOX)?;
        let meta_cf = self.cf(CF_META)?;
        let mut batch = WriteBatch::default();
        let mut deleted = 0;
        let mut deleted_bytes = 0_u64;
        let mut current_key: Option<Vec<u8>> = None;
        let mut retained_anchor = false;

        for row in self.db.iterator_cf(
            versions_cf,
            IteratorMode::From(&self.scope, Direction::Forward),
        ) {
            let (encoded_key, value) = row?;
            if !encoded_key.starts_with(&self.scope) {
                break;
            }
            let (logical_key, version) = decode_versioned_key(self.unscoped(&encoded_key)?)?;
            if current_key.as_deref() != Some(logical_key.as_slice()) {
                current_key = Some(logical_key);
                retained_anchor = false;
            }
            if version <= safe_watermark && !retained_anchor {
                retained_anchor = true;
            } else if version < safe_watermark && retained_anchor {
                batch.delete_cf(versions_cf, &encoded_key);
                deleted += 1;
                deleted_bytes = deleted_bytes
                    .saturating_add((encoded_key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        for row in self.db.iterator_cf(
            applied_cf,
            IteratorMode::From(&self.scope, Direction::Forward),
        ) {
            let (encoded_key, value) = row?;
            if !encoded_key.starts_with(&self.scope) {
                break;
            }
            let version = decode_u64(self.unscoped(&encoded_key)?, "applied bundle version")?;
            if version < safe_watermark {
                batch.delete_cf(applied_cf, &encoded_key);
                deleted += 1;
                deleted_bytes = deleted_bytes
                    .saturating_add((encoded_key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        let outbox_prefix = self.key(b"event/");
        for row in self.db.iterator_cf(
            outbox_cf,
            IteratorMode::From(&outbox_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&outbox_prefix) {
                break;
            }
            let record: OutboxRecord = serde_json::from_slice(&value)?;
            if record.state == OutboxState::Delivered && record.commit_version < safe_watermark {
                batch.delete_cf(outbox_cf, &key);
                deleted += 1;
                deleted_bytes = deleted_bytes
                    .saturating_add((key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        self.collect_completed_jobs(
            materialisation_cf,
            b"object-job/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"shard-repair/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"local-upgrade/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"index-finalization/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"personaldb-postcommit/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"git-source-postcommit/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"hf-ingestion-postcommit/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"bucket-locator-finalization/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"object-link-finalization/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        self.collect_completed_jobs(
            materialisation_cf,
            b"idempotency-result/",
            safe_watermark,
            &mut batch,
            &mut deleted,
            &mut deleted_bytes,
        )?;
        batch.put_cf(
            meta_cf,
            self.key(GC_WATERMARK_KEY),
            safe_watermark.to_be_bytes(),
        );
        self.db.write_opt(batch, &durable_write_options())?;
        crate::perf::record_mvcc_gc(safe_watermark, deleted_bytes, started_at.elapsed());
        tracing::info!(
            operation = "gc.mvcc",
            watermark = safe_watermark,
            deleted_records = deleted,
            reclaimed_bytes = deleted_bytes,
            "completed MVCC garbage collection"
        );
        Ok(deleted)
    }

    fn collect_completed_jobs(
        &self,
        cf: &ColumnFamily,
        suffix: &[u8],
        safe_watermark: CommitVersion,
        batch: &mut WriteBatch,
        deleted: &mut usize,
        deleted_bytes: &mut u64,
    ) -> Result<()> {
        let prefix = self.key(suffix);
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let completed_below_watermark = if suffix == b"object-job/" {
                let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
                record.state == ObjectMaterialisationState::Complete
                    && record.job.originating_snapshot_version < safe_watermark
            } else if suffix == b"shard-repair/" {
                let record: ShardRepairRecord = serde_json::from_slice(&value)?;
                (record.state == ShardRepairState::Complete
                    || self.shard_repair_published_at(&record, safe_watermark)?)
                    && record.job.originating_snapshot_version < safe_watermark
            } else if suffix == b"local-upgrade/" {
                let record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&value)?;
                record.state == LocalDurabilityUpgradeState::Complete
                    && record.job.commit_version < safe_watermark
            } else if suffix == b"index-finalization/" {
                let record: IndexFinalizationRecord = serde_json::from_slice(&value)?;
                record.state == IndexFinalizationState::Complete
                    && record.commit_version < safe_watermark
            } else if suffix == b"personaldb-postcommit/" {
                let record: PersonalDbPostCommitRecord = serde_json::from_slice(&value)?;
                record.state == PersonalDbPostCommitState::Complete
                    && record.commit_version < safe_watermark
            } else if suffix == b"git-source-postcommit/" {
                let record: GitSourcePostCommitRecord = serde_json::from_slice(&value)?;
                record.state == GitSourcePostCommitState::Complete
                    && record.commit_version < safe_watermark
            } else if suffix == b"hf-ingestion-postcommit/" {
                let record: HfIngestionPostCommitRecord = serde_json::from_slice(&value)?;
                record.state == HfIngestionPostCommitState::Complete
                    && record.commit_version < safe_watermark
            } else if suffix == b"bucket-locator-finalization/" {
                let record: BucketLocatorFinalizationRecord = serde_json::from_slice(&value)?;
                record.state == BucketLocatorFinalizationState::Complete
                    && record.commit_version < safe_watermark
            } else if suffix == b"object-link-finalization/" {
                let record: ObjectLinkFinalizationRecord = serde_json::from_slice(&value)?;
                record.state == ObjectLinkFinalizationState::Complete
                    && record.commit_version < safe_watermark
            } else if suffix == b"idempotency-result/" {
                let record: CommittedIdempotencyResult = serde_json::from_slice(&value)?;
                record.commit_version < safe_watermark
            } else {
                bail!("unknown completed materialisation job family");
            };
            if completed_below_watermark {
                batch.delete_cf(cf, &key);
                *deleted += 1;
                *deleted_bytes = deleted_bytes
                    .saturating_add((key.len() as u64).saturating_add(value.len() as u64));
            }
        }
        Ok(())
    }

    fn read_meta_version(&self, key: &[u8]) -> Result<CommitVersion> {
        self.db
            .get_cf(self.cf(CF_META)?, self.key(key))?
            .map(|bytes| decode_u64(&bytes, "MVCC metadata version"))
            .transpose()
            .map(|value| value.unwrap_or(0))
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing MVCC RocksDB column family {name}"))
    }

    fn key(&self, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.scope.len() + suffix.len());
        key.extend_from_slice(&self.scope);
        key.extend_from_slice(suffix);
        key
    }

    fn idempotency_result_key(&self, transaction_id: &str, namespace: &str, key: &str) -> Vec<u8> {
        let mut hash = Sha256::new();
        hash.update(b"anvil.mvcc.idempotency-result.v1");
        for component in [transaction_id, namespace, key] {
            hash.update((component.len() as u64).to_be_bytes());
            hash.update(component.as_bytes());
        }
        self.key(format!("idempotency-result/{:x}", hash.finalize()).as_bytes())
    }

    fn unscoped<'a>(&self, key: &'a [u8]) -> Result<&'a [u8]> {
        key.strip_prefix(self.scope.as_slice())
            .ok_or_else(|| anyhow!("MVCC key belongs to another cluster"))
    }
}
