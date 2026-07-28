impl MvccStore {
    /// Durably records that a committed `local` transaction lost its sole
    /// holder. Re-observing the same consensus-derived violation is
    /// idempotent; conflicting evidence for one commit version fails closed.
    pub fn record_local_durability_violation(
        &self,
        record: &LocalDurabilityViolationRecord,
    ) -> Result<bool> {
        let cf = self.cf(CF_META)?;
        let mut suffix = LOCAL_DURABILITY_VIOLATION_PREFIX.to_vec();
        suffix.extend_from_slice(&record.commit_version.to_be_bytes());
        let key = self.key(&suffix);
        let bytes = serde_json::to_vec(record)?;
        if let Some(existing) = self.db.get_cf(cf, &key)? {
            if existing.as_slice() != bytes {
                bail!("local durability violation identity collision");
            }
            return Ok(false);
        }
        self.db
            .put_cf_opt(cf, key, bytes, &durable_write_options())?;
        Ok(true)
    }

    pub fn local_durability_violations(&self) -> Result<Vec<LocalDurabilityViolationRecord>> {
        let cf = self.cf(CF_META)?;
        let prefix = self.key(LOCAL_DURABILITY_VIOLATION_PREFIX);
        let mut records = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            records.push(serde_json::from_slice(&value)?);
        }
        Ok(records)
    }

    pub fn claim_object_materialisation(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<(String, ObjectMaterialisationRecord)>> {
        self.claim_object_materialisation_where(worker_id, now_unix_ms, lease_ms, |_| true)
    }

    pub fn claim_object_materialisation_where(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&ObjectMaterialisationRecord) -> bool,
    ) -> Result<Option<(String, ObjectMaterialisationRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("materialisation worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"object-job/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
            if !record.claimable(now_unix_ms) {
                continue;
            }
            if !eligible(&record) {
                continue;
            }
            record.state = ObjectMaterialisationState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("materialisation lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                .strip_prefix("object-job/")
                .context("invalid materialisation job key")?
                .to_string();
            return Ok(Some((id, record)));
        }
        Ok(None)
    }

    pub fn claim_object_materialisation_authorized(
        &self,
        worker_prefix: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        authority: impl Fn(&ObjectMaterialisationRecord) -> Option<String>,
    ) -> Result<Option<(String, ObjectMaterialisationRecord)>> {
        self.claim_object_materialisation_where(worker_prefix, now_unix_ms, lease_ms, |record| {
            authority(record).is_some()
        })
        .and_then(|claimed| {
            let Some((job_id, mut record)) = claimed else {
                return Ok(None);
            };
            let owner =
                authority(&record).context("materialisation assignment changed at claim")?;
            // Rebind the just-acquired local lease to the exact assignment
            // generation. The transition lock in the nested claim has been
            // released, so use the normal fenced transition.
            self.transition_object_materialisation(&job_id, worker_prefix, |current| {
                current.lease_owner = Some(owner.clone());
                Ok(())
            })?;
            record.lease_owner = Some(owner);
            Ok(Some((job_id, record)))
        })
    }

    pub fn retry_object_materialisation(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_object_materialisation(job_id, worker_id, |record| {
            record.state = ObjectMaterialisationState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_object_materialisation(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_object_materialisation(job_id, worker_id, |record| {
            record.state = ObjectMaterialisationState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_local_durability_upgrade(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<(String, LocalDurabilityUpgradeRecord)>> {
        self.claim_local_durability_upgrade_where(worker_id, now_unix_ms, lease_ms, |_| true)
    }

    pub fn claim_local_durability_upgrade_where(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&LocalDurabilityUpgradeRecord) -> bool,
    ) -> Result<Option<(String, LocalDurabilityUpgradeRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("durability-upgrade worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
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
            let mut record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&value)?;
            if !record.claimable(now_unix_ms) || !eligible(&record) {
                continue;
            }
            record.state = LocalDurabilityUpgradeState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("durability-upgrade lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                .strip_prefix("local-upgrade/")
                .context("invalid durability-upgrade job key")?
                .to_string();
            return Ok(Some((id, record)));
        }
        Ok(None)
    }

    /// Returns the durable promotion record for a committed local object.
    ///
    /// Local object hashes are content identities, so the same bytes may be
    /// referenced by more than one object version. In that case every matching
    /// record describes the same physical promotion and the oldest stable job
    /// identity is returned.
    pub fn local_durability_upgrade_for_object(
        &self,
        object_hash: &str,
    ) -> Result<Option<(String, LocalDurabilityUpgradeRecord)>> {
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"local-upgrade/");
        let mut matches = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&value)?;
            if record
                .job
                .objects
                .iter()
                .any(|object| object.local_manifest.object_hash == object_hash)
            {
                let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                    .strip_prefix("local-upgrade/")
                    .context("invalid durability-upgrade job key")?
                    .to_string();
                matches.push((id, record));
            }
        }
        matches.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(matches.into_iter().next())
    }

    /// Idempotently makes an existing committed promotion immediately
    /// claimable. The immutable commit-created job remains the authority; a
    /// public request cannot weaken or rewrite its target.
    pub fn request_local_durability_upgrade_for_object(
        &self,
        object_hash: &str,
        target: crate::mvcc_transaction::DurabilityLevel,
    ) -> Result<Option<(String, LocalDurabilityUpgradeRecord)>> {
        let Some((job_id, _)) = self.local_durability_upgrade_for_object(object_hash)? else {
            return Ok(None);
        };
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("local-upgrade/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("local durability upgrade disappeared while requesting it")?;
        let mut record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&bytes)?;
        let rank = |durability| match durability {
            crate::mvcc_transaction::DurabilityLevel::Local => 0_u8,
            crate::mvcc_transaction::DurabilityLevel::Quorum => 1,
            crate::mvcc_transaction::DurabilityLevel::Erasure => 2,
        };
        if rank(record.job.target) < rank(target) {
            bail!("committed durability-upgrade intent does not satisfy requested target");
        }
        if record.state == LocalDurabilityUpgradeState::Pending && record.next_attempt_unix_ms != 0
        {
            record.next_attempt_unix_ms = 0;
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
        }
        Ok(Some((job_id, record)))
    }

    pub fn retry_local_durability_upgrade(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_local_durability_upgrade(job_id, worker_id, |record| {
            record.state = LocalDurabilityUpgradeState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.last_error = Some(error.to_string());
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            Ok(())
        })
    }

    pub fn rebind_local_durability_upgrade_lease(
        &self,
        job_id: &str,
        current_owner: &str,
        assignment_owner: &str,
    ) -> Result<()> {
        if assignment_owner.trim().is_empty() {
            bail!("assignment-fenced lease owner is required");
        }
        self.transition_local_durability_upgrade(job_id, current_owner, |record| {
            record.lease_owner = Some(assignment_owner.to_string());
            Ok(())
        })
    }

    pub fn complete_local_durability_upgrade(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_local_durability_upgrade(job_id, worker_id, |record| {
            record.state = LocalDurabilityUpgradeState::Complete;
            record.last_error = None;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            Ok(())
        })
    }

    pub fn claim_shard_repair(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<(String, ShardRepairRecord)>> {
        self.claim_shard_repair_where(worker_id, now_unix_ms, lease_ms, |_| true)
    }

    pub fn claim_index_finalization(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
    ) -> Result<Option<(String, IndexFinalizationRecord)>> {
        self.claim_index_finalization_where(worker_id, now_unix_ms, lease_ms, |_| true)
    }

    fn claim_index_finalization_where(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&IndexFinalizationRecord) -> bool,
    ) -> Result<Option<(String, IndexFinalizationRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("index finalization worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"index-finalization/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: IndexFinalizationRecord = serde_json::from_slice(&value)?;
            if !record.claimable(now_unix_ms) || !eligible(&record) {
                continue;
            }
            record.state = IndexFinalizationState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("index finalization lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                .strip_prefix("index-finalization/")
                .context("invalid index finalization key")?
                .to_string();
            return Ok(Some((id, record)));
        }
        Ok(None)
    }

    pub fn claim_index_finalization_authorized(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&IndexFinalizationRecord) -> Option<String>,
    ) -> Result<Option<(String, IndexFinalizationRecord)>> {
        self.claim_index_finalization_where(worker_id, now_unix_ms, lease_ms, |record| {
            eligible(record).is_some()
        })
        .and_then(|claimed| {
            let Some((job_id, mut record)) = claimed else {
                return Ok(None);
            };
            let owner = eligible(&record).context("index assignment changed at claim")?;
            self.transition_index_finalization(&job_id, worker_id, |current| {
                current.lease_owner = Some(owner.clone());
                Ok(())
            })?;
            record.lease_owner = Some(owner);
            Ok(Some((job_id, record)))
        })
    }

    pub fn retry_index_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_index_finalization(job_id, worker_id, |record| {
            record.state = IndexFinalizationState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_index_finalization(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_index_finalization(job_id, worker_id, |record| {
            record.state = IndexFinalizationState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_personaldb_postcommit_authorized(
        &self,
        worker_id: &str,
        now: u64,
        lease_ms: u64,
        eligible: impl Fn(&PersonalDbPostCommitRecord) -> Option<String>,
    ) -> Result<Option<(String, PersonalDbPostCommitRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("PersonalDB postcommit worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"personaldb-postcommit/");
        let mut incomplete = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: PersonalDbPostCommitRecord = serde_json::from_slice(&value)?;
            if record.state == PersonalDbPostCommitState::Complete {
                continue;
            }
            incomplete.push((key, record));
        }
        let candidate = incomplete
            .iter()
            .filter_map(|(key, record)| eligible(record).map(|owner| (key, record, owner)))
            .filter(|(_, candidate, _)| {
                !incomplete.iter().any(|(_, other)| {
                    other.job.tenant_id == candidate.job.tenant_id
                        && other.job.database_id == candidate.job.database_id
                        && other.job.log_index < candidate.job.log_index
                })
            })
            .min_by_key(|(_, record, _)| {
                (
                    record.job.tenant_id,
                    record.job.database_id.as_str(),
                    record.job.log_index,
                )
            });
        let Some((key, record, owner)) = candidate else {
            return Ok(None);
        };
        let key = key.clone();
        let mut record = record.clone();
        if !record.claimable(now) {
            // Never overtake an earlier running/backed-off source commit.
            return Ok(None);
        }
        record.state = PersonalDbPostCommitState::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.lease_owner = Some(owner);
        record.lease_expires_unix_ms = Some(
            now.checked_add(lease_ms)
                .context("PersonalDB job lease overflow")?,
        );
        self.db.put_cf_opt(
            cf,
            &key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
            .strip_prefix("personaldb-postcommit/")
            .context("invalid PersonalDB postcommit key")?
            .to_string();
        Ok(Some((id, record)))
    }

    pub fn retry_personaldb_postcommit(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_personaldb_postcommit(job_id, worker_id, |record| {
            record.state = PersonalDbPostCommitState::Pending;
            record.next_attempt_unix_ms = next_attempt;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_personaldb_postcommit(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_personaldb_postcommit(job_id, worker_id, |record| {
            record.state = PersonalDbPostCommitState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_git_source_postcommit_authorized(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&GitSourcePostCommitRecord) -> Option<String>,
    ) -> Result<Option<(String, GitSourcePostCommitRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("GitSource postcommit worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"git-source-postcommit/");
        let mut incomplete = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: GitSourcePostCommitRecord = serde_json::from_slice(&value)?;
            if record.state == GitSourcePostCommitState::Complete {
                continue;
            }
            incomplete.push((key, record));
        }
        let candidate = incomplete
            .iter()
            .filter_map(|(key, record)| eligible(record).map(|owner| (key, record, owner)))
            .filter(|(_, candidate, _)| {
                !incomplete.iter().any(|(_, other)| {
                    other.job.tenant_id == candidate.job.tenant_id
                        && other.job.repository_id == candidate.job.repository_id
                        && other.job.generation < candidate.job.generation
                })
            })
            .min_by_key(|(_, record, _)| {
                (
                    record.job.tenant_id,
                    record.job.repository_id.as_str(),
                    record.job.generation,
                )
            });
        let Some((key, record, owner)) = candidate else {
            return Ok(None);
        };
        let key = key.clone();
        let mut record = record.clone();
        if !record.claimable(now_unix_ms) {
            // Never overtake an earlier running or backed-off repository generation.
            return Ok(None);
        }
        record.state = GitSourcePostCommitState::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.lease_owner = Some(owner);
        record.lease_expires_unix_ms = Some(
            now_unix_ms
                .checked_add(lease_ms)
                .context("GitSource job lease overflow")?,
        );
        self.db.put_cf_opt(
            cf,
            &key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
            .strip_prefix("git-source-postcommit/")
            .context("invalid GitSource postcommit key")?
            .to_string();
        Ok(Some((id, record)))
    }

    pub fn retry_git_source_postcommit(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_git_source_postcommit(job_id, worker_id, |record| {
            record.state = GitSourcePostCommitState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_git_source_postcommit(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_git_source_postcommit(job_id, worker_id, |record| {
            record.state = GitSourcePostCommitState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_hf_ingestion_postcommit_authorized(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&HfIngestionPostCommitRecord) -> Option<String>,
    ) -> Result<Option<(String, HfIngestionPostCommitRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("Hugging Face ingestion postcommit worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"hf-ingestion-postcommit/");
        let mut candidates = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: HfIngestionPostCommitRecord = serde_json::from_slice(&value)?;
            if record.state != HfIngestionPostCommitState::Complete {
                candidates.push((key, record));
            }
        }
        let candidate = candidates
            .iter()
            .filter_map(|(key, record)| eligible(record).map(|owner| (key, record, owner)))
            .filter(|(_, candidate, _)| {
                !candidates.iter().any(|(_, other)| {
                    other.job.tenant_id == candidate.job.tenant_id
                        && other.job.ingestion_id < candidate.job.ingestion_id
                })
            })
            .min_by_key(|(_, record, _)| (record.job.tenant_id, record.job.ingestion_id));
        let Some((key, record, owner)) = candidate else {
            return Ok(None);
        };
        let key = key.clone();
        let mut record = record.clone();
        if !record.claimable(now_unix_ms) {
            return Ok(None);
        }
        record.state = HfIngestionPostCommitState::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.lease_owner = Some(owner);
        record.lease_expires_unix_ms = Some(
            now_unix_ms
                .checked_add(lease_ms)
                .context("Hugging Face ingestion job lease overflow")?,
        );
        self.db.put_cf_opt(
            cf,
            &key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
            .strip_prefix("hf-ingestion-postcommit/")
            .context("invalid Hugging Face ingestion postcommit key")?
            .to_string();
        Ok(Some((id, record)))
    }

    pub fn find_hf_ingestion_postcommit_by_transaction(
        &self,
        transaction_id: &str,
    ) -> Result<Option<HfIngestionPostCommitRecord>> {
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"hf-ingestion-postcommit/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: HfIngestionPostCommitRecord = serde_json::from_slice(&value)?;
            if record.job.transaction_id == transaction_id {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    pub fn retry_hf_ingestion_postcommit(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_hf_ingestion_postcommit(job_id, worker_id, |record| {
            record.state = HfIngestionPostCommitState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_hf_ingestion_postcommit(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_hf_ingestion_postcommit(job_id, worker_id, |record| {
            record.state = HfIngestionPostCommitState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_object_link_finalization_authorized(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&ObjectLinkFinalizationRecord) -> Option<String>,
    ) -> Result<Option<(String, ObjectLinkFinalizationRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("object-link finalization worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"object-link-finalization/");
        let mut incomplete = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: ObjectLinkFinalizationRecord = serde_json::from_slice(&value)?;
            if record.state != ObjectLinkFinalizationState::Complete {
                incomplete.push((key, record));
            }
        }
        let candidate = incomplete
            .iter()
            .filter_map(|(key, record)| eligible(record).map(|owner| (key, record, owner)))
            .filter(|(_, candidate, _)| {
                !incomplete.iter().any(|(_, other)| {
                    other.job.tenant_id == candidate.job.tenant_id
                        && other.job.bucket_id == candidate.job.bucket_id
                        && other.job.link_key == candidate.job.link_key
                        && other.job.generation < candidate.job.generation
                })
            })
            .min_by_key(|(_, record, _)| {
                (
                    record.job.tenant_id,
                    record.job.bucket_id,
                    record.job.link_key.as_str(),
                    record.job.generation,
                )
            });
        let Some((key, record, owner)) = candidate else {
            return Ok(None);
        };
        let key = key.clone();
        let mut record = record.clone();
        if !record.claimable(now_unix_ms) {
            return Ok(None);
        }
        record.state = ObjectLinkFinalizationState::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.lease_owner = Some(owner);
        record.lease_expires_unix_ms = Some(
            now_unix_ms
                .checked_add(lease_ms)
                .context("object-link finalization lease overflow")?,
        );
        self.db.put_cf_opt(
            cf,
            &key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
            .strip_prefix("object-link-finalization/")
            .context("invalid object-link finalization key")?
            .to_string();
        Ok(Some((id, record)))
    }

    pub fn retry_object_link_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_object_link_finalization(job_id, worker_id, |record| {
            record.state = ObjectLinkFinalizationState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_object_link_finalization(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_object_link_finalization(job_id, worker_id, |record| {
            record.state = ObjectLinkFinalizationState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_bucket_locator_finalization_authorized(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&BucketLocatorFinalizationRecord) -> Option<String>,
    ) -> Result<Option<(String, BucketLocatorFinalizationRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("bucket locator finalization worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"bucket-locator-finalization/");
        let mut incomplete = Vec::new();
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: BucketLocatorFinalizationRecord = serde_json::from_slice(&value)?;
            if record.state != BucketLocatorFinalizationState::Complete {
                incomplete.push((key, record));
            }
        }
        let candidate = incomplete
            .iter()
            .filter_map(|(key, record)| eligible(record).map(|owner| (key, record, owner)))
            .filter(|(_, candidate, _)| {
                !incomplete.iter().any(|(_, other)| {
                    other.job.target_logical_identity() == candidate.job.target_logical_identity()
                        && (other.commit_version, other.job.operation_sequence)
                            < (candidate.commit_version, candidate.job.operation_sequence)
                })
            })
            .min_by_key(|(_, record, _)| (record.commit_version, record.job.operation_sequence));
        let Some((key, record, owner)) = candidate else {
            return Ok(None);
        };
        let key = key.clone();
        let mut record = record.clone();
        if !record.claimable(now_unix_ms) {
            return Ok(None);
        }
        record.state = BucketLocatorFinalizationState::Running;
        record.attempts = record.attempts.saturating_add(1);
        record.lease_owner = Some(owner);
        record.lease_expires_unix_ms = Some(
            now_unix_ms
                .checked_add(lease_ms)
                .context("bucket locator finalization lease overflow")?,
        );
        self.db.put_cf_opt(
            cf,
            &key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
            .strip_prefix("bucket-locator-finalization/")
            .context("invalid bucket locator finalization key")?
            .to_string();
        Ok(Some((id, record)))
    }

    pub fn retry_bucket_locator_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_bucket_locator_finalization(job_id, worker_id, |record| {
            record.state = BucketLocatorFinalizationState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_bucket_locator_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
    ) -> Result<()> {
        self.transition_bucket_locator_finalization(job_id, worker_id, |record| {
            record.state = BucketLocatorFinalizationState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn claim_shard_repair_where(
        &self,
        worker_id: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        eligible: impl Fn(&ShardRepairRecord) -> bool,
    ) -> Result<Option<(String, ShardRepairRecord)>> {
        if worker_id.trim().is_empty() || lease_ms == 0 {
            bail!("shard repair worker and lease must be non-empty");
        }
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"shard-repair/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let mut record: ShardRepairRecord = serde_json::from_slice(&value)?;
            if !record.claimable(now_unix_ms) {
                continue;
            }
            if !eligible(&record) {
                continue;
            }
            record.state = ShardRepairState::Running;
            record.attempts = record.attempts.saturating_add(1);
            record.lease_owner = Some(worker_id.to_string());
            record.lease_expires_unix_ms = Some(
                now_unix_ms
                    .checked_add(lease_ms)
                    .context("shard repair lease expiry overflow")?,
            );
            self.db.put_cf_opt(
                cf,
                &key,
                serde_json::to_vec(&record)?,
                &durable_write_options(),
            )?;
            let id = String::from_utf8(self.unscoped(&key)?.to_vec())?
                .strip_prefix("shard-repair/")
                .context("invalid shard repair key")?
                .to_string();
            return Ok(Some((id, record)));
        }
        Ok(None)
    }

    pub fn claim_shard_repair_authorized(
        &self,
        worker_prefix: &str,
        now_unix_ms: u64,
        lease_ms: u64,
        authority: impl Fn(&ShardRepairRecord) -> Option<String>,
    ) -> Result<Option<(String, ShardRepairRecord)>> {
        self.claim_shard_repair_where(worker_prefix, now_unix_ms, lease_ms, |record| {
            authority(record).is_some()
        })
        .and_then(|claimed| {
            let Some((job_id, mut record)) = claimed else {
                return Ok(None);
            };
            let owner = authority(&record).context("repair assignment changed at claim")?;
            self.transition_shard_repair(&job_id, worker_prefix, |current| {
                current.lease_owner = Some(owner.clone());
                Ok(())
            })?;
            record.lease_owner = Some(owner);
            Ok(Some((job_id, record)))
        })
    }

    pub fn retry_shard_repair(
        &self,
        job_id: &str,
        worker_id: &str,
        next_attempt_unix_ms: u64,
        error: &str,
    ) -> Result<()> {
        self.transition_shard_repair(job_id, worker_id, |record| {
            record.state = ShardRepairState::Pending;
            record.next_attempt_unix_ms = next_attempt_unix_ms;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = Some(error.to_string());
            Ok(())
        })
    }

    pub fn complete_shard_repair(&self, job_id: &str, worker_id: &str) -> Result<()> {
        self.transition_shard_repair(job_id, worker_id, |record| {
            record.state = ShardRepairState::Complete;
            record.lease_owner = None;
            record.lease_expires_unix_ms = None;
            record.last_error = None;
            Ok(())
        })
    }

    pub fn shard_repair_record(&self, job_id: &str) -> Result<Option<ShardRepairRecord>> {
        if job_id.trim().is_empty() {
            bail!("shard repair job ID is required");
        }
        self.db
            .get_cf(
                self.cf(CF_MATERIALISATION)?,
                self.key(format!("shard-repair/{job_id}").as_bytes()),
            )?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    pub fn has_incomplete_object_materialisations(&self) -> Result<bool> {
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"object-job/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
            if record.state != ObjectMaterialisationState::Complete {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn object_materialisation_record(
        &self,
        job_id: &str,
    ) -> Result<Option<ObjectMaterialisationRecord>> {
        if job_id.trim().is_empty() {
            bail!("materialisation job ID must be non-empty");
        }
        self.db
            .get_cf(
                self.cf(CF_MATERIALISATION)?,
                self.key(format!("object-job/{job_id}").as_bytes()),
            )?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    /// Reports durable unfinished work which must constrain the candidate GC
    /// watermark before `AdvanceGcWatermark` is proposed to consensus.
    pub fn unfinished_work_pins(&self) -> Result<UnfinishedWorkPins> {
        let mut pins = UnfinishedWorkPins::default();
        // An outbox row owns a complete, independently durable copy of the
        // event payload. It does not read transaction history while
        // delivering, and pending rows are never removed by `garbage_collect`.
        // Treating it as a version-history pin would also be incorrect in a
        // cluster: every replica applies the row, but only its Raft-assigned
        // worker marks a local copy Delivered, so non-owner replicas would pin
        // the cluster watermark forever.
        let materialisation_cf = self.cf(CF_MATERIALISATION)?;
        let object_prefix = self.key(b"object-job/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&object_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&object_prefix) {
                break;
            }
            let record: ObjectMaterialisationRecord = serde_json::from_slice(&value)?;
            if record.state != ObjectMaterialisationState::Complete {
                pins.materialisation_snapshots
                    .insert(record.job.originating_snapshot_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }

        let repair_prefix = self.key(b"shard-repair/");
        let repair_snapshot = self.readable_version()?;
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&repair_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&repair_prefix) {
                break;
            }
            let record: ShardRepairRecord = serde_json::from_slice(&value)?;
            if record.state != ShardRepairState::Complete {
                // The repair journal is local and only the Raft-assigned
                // worker transitions its copy to Complete.  A committed,
                // locally applied placement overlay is the cluster-wide
                // completion fact for every other replica: keeping their
                // Pending copies as GC pins would prevent the overlay itself
                // from ever becoming eligible for physical retirement.
                if self.shard_repair_published_at(&record, repair_snapshot)? {
                    continue;
                }
                pins.repair_snapshots
                    .insert(record.job.originating_snapshot_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        let upgrade_prefix = self.key(b"local-upgrade/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&upgrade_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&upgrade_prefix) {
                break;
            }
            let record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&value)?;
            if record.state != LocalDurabilityUpgradeState::Complete {
                pins.materialisation_snapshots
                    .insert(record.job.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        let index_prefix = self.key(b"index-finalization/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&index_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&index_prefix) {
                break;
            }
            let record: IndexFinalizationRecord = serde_json::from_slice(&value)?;
            if record.state != IndexFinalizationState::Complete {
                pins.materialisation_snapshots.insert(record.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        let personaldb_prefix = self.key(b"personaldb-postcommit/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&personaldb_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&personaldb_prefix) {
                break;
            }
            let record: PersonalDbPostCommitRecord = serde_json::from_slice(&value)?;
            if record.state != PersonalDbPostCommitState::Complete {
                pins.materialisation_snapshots.insert(record.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        let git_source_prefix = self.key(b"git-source-postcommit/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&git_source_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&git_source_prefix) {
                break;
            }
            let record: GitSourcePostCommitRecord = serde_json::from_slice(&value)?;
            if record.state != GitSourcePostCommitState::Complete {
                pins.materialisation_snapshots.insert(record.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        let hf_ingestion_prefix = self.key(b"hf-ingestion-postcommit/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&hf_ingestion_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&hf_ingestion_prefix) {
                break;
            }
            let record: HfIngestionPostCommitRecord = serde_json::from_slice(&value)?;
            if record.state != HfIngestionPostCommitState::Complete {
                pins.materialisation_snapshots.insert(record.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        let bucket_locator_prefix = self.key(b"bucket-locator-finalization/");
        let object_link_prefix = self.key(b"object-link-finalization/");
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&object_link_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&object_link_prefix) {
                break;
            }
            let record: ObjectLinkFinalizationRecord = serde_json::from_slice(&value)?;
            if record.state != ObjectLinkFinalizationState::Complete {
                pins.materialisation_snapshots.insert(record.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        for row in self.db.iterator_cf(
            materialisation_cf,
            IteratorMode::From(&bucket_locator_prefix, Direction::Forward),
        ) {
            let (key, value) = row?;
            if !key.starts_with(&bucket_locator_prefix) {
                break;
            }
            let record: BucketLocatorFinalizationRecord = serde_json::from_slice(&value)?;
            if record.state != BucketLocatorFinalizationState::Complete {
                pins.materialisation_snapshots.insert(record.commit_version);
                pins.transaction_ids.insert(record.job.transaction_id);
            }
        }
        Ok(pins)
    }

    fn shard_repair_published_at(
        &self,
        record: &ShardRepairRecord,
        snapshot_version: CommitVersion,
    ) -> Result<bool> {
        let key = LogicalKey {
            table_id: crate::mvcc_shard_repair::ShardPlacementOverlay::TABLE_ID,
            application_key: record.job.target_logical_identity.as_bytes().to_vec(),
        };
        let Some(row) = self.read_at(&key, snapshot_version)? else {
            return Ok(false);
        };
        let overlay: crate::mvcc_shard_repair::ShardPlacementOverlay =
            serde_json::from_slice(&row.value)?;
        self.validate_shard_placement_overlay(&key, &overlay)?;
        let source = &record.job.source_manifest;
        let replacement = &overlay.replacement_manifest;
        if overlay.cluster_id != record.job.cluster_id
            || overlay.target_logical_identity != record.job.target_logical_identity
            || overlay.source_manifest_hash != record.job.source_manifest_hash
            || replacement.cluster_id != source.cluster_id
            || replacement.object_identity != source.object_identity
            || replacement.object_hash != source.object_hash
            || replacement.object_length != source.object_length
            || replacement.encoding_generation != source.encoding_generation
            || replacement.data_shards != source.data_shards
            || replacement.parity_shards != source.parity_shards
            || replacement.shard_bytes != source.shard_bytes
            || replacement.stripe_count != source.stripe_count
        {
            return Ok(false);
        }
        let every_replacement_is_live = record.job.missing.iter().all(|missing| {
            overlay
                .replacement_manifest
                .placements
                .iter()
                .any(|placement| {
                    placement.stripe_ordinal == missing.stripe_ordinal
                        && placement.shard_ordinal == missing.shard_ordinal
                        && placement.node_id == missing.target.node.node_id
                        && placement.node_incarnation == missing.target.node.incarnation
                        && placement.failure_domain == missing.target.failure_domain
                })
        });
        let every_old_placement_is_retired = record.job.retiring.iter().all(|retiring| {
            overlay
                .retired_after_commit
                .iter()
                .any(|retired| retired == retiring)
                && !overlay
                    .replacement_manifest
                    .placements
                    .iter()
                    .any(|placement| {
                        placement.stripe_ordinal == retiring.stripe_ordinal
                            && placement.shard_ordinal == retiring.shard_ordinal
                            && placement.node_id == retiring.node_id
                            && placement.node_incarnation == retiring.node_incarnation
                            && placement.failure_domain == retiring.failure_domain
                    })
        });
        Ok(every_replacement_is_live && every_old_placement_is_retired)
    }

    fn validate_shard_placement_overlay(
        &self,
        key: &LogicalKey,
        overlay: &crate::mvcc_shard_repair::ShardPlacementOverlay,
    ) -> Result<()> {
        overlay.replacement_manifest.validate()?;
        if overlay.schema != crate::mvcc_shard_repair::ShardPlacementOverlay::SCHEMA
            || overlay.cluster_id != self.cluster_id
            || overlay.target_logical_identity.as_bytes() != key.application_key.as_slice()
            || overlay.replacement_manifest.cluster_id != overlay.cluster_id
            || overlay.source_manifest_hash.len() != 64
            || !overlay
                .source_manifest_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            bail!("invalid stored shard placement overlay");
        }
        Ok(())
    }

    /// Returns object-shard transfers whose retirement is authorised by a
    /// placement overlay already below the locally applied cluster GC
    /// watermark. Incomplete repair jobs pin their source and retiring
    /// placements, so a retry never loses bytes it may still need.
    pub fn retirable_object_shard_transfers(
        &self,
        local_node: &NodeIncarnation,
    ) -> Result<BTreeSet<uuid::Uuid>> {
        let watermark = self.gc_watermark()?;
        let mut authorised = BTreeSet::new();
        let mut replacement_live = BTreeSet::new();
        for (key, row) in self.scan_table_prefix_at(
            crate::mvcc_shard_repair::ShardPlacementOverlay::TABLE_ID,
            b"",
            watermark,
        )? {
            let overlay: crate::mvcc_shard_repair::ShardPlacementOverlay =
                serde_json::from_slice(&row.value)?;
            self.validate_shard_placement_overlay(&key, &overlay)?;
            replacement_live.extend(
                overlay
                    .replacement_manifest
                    .placements
                    .iter()
                    .filter(|placement| placement_is_local(placement, local_node))
                    .map(|placement| placement.transfer_id),
            );
            authorised.extend(
                overlay
                    .retired_after_commit
                    .into_iter()
                    .filter(|placement| placement_is_local(placement, local_node))
                    .map(|placement| placement.transfer_id),
            );
        }

        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"shard-repair/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: ShardRepairRecord = serde_json::from_slice(&value)?;
            if record.state == ShardRepairState::Complete {
                continue;
            }
            if self.shard_repair_published_at(&record, watermark)? {
                continue;
            }
            for placement in record
                .job
                .source_manifest
                .placements
                .iter()
                .chain(record.job.retiring.iter())
                .filter(|placement| placement_is_local(placement, local_node))
            {
                authorised.remove(&placement.transfer_id);
            }
        }
        // Catalog rows are immutable source manifests. A committed overlay is
        // the authoritative cut-over for its explicitly retired placements;
        // replacement placements remain live even if a malformed/duplicate
        // overlay were ever to mention the same transfer.
        authorised.retain(|transfer_id| !replacement_live.contains(transfer_id));
        Ok(authorised)
    }

    /// Every transfer still reachable from a live manifest or unfinished
    /// shard job. Orphan provisional retirement subtracts this set after its
    /// independent GC-watermark and grace proofs.
    pub fn protected_object_shard_transfers(
        &self,
        local_node: &NodeIncarnation,
    ) -> Result<BTreeSet<uuid::Uuid>> {
        let watermark = self.gc_watermark()?;
        let mut protected = BTreeSet::new();
        for (_, row) in self.scan_table_prefix_at(
            crate::mvcc_shard_repair::SHARD_MANIFEST_CATALOG_TABLE_ID,
            b"manifest/",
            watermark,
        )? {
            let manifest: crate::object_shard_manifest::PhysicalObjectShardManifest =
                serde_json::from_slice(&row.value)?;
            manifest.validate()?;
            protected.extend(
                manifest
                    .placements
                    .into_iter()
                    .filter(|placement| placement_is_local(placement, local_node))
                    .map(|placement| placement.transfer_id),
            );
        }
        for (key, row) in self.scan_table_prefix_at(
            crate::mvcc_shard_repair::ShardPlacementOverlay::TABLE_ID,
            b"",
            watermark,
        )? {
            let overlay: crate::mvcc_shard_repair::ShardPlacementOverlay =
                serde_json::from_slice(&row.value)?;
            self.validate_shard_placement_overlay(&key, &overlay)?;
            protected.extend(
                overlay
                    .replacement_manifest
                    .placements
                    .into_iter()
                    .filter(|placement| placement_is_local(placement, local_node))
                    .map(|placement| placement.transfer_id),
            );
        }
        let cf = self.cf(CF_MATERIALISATION)?;
        let prefix = self.key(b"shard-repair/");
        for row in self
            .db
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let record: ShardRepairRecord = serde_json::from_slice(&value)?;
            if record.state == ShardRepairState::Complete {
                continue;
            }
            if self.shard_repair_published_at(&record, watermark)? {
                continue;
            }
            protected.extend(
                record
                    .job
                    .source_manifest
                    .placements
                    .iter()
                    .chain(record.job.retiring.iter())
                    .filter(|placement| placement_is_local(placement, local_node))
                    .map(|placement| placement.transfer_id),
            );
        }
        Ok(protected)
    }

    fn transition_object_materialisation(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut ObjectMaterialisationRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("object-job/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("materialisation job not found")?;
        let mut record: ObjectMaterialisationRecord = serde_json::from_slice(&bytes)?;
        if record.state != ObjectMaterialisationState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("materialisation job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_shard_repair(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut ShardRepairRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("shard-repair/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("shard repair not found")?;
        let mut record: ShardRepairRecord = serde_json::from_slice(&bytes)?;
        if record.state != ShardRepairState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("shard repair is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_index_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut IndexFinalizationRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("index-finalization/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("index finalization job not found")?;
        let mut record: IndexFinalizationRecord = serde_json::from_slice(&bytes)?;
        if record.state != IndexFinalizationState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("index finalization job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_personaldb_postcommit(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut PersonalDbPostCommitRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("personaldb-postcommit/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("PersonalDB postcommit job not found")?;
        let mut record: PersonalDbPostCommitRecord = serde_json::from_slice(&bytes)?;
        if record.state != PersonalDbPostCommitState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("PersonalDB postcommit job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_git_source_postcommit(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut GitSourcePostCommitRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("git-source-postcommit/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("GitSource postcommit job not found")?;
        let mut record: GitSourcePostCommitRecord = serde_json::from_slice(&bytes)?;
        if record.state != GitSourcePostCommitState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("GitSource postcommit job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_hf_ingestion_postcommit(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut HfIngestionPostCommitRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("hf-ingestion-postcommit/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("Hugging Face ingestion postcommit job not found")?;
        let mut record: HfIngestionPostCommitRecord = serde_json::from_slice(&bytes)?;
        if record.state != HfIngestionPostCommitState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("Hugging Face ingestion postcommit job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_object_link_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut ObjectLinkFinalizationRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("object-link-finalization/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("object-link finalization job not found")?;
        let mut record: ObjectLinkFinalizationRecord = serde_json::from_slice(&bytes)?;
        if record.state != ObjectLinkFinalizationState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("object-link finalization job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_bucket_locator_finalization(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut BucketLocatorFinalizationRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("bucket-locator-finalization/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("bucket locator finalization job not found")?;
        let mut record: BucketLocatorFinalizationRecord = serde_json::from_slice(&bytes)?;
        if record.state != BucketLocatorFinalizationState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("bucket locator finalization job is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }

    fn transition_local_durability_upgrade(
        &self,
        job_id: &str,
        worker_id: &str,
        update: impl FnOnce(&mut LocalDurabilityUpgradeRecord) -> Result<()>,
    ) -> Result<()> {
        let _transition = self.materialisation_transition.lock().unwrap();
        let cf = self.cf(CF_MATERIALISATION)?;
        let key = self.key(format!("local-upgrade/{job_id}").as_bytes());
        let bytes = self
            .db
            .get_cf(cf, &key)?
            .context("local durability upgrade not found")?;
        let mut record: LocalDurabilityUpgradeRecord = serde_json::from_slice(&bytes)?;
        if record.state != LocalDurabilityUpgradeState::Running
            || record.lease_owner.as_deref() != Some(worker_id)
        {
            bail!("local durability upgrade is not leased by this worker");
        }
        update(&mut record)?;
        self.db.put_cf_opt(
            cf,
            key,
            serde_json::to_vec(&record)?,
            &durable_write_options(),
        )?;
        Ok(())
    }
}
