//! Object journal facts installed atomically at MVCC apply time.
//!
//! Foreground object transactions cannot allocate sequence numbers from a
//! shared per-bucket head without making unrelated keys conflict. This intent
//! is part of the certified bundle; the Raft-assigned commit version and the
//! canonical job/entry ordinals provide its deterministic cursor at apply.

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::{
    mvcc_product::ProductMutation,
    persistence::{Bucket, Object, ObjectWatchEvent},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectJournalCommitJob {
    pub schema: String,
    pub cluster_id: String,
    pub transaction_id: String,
    pub bucket: Bucket,
    pub entries: Vec<ObjectJournalCommitEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectJournalCommitEntry {
    pub object: Object,
    pub metadata_record_kind: String,
    pub metadata_payload: Vec<u8>,
    pub watch_event: ObjectWatchEvent,
    /// Projection facts captured at the transaction snapshot and installed
    /// with a generation derived from the authoritative Raft commit cursor.
    ///
    /// Older v1 jobs already contain their projection writes in the certified
    /// bundle, so an absent value intentionally means "journal facts only".
    #[serde(default)]
    pub(crate) projection_snapshot: Option<crate::metadata_journal::ObjectProjectionSnapshot>,
}

impl ObjectJournalCommitJob {
    pub const LEGACY_SCHEMA: &'static str = "anvil.object-journal-commit.v1";
    pub const SCHEMA: &'static str = "anvil.object-journal-commit.v2";
    const MAX_JOBS_PER_TRANSACTION: usize = 256;
    const MAX_ENTRIES_PER_JOB: usize = 256;

    pub fn new(
        cluster_id: impl Into<String>,
        transaction_id: impl Into<String>,
        bucket: Bucket,
        entries: Vec<ObjectJournalCommitEntry>,
    ) -> Result<Self> {
        let job = Self {
            schema: Self::SCHEMA.into(),
            cluster_id: cluster_id.into(),
            transaction_id: transaction_id.into(),
            bucket,
            entries,
        };
        job.validate()?;
        Ok(job)
    }

    pub fn validate(&self) -> Result<()> {
        if !Self::is_schema(&self.schema)
            || self.cluster_id.trim().is_empty()
            || self.transaction_id.trim().is_empty()
            || self.bucket.id <= 0
            || self.bucket.tenant_id < 0
            || self.bucket.name.trim().is_empty()
            || self.entries.is_empty()
            || self.entries.len() > Self::MAX_ENTRIES_PER_JOB
        {
            bail!("invalid object journal commit job");
        }
        if self.schema == Self::SCHEMA
            && self
                .entries
                .iter()
                .any(|entry| entry.projection_snapshot.is_none())
        {
            bail!("object journal commit v2 entry is missing its projection snapshot");
        }
        if self.schema == Self::LEGACY_SCHEMA
            && self
                .entries
                .iter()
                .any(|entry| entry.projection_snapshot.is_some())
        {
            bail!("legacy object journal commit entry contains a v2 projection snapshot");
        }
        for entry in &self.entries {
            if entry.object.tenant_id != self.bucket.tenant_id
                || entry.object.bucket_id != self.bucket.id
                || entry.metadata_record_kind.trim().is_empty()
                || entry.metadata_payload.is_empty()
                || entry.watch_event.tenant_id != self.bucket.tenant_id
                || entry.watch_event.bucket_id != self.bucket.id
                || entry.watch_event.bucket_name != self.bucket.name
                || entry.watch_event.key != entry.object.key
                || entry.watch_event.version_id != Some(entry.object.version_id)
                || entry.watch_event.mutation_id != entry.object.mutation_id
            {
                bail!("object journal commit entry does not match its bucket and object");
            }
        }
        Ok(())
    }

    pub fn is_schema(schema: &str) -> bool {
        schema == Self::LEGACY_SCHEMA || schema == Self::SCHEMA
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let job: Self = serde_json::from_slice(bytes)?;
        job.validate()?;
        if job.canonical_bytes()?.as_slice() != bytes {
            bail!("object journal commit job is not canonically encoded");
        }
        Ok(job)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn committed_mutations(
        &self,
        commit_version: u64,
        job_ordinal: usize,
    ) -> Result<Vec<ProductMutation>> {
        self.validate()?;
        if job_ordinal >= Self::MAX_JOBS_PER_TRANSACTION {
            bail!("too many materialisation jobs for object journal cursor");
        }
        let mut mutations = Vec::with_capacity(self.entries.len().saturating_mul(5));
        for (entry_ordinal, entry) in self.entries.iter().enumerate() {
            let cursor = commit_cursor(commit_version, job_ordinal, entry_ordinal)?;
            let (entry, projection_mutations) = crate::metadata_journal::prepare_committed_entry(
                &self.bucket,
                entry,
                cursor,
                &self.transaction_id,
            )?;
            mutations.extend(projection_mutations);
            mutations.extend(
                crate::metadata_journal::mvcc_event::committed_event_mutations(
                    &self.bucket,
                    &entry,
                    cursor,
                )?,
            );
            mutations.extend(crate::watch_log::committed_event_mutations(
                &self.bucket,
                &entry.object,
                &entry.watch_event,
                cursor,
            )?);
        }
        Ok(mutations)
    }
}

pub fn commit_cursor(commit_version: u64, job_ordinal: usize, entry_ordinal: usize) -> Result<u64> {
    if job_ordinal >= ObjectJournalCommitJob::MAX_JOBS_PER_TRANSACTION
        || entry_ordinal >= ObjectJournalCommitJob::MAX_ENTRIES_PER_JOB
    {
        bail!("object journal cursor ordinal exceeds its encoding");
    }
    if commit_version > (u64::MAX >> 16) {
        bail!("object journal commit version exceeds cursor encoding");
    }
    let version = commit_version << 16;
    let ordinal = (u64::try_from(job_ordinal)? << 8) | u64::try_from(entry_ordinal)?;
    version
        .checked_add(ordinal + 1)
        .ok_or_else(|| anyhow!("object journal cursor overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn job(transaction_id: &str, object_key: &str) -> ObjectJournalCommitJob {
        let bucket = Bucket {
            id: 11,
            tenant_id: 7,
            name: "objects".into(),
            region: "local".into(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let created_at = Utc::now();
        let object = Object {
            id: 1,
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            key: object_key.into(),
            kind: Default::default(),
            content_hash: "payload-hash".into(),
            size: 4,
            etag: "payload-hash".into(),
            content_type: Some("application/json".into()),
            version_id,
            mutation_id,
            index_policy_snapshot: "policy".into(),
            user_metadata_hash: "metadata".into(),
            authz_revision: 1,
            record_hash: "record".into(),
            created_at,
            deleted_at: None,
            storage_class: Some("local".into()),
            user_meta: None,
            shard_map: None,
            checksum: None,
            link: None,
        };
        let event = ObjectWatchEvent {
            id: 0,
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            bucket_name: bucket.name.clone(),
            key: object.key.clone(),
            event_type: "put".into(),
            version_id: Some(version_id),
            mutation_id,
            payload_hash: object.content_hash.clone(),
            etag: Some(object.etag.clone()),
            size: object.size,
            is_delete_marker: false,
            created_at,
        };
        let job = ObjectJournalCommitJob {
            schema: ObjectJournalCommitJob::LEGACY_SCHEMA.into(),
            cluster_id: "cluster".into(),
            transaction_id: transaction_id.into(),
            bucket,
            entries: vec![ObjectJournalCommitEntry {
                object,
                metadata_record_kind: "object_metadata.object_version".into(),
                metadata_payload: b"metadata".to_vec(),
                watch_event: event,
                projection_snapshot: None,
            }],
        };
        job.validate().unwrap();
        job
    }

    #[test]
    fn cursors_are_ordered_by_commit_then_canonical_ordinals() {
        assert!(commit_cursor(7, 0, 0).unwrap() < commit_cursor(7, 0, 1).unwrap());
        assert!(commit_cursor(7, 0, 255).unwrap() < commit_cursor(7, 1, 0).unwrap());
        assert!(commit_cursor(7, 255, 255).unwrap() < commit_cursor(8, 0, 0).unwrap());
        assert!(commit_cursor(u64::MAX, 0, 0).is_err());
    }

    #[test]
    fn unrelated_object_jobs_derive_distinct_immutable_facts_without_stream_heads() {
        let first = job("transaction-a", "a").committed_mutations(9, 0).unwrap();
        let second = job("transaction-b", "b")
            .committed_mutations(10, 0)
            .unwrap();
        assert!(
            first
                .iter()
                .chain(&second)
                .all(|mutation| mutation.key.table_id != crate::core_store::TABLE_STREAM_HEAD_ROW)
        );
        let first_keys = first
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            second
                .iter()
                .all(|mutation| !first_keys.contains(&mutation.key))
        );
    }
}
