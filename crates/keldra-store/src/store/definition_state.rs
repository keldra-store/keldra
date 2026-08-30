use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;
use rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};

use super::{CF_DEFINITION_STATE, Store};
use crate::definition_state::{
    DefinitionAssignment, DefinitionAssignmentCursor, DefinitionAssignmentMutation,
    DefinitionAssignmentPage, DefinitionCheckpoint, DefinitionConsumerKind, DefinitionKind,
    DefinitionLocator, DefinitionLocatorCursor, DefinitionLocatorPage, DefinitionOperation,
    DefinitionStateError, DefinitionTransition, MAX_DEFINITION_STATE_SCAN_RECORDS, validate_fence,
};
use crate::key::STORAGE_KEY_FORMAT_VERSION;
use crate::{
    DeletedDefinitionCleanup, IndexRetentionDueError, PlacementLogId, SourceId, VersionId,
};

const LOCATOR_DOMAIN: u8 = b'L';
const ASSIGNMENT_DOMAIN: u8 = b'A';
const CHECKPOINT_DOMAIN: u8 = b'C';
const RECONCILIATION_DOMAIN: u8 = b'R';
const VALUE_FORMAT: u8 = 1;
const LOCATOR_VALUE_FORMAT: u8 = 2;
const LOCATOR_KEY_FIXED_BYTES: usize = 1 + 1 + 1 + 8 + 8;
const ASSIGNMENT_KEY_BYTES: usize = 1 + 1 + 1 + 8 + 8 + 8;
const CHECKPOINT_KEY_BYTES: usize = 1 + 1 + 1 + 2;
const RECONCILIATION_KEY_BYTES: usize = 1 + 1;
const LOCATOR_VALUE_BYTES: usize = 1 + 1 + 8 + 8;
const CHECKPOINT_VALUE_BYTES: usize = 1 + 32 + 8 + 8 + 8;
const RECONCILIATION_VALUE_BYTES: usize = 1 + 8 + 8;
const MAX_DEFINITION_STATE_CURSOR_BYTES: usize = LOCATOR_KEY_FIXED_BYTES + MAX_OBJECT_PATH_BYTES;

impl Store {
    pub fn definition_locator(
        &self,
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
    ) -> Result<Option<DefinitionLocator>, DefinitionStateError> {
        let key = locator_key(kind, tenant_id, bucket_id, path)?;
        self.db
            .get_cf(self.definition_state_cf()?, &key)
            .map_err(state_storage)?
            .map(|value| decode_locator(&key, &value))
            .transpose()
    }

    /// Exact-reads the two typed locator domains for one path. This never
    /// scans object heads or infers a definition from opaque payload bytes.
    pub fn definition_locator_for_path(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
    ) -> Result<Option<DefinitionLocator>, DefinitionStateError> {
        let mut selected = None;
        for kind in DefinitionKind::ALL {
            if let Some(locator) = self.definition_locator(kind, tenant_id, bucket_id, path)? {
                if selected.is_some() {
                    return Err(DefinitionStateError::Malformed(
                        "one definition path has multiple typed locators".into(),
                    ));
                }
                selected = Some(locator);
            }
        }
        Ok(selected)
    }

    pub fn scan_definition_locators(
        &self,
        kind: Option<DefinitionKind>,
        cursor: Option<&DefinitionLocatorCursor>,
        limit: u32,
    ) -> Result<DefinitionLocatorPage, DefinitionStateError> {
        self.scan_definition_locators_inner(locator_prefix(kind), cursor, limit)
    }

    /// Scans only the locator records for one stable numeric bucket. The
    /// caller never pays for definitions in another bucket or object heads.
    pub fn scan_definition_locators_by_bucket(
        &self,
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
        cursor: Option<&DefinitionLocatorCursor>,
        limit: u32,
    ) -> Result<DefinitionLocatorPage, DefinitionStateError> {
        require_nonzero(tenant_id, "tenant ID")?;
        require_nonzero(bucket_id, "bucket ID")?;
        self.scan_definition_locators_inner(
            locator_bucket_prefix(kind, tenant_id, bucket_id),
            cursor,
            limit,
        )
    }

    fn scan_definition_locators_inner(
        &self,
        prefix: Vec<u8>,
        cursor: Option<&DefinitionLocatorCursor>,
        limit: u32,
    ) -> Result<DefinitionLocatorPage, DefinitionStateError> {
        validate_scan_limit(limit)?;
        let after = cursor.map(|cursor| cursor.0.as_slice());
        if after.is_some_and(|key| !key.starts_with(&prefix)) {
            return Err(DefinitionStateError::InvalidCursor);
        }
        let start = after.unwrap_or(&prefix);
        let mut locators = Vec::with_capacity(limit as usize);
        let mut inspected = 0_usize;
        let mut last_key = None;
        let mut truncated = false;
        for item in self.db.iterator_cf(
            self.definition_state_cf()?,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, value) = item.map_err(state_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after.is_some_and(|after| key.as_ref() <= after) {
                continue;
            }
            if inspected == limit as usize {
                truncated = true;
                break;
            }
            inspected += 1;
            last_key = Some(key.to_vec());
            match decode_locator(&key, &value) {
                Ok(locator) => locators.push(locator),
                Err(error) => report_corrupt_record("locator", &key, &error),
            }
        }
        Ok(DefinitionLocatorPage {
            locators,
            next_cursor: truncated.then(|| DefinitionLocatorCursor(last_key.expect("full page"))),
        })
    }

    pub fn definition_assignment(
        &self,
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
        definition_id: u64,
    ) -> Result<Option<DefinitionAssignment>, DefinitionStateError> {
        let key = assignment_key(kind, tenant_id, bucket_id, definition_id)?;
        self.db
            .get_cf(self.definition_state_cf()?, &key)
            .map_err(state_storage)?
            .map(|value| decode_assignment(&key, &value))
            .transpose()
    }

    pub fn scan_definition_assignments(
        &self,
        cursor: Option<&DefinitionAssignmentCursor>,
        limit: u32,
    ) -> Result<DefinitionAssignmentPage, DefinitionStateError> {
        self.scan_definition_assignments_inner(assignment_prefix(None, None, None), cursor, limit)
    }

    pub fn scan_definition_assignments_by_kind(
        &self,
        kind: DefinitionKind,
        cursor: Option<&DefinitionAssignmentCursor>,
        limit: u32,
    ) -> Result<DefinitionAssignmentPage, DefinitionStateError> {
        self.scan_definition_assignments_inner(
            assignment_prefix(Some(kind), None, None),
            cursor,
            limit,
        )
    }

    pub fn scan_definition_assignments_by_bucket(
        &self,
        kind: DefinitionKind,
        tenant_id: u64,
        bucket_id: u64,
        cursor: Option<&DefinitionAssignmentCursor>,
        limit: u32,
    ) -> Result<DefinitionAssignmentPage, DefinitionStateError> {
        require_nonzero(tenant_id, "tenant ID")?;
        require_nonzero(bucket_id, "bucket ID")?;
        self.scan_definition_assignments_inner(
            assignment_prefix(Some(kind), Some(tenant_id), Some(bucket_id)),
            cursor,
            limit,
        )
    }

    fn scan_definition_assignments_inner(
        &self,
        prefix: Vec<u8>,
        cursor: Option<&DefinitionAssignmentCursor>,
        limit: u32,
    ) -> Result<DefinitionAssignmentPage, DefinitionStateError> {
        validate_scan_limit(limit)?;
        let after = cursor.map(|cursor| cursor.0.as_slice());
        if after.is_some_and(|key| !key.starts_with(&prefix)) {
            return Err(DefinitionStateError::InvalidCursor);
        }
        let start = after.unwrap_or(&prefix);
        let mut assignments = Vec::with_capacity(limit as usize);
        let mut inspected = 0_usize;
        let mut last_key = None;
        let mut truncated = false;
        for item in self.db.iterator_cf(
            self.definition_state_cf()?,
            IteratorMode::From(start, Direction::Forward),
        ) {
            let (key, value) = item.map_err(state_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            if after.is_some_and(|after| key.as_ref() <= after) {
                continue;
            }
            if inspected == limit as usize {
                truncated = true;
                break;
            }
            inspected += 1;
            last_key = Some(key.to_vec());
            match decode_assignment(&key, &value) {
                Ok(assignment) => assignments.push(assignment),
                Err(error) => report_corrupt_record("assignment", &key, &error),
            }
        }
        Ok(DefinitionAssignmentPage {
            assignments,
            next_cursor: truncated
                .then(|| DefinitionAssignmentCursor(last_key.expect("full page"))),
        })
    }

    pub fn definition_checkpoint(
        &self,
        consumer_kind: DefinitionConsumerKind,
        source_node_id: u16,
    ) -> Result<Option<DefinitionCheckpoint>, DefinitionStateError> {
        // Assignment pages persist their cursor before publishing the matching
        // process-local notification. Taking the same lock here ensures a
        // caller which observes that cursor can subsequently drain every
        // notification through it without a write/notify race.
        let _guard = self.definition_state_lock.lock().map_err(|_| {
            DefinitionStateError::Storage("definition-state lock is poisoned".into())
        })?;
        let key = checkpoint_key(consumer_kind, source_node_id)?;
        self.db
            .get_cf(self.definition_state_cf()?, &key)
            .map_err(state_storage)?
            .map(|value| decode_checkpoint(&key, &value))
            .transpose()
    }

    /// Returns the last membership fence whose complete index and accounting
    /// locator inventories were reconciled by this node. This is local recovery
    /// progress only; membership and ordinary definition objects remain the
    /// authorities.
    pub fn definition_reconciliation_fence(
        &self,
    ) -> Result<Option<PlacementLogId>, DefinitionStateError> {
        let key = reconciliation_key();
        self.db
            .get_cf(self.definition_state_cf()?, key)
            .map_err(state_storage)?
            .map(|value| decode_reconciliation_fence(key, &value))
            .transpose()
    }

    /// Durably marks one fully completed membership-triggered locator
    /// reconciliation. Interrupted work never calls this method and is
    /// therefore repeated after restart.
    pub fn complete_definition_reconciliation(
        &self,
        fence: PlacementLogId,
    ) -> Result<(), DefinitionStateError> {
        validate_fence(fence)?;
        let _guard = self.definition_state_lock.lock().map_err(|_| {
            DefinitionStateError::Storage("definition-state lock is poisoned".into())
        })?;
        if let Some(existing) = self.definition_reconciliation_fence()? {
            match fence_key(existing).cmp(&fence_key(fence)) {
                std::cmp::Ordering::Greater => {
                    return Err(DefinitionStateError::ReconciliationFenceRegression);
                }
                std::cmp::Ordering::Equal => return Ok(()),
                std::cmp::Ordering::Less => {}
            }
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .put_cf_opt(
                self.definition_state_cf()?,
                reconciliation_key(),
                encode_reconciliation_fence(fence),
                &options,
            )
            .map_err(state_storage)
    }

    /// Applies one idempotent assignment-delivery page and its source cursor
    /// in one local batch. Assignment records remain disposable projections.
    pub fn apply_definition_assignment_page(
        &self,
        mutations: &[DefinitionAssignmentMutation],
        checkpoint: &DefinitionCheckpoint,
    ) -> Result<(), DefinitionStateError> {
        checkpoint.validate()?;
        validate_assignment_mutation_count(mutations.len())?;
        let expected_kind = checkpoint.consumer_kind.definition_kind();
        for mutation in mutations {
            mutation.validate()?;
            if mutation.kind() != expected_kind
                || mutation.observed_fence() != checkpoint.observed_fence
            {
                return Err(DefinitionStateError::AssignmentCheckpointMismatch);
            }
        }

        let _guard = self.definition_state_lock.lock().map_err(|_| {
            DefinitionStateError::Storage("definition-state lock is poisoned".into())
        })?;
        let checkpoint_key =
            checkpoint_key(checkpoint.consumer_kind, checkpoint.source_id.node_id)?;
        let existing_checkpoint = self
            .db
            .get_cf(self.definition_state_cf()?, &checkpoint_key)
            .map_err(state_storage)?
            .map(|value| decode_checkpoint(&checkpoint_key, &value))
            .transpose()?;
        if existing_checkpoint.is_some_and(|existing| {
            fence_key(existing.observed_fence) > fence_key(checkpoint.observed_fence)
                || (existing.observed_fence == checkpoint.observed_fence
                    && existing.source_id.source_epoch == checkpoint.source_id.source_epoch
                    && existing.next_offset > checkpoint.next_offset)
        }) {
            return Err(DefinitionStateError::CheckpointRegression);
        }
        let mut batch = WriteBatch::default();
        let assignments_changed = self.stage_assignment_mutations(&mut batch, mutations)?;
        batch.put_cf(
            self.definition_state_cf()?,
            checkpoint_key,
            encode_checkpoint(checkpoint),
        );
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(state_storage)?;
        if !assignments_changed.is_empty() {
            self.notify_definition_assignment_changes(assignments_changed);
        }
        Ok(())
    }

    /// Applies bounded assignment changes during membership owner transfer.
    /// There is no source cursor because membership handoff is not a second
    /// journal. Callers still revalidate assignments against current placement
    /// and the authoritative ordinary definition object before use.
    pub fn apply_definition_assignment_mutations(
        &self,
        mutations: &[DefinitionAssignmentMutation],
    ) -> Result<(), DefinitionStateError> {
        validate_assignment_mutation_count(mutations.len())?;
        for mutation in mutations {
            mutation.validate()?;
        }
        let _guard = self.definition_state_lock.lock().map_err(|_| {
            DefinitionStateError::Storage("definition-state lock is poisoned".into())
        })?;
        let mut batch = WriteBatch::default();
        let changed = self.stage_assignment_mutations(&mut batch, mutations)?;
        if changed.is_empty() {
            return Ok(());
        }
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(state_storage)?;
        self.notify_definition_assignment_changes(changed);
        Ok(())
    }

    /// Deletes one disposable assignment only while it still exactly matches
    /// the value a caller revalidated. A concurrent placement repair can
    /// replace the same identity and object version; that newer value must not
    /// be removed by a delayed stale-assignment cleanup.
    pub fn remove_definition_assignment_if_matches(
        &self,
        expected: &DefinitionAssignment,
    ) -> Result<bool, DefinitionStateError> {
        expected.validate()?;
        let _guard = self.definition_state_lock.lock().map_err(|_| {
            DefinitionStateError::Storage("definition-state lock is poisoned".into())
        })?;
        let key = assignment_key(
            expected.kind,
            expected.tenant_id,
            expected.bucket_id,
            expected.definition_id,
        )?;
        let stored = self
            .db
            .get_cf(self.definition_state_cf()?, &key)
            .map_err(state_storage)?
            .map(|value| decode_assignment(&key, &value))
            .transpose()?;
        if stored.as_ref() != Some(expected) {
            return Ok(false);
        }

        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db
            .delete_cf_opt(self.definition_state_cf()?, key, &options)
            .map_err(state_storage)?;
        self.notify_definition_assignment_changes(vec![DefinitionAssignmentMutation::Remove {
            kind: expected.kind,
            tenant_id: expected.tenant_id,
            bucket_id: expected.bucket_id,
            definition_id: expected.definition_id,
            object_version: expected.object_version,
            observed_fence: expected.observed_fence,
        }]);
        Ok(true)
    }

    /// Process-local bounded delivery of committed assigned-state changes.
    /// Lag is explicit and callers then reconcile from RocksDB in bounded
    /// pages; this queue is disposable and never an authority.
    pub fn subscribe_definition_assignment_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>> {
        self.definition_assignment_notify.subscribe()
    }

    /// Visits one exact committed assignment inventory and returns a receiver
    /// whose first possible notification is newer than that inventory.
    ///
    /// Assignment writes and notification publication use the same
    /// `definition_state_lock`. Holding it through the scan and subscription
    /// closes the gap which a lossy notification queue cannot close itself.
    /// The visitor permits callers to build their own compact inventory without
    /// allocating a second unbounded `Vec` in Store.
    pub fn visit_definition_assignment_snapshot(
        &self,
        kind: DefinitionKind,
        mut visitor: impl FnMut(DefinitionAssignment),
    ) -> Result<
        tokio::sync::broadcast::Receiver<Vec<DefinitionAssignmentMutation>>,
        DefinitionStateError,
    > {
        let _guard = self.definition_state_lock.lock().map_err(|_| {
            DefinitionStateError::Storage("definition-state lock is poisoned".into())
        })?;
        let prefix = assignment_prefix(Some(kind), None, None);
        for item in self.db.iterator_cf(
            self.definition_state_cf()?,
            IteratorMode::From(&prefix, Direction::Forward),
        ) {
            let (key, value) = item.map_err(state_storage)?;
            if !key.starts_with(&prefix) {
                break;
            }
            match decode_assignment(&key, &value) {
                Ok(assignment) => visitor(assignment),
                Err(error) => report_corrupt_record("assignment", &key, &error),
            }
        }
        Ok(self.definition_assignment_notify.subscribe())
    }

    fn stage_assignment_mutations(
        &self,
        batch: &mut WriteBatch,
        mutations: &[DefinitionAssignmentMutation],
    ) -> Result<Vec<DefinitionAssignmentMutation>, DefinitionStateError> {
        let mut changed = Vec::new();
        let mut pending = BTreeMap::<Vec<u8>, PendingAssignment>::new();
        for mutation in mutations {
            let (kind, tenant_id, bucket_id, definition_id) = mutation.identity();
            let key = assignment_key(kind, tenant_id, bucket_id, definition_id)?;
            if !pending.contains_key(key.as_slice()) {
                let assignment = self
                    .db
                    .get_cf(self.definition_state_cf()?, &key)
                    .map_err(state_storage)?
                    .map(|value| decode_assignment(&key, &value))
                    .transpose()?;
                pending.insert(key.to_vec(), PendingAssignment::from_stored(assignment));
            }
            let state = pending.get_mut(key.as_slice()).expect("inserted above");
            if state.is_newer_than(mutation) {
                continue;
            }
            if let DefinitionAssignmentMutation::Delete(deletion) = mutation
                && deletion.kind == DefinitionKind::Index
            {
                self.stage_deleted_definition_cleanup(
                    batch,
                    &DeletedDefinitionCleanup {
                        tenant_id: deletion.tenant_id,
                        bucket_id: deletion.bucket_id,
                        index_id: deletion.definition_id,
                        definition_path: deletion.definition_path.clone(),
                        definition_object_version: deletion.object_version,
                        due_at_unix_millis: now_unix_millis()?,
                    },
                )
                .map_err(retention_due_state)?;
            }
            match mutation {
                DefinitionAssignmentMutation::Upsert(assignment)
                    if state.assignment.as_ref() != Some(assignment) =>
                {
                    batch.put_cf(
                        self.definition_state_cf()?,
                        &key,
                        encode_assignment(assignment)?,
                    );
                    changed.push(mutation.clone());
                }
                DefinitionAssignmentMutation::Delete(_) => {
                    if state.assignment.is_some() {
                        batch.delete_cf(self.definition_state_cf()?, &key);
                    }
                    changed.push(mutation.clone());
                }
                DefinitionAssignmentMutation::Remove { .. } if state.assignment.is_some() => {
                    batch.delete_cf(self.definition_state_cf()?, &key);
                    changed.push(mutation.clone());
                }
                DefinitionAssignmentMutation::Upsert(_)
                | DefinitionAssignmentMutation::Remove { .. } => {}
            }
            state.accept(mutation);
        }
        Ok(changed)
    }

    fn notify_definition_assignment_changes(&self, mutations: Vec<DefinitionAssignmentMutation>) {
        let _ = self.definition_assignment_notify.send(mutations);
    }

    pub(crate) fn stage_definition_transition(
        &self,
        batch: &mut WriteBatch,
        transition: &DefinitionTransition,
    ) -> Result<(), DefinitionStateError> {
        transition.validate()?;
        let key = locator_key(
            transition.kind,
            transition.tenant_id,
            transition.bucket_id,
            &transition.path,
        )?;
        batch.put_cf(
            self.definition_state_cf()?,
            key,
            encode_locator(&transition.locator()),
        );
        Ok(())
    }

    pub(crate) fn definition_transition_is_applied(
        &self,
        transition: &DefinitionTransition,
    ) -> Result<bool, crate::MutationError> {
        transition
            .validate()
            .map_err(|error| crate::MutationError::InvalidObjectMutation(error.to_string()))?;
        let stored = self
            .definition_locator(
                transition.kind,
                transition.tenant_id,
                transition.bucket_id,
                &transition.path,
            )
            .map_err(|error| crate::MutationError::Storage(error.to_string()))?;
        Ok(stored.as_ref() == Some(&transition.locator()))
    }

    fn definition_state_cf(&self) -> Result<&rocksdb::ColumnFamily, DefinitionStateError> {
        self.cf(CF_DEFINITION_STATE).map_err(state_storage)
    }
}

struct PendingAssignment {
    assignment: Option<DefinitionAssignment>,
    object_version: Option<VersionId>,
    observed_fence: Option<PlacementLogId>,
}

impl PendingAssignment {
    fn from_stored(assignment: Option<DefinitionAssignment>) -> Self {
        let object_version = assignment.as_ref().map(|value| value.object_version);
        let observed_fence = assignment.as_ref().map(|value| value.observed_fence);
        Self {
            assignment,
            object_version,
            observed_fence,
        }
    }

    fn is_newer_than(&self, mutation: &DefinitionAssignmentMutation) -> bool {
        self.object_version.is_some_and(|version| {
            version > mutation.object_version()
                || (version == mutation.object_version()
                    && self.observed_fence.is_some_and(|fence| {
                        fence_key(fence) > fence_key(mutation.observed_fence())
                    }))
        })
    }

    fn accept(&mut self, mutation: &DefinitionAssignmentMutation) {
        self.object_version = Some(mutation.object_version());
        self.observed_fence = Some(mutation.observed_fence());
        self.assignment = match mutation {
            DefinitionAssignmentMutation::Upsert(assignment) => Some(assignment.clone()),
            DefinitionAssignmentMutation::Delete(_)
            | DefinitionAssignmentMutation::Remove { .. } => None,
        };
    }
}

pub(crate) fn validate_locator_cursor(key: &[u8]) -> Result<(), DefinitionStateError> {
    validate_raw_cursor(key, LOCATOR_DOMAIN)
}

pub(crate) fn validate_assignment_cursor(key: &[u8]) -> Result<(), DefinitionStateError> {
    validate_raw_cursor(key, ASSIGNMENT_DOMAIN)
}

fn validate_raw_cursor(key: &[u8], domain: u8) -> Result<(), DefinitionStateError> {
    if key.len() < 2
        || key.len() > MAX_DEFINITION_STATE_CURSOR_BYTES
        || key[0] != STORAGE_KEY_FORMAT_VERSION
        || key[1] != domain
    {
        return Err(DefinitionStateError::InvalidCursor);
    }
    Ok(())
}

fn report_corrupt_record(domain: &'static str, key: &[u8], error: &DefinitionStateError) {
    tracing::error!(
        definition_state.domain = domain,
        definition_state.key_bytes = key.len(),
        monotonic_counter.keldra_definition_state_corrupt_records_total = 1_u64,
        %error,
        "skipping corrupt disposable definition-state record"
    );
}

pub(crate) fn locator_key(
    kind: DefinitionKind,
    tenant_id: u64,
    bucket_id: u64,
    path: &str,
) -> Result<Vec<u8>, DefinitionStateError> {
    require_nonzero(tenant_id, "tenant ID")?;
    require_nonzero(bucket_id, "bucket ID")?;
    crate::ObjectKey::new("typed", "definitions", path)
        .map_err(|error| DefinitionStateError::Malformed(error.to_string()))?;
    let mut key = Vec::with_capacity(LOCATOR_KEY_FIXED_BYTES + path.len());
    key.extend_from_slice(&[STORAGE_KEY_FORMAT_VERSION, LOCATOR_DOMAIN, kind as u8]);
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&bucket_id.to_be_bytes());
    key.extend_from_slice(path.as_bytes());
    Ok(key)
}

fn decode_locator_key(
    key: &[u8],
) -> Result<(DefinitionKind, u64, u64, String), DefinitionStateError> {
    if key.len() <= LOCATOR_KEY_FIXED_BYTES
        || key[0] != STORAGE_KEY_FORMAT_VERSION
        || key[1] != LOCATOR_DOMAIN
    {
        return Err(DefinitionStateError::Malformed(
            "definition locator key is malformed".into(),
        ));
    }
    let kind = DefinitionKind::from_byte(key[2])?;
    let tenant_id = read_u64(&key[3..11])?;
    let bucket_id = read_u64(&key[11..19])?;
    let path = std::str::from_utf8(&key[19..])
        .map_err(|_| DefinitionStateError::Malformed("locator path is not UTF-8".into()))?
        .to_owned();
    locator_key(kind, tenant_id, bucket_id, &path)?;
    Ok((kind, tenant_id, bucket_id, path))
}

fn locator_prefix(kind: Option<DefinitionKind>) -> Vec<u8> {
    let mut prefix = vec![STORAGE_KEY_FORMAT_VERSION, LOCATOR_DOMAIN];
    if let Some(kind) = kind {
        prefix.push(kind as u8);
    }
    prefix
}

fn locator_bucket_prefix(kind: DefinitionKind, tenant_id: u64, bucket_id: u64) -> Vec<u8> {
    let mut prefix = locator_prefix(Some(kind));
    prefix.extend_from_slice(&tenant_id.to_be_bytes());
    prefix.extend_from_slice(&bucket_id.to_be_bytes());
    prefix
}

pub(crate) fn encode_locator(locator: &DefinitionLocator) -> [u8; LOCATOR_VALUE_BYTES] {
    let mut value = [0; LOCATOR_VALUE_BYTES];
    value[0] = LOCATOR_VALUE_FORMAT;
    value[1] = locator.operation as u8;
    value[2..10].copy_from_slice(&locator.definition_id.to_be_bytes());
    value[10..18].copy_from_slice(&locator.object_version.0.to_be_bytes());
    value
}

pub(crate) fn decode_locator(
    key: &[u8],
    value: &[u8],
) -> Result<DefinitionLocator, DefinitionStateError> {
    let value: &[u8; LOCATOR_VALUE_BYTES] = value.try_into().map_err(|_| {
        DefinitionStateError::Malformed("definition locator value is malformed".into())
    })?;
    if value[0] != LOCATOR_VALUE_FORMAT {
        return Err(DefinitionStateError::Malformed(
            "definition locator value format is unsupported".into(),
        ));
    }
    let (kind, tenant_id, bucket_id, path) = decode_locator_key(key)?;
    let locator = DefinitionLocator {
        kind,
        tenant_id,
        bucket_id,
        definition_id: read_u64(&value[2..10])?,
        path,
        object_version: VersionId(read_u64(&value[10..18])?),
        operation: DefinitionOperation::from_byte(value[1])?,
    };
    locator.validate()?;
    Ok(locator)
}

fn assignment_key(
    kind: DefinitionKind,
    tenant_id: u64,
    bucket_id: u64,
    definition_id: u64,
) -> Result<[u8; ASSIGNMENT_KEY_BYTES], DefinitionStateError> {
    require_nonzero(tenant_id, "tenant ID")?;
    require_nonzero(bucket_id, "bucket ID")?;
    require_nonzero(definition_id, "definition ID")?;
    let mut key = [0; ASSIGNMENT_KEY_BYTES];
    key[..3].copy_from_slice(&[STORAGE_KEY_FORMAT_VERSION, ASSIGNMENT_DOMAIN, kind as u8]);
    key[3..11].copy_from_slice(&tenant_id.to_be_bytes());
    key[11..19].copy_from_slice(&bucket_id.to_be_bytes());
    key[19..27].copy_from_slice(&definition_id.to_be_bytes());
    Ok(key)
}

fn decode_assignment_key(
    key: &[u8],
) -> Result<(DefinitionKind, u64, u64, u64), DefinitionStateError> {
    let key: &[u8; ASSIGNMENT_KEY_BYTES] = key.try_into().map_err(|_| {
        DefinitionStateError::Malformed("definition assignment key is malformed".into())
    })?;
    if key[0] != STORAGE_KEY_FORMAT_VERSION || key[1] != ASSIGNMENT_DOMAIN {
        return Err(DefinitionStateError::Malformed(
            "definition assignment key is malformed".into(),
        ));
    }
    let identity = (
        DefinitionKind::from_byte(key[2])?,
        read_u64(&key[3..11])?,
        read_u64(&key[11..19])?,
        read_u64(&key[19..27])?,
    );
    Ok(identity)
}

fn assignment_prefix(
    kind: Option<DefinitionKind>,
    tenant_id: Option<u64>,
    bucket_id: Option<u64>,
) -> Vec<u8> {
    let mut prefix = vec![STORAGE_KEY_FORMAT_VERSION, ASSIGNMENT_DOMAIN];
    if let Some(kind) = kind {
        prefix.push(kind as u8);
    }
    if let Some(tenant_id) = tenant_id {
        prefix.extend_from_slice(&tenant_id.to_be_bytes());
    }
    if let Some(bucket_id) = bucket_id {
        prefix.extend_from_slice(&bucket_id.to_be_bytes());
    }
    prefix
}

fn encode_assignment(assignment: &DefinitionAssignment) -> Result<Vec<u8>, DefinitionStateError> {
    assignment.validate()?;
    let path_bytes = assignment.definition_path.as_bytes();
    let path_length = u32::try_from(path_bytes.len()).map_err(|_| {
        DefinitionStateError::Malformed("definition assignment path is too long".into())
    })?;
    let mut value = Vec::with_capacity(1 + 4 + path_bytes.len() + 8 + 8 + 8 + 1);
    value.push(VALUE_FORMAT);
    value.extend_from_slice(&path_length.to_be_bytes());
    value.extend_from_slice(path_bytes);
    value.extend_from_slice(&assignment.object_version.0.to_be_bytes());
    value.extend_from_slice(&assignment.observed_fence.term.to_be_bytes());
    value.extend_from_slice(&assignment.observed_fence.index.to_be_bytes());
    value.push(assignment.rank);
    Ok(value)
}

fn decode_assignment(
    key: &[u8],
    value: &[u8],
) -> Result<DefinitionAssignment, DefinitionStateError> {
    let minimum = 1 + 4 + 8 + 8 + 8 + 1;
    if value.len() < minimum || value[0] != VALUE_FORMAT {
        return Err(DefinitionStateError::Malformed(
            "definition assignment value is malformed or unsupported".into(),
        ));
    }
    let path_length = u32::from_be_bytes(value[1..5].try_into().expect("fixed slice")) as usize;
    let expected = minimum.checked_add(path_length).ok_or_else(|| {
        DefinitionStateError::Malformed("definition assignment length overflow".into())
    })?;
    if value.len() != expected {
        return Err(DefinitionStateError::Malformed(
            "definition assignment value length is malformed".into(),
        ));
    }
    let path_end = 5 + path_length;
    let (kind, tenant_id, bucket_id, definition_id) = decode_assignment_key(key)?;
    let assignment = DefinitionAssignment {
        kind,
        tenant_id,
        bucket_id,
        definition_id,
        definition_path: std::str::from_utf8(&value[5..path_end])
            .map_err(|_| DefinitionStateError::Malformed("assignment path is not UTF-8".into()))?
            .to_owned(),
        object_version: VersionId(read_u64(&value[path_end..path_end + 8])?),
        observed_fence: PlacementLogId {
            term: read_u64(&value[path_end + 8..path_end + 16])?,
            index: read_u64(&value[path_end + 16..path_end + 24])?,
        },
        rank: value[path_end + 24],
    };
    assignment.validate()?;
    Ok(assignment)
}

fn checkpoint_key(
    consumer: DefinitionConsumerKind,
    source_node_id: u16,
) -> Result<[u8; CHECKPOINT_KEY_BYTES], DefinitionStateError> {
    if source_node_id == 0 {
        return Err(DefinitionStateError::Malformed(
            "definition checkpoint source node must be non-zero".into(),
        ));
    }
    let mut key = [0; CHECKPOINT_KEY_BYTES];
    key[..3].copy_from_slice(&[
        STORAGE_KEY_FORMAT_VERSION,
        CHECKPOINT_DOMAIN,
        consumer as u8,
    ]);
    key[3..5].copy_from_slice(&source_node_id.to_be_bytes());
    Ok(key)
}

const fn reconciliation_key() -> &'static [u8; RECONCILIATION_KEY_BYTES] {
    &[STORAGE_KEY_FORMAT_VERSION, RECONCILIATION_DOMAIN]
}

fn encode_reconciliation_fence(fence: PlacementLogId) -> [u8; RECONCILIATION_VALUE_BYTES] {
    let mut value = [0; RECONCILIATION_VALUE_BYTES];
    value[0] = VALUE_FORMAT;
    value[1..9].copy_from_slice(&fence.term.to_be_bytes());
    value[9..17].copy_from_slice(&fence.index.to_be_bytes());
    value
}

fn decode_reconciliation_fence(
    key: &[u8],
    value: &[u8],
) -> Result<PlacementLogId, DefinitionStateError> {
    if key != reconciliation_key() || value.len() != RECONCILIATION_VALUE_BYTES {
        return Err(DefinitionStateError::Malformed(
            "definition reconciliation record is malformed".into(),
        ));
    }
    if value[0] != VALUE_FORMAT {
        return Err(DefinitionStateError::Malformed(
            "definition reconciliation record format is unsupported".into(),
        ));
    }
    let fence = PlacementLogId {
        term: read_u64(&value[1..9])?,
        index: read_u64(&value[9..17])?,
    };
    validate_fence(fence)?;
    Ok(fence)
}

fn encode_checkpoint(checkpoint: &DefinitionCheckpoint) -> [u8; CHECKPOINT_VALUE_BYTES] {
    let mut value = [0; CHECKPOINT_VALUE_BYTES];
    value[0] = VALUE_FORMAT;
    value[1..33].copy_from_slice(&checkpoint.source_id.source_epoch);
    value[33..41].copy_from_slice(&checkpoint.next_offset.to_be_bytes());
    value[41..49].copy_from_slice(&checkpoint.observed_fence.term.to_be_bytes());
    value[49..57].copy_from_slice(&checkpoint.observed_fence.index.to_be_bytes());
    value
}

fn decode_checkpoint(
    key: &[u8],
    value: &[u8],
) -> Result<DefinitionCheckpoint, DefinitionStateError> {
    let key: &[u8; CHECKPOINT_KEY_BYTES] = key.try_into().map_err(|_| {
        DefinitionStateError::Malformed("definition checkpoint key is malformed".into())
    })?;
    let value: &[u8; CHECKPOINT_VALUE_BYTES] = value.try_into().map_err(|_| {
        DefinitionStateError::Malformed("definition checkpoint value is malformed".into())
    })?;
    if key[0] != STORAGE_KEY_FORMAT_VERSION
        || key[1] != CHECKPOINT_DOMAIN
        || value[0] != VALUE_FORMAT
    {
        return Err(DefinitionStateError::Malformed(
            "definition checkpoint format is unsupported".into(),
        ));
    }
    let checkpoint = DefinitionCheckpoint {
        consumer_kind: DefinitionConsumerKind::from_byte(key[2])?,
        source_id: SourceId {
            node_id: u16::from_be_bytes(key[3..5].try_into().expect("fixed slice")),
            source_epoch: value[1..33].try_into().expect("fixed slice"),
        },
        // Offset zero is the valid beginning of a newly-created source
        // journal. Unlike stable IDs and fences it must not be rejected by
        // the non-zero integer decoder.
        next_offset: u64::from_be_bytes(value[33..41].try_into().expect("fixed slice")),
        observed_fence: PlacementLogId {
            term: read_u64(&value[41..49])?,
            index: read_u64(&value[49..57])?,
        },
    };
    checkpoint.validate()?;
    Ok(checkpoint)
}

fn validate_scan_limit(limit: u32) -> Result<(), DefinitionStateError> {
    if limit == 0 || limit > MAX_DEFINITION_STATE_SCAN_RECORDS {
        Err(DefinitionStateError::InvalidScanLimit)
    } else {
        Ok(())
    }
}

fn validate_assignment_mutation_count(count: usize) -> Result<(), DefinitionStateError> {
    if count > MAX_DEFINITION_STATE_SCAN_RECORDS as usize {
        Err(DefinitionStateError::Malformed(format!(
            "definition assignment page exceeds {MAX_DEFINITION_STATE_SCAN_RECORDS} mutations"
        )))
    } else {
        Ok(())
    }
}

fn read_u64(value: &[u8]) -> Result<u64, DefinitionStateError> {
    let value =
        u64::from_be_bytes(value.try_into().map_err(|_| {
            DefinitionStateError::Malformed("fixed-width integer is malformed".into())
        })?);
    require_nonzero(value, "stable integer")?;
    Ok(value)
}

fn require_nonzero(value: u64, label: &str) -> Result<(), DefinitionStateError> {
    if value == 0 {
        Err(DefinitionStateError::Malformed(format!(
            "{label} must be non-zero"
        )))
    } else {
        Ok(())
    }
}

fn fence_key(fence: PlacementLogId) -> (u64, u64) {
    (fence.term, fence.index)
}

fn state_storage(error: impl std::fmt::Display) -> DefinitionStateError {
    DefinitionStateError::Storage(error.to_string())
}

fn retention_due_state(error: IndexRetentionDueError) -> DefinitionStateError {
    match error {
        IndexRetentionDueError::Malformed(message) => DefinitionStateError::Malformed(message),
        IndexRetentionDueError::Storage(message) => DefinitionStateError::Storage(message),
    }
}

fn now_unix_millis() -> Result<u64, DefinitionStateError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(state_storage)?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| DefinitionStateError::Storage("system time exceeds u64 millis".into()))
}

#[cfg(test)]
mod tests;
