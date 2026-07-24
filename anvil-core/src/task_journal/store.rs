use super::{
    model::{
        DecodedTaskQueueRow, PendingProjection, TaskAuditEvent, TaskEntry, TaskJournalEntry,
        TaskOrder, TaskQueueRow, current_key, current_prefix, decode_queue_row, encode_queue_row,
        encode_task_audit, journal_key, pending_key, pending_prefix, row_root_key,
    },
    task_queue_partition_id, task_queue_partition_principal,
};
use crate::{
    core_store::{
        CF_LEASES_FENCES, CoreMetaStore, CoreMutationBatch, CoreMutationBatchReceipt,
        CoreMutationOperation, CoreMutationPrecondition, CoreMutationRootPublication, CoreStore,
        CoreTransactionState, TABLE_TASK_CURRENT_ROW, core_meta_payload_digest,
        core_meta_record_tuple_key,
    },
    formats::writer::WriterFamily,
    persistence::{TaskPage, TaskRecord},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};

const MAX_TASK_PAGE_ROWS: usize = 1_000;
const MAX_QUEUE_CAS_ATTEMPTS: usize = 64;

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
use super::model::journal_prefix;

#[cfg(test)]
const SOURCE_PAGE_ROWS: usize = 128;

#[cfg(test)]
thread_local! {
    static TASK_ROW_VISITS: Cell<u64> = const { Cell::new(0) };
}

#[derive(Debug, Clone)]
pub(super) struct RowSnapshot {
    pub payload: Option<Vec<u8>>,
    pub decoded: Option<DecodedTaskQueueRow>,
}

pub(super) struct QueueStore {
    storage: Storage,
    meta: CoreMetaStore,
}

impl QueueStore {
    pub fn open(storage: &Storage) -> Result<Self> {
        Ok(Self {
            storage: storage.clone(),
            meta: CoreMetaStore::open(storage.core_store_meta_path())?,
        })
    }

    pub fn snapshot(&self, key: &[u8]) -> Result<RowSnapshot> {
        // Mutation snapshots are exact write preconditions; commit must compare
        // the physical canonical bytes even while another publication is staged.
        let payload = self
            .meta
            .get(CF_LEASES_FENCES, TABLE_TASK_CURRENT_ROW, key)?;
        record_row_visits(1);
        let decoded = payload
            .as_deref()
            .map(decode_queue_row)
            .transpose()
            .context("decode task queue point row")?;
        Ok(RowSnapshot { payload, decoded })
    }

    fn visible_snapshot(&self, core_store: &CoreStore, key: &[u8]) -> Result<RowSnapshot> {
        let payload =
            core_store.read_coremeta_row(CF_LEASES_FENCES, TABLE_TASK_CURRENT_ROW, key)?;
        record_row_visits(1);
        let decoded = payload
            .as_deref()
            .map(decode_queue_row)
            .transpose()
            .context("decode visible task queue point row")?;
        Ok(RowSnapshot { payload, decoded })
    }

    pub fn read_task(&self, core_store: &CoreStore, task_id: i64) -> Result<Option<TaskEntry>> {
        let snapshot = self.visible_snapshot(core_store, &current_key(task_id)?)?;
        match snapshot.decoded.map(|decoded| decoded.row) {
            None => Ok(None),
            Some(TaskQueueRow::Task(entry)) if entry.task.id == task_id => Ok(Some(entry)),
            Some(_) => bail!("task current row has the wrong row kind or scope"),
        }
    }

    pub fn first_due_task(
        &self,
        core_store: &CoreStore,
        now: DateTime<Utc>,
    ) -> Result<Option<TaskEntry>> {
        let Some(projection) = self.first_pending(core_store)? else {
            return Ok(None);
        };
        if !projection.order.is_due(now)? {
            return Ok(None);
        }
        let Some(entry) = self.read_task(core_store, projection.order.task_id)? else {
            bail!("task pending projection references a missing task");
        };
        if TaskOrder::from_task(&entry.task)? != projection.order {
            bail!("task pending projection does not match the current task row");
        }
        Ok(Some(entry))
    }

    pub fn list_tasks_page(
        &self,
        core_store: &CoreStore,
        after_tuple_key: Option<&[u8]>,
        page_size: usize,
    ) -> Result<TaskPage> {
        if !(1..=MAX_TASK_PAGE_ROWS).contains(&page_size) {
            bail!("task page size must be between 1 and {MAX_TASK_PAGE_ROWS}");
        }
        let prefix = current_prefix()?;
        if after_tuple_key.is_some_and(|cursor| !cursor.starts_with(&prefix)) {
            bail!("task page cursor is outside the task collection");
        }
        let mut rows = core_store.scan_coremeta_prefix_page(
            CF_LEASES_FENCES,
            TABLE_TASK_CURRENT_ROW,
            &prefix,
            after_tuple_key,
            page_size + 1,
        )?;
        record_row_visits(rows.len());
        let has_more = rows.len() > page_size;
        if has_more {
            rows.truncate(page_size);
        }
        let next_tuple_key = if has_more {
            Some(
                core_meta_record_tuple_key(
                    &rows
                        .last()
                        .ok_or_else(|| anyhow!("task current page lost its final row"))?
                        .key,
                )?
                .to_vec(),
            )
        } else {
            None
        };
        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let decoded = decode_queue_row(&row.payload).context("decode task current row")?;
            let TaskQueueRow::Task(entry) = decoded.row else {
                bail!("task current prefix contains another row kind");
            };
            let tuple_key = core_meta_record_tuple_key(&row.key)?;
            if tuple_key != current_key(entry.task.id)?.as_slice() {
                bail!("task current row key does not match task id");
            }
            tasks.push(entry.task);
        }
        Ok(TaskPage {
            tasks,
            next_tuple_key,
        })
    }

    fn first_pending(&self, core_store: &CoreStore) -> Result<Option<PendingProjection>> {
        let prefix = pending_prefix()?;
        let rows = core_store.scan_coremeta_prefix_page(
            CF_LEASES_FENCES,
            TABLE_TASK_CURRENT_ROW,
            &prefix,
            None,
            1,
        )?;
        record_row_visits(rows.len());
        let Some(record) = rows.into_iter().next() else {
            return Ok(None);
        };
        let key = core_meta_record_tuple_key(&record.key)?;
        let decoded =
            decode_queue_row(&record.payload).context("decode first task pending projection")?;
        let TaskQueueRow::Pending(projection) = decoded.row else {
            bail!("task pending prefix contains another row kind");
        };
        if key != pending_key(&projection.order)?.as_slice() {
            bail!("task pending projection key does not match its payload");
        }
        Ok(Some(projection))
    }
}

pub(super) struct TaskMutation {
    store: QueueStore,
    transaction_id: String,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    additional_preconditions: Vec<CoreMutationPrecondition>,
    initial: BTreeMap<Vec<u8>, RowSnapshot>,
    desired: BTreeMap<Vec<u8>, Option<TaskQueueRow>>,
    audit: Vec<TaskAuditEvent>,
}

impl TaskMutation {
    pub fn new(
        storage: &Storage,
        fence_token: u64,
        partition_precondition: Option<CoreMutationPrecondition>,
    ) -> Result<Self> {
        Ok(Self {
            store: QueueStore::open(storage)?,
            transaction_id: format!("task-queue:{}", uuid::Uuid::new_v4()),
            fence_token,
            partition_precondition,
            additional_preconditions: Vec::new(),
            initial: BTreeMap::new(),
            desired: BTreeMap::new(),
            audit: Vec::new(),
        })
    }

    pub fn read(&mut self, key: &[u8]) -> Result<Option<TaskQueueRow>> {
        if let Some(desired) = self.desired.get(key) {
            return Ok(desired.clone());
        }
        self.ensure_snapshot(key)?;
        Ok(self
            .initial
            .get(key)
            .and_then(|snapshot| snapshot.decoded.as_ref())
            .map(|decoded| decoded.row.clone()))
    }

    pub fn read_task(&mut self, task_id: i64) -> Result<Option<TaskEntry>> {
        match self.read(&current_key(task_id)?)? {
            None => Ok(None),
            Some(TaskQueueRow::Task(entry)) if entry.task.id == task_id => Ok(Some(entry)),
            Some(_) => bail!("task current point row has the wrong row kind or scope"),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, row: TaskQueueRow) -> Result<()> {
        self.ensure_snapshot(&key)?;
        self.desired.insert(key, Some(row));
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.ensure_snapshot(&key)?;
        self.desired.insert(key, None);
        Ok(())
    }

    pub fn audit(&mut self, event: TaskAuditEvent) {
        self.audit.push(event);
    }

    pub fn add_precondition(&mut self, precondition: CoreMutationPrecondition) {
        self.additional_preconditions.push(precondition);
    }

    pub async fn commit(mut self) -> Result<()> {
        self.materialize_audit_rows()?;
        if self.desired.is_empty() {
            return Ok(());
        }
        let desired_keys = self.desired.keys().cloned().collect::<Vec<_>>();
        for key in desired_keys {
            self.ensure_snapshot(&key)?;
        }
        let created_at_unix_nanos = current_unix_nanos()?;
        let partition_id = hex::encode(task_queue_partition_id());
        let core_store = CoreStore::new(self.store.storage.clone()).await?;
        let mutation_roots = self.mutation_roots()?;
        let mut root_publications = vec![
            CoreMutationRootPublication::new(
                partition_id.clone(),
                WriterFamily::CoreControl.as_str(),
            )
            .coordinator(),
        ];
        root_publications.extend(
            mutation_roots
                .iter()
                .filter(|root| root.as_str() != partition_id.as_str())
                .cloned()
                .map(|root| {
                    CoreMutationRootPublication::new(root, WriterFamily::CoreControl.as_str())
                }),
        );
        let mut preconditions = self
            .partition_precondition
            .take()
            .into_iter()
            .collect::<Vec<_>>();
        preconditions.append(&mut self.additional_preconditions);
        let mut operations = Vec::new();

        for (key, desired) in &self.desired {
            let snapshot = self
                .initial
                .get(key)
                .ok_or_else(|| anyhow!("task mutation lost a row snapshot"))?;
            if desired.is_none() && snapshot.payload.is_none() {
                continue;
            }
            preconditions.push(row_precondition(key, snapshot));
            match desired {
                Some(row) => {
                    operations.push(CoreMutationOperation::CoreMetaPut {
                        partition_id: partition_id.clone(),
                        cf: CF_LEASES_FENCES.to_string(),
                        table_id: TABLE_TASK_CURRENT_ROW,
                        tuple_key: key.clone(),
                        payload: encode_queue_row(row, created_at_unix_nanos)?,
                    });
                }
                None => operations.push(CoreMutationOperation::CoreMetaDelete {
                    partition_id: partition_id.clone(),
                    cf: CF_LEASES_FENCES.to_string(),
                    table_id: TABLE_TASK_CURRENT_ROW,
                    tuple_key: key.clone(),
                }),
            }
        }
        if operations.is_empty() {
            return Ok(());
        }
        let receipt = core_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: self.transaction_id,
                scope_partition: partition_id,
                committed_by_principal: task_queue_partition_principal(),
                root_publications,
                preconditions,
                operations,
            })
            .await?;
        require_committed_task_mutation(&receipt)
    }

    fn materialize_audit_rows(&mut self) -> Result<()> {
        let events = std::mem::take(&mut self.audit);
        let mutation_id = self.transaction_id.clone();
        for (ordinal, event) in events.into_iter().enumerate() {
            let ordinal = u32::try_from(ordinal).context("task audit ordinal exceeds u32")?;
            let task_id = event.task_id();
            let payload = encode_task_audit(&event, self.fence_token, &mutation_id)?;
            self.put(
                journal_key(task_id, &mutation_id, ordinal)?,
                TaskQueueRow::Journal(TaskJournalEntry {
                    task_id,
                    mutation_id: mutation_id.clone(),
                    ordinal,
                    fence_token: self.fence_token,
                    payload,
                }),
            )?;
        }
        Ok(())
    }

    fn mutation_roots(&self) -> Result<BTreeSet<String>> {
        let mut roots = BTreeSet::new();
        for (key, desired) in &self.desired {
            let snapshot = self
                .initial
                .get(key)
                .ok_or_else(|| anyhow!("task mutation lost a row snapshot"))?;
            if desired.is_none() && snapshot.payload.is_none() {
                continue;
            }
            let initial_root = snapshot
                .decoded
                .as_ref()
                .map(|decoded| row_root_key(&decoded.row));
            let desired_root = desired.as_ref().map(row_root_key);
            if let (Some(initial_root), Some(desired_root)) = (&initial_root, &desired_root)
                && initial_root != desired_root
            {
                bail!("task mutation cannot move a physical row between roots");
            }
            let root = desired_root.or(initial_root).ok_or_else(|| {
                anyhow!("task mutation cannot delete a row that was already absent")
            })?;
            roots.insert(root);
        }
        Ok(roots)
    }

    fn ensure_snapshot(&mut self, key: &[u8]) -> Result<()> {
        if !self.initial.contains_key(key) {
            self.initial.insert(key.to_vec(), self.store.snapshot(key)?);
        }
        Ok(())
    }
}

pub(super) fn require_committed_task_mutation(receipt: &CoreMutationBatchReceipt) -> Result<()> {
    if receipt.state == CoreTransactionState::Committed {
        return Ok(());
    }
    bail!(
        "task queue mutation {} did not commit: {}",
        receipt.transaction_id,
        receipt
            .finalisation_error
            .as_deref()
            .unwrap_or("unknown finalisation failure")
    )
}

pub(super) fn is_queue_cas_conflict(error: &anyhow::Error) -> bool {
    if crate::core_store::is_retryable_mutation_conflict(error) {
        return true;
    }
    error.chain().any(|cause| {
        let message = cause.to_string();
        (message.contains(&format!("{TABLE_TASK_CURRENT_ROW:#06x}"))
            && (message.contains("target mismatch")
                || message.contains("must be absent")
                || message.contains("must be present")
                || message.contains("generation mismatch")
                || message.contains("payload hash mismatch")))
            || message.contains("CoreStore root CAS expected generation mismatch")
            || message.contains("CoreStore root CAS expected generation missing")
    })
}

pub(super) fn max_queue_cas_attempts() -> usize {
    MAX_QUEUE_CAS_ATTEMPTS
}

fn row_precondition(key: &[u8], snapshot: &RowSnapshot) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_CURRENT_ROW,
        tuple_key: key.to_vec(),
        expected_payload_hash: snapshot
            .payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(TABLE_TASK_CURRENT_ROW, payload)),
        require_absent: snapshot.payload.is_none(),
        require_present: snapshot.payload.is_some(),
    }
}

fn current_unix_nanos() -> Result<u64> {
    let nanos = Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp cannot be represented as nanoseconds"))?;
    u64::try_from(nanos).context("current timestamp is before the unix epoch")
}

fn record_row_visits(rows: usize) {
    #[cfg(test)]
    TASK_ROW_VISITS.with(|visits| visits.set(visits.get().saturating_add(rows as u64)));
    #[cfg(not(test))]
    let _ = rows;
}

#[cfg(test)]
pub(crate) fn reset_task_row_visits_for_test() {
    TASK_ROW_VISITS.with(|visits| visits.set(0));
}

#[cfg(test)]
pub(crate) fn task_row_visits_for_test() -> u64 {
    TASK_ROW_VISITS.with(Cell::get)
}

#[cfg(test)]
pub(crate) async fn read_task_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    let store = CoreStore::new(storage.clone()).await?;
    let prefix = journal_prefix()?;
    let mut after = None;
    let mut fences = Vec::new();
    loop {
        let rows = store.scan_coremeta_prefix_page(
            CF_LEASES_FENCES,
            TABLE_TASK_CURRENT_ROW,
            &prefix,
            after.as_deref(),
            SOURCE_PAGE_ROWS,
        )?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let decoded = decode_queue_row(&row.payload)?;
            let TaskQueueRow::Journal(entry) = decoded.row else {
                bail!("task journal prefix contains another row kind");
            };
            let tuple_key = core_meta_record_tuple_key(&row.key)?;
            if tuple_key != journal_key(entry.task_id, &entry.mutation_id, entry.ordinal)? {
                bail!("task journal key does not match its payload");
            }
            fences.push(super::model::decode_task_audit_fence(&entry.payload)?);
        }
        if rows.len() < SOURCE_PAGE_ROWS {
            break;
        }
        after = Some(
            core_meta_record_tuple_key(
                &rows
                    .last()
                    .ok_or_else(|| anyhow!("task journal page lost its final row"))?
                    .key,
            )?
            .to_vec(),
        );
    }
    Ok(fences)
}
