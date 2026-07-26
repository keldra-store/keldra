use super::{
    model::{
        DecodedTaskQueueRow, PendingProjection, TaskAuditEvent, TaskEntry, TaskOrder, TaskQueueRow,
        current_key, current_prefix, decode_queue_row, encode_queue_row, encode_task_audit,
        pending_key, pending_prefix,
    },
    task_queue_partition_principal,
};
use crate::{
    core_store::{
        CF_LEASES_FENCES, TABLE_STREAM_HEAD_ROW, TABLE_STREAM_RECORD_INDEX_ROW,
        TABLE_TASK_CURRENT_ROW,
    },
    formats::hash32,
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::{
        ProductMutation, coremeta_application_prefix, coremeta_logical_key,
        coremeta_tuple_from_logical_key, stream_logical_key,
    },
    mvcc_transaction::{CertificationResult, LogicalKey, PredicateKind, ReadConsistency},
    persistence::{TaskPage, TaskRecord},
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const MAX_TASK_PAGE_ROWS: usize = 1_000;
const MAX_QUEUE_CAS_ATTEMPTS: usize = 64;
const TASK_JOURNAL_HEAD_SCHEMA: &str = "anvil.task.journal-head.v2";
const TASK_JOURNAL_EVENT_SCHEMA: &str = "anvil.task.journal-event.v2";
const TASK_JOURNAL_STREAM_ID: &str = "task_queue:global";

#[derive(Debug, Clone)]
pub(super) struct RowSnapshot {
    pub payload: Option<Vec<u8>>,
    pub decoded: Option<DecodedTaskQueueRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskJournalHead {
    schema: String,
    last_sequence: u64,
    last_event_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskJournalEvent {
    schema: String,
    sequence: u64,
    previous_event_hash: String,
    event_hash: String,
    mutation_id: String,
    task_id: i64,
    payload_ref: String,
    payload: Vec<u8>,
}

pub(super) struct QueueStore<'a> {
    mvcc: &'a MvccSubsystem,
    snapshot_version: u64,
}

impl<'a> QueueStore<'a> {
    pub fn open(mvcc: &'a MvccSubsystem) -> Result<Self> {
        Ok(Self {
            mvcc,
            snapshot_version: mvcc.runtime.applied_version()?,
        })
    }

    pub fn at_snapshot(mvcc: &'a MvccSubsystem, snapshot_version: u64) -> Self {
        Self {
            mvcc,
            snapshot_version,
        }
    }

    pub fn snapshot(&self, key: &[u8]) -> Result<RowSnapshot> {
        let logical_key = queue_logical_key(key)?;
        let payload = self
            .mvcc
            .runtime
            .read_at(&logical_key, self.snapshot_version)?
            .map(|row| row.value);
        let decoded = payload
            .as_deref()
            .map(decode_queue_row)
            .transpose()
            .context("decode task queue point row")?;
        Ok(RowSnapshot { payload, decoded })
    }

    pub fn read_task(&self, task_id: i64) -> Result<Option<TaskEntry>> {
        let snapshot = self.snapshot(&current_key(task_id)?)?;
        match snapshot.decoded.map(|decoded| decoded.row) {
            None => Ok(None),
            Some(TaskQueueRow::Task(entry)) if entry.task.id == task_id => Ok(Some(entry)),
            Some(_) => bail!("task current row has the wrong row kind or scope"),
        }
    }

    pub fn first_due_task(&self, now: DateTime<Utc>) -> Result<Option<TaskEntry>> {
        let Some(projection) = self.first_pending()? else {
            return Ok(None);
        };
        if !projection.order.is_due(now)? {
            return Ok(None);
        }
        let Some(entry) = self.read_task(projection.order.task_id)? else {
            bail!("task pending projection references a missing task");
        };
        if TaskOrder::from_task(&entry.task)? != projection.order {
            bail!("task pending projection does not match the current task row");
        }
        Ok(Some(entry))
    }

    pub fn list_tasks_page(
        &self,
        after_tuple_key: Option<&[u8]>,
        page_size: usize,
    ) -> Result<TaskPage> {
        if !(1..=MAX_TASK_PAGE_ROWS).contains(&page_size) {
            bail!("task page size must be between 1 and {MAX_TASK_PAGE_ROWS}");
        }
        let tuple_prefix = current_prefix()?;
        if after_tuple_key.is_some_and(|cursor| !cursor.starts_with(&tuple_prefix)) {
            bail!("task page cursor is outside the task collection");
        }
        let prefix = coremeta_application_prefix(CF_LEASES_FENCES, &tuple_prefix)?;
        let mut rows = self.mvcc.runtime.scan_table_prefix_at(
            TABLE_TASK_CURRENT_ROW,
            &prefix,
            self.snapshot_version,
        )?;
        if let Some(after) = after_tuple_key {
            rows.retain(|(key, _)| {
                coremeta_tuple_from_logical_key(key, CF_LEASES_FENCES)
                    .is_ok_and(|tuple| tuple > after)
            });
        }
        let has_more = rows.len() > page_size;
        if has_more {
            rows.truncate(page_size);
        }
        let next_tuple_key = if has_more {
            Some(
                coremeta_tuple_from_logical_key(
                    &rows
                        .last()
                        .ok_or_else(|| anyhow!("task current page lost its final row"))?
                        .0,
                    CF_LEASES_FENCES,
                )?
                .to_vec(),
            )
        } else {
            None
        };
        let mut tasks = Vec::with_capacity(rows.len());
        for (logical_key, row) in rows {
            let decoded = decode_queue_row(&row.value).context("decode task current row")?;
            let TaskQueueRow::Task(entry) = decoded.row else {
                bail!("task current prefix contains another row kind");
            };
            let tuple_key = coremeta_tuple_from_logical_key(&logical_key, CF_LEASES_FENCES)?;
            if tuple_key != current_key(entry.task.id)?.as_slice() {
                bail!("task current row key does not match task id");
            }
            tasks.push(entry.task);
        }
        Ok(TaskPage {
            tasks,
            next_tuple_key,
            snapshot_version: self.snapshot_version,
        })
    }

    fn first_pending(&self) -> Result<Option<PendingProjection>> {
        let tuple_prefix = pending_prefix()?;
        let prefix = coremeta_application_prefix(CF_LEASES_FENCES, &tuple_prefix)?;
        let mut rows = self.mvcc.runtime.scan_table_prefix_at(
            TABLE_TASK_CURRENT_ROW,
            &prefix,
            self.snapshot_version,
        )?;
        let Some((logical_key, record)) = rows.drain(..).next() else {
            return Ok(None);
        };
        let key = coremeta_tuple_from_logical_key(&logical_key, CF_LEASES_FENCES)?;
        let decoded =
            decode_queue_row(&record.value).context("decode first task pending projection")?;
        let TaskQueueRow::Pending(projection) = decoded.row else {
            bail!("task pending prefix contains another row kind");
        };
        if key != pending_key(&projection.order)?.as_slice() {
            bail!("task pending projection key does not match its payload");
        }
        Ok(Some(projection))
    }
}

pub(super) struct TaskMutation<'a> {
    store: QueueStore<'a>,
    transaction_id: String,
    fence_token: u64,
    additional_predicates: Vec<(LogicalKey, PredicateKind)>,
    initial: BTreeMap<Vec<u8>, RowSnapshot>,
    desired: BTreeMap<Vec<u8>, Option<TaskQueueRow>>,
    audit: Vec<TaskAuditEvent>,
}

impl<'a> TaskMutation<'a> {
    pub fn new(mvcc: &'a MvccSubsystem, fence_token: u64) -> Result<Self> {
        Ok(Self {
            store: QueueStore::open(mvcc)?,
            transaction_id: format!("task-queue:{}", uuid::Uuid::new_v4()),
            fence_token,
            additional_predicates: Vec::new(),
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

    pub fn add_predicate(&mut self, predicate: (LogicalKey, PredicateKind)) {
        self.additional_predicates.push(predicate);
    }

    pub async fn commit(mut self) -> Result<()> {
        if self.desired.is_empty() && self.audit.is_empty() {
            return Ok(());
        }
        let desired_keys = self.desired.keys().cloned().collect::<Vec<_>>();
        for key in desired_keys {
            self.ensure_snapshot(&key)?;
        }
        let created_at_unix_nanos = current_unix_nanos()?;
        let mut mutations = Vec::new();
        let mut predicates = std::mem::take(&mut self.additional_predicates);
        for (key, desired) in &self.desired {
            let snapshot = self
                .initial
                .get(key)
                .ok_or_else(|| anyhow!("task mutation lost a row snapshot"))?;
            if desired.is_none() && snapshot.payload.is_none() {
                continue;
            }
            let logical_key = queue_logical_key(key)?;
            predicates.push((
                logical_key.clone(),
                predicate_for(snapshot.payload.as_deref()),
            ));
            match desired {
                Some(row) => mutations.push(ProductMutation::put(
                    logical_key,
                    encode_queue_row(row, created_at_unix_nanos)?,
                )),
                None => mutations.push(ProductMutation::delete(logical_key)),
            }
        }
        self.plan_audit_events(&mut mutations, &mut predicates)?;
        if mutations.is_empty() {
            return Ok(());
        }
        commit_task_mutation(self.store.mvcc, &self.transaction_id, mutations, predicates).await
    }

    fn plan_audit_events(
        &self,
        mutations: &mut Vec<ProductMutation>,
        predicates: &mut Vec<(LogicalKey, PredicateKind)>,
    ) -> Result<()> {
        if self.audit.is_empty() {
            return Ok(());
        }
        let head_key = stream_logical_key(TABLE_STREAM_HEAD_ROW, TASK_JOURNAL_STREAM_ID, None)?;
        let observed = self
            .store
            .mvcc
            .runtime
            .read_at(&head_key, self.store.snapshot_version)?
            .map(|row| row.value);
        let mut head =
            observed
                .as_deref()
                .map(decode_head)
                .transpose()?
                .unwrap_or(TaskJournalHead {
                    schema: TASK_JOURNAL_HEAD_SCHEMA.to_string(),
                    last_sequence: 0,
                    last_event_hash: String::new(),
                });
        predicates.push((head_key.clone(), predicate_for(observed.as_deref())));
        for (ordinal, audit) in self.audit.iter().enumerate() {
            let payload = encode_task_audit(audit, self.fence_token, &self.transaction_id)?;
            head.last_sequence = head
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("task journal sequence overflow"))?;
            let payload_ref = format!("inline:sha256:{}", hex::encode(hash32(&payload)));
            let event_hash = event_hash(
                head.last_sequence,
                &head.last_event_hash,
                &self.transaction_id,
                u32::try_from(ordinal)?,
                &payload_ref,
            );
            let event = TaskJournalEvent {
                schema: TASK_JOURNAL_EVENT_SCHEMA.to_string(),
                sequence: head.last_sequence,
                previous_event_hash: head.last_event_hash.clone(),
                event_hash: event_hash.clone(),
                mutation_id: self.transaction_id.clone(),
                task_id: audit.task_id(),
                payload_ref,
                payload,
            };
            let event_key = stream_logical_key(
                TABLE_STREAM_RECORD_INDEX_ROW,
                TASK_JOURNAL_STREAM_ID,
                Some(head.last_sequence),
            )?;
            predicates.push((event_key.clone(), PredicateKind::Absent));
            mutations.push(ProductMutation::put(event_key, serde_json::to_vec(&event)?));
            head.last_event_hash = event_hash;
        }
        mutations.push(ProductMutation::put(head_key, serde_json::to_vec(&head)?));
        Ok(())
    }

    fn ensure_snapshot(&mut self, key: &[u8]) -> Result<()> {
        if !self.initial.contains_key(key) {
            self.initial.insert(key.to_vec(), self.store.snapshot(key)?);
        }
        Ok(())
    }
}

async fn commit_task_mutation(
    mvcc: &MvccSubsystem,
    idempotency_key: &str,
    mutations: Vec<ProductMutation>,
    predicates: Vec<(LogicalKey, PredicateKind)>,
) -> Result<()> {
    let principal = task_queue_partition_principal();
    let assignment = mvcc
        .reconcile_work_assignment("task-queue", "global")
        .await?
        .ok_or_else(|| anyhow!("local node does not own the task queue assignment"))?;
    let now = now_unix_ms();
    let handle = mvcc
        .open_transactions
        .begin(
            mvcc.runtime.as_ref(),
            mvcc.cluster_id(),
            &principal,
            idempotency_key,
            std::time::Duration::from_secs(30),
            crate::mvcc_transaction::DurabilityLevel::Quorum,
            ReadConsistency::Linearized,
            now,
        )
        .await?;
    let status = mvcc
        .open_transactions
        .status(&handle.transaction_id, &principal, now)?;
    if status.state == "open" {
        mvcc.stage_product_mutations(&handle.transaction_id, &principal, mutations, now)?;
        for (key, kind) in predicates {
            mvcc.stage_predicate(&handle.transaction_id, &principal, key, kind, now)?;
        }
        mvcc.stage_assignment_guard(&handle.transaction_id, &principal, &assignment, now)?;
    }
    let outcome = mvcc
        .open_transactions
        .commit(
            mvcc.runtime.as_ref(),
            &handle.transaction_id,
            &principal,
            now_unix_ms(),
        )
        .await?;
    match outcome.certification {
        CertificationResult::Committed { .. } => Ok(()),
        CertificationResult::Aborted { reason } => {
            bail!("task queue MVCC conflict: {reason:?}")
        }
    }
}

pub(super) fn is_queue_cas_conflict(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("task queue MVCC conflict")
        || message.contains("predicate")
        || message.contains("observed")
}

pub(super) fn max_queue_cas_attempts() -> usize {
    MAX_QUEUE_CAS_ATTEMPTS
}

fn queue_logical_key(tuple_key: &[u8]) -> Result<LogicalKey> {
    coremeta_logical_key(CF_LEASES_FENCES, TABLE_TASK_CURRENT_ROW, tuple_key)
}

fn predicate_for(payload: Option<&[u8]>) -> PredicateKind {
    payload
        .map(|payload| PredicateKind::ValueHash(*blake3::hash(payload).as_bytes()))
        .unwrap_or(PredicateKind::Absent)
}

fn decode_head(payload: &[u8]) -> Result<TaskJournalHead> {
    let head: TaskJournalHead = serde_json::from_slice(payload)?;
    if head.schema != TASK_JOURNAL_HEAD_SCHEMA
        || (head.last_sequence == 0) != head.last_event_hash.is_empty()
    {
        bail!("task journal MVCC head is invalid");
    }
    Ok(head)
}

fn event_hash(
    sequence: u64,
    previous_hash: &str,
    mutation_id: &str,
    ordinal: u32,
    payload_ref: &str,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes.extend_from_slice(previous_hash.as_bytes());
    bytes.extend_from_slice(mutation_id.as_bytes());
    bytes.extend_from_slice(&ordinal.to_be_bytes());
    bytes.extend_from_slice(payload_ref.as_bytes());
    hex::encode(hash32(&bytes))
}

fn current_unix_nanos() -> Result<u64> {
    let nanos = Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp cannot be represented as nanoseconds"))?;
    u64::try_from(nanos).context("current timestamp is before the unix epoch")
}

fn now_unix_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}
