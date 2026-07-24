use crate::{
    core_store::{
        CoreMetaRowCommonProto, CoreMetaTuplePart, CoreMetaVisibilityState,
        core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
    },
    formats::hash32,
    persistence::TaskRecord,
    tasks::{TaskStatus, TaskType},
};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use prost::{Message, Oneof};
use serde_json::Value as JsonValue;

pub(super) const TASK_QUEUE_ROW_SCHEMA: &str = "anvil.core.task_queue_row.v1";
pub(super) const TASK_AUDIT_SCHEMA: &str = "anvil.core.task_audit.v1";
pub(super) const TASK_QUEUE_REALM_ID: &str = "anvil.system.task_queue";
pub(super) const TASK_ROW_MAX_PROTO_BYTES: usize = 16 * 1024;
const TASK_QUEUE_CANDIDATE_GENERATION: u64 = 1;
const TASK_QUEUE_CANDIDATE_TRANSACTION_ID: &str = "task-queue-candidate";

const CURRENT_PREFIX: &str = "task_queue/current";
const ALLOCATOR_KEY: &str = "task_queue/allocator";
const PENDING_PREFIX: &str = "task_queue/pending";
const RUNNING_PREFIX: &str = "task_queue/running";
const DEDUPE_PREFIX: &str = "task_queue/live_dedupe";
const GROUP_PREFIX: &str = "task_queue/group";
const JOURNAL_PREFIX: &str = "task_queue/journal";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct TaskOrder {
    pub scheduled_at_nanos: i64,
    pub priority: i32,
    pub created_at_nanos: i64,
    pub task_id: i64,
}

impl TaskOrder {
    pub fn from_task(task: &TaskRecord) -> Result<Self> {
        Ok(Self {
            scheduled_at_nanos: timestamp_nanos(task.scheduled_at, "scheduled_at")?,
            priority: task.priority,
            created_at_nanos: timestamp_nanos(task.created_at, "created_at")?,
            task_id: task.id,
        })
    }

    pub fn is_due(&self, now: DateTime<Utc>) -> Result<bool> {
        Ok(self.scheduled_at_nanos <= timestamp_nanos(now, "now")?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TaskGroupIdentity {
    pub kind: String,
    pub hash: String,
    pub cursor: u128,
}

#[derive(Debug, Clone)]
pub(super) struct TaskEntry {
    pub task: TaskRecord,
    pub dedupe_hash: Option<String>,
    pub group: Option<TaskGroupIdentity>,
}

#[derive(Debug, Clone)]
pub(super) struct TaskAllocator {
    pub max_task_id: i64,
}

#[derive(Debug, Clone)]
pub(super) struct PendingProjection {
    pub order: TaskOrder,
}

#[derive(Debug, Clone)]
pub(super) struct RunningProjection {
    pub task_id: i64,
    pub task_type: TaskType,
    pub group_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct LiveDedupeHead {
    pub dedupe_hash: String,
    pub task_id: i64,
}

#[derive(Debug, Clone)]
pub(super) struct TaskGroupHead {
    pub kind: String,
    pub group_hash: String,
    pub running_task_id: Option<i64>,
    pub pending_task_id: Option<i64>,
}

#[derive(Debug, Clone)]
pub(super) struct TaskJournalEntry {
    pub task_id: i64,
    pub mutation_id: String,
    pub ordinal: u32,
    pub fence_token: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(super) enum TaskQueueRow {
    Task(TaskEntry),
    Allocator(TaskAllocator),
    Pending(PendingProjection),
    Running(RunningProjection),
    Dedupe(LiveDedupeHead),
    Group(TaskGroupHead),
    Journal(TaskJournalEntry),
}

#[derive(Debug, Clone)]
pub(super) struct DecodedTaskQueueRow {
    pub row: TaskQueueRow,
    pub publication_generation: u64,
}

#[derive(Debug, Clone)]
pub(super) enum TaskAuditEvent {
    Enqueued {
        task: TaskRecord,
    },
    Claimed {
        task_id: i64,
        updated_at: DateTime<Utc>,
    },
    StatusUpdated {
        task_id: i64,
        status: TaskStatus,
        updated_at: DateTime<Utc>,
    },
    Failed {
        task_id: i64,
        error: String,
        attempts: i32,
        scheduled_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    },
}

impl TaskAuditEvent {
    pub fn task_id(&self) -> i64 {
        match self {
            Self::Enqueued { task } => task.id,
            Self::Claimed { task_id, .. }
            | Self::StatusUpdated { task_id, .. }
            | Self::Failed { task_id, .. } => *task_id,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct TaskQueueRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(oneof = "task_queue_row_proto::Row", tags = "3, 4, 5, 7, 8, 9, 10")]
    row: Option<task_queue_row_proto::Row>,
}

mod task_queue_row_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Row {
        #[prost(message, tag = "3")]
        Task(TaskEntryProto),
        #[prost(message, tag = "4")]
        Allocator(TaskAllocatorProto),
        #[prost(message, tag = "5")]
        Pending(TaskOrderProto),
        #[prost(message, tag = "7")]
        Running(RunningProjectionProto),
        #[prost(message, tag = "8")]
        Dedupe(LiveDedupeHeadProto),
        #[prost(message, tag = "9")]
        Group(TaskGroupHeadProto),
        #[prost(message, tag = "10")]
        Journal(TaskJournalEntryProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct TaskEntryProto {
    #[prost(message, optional, tag = "1")]
    task: Option<TaskRecordProto>,
    #[prost(string, optional, tag = "2")]
    dedupe_hash: Option<String>,
    #[prost(message, optional, tag = "3")]
    group: Option<TaskGroupIdentityProto>,
}

#[derive(Clone, PartialEq, Message)]
struct TaskAllocatorProto {
    #[prost(int64, tag = "1")]
    max_task_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct TaskOrderProto {
    #[prost(int32, tag = "1")]
    priority: i32,
    #[prost(int64, tag = "2")]
    scheduled_at_nanos: i64,
    #[prost(int64, tag = "3")]
    created_at_nanos: i64,
    #[prost(int64, tag = "4")]
    task_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct TaskGroupIdentityProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    hash: String,
    #[prost(bytes, tag = "3")]
    cursor_be: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RunningProjectionProto {
    #[prost(int64, tag = "1")]
    task_id: i64,
    #[prost(enumeration = "TaskTypeProto", tag = "2")]
    task_type: i32,
    #[prost(string, optional, tag = "3")]
    group_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct LiveDedupeHeadProto {
    #[prost(string, tag = "1")]
    dedupe_hash: String,
    #[prost(int64, tag = "2")]
    task_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct TaskGroupHeadProto {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(string, tag = "2")]
    group_hash: String,
    #[prost(int64, optional, tag = "3")]
    running_task_id: Option<i64>,
    #[prost(int64, optional, tag = "4")]
    pending_task_id: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct TaskJournalEntryProto {
    #[prost(int64, tag = "1")]
    task_id: i64,
    #[prost(string, tag = "2")]
    mutation_id: String,
    #[prost(uint32, tag = "3")]
    ordinal: u32,
    #[prost(uint64, tag = "4")]
    fence_token: u64,
    #[prost(bytes, tag = "5")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct TaskRecordProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(enumeration = "TaskTypeProto", tag = "2")]
    task_type: i32,
    #[prost(message, optional, tag = "3")]
    payload: Option<JsonValueProto>,
    #[prost(int32, tag = "4")]
    priority: i32,
    #[prost(enumeration = "TaskStatusProto", tag = "5")]
    status: i32,
    #[prost(int32, tag = "6")]
    attempts: i32,
    #[prost(string, optional, tag = "7")]
    last_error: Option<String>,
    #[prost(string, tag = "8")]
    scheduled_at: String,
    #[prost(string, tag = "9")]
    created_at: String,
    #[prost(string, tag = "10")]
    updated_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskTypeProto {
    Unspecified = 0,
    DeleteObject = 1,
    DeleteBucket = 2,
    ObjectMetadataCompaction = 3,
    IndexBuild = 4,
    RebalanceShard = 5,
    HfIngestion = 6,
    AuthzMaterialization = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskStatusProto {
    Unspecified = 0,
    Pending = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
}

#[derive(Clone, PartialEq, Message)]
struct JsonValueProto {
    #[prost(oneof = "json_value_proto::Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    kind: Option<json_value_proto::Kind>,
}

mod json_value_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(bool, tag = "1")]
        Null(bool),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(int64, tag = "3")]
        I64(i64),
        #[prost(uint64, tag = "4")]
        U64(u64),
        #[prost(double, tag = "5")]
        F64(f64),
        #[prost(string, tag = "6")]
        String(String),
        #[prost(message, tag = "7")]
        Array(JsonArrayProto),
        #[prost(message, tag = "8")]
        Object(JsonObjectProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct JsonArrayProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<JsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct JsonObjectProto {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<JsonObjectEntryProto>,
}

#[derive(Clone, PartialEq, Message)]
struct JsonObjectEntryProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<JsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct TaskAuditProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(enumeration = "TaskAuditKindProto", tag = "2")]
    event: i32,
    #[prost(message, optional, tag = "3")]
    task: Option<TaskRecordProto>,
    #[prost(int64, optional, tag = "4")]
    task_id: Option<i64>,
    #[prost(enumeration = "TaskStatusProto", optional, tag = "5")]
    status: Option<i32>,
    #[prost(string, optional, tag = "6")]
    error: Option<String>,
    #[prost(int32, optional, tag = "7")]
    attempts: Option<i32>,
    #[prost(string, optional, tag = "8")]
    scheduled_at: Option<String>,
    #[prost(string, optional, tag = "9")]
    updated_at: Option<String>,
    #[prost(uint64, tag = "10")]
    fence_token: u64,
    #[prost(string, tag = "11")]
    mutation_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskAuditKindProto {
    Unspecified = 0,
    Enqueued = 1,
    Claimed = 2,
    StatusUpdated = 3,
    Failed = 4,
}

impl TaskAuditProto {
    fn validated_task_id(&self) -> Result<i64> {
        match TaskAuditKindProto::try_from(self.event)
            .map_err(|_| anyhow!("task audit body has an invalid event kind"))?
        {
            TaskAuditKindProto::Enqueued => self
                .task
                .as_ref()
                .ok_or_else(|| anyhow!("enqueued task audit is missing its task"))
                .and_then(|task| require_task_id(task.id)),
            TaskAuditKindProto::Claimed
            | TaskAuditKindProto::StatusUpdated
            | TaskAuditKindProto::Failed => self
                .task_id
                .ok_or_else(|| anyhow!("task audit is missing its task id"))
                .and_then(require_task_id),
            TaskAuditKindProto::Unspecified => bail!("task audit event kind is unspecified"),
        }
    }
}

pub(super) fn encode_queue_row(row: &TaskQueueRow, created_at_unix_nanos: u64) -> Result<Vec<u8>> {
    let root_key_hash = core_meta_root_key_hash(&row_root_key(row));
    let row = match row {
        TaskQueueRow::Task(entry) => task_queue_row_proto::Row::Task(TaskEntryProto {
            task: Some(task_record_to_proto(&entry.task)?),
            dedupe_hash: entry.dedupe_hash.clone(),
            group: entry.group.as_ref().map(group_identity_to_proto),
        }),
        TaskQueueRow::Allocator(allocator) => {
            task_queue_row_proto::Row::Allocator(TaskAllocatorProto {
                max_task_id: allocator.max_task_id,
            })
        }
        TaskQueueRow::Pending(projection) => {
            task_queue_row_proto::Row::Pending(task_order_to_proto(&projection.order))
        }
        TaskQueueRow::Running(projection) => {
            task_queue_row_proto::Row::Running(RunningProjectionProto {
                task_id: projection.task_id,
                task_type: task_type_to_proto(projection.task_type) as i32,
                group_hash: projection.group_hash.clone(),
            })
        }
        TaskQueueRow::Dedupe(head) => task_queue_row_proto::Row::Dedupe(LiveDedupeHeadProto {
            dedupe_hash: head.dedupe_hash.clone(),
            task_id: head.task_id,
        }),
        TaskQueueRow::Group(head) => task_queue_row_proto::Row::Group(TaskGroupHeadProto {
            kind: head.kind.clone(),
            group_hash: head.group_hash.clone(),
            running_task_id: head.running_task_id,
            pending_task_id: head.pending_task_id,
        }),
        TaskQueueRow::Journal(entry) => task_queue_row_proto::Row::Journal(TaskJournalEntryProto {
            task_id: entry.task_id,
            mutation_id: entry.mutation_id.clone(),
            ordinal: entry.ordinal,
            fence_token: entry.fence_token,
            payload: entry.payload.clone(),
        }),
    };
    encode_deterministic(
        &TaskQueueRowProto {
            common: Some(core_meta_committed_row_common(
                TASK_QUEUE_REALM_ID,
                root_key_hash,
                TASK_QUEUE_CANDIDATE_GENERATION,
                TASK_QUEUE_CANDIDATE_TRANSACTION_ID,
                created_at_unix_nanos,
            )),
            schema: TASK_QUEUE_ROW_SCHEMA.to_string(),
            row: Some(row),
        },
        "task queue CoreMeta row",
    )
}

pub(super) fn decode_queue_row(bytes: &[u8]) -> Result<DecodedTaskQueueRow> {
    if bytes.len() > TASK_ROW_MAX_PROTO_BYTES {
        bail!("task queue CoreMeta row exceeds maximum size");
    }
    let proto = decode_deterministic::<TaskQueueRowProto>(bytes, "task queue CoreMeta row")?;
    if proto.schema != TASK_QUEUE_ROW_SCHEMA {
        bail!("task queue CoreMeta row has invalid schema");
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("task queue CoreMeta row is missing common metadata"))?;
    let row = match proto
        .row
        .ok_or_else(|| anyhow!("task queue CoreMeta row is missing row payload"))?
    {
        task_queue_row_proto::Row::Task(entry) => TaskQueueRow::Task(TaskEntry {
            task: task_record_from_proto(
                entry
                    .task
                    .ok_or_else(|| anyhow!("task queue task row is missing task"))?,
            )?,
            dedupe_hash: entry.dedupe_hash,
            group: entry.group.map(group_identity_from_proto).transpose()?,
        }),
        task_queue_row_proto::Row::Allocator(allocator) => {
            if allocator.max_task_id < 0 {
                bail!("task queue allocator is negative");
            }
            TaskQueueRow::Allocator(TaskAllocator {
                max_task_id: allocator.max_task_id,
            })
        }
        task_queue_row_proto::Row::Pending(order) => TaskQueueRow::Pending(PendingProjection {
            order: task_order_from_proto(order)?,
        }),
        task_queue_row_proto::Row::Running(projection) => {
            TaskQueueRow::Running(RunningProjection {
                task_id: require_task_id(projection.task_id)?,
                task_type: task_type_from_proto(projection.task_type)?,
                group_hash: projection.group_hash,
            })
        }
        task_queue_row_proto::Row::Dedupe(head) => TaskQueueRow::Dedupe(LiveDedupeHead {
            dedupe_hash: require_hash(head.dedupe_hash, "dedupe hash")?,
            task_id: require_task_id(head.task_id)?,
        }),
        task_queue_row_proto::Row::Group(head) => TaskQueueRow::Group(TaskGroupHead {
            kind: require_nonempty(head.kind, "task group kind")?,
            group_hash: require_hash(head.group_hash, "task group hash")?,
            running_task_id: head.running_task_id.map(require_task_id).transpose()?,
            pending_task_id: head.pending_task_id.map(require_task_id).transpose()?,
        }),
        task_queue_row_proto::Row::Journal(entry) => {
            let entry = TaskJournalEntry {
                task_id: require_task_id(entry.task_id)?,
                mutation_id: require_nonempty(entry.mutation_id, "task journal mutation id")?,
                ordinal: entry.ordinal,
                fence_token: entry.fence_token,
                payload: entry.payload,
            };
            let audit = decode_task_audit(&entry.payload)?;
            if audit.validated_task_id()? != entry.task_id
                || audit.mutation_id != entry.mutation_id
                || audit.fence_token != entry.fence_token
            {
                bail!("task journal row metadata does not match its audit payload");
            }
            TaskQueueRow::Journal(entry)
        }
    };
    if common.realm_id != TASK_QUEUE_REALM_ID
        || common.root_key_hash != core_meta_root_key_hash(&row_root_key(&row))
        || common.visibility_state != CoreMetaVisibilityState::Committed as i32
        || common.root_generation == 0
        || common.transaction_id.is_empty()
        || common.payload_schema_version != 1
    {
        bail!("task queue CoreMeta row has invalid common metadata");
    }
    Ok(DecodedTaskQueueRow {
        row,
        publication_generation: common.root_generation,
    })
}

pub(super) fn row_root_key(row: &TaskQueueRow) -> String {
    match row {
        TaskQueueRow::Task(entry) => format!("task_queue/task/{}", entry.task.id),
        TaskQueueRow::Allocator(_) => "task_queue/allocator".to_string(),
        TaskQueueRow::Pending(projection) => {
            format!("task_queue/task/{}", projection.order.task_id)
        }
        TaskQueueRow::Running(projection) => {
            format!("task_queue/task/{}", projection.task_id)
        }
        TaskQueueRow::Dedupe(head) => format!("task_queue/dedupe/{}", head.dedupe_hash),
        TaskQueueRow::Group(head) => {
            format!("task_queue/group/{}/{}", head.kind, head.group_hash)
        }
        TaskQueueRow::Journal(entry) => format!("task_queue/task/{}", entry.task_id),
    }
}

pub(super) fn encode_task_audit(
    event: &TaskAuditEvent,
    fence_token: u64,
    mutation_id: &str,
) -> Result<Vec<u8>> {
    let mut proto = TaskAuditProto {
        schema: TASK_AUDIT_SCHEMA.to_string(),
        event: TaskAuditKindProto::Unspecified as i32,
        task: None,
        task_id: None,
        status: None,
        error: None,
        attempts: None,
        scheduled_at: None,
        updated_at: None,
        fence_token,
        mutation_id: mutation_id.to_string(),
    };
    match event {
        TaskAuditEvent::Enqueued { task } => {
            proto.event = TaskAuditKindProto::Enqueued as i32;
            proto.task = Some(task_record_to_proto(task)?);
        }
        TaskAuditEvent::Claimed {
            task_id,
            updated_at,
        } => {
            proto.event = TaskAuditKindProto::Claimed as i32;
            proto.task_id = Some(*task_id);
            proto.updated_at = Some(updated_at.to_rfc3339());
        }
        TaskAuditEvent::StatusUpdated {
            task_id,
            status,
            updated_at,
        } => {
            proto.event = TaskAuditKindProto::StatusUpdated as i32;
            proto.task_id = Some(*task_id);
            proto.status = Some(task_status_to_proto(*status) as i32);
            proto.updated_at = Some(updated_at.to_rfc3339());
        }
        TaskAuditEvent::Failed {
            task_id,
            error,
            attempts,
            scheduled_at,
            updated_at,
        } => {
            proto.event = TaskAuditKindProto::Failed as i32;
            proto.task_id = Some(*task_id);
            proto.error = Some(error.clone());
            proto.attempts = Some(*attempts);
            proto.scheduled_at = Some(scheduled_at.to_rfc3339());
            proto.updated_at = Some(updated_at.to_rfc3339());
        }
    }
    encode_deterministic(&proto, "task audit body")
}

fn decode_task_audit(bytes: &[u8]) -> Result<TaskAuditProto> {
    let proto = decode_deterministic::<TaskAuditProto>(bytes, "task audit body")?;
    if proto.schema != TASK_AUDIT_SCHEMA {
        bail!("task audit body has invalid schema");
    }
    proto.validated_task_id()?;
    require_nonempty(proto.mutation_id.clone(), "task audit mutation id")?;
    Ok(proto)
}

#[cfg(test)]
pub(super) fn decode_task_audit_fence(bytes: &[u8]) -> Result<u64> {
    Ok(decode_task_audit(bytes)?.fence_token)
}

pub(super) fn allocator_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(ALLOCATOR_KEY)])
}

pub(super) fn current_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(CURRENT_PREFIX)])
}

pub(super) fn current_key(task_id: i64) -> Result<Vec<u8>> {
    require_task_id(task_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CURRENT_PREFIX),
        CoreMetaTuplePart::I64(task_id),
    ])
}

pub(super) fn pending_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(PENDING_PREFIX)])
}

pub(super) fn pending_key(order: &TaskOrder) -> Result<Vec<u8>> {
    require_task_id(order.task_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(PENDING_PREFIX),
        CoreMetaTuplePart::I64(order.scheduled_at_nanos),
        CoreMetaTuplePart::I64(i64::from(order.priority)),
        CoreMetaTuplePart::I64(order.created_at_nanos),
        CoreMetaTuplePart::I64(order.task_id),
    ])
}

pub(super) fn running_key(entry: &TaskEntry) -> Result<Vec<u8>> {
    require_task_id(entry.task.id)?;
    let group = entry
        .group
        .as_ref()
        .map(|group| group.hash.as_str())
        .unwrap_or("none");
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(RUNNING_PREFIX),
        CoreMetaTuplePart::Utf8(entry.task.task_type.as_str()),
        CoreMetaTuplePart::Utf8(group),
        CoreMetaTuplePart::I64(entry.task.id),
    ])
}

pub(super) fn dedupe_key(hash: &str) -> Result<Vec<u8>> {
    require_hash(hash.to_string(), "dedupe hash")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(DEDUPE_PREFIX),
        CoreMetaTuplePart::Hash(hash),
    ])
}

pub(super) fn group_key(group: &TaskGroupIdentity) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(GROUP_PREFIX),
        CoreMetaTuplePart::Utf8(&group.kind),
        CoreMetaTuplePart::Hash(&group.hash),
    ])
}

pub(super) fn journal_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(JOURNAL_PREFIX)])
}

pub(super) fn journal_key(task_id: i64, mutation_id: &str, ordinal: u32) -> Result<Vec<u8>> {
    require_task_id(task_id)?;
    require_nonempty(mutation_id.to_string(), "task journal mutation id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(JOURNAL_PREFIX),
        CoreMetaTuplePart::I64(task_id),
        CoreMetaTuplePart::Utf8(mutation_id),
        CoreMetaTuplePart::U64(u64::from(ordinal)),
    ])
}

pub(super) fn task_identity_hash(task_type: TaskType, payload: &JsonValue) -> Result<String> {
    let mut bytes = task_type.as_str().as_bytes().to_vec();
    bytes.push(0);
    bytes.extend(encode_deterministic(
        &json_value_to_proto(payload)?,
        "task identity payload",
    )?);
    Ok(hash_key(&bytes))
}

pub(super) fn task_group_identity(
    task_type: TaskType,
    payload: &JsonValue,
) -> Result<Option<TaskGroupIdentity>> {
    match task_type {
        TaskType::IndexBuild => {
            let tenant_id = json_i64(payload, "tenant_id")
                .ok_or_else(|| anyhow!("index build payload must include tenant_id"))?;
            let bucket_id = json_i64(payload, "bucket_id")
                .ok_or_else(|| anyhow!("index build payload must include bucket_id"))?;
            let index_id = json_i64(payload, "index_id")
                .ok_or_else(|| anyhow!("index build payload must include index_id"))?;
            let index_version = json_i64(payload, "index_version")
                .ok_or_else(|| anyhow!("index build payload must include index_version"))?;
            let cursor = json_u128(payload, "source_cursor").ok_or_else(|| {
                anyhow!("index build payload must include nonnegative source_cursor")
            })?;
            let identity =
                format!("index-build\0{tenant_id}\0{bucket_id}\0{index_id}\0{index_version}");
            Ok(Some(TaskGroupIdentity {
                kind: "index_build".to_string(),
                hash: hash_key(identity.as_bytes()),
                cursor,
            }))
        }
        TaskType::AuthzMaterialization => {
            let tenant_id = json_i64(payload, "tenant_id")
                .ok_or_else(|| anyhow!("authz materialization payload must include tenant_id"))?;
            let cursor = json_u128(payload, "target_revision").ok_or_else(|| {
                anyhow!("authz materialization payload must include nonnegative target_revision")
            })?;
            Ok(Some(TaskGroupIdentity {
                kind: "authz_materialization".to_string(),
                hash: hash_key(format!("authz-materialization\0{tenant_id}").as_bytes()),
                cursor,
            }))
        }
        _ => Ok(None),
    }
}

fn hash_key(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(hash32(bytes)))
}

fn group_identity_to_proto(group: &TaskGroupIdentity) -> TaskGroupIdentityProto {
    TaskGroupIdentityProto {
        kind: group.kind.clone(),
        hash: group.hash.clone(),
        cursor_be: group.cursor.to_be_bytes().to_vec(),
    }
}

fn group_identity_from_proto(proto: TaskGroupIdentityProto) -> Result<TaskGroupIdentity> {
    let cursor_be: [u8; 16] = proto
        .cursor_be
        .try_into()
        .map_err(|_| anyhow!("task group cursor must be 16 bytes"))?;
    Ok(TaskGroupIdentity {
        kind: require_nonempty(proto.kind, "task group kind")?,
        hash: require_hash(proto.hash, "task group hash")?,
        cursor: u128::from_be_bytes(cursor_be),
    })
}

fn task_order_to_proto(order: &TaskOrder) -> TaskOrderProto {
    TaskOrderProto {
        priority: order.priority,
        scheduled_at_nanos: order.scheduled_at_nanos,
        created_at_nanos: order.created_at_nanos,
        task_id: order.task_id,
    }
}

fn task_order_from_proto(proto: TaskOrderProto) -> Result<TaskOrder> {
    Ok(TaskOrder {
        scheduled_at_nanos: proto.scheduled_at_nanos,
        priority: proto.priority,
        created_at_nanos: proto.created_at_nanos,
        task_id: require_task_id(proto.task_id)?,
    })
}

fn task_record_to_proto(task: &TaskRecord) -> Result<TaskRecordProto> {
    Ok(TaskRecordProto {
        id: task.id,
        task_type: task_type_to_proto(task.task_type) as i32,
        payload: Some(json_value_to_proto(&task.payload)?),
        priority: task.priority,
        status: task_status_to_proto(task.status) as i32,
        attempts: task.attempts,
        last_error: task.last_error.clone(),
        scheduled_at: task.scheduled_at.to_rfc3339(),
        created_at: task.created_at.to_rfc3339(),
        updated_at: task.updated_at.to_rfc3339(),
    })
}

fn task_record_from_proto(proto: TaskRecordProto) -> Result<TaskRecord> {
    Ok(TaskRecord {
        id: require_task_id(proto.id)?,
        task_type: task_type_from_proto(proto.task_type)?,
        payload: json_value_from_proto(
            proto
                .payload
                .ok_or_else(|| anyhow!("task record is missing payload"))?,
        )?,
        priority: proto.priority,
        status: task_status_from_proto(proto.status)?,
        attempts: proto.attempts,
        last_error: proto.last_error,
        scheduled_at: parse_time(&proto.scheduled_at, "scheduled_at")?,
        created_at: parse_time(&proto.created_at, "created_at")?,
        updated_at: parse_time(&proto.updated_at, "updated_at")?,
    })
}

fn task_type_to_proto(value: TaskType) -> TaskTypeProto {
    match value {
        TaskType::DeleteObject => TaskTypeProto::DeleteObject,
        TaskType::DeleteBucket => TaskTypeProto::DeleteBucket,
        TaskType::ObjectMetadataCompaction => TaskTypeProto::ObjectMetadataCompaction,
        TaskType::IndexBuild => TaskTypeProto::IndexBuild,
        TaskType::RebalanceShard => TaskTypeProto::RebalanceShard,
        TaskType::HFIngestion => TaskTypeProto::HfIngestion,
        TaskType::AuthzMaterialization => TaskTypeProto::AuthzMaterialization,
    }
}

fn task_type_from_proto(value: i32) -> Result<TaskType> {
    Ok(
        match TaskTypeProto::try_from(value)
            .map_err(|_| anyhow!("task record has invalid task type"))?
        {
            TaskTypeProto::Unspecified => bail!("task record has unspecified task type"),
            TaskTypeProto::DeleteObject => TaskType::DeleteObject,
            TaskTypeProto::DeleteBucket => TaskType::DeleteBucket,
            TaskTypeProto::ObjectMetadataCompaction => TaskType::ObjectMetadataCompaction,
            TaskTypeProto::IndexBuild => TaskType::IndexBuild,
            TaskTypeProto::RebalanceShard => TaskType::RebalanceShard,
            TaskTypeProto::HfIngestion => TaskType::HFIngestion,
            TaskTypeProto::AuthzMaterialization => TaskType::AuthzMaterialization,
        },
    )
}

fn task_status_to_proto(value: TaskStatus) -> TaskStatusProto {
    match value {
        TaskStatus::Pending => TaskStatusProto::Pending,
        TaskStatus::Running => TaskStatusProto::Running,
        TaskStatus::Completed => TaskStatusProto::Completed,
        TaskStatus::Failed => TaskStatusProto::Failed,
    }
}

fn task_status_from_proto(value: i32) -> Result<TaskStatus> {
    Ok(
        match TaskStatusProto::try_from(value)
            .map_err(|_| anyhow!("task record has invalid task status"))?
        {
            TaskStatusProto::Unspecified => bail!("task record has unspecified task status"),
            TaskStatusProto::Pending => TaskStatus::Pending,
            TaskStatusProto::Running => TaskStatus::Running,
            TaskStatusProto::Completed => TaskStatus::Completed,
            TaskStatusProto::Failed => TaskStatus::Failed,
        },
    )
}

fn json_value_to_proto(value: &JsonValue) -> Result<JsonValueProto> {
    let kind = match value {
        JsonValue::Null => json_value_proto::Kind::Null(true),
        JsonValue::Bool(value) => json_value_proto::Kind::Bool(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                json_value_proto::Kind::I64(value)
            } else if let Some(value) = number.as_u64() {
                json_value_proto::Kind::U64(value)
            } else {
                json_value_proto::Kind::F64(number.as_f64().ok_or_else(|| {
                    anyhow!("task JSON number cannot be represented deterministically")
                })?)
            }
        }
        JsonValue::String(value) => json_value_proto::Kind::String(value.clone()),
        JsonValue::Array(values) => json_value_proto::Kind::Array(JsonArrayProto {
            values: values
                .iter()
                .map(json_value_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        JsonValue::Object(map) => {
            let mut entries = map
                .iter()
                .map(|(key, value)| {
                    Ok(JsonObjectEntryProto {
                        key: key.clone(),
                        value: Some(json_value_to_proto(value)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            json_value_proto::Kind::Object(JsonObjectProto { entries })
        }
    };
    Ok(JsonValueProto { kind: Some(kind) })
}

fn json_value_from_proto(proto: JsonValueProto) -> Result<JsonValue> {
    Ok(
        match proto
            .kind
            .ok_or_else(|| anyhow!("task JSON value is missing kind"))?
        {
            json_value_proto::Kind::Null(_) => JsonValue::Null,
            json_value_proto::Kind::Bool(value) => JsonValue::Bool(value),
            json_value_proto::Kind::I64(value) => JsonValue::Number(value.into()),
            json_value_proto::Kind::U64(value) => JsonValue::Number(value.into()),
            json_value_proto::Kind::F64(value) => JsonValue::Number(
                serde_json::Number::from_f64(value)
                    .ok_or_else(|| anyhow!("task JSON f64 is not finite"))?,
            ),
            json_value_proto::Kind::String(value) => JsonValue::String(value),
            json_value_proto::Kind::Array(array) => JsonValue::Array(
                array
                    .values
                    .into_iter()
                    .map(json_value_from_proto)
                    .collect::<Result<Vec<_>>>()?,
            ),
            json_value_proto::Kind::Object(object) => {
                let mut map = serde_json::Map::new();
                for entry in object.entries {
                    map.insert(
                        entry.key,
                        json_value_from_proto(
                            entry
                                .value
                                .ok_or_else(|| anyhow!("task JSON object value is missing"))?,
                        )?,
                    );
                }
                JsonValue::Object(map)
            }
        },
    )
}

fn encode_deterministic<M>(message: &M, label: &str) -> Result<Vec<u8>>
where
    M: Message + Default,
{
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    if bytes.len() > TASK_ROW_MAX_PROTO_BYTES {
        bail!("{label} exceeds maximum size");
    }
    let decoded = M::decode(bytes.as_slice())?;
    let mut canonical = Vec::with_capacity(decoded.encoded_len());
    decoded.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministic protobuf");
    }
    Ok(bytes)
}

fn decode_deterministic<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let value = M::decode(bytes)?;
    let mut canonical = Vec::with_capacity(value.encoded_len());
    value.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministic protobuf");
    }
    Ok(value)
}

fn timestamp_nanos(value: DateTime<Utc>, field: &str) -> Result<i64> {
    value
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("task timestamp {field} cannot be represented in nanoseconds"))
}

fn parse_time(value: &str, field: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("parse task timestamp {field}"))?
        .with_timezone(&Utc))
}

fn require_task_id(task_id: i64) -> Result<i64> {
    if task_id <= 0 {
        bail!("task id must be positive");
    }
    Ok(task_id)
}

fn require_nonempty(value: String, field: &str) -> Result<String> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(value)
}

fn require_hash(value: String, field: &str) -> Result<String> {
    let Some((algorithm, hex_value)) = value.split_once(':') else {
        bail!("{field} must be algorithm:hex");
    };
    if algorithm != "sha256"
        || hex_value.len() != 64
        || !hex_value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        bail!("{field} must be sha256:hex32");
    }
    Ok(value)
}

fn json_i64(payload: &JsonValue, field: &str) -> Option<i64> {
    payload.get(field)?.as_i64()
}

fn json_u128(payload: &JsonValue, field: &str) -> Option<u128> {
    if let Some(value) = payload.get(field)?.as_u64() {
        return Some(u128::from(value));
    }
    payload.get(field)?.as_str()?.parse().ok()
}
