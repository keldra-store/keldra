use super::{
    AUTHZ_TUPLE_BATCH_RECORD_KIND, AUTHZ_TUPLE_RECORD_KIND, authz_tuple_stream_id,
    decode_authz_tuple_batch_journal_body, decode_authz_tuple_batch_journal_body_fence,
    decode_authz_tuple_journal_body, decode_authz_tuple_journal_body_fence, latest_authz_revision,
};
use crate::{
    authz_head,
    authz_segment::{self, DecodedAuthzSegment},
    authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID,
    core_store::{CoreMutationPrecondition, CoreStore, ReadStream, StreamRecord},
    persistence::AuthzTupleRecord,
    storage::Storage,
    task_execution_guard::TaskExecutionGuard,
};
use anyhow::{Context, Result, anyhow, bail};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock, Weak},
};

const AUTHZ_INCREMENTAL_SOURCE_PAGE_SIZE: usize = 1;
const AUTHZ_INCREMENTAL_SOURCE_SCAN_LIMIT: usize = 256;
const AUTHZ_REBUILD_SOURCE_PAGE_SIZE: usize = 1_000;

static AUTHZ_MATERIALIZATION_LOCKS: LazyLock<
    std::sync::Mutex<BTreeMap<i64, Weak<tokio::sync::Mutex<()>>>>,
> = LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthzMaterializationOutcome {
    pub processed_revision: u64,
    pub source_cursor: u64,
    pub source_record_count: u64,
    pub source_records_hash: String,
    pub generation: u64,
    pub segment_ref: String,
    pub materialized_at: String,
    pub source_rows_visited: usize,
}

#[derive(Clone, Copy)]
enum AuthzPublication<'a> {
    Direct,
    Task {
        guard: &'a TaskExecutionGuard,
        source_partition_precondition: &'a CoreMutationPrecondition,
    },
}

struct AuthzSourceEvent {
    source_cursor: u64,
    revision: u64,
    records: Vec<AuthzTupleRecord>,
    fence_token: u64,
}

struct IncrementalSourceRead {
    event: Option<AuthzSourceEvent>,
    cursor_before_event: u64,
    scanned_cursor: u64,
    source_rows_visited: usize,
}

struct RebuildSource {
    records: Vec<AuthzTupleRecord>,
    source_cursor: u64,
    latest_fence_token: u64,
    events_visited: usize,
}

pub(crate) async fn materialize_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
    source_fence_token: u64,
) -> Result<String> {
    let target_revision = u64::try_from(latest_authz_revision(storage, tenant_id).await?)
        .context("authorization revision must be nonnegative")?;
    Ok(materialize_authz_state_at_revision(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
        AuthzPublication::Direct,
    )
    .await?
    .segment_ref)
}

pub(crate) async fn materialize_authz_tuple_segment_at_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<String> {
    Ok(materialize_authz_state_at_revision(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
        AuthzPublication::Direct,
    )
    .await?
    .segment_ref)
}

pub(crate) async fn materialize_authz_derived_state_at_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    materialize_authz_state_at_revision(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
        AuthzPublication::Direct,
    )
    .await
}

pub(crate) async fn materialize_authz_derived_state_through_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    let mut previous_revision = None;
    let mut step_target = if authz_segment::latest_authz_tuple_segment_record(storage, tenant_id)
        .await?
        .is_none()
    {
        1
    } else {
        target_revision
    };

    loop {
        let outcome = materialize_authz_state_at_revision(
            storage,
            tenant_id,
            step_target,
            source_fence_token,
            AuthzPublication::Direct,
        )
        .await?;
        if outcome.processed_revision >= target_revision {
            return Ok(outcome);
        }
        if previous_revision == Some(outcome.processed_revision) {
            bail!(
                "authorization materialization made no progress before revision {target_revision}"
            );
        }
        previous_revision = Some(outcome.processed_revision);
        step_target = target_revision;
    }
}

impl AuthzMaterializationOutcome {
    pub(crate) async fn materialize_for_task_at_revision(
        storage: &Storage,
        tenant_id: i64,
        target_revision: u64,
        source_fence_token: u64,
        guard: &TaskExecutionGuard,
        source_partition_precondition: &CoreMutationPrecondition,
    ) -> Result<Self> {
        materialize_authz_state_at_revision(
            storage,
            tenant_id,
            target_revision,
            source_fence_token,
            AuthzPublication::Task {
                guard,
                source_partition_precondition,
            },
        )
        .await
    }
}

fn materialize_authz_state_at_revision<'a>(
    storage: &'a Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    publication: AuthzPublication<'a>,
) -> Pin<Box<dyn Future<Output = Result<AuthzMaterializationOutcome>> + Send + 'a>> {
    Box::pin(materialize_authz_state_at_revision_inner(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
        publication,
    ))
}

async fn materialize_authz_state_at_revision_inner(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    publication: AuthzPublication<'_>,
) -> Result<AuthzMaterializationOutcome> {
    validate_target_revision(storage, tenant_id, target_revision).await?;
    let lock = materialization_lock(tenant_id)?;
    let _guard = lock.lock().await;

    let Some(head) = authz_segment::latest_authz_tuple_segment_record(storage, tenant_id).await?
    else {
        return initialize_authz_materialization(
            storage,
            tenant_id,
            target_revision,
            source_fence_token,
            publication,
        )
        .await;
    };
    if head.generation >= target_revision {
        let segment_ref =
            authz_segment::existing_authz_tuple_segment_ref(storage, tenant_id, target_revision)
                .await?
                .ok_or_else(|| {
                    anyhow!("AuthzRevisionUnavailable: materialized segment is missing")
                })?;
        let segment = load_materialized_segment(storage, tenant_id, target_revision).await?;
        return outcome_from_segment(segment, segment_ref, 0);
    }

    let next_revision = head
        .generation
        .checked_add(1)
        .ok_or_else(|| anyhow!("authorization materialization revision overflow"))?;
    let previous = load_materialized_segment(storage, tenant_id, head.generation).await?;
    let IncrementalSourceRead {
        event: next_event,
        cursor_before_event,
        scanned_cursor,
        source_rows_visited,
    } = read_next_source_event(storage, tenant_id, head.source_cursor).await?;
    let (mutations, source_cursor, event_fence_token) = match next_event {
        Some(event) if event.revision == next_revision => {
            (event.records, event.source_cursor, event.fence_token)
        }
        Some(event) if event.revision > next_revision => (Vec::new(), cursor_before_event, 0),
        Some(event) => bail!(
            "authorization source cursor is ahead of materialization: source revision {}, next materialization revision {next_revision}",
            event.revision
        ),
        None => (Vec::new(), scanned_cursor, 0),
    };
    require_available_revision_source(storage, tenant_id, next_revision, &mutations).await?;
    let effective_fence = event_fence_token.max(source_fence_token);

    let staged = if authz_segment::authz_tuple_segment_requires_checkpoint(
        storage,
        tenant_id,
        next_revision,
    )
    .await?
    {
        let active = authz_segment::apply_authz_tuple_mutations(
            tenant_id,
            &previous.records,
            &mutations,
            next_revision,
        )?;
        authz_segment::stage_authz_tuple_checkpoint_segment(
            storage,
            tenant_id,
            &active,
            Some(&previous),
            next_revision,
            source_cursor,
            effective_fence,
        )
        .await?
    } else {
        authz_segment::stage_authz_tuple_delta_segment(
            storage,
            tenant_id,
            &previous,
            &mutations,
            next_revision,
            source_cursor,
            effective_fence,
        )
        .await?
    };
    let segment_ref = publish_staged_segment(storage, staged, publication).await?;
    let segment = load_materialized_segment(storage, tenant_id, next_revision).await?;
    outcome_from_segment(segment, segment_ref, source_rows_visited)
}

async fn initialize_authz_materialization(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    publication: AuthzPublication<'_>,
) -> Result<AuthzMaterializationOutcome> {
    let current_revision = u64::try_from(latest_authz_revision(storage, tenant_id).await?)
        .context("authorization revision must be nonnegative")?;
    if target_revision != 1 || current_revision != 1 {
        bail!(
            "AuthzMaterializationRepairRequired: no durable materialization head exists for current revision {current_revision}"
        );
    }
    let IncrementalSourceRead {
        event,
        cursor_before_event,
        scanned_cursor,
        source_rows_visited,
    } = read_next_source_event(storage, tenant_id, 0).await?;
    let (mutations, source_cursor, event_fence_token) = match event {
        Some(event) if event.revision == 1 => {
            (event.records, event.source_cursor, event.fence_token)
        }
        Some(event) if event.revision > 1 => (Vec::new(), cursor_before_event, 0),
        Some(event) => bail!(
            "authorization source starts before the initial materialization revision: {}",
            event.revision
        ),
        None => (Vec::new(), scanned_cursor, 0),
    };
    require_available_revision_source(storage, tenant_id, 1, &mutations).await?;
    let active = authz_segment::apply_authz_tuple_mutations(tenant_id, &[], &mutations, 1)?;
    let staged = authz_segment::stage_authz_tuple_checkpoint_segment(
        storage,
        tenant_id,
        &active,
        None,
        1,
        source_cursor,
        event_fence_token.max(source_fence_token),
    )
    .await?;
    let segment_ref = publish_staged_segment(storage, staged, publication).await?;
    let segment = load_materialized_segment(storage, tenant_id, 1).await?;
    outcome_from_segment(segment, segment_ref, source_rows_visited)
}

async fn publish_staged_segment(
    storage: &Storage,
    staged: authz_segment::StagedAuthzTupleSegment,
    publication: AuthzPublication<'_>,
) -> Result<String> {
    match publication {
        AuthzPublication::Direct => {
            authz_segment::publish_staged_authz_tuple_segment(storage, staged, &[]).await
        }
        AuthzPublication::Task {
            guard,
            source_partition_precondition,
        } => {
            let source_partition_precondition = source_partition_precondition.clone();
            guard
                .publication_permit()
                .await?
                .publish_with(move |task_lease_precondition| async move {
                    let preconditions = [source_partition_precondition, task_lease_precondition];
                    authz_segment::publish_staged_authz_tuple_segment(
                        storage,
                        staged,
                        &preconditions,
                    )
                    .await
                })
                .await
        }
    }
}

pub(crate) async fn rebuild_authz_materialization_at_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    validate_target_revision(storage, tenant_id, target_revision).await?;
    let lock = materialization_lock(tenant_id)?;
    let _guard = lock.lock().await;
    let source = collect_source_records_for_rebuild(storage, tenant_id, target_revision).await?;
    let active = active_records_at_revision(source.records, target_revision);
    let derived = crate::authz_userset_index::build_expected_derived_userset_index_at_revision(
        storage,
        tenant_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID,
        target_revision,
    )
    .await?;
    crate::authz_userset_index::write_derived_userset_index(storage, &derived).await?;
    let segment_ref = authz_segment::write_authz_tuple_checkpoint_segment(
        storage,
        tenant_id,
        &active,
        None,
        target_revision,
        source.source_cursor,
        source.latest_fence_token.max(source_fence_token),
    )
    .await?;
    let segment = load_materialized_segment(storage, tenant_id, target_revision).await?;
    outcome_from_segment(segment, segment_ref, source.events_visited)
}

pub(super) async fn collect_authz_tuple_records_for_rebuild(
    storage: &Storage,
    tenant_id: i64,
    through_revision: Option<u64>,
) -> Result<Vec<AuthzTupleRecord>> {
    let through_revision = match through_revision {
        Some(revision) => revision,
        None => u64::try_from(latest_authz_revision(storage, tenant_id).await?)
            .context("authorization revision must be nonnegative")?,
    };
    Ok(
        collect_source_records_for_rebuild(storage, tenant_id, through_revision)
            .await?
            .records,
    )
}

async fn read_next_source_event(
    storage: &Storage,
    tenant_id: i64,
    after_source_cursor: u64,
) -> Result<IncrementalSourceRead> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut scanned_cursor = after_source_cursor;
    let mut source_rows_visited = 0_usize;
    for _ in 0..AUTHZ_INCREMENTAL_SOURCE_SCAN_LIMIT {
        let page_start_cursor = scanned_cursor;
        let page = core_store
            .read_stream_page(ReadStream {
                stream_id: authz_tuple_stream_id(tenant_id),
                after_sequence: page_start_cursor,
                limit: AUTHZ_INCREMENTAL_SOURCE_PAGE_SIZE,
            })
            .await?;
        if page.records.len() > AUTHZ_INCREMENTAL_SOURCE_PAGE_SIZE {
            bail!("authorization source page exceeded its requested bound");
        }
        if let Some(record) = page.records.into_iter().next() {
            if record.sequence <= scanned_cursor {
                bail!("authorization source event did not advance its continuation");
            }
            // Stream sequences are continuation tokens and may be physically sparse.
            source_rows_visited = source_rows_visited
                .checked_add(1)
                .ok_or_else(|| anyhow!("authorization source work count overflow"))?;
            require_incremental_source_work_bound(source_rows_visited)?;
            let cursor_before_event = scanned_cursor;
            let event = decode_source_event(tenant_id, record)
                .context("decode next authorization source event")?;
            return Ok(IncrementalSourceRead {
                scanned_cursor: event.source_cursor,
                event: Some(event),
                cursor_before_event,
                source_rows_visited,
            });
        }

        if page.next_sequence < page_start_cursor {
            bail!("authorization source page moved its continuation backwards");
        }
        if page.next_sequence > page_start_cursor {
            // An invisible source row is still one bounded unit of work.
            source_rows_visited = source_rows_visited
                .checked_add(1)
                .ok_or_else(|| anyhow!("authorization source work count overflow"))?;
            require_incremental_source_work_bound(source_rows_visited)?;
            scanned_cursor = page.next_sequence;
        }
        if page.has_more && page.next_sequence == page_start_cursor {
            bail!(
                "AuthzMaterializationSourcePending: authorization source cannot advance past a pending row"
            );
        }
        if !page.has_more {
            return Ok(IncrementalSourceRead {
                event: None,
                cursor_before_event: scanned_cursor,
                scanned_cursor,
                source_rows_visited,
            });
        }
    }
    bail!(
        "AuthzMaterializationSourceWindowExceeded: no visible authorization event within {AUTHZ_INCREMENTAL_SOURCE_SCAN_LIMIT} source rows"
    )
}

fn require_incremental_source_work_bound(source_rows_visited: usize) -> Result<()> {
    if source_rows_visited > AUTHZ_INCREMENTAL_SOURCE_SCAN_LIMIT {
        bail!(
            "AuthzMaterializationSourceWindowExceeded: source read crossed more than {AUTHZ_INCREMENTAL_SOURCE_SCAN_LIMIT} rows"
        );
    }
    Ok(())
}

async fn collect_source_records_for_rebuild(
    storage: &Storage,
    tenant_id: i64,
    through_revision: u64,
) -> Result<RebuildSource> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut records = Vec::new();
    let mut source_cursor = 0_u64;
    let mut latest_fence_token = 0_u64;
    let mut events_visited = 0_usize;
    loop {
        let previous_cursor = source_cursor;
        let page = core_store
            .read_stream_page(ReadStream {
                stream_id: authz_tuple_stream_id(tenant_id),
                after_sequence: previous_cursor,
                limit: AUTHZ_REBUILD_SOURCE_PAGE_SIZE,
            })
            .await?;
        let page_next_sequence = page.next_sequence;
        let page_has_more = page.has_more;
        let mut reached_later_revision = false;
        for record in page.records {
            events_visited = events_visited
                .checked_add(1)
                .ok_or_else(|| anyhow!("authorization rebuild event count overflow"))?;
            let event = decode_source_event(tenant_id, record)?;
            if event.revision > through_revision {
                reached_later_revision = true;
                break;
            }
            source_cursor = event.source_cursor;
            latest_fence_token = event.fence_token;
            records.extend(event.records);
        }
        if reached_later_revision {
            break;
        }
        if page_next_sequence < source_cursor {
            bail!("authorization rebuild source page moved its continuation backwards");
        }
        source_cursor = page_next_sequence;
        if !page_has_more {
            break;
        }
        if source_cursor <= previous_cursor {
            bail!("authorization rebuild source page did not advance its continuation");
        }
    }
    Ok(RebuildSource {
        records,
        source_cursor,
        latest_fence_token,
        events_visited,
    })
}

fn decode_source_event(tenant_id: i64, record: StreamRecord) -> Result<AuthzSourceEvent> {
    let (records, fence_token) = match record.record_kind.as_str() {
        AUTHZ_TUPLE_RECORD_KIND => (
            vec![decode_authz_tuple_journal_body(&record.payload)?],
            decode_authz_tuple_journal_body_fence(&record.payload)?,
        ),
        AUTHZ_TUPLE_BATCH_RECORD_KIND => (
            decode_authz_tuple_batch_journal_body(&record.payload)?,
            decode_authz_tuple_batch_journal_body_fence(&record.payload)?,
        ),
        _ => bail!("authorization tuple stream record kind mismatch"),
    };
    let revision = records
        .first()
        .ok_or_else(|| anyhow!("authorization source event has no tuple records"))?
        .revision;
    if revision <= 0
        || records
            .iter()
            .any(|item| item.tenant_id != tenant_id || item.revision != revision)
    {
        bail!("authorization source event scope mismatch");
    }
    Ok(AuthzSourceEvent {
        source_cursor: record.sequence,
        revision: u64::try_from(revision)?,
        records,
        fence_token,
    })
}

fn active_records_at_revision(
    mut records: Vec<AuthzTupleRecord>,
    target_revision: u64,
) -> Vec<AuthzTupleRecord> {
    records.retain(|record| u64::try_from(record.revision).is_ok_and(|r| r <= target_revision));
    authz_segment::active_authz_tuple_records(&records)
}

async fn require_available_revision_source(
    storage: &Storage,
    tenant_id: i64,
    revision: u64,
    mutations: &[AuthzTupleRecord],
) -> Result<()> {
    let head = authz_head::read(storage, tenant_id).await?.head;
    if mutations.is_empty() && head.schema_revision != revision {
        if head.tuple_revision >= revision {
            bail!("AuthzRevisionUnavailable: tuple source event is missing");
        }
        bail!(
            "AuthzMaterializationRepairRequired: revision {revision} cannot be identified from the durable materialization head"
        );
    }
    Ok(())
}

async fn validate_target_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
) -> Result<()> {
    if target_revision == 0 {
        bail!("authorization materialization target revision must be nonzero");
    }
    let current_revision = u64::try_from(latest_authz_revision(storage, tenant_id).await?)?;
    if target_revision > current_revision {
        bail!(
            "AuthzRevisionUnavailable: current authorization revision is {current_revision}, requested {target_revision}"
        );
    }
    Ok(())
}

async fn load_materialized_segment(
    storage: &Storage,
    tenant_id: i64,
    revision: u64,
) -> Result<DecodedAuthzSegment> {
    authz_segment::read_authz_tuple_segment_at_revision(storage, tenant_id, revision)
        .await?
        .ok_or_else(|| anyhow!("AuthzRevisionUnavailable: materialized segment is missing"))
}

fn outcome_from_segment(
    segment: DecodedAuthzSegment,
    segment_ref: String,
    source_rows_visited: usize,
) -> Result<AuthzMaterializationOutcome> {
    let checkpoint = segment
        .revision_checkpoints
        .last()
        .ok_or_else(|| anyhow!("authorization materialized segment is missing its checkpoint"))?;
    if checkpoint.revision != segment.header.generation {
        bail!("authorization materialized segment checkpoint revision mismatch");
    }
    Ok(AuthzMaterializationOutcome {
        processed_revision: segment.header.generation,
        source_cursor: segment.header.source_stream_cursor,
        source_record_count: checkpoint.tuple_record_count,
        source_records_hash: checkpoint.tuple_records_hash.clone(),
        generation: segment.header.generation,
        segment_ref,
        materialized_at: segment.header.created_at.clone(),
        source_rows_visited,
    })
}

fn materialization_lock(tenant_id: i64) -> Result<Arc<tokio::sync::Mutex<()>>> {
    let mut locks = AUTHZ_MATERIALIZATION_LOCKS
        .lock()
        .map_err(|_| anyhow!("authorization materialization lock is poisoned"))?;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&tenant_id).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(tenant_id, Arc::downgrade(&lock));
    Ok(lock)
}
