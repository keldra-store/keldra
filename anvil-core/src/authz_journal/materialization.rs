use super::{
    AUTHZ_TUPLE_JOURNAL_ROW_KIND, decode_authz_tuple_batch_journal_body,
    decode_authz_tuple_batch_journal_body_fence, latest_authz_revision,
};
use crate::{
    authz_head,
    authz_segment::{self, DecodedAuthzSegment},
    authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID,
    core_store::{CF_AUTHZ, CoreMetaTuplePart, TABLE_AUTHZ_TUPLE_JOURNAL_ROW, core_meta_tuple_key},
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
        source_head_predicate: &'a (
            anvil_mvcc_consensus::LogicalKey,
            anvil_mvcc_consensus::PredicateKind,
        ),
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    source_fence_token: u64,
) -> Result<String> {
    let target_revision = u64::try_from(latest_authz_revision(mvcc, tenant_id)?)
        .context("authorization revision must be nonnegative")?;
    Ok(materialize_authz_state_at_revision(
        storage,
        mvcc,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<String> {
    Ok(materialize_authz_state_at_revision(
        storage,
        mvcc,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    materialize_authz_state_at_revision(
        storage,
        mvcc,
        tenant_id,
        target_revision,
        source_fence_token,
        AuthzPublication::Direct,
    )
    .await
}

pub(crate) async fn materialize_authz_derived_state_through_revision(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    let mut previous_revision = None;
    let mut step_target = if authz_segment::latest_authz_tuple_segment_record(mvcc, tenant_id)
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
            mvcc,
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
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        tenant_id: i64,
        target_revision: u64,
        source_fence_token: u64,
        guard: &TaskExecutionGuard,
        source_head_predicate: &(
            anvil_mvcc_consensus::LogicalKey,
            anvil_mvcc_consensus::PredicateKind,
        ),
    ) -> Result<Self> {
        materialize_authz_state_at_revision(
            storage,
            mvcc,
            tenant_id,
            target_revision,
            source_fence_token,
            AuthzPublication::Task {
                guard,
                source_head_predicate,
            },
        )
        .await
    }
}

fn materialize_authz_state_at_revision<'a>(
    storage: &'a Storage,
    mvcc: &'a crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    publication: AuthzPublication<'a>,
) -> Pin<Box<dyn Future<Output = Result<AuthzMaterializationOutcome>> + Send + 'a>> {
    Box::pin(materialize_authz_state_at_revision_inner(
        storage,
        mvcc,
        tenant_id,
        target_revision,
        source_fence_token,
        publication,
    ))
}

async fn materialize_authz_state_at_revision_inner(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    publication: AuthzPublication<'_>,
) -> Result<AuthzMaterializationOutcome> {
    validate_target_revision(mvcc, tenant_id, target_revision)?;
    let lock = materialization_lock(tenant_id)?;
    let _guard = lock.lock().await;

    let Some(head) = authz_segment::latest_authz_tuple_segment_record(mvcc, tenant_id).await?
    else {
        return initialize_authz_materialization(
            storage,
            mvcc,
            tenant_id,
            target_revision,
            source_fence_token,
            publication,
        )
        .await;
    };
    if head.generation >= target_revision {
        let segment_ref =
            authz_segment::existing_authz_tuple_segment_ref(mvcc, tenant_id, target_revision)
                .await?
                .ok_or_else(|| {
                    anyhow!("AuthzRevisionUnavailable: materialized segment is missing")
                })?;
        let segment = load_materialized_segment(storage, mvcc, tenant_id, target_revision).await?;
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
    } = read_next_source_event(mvcc, tenant_id, head.source_cursor)?;
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
    require_available_revision_source(mvcc, tenant_id, next_revision, &mutations)?;
    let effective_fence = event_fence_token.max(source_fence_token);

    let staged =
        if authz_segment::authz_tuple_segment_requires_checkpoint(mvcc, tenant_id, next_revision)
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
                mvcc,
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
                mvcc,
                tenant_id,
                &previous,
                &mutations,
                next_revision,
                source_cursor,
                effective_fence,
            )
            .await?
        };
    publish_derived_userset_index(storage, mvcc, tenant_id, next_revision, publication).await?;
    let segment_ref = publish_staged_segment(mvcc, staged, publication).await?;
    let segment = load_materialized_segment(storage, mvcc, tenant_id, next_revision).await?;
    outcome_from_segment(segment, segment_ref, source_rows_visited)
}

async fn initialize_authz_materialization(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    publication: AuthzPublication<'_>,
) -> Result<AuthzMaterializationOutcome> {
    let current_revision = u64::try_from(latest_authz_revision(mvcc, tenant_id)?)
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
    } = read_next_source_event(mvcc, tenant_id, 0)?;
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
    require_available_revision_source(mvcc, tenant_id, 1, &mutations)?;
    let active = authz_segment::apply_authz_tuple_mutations(tenant_id, &[], &mutations, 1)?;
    let staged = authz_segment::stage_authz_tuple_checkpoint_segment(
        storage,
        mvcc,
        tenant_id,
        &active,
        None,
        1,
        source_cursor,
        event_fence_token.max(source_fence_token),
    )
    .await?;
    publish_derived_userset_index(storage, mvcc, tenant_id, 1, publication).await?;
    let segment_ref = publish_staged_segment(mvcc, staged, publication).await?;
    let segment = load_materialized_segment(storage, mvcc, tenant_id, 1).await?;
    outcome_from_segment(segment, segment_ref, source_rows_visited)
}

async fn publish_staged_segment(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    staged: authz_segment::StagedAuthzTupleSegment,
    publication: AuthzPublication<'_>,
) -> Result<String> {
    match publication {
        AuthzPublication::Direct => {
            authz_segment::publish_staged_authz_tuple_segment(mvcc, staged, &[]).await
        }
        AuthzPublication::Task {
            guard,
            source_head_predicate,
        } => {
            let source_head_predicate = source_head_predicate.clone();
            guard
                .publish_mvcc_with(move |task_lease_predicate| async move {
                    let preconditions = [source_head_predicate, task_lease_predicate];
                    authz_segment::publish_staged_authz_tuple_segment(mvcc, staged, &preconditions)
                        .await
                })
                .await
        }
    }
}

async fn publish_derived_userset_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    publication: AuthzPublication<'_>,
) -> Result<()> {
    let derived = crate::authz_userset_index::build_expected_derived_userset_index_at_revision(
        storage,
        mvcc,
        tenant_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID,
        target_revision,
    )
    .await?;
    match publication {
        AuthzPublication::Direct => {
            crate::authz_userset_index::write_derived_userset_index(storage, mvcc, &derived).await
        }
        AuthzPublication::Task {
            guard,
            source_head_predicate,
        } => {
            let source_head_predicate = source_head_predicate.clone();
            guard
                .publish_mvcc_with(move |task_lease_predicate| async move {
                    let preconditions = [source_head_predicate, task_lease_predicate];
                    crate::authz_userset_index::write_derived_userset_index_with_predicates(
                        storage,
                        mvcc,
                        &derived,
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
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    validate_target_revision(mvcc, tenant_id, target_revision)?;
    let lock = materialization_lock(tenant_id)?;
    let _guard = lock.lock().await;
    let source = collect_source_records_for_rebuild(mvcc, tenant_id, target_revision)?;
    let active = active_records_at_revision(source.records, target_revision);
    let derived = crate::authz_userset_index::build_expected_derived_userset_index_at_revision(
        storage,
        mvcc,
        tenant_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID,
        target_revision,
    )
    .await?;
    crate::authz_userset_index::write_derived_userset_index(storage, mvcc, &derived).await?;
    let segment_ref = authz_segment::write_authz_tuple_checkpoint_segment(
        storage,
        mvcc,
        tenant_id,
        &active,
        None,
        target_revision,
        source.source_cursor,
        source.latest_fence_token.max(source_fence_token),
    )
    .await?;
    let segment = load_materialized_segment(storage, mvcc, tenant_id, target_revision).await?;
    outcome_from_segment(segment, segment_ref, source.events_visited)
}

pub(super) async fn collect_authz_tuple_records_for_rebuild(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    through_revision: Option<u64>,
) -> Result<Vec<AuthzTupleRecord>> {
    let through_revision = match through_revision {
        Some(revision) => revision,
        None => u64::try_from(latest_authz_revision(mvcc, tenant_id)?)
            .context("authorization revision must be nonnegative")?,
    };
    Ok(collect_source_records_for_rebuild(mvcc, tenant_id, through_revision)?.records)
}

fn read_next_source_event(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    after_source_cursor: u64,
) -> Result<IncrementalSourceRead> {
    let snapshot_version = mvcc.runtime.applied_version()?;
    let mut events =
        scan_source_events_at(mvcc, snapshot_version, tenant_id, after_source_cursor, 1)?;
    let event = events.pop();
    let scanned_cursor = event
        .as_ref()
        .map_or(after_source_cursor, |event| event.source_cursor);
    Ok(IncrementalSourceRead {
        event,
        cursor_before_event: after_source_cursor,
        scanned_cursor,
        source_rows_visited: usize::from(scanned_cursor > after_source_cursor),
    })
}

fn collect_source_records_for_rebuild(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    through_revision: u64,
) -> Result<RebuildSource> {
    let snapshot_version = mvcc.runtime.applied_version()?;
    let mut records = Vec::new();
    let mut source_cursor = 0_u64;
    let mut latest_fence_token = 0_u64;
    let mut events_visited = 0_usize;
    loop {
        let events = scan_source_events_at(
            mvcc,
            snapshot_version,
            tenant_id,
            source_cursor,
            AUTHZ_REBUILD_SOURCE_PAGE_SIZE,
        )?;
        if events.is_empty() {
            break;
        }
        let event_count = events.len();
        for event in events {
            events_visited = events_visited
                .checked_add(1)
                .ok_or_else(|| anyhow!("authorization rebuild event count overflow"))?;
            if event.revision > through_revision {
                return Ok(RebuildSource {
                    records,
                    source_cursor,
                    latest_fence_token,
                    events_visited,
                });
            }
            source_cursor = event.source_cursor;
            latest_fence_token = event.fence_token;
            records.extend(event.records);
        }
        if event_count < AUTHZ_REBUILD_SOURCE_PAGE_SIZE {
            break;
        }
    }
    Ok(RebuildSource {
        records,
        source_cursor,
        latest_fence_token,
        events_visited,
    })
}

fn decode_source_event(
    tenant_id: i64,
    source_cursor: u64,
    payload: &[u8],
) -> Result<AuthzSourceEvent> {
    let records = decode_authz_tuple_batch_journal_body(payload)?;
    let fence_token = decode_authz_tuple_batch_journal_body_fence(payload)?;
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
        source_cursor,
        revision: u64::try_from(revision)?,
        records,
        fence_token,
    })
}

fn scan_source_events_at(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    snapshot_version: u64,
    tenant_id: i64,
    after_revision: u64,
    limit: usize,
) -> Result<Vec<AuthzSourceEvent>> {
    let prefix = core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(AUTHZ_TUPLE_JOURNAL_ROW_KIND),
        CoreMetaTuplePart::I64(tenant_id),
    ])?;
    let application_prefix = crate::mvcc_product::coremeta_application_prefix(CF_AUTHZ, &prefix)?;
    let mut events = Vec::with_capacity(limit);
    for (_, row) in mvcc.runtime.scan_table_prefix_at(
        TABLE_AUTHZ_TUPLE_JOURNAL_ROW,
        &application_prefix,
        snapshot_version,
    )? {
        let records = decode_authz_tuple_batch_journal_body(&row.value)?;
        let revision = records
            .first()
            .ok_or_else(|| anyhow!("authorization source event has no tuple records"))?
            .revision;
        let revision = u64::try_from(revision)?;
        if revision <= after_revision {
            continue;
        }
        events.push(decode_source_event(tenant_id, revision, &row.value)?);
        if events.len() == limit {
            break;
        }
    }
    Ok(events)
}

fn active_records_at_revision(
    mut records: Vec<AuthzTupleRecord>,
    target_revision: u64,
) -> Vec<AuthzTupleRecord> {
    records.retain(|record| u64::try_from(record.revision).is_ok_and(|r| r <= target_revision));
    authz_segment::active_authz_tuple_records(&records)
}

fn require_available_revision_source(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    revision: u64,
    mutations: &[AuthzTupleRecord],
) -> Result<()> {
    let head = authz_head::read_at_mvcc(mvcc, tenant_id, mvcc.runtime.applied_version()?)?;
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

fn validate_target_revision(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    target_revision: u64,
) -> Result<()> {
    if target_revision == 0 {
        bail!("authorization materialization target revision must be nonzero");
    }
    let current_revision = u64::try_from(latest_authz_revision(mvcc, tenant_id)?)?;
    if target_revision > current_revision {
        bail!(
            "AuthzRevisionUnavailable: current authorization revision is {current_revision}, requested {target_revision}"
        );
    }
    Ok(())
}

async fn load_materialized_segment(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    tenant_id: i64,
    revision: u64,
) -> Result<DecodedAuthzSegment> {
    authz_segment::read_authz_tuple_segment_at_revision(storage, mvcc, tenant_id, revision)
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
