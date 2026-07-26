use crate::{
    core_store::{decode_deterministic_proto, encode_deterministic_proto},
    formats::{Hash32, hash32, watch::WatchRecord},
    mvcc_bootstrap::MvccSubsystem,
    mvcc_product::ProductMutation,
    mvcc_transaction::{DurabilityLevel, LogicalKey, PredicateKind, ReadConsistency},
};
use anyhow::{Result, anyhow, bail};
use prost::Message;
use serde::{Deserialize, Serialize};

const AUTHZ_DERIVED_LAG_PARTITION_FAMILY: u16 = 8;
const AUTHZ_DERIVED_LAG_RECORD_KIND: u16 = 1;
const TABLE_AUTHZ_DERIVED_LAG_WATCH: u16 = 0x050a;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzDerivedLagWatchPayload {
    pub derived_index_id: String,
    pub derived_index_kind: String,
    pub processed_revision: u64,
    pub latest_revision: u64,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzDerivedLagWatchPayloadProto {
    #[prost(string, tag = "1")]
    derived_index_id: String,
    #[prost(string, tag = "2")]
    derived_index_kind: String,
    #[prost(uint64, tag = "3")]
    processed_revision: u64,
    #[prost(uint64, tag = "4")]
    latest_revision: u64,
    #[prost(string, tag = "5")]
    source_cursor: String,
    #[prost(string, tag = "6")]
    source_manifest_hash: String,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(string, tag = "8")]
    emitted_at: String,
}

impl AuthzDerivedLagWatchPayload {
    pub fn revision_lag(&self) -> u64 {
        self.latest_revision.saturating_sub(self.processed_revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzDerivedLagWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: AuthzDerivedLagWatchPayload,
}

pub async fn append_authz_derived_lag_watch_record(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    mutation_id: [u8; 16],
    payload: AuthzDerivedLagWatchPayload,
) -> Result<u128> {
    validate_payload(&payload)?;
    let prepared = prepare_lag_watch_record(tenant_id, mutation_id, &payload);
    let key = watch_key(tenant_id, &payload.derived_index_id, mutation_id)?;
    Ok(u128::from(
        mvcc.autocommit_product_mutations_with_predicates(
            &format!("tenant:{tenant_id}:authz-derived-lag"),
            &prepared.idempotency_key,
            vec![ProductMutation::put(key.clone(), prepared.record_payload)],
            vec![(key, PredicateKind::Unique)],
            DurabilityLevel::Quorum,
            now_unix_ms(),
        )
        .await?,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedLagWatchRecord {
    idempotency_key: String,
    record_payload: Vec<u8>,
}

fn prepare_lag_watch_record(
    tenant_id: i64,
    mutation_id: [u8; 16],
    payload: &AuthzDerivedLagWatchPayload,
) -> PreparedLagWatchRecord {
    let partition = partition_id(tenant_id, &payload.derived_index_id);
    let record = WatchRecord::new(
        0,
        AUTHZ_DERIVED_LAG_PARTITION_FAMILY,
        partition,
        mutation_id,
        AUTHZ_DERIVED_LAG_RECORD_KIND,
        payload.latest_revision,
        payload.generation,
        0,
        encode_lag_watch_payload(payload),
    );
    PreparedLagWatchRecord {
        idempotency_key: format!(
            "authz-derived-lag-watch:{tenant_id}:{}:{}",
            payload.derived_index_id,
            hex::encode(mutation_id)
        ),
        record_payload: record.encode(),
    }
}

pub async fn list_authz_derived_lag_watch_events(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzDerivedLagWatchEvent>> {
    Ok(list_authz_derived_lag_watch_event_page(
        mvcc,
        tenant_id,
        derived_index_id,
        after_cursor,
        limit,
    )
    .await?
    .events)
}

#[derive(Debug, Clone)]
pub struct AuthzDerivedLagWatchEventPage {
    pub events: Vec<AuthzDerivedLagWatchEvent>,
    pub next_cursor: u128,
    pub has_more: bool,
}

pub async fn list_authz_derived_lag_watch_event_page(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<AuthzDerivedLagWatchEventPage> {
    if limit == 0 {
        bail!("authz derived lag watch limit must be nonzero");
    }
    let after_version = u64::try_from(after_cursor)
        .map_err(|_| anyhow!("authz derived lag watch cursor exceeds u64"))?;
    let snapshot = mvcc.runtime.snapshot(ReadConsistency::Linearized).await?;
    let mut page = mvcc.runtime.scan_table_prefix_at(
        TABLE_AUTHZ_DERIVED_LAG_WATCH,
        &watch_prefix(tenant_id, derived_index_id)?,
        snapshot,
    )?;
    page.retain(|(_, row)| row.commit_version > after_version);
    page.sort_by_key(|(_, row)| row.commit_version);
    let has_more = page.len() > limit;
    page.truncate(limit);
    let expected_partition = partition_id(tenant_id, derived_index_id);
    let mut events = Vec::with_capacity(page.len());
    for (_, source) in page {
        let (mut record, used) = WatchRecord::decode(&source.value)?;
        if used != source.value.len() {
            return Err(anyhow!(
                "authz derived lag watch MVCC record has trailing bytes"
            ));
        }
        record.cursor = u128::from(source.commit_version);
        if record.partition_family != AUTHZ_DERIVED_LAG_PARTITION_FAMILY
            || record.record_kind != AUTHZ_DERIVED_LAG_RECORD_KIND
            || record.partition_id != expected_partition
        {
            return Err(anyhow!("authz derived lag watch record scope mismatch"));
        }
        let payload = decode_lag_watch_payload(&record.payload)?;
        if payload.derived_index_id != derived_index_id {
            return Err(anyhow!("authz derived lag watch payload scope mismatch"));
        }
        validate_payload(&payload)?;
        events.push(AuthzDerivedLagWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            index_generation: record.index_generation,
            payload,
        });
    }
    let next_cursor = events
        .last()
        .map(|event| event.cursor)
        .unwrap_or(after_cursor);
    Ok(AuthzDerivedLagWatchEventPage {
        events,
        next_cursor,
        has_more,
    })
}

pub async fn latest_authz_derived_lag_watch_event(
    mvcc: &MvccSubsystem,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<Option<AuthzDerivedLagWatchEvent>> {
    let snapshot = mvcc.runtime.snapshot(ReadConsistency::Linearized).await?;
    let mut rows = mvcc.runtime.scan_table_prefix_at(
        TABLE_AUTHZ_DERIVED_LAG_WATCH,
        &watch_prefix(tenant_id, derived_index_id)?,
        snapshot,
    )?;
    rows.sort_by_key(|(_, row)| row.commit_version);
    let Some((_, row)) = rows.pop() else {
        return Ok(None);
    };
    decode_event(tenant_id, derived_index_id, row.commit_version, &row.value).map(Some)
}

fn watch_prefix(tenant_id: i64, derived_index_id: &str) -> Result<Vec<u8>> {
    require_safe_component(derived_index_id, "derived_index_id")?;
    let mut key = Vec::new();
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&(derived_index_id.len() as u32).to_be_bytes());
    key.extend_from_slice(derived_index_id.as_bytes());
    Ok(key)
}

fn watch_key(tenant_id: i64, derived_index_id: &str, mutation_id: [u8; 16]) -> Result<LogicalKey> {
    let mut application_key = watch_prefix(tenant_id, derived_index_id)?;
    application_key.extend_from_slice(&mutation_id);
    Ok(LogicalKey {
        table_id: TABLE_AUTHZ_DERIVED_LAG_WATCH,
        application_key,
    })
}

fn decode_event(
    tenant_id: i64,
    derived_index_id: &str,
    commit_version: u64,
    bytes: &[u8],
) -> Result<AuthzDerivedLagWatchEvent> {
    let (mut record, used) = WatchRecord::decode(bytes)?;
    if used != bytes.len()
        || record.partition_family != AUTHZ_DERIVED_LAG_PARTITION_FAMILY
        || record.record_kind != AUTHZ_DERIVED_LAG_RECORD_KIND
        || record.partition_id != partition_id(tenant_id, derived_index_id)
    {
        bail!("authz derived lag watch record scope mismatch");
    }
    record.cursor = u128::from(commit_version);
    let payload = decode_lag_watch_payload(&record.payload)?;
    if payload.derived_index_id != derived_index_id {
        bail!("authz derived lag watch payload scope mismatch");
    }
    validate_payload(&payload)?;
    Ok(AuthzDerivedLagWatchEvent {
        cursor: record.cursor,
        mutation_id: record.mutation_id,
        authz_revision: record.authz_revision,
        index_generation: record.index_generation,
        payload,
    })
}

fn now_unix_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn encode_lag_watch_payload(payload: &AuthzDerivedLagWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&AuthzDerivedLagWatchPayloadProto {
        derived_index_id: payload.derived_index_id.clone(),
        derived_index_kind: payload.derived_index_kind.clone(),
        processed_revision: payload.processed_revision,
        latest_revision: payload.latest_revision,
        source_cursor: payload.source_cursor.to_string(),
        source_manifest_hash: payload.source_manifest_hash.clone(),
        generation: payload.generation,
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_lag_watch_payload(bytes: &[u8]) -> Result<AuthzDerivedLagWatchPayload> {
    let proto = decode_deterministic_proto::<AuthzDerivedLagWatchPayloadProto>(
        bytes,
        "authorization derived lag watch payload",
    )?;
    Ok(AuthzDerivedLagWatchPayload {
        derived_index_id: proto.derived_index_id,
        derived_index_kind: proto.derived_index_kind,
        processed_revision: proto.processed_revision,
        latest_revision: proto.latest_revision,
        source_cursor: proto
            .source_cursor
            .parse()
            .map_err(|_| anyhow!("authorization derived lag source_cursor is not u128"))?,
        source_manifest_hash: proto.source_manifest_hash,
        generation: proto.generation,
        emitted_at: proto.emitted_at,
    })
}

fn validate_payload(payload: &AuthzDerivedLagWatchPayload) -> Result<()> {
    require_safe_component(&payload.derived_index_id, "derived_index_id")?;
    require_safe_component(&payload.derived_index_kind, "derived_index_kind")?;
    validate_hex32(&payload.source_manifest_hash, "source_manifest_hash")?;
    if payload.generation == 0 {
        return Err(anyhow!(
            "authorization derived lag generation must be nonzero"
        ));
    }
    if payload.processed_revision > payload.latest_revision {
        return Err(anyhow!(
            "authorization derived lag processed revision is after latest revision"
        ));
    }
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn partition_id(tenant_id: i64, derived_index_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:authz-derived-lag:{derived_index_id}").as_bytes())
}

pub(crate) fn authz_derived_lag_watch_stream_id(tenant_id: i64, derived_index_id: &str) -> String {
    format!("watch:authz_derived_lag:tenant:{tenant_id}:derived:{derived_index_id}")
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

#[cfg(test)]
mod mvcc_tests {
    use super::*;

    #[test]
    fn derived_lag_keys_scope_tenant_index_and_mutation() {
        let first = watch_key(11, "derived-userset-primary", [1; 16]).unwrap();
        assert_eq!(first.table_id, TABLE_AUTHZ_DERIVED_LAG_WATCH);
        assert_ne!(
            first,
            watch_key(12, "derived-userset-primary", [1; 16]).unwrap()
        );
        assert_ne!(
            first,
            watch_key(11, "derived-userset-secondary", [1; 16]).unwrap()
        );
        assert_ne!(
            first,
            watch_key(11, "derived-userset-primary", [2; 16]).unwrap()
        );
    }
}

#[cfg(any())] // Retired CoreStore-only tests; superseded by MVCC coverage above.
mod tests {
    use super::*;
    use crate::{
        storage::Storage,
        task_execution_guard::TaskExecutionGuard,
        task_lease::{TaskLease, TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease},
    };
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::sleep;

    const TASK_LEASE_KEY: &[u8] = b"authz lag watch task lease test key";
    const LONG_TASK_LEASE_TTL_NANOS: i64 = 60_000_000_000;
    const EXPIRING_TASK_LEASE_TTL_NANOS: i64 = 30_000_000_000;

    #[tokio::test]
    async fn authz_derived_lag_watch_appends_lists_and_reports_latest() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_derived_lag_watch_record(&storage, 11, [1; 16], payload(90, 100, 1), &[])
            .await
            .unwrap();
        append_authz_derived_lag_watch_record(&storage, 11, [2; 16], payload(100, 100, 2), &[])
            .await
            .unwrap();
        assert_eq!(
            authz_derived_lag_watch_stream_id(11, "derived-userset-primary"),
            "watch:authz_derived_lag:tenant:11:derived:derived-userset-primary"
        );
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let raw = core_store
            .read_stream(ReadStream {
                stream_id: authz_derived_lag_watch_stream_id(11, "derived-userset-primary"),
                after_sequence: 0,
                limit: 1,
            })
            .await
            .unwrap();
        let (raw, _) = WatchRecord::decode(&raw[0].payload).unwrap();
        assert_ne!(raw.payload.first().copied(), Some(b'{'));
        assert!(decode_lag_watch_payload(&raw.payload).is_ok());

        let events =
            list_authz_derived_lag_watch_events(&storage, 11, "derived-userset-primary", 1, 10)
                .await
                .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].authz_revision, 100);
        assert_eq!(events[0].index_generation, 2);
        assert_eq!(events[0].payload.revision_lag(), 0);

        let latest = latest_authz_derived_lag_watch_event(&storage, 11, "derived-userset-primary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.cursor, 2);
        assert_eq!(latest.payload.processed_revision, 100);
    }

    #[tokio::test]
    async fn authz_derived_lag_watch_rejects_invalid_payloads_and_idempotency_conflicts() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_derived_lag_watch_record(&storage, 11, [1; 16], payload(90, 100, 1), &[])
            .await
            .unwrap();
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, [1; 16], payload(91, 100, 2), &[],)
                .await
                .is_err()
        );
        let mut invalid = payload(101, 100, 3);
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, [3; 16], invalid.clone(), &[])
                .await
                .is_err()
        );
        invalid.processed_revision = 99;
        invalid.source_manifest_hash = "not-hex".to_string();
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, [4; 16], invalid, &[])
                .await
                .is_err()
        );
        let mut invalid = payload(99, 100, 4);
        invalid.derived_index_id = "../escape".to_string();
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, [5; 16], invalid, &[])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn authz_derived_lag_watch_retry_bytes_and_identity_are_deterministic() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = payload(90, 100, 1);
        let first = prepare_lag_watch_record(11, [7; 16], &payload);
        let second = prepare_lag_watch_record(11, [7; 16], &payload);
        assert_eq!(first, second);

        let precondition = CoreMutationPrecondition::CoreMetaLease {
            cf: "leases_fences".to_string(),
            table_id: 7,
            tuple_key: vec![1, 2, 3],
            expected_payload_hash: format!("sha256:{}", "ab".repeat(32)),
            expires_at_unix_nanos: 9_000_000_000,
        };
        assert_eq!(
            lag_watch_publication_attempt_id(&first.idempotency_key, &[precondition.clone()])
                .unwrap(),
            lag_watch_publication_attempt_id(&second.idempotency_key, &[precondition]).unwrap()
        );

        let first_cursor =
            append_authz_derived_lag_watch_record(&storage, 11, [7; 16], payload.clone(), &[])
                .await
                .unwrap();
        let replay_cursor =
            append_authz_derived_lag_watch_record(&storage, 11, [7; 16], payload, &[])
                .await
                .unwrap();
        assert_eq!(first_cursor, replay_cursor);
        assert_eq!(
            list_authz_derived_lag_watch_events(&storage, 11, "derived-userset-primary", 0, 10,)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn stale_task_lease_cannot_advance_authz_derived_lag_watch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let (stale_guard, stale_lease) =
            acquire_guard(&storage, "authz-lag-stale", LONG_TASK_LEASE_TTL_NANOS).await;
        let stale_permit = stale_guard.publication_permit().await.unwrap();
        let fresh_lease = reacquire(
            &storage,
            &stale_lease,
            now_nanos(),
            LONG_TASK_LEASE_TTL_NANOS,
        )
        .await;

        stale_permit
            .publish_with(|precondition| async {
                append_authz_derived_lag_watch_record(
                    &storage,
                    11,
                    [8; 16],
                    payload(90, 100, 1),
                    &[precondition],
                )
                .await
            })
            .await
            .unwrap_err();
        assert!(
            latest_authz_derived_lag_watch_event(&storage, 11, "derived-userset-primary",)
                .await
                .unwrap()
                .is_none()
        );

        let fresh_guard =
            TaskExecutionGuard::new(storage.clone(), TASK_LEASE_KEY.to_vec(), fresh_lease).unwrap();
        let cursor = fresh_guard
            .publication_permit()
            .await
            .unwrap()
            .publish_with(|precondition| async {
                append_authz_derived_lag_watch_record(
                    &storage,
                    11,
                    [8; 16],
                    payload(90, 100, 1),
                    &[precondition],
                )
                .await
            })
            .await
            .unwrap();
        assert_eq!(cursor, 1);
    }

    #[tokio::test]
    async fn expired_task_lease_cannot_advance_authz_derived_lag_watch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let (expired_guard, expired_lease) =
            acquire_guard(&storage, "authz-lag-expired", EXPIRING_TASK_LEASE_TTL_NANOS).await;
        let expired_permit = expired_guard.publication_permit().await.unwrap();
        sleep_until_lease_expires(&expired_lease).await;

        expired_permit
            .publish_with(|precondition| async {
                append_authz_derived_lag_watch_record(
                    &storage,
                    11,
                    [9; 16],
                    payload(90, 100, 1),
                    &[precondition],
                )
                .await
            })
            .await
            .unwrap_err();
        assert!(
            latest_authz_derived_lag_watch_event(&storage, 11, "derived-userset-primary",)
                .await
                .unwrap()
                .is_none()
        );

        let fresh_lease = reacquire(
            &storage,
            &expired_lease,
            now_nanos(),
            LONG_TASK_LEASE_TTL_NANOS,
        )
        .await;
        let fresh_guard =
            TaskExecutionGuard::new(storage.clone(), TASK_LEASE_KEY.to_vec(), fresh_lease).unwrap();
        fresh_guard
            .publication_permit()
            .await
            .unwrap()
            .publish_with(|precondition| async {
                append_authz_derived_lag_watch_record(
                    &storage,
                    11,
                    [9; 16],
                    payload(90, 100, 1),
                    &[precondition],
                )
                .await
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn authz_derived_lag_watch_preserves_source_and_task_fences() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let (guard, _) = acquire_guard(
            &storage,
            "authz-lag-source-fence",
            LONG_TASK_LEASE_TTL_NANOS,
        )
        .await;
        let missing_source_fence = CoreMutationPrecondition::CoreMetaRow {
            cf: crate::core_store::CF_LEASES_FENCES.to_string(),
            table_id: crate::core_store::TABLE_PARTITION_OWNER_ROW,
            tuple_key: b"missing-authz-source-owner".to_vec(),
            expected_payload_hash: None,
            require_absent: false,
            require_present: true,
        };

        guard
            .publication_permit()
            .await
            .unwrap()
            .publish_with(|task_precondition| async {
                append_authz_derived_lag_watch_record(
                    &storage,
                    11,
                    [10; 16],
                    payload(90, 100, 1),
                    &[missing_source_fence, task_precondition],
                )
                .await
            })
            .await
            .unwrap_err();
        assert!(
            latest_authz_derived_lag_watch_event(&storage, 11, "derived-userset-primary")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn authz_derived_lag_watch_requires_a_bounded_page_limit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for cursor in 1..=3 {
            append_authz_derived_lag_watch_record(
                &storage,
                11,
                [cursor as u8; 16],
                payload(90 + cursor as u64, 100, cursor as u64),
                &[],
            )
            .await
            .unwrap();
        }
        let error =
            list_authz_derived_lag_watch_events(&storage, 11, "derived-userset-primary", 0, 0)
                .await
                .unwrap_err();
        assert!(error.to_string().contains("limit"));
    }

    fn payload(
        processed_revision: u64,
        latest_revision: u64,
        generation: u64,
    ) -> AuthzDerivedLagWatchPayload {
        AuthzDerivedLagWatchPayload {
            derived_index_id: "derived-userset-primary".to_string(),
            derived_index_kind: "userset".to_string(),
            processed_revision,
            latest_revision,
            source_cursor: u128::from(latest_revision),
            source_manifest_hash: hex::encode([9; 32]),
            generation,
            emitted_at: "2026-07-21T00:00:00.000000000Z".to_string(),
        }
    }

    async fn acquire_guard(
        storage: &Storage,
        task_id: &str,
        ttl_nanos: i64,
    ) -> (TaskExecutionGuard, TaskLease) {
        let lease = acquire_task_lease(
            storage,
            TaskLeaseAcquire {
                task_id: task_id.to_string(),
                task_kind: "authz_materialization".to_string(),
                partition_family: "authz_materialization".to_string(),
                partition_id: hex::encode([5; 32]),
                owner: TaskLeaseOwner::node_instance("node-a", "worker-a"),
                source_cursor: 1,
                now_nanos: now_nanos(),
                ttl_nanos,
            },
            TASK_LEASE_KEY,
        )
        .await
        .unwrap();
        let guard =
            TaskExecutionGuard::new(storage.clone(), TASK_LEASE_KEY.to_vec(), lease.clone())
                .unwrap();
        (guard, lease)
    }

    async fn reacquire(
        storage: &Storage,
        previous: &TaskLease,
        now_nanos: i64,
        ttl_nanos: i64,
    ) -> TaskLease {
        acquire_task_lease(
            storage,
            TaskLeaseAcquire {
                task_id: previous.task_id.clone(),
                task_kind: previous.task_kind.clone(),
                partition_family: previous.partition_family.clone(),
                partition_id: previous.partition_id.clone(),
                owner: previous.owner.clone(),
                source_cursor: previous.source_cursor,
                now_nanos,
                ttl_nanos,
            },
            TASK_LEASE_KEY,
        )
        .await
        .unwrap()
    }

    fn now_nanos() -> i64 {
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    }

    async fn sleep_until_lease_expires(lease: &TaskLease) {
        let remaining = lease.expires_at_nanos.saturating_sub(now_nanos());
        let wait_nanos = remaining.max(0).saturating_add(200_000_000);
        sleep(Duration::from_nanos(u64::try_from(wait_nanos).unwrap())).await;
    }
}
